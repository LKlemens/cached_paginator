defmodule CachedPaginator do
  @moduledoc """
  ETS-backed pagination cache with cursor support.

  Cache expensive query results once, paginate efficiently with stable cursors.
  Multiple users requesting the same data share the cached result, avoiding
  repeated expensive queries.

  ## Features

  - **Query caching**: Run expensive query once, serve all page requests from cache
  - **Locking**: Concurrent requests for same data wait for ongoing query
  - **Cursor stability**: Same result set across all pages during session
  - **Two-tier TTL**: Fresh data for new sessions, extended TTL for ongoing pagination
  - **Backpressure**: Max tables limit prevents unbounded growth
  - **Telemetry**: Full observability (hits, misses, table count, memory usage)

  ## Usage

      # Start a cache instance
      CachedPaginator.start_link(name: :my_cache)

      # Cache query results and paginate
      {data, cursor} = CachedPaginator.get_or_create(:my_cache, filters, fn ->
        # expensive query
        MyRepo.all(query)
      end)

      # Fetch a page range
      ids = CachedPaginator.fetch_range(table, 0, 19)

  ## TTL Rules

  | Access Type                      | TTL        | Purpose                        |
  |----------------------------------|------------|--------------------------------|
  | No cursor OR first_page          | fresh_ttl  | Fresh data for new sessions    |
  | With cursor, not first_page      | max_ttl    | Session can continue           |

  ## Configuration

      CachedPaginator.start_link(
        name: :my_cache,           # required
        fresh_ttl: 300,            # ms, default: 300
        max_ttl: 10_000,           # ms, default: 10_000
        sweep_interval: 5_000,     # ms, default: 5_000
        max_tables: 100            # default: 100
      )
  """

  use GenServer

  require Logger

  @default_fresh_ttl 300
  @default_max_ttl 10_000
  @default_sweep_interval 5_000
  @default_max_tables 100
  @wait_poll_interval 50

  @type filters :: term()
  @type cache_key :: {filter_hash :: non_neg_integer(), created_at :: integer()}
  @type cursor :: binary()
  @type table_ref :: :ets.tid()
  @type cache_data :: {table_ref(), non_neg_integer()}
  @type name :: GenServer.name()

  # API

  @doc """
  Starts a CachedPaginator instance.

  ## Options

  - `:name` (required) - The name to register the GenServer
  - `:fresh_ttl` - TTL in ms for fresh cache entries (default: 300)
  - `:max_ttl` - TTL in ms for cursor-based access (default: 10_000)
  - `:sweep_interval` - Interval in ms for cleanup sweep (default: 5_000)
  - `:max_tables` - Maximum number of ETS tables (default: 100)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Store indexed data. Always creates a new cache entry with current timestamp.
  Returns both cache_data and the cache_key (composite key).
  """
  @spec store(name(), filters(), [term()]) :: {cache_data(), cache_key()}
  def store(name, filters, ids) do
    GenServer.call(name, {:store, filters, ids})
  end

  @doc """
  Get cached data or create it if missing, with cursor-based session support.

  ## Options

  - `:first_page` - when true, ignores cursor and uses fresh TTL.
    Use for page 1 requests to ensure users get fresh data on restart.

  ## TTL Behavior

  - `first_page: true` → uses fresh TTL
  - cursor matches filters, not first page → uses max TTL
  - no cursor or mismatch → uses fresh TTL

  Returns `{cache_data, cursor}` where cursor can be passed to subsequent requests.
  """
  @spec get_or_create(name(), filters(), (-> [term()]), cursor() | nil, keyword()) ::
          {cache_data(), cursor()}
  def get_or_create(name, filters, fetch_fn, cursor \\ nil, opts \\ []) do
    config = get_config(name)
    filter_hash = :erlang.phash2(filters)
    first_page = Keyword.get(opts, :first_page, false)

    result =
      case {decode_cursor(cursor), first_page} do
        {_, true} ->
          get(name, filters, config)

        {{:ok, {cursor_filter_hash, _} = cursor_key}, false}
        when cursor_filter_hash == filter_hash ->
          get_by_cursor(name, cursor_key, config)

        _ ->
          get(name, filters, config)
      end

    case result do
      {:ok, data, cache_key} ->
        CachedPaginator.Telemetry.emit_hit(name, filter_hash)
        {data, encode_cursor(cache_key)}

      :miss ->
        CachedPaginator.Telemetry.emit_miss(name, filter_hash)
        {data, cache_key} = locked_fetch_and_store(name, filters, fetch_fn, config)
        {data, encode_cursor(cache_key)}
    end
  end

  @doc """
  Get cached data by cursor. Checks max TTL.

  Direct ETS read using composite key - no GenServer call.
  Returns :miss if cache is older than max TTL.
  """
  @spec get_by_cursor(name(), cache_key(), map()) :: {:ok, cache_data(), cache_key()} | :miss
  def get_by_cursor(name, {filter_hash, created_at} = key, config \\ nil)
      when is_integer(filter_hash) and is_integer(created_at) do
    config = config || get_config(name)
    now = System.monotonic_time(:millisecond)

    case :ets.lookup(config.registry, key) do
      [{^key, table, size}] when now - created_at <= config.max_ttl ->
        {:ok, {table, size}, key}

      _ ->
        :miss
    end
  end

  @doc """
  Get indexed data for filters. Direct ETS read - no GenServer call.

  Uses latest_index for O(1) lookup. Returns fresh cache (< fresh_ttl old).
  Use for first page / new session requests.
  """
  @spec get(name(), filters(), map()) :: {:ok, cache_data(), cache_key()} | :miss
  def get(name, filters, config \\ nil) do
    config = config || get_config(name)
    filter_hash = :erlang.phash2(filters)
    now = System.monotonic_time(:millisecond)

    case :ets.lookup(config.latest_index, filter_hash) do
      [{^filter_hash, created_at, table, size}] when now - created_at <= config.fresh_ttl ->
        {:ok, {table, size}, {filter_hash, created_at}}

      _ ->
        :miss
    end
  end

  @doc """
  Clears all cached pagination data.
  """
  @spec clear(name()) :: :ok
  def clear(name) do
    GenServer.call(name, :clear)
  end

  @doc """
  Fetch a range of IDs from the cache table using direct key lookup.
  This is O(page_size) regardless of table size or position.
  """
  @spec fetch_range(table_ref(), non_neg_integer(), non_neg_integer()) :: [term()]
  def fetch_range(table, start_idx, end_idx) do
    Enum.reduce(end_idx..start_idx//-1, [], fn idx, acc ->
      case :ets.lookup(table, idx) do
        [{^idx, id}] -> [id | acc]
        [] -> acc
      end
    end)
  end

  @doc """
  Encodes a cache key into a cursor string.
  """
  @spec encode_cursor(cache_key()) :: cursor()
  def encode_cursor({_filter_hash, _created_at} = cache_key) do
    cache_key
    |> :erlang.term_to_binary()
    |> Base.url_encode64(padding: false)
  end

  @doc """
  Decodes a cursor string into a cache key.
  """
  @spec decode_cursor(cursor() | nil) :: {:ok, cache_key()} | :error
  def decode_cursor(nil), do: :error

  def decode_cursor(cursor) when is_binary(cursor) do
    with {:ok, binary} <- Base.url_decode64(cursor, padding: false),
         {:ok, {filter_hash, created_at} = key}
         when is_integer(filter_hash) and is_integer(created_at) <- safe_binary_to_term(binary) do
      {:ok, key}
    else
      _ -> :error
    end
  end

  @doc """
  Returns current stats for the cache instance.
  """
  @spec stats(name()) :: map()
  def stats(name) do
    GenServer.call(name, :stats)
  end

  # Private API functions

  defp get_config(name) do
    :persistent_term.get({__MODULE__, name, :config})
  end

  defp locked_fetch_and_store(name, filters, fetch_fn, config) do
    filter_hash = :erlang.phash2(filters)

    if :ets.insert_new(config.locks, {filter_hash, true}) do
      try do
        start_time = System.monotonic_time()
        ids = fetch_fn.()
        duration = System.monotonic_time() - start_time

        CachedPaginator.Telemetry.emit_store(name, filter_hash, length(ids), duration)
        store(name, filters, ids)
      after
        :ets.delete(config.locks, filter_hash)
      end
    else
      Process.sleep(@wait_poll_interval)

      case get(name, filters, config) do
        {:ok, data, cache_key} -> {data, cache_key}
        :miss -> locked_fetch_and_store(name, filters, fetch_fn, config)
      end
    end
  end

  defp safe_binary_to_term(binary) do
    {:ok, :erlang.binary_to_term(binary, [:safe])}
  rescue
    ArgumentError -> :error
  end

  # GenServer callbacks

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    fresh_ttl = Keyword.get(opts, :fresh_ttl, @default_fresh_ttl)
    max_ttl = Keyword.get(opts, :max_ttl, @default_max_ttl)
    sweep_interval = Keyword.get(opts, :sweep_interval, @default_sweep_interval)
    max_tables = Keyword.get(opts, :max_tables, @default_max_tables)

    # create unique ETS table names for this instance
    registry = :"#{name}_registry"
    latest_index = :"#{name}_latest_index"
    locks = :"#{name}_locks"

    :ets.new(registry, [:set, :public, :named_table, read_concurrency: true])
    :ets.new(latest_index, [:set, :public, :named_table, read_concurrency: true])
    :ets.new(locks, [:set, :public, :named_table])

    config = %{
      name: name,
      registry: registry,
      latest_index: latest_index,
      locks: locks,
      fresh_ttl: fresh_ttl,
      max_ttl: max_ttl,
      max_tables: max_tables
    }

    :persistent_term.put({__MODULE__, name, :config}, config)

    schedule_sweep(sweep_interval)

    state = %{
      config: config,
      sweep_interval: sweep_interval,
      pool: [],
      table_count: 0
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:store, filters, ids}, from, state) do
    %{config: config, pool: pool, table_count: table_count} = state

    if table_count >= config.max_tables and pool == [] do
      Logger.error(
        "[CachedPaginator] max tables (#{config.max_tables}) reached, waiting for available table"
      )

      CachedPaginator.Telemetry.emit_wait(config.name)
      send(self(), {:retry_store, filters, ids, from})
      {:noreply, state}
    else
      {result, new_state} = do_store(filters, ids, state)
      {:reply, result, new_state}
    end
  end

  @impl true
  def handle_call(:clear, _from, state) do
    %{config: config, pool: pool} = state

    tables =
      :ets.tab2list(config.registry)
      |> Enum.map(fn {_key, table, _size} -> table end)

    :ets.delete_all_objects(config.registry)
    :ets.delete_all_objects(config.latest_index)
    :ets.delete_all_objects(config.locks)

    {:reply, :ok, %{state | pool: tables ++ pool}}
  end

  @impl true
  def handle_call(:stats, _from, state) do
    %{config: config, pool: pool, table_count: table_count} = state

    memory_bytes = calculate_memory(config.registry, pool)

    stats = %{
      table_count: table_count,
      pool_size: length(pool),
      memory_bytes: memory_bytes,
      max_tables: config.max_tables
    }

    {:reply, stats, state}
  end

  @impl true
  def handle_info({:retry_store, filters, ids, from}, state) do
    %{config: config, pool: pool, table_count: table_count} = state

    if table_count >= config.max_tables and pool == [] do
      Process.send_after(self(), {:retry_store, filters, ids, from}, @wait_poll_interval)
      {:noreply, state}
    else
      {result, new_state} = do_store(filters, ids, state)
      GenServer.reply(from, result)
      {:noreply, new_state}
    end
  end

  @impl true
  def handle_info(:sweep_expired, state) do
    %{config: config, pool: pool, table_count: table_count, sweep_interval: sweep_interval} =
      state

    now = System.monotonic_time(:millisecond)
    expired_tables = collect_expired(config.registry, config.max_ttl, now)

    Enum.each(expired_tables, fn {{filter_hash, created_at} = key, _table} ->
      :ets.delete(config.registry, key)

      case :ets.lookup(config.latest_index, filter_hash) do
        [{^filter_hash, ^created_at, _, _}] ->
          :ets.delete(config.latest_index, filter_hash)

        _ ->
          :ok
      end
    end)

    expired_table_refs = Enum.map(expired_tables, fn {_key, table} -> table end)
    new_pool = expired_table_refs ++ pool

    # emit telemetry with current stats
    active_tables = table_count - length(expired_table_refs)
    memory_bytes = calculate_memory(config.registry, new_pool)

    CachedPaginator.Telemetry.emit_sweep(config.name, %{
      table_count: active_tables,
      pool_size: length(new_pool),
      memory_bytes: memory_bytes,
      expired_count: length(expired_tables)
    })

    schedule_sweep(sweep_interval)

    {:noreply, %{state | pool: new_pool, table_count: active_tables}}
  end

  defp do_store(filters, ids, state) do
    %{config: config, pool: pool, table_count: table_count} = state

    filter_hash = :erlang.phash2(filters)
    now = System.monotonic_time(:millisecond)
    cache_key = {filter_hash, now}

    {table, new_pool, new_table_count} =
      case pool do
        [t | rest] ->
          :ets.delete_all_objects(t)
          {t, rest, table_count}

        [] ->
          t = :ets.new(:cached_paginator_data, [:ordered_set, :public, read_concurrency: true])
          {t, [], table_count + 1}
      end

    {table, size} = populate_table(table, ids)

    :ets.insert(config.registry, {cache_key, table, size})
    :ets.insert(config.latest_index, {filter_hash, now, table, size})

    result = {{table, size}, cache_key}
    new_state = %{state | pool: new_pool, table_count: new_table_count}

    {result, new_state}
  end

  defp schedule_sweep(interval) do
    Process.send_after(self(), :sweep_expired, interval)
  end

  defp collect_expired(registry, max_ttl, now) do
    :ets.foldl(
      fn {{_filter_hash, created_at} = key, table, _size}, acc ->
        if now - created_at > max_ttl do
          [{key, table} | acc]
        else
          acc
        end
      end,
      [],
      registry
    )
  end

  defp populate_table(table, ids) do
    entries = Enum.with_index(ids, fn id, idx -> {idx, id} end)
    :ets.insert(table, entries)
    {table, length(ids)}
  end

  defp calculate_memory(registry, pool) do
    # memory for registry tables
    registry_tables =
      :ets.foldl(
        fn {_key, table, _size}, acc -> [table | acc] end,
        [],
        registry
      )

    all_tables = registry_tables ++ pool

    Enum.reduce(all_tables, 0, fn table, acc ->
      case :ets.info(table, :memory) do
        :undefined -> acc
        # :ets.info returns memory in words, convert to bytes
        words -> acc + words * :erlang.system_info(:wordsize)
      end
    end)
  end
end
