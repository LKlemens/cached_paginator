defmodule CachedPaginator do
  @moduledoc """
  ETS-backed pagination cache with keyset cursor support.

  Cache expensive query results once, paginate efficiently with stable cursors.
  Multiple users requesting the same data share the cached result, avoiding
  repeated expensive queries.

  Uses a pre-initialized pool of shared ETS `ordered_set` tables. Multiple query
  results coexist in each table using composite keys `{cache_key, sort_key}`.

  ## Features

  - **Query caching**: Run expensive query once, serve all page requests from cache
  - **Locking**: Concurrent requests for same data wait for ongoing query
  - **Keyset pagination**: Cursor encodes last sort key, stable across cache transitions
  - **Telemetry**: Full observability (hits, misses, table count, memory usage)

  ## Data Format

  Items must be tuples where the last element is the value and all preceding
  elements form the composite sort key:

      # Single sort key:
      {sort_key, value}

      # Composite sort key:
      {sort_key1, sort_key2, value}

  ## Usage

      # Start a cache instance
      CachedPaginator.start_link(name: :my_cache)

      # Cache query results and paginate
      {cache_location, cursor} = CachedPaginator.get_or_create(:my_cache, filters, fn ->
        # expensive query — return list of {sort_key, value} tuples
        # e.g select(query, [m], {m.start_time, m.id})
        MyRepo.all(query)
      end)

      # Fetch a page using keyset cursor
      {table, cache_key, size} = cache_location
      {items, updated_cursor} = CachedPaginator.fetch_after(table, cache_key, cursor, 20)

  ## Configuration

      CachedPaginator.start_link(
        name: :my_cache,           # required
        ttl: 300,                  # ms, default: 300
        sweep_interval: 5_000,     # ms, default: 5_000
        pool_size: 100             # default: 100
      )
  """

  @doc """
  Injects wrapper functions with the cache name pre-filled.

  Reads configuration from application env at startup, merged with runtime opts.

  ## Usage

      defmodule MyApp.PaginationCache do
        use CachedPaginator, otp_app: :my_app
      end

  Config in `config/config.exs`:

      config :my_app, MyApp.PaginationCache,
        ttl: 300,
        sweep_interval: 5_000,
        pool_size: 100

  ## Injected Functions

  - `start_link/1` — starts the cache, merging app env + runtime opts
  - `child_spec/1` — for supervision tree placement
  - `get/1` — lookup by filters
  - `get_or_create/2,3` — cache-through with optional cursor
  - `store/2` — store items for filters
  - `clear/0` — clear all cached data
  - `stats/0` — memory and pool stats
  - `fetch_after/4`, `encode_cursor/2`, `decode_cursor/1` — delegated directly
  """
  defmacro __using__(opts) do
    otp_app = Keyword.fetch!(opts, :otp_app)

    quote do
      @otp_app unquote(otp_app)

      @spec start_link(keyword()) :: GenServer.on_start()
      def start_link(opts \\ []) do
        app_config = Application.get_env(@otp_app, __MODULE__, [])

        merged =
          app_config
          |> Keyword.merge(opts)
          |> Keyword.put_new(:name, __MODULE__)

        CachedPaginator.start_link(merged)
      end

      def child_spec(opts) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]},
          type: :worker
        }
      end

      @spec get(CachedPaginator.filters()) ::
              {:ok, CachedPaginator.cache_location(), CachedPaginator.cache_key()} | :miss
      def get(filters), do: CachedPaginator.get(__MODULE__, filters)

      @spec get_or_create(CachedPaginator.filters(), (-> [tuple()])) ::
              {CachedPaginator.cache_location(), CachedPaginator.cursor()}
      def get_or_create(filters, fetch_fn),
        do: CachedPaginator.get_or_create(__MODULE__, filters, fetch_fn)

      @spec get_or_create(
              CachedPaginator.filters(),
              (-> [tuple()]),
              CachedPaginator.cursor() | nil
            ) ::
              {CachedPaginator.cache_location(), CachedPaginator.cursor()}
      def get_or_create(filters, fetch_fn, cursor),
        do: CachedPaginator.get_or_create(__MODULE__, filters, fetch_fn, cursor)

      @spec store(CachedPaginator.filters(), [tuple()]) ::
              {CachedPaginator.cache_location(), CachedPaginator.cache_key()}
      def store(filters, items), do: CachedPaginator.store(__MODULE__, filters, items)

      @spec clear() :: :ok
      def clear, do: CachedPaginator.clear(__MODULE__)

      @spec stats() :: map()
      def stats, do: CachedPaginator.stats(__MODULE__)

      defdelegate fetch_after(table, cache_key, cursor, limit), to: CachedPaginator
      defdelegate encode_cursor(cache_key, last_sort_key), to: CachedPaginator
      defdelegate decode_cursor(cursor), to: CachedPaginator

      defoverridable start_link: 1, child_spec: 1
    end
  end

  use GenServer

  require Logger

  @default_ttl 500
  @default_sweep_interval 5_000
  @default_pool_size 100
  @wait_poll_interval 50

  @type filters :: term()
  @type cache_key :: {filter_hash :: non_neg_integer(), created_at :: integer()}
  @type cursor :: binary()
  @type table_ref :: :ets.tid()
  @type cache_location :: {table_ref(), cache_key(), non_neg_integer()}
  @type name :: GenServer.name()

  # API

  @doc """
  Starts a CachedPaginator instance.

  ## Options

  - `:name` (required) - The name to register the GenServer
  - `:ttl` - TTL in ms for cache entries (default: 300)
  - `:sweep_interval` - Interval in ms for cleanup sweep (default: 5_000)
  - `:pool_size` - Number of pre-initialized ETS tables (default: 100)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Store indexed data. Always creates a new cache entry with current timestamp.
  Returns cache_location and cache_key.

  Items must be tuples where the last element is the value and preceding elements
  form the sort key.
  """
  @spec store(name(), filters(), [tuple()]) :: {cache_location(), cache_key()}
  def store(name, filters, items) do
    GenServer.call(name, {:store, filters, items})
  end

  @doc """
  Get cached data or create it if missing, with cursor-based session support.

  ## Return Value

  Returns `{cache_location, cursor}`.

  The cursor encodes the last sort key from previous `fetch_after` calls. When the
  underlying data changes (cache miss), the last sort key is preserved in the new
  cursor so keyset pagination continues seamlessly.
  """
  @spec get_or_create(name(), filters(), (-> [tuple()]), cursor() | nil) ::
          {cache_location(), cursor()}
  def get_or_create(name, filters, fetch_fn, cursor \\ nil) do
    config = get_config(name)
    filter_hash = :erlang.phash2(filters)
    decoded_cursor = decode_cursor(cursor)

    case get(name, filters) do
      {:ok, data, cache_key} ->
        CachedPaginator.Telemetry.emit_hit(name, filter_hash)
        last_sort_key = extract_last_sort_key(decoded_cursor)
        {data, encode_cursor(cache_key, last_sort_key)}

      :miss ->
        CachedPaginator.Telemetry.emit_miss(name, filter_hash)
        last_sort_key = extract_last_sort_key(decoded_cursor)

        {data, cache_key} =
          locked_fetch_and_store(name, filters, fetch_fn, config)

        new_cursor = encode_cursor(cache_key, last_sort_key)
        {data, new_cursor}
    end
  end

  @doc """
  Get indexed data for filters. Direct ETS read - no GenServer call.

  Uses latest_index for O(1) lookup. Returns cached data within TTL.
  """
  @spec get(name(), filters()) ::
          {:ok, cache_location(), cache_key()} | :miss
  def get(name, filters) do
    config = get_config(name)
    filter_hash = :erlang.phash2(filters)
    now = System.monotonic_time(:millisecond)

    case :ets.lookup(config.latest_index, filter_hash) do
      [{^filter_hash, created_at, table, size}]
      when now - created_at <= config.ttl ->
        {:ok, {table, {filter_hash, created_at}, size}, {filter_hash, created_at}}

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
  Fetch items after the cursor's last sort key using keyset pagination.

  Walks the ordered ETS table from the last sort key encoded in the cursor,
  collecting up to `limit` items. Returns the items and an updated cursor
  with the new last sort key.

  ## Examples

      {items, updated_cursor} = CachedPaginator.fetch_after(table, cache_key, cursor, 20)
  """
  @spec fetch_after(table_ref(), cache_key(), cursor(), non_neg_integer()) ::
          {[term()], cursor()}
  def fetch_after(table, cache_key, cursor, limit) do
    decoded = decode_cursor(cursor)
    last_sort_key = extract_last_sort_key(decoded)

    start_key =
      if last_sort_key do
        {cache_key, last_sort_key}
      else
        # Position just before the first item for this cache_key.
        # In Erlang term ordering, a bare tuple {cache_key} is smaller than
        # any {cache_key, sort_key} tuple since shorter tuples sort first.
        {cache_key}
      end

    {values, new_last_sk} = collect_next(table, start_key, cache_key, limit, [], nil)

    updated_cursor = update_cursor_sort_key(decoded, new_last_sk)
    {values, updated_cursor}
  end

  @doc """
  Encodes a cache key and last_sort_key into a cursor string.
  """
  @spec encode_cursor(cache_key(), term()) :: cursor()
  def encode_cursor({filter_hash, created_at}, last_sort_key) do
    {filter_hash, created_at, last_sort_key}
    |> :erlang.term_to_binary()
    |> Base.url_encode64(padding: false)
  end

  @doc """
  Decodes a cursor string into a 3-tuple.

  Returns `{:ok, {filter_hash, created_at, last_sort_key}}` or `:error`.
  """
  @spec decode_cursor(cursor() | nil) ::
          {:ok, {non_neg_integer(), integer(), term()}}
          | :error
  def decode_cursor(nil), do: :error

  def decode_cursor(cursor) when is_binary(cursor) do
    with {:ok, binary} <- Base.url_decode64(cursor, padding: false),
         {:ok, term} <- safe_binary_to_term(binary) do
      case term do
        {filter_hash, created_at, last_sort_key}
        when is_integer(filter_hash) and is_integer(created_at) ->
          {:ok, {filter_hash, created_at, last_sort_key}}

        _ ->
          :error
      end
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

  defp extract_last_sort_key({:ok, {_, _, last_sort_key}}), do: last_sort_key
  defp extract_last_sort_key(_), do: nil

  defp update_cursor_sort_key({:ok, {filter_hash, created_at, _old_sk}}, new_sk) do
    encode_cursor({filter_hash, created_at}, new_sk)
  end

  defp update_cursor_sort_key(:error, _new_sk), do: nil

  # Walks the ordered_set ETS table from `current_key` using `:ets.next/2`,
  # collecting up to `remaining` items that belong to `cache_key`.
  # Items are prepended to the accumulator (O(1) per item) and reversed at the
  # end, which is more efficient than appending or scanning the whole table
  # with `:ets.select/3`.
  defp collect_next(_table, _key, _cache_key, 0, acc, last_sk),
    do: {Enum.reverse(acc), last_sk}

  defp collect_next(table, current_key, cache_key, remaining, acc, _last_sk) do
    case :ets.next(table, current_key) do
      :"$end_of_table" ->
        {Enum.reverse(acc), if(acc == [], do: nil, else: elem(current_key, 1))}

      {^cache_key, sort_key} = next_key ->
        [{_, value}] = :ets.lookup(table, next_key)
        collect_next(table, next_key, cache_key, remaining - 1, [value | acc], sort_key)

      _other_cache_key ->
        {Enum.reverse(acc), if(acc == [], do: nil, else: elem(current_key, 1))}
    end
  end

  defp locked_fetch_and_store(name, filters, fetch_fn, config) do
    filter_hash = :erlang.phash2(filters)

    if :ets.insert_new(config.locks, {filter_hash, true}) do
      try do
        start_time = System.monotonic_time()
        items = fetch_fn.()
        duration = System.monotonic_time() - start_time

        CachedPaginator.Telemetry.emit_store(name, filter_hash, length(items), duration)
        store(name, filters, items)
      after
        :ets.delete(config.locks, filter_hash)
      end
    else
      Process.sleep(@wait_poll_interval)

      case get(name, filters) do
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
    ttl = Keyword.get(opts, :ttl, @default_ttl)
    sweep_interval = Keyword.get(opts, :sweep_interval, @default_sweep_interval)
    pool_size = Keyword.get(opts, :pool_size, @default_pool_size)

    # create unique ETS table names for this instance
    registry = :"#{name}_registry"
    latest_index = :"#{name}_latest_index"
    locks = :"#{name}_locks"

    # all cache entries for expiration sweep: {cache_key, table, size}
    :ets.new(registry, [:set, :public, :named_table, read_concurrency: true])
    # O(1) lookup for most recent entry per filter: {filter_hash, created_at, table, size}
    :ets.new(latest_index, [:set, :public, :named_table, read_concurrency: true])
    # mutex for deduplicating concurrent fetches: {filter_hash, true}
    :ets.new(locks, [:set, :public, :named_table])

    # pool of shared ordered_set tables holding cached data: {{cache_key, sort_key}, value}
    pool =
      for i <- 1..pool_size do
        :ets.new(:"#{name}_data_#{i}", [:ordered_set, :public, read_concurrency: true])
      end

    config = %{
      name: name,
      registry: registry,
      latest_index: latest_index,
      locks: locks,
      ttl: ttl,
      pool_size: pool_size
    }

    :persistent_term.put({__MODULE__, name, :config}, config)

    schedule_sweep(sweep_interval)

    state = %{
      config: config,
      sweep_interval: sweep_interval,
      pool: pool,
      next_table: 0,
      sweep_ref: nil
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:store, filters, items}, _from, state) do
    {result, new_state} = do_store(filters, items, state)
    {:reply, result, new_state}
  end

  @impl true
  def handle_call(:clear, _from, state) do
    %{config: config, pool: pool} = state

    :ets.delete_all_objects(config.registry)
    :ets.delete_all_objects(config.latest_index)
    :ets.delete_all_objects(config.locks)

    Enum.each(pool, &:ets.delete_all_objects/1)

    {:reply, :ok, %{state | next_table: 0}}
  end

  @impl true
  def handle_call(:stats, _from, state) do
    %{config: config, pool: pool} = state

    memory_bytes = calculate_memory(pool)

    stats = %{
      pool_size: config.pool_size,
      memory_bytes: memory_bytes
    }

    {:reply, stats, state}
  end

  @impl true
  def handle_info(:sweep_expired, %{sweep_ref: ref} = state) when is_reference(ref) do
    # sweep already in progress, skip this tick
    schedule_sweep(state.sweep_interval)
    {:noreply, state}
  end

  def handle_info(:sweep_expired, state) do
    %{config: config, pool: pool} = state

    task =
      Task.async(fn ->
        now = System.monotonic_time(:millisecond)
        expired = collect_expired(config.registry, config.ttl, now)

        Enum.each(expired, fn {{filter_hash, created_at} = key, table} ->
          :ets.delete(config.registry, key)
          :ets.match_delete(table, {{key, :_}, :_})

          case :ets.lookup(config.latest_index, filter_hash) do
            [{^filter_hash, ^created_at, _, _}] ->
              :ets.delete(config.latest_index, filter_hash)

            _ ->
              :ok
          end
        end)

        memory_bytes = calculate_memory(pool)

        {length(expired), memory_bytes}
      end)

    {:noreply, %{state | sweep_ref: task.ref}}
  end

  @impl true
  def handle_info({ref, {expired_count, memory_bytes}}, %{sweep_ref: ref} = state) do
    Process.demonitor(ref, [:flush])

    CachedPaginator.Telemetry.emit_sweep(state.config.name, %{
      pool_size: state.config.pool_size,
      memory_bytes: memory_bytes,
      expired_count: expired_count
    })

    schedule_sweep(state.sweep_interval)

    {:noreply, %{state | sweep_ref: nil}}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{sweep_ref: ref} = state) do
    Logger.warning("sweep task crashed: #{inspect(reason)}")
    schedule_sweep(state.sweep_interval)
    {:noreply, %{state | sweep_ref: nil}}
  end

  defp do_store(filters, items, state) do
    %{config: config, pool: pool, next_table: next_table} = state

    filter_hash = :erlang.phash2(filters)
    now = System.monotonic_time(:millisecond)
    cache_key = {filter_hash, now}

    # round-robin table assignment
    table = Enum.at(pool, next_table)
    new_next = rem(next_table + 1, config.pool_size)

    size = populate_table(table, cache_key, items)

    :ets.insert(config.registry, {cache_key, table, size})
    :ets.insert(config.latest_index, {filter_hash, now, table, size})

    result = {{table, cache_key, size}, cache_key}
    new_state = %{state | next_table: new_next}

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

  defp populate_table(table, cache_key, items) do
    entries =
      Enum.map(items, fn item ->
        sort_key = Tuple.delete_at(item, tuple_size(item) - 1)
        value = elem(item, tuple_size(item) - 1)
        {{cache_key, sort_key}, value}
      end)

    :ets.insert(table, entries)
    length(items)
  end

  defp calculate_memory(pool) do
    Enum.reduce(pool, 0, fn table, acc ->
      case :ets.info(table, :memory) do
        :undefined -> acc
        # :ets.info returns memory in words, convert to bytes
        words -> acc + words * :erlang.system_info(:wordsize)
      end
    end)
  end
end
