defmodule CachedPaginator.Telemetry do
  @moduledoc """
  Telemetry events for CachedPaginator.

  ## Events

  ### `[:cached_paginator, :hit]`

  Emitted when a cache hit occurs.

  Measurements: `%{}`
  Metadata: `%{cache: atom(), filter_hash: integer()}`

  ### `[:cached_paginator, :miss]`

  Emitted when a cache miss occurs.

  Measurements: `%{}`
  Metadata: `%{cache: atom(), filter_hash: integer()}`

  ### `[:cached_paginator, :store]`

  Emitted when data is stored in cache.

  Measurements: `%{duration: integer(), count: integer()}`
  Metadata: `%{cache: atom(), filter_hash: integer()}`

  ### `[:cached_paginator, :sweep]`

  Emitted during periodic cleanup sweep.

  Measurements: `%{pool_size: integer(), memory_bytes: integer(), expired_count: integer()}`
  Metadata: `%{cache: atom()}`

  ## Example Usage

      :telemetry.attach(
        "my-handler",
        [:cached_paginator, :sweep],
        fn _event, measurements, metadata, _config ->
          Logger.info("Cache \#{metadata.cache}: \#{measurements.pool_size} tables, \#{measurements.memory_bytes} bytes")
        end,
        nil
      )
  """

  @doc false
  @spec emit_hit(atom(), integer()) :: :ok
  def emit_hit(cache, filter_hash) do
    :telemetry.execute(
      [:cached_paginator, :hit],
      %{},
      %{cache: cache, filter_hash: filter_hash}
    )
  end

  @doc false
  @spec emit_miss(atom(), integer()) :: :ok
  def emit_miss(cache, filter_hash) do
    :telemetry.execute(
      [:cached_paginator, :miss],
      %{},
      %{cache: cache, filter_hash: filter_hash}
    )
  end

  @doc false
  @spec emit_store(atom(), integer(), integer(), integer()) :: :ok
  def emit_store(cache, filter_hash, count, duration) do
    :telemetry.execute(
      [:cached_paginator, :store],
      %{duration: duration, count: count},
      %{cache: cache, filter_hash: filter_hash}
    )
  end

  @doc false
  @spec emit_sweep(atom(), map()) :: :ok
  def emit_sweep(cache, measurements) do
    :telemetry.execute(
      [:cached_paginator, :sweep],
      measurements,
      %{cache: cache}
    )
  end
end
