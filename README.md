# CachedPaginator

ETS-backed pagination cache for Elixir applications.

## The Problem

In many applications, users repeatedly request the same expensive query but for different pages:

```
User A: GET /items?filters=X&page=1  → runs expensive query
User B: GET /items?filters=X&page=2  → runs the SAME expensive query again
User C: GET /items?filters=X&page=1  → runs the SAME expensive query again
...
```

Each page request triggers the full query, even though the underlying data hasn't changed. This wastes database resources and increases latency.

## The Solution

CachedPaginator caches query results once, then serves all page requests from the cache:

```
User A: GET /items?filters=X&page=1  → runs query, caches result
User B: GET /items?filters=X&page=2  → O(1) ETS lookup, no query
User C: GET /items?filters=X&page=1  → O(1) ETS lookup, no query
```

### Key Features

- **Query caching**: Run expensive query once, serve all page requests from cache
- **Locking**: When User A triggers a query, Users B and C wait for it to complete and reuse the same result (no thundering herd)
- **Cursor stability**: Users get consistent result sets across all pages during their session (no drift from concurrent inserts/deletes)
- **Two-tier TTL**: Fresh data (300ms) for new sessions, extended TTL (10s) for ongoing pagination
- **Backpressure**: Max tables limit prevents unbounded memory growth
- **Telemetry**: Full observability (hits, misses, table count, memory usage)

## Installation

Add to your `mix.exs`:

```elixir
def deps do
  [
    {:cached_paginator, "~> 0.1.0"}
  ]
end
```

## Usage

### Start a cache instance

```elixir
# In your supervision tree
children = [
  {CachedPaginator, name: :my_cache}
]

# Or with custom options
{CachedPaginator,
  name: :my_cache,
  fresh_ttl: 500,        # ms - TTL for new sessions
  max_ttl: 15_000,       # ms - TTL for cursor-based access
  sweep_interval: 5_000, # ms - cleanup interval
  max_tables: 200        # max ETS tables before backpressure
}
```

### Cache and paginate

```elixir
def list_items(filters, cursor, page_size) do
  first_page = is_nil(cursor)

  # get_or_create runs the query only on cache miss
  {{table, total_count}, cursor} =
    CachedPaginator.get_or_create(:my_cache, filters, fn ->
      # expensive query - runs once, cached for all users
      Repo.all(from i in Item, where: ^filters, select: i.id, order_by: [desc: i.inserted_at])
    end, cursor, first_page: first_page)

  # fetch page from cache - O(page_size) regardless of position
  start_idx = parse_page(cursor) * page_size
  end_idx = start_idx + page_size - 1
  ids = CachedPaginator.fetch_range(table, start_idx, end_idx)

  # load full records
  items = Repo.all(from i in Item, where: i.id in ^ids)

  %{
    items: items,
    cursor: cursor,
    total_count: total_count,
    has_more: end_idx < total_count - 1
  }
end
```

### Direct API

```elixir
# Store data manually
{{table, size}, cache_key} = CachedPaginator.store(:my_cache, filters, ids)

# Get cached data (fresh TTL)
{:ok, {table, size}, cache_key} = CachedPaginator.get(:my_cache, filters)

# Get by cursor (extended TTL)
{:ok, {table, size}, cache_key} = CachedPaginator.get_by_cursor(:my_cache, cache_key)

# Fetch range from table
ids = CachedPaginator.fetch_range(table, 0, 19)

# Clear all cached data
CachedPaginator.clear(:my_cache)

# Get stats
%{table_count: 5, pool_size: 2, memory_bytes: 1024} = CachedPaginator.stats(:my_cache)
```

## How It Works

### ETS Structure

Each cache entry stores IDs indexed by position:

```
ETS Table:
{0, "id_abc"}
{1, "id_def"}
{2, "id_ghi"}
...
```

This allows O(1) lookup for any page range via `fetch_range/3`.

### Two-Tier TTL

| Access Type | TTL | Purpose |
|-------------|-----|---------|
| No cursor / first page | `fresh_ttl` (300ms) | Fresh data for new sessions |
| With cursor | `max_ttl` (10s) | Session can continue paginating |

### Locking (Thundering Herd Prevention)

When multiple users request the same uncached data simultaneously:

1. First request acquires lock and runs query
2. Concurrent requests wait (poll every 50ms)
3. Once cached, all waiting requests get the same result

### Backpressure

When `max_tables` is reached:
- New store requests wait for tables to return to pool
- Error logged: "max tables reached, waiting for available table"
- Telemetry event emitted

## Telemetry

### Events

| Event | Measurements | Metadata |
|-------|--------------|----------|
| `[:cached_paginator, :hit]` | - | `cache`, `filter_hash` |
| `[:cached_paginator, :miss]` | - | `cache`, `filter_hash` |
| `[:cached_paginator, :store]` | `duration`, `count` | `cache`, `filter_hash` |
| `[:cached_paginator, :sweep]` | `table_count`, `pool_size`, `memory_bytes`, `expired_count` | `cache` |
| `[:cached_paginator, :wait]` | - | `cache` |

### Example

```elixir
:telemetry.attach(
  "cache-metrics",
  [:cached_paginator, :sweep],
  fn _event, measurements, metadata, _config ->
    Logger.info("""
    Cache #{metadata.cache}:
      Tables: #{measurements.table_count}
      Memory: #{measurements.memory_bytes} bytes
      Expired: #{measurements.expired_count}
    """)
  end,
  nil
)
```

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `:name` | required | GenServer name for this instance |
| `:fresh_ttl` | 300 | TTL (ms) for fresh cache entries |
| `:max_ttl` | 10_000 | TTL (ms) for cursor-based access |
| `:sweep_interval` | 5_000 | Cleanup interval (ms) |
| `:max_tables` | 100 | Max ETS tables before backpressure |

## License

MIT
