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
- **Keyset pagination**: Cursor encodes last sort key, stable across cache transitions
- **TTL + sweep**: Configurable TTL with periodic cleanup
- **ETS pool**: Pre-initialized pool of shared `ordered_set` tables (round-robin)
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
  ttl: 500,              # ms - cache entry TTL
  sweep_interval: 5_000, # ms - cleanup interval
  pool_size: 100         # pre-initialized ETS tables
}
```

### Cache and paginate

```elixir
def list_items(filters, cursor, page_size) do
  {cache_location, cursor} =
    CachedPaginator.get_or_create(:my_cache, filters, fn ->
      # return {sort_key, value} tuples
      Repo.all(from i in Item, where: ^filters, select: {i.inserted_at, i.id})
    end, cursor)

  {table, cache_key, _size} = cache_location
  {items, updated_cursor} = CachedPaginator.fetch_after(table, cache_key, cursor, page_size)

  %{items: items, cursor: updated_cursor}
end
```

## How It Works

### ETS Structure

Each cache entry stores items in a shared `ordered_set` ETS table using composite keys:

```
Data pool table (ordered_set):
{{cache_key, {sort_key}}, value}
{{cache_key, {sort_key1, sort_key2}}, value}
```

The `ordered_set` table type keeps keys sorted by Erlang term ordering. Combined with composite `{cache_key, sort_key}` keys, this enables efficient keyset pagination via `:ets.next/2` - walking forward from the last sort key to collect the next page.

### TTL

Cache entries expire after `ttl` milliseconds (default: 500ms). A periodic sweep runs every `sweep_interval` ms to clean up expired entries.

### Locking (Thundering Herd Prevention)

When multiple users request the same uncached data simultaneously:

1. First request acquires lock and runs query
2. Concurrent requests wait (poll every 50ms)
3. Once cached, all waiting requests get the same result

### ETS Pool

Tables are pre-initialized at startup and assigned to new cache entries via round-robin. Multiple cache entries coexist in the same table using composite keys, so table count stays constant regardless of how many queries are cached.

## Telemetry

### Events

| Event | Measurements | Metadata |
|-------|--------------|----------|
| `[:cached_paginator, :hit]` | - | `cache`, `filter_hash` |
| `[:cached_paginator, :miss]` | - | `cache`, `filter_hash` |
| `[:cached_paginator, :store]` | `duration`, `count` | `cache`, `filter_hash` |
| `[:cached_paginator, :sweep]` | `pool_size`, `memory_bytes`, `expired_count` | `cache` |

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `:name` | required | GenServer name for this instance |
| `:ttl` | 500 | Cache entry TTL (ms) |
| `:sweep_interval` | 5_000 | Cleanup interval (ms) |
| `:pool_size` | 100 | Pre-initialized ETS tables |

## License

MIT
