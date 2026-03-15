defmodule CachedPaginatorTest do
  use ExUnit.Case, async: false

  setup do
    name = :"test_cache_#{:erlang.unique_integer([:positive])}"
    {:ok, pid} = CachedPaginator.start_link(name: name, ttl: 100)

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
    end)

    %{cache: name}
  end

  describe "store/3" do
    test "stores tuple items and returns cache_data and cache_key", %{cache: cache} do
      filters = [status: :active]
      items = [{1, "id1"}, {2, "id2"}, {3, "id3"}]

      {{table, cache_key, size}, {filter_hash, created_at}} =
        CachedPaginator.store(cache, filters, items)

      assert is_reference(table)
      assert size == 3
      assert is_integer(filter_hash)
      assert is_integer(created_at)
      assert cache_key == {filter_hash, created_at}
    end

    test "stores composite sort key items", %{cache: cache} do
      items = [
        {~U[2026-03-16 14:00:00Z], 10, "market_abc"},
        {~U[2026-03-16 15:00:00Z], 20, "market_def"}
      ]

      {{table, cache_key, size}, _key} = CachedPaginator.store(cache, [a: 1], items)

      assert size == 2

      # Items should be retrievable via fetch_after
      cursor = CachedPaginator.encode_cursor(cache_key, nil)
      {values, _cursor} = CachedPaginator.fetch_after(table, cache_key, cursor, 10)
      assert values == ["market_abc", "market_def"]
    end
  end

  describe "get/3" do
    test "returns :miss when no cache exists", %{cache: cache} do
      assert :miss == CachedPaginator.get(cache, status: :new)
    end

    test "returns cached data when fresh", %{cache: cache} do
      filters = [status: :active]
      items = [{1, "id1"}, {2, "id2"}]

      CachedPaginator.store(cache, filters, items)

      assert {:ok, {_table, _cache_key, 2}, _key} =
               CachedPaginator.get(cache, filters)
    end

    test "returns :miss when cache expired", %{cache: cache} do
      filters = [status: :active]
      CachedPaginator.store(cache, filters, [{1, "id1"}])

      # wait for fresh_ttl (100ms) to expire
      Process.sleep(150)

      assert :miss == CachedPaginator.get(cache, filters)
    end
  end

  describe "get_or_create/5" do
    test "creates cache on miss", %{cache: cache} do
      filters = [status: :active]
      fetch_fn = fn -> [{1, "id1"}, {2, "id2"}, {3, "id3"}] end

      {{table, _cache_key, size}, cursor} =
        CachedPaginator.get_or_create(cache, filters, fetch_fn)

      assert is_reference(table)
      assert size == 3
      assert is_binary(cursor)
    end

    test "returns cached data on hit", %{cache: cache} do
      filters = [status: :active]
      items = [{1, "id1"}, {2, "id2"}]
      fetch_count = :counters.new(1, [:atomics])

      fetch_fn = fn ->
        :counters.add(fetch_count, 1, 1)
        items
      end

      # first call - creates cache
      {data1, cursor1} = CachedPaginator.get_or_create(cache, filters, fetch_fn)
      # second call - should hit cache
      {data2, cursor2} = CachedPaginator.get_or_create(cache, filters, fetch_fn)

      assert data1 == data2
      assert cursor1 == cursor2
      assert :counters.get(fetch_count, 1) == 1
    end

    test "always returns 2-tuple (no data_changed)", %{cache: cache} do
      filters = [status: :active]

      {_data, cursor} =
        CachedPaginator.get_or_create(cache, filters, fn -> [{1, "id1"}, {2, "id2"}] end)

      # wait for TTL to expire
      Process.sleep(150)

      # different data — still just a 2-tuple
      result =
        CachedPaginator.get_or_create(cache, filters, fn -> [{3, "id3"}, {4, "id4"}] end, cursor)

      assert {_data, _cursor} = result
      assert tuple_size(result) == 2
    end

    test "expired cursor preserves last_sort_key for seamless continuation", %{cache: cache} do
      filters = [status: :active]
      items = [{1, "a"}, {2, "b"}, {3, "c"}, {4, "d"}, {5, "e"}]

      {data, cursor} = CachedPaginator.get_or_create(cache, filters, fn -> items end)

      # Fetch first 2 items
      {table, cache_key, _size} = data
      {fetched, cursor} = CachedPaginator.fetch_after(table, cache_key, cursor, 2)
      assert fetched == ["a", "b"]

      # Wait for TTL to expire
      Process.sleep(150)

      # New data with same sort keys — cursor's last_sort_key preserved
      new_items = [{1, "a"}, {2, "b"}, {3, "c"}, {4, "d"}, {5, "e"}]

      {data2, cursor2} =
        CachedPaginator.get_or_create(cache, filters, fn -> new_items end, cursor)

      # Continue pagination from where we left off
      {table2, cache_key2, _size2} = data2
      {fetched2, _cursor3} = CachedPaginator.fetch_after(table2, cache_key2, cursor2, 3)
      assert fetched2 == ["c", "d", "e"]
    end

    test "concurrent requests wait for ongoing fetch", %{cache: cache} do
      filters = [status: :active]
      fetch_count = :counters.new(1, [:atomics])

      fetch_fn = fn ->
        :counters.add(fetch_count, 1, 1)
        Process.sleep(100)
        [{1, "id1"}, {2, "id2"}]
      end

      # spawn multiple concurrent requests
      tasks =
        for _ <- 1..5 do
          Task.async(fn ->
            CachedPaginator.get_or_create(cache, filters, fetch_fn)
          end)
        end

      results = Task.await_many(tasks, 5000)

      # all should get same data
      [{first_data, first_cursor} | rest] = results

      for {data, cursor} <- rest do
        assert data == first_data
        assert cursor == first_cursor
      end

      # fetch should only be called once
      assert :counters.get(fetch_count, 1) == 1
    end
  end

  describe "fetch_after/4" do
    test "returns items in sort key order", %{cache: cache} do
      filters = [status: :active]
      items = [{3, "c"}, {1, "a"}, {2, "b"}]
      {{table, cache_key, _size}, _key} = CachedPaginator.store(cache, filters, items)

      cursor = CachedPaginator.encode_cursor(cache_key, nil)
      {values, _cursor} = CachedPaginator.fetch_after(table, cache_key, cursor, 10)

      # ordered_set sorts by key, so items come back in sort key order
      assert values == ["a", "b", "c"]
    end

    test "returns correct items and updated cursor with last_sort_key", %{cache: cache} do
      filters = [status: :active]
      items = [{1, "a"}, {2, "b"}, {3, "c"}, {4, "d"}, {5, "e"}]
      {{table, cache_key, _size}, _key} = CachedPaginator.store(cache, filters, items)

      cursor = CachedPaginator.encode_cursor(cache_key, nil)

      # Fetch first 3
      {values, cursor2} = CachedPaginator.fetch_after(table, cache_key, cursor, 3)
      assert values == ["a", "b", "c"]

      # Cursor should have last_sort_key = {3}
      assert {:ok, {_, _, {3}}} = CachedPaginator.decode_cursor(cursor2)

      # Fetch next 2
      {values2, cursor3} = CachedPaginator.fetch_after(table, cache_key, cursor2, 2)
      assert values2 == ["d", "e"]

      assert {:ok, {_, _, {5}}} = CachedPaginator.decode_cursor(cursor3)
    end

    test "returns empty list at end of data", %{cache: cache} do
      items = [{1, "a"}, {2, "b"}]
      {{table, cache_key, _size}, _key} = CachedPaginator.store(cache, [a: 1], items)

      cursor = CachedPaginator.encode_cursor(cache_key, nil)

      # Fetch all
      {values, cursor2} = CachedPaginator.fetch_after(table, cache_key, cursor, 10)
      assert values == ["a", "b"]

      # Fetch again — should be empty
      {values2, _cursor3} = CachedPaginator.fetch_after(table, cache_key, cursor2, 10)
      assert values2 == []
    end

    test "continues correctly after last_sort_key in new snapshot (keyset stability)", %{
      cache: cache
    } do
      filters = [status: :active]

      # First snapshot: items 1-5
      items1 = [{1, "a"}, {2, "b"}, {3, "c"}, {4, "d"}, {5, "e"}]
      {{table1, cache_key1, _}, _key1} = CachedPaginator.store(cache, filters, items1)

      # Fetch first 2 items
      cursor = CachedPaginator.encode_cursor(cache_key1, nil)
      {values, cursor2} = CachedPaginator.fetch_after(table1, cache_key1, cursor, 2)
      assert values == ["a", "b"]

      # Second snapshot: "b" removed, "f" added at end
      items2 = [{1, "a"}, {3, "c"}, {4, "d"}, {5, "e"}, {6, "f"}]
      {{table2, cache_key2, _}, _key2} = CachedPaginator.store(cache, filters, items2)

      # Preserve last_sort_key from cursor2, update cache_key
      {:ok, {_, _, last_sk}} = CachedPaginator.decode_cursor(cursor2)
      new_cursor = CachedPaginator.encode_cursor(cache_key2, last_sk)

      # Continue from where we left off — should get items after sort_key {2}
      {values2, _cursor3} = CachedPaginator.fetch_after(table2, cache_key2, new_cursor, 10)
      assert values2 == ["c", "d", "e", "f"]
    end

    test "with composite sort keys", %{cache: cache} do
      items = [
        {~U[2026-03-16 14:00:00Z], 10, "market_a"},
        {~U[2026-03-16 14:00:00Z], 20, "market_b"},
        {~U[2026-03-16 15:00:00Z], 10, "market_c"}
      ]

      {{table, cache_key, _size}, _key} = CachedPaginator.store(cache, [a: 1], items)

      cursor = CachedPaginator.encode_cursor(cache_key, nil)

      {values, cursor2} = CachedPaginator.fetch_after(table, cache_key, cursor, 2)
      assert values == ["market_a", "market_b"]

      {values2, _cursor3} = CachedPaginator.fetch_after(table, cache_key, cursor2, 10)
      assert values2 == ["market_c"]
    end

    test "multiple queries coexist in shared tables", %{cache: cache} do
      {{table1, key1, _}, _} = CachedPaginator.store(cache, [a: 1], [{1, "x"}, {2, "y"}])

      {{table2, key2, _}, _} =
        CachedPaginator.store(cache, [b: 2], [{1, "p"}, {2, "q"}, {3, "r"}])

      cursor1 = CachedPaginator.encode_cursor(key1, nil)
      cursor2 = CachedPaginator.encode_cursor(key2, nil)

      {v1, _} = CachedPaginator.fetch_after(table1, key1, cursor1, 10)
      {v2, _} = CachedPaginator.fetch_after(table2, key2, cursor2, 10)

      assert v1 == ["x", "y"]
      assert v2 == ["p", "q", "r"]
    end
  end

  describe "cursor encoding/decoding" do
    test "round-trip with nil last_sort_key" do
      cache_key = {12345, 67890}
      cursor = CachedPaginator.encode_cursor(cache_key, nil)

      assert {:ok, {12345, 67890, nil}} = CachedPaginator.decode_cursor(cursor)
    end

    test "round-trip with last_sort_key" do
      cache_key = {12345, 67890}
      cursor = CachedPaginator.encode_cursor(cache_key, {42})

      assert {:ok, {12345, 67890, {42}}} = CachedPaginator.decode_cursor(cursor)
    end

    test "round-trip with composite last_sort_key" do
      cache_key = {12345, 67890}

      cursor =
        CachedPaginator.encode_cursor(cache_key, {~U[2026-03-16 14:00:00Z], 10})

      assert {:ok, {12345, 67890, {~U[2026-03-16 14:00:00Z], 10}}} =
               CachedPaginator.decode_cursor(cursor)
    end

    test "decode_cursor returns :error for nil" do
      assert :error == CachedPaginator.decode_cursor(nil)
    end

    test "decode_cursor returns :error for invalid cursor" do
      assert :error == CachedPaginator.decode_cursor("invalid")
      assert :error == CachedPaginator.decode_cursor("")
    end
  end

  describe "clear/1" do
    test "clears all cached data", %{cache: cache} do
      CachedPaginator.store(cache, [a: 1], [{1, "id1"}])
      CachedPaginator.store(cache, [b: 2], [{1, "id2"}])

      assert {:ok, _, _} = CachedPaginator.get(cache, a: 1)
      assert {:ok, _, _} = CachedPaginator.get(cache, b: 2)

      :ok = CachedPaginator.clear(cache)

      assert :miss == CachedPaginator.get(cache, a: 1)
      assert :miss == CachedPaginator.get(cache, b: 2)
    end
  end

  describe "stats/1" do
    test "returns current stats", %{cache: cache} do
      CachedPaginator.store(cache, [a: 1], [{1, "id1"}, {2, "id2"}])
      CachedPaginator.store(cache, [b: 2], [{1, "id3"}])

      stats = CachedPaginator.stats(cache)

      assert stats.pool_size == 100
      assert is_integer(stats.memory_bytes)
      assert stats.memory_bytes > 0
    end
  end

  describe "telemetry" do
    test "emits hit event on cache hit", %{cache: cache} do
      ref = make_ref()
      test_pid = self()

      :telemetry.attach(
        ref,
        [:cached_paginator, :hit],
        fn _event, _measurements, metadata, _ ->
          send(test_pid, {:hit, metadata})
        end,
        nil
      )

      CachedPaginator.store(cache, [a: 1], [{1, "id1"}])
      CachedPaginator.get_or_create(cache, [a: 1], fn -> [{1, "id1"}] end)

      assert_receive {:hit, %{cache: ^cache}}, 1000

      :telemetry.detach(ref)
    end

    test "emits miss event on cache miss", %{cache: cache} do
      ref = make_ref()
      test_pid = self()

      :telemetry.attach(
        ref,
        [:cached_paginator, :miss],
        fn _event, _measurements, metadata, _ ->
          send(test_pid, {:miss, metadata})
        end,
        nil
      )

      CachedPaginator.get_or_create(cache, [new: 1], fn -> [{1, "id1"}] end)

      assert_receive {:miss, %{cache: ^cache}}, 1000

      :telemetry.detach(ref)
    end

    test "emits store event when storing data", %{cache: cache} do
      ref = make_ref()
      test_pid = self()

      :telemetry.attach(
        ref,
        [:cached_paginator, :store],
        fn _event, measurements, metadata, _ ->
          send(test_pid, {:store, measurements, metadata})
        end,
        nil
      )

      CachedPaginator.get_or_create(cache, [a: 1], fn -> [{1, "id1"}, {2, "id2"}] end)

      assert_receive {:store, %{count: 2, duration: _}, %{cache: ^cache}}, 1000

      :telemetry.detach(ref)
    end
  end
end
