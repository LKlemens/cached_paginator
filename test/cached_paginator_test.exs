defmodule CachedPaginatorTest do
  use ExUnit.Case, async: false

  setup do
    name = :"test_cache_#{:erlang.unique_integer([:positive])}"
    {:ok, pid} = CachedPaginator.start_link(name: name, fresh_ttl: 100, max_ttl: 500)

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
    end)

    %{cache: name}
  end

  describe "store/3" do
    test "stores data and returns cache_data, cache_key, and result_hash", %{cache: cache} do
      filters = [status: :active]
      ids = ["id1", "id2", "id3"]

      {{table, cache_key, size}, {filter_hash, created_at}, result_hash} =
        CachedPaginator.store(cache, filters, ids)

      assert is_reference(table)
      assert size == 3
      assert is_integer(filter_hash)
      assert is_integer(created_at)
      assert cache_key == {filter_hash, created_at}
      assert result_hash == :erlang.phash2(ids)
    end
  end

  describe "get/3" do
    test "returns :miss when no cache exists", %{cache: cache} do
      assert :miss == CachedPaginator.get(cache, status: :new)
    end

    test "returns cached data when fresh", %{cache: cache} do
      filters = [status: :active]
      ids = ["id1", "id2"]

      CachedPaginator.store(cache, filters, ids)

      assert {:ok, {_table, _cache_key, 2}, _key, _result_hash} =
               CachedPaginator.get(cache, filters)
    end

    test "returns :miss when cache expired", %{cache: cache} do
      filters = [status: :active]
      CachedPaginator.store(cache, filters, ["id1"])

      # wait for fresh_ttl (100ms) to expire
      Process.sleep(150)

      assert :miss == CachedPaginator.get(cache, filters)
    end
  end

  describe "get_by_cursor/3" do
    test "returns cached data within max_ttl", %{cache: cache} do
      filters = [status: :active]
      {_data, cache_key, _result_hash} = CachedPaginator.store(cache, filters, ["id1", "id2"])

      # wait past fresh_ttl but within max_ttl
      Process.sleep(150)

      assert {:ok, {_table, ^cache_key, 2}, ^cache_key, _result_hash} =
               CachedPaginator.get_by_cursor(cache, cache_key)
    end

    test "returns :miss when cache exceeded max_ttl", %{cache: cache} do
      filters = [status: :active]
      {_data, cache_key, _result_hash} = CachedPaginator.store(cache, filters, ["id1"])

      # wait for max_ttl (500ms) to expire
      Process.sleep(550)

      assert :miss == CachedPaginator.get_by_cursor(cache, cache_key)
    end
  end

  describe "get_or_create/5" do
    test "creates cache on miss", %{cache: cache} do
      filters = [status: :active]
      fetch_fn = fn -> ["id1", "id2", "id3"] end

      {{table, _cache_key, size}, cursor} =
        CachedPaginator.get_or_create(cache, filters, fetch_fn)

      assert is_reference(table)
      assert size == 3
      assert is_binary(cursor)
    end

    test "returns cached data on hit without data_changed", %{cache: cache} do
      filters = [status: :active]
      ids = ["id1", "id2"]
      fetch_count = :counters.new(1, [:atomics])

      fetch_fn = fn ->
        :counters.add(fetch_count, 1, 1)
        ids
      end

      # first call - creates cache
      {data1, cursor1} = CachedPaginator.get_or_create(cache, filters, fetch_fn)
      # second call - should hit cache
      {data2, cursor2} = CachedPaginator.get_or_create(cache, filters, fetch_fn)

      assert data1 == data2
      assert cursor1 == cursor2
      assert :counters.get(fetch_count, 1) == 1
    end

    test "uses cursor for extended TTL", %{cache: cache} do
      filters = [status: :active]
      {_data, cursor} = CachedPaginator.get_or_create(cache, filters, fn -> ["id1"] end)

      # wait past fresh_ttl (100ms)
      Process.sleep(150)

      # without cursor - should miss (fresh_ttl expired)
      # with cursor - should hit (within max_ttl)
      {{_table, _cache_key, size}, _cursor} =
        CachedPaginator.get_or_create(cache, filters, fn -> ["new"] end, cursor)

      # should still get original data
      assert size == 1
    end

    test "first_page: true ignores cursor and uses fresh_ttl", %{cache: cache} do
      filters = [status: :active]
      {_data, cursor} = CachedPaginator.get_or_create(cache, filters, fn -> ["id1"] end)

      # wait past fresh_ttl
      Process.sleep(150)

      # with first_page: true, should fetch new data
      {{_table, _cache_key, size}, _cursor} =
        CachedPaginator.get_or_create(cache, filters, fn -> ["new1", "new2"] end, cursor,
          first_page: true
        )

      assert size == 2
    end

    test "expired cursor with same data does not return :data_changed", %{cache: cache} do
      filters = [status: :active]
      ids = ["id1", "id2", "id3"]

      {_data, cursor} = CachedPaginator.get_or_create(cache, filters, fn -> ids end)

      # wait for both fresh_ttl AND max_ttl to expire
      Process.sleep(550)

      # same data → no :data_changed signal
      result = CachedPaginator.get_or_create(cache, filters, fn -> ids end, cursor)
      assert {_data, _cursor} = result
      assert tuple_size(result) == 2
    end

    test "expired cursor with different data returns :data_changed", %{cache: cache} do
      filters = [status: :active]

      {_data, cursor} = CachedPaginator.get_or_create(cache, filters, fn -> ["id1", "id2"] end)

      # wait for both fresh_ttl AND max_ttl to expire
      Process.sleep(550)

      # different data → :data_changed signal
      {_data, _cursor, signal} =
        CachedPaginator.get_or_create(cache, filters, fn -> ["id3", "id4", "id5"] end, cursor)

      assert signal == :data_changed
    end

    test "no cursor never returns :data_changed", %{cache: cache} do
      filters = [status: :active]

      result = CachedPaginator.get_or_create(cache, filters, fn -> ["id1"] end)
      assert {_data, _cursor} = result
      assert tuple_size(result) == 2

      # even with expired fresh cache
      Process.sleep(150)
      result2 = CachedPaginator.get_or_create(cache, filters, fn -> ["different"] end)
      assert {_data, _cursor} = result2
      assert tuple_size(result2) == 2
    end

    test "concurrent requests wait for ongoing fetch", %{cache: cache} do
      filters = [status: :active]
      fetch_count = :counters.new(1, [:atomics])

      fetch_fn = fn ->
        :counters.add(fetch_count, 1, 1)
        Process.sleep(100)
        ["id1", "id2"]
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

  describe "fetch_range/4" do
    test "fetches range of ids from cache table", %{cache: cache} do
      filters = [status: :active]
      ids = ["a", "b", "c", "d", "e"]
      {{table, cache_key, _size}, _key, _rh} = CachedPaginator.store(cache, filters, ids)

      assert ["b", "c", "d"] == CachedPaginator.fetch_range(table, cache_key, 1, 3)
      assert ["a", "b"] == CachedPaginator.fetch_range(table, cache_key, 0, 1)
      assert ["e"] == CachedPaginator.fetch_range(table, cache_key, 4, 4)
    end

    test "handles out of bounds gracefully", %{cache: cache} do
      filters = [status: :active]
      {{table, cache_key, _size}, _key, _rh} = CachedPaginator.store(cache, filters, ["a", "b"])

      assert ["b"] == CachedPaginator.fetch_range(table, cache_key, 1, 5)
      assert [] == CachedPaginator.fetch_range(table, cache_key, 10, 15)
    end

    test "multiple queries coexist in shared tables", %{cache: cache} do
      {{table1, key1, _}, _, _} = CachedPaginator.store(cache, [a: 1], ["x", "y"])
      {{table2, key2, _}, _, _} = CachedPaginator.store(cache, [b: 2], ["p", "q", "r"])

      assert ["x", "y"] == CachedPaginator.fetch_range(table1, key1, 0, 1)
      assert ["p", "q", "r"] == CachedPaginator.fetch_range(table2, key2, 0, 2)
    end
  end

  describe "cursor encoding/decoding" do
    test "encode_cursor and decode_cursor round-trip with result_hash" do
      cache_key = {12345, 67890}
      result_hash = 99999
      cursor = CachedPaginator.encode_cursor(cache_key, result_hash)

      assert {:ok, {12345, 67890, 99999}} = CachedPaginator.decode_cursor(cursor)
    end

    test "decode_cursor returns :error for legacy 2-tuple cursors" do
      legacy_cursor =
        {12345, 67890}
        |> :erlang.term_to_binary()
        |> Base.url_encode64(padding: false)

      assert :error == CachedPaginator.decode_cursor(legacy_cursor)
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
      CachedPaginator.store(cache, [a: 1], ["id1"])
      CachedPaginator.store(cache, [b: 2], ["id2"])

      assert {:ok, _, _, _} = CachedPaginator.get(cache, a: 1)
      assert {:ok, _, _, _} = CachedPaginator.get(cache, b: 2)

      :ok = CachedPaginator.clear(cache)

      assert :miss == CachedPaginator.get(cache, a: 1)
      assert :miss == CachedPaginator.get(cache, b: 2)
    end
  end

  describe "stats/1" do
    test "returns current stats", %{cache: cache} do
      CachedPaginator.store(cache, [a: 1], ["id1", "id2"])
      CachedPaginator.store(cache, [b: 2], ["id3"])

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

      CachedPaginator.store(cache, [a: 1], ["id1"])
      CachedPaginator.get_or_create(cache, [a: 1], fn -> ["id1"] end)

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

      CachedPaginator.get_or_create(cache, [new: 1], fn -> ["id1"] end)

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

      CachedPaginator.get_or_create(cache, [a: 1], fn -> ["id1", "id2"] end)

      assert_receive {:store, %{count: 2, duration: _}, %{cache: ^cache}}, 1000

      :telemetry.detach(ref)
    end

    test "emits data_changed event when data differs on expired cursor", %{cache: cache} do
      ref = make_ref()
      test_pid = self()

      :telemetry.attach(
        ref,
        [:cached_paginator, :data_changed],
        fn _event, _measurements, metadata, _ ->
          send(test_pid, {:data_changed, metadata})
        end,
        nil
      )

      filters = [a: 1]
      {_data, cursor} = CachedPaginator.get_or_create(cache, filters, fn -> ["id1"] end)

      # wait for max_ttl to expire
      Process.sleep(550)

      CachedPaginator.get_or_create(cache, filters, fn -> ["id2", "id3"] end, cursor)

      assert_receive {:data_changed, %{cache: ^cache}}, 1000

      :telemetry.detach(ref)
    end
  end
end
