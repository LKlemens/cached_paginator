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
    test "stores data and returns cache_data and cache_key", %{cache: cache} do
      filters = [status: :active]
      ids = ["id1", "id2", "id3"]

      {{table, size}, {filter_hash, created_at}} = CachedPaginator.store(cache, filters, ids)

      assert is_reference(table)
      assert size == 3
      assert is_integer(filter_hash)
      assert is_integer(created_at)
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

      assert {:ok, {_table, 2}, _key} = CachedPaginator.get(cache, filters)
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
      {_data, cache_key} = CachedPaginator.store(cache, filters, ["id1", "id2"])

      # wait past fresh_ttl but within max_ttl
      Process.sleep(150)

      assert {:ok, {_table, 2}, ^cache_key} = CachedPaginator.get_by_cursor(cache, cache_key)
    end

    test "returns :miss when cache exceeded max_ttl", %{cache: cache} do
      filters = [status: :active]
      {_data, cache_key} = CachedPaginator.store(cache, filters, ["id1"])

      # wait for max_ttl (500ms) to expire
      Process.sleep(550)

      assert :miss == CachedPaginator.get_by_cursor(cache, cache_key)
    end
  end

  describe "get_or_create/5" do
    test "creates cache on miss", %{cache: cache} do
      filters = [status: :active]
      fetch_fn = fn -> ["id1", "id2", "id3"] end

      {{table, size}, cursor} = CachedPaginator.get_or_create(cache, filters, fetch_fn)

      assert is_reference(table)
      assert size == 3
      assert is_binary(cursor)
    end

    test "returns cached data on hit", %{cache: cache} do
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
      {{_table, size}, _cursor} =
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
      {{_table, size}, _cursor} =
        CachedPaginator.get_or_create(cache, filters, fn -> ["new1", "new2"] end, cursor,
          first_page: true
        )

      assert size == 2
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

  describe "fetch_range/3" do
    test "fetches range of ids from cache table", %{cache: cache} do
      filters = [status: :active]
      ids = ["a", "b", "c", "d", "e"]
      {{table, _size}, _key} = CachedPaginator.store(cache, filters, ids)

      assert ["b", "c", "d"] == CachedPaginator.fetch_range(table, 1, 3)
      assert ["a", "b"] == CachedPaginator.fetch_range(table, 0, 1)
      assert ["e"] == CachedPaginator.fetch_range(table, 4, 4)
    end

    test "handles out of bounds gracefully", %{cache: cache} do
      filters = [status: :active]
      {{table, _size}, _key} = CachedPaginator.store(cache, filters, ["a", "b"])

      assert ["b"] == CachedPaginator.fetch_range(table, 1, 5)
      assert [] == CachedPaginator.fetch_range(table, 10, 15)
    end
  end

  describe "cursor encoding/decoding" do
    test "encode_cursor and decode_cursor are inverse operations" do
      cache_key = {12345, 67890}
      cursor = CachedPaginator.encode_cursor(cache_key)

      assert {:ok, ^cache_key} = CachedPaginator.decode_cursor(cursor)
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

      assert {:ok, _, _} = CachedPaginator.get(cache, a: 1)
      assert {:ok, _, _} = CachedPaginator.get(cache, b: 2)

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

      assert stats.table_count == 2
      assert stats.pool_size == 0
      assert is_integer(stats.memory_bytes)
      assert stats.memory_bytes > 0
    end
  end

  describe "max_tables backpressure" do
    test "waits when max_tables reached" do
      name = :"backpressure_test_#{:erlang.unique_integer([:positive])}"

      {:ok, pid} =
        CachedPaginator.start_link(
          name: name,
          max_tables: 2,
          max_ttl: 200,
          sweep_interval: 100
        )

      # fill up tables
      CachedPaginator.store(name, [a: 1], ["id1"])
      CachedPaginator.store(name, [b: 2], ["id2"])

      stats = CachedPaginator.stats(name)
      assert stats.table_count == 2

      # this should wait until sweep frees a table
      task =
        Task.async(fn ->
          CachedPaginator.store(name, [c: 3], ["id3"])
        end)

      # give time for sweep to run
      result = Task.await(task, 1000)
      assert {{_table, 1}, _key} = result

      GenServer.stop(pid)
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
  end
end
