defmodule MergeTest do
  use ExUnit.Case, async: true

  test "merge 2 new 1 close" do
    dir = Briefly.create!(type: :directory)
    {:ok, ^dir} = Bic.new(dir)

    :ok = Bic.put(dir, :a, 1)
    :ok = Bic.put(dir, :b, 2)
    :ok = Bic.put(dir, :c, 3)

    :ok = Bic.close(dir)
    {:ok, ^dir} = Bic.new(dir)
    :ok = Bic.put(dir, :a, 10)
    :ok = Bic.put(dir, :b, 12)

    # one old file because two opens and one close
    assert Enum.sort(File.ls!(dir)) == ["1", "2"]

    :ok = Bic.merge_async(dir)
    Bic.merge_await(dir)

    assert File.ls!(dir) == ["1", "2"]

    assert {:ok, 10} == Bic.fetch(dir, :a)
    assert {:ok, 12} == Bic.fetch(dir, :b)
    assert {:ok, 3} == Bic.fetch(dir, :c)

    Bic.close(dir)
  end

  test "merge 3 new 2 close" do
    dir = Briefly.create!(type: :directory)
    {:ok, ^dir} = Bic.new(dir)

    :ok = Bic.put(dir, :a, 1)
    :ok = Bic.put(dir, :b, 2)
    :ok = Bic.put(dir, :c, 3)

    :ok = Bic.close(dir)
    {:ok, ^dir} = Bic.new(dir)
    :ok = Bic.put(dir, :a, 10)
    :ok = Bic.put(dir, :b, 12)

    :ok = Bic.close(dir)
    {:ok, ^dir} = Bic.new(dir)

    # two old files and the active file,
    # because 3 opens and 2 closes
    assert Enum.sort(File.ls!(dir)) == ["1", "2", "3"]

    :ok = Bic.merge_async(dir)
    Bic.merge_await(dir)

    assert File.ls!(dir) == ["1", "3"]

    assert {:ok, 10} == Bic.fetch(dir, :a)
    assert {:ok, 12} == Bic.fetch(dir, :b)
    assert {:ok, 3} == Bic.fetch(dir, :c)

    Bic.close(dir)
  end

  test "merge 3 new 2 close with deletes" do
    dir = Briefly.create!(type: :directory)
    {:ok, ^dir} = Bic.new(dir)

    :ok = Bic.put(dir, :a, 1)
    :ok = Bic.put(dir, :b, 2)
    :ok = Bic.put(dir, :c, 3)

    :ok = Bic.close(dir)
    {:ok, ^dir} = Bic.new(dir)
    :ok = Bic.put(dir, :a, 10)
    :ok = Bic.put(dir, :b, 12)
    :ok = Bic.delete(dir, :b)

    :ok = Bic.close(dir)
    {:ok, ^dir} = Bic.new(dir)

    # two old files and the active file,
    # because 3 opens and 2 closes
    assert Enum.sort(File.ls!(dir)) == ["1", "2", "3"]

    :ok = Bic.merge_async(dir)
    Bic.merge_await(dir)

    assert [:a, :c] == Bic.keys(dir) |> Enum.sort()
    assert {:ok, 10} == Bic.fetch(dir, :a)
    assert :error == Bic.fetch(dir, :b)
    assert {:ok, 3} == Bic.fetch(dir, :c)

    assert File.ls!(dir) == ["1", "3"]

    :ok = Bic.close(dir)
    {:ok, ^dir} = Bic.new(dir)

    assert [:a, :c] == Bic.keys(dir) |> Enum.sort()
    assert {:ok, 10} == Bic.fetch(dir, :a)
    assert :error == Bic.fetch(dir, :b)
    assert {:ok, 3} == Bic.fetch(dir, :c)

    Bic.close(dir)
  end

  test "lock prevents stale reads" do
    dir = Briefly.create!(type: :directory)
    {:ok, ^dir} = Bic.new(dir)

    :ok = Bic.put(dir, :a, 1)
    :ok = Bic.put(dir, :b, 2)
    :ok = Bic.put(dir, :c, 3)

    :ok = Bic.close(dir)
    {:ok, ^dir} = Bic.new(dir)
    :ok = Bic.put(dir, :a, 10)
    :ok = Bic.put(dir, :b, 12)

    # one old file because two opens and one close
    assert Enum.sort(File.ls!(dir)) == ["1", "2"]

    test_pid = self()

    # do a bunch of reads as fast as possible to simulate
    # a real read workload on a database that is going to merge
    child =
      spawn(fn ->
        lock_error =
          Stream.repeatedly(fn ->
            Bic.fetch(dir, :a)
          end)
          |> Enum.find(fn
            {:error, :database_is_locked_for_merge} ->
              {:error, :database_is_locked_for_merge}

            _ ->
              nil
          end)

        Process.send(test_pid, {self(), lock_error}, [])
      end)

    :ok = Bic.merge_async(dir)

    receive do
      {^child, lock_error} ->
        assert lock_error == {:error, :database_is_locked_for_merge}
    end

    Bic.merge_await(dir)

    assert File.ls!(dir) == ["1", "2"]

    assert {:ok, 10} == Bic.fetch(dir, :a)
    assert {:ok, 12} == Bic.fetch(dir, :b)
    assert {:ok, 3} == Bic.fetch(dir, :c)

    Bic.close(dir)
  end

  test "does not double merge" do
    dir = Briefly.create!(type: :directory)
    {:ok, ^dir} = Bic.new(dir)

    :ok = Bic.put(dir, :a, 1)
    :ok = Bic.put(dir, :b, 2)
    :ok = Bic.put(dir, :c, 3)
    :ok = Bic.merge_async(dir)
    assert {:error, :merge_already_started} == Bic.merge_async(dir)
    Bic.merge_await(dir)
    Bic.close(dir)
  end

  test "a lot of db files, same key" do
    dir = Briefly.create!(type: :directory)

    ops =
      Enum.map(1..100, fn _ ->
        {:ok, ^dir} = Bic.new(dir)
        value = :rand.bytes(10)
        :ok = Bic.put(dir, :k, value)
        :ok = Bic.close(dir)
        value
      end)

    {:ok, ^dir} = Bic.new(dir)
    :ok = Bic.merge_async(dir)
    {:ok, _} = Bic.merge_await(dir, :infinity)
    assert 2 == Enum.count(File.ls!(dir))
    assert {:ok, List.last(ops)} == Bic.fetch(dir, :k)
  end

  test "a lot of db files, many keys" do
    dir = Briefly.create!(type: :directory)

    keys =
      Enum.map(1..10, fn _ ->
        :rand.bytes(5)
      end)

    values =
      Enum.map(1..10, fn _ ->
        :rand.bytes(5)
      end)

    inserts =
      Enum.flat_map(1..100, fn _ ->
        {:ok, ^dir} = Bic.new(dir)

        number_of_inserts = :rand.uniform(100)

        inserts =
          Enum.map(1..number_of_inserts, fn _ ->
            key = Enum.random(keys)
            value = Enum.random(values)
            :ok = Bic.put(dir, key, value)

            {
              key,
              value,
              :erlang.unique_integer([:positive, :monotonic])
            }
          end)

        :ok = Bic.close(dir)

        inserts
      end)

    # IO.inspect(Enum.count(inserts), label: "total inserts")

    most_recent_inserts =
      inserts
      |> Enum.group_by(fn {k, _v, _i} ->
        k
      end)
      |> Enum.map(fn {_k, inserts} ->
        inserts
        |> Enum.sort_by(fn {_k, _v, i} -> i end, :desc)
        |> List.first()
      end)

    {:ok, ^dir} = Bic.new(dir)
    :ok = Bic.merge_async(dir)
    {:ok, _} = Bic.merge_await(dir, :infinity)

    Enum.each(most_recent_inserts, fn {k, v, _i} ->
      assert {:ok, v} == Bic.fetch(dir, k)
    end)

    # IO.inspect(File.ls!(dir), label: "files")
    # IO.inspect(Enum.count(Bic.keys(dir)), label: "keycount")

    assert Enum.sort(Enum.map(most_recent_inserts, fn {k, _v, _i} -> k end)) ==
             Enum.sort(Bic.keys(dir))
  end
end
