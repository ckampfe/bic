defmodule BicTest do
  use ExUnit.Case
  # doctest Bic

  # setup_all do
  #   Briefly.start(nil, nil)
  #   :application.ensure_started(:briefly)
  #   on_exit(fn -> Briefly.cleanup() end)
  # end

  # setup do
  #   on_exit(fn ->
  #     Briefly.cleanup()
  #   end)

  #   dir = Briefly.create!(type: :directory)
  #   [dir: dir]
  # end

  test "roundtrip simple" do
    dir = Briefly.create!(type: :directory)

    key = "some key"
    value = %{some: 123, value: [:a, "b", :c], x: %{}}
    {:ok, ^dir} = Bic.new(dir)
    :ok = Bic.put(dir, key, value)

    assert {:ok, value} == Bic.fetch(dir, key)

    Bic.close(dir)
  end

  test "roundtrip interleaved" do
    dir = Briefly.create!(type: :directory)

    key1 = "key1"
    key2 = "key2"
    value1 = %{some: 123, value: [:a, "b", :c], x: %{}}
    value2 = :zzzzzzzzz
    value3 = :xxxxxxxx
    value4 = []

    {:ok, ^dir} = Bic.new(dir)

    :ok = Bic.put(dir, key1, value1)
    assert {:ok, value1} == Bic.fetch(dir, key1)

    :ok = Bic.put(dir, key2, value2)
    assert {:ok, value2} == Bic.fetch(dir, key2)

    :ok = Bic.put(dir, key1, value3)
    assert {:ok, value3} == Bic.fetch(dir, key1)

    assert {:ok, value2} == Bic.fetch(dir, key2)

    :ok = Bic.put(dir, key1, value4)
    assert {:ok, value4} == Bic.fetch(dir, key1)

    Bic.close(dir)
  end

  test "delete" do
    dir = Briefly.create!(type: :directory)

    {:ok, ^dir} = Bic.new(dir)
    :ok = Bic.put(dir, "k", "v")
    assert {:ok, "v"} == Bic.fetch(dir, "k")
    :ok = Bic.delete(dir, "k")
    assert {:ok, nil} == Bic.fetch(dir, "k")

    Bic.close(dir)
  end

  test "multiple dbs" do
    dir1 = Briefly.create!(type: :directory)

    assert {:ok, dir1} == Bic.new(dir1)

    dir2 = Briefly.create!(type: :directory)

    assert {:ok, dir2} == Bic.new(dir2)

    Bic.put(dir1, "a", "b")
    Bic.put(dir2, "c", "d")

    assert {:ok, "b"} == Bic.fetch(dir1, "a")
    assert {:ok, nil} == Bic.fetch(dir1, "c")
    assert {:ok, "d"} == Bic.fetch(dir2, "c")
    assert {:ok, nil} == Bic.fetch(dir2, "a")

    Bic.close(dir1)
    Bic.close(dir2)
  end

  test "loads database files" do
    dir = Briefly.create!(type: :directory)

    # 1
    {:ok, ^dir} = Bic.new(dir)
    :ok = Bic.put(dir, "a", "b")
    :ok = Bic.close(dir)

    # 2
    {:ok, ^dir} = Bic.new(dir)
    assert {:ok, nil} == Bic.fetch(dir, "b")
    :ok = Bic.put(dir, "c", "d")
    :ok = Bic.put(dir, "e", "f")
    :ok = Bic.close(dir)

    # 3
    {:ok, ^dir} = Bic.new(dir)
    assert {:ok, "b"} == Bic.fetch(dir, "a")
    assert {:ok, "d"} == Bic.fetch(dir, "c")
    assert {:ok, "f"} == Bic.fetch(dir, "e")
    assert ["a", "c", "e"] == Bic.keys(dir) |> Enum.sort()
    :ok = Bic.delete(dir, "c")
    :ok = Bic.close(dir)

    # 4
    {:ok, ^dir} = Bic.new(dir)
    assert {:ok, nil} == Bic.fetch(dir, "c")
    assert ["a", "e"] == Bic.keys(dir) |> Enum.sort()
    :ok = Bic.put(dir, "e", "g")
    assert {:ok, "g"} == Bic.fetch(dir, "e")
  end
end
