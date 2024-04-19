defmodule BicTest do
  use ExUnit.Case, async: true
  doctest Bic

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
    assert :error == Bic.fetch(dir, "k")

    Bic.close(dir)
  end

  test "nil is a valid value" do
    dir = Briefly.create!(type: :directory)
    assert {:ok, dir} == Bic.new(dir)
    assert :ok == Bic.put(dir, "a", nil)
    assert {:ok, nil} == Bic.fetch(dir, "a")
  end

  test "multiple dbs" do
    dir1 = Briefly.create!(type: :directory)

    assert {:ok, dir1} == Bic.new(dir1)

    dir2 = Briefly.create!(type: :directory)

    assert {:ok, dir2} == Bic.new(dir2)

    Bic.put(dir1, "a", "b")
    Bic.put(dir2, "c", "d")

    assert {:ok, "b"} == Bic.fetch(dir1, "a")
    assert :error == Bic.fetch(dir1, "c")
    assert {:ok, "d"} == Bic.fetch(dir2, "c")
    assert :error == Bic.fetch(dir2, "a")

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
    assert :error == Bic.fetch(dir, "b")
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
    assert :error == Bic.fetch(dir, "c")
    assert ["a", "e"] == Bic.keys(dir) |> Enum.sort()
    :ok = Bic.put(dir, "e", "g")
    assert {:ok, "g"} == Bic.fetch(dir, "e")
  end

  test "update/4 key is present" do
    dir = Briefly.create!(type: :directory)
    assert {:ok, dir} == Bic.new(dir)
    assert :ok == Bic.put(dir, "a", "b")

    assert {:ok, "bbbb"} ==
             Bic.update(dir, "a", fn current ->
               current <> current <> current <> current
             end)

    assert {:ok, "bbbb"} == Bic.fetch(dir, "a")
  end

  test "update/4 key is not present" do
    dir = Briefly.create!(type: :directory)
    assert {:ok, dir} == Bic.new(dir)

    assert {:ok, "xxxx"} ==
             Bic.update(dir, "a", "x", fn current ->
               current <> current <> current <> current
             end)

    assert {:ok, "xxxx"} == Bic.fetch(dir, "a")

    assert :ok == Bic.put(dir, 1, nil)

    # nil is not treated differently,
    # it is a valid value to insert.
    # if you want the key to not exist,
    # you must `delete` it.
    assert {:ok, nil} ==
             Bic.update(dir, 1, fn current ->
               current
             end)

    assert {:ok, nil} == Bic.fetch(dir, 1)
  end

  test "max file size" do
    dir = Briefly.create!(type: :directory)
    {:ok, ^dir} = Bic.new(dir, max_file_size_bytes: 100)

    # file initially contains 0 bytes
    :ok = Bic.put(dir, :a, 10)
    # file contains 35 bytes
    :ok = Bic.put(dir, :b, 43)
    # file contains 70 bytes
    :ok = Bic.put(dir, :c, 10)
    # file contains 105 bytes, triggers new file creation
    :ok = Bic.put(dir, :a, 20)
    # new file contains 35 bytes, old file still exists

    files = File.ls!(dir)
    assert ["1", "2"] == Enum.sort(files)
    assert Enum.count(files) == 2

    assert {:ok, 20} == Bic.fetch(dir, :a)
    assert {:ok, 43} == Bic.fetch(dir, :b)
    assert {:ok, 10} == Bic.fetch(dir, :c)
  end
end
