defmodule Bic.Writer do
  use GenServer, restart: :transient
  alias Bic.Binary
  require Logger

  @hash_size Binary.hash_size()

  def start_link(%{db_directory: db_directory} = args) when is_binary(db_directory) do
    GenServer.start_link(__MODULE__, args, name: {:via, Registry, {Bic.Registry, db_directory}})
  end

  def write(db_directory, key, value) do
    [{pid, _}] = Registry.lookup(Bic.Registry, db_directory)
    GenServer.call(pid, {:write, key, {:insert, value}})
  end

  def delete_key(db_directory, key) do
    [{pid, _}] = Registry.lookup(Bic.Registry, db_directory)
    GenServer.call(pid, {:delete, key})
  end

  def stop(db_directory) do
    [{pid, _}] = Registry.lookup(Bic.Registry, db_directory)
    GenServer.call(pid, :stop)
  end

  # this is all in init (rather than handle_continue) because
  # otherwise genserver is asynchronous and it is possible
  # for the process/db to appear as though it is up,
  # while not actually being ready to accept queries,
  # because the database files have not yet been loaded.
  # having it in it
  #
  # is this the right thing to do? not sure.
  # but the race condition *is possible* if we use handle_continue instead.
  @impl GenServer
  def init(%{db_directory: db_directory} = state) do
    keydir_tid = :ets.new(:keydir, [:protected, :set, read_concurrency: true])
    :ok = Bic.DatabaseManager.new(db_directory, keydir_tid)

    db_files =
      db_directory
      |> File.ls!()
      |> Stream.map(fn file_name ->
        case Integer.parse(file_name) do
          {i, _} -> {:ok, i}
          _ -> nil
        end
      end)
      |> Stream.filter(fn
        {:ok, _i} -> true
        _ -> false
      end)
      |> Stream.map(fn {:ok, i} -> i end)

    latest_file_id =
      db_files
      |> Enum.sort(:desc)
      |> List.first()

    active_file_id =
      case latest_file_id do
        nil ->
          1

        id ->
          id + 1
      end

    {entries, latest_tx_id} = Bic.Loader.load(db_directory)

    entries =
      Enum.map(entries, fn {key, {file_id, value_size, value_position, tx_id}} ->
        {key, file_id, value_size, value_position, tx_id}
      end)

    tx_id = latest_tx_id + 1

    true = :ets.insert(keydir_tid, entries)

    {:ok, active_file} =
      [db_directory, to_string(active_file_id)]
      |> Path.join()
      |> File.open([:exclusive, :append, :raw])

    state =
      state
      |> Map.put(:active_file, active_file)
      |> Map.put(:active_file_id, active_file_id)
      |> Map.put(:keydir, keydir_tid)
      |> Map.put(:offset, 0)
      |> Map.put(:tx_id, tx_id)

    # {:ok, args, {:continue, :setup}}
    {:ok, state}
  end

  # @impl GenServer
  # def handle_continue(:setup, %{db_directory: db_directory} = state) do

  #   {:noreply, state}
  # end

  @impl GenServer
  def handle_call(
        {:write, key, value},
        _from,
        %{
          keydir: keydir,
          active_file: active_file,
          offset: offset,
          active_file_id: active_file_id,
          tx_id: tx_id
        } = state
      ) do
    tx_id = tx_id + 1
    encoded_tx_id = Binary.encode_u128_be(tx_id)
    encoded_key = Binary.encode_term(key)

    encoded_value =
      case value do
        {:insert, v} ->
          Binary.encode_term(v)

        # pass through untouched
        {:delete, v} ->
          v
      end

    key_size =
      byte_size(encoded_key)

    value_size =
      byte_size(encoded_value)

    encoded_key_size = Binary.encode_u32_be(key_size)
    encoded_value_size = Binary.encode_u32_be(value_size)

    payload =
      [
        encoded_tx_id,
        encoded_key_size,
        encoded_value_size,
        encoded_key,
        encoded_value
      ]

    hash =
      Binary.hash(payload)

    # guarantee that the hash length is always 64 bytes
    # :crypto.hash/2 can return anything, I don't think
    # the API guarantees that it is 64 bytes,
    # so we need to guard it
    @hash_size = byte_size(hash)

    # an iolist, so there is no further serialization,
    # we write this directly to disk
    entry = [hash, payload]

    :ok = IO.binwrite(active_file, entry)

    # size of hash + size of serialized payload
    value_position = offset + Binary.header_size() + key_size

    :ets.insert(keydir, {
      key,
      active_file_id,
      value_size,
      value_position,
      tx_id
    })

    entry_size = Binary.header_size() + key_size + value_size

    state =
      state
      |> Map.update!(:offset, fn offset ->
        offset + entry_size
      end)
      |> Map.put(:tx_id, tx_id)

    {:reply, :ok, state}
  end

  def handle_call(
        {:delete, key},
        from,
        %{keydir: keydir} = state
      ) do
    case :ets.lookup(keydir, key) do
      [] ->
        {:reply, :ok, state}

      _ ->
        {:reply, :ok, state} =
          handle_call({:write, key, {:delete, Binary.tombstone()}}, from, state)

        :ets.delete(keydir, key)

        {:reply, :ok, state}
    end
  end

  def handle_call(:stop, _from, %{db_directory: db_directory} = state) do
    :ok = Registry.unregister(Bic.Registry, db_directory)
    # this shouldn't have to automatically be deleted, as it will go away
    # when the owning process (this process) shuts down
    # true = :ets.delete(keydir_tid)
    :ok = Bic.DatabaseManager.remove(db_directory)
    {:stop, :shutdown, :ok, state}
  end
end
