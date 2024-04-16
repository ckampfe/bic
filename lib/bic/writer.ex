defmodule Bic.Writer do
  @moduledoc false

  use GenServer, restart: :transient
  alias Bic.{Binary, DatabaseManager, Keydir, Reader}
  require Logger

  @hash_size Binary.hash_size()

  def start_link(%{db_directory: db_directory, options: _options} = args)
      when is_binary(db_directory) do
    GenServer.start_link(__MODULE__, args, name: {:via, Registry, {Bic.Registry, db_directory}})
  end

  def write(db_directory, key, value) do
    [{pid, _}] = Registry.lookup(Bic.Registry, db_directory)
    GenServer.call(pid, {:write, key, {:insert, value}})
  end

  def update(db_directory, key, default, fun) do
    [{pid, _}] = Registry.lookup(Bic.Registry, db_directory)
    GenServer.call(pid, {:update, key, default, fun})
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
    keydir = Keydir.new()
    :ok = DatabaseManager.register(db_directory, keydir)

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

    true = Keydir.insert(keydir, entries)

    {:ok, active_file} =
      [db_directory, to_string(active_file_id)]
      |> Path.join()
      |> File.open([:append, :raw])

    state =
      state
      |> Map.put(:active_file, active_file)
      |> Map.put(:active_file_id, active_file_id)
      |> Map.put(:keydir, keydir)
      |> Map.put(:offset, 0)
      |> Map.put(:tx_id, latest_tx_id + 1)

    {:ok, state}
  end

  @impl GenServer
  def handle_call(
        {:write, key, value},
        _from,
        %{
          db_directory: db_directory,
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

        # pass through untouched,
        # as when reading we just do a direct binary comparison
        # against this value
        :delete ->
          Binary.tombstone()
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

    @hash_size = byte_size(hash)

    # an iolist, so there is no further serialization,
    # we write this directly to disk
    entry = [hash, payload]

    :ok = IO.binwrite(active_file, entry)
    :ok = :file.datasync(active_file)

    value_position = offset + Binary.header_size() + key_size

    Keydir.insert(
      keydir,
      {
        key,
        active_file_id,
        value_size,
        value_position,
        tx_id
      }
    )

    entry_size = Binary.header_size() + key_size + value_size

    state =
      state
      |> Map.update!(:offset, fn offset ->
        offset + entry_size
      end)
      |> Map.put(:tx_id, tx_id)

    state =
      if state[:offset] >= state[:options][:max_file_size_bytes] do
        File.close(active_file)

        active_file_id = active_file_id + 1

        {:ok, new_active_file} =
          [db_directory, to_string(active_file_id)]
          |> Path.join()
          |> File.open([:append, :raw])

        state
        |> Map.put(:active_file_id, active_file_id)
        |> Map.put(:offset, 0)
        |> Map.put(:active_file, new_active_file)
      else
        state
      end

    {:reply, :ok, state}
  end

  def handle_call(
        {:delete, key},
        from,
        %{keydir: keydir} = state
      ) do
    case Keydir.fetch(keydir, key) do
      {:ok, _} ->
        {:reply, :ok, state} =
          handle_call({:write, key, :delete}, from, state)

        Keydir.delete(keydir, key)

        {:reply, :ok, state}

      :error ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:update, key, default, fun}, from, %{db_directory: db_directory} = state) do
    case Reader.fetch(db_directory, key) do
      {:ok, value} ->
        new_value = fun.(value)
        {:reply, :ok, state} = handle_call({:write, key, {:insert, new_value}}, from, state)
        {:reply, {:ok, new_value}, state}

      :error ->
        new_value = fun.(default)
        {:reply, :ok, state} = handle_call({:write, key, {:insert, new_value}}, from, state)
        {:reply, {:ok, new_value}, state}

      {:error, _} = e ->
        {:reply, e, state}
    end
  end

  def handle_call(:stop, _from, %{db_directory: db_directory} = state) do
    :ok = Registry.unregister(Bic.Registry, db_directory)
    :ok = DatabaseManager.unregister(db_directory)
    {:stop, :shutdown, :ok, state}
  end
end
