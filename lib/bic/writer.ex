defmodule Bic.Writer do
  @moduledoc false
  use GenServer, restart: :transient
  alias Bic.{Binary, DatabaseManager, Keydir, Loader, Lock, Merger, Reader}
  require Bic.Lock
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

  def merge_async(db_directory) do
    [{pid, _}] = Registry.lookup(Bic.Registry, db_directory)
    GenServer.call(pid, :merge_async)
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

    db_files = Loader.db_files(db_directory)

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

    {entries, latest_tx_id} = Bic.Loader.load(db_directory, db_files)

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

    Lock.new({Bic, db_directory, :merge_lock})

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

  @impl GenServer
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

  @impl GenServer
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

  def handle_call(
        :merge_async,
        _from,
        %{merge_ref: merge_ref} =
          state
      )
      when not is_nil(merge_ref) do
    {:reply, {:error, :merge_already_started}, state}
  end

  @impl GenServer
  def handle_call(
        :merge_async,
        {from, _ref},
        %{db_directory: db_directory, active_file_id: active_file_id, options: options} = state
      ) do
    task =
      Task.Supervisor.async_nolink(Bic.MergeSupervisor, fn ->
        max_file_size_bytes = Keyword.fetch!(options, :max_file_size_bytes)

        nonactive_db_files =
          Bic.Loader.nonactive_db_files(db_directory, active_file_id) |> Enum.into([])

        merged_records = Bic.Merger.load(db_directory, nonactive_db_files) |> Enum.into([])

        # TODO this should report back:
        # 1. how many old files there were
        # 2. how many old records there were
        # 3. how many old records there are now
        Merger.write_records_for_merge(
          db_directory,
          merged_records,
          max_file_size_bytes,
          nonactive_db_files
        )

        # TODO
        # - `merge` function in `loader.ex` that is like `load`,
        #   but instead of returning keydir Entries it returns the actual
        #   on-disk payloads.
        # - `bulk` module
        # - automerge functionality of some kind. ideas:
        #   - every time an existing key is written to, increment a counter.
        #     when `counter / size(keyspace)` becomes large enough, run a merge.
        #     example: 10,000 keys, each has 4 writes total, so counter is 30,000
        #     (first write to each key doesn't count).
        #     "merge factor" is then 3.
        #     interesting effects:
        #       - counter only grows (until merge)
        #       - deletes decrease the size of the keyspace, which in turn
        #         increases the "merge factor":
        #         i.e. you have 10,000 keys and 30,000 counter and you
        #         delete 2,000 keys, leaving 8,000 live keys,
        #         merge factor becomes 3.75 instead of 3.0
        #       - alternatively, also increment the counter on deletes,
        #         so deleting 2,000 keys goes to counter = 32,000,
        #         keyspace 8,000, merge factor 4.0
        #   - probably need some kind ability to take actual on-disk size into
        #     effect. don't trigger merge if the on-disk size is only 40MB, etc.
        #     have the ability to set a threshold, so merging is only enabled
        #     when total db disk usage >= i.e., 5GB, or whatever.
        # "foooooooooo"
      end)

    state =
      state
      |> Map.put(:merge_ref, task.ref)
      |> Map.put(:merge_reply_to, from)

    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call(:stop, _from, %{db_directory: db_directory} = state) do
    :ok = Registry.unregister(Bic.Registry, db_directory)
    :ok = DatabaseManager.unregister(db_directory)
    {:stop, :shutdown, :ok, state}
  end

  @doc """
  this is the critical section (in terms of coordination).
  the single-threaded section of the merge occurs here.
  we have to perform:
  - insertions into keydir
  - removal of old files
  - renaming of merge files into regular files
  - reply to the subscribing process with the results
  """
  @impl GenServer
  def handle_info(
        {ref, answer},
        %{
          merge_ref: ref,
          merge_reply_to: reply_to,
          db_directory: db_directory,
          keydir: keydir,
          active_file_id: active_file_id
        } =
          state
      ) do
    # `:flush` here removes the :DOWN message,
    # so we don't receive it
    Process.demonitor(ref, [:flush])

    nonactive_file_ids =
      Loader.nonactive_db_files(db_directory, active_file_id)
      |> Enum.into([])

    Lock.with_lock Lock.get_handle({Bic, db_directory, :merge_lock}) do
      Enum.each(nonactive_file_ids, fn nonactive_file_id ->
        File.rm(Path.join([db_directory, to_string(nonactive_file_id)]))
      end)

      merge_files =
        File.ls!(db_directory)
        |> Enum.filter(fn f ->
          Path.extname(f) == ".merge"
        end)

      Enum.each(merge_files, fn merge_file ->
        rename_source =
          Path.join([db_directory, merge_file])

        rename_target =
          Path.join([db_directory, Path.rootname(merge_file, ".merge")])

        File.rename!(
          rename_source,
          rename_target
        )
      end)

      Keydir.insert_if_later(keydir, answer)
    end

    state =
      state
      |> Map.delete(:merge_ref)
      |> Map.delete(:merge_reply_to)

    # todo this should report back to the reply_to process
    # the number of files and records on disk before and after the merge,
    # and how long it took to perform the merge in wall clock time
    Process.send(reply_to, {:ok, :ok}, [])

    {:noreply, state}
  end

  @impl GenServer
  def handle_info(
        {:DOWN, ref, :process, _pid, reason},
        %{db_directory: db_directory, merge_ref: ref, merge_reply_to: reply_to} = state
      ) do
    if reason != :normal do
      Process.send(reply_to, {:error, reason}, [])
      Logger.warning("merge process for #{db_directory} failed!")
    end

    state =
      state
      |> Map.delete(:merge_ref)
      |> Map.delete(:merge_reply_to)

    {:noreply, state}
  end

  @impl GenServer
  def handle_info(msg, state) do
    Logger.warning("unrecognized message: #{inspect(msg)}")
    {:noreply, state}
  end
end
