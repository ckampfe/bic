defmodule Bic.Reader do
  @moduledoc false

  alias Bic.{Binary, DatabaseManager, Keydir, Lock}

  def fetch(db_directory, key) when is_binary(db_directory) do
    with {:merge_lock, :unlocked} <-
           {:merge_lock, Lock.status(Lock.get_handle({Bic, db_directory, :merge_lock}))},
         {:database_tid_lookup, {:ok, keydir_tid}} <-
           {:database_tid_lookup, Bic.DatabaseManager.fetch(db_directory)},
         {:key_lookup, {:ok, {_key, file_id, value_size, value_offset, _tx_id}}} <-
           {:key_lookup, Keydir.fetch(keydir_tid, key)},
         db_file = Path.join([db_directory, to_string(file_id)]),
         {:file_open, {:ok, file}} <- {:file_open, File.open(db_file, [:read, :raw])},
         {:file_seek, {:ok, _}} <- {:file_seek, :file.position(file, value_offset)},
         {:file_read, value_bytes} <- {:file_read, IO.binread(file, value_size)} do
      {:ok, Binary.decode_binary(value_bytes)}
    else
      {:database_tid_lookup, :error} ->
        {:error, {:database_does_not_exist, db_directory}}

      # this returns `:error` to match
      # Elixir's `Map.fetch/2`
      {:key_lookup, :error} ->
        :error

      {:file_open, e} ->
        e

      {:file_seek, e} ->
        e

      {:file_read, e} ->
        e

      {:merge_lock, :locked} ->
        {:error, :database_is_locked_for_merge}
    end
  end

  def keys(db_directory) do
    case DatabaseManager.fetch(db_directory) do
      {:ok, keydir_tid} ->
        Keydir.keys(keydir_tid)

      :error ->
        []
    end
  end
end
