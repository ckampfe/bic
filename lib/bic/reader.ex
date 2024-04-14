defmodule Bic.Reader do
  @moduledoc false

  alias Bic.{Binary, DatabaseManager, Keydir}

  def fetch(db_directory, key) when is_binary(db_directory) do
    with {:database_tid_lookup, {:ok, keydir_tid}} <-
           {:database_tid_lookup, Bic.DatabaseManager.fetch(db_directory)},
         {:key_lookup, {:ok, {_key, active_file_id, value_size, value_offset, _tx_id}}} <-
           {:key_lookup, Keydir.fetch(keydir_tid, key)},
         db_file = Path.join([db_directory, to_string(active_file_id)]),
         {:file_open, {:ok, file}} <- {:file_open, File.open(db_file, [:read, :raw])},
         {:file_seek, {:ok, _}} <- {:file_seek, :file.position(file, value_offset)},
         {:file_read, value_bytes} <- {:file_read, IO.binread(file, value_size)} do
      {:ok, Binary.decode_binary(value_bytes)}
    else
      {:database_tid_lookup, :error} ->
        {:error, {:database_does_not_exist, db_directory}}

      {:key_lookup, :error} ->
        {:ok, nil}

      {:file_open, e} ->
        e

      {:file_seek, e} ->
        e

      {:file_read, e} ->
        e
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
