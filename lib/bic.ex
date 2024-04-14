defmodule Bic do
  @moduledoc """
  Documentation for `Bic`, an implementation of [Bitcask](https://riak.com/assets/bitcask-intro.pdf).
  """

  @doc """
  f data exists in the directory, it will be loaded.
  """
  @spec new(binary()) :: :ignore | {:error, any()} | {:ok, binary()} | {:ok, pid(), any()}
  def new(db_directory) when is_binary(db_directory) do
    with {:ok, _writer_pid} <- Bic.WriterSupervisor.start_child(db_directory) do
      {:ok, db_directory}
    else
      e ->
        e
    end
  end

  @doc """
  Insert a key/value into the database.
  """
  @spec put(binary(), any(), any()) :: :ok
  def put(db_directory, key, value) when is_binary(db_directory) do
    Bic.Writer.write(db_directory, key, value)
  end

  @doc """
  Get a value from the database.
  """
  @spec fetch(binary(), any()) ::
          {:ok, any()} | {:error, atom() | {:database_does_not_exist, binary()}}
  def fetch(db_directory, key) when is_binary(db_directory) do
    case Bic.Reader.fetch(db_directory, key) do
      {:ok, nil} ->
        {:ok, nil}

      {:ok, value} ->
        {:ok, value}

      {:error, _} = e ->
        e
    end
  end

  @doc """
  Delete a key/value from the database.
  This is accomplished by inserting a tombstone value that
  is removed on the next database merge.
  """
  @spec delete(binary(), any()) :: :ok
  def delete(db_directory, key) when is_binary(db_directory) do
    Bic.Writer.delete_key(db_directory, key)
  end

  @doc """
  Return the list of active keys for the database.
  This only lists live keys and does not include any
  keys that have been deleted.
  """
  @spec keys(binary()) :: list()
  def keys(db_directory) when is_binary(db_directory) do
    Bic.Reader.keys(db_directory)
  end

  @doc """
  Shuts down the database.
  When this operation completes,
  subsequent calls to the functions in this module for the given `db_directory`
  will fail.
  """
  @spec close(binary()) :: any()
  def close(db_directory) when is_binary(db_directory) do
    Bic.Writer.stop(db_directory)
  end

  @doc """
  Not yet implemented.
  """
  def merge(db_directory) when is_binary(db_directory) do
    {:error, :todo}
  end
end
