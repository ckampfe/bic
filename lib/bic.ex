defmodule Bic do
  @moduledoc """
  Documentation for `Bic`.
  """

  def new(db_directory) when is_binary(db_directory) do
    with {:ok, _writer_pid} <- Bic.WriterSupervisor.start_child(db_directory) do
      {:ok, db_directory}
    else
      e ->
        e
    end
  end

  def put(db_directory, key, value) when is_binary(db_directory) do
    Bic.Writer.write(db_directory, key, value)
  end

  def fetch(db_directory, key) when is_binary(db_directory) do
    case Bic.Reader.read_value(db_directory, key) do
      {:ok, nil} ->
        {:ok, nil}

      {:ok, value} ->
        {:ok, value}

      {:error, _} = e ->
        e
    end
  end

  # def read_all(db_directory, key) when is_binary(db_directory) do
  #   Bic.Reader.read_all(db_directory, key)
  # end

  def delete(db_directory, key) when is_binary(db_directory) do
    Bic.Writer.delete_key(db_directory, key)
  end

  def keys(db_directory) when is_binary(db_directory) do
    Bic.Reader.keys(db_directory)
  end

  def close(db_directory) when is_binary(db_directory) do
    Bic.Writer.stop(db_directory)
  end
end
