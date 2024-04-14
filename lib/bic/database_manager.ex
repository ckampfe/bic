defmodule Bic.DatabaseManager do
  @moduledoc false

  @table :bic_databases

  def create() do
    @table =
      :ets.new(@table, [
        :public,
        :named_table,
        :set,
        read_concurrency: true,
        write_concurrency: :auto
      ])

    :ok
  end

  def register(db_directory, tid) do
    case :ets.lookup(@table, db_directory) do
      [] ->
        :ets.insert(@table, {db_directory, tid})
        :ok

      _ ->
        {:error, :database_already_exists}
    end
  end

  @spec fetch(binary()) :: {:ok, :ets.tid()} | :error
  def fetch(db_directory) do
    case :ets.lookup(@table, db_directory) do
      [{^db_directory, keydir_tid}] -> {:ok, keydir_tid}
      _ -> :error
    end
  end

  def unregister(db_directory) do
    true = :ets.delete(@table, db_directory)
    :ok
  end
end
