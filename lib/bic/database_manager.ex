defmodule Bic.DatabaseManager do
  use GenServer

  # ets schema is:
  # {db_directory (binary), keydir (ets table id)}
  @table :bic_databases

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def new(directory, tid) do
    GenServer.call(__MODULE__, {:new, directory, tid})
  end

  def remove(db_directory) do
    GenServer.call(__MODULE__, {:remove, db_directory})
  end

  @impl GenServer
  def init(args) do
    {:ok, args, {:continue, :setup}}
  end

  @impl GenServer
  def handle_continue(:setup, state) do
    @table =
      :ets.new(@table, [:protected, :named_table, :set, read_concurrency: true])

    {:noreply, state}
  end

  @impl GenServer
  def handle_call({:new, db_directory, tid}, _from, state) do
    case :ets.lookup(@table, db_directory) do
      [] ->
        :ets.insert(@table, {db_directory, tid})
        {:reply, :ok, state}

      _ ->
        {:reply, {:error, :database_already_exists}, state}
    end
  end

  @impl GenServer
  def handle_call({:remove, db_directory}, _from, state) do
    true = :ets.delete(@table, db_directory)
    {:reply, :ok, state}
  end
end
