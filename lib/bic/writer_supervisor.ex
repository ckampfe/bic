defmodule Bic.WriterSupervisor do
  @moduledoc false

  use DynamicSupervisor

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  def start_child(db_directory, options) do
    spec = {Bic.Writer, %{db_directory: db_directory, options: options}}
    DynamicSupervisor.start_child(__MODULE__, spec)
  end

  @impl true
  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
