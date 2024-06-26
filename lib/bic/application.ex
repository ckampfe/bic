defmodule Bic.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    Bic.DatabaseManager.create()

    children = [
      {Registry, keys: :unique, name: Bic.Registry},
      {Task.Supervisor, name: Bic.MergeSupervisor},
      {Bic.WriterSupervisor, %{}}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Bic.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
