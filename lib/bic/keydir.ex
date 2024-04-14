defmodule Bic.Keydir do
  @moduledoc false

  @spec new() :: :ets.tid()
  def new() do
    :ets.new(:keydir, [:protected, :set, read_concurrency: true])
  end

  @spec fetch(:ets.tid(), any()) :: {:ok, tuple()} | :error
  def fetch(keydir, key) do
    case :ets.lookup(keydir, key) do
      [] ->
        :error

      [{key, active_file_id, value_size, value_offset, tx_id}] ->
        {:ok, {key, active_file_id, value_size, value_offset, tx_id}}
    end
  end

  @spec insert(:ets.tid(), [tuple()] | tuple()) :: true
  def insert(keydir, entry_or_entries)
      when is_tuple(entry_or_entries) or is_list(entry_or_entries) do
    :ets.insert(keydir, entry_or_entries)
  end

  @spec delete(:ets.tid(), any()) :: true
  def delete(keydir, key) do
    :ets.delete(keydir, key)
  end

  @spec keys(:ets.tid()) :: list()
  def keys(keydir) do
    :ets.select(keydir, [{{:"$1", :_, :_, :_, :_}, [], [:"$1"]}])
  end
end
