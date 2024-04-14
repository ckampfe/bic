defmodule Bic.Keydir do
  @moduledoc false

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
    true = :ets.insert(keydir, entry_or_entries)
  end

  @spec keys(:ets.tid()) :: list()
  def keys(keydir) do
    :ets.select(keydir, [{{:"$1", :_, :_, :_, :_}, [], [:"$1"]}])
  end
end
