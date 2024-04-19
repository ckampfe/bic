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

      [{key, file_id, value_size, value_offset, tx_id}] ->
        {:ok, {key, file_id, value_size, value_offset, tx_id}}
    end
  end

  @spec insert(:ets.tid(), [tuple()] | tuple()) :: true
  def insert(keydir, entry_or_entries)
      when is_tuple(entry_or_entries) or is_list(entry_or_entries) do
    :ets.insert(keydir, entry_or_entries)
  end

  @spec insert_if_later(:ets.tid(), list()) :: :ok
  def insert_if_later(keydir, entries) when is_list(entries) do
    Enum.each(entries, fn {key, _, _, _, new_tx_id} = new_entry ->
      case fetch(keydir, key) do
        # if the new txid is >= the existing txid for a given key,
        # that means the key is still live, and the location of the
        # record this entry points to could be in a new file on disk,
        # given the merge
        {:ok, {_, _, _, _, existing_tx_id}} ->
          if new_tx_id >= existing_tx_id do
            insert(keydir, new_entry)
          end

        # if the key does not exist, that means it was deleted
        # from the keydir and active file, so do not insert anything,
        # as by definition all records in the nonactive db files are
        # as old or older than records in the keydir/active db file
        :error ->
          nil
      end
    end)
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
