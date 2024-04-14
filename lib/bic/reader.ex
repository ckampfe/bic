defmodule Bic.Reader do
  @moduledoc """
  Note: this is not a process, it's just a module!
  If you want more read parallelism, spawn more processes to call these functions.
  """

  alias Bic.Binary

  # @hash_size Binary.hash_size()
  # @tx_id_size Binary.tx_id_size()
  # @key_size_size Binary.key_size_size()
  # @value_size_size Binary.value_size_size()
  # @header_size Binary.header_size()

  def read_value(db_directory, key) when is_binary(db_directory) do
    with {:database_tid_lookup, [{^db_directory, keydir_tid}]} <-
           {:database_tid_lookup, :ets.lookup(:bic_databases, db_directory)},
         {:key_lookup, [{_key, active_file_id, value_size, value_offset, _timestamp}]} <-
           {:key_lookup, :ets.lookup(keydir_tid, key)},
         db_file = Path.join([db_directory, to_string(active_file_id)]),
         {:file_open, {:ok, file}} <- {:file_open, File.open(db_file, [:read, :raw])},
         {:file_seek, {:ok, _}} <- {:file_seek, :file.position(file, value_offset)},
         {:file_read, value_bytes} <- {:file_read, IO.binread(file, value_size)} do
      {:ok, Binary.decode_binary(value_bytes)}
    else
      {:database_tid_lookup, []} ->
        {:error, {:database_does_not_exist, db_directory}}

      {:key_lookup, []} ->
        {:ok, nil}

      {:file_open, e} ->
        e

      {:file_seek, e} ->
        e

      {:file_read, e} ->
        e
    end
  end

  # @doc """
  # Read the entire on-disk binary and deserialize it.
  # The on-disk schema is:
  # <<
  #   hash of serialized payload (binary, 64 bytes long),
  #   :erlang.term_to_binary(
  #     [
  #       timestamp (integer),
  #       key_size (integer),
  #       value_size (integer),
  #       key (binary),
  #       value (term)
  #     ]
  #   )
  # >>
  # """
  # def read_all(db_directory, key) do
  #   with {:database_tid_lookup, [{^db_directory, keydir_tid}]} <-
  #          {:database_tid_lookup, :ets.lookup(:bic_databases, db_directory)},
  #        {:key_lookup, [{_key, active_file_id, entry_length, offset, _timestamp}]} <-
  #          {:key_lookup, :ets.lookup(keydir_tid, key)},
  #        db_file = Path.join([db_directory, to_string(active_file_id)]),
  #        {:file_open, {:ok, file}} <- {:file_open, File.open(db_file, [:read, :raw])},
  #        {:file_seek, {:ok, _}} <- {:file_seek, :file.position(file, offset)},
  #        {:file_read, bin} <- {:file_read, IO.binread(file, entry_length)} do
  #     key_value_size = entry_length - @header_size

  #     <<hash::binary-size(@hash_size), encoded_tx_id::binary-size(@tx_id_size),
  #       encoded_key_size::binary-size(@key_size_size),
  #       encoded_value_size::binary-size(@value_size_size),
  #       encoded_key_value::binary-size(key_value_size)>> = bin

  #     case Binary.hash([
  #            encoded_tx_id,
  #            encoded_key_size,
  #            encoded_value_size,
  #            encoded_key_value
  #          ]) do
  #       # if the hashes match
  #       ^hash ->
  #         tx_id = Binary.decode_u128_be(encoded_tx_id)
  #         key_size = Binary.decode_u32_be(encoded_key_size)
  #         value_size = Binary.decode_u32_be(encoded_value_size)
  #         <<key::binary-size(key_size), value::binary-size(value_size)>> = encoded_key_value

  #         # deserialized payload is always a list
  #         out = [
  #           hash,
  #           tx_id,
  #           key_size,
  #           value_size,
  #           Binary.decode_binary(key),
  #           if value_size > 1 do
  #             Binary.decode_binary(value)
  #           else
  #             value
  #           end
  #         ]

  #         {:ok, out}

  #       bad_hash ->
  #         {:error, :hashes_do_not_match,
  #          %{
  #            bad_hash: bad_hash,
  #            bad_payload: [
  #              encoded_tx_id,
  #              encoded_key_size,
  #              encoded_value_size,
  #              encoded_key_value
  #            ]
  #          }}
  #     end
  #   else
  #     {:database_tid_lookup, []} ->
  #       {:error, {:database_does_not_exist, db_directory}}

  #     {:key_lookup, []} ->
  #       {:ok, nil}

  #     {:file_open, e} ->
  #       e

  #     {:file_seek, e} ->
  #       e

  #     {:file_read, e} ->
  #       e
  #   end
  # end

  @doc """
  Get all of the in-memory keys for this database
  """
  def keys(db_directory) do
    [{^db_directory, keydir_tid}] = :ets.lookup(:bic_databases, db_directory)
    :ets.select(keydir_tid, [{{:"$1", :_, :_, :_, :_}, [], [:"$1"]}])
  end
end
