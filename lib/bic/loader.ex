defmodule Bic.Loader do
  @moduledoc false

  require Logger
  alias Bic.Binary
  # merge process:
  # - get all non-active db files
  # - iterate over those files, building a new keydir,
  # - keeping only the latest nondeleted keyvalues
  # - write new db files
  # - swap in newest keydir, atomically, so that readers will have an atomic
  #   view of live keys
  # - delete old db files
  #
  # - how to name/number new db files?
  #   reusing existing numbers in some way, as it is guaranteed that
  #   the new keyspace/entryspace size
  #   will be <= previous keyspace/entryspace size
  # - how to number entries? keep latest txid, as it is guaranteed to be
  #   less than any txid in the current active file. this also allows
  #   the writer process to continue uninterrupted
  #
  # ***TODO***
  # figure out the differences between loading files and merging files
  # right now `load_data_files` kinda does both.
  # merging files need to rewrite data, so they need the entire entry loaded,
  # vs. loading data, which only needs to create an accurate keydir
  # how to reconcile this? seems like a lot of duplicated logic

  @hash_size Binary.hash_size()
  @tx_id_size Binary.tx_id_size()
  @key_size_size Binary.key_size_size()
  @value_size_size Binary.value_size_size()
  @header_size @hash_size + @tx_id_size + @key_size_size + @value_size_size

  @doc """
  keydir needs,
    from the paper:
      `key -> {file_id, value_size, value_position (offset), timestamp}`

    translated into ets:
      `{key, file_id, entry_size, offset, tx_id}`
  """
  def load(db_directory) do
    {usec, return} =
      :timer.tc(fn ->
        file_ids =
          db_directory
          |> File.ls!()
          |> Stream.map(fn file_name ->
            case Integer.parse(file_name) do
              {i, _} -> {:ok, i}
              _ -> nil
            end
          end)
          |> Stream.filter(fn
            {:ok, _i} -> true
            _ -> false
          end)
          |> Stream.map(fn {:ok, file_id} -> file_id end)
          |> Enum.sort()

        Logger.debug("loading files: #{inspect(file_ids)}")

        file_entries =
          file_ids
          |> Task.async_stream(fn file_id ->
            Logger.debug("loading file: #{file_id}")
            read_entries_from_file(db_directory, file_id)
          end)
          |> Enum.map(fn {:ok, %{entries: entries}} -> entries end)

        merged_entries =
          file_entries
          |> Enum.reduce(%{}, fn file_entries, all_entries ->
            Map.merge(
              all_entries,
              file_entries,
              fn _k,
                 {_liveness1,
                  {
                    _file_id1,
                    _value_size1,
                    _value_position1,
                    tx_id1
                  }} =
                   v1,
                 {_liveness2,
                  {
                    _file_id2,
                    _value_size2,
                    _value_position2,
                    tx_id2
                  }} =
                   v2 ->
                if tx_id1 > tx_id2 do
                  v1
                else
                  v2
                end
              end
            )
          end)
          |> Stream.filter(fn
            {_key, {:live, _entry}} ->
              true

            {_key, {:deleted, _entry}} ->
              false
          end)
          |> Stream.map(fn {key, {_, entry}} ->
            {key, entry}
          end)

        latest_tx_id =
          Enum.reduce(merged_entries, 0, fn {_key, {_, _, _, tx_id}}, acc ->
            if tx_id > acc do
              tx_id
            else
              acc
            end
          end)

        {merged_entries, latest_tx_id}
      end)

    Logger.debug("loaded entries in #{usec / 1000}ms")

    return
  end

  defp read_entries_from_file(db_directory, file_id) do
    path =
      Path.join([db_directory, to_string(file_id)])

    path
    |> File.stream!(4096)
    |> Enum.reduce(
      %{offset: 0, buf: <<>>, entries: %{}},
      fn bytes_chunk, %{offset: offset, buf: buf, entries: entries} = file_acc ->
        buf = buf <> bytes_chunk

        case read_loop(file_id, buf, offset, entries) do
          # if read_loop returns :ok, that means
          # it has read the entire buf to the end,
          # so we set the loop buf to the empty binary (`<<>>`).
          {:ok, bytes_read, entries} ->
            file_acc
            |> Map.put(:buf, <<>>)
            |> Map.put(:entries, entries)
            |> Map.update!(:offset, fn offset -> offset + bytes_read end)

          {:need_more, _n, rest, entries} ->
            file_acc
            |> Map.put(:buf, rest)
            |> Map.put(:entries, entries)

          {:error, :bad_hash,
           %{
             hash_from_disk: hash_from_disk,
             computed_hash: computed_hash
           }} ->
            Logger.warning(
              "hashes did not match. hash from disk: #{hash_from_disk}. computed hash: #{computed_hash}"
            )

            raise "todo"
        end
      end
    )
  end

  defp read_loop(_file_id, buf, offset, entries) when byte_size(buf) == 0 do
    {:ok, offset, entries}
  end

  defp read_loop(file_id, buf, offset, entries) do
    with {:ok,
          %{
            hash: hash,
            encoded_tx_id: encoded_tx_id,
            encoded_key_size: encoded_key_size,
            encoded_value_size: encoded_value_size,
            tx_id: tx_id,
            key_size: key_size,
            value_size: value_size
          }, header_bytes_read, rest1} <- read_header(buf),
         {:ok,
          %{
            encoded_key: encoded_key,
            encoded_value: encoded_value,
            key: key
            # value: _value
          }, payload_bytes_read, rest2} <- read_payload(rest1, key_size, value_size) do
      payload =
        [
          encoded_tx_id,
          encoded_key_size,
          encoded_value_size,
          encoded_key,
          encoded_value
        ]

      computed_hash =
        Binary.hash(payload)

      if computed_hash == hash do
        value_position = offset + header_bytes_read + key_size
        new_offset = offset + header_bytes_read + payload_bytes_read

        Logger.debug(
          "loaded entry: #{inspect({key, file_id, value_size, value_position, tx_id})}"
        )

        entry =
          if encoded_value == Binary.tombstone() do
            {:deleted,
             {
               file_id,
               value_size,
               value_position,
               tx_id
             }}
          else
            {:live,
             {
               file_id,
               value_size,
               value_position,
               tx_id
             }}
          end

        new_entries =
          Map.put(entries, key, entry)

        read_loop(
          file_id,
          rest2,
          new_offset,
          new_entries
        )
      else
        {:error, :bad_hash,
         %{
           hash_from_disk: hash,
           computed_hash: computed_hash
         }}
      end
    else
      {:need_more, n} ->
        {:need_more, n, buf, entries}
    end
  end

  defp read_header(buf) when byte_size(buf) < @header_size do
    {:need_more, @header_size - byte_size(buf)}
  end

  defp read_header(buf) when is_binary(buf) and byte_size(buf) >= @header_size do
    <<
      hash::binary-size(@hash_size),
      encoded_tx_id::binary-size(@tx_id_size),
      encoded_key_size::binary-size(@key_size_size),
      encoded_value_size::binary-size(@value_size_size),
      rest::binary
    >> = buf

    tx_id = Bic.Binary.decode_u128_be(encoded_tx_id)
    key_size = Bic.Binary.decode_u32_be(encoded_key_size)
    value_size = Bic.Binary.decode_u32_be(encoded_value_size)

    {:ok,
     %{
       hash: hash,
       encoded_tx_id: encoded_tx_id,
       encoded_key_size: encoded_key_size,
       encoded_value_size: encoded_value_size,
       tx_id: tx_id,
       key_size: key_size,
       value_size: value_size
     }, @hash_size + @tx_id_size + @key_size_size + @value_size_size, rest}
  end

  defp read_payload(buf, key_size, value_size) when byte_size(buf) < key_size + value_size do
    {:need_more, key_size + value_size - byte_size(buf)}
  end

  defp read_payload(buf, key_size, value_size)
       when byte_size(buf) >= key_size + value_size do
    <<
      encoded_key::binary-size(key_size),
      encoded_value::binary-size(value_size),
      rest::binary
    >> =
      buf

    {:ok,
     %{
       encoded_key: encoded_key,
       encoded_value: encoded_value,
       key: :erlang.binary_to_term(encoded_key)
       #  value: :erlang.binary_to_term(encoded_value)
     }, key_size + value_size, rest}
  end
end
