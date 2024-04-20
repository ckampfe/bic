defmodule Bic.Merger do
  @moduledoc false

  require Logger
  alias Bic.Binary

  @hash_size Binary.hash_size()
  @tx_id_size Binary.tx_id_size()
  @key_size_size Binary.key_size_size()
  @value_size_size Binary.value_size_size()
  @header_size @hash_size + @tx_id_size + @key_size_size + @value_size_size

  def load(db_directory, file_ids) do
    {usec, return} =
      :timer.tc(fn ->
        file_ids = Enum.sort(file_ids)

        Logger.debug("loading files: #{inspect(file_ids)}")

        file_records =
          file_ids
          |> Task.async_stream(fn file_id ->
            Logger.debug("loading file: #{file_id}")
            # {key, tx_id, binary_of_record_on_disk}
            read_entries_from_file(db_directory, file_id)
          end)
          |> Enum.map(fn {:ok, %{entries: entries}} -> entries end)

        merged_records =
          file_records
          |> Enum.reduce(%{}, fn file_records, all_entries ->
            Map.merge(
              all_entries,
              file_records,
              fn _k,
                 {_liveness1, tx_id1, _record1, _, _, _} =
                   v1,
                 {_liveness2, tx_id2, _record2, _, _, _} =
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
            {_key, {:live, _tx_id1, _record, _header_bytes_read, _key_size, _value_size}} ->
              true

            {_key, {:deleted, _tx_id2, _record, _header_bytes_read, _key_size, _value_size}} ->
              false
          end)
          |> Stream.map(fn {key,
                            {_liveness, tx_id, record, header_bytes_read, key_size, value_size}} ->
            {key, tx_id, record, header_bytes_read, key_size, value_size}
          end)

        merged_records
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
            value_size: value_size,
            raw_header: raw_header
          }, header_bytes_read, rest1} <- read_header(buf),
         {:ok,
          %{
            encoded_key: encoded_key,
            encoded_value: encoded_value,
            key: key,
            # value: _value
            raw_payload: raw_payload
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
            {
              :deleted,
              tx_id,
              [raw_header, raw_payload],
              header_bytes_read,
              key_size,
              value_size
            }
          else
            {
              :live,
              tx_id,
              [raw_header, raw_payload],
              header_bytes_read,
              key_size,
              value_size
            }
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
       value_size: value_size,
       raw_header: [hash, encoded_tx_id, encoded_key_size, encoded_value_size]
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
       key: :erlang.binary_to_term(encoded_key),
       raw_payload: [encoded_key, encoded_value]
     }, key_size + value_size, rest}
  end

  # TODO all of this is really bad
  def write_records_for_merge(_, [], _, _) do
    []
  end

  def write_records_for_merge(
        db_directory,
        records,
        max_file_size_bytes,
        [file_id | _remaining_file_ids] = file_ids
      ) do
    path =
      Path.join([db_directory, to_string(file_id) <> ".merge"])

    file = File.open!(path, [:append, :raw])

    offset = 0

    keydir_entries = []

    args = %{
      db_directory: db_directory,
      records: records,
      max_file_size_bytes: max_file_size_bytes,
      offset: offset,
      file_ids: file_ids,
      current_file: file,
      current_file_id: file_id,
      keydir_entries: keydir_entries
    }

    write_records_for_merge(args)
  end

  defp write_records_for_merge(%{records: [], keydir_entries: keydir_entries}) do
    keydir_entries
  end

  defp write_records_for_merge(%{
         db_directory: db_directory,
         records: [
           {key, tx_id, record, header_bytes_read, key_size, value_size}
           | records
         ],
         max_file_size_bytes: max_file_size_bytes,
         offset: offset,
         file_ids: [next_file_id | remaining_file_ids] = all_file_ids,
         current_file: current_file,
         current_file_id: current_file_id,
         keydir_entries: keydir_entries
       }) do
    %{file: file, file_id: file_id, file_ids: file_ids} =
      if offset >= max_file_size_bytes do
        path =
          Path.join([db_directory, to_string(next_file_id) <> ".merge"])

        new_file = File.open!(path, [:append, :raw])

        %{file: new_file, file_id: next_file_id, file_ids: remaining_file_ids}
      else
        %{file: current_file, file_id: current_file_id, file_ids: all_file_ids}
      end

    IO.binwrite(file, record)

    value_offset = offset + header_bytes_read + key_size

    keydir_entry = {
      key,
      current_file_id,
      value_size,
      value_offset,
      tx_id
    }

    write_records_for_merge(%{
      db_directory: db_directory,
      records: records,
      max_file_size_bytes: max_file_size_bytes,
      offset: offset + :erlang.iolist_size(record),
      file_ids: file_ids,
      current_file: file,
      current_file_id: file_id,
      keydir_entries: [keydir_entry | keydir_entries]
    })
  end
end
