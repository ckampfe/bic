defmodule Bic.Binary do
  @hash_size 4
  @tx_id_size 16
  @key_size_size 4
  @value_size_size 4
  @header_size @hash_size + @tx_id_size + @key_size_size + @value_size_size

  # TODO make this something smaller/simpler.
  # serializing an atom this big is stupid.
  # every value is serialized with :erlang.term_to_binary,
  # so we could have a special case value that is the null byte
  # or something like that.
  # will have to change the implementation of `write`/`delete` to accomplish this
  # @tombstone :__BIC_INTERNAL_DELETE
  # @tombstone_bytes :erlang.term_to_binary(@tombstone)
  @tombstone <<0>>
  @tombstone_bytes <<0>>

  @spec encode_term(any()) :: binary()
  def encode_term(term) do
    :erlang.term_to_binary(term, [:deterministic])
  end

  @spec decode_binary(binary()) :: any()
  def decode_binary(binary) do
    :erlang.binary_to_term(binary)
  end

  @spec encode_u32_be(integer()) :: <<_::32>>
  def encode_u32_be(i) when is_integer(i) do
    <<i::integer-32-unsigned-big>>
  end

  @spec encode_u128_be(integer()) :: <<_::128>>
  def encode_u128_be(i) when is_integer(i) do
    <<i::integer-128-unsigned-big>>
  end

  @spec decode_u32_be(<<_::32>>) :: non_neg_integer()
  def decode_u32_be(<<i::integer-32-unsigned-big>>) do
    i
  end

  @spec decode_u128_be(<<_::128>>) :: non_neg_integer()
  def decode_u128_be(<<i::integer-128-unsigned-big>>) do
    i
  end

  @spec hash(any()) :: <<_::32>>
  def hash(term) do
    hash = :erlang.phash2(term)
    encode_u32_be(hash)
  end

  def hash_size() do
    @hash_size
  end

  def header_size() do
    @header_size
  end

  def tombstone() do
    @tombstone
  end

  def tombstone_bytes() do
    @tombstone_bytes
  end

  def tx_id_size() do
    @tx_id_size
  end

  def key_size_size() do
    @key_size_size
  end

  def value_size_size() do
    @value_size_size
  end
end
