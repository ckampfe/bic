defmodule Bic do
  @moduledoc """
  Documentation for `Bic`, an implementation of [Bitcask](https://riak.com/assets/bitcask-intro.pdf).
  """

  @default_max_size_bytes 2 ** 28

  @doc """
  Create a new database in `db_directory`.
  If data exists in the directory, it will be loaded.
  The database is ready to read and write when this function returns.
  If the size of a database file exceeds `max_file_size_bytes`, a new file is created
  and the old file is no longer written to and considered read-only.
  """
  @spec new(binary()) :: :ignore | {:error, any()} | {:ok, binary()} | {:ok, pid(), any()}
  def new(db_directory, options \\ [max_file_size_bytes: @default_max_size_bytes])
      when is_binary(db_directory) do
    with {:ok, _writer_pid} <- Bic.WriterSupervisor.start_child(db_directory, options) do
      {:ok, db_directory}
    else
      e ->
        e
    end
  end

  @doc """
  Insert a key/value into the database.

  Does not block readers.
  """
  @spec put(binary(), any(), any()) :: :ok
  def put(db_directory, key, value) when is_binary(db_directory) do
    Bic.Writer.write(db_directory, key, value)
  end

  @doc """
  Get a value from the database.

  Import note: this function is not necessarily [serializable](https://en.wikipedia.org/wiki/Read%E2%80%93write_conflict)
  with regard to concurrent writes.

  That is, read and write operations on the same database to the same key
  do not block each other, and as a consequence, if a write occurs in another
  process at exactly the same time, it is possible for this function to return
  a totally valid, consistent, but now stale value with respect to the other write.
  This is an explicit tradeoff of the design of Bitcask and not a bug!

  In normal operation this function will always return either a result or an error,
  but it can return a special error `{:error, :database_is_locked_for_merge}` when the database
  is in its critical section during a `merge` operation.
  This merge error cannot occur unless an active merge is taking place in this database,
  and it can only occur during a small critical section of that merge.
  """
  @spec fetch(binary(), any()) ::
          {:ok, any()} | {:error, atom() | {:database_does_not_exist, binary()}}
  def fetch(db_directory, key) when is_binary(db_directory) do
    case Bic.Reader.fetch(db_directory, key) do
      {:ok, value} ->
        {:ok, value}

      :error ->
        :error

      {:error, _} = e ->
        e
    end
  end

  @doc """
  This function performs, in order:

  1. A `fetch` for a given `key`.
  2. Calls `fun` with the result of `fetch`.
  3. Writes the result of `fun` to disk for `key`.

  This operation is [serializable](https://en.wikipedia.org/wiki/Isolation_(database_systems)).
  That is, it is guaranteed that nothing will mutate the value `key`
  points to between the initial `fetch` and the final `put`.
  This operation takes place "single threaded" with nothing
  else interleaved between the read, the execution of `fun`,
  and the write. This is in contrast to calling `fetch` and then `put` yourself,
  where it is entirely possible that another process can
  call `put` after you have called `fetch`, but *before* you have called `put`.

  As with `put` and `delete`, nothing else can be written
  to the database while this operation is in progress.
  For this reason, be very careful not to run blocking operations in `fun`,
  as they will block any other mutations (writes, deletes, updates) to the database.

  If `key` does not exist, `default` is passed
  to `fun`, which then executes normally.
  ***Note that this behavior is different different than
  how Elixir's builtin `Map.update/4` works.***

  Returns the result of `fun` as `{:ok, new_value}`.
  """
  @spec update(binary(), any(), any(), fun(any())) :: any()
  def update(db_directory, key, default \\ nil, fun)
      when is_binary(db_directory) and
             is_function(fun, 1) do
    Bic.Writer.update(db_directory, key, default, fun)
  end

  @doc """
  Delete a key/value from the database.
  This is accomplished by inserting a tombstone value that
  is removed on the next database merge.

  Does not block readers.
  """
  @spec delete(binary(), any()) :: :ok
  def delete(db_directory, key) when is_binary(db_directory) do
    Bic.Writer.delete_key(db_directory, key)
  end

  @doc """
  Return the list of active keys for the database.
  This only lists live keys and does not include any
  keys that have been deleted.

  Does not block writers.
  """
  @spec keys(binary()) :: list()
  def keys(db_directory) when is_binary(db_directory) do
    Bic.Reader.keys(db_directory)
  end

  @doc """
  Shuts down the database.
  When this operation completes,
  subsequent calls to the functions in this module for the given `db_directory`
  will fail.
  """
  @spec close(binary()) :: :ok
  def close(db_directory) when is_binary(db_directory) do
    Bic.Writer.stop(db_directory)
  end

  @doc """
  Starts a merge asynchronously, does not block the caller.

  A merge is a destructive operation that cleans out old records
  and old database files, producing the set of database files containing only
  relevant records. A record is considered relevant if it is the most recent record for a given key
  and it is live (i.e. not deleted). This means any `delete` operations will be removed from the database files
  permanently.

  Although this function is async and returns immediately,
  note that there is still a small critical section in the database writer process
  in order to delete old database files and rename new ones.
  This critical section will block any other mutations (`put`, `delete`, `update`)
  for its duration.
  """
  @spec merge_async(binary()) :: :ok
  def merge_async(db_directory) when is_binary(db_directory) do
    Bic.Writer.merge_async(db_directory)
  end

  @spec merge_await(binary(), non_neg_integer() | :infinity) :: {:ok, any()} | {:error, any()}
  def merge_await(db_directory, timeout \\ :timer.seconds(5))

  @doc """
  Wait for an async merge to complete and receives its results.
  Blocks the calling process for `timeout` milliseconds,
  and errors if time elapsed exceeds `timeout`.
  """
  def merge_await(db_directory, timeout) when is_binary(db_directory) do
    receive do
      {:ok, value} ->
        {:ok, value}

      {:error, error} ->
        {:error, error}
    after
      timeout ->
        raise "timeout after #{inspect(timeout)}ms waiting for merge for #{db_directory}"
    end
  end

  @doc """
  TODO
  block until all pending writes are on disk...
  add an internal write buffer of configurable size
  """
  def flush() do
    raise "todo"
  end
end
