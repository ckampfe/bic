defmodule Bic.Lock do
  @moduledoc false

  @compile {:inline, [get_handle: 1, status: 1, lock: 1, unlock: 1]}

  @doc """
  Create a lock, globally accessible with the given key.
  """
  @spec new(any()) :: :ok
  def new(key) do
    :persistent_term.put(key, :atomics.new(1, []))
  end

  @doc """
  Gets a handle to the lock.
  Does not actually lock the lock.
  """
  @spec get_handle(any()) :: :atomics.atomics_ref()
  def get_handle(key) do
    :persistent_term.get(key)
  end

  @doc """
  Locks a lock. Raises if the lock is already locked.
  """
  @spec lock(:atomics.atomics_ref()) :: :ok
  def lock(lock) do
    :ok = :atomics.compare_exchange(lock, 1, 0, 1)
  end

  @doc """
  Unlocks a lock. Raises if the lock is already unlocked.
  """
  @spec unlock(:atomics.atomics_ref()) :: :ok
  def unlock(lock) do
    :ok = :atomics.compare_exchange(lock, 1, 1, 0)
  end

  @doc """
  Gets the status of a lock.
  """
  @spec status(:atomics.atomics_ref()) :: :locked | :unlocked
  def status(lock) do
    case :atomics.get(lock, 1) do
      0 -> :unlocked
      1 -> :locked
    end
  end

  @doc """
  Locks the given `lock`, runs the given `block`, and
  then unlocks the `lock`.
  """
  defmacro with_lock(lock, do: block) do
    quote do
      Bic.Lock.lock(unquote(lock))

      try do
        unquote(block)
      rescue
        e ->
          reraise(e, __STACKTRACE__)
      after
        Bic.Lock.unlock(unquote(lock))
      end
    end
  end
end
