# Bic

An implementation of [Bitcask](https://en.wikipedia.org/wiki/Bitcask).

See [the paper](https://riak.com/assets/bitcask-intro.pdf).

## Todo

- [x] create databases
- [x] open existing databases
- [x] merge database files
- [x] put keys
- [x] fetch keys
- [x] delete keys
- [x] migrate to new file when max file size reached
- [ ] error recovery when hashes do not match
- [ ] todo examples on public fns
- [x] some kind of lock to prevent stale reads during merge
- [ ] investigate mechanisms for what to do if keydir is locked during merge
- [ ] the existing "lock" is only valid at the moment it is returned.
      it has no lexical scope or anything like that,
      meaning that any of the subsequent file operations
      could be invalid and unsafe. need to figure out some way to manage this.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `bic` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:bic, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/bic>.
