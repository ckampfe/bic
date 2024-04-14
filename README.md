# Bic

An implementation of [Bitcask](https://en.wikipedia.org/wiki/Bitcask).

See [the paper](https://riak.com/assets/bitcask-intro.pdf).

## Todo

- [x] create databases
- [x] open existing databases
- [ ] merge database files
- [x] put keys
- [x] fetch keys
- [x] delete keys

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

