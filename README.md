# Kafka

  Kafka module provides functionality to produce and consume messages from Kafka using library kafka_ex
  using this module, you can put, get messages to a topic.
  you can also create a consumer group process using consume method.

  We found some issues with kafka_ex especially when kafka goes down (for any reaosn), kafka_ex application
  does not restart automatically once kafka is back online.
  
## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `kafka` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:kafka, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/kafka](https://hexdocs.pm/kafka).

