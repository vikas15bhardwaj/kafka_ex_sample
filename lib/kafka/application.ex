defmodule Kafka.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    # import Supervisor.Spec

    # consumer_group_opts = [
    #   # setting for the ConsumerGroup
    #   heartbeat_interval: 1_000,
    #   # this setting will be forwarded to the GenConsumer
    #   commit_interval: 1_000
    # ]

    children = [
      # ... other children
      # supervisor(KafkaEx.ConsumerGroup, [
      #   Kafkafun.Reader,
      #   "waterpark_group",
      #   ["bzt_test"],
      #   consumer_group_opts
      # ])
      {Registry, keys: :unique, name: Registry.KafkaWorkerProcess},
      Kafka.Supervisor,
      Kafka.ConsumerSupervisor
      # supervisor(KafkaEx.Supervisor, server: nil, max_restarts: 100, max_seconds: 10)
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one]
    Supervisor.start_link(children, opts)
  end
end
