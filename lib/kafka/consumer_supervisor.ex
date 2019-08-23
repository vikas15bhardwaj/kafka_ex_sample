defmodule Kafka.ConsumerSupervisor do
  use DynamicSupervisor
  # import Supervisor.Spec

  def start_link(init_args) do
    Application.ensure_all_started(:kafka_ex, :permanent)
    DynamicSupervisor.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  #
  def start_child(%Kafka{brokers: brokers}, topic, group, _opts \\ []) do
    spec = %{
      id: KafkaEx.ConsumerGroup,
      start:
        {KafkaEx.ConsumerGroup, :start_link,
         [
           Kafka.Consumer,
           group,
           [topic],
           [uris: brokers]
         ]}
    }

    DynamicSupervisor.start_child(__MODULE__, spec)
  end

  @impl true
  def init(_init_args) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
