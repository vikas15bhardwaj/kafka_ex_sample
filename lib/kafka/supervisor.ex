defmodule Kafka.Supervisor do
  use DynamicSupervisor

  def start_link(init_args) do
    DynamicSupervisor.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  def start_child(init_args) do
    spec = {Kafka, init_args}
    DynamicSupervisor.start_child(__MODULE__, spec)
  end

  @impl true
  def init(_init_args) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
