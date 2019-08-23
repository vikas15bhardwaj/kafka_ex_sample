defmodule Kafka do
  use GenServer
  require Logger

  defstruct brokers: [{}], consumer_group: nil

  @type t() :: %{
          brokers: [%{}],
          consumer_group: String.t()
        }
  @moduledoc """
  Kafka module provides functionality to produce and consume messages from Kafka using library kafka_ex
  using this module, you can put, get messages to a topic.
  you can also create a consumer group process using consume method.

  We found some issues with kafka_ex especially when kafka goes down (for any reaosn), kafka_ex application
  does not restart automatically once kafka is back online.

  """
  # API client

  @doc """
  start a new broker connection
  iex>broker = %Kafka{brokers: [{"localhost", 9092}]}
  iex>{:ok, _pid} = Kafka.new(broker)
  iex>{:ok, offset} = Kafka.put(broker, %{topic: "bzt_test", content: "doc-test1", partition_key: nil, required_acks: 1})
  iex>Kafka.get(broker, "bzt_test", 0, offset)
  :ok
  """
  @spec new(broker: Kafka.t()) :: {:ok, pid()}
  def new(broker) do
    Kafka.Supervisor.start_child(broker)
  end

  @doc """
  put message to a kafka topic for a given key
  """
  # @impl true
  def put(
        broker,
        message
      ) do
    GenServer.call(
      via_tuple(get_key(broker)),
      {:put, message}
    )
  end

  @doc """
  get message from kafka topic for a consumer group
  """
  # @impl true
  def get(broker, topic, partition, offset \\ nil) do
    case KafkaEx.fetch(topic, partition,
           worker_name: get_key(broker),
           wait_time: 100,
           offset: offset
         ) do
      :topic_not_found ->
        {:error, :topic_not_found}

      response ->
        Enum.each(response, fn response ->
          Enum.each(response.partitions, fn x ->
            Enum.each(x.message_set, fn m -> IO.inspect(m.value) end)
          end)
        end)
    end
  end

  @doc """
  consume messages from kafka topic in a stream manner. multiple consumers can be added to distribution
  load.
  iex>broker = %Kafka{brokers: [{"localhost", 9092}], consumer_group: "xyz"}
  iex>{:ok, _pid} = Kafka.consume(broker, "bzt_test")
  iex>:ok
  :ok
  """
  def consume(broker, topic, opts \\ [])

  def consume(%Kafka{consumer_group: consumer_group} = broker, topic, opts)
      when consumer_group != nil and topic != nil do
    Kafka.ConsumerSupervisor.start_child(broker, topic, consumer_group, opts)
  end

  def consume(_broker, _topic, _opts) do
    {:error, :no_consumer_group}
  end

  def lookup_reg(broker) do
    Registry.lookup(Registry.KafkaWorkerProcess, get_key(broker))
  end

  # gen server....
  def start_link(init_arg) do
    case GenServer.start_link(__MODULE__, init_arg, name: via_tuple(get_key(init_arg))) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
    end
  end

  @impl true
  def init(%Kafka{consumer_group: consumer_group} = init_arg)
      when consumer_group == nil do
    worker_init = [
      uris: init_arg.brokers
    ]

    initialize_kafkaex_worker(init_arg, worker_init)
  end

  @impl true
  def init(init_arg) do
    IO.puts("kafka init2")

    worker_init = [
      uris: init_arg.brokers,
      consumer_group: init_arg.consumer_group
    ]

    initialize_kafkaex_worker(init_arg, worker_init)
  end

  @impl true
  def handle_call({:put, message}, _from, state) do
    name = get_key(state)

    {:ok, offset} =
      KafkaEx.produce(message.topic, 0, message.content,
        worker_name: name,
        required_acks: message.required_acks,
        key: message.partition_key
      )

    {:reply, {:ok, offset}, state}
  end

  # private functions....
  defp via_tuple(key) do
    {:via, Registry, {Registry.KafkaWorkerProcess, key}}
  end

  defp get_key(%Kafka{} = init_arg) do
    :"#{inspect(init_arg.brokers)}_#{init_arg.consumer_group}"
  end

  defp initialize_kafkaex_worker(init_arg, worker_init) do
    name = get_key(init_arg)

    case KafkaEx.create_worker(name, worker_init) do
      {:ok, _pid} ->
        {:ok, init_arg}

      {:error, {:already_started, _pid}} ->
        {:ok, init_arg}
    end
  end
end
