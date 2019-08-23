defmodule KafkaTest do
  use ExUnit.Case
  doctest Kafka

  test "put a message to kafka topic" do
    broker = %Kafka{brokers: [{"localhost", 9092}]}
    {:ok, _} = Kafka.new(broker)

    message = %{topic: "bzt_test", content: "hi-test1", partition_key: nil, required_acks: 1}

    assert {:ok, _} = Kafka.put(broker, message)
  end

  test "put multiple single messages to kafka topic" do
    broker = %Kafka{brokers: [{"localhost", 9092}]}
    {:ok, _} = Kafka.new(broker)
    message = %{topic: "bzt_test", content: "hi2-test2", partition_key: nil, required_acks: 1}
    assert {:ok, _} = Kafka.put(broker, message)

    message = %{topic: "bzt_test", content: "hi3-test2", partition_key: nil, required_acks: 1}
    assert {:ok, _} = Kafka.put(broker, message)
  end

  # test "get message from kafka topic" do
  #   assert {:ok, "M"} == Kafka.get("waterpark.kafka.test.topic", "waterpark.kafka.test.group")
  # end
end
