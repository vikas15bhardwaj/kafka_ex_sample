defmodule KafkaEx.Protocol.Produce do
  alias KafkaEx.Protocol
  alias KafkaEx.Compression
  import KafkaEx.Protocol.Common

  @moduledoc """
  Implementation of the Kafka Produce request and response APIs
  """

  defmodule Request do
    @moduledoc """
    - required_acks: indicates how many acknowledgements the servers should
    receive before responding to the request. If it is 0 the server will not
    send any response (this is the only case where the server will not reply
    to a request). If it is 1, the server will wait the data is written to the
    local log before sending a response. If it is -1 the server will block until
    the message is committed by all in sync replicas before sending a response.
    For any number > 1 the server will block waiting for this number of
    acknowledgements to occur (but the server will never wait for more
    acknowledgements than there are in-sync replicas), default is 0
    - timeout: provides a maximum time in milliseconds the server can await the
    receipt of the number of acknowledgements in RequiredAcks, default is 100
    milliseconds
    """
    defstruct topic: nil,
              partition: nil,
              required_acks: 0,
              timeout: 0,
              compression: :none,
              messages: []

    @type t :: %Request{
            topic: binary,
            partition: integer,
            required_acks: integer,
            timeout: integer,
            compression: atom,
            messages: list
          }
  end

  defmodule Message do
    @moduledoc """
    - key: is used for partition assignment, can be nil, when none is provided
    it is defaulted to nil
    - value: is the message to be written to kafka logs.
    """
    defstruct key: nil, value: nil
    @type t :: %Message{key: binary, value: binary}
  end

  defmodule Response do
    @moduledoc false
    defstruct topic: nil, partitions: []
    @type t :: %Response{topic: binary, partitions: list}
  end

  def create_request(correlation_id, client_id, %Request{
        topic: topic,
        partition: partition,
        required_acks: required_acks,
        timeout: timeout,
        compression: compression,
        messages: messages
      }) do
    {message_set, mssize} = create_message_set(messages, compression)

    [
      KafkaEx.Protocol.create_request(:produce, correlation_id, client_id),
      <<required_acks::16-signed, timeout::32-signed, 1::32-signed>>,
      <<byte_size(topic)::16-signed, topic::binary, 1::32-signed,
        partition::32-signed, mssize::32-signed>>,
      message_set
    ]
  end

  def parse_response(
        <<_correlation_id::32-signed, num_topics::32-signed, rest::binary>>
      ),
      do: parse_topics(num_topics, rest, __MODULE__)

  def parse_response(unknown), do: unknown

  defp create_message_set([], _compression_type), do: {"", 0}

  defp create_message_set(messages, :none) do
    create_message_set_uncompressed(messages)
  end

  defp create_message_set(messages, compression_type) do
    {message_set, _} = create_message_set(messages, :none)

    {compressed_message_set, attribute} =
      Compression.compress(compression_type, message_set)

    {message, msize} = create_message(compressed_message_set, nil, attribute)

    {[<<0::64-signed>>, <<msize::32-signed>>, message], 8 + 4 + msize}
  end

  defp create_message_set_uncompressed([
         %Message{key: key, value: value} | messages
       ]) do
    {message, msize} = create_message(value, key)
    message_set = [<<0::64-signed>>, <<msize::32-signed>>, message]
    {message_set2, ms2size} = create_message_set(messages, :none)
    {[message_set, message_set2], 8 + 4 + msize + ms2size}
  end

  defp create_message(value, key, attributes \\ 0) do
    {bkey, skey} = bytes(key)
    {bvalue, svalue} = bytes(value)
    sub = [<<0::8, attributes::8-signed>>, bkey, bvalue]
    crc = :erlang.crc32(sub)
    {[<<crc::32>>, sub], 4 + 2 + skey + svalue}
  end

  defp bytes(nil), do: {<<-1::32-signed>>, 4}

  defp bytes(data) do
    case :erlang.iolist_size(data) do
      0 -> {<<0::32>>, 4}
      size -> {[<<size::32>>, data], 4 + size}
    end
  end

  def parse_partitions(0, rest, partitions), do: {partitions, rest}

  def parse_partitions(
        partitions_size,
        <<partition::32-signed, error_code::16-signed, offset::64,
          rest::binary>>,
        partitions
      ) do
    parse_partitions(partitions_size - 1, rest, [
      %{
        partition: partition,
        error_code: Protocol.error(error_code),
        offset: offset
      }
      | partitions
    ])
  end
end
