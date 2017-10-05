# frozen_string_literal: true

require 'kafka'
require 'kafka_worker'
require 'rspec'
require 'json'

Dir[File.join(File.dirname(__FILE__), 'handlers/*.rb')].each { |f| require f }

describe KafkaWorker do
  before(:all) do
    # setup kafka producer
    kafka = Kafka.new(
      seed_brokers: '127.0.0.1:9092', client_id: 'kafka-worker-spec', logger: KafkaWorker.logger
    )
    @kafka_producer = kafka.async_producer(delivery_interval: 1)

    # setup kafka worker
    @kafka_worker = KafkaWorker::Worker.new(
      kafka_ips: ['127.0.0.1:9092'], client_id: 'test', group_id:  'test', offset_commit_interval: 1
    )
    expect(::KafkaWorker.handlers.count).to eq(3)
    Thread.new { @kafka_worker.run }

    sleep 10
  end

  after(:all) do
    @kafka_worker.stop_consumer
    @kafka_producer.shutdown
  end

  before(:each) do
    KafkaWorker.handlers.each { |h| h.processed_messages_count = 0 }
  end

  it "consume events and pushes a value into a class object" do
    message_value = 'Hello World!'
    @kafka_producer.produce(message_value, topic: 'hello')
    sleep 10
    expect(HelloHandler.processed_messages_count).to eq(1)
    expect(HelloHandler.message_value).to eq message_value
  end

  it "catch an exception if there is in the handler code" do
    message_value = 'An error is coming.'
    @kafka_producer.produce(message_value, topic: 'error')
    sleep 10
    expect(ForceErrorHandler.error_value.to_s).to eq 'default error message'
    expect(ForceErrorHandler.processed_messages_count).to eq(5)
    sleep 10
    client = Kafka.new(seed_brokers: '127.0.0.1:9092', client_id: 'test-error-checker')
    messages = client.fetch_messages(topic: 'error-failed', partition: 0, offset: :earliest)
    expect(messages).not_to be_empty
    latest_msg = JSON.parse(messages.last.value)
    expect(latest_msg).to include("failed_at",
      "error" => "default error message",
      "handler" => "ForceErrorHandler",
      "message" => include("topic" => "error", "value" => "An error is coming."))
  end

  it "consume events and pushes a value into a class object" do
    message_value = { say: 'Hello World!' }
    @kafka_producer.produce(message_value.to_json, topic: 'perform-hello')
    sleep 10
    expect(PerformOverridingHandler.processed_messages_count).to eq(1)
    expect(PerformOverridingHandler.hash_value).to eq message_value.with_indifferent_access
  end
end
