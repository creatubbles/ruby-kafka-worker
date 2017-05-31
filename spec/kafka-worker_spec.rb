require 'kafka'
require 'kafka-worker'
require 'rspec'
require 'json'

class HelloHandler
  include KafkaWorker::Handler
  consumes "hello"
  retry_interval 0
  @@message_value = nil
  def handle(message)
    puts "HelloHandler received the following message: #{message.value}"
    @@message_value = message.value
  end

  def self.message_value
    @@message_value
  end
end


class CustomError < StandardError
end

class ForceErrorHandler
  include KafkaWorker::Handler
  consumes "error"
  retry_interval 0

  @@error_value = nil
  def handle(message)
    puts "ForceErrorHandler is going to raise CustomError"
    raise CustomError, 'default error message'
  end

  def on_error(message, err)
    puts err
    @@error_value = err
  end

  def self.error_value
    @@error_value
  end
end

class ForceErrorAndRetryHandler
  include KafkaWorker::Handler
  consumes "retry"
  retry_interval 0

  @@error_value = 0
  def handle(message)
    puts "ForceErrorAndRetryHandler is going to raise CustomError"
    raise CustomError, 'default error message'
  end

  def on_error(message, err)
    @@error_value += 1
    puts 'updated error_value'
    puts @@error_value
  end

  def self.error_value
    @@error_value
  end
end

class PerformOverridingHandler
  include KafkaWorker::Handler
  consumes "perform-hello"
  retry_interval 0
  @@hash_value = nil
  def perform(hash)
    puts "PerformOverridingHandler received the following hash: #{hash}"
    @@hash_value = hash
  end

  def self.hash_value
    @@hash_value
  end
end


describe KafkaWorker do
  before (:all) do
    # make a producer to push events to the KafkaWorker
    @kafka = Kafka.new(
      seed_brokers: '127.0.0.1:9092',
      client_id: 'kafka-worker-spec',
      logger: Logger.new(STDOUT)
    )

    @kafka_producer = @kafka.async_producer(
      delivery_interval: 1,
    )

    opts = {
      kafka_ips: ['127.0.0.1:9092'],
      client_id: 'test',
      group_id:  'test',
      offset_commit_interval: 1
    }

    @kw = KafkaWorker::Worker.new(opts)

    Thread.new do
      @kw.run
    end
    # circle ci is slow, need to wait for connection before starting
    sleep 10
  end

  after(:all) do
    @kw.stop_consumer
    @kafka_producer.shutdown
  end

  it "consume events and pushes a value into a class object" do
    message_value = 'Hello World!'
    @kafka_producer.produce(message_value, topic: 'hello')
    sleep 10
    expect(HelloHandler.message_value).to eq message_value
  end

  it "catch an exception if there is in the handler code" do
    message_value = 'An error is coming.'
    @kafka_producer.produce(message_value, topic: 'error')
    sleep 10
    expect(ForceErrorHandler.error_value.to_s).to eq 'default error message'
  end

  it "retry if there is an exception" do
    message_value = 'An error is coming.'
    @kafka_producer.produce(message_value, topic: 'retry')
    sleep 10
    expect(Integer(ForceErrorAndRetryHandler.error_value.to_s)).to be > 1
  end

  it "consume events and pushes a value into a class object" do
    message_value = { say: 'Hello World!' }
    @kafka_producer.produce(message_value.to_json, topic: 'perform-hello')
    sleep 10
    expect(PerformOverridingHandler.hash_value).to eq message_value.with_indifferent_access
  end
end
