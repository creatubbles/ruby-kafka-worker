require 'kafka'
require 'kafka-worker'
require 'rspec'

class HelloHandler
  include KafkaWorker::Handler
  consumes "hello"
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
    sleep 30
  end
  
  after(:all) do
    @kw.stop_consumer
    @kafka_producer.shutdown
  end
  
  it "consume events and pushes a value into a class object" do
    message_value = 'Hello World!'
    @kafka_producer.produce(message_value, topic:'hello')
    sleep 30
    expect(HelloHandler.message_value).to eq message_value
  end

  it "catch an exception if there is in the handler code" do
    message_value = 'An error is coming.'
    @kafka_producer.produce(message_value, topic:'error')
    sleep 30
    expect(ForceErrorHandler.error_value.to_s).to eq 'default error message'
  end  
end
