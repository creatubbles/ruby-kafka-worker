require 'active_support/concern'
require 'kafka'

module KafkaWorker
  class Worker

    def self.handlers
      @handlers ||= []
    end

    def initialize(opts)
      @logger = Logger.new(STDOUT)

      kafka_ips = opts.delete(:kafka_ips)
      client_id = opts.delete(:client_id)
      group_id  = opts.delete(:group_id)
      @kafka_consumer = init_kafka_consumer(kafka_ips, client_id, group_id)
    end

    def run
      handlers = self.class.handlers.dup

      handlers.each do |handler|
        @kafka_consumer.subscribe(handler.topic, start_from_beginning: false)
      end

      @kafka_consumer.each_message do |message|
        handlers.each do |handler|
          if message.topic == handler.topic
            handler_obj = handler.new
            handler_obj.logger = @logger
            begin
              handler_obj.handle(message)
            rescue Exception => err
              handler_obj.on_error(message, err)
            end
          end
        end
      end
    end

    def stop_consumer
      @logger.info("Stopping KafkaWorker::Worker @kafka_consumer")
      @kafka_consumer.stop
    rescue Exception => err
      @logger.debug("Could not KafkaWorker::Worker @kafka_consumer: #{err}")
    end

    private
    def init_kafka_consumer(kafka_ips, client_id, group_id)
      opts = {
        seed_brokers: kafka_ips,
        client_id:    client_id,
        logger:       @logger,
      }
      kafka = Kafka.new(opts)
      kafka.consumer(
        group_id: group_id,
        # Increase offset commit frequency to once every 5 seconds.
        offset_commit_interval:  5,
        # Commit offsets when 1 messages have been processed. Prevent duplication.
        offset_commit_threshold: 1)
    end
  end


  module Handler
    extend ActiveSupport::Concern

    included do
      Worker.handlers << self
      attr_accessor :logger
    end
    
    class_methods do
      def consumes(topic)
        @topic = topic
      end

      def topic
        @topic
      end
    end

    def handle(message)
      # override me
    end

    def on_error(message, err)
      logger.error("#{self.class.name} failed on message: #{message.inspect} with error: #{err}")
    end
  end
end


=begin
# created only once
class ClassAbstractCtbHandler
  cattr_accessor :ctb_client
end

# created each time useful if you need to mutate
class AbstractCtbHandler  
  attr_accessor :ctb_client
  def initialize
    @ctb_client = Ctb::Creatubbles.new(
      client_id: ENV['CREATUBBLES_CLIENT_ID'],
      client_secret: ENV['CREATUBBLES_CLIENT_SECRET'],
      api_url: ENV['CREATUBBLES_API_URL'] || Creatubbles::DEFAULT_API_URL 
    )
  end
end

class Handler < AbstractCtbHandler
  include KafkaWorker::Handler
  consumes "hello"

  def handle(message)
    puts 'handle hello'
    puts message.inspect
    ctb_client
  end
end

class SecondHandler < ClassAbstractCtbHandler
  include KafkaWorker::Handler
  consumes "goodbye"

  def handle(message)
    puts 'handle goodbye'
    puts message.inspect
    ctb_client
  end
end

ClassAbstractCtbHandler.ctb_client = Ctb::Creatubbles.new(
  client_id: ENV['CREATUBBLES_CLIENT_ID'],
  client_secret: ENV['CREATUBBLES_CLIENT_SECRET'],
  api_url: ENV['CREATUBBLES_API_URL'] || Creatubbles::DEFAULT_API_URL 
)

opts = {
#   client_id: ENV['CREATUBBLES_CLIENT_ID'],
#   client_secret: ENV['CREATUBBLES_CLIENT_SECRET'],
#   api_url: ENV['CREATUBBLES_API_URL'] || Creatubbles::DEFAULT_API_URL,
   kafka_ips: "127.0.0.1:9092",
   client_id: 'test',
   group_id: 'test'
}
kw = KafkaWorker::Worker.new(opts)
kw.run
trap("QUIT") { kw.stop_consumer }
=end


=begin
opts = {
   client_id: ENV['CREATUBBLES_CLIENT_ID'],
   client_secret: ENV['CREATUBBLES_CLIENT_SECRET'],
   api_url: ENV['CREATUBBLES_API_URL'] || Creatubbles::DEFAULT_API_URL,
   kafka_ips: (ENV['KAFKA_ENDPOINTS'] || "localhost").split(",")
}
=end

=begin
opts = {
   kafka_ips: (ENV['KAFKA_ENDPOINTS'] || "localhost").split(",")
}
k = KafkaWorker::Worker.new(opts)
k.run
=end
