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
