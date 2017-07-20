require 'active_support/concern'
require 'active_support/notifications'
require 'active_support/core_ext/hash'
require 'kafka'

module KafkaWorker
  class Worker

    def self.handlers
      @handlers ||= []
    end

    def initialize(opts)
      @logger = opts[:logger] || Logger.new(STDOUT).tap{|l| l.level = Logger::INFO }

      ActiveSupport::Notifications.subscribe('request.connection.kafka') do |*args|
        event = ActiveSupport::Notifications::Event.new(*args)
        @logger.debug("Received notification `#{event.name}` with payload: #{event.payload.inspect}")
      end

      @kafka_consumer = init_kafka_consumer(opts)
    end

    def run
      handlers = self.class.handlers.dup

      handlers.each do |handler|
        @kafka_consumer.subscribe(handler.topic, start_from_beginning: handler.start_from_beginning)
      end

      @kafka_consumer.each_message do |message|
        @logger.info("kafka_consumer.received message #{message.topic}, value #{message.value}")
        handlers.each do |handler|
          if message.topic == handler.topic
            handler_obj = handler.new
            handler_obj.logger = @logger

            tries = 5

            begin
              handler_obj.handle(message)

            rescue Exception => err
              error_message = "#{self.class.name} failed on message: #{message.inspect} with error: #{err}"

              @logger.error(error_message)
              capture_exception(err, error_message)

              handler_obj.on_error(message, err)

              if (tries -= 1) > 0
                sleep(handler.retry_interval)
                retry
              end
            end
          end
        end
      end
    end

    def stop_consumer
      @logger.info("Stopping KafkaWorker::Worker @kafka_consumer")
      @kafka_consumer.stop
    rescue Exception => err
      @logger.warn("Could not stop KafkaWorker::Worker @kafka_consumer: #{err}")
    end

    private

    def init_kafka_consumer(opts)
      kafka = Kafka.new(seed_brokers: opts[:kafka_ips], client_id: opts[:client_id], logger: @logger)
      kafka.consumer(
        group_id: opts[:group_id],
        # Increase offset commit frequency to once every 5 seconds.
        offset_commit_interval: opts[:offset_commit_interval] || 5,
        # Commit offsets when 1 messages have been processed. Prevent duplication.
        offset_commit_threshold: opts[:offset_commit_threshold] || 1)
    end

    def capture_exception(err, error_message)
    end
  end


  module Handler
    extend ActiveSupport::Concern

    included do
      Worker.handlers << self
      attr_accessor :logger
    end

    class_methods do
      @retry_interval = 60
      @start_from_beginning = false

      def consumes(topic)
        @topic = topic
      end

      def topic
        @topic
      end

      def retry_interval(val=nil)
        @retry_interval = val if val
        @retry_interval
      end

      def start_from_beginning(val=nil)
        @start_from_beginning = val if val
        @start_from_beginning
      end
    end

    def handle(message)
      # override me
      perform(JSON.parse(message.value).with_indifferent_access)
    end

    def perform(hash)
      # override me
    end

    def on_error(message, err)
      # override me
    end
  end
end

env = ENV['RACK'] || ENV['ENV_DOMAIN_NAME'] || ENV['RACK_ENV'] || ENV['RAILS_ENV'] || 'development'

if defined?(Raven)
  Raven.configure do |config|
    config.current_environment = env
  end
  config.release = ENV['RELEASE'] if ENV['RELEASE'].present?
  config.sanitize_fields = Rails.application.config.filter_parameters.map(&:to_s)
  config.current_environment = env

  module KafkaWorker
    class Worker
      def capture_exception(err, error_message)
        Raven.capture_exception(err, extra: {'message' => error_message})
      end
    end
  end
elsif defined?(Rollbar)
  Rollbar.configure do |config|
    if ENV['ROLLBAR_ACCESS_TOKEN'] && ['staging', 'production'].include?(env)
      config.access_token = ENV['ROLLBAR_ACCESS_TOKEN']
      config.enabled      = true
    else
      config.enabled      = false
    end
  end

  module KafkaWorker
    class Worker
      def capture_exception(err, error_message)
        Rollbar.error(err, error_message)
      end
    end
  end
end
