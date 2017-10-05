# frozen_string_literal: true

module KafkaWorker
  class Worker
    def self.handlers
      @handlers ||= []
    end

    def initialize(opts)
      @logger = opts[:logger] || Logger.new(STDOUT).tap { |l| l.level = Logger::INFO }

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
          next unless message.topic == handler.topic

          handler_obj = handler.new
          handler_obj.logger = @logger

          tries = 5

          begin
            handler_obj.handle(message)
          rescue => err
            error_message = "#{self.class.name} failed on message: #{message.inspect} with error: #{err}"

            @logger.error(error_message)
            capture_exception(err, error_message)

            handler_obj.on_error(message, err)

            if (tries -= 1).positive?
              sleep(handler.retry_interval)
              retry
            end
          end
        end
      end
    end

    def stop_consumer
      @logger.info("Stopping KafkaWorker::Worker @kafka_consumer")
      @kafka_consumer.stop
    rescue => err
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
        offset_commit_threshold: opts[:offset_commit_threshold] || 1
      )
    end

    def capture_exception(err, error_message)
    end
  end
end
