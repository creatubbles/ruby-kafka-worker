# frozen_string_literal: true

module KafkaWorker
  class Worker
    def initialize(seed_brokers, client_id, group_id, opts={})
      @kafka = Kafka.new(seed_brokers: seed_brokers, client_id: client_id, logger: KafkaWorker.kafka_logger)
      @kafka_consumer = @kafka.consumer(
        group_id: group_id,
        # Increase offset commit frequency to once every 5 seconds.
        offset_commit_interval: opts[:offset_commit_interval] || 5,
        # Commit offsets when 1 messages have been processed. Prevent duplication.
        offset_commit_threshold: opts[:offset_commit_threshold] || 1
      )
      @handlers = ::KafkaWorker.handlers.dup
      @executor = Class.new(ActiveSupport::Executor)
    end

    def run
      ActiveSupport::Notifications.instrument("kafka_worker.before_run")

      @handlers.each do |handler|
        @kafka_consumer.subscribe(handler.topic, start_from_beginning: handler.start_from_beginning)
      end

      @kafka_consumer.each_message do |message|
        ActiveSupport::Notifications.instrument("kafka_worker.process_message", message: message) do
          process_message(message)
        end
      end
    end

    def process_message(message)
      @handlers.each do |handler|
        next unless message.topic == handler.topic

        tries = 5
        begin
          state = @executor.run!
          handler_obj = handler.new
          handler_obj.logger = KafkaWorker.logger
          handler_obj.handle(message)
          state.complete!
        rescue => err
          error_message = "Failed on message #{message.topic}/#{message.offset}, value #{message.value.inspect} with error: #{err}"
          ActiveSupport::Notifications.instrument("kafka_worker.processing_error", message: message, handler: handler.to_s, error: err, error_message: error_message) do
            handler_obj.on_error(message, err)
          end
          state.complete!
          if (tries -= 1).positive?
            sleep handler.retry_interval
            retry
          else
            ActiveSupport::Notifications.instrument("kafka_worker.giving_up_processing", message: message, handler: handler.to_s, error: err.to_s) do
              publish_to_error_topic(message, handler, err.to_s)
            end
          end
        end
      end
    end

    def stop_consumer
      KafkaWorker.logger.info("Stopping KafkaWorker::Worker @kafka_consumer")
      @kafka_consumer.stop
    rescue => err
      KafkaWorker.logger.warn("Could not stop KafkaWorker::Worker @kafka_consumer: #{err}")
    end

    private

    def publish_to_error_topic(orig_message, handler, error)
      error_topic_deliver_tries = 0
      message = {
        failed_at: Time.now,
        error: error,
        handler: handler.to_s,
        message: {
          key: orig_message.key,
          topic: orig_message.topic,
          offset: orig_message.offset,
          value: orig_message.value
        }
      }.to_json
      topic = "#{orig_message.topic}-failed"
      begin
        @kafka.deliver_message(message, topic: topic)
      rescue Kafka::LeaderNotAvailable => err
        if error_topic_deliver_tries >= 4
          ActiveSupport::Notifications.instrument("kafka_worker.publish_to_error_topic_failed", message: message, topic: topic, error: err)
        else
          KafkaWorker.logger.info("publising to #{topic} failed (#{err}), retying")
          error_topic_deliver_tries += 1
          sleep 1
          retry
        end
      rescue => err
        ActiveSupport::Notifications.instrument("kafka_worker.publish_to_error_topic_failed", message: message, topic: topic, error: err)
      end
    end
  end
end
