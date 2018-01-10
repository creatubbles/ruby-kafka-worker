# frozen_string_literal: true

require 'active_support/concern'
require 'active_support/notifications'
require 'active_support/core_ext/hash'
require 'kafka'
require 'kafka_worker/worker'
require 'kafka_worker/handler'

module KafkaWorker
  def self.environment
    ENV['RACK_ENV'] || ENV['RAILS_ENV'] || 'development'
  end

  def self.production_env?
    environment == 'production'
  end

  def self.handlers
    @handlers ||= []
  end

  mattr_writer :logging_level
  def self.logging_level
    @logging_level ||= Logger::INFO
  end

  mattr_writer :logger
  def self.logger
    @logger ||= Logger.new(STDOUT).tap { |l| l.level = logging_level }
  end

  mattr_writer :kafka_logging_level
  def self.kafka_logging_level
    @kafka_logging_level ||= production_env? ? Logger::WARN : Logger::INFO
  end

  mattr_writer :kafka_logger
  def self.kafka_logger
    @kafka_logger ||= Logger.new(STDOUT).tap { |l| l.level = kafka_logging_level }
  end
end

ActiveSupport::Notifications.subscribe('request.connection.kafka') do |*data|
  event = ActiveSupport::Notifications::Event.new(*data)
  KafkaWorker.logger.debug("Received notification `#{event.name}` with payload: #{event.payload.inspect}")
end

ActiveSupport::Notifications.subscribe('kafka_worker.process_message') do |*data|
  event = ActiveSupport::Notifications::Event.new(*data)
  message = event.payload[:message]
  KafkaWorker.logger.info("Received message #{message.topic}, value #{message.value.inspect}")
end

ActiveSupport::Notifications.subscribe('kafka_worker.processing_error') do |*data|
  event = ActiveSupport::Notifications::Event.new(*data)
  KafkaWorker.logger.error(event.payload[:error_message])
end

ActiveSupport::Notifications.subscribe('kafka_worker.giving_up_processing') do |*data|
  event = ActiveSupport::Notifications::Event.new(*data)
  message = event.payload[:message]
  KafkaWorker.logger.info("Failed on message #{message.topic}/#{message.offset} 5 times, giving up")
end

ActiveSupport::Notifications.subscribe("kafka_worker.publish_to_error_topic_failed") do |*data|
  event = ActiveSupport::Notifications::Event.new(*data)
  message = event.payload[:message]
  topic = event.payload[:topic]
  err = event.payload[:error]
  KafkaWorker.logger.error("Could not publish #{message} to topic #{topic}: #{err}")
end

if defined?(Raven)
  Raven.configure do |config|
    config.current_environment = ENV['RACK'] || ENV['RAILS_ENV'] || 'development'
  end

  ActiveSupport::Notifications.subscribe('kafka_worker.processing_error') do |*data|
    event = ActiveSupport::Notifications::Event.new(*data)
    err = event.payload[:error]
    error_message = event.payload[:error_message]
    Raven.capture_exception(err, extra: { 'message' => error_message })
  end

  ActiveSupport::Notifications.subscribe("kafka_worker.publish_to_error_topic_failed") do |*data|
    event = ActiveSupport::Notifications::Event.new(*data)
    message = event.payload[:message]
    topic = event.payload[:topic]
    err = event.payload[:error]
    Raven.capture_exception(err, extra: { 'message' => message, 'topic' => topic })
  end
end
