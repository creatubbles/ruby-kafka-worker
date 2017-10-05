# frozen_string_literal: true

require 'active_support/concern'
require 'active_support/notifications'
require 'active_support/core_ext/hash'
require 'kafka'
require 'kafka_worker/worker'
require 'kafka_worker/handler'

module KafkaWorker
  def self.handlers
    @handlers ||= []
  end

  mattr_writer :logger
  def self.logger
    @logger ||= Logger.new(STDOUT).tap { |l| l.level = Logger::INFO }
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
end
