# frozen_string_literal: true

require 'active_support/concern'
require 'active_support/notifications'
require 'active_support/core_ext/hash'
require 'kafka'
require 'kafka_worker/worker'
require 'kafka_worker/handler'

if defined?(Rollbar)
  Rollbar.configure do |config|
    if ENV['ROLLBAR_ACCESS_TOKEN'] && ['staging', 'production'].include?(ENV['ENV_DOMAIN_NAME'] || Rails.env)
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
elsif defined?(Raven)
  Raven.configure do |config|
    config.current_environment = ENV['RACK'] || ENV['RAILS_ENV'] || 'development'
  end

  module KafkaWorker
    class Worker
      def capture_exception(err, error_message)
        Raven.capture_exception(err, extra: {'message' => error_message})
      end
    end
  end
end
