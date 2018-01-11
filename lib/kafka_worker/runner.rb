# frozen_string_literal: true

require 'kafka_worker'

module KafkaWorker
  module Runner
    class << self
      def run
        STDOUT.sync = true # for logging
        logger.info("running in #{environment} environment") unless production_env?
        unless kafka_endpoints.present?
          logger.error("environment variable KAFKA_ENDPOINTS not set")
          return
        end
        unless group_id.present?
          logger.error("could not compute group_id, set environment variable APP")
          return
        end
        unless client_id.present? && client_id != group_id
          logger.error("could not compute client_id, set environment variable APP, RELEASE, RACK, HOSTNAME")
          return
        end
        if rails_application
          Dir[Rails.root.join("app/kafka_handlers/*.rb")].each { |f| require f }
        end
        unless handlers_registered?
          logger.error("no handlers registered")
          return
        end

        logger.info("Starting KafkaWorker #{client_id} for group #{group_id}")
        kw = KafkaWorker::Worker.new(kafka_seed_brokers, client_id, group_id)
        kw.run
        trap("QUIT") { kw.stop_consumer }
      end

      delegate :logger, :environment, :production_env?, to: :KafkaWorker

      def rails_application
        Rails.application if defined?(Rails)
      end

      def kafka_endpoints
        ENV['KAFKA_ENDPOINTS'] || (production_env? ? nil : 'localhost')
      end

      def kafka_seed_brokers
        kafka_endpoints.split(',')
      end

      def group_id
        ENV['KAFKA_GROUP'] || ENV['APP'] || guess_group_id
      end

      def guess_group_id
        rails_application.class.name.split("::").first.underscore if rails_application
      end

      def client_id
        [group_id, ENV['RELEASE'], ENV['RACK'], `hostname`].compact.map(&:strip).join('-')
      end

      def handlers_registered?
        KafkaWorker.handlers.any?
      end
    end
  end
end
