# frozen_string_literal: true

module KafkaWorker
  module Handler
    extend ActiveSupport::Concern

    included do
      ::KafkaWorker.handlers << self
      attr_accessor :logger
      @retry_interval = 5
      @start_from_beginning = false
    end

    class_methods do
      attr_reader :topic

      def consumes(topic)
        @topic = topic
      end

      def retry_interval(val = nil)
        @retry_interval = val if val
        @retry_interval
      end

      def start_from_beginning(val = nil)
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
