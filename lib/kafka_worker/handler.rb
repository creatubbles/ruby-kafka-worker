# frozen_string_literal: true

module KafkaWorker
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
