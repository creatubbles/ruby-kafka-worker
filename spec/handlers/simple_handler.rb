# frozen_string_literal: true

class SimpleHandler
  include KafkaWorker::Handler
  consumes "simple"
  mattr_accessor :processed_messages_count

  def perform(message)
    message
  end
end
