# frozen_string_literal: true

class PerformOverridingHandler
  include KafkaWorker::Handler
  consumes "perform-hello"
  retry_interval 0

  mattr_accessor :hash_value
  mattr_accessor :processed_messages_count

  def perform(hash)
    puts "PerformOverridingHandler received the following hash: #{hash}"
    self.class.hash_value = hash
    self.class.processed_messages_count += 1
  end
end
