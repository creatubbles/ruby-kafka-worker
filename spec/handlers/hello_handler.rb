# frozen_string_literal: true

class HelloHandler
  include KafkaWorker::Handler
  consumes "hello"
  retry_interval 0

  mattr_accessor :message_value
  mattr_accessor :processed_messages_count

  def handle(message)
    puts "HelloHandler received the following message: #{message.value}"
    self.class.message_value = message.value
    self.class.processed_messages_count += 1
  end
end
