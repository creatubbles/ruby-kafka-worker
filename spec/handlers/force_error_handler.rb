# frozen_string_literal: true

class ForceErrorHandler
  include KafkaWorker::Handler
  consumes "error"
  retry_interval 0

  mattr_accessor :message_value
  mattr_accessor :error_value
  mattr_accessor :processed_messages_count

  def handle(message)
    self.class.processed_messages_count += 1
    puts "got #{message}; ForceErrorHandler is going to raise error"
    raise 'default error message'
  end

  def on_error(message, err)
    puts err
    self.class.message_value = message.value
    self.class.error_value = err
  end
end
