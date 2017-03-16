require 'kafka'

# bin files in gemspec

module KafkaWorker
  class Worker

    def self.handlers
      @handlers ||= []
    end

    def initialize(opts)
      @logger = Logger.new(STDOUT)

      kafka_ips = opts.delete(:kafka_ips)
      @kafka_consumer = init_kafka_consumer(kafka_ips, 'warehouse-kafka-worker', 'warehouse-kafka-worker-consumer')
    end

    def run
      handlers = self.class.handlers.dup

      handlers.each do |handler|
        @kafka_consumer.subscribe(handler.topic, start_from_beginning: false)
      end

      @kafka_consumer.each_message do |message|
        handlers.each do |handler|
          if message.topic == handler.topic
            handler_obj = handler.new(@logger)
            begin
              handler_obj.handle(message)
            rescue err
              handler_obj.on_error(message, err)
            end
          end
        end
      end
    end

    private
    def init_kafka_consumer(kafka_ips, client_id, group_id)
      opts = {
        seed_brokers: kafka_ips,
        client_id: client_id,
        logger: @logger,
      }
      kafka = Kafka.new(opts)
      kafka.consumer(
        group_id: group_id,
        # Increase offset commit frequency to once every 5 seconds.
        offset_commit_interval: 10,
        # Commit offsets when 1 messages have been processed. Prevent duplication.
        offset_commit_threshold: 1)
    end
  end

  class Handler

    attr_reader :logger

    # https://ruby-doc.org/core-2.2.0/Class.html#method-i-inherited
    def self.inherited(clazz)
      Worker.handlers << clazz
    end

    def self.consumes(topic)
      @topic = topic
    end

    def self.topic
      @topic
    end

    def initialize(logger)
      @logger = logger
    end

    def handle(message)
      # override me
    end

    def on_error(message, err)
      logger.error("#{self.class.name} failed on message: #{message.inspect} with error: #{err}")
    end
  end
end

=begin
class Handler < KafkaWorker::Handler
  consumes "hello"

  def handle(message)
    puts 'handle hello'
    puts message.inspect
  end
end

class SecondHandler < KafkaWorker::Handler
  consumes "goodbye"

  def handle(message)
    puts 'handle goodbye'
    puts message.inspect
  end
end


opts = {
   client_id: ENV['CREATUBBLES_CLIENT_ID'],
   client_secret: ENV['CREATUBBLES_CLIENT_SECRET'],
   api_url: ENV['CREATUBBLES_API_URL'] || Creatubbles::DEFAULT_API_URL,
   kafka_ips: (ENV['KAFKA_ENDPOINTS'] || "localhost").split(",")
}
kw = KafkaWorker::Warehouse.new(opts)
kw.run
trap("QUIT") { kw.stop_consumer }
=end
=begin
opts = {
   client_id: ENV['CREATUBBLES_CLIENT_ID'],
   client_secret: ENV['CREATUBBLES_CLIENT_SECRET'],
   api_url: ENV['CREATUBBLES_API_URL'] || Creatubbles::DEFAULT_API_URL,
   kafka_ips: (ENV['KAFKA_ENDPOINTS'] || "localhost").split(",")
}
=end

=begin
opts = {
   kafka_ips: (ENV['KAFKA_ENDPOINTS'] || "localhost").split(",")
}
k = KafkaWorker::Worker.new(opts)
k.run
=end
#Handler.test
#Handler.backend "something else"
