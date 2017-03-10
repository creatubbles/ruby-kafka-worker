require 'kafka'
=begin
class OrderServiceHandler < KafkaWorker::BaseHandler
  consumes "warehouse-asset-approved", "warehouse-asset-rejected"
  def handle(message)
    ...
  end
end


#puts (respond_to? :test=)
#puts (defined? self.text=())
#puts (respond_to? :nothing_func=)
singleton_class.send(:define_method, "backend") do |par|
  puts "Born from the ashes!"
  puts par
end
#define_singleton_method("backend") do
#  puts "Born from the ashes!"
#end
puts self.respond_to?(:nothing_func)
puts self.respond_to?(:abc)
puts self.respond_to?(:backend)
puts self.respond_to?(:test)
=end

=begin
  def self.included(base)
    class << base
      alias_method :_new, :new

      define_method :new do
        _new.tap do |instance|
          instance.send(:after_init)
        end
      end
    end
  end

  def after_init
    puts 'after_init'
  end
=end

# !self.respond_to?(topic_handler_symbol) &&

module KafkaWorker
#  def tps
#    @tps ||= []
#  end

  def consumes(*topics)
    errors = []
    topics.map do |topic|
      topic_adjusted       = topic.gsub(/-/, "_").downcase
      topic_handler_symbol = "#{topic_adjusted}_handler".to_sym
      if !self.method_defined?(topic_handler_symbol)
        errors << "The class '#{self.name}' consumes the kafka topic '#{topic}'. It requires the method '#{topic_adjusted}_handler(message)'."
      end
    end

    if !errors.empty?
      Kernel.abort(errors.join("\n"))
    else
      # subscribe to topics and call methods
      # inject topics into BaseHandler
      define_method("topics") do
        topics
      end
      #self.class.topics = topics
      #puts self.methods
    end
  end

  class BaseHandler
    extend KafkaWorker
    attr_reader :kafka_consumer, :logger

    def initialize(opts)
      @logger = Logger.new(STDOUT)

      kafka_ips = opts.delete(:kafka_ips)
      @kafka_consumer = init_kafka_consumer(kafka_ips, 'warehouse-kafka-worker', 'warehouse-kafka-worker-consumer')
    end

    def run
      topics.map do |topic|
        kafka_consumer.subscribe(topic, start_from_beginning: false)
      end

      kafka_consumer.each_message do |message|
        
      end
    end

    private
    def init_kafka_consumer(kafka_ips, client_id, group_id)
      opts = {
        seed_brokers: kafka_ips,
        client_id: client_id,
        logger: logger,
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
end

class Handler < KafkaWorker::BaseHandler
  def hello_handler(message)
  end

  def goodbye_handler(message)
  end

  def ctb_web_handler(message)
  end

  consumes "hello", "goodbye", "ctb-web"
end


=begin
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
opts = {
   kafka_ips: (ENV['KAFKA_ENDPOINTS'] || "localhost").split(",")
}
h = Handler.new(opts)
h.run
#Handler.test
#Handler.backend "something else"