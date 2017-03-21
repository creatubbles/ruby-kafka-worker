require 'kafka'
require 'kafka-worker'
require 'rspec'

=begin
if Rails.env.development?
  $kafka = Kafka.new(
    seed_brokers: (ENV['KAFKA_ENDPOINTS'] || "localhost").split(","),
    client_id: "ctbweb",
    logger: Rails.logger
  )
else
  $kafka = Kafka.new(
    seed_brokers: (ENV['KAFKA_ENDPOINTS'] || "localhost").split(","),
    client_id: "ctbweb",
    logger: Rails.logger,
    ssl_ca_cert: File.read('ssl/ca-cert.pem'),
    ssl_client_cert: File.read('ssl/client-cert.pem'),
    ssl_client_cert_key: File.read('ssl/client-key.pem')
  )
end

# Set up an asynchronous producer that delivers its buffered messages
# every ten seconds:
$kafka_producer = $kafka.async_producer(
  delivery_interval: 10,
)

# Make sure to shut down the producer when exiting.
at_exit { $kafka_producer.shutdown }
=end

describe KafkaWorker do
  before (:all) do
    # make a producer to push events to the KafkaWorker
    @kafka = Kafka.new(
      seed_brokers: '127.0.0.1:9092',
      client_id: 'kafka-worker-spec',
      logger: Logger.new(STDOUT)
    )
    
    @kafka_producer = @kafka.async_producer(
      delivery_interval: 1,
    )
    
=begin    
    class GoodbyeHandler < KafkaWorker::Handler
      consumes "goodbye"
      def handle(message)
        puts 'got a goodbye message'
        $goodbyes << 'goodbye'
      end
    end
=end    

  end
  
  before(:each) do 
    # each handler needs a unique class name

  end
  
  after(:all) do
#    Process.kill("QUIT", @kw_pid)
#    Process.wait
    @kafka_producer.shutdown
  end

  it "consume events and pushes a value into a class object" do
    class HelloHandler < KafkaWorker::Handler
      consumes "hello"
      @@received_msg = false
      def handle(message)
        puts 'got a hello message'
        @@received_msg = true
        puts @@received_msg
      end
      
      def self.received_msg
        @@received_msg
      end
    end  

    opts = {
      kafka_ips: ['127.0.0.1:9092'],
      client_id: 'test',
      group_id:  'test'
    }
    
    @kw_pid = fork do
      kw = KafkaWorker::Worker.new(opts)
      kw.run
    end

    @kafka_producer.produce('hello', topic:'hello')
    #sleep 6
    sleep 4
    puts HelloHandler.received_msg
    expect(HelloHandler.received_msg).to eq true

    Process.kill("QUIT", @kw_pid)
    Process.wait

  end
end