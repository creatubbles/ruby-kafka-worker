require 'kafka-worker'

class User
  attr_accessor :id
  def initialize(id)
    @id = id
  end
end

# created each time useful if you need to mutate
class AbstractHandler
  attr_accessor :user
  def initialize
    @user = User.new(1)
  end
end

class FirstHandler < AbstractHandler
  include KafkaWorker::Handler
  consumes "hello"

  def handle(message)
    puts 'handle hello'
    puts "previous user.id value is #{user.id}"
    user.id = message.value
    puts "new user.id value is #{user.id}"
  end
end

class SecondHandler < AbstractHandler
  include KafkaWorker::Handler
  consumes "goodbye"

  def handle(message)
    puts 'handle goodbye'
    puts "previous user.id value is #{user.id}"
    user.id = message.value
    puts "new user.id value is #{user.id}"
  end
end

opts = {
   kafka_ips: "127.0.0.1:9092",
   client_id: 'test',
   group_id: 'test'
}

kw = KafkaWorker::Worker.new(opts)
kw.run
trap("QUIT") { kw.stop_consumer }

=begin
you can post messages in the command line if you are running kafka locally
and see the results in the process running this program
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic goodbye
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic goodbye
=end