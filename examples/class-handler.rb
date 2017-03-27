require 'active_support/all'
require 'kafka-worker'

# this handler is useful if you want to initiate a single value that is the 
# same for all handler classes that derive this class

# the cattr_accessor iscreated only once, shared by all handlers, any change 
# to @user will be reflected in all handlers
class ClassAbstractCtbHandler
  cattr_accessor :user
end

class FirstHandler < ClassAbstractCtbHandler
  include KafkaWorker::Handler
  consumes "hello"

  def handle(message)
    puts 'handle hello'
    puts message.inspect
    puts user.id
  end
end

class SecondHandler < ClassAbstractCtbHandler
  include KafkaWorker::Handler
  consumes "goodbye"

  def handle(message)
    puts 'handle goodbye'
    puts message.inspect
    puts user.id
  end
end

class User
  attr_reader :id
  def initialize(id)
    @id = id
  end
end

ClassAbstractCtbHandler.user = User.new(1)
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