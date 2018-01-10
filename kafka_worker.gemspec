$:.unshift(File.join(File.dirname(__FILE__), 'lib'))

require 'kafka_worker/version'
Gem::Specification.new do |s|
  s.name         = 'kafka_worker'
  s.version      = KafkaWorker::VERSION
  s.author       = 'Creatubbles'
  s.email        = 'support@creatubbles.com'
  s.summary      = "Kafka consumer library"
  s.description  = "A kafka client library that simplifies kafka consumers."
  s.files        = ['lib/kafka_worker.rb']
  s.homepage     = 'https://github.com/creatubbles/ruby-kafka-worker'

  s.add_dependency('activesupport')
  s.add_dependency('ruby-kafka', '0.5.2')
end
