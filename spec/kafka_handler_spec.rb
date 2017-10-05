# frozen_string_literal: true

require 'kafka_worker'
require 'rspec'

Dir[File.join(File.dirname(__FILE__), 'handlers/*.rb')].each { |f| require f }

describe KafkaWorker::Handler do
  it "has default retry_interval" do
    expect(SimpleHandler.retry_interval).to eq(60)
    expect(HelloHandler.retry_interval).to eq(0)
  end

  it "doesn't start from beginnign" do
    expect(SimpleHandler.start_from_beginning).to eq(false)
    expect(HelloHandler.start_from_beginning).to eq(false)
  end

  it "returns topic" do
    expect(SimpleHandler.topic).to eq("simple")
    expect(HelloHandler.topic).to eq("hello")
  end
end
