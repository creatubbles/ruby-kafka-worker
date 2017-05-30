module Test
  class KafkaMessage
    attr_reader :topic, :value

    def initialize(topic, value)
      @topic = topic
      @value = value.to_json
    end
  end
end
