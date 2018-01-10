# kafka-worker

Generalizes Kafka initalization and event consumption. Now you just need to register Handler classes for each consumed event, which can be as simple as:

```ruby
class MessageLoggingHandler
  include KafkaWorker::Handler
  consumes 'topic'
  def handle(message)
    puts message.value
  end
end
```

## Installing kafka-worker

Add the following line to your Gemfile

```
gem 'kafka-worker', github: 'creatubbles/ruby-kafka-worker'
```

### Rails

For Rails you can then run the install generator:

```
rails generate kafka_worker:install
```

### Other (non-Rails projects)

Alternatively if you're not using Rails as base for your project, create a directory `app/kafka_handlers` for the handlers (see below) and create a script like the one below to start the worker:

```ruby
#!/usr/bin/env ruby
require_relative '../config/environment' # load your environment, including the Kafka handlers
require 'kafka_worker/runner'            # load the runner script
KafkaWorker::Runner.run
```

## Writing handlers

For each Kafka topic you want to respond to, create a class that includes `KafkaWorker::Handler`, `consumes('name-of-topic')` and a custom definition of the `handle(message)` method.

For instance:
```ruby
class HelloWorldTopicHandler
  include KafkaWorker::Handler
  consumes 'hello-world'

  def handle(message)
    # handle message.value
  end
end
```

Now `HelloWorldTopicHandler#handle` will be called every time we receive a message for the topic `hello-world`.

If you're passing along JSON encoded messages, we can also overwrite `perform` instead of `handle`. The default implementation of `handle` parses the JSON message into a hash and passes it to `perform`. Thus it's usually enough to override `perform(hash)` in your handler:

```ruby
class HelloWorldTopicHandler
  include KafkaWorker::Handler
  consumes 'hello-world'

  def perform(hash)
    # handle JSON message parsed into hash
  end
end
```

You can define multiple handlers across separate files and they will all be automatically registered in `KafkaWorker::Handler` and called for the topics they consume.

### Advanced handlers

If you want to initialize and share values between handlers, you need to declare a class that the handler will subclass, and then initialize the variables in an initializer script.

For example, if you have the following handlers:

```ruby
class BaseKafkaHandler
  cattr_accessor :x
end

class ChildTopicHandler < BaseKafkaHandler
  include KafkaWorker::Handler
  consumes 'hello-world'

  def handle(message)
    print x
  end
end

class SecondChildTopicHandler < BaseKafkaHandler
  include KafkaWorker::Handler
  consumes 'goodbye'

  def handle(message)
    print x
  end
end
```

Then you can put the following code into `config/initializers/kafka_worker.rb` (Rails) or some similar file you're requiring from your custom start script to run before the worker starts:

```ruby
ActiveSupport::Notifications.subscribe('kafka_worker.before_run') do
  BaseKafkaHandler.x = "this is shared"
end
```

### Handling errors

You can override `on_error(message, err)` in each handler to handle errors on the handler.

If you'd like a more generic approach, you can also subscribe to the error event. See `lib/kafka_worker.rb` for details.

#### Sentry support

This gem will automatically log errors to Sentry if you have the `sentry-raven` gem installed and loaded and `SENTRY_DSN` is in your environment or you somehow else initialize the gem.

## How to test topic handlers

### Handler overriding `handle` method

Handler:
```ruby
class HelloWorldTopicHandler
  include KafkaWorker::Handler
  consumes 'hello-world'

  def handle(message)
    print JSON.parse(message.value)['say']
  end
end
```

Test:
```ruby
require 'test/kafka_message'
describe HelloWorldTopicHandler do
  it 'prints say message' do
    expect do
      HelloWorldTopicHandler.new.handle(Test::KafkaMessage.new('hello-world', {
        say: 'Hello!'
      }))
    end.to output('Hello!').to_stdout
  end
end
```

### Handler overriding `perform` method

Handler:
```ruby
class HelloWorldTopicHandler
  include KafkaWorker::Handler
  consumes 'hello-world'

  def perform(message)
    print message[:say]
  end
end
```

Test:
```ruby
require 'test/kafka_message'
describe HelloWorldTopicHandler do
  it 'prints say message' do
    expect do
      HelloWorldTopicHandler.new.perform({
        say: 'Hello!'
      })
    end.to output('Hello!').to_stdout
  end
end
```
