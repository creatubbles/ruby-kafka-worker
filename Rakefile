require 'bundler/gem_tasks'
require 'rake'
require 'rake/testtask'
require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new(:spec) do |t|
  t.pattern = 'spec/kafka-worker-spec.rb'
end
task :default => :spec