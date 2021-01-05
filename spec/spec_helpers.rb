# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/logging/logger"

LogStash::Logging::Logger::configure_logging("debug")

RSpec.configure do |config|
  # register around filter that captures stdout and stderr
  config.around(:each) do |example|
    $stdout = StringIO.new
    $stderr = StringIO.new

    example.run

    example.metadata[:stdout] = $stdout.string
    example.metadata[:stderr] = $stderr.string

    $stdout = STDOUT
    $stderr = STDERR
  end
end