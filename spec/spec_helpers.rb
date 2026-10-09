# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/logging/logger"
require 'logstash/outputs/kusto'

LogStash::Logging::Logger::configure_logging("debug")

RSpec.configure do |config|
  # Unit tests exercise real SDK property/auth builders, but never create a
  # network client. Individual upload/factory tests provide explicit behavior.
  config.before(:each) do
    factory = Java::com.microsoft.azure.kusto.ingest.IngestClientFactory
    client = double('offline Kusto client', close: nil, ingestFromFile: nil)
    allow(factory).to receive(:createClient).and_return(client)
    allow(factory).to receive(:createManagedStreamingIngestClient).and_return(client)
  end

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
