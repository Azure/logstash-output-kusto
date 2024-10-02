# encoding: utf-8
require_relative "../spec_helpers.rb"
require 'logstash/outputs/kusto'
require 'logstash/codecs/plain'
require 'logstash/event'

describe LogStash::Outputs::Kusto do

  let(:options) { { "path" => "./kusto_tst/%{+YYYY-MM-dd-HH-mm}",
    "ingest_url" => "https://ingest-sdkse2etest.eastus.kusto.windows.net/",
    "app_id" => "myid",
    "app_key" => "mykey",
    "app_tenant" => "mytenant",
    "database" => "mydatabase",
    "table" => "mytable",
    "json_mapping" => "mymapping",
    "proxy_host" => "localhost",
    "proxy_port" => 3128,
    "proxy_protocol" => "https",
    "max_size" => 2000,
    "max_interval" => 10
  } }
  
  describe '#initialize' do
    it 'initializes with the correct options' do
      RSpec.configuration.reporter.message("Running test: initializes with the correct options")
      kusto = described_class.new(options.merge("app_key" => LogStash::Util::Password.new("mykey")))
      expect(kusto.instance_variable_get(:@path)).to eq("./kusto_tst/%{+YYYY-MM-dd-HH-mm}")
      expect(kusto.instance_variable_get(:@ingest_url)).to eq("https://ingest-sdkse2etest.eastus.kusto.windows.net/")
      expect(kusto.instance_variable_get(:@app_id)).to eq("myid")
      expect(kusto.instance_variable_get(:@app_key).value).to eq("mykey")
      expect(kusto.instance_variable_get(:@app_tenant)).to eq("mytenant")
      expect(kusto.instance_variable_get(:@database)).to eq("mydatabase")
      expect(kusto.instance_variable_get(:@table)).to eq("mytable")
      expect(kusto.instance_variable_get(:@json_mapping)).to eq("mymapping")
      expect(kusto.instance_variable_get(:@proxy_host)).to eq("localhost")
      expect(kusto.instance_variable_get(:@proxy_port)).to eq(3128)
      expect(kusto.instance_variable_get(:@proxy_protocol)).to eq("https")
      expect(kusto.instance_variable_get(:@max_size)).to eq(2000)
      expect(kusto.instance_variable_get(:@max_interval)).to eq(10)
      RSpec.configuration.reporter.message("Completed test: initializes with the correct options")
    end
  end

  describe '#multi_receive_encoded' do
    it 'processes events and adds them to the buffer' do
      RSpec.configuration.reporter.message("Running test: processes events and adds them to the buffer")
      kusto = described_class.new(options)
      kusto.register

      events = [LogStash::Event.new("message" => "test1"), LogStash::Event.new("message" => "test2")]
      encoded_events = events.map { |e| [e, e.to_json] }
      kusto.multi_receive_encoded(encoded_events)

      buffer = kusto.instance_variable_get(:@buffer)
      expect(buffer.instance_variable_get(:@buffer).size).to eq(2)
      RSpec.configuration.reporter.message("Completed test: processes events and adds them to the buffer")
    end
  
    it 'handles errors during event processing' do
      RSpec.configuration.reporter.message("Running test: handles errors during event processing")
      kusto = described_class.new(options)
      kusto.register

      allow(kusto.instance_variable_get(:@buffer)).to receive(:<<).and_raise(StandardError.new("Test error"))
      events = [LogStash::Event.new("message" => "test1")]
      encoded_events = events.map { |e| [e, e.to_json] }

      expect { kusto.multi_receive_encoded(encoded_events) }.not_to raise_error
      RSpec.configuration.reporter.message("Completed test: handles errors during event processing")
    end
  end

  describe '#register' do
    it 'raises an error for invalid configurations' do
      RSpec.configuration.reporter.message("Running test: raises an error for invalid configurations")
      invalid_options = options.merge("ingest_url" => nil)
      expect { described_class.new(invalid_options).register }.to raise_error(LogStash::ConfigurationError)
      RSpec.configuration.reporter.message("Completed test: raises an error for invalid configurations")
    end
  end

  describe '#flush_buffer' do
    it 'handles errors during buffer flushing' do
      RSpec.configuration.reporter.message("Running test: handles errors during buffer flushing")
      kusto = described_class.new(options)
      kusto.register

      allow(kusto.instance_variable_get(:@ingestor)).to receive(:upload_async).and_raise(StandardError.new("Test error"))
      events = [LogStash::Event.new("message" => "test1")]
      encoded_events = events.map { |e| [e, e.to_json] }
      kusto.multi_receive_encoded(encoded_events)

      expect { kusto.flush_buffer(encoded_events) }.not_to raise_error
      RSpec.configuration.reporter.message("Completed test: handles errors during buffer flushing")
    end

    it 'flushes the buffer when max_size is reached' do
      RSpec.configuration.reporter.message("Running test: flushes the buffer when max_size is reached")
      kusto = described_class.new(options.merge("max_size" => 1)) # Set max_size to 1MB for testing
      kusto.register

      events = [LogStash::Event.new("message" => "test1")]
      encoded_events = events.map { |e| [e, e.to_json] }
      expect(kusto.instance_variable_get(:@ingestor)).to receive(:upload_async).with(anything)
      kusto.multi_receive_encoded(encoded_events)
      kusto.flush_buffer(encoded_events) # Pass the encoded events
      RSpec.configuration.reporter.message("Completed test: flushes the buffer when max_size is reached")
    end

    it 'flushes the buffer when max_interval is reached' do
      RSpec.configuration.reporter.message("Running test: flushes the buffer when max_interval is reached")
      kusto = described_class.new(options.merge("max_interval" => 1)) # Set max_interval to 1 second for testing
      kusto.register

      events = [LogStash::Event.new("message" => "test1")]
      encoded_events = events.map { |e| [e, e.to_json] }
      kusto.multi_receive_encoded(encoded_events)
      sleep(2) # Wait for the interval to pass

      expect(kusto.instance_variable_get(:@ingestor)).to receive(:upload_async).with(anything)
      kusto.flush_buffer(encoded_events) # Pass the encoded events
      RSpec.configuration.reporter.message("Completed test: flushes the buffer when max_interval is reached")
    end

    it 'eventually flushes without receiving additional events based on max_interval' do
      RSpec.configuration.reporter.message("Running test: eventually flushes without receiving additional events based on max_interval")
      kusto = described_class.new(options.merge("max_interval" => 1)) # Set max_interval to 1 second for testing
      kusto.register
    
      events = [LogStash::Event.new("message" => "test1")]
      encoded_events = events.map { |e| [e, e.to_json] }
      kusto.multi_receive_encoded(encoded_events)
    
      # Wait for the interval to pass
      sleep(2)
    
      expect(kusto.instance_variable_get(:@ingestor)).to receive(:upload_async).with(anything)
      kusto.flush_buffer(encoded_events) # Pass the encoded events
      RSpec.configuration.reporter.message("Completed test: eventually flushes without receiving additional events based on max_interval")
    end
  end

  describe '#close' do
    it 'shuts down the buffer and ingestor' do
      RSpec.configuration.reporter.message("Running test: shuts down the buffer and ingestor")
      kusto = described_class.new(options)
      kusto.register

      expect(kusto.instance_variable_get(:@buffer)).to receive(:shutdown)
      expect(kusto.instance_variable_get(:@ingestor)).to receive(:stop)

      kusto.close
      RSpec.configuration.reporter.message("Completed test: shuts down the buffer and ingestor")
    end
  end
end