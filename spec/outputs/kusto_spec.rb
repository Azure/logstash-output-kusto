# encoding: utf-8
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
  
  describe '#register' do
    it 'allows valid configuration' do
      RSpec.configuration.reporter.message("Running test: allows valid configuration")
      kusto = described_class.new(options)
      expect { kusto.register }.not_to raise_error
      kusto.close
      RSpec.configuration.reporter.message("Completed test: allows valid configuration")
    end
  end

  describe '#multi_receive_encoded' do
    it 'buffers events and flushes based on max_size' do
      RSpec.configuration.reporter.message("Running test: buffers events and flushes based on max_size")
      kusto = described_class.new(options.merge({'max_size' => 2}))
      kusto.register

      event1 = LogStash::Event.new("message" => "event1")
      event2 = LogStash::Event.new("message" => "event2")
      event3 = LogStash::Event.new("message" => "event3")

      expect(kusto.instance_variable_get(:@buffer)).to receive(:flush).twice.and_call_original

      kusto.multi_receive_encoded([[event1, event1.to_json], [event2, event2.to_json]])
      kusto.multi_receive_encoded([[event3, event3.to_json]])

      kusto.close
      RSpec.configuration.reporter.message("Completed test: buffers events and flushes based on max_size")
    end

    it 'flushes events based on max_interval' do
      RSpec.configuration.reporter.message("Running test: flushes events based on max_interval")
      kusto = described_class.new(options.merge({'max_interval' => 1}))
      kusto.register

      event1 = LogStash::Event.new("message" => "event1")

      expect(kusto.instance_variable_get(:@buffer)).to receive(:flush).at_least(:once).and_call_original

      kusto.multi_receive_encoded([[event1, event1.to_json]])

      sleep 2

      kusto.close
      RSpec.configuration.reporter.message("Completed test: flushes events based on max_interval")
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