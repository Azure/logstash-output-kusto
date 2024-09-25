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