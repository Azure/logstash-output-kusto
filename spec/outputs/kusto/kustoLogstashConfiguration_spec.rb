# encoding: utf-8
require_relative "../../spec_helpers.rb"
require 'logstash/outputs/kusto'
require 'logstash/outputs/kusto/kustoLogstashConfiguration'

describe LogStash::Outputs::KustoInternal::KustoLogstashConfiguration do

  let(:ingest_url) { "https://ingest-sdkse2etest.eastus.kusto.windows.net/" }
  let(:app_id) { "myid" }
  let(:app_key) { LogStash::Util::Password.new("mykey") }
  let(:app_tenant) { "mytenant" }
  let(:managed_identity) { "managed_identity" }  
  let(:database) { "mydatabase" }
  let(:table) { "mytable" }
  let(:proxy_host) { "localhost" }
  let(:proxy_port) { 80 }
  let(:proxy_protocol) { "http" }
  let(:json_mapping) { "mymapping" }
  let(:delete_local) { false }
  let(:logger) { spy(:logger) }
  let(:proxy_aad_only) { false }

  describe '#initialize' do
    it 'does not throw an error when initializing' do
      # note that this will cause an internal error since connection is being tried.
      # however we still want to test that all the java stuff is working as expected
      expect { 
        kustoLogstashOutputConfiguration = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, database, table, json_mapping, delete_local, proxy_host , proxy_port , proxy_protocol, proxy_aad_only, logger)
        kustoLogstashOutputConfiguration.validate_config()
      }.not_to raise_error
    end
    
    dynamic_name_array = ['/a%{name}/', '/a %{name}/', '/a- %{name}/', '/a- %{name}']

    context 'doesnt allow database to have some dynamic part' do
      dynamic_name_array.each do |test_database|
        it "with database: #{test_database}" do
          expect {
            kustoLogstashOutputConfiguration = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, test_database, table, json_mapping, delete_local, proxy_host, proxy_port,proxy_protocol, proxy_aad_only,logger)
            kustoLogstashOutputConfiguration.validate_config()
          }.to raise_error(LogStash::ConfigurationError)          
        end
      end
    end

    context 'doesnt allow table to have some dynamic part' do
      dynamic_name_array.each do |test_table|
        it "with database: #{test_table}" do
          expect {
            kustoLogstashOutputConfiguration = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity,database, test_table, json_mapping, delete_local, proxy_host, proxy_port,proxy_protocol, proxy_aad_only,logger)
            kustoLogstashOutputConfiguration.validate_config()
          }.to raise_error(LogStash::ConfigurationError)          
        end
      end
    end

    context 'doesnt allow mapping to have some dynamic part' do
      dynamic_name_array.each do |json_mapping|
        it "with database: #{json_mapping}" do
          expect {
            kustoLogstashOutputConfiguration = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity,database, table, json_mapping, delete_local, proxy_host, proxy_port,proxy_protocol, proxy_aad_only,logger)
            kustoLogstashOutputConfiguration.validate_config()
          }.to raise_error(LogStash::ConfigurationError)          
        end
      end
    end

    context 'proxy protocol has to be http or https' do
      it "with proxy protocol: socks" do
        expect {
          kustoLogstashOutputConfiguration = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity,database, table, json_mapping, delete_local, proxy_host, proxy_port,'socks', proxy_aad_only,logger)
          kustoLogstashOutputConfiguration.validate_config()
        }.to raise_error(LogStash::ConfigurationError)          
      end
    end

    context 'one of appid or managedid has to be provided' do
      it "with empty managed identity and appid" do
        expect {
          kustoLogstashOutputConfiguration = described_class.new(ingest_url, "", app_key, app_tenant, "",database, table, json_mapping, delete_local, proxy_host, proxy_port,'socks', proxy_aad_only,logger)
          kustoLogstashOutputConfiguration.validate_config()
        }.to raise_error(LogStash::ConfigurationError)          
      end
    end

    context 'if proxy_aad is provided' do
      it "proxy details should be provided" do
        expect {
          kustoLogstashOutputConfiguration = described_class.new(ingest_url, "", app_key, app_tenant, "",database, table, json_mapping, delete_local, nil, nil,'https', true,logger)
          kustoLogstashOutputConfiguration.validate_config()
        }.to raise_error(LogStash::ConfigurationError)          
      end
    end
  end
end
