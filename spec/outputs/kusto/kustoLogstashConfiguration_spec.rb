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
  let(:cliauth) { false }
  let(:table) { "mytable" }
  let(:proxy_host) { "localhost" }
  let(:proxy_port) { 80 }
  let(:proxy_protocol) { "http" }
  let(:json_mapping) { "mymapping" }
  let(:delete_local) { false }
  let(:logger) { spy(:logger) }
  let(:proxy_aad_only) { false }

  let(:kusto_ingest_base) { LogStash::Outputs::KustoInternal::KustoIngestConfiguration.new(ingest_url, database, table, json_mapping) }
  let(:kusto_auth_base) { LogStash::Outputs::KustoInternal::KustoAuthConfiguration.new(app_id, app_key, app_tenant, managed_identity, cliauth) }
  let(:kusto_proxy_base) {  LogStash::Outputs::KustoInternal::KustoProxyConfiguration.new(proxy_host , proxy_port , proxy_protocol, false) }

  describe '#initialize' do
    it 'does not throw an error when initializing' do
      # note that this will cause an internal error since connection is being tried.
      # however we still want to test that all the java stuff is working as expected
      expect {
        kustoLogstashOutputConfiguration = described_class.new(kusto_ingest_base, kusto_auth_base , kusto_proxy_base, logger)
        kustoLogstashOutputConfiguration.validate_config()
      }.not_to raise_error
    end
    
    dynamic_name_array = ['/a%{name}/', '/a %{name}/', '/a- %{name}/', '/a- %{name}']

    context 'doesnt allow database to have some dynamic part' do
      dynamic_name_array.each do |test_database|
        it "with database: #{test_database}" do
          expect {
            kusto_ingest = LogStash::Outputs::KustoInternal::KustoIngestConfiguration.new(ingest_url, test_database, table, json_mapping)
            kustoLogstashOutputConfiguration = described_class.new(kusto_ingest, kusto_auth_base , kusto_proxy_base, logger)
            kustoLogstashOutputConfiguration.validate_config()
          }.to raise_error(LogStash::ConfigurationError)          
        end
      end
    end

    context 'doesnt allow table to have some dynamic part' do
      dynamic_name_array.each do |test_table|
        it "with database: #{test_table}" do
          expect {
            kusto_ingest = LogStash::Outputs::KustoInternal::KustoIngestConfiguration.new(ingest_url, database, test_table, json_mapping)
            kustoLogstashOutputConfiguration = described_class.new(kusto_ingest, kusto_auth_base , kusto_proxy_base, logger)
            kustoLogstashOutputConfiguration.validate_config()
          }.to raise_error(LogStash::ConfigurationError)          
        end
      end
    end

    context 'doesnt allow mapping to have some dynamic part' do
      dynamic_name_array.each do |json_mapping|
        it "with database: #{json_mapping}" do
          expect {
            kusto_ingest = LogStash::Outputs::KustoInternal::KustoIngestConfiguration.new(ingest_url, database, table, json_mapping)
            kustoLogstashOutputConfiguration = described_class.new(kusto_ingest, kusto_auth_base , kusto_proxy_base, logger)
            kustoLogstashOutputConfiguration.validate_config()
          }.to raise_error(LogStash::ConfigurationError)          
        end
      end
    end

    context 'proxy protocol has to be http or https' do
      it "with proxy protocol: socks" do
        expect {
          kusto_proxy =  LogStash::Outputs::KustoInternal::KustoProxyConfiguration.new(proxy_host , proxy_port , 'socks', false)
          kustoLogstashOutputConfiguration = described_class.new(kusto_ingest_base, kusto_auth_base , kusto_proxy, logger)
          kustoLogstashOutputConfiguration.validate_config()
        }.to raise_error(LogStash::ConfigurationError)          
      end
    end

    context 'one of appid or managedid or cli_auth has to be provided' do
      it "with empty managed identity and appid" do
        expect {
          kusto_auth = LogStash::Outputs::KustoInternal::KustoAuthConfiguration.new("", app_key, app_tenant, "", false)
          kustoLogstashOutputConfiguration = described_class.new(kusto_ingest_base, kusto_auth , kusto_proxy_base, logger)
          kustoLogstashOutputConfiguration.validate_config()
        }.to raise_error(LogStash::ConfigurationError)          
      end
    end

    context 'if proxy_aad is provided' do
      it "proxy details should be provided" do
        expect {
          kusto_proxy =  LogStash::Outputs::KustoInternal::KustoProxyConfiguration.new("" , "" , proxy_protocol, true)
          kustoLogstashOutputConfiguration = described_class.new(kusto_ingest_base, kusto_auth_base , kusto_proxy, logger)
          kustoLogstashOutputConfiguration.validate_config()
        }.to raise_error(LogStash::ConfigurationError)          
      end
    end
  end
end