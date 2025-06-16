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
  let(:max_items) { 100 }
  let(:plugin_flush_interval) { 10 }
  let(:max_batch_size) { 10 }
  let(:process_failed_batches_on_startup) { false }
  let(:upload_concurrent_count) { 3 }
  let(:upload_queue_size) { 30 }

  let(:kusto_ingest_base) { LogStash::Outputs::KustoInternal::KustoIngestConfiguration.new(ingest_url, database, table, json_mapping) }
  let(:kusto_auth_base) { LogStash::Outputs::KustoInternal::KustoAuthConfiguration.new(app_id, app_key, app_tenant, managed_identity, cliauth) }
  let(:kusto_proxy_base) {  LogStash::Outputs::KustoInternal::KustoProxyConfiguration.new(proxy_host , proxy_port , proxy_protocol, proxy_aad_only) }
  let(:kusto_flush_config) { LogStash::Outputs::KustoInternal::KustoFlushConfiguration.new(max_items, plugin_flush_interval, max_batch_size, process_failed_batches_on_startup) }
  let(:kusto_upload_config) { LogStash::Outputs::KustoInternal::KustoUploadConfiguration.new(upload_concurrent_count, upload_queue_size) }

  describe '#initialize' do
    it 'does not throw an error when initializing' do
      expect {
        kustoLogstashOutputConfiguration = described_class.new(kusto_ingest_base, kusto_auth_base , kusto_proxy_base, kusto_flush_config, kusto_upload_config, logger)
        kustoLogstashOutputConfiguration.validate_config()
      }.not_to raise_error
    end

    it 'exposes all configuration accessors' do
      config = described_class.new(kusto_ingest_base, kusto_auth_base, kusto_proxy_base, kusto_flush_config, kusto_upload_config, logger)
      expect(config.kusto_ingest).to eq(kusto_ingest_base)
      expect(config.kusto_auth).to eq(kusto_auth_base)
      expect(config.kusto_proxy).to eq(kusto_proxy_base)
      expect(config.kusto_flush_config).to eq(kusto_flush_config)
      expect(config.kusto_upload_config).to eq(kusto_upload_config)
    end
  end

  describe LogStash::Outputs::KustoInternal::KustoAuthConfiguration do
    it 'returns correct values for auth config' do
      auth = described_class.new(app_id, app_key, app_tenant, managed_identity, cliauth)
      expect(auth.app_id).to eq(app_id)
      expect(auth.app_key).to eq(app_key)
      expect(auth.app_tenant).to eq(app_tenant)
      expect(auth.managed_identity_id).to eq(managed_identity)
      expect(auth.cli_auth).to eq(cliauth)
    end
  end

  describe LogStash::Outputs::KustoInternal::KustoProxyConfiguration do
    it 'returns correct values for proxy config' do
      proxy = described_class.new(proxy_host, proxy_port, proxy_protocol, proxy_aad_only)
      expect(proxy.proxy_host).to eq(proxy_host)
      expect(proxy.proxy_port).to eq(proxy_port)
      expect(proxy.proxy_protocol).to eq(proxy_protocol)
      expect(proxy.proxy_aad_only).to eq(proxy_aad_only)
      expect(proxy.is_direct_conn).to eq(false)
    end
  end

  describe LogStash::Outputs::KustoInternal::KustoIngestConfiguration do
    it 'returns correct values for ingest config' do
      ingest = described_class.new(ingest_url, database, table, json_mapping)
      expect(ingest.ingest_url).to eq(ingest_url)
      expect(ingest.database).to eq(database)
      expect(ingest.table).to eq(table)
      expect(ingest.json_mapping).to eq(json_mapping)
      expect(ingest.is_mapping_ref_provided).to eq(true)
    end
  end

  describe LogStash::Outputs::KustoInternal::KustoFlushConfiguration do
    it 'returns correct values for flush config' do
      flush = described_class.new(max_items, plugin_flush_interval, max_batch_size, process_failed_batches_on_startup)
      expect(flush.max_items).to eq(max_items)
      expect(flush.plugin_flush_interval).to eq(plugin_flush_interval)
      expect(flush.max_batch_size).to eq(max_batch_size)
      expect(flush.process_failed_batches_on_startup).to eq(process_failed_batches_on_startup)
    end
  end

  describe LogStash::Outputs::KustoInternal::KustoUploadConfiguration do
    it 'returns correct values for upload config' do
      upload = described_class.new(upload_concurrent_count, upload_queue_size)
      expect(upload.upload_concurrent_count).to eq(upload_concurrent_count)
      expect(upload.upload_queue_size).to eq(upload_queue_size)
    end
  end
end