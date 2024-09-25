# encoding: utf-8
require_relative "../../spec_helpers.rb"
require 'logstash/outputs/kusto'
require 'logstash/outputs/kusto/ingestor'

describe LogStash::Outputs::Kusto::Ingestor do

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
  let(:logger) { spy('logger') }

  describe 'Ingestor' do

    it 'does not throw an error when initializing' do
      RSpec.configuration.reporter.message("Running test: does not throw an error when initializing")
      expect { 
        ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, database, table, json_mapping, proxy_host, proxy_port, proxy_protocol, logger)
        ingestor.stop
      }.not_to raise_error
      RSpec.configuration.reporter.message("Completed test: does not throw an error when initializing")
    end
  
    dynamic_name_array = ['/a%{name}/', '/a %{name}/', '/a- %{name}/', '/a- %{name}']

    context 'doesnt allow database to have some dynamic part' do
      dynamic_name_array.each do |test_database|
        it "with database: #{test_database}" do
          RSpec.configuration.reporter.message("Running test: doesnt allow database to have some dynamic part with database: #{test_database}")
          expect {
            ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, test_database, table, json_mapping, proxy_host, proxy_port, proxy_protocol, logger)
            ingestor.stop
          }.to raise_error(LogStash::ConfigurationError)
          RSpec.configuration.reporter.message("Completed test: doesnt allow database to have some dynamic part with database: #{test_database}")
        end
      end
    end

    context 'doesnt allow table to have some dynamic part' do
      dynamic_name_array.each do |test_table|
        it "with table: #{test_table}" do
          RSpec.configuration.reporter.message("Running test: doesnt allow table to have some dynamic part with table: #{test_table}")
          expect {
            ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, database, test_table, json_mapping, proxy_host, proxy_port, proxy_protocol, logger)
            ingestor.stop
          }.to raise_error(LogStash::ConfigurationError)
          RSpec.configuration.reporter.message("Completed test: doesnt allow table to have some dynamic part with table: #{test_table}")
        end
      end
    end

    context 'doesnt allow mapping to have some dynamic part' do
      dynamic_name_array.each do |json_mapping|
        it "with mapping: #{json_mapping}" do
          RSpec.configuration.reporter.message("Running test: doesnt allow mapping to have some dynamic part with mapping: #{json_mapping}")
          expect {
            ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, database, table, json_mapping, proxy_host, proxy_port, proxy_protocol, logger)
            ingestor.stop
          }.to raise_error(LogStash::ConfigurationError)
          RSpec.configuration.reporter.message("Completed test: doesnt allow mapping to have some dynamic part with mapping: #{json_mapping}")
        end
      end
    end

    context 'proxy protocol has to be http or https' do
      it "with proxy protocol: socks" do
        RSpec.configuration.reporter.message("Running test: proxy protocol has to be http or https with proxy protocol: socks")
        expect {
          ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, database, table, json_mapping, proxy_host, proxy_port, 'socks', logger)
          ingestor.stop
        }.to raise_error(LogStash::ConfigurationError)
        RSpec.configuration.reporter.message("Completed test: proxy protocol has to be http or https with proxy protocol: socks")
      end
    end

    context 'one of appid or managedid has to be provided' do
      it "with empty managed identity and appid" do
        RSpec.configuration.reporter.message("Running test: one of appid or managedid has to be provided with empty managed identity and appid")
        expect {
          ingestor = described_class.new(ingest_url, "", app_key, app_tenant, "", cliauth, database, table, json_mapping, proxy_host, proxy_port,'socks',logger)
          ingestor.stop
        }.to raise_error(LogStash::ConfigurationError)
        RSpec.configuration.reporter.message("Completed test: one of appid or managedid has to be provided with empty managed identity and appid")
      end
    end

  end

  # describe 'receiving events' do

  #   context 'with non-zero flush interval' do
  #     let(:temporary_output_file) { Stud::Temporary.pathname }

  #     let(:event_count) { 100 }
  #     let(:flush_interval) { 5 }

  #     let(:events) do
  #       event_count.times.map do |idx|
  #         LogStash::Event.new('subject' => idx)
  #       end
  #     end

  #     let(:output) { described_class.new(options.merge( {'path' => temporary_output_file, 'flush_interval' => flush_interval, 'delete_temp_files' => false } )) }

  #     before(:each) { output.register }
      
  #     after(:each) do
  #       output.close
  #       File.exist?(temporary_output_file) && File.unlink(temporary_output_file)
  #       File.exist?(temporary_output_file + '.kusto') && File.unlink(temporary_output_file + '.kusto')
  #     end

  #     it 'eventually flushes without receiving additional events' do
  #       output.multi_receive_encoded(events)

  #       # events should not all be flushed just yet...
  #       expect(File.read(temporary_output_file)).to satisfy("have less than #{event_count} lines") do |contents|
  #         contents && contents.lines.count < event_count
  #       end

  #       # wait for the flusher to run...
  #       sleep(flush_interval + 1)

  #       # events should all be flushed
  #       expect(File.read(temporary_output_file)).to satisfy("have exactly #{event_count} lines") do |contents|
  #         contents && contents.lines.count == event_count
  #       end
  #     end
  #   end

  # end
end
