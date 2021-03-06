# encoding: utf-8
require_relative "../../spec_helpers.rb"
require 'logstash/outputs/kusto'
require 'logstash/outputs/kusto/ingestor'

describe LogStash::Outputs::Kusto::Ingestor do

  let(:ingest_url) { "https://ingest-sdkse2etest.eastus.kusto.windows.net/" }
  let(:app_id) { "myid" }
  let(:app_key) { LogStash::Util::Password.new("mykey") }
  let(:app_tenant) { "mytenant" }
  let(:database) { "mydatabase" }
  let(:table) { "mytable" }
  let(:json_mapping) { "mymapping" }
  let(:delete_local) { false }
  let(:logger) { spy('logger') }

  describe '#initialize' do

    it 'does not throw an error when initializing' do
      # note that this will cause an internal error since connection is being tried.
      # however we still want to test that all the java stuff is working as expected
      expect { 
        ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, database, table, json_mapping, delete_local, logger)
        ingestor.stop
      }.not_to raise_error
    end
    
    dynamic_name_array = ['/a%{name}/', '/a %{name}/', '/a- %{name}/', '/a- %{name}']

    context 'doesnt allow database to have some dynamic part' do
      dynamic_name_array.each do |test_database|
        it "with database: #{test_database}" do
          expect {
            ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, test_database, table, json_mapping, delete_local, logger)
            ingestor.stop
          }.to raise_error(LogStash::ConfigurationError)          
        end
      end
    end

    context 'doesnt allow table to have some dynamic part' do
      dynamic_name_array.each do |test_table|
        it "with database: #{test_table}" do
          expect {
            ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, database, test_table, json_mapping, delete_local, logger)
            ingestor.stop
          }.to raise_error(LogStash::ConfigurationError)          
        end
      end
    end

    context 'doesnt allow mapping to have some dynamic part' do
      dynamic_name_array.each do |test_json_mapping|
        it "with database: #{test_json_mapping}" do
          expect {
            ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, database, table, test_json_mapping, delete_local, logger)
            ingestor.stop
          }.to raise_error(LogStash::ConfigurationError)          
        end
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
