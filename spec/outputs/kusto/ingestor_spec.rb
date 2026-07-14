# encoding: utf-8
require_relative "../../spec_helpers.rb"
require 'logstash/outputs/kusto'
require 'logstash/outputs/kusto/ingestor'
require 'tempfile'

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
  let(:delete_local) { false }
  let(:logger) { spy('logger') }

  describe '#initialize' do

    it 'does not throw an error when initializing' do
      # note that this will cause an internal error since connection is being tried.
      # however we still want to test that all the java stuff is working as expected
      expect { 
        ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, database, table, json_mapping, delete_local, proxy_host, proxy_port,proxy_protocol, logger)
        ingestor.stop
      }.not_to raise_error
    end
    
    dynamic_name_array = ['/a%{name}/', '/a %{name}/', '/a- %{name}/', '/a- %{name}']

    context 'doesnt allow database to have some dynamic part' do
      dynamic_name_array.each do |test_database|
        it "with database: #{test_database}" do
          expect {
            ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, test_database, table, json_mapping, delete_local, proxy_host, proxy_port,proxy_protocol,logger)
            ingestor.stop
          }.to raise_error(LogStash::ConfigurationError)          
        end
      end
    end

    context 'doesnt allow table to have some dynamic part' do
      dynamic_name_array.each do |test_table|
        it "with database: #{test_table}" do
          expect {
            ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, database, test_table, json_mapping, delete_local, proxy_host, proxy_port,proxy_protocol,logger)
            ingestor.stop
          }.to raise_error(LogStash::ConfigurationError)          
        end
      end
    end

    context 'doesnt allow mapping to have some dynamic part' do
      dynamic_name_array.each do |json_mapping|
        it "with database: #{json_mapping}" do
          expect {
            ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, database, table, json_mapping, delete_local, proxy_host, proxy_port,proxy_protocol,logger)
            ingestor.stop
          }.to raise_error(LogStash::ConfigurationError)          
        end
      end
    end

    context 'proxy protocol has to be http or https' do
      it "with proxy protocol: socks" do
        expect {
          ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, database, table, json_mapping, delete_local, proxy_host, proxy_port,'socks',logger)
          ingestor.stop
        }.to raise_error(LogStash::ConfigurationError)          
      end
    end

    context 'one of appid or managedid has to be provided' do
      it "with empty managed identity and appid" do
        expect {
          ingestor = described_class.new(ingest_url, "", app_key, app_tenant, "", cliauth, database, table, json_mapping, delete_local, proxy_host, proxy_port,'socks',logger)
          ingestor.stop
        }.to raise_error(LogStash::ConfigurationError)          
      end
    end

  end

  describe '#upload with managed streaming' do
    let(:streaming_client) { double('managed streaming client') }
    let(:threadpool) { double('threadpool', shutdown: nil, wait_for_termination: nil) }
    let(:sleeper) { double('sleeper', call: nil) }
    let(:streaming_metric) { double('streaming metric', increment: nil) }
    let(:scheduled_retries) { [] }
    let(:scheduler) do
      lambda do |delay, &task|
        scheduled_task = double('scheduled task', complete?: false, cancel: nil)
        scheduled_retries << { delay: delay, task: task, scheduled_task: scheduled_task }
        scheduled_task
      end
    end
    let(:ingestor) do
      described_class.new(
        ingest_url,
        app_id,
        app_key,
        app_tenant,
        managed_identity,
        cliauth,
        database,
        table,
        json_mapping,
        delete_local,
        proxy_host,
        proxy_port,
        proxy_protocol,
        logger,
        threadpool,
        'managed_streaming',
        2,
        0.01,
        streaming_client,
        sleeper,
        streaming_metric,
        scheduler
      )
    end

    def with_streaming_file(payload = "{\"id\":1}\n")
      file = Tempfile.new(['kusto-streaming', '.json'])
      file.binmode
      file.write(payload)
      file.close
      yield file.path
    ensure
      file&.unlink
    end

    def ingestion_result(status)
      double(
        'ingestion result',
        getIngestionStatusCollection: [
          double('ingestion status', status: status, details: nil, errorCode: nil)
        ]
      )
    end

    %w[Succeeded Queued Pending].each do |status|
      it "accepts #{status} and deletes the completed spool file" do
        allow(streaming_client).to receive(:ingestFromFile).and_return(ingestion_result(status))

        with_streaming_file do |path|
          expect(ingestor.upload(path, true)).to eq(status)
          expect(File.exist?(path)).to be(false)
        end
      end
    end

    %w[Skipped PartiallySucceeded].each do |status|
      it "quarantines final #{status} without replaying the whole file" do
        allow(streaming_client).to receive(:ingestFromFile).and_return(ingestion_result(status))

        with_streaming_file do |path|
          expect(ingestor.upload(path, true)).to eq(status)
          expect(File.exist?(path)).to be(false)
          quarantine_path = "#{path}.#{status.downcase}"
          expect(File.exist?(quarantine_path)).to be(true)
          File.delete(quarantine_path)
        end
        expect(logger).to have_received(:warn)
        expect(logger).to have_received(:error).with(
          'Managed streaming request was quarantined after a final non-success status.',
          hash_including(status: status)
        )
        expect(streaming_client).to have_received(:ingestFromFile).once
      end
    end

    it 'retains the spool file when Kusto returns Failed' do
      allow(streaming_client).to receive(:ingestFromFile).and_return(ingestion_result('Failed'))

      with_streaming_file do |path|
        expect(ingestor.upload(path, true)).to be_nil
        expect(File.exist?(path)).to be(true)
      end
      expect(streaming_metric).to have_received(:increment).with(:failures)
    end

    it 'retains the spool file when the SDK returns no status' do
      result = double('ingestion result', getIngestionStatusCollection: [])
      allow(streaming_client).to receive(:ingestFromFile).and_return(result)

      with_streaming_file do |path|
        expect(ingestor.upload(path, true)).to be_nil
        expect(File.exist?(path)).to be(true)
      end
    end

    it 'passes the complete spool file and a source id to the SDK' do
      payload = "{\"id\":1}\n{\"id\":2}\n"
      captured_path = nil
      captured_source_id = nil
      allow(streaming_client).to receive(:ingestFromFile) do |source, _properties|
        captured_path = source.getFilePath
        captured_source_id = source.getSourceId
        ingestion_result('Succeeded')
      end

      with_streaming_file(payload) do |path|
        ingestor.upload(path, false)
        expect(captured_path).to eq(path)
      end
      expect(captured_source_id).not_to be_nil
    end

    it 'reuses the source id when a spool file is recovered' do
      source_ids = []
      results = [ingestion_result('Failed'), ingestion_result('Succeeded')]
      allow(streaming_client).to receive(:ingestFromFile) do |source, _properties|
        source_ids << source.getSourceId.to_s
        results.shift
      end

      with_streaming_file do |path|
        ingestor.upload(path, true)
        ingestor.upload(path, false)
        File.delete("#{path}.completed")
      end

      expect(source_ids.uniq.length).to eq(1)
    end

    it 'marks accepted files completed when debug retention is enabled' do
      allow(streaming_client).to receive(:ingestFromFile).and_return(ingestion_result('Succeeded'))

      with_streaming_file do |path|
        expect(ingestor.upload(path, false)).to eq('Succeeded')
        expect(File.exist?(path)).to be(false)
        expect(File.exist?("#{path}.completed")).to be(true)
        File.delete("#{path}.completed")
      end
    end

    it 'retries transient service failures with bounded backoff and a stable source id' do
      transient_error =
        Java::com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException.new('transient')
      attempts = 0
      source_ids = []
      allow(streaming_client).to receive(:ingestFromFile) do |source, _properties|
        attempts += 1
        source_ids << source.getSourceId.to_s
        raise transient_error if attempts <= 2

        ingestion_result('Succeeded')
      end

      with_streaming_file do |path|
        expect(ingestor.upload(path, true)).to eq('Succeeded')
      end
      expect(sleeper).to have_received(:call).with(0.01).ordered
      expect(sleeper).to have_received(:call).with(0.02).ordered
      expect(source_ids.uniq.length).to eq(1)
    end

    it 'does not retry client failures and retains the spool file' do
      client_error =
        Java::com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException.new('permanent')
      allow(streaming_client).to receive(:ingestFromFile).and_raise(client_error)

      with_streaming_file do |path|
        expect(ingestor.upload(path, true)).to be_nil
        expect(File.exist?(path)).to be(true)
      end
      expect(streaming_client).to have_received(:ingestFromFile).once
      expect(sleeper).not_to have_received(:call)
    end

    it 'does not retry permanent service failures and retains the spool file' do
      data_error =
        Java::com.microsoft.azure.kusto.data.exceptions.DataServiceException.new(
          'activity-id',
          'permanent',
          true
        )
      service_error =
        Java::com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException.new(
          'permanent',
          data_error
        )
      allow(streaming_client).to receive(:ingestFromFile).and_raise(service_error)

      with_streaming_file do |path|
        expect(ingestor.upload(path, true)).to be_nil
        expect(File.exist?(path)).to be(true)
      end
      expect(streaming_client).to have_received(:ingestFromFile).once
      expect(sleeper).not_to have_received(:call)
    end

    it 'retains the spool file after transient retry attempts are exhausted' do
      transient_error =
        Java::com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException.new('transient')
      allow(streaming_client).to receive(:ingestFromFile).and_raise(transient_error)

      with_streaming_file do |path|
        expect(ingestor.upload(path, true)).to be_nil
        expect(File.exist?(path)).to be(true)
      end
      expect(streaming_client).to have_received(:ingestFromFile).exactly(3).times
      expect(scheduled_retries.map { |retry_item| retry_item[:delay] }).to eq([0.04])
    end

    it 'requeues an exhausted transient failure without changing its source id' do
      transient_error =
        Java::com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException.new('transient')
      attempts = 0
      source_ids = []
      allow(streaming_client).to receive(:ingestFromFile) do |source, _properties|
        attempts += 1
        source_ids << source.getSourceId.to_s
        raise transient_error if attempts <= 3

        ingestion_result('Succeeded')
      end
      allow(threadpool).to receive(:remaining_capacity).and_return(10)
      allow(threadpool).to receive(:post).and_yield

      with_streaming_file do |path|
        expect(ingestor.upload(path, true)).to be_nil
        expect(File.exist?(path)).to be(true)

        scheduled_retries.fetch(0).fetch(:task).call

        expect(File.exist?(path)).to be(false)
      end
      expect(streaming_client).to have_received(:ingestFromFile).exactly(4).times
      expect(source_ids.uniq.length).to eq(1)
    end

    it 'retains the spool file for unexpected SDK failures' do
      allow(streaming_client).to receive(:ingestFromFile).and_raise(StandardError, 'unexpected')

      with_streaming_file do |path|
        expect(ingestor.upload(path, true)).to be_nil
        expect(File.exist?(path)).to be(true)
      end
      expect(streaming_metric).to have_received(:increment).with(:failures)
    end

    it 'drains the worker pool and closes the managed streaming client on stop' do
      allow(streaming_client).to receive(:close)
      scheduled_task = double('scheduled task', complete?: false, cancel: nil)
      scheduled_retries << {
        delay: 1,
        task: -> {},
        scheduled_task: scheduled_task
      }
      ingestor.instance_variable_get(:@scheduled_retries) << scheduled_task

      ingestor.stop

      expect(streaming_client).to have_received(:close)
      expect(scheduled_task).to have_received(:cancel)
      expect(threadpool).to have_received(:shutdown)
      expect(threadpool).to have_received(:wait_for_termination).with(nil)
    end
  end

  describe 'managed streaming client creation' do
    it 'uses the managed streaming factory and lets the SDK derive both endpoints' do
      factory = Java::com.microsoft.azure.kusto.ingest.IngestClientFactory
      client = double('managed client', close: nil)
      allow(factory).to receive(:createManagedStreamingIngestClient).and_return(client)
      threadpool = double('threadpool', shutdown: nil, wait_for_termination: nil)

      ingestor = described_class.new(
        ingest_url,
        app_id,
        app_key,
        app_tenant,
        managed_identity,
        cliauth,
        database,
        table,
        json_mapping,
        delete_local,
        nil,
        proxy_port,
        proxy_protocol,
        logger,
        threadpool,
        'managed_streaming'
      )
      ingestor.stop

      expect(factory).to have_received(:createManagedStreamingIngestClient).once
    end

    it 'enables endpoint correction when creating a managed client with a proxy' do
      factory = Java::com.microsoft.azure.kusto.ingest.IngestClientFactory
      client = double('managed client', close: nil)
      allow(factory).to receive(:createManagedStreamingIngestClient).and_return(client)
      threadpool = double('threadpool', shutdown: nil, wait_for_termination: nil)

      ingestor = described_class.new(
        ingest_url,
        app_id,
        app_key,
        app_tenant,
        managed_identity,
        cliauth,
        database,
        table,
        json_mapping,
        delete_local,
        proxy_host,
        proxy_port,
        proxy_protocol,
        logger,
        threadpool,
        'managed_streaming'
      )
      ingestor.stop

      expect(factory).to have_received(:createManagedStreamingIngestClient)
        .with(anything, anything, true)
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
