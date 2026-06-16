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
  let(:dynamic_routing) { false }
  let(:logger) { spy('logger') }

  describe '#initialize' do

    it 'does not throw an error when initializing' do
      # note that this will cause an internal error since connection is being tried.
      # however we still want to test that all the java stuff is working as expected
      expect { 
        ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, database, table, json_mapping, dynamic_routing, delete_local, proxy_host, proxy_port,proxy_protocol, logger)
        ingestor.stop
      }.not_to raise_error
    end
    
    dynamic_name_array = ['/a%{name}/', '/a %{name}/', '/a- %{name}/', '/a- %{name}']

    context 'doesnt allow database to have some dynamic part' do
      dynamic_name_array.each do |test_database|
        it "with database: #{test_database}" do
          expect {
            ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, test_database, table, json_mapping, dynamic_routing, delete_local, proxy_host, proxy_port,proxy_protocol,logger)
            ingestor.stop
          }.to raise_error(LogStash::ConfigurationError)          
        end
      end
    end

    context 'doesnt allow table to have some dynamic part' do
      dynamic_name_array.each do |test_table|
        it "with database: #{test_table}" do
          expect {
            ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, database, test_table, json_mapping, dynamic_routing, delete_local, proxy_host, proxy_port,proxy_protocol,logger)
            ingestor.stop
          }.to raise_error(LogStash::ConfigurationError)          
        end
      end
    end

    context 'doesnt allow mapping to have some dynamic part' do
      dynamic_name_array.each do |json_mapping|
        it "with database: #{json_mapping}" do
          expect {
            ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, database, table, json_mapping, dynamic_routing, delete_local, proxy_host, proxy_port,proxy_protocol,logger)
            ingestor.stop
          }.to raise_error(LogStash::ConfigurationError)          
        end
      end
    end

    context 'allows dynamic database/table/mapping when dynamic routing is enabled' do
      dynamic_name_array.each do |dynamic_value|
        it "with dynamic value: #{dynamic_value}" do
          expect {
            ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, dynamic_value, dynamic_value, dynamic_value, true, delete_local, proxy_host, proxy_port, proxy_protocol, logger)
            ingestor.stop
          }.not_to raise_error
        end
      end
    end

    context 'proxy protocol has to be http or https' do
      it "with proxy protocol: socks" do
        expect {
          ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, database, table, json_mapping, dynamic_routing, delete_local, proxy_host, proxy_port,'socks',logger)
          ingestor.stop
        }.to raise_error(LogStash::ConfigurationError)          
      end
    end

    context 'one of appid or managedid has to be provided' do
      it "with empty managed identity and appid" do
        # Use a valid proxy protocol so the failure is attributable to the missing
        # credentials, not to proxy-protocol validation. With no app_id/app_key and
        # no managed identity (and cli_auth disabled) config validation must fail.
        expect {
          ingestor = described_class.new(ingest_url, nil, nil, app_tenant, nil, cliauth, database, table, json_mapping, dynamic_routing, delete_local, proxy_host, proxy_port,'http',logger)
          ingestor.stop
        }.to raise_error(LogStash::ConfigurationError)          
      end
    end

  end

  describe 'vendored streaming fallback policy' do
    it 'queues uncompressed JSON above the SDK estimate and hard size boundaries' do
      policy = Java::com.microsoft.azure.kusto.ingest.ManagedStreamingQueuingPolicy::Default
      json_format =
        Java::com.microsoft.azure.kusto.ingest.IngestionProperties::DataFormat::JSON
      six_mib = 6 * 1024 * 1024
      ten_mib = 10 * 1024 * 1024

      expect(policy.shouldUseQueuedIngestion(six_mib, false, json_format)).to be(false)
      expect(policy.shouldUseQueuedIngestion(six_mib + 1, false, json_format)).to be(true)
      expect(policy.shouldUseQueuedIngestion(ten_mib, true, json_format)).to be(true)
      expect(policy.shouldUseQueuedIngestion(ten_mib + 1, false, json_format)).to be(true)
    end
  end

  describe '#upload with streaming' do
    let(:streaming_client) { double('streaming client') }
    let(:threadpool) { double('threadpool', shutdown: nil, wait_for_termination: nil) }
    let(:sleeper) { double('sleeper', call: nil) }
    let(:streaming_metric) { double('streaming metric', increment: nil) }
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
        dynamic_routing,
        delete_local,
        proxy_host,
        proxy_port,
        proxy_protocol,
        logger,
        threadpool,
        'streaming',
        2,
        0.01,
        streaming_client,
        sleeper,
        streaming_metric
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
          'Streaming request was quarantined after a final non-success status.',
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

    it 'quarantines a mixed status collection instead of trusting the first status' do
      result = double(
        'ingestion result',
        getIngestionStatusCollection: [
          double('succeeded status', status: 'Succeeded'),
          double('failed status', status: 'Failed')
        ]
      )
      allow(streaming_client).to receive(:ingestFromFile).and_return(result)

      with_streaming_file do |path|
        expect(ingestor.upload(path, true)).to eq('PartiallySucceeded')
        quarantine_path = "#{path}.partiallysucceeded"
        expect(File.exist?(quarantine_path)).to be(true)
        File.delete(quarantine_path)
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

    it 'removes a committed batch directory after its final spool file completes' do
      allow(streaming_client).to receive(:ingestFromFile).and_return(ingestion_result('Succeeded'))

      Dir.mktmpdir('kusto-streaming-batch') do |directory|
        batch_directory = File.join(directory, 'batch-completed.ready')
        Dir.mkdir(batch_directory)
        first_path = File.join(
          batch_directory,
          'stream-000000-11111111-1111-1111-1111-111111111111.json'
        )
        second_path = File.join(
          batch_directory,
          'stream-000001-22222222-2222-2222-2222-222222222222.json'
        )
        File.write(first_path, "{\"id\":1}\n")
        File.write(second_path, "{\"id\":2}\n")

        expect(ingestor.upload(first_path, true)).to eq('Succeeded')
        expect(Dir.exist?(batch_directory)).to be(true)
        expect(ingestor.upload(second_path, true)).to eq('Succeeded')
        expect(Dir.exist?(batch_directory)).to be(false)
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

    it 'applies backpressure and starts another retry cycle after transient retries are exhausted' do
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

      with_streaming_file do |path|
        expect(ingestor.upload(path, true)).to eq('Succeeded')
        expect(File.exist?(path)).to be(false)
      end
      expect(streaming_client).to have_received(:ingestFromFile).exactly(4).times
      expect(sleeper).to have_received(:call).with(0.01).once
      expect(sleeper).to have_received(:call).with(0.02).once
      expect(sleeper).to have_received(:call).with(0.04).once
      expect(streaming_metric).to have_received(:increment).with(:retry_cycles)
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

    it 'logs a missing spool file without allowing the error to escape the upload task' do
      file = Tempfile.new(['missing-kusto-streaming', '.json'])
      path = file.path
      file.close!

      expect(streaming_client).not_to receive(:ingestFromFile)
      expect { ingestor.upload(path, true) }.not_to raise_error
      expect(logger).to have_received(:error).with(
        "File doesn't exist! Unrecoverable error.",
        hash_including(exception: Errno::ENOENT, path: path)
      )
    end

    it 'ingests synchronously when the streaming executor rejects a committed spool file' do
      allow(threadpool).to receive(:remaining_capacity).and_return(10)
      allow(threadpool).to receive(:post).and_raise(Concurrent::RejectedExecutionError)
      allow(streaming_client).to receive(:ingestFromFile).and_return(ingestion_result('Succeeded'))

      with_streaming_file do |path|
        expect(ingestor.upload_async(path, true)).to eq('Succeeded')
        expect(File.exist?(path)).to be(false)
      end
      expect(logger).to have_received(:error).with(
        'Failed to enqueue Kusto ingestion.',
        hash_including(path: kind_of(String))
      )
      expect(logger).to have_received(:warn).with(
        'Streaming executor rejected a request; ingesting synchronously for backpressure.',
        hash_including(path: kind_of(String))
      )
    end

    it 'preserves queued-mode enqueue failure behavior' do
      allow(threadpool).to receive(:remaining_capacity).and_return(10)
      allow(threadpool).to receive(:post).and_raise(Concurrent::RejectedExecutionError)
      queued_ingestor = described_class.new(
        ingest_url,
        app_id,
        app_key,
        app_tenant,
        managed_identity,
        cliauth,
        database,
        table,
        json_mapping,
        dynamic_routing,
        delete_local,
        proxy_host,
        proxy_port,
        proxy_protocol,
        logger,
        threadpool,
        'queued',
        2,
        1,
        streaming_client
      )

      expect do
        queued_ingestor.upload_async('/queued/request.json', true)
      end.to raise_error(Concurrent::RejectedExecutionError)
    end

    it 'drains the worker pool and closes the streaming client on stop' do
      allow(streaming_client).to receive(:close)

      ingestor.stop

      expect(streaming_client).to have_received(:close)
      expect(threadpool).to have_received(:shutdown)
      expect(threadpool).to have_received(:wait_for_termination).with(nil)
    end

    it 'interrupts an in-flight retry backoff during shutdown' do
      transient_error =
        Java::com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException.new('transient')
      attempted = Queue.new
      allow(streaming_client).to receive(:ingestFromFile) do
        attempted << true
        raise transient_error
      end
      allow(streaming_client).to receive(:close)
      interruptible = described_class.new(
        ingest_url,
        app_id,
        app_key,
        app_tenant,
        managed_identity,
        cliauth,
        database,
        table,
        json_mapping,
        dynamic_routing,
        delete_local,
        proxy_host,
        proxy_port,
        proxy_protocol,
        logger,
        threadpool,
        'streaming',
        2,
        30,
        streaming_client,
        nil,
        streaming_metric
      )

      with_streaming_file do |path|
        upload_thread = Thread.new { interruptible.upload(path, true) }
        attempted.pop
        interruptible.stop

        expect(upload_thread.join(1)).to eq(upload_thread)
        expect(File.exist?(path)).to be(true)
      end
    end

    it 'skips completion directory fsync on Windows' do
      allocated = described_class.allocate
      allow(Gem).to receive(:win_platform?).and_return(true)
      expect(File).not_to receive(:open)

      expect { allocated.send(:fsync_directory, 'C:/spool') }.not_to raise_error
    end
  end

  describe 'streaming client creation' do
    it 'uses the SDK streaming factory and lets the SDK derive both endpoints' do
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
        dynamic_routing,
        delete_local,
        nil,
        proxy_port,
        proxy_protocol,
        logger,
        threadpool,
        'streaming'
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
        dynamic_routing,
        delete_local,
        proxy_host,
        proxy_port,
        proxy_protocol,
        logger,
        threadpool,
        'streaming'
      )
      ingestor.stop

      expect(factory).to have_received(:createManagedStreamingIngestClient)
        .with(anything, anything, true)
    end
  end

  describe '#decode_routing_target' do
    let(:ingestor) do
      described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, "%{db}", "%{table}", "%{mapping}", true, delete_local, proxy_host, proxy_port, proxy_protocol, logger)
    end

    after(:each) { ingestor.stop }

    it 'decodes database, table and mapping from the file name' do
      target = ingestor.decode_routing_target("/tmp/kusto/2024-01-01.kusto~mydb~mytable~mymapping")
      expect(target[:database]).to eq('mydb')
      expect(target[:table]).to eq('mytable')
      expect(target[:mapping]).to eq('mymapping')
    end

    it 'treats an empty mapping segment as no mapping' do
      target = ingestor.decode_routing_target("/tmp/kusto/2024-01-01.kusto~mydb~mytable~")
      expect(target[:database]).to eq('mydb')
      expect(target[:table]).to eq('mytable')
      expect(target[:mapping]).to be_nil
    end

    it 'is unaffected by dots in the path prefix' do
      target = ingestor.decode_routing_target("/tmp/kusto/2024.01.01-10.30.kusto~mydb~mytable~mymapping")
      expect(target[:database]).to eq('mydb')
      expect(target[:table]).to eq('mytable')
      expect(target[:mapping]).to eq('mymapping')
    end

    it 'returns nil when the routing marker is absent (dead-letter file)' do
      expect(ingestor.decode_routing_target("/tmp/kusto/_filepath_failures")).to be_nil
    end

    it 'returns nil when database or table did not resolve' do
      expect(ingestor.decode_routing_target("/tmp/kusto/2024-01-01.kusto~mydb~")).to be_nil
    end

    it 'treats an unresolved mapping field reference as no mapping (mapping is optional)' do
      target = ingestor.decode_routing_target("/tmp/kusto/2024-01-01.kusto~mydb~mytable~%{[@metadata][mapping]}")
      expect(target[:database]).to eq('mydb')
      expect(target[:table]).to eq('mytable')
      expect(target[:mapping]).to be_nil
    end

    it 'returns nil when the mapping decoded to a genuinely invalid value' do
      # A resolved-but-invalid mapping (here a path separator) must not be silently
      # dropped: the event is unroutable rather than ingested with the wrong mapping.
      expect(ingestor.decode_routing_target("/tmp/kusto/2024-01-01.kusto~mydb~mytable~bad%2Fmapping")).to be_nil
    end

    it 'round-trips database/table/mapping containing dots and spaces' do
      db = LogStash::Outputs::Kusto.encode_routing_segment('Security.Events')
      table = LogStash::Outputs::Kusto.encode_routing_segment('App Logs')
      mapping = LogStash::Outputs::Kusto.encode_routing_segment('My.Mapping')
      # Encoded segments contain no '.', '~' or '/'.
      [db, table, mapping].each do |seg|
        expect(seg).not_to include('.')
        expect(seg).not_to include('~')
        expect(seg).not_to include('/')
      end
      target = ingestor.decode_routing_target("/tmp/a.b.c-12.34.kusto~#{db}~#{table}~#{mapping}")
      expect(target[:database]).to eq('Security.Events')
      expect(target[:table]).to eq('App Logs')
      expect(target[:mapping]).to eq('My.Mapping')
    end

    it 'is not fooled by a database value that itself ends in the marker text' do
      # "a.kusto" must not create a spurious marker; encoding removes the dot.
      seg = LogStash::Outputs::Kusto.encode_routing_segment('a.kusto')
      target = ingestor.decode_routing_target("/tmp/2024-01-01.kusto~#{seg}~mytable~")
      expect(target[:database]).to eq('a.kusto')
      expect(target[:table]).to eq('mytable')
    end
  end

  describe '#ingestion_properties_for' do
    let(:ingestor) do
      described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, "%{db}", "%{table}", "%{mapping}", true, delete_local, proxy_host, proxy_port, proxy_protocol, logger)
    end

    after(:each) { ingestor.stop }

    it 'returns nil for a file without a decodable routing target (upload is skipped)' do
      expect(ingestor.ingestion_properties_for("/tmp/kusto/_filepath_failures")).to be_nil
    end

    it 'deletes an undecodable dynamic file on upload instead of leaving it for infinite retry' do
      require 'tmpdir'
      Dir.mktmpdir do |dir|
        # Marker present but only one segment -> decode returns nil (unroutable).
        path = File.join(dir, '2024-01-01.kusto~onlydb')
        File.write(path, '{"a":1}')
        ingestor.upload(path, true)
        expect(File.exist?(path)).to be false
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
