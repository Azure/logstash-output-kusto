# encoding: utf-8
require_relative '../spec_helpers'
require 'logstash/outputs/kusto'
require 'logstash/codecs/plain'
require 'logstash/event'
require 'tmpdir'

describe LogStash::Outputs::Kusto do

  let(:options) { { "path" => "./kusto_tst/%{+YYYY-MM-dd-HH-mm}",
    "ingest_url" => "https://ingest-sdkse2etest.eastus.kusto.windows.net/",
    "app_id" => "myid",
    "app_key" => "mykey",
    "app_tenant" => "mytenant",
    "database" => "mydatabase",
    "table" => "mytable",
    "json_mapping" => "mymapping",
    "flush_interval" => 0,
    "proxy_host" => "localhost",
    "proxy_port" => 3128,
    "proxy_protocol" => "https"
  } }

  # Shared dynamic-routing config (database/table/mapping are field references).
  let(:dynamic_options) { options.merge(
    'path' => './kusto_tst/%{+YYYY-MM-dd-HH-mm}',
    'table' => '%{[@metadata][table]}',
    'database' => '%{[@metadata][database]}',
    'json_mapping' => '%{[@metadata][mapping]}'
  ) }

  describe '#register' do
    it 'defaults ingestion mode to queued' do
      kusto = described_class.new(options)

      expect(kusto.ingestion_mode).to eq('queued')
    end

    it 'requires the explicit streaming mode name' do
      expect do
        described_class.new(options.merge('ingestion_mode' => 'managed_streaming'))
      end.to raise_error(LogStash::ConfigurationError)
    end

    [
      { 'database' => '%{database}' },
      { 'table' => '%{table}' },
      { 'json_mapping' => '%{mapping}' },
      { 'json_mapping' => nil, 'mapping' => '%{mapping}' },
      { 'dynamic_event_routing' => true }
    ].each do |routing_settings|
      it "rejects streaming with dynamic routing settings #{routing_settings.inspect}" do
        Dir.mktmpdir('kusto-dynamic-streaming') do |directory|
          kusto = described_class.new(options.merge(routing_settings).merge(
            'ingestion_mode' => 'streaming',
            'streaming_temp_directory' => directory
          ))
          expect(LogStash::Outputs::Kusto::Ingestor).not_to receive(:new)

          expect { kusto.register }.to raise_error(
            LogStash::ConfigurationError,
            'Dynamic event routing is only supported with ingestion_mode => queued.'
          )
          expect(Dir.children(directory)).to be_empty
        end
      end
    end

    it 'doesnt allow the path to start with a dynamic string' do
      kusto = described_class.new(options.merge( {'path' => '/%{name}'} ))
      expect { kusto.register }.to raise_error(LogStash::ConfigurationError)
      kusto.close
    end

    it 'path must include a dynamic string to allow file rotation' do
      kusto = described_class.new(options.merge( {'path' => '/{name}'} ))
      expect { kusto.register }.to raise_error(LogStash::ConfigurationError)
      kusto.close
    end


    dynamic_name_array = ['/a%{name}/', '/a %{name}/', '/a- %{name}/', '/a- %{name}']

    context 'doesnt allow the root directory to have some dynamic part' do
      dynamic_name_array.each do |test_path|
         it "with path: #{test_path}" do
           kusto = described_class.new(options.merge( {'path' => test_path} ))
           expect { kusto.register }.to raise_error(LogStash::ConfigurationError)
           kusto.close
         end
       end
    end

    it 'allow to have dynamic part after the file root' do
      kusto = described_class.new(options.merge({'path' => '/tmp/%{name}'}))
      expect { kusto.register }.not_to raise_error
      kusto.close
    end

    it 'requires path for queued ingestion' do
      queued_options = options.reject { |key, _| key == 'path' }
      kusto = described_class.new(queued_options)

      expect { kusto.register }.to raise_error(LogStash::ConfigurationError, /path/)
    end

    it 'does not require path for streaming ingestion' do
      Dir.mktmpdir('kusto-streaming') do |directory|
        streaming_options = options
          .reject { |key, _| key == 'path' }
          .merge(
            'ingestion_mode' => 'streaming',
            'streaming_temp_directory' => directory
          )
        ingestor = instance_double(LogStash::Outputs::Kusto::Ingestor, stop: nil)
        allow(LogStash::Outputs::Kusto::Ingestor).to receive(:new).and_return(ingestor)
        kusto = described_class.new(streaming_options)

        expect { kusto.register }.not_to raise_error
        kusto.close
      end
    end

    it 'uses a private destination spool below path.data by default' do
      Dir.mktmpdir('kusto-path-data') do |path_data|
        allow(LogStash::SETTINGS).to receive(:get).and_call_original
        allow(LogStash::SETTINGS).to receive(:get).with('path.data').and_return(path_data)
        ingestor = instance_double(LogStash::Outputs::Kusto::Ingestor, stop: nil)
        allow(LogStash::Outputs::Kusto::Ingestor).to receive(:new).and_return(ingestor)
        kusto = described_class.new(
          options.reject { |key, _| key == 'path' }.merge(
            'ingestion_mode' => 'streaming'
          )
        )

        kusto.register

        spool = kusto.instance_variable_get(:@streaming_temp_directory)
        expect(spool).to start_with(File.realpath(path_data))
        expect(File.stat(spool).mode & 0o777).to eq(0o700)
        kusto.close
      end
    end

    it 'rejects unsafe streaming file and directory modes' do
      {
        'dir_mode' => 0o777,
        'file_mode' => 0o666
      }.each do |setting, mode|
        Dir.mktmpdir('kusto-unsafe-mode') do |directory|
          kusto = described_class.new(options.merge(
            'ingestion_mode' => 'streaming',
            'streaming_temp_directory' => directory,
            setting => mode
          ))

          expect { kusto.register }.to raise_error(LogStash::ConfigurationError, /must not allow/)
        end
      end
    end

    it 'rejects a symlink as the streaming spool directory' do
      skip 'Windows symlink creation requires elevated privileges' if Gem.win_platform?

      Dir.mktmpdir('kusto-streaming-symlink') do |directory|
        target = File.join(directory, 'target')
        link = File.join(directory, 'link')
        Dir.mkdir(target)
        File.symlink(target, link)
        kusto = described_class.new(options.merge(
          'ingestion_mode' => 'streaming',
          'streaming_temp_directory' => link
        ))

        expect { kusto.register }.to raise_error(LogStash::ConfigurationError, /must not be a symlink/)
      end
    end

    it 'rejects a spool below a non-sticky world-writable parent' do
      skip 'POSIX permission validation is not available on Windows' if Gem.win_platform?

      Dir.mktmpdir('kusto-streaming-parent') do |directory|
        unsafe_parent = File.join(directory, 'unsafe')
        Dir.mkdir(unsafe_parent, 0o777)
        File.chmod(0o777, unsafe_parent)
        spool = File.join(unsafe_parent, 'spool')
        kusto = described_class.new(options.merge(
          'ingestion_mode' => 'streaming',
          'streaming_temp_directory' => spool
        ))

        expect { kusto.register }.to raise_error(
          LogStash::ConfigurationError,
          /writable by untrusted users/
        )
      end
    end

    it 'skips unsupported directory fsync on Windows' do
      output = described_class.allocate
      allow(Gem).to receive(:win_platform?).and_return(true)
      expect(File).not_to receive(:open)

      expect { output.send(:fsync_directory, 'C:/spool') }.not_to raise_error
    end

    it 'recovers streaming spool files after restart' do
      Dir.mktmpdir('kusto-streaming-recovery') do |directory|
        batch_directory = File.join(directory, 'batch-existing.ready')
        FileUtils.mkdir_p(batch_directory)
        recovered_file = File.join(
          batch_directory,
          'stream-000000-2a11f3ee-9dd7-42ae-99bc-89046a8b8d65.json'
        )
        File.write(recovered_file, "{\"id\":1}\n")
        ingestor = instance_double(
          LogStash::Outputs::Kusto::Ingestor,
          upload_async: nil,
          stop: nil
        )
        allow(LogStash::Outputs::Kusto::Ingestor).to receive(:new).and_return(ingestor)
        kusto = described_class.new(options.merge(
          'ingestion_mode' => 'streaming',
          'streaming_temp_directory' => directory
        ))

        expect(ingestor).to receive(:upload_async).with(File.realpath(recovered_file), true)
        kusto.register
        kusto.close
      end
    end

    it 'discards incomplete streaming batches during recovery' do
      Dir.mktmpdir('kusto-streaming-recovery') do |directory|
        incomplete_directory = File.join(directory, '.batch-incomplete.tmp')
        FileUtils.mkdir_p(incomplete_directory)
        File.write(File.join(incomplete_directory, 'stream-000000.json'), "{\"id\":1}\n")
        ingestor = instance_double(
          LogStash::Outputs::Kusto::Ingestor,
          upload_async: nil,
          stop: nil
        )
        allow(LogStash::Outputs::Kusto::Ingestor).to receive(:new).and_return(ingestor)
        kusto = described_class.new(options.merge(
          'ingestion_mode' => 'streaming',
          'streaming_temp_directory' => directory
        ))

        kusto.register

        expect(ingestor).not_to have_received(:upload_async)
        expect(File.exist?(incomplete_directory)).to be(false)
        kusto.close
      end
    end

    it 'prevents multiple outputs from sharing one streaming spool' do
      Dir.mktmpdir('kusto-streaming-lock') do |directory|
        ingestor = instance_double(LogStash::Outputs::Kusto::Ingestor, stop: nil)
        allow(LogStash::Outputs::Kusto::Ingestor).to receive(:new).and_return(ingestor)
        first = described_class.new(options.merge(
          'ingestion_mode' => 'streaming',
          'streaming_temp_directory' => directory
        ))
        second = described_class.new(options.merge(
          'ingestion_mode' => 'streaming',
          'streaming_temp_directory' => directory
        ))

        first.register
        expect { second.register }.to raise_error(LogStash::ConfigurationError, /already in use/)
        first.close
      end
    end

    it 'rejects unsafe files during streaming recovery' do
      skip 'Windows symlink creation requires elevated privileges' if Gem.win_platform?

      Dir.mktmpdir('kusto-streaming-recovery') do |directory|
        batch_directory = File.join(directory, 'batch-existing.ready')
        FileUtils.mkdir_p(batch_directory)
        outside_file = File.join(directory, 'outside.json')
        File.write(outside_file, "{\"id\":\"forged\"}\n")
        recovered_file = File.join(
          batch_directory,
          'stream-000000-2a11f3ee-9dd7-42ae-99bc-89046a8b8d65.json'
        )
        File.symlink(outside_file, recovered_file)
        ingestor = instance_double(LogStash::Outputs::Kusto::Ingestor, stop: nil)
        allow(LogStash::Outputs::Kusto::Ingestor).to receive(:new).and_return(ingestor)
        kusto = described_class.new(options.merge(
          'ingestion_mode' => 'streaming',
          'streaming_temp_directory' => directory
        ))

        expect { kusto.register }.to raise_error(LogStash::ConfigurationError, /unsafe spool file/)
      end
    end

    it 'rejects a non-positive streaming request size' do
      kusto = described_class.new(options.merge(
        'ingestion_mode' => 'streaming',
        'streaming_max_request_bytes' => 0
      ))

      expect { kusto.register }.to raise_error(LogStash::ConfigurationError, /streaming_max_request_bytes/)
    end

    {
      'streaming_max_retry_attempts' => -1,
      'streaming_retry_backoff_seconds' => 0,
      'streaming_concurrent_requests' => 0
    }.each do |setting, value|
      it "rejects invalid #{setting}" do
        kusto = described_class.new(options.merge(
          'ingestion_mode' => 'streaming',
          setting => value
        ))

        expect { kusto.register }.to raise_error(LogStash::ConfigurationError, /#{setting}/)
      end
    end

  end

  describe '#multi_receive_encoded with streaming' do
    let(:streaming_temp_directory) { Dir.mktmpdir('kusto-streaming-spec') }
    let(:streaming_options) do
      options.reject { |key, _| key == 'path' }.merge(
        'ingestion_mode' => 'streaming',
        'streaming_max_request_bytes' => 10,
        'streaming_temp_directory' => streaming_temp_directory,
        'delete_temp_files' => false
      )
    end
    let(:ingestor) do
      instance_double(LogStash::Outputs::Kusto::Ingestor, upload_async: nil, stop: nil)
    end
    let(:kusto) { described_class.new(streaming_options) }

    before do
      @uploads = []
      @uploads_mutex = Mutex.new
      allow(ingestor).to receive(:upload_async) do |path, delete_on_success|
        @uploads_mutex.synchronize do
          @uploads << {
            path: path,
            payload: File.binread(path),
            delete_on_success: delete_on_success
          }
        end
      end
      allow(LogStash::Outputs::Kusto::Ingestor).to receive(:new).and_return(ingestor)
      kusto.register
    end

    after do
      kusto.close
      FileUtils.rm_rf(streaming_temp_directory)
    end

    it 'writes byte-bounded spool files and enqueues them in order' do
      events_and_encoded = [
        [LogStash::Event.new('id' => 1), "123456\n"],
        [LogStash::Event.new('id' => 2), "789\n"],
        [LogStash::Event.new('id' => 3), "abcde\n"]
      ]

      kusto.multi_receive_encoded(events_and_encoded)

      expect(@uploads.map { |upload| upload[:payload] })
        .to eq(["123456\n", "789\nabcde\n"])
      expect(@uploads).to all(include(delete_on_success: false))
      expect(@uploads.map { |upload| File.basename(File.dirname(upload[:path])) }.uniq.length).to eq(1)
      expect(File.basename(File.dirname(@uploads.first[:path]))).to match(/\Abatch-.*\.ready\z/)
      expect(Dir.glob(File.join(streaming_temp_directory, '.batch-*.tmp'))).to be_empty
      expect(File.stat(File.dirname(@uploads.first[:path])).mode & 0o777).to eq(0o700)
      expect(File.stat(@uploads.first[:path]).mode & 0o777).to eq(0o600)
    end

    it 'writes a single event larger than the threshold intact' do
      oversized = "#{'x' * 20}\n"

      kusto.multi_receive_encoded([[LogStash::Event.new('id' => 1), oversized]])

      expect(@uploads.map { |upload| upload[:payload] }).to eq([oversized])
    end

    it 'does not call Kusto for an empty Logstash batch' do
      kusto.multi_receive_encoded([])

      expect(@uploads).to be_empty
    end

    it 'uses encoded byte size when creating requests' do
      multibyte = "\u20ac\u20ac\n"

      kusto.multi_receive_encoded([
        [LogStash::Event.new('id' => 1), multibyte],
        [LogStash::Event.new('id' => 2), "1234\n"]
      ])

      expect(@uploads.map { |upload| upload[:payload].bytes })
        .to eq([multibyte.bytes, "1234\n".bytes])
    end

    it 'creates every spool file before starting ingestion' do
      events_and_encoded = [
        [LogStash::Event.new('id' => 1), "123456\n"],
        [LogStash::Event.new('id' => 2), "789\n"],
        [LogStash::Event.new('id' => 3), "abcde\n"]
      ]
      observed_file_count = nil
      allow(ingestor).to receive(:upload_async) do |path, _delete_on_success|
        observed_file_count ||= Dir.glob(
          File.join(streaming_temp_directory, 'batch-*.ready', '*.json')
        ).length
        @uploads << { path: path, payload: File.binread(path) }
      end

      kusto.multi_receive_encoded(events_and_encoded)

      expect(observed_file_count).to eq(2)
    end

    it 'cleans the entire uncommitted batch when a spool write fails' do
      allow(kusto).to receive(:write_streaming_file).and_call_original
      allow(kusto).to receive(:write_streaming_file).with(anything, ["abcde\n"])
        .and_raise(Errno::ENOSPC)

      expect do
        kusto.multi_receive_encoded([
          [LogStash::Event.new('id' => 1), "123456\n"],
          [LogStash::Event.new('id' => 2), "abcde\n"]
        ])
      end.to raise_error(Errno::ENOSPC)

      expect(@uploads).to be_empty
      expect(Dir.glob(File.join(streaming_temp_directory, '.batch-*.tmp'))).to be_empty
      expect(Dir.glob(File.join(streaming_temp_directory, 'batch-*.ready'))).to be_empty
    end

    it 'preserves a committed batch when a post-rename operation fails' do
      spool_directory = kusto.instance_variable_get(:@streaming_temp_directory)
      allow(kusto).to receive(:fsync_directory).and_call_original
      allow(kusto).to receive(:fsync_directory).with(spool_directory)
        .and_raise(Errno::EIO)

      expect do
        kusto.multi_receive_encoded([
          [LogStash::Event.new('id' => 1), "durable\n"]
        ])
      end.to raise_error(Errno::EIO)

      expect(@uploads).to be_empty
      expect(Dir.glob(File.join(streaming_temp_directory, '.batch-*.tmp'))).to be_empty
      committed_files = Dir.glob(
        File.join(streaming_temp_directory, 'batch-*.ready', 'stream-*.json')
      )
      expect(committed_files.length).to eq(1)
      expect(File.binread(committed_files.first)).to eq("durable\n")
    end

    it 'creates collision-free atomic batches from concurrent Logstash workers' do
      threads = 8.times.map do |index|
        Thread.new do
          kusto.multi_receive_encoded([
            [LogStash::Event.new('id' => index), "#{index}-payload\n"]
          ])
        end
      end
      threads.each(&:join)

      paths = @uploads.map { |upload| upload[:path] }
      expect(paths.uniq.length).to eq(8)
      expect(paths.map { |path| File.dirname(path) }.uniq.length).to eq(8)
      expect(Dir.glob(File.join(streaming_temp_directory, '.batch-*.tmp'))).to be_empty
    end
  end

  describe 'dynamic routing' do

    it 'registers without error when table/database are field references' do
      kusto = described_class.new(dynamic_options)
      expect { kusto.register }.not_to raise_error
      kusto.close
    end

    it 'routes a fully-resolved event to a file carrying the routing marker' do
      kusto = described_class.new(dynamic_options)
      kusto.register
      event = LogStash::Event.new
      event.set('[@metadata][database]', 'mydb')
      event.set('[@metadata][table]', 'mytable')
      event.set('[@metadata][mapping]', 'mymapping')
      path = kusto.send(:event_path, event)
      expect(path).to include('.kusto~mydb~mytable~mymapping')
      expect(path).not_to eq(kusto.failure_path)
      kusto.close
    end

    it 'drops an event missing the routing fields when the DLQ is disabled' do
      kusto = described_class.new(dynamic_options)
      kusto.register
      kusto.instance_variable_set(:@dlq_writer, nil)
      event = LogStash::Event.new
      path = kusto.send(:event_path, event)
      expect(path).to be_nil # dropped: not written to any temp file
      kusto.close
    end

    it 'drops an event whose resolved value is invalid (contains a path separator) when the DLQ is disabled' do
      kusto = described_class.new(dynamic_options)
      kusto.register
      kusto.instance_variable_set(:@dlq_writer, nil)
      event = LogStash::Event.new
      event.set('[@metadata][database]', 'mydb')
      event.set('[@metadata][table]', 'bad/table')
      event.set('[@metadata][mapping]', 'mymapping')
      path = kusto.send(:event_path, event)
      expect(path).to be_nil
      kusto.close
    end

    it 'routes an event whose target contains dots and spaces (valid ADX names)' do
      kusto = described_class.new(dynamic_options)
      kusto.register
      event = LogStash::Event.new
      event.set('[@metadata][database]', 'Security.Events')
      event.set('[@metadata][table]', 'App Logs')
      event.set('[@metadata][mapping]', 'My.Mapping')
      path = kusto.send(:event_path, event)
      expect(path).not_to be_nil
      # The decoded target must round-trip back to the original ADX names.
      target = described_class.decode_routing_target(path)
      expect(target[:database]).to eq('Security.Events')
      expect(target[:table]).to eq('App Logs')
      expect(target[:mapping]).to eq('My.Mapping')
      kusto.close
    end

    it 'allows a missing (empty) mapping while still routing on database/table' do
      kusto = described_class.new(dynamic_options)
      kusto.register
      event = LogStash::Event.new
      event.set('[@metadata][database]', 'mydb')
      event.set('[@metadata][table]', 'mytable')
      path = kusto.send(:event_path, event)
      expect(path).not_to eq(kusto.failure_path)
      expect(path).to include('.kusto~mydb~mytable~')
      kusto.close
    end

    it 'partitions one batch into a separate temp file per (database, table, mapping)' do
      # The central dynamic-routing behaviour: two valid events resolving to different
      # targets in a single batch must be written to two different temp files.
      kusto = described_class.new(dynamic_options)
      kusto.register
      written_paths = []
      writer = double('writer', write: nil, flush: nil)
      allow(kusto).to receive(:open) { |p| written_paths << p; writer }

      e1 = LogStash::Event.new
      e1.set('[@metadata][database]', 'mydb')
      e1.set('[@metadata][table]', 'orders')
      e1.set('[@metadata][mapping]', 'mymap')
      e2 = LogStash::Event.new
      e2.set('[@metadata][database]', 'mydb')
      e2.set('[@metadata][table]', 'clicks')
      e2.set('[@metadata][mapping]', 'mymap')

      kusto.multi_receive_encoded([[e1, '{"a":1}'], [e2, '{"b":2}']])

      expect(written_paths.uniq.length).to eq(2)
      expect(written_paths.any? { |p| p.include?('.kusto~mydb~orders~mymap') }).to be true
      expect(written_paths.any? { |p| p.include?('.kusto~mydb~clicks~mymap') }).to be true
      kusto.close
    end

    it 'writes the valid event and drops the unroutable one in a mixed batch (DLQ disabled)' do
      # One bad route must not prevent valid events in the same batch from being
      # written.
      kusto = described_class.new(dynamic_options)
      kusto.register
      kusto.instance_variable_set(:@dlq_writer, nil)
      written_paths = []
      writer = double('writer', write: nil, flush: nil)
      allow(kusto).to receive(:open) { |p| written_paths << p; writer }

      good = LogStash::Event.new
      good.set('[@metadata][database]', 'mydb')
      good.set('[@metadata][table]', 'orders')
      good.set('[@metadata][mapping]', 'mymap')
      bad = LogStash::Event.new # no routing fields -> unroutable -> dropped

      kusto.multi_receive_encoded([[good, '{"a":1}'], [bad, '{"b":2}']])

      expect(written_paths.uniq.length).to eq(1)
      expect(written_paths.first).to include('.kusto~mydb~orders~mymap')
      kusto.close
    end

    it 'treats a decoded value longer than the ADX limit as unroutable' do
      long = 'a' * (described_class::ROUTING_VALUE_MAX_LENGTH + 1)
      path = "/tmp/x.kusto~#{long}~mytable~"
      expect(described_class.decode_routing_target(path)).to be_nil
    end

    it 'drops an event whose resolved table exceeds the ADX length limit (DLQ disabled)' do
      kusto = described_class.new(dynamic_options)
      kusto.register
      kusto.instance_variable_set(:@dlq_writer, nil)
      event = LogStash::Event.new
      event.set('[@metadata][database]', 'mydb')
      event.set('[@metadata][table]', 'a' * (described_class::ROUTING_VALUE_MAX_LENGTH + 1))
      event.set('[@metadata][mapping]', 'mymapping')
      path = kusto.send(:event_path, event)
      expect(path).to be_nil
      kusto.close
    end

  end

  describe 'dynamic routing - crash recovery' do

    it 'recovers static leftover files when database/table contain dots' do
      require 'tmpdir'
      Dir.mktmpdir do |dir|
        kusto = described_class.new(options.merge(
          'path' => "#{dir}/tmp%{+YYYY-MM-dd-HH-mm}",
          'database' => 'Security.Events',
          'table' => 'App.Logs',
          'recovery' => false
        ))
        kusto.register
        # A leftover file with the exact static suffix, and a decoy that would
        # match if the dots were treated as regex wildcards.
        match = File.join(dir, 'tmp2024.Security.Events.App.Logs')
        decoy = File.join(dir, 'tmpXSecurityXEventsXAppXLogs')
        File.write(match, '{}')
        File.write(decoy, '{}')
        sent = []
        allow(kusto).to receive(:kusto_send_file) { |f| sent << f }
        kusto.send(:recover_past_files)
        expect(sent).to include(match)
        expect(sent).not_to include(decoy)
        kusto.close
      end
    end

    it 'computes a recovery scan directory free of field references for a relative path' do
      kusto = described_class.new(options.merge(
        'path' => './kusto_tst/%{+YYYY-MM-dd-HH-mm}',
        'table' => '%{[@metadata][table]}',
        'database' => 'mydb'
      ))
      kusto.register
      scan_dir = kusto.send(:recovery_scan_dir)
      expect(scan_dir).not_to include('%')
      expect(scan_dir.chomp('/')).to eq(File.expand_path('./kusto_tst'))
      kusto.close
    end

    it 'locates the scan directory even when the path uses backslash separators' do
      kusto = described_class.new(options.merge(
        'path' => './kusto_tst/%{+YYYY-MM-dd-HH-mm}',
        'table' => '%{[@metadata][table]}',
        'database' => 'mydb'
      ))
      kusto.register
      # Force a backslash-style @path (as some Windows/JRuby setups could yield)
      # and confirm the directory boundary before the field reference is still found.
      allow(Gem).to receive(:win_platform?).and_return(true)
      kusto.instance_variable_set(:@path, 'C:\\logs\\kusto\\out-%{+YYYY-MM-dd-HH-mm}')
      scan_dir = kusto.send(:recovery_scan_dir)
      expect(scan_dir).not_to include('%')
      expect(scan_dir).to eq('C:\\logs\\kusto\\')
      kusto.close
    end

    it 'recovers only dynamic temp files carrying this output\'s owner tag and routing marker' do
      require 'tmpdir'
      Dir.mktmpdir do |dir|
        kusto = described_class.new(options.merge(
          'path' => "#{dir}/out-%{+YYYY-MM-dd-HH-mm}",
          'table' => '%{[@metadata][table]}',
          'database' => 'mydb',
          'recovery' => false
        ))
        kusto.register
        owner_tag = kusto.instance_variable_get(:@routing_owner_tag)
        # A leftover file written by THIS output (carries our owner tag) and a
        # foreign file written by a different output (different owner tag) that
        # shares the same path root.
        mine = File.join(dir, "out-2024#{owner_tag}.kusto~mydb~mytable~")
        foreign = File.join(dir, 'out-2024.kustoid-deadbeefdeadbeef.kusto~mydb~mytable~')
        # A stray file that merely contains our owner tag but NOT the routing
        # marker right after it (e.g. an unrelated artefact). It must NOT be
        # recovered, so the ingestor never deletes it as an invalid routing file.
        tag_only = File.join(dir, "out-2024#{owner_tag}.log")
        File.write(mine, '{}')
        File.write(foreign, '{}')
        File.write(tag_only, '{}')
        sent = []
        allow(kusto).to receive(:kusto_send_file) { |f| sent << f }
        kusto.send(:recover_past_files)
        expect(sent).to include(mine)
        expect(sent).not_to include(foreign)
        expect(sent).not_to include(tag_only)
        kusto.close
      end
    end

  end

  describe '#inside_file_root?' do

    it 'interprets backslash separators only on Windows' do
      kusto = described_class.new(options.merge('path' => '/tmp/kusto/%{+YYYY-MM-dd-HH-mm}'))
      kusto.register
      root = kusto.instance_variable_get(:@file_root)
      expect(kusto.send(:inside_file_root?, "#{root}/sub/file.txt")).to be true
      expect(kusto.send(:inside_file_root?, "#{root}\\sub\\file.txt")).to eq(Gem.win_platform?)
      kusto.close
    end

    it 'treats a sibling directory sharing a name prefix as outside the root' do
      kusto = described_class.new(options.merge('path' => '/tmp/kustoroot/%{+YYYY-MM-dd-HH-mm}'))
      kusto.register
      root = kusto.instance_variable_get(:@file_root)
      # "<root>-evil" shares the prefix but must not be considered inside.
      expect(kusto.send(:inside_file_root?, "#{root}-evil/file.txt")).to be false
      kusto.close
    end

  end

  describe '#warn_if_too_many_open_files' do

    it 'warns once when the open file count crosses the threshold and re-arms after it recovers' do
      kusto = described_class.new(dynamic_options)
      kusto.register
      logger = spy('logger')
      kusto.instance_variable_set(:@logger, logger)
      kusto.instance_variable_set(:@open_files_warning_threshold, 2)

      kusto.instance_variable_set(:@files, { 'a' => 1, 'b' => 2 })
      kusto.send(:warn_if_too_many_open_files)
      kusto.send(:warn_if_too_many_open_files) # latched: must not warn again
      expect(logger).to have_received(:warn).with(/temporary files open/, anything).once

      kusto.instance_variable_set(:@files, {}) # drops below threshold -> re-arm
      kusto.send(:warn_if_too_many_open_files)
      kusto.instance_variable_set(:@files, { 'a' => 1, 'b' => 2 })
      kusto.send(:warn_if_too_many_open_files)
      expect(logger).to have_received(:warn).with(/temporary files open/, anything).twice
      kusto.close
    end

    it 'does not warn when the threshold is 0 (disabled)' do
      kusto = described_class.new(dynamic_options)
      kusto.register
      logger = spy('logger')
      kusto.instance_variable_set(:@logger, logger)
      kusto.instance_variable_set(:@open_files_warning_threshold, 0)
      kusto.instance_variable_set(:@files, { 'a' => 1, 'b' => 2, 'c' => 3 })
      kusto.send(:warn_if_too_many_open_files)
      expect(logger).not_to have_received(:warn).with(/temporary files open/, anything)
      kusto.close
    end

  end

  describe 'dynamic routing - register-time validation' do

    let(:dyn) { options.merge('path' => './kusto_tst/%{+YYYY-MM-dd-HH-mm}', 'table' => '%{[@metadata][table]}') }

    it 'fails fast when a static database is empty in dynamic mode' do
      kusto = described_class.new(dyn.merge('database' => ''))
      expect { kusto.register }.to raise_error(LogStash::ConfigurationError, /database must not be empty/)
      kusto.close
    end

    it 'fails fast when a static database contains a path separator' do
      kusto = described_class.new(dyn.merge('database' => 'bad/db'))
      expect { kusto.register }.to raise_error(LogStash::ConfigurationError, /must contain only/)
      kusto.close
    end

    it 'accepts a valid static database combined with a dynamic table' do
      kusto = described_class.new(dyn.merge('database' => 'my_db-1'))
      expect { kusto.register }.not_to raise_error
      kusto.close
    end

    it 'accepts a static database containing dots (a valid ADX name)' do
      kusto = described_class.new(dyn.merge('database' => 'Security.Events'))
      expect { kusto.register }.not_to raise_error
      kusto.close
    end

    it 'fails fast when a static json_mapping contains a path separator' do
      kusto = described_class.new(dyn.merge('database' => 'mydb', 'json_mapping' => 'bad/mapping'))
      expect { kusto.register }.to raise_error(LogStash::ConfigurationError, /json_mapping static value.*must contain only/)
      kusto.close
    end

    it 'allows a static json_mapping to be absent (mapping is optional)' do
      kusto = described_class.new(dyn.merge('database' => 'mydb').reject { |k, _| k == 'json_mapping' })
      expect { kusto.register }.not_to raise_error
      kusto.close
    end

    it 'accepts a valid static json_mapping' do
      kusto = described_class.new(dyn.merge('database' => 'mydb', 'json_mapping' => 'my_mapping-1'))
      expect { kusto.register }.not_to raise_error
      kusto.close
    end

    it 'fails fast when a static database exceeds the 1024-character ADX limit' do
      kusto = described_class.new(dyn.merge('database' => 'a' * (described_class::ROUTING_VALUE_MAX_LENGTH + 1)))
      expect { kusto.register }.to raise_error(LogStash::ConfigurationError, /1024 characters or fewer/)
      kusto.close
    end

    it 'rejects a negative dynamic_routing_max_open_files at register time' do
      kusto = described_class.new(dyn.merge('dynamic_routing_max_open_files' => -1))
      expect { kusto.register }.to raise_error(LogStash::ConfigurationError, /dynamic_routing_max_open_files must be 0/)
      kusto.close
    end

    it 'rejects a negative dynamic_routing_open_files_warning_threshold at register time' do
      kusto = described_class.new(dyn.merge('dynamic_routing_open_files_warning_threshold' => -5))
      expect { kusto.register }.to raise_error(LogStash::ConfigurationError, /dynamic_routing_open_files_warning_threshold must be 0/)
      kusto.close
    end

  end

  describe 'dynamic routing - dead letter queue' do

    let(:dlq_writer) { double('dlq_writer') }

    it 'sends an unroutable event to the DLQ and writes nothing to disk when DLQ is enabled' do
      kusto = described_class.new(dynamic_options)
      kusto.register
      # simulate DLQ being enabled
      kusto.instance_variable_set(:@dlq_writer, dlq_writer)

      event = LogStash::Event.new # missing routing fields
      expect(dlq_writer).to receive(:write).with(event, kind_of(String))
      path = kusto.send(:event_path, event)
      expect(path).to be_nil # signals caller to not write to any temp file
      kusto.close
    end

    it 'drops the event (returns nil) when the DLQ is disabled' do
      kusto = described_class.new(dynamic_options)
      kusto.register
      kusto.instance_variable_set(:@dlq_writer, nil)

      event = LogStash::Event.new
      path = kusto.send(:event_path, event)
      expect(path).to be_nil
      kusto.close
    end

    it 'does not write outside the files root when a resolved value attempts path traversal' do
      kusto = described_class.new(dynamic_options)
      kusto.register
      kusto.instance_variable_set(:@dlq_writer, nil)
      event = LogStash::Event.new
      event.set('[@metadata][database]', 'mydb')
      event.set('[@metadata][table]', '../../../../tmp/evil')
      event.set('[@metadata][mapping]', 'mymapping')
      path = kusto.send(:event_path, event)
      # The path separators are percent-encoded in the file name (so no traversal
      # can occur) and the decoded value fails routing validation, so the event is
      # treated as unroutable (dropped, DLQ disabled).
      expect(path).to be_nil
      kusto.close
    end

    it 'never writes to the failure file in dynamic mode, even with create_if_deleted => false' do
      # A routable event whose temp file is reported deleted must not fall back to
      # the failure file in dynamic mode (it could not be ingested anyway); it is
      # routed through the DLQ/drop handler instead.
      kusto = described_class.new(dynamic_options.merge('create_if_deleted' => false))
      kusto.register
      kusto.instance_variable_set(:@dlq_writer, nil)
      allow(kusto).to receive(:deleted?).and_return(true)
      allow(kusto).to receive(:cached?).and_return(true) # an externally deleted active writer

      event = LogStash::Event.new
      event.set('[@metadata][database]', 'mydb')
      event.set('[@metadata][table]', 'mytable')
      event.set('[@metadata][mapping]', 'mymapping')
      path = kusto.send(:event_path, event)
      expect(path).to be_nil
      expect(path).not_to eq(kusto.failure_path)
      kusto.close
    end

    it 'emits a DROPPED warning per batch when the DLQ is disabled' do
      kusto = described_class.new(dynamic_options)
      kusto.register
      kusto.instance_variable_set(:@dlq_writer, nil)
      logger = spy('logger')
      kusto.instance_variable_set(:@logger, logger)

      events = Array.new(3) { [LogStash::Event.new, '{"a":1}'] } # all unroutable
      kusto.multi_receive_encoded(events)
      expect(logger).to have_received(:warn).with(/3 event\(s\).*DROPPED/).once
      kusto.close
    end

    it 'treats a dummy (no-op) DLQ writer as disabled so events are not silently dropped' do
      kusto = described_class.new(dynamic_options)
      kusto.register

      # Stand-ins whose class name matches Logstash's no-op writer.
      dummy_class = Class.new do
        def self.name; 'LogStash::Util::DummyDeadLetterQueueWriter'; end
      end
      direct_dummy = dummy_class.new
      wrapped_dummy = double('wrapper', inner_writer: direct_dummy)
      real_writer = double('real_writer', inner_writer: double('inner'))

      expect(kusto.send(:dummy_dlq_writer?, direct_dummy)).to be true
      expect(kusto.send(:dummy_dlq_writer?, nil)).to be true
      expect(kusto.send(:dummy_dlq_writer?, real_writer)).to be false

      # A directly-supplied dummy must be detected as "disabled" so the plugin
      # drops events (and warns) rather than handing them to a no-op writer.
      ctx = double('execution_context', dlq_writer: direct_dummy)
      allow(kusto).to receive(:execution_context).and_return(ctx)
      expect(kusto.send(:dlq_enabled?)).to be false

      # A wrapped dummy (inner_writer is a dummy) is likewise disabled.
      ctx_wrapped = double('execution_context', dlq_writer: wrapped_dummy)
      allow(kusto).to receive(:execution_context).and_return(ctx_wrapped)
      expect(kusto.send(:dlq_enabled?)).to be false

      kusto.close
    end

    it 'does not write DLQ-routed events to any temp file (multi_receive_encoded skips nil paths)' do
      kusto = described_class.new(dynamic_options)
      kusto.register
      kusto.instance_variable_set(:@dlq_writer, dlq_writer)
      allow(dlq_writer).to receive(:write)

      event = LogStash::Event.new # missing routing fields -> DLQ
      # open must never be called because there is no path to write to
      expect(kusto).not_to receive(:open)
      kusto.multi_receive_encoded([[event, '{"a":1}']])
      kusto.close
    end

    it 'emits a single aggregated warning per batch rather than one per event' do
      kusto = described_class.new(dynamic_options)
      kusto.register
      kusto.instance_variable_set(:@dlq_writer, dlq_writer)
      allow(dlq_writer).to receive(:write)
      logger = spy('logger')
      kusto.instance_variable_set(:@logger, logger)

      events = Array.new(5) { [LogStash::Event.new, '{"a":1}'] } # all unroutable -> DLQ
      kusto.multi_receive_encoded(events)
      expect(logger).to have_received(:warn).with(/5 event\(s\) in this batch could not be routed/).once
      kusto.close
    end

    it 'includes a per-category breakdown of unroutable reasons in the batch warning' do
      # The per-batch warning must show WHICH field(s) failed (not just a total)
      # so operators can triage even when the DLQ is disabled. Build a mixed batch:
      # two events missing the table field and one whose table is ADX-valid but
      # encodes over the filesystem name limit.
      kusto = described_class.new(dynamic_options)
      kusto.register
      kusto.instance_variable_set(:@dlq_writer, nil) # disabled -> DROPPED path
      logger = spy('logger')
      kusto.instance_variable_set(:@logger, logger)

      missing_table = Array.new(2) do
        e = LogStash::Event.new
        e.set('[@metadata][database]', 'mydb') # table unset -> missing table
        [e, '{"a":1}']
      end
      long_name = LogStash::Event.new
      long_name.set('[@metadata][database]', 'mydb')
      long_name.set('[@metadata][table]', 'a' * 1024) # over filesystem name limit
      long_name.set('[@metadata][mapping]', 'mymapping')

      kusto.multi_receive_encoded(missing_table + [[long_name, '{"a":1}']])

      expect(logger).to have_received(:warn).with(
        /3 event\(s\).*DROPPED.*2 missing or invalid table.*1 filename over filesystem limit/
      ).once
      kusto.close
    end

    it 'treats an over-long encoded basename as unroutable (drop or DLQ)' do
      # When a routing value encodes to exceed the filesystem 255-byte filename
      # limit, it should be treated as unroutable instead of attempting File.new
      # and crashing the batch with Errno::ENAMETOOLONG.
      kusto = described_class.new(dynamic_options)
      kusto.register
      kusto.instance_variable_set(:@dlq_writer, dlq_writer)
      allow(dlq_writer).to receive(:write)

      # Lowercase ASCII needs no percent escapes: this segment alone is 1024
      # bytes, exceeding the 255-byte basename limit. The event should go to
      # DLQ without attempting to open an overlong filename.
      event = LogStash::Event.new
      event.set('[@metadata][database]', 'db')
      event.set('[@metadata][table]', 'a' * 1024)
      event.set('[@metadata][mapping]', 'mymapping')
      path = kusto.send(:event_path, event)
      # Path should be nil because the basename exceeds the filesystem limit.
      expect(path).to be_nil
      # Event should be routed to DLQ (not written to a temp file).
      expect(dlq_writer).to have_received(:write).with(event, kind_of(String)).once
      kusto.close
    end

    it 'matches owner-tag in basename only (not in directory names)' do
      # Ensure that a file sitting under a directory whose name contains the
      # owner-tag + marker substring is not mistakenly treated as owned by this
      # output. We test this via the dynamic_temp_file_owned_by_this_output?
      # predicate.
      kusto = described_class.new(dynamic_options)
      kusto.register

      owner_tag = kusto.instance_variable_get(:@routing_owner_tag)
      marker = described_class::ROUTING_MARKER
      fake_owned_substring = "#{owner_tag}#{marker}"

      # File in a directory whose name contains owner_tag + marker.
      # Should NOT be considered owned by this output (only basename matters).
      directory_with_tag = "./kusto_tst/prefix_#{fake_owned_substring}_suffix"
      file_under_tagged_dir = File.join(directory_with_tag, 'somefile.txt')
      result = kusto.send(:dynamic_temp_file_owned_by_this_output?, file_under_tagged_dir)
      expect(result).to be false

      # A file whose basename actually contains the tag + marker should be owned.
      owned_file = "./kusto_tst/prefix_#{fake_owned_substring}_suffix.txt"
      result = kusto.send(:dynamic_temp_file_owned_by_this_output?, owned_file)
      expect(result).to be true

      kusto.close
    end

    it 'treats composite-unresolved mapping (e.g. prefix_%{...}) as unroutable' do
      # When a mapping field reference is part of a composite value (e.g.
      # "prefix_%{missing_field}") and the field is missing, the resolved value
      # contains a literal "%{...}". Unlike an exact single field reference (which
      # routes without mapping), a composite unresolved should be unroutable to
      # avoid silent data loss (ingesting without the intended mapping).
      kusto = described_class.new(dynamic_options.merge('json_mapping' => 'prefix_%{[@metadata][custom_mapping]}'))
      kusto.register
      kusto.instance_variable_set(:@dlq_writer, dlq_writer)
      allow(dlq_writer).to receive(:write)

      event = LogStash::Event.new
      event.set('[@metadata][database]', 'mydb')
      event.set('[@metadata][table]', 'mytable')
      # Intentionally do NOT set [@metadata][custom_mapping], so it remains unresolved.
      path = kusto.send(:event_path, event)
      # Should be treated as unroutable (sent to DLQ or dropped).
      expect(path).to be_nil
      expect(dlq_writer).to have_received(:write).with(event, kind_of(String)).once
      kusto.close
    end

    it 'routes without mapping when an exact single field reference is unresolved' do
      # An exact field reference like json_mapping => "%{[@metadata][mapping]}"
      # with a missing field should route without mapping (mapping = nil), which
      # is the existing intended behavior. We verify this is unchanged by the fix.
      kusto = described_class.new(dynamic_options)
      kusto.register

      event = LogStash::Event.new
      event.set('[@metadata][database]', 'mydb')
      event.set('[@metadata][table]', 'mytable')
      # Do NOT set mapping, so %{[@metadata][mapping]} resolves to the literal "%{...}".
      path = kusto.send(:event_path, event)
      # Should be routable (exact unresolved field reference -> mapping = nil).
      expect(path).not_to be_nil
      expect(path).to include('mydb~mytable~')
      kusto.close
    end

    it 'reports a database-specific reason to the DLQ when the database field is missing' do
      kusto = described_class.new(dynamic_options)
      kusto.register
      kusto.instance_variable_set(:@dlq_writer, dlq_writer)

      event = LogStash::Event.new # no routing fields at all
      event.set('[@metadata][table]', 'mytable')
      expect(dlq_writer).to receive(:write).with(event, /database field is missing/)
      kusto.send(:event_path, event)
      kusto.close
    end

    it 'reports a table-specific reason to the DLQ when only the table field is missing' do
      kusto = described_class.new(dynamic_options)
      kusto.register
      kusto.instance_variable_set(:@dlq_writer, dlq_writer)

      event = LogStash::Event.new
      event.set('[@metadata][database]', 'mydb') # database resolves, table does not
      expect(dlq_writer).to receive(:write).with(event, /table field is missing/)
      kusto.send(:event_path, event)
      kusto.close
    end

    it 'reports a json_mapping-specific reason to the DLQ for a composite-unresolved mapping' do
      kusto = described_class.new(dynamic_options.merge('json_mapping' => 'prefix_%{[@metadata][custom_mapping]}'))
      kusto.register
      kusto.instance_variable_set(:@dlq_writer, dlq_writer)

      event = LogStash::Event.new
      event.set('[@metadata][database]', 'mydb')
      event.set('[@metadata][table]', 'mytable')
      # custom_mapping intentionally unset -> composite "prefix_%{...}" remains.
      expect(dlq_writer).to receive(:write).with(event, /json_mapping/)
      kusto.send(:event_path, event)
      kusto.close
    end

    it 'reports a filesystem-limit reason to the DLQ when the encoded file name is too long' do
      kusto = described_class.new(dynamic_options)
      kusto.register
      kusto.instance_variable_set(:@dlq_writer, dlq_writer)

      event = LogStash::Event.new
      event.set('[@metadata][database]', 'db')
      event.set('[@metadata][table]', 'a' * 1024) # ADX-valid but encodes over the byte limit
      event.set('[@metadata][mapping]', 'mymapping')
      expect(dlq_writer).to receive(:write).with(event, /filesystem limit/)
      kusto.send(:event_path, event)
      kusto.close
    end

    it 'propagates EMFILE rather than reporting success for an unwritten route' do
      # Unlike invalid routing data, a storage failure is not a DLQ/drop policy.
      # Some earlier routes may already have been written; preserve the existing
      # queued error contract instead of silently acknowledging the failed route.
      kusto = described_class.new(dynamic_options)
      kusto.register
      logger = spy('logger')
      kusto.instance_variable_set(:@logger, logger)

      good = LogStash::Event.new
      good.set('[@metadata][database]', 'db'); good.set('[@metadata][table]', 'good'); good.set('[@metadata][mapping]', 'm')
      bad = LogStash::Event.new
      bad.set('[@metadata][database]', 'db'); bad.set('[@metadata][table]', 'bad'); bad.set('[@metadata][mapping]', 'm')

      good_path = kusto.send(:generate_filepath, good)
      bad_path = kusto.send(:generate_filepath, bad)
      good_writer = double('writer', write: nil, flush: nil)
      allow(kusto).to receive(:open).with(good_path).and_return(good_writer)
      allow(kusto).to receive(:open).with(bad_path).and_raise(Errno::EMFILE)

      expect { kusto.multi_receive_encoded([[good, '{"a":1}'], [bad, '{"a":1}']]) }.to raise_error(Errno::EMFILE)
      expect(good_writer).to have_received(:write).with('{"a":1}')
      kusto.close
    end

    it 'caps concurrent open files and routes events for excess routes to the DLQ' do
      # With a cap of 2, only the first two distinct routes in the batch are
      # accepted; events for further new routes are dead-lettered (while still in
      # hand) rather than risking file-descriptor exhaustion.
      kusto = described_class.new(dynamic_options.merge('dynamic_routing_max_open_files' => 2))
      kusto.register
      kusto.instance_variable_set(:@dlq_writer, dlq_writer)
      allow(dlq_writer).to receive(:write)
      allow(kusto).to receive(:open).and_return(double('writer', write: nil, flush: nil))

      events = %w[t1 t2 t3 t4].map do |t|
        e = LogStash::Event.new
        e.set('[@metadata][database]', 'db'); e.set('[@metadata][table]', t); e.set('[@metadata][mapping]', 'm')
        [e, '{"a":1}']
      end
      kusto.multi_receive_encoded(events)
      # 4 distinct routes, cap 2 -> two accepted, two sent to the DLQ.
      expect(dlq_writer).to have_received(:write).with(anything, /open temporary file limit \(2\) reached/).twice
      kusto.close
    end

    it 'counts files already open from earlier batches against the cap (global invariant)' do
      # The cap must be global across batches (and, by extension, across shared
      # worker threads), not per-call: a route already open from a previous batch
      # consumes capacity. With one file already open and a cap of 1, a NEW route
      # in this batch must be dead-lettered while the already-open route still
      # writes. The accounting and the open happen under one @io_mutex section.
      kusto = described_class.new(dynamic_options.merge('dynamic_routing_max_open_files' => 1))
      kusto.register
      kusto.instance_variable_set(:@dlq_writer, dlq_writer)
      allow(dlq_writer).to receive(:write)
      writer = double('writer', write: nil, flush: nil)
      allow(kusto).to receive(:open).and_return(writer)

      existing = LogStash::Event.new
      existing.set('[@metadata][database]', 'db'); existing.set('[@metadata][table]', 'existing'); existing.set('[@metadata][mapping]', 'm')
      existing_path = kusto.send(:generate_filepath, existing)
      # Simulate a file already open from a prior batch.
      kusto.instance_variable_get(:@files)[existing_path] = writer

      fresh = LogStash::Event.new
      fresh.set('[@metadata][database]', 'db'); fresh.set('[@metadata][table]', 'fresh'); fresh.set('[@metadata][mapping]', 'm')

      kusto.multi_receive_encoded([[fresh, '{"a":1}']])
      # Cap already full (1 open) -> the fresh route is dead-lettered.
      expect(dlq_writer).to have_received(:write).with(fresh, /open temporary file limit \(1\) reached/).once
      kusto.close
    end

    it 'does not enforce the open-file cap when it is left at the default (0 = disabled)' do
      kusto = described_class.new(dynamic_options) # no dynamic_routing_max_open_files
      kusto.register
      kusto.instance_variable_set(:@dlq_writer, dlq_writer)
      allow(kusto).to receive(:open).and_return(double('writer', write: nil, flush: nil))

      events = %w[t1 t2 t3 t4 t5].map do |t|
        e = LogStash::Event.new
        e.set('[@metadata][database]', 'db'); e.set('[@metadata][table]', t); e.set('[@metadata][mapping]', 'm')
        [e, '{"a":1}']
      end
      # No event should be dead-lettered for an open-file cap when it is disabled.
      expect(dlq_writer).not_to receive(:write).with(anything, /open temporary file limit/)
      kusto.multi_receive_encoded(events)
      kusto.close
    end

    it 'runs interval stale cleanup under @io_mutex so the cleaner thread never mutates @files unlocked' do
      # With stale_cleanup_type => "interval" the cleaner runs on its own thread,
      # concurrently with worker threads under `concurrency :shared`. Its @files
      # mutation must be serialized through @io_mutex; the interval entry point
      # (close_stale_files) must acquire the lock, while the in-mutex callers use
      # close_stale_files_locked (Ruby Mutex is not reentrant).
      kusto = described_class.new(dynamic_options.merge('stale_cleanup_type' => 'interval', 'stale_cleanup_interval' => 1))
      kusto.register
      mutex = kusto.instance_variable_get(:@io_mutex)
      allow(mutex).to receive(:synchronize).and_call_original

      # A stale (inactive) file the cycle should close, queue for ingest, and
      # remove from @files — all under the lock.
      fd = double('writer', path: '/tmp/kusto/stale.kusto~db~t~m')
      allow(fd).to receive(:active).and_return(false)
      allow(fd).to receive(:active=)
      allow(fd).to receive(:close)
      files = kusto.instance_variable_get(:@files)
      files['/tmp/kusto/stale.kusto~db~t~m'] = fd
      kusto.instance_variable_set(:@last_stale_cleanup_cycle, Time.now - 3600)
      allow(kusto).to receive(:kusto_send_file)

      kusto.send(:close_stale_files) # the interval-thread entry point

      expect(mutex).to have_received(:synchronize).at_least(:once)
      expect(files).not_to have_key('/tmp/kusto/stale.kusto~db~t~m')
      expect(kusto).to have_received(:kusto_send_file).with('/tmp/kusto/stale.kusto~db~t~m')
      kusto.close
    end

    it 'gives outputs distinct owner tags when recovery_owner_id differs, and identical tags otherwise' do
      base = dynamic_options
      k1 = described_class.new(base.merge('recovery_owner_id' => 'one')); k1.register
      k2 = described_class.new(base.merge('recovery_owner_id' => 'two')); k2.register
      # Distinct recovery_owner_id -> distinct recovery ownership.
      expect(k1.instance_variable_get(:@routing_owner_tag)).not_to eq(k2.instance_variable_get(:@routing_owner_tag))

      # Identical routing config and no recovery_owner_id -> same tag (documented).
      k3 = described_class.new(base); k3.register
      k4 = described_class.new(base); k4.register
      expect(k3.instance_variable_get(:@routing_owner_tag)).to eq(k4.instance_variable_get(:@routing_owner_tag))
      [k1, k2, k3, k4].each(&:close)
    end

    it 'does not recover dynamic temp files written under a different routing configuration' do
      kusto_a = described_class.new(dynamic_options)
      kusto_a.register
      tag_a = kusto_a.instance_variable_get(:@routing_owner_tag)

      # Change a routing setting -> different owner id -> different temp-file tag.
      kusto_b = described_class.new(dynamic_options.merge('database' => 'other_%{[@metadata][database]}'))
      kusto_b.register
      tag_b = kusto_b.instance_variable_get(:@routing_owner_tag)
      expect(tag_a).not_to eq(tag_b)

      # A file owned by A is not recognised as owned by B, so B will not recover it.
      a_file = "x#{tag_a}#{described_class::ROUTING_MARKER}db~t~m"
      expect(kusto_b.send(:dynamic_temp_file_owned_by_this_output?, a_file)).to be false
      kusto_a.close
      kusto_b.close
    end

  end

end
