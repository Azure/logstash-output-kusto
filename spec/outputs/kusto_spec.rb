# encoding: utf-8
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
    "proxy_host" => "localhost",
    "proxy_port" => 3128,
    "proxy_protocol" => "https"
  } }

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

end
