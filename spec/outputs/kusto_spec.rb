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

    it 'does not require path for managed streaming ingestion' do
      streaming_options = options
        .reject { |key, _| key == 'path' }
        .merge('ingestion_mode' => 'managed_streaming')
      ingestor = instance_double(LogStash::Outputs::Kusto::Ingestor, stop: nil)
      allow(LogStash::Outputs::Kusto::Ingestor).to receive(:new).and_return(ingestor)
      kusto = described_class.new(streaming_options)

      expect { kusto.register }.not_to raise_error
      kusto.close
    end

    it 'recovers managed streaming spool files after restart' do
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
          'ingestion_mode' => 'managed_streaming',
          'streaming_temp_directory' => directory
        ))

        expect(ingestor).to receive(:upload_async).with(recovered_file, true)
        kusto.register
        kusto.close
      end
    end

    it 'discards incomplete managed streaming batches during recovery' do
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
          'ingestion_mode' => 'managed_streaming',
          'streaming_temp_directory' => directory
        ))

        kusto.register

        expect(ingestor).not_to have_received(:upload_async)
        expect(File.exist?(incomplete_directory)).to be(false)
        kusto.close
      end
    end

    it 'prevents multiple outputs from sharing one managed streaming spool' do
      Dir.mktmpdir('kusto-streaming-lock') do |directory|
        ingestor = instance_double(LogStash::Outputs::Kusto::Ingestor, stop: nil)
        allow(LogStash::Outputs::Kusto::Ingestor).to receive(:new).and_return(ingestor)
        first = described_class.new(options.merge(
          'ingestion_mode' => 'managed_streaming',
          'streaming_temp_directory' => directory
        ))
        second = described_class.new(options.merge(
          'ingestion_mode' => 'managed_streaming',
          'streaming_temp_directory' => directory
        ))

        first.register
        expect { second.register }.to raise_error(LogStash::ConfigurationError, /already in use/)
        first.close
      end
    end

    it 'rejects a non-positive streaming request size' do
      kusto = described_class.new(options.merge(
        'ingestion_mode' => 'managed_streaming',
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
          'ingestion_mode' => 'managed_streaming',
          setting => value
        ))

        expect { kusto.register }.to raise_error(LogStash::ConfigurationError, /#{setting}/)
      end
    end

  end

  describe '#multi_receive_encoded with managed streaming' do
    let(:streaming_temp_directory) { Dir.mktmpdir('kusto-streaming-spec') }
    let(:streaming_options) do
      options.reject { |key, _| key == 'path' }.merge(
        'ingestion_mode' => 'managed_streaming',
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
      allow(ingestor).to receive(:upload_async) do |path, delete_on_success|
        @uploads << {
          path: path,
          payload: File.binread(path),
          delete_on_success: delete_on_success
        }
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
  end

end
