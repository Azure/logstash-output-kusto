# encoding: utf-8
require_relative "../spec_helpers.rb"
require 'logstash/outputs/kusto'
require 'logstash/codecs/plain'
require 'logstash/event'
require 'fileutils'

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

  # Registers the plugin without constructing a real Java Kusto client (no
  # cluster contact in unit tests). Returns the plugin; its @ingestor is a stub
  # that records upload_async/stop as no-ops.
  def register_without_client(kusto)
    allow(LogStash::Outputs::Kusto::Ingestor).to receive(:new)
      .and_return(double('ingestor', upload_async: nil, stop: nil))
    kusto.register
    kusto
  end

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

  end

  describe '#rotate_by' do

    it 'defaults to event' do
      kusto = described_class.new(options)
      expect(kusto.rotate_by).to eq('event')
    end

    it 'accepts processing' do
      kusto = described_class.new(options.merge({'rotate_by' => 'processing'}))
      expect(kusto.rotate_by).to eq('processing')
    end

    it 'rejects an unknown value' do
      expect {
        described_class.new(options.merge({'rotate_by' => 'bogus'}))
      }.to raise_error(LogStash::ConfigurationError)
    end

  end

  describe '#generate_filepath' do

    let(:path_pattern) { './kusto_tst/%{+YYYY-MM-dd-HH-mm}' }

    def event_at(utc_time)
      event = LogStash::Event.new('message' => 'hello')
      event.timestamp = LogStash::Timestamp.new(utc_time)
      event
    end

    let(:batch_time)    { LogStash::Timestamp.new(Time.utc(2024, 6, 15, 12, 30, 0)) }
    let(:old_event)     { event_at(Time.utc(2018, 1, 1, 0, 0, 0)) }
    let(:future_event)  { event_at(Time.utc(2030, 12, 31, 23, 59, 0)) }

    context 'when rotate_by is event (default)' do
      it 'rotates using each event timestamp, so skewed events land in different files' do
        kusto = described_class.new(options)
        kusto.instance_variable_set(:@path, path_pattern)
        path_old = kusto.send(:generate_filepath, old_event, batch_time)
        path_future = kusto.send(:generate_filepath, future_event, batch_time)
        expect(path_old).not_to eq(path_future)
      end
    end

    context 'when rotate_by is processing' do
      let(:kusto) do
        instance = described_class.new(options.merge({'rotate_by' => 'processing'}))
        instance.instance_variable_set(:@path, path_pattern)
        instance
      end

      it 'rotates using the batch processing time, so skewed events land in the same file' do
        path_old = kusto.send(:generate_filepath, old_event, batch_time)
        path_future = kusto.send(:generate_filepath, future_event, batch_time)
        expect(path_old).to eq(path_future)
      end

      it 'resolves the time pattern from the batch processing time, not the event time' do
        # Reference: what event-time rotation produces for an event AT the batch time.
        reference = described_class.new(options)
        reference.instance_variable_set(:@path, path_pattern)
        expected = reference.send(:generate_filepath, event_at(Time.utc(2024, 6, 15, 12, 30, 0)), batch_time)

        expect(kusto.send(:generate_filepath, old_event, batch_time)).to eq(expected)
      end

      it 'restores the original event timestamp after resolving the path' do
        original = old_event.timestamp
        kusto.send(:generate_filepath, old_event, batch_time)
        expect(old_event.timestamp).to eq(original)
      end

      it 'still resolves %{field} references from the event' do
        kusto.instance_variable_set(:@path, './kusto_tst/%{src}/%{+YYYY-MM-dd-HH-mm}')
        event = event_at(Time.utc(2018, 1, 1, 0, 0, 0))
        event.set('src', 'hostA')
        expect(kusto.send(:generate_filepath, event, batch_time)).to include('hostA')
      end

      it 'resolves a literal %{[@timestamp]} reference from processing time (documented caveat)' do
        # See generate_filepath: %{[@timestamp]} reflects the swapped processing time.
        kusto.instance_variable_set(:@path, './kusto_tst/%{[@timestamp]}')
        path = kusto.send(:generate_filepath, old_event, batch_time)
        expect(path).to include('2024-06-15')
        expect(path).not_to include('2018')
      end

      it 'handles an event whose @timestamp was removed without raising' do
        event = LogStash::Event.new('message' => 'hello')
        event.remove(LogStash::Event::TIMESTAMP)
        expect {
          path = kusto.send(:generate_filepath, event, batch_time)
          expect(path).to eq('./kusto_tst/2024-06-15-12-30')
        }.not_to raise_error
      end

      it 'leaves a removed @timestamp absent after resolving the path' do
        event = LogStash::Event.new('message' => 'hello')
        event.remove(LogStash::Event::TIMESTAMP)
        kusto.send(:generate_filepath, event, batch_time)
        expect(event.get(LogStash::Event::TIMESTAMP)).to be_nil
      end
    end

  end

  describe 'rotate_by startup recommendation' do

    it 'warns when rotate_by is not explicitly set' do
      kusto = described_class.new(options)
      logger = spy('logger')
      kusto.instance_variable_set(:@logger, logger)
      register_without_client(kusto)
      expect(logger).to have_received(:warn).with(/rotate_by/)
      kusto.close
    end

    it 'does not warn when rotate_by is explicitly set' do
      kusto = described_class.new(options.merge({'rotate_by' => 'event'}))
      logger = spy('logger')
      kusto.instance_variable_set(:@logger, logger)
      register_without_client(kusto)
      expect(logger).not_to have_received(:warn).with(/rotate_by/)
      kusto.close
    end

  end

  describe '#multi_receive_encoded' do

    let(:mre_path) { './kusto_tst_mre/%{+YYYY-MM-dd-HH-mm}' }

    after { FileUtils.rm_rf('./kusto_tst_mre') }

    def receiving_kusto(rotate_by)
      kusto = described_class.new(options.merge({'rotate_by' => rotate_by, 'path' => mre_path}))
      register_without_client(kusto)
    end

    def event_at(utc_time)
      event = LogStash::Event.new('message' => 'hello')
      event.timestamp = LogStash::Timestamp.new(utc_time)
      event
    end

    let(:skewed_batch) do
      [[event_at(Time.utc(2018, 1, 1, 0, 0, 0)), "a\n"],
       [event_at(Time.utc(2030, 12, 31, 23, 59, 0)), "b\n"]]
    end

    it 'writes skewed events in one batch to a single file when rotate_by is processing' do
      kusto = receiving_kusto('processing')
      kusto.multi_receive_encoded(skewed_batch)
      expect(kusto.instance_variable_get(:@files).keys.size).to eq(1)
      kusto.close
    end

    it 'writes skewed events in one batch to separate files when rotate_by is event' do
      kusto = receiving_kusto('event')
      kusto.multi_receive_encoded(skewed_batch)
      expect(kusto.instance_variable_get(:@files).keys.size).to eq(2)
      kusto.close
    end

  end

end
