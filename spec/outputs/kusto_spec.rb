# encoding: utf-8
require 'logstash/outputs/kusto'
require 'logstash/codecs/plain'
require 'logstash/event'

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

  # Shared dynamic-routing config (database/table/mapping are field references).
  let(:dynamic_options) { options.merge(
    'path' => './kusto_tst/%{+YYYY-MM-dd-HH-mm}',
    'table' => '%{[@metadata][table]}',
    'database' => '%{[@metadata][database]}',
    'json_mapping' => '%{[@metadata][mapping]}'
  ) }

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

    it 'drops an event whose resolved identifier is invalid when the DLQ is disabled' do
      kusto = described_class.new(dynamic_options)
      kusto.register
      kusto.instance_variable_set(:@dlq_writer, nil)
      event = LogStash::Event.new
      event.set('[@metadata][database]', 'mydb')
      event.set('[@metadata][table]', 'bad.table.name')
      event.set('[@metadata][mapping]', 'mymapping')
      path = kusto.send(:event_path, event)
      expect(path).to be_nil
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

  end

  describe 'dynamic routing - register-time validation' do

    let(:dyn) { options.merge('path' => './kusto_tst/%{+YYYY-MM-dd-HH-mm}', 'table' => '%{[@metadata][table]}') }

    it 'fails fast when a static database is empty in dynamic mode' do
      kusto = described_class.new(dyn.merge('database' => ''))
      expect { kusto.register }.to raise_error(LogStash::ConfigurationError, /database must not be empty/)
      kusto.close
    end

    it 'fails fast when a static database contains characters outside the allowlist' do
      kusto = described_class.new(dyn.merge('database' => 'bad.db.name'))
      # The error must use the human-readable allowlist, not a raw regexp dump.
      expect { kusto.register }.to raise_error(LogStash::ConfigurationError, /must match \[A-Za-z0-9_-\]\+/)
      kusto.close
    end

    it 'accepts a valid static database combined with a dynamic table' do
      kusto = described_class.new(dyn.merge('database' => 'my_db-1'))
      expect { kusto.register }.not_to raise_error
      kusto.close
    end

    it 'fails fast when a static json_mapping contains characters outside the allowlist' do
      kusto = described_class.new(dyn.merge('database' => 'mydb', 'json_mapping' => 'bad.mapping'))
      expect { kusto.register }.to raise_error(LogStash::ConfigurationError, /json_mapping static value.*must match/)
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
      # Either the outside-root guard or the identifier allowlist rejects it;
      # in both cases the event is treated as unroutable (dropped, DLQ disabled).
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

  end

end
