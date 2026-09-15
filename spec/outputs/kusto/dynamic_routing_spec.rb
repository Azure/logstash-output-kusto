# encoding: utf-8
require_relative '../../spec_helpers'
require 'logstash/outputs/kusto'
require 'tmpdir'
require 'securerandom'

describe LogStash::Outputs::Kusto, 'dynamic routing safety' do
  let(:logger) { spy('logger') }
  let(:dlq) { spy('DLQ') }
  let(:uploads) { [] }
  let(:uploader) { double('uploader', stop: nil) }

  before do
    @directory = Dir.mktmpdir('kusto-routing')
    @outputs = []
    allow(described_class::Ingestor).to receive(:new).and_return(uploader)
    allow(uploader).to receive(:upload_async) { |path, _delete| uploads << path }
  end

  after do
    @outputs.each(&:close)
    FileUtils.remove_entry(@directory)
  end

  def output(overrides = {})
    plugin = described_class.new({
      'path' => "#{@directory}/out-%{+YYYY-MM-dd-HH-mm}",
      'ingest_url' => 'https://ingest-test.kusto.windows.net',
      'app_id' => 'test-app', 'app_key' => 'test-key', 'app_tenant' => 'test-tenant',
      'database' => '%{[target_database]}', 'table' => '%{[kusto_table]}',
      'json_mapping' => '%{[@metadata][mapping]}', 'flush_interval' => 0, 'recovery' => false
    }.merge(overrides))
    plugin.instance_variable_set(:@logger, logger)
    plugin.register
    plugin.instance_variable_set(:@dlq_writer, dlq)
    @outputs << plugin
    plugin
  end

  def event(database = 'db', table = 'orders', mapping = 'mapping')
    LogStash::Event.new(
      'target_database' => database, 'kusto_table' => table,
      '@metadata' => { 'mapping' => mapping }, '@timestamp' => '2026-09-15T01:00:00Z'
    )
  end

  def writers(plugin)
    plugin.instance_variable_get(:@files)
  end

  def retire(plugin)
    writers(plugin).each_value { |writer| writer.active = false }
    plugin.instance_variable_set(:@last_stale_cleanup_cycle, Time.now - 60)
    plugin.send(:close_stale_files)
  end

  it 'partitions real files by database, table AND mapping, reusing a writer only for the same tuple' do
    plugin = output
    tuples = [
      ['db', 'orders', 'one'], ['other_db', 'orders', 'one'],
      ['db', 'clicks', 'one'], ['db', 'orders', 'two'], ['db', 'orders', nil]
    ]
    batch = tuples.each_with_index.map { |tuple, index| [event(*tuple), "{\"id\":#{index}}\n"] }
    plugin.multi_receive_encoded(batch + [batch.first])

    expect(writers(plugin).length).to eq(5)
    contents = writers(plugin).values.to_h do |writer|
      target = described_class.decode_routing_target(writer.path)
      [[target[:database], target[:table], target[:mapping]], File.binread(writer.path)]
    end
    tuples.each_with_index do |tuple, index|
      expect(contents.fetch(tuple)).to eq("{\"id\":#{index}}\n" * (index.zero? ? 2 : 1))
    end
  end

  it 'keeps case-distinct destinations separate even on a case-insensitive filesystem' do
    plugin = output
    upper = event('db', 'Orders', 'Map')
    lower = event('db', 'orders', 'map')
    plugin.multi_receive_encoded([[upper, "upper\n"], [lower, "lower\n"]])
    paths = writers(plugin).values.map(&:path)

    expect(paths.map(&:downcase).uniq.length).to eq(2)
    expect(paths.map { |path| File.binread(path) }).to contain_exactly("upper\n", "lower\n")
    encoded = %w[Orders orders].map { |value| described_class.encode_routing_segment(value).downcase }
    expect(encoded.uniq.length).to eq(2)
  end

  %w[cap events interval].each do |cleanup|
    it "never changes or reopens an upload after #{cleanup} cleanup in the same time window" do
      plugin = output('dynamic_routing_max_open_files' => cleanup == 'cap' ? 1 : 0)
      plugin.multi_receive_encoded([[event, "first\n"]])
      first_path = writers(plugin).values.first.path
      if cleanup == 'cap'
        writers(plugin).each_value { |writer| writer.active = false }
        plugin.instance_variable_set(:@last_stale_cleanup_cycle, Time.now - 60)
      elsif cleanup == 'interval'
        Thread.new { retire(plugin) }.value
      else
        retire(plugin)
      end
      plugin.multi_receive_encoded([[event, "second\n"]])
      second_path = writers(plugin).values.first.path

      expect(uploads).to eq([first_path])
      expect(second_path).not_to eq(first_path)
      expect(File.binread(first_path)).to eq("first\n")
      File.delete(first_path) # Complete the delayed upload only after the next batch was written.
      expect(File.binread(second_path)).to eq("second\n")
    end
  end

  it 'allocates a new physical generation after a filename collision without overwriting the old one' do
    allow(SecureRandom).to receive(:hex).with(16).and_return('a' * 32, 'a' * 32, 'b' * 32)
    plugin = output
    plugin.multi_receive_encoded([[event, "first\n"]])
    first_path = writers(plugin).values.first.path
    retire(plugin)
    plugin.multi_receive_encoded([[event, "second\n"]])
    second_path = writers(plugin).values.first.path

    expect(second_path).not_to eq(first_path)
    expect(File.binread(first_path)).to eq("first\n")
    expect(File.binread(second_path)).to eq("second\n")
  end

  it 'handles synchronous upload completion without deleting or reusing the next generation' do
    allow(uploader).to receive(:upload_async) { |path, _delete| File.delete(path) }
    plugin = output
    plugin.multi_receive_encoded([[event, "first\n"]])
    first_path = writers(plugin).values.first.path
    retire(plugin)
    plugin.multi_receive_encoded([[event, "second\n"]])

    expect(File.exist?(first_path)).to be(false)
    expect(writers(plugin).values.first.path).not_to eq(first_path)
    expect(File.binread(writers(plugin).values.first.path)).to eq("second\n")
  end

  it 'retains a closed generation if enqueue fails, and never returns that closed writer to later events' do
    plugin = output
    plugin.multi_receive_encoded([[event, "first\n"]])
    first_path = writers(plugin).values.first.path
    allow(uploader).to receive(:upload_async).and_raise(Concurrent::RejectedExecutionError)
    expect { retire(plugin) }.to raise_error(Concurrent::RejectedExecutionError)
    allow(uploader).to receive(:upload_async) { |path, _delete| uploads << path }
    plugin.multi_receive_encoded([[event, "second\n"]])

    expect(writers(plugin).values.first.path).not_to eq(first_path)
    expect(File.binread(first_path)).to eq("first\n")
  end

  [true, false].each do |recovery|
    it "does not append to a legacy leftover file with recovery=#{recovery}" do
      original = output
      legacy_path = original.send(:generate_filepath, event)
      File.write(legacy_path, "old\n")
      restarted = output('recovery' => recovery)
      restarted.multi_receive_encoded([[event, "new\n"]])

      expect(uploads.include?(legacy_path)).to eq(recovery)
      expect(File.binread(legacy_path)).to eq("old\n")
      expect(writers(restarted).values.first.path).not_to eq(legacy_path)
      expect(File.binread(writers(restarted).values.first.path)).to eq("new\n")
    end
  end

  it 'recovers closed generation files by their persisted destination, not the next event' do
    original = output
    original.multi_receive_encoded([[event('db', 'Orders', 'map'), "old\n"]])
    path = writers(original).values.first.path
    writers(original).each_value(&:close)
    writers(original).clear # Simulate restart before the file was handed to the uploader.

    restarted = output('recovery' => true)
    expect(uploads).to eq([path])
    restarted.multi_receive_encoded([[event('db', 'Orders', 'map'), "new\n"]])
    expect(File.binread(path)).to eq("old\n")
    expect(writers(restarted).values.first.path).not_to eq(path)
  end

  it 'retains invalid owned files without uploading them, and ignores other owners' do
    plugin = output
    owner = plugin.instance_variable_get(:@routing_owner_tag)
    invalid = File.join(@directory, "out#{owner}.kusto~db~orders~%FF")
    foreign = File.join(@directory, "out#{owner}.kusto~prefix.kustoid-deadbeefdeadbeef.kusto~db~orders~map")
    File.write(invalid, "invalid\n")
    File.write(foreign, "foreign\n")
    2.times { plugin.send(:recover_past_files) }

    expect(uploads).to be_empty
    expect(File.binread(invalid)).to eq("invalid\n")
    expect(File.binread(foreign)).to eq("foreign\n")
    expect(logger).to have_received(:warn).with(/invalid.*routing|routing.*invalid/i, hash_including(path: invalid)).twice
  end

  it 'lets create_if_deleted=false create new routes but dead-letters an externally deleted active file' do
    plugin = output('create_if_deleted' => false)
    plugin.multi_receive_encoded([[event, "first\n"]])
    expect(writers(plugin).length).to eq(1)
    writer = writers(plugin).values.first
    writer.close
    File.delete(writer.path)
    plugin.multi_receive_encoded([[event, "second\n"]])

    expect(dlq).to have_received(:write).with(anything, /create_if_deleted is false/).once
    expect(File.exist?(plugin.failure_path)).to be(false)
  end

  it 'keeps the cap global across concurrent pipeline workers using real file writers' do
    plugin = output('dynamic_routing_max_open_files' => 2)
    start = Queue.new
    threads = 6.times.map do |index|
      Thread.new do
        start.pop
        plugin.multi_receive_encoded([[event('db', "table#{index}"), "#{index}\n"]])
      end
    end
    6.times { start << true }
    threads.each(&:value)

    expect(writers(plugin).length).to eq(2)
    expect(dlq).to have_received(:write).with(anything, /open temporary file limit/).exactly(4).times
    expect(writers(plugin).values.map { |writer| File.binread(writer.path).lines.length }).to eq([1, 1])
  end

  it 'rejects names that fit only before the generation token is added' do
    plugin = output
    candidate = event('db', 'x', nil)
    base = File.basename(plugin.send(:generate_filepath, candidate)).bytesize - 1
    candidate.set('kusto_table', 'a' * (255 - base))

    expect(plugin.send(:event_path, candidate)).to be_nil
    expect(dlq).to have_received(:write).with(candidate, /filesystem limit/)
  end

  %w[static dynamic].each do |mode|
    %i[open write flush].each do |operation|
      it "propagates #{mode} #{operation} failures instead of acknowledging unwritten events" do
        overrides = mode == 'static' ? { 'database' => 'db', 'table' => 'orders', 'json_mapping' => 'map' } : {}
        plugin = output(overrides)
        writer = double('failed writer', write: nil, flush: nil)
        allow(plugin).to receive(:open).and_return(writer)
        allow(operation == :open ? plugin : writer).to receive(operation).and_raise(Errno::ENOSPC)

        expect { plugin.multi_receive_encoded([[event, "payload\n"]]) }.to raise_error(Errno::ENOSPC)
      end
    end
  end

  it 'does not acknowledge a DLQ failure as a successfully rejected event' do
    plugin = output
    allow(dlq).to receive(:write).and_raise(IOError, 'DLQ unavailable')
    expect { plugin.multi_receive_encoded([[event('', 'table'), "payload\n"]]) }.to raise_error(IOError)
    expect(writers(plugin)).to be_empty
  end

  %w[database table json_mapping].each do |setting|
    it "rejects a whitespace-only static #{setting} in dynamic mode" do
      expect { output(setting => '   ') }.to raise_error(LogStash::ConfigurationError)
    end
  end

  %w[database table mapping].each_with_index do |setting, index|
    it "rejects whitespace-only and malformed UTF-8 resolved #{setting}" do
      ['%20%20', '%FF'].each do |value|
        segments = %w[db orders map]
        segments[index] = value
        expect(described_class.decode_routing_target("out.kusto~#{segments.join('~')}")).to be_nil
      end
    end
  end

  %w[dynamic_routing_max_open_files dynamic_routing_open_files_warning_threshold].each do |setting|
    [-1, 0.5, 1.5, Float::INFINITY, Float::NAN].each do |value|
      it "rejects non-integral or non-finite #{setting}=#{value}" do
        expect { output(setting => value) }.to raise_error(LogStash::ConfigurationError)
      end
    end
  end

  it 'does not treat a POSIX sibling containing a literal backslash as a child directory' do
    skip 'POSIX filename semantics' if Gem.win_platform?
    plugin = output
    root = plugin.instance_variable_get(:@file_root)
    expect(plugin.send(:inside_file_root?, "#{root}\\sibling/file.json")).to be(false)
  end

  it 'closes an externally unlinked active descriptor before replacing its generation on POSIX' do
    skip 'POSIX unlink semantics' if Gem.win_platform?
    plugin = output
    plugin.multi_receive_encoded([[event, "first\n"]])
    original = writers(plugin).values.first
    File.unlink(original.path)
    plugin.multi_receive_encoded([[event, "second\n"]])

    expect(original.closed?).to be(true)
    expect(File.binread(writers(plugin).values.first.path)).to eq("second\n")
  end

  it 'does not hand an unclosed writer to the uploader when closing fails' do
    plugin = output
    plugin.multi_receive_encoded([[event, "first\n"]])
    writer = writers(plugin).values.first
    allow(writer).to receive(:close).and_raise(IOError, 'close failed')
    expect { retire(plugin) }.to raise_error(IOError, 'close failed')
    expect(uploads).to be_empty
    expect(writers(plugin).values).to eq([writer])
    allow(writer).to receive(:close).and_call_original
  end

  it 'accepts a physical basename exactly at the byte budget, but rejects the next byte' do
    plugin = output('json_mapping' => nil)
    candidate = event('db', 'x')
    base = File.basename(plugin.send(:generate_filepath, candidate)).bytesize - 1
    allowed = 255 - base - described_class::ROUTING_GENERATION_BYTES
    candidate.set('kusto_table', 'a' * allowed)
    plugin.multi_receive_encoded([[candidate, "fits\n"]])
    expect(File.basename(writers(plugin).values.first.path).bytesize).to eq(255)
    candidate.set('kusto_table', 'a' * (allowed + 1))
    expect(plugin.send(:event_path, candidate)).to be_nil
  end

  it 'runs the original issue through the real JSON codec, executor and Java properties without network access' do
    allow(described_class::Ingestor).to receive(:new).and_call_original
    factory = Java::com.microsoft.azure.kusto.ingest.IngestClientFactory
    client = double('SDK boundary', close: nil)
    received = Queue.new
    allow(factory).to receive(:createClient).and_return(client)
    allow(client).to receive(:ingestFromFile) do |source, properties|
      received << {
        database: properties.getDatabaseName, table: properties.getTableName,
        mapping: properties.getIngestionMapping&.getIngestionMappingReference,
        format: properties.getDataFormat.to_s,
        events: File.readlines(source.getFilePath).map { |line| LogStash::Json.load(line) }
      }
      nil
    end
    plugin = output('table' => '%{[app]}_%{[event_type]}')
    destinations = [
      ['db', 'Orders', 'created', 'one'], ['db', 'orders', 'created', 'one'],
      ['other_db', 'Orders', 'created', 'one'], ['db', 'Orders', 'created', 'two']
    ]
    events = destinations.each_with_index.map do |(db, app, type, mapping), index|
      ev = event(db, nil, mapping)
      ev.set('app', app)
      ev.set('event_type', type)
      ev.set('id', index)
      ev
    end
    plugin.multi_receive(events)
    plugin.close # Drains actual asynchronous workers and deletes uploaded files.
    @outputs.delete(plugin)

    expect(received.size).to eq(destinations.length)
    actual = destinations.length.times.map { received.pop }.sort_by { |item| item[:events].first['id'] }
    actual.each_with_index do |item, index|
      db, app, type, mapping = destinations[index]
      expect(item.values_at(:database, :table, :mapping)).to eq([db, "#{app}_#{type}", mapping])
      expect(item[:format]).to eq('JSON')
      expect(item[:events].map { |payload| payload['id'] }).to eq([index])
      expect(item[:events].first).not_to have_key('@metadata')
    end
    expect(Dir.children(@directory)).to be_empty
    expect(client).to have_received(:close).once
  end
end