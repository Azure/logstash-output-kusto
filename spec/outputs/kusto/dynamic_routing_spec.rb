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

  def join_workers(threads)
    errors = []
    threads.each do |thread|
      begin
        thread.value
      rescue Exception => e # RSpec assertion failures must not skip joining later workers.
        errors << e
      end
    end
    raise errors.first unless errors.empty?
  end

  def retire(plugin)
    writers(plugin).each_value { |writer| writer.active = false }
    plugin.instance_variable_set(:@last_stale_cleanup_cycle, Time.now - 60)
    plugin.send(:close_stale_files)
  end

  def unmapped_event(kind, table = 'orders')
    ev = event('db', table, nil)
    ev.remove('[@metadata][mapping]') if kind == :missing
    ev.set('[@metadata][mapping]', '') if kind == :empty
    ev.set('id', kind.to_s)
    ev
  end

  def stored_ids(plugin)
    writers(plugin).values.flat_map do |writer|
      File.readlines(writer.path).map { |line| LogStash::Json.load(line)['id'] }
    end
  end

  context 'optional mapping normalization' do
    [0, 1].product([true, false], [true, false]).each do |cap, dlq_enabled, single_batch|
      it "shares one writer with cap=#{cap}, DLQ=#{dlq_enabled}, single_batch=#{single_batch}, in every order" do
        %i[missing null empty].permutation.each do |order|
          # Event-driven cleanup is intentionally not due during these batches.
          plugin = output('dynamic_routing_max_open_files' => cap, 'stale_cleanup_interval' => 60_000)
          plugin.instance_variable_set(:@dlq_writer, nil) unless dlq_enabled
          events = order.map { |kind| unmapped_event(kind) }
          if single_batch
            plugin.multi_receive(events)
          else
            events.each { |ev| plugin.multi_receive([ev]) }
          end

          expect(events.map { |ev| plugin.send(:generate_filepath, ev) }.uniq.length).to eq(1)
          expect(writers(plugin).length).to eq(1)
          expect(stored_ids(plugin)).to contain_exactly('missing', 'null', 'empty')
        end
        expect(dlq).not_to have_received(:write)
        expect(logger).not_to have_received(:warn).with(/event\(s\).*could not be routed/)
      end
    end

    [255, 256].each do |physical_bytes|
      it "applies the same #{physical_bytes}-byte filename boundary to every absent mapping form" do
        plugin = output('dynamic_routing_max_open_files' => 1)
        empty = unmapped_event(:empty, 'x')
        overhead = File.basename(plugin.send(:generate_filepath, empty)).bytesize - 1 +
                   described_class::ROUTING_GENERATION_BYTES
        table = 'a' * (physical_bytes - overhead)
        events = %i[missing null empty].map { |kind| unmapped_event(kind, table) }
        sizes = events.map do |ev|
          File.basename(plugin.send(:generate_filepath, ev)).bytesize + described_class::ROUTING_GENERATION_BYTES
        end
        expect(sizes).to eq([physical_bytes] * 3)
        plugin.multi_receive(events)

        if physical_bytes == 255
          expect(writers(plugin).length).to eq(1)
          expect(File.basename(writers(plugin).values.first.path).bytesize).to eq(255)
          expect(stored_ids(plugin)).to contain_exactly('missing', 'null', 'empty')
          expect(dlq).not_to have_received(:write)
        else
          expect(writers(plugin)).to be_empty
          events.each { |ev| expect(dlq).to have_received(:write).with(ev, /filesystem limit/).once }
        end
      end
    end

    it 'shares one cap slot across concurrent workers with different absent mapping forms' do
      plugin = output('dynamic_routing_max_open_files' => 1, 'stale_cleanup_interval' => 60_000)
      start = Queue.new
      threads = (%i[missing null empty] * 3).each_with_index.map do |kind, index|
        Thread.new do
          ev = unmapped_event(kind)
          ev.set('id', index)
          start.pop
          plugin.multi_receive([ev])
        end
      end
      threads.length.times { start << true }
      join_workers(threads)

      expect(writers(plugin).length).to eq(1)
      expect(stored_ids(plugin)).to contain_exactly(*(0...9).to_a)
      expect(dlq).not_to have_received(:write)
    end

    it 'normalizes resolved values, not templates, and keeps real mappings, destinations and windows separate' do
      plugin = output
      events = %i[missing null empty].map { |kind| unmapped_event(kind) }
      %w[Map map].each { |mapping| events << event('db', 'orders', mapping) }
      events << unmapped_event(:missing, 'other_table')
      other_database = unmapped_event(:empty)
      other_database.set('target_database', 'other_db')
      events << other_database
      later = unmapped_event(:null)
      later.set('@timestamp', LogStash::Timestamp.new(Time.utc(2026, 9, 15, 1, 1)))
      events << later
      plugin.multi_receive(events)

      expect(writers(plugin).length).to eq(6)
      targets = writers(plugin).values.map { |writer| described_class.decode_routing_target(writer.path) }
      expect(targets.count { |target| target == { database: 'db', table: 'orders', mapping: nil } }).to eq(2)
      expect(targets.map { |target| target[:mapping] }.compact).to contain_exactly('Map', 'map')
      expect(dlq).not_to have_received(:write)
    end

    [
      '%{mapping}', '%{[mapping]}', "%{[#{'m' * 1019}]}"
    ].each do |template|
      it "normalizes a missing exact reference of #{template.length} characters without charging it to the filename" do
        plugin = output('json_mapping' => template)
        ev = unmapped_event(:missing)
        path = plugin.send(:generate_filepath, ev)
        expect(path).to end_with('.kusto~db~orders~')
        plugin.multi_receive([ev])
        expect(writers(plugin).length).to eq(1)
        expect(dlq).not_to have_received(:write)
      end
    end

    ['prefix_%{missing}', '%{missing}_suffix', '%{one}%{two}', "%{missing}\n",
     '%{}', '%{missing', '   ', 'bad/mapping', '%2F', "%{#{'m' * 1022}}"].each do |value|
      it "does not normalize an invalid resolved mapping #{value.length > 80 ? '(overlong reference)' : value.inspect}" do
        plugin = output
        ev = event('db', 'orders', value)
        path = plugin.send(:generate_filepath, ev)
        expect(described_class.classify_routing_target(path).last).to eq('invalid json_mapping')
        plugin.multi_receive([ev])
        expect(writers(plugin)).to be_empty
        expect(dlq).to have_received(:write).with(ev, /json_mapping/).once
      end
    end

    it 'does not erase invalid UTF-8 bytes inside an exact-looking mapping reference' do
      plugin = output
      ev = unmapped_event(:missing)
      # Exercise malformed bytes at the sprintf boundary, without Java sanitizing the event string.
      invalid = "%{\xFF}".b
      allow(ev).to receive(:sprintf).and_call_original
      allow(ev).to receive(:sprintf).with('%{[@metadata][mapping]}').and_return(invalid)
      plugin.multi_receive_encoded([[ev, "payload\n"]])

      expect(writers(plugin)).to be_empty
      expect(dlq).to have_received(:write).with(ev, /invalid UTF-8/).once
      expect(invalid.encoding).to eq(Encoding::ASCII_8BIT)
    end

    it 'preserves literal percent escapes until validation rather than decoding them a second time' do
      plugin = output
      ev = event('db', 'orders', '%25%7Bmissing%7D')
      plugin.multi_receive([ev])
      expect(writers(plugin)).to be_empty
      expect(dlq).to have_received(:write).with(ev, /json_mapping/).once
    end

    it 'rejects a missing field in a composite template, but preserves its resolved literal when the field is empty' do
      plugin = output('json_mapping' => 'prefix_%{[@metadata][mapping]}')
      missing = unmapped_event(:missing)
      empty = unmapped_event(:empty)
      plugin.multi_receive([missing, empty])

      expect(dlq).to have_received(:write).with(missing, /composite/).once
      expect(writers(plugin).length).to eq(1)
      expect(described_class.decode_routing_target(writers(plugin).values.first.path)[:mapping]).to eq('prefix_')
      expect(stored_ids(plugin)).to eq(['empty'])
    end

    it 'recovers legacy unresolved and empty mapping filenames without changing ownership or appending to them' do
      original = output
      owner = original.instance_variable_get(:@routing_owner_tag)
      encoded = described_class.encode_routing_segment('%{[@metadata][mapping]}')
      legacy = File.join(@directory, "legacy#{owner}.kusto~db~orders~#{encoded}")
      empty = File.join(@directory, "empty#{owner}.kusto~db~orders~")
      [legacy, empty].each { |path| File.write(path, "old\n") }
      restarted = output('recovery' => true)

      expect(restarted.instance_variable_get(:@routing_owner_tag)).to eq(owner)
      expect(uploads).to contain_exactly(legacy, empty)
      [legacy, empty].each do |path|
        expect(described_class.decode_routing_target(path)).to eq(database: 'db', table: 'orders', mapping: nil)
      end
      restarted.multi_receive(%i[missing null empty].map { |kind| unmapped_event(kind) })
      expect(writers(restarted).length).to eq(1)
      expect([legacy, empty]).not_to include(writers(restarted).values.first.path)
      expect(stored_ids(restarted)).to contain_exactly('missing', 'null', 'empty')
      [legacy, empty].each { |path| expect(File.read(path)).to eq("old\n") }
    end
  end

  context 'warning re-arming after cleanup' do
    it 'warns again after background cleanup between bursts, without an extra below-threshold receive call' do
      # No automatic timer: run the real cleanup entry point on a separate thread, deterministically.
      plugin = output('stale_cleanup_type' => 'interval', 'stale_cleanup_interval' => 0,
                      'dynamic_routing_open_files_warning_threshold' => 1)
      plugin.multi_receive([unmapped_event(:empty)])
      Thread.new do
        plugin.send(:close_stale_files) # Marks active writers inactive.
        plugin.send(:close_stale_files) # Removes the inactive writers.
      end.value

      expect(writers(plugin)).to be_empty
      expect(plugin.instance_variable_get(:@open_files_warning_active)).to be(false)
      plugin.multi_receive([unmapped_event(:empty)])
      expect(logger).to have_received(:warn).with(/Dynamic routing currently/, anything).twice
    end

    it 'warns again when cap-driven cleanup and refill happen in the same receive call' do
      plugin = output('dynamic_routing_max_open_files' => 1, 'dynamic_routing_open_files_warning_threshold' => 1)
      plugin.multi_receive([unmapped_event(:empty)])
      writers(plugin).each_value { |writer| writer.active = false }
      plugin.instance_variable_set(:@last_stale_cleanup_cycle, Time.now - 60)
      plugin.multi_receive([unmapped_event(:missing)])

      expect(writers(plugin).length).to eq(1)
      expect(uploads.length).to eq(1)
      expect(logger).to have_received(:warn).with(/Dynamic routing currently/, anything).twice
      expect(dlq).not_to have_received(:write)
    end

    it 'does not re-arm when cleanup leaves the count exactly at the threshold' do
      plugin = output('stale_cleanup_type' => 'interval', 'stale_cleanup_interval' => 0,
                      'dynamic_routing_open_files_warning_threshold' => 2)
      plugin.multi_receive(%w[one two three].map { |table| event('db', table) })
      writers(plugin).values.first.active = false
      plugin.send(:close_stale_files)

      expect(writers(plugin).length).to eq(2)
      expect(plugin.instance_variable_get(:@open_files_warning_active)).to be(true)
      plugin.multi_receive([event('db', 'four')])
      expect(logger).to have_received(:warn).with(/Dynamic routing currently/, anything).once
    end

    it 're-arms before enqueue so an upload handoff failure does not lose the below-threshold transition' do
      plugin = output('dynamic_routing_open_files_warning_threshold' => 1)
      plugin.multi_receive([event])
      allow(uploader).to receive(:upload_async).and_raise(Concurrent::RejectedExecutionError)
      expect { retire(plugin) }.to raise_error(Concurrent::RejectedExecutionError)
      expect(writers(plugin)).to be_empty
      expect(plugin.instance_variable_get(:@open_files_warning_active)).to be(false)

      allow(uploader).to receive(:upload_async) { |path, _delete| uploads << path }
      plugin.multi_receive([event])
      expect(logger).to have_received(:warn).with(/Dynamic routing currently/, anything).twice
    end

    it 'keeps warnings disabled across cleanup when the threshold is zero' do
      plugin = output('dynamic_routing_open_files_warning_threshold' => 0)
      plugin.multi_receive([event])
      retire(plugin)
      plugin.multi_receive([event])
      expect(logger).not_to have_received(:warn).with(/Dynamic routing currently/, anything)
    end
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
    join_workers(threads)

    expect(writers(plugin).length).to eq(2)
    expect(dlq).to have_received(:write).with(anything, /open temporary file limit/).exactly(4).times
    expect(writers(plugin).values.map { |writer| File.binread(writer.path).lines.length }).to eq([1, 1])
  end

  [RuntimeError, RSpec::Expectations::ExpectationNotMetError].each do |error_class|
    it "joins every worker before re-raising #{error_class}" do
      first, second, last = Array.new(3) { double('worker') }
      failure = error_class.new('first worker failed')
      expect(first).to receive(:value).ordered.and_raise(failure)
      expect(second).to receive(:value).ordered.and_raise('another worker failed')
      expect(last).to receive(:value).ordered.and_return(nil)

      expect { join_workers([first, second, last]) }.to raise_error { |error| expect(error).to equal(failure) }
    end
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