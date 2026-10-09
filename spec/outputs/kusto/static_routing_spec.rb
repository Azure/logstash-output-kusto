# encoding: utf-8
require_relative '../../spec_helpers'
require 'tmpdir'
require 'timeout'

describe LogStash::Outputs::Kusto, 'static queued preparation' do
  let(:logger) { spy('logger') }
  let(:uploader) { double('offline uploader', upload_async: nil, stop: nil) }

  before do
    @directory = Dir.mktmpdir('kusto-static-routing')
    @outputs = []
    allow(described_class::Ingestor).to receive(:new).and_return(uploader)
  end

  after do
    @outputs.each(&:close)
    FileUtils.remove_entry(@directory)
  end

  def output(overrides = {})
    plugin = described_class.new({
      'path' => "#{@directory}/%{[host]}/out-%{+YYYY-MM-dd-HH-mm}",
      'ingest_url' => 'https://ingest-offline.kusto.windows.net',
      'app_id' => 'test', 'app_key' => 'test', 'app_tenant' => 'test',
      'database' => 'db', 'table' => 'orders', 'json_mapping' => 'map',
      'flush_interval' => 0, 'recovery' => false, 'stale_cleanup_interval' => 60_000
    }.merge(overrides))
    plugin.instance_variable_set(:@logger, logger)
    plugin.register
    @outputs << plugin
    plugin
  end

  def event(id, host = 'host')
    LogStash::Event.new('id' => id, 'host' => host, '@timestamp' => '2026-09-18T12:00:00Z')
  end

  def ids(path)
    File.readlines(path).map { |line| LogStash::Json.load(line)['id'] }
  end

  def join_workers(threads)
    errors = []
    threads.each do |thread|
      begin
        thread.value
      rescue Exception => e # Join remaining workers even when an RSpec expectation raises.
        errors << e
      end
    end
    raise errors.first unless errors.empty?
  end

  [true, false].each do |recreate|
    it "prepares paths outside the lock but checks and writes shared state inside it (recreate=#{recreate})" do
      plugin = output('create_if_deleted' => recreate)
      mutex = plugin.instance_variable_get(:@io_mutex)
      ev = event(1)
      allow(ev).to receive(:sprintf).and_wrap_original do |method, *args|
        expect(mutex.owned?).to be(false)
        method.call(*args)
      end
      allow(plugin).to receive(:inside_file_root?).and_wrap_original do |method, *args|
        expect(mutex.owned?).to be(false)
        method.call(*args)
      end
      %i[deleted? cached? close_stale_files_locked].each do |name|
        allow(plugin).to receive(name).and_wrap_original do |method, *args|
          expect(mutex.owned?).to be(true)
          method.call(*args)
        end
      end
      allow(plugin).to receive(:open).and_wrap_original do |method, *args|
        expect(mutex.owned?).to be(true)
        writer = method.call(*args)
        %i[write flush].each do |name|
          allow(writer).to receive(name).and_wrap_original do |operation, *values|
            expect(mutex.owned?).to be(true)
            operation.call(*values)
          end
        end
        writer
      end

      plugin.multi_receive([ev])
      path = recreate ? plugin.send(:generate_filepath, ev) : plugin.failure_path
      expect(ids(path)).to eq([1])
    end
  end

  it 'does not expand the fixed root again for any event after registration' do
    plugin = output
    root = plugin.instance_variable_get(:@file_root)
    allow(File).to receive(:expand_path).and_call_original
    expect(File).not_to receive(:expand_path).with(root)
    plugin.multi_receive(10.times.map { |i| event(i) })
    expect(ids(plugin.send(:generate_filepath, event(0)))).to eq((0...10).to_a)
  end

  it 'allows another worker to finish while one worker is still formatting an event' do
    plugin = output
    entered, release, finished = Queue.new, Queue.new, Queue.new
    slow_event = event(1)
    allow(slow_event).to receive(:sprintf).and_wrap_original do |method, *args|
      entered << true
      release.pop
      method.call(*args)
    end
    workers = []
    begin
      workers << Thread.new { plugin.multi_receive([slow_event]) }
      Timeout.timeout(10) { entered.pop }
      workers << Thread.new { plugin.multi_receive([event(2)]); finished << true }
      Timeout.timeout(10) { finished.pop }
    ensure
      release << true
      join_workers(workers)
    end
    expect(ids(plugin.send(:generate_filepath, event(0)))).to eq([2, 1])
  end

  it 'keeps every event and within-batch order across concurrent workers and eight paths' do
    plugin = output('dynamic_routing_max_open_files' => 1, 'dynamic_routing_open_files_warning_threshold' => 1)
    start = Queue.new
    workers = 4.times.map do |worker|
      Thread.new do
        start.pop
        plugin.multi_receive(128.times.map { |i| event(worker * 128 + i, "h#{i % 8}") })
      end
    end
    workers.length.times { start << true }
    join_workers(workers)
    paths = plugin.instance_variable_get(:@files).keys
    expect(paths.size).to eq(8)
    expect(paths).to all(end_with('out-2026-09-18-12-00.db.orders'))
    expect(paths.flat_map { |path| ids(path) }.sort).to eq((0...512).to_a)
    paths.each do |path|
      values = ids(path)
      4.times do |worker|
        batch_ids = values.select { |id| id / 128 == worker }
        expect(batch_ids).to eq(batch_ids.sort)
      end
    end
    expect(logger).not_to have_received(:warn).with(/Dynamic routing currently/, anything)
  end

  it 'preserves relative paths, event timestamps, and input events' do
    relative = "./kusto-static-relative-#{File.basename(@directory)}/%{[host]}/out-%{+YYYY-MM-dd-HH-mm}"
    plugin = output('path' => relative)
    ev = event(7, 'original')
    original = ev.to_hash
    begin
      plugin.multi_receive([ev])
      path = File.expand_path(relative.sub('%{[host]}', 'original').sub('%{+YYYY-MM-dd-HH-mm}', '2026-09-18-12-00') + '.db.orders')
      expect(ids(path)).to eq([7])
      expect(ev.to_hash).to eq(original)
    ensure
      plugin.close
      @outputs.delete(plugin)
      FileUtils.remove_entry(File.expand_path(relative.split('/%{').first))
    end
  end

  it 'keeps traversal and sibling-prefix paths in the static failure file' do
    plugin = output
    plugin.multi_receive([event(1, '../outside'), event(2, "../#{File.basename(@directory)}-evil")])
    expect(plugin.instance_variable_get(:@files).keys).to eq([plugin.failure_path])
    expect(ids(plugin.failure_path)).to eq([1, 2])
  end

  it 'preserves POSIX backslashes as filename characters during containment checks' do
    skip 'POSIX path semantics' if Gem.win_platform?
    plugin = output
    root = plugin.instance_variable_get(:@file_root)
    expect(plugin.send(:inside_file_root?, "#{root}\\sibling/file")).to be(false)
    expect(plugin.send(:inside_file_root?, "#{root}/nested\\name/file")).to be(true)
  end

  it 'preserves input order when interleaved missing paths converge on the failure file' do
    plugin = output('create_if_deleted' => false)
    plugin.multi_receive([event(1, 'a'), event(2, 'b'), event(3, 'a'), event(4, '../outside'), event(5, 'b')])
    expect(ids(plugin.failure_path)).to eq([1, 2, 3, 4, 5])
    expect(plugin.instance_variable_get(:@files).keys).to eq([plugin.failure_path])
  end

  it 'appends to a pre-existing file when recreation is disabled' do
    plugin = output('create_if_deleted' => false)
    ev = event(2)
    path = plugin.send(:generate_filepath, ev)
    FileUtils.mkdir_p(File.dirname(path))
    File.write(path, "{\"id\":1}\n")
    plugin.multi_receive([ev])
    expect(ids(path)).to eq([1, 2])
    expect(File.exist?(plugin.failure_path)).to be(false)
  end

  [true, false].each do |exists_at_write|
    it "checks file existence after preparation, inside the lock (exists=#{exists_at_write})" do
      plugin = output('create_if_deleted' => false)
      ev = event(1)
      path = plugin.send(:generate_filepath, ev)
      FileUtils.mkdir_p(File.dirname(path))
      File.write(path, '') unless exists_at_write
      entered, release = Queue.new, Queue.new
      allow(plugin).to receive(:inside_file_root?).and_wrap_original do |method, *args|
        result = method.call(*args)
        entered << true
        release.pop
        result
      end
      worker = Thread.new { plugin.multi_receive([ev]) }
      begin
        Timeout.timeout(10) { entered.pop }
        exists_at_write ? File.write(path, '') : File.delete(path)
      ensure
        release << true
        join_workers([worker])
      end
      expect(ids(exists_at_write ? path : plugin.failure_path)).to eq([1])
    end
  end

  %i[open write flush].each do |operation|
    it "propagates foreground #{operation} failures" do
      plugin = output
      if operation == :open
        allow(plugin).to receive(:open).and_raise(Errno::ENOSPC)
      else
        plugin.multi_receive([event(0)])
        writer = plugin.instance_variable_get(:@files).values.first
        allow(writer).to receive(operation).and_raise(Errno::ENOSPC)
      end
      expect { plugin.multi_receive([event(1)]) }.to raise_error(Errno::ENOSPC)
    end
  end

  it 'keeps periodic flushing, writer closure, and interval cleanup under the lock' do
    plugin = output('stale_cleanup_type' => 'interval', 'stale_cleanup_interval' => 0)
    plugin.multi_receive([event(1)])
    writer = plugin.instance_variable_get(:@files).values.first
    mutex = plugin.instance_variable_get(:@io_mutex)
    %i[flush close].each do |operation|
      allow(writer).to receive(operation).and_wrap_original do |method, *args|
        expect(mutex.owned?).to be(true)
        method.call(*args)
      end
    end
    plugin.send(:flush_pending_files)
    Thread.new do
      plugin.send(:close_stale_files)
      plugin.send(:close_stale_files)
    end.value
    expect(plugin.instance_variable_get(:@files)).to be_empty
    expect(uploader).to have_received(:upload_async).with(writer.path, true).once
    expect(ids(writer.path)).to eq([1])
  end

  it 'lets cleanup retire a writer during preparation without reusing the closed descriptor' do
    plugin = output('stale_cleanup_type' => 'interval', 'stale_cleanup_interval' => 0)
    plugin.multi_receive([event(1)])
    original = plugin.instance_variable_get(:@files).values.first
    entered, release, cleaned = Queue.new, Queue.new, Queue.new
    ev = event(2)
    allow(ev).to receive(:sprintf).and_wrap_original do |method, *args|
      entered << true
      release.pop
      method.call(*args)
    end
    workers = []
    begin
      workers << Thread.new { plugin.multi_receive([ev]) }
      Timeout.timeout(10) { entered.pop }
      workers << Thread.new do
        plugin.send(:close_stale_files)
        plugin.send(:close_stale_files)
        cleaned << true
      end
      Timeout.timeout(10) { cleaned.pop }
      expect(original.closed?).to be(true)
    ensure
      release << true
      join_workers(workers)
    end
    current = plugin.instance_variable_get(:@files).values.first
    expect(current).not_to equal(original)
    expect(ids(current.path)).to eq([1, 2])
    # Static filenames still append; immutable upload generations are dynamic-only.
  end

  it 'preserves periodic-flusher behavior and custom encoded bytes' do
    plugin = output
    plugin.multi_receive_encoded([[event(0), "first|payload\n"]])
    writer = plugin.instance_variable_get(:@files).values.first
    flusher = double('running flusher', alive?: true, stop: nil)
    plugin.instance_variable_set(:@flusher, flusher)
    allow(writer).to receive(:flush).and_call_original
    plugin.multi_receive_encoded([[event(1), "second|payload\n"]])
    expect(writer).not_to have_received(:flush)
    plugin.send(:flush_pending_files)
    expect(writer).to have_received(:flush).once
    expect(File.binread(writer.path)).to eq("first|payload\nsecond|payload\n")
  end

  it 'runs due event-driven cleanup on an empty batch without opening a new file' do
    plugin = output('stale_cleanup_interval' => 0)
    plugin.multi_receive([event(1)])
    original = plugin.instance_variable_get(:@files).values.first
    plugin.multi_receive_encoded([])
    expect(plugin.instance_variable_get(:@files)).to be_empty
    expect(uploader).to have_received(:upload_async).with(original.path, true).once
    expect(ids(original.path)).to eq([1])
  end

  it 'recovers static files under a relative configured path without scanning outside its root' do
    relative_root = "./kusto-static-recovery-#{File.basename(@directory)}"
    FileUtils.mkdir_p(relative_root)
    mine = File.expand_path(File.join(relative_root, 'old.db.orders'))
    File.write(mine, "{}\n")
    begin
      output('path' => "#{relative_root}/out-%{+YYYY-MM-dd-HH-mm}", 'recovery' => true)
      expect(uploader).to have_received(:upload_async).with(mine, true).once
      expect(uploader).to have_received(:upload_async).once
    ensure
      FileUtils.remove_entry(relative_root)
    end
  end

  it 'does not use the static branch for dynamic destinations' do
    plugin = output('table' => '%{[host]}', 'dynamic_routing_max_open_files' => 1)
    dlq = spy('DLQ')
    plugin.instance_variable_set(:@dlq_writer, dlq)
    ev = event(1, 'one')
    allow(ev).to receive(:sprintf).and_wrap_original do |method, *args|
      expect(plugin.instance_variable_get(:@io_mutex).owned?).to be(true)
      method.call(*args)
    end
    plugin.multi_receive([ev, event(2, 'two')])
    expect(plugin.instance_variable_get(:@files).size).to eq(1)
    expect(dlq).to have_received(:write).with(anything, /open temporary file limit/).once
  end

  it 'retains literal destination matching and ignores directories during static recovery' do
    plugin = output('database' => 'db.prod', 'table' => 'orders.live')
    mine = File.join(@directory, 'old.db.prod.orders.live')
    File.write(mine, "{}\n")
    File.write(File.join(@directory, 'old.dbXprod.ordersXlive'), "{}\n")
    Dir.mkdir(File.join(@directory, 'directory.db.prod.orders.live'))
    plugin.send(:recover_past_files)
    expect(uploader).to have_received(:upload_async).with(mine, true).once
    expect(uploader).to have_received(:upload_async).once
  end
end