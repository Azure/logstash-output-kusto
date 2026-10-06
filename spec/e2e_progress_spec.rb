# encoding: utf-8
require_relative 'spec_helpers'
require_relative '../e2e/e2e'
require 'json'

describe E2E, 'readiness and live ingestion evidence' do
  let(:harness) { described_class.new }
  let(:pid) { 4242 }
  let(:client) { double('query client', close: nil) }
  let(:rows) { CSV.read(harness.instance_variable_get(:@csv_file)) }

  before do
    @now = 10.0
    @alive = true
    @directory = harness.instance_variable_get(:@work_directory)
    FileUtils.mkdir_p(@directory)
    @log = File.join(@directory, 'logstash.log')
    harness.instance_variable_set(:@logstash_log, @log)
    harness.instance_variable_set(:@logstash_pid, pid)
    harness.instance_variable_set(:@logstash_process_group, false)
    harness.instance_variable_set(:@query_client, client)
    harness.instance_variable_set(:@require_second_database, false)
    [@log, harness.instance_variable_get(:@input_file), harness.instance_variable_get(:@output_file)].each do |path|
      File.write(path, '')
    end
    allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { @now }
    allow(harness).to receive(:sleep) { |seconds| @now += seconds }
    allow(harness).to receive(:warn)
    status = instance_double(Process::Status, success?: true, exitstatus: 0, termsig: nil)
    allow(Process).to receive(:waitpid2).with(pid, Process::WNOHANG) { @alive ? nil : [pid, status] }
    allow(Process).to receive(:kill).with(0, anything) { raise Errno::ESRCH unless @alive; 1 }
    allow(Process).to receive(:kill).with('TERM', anything) { @alive = false; 1 }
  end

  after do
    FileUtils.remove_entry(@directory)
  end

  def output_rows(values)
    values.map { |row| JSON.generate('rownumber' => row[0], 'rowguid' => row[1]) + "\n" }.join
  end

  it 'waits for pipeline readiness rather than a fixed startup sleep' do
    File.write(@log, 'Starting Logstash')
    allow(harness).to receive(:sleep) do |seconds|
      @now += seconds
      File.write(@log, '[INFO ][logstash.javapipeline][main] Pipeline started')
    end

    harness.wait_for_readiness(2)

    expect(harness).to have_received(:sleep).once
    expect(File.read(harness.instance_variable_get(:@input_file))).to eq('')
  end

  it 'reports a bounded readiness timeout with diagnostic paths' do
    expect { harness.wait_for_readiness(0) }
      .to raise_error(E2E::ProgressError, /Timed out waiting for pipeline readiness/)
    expect(harness).to have_received(:warn).with(/logstash\.log/)
  end

  it 'retains readiness evidence even when later startup messages exceed the diagnostic tail' do
    File.write(@log, "Pipeline started\n" + "later startup message\n" * 2_000)

    expect { harness.wait_for_readiness(0) }.not_to raise_error
    expect(harness).not_to have_received(:sleep)
  end

  it 'does not accept a readiness log after the process has already exited' do
    File.write(@log, 'Pipeline started')
    @alive = false

    expect { harness.wait_for_readiness(1) }.to raise_error(E2E::ProgressError, /exited before/)
    expect(harness.instance_variable_get(:@logstash_pid)).to eq(pid)
  end

  it 'detects a reaped leader even when other members of its process group remain' do
    harness.instance_variable_set(:@logstash_process_group, true)
    status = instance_double(Process::Status, success?: true, exitstatus: 0, termsig: nil)
    allow(Process).to receive(:waitpid2).with(pid, Process::WNOHANG).and_return([pid, status])

    expect { harness.wait_for_readiness(0) }.to raise_error(E2E::ProgressError, /exited before/)
    expect(harness.instance_variable_get(:@logstash_pid)).to eq(pid)
    expect(harness.instance_variable_get(:@logstash_terminated)).not_to be(true)
  end

  it 'waits for every expected row identity, ignoring an incomplete last JSON line' do
    output = harness.instance_variable_get(:@output_file)
    File.write(output, output_rows(rows.take(1)) + '{"rownumber":')
    allow(harness).to receive(:sleep) do |seconds|
      @now += seconds
      File.write(output, output_rows(rows.reverse))
    end

    harness.wait_for_input(2)

    expect(harness).to have_received(:sleep).once
  end

  %i[duplicate wrong_guid].each do |kind|
    it "rejects #{kind} input evidence even when the row count matches" do
      actual = rows.map(&:dup)
      kind == :duplicate ? actual[-1] = actual.first : actual[-1][1] = 'wrong-guid'
      File.write(harness.instance_variable_get(:@output_file), output_rows(actual))

      expect { harness.wait_for_input(1) }.to raise_error(E2E::ProgressError, /does not match the input/)
    end
  end

  it 'reports incomplete consumption instead of proceeding to shutdown' do
    File.write(harness.instance_variable_get(:@output_file), output_rows(rows.take(1)))

    expect { harness.wait_for_input(0) }.to raise_error(E2E::ProgressError, /Timed out waiting for input consumption/)
    expect(harness).to have_received(:warn).with(/observed_rows=1/)
  end

  it 'uses one shared deadline for all four pre-shutdown destination checks' do
    calls = []
    allow(harness).to receive(:validate_table_rows) do |_table, _rows, _attempts, **options|
      calls << options
      @now += 1
    end

    harness.assert_data(while_running: true)

    expect(calls.length).to eq(4)
    expect(calls.map { |options| options[:while_running] }).to eq([true] * 4)
    expect(calls.map { |options| options[:deadline] }.uniq.length).to eq(1)
  end

  it 'does not accept rows uploaded by shutdown after the writer exits' do
    result = double('result', count: 0)
    allow(client).to receive(:executeQuery) do
      @alive = false
      double('query', getPrimaryResults: result)
    end

    expect { harness.validate_table_rows('table', [], 1, while_running: true) }
      .to raise_error(E2E::ProgressError, /exited before/)
  end

  it 'fails before querying when the live process already exited' do
    @alive = false
    expect(client).not_to receive(:executeQuery)

    expect { harness.validate_table_rows('table', [], 1, while_running: true) }
      .to raise_error(E2E::ProgressError, /exited before/)
  end

  it 'does not issue a query after the shared ingestion deadline' do
    expect(client).not_to receive(:executeQuery)

    expect { harness.validate_table_rows('table', [], 120, deadline: @now) }
      .to raise_error('Failed after timeouts validating table table')
  end

  it 'sets a real SDK timeout bounded by the remaining deadline' do
    result = double('result', count: 0)
    allow(client).to receive(:executeQuery) do |_database, _query, properties|
      expect(properties).to be_a(Java::com.microsoft.azure.kusto.data.ClientRequestProperties)
      expect(properties.getTimeoutInMilliSec).to be > 0
      expect(properties.getTimeoutInMilliSec).to be <= 2_000
      double('query', getPrimaryResults: result)
    end

    harness.validate_table_rows('table', [], 1, deadline: @now + 2)
    expect(client).to have_received(:executeQuery).once
  end

  context 'run sequencing' do
    before do
      harness.instance_variable_set(:@logstash_pid, nil)
      harness.instance_variable_set(:@engine_url, 'https://test.kusto.windows.net')
      allow(harness).to receive(:spawn).and_return(pid)
      allow($kusto_java.data.ClientFactory).to receive(:createClient).and_return(client)
      allow(harness).to receive(:create_table_and_mapping)
      allow(harness).to receive(:drop_and_cleanup)
    end

    it 'proves idle delivery while running, then confirms shutdown and reconciles again' do
      order = []
      allow(harness).to receive(:wait_for_readiness) do
        expect(File.read(harness.instance_variable_get(:@input_file))).to eq('')
        order << :ready
      end
      allow(harness).to receive(:wait_for_input) do
        expect(File.read(harness.instance_variable_get(:@input_file)))
          .to eq(File.read(harness.instance_variable_get(:@csv_file)))
        order << :consumed
      end
      allow(harness).to receive(:assert_data) do |while_running: false|
        expect(harness.instance_variable_get(:@logstash_pid).nil?).to eq(!while_running)
        order << (while_running ? :live : :final)
      end
      allow(Process).to receive(:kill).with('TERM', anything) { order << :term; @alive = false; 1 }

      harness.start

      expect(order).to eq(%i[ready consumed live term final])
      expect(client).to have_received(:close).once
    end

    it 'cannot convert failed idle delivery into a pass through shutdown uploads' do
      allow(harness).to receive(:wait_for_readiness)
      allow(harness).to receive(:wait_for_input)
      expect(harness).to receive(:assert_data).with(while_running: true).and_raise('idle delivery failed')
      expect(harness).not_to receive(:assert_data).with(no_args)

      expect { harness.start }.to raise_error('idle delivery failed')
      expect(Process).to have_received(:kill).with('TERM', anything).once
      expect(harness).to have_received(:drop_and_cleanup).once
      expect(client).to have_received(:close).once
    end
  end

  context 'cross-database qualification' do
    [nil, '', 'db', 'DB', '$(TEST_SECOND_DATABASE)'].each do |second|
      it "rejects an unqualified second database #{second.inspect} before creating a client" do
        harness.instance_variable_set(:@database, 'db')
        harness.instance_variable_set(:@even_database, second)
        harness.instance_variable_set(:@require_second_database, true)
        expect($kusto_java.data.ClientFactory).not_to receive(:createClient)

        expect { harness.start }.to raise_error(ArgumentError, /TEST_SECOND_DATABASE.*distinct/)
      end
    end

    it 'accepts explicitly distinct databases without provisioning either one' do
      harness.instance_variable_set(:@database, 'db')
      harness.instance_variable_set(:@even_database, 'other_db')
      harness.instance_variable_set(:@require_second_database, true)
      expect(client).not_to receive(:executeMgmt)

      expect { harness.validate_test_databases }.not_to raise_error
      expect(harness.destinations.last(2).map(&:first)).to eq(%w[db other_db])
    end

    it 'labels an optional single-database smoke run as lacking cross-database coverage' do
      harness.instance_variable_set(:@database, 'db')
      harness.instance_variable_set(:@even_database, 'db')

      harness.validate_test_databases

      expect(harness).to have_received(:warn).with(/cross-database routing is NOT covered/)
    end

    it 'uses the required second database environment settings in the generated config' do
      allow(ENV).to receive(:[]).and_call_original
      allow(ENV).to receive(:[]).with('TEST_DATABASE').and_return('db')
      allow(ENV).to receive(:[]).with('E2E_REQUIRE_SECOND_DATABASE').and_return('true')
      allow(ENV).to receive(:fetch).and_call_original
      allow(ENV).to receive(:fetch).with('TEST_SECOND_DATABASE', 'db').and_return('other_db')
      configured = described_class.new

      expect { configured.validate_test_databases }.not_to raise_error
      expect(configured.destinations.last(2).map(&:first)).to eq(%w[db other_db])
      expect(configured.instance_variable_get(:@logstash_config)).to include("rn.odd? ? 'db' : 'other_db'")
      expect(configured.instance_variable_get(:@require_second_database)).to be(true)
    end
  end
end