# encoding: utf-8
require_relative 'spec_helpers'
require_relative '../e2e/e2e'

describe E2E do
  let(:harness) { described_class.new }
  let(:query_client) { double('query client') }
  let(:row) { CSV.read(File.expand_path('../e2e/dataset.csv', __dir__)).first }

  before do
    harness.instance_variable_set(:@query_client, query_client)
    allow(harness).to receive(:sleep)
  end

  def respond_with(values)
    results = double('results', count: 1, next: true)
    allow(results).to receive(:getObject) { |index| values[index] }
    allow(results).to receive(:getString) { |index| values[index].to_s }
    allow(query_client).to receive(:executeQuery).and_return(double('query', getPrimaryResults: results))
    results
  end

  def expected_values(mapped:)
    row.dup.tap do |values|
      values[4] = values[4] == '1' ? 'true' : 'false'
      values[12] = values[12].sub('.0000000', '')
      values[15] = values[1] if mapped
    end
  end

  it 'asserts the actual mapped column instead of replacing it with expected data' do
    respond_with(expected_values(mapped: false))
    expect do
      harness.validate_table_rows('routed_table', [row], 1, mapped: true)
    end.to raise_error(/column 15/)
  end

  [true, false].each do |mapped|
    it "accepts the correct column values with mapped=#{mapped}" do
      respond_with(expected_values(mapped: mapped))
      expect do
        harness.validate_table_rows('routed_table', [row], 1, mapped: mapped, database: 'routed_database')
      end.not_to raise_error
      expect(query_client).to have_received(:executeQuery).with('routed_database', /routed_table/)
    end
  end

  it 'uses separate mapping names for each dynamic destination and drains idle files' do
    destinations = harness.destinations
    expect(destinations.last(2).map(&:last).uniq.length).to eq(2)
    config = harness.instance_variable_get(:@logstash_config)
    expect(config.scan('stale_cleanup_type => "interval"').length).to eq(3)
    expect(config).to include('database => "%{[@metadata][kusto_database]}"')
    expect(config).to include('table => "%{[@metadata][kusto_table]}"')
    expect(config).to include('json_mapping => "%{[@metadata][kusto_mapping]}"')
  end

  it 'gives each test run independent table names and local file paths' do
    other = described_class.new
    expect(harness.destinations.map { |tuple| tuple[1] } & other.destinations.map { |tuple| tuple[1] }).to be_empty
    expect(harness.instance_variable_get(:@input_file)).not_to eq(other.instance_variable_get(:@input_file))
  end

  it 'validates both static tables and both routed subsets with the upstream ingestion retry allowance' do
    rows = CSV.read(harness.instance_variable_get(:@csv_file))
    mapped, unmapped, odd, even = harness.destinations
    expect(harness).to receive(:validate_table_rows).with(mapped[1], rows, 120, mapped: true).ordered
    expect(harness).to receive(:validate_table_rows).with(unmapped[1], rows, 120, mapped: false).ordered
    expect(harness).to receive(:validate_table_rows)
      .with(odd[1], rows.select { |item| item[0].to_i.odd? }, 120, mapped: true).ordered
    expect(harness).to receive(:validate_table_rows)
      .with(even[1], rows.select { |item| item[0].to_i.even? }, 120, mapped: true, database: even[0]).ordered

    harness.assert_data
  end

  it 'retries a failed query without trying to read an unavailable result' do
    ready = respond_with(expected_values(mapped: true))
    attempts = 0
    allow(query_client).to receive(:executeQuery) do
      attempts += 1
      raise 'temporary query failure' if attempts == 1
      double('query', getPrimaryResults: ready)
    end

    harness.validate_table_rows('routed_table', [row], 2, mapped: true)
    expect(attempts).to eq(2)
    expect(ready).to have_received(:next).once
  end

  it 'waits for the expected row count before reading and comparing data' do
    ready = respond_with(expected_values(mapped: false))
    partial = double('partial ingestion', count: 0)
    expect(partial).not_to receive(:next)
    allow(query_client).to receive(:executeQuery).and_return(
      double('partial query', getPrimaryResults: partial), double('ready query', getPrimaryResults: ready)
    )

    harness.validate_table_rows('static_table', [row], 2)
    expect(query_client).to have_received(:executeQuery).twice
    expect(ready).to have_received(:next).once
  end

  it 'fails after the retry limit instead of reporting a partially ingested table as successful' do
    partial = double('partial ingestion', count: 0)
    allow(query_client).to receive(:executeQuery).and_return(double('query', getPrimaryResults: partial))
    expect(partial).not_to receive(:next)

    expect { harness.validate_table_rows('incomplete_table', [row], 2) }
      .to raise_error('Failed after timeouts validating table incomplete_table')
    expect(query_client).to have_received(:executeQuery).twice
  end

  context 'Logstash process cleanup' do
    let(:pid) { 4242 }

    before do
      harness.instance_variable_set(:@lslocalpath, '/logstash with spaces/bin/logstash')
      allow(harness).to receive(:spawn).and_return(pid)
      allow(harness).to receive(:wait_for_exit).and_return(true)
      allow(Process).to receive(:kill)
    end

    after do
      FileUtils.rm_rf(harness.instance_variable_get(:@work_directory))
    end

    [false, true].each do |windows|
      it "stops Logstash before returning, preserving isolated paths (Windows=#{windows})" do
        allow(Gem).to receive(:win_platform?).and_return(windows)
        directory = harness.instance_variable_get(:@work_directory)
        arguments = ['/logstash with spaces/bin/logstash', '-f', File.join(directory, 'logstash.conf'),
                     '--path.data', File.join(directory, 'data')]
        process_options = windows ? {} : { pgroup: true }

        harness.run_logstash
        harness.stop_logstash # Cleanup from start's ensure must be idempotent.

        # Match keyword forwarding: Ruby 2.6 retains **{}, whereas Ruby 3 omits it.
        expect(harness).to have_received(:spawn).with(*arguments, **process_options).once
        expect(Process).to have_received(:kill).with('TERM', windows ? pid : -pid).once
        expect(harness).to have_received(:wait_for_exit).with(pid, 30).once
        expect(harness.instance_variable_get(:@logstash_pid)).to be_nil
        expect(File.read(harness.instance_variable_get(:@input_file)))
          .to eq(File.read(harness.instance_variable_get(:@csv_file)))
      end
    end

    it 'stops the process group even when preparing the input fails' do
      allow(Gem).to receive(:win_platform?).and_return(false)
      allow(File).to receive(:read).and_call_original
      allow(File).to receive(:read).with(harness.instance_variable_get(:@csv_file)).and_raise(IOError, 'input failed')

      expect { harness.run_logstash }.to raise_error(IOError, 'input failed')
      expect(Process).to have_received(:kill).with('TERM', -pid).once
      expect(harness.instance_variable_get(:@logstash_pid)).to be_nil
    end

    it 'escalates to group KILL with a bounded wait when TERM does not stop the child' do
      harness.instance_variable_set(:@logstash_pid, pid)
      harness.instance_variable_set(:@logstash_process_group, true)
      allow(harness).to receive(:wait_for_exit).with(pid, 30).and_return(false)
      allow(harness).to receive(:wait_for_exit).with(pid, 10).and_return(true)
      expect(Process).to receive(:kill).with('TERM', -pid).ordered
      expect(Process).to receive(:kill).with('KILL', -pid).ordered

      harness.stop_logstash
      expect(harness).to have_received(:wait_for_exit).with(pid, 10).once
      expect(harness.instance_variable_get(:@logstash_pid)).to be_nil
    end

    it 'tolerates an already exited process and does not signal it a second time' do
      harness.instance_variable_set(:@logstash_pid, pid)
      harness.instance_variable_set(:@logstash_process_group, true)
      allow(Process).to receive(:kill).with('TERM', -pid).and_raise(Errno::ESRCH)

      expect { harness.stop_logstash; harness.stop_logstash }.not_to raise_error
      expect(Process).to have_received(:kill).once
      expect(harness).not_to have_received(:wait_for_exit)
    end

    it 'does not signal any process when spawn fails' do
      allow(harness).to receive(:spawn).and_raise(Errno::ENOENT)

      expect { harness.run_logstash }.to raise_error(Errno::ENOENT)
      harness.stop_logstash
      expect(Process).not_to have_received(:kill)
    end

    it 'drains Logstash before validating ADX and closes a close-capable client' do
      allow(Gem).to receive(:win_platform?).and_return(false)
      harness.instance_variable_set(:@engine_url, 'https://test.kusto.windows.net')
      allow($kusto_java.data.ClientFactory).to receive(:createClient).and_return(query_client)
      allow(harness).to receive(:create_table_and_mapping)
      expect(Process).to receive(:kill).with('TERM', -pid).ordered
      expect(harness).to receive(:assert_data).ordered
      expect(harness).to receive(:drop_and_cleanup).ordered
      expect(query_client).to receive(:close).ordered

      harness.start
    end
  end

  it 'closes a close-capable client even when test-table cleanup fails' do
    harness.instance_variable_set(:@engine_url, 'https://test.kusto.windows.net')
    allow($kusto_java.data.ClientFactory).to receive(:createClient).and_return(query_client)
    allow(harness).to receive(:create_table_and_mapping)
    allow(harness).to receive(:run_logstash)
    allow(harness).to receive(:assert_data)
    allow(harness).to receive(:stop_logstash)
    allow(harness).to receive(:drop_and_cleanup).and_raise('cleanup failed')
    expect(query_client).to receive(:close)

    expect { harness.start }.to raise_error('cleanup failed')
    expect(harness).to have_received(:stop_logstash)
  end

  context 'query-client cleanup compatibility' do
    let(:query_client) { instance_double(Java::com.microsoft.azure.kusto.data.Client) }

    before do
      harness.instance_variable_set(:@engine_url, 'https://test.kusto.windows.net')
      allow($kusto_java.data.ClientFactory).to receive(:createClient).and_return(query_client)
      allow(harness).to receive(:create_table_and_mapping)
      allow(harness).to receive(:run_logstash)
      allow(harness).to receive(:assert_data)
      allow(harness).to receive(:stop_logstash)
      allow(harness).to receive(:drop_and_cleanup)
    end

    it 'finishes successfully with the pinned SDK query-client API' do
      expect(query_client).not_to respond_to(:close)
      expect { harness.start }.not_to raise_error
      expect(harness).to have_received(:assert_data).once
      expect(harness).to have_received(:stop_logstash).once
      expect(harness).to have_received(:drop_and_cleanup).once
    end

    it 'preserves a validation error when the query client cannot be closed' do
      allow(harness).to receive(:assert_data).and_raise('validation failed')

      expect { harness.start }.to raise_error('validation failed')
      expect(harness).to have_received(:stop_logstash).once
      expect(harness).to have_received(:drop_and_cleanup).once
    end

    it 'preserves a table-cleanup error when the query client cannot be closed' do
      allow(harness).to receive(:drop_and_cleanup).and_raise('cleanup failed')

      expect { harness.start }.to raise_error('cleanup failed')
      expect(harness).to have_received(:stop_logstash).once
    end
  end
end