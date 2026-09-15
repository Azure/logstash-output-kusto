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

  it 'closes the SDK client even when test-table cleanup fails' do
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
end