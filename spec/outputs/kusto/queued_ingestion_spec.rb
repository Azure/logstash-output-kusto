# encoding: utf-8
require_relative '../../spec_helpers'
require 'tmpdir'

describe LogStash::Outputs::Kusto, 'queued ingestion compatibility' do
  let(:logger) { spy('logger') }
  let(:client) { double('SDK network boundary', close: nil) }
  let(:received) { Queue.new }

  before do
    @directory = Dir.mktmpdir('kusto-queued-compatibility')
    @plugin = nil
    factory = Java::com.microsoft.azure.kusto.ingest.IngestClientFactory
    allow(factory).to receive(:createClient).and_return(client)
    allow(client).to receive(:ingestFromFile) do |source, properties|
      received << {
        database: properties.getDatabaseName,
        table: properties.getTableName,
        mapping: properties.getIngestionMapping&.getIngestionMappingReference,
        format: properties.getDataFormat.to_s,
        events: File.readlines(source.getFilePath).map { |line| LogStash::Json.load(line) }
      }
      nil
    end
  end

  after do
    @plugin.close if @plugin
    FileUtils.remove_entry(@directory)
  end

  [
    ['static with mapping', { 'json_mapping' => 'map' }, 'map'],
    ['static without mapping', {}, nil],
    ['deprecated mapping fallback', { 'mapping' => 'legacy' }, 'legacy'],
    ['json_mapping takes precedence', { 'json_mapping' => 'selected', 'mapping' => 'legacy' }, 'selected'],
    ['dynamic optional mapping', { 'json_mapping' => '%{[@metadata][mapping]}' }, nil],
    ['deprecated dynamic mapping fallback', { 'mapping' => '%{[@metadata][mapping]}' }, nil],
    ['forced dynamic without mapping', { 'dynamic_event_routing' => true }, nil]
  ].each do |name, overrides, expected_mapping|
    it "uploads #{name} through the real codec, files, executor and Java properties" do
      @plugin = described_class.new({
        'path' => "#{@directory}/out-%{+YYYY-MM-dd-HH-mm}",
        'ingest_url' => 'https://ingest-test.kusto.windows.net',
        'app_id' => 'test-app', 'app_key' => 'test-key', 'app_tenant' => 'test-tenant',
        'database' => 'db', 'table' => 'orders', 'json_mapping' => nil,
        'flush_interval' => 0, 'recovery' => false
      }.merge(overrides))
      @plugin.instance_variable_set(:@logger, logger)
      @plugin.register
      events = %i[missing null empty].each_with_index.map do |kind, index|
        ev = LogStash::Event.new('id' => index, '@timestamp' => '2026-09-16T01:00:00Z')
        ev.set('[@metadata][mapping]', nil) if kind == :null
        ev.set('[@metadata][mapping]', '') if kind == :empty
        ev
      end
      @plugin.multi_receive(events)
      @plugin.close # Drain the real concurrent executor before asserting on its results.
      @plugin = nil

      expect(received.size).to eq(1)
      actual = received.pop
      expect(actual.values_at(:database, :table, :mapping, :format)).to eq(['db', 'orders', expected_mapping, 'JSON'])
      expect(actual[:events].map { |ev| ev['id'] }).to eq([0, 1, 2])
      expect(actual[:events]).to all(satisfy { |ev| !ev.key?('@metadata') })
      expect(Dir.children(@directory)).to be_empty
      expect(client).to have_received(:close).once
    end
  end

  it 'never submits reference-looking event mapping values as unmapped data' do
    template = '%{[@metadata][mapping]}'
    @plugin = described_class.new(
      'path' => "#{@directory}/out-%{+YYYY-MM-dd-HH-mm}",
      'ingest_url' => 'https://ingest-test.kusto.windows.net',
      'app_id' => 'test-app', 'app_key' => 'test-key', 'app_tenant' => 'test-tenant',
      'database' => 'db', 'table' => 'orders', 'json_mapping' => template,
      'flush_interval' => 0, 'recovery' => false
    )
    @plugin.instance_variable_set(:@logger, logger)
    @plugin.register
    dlq = spy('DLQ')
    @plugin.instance_variable_set(:@dlq_writer, dlq)
    events = [nil, '', 'real_mapping', '%{other}', template].each_with_index.map do |mapping, index|
      LogStash::Event.new('id' => index, '@metadata' => { 'mapping' => mapping },
                         '@timestamp' => '2026-09-16T01:00:00Z')
    end
    @plugin.multi_receive(events)
    @plugin.close
    @plugin = nil

    expect(received.size).to eq(2)
    actual = 2.times.map { received.pop }.to_h { |item| [item[:mapping], item[:events].map { |ev| ev['id'] }] }
    expect(actual).to eq(nil => [0, 1], 'real_mapping' => [2])
    events.last(2).each { |ev| expect(dlq).to have_received(:write).with(ev, /json_mapping.*unresolved/).once }
    expect(Dir.children(@directory)).to be_empty
    expect(client).to have_received(:close).once
  end
end