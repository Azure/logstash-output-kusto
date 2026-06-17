# encoding: utf-8
require_relative "../../spec_helpers.rb"
require 'logstash/outputs/kusto'
require 'logstash/outputs/kusto/ingestor'

describe LogStash::Outputs::Kusto::Ingestor do

  let(:ingest_url) { "https://ingest-sdkse2etest.eastus.kusto.windows.net/" }
  let(:app_id) { "myid" }
  let(:app_key) { LogStash::Util::Password.new("mykey") }
  let(:app_tenant) { "mytenant" }
  let(:managed_identity) { "managed_identity" }  
  let(:database) { "mydatabase" }
  let(:cliauth) { false }
  let(:table) { "mytable" }
  let(:proxy_host) { "localhost" }
  let(:proxy_port) { 80 }
  let(:proxy_protocol) { "http" }
  let(:json_mapping) { "mymapping" }
  let(:delete_local) { false }
  let(:dynamic_routing) { false }
  let(:logger) { spy('logger') }

  describe '#initialize' do

    it 'does not throw an error when initializing' do
      # note that this will cause an internal error since connection is being tried.
      # however we still want to test that all the java stuff is working as expected
      expect { 
        ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, database, table, json_mapping, dynamic_routing, delete_local, proxy_host, proxy_port,proxy_protocol, logger)
        ingestor.stop
      }.not_to raise_error
    end
    
    dynamic_name_array = ['/a%{name}/', '/a %{name}/', '/a- %{name}/', '/a- %{name}']

    context 'doesnt allow database to have some dynamic part' do
      dynamic_name_array.each do |test_database|
        it "with database: #{test_database}" do
          expect {
            ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, test_database, table, json_mapping, dynamic_routing, delete_local, proxy_host, proxy_port,proxy_protocol,logger)
            ingestor.stop
          }.to raise_error(LogStash::ConfigurationError)          
        end
      end
    end

    context 'doesnt allow table to have some dynamic part' do
      dynamic_name_array.each do |test_table|
        it "with database: #{test_table}" do
          expect {
            ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, database, test_table, json_mapping, dynamic_routing, delete_local, proxy_host, proxy_port,proxy_protocol,logger)
            ingestor.stop
          }.to raise_error(LogStash::ConfigurationError)          
        end
      end
    end

    context 'doesnt allow mapping to have some dynamic part' do
      dynamic_name_array.each do |json_mapping|
        it "with database: #{json_mapping}" do
          expect {
            ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, database, table, json_mapping, dynamic_routing, delete_local, proxy_host, proxy_port,proxy_protocol,logger)
            ingestor.stop
          }.to raise_error(LogStash::ConfigurationError)          
        end
      end
    end

    context 'allows dynamic database/table/mapping when dynamic routing is enabled' do
      dynamic_name_array.each do |dynamic_value|
        it "with dynamic value: #{dynamic_value}" do
          expect {
            ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, dynamic_value, dynamic_value, dynamic_value, true, delete_local, proxy_host, proxy_port, proxy_protocol, logger)
            ingestor.stop
          }.not_to raise_error
        end
      end
    end

    context 'proxy protocol has to be http or https' do
      it "with proxy protocol: socks" do
        expect {
          ingestor = described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, database, table, json_mapping, dynamic_routing, delete_local, proxy_host, proxy_port,'socks',logger)
          ingestor.stop
        }.to raise_error(LogStash::ConfigurationError)          
      end
    end

    context 'one of appid or managedid has to be provided' do
      it "with empty managed identity and appid" do
        # Use a valid proxy protocol so the failure is attributable to the missing
        # credentials, not to proxy-protocol validation. With no app_id/app_key and
        # no managed identity (and cli_auth disabled) config validation must fail.
        expect {
          ingestor = described_class.new(ingest_url, nil, nil, app_tenant, nil, cliauth, database, table, json_mapping, dynamic_routing, delete_local, proxy_host, proxy_port,'http',logger)
          ingestor.stop
        }.to raise_error(LogStash::ConfigurationError)          
      end
    end

  end

  describe '#decode_routing_target' do
    let(:ingestor) do
      described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, "%{db}", "%{table}", "%{mapping}", true, delete_local, proxy_host, proxy_port, proxy_protocol, logger)
    end

    after(:each) { ingestor.stop }

    it 'decodes database, table and mapping from the file name' do
      target = ingestor.decode_routing_target("/tmp/kusto/2024-01-01.kusto~mydb~mytable~mymapping")
      expect(target[:database]).to eq('mydb')
      expect(target[:table]).to eq('mytable')
      expect(target[:mapping]).to eq('mymapping')
    end

    it 'treats an empty mapping segment as no mapping' do
      target = ingestor.decode_routing_target("/tmp/kusto/2024-01-01.kusto~mydb~mytable~")
      expect(target[:database]).to eq('mydb')
      expect(target[:table]).to eq('mytable')
      expect(target[:mapping]).to be_nil
    end

    it 'is unaffected by dots in the path prefix' do
      target = ingestor.decode_routing_target("/tmp/kusto/2024.01.01-10.30.kusto~mydb~mytable~mymapping")
      expect(target[:database]).to eq('mydb')
      expect(target[:table]).to eq('mytable')
      expect(target[:mapping]).to eq('mymapping')
    end

    it 'returns nil when the routing marker is absent (dead-letter file)' do
      expect(ingestor.decode_routing_target("/tmp/kusto/_filepath_failures")).to be_nil
    end

    it 'returns nil when database or table did not resolve' do
      expect(ingestor.decode_routing_target("/tmp/kusto/2024-01-01.kusto~mydb~")).to be_nil
    end

    it 'treats an unresolved mapping field reference as no mapping (mapping is optional)' do
      target = ingestor.decode_routing_target("/tmp/kusto/2024-01-01.kusto~mydb~mytable~%{[@metadata][mapping]}")
      expect(target[:database]).to eq('mydb')
      expect(target[:table]).to eq('mytable')
      expect(target[:mapping]).to be_nil
    end

    it 'returns nil when the mapping decoded to a genuinely invalid value' do
      # A resolved-but-invalid mapping (here a path separator) must not be silently
      # dropped: the event is unroutable rather than ingested with the wrong mapping.
      expect(ingestor.decode_routing_target("/tmp/kusto/2024-01-01.kusto~mydb~mytable~bad%2Fmapping")).to be_nil
    end

    it 'round-trips database/table/mapping containing dots and spaces' do
      db = LogStash::Outputs::Kusto.encode_routing_segment('Security.Events')
      table = LogStash::Outputs::Kusto.encode_routing_segment('App Logs')
      mapping = LogStash::Outputs::Kusto.encode_routing_segment('My.Mapping')
      # Encoded segments contain no '.', '~' or '/'.
      [db, table, mapping].each do |seg|
        expect(seg).not_to include('.')
        expect(seg).not_to include('~')
        expect(seg).not_to include('/')
      end
      target = ingestor.decode_routing_target("/tmp/a.b.c-12.34.kusto~#{db}~#{table}~#{mapping}")
      expect(target[:database]).to eq('Security.Events')
      expect(target[:table]).to eq('App Logs')
      expect(target[:mapping]).to eq('My.Mapping')
    end

    it 'is not fooled by a database value that itself ends in the marker text' do
      # "a.kusto" must not create a spurious marker; encoding removes the dot.
      seg = LogStash::Outputs::Kusto.encode_routing_segment('a.kusto')
      target = ingestor.decode_routing_target("/tmp/2024-01-01.kusto~#{seg}~mytable~")
      expect(target[:database]).to eq('a.kusto')
      expect(target[:table]).to eq('mytable')
    end

    it 'round-trips non-ASCII (multi-byte UTF-8) database/table/mapping exactly' do
      # Bytes >= 0x80 (e.g. é, Σ, 日) are percent-encoded per byte and must be
      # reconstructed byte-for-byte on decode, not as UTF-8 codepoints.
      database = "caf\u00E9_M\u00FCnchen"   # café_München
      table    = "\u65E5\u672C\u8A9E"        # 日本語
      mapping  = "\u03A3.Map"                # Σ.Map
      db_enc = LogStash::Outputs::Kusto.encode_routing_segment(database)
      tb_enc = LogStash::Outputs::Kusto.encode_routing_segment(table)
      mp_enc = LogStash::Outputs::Kusto.encode_routing_segment(mapping)
      target = ingestor.decode_routing_target("/tmp/2024-01-01.kusto~#{db_enc}~#{tb_enc}~#{mp_enc}")
      expect(target[:database]).to eq(database)
      expect(target[:table]).to eq(table)
      expect(target[:mapping]).to eq(mapping)
    end
  end

  describe '#ingestion_properties_for' do
    let(:ingestor) do
      described_class.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cliauth, "%{db}", "%{table}", "%{mapping}", true, delete_local, proxy_host, proxy_port, proxy_protocol, logger)
    end

    after(:each) { ingestor.stop }

    it 'returns nil for a file without a decodable routing target (upload is skipped)' do
      expect(ingestor.ingestion_properties_for("/tmp/kusto/_filepath_failures")).to be_nil
    end

    it 'builds ingestion properties from the route decoded from each file name' do
      # The core dynamic contract: the destination passed to the Java SDK must be
      # the database/table/mapping decoded from the file name, per file.
      props = double('ingestion_properties')
      expect(ingestor).to receive(:build_ingestion_properties).with('routedb', 'routetable', 'routemap').and_return(props)
      result = ingestor.ingestion_properties_for("/tmp/kusto/2024-01-01.kusto~routedb~routetable~routemap")
      expect(result).to eq(props)
    end

    it 'builds ingestion properties without a mapping when the mapping segment is empty' do
      props = double('ingestion_properties')
      expect(ingestor).to receive(:build_ingestion_properties).with('routedb', 'routetable', nil).and_return(props)
      result = ingestor.ingestion_properties_for("/tmp/kusto/2024-01-01.kusto~routedb~routetable~")
      expect(result).to eq(props)
    end

    it 'deletes an undecodable dynamic file on upload instead of leaving it for infinite retry' do
      require 'tmpdir'
      Dir.mktmpdir do |dir|
        # Marker present but only one segment -> decode returns nil (unroutable).
        path = File.join(dir, '2024-01-01.kusto~onlydb')
        File.write(path, '{"a":1}')
        ingestor.upload(path, true)
        expect(File.exist?(path)).to be false
      end
    end
  end

  # describe 'receiving events' do

  #   context 'with non-zero flush interval' do
  #     let(:temporary_output_file) { Stud::Temporary.pathname }

  #     let(:event_count) { 100 }
  #     let(:flush_interval) { 5 }

  #     let(:events) do
  #       event_count.times.map do |idx|
  #         LogStash::Event.new('subject' => idx)
  #       end
  #     end

  #     let(:output) { described_class.new(options.merge( {'path' => temporary_output_file, 'flush_interval' => flush_interval, 'delete_temp_files' => false } )) }

  #     before(:each) { output.register }
      
  #     after(:each) do
  #       output.close
  #       File.exist?(temporary_output_file) && File.unlink(temporary_output_file)
  #       File.exist?(temporary_output_file + '.kusto') && File.unlink(temporary_output_file + '.kusto')
  #     end

  #     it 'eventually flushes without receiving additional events' do
  #       output.multi_receive_encoded(events)

  #       # events should not all be flushed just yet...
  #       expect(File.read(temporary_output_file)).to satisfy("have less than #{event_count} lines") do |contents|
  #         contents && contents.lines.count < event_count
  #       end

  #       # wait for the flusher to run...
  #       sleep(flush_interval + 1)

  #       # events should all be flushed
  #       expect(File.read(temporary_output_file)).to satisfy("have exactly #{event_count} lines") do |contents|
  #         contents && contents.lines.count == event_count
  #       end
  #     end
  #   end

  # end
end
