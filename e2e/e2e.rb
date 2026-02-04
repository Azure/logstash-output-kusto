require '../lib/logstash-output-kusto_jars'
require 'csv'

$kusto_java = Java::com.microsoft.azure.kusto

class E2E

  def initialize
    super
    @input_file = "/tmp/input_file.txt"
    @output_file = "output_file.txt"
    @columns = "(rownumber:int, rowguid:string, xdouble:real, xfloat:real, xbool:bool, xint16:int, xint32:int, xint64:long, xuint8:long, xuint16:long, xuint32:long, xuint64:long, xdate:datetime, xsmalltext:string, xtext:string, xnumberAsText:string, xtime:timespan, xtextWithNulls:string, xdynamicWithNulls:dynamic)"
    @csv_columns = '"rownumber", "rowguid", "xdouble", "xfloat", "xbool", "xint16", "xint32", "xint64", "xuint8", "xuint16", "xuint32", "xuint64", "xdate", "xsmalltext", "xtext", "xnumberAsText", "xtime", "xtextWithNulls", "xdynamicWithNulls"'
    @column_count = 19
    @engine_url = ENV["ENGINE_URL"]
    @ingest_url = ENV["INGEST_URL"]
    @database = ENV['TEST_DATABASE']
    @lslocalpath = ENV['LS_LOCAL_PATH']
    puts "DEBUG: ENV['LS_LOCAL_PATH'] = #{ENV['LS_LOCAL_PATH'].inspect}"
    if @lslocalpath.nil?
      @lslocalpath = "/usr/share/logstash/bin/logstash"
    end
    puts "DEBUG: @lslocalpath = #{@lslocalpath}"
    @table_with_mapping = "RubyE2E#{Time.now.getutc.to_i}"
    @table_without_mapping = "RubyE2ENoMapping#{Time.now.getutc.to_i}"
    @table_dynamic = "RubyE2EDynamic#{Time.now.getutc.to_i}"
    @mapping_name = "test_mapping"
    @csv_file = "dataset.csv"

    @logstash_config = %{
  input {
    file { path => "#{@input_file}"}
  }
  filter {
    csv { columns => [#{@csv_columns}]}
    # Add metadata for dynamic routing test
    mutate {
      add_field => {
        "[@metadata][database]" => "#{@database}"
        "[@metadata][table]" => "#{@table_dynamic}"
        "[@metadata][mapping]" => "#{@mapping_name}"
      }
    }
  }
  output {
    file { path => "#{@output_file}"}
    stdout { codec => rubydebug }
    # Test 1: Static routing with mapping
    kusto {
      path => "tmp%{+YYYY-MM-dd-HH-mm}"
      ingest_url => "#{@ingest_url}"
      cli_auth => true
      database => "#{@database}"
      table => "#{@table_with_mapping}"
      json_mapping => "#{@mapping_name}"
    }
    # Test 2: Static routing without mapping
    kusto {
      path => "nomaptmp%{+YYYY-MM-dd-HH-mm}"
      cli_auth => true
      ingest_url => "#{@ingest_url}"
      database => "#{@database}"
      table => "#{@table_without_mapping}"
    }
    # Test 3: Dynamic routing with metadata fields
    kusto {
      path => "dynamictmp%{+YYYY-MM-dd-HH-mm}"
      cli_auth => true
      ingest_url => "#{@ingest_url}"
      database => "placeholder"
      table => "placeholder"
      dynamic_event_routing => true
    }
  }
}
  end

  def create_table_and_mapping
    puts "\n[#{Time.now}] === PHASE 1: Creating tables and mappings ==="
    Array[@table_with_mapping, @table_without_mapping, @table_dynamic].each { |tableop| 
      puts "[#{Time.now}] Creating table #{tableop}"
      puts "[#{Time.now}]   - Dropping table if exists..."
      @query_client.executeMgmt(@database, ".drop table #{tableop} ifexists")
      sleep(1)
      puts "[#{Time.now}]   - Creating table with schema..."
      @query_client.executeMgmt(@database, ".create table #{tableop} #{@columns}")
      puts "[#{Time.now}]   - Setting ingestion batching policy..."
      @query_client.executeMgmt(@database, ".alter table #{tableop} policy ingestionbatching @'{\"MaximumBatchingTimeSpan\":\"00:00:10\", \"MaximumNumberOfItems\": 1, \"MaximumRawDataSizeMB\": 100}'")
      puts "[#{Time.now}]   ✓ Table #{tableop} created successfully"
    }
    # Mapping for tables that need it
    puts "[#{Time.now}] Creating JSON mappings..."
    Array[@table_with_mapping, @table_dynamic].each { |tableop|
      puts "[#{Time.now}]   - Creating mapping '#{@mapping_name}' for table #{tableop}..."
      @query_client.executeMgmt(@database, ".create table #{tableop} ingestion json mapping '#{@mapping_name}' '#{File.read("dataset_mapping.json")}'")
      puts "[#{Time.now}]   ✓ Mapping created for #{tableop}"
    }
    puts "[#{Time.now}] ✓ All tables and mappings created successfully\n"
  end


  def drop_and_cleanup
    puts "\n[#{Time.now}] === PHASE 4: Cleanup ==="
    Array[@table_with_mapping, @table_without_mapping, @table_dynamic].each { |tableop| 
      puts "[#{Time.now}] Dropping table #{tableop}..."
      @query_client.executeMgmt(@database, ".drop table #{tableop} ifexists")
      puts "[#{Time.now}]   ✓ Table #{tableop} dropped"
      sleep(1)
    }
    puts "[#{Time.now}] ✓ Cleanup completed successfully\n"
  end

  def run_logstash
    puts "\n[#{Time.now}] === PHASE 2: Running Logstash ==="
    puts "[#{Time.now}] Writing logstash configuration..."
    File.write("logstash.conf", @logstash_config)
    logstashpath = File.absolute_path("logstash.conf")
    puts "[#{Time.now}]   - Config file: #{logstashpath}"
    
    puts "[#{Time.now}] Preparing input/output files..."
    File.write(@output_file, "")
    File.write(@input_file, "")
    puts "[#{Time.now}]   - Input file: #{@input_file}"
    puts "[#{Time.now}]   - Output file: #{@output_file}"
    
    lscommand = "#{@lslocalpath} -f #{logstashpath}"
    pid = spawn(lscommand)
    puts "[#{Time.now}]   - Process ID: #{pid}"
    sleep(60)
    data = File.read(@csv_file)
    csv_lines = data.lines.count
    f = File.open(@input_file, "a")
    f.write(data)
    f.close
    sleep(60)
    puts "[#{Time.now}] ✓ Logstash processing phase completed\n"
  end

  def assert_data
    puts "\n[#{Time.now}] === PHASE 3: Data Validation ==="
    max_timeout = 10
    csv_data = CSV.read(@csv_file)
    puts "[#{Time.now}] Expected data: #{csv_data.length} rows from CSV\n"
    
    Array[@table_with_mapping, @table_without_mapping, @table_dynamic].each_with_index { |tableop, table_idx| 
      
      (0...max_timeout).each do |attempt|
        begin
          puts "[#{Time.now}]   Attempt #{attempt + 1}/#{max_timeout}: Querying table..."
          sleep(5)
          
          query = @query_client.executeQuery(@database, "#{tableop} | sort by rownumber asc")
          result = query.getPrimaryResults()
          actual_count = result.count()
          
          puts "[#{Time.now}]   Query result: #{actual_count} rows found"
          
          if actual_count != csv_data.length
            raise "Wrong count - expected #{csv_data.length}, got #{actual_count} in table #{tableop}"
          end
                    
        rescue Exception => e
          puts "[#{Time.now}]   ✗ Error on attempt #{attempt + 1}: #{e}"
          if attempt == max_timeout - 1
            raise "Failed after #{max_timeout} attempts: #{e}"
          end
          next
        end
        
        # Validate each row
        (0...csv_data.length).each do |i|
          result.next()
          
          (0...@column_count).each do |j|
            csv_item = csv_data[i][j]
            result_item = result.getObject(j) == nil ? "null" : result.getString(j)
            #special cases for data that is different in csv vs kusto
            if j == 4 #kusto boolean field
              csv_item = csv_item.to_s == "1" ? "true" : "false"
            elsif j == 12 # date formatting
              csv_item = csv_item.sub(".0000000", "")
            elsif j == 15 # numbers as text
              result_item = i.to_s
            elsif j == 17 #null
              next
            end
            
            if csv_item != result_item
              puts "[#{Time.now}]     ✗ Mismatch at row #{i}, column #{j}:"
              puts "[#{Time.now}]       Expected (CSV): #{csv_item}"
              puts "[#{Time.now}]       Actual (Kusto): #{result_item}"
              raise "Result doesn't match CSV in table #{tableop} at row #{i}, column #{j}"
            end
          end
        end
        break
      end
    }
  end

  def start
    puts "\n" + "="*80
    puts "[#{Time.now}] E2E TEST STARTED"
    puts "="*80
    puts "[#{Time.now}] Configuration:"
    puts "[#{Time.now}]   - Engine URL: #{@engine_url}"
    puts "[#{Time.now}]   - Ingest URL: #{@ingest_url}"
    puts "[#{Time.now}]   - Database: #{@database}"
    puts "[#{Time.now}]   - Table (with mapping): #{@table_with_mapping}"
    puts "[#{Time.now}]   - Table (without mapping): #{@table_without_mapping}"
    puts "[#{Time.now}]   - Table (dynamic routing): #{@table_dynamic}"
    puts "[#{Time.now}]   - Mapping name: #{@mapping_name}"
    puts "[#{Time.now}]   - Logstash path: #{@lslocalpath}"
    puts "="*80 + "\n"
    
    begin
      puts "[#{Time.now}] Initializing Kusto client..."
      @query_client = $kusto_java.data.ClientFactory.createClient($kusto_java.data.auth.ConnectionStringBuilder::createWithAzureCli(@engine_url))
      puts "[#{Time.now}] ✓ Kusto client initialized\n"
      
      create_table_and_mapping
      run_logstash
      assert_data
      
      puts "\n" + "="*80
      puts "[#{Time.now}] ✓✓✓ E2E TEST COMPLETED SUCCESSFULLY ✓✓✓"
      puts "="*80 + "\n"
    rescue Exception => e
      puts "\n" + "="*80
      puts "[#{Time.now}] ✗✗✗ E2E TEST FAILED ✗✗✗"
      puts "[#{Time.now}] Error: #{e.class}"
      puts "[#{Time.now}] Message: #{e.message}"
      puts "[#{Time.now}] Backtrace:"
      e.backtrace.each { |line| puts "[#{Time.now}]   #{line}" }
      puts "="*80 + "\n"
      raise
    ensure
      # Always cleanup tables, whether test passes or fails
      drop_and_cleanup if @query_client
    end
  end  
end

E2E::new().start