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
    if @lslocalpath.nil?
      @lslocalpath = "/usr/share/logstash/bin/logstash"
    end
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
    # Inject routing metadata used by the dynamic kusto output below.
    mutate { add_field => { "[@metadata][kusto_table]" => "#{@table_dynamic}" } }
  }
  output {
    file { path => "#{@output_file}"}
    stdout { codec => rubydebug }
    kusto {
      path => "tmp%{+YYYY-MM-dd-HH-mm}.txt"
      ingest_url => "#{@ingest_url}"
      cli_auth => true
      database => "#{@database}"
      table => "#{@table_with_mapping}"
      json_mapping => "#{@mapping_name}"
    }
    kusto {
      path => "nomaptmp%{+YYYY-MM-dd-HH-mm}.txt"
      cli_auth => true
      ingest_url => "#{@ingest_url}"
      database => "#{@database}"
      table => "#{@table_without_mapping}"
    }
    # Dynamic routing: the destination table is resolved per-event from
    # event metadata. Database stays static, table is dynamic.
    kusto {
      path => "dyntmp%{+YYYY-MM-dd-HH-mm}.txt"
      cli_auth => true
      ingest_url => "#{@ingest_url}"
      database => "#{@database}"
      table => "%{[@metadata][kusto_table]}"
      json_mapping => "#{@mapping_name}"
    }
  }
}
  end

  def create_table_and_mapping
    Array[@table_with_mapping, @table_without_mapping, @table_dynamic].each { |tableop|
      puts "Creating table #{tableop}"
      @query_client.executeMgmt(@database, ".drop table #{tableop} ifexists")
      sleep(1)
      @query_client.executeMgmt(@database, ".create table #{tableop} #{@columns}")
      @query_client.executeMgmt(@database, ".alter table #{tableop} policy ingestionbatching @'{\"MaximumBatchingTimeSpan\":\"00:00:10\", \"MaximumNumberOfItems\": 1, \"MaximumRawDataSizeMB\": 100}'")
    }
    # Mapping for the tables that use it (static-with-mapping and dynamic).
    Array[@table_with_mapping, @table_dynamic].each { |tableop|
      @query_client.executeMgmt(@database, ".create table #{tableop} ingestion json mapping '#{@mapping_name}' '#{File.read("dataset_mapping.json")}'")
    }
  end


  def drop_and_cleanup
    Array[@table_with_mapping, @table_without_mapping, @table_dynamic].each { |tableop|
      puts "Dropping table #{tableop}"
      @query_client.executeMgmt(@database, ".drop table #{tableop} ifexists")
      sleep(1)
    }
  end

  def run_logstash
    File.write("logstash.conf", @logstash_config)
    logstashpath = File.absolute_path("logstash.conf")
    File.write(@output_file, "")
    File.write(@input_file, "")
    lscommand = "#{@lslocalpath} -f #{logstashpath}"
    puts "Running logstash from config path #{logstashpath} and final command #{lscommand}"
    spawn(lscommand)
    sleep(60)
    data = File.read(@csv_file)
    f = File.open(@input_file, "a")
    f.write(data)
    f.close
    sleep(60)
    puts File.read(@output_file)
  end

  def assert_data
    max_timeout = 10
    csv_data = CSV.read(@csv_file)
    # Validate every table the pipeline wrote to, including the dynamic-routing
    # table, which is fed via [@metadata][kusto_table]. Each table is
    # validated independently and retried until the full dataset has landed, so a
    # failure (or missing rows) in any one table fails the test.
    Array[@table_with_mapping, @table_without_mapping, @table_dynamic].each { |tableop|
      puts "Validating results for table #{tableop}"
      validated = false
      (0...max_timeout).each do |_|
        sleep(5)
        begin
          query = @query_client.executeQuery(@database, "#{tableop} | sort by rownumber asc")
          result = query.getPrimaryResults()
        rescue Exception => e
          puts "Error querying #{tableop}: #{e}"
          next
        end
        # Ingestion is asynchronous; keep retrying until all rows have landed.
        actual_count = result.count()
        if actual_count != csv_data.length
          puts "Waiting for #{tableop}: expected #{csv_data.length} rows, got #{actual_count}"
          next
        end
        # Row-by-row validation against the source CSV.
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
            raise "Result Doesn't match csv in table #{tableop} at row #{i}, column #{j}" unless csv_item == result_item
          end
        end
        puts "Table #{tableop} validated successfully (#{csv_data.length} rows)"
        validated = true
        break
      end
      raise "Failed after timeouts validating table #{tableop}" unless validated
    }
  end

  def start
    @query_client = $kusto_java.data.ClientFactory.createClient($kusto_java.data.auth.ConnectionStringBuilder::createWithAzureCli(@engine_url))
    create_table_and_mapping
    run_logstash
    assert_data
    drop_and_cleanup    
  end  
end

E2E::new().start