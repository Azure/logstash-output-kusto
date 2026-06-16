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
    @table_dynamic_odd = "RubyE2EDynamicOdd#{Time.now.getutc.to_i}"
    @table_dynamic_even = "RubyE2EDynamicEven#{Time.now.getutc.to_i}"
    @mapping_name = "test_mapping"
    @csv_file = "dataset.csv"

    @logstash_config = %{
  input {
    file { path => "#{@input_file}"}
  }
  filter {
    csv { columns => [#{@csv_columns}]}
    # Route each event to a different ADX table based on its content: odd
    # rownumbers go to one table, even rownumbers to another. A single dynamic
    # kusto output below then fans these out to two tables, which is the core
    # multi-destination scenario.
    ruby {
      code => "
        rn = event.get('rownumber').to_i
        event.set('[@metadata][kusto_table]', rn.odd? ? '#{@table_dynamic_odd}' : '#{@table_dynamic_even}')
      "
    }
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
    # Dynamic routing: a single output fans events out to two ADX tables, with
    # the destination table resolved per event from [@metadata][kusto_table].
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
    Array[@table_with_mapping, @table_without_mapping, @table_dynamic_odd, @table_dynamic_even].each { |tableop|
      puts "Creating table #{tableop}"
      @query_client.executeMgmt(@database, ".drop table #{tableop} ifexists")
      sleep(1)
      @query_client.executeMgmt(@database, ".create table #{tableop} #{@columns}")
      @query_client.executeMgmt(@database, ".alter table #{tableop} policy ingestionbatching @'{\"MaximumBatchingTimeSpan\":\"00:00:10\", \"MaximumNumberOfItems\": 1, \"MaximumRawDataSizeMB\": 100}'")
    }
    # Mapping for the tables that use it (static-with-mapping and both dynamic).
    Array[@table_with_mapping, @table_dynamic_odd, @table_dynamic_even].each { |tableop|
      @query_client.executeMgmt(@database, ".create table #{tableop} ingestion json mapping '#{@mapping_name}' '#{File.read("dataset_mapping.json")}'")
    }
  end


  def drop_and_cleanup
    Array[@table_with_mapping, @table_without_mapping, @table_dynamic_odd, @table_dynamic_even].each { |tableop|
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
    # Static tables receive the full dataset and are validated row-by-row.
    Array[@table_with_mapping, @table_without_mapping].each { |tableop|
      puts "Validating results for table #{tableop}"
      validate_table_rows(tableop, csv_data, max_timeout)
    }

    # Dynamic routing proof: a single output fanned events out to two tables by
    # odd/even rownumber. Validate that each table received exactly its subset,
    # which proves multiple dynamic destinations from one output.
    odd_rows = csv_data.select { |row| row[0].to_i.odd? }
    even_rows = csv_data.select { |row| row[0].to_i.even? }
    puts "Validating dynamic routing: #{odd_rows.length} odd rows -> #{@table_dynamic_odd}, #{even_rows.length} even rows -> #{@table_dynamic_even}"
    validate_table_rows(@table_dynamic_odd, odd_rows, max_timeout)
    validate_table_rows(@table_dynamic_even, even_rows, max_timeout)
  end

  # Validates that an ADX table eventually contains exactly the expected rows
  # (retried because ingestion is asynchronous), comparing column by column.
  def validate_table_rows(tableop, expected_rows, max_timeout)
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
      actual_count = result.count()
      if actual_count != expected_rows.length
        puts "Waiting for #{tableop}: expected #{expected_rows.length} rows, got #{actual_count}"
        next
      end
      (0...expected_rows.length).each do |i|
        result.next()
        (0...@column_count).each do |j|
          csv_item = expected_rows[i][j]
          result_item = result.getObject(j) == nil ? "null" : result.getString(j)
          #special cases for data that is different in csv vs kusto
          if j == 4 #kusto boolean field
            csv_item = csv_item.to_s == "1" ? "true" : "false"
          elsif j == 12 # date formatting
            csv_item = csv_item.sub(".0000000", "")
          elsif j == 15 # numbers as text
            result_item = expected_rows[i][0].to_s
          elsif j == 17 #null
            next
          end
          raise "Result Doesn't match csv in table #{tableop} at row #{i}, column #{j}" unless csv_item == result_item
        end
      end
      puts "Table #{tableop} validated successfully (#{expected_rows.length} rows)"
      validated = true
      break
    end
    raise "Failed after timeouts validating table #{tableop}" unless validated
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