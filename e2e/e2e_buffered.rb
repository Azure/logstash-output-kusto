require '../lib/logstash-output-kusto_jars'
require 'csv'

$kusto_java = Java::com.microsoft.azure.kusto

class E2EBuffered

  def initialize
    super
    @input_file = File.expand_path("input_file_buffered.txt", __dir__)
    @output_file = File.expand_path("output_file_buffered.txt", __dir__)
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
    @table_size_flush = "RubyE2ESize#{Time.now.getutc.to_i}"
    @table_time_flush = "RubyE2ETime#{Time.now.getutc.to_i}"
    @mapping_name = "test_mapping"
    @csv_file = "dataset.csv"

    # Size-based flush: small max_batch_size (1 KB) so the buffer flushes quickly by size,
    # with a long interval so time-based flush does not trigger first.
    # max_items is set high so it does not trigger before size does.
    @logstash_config_size = %{
  input {
    file { path => "#{@input_file}"}
  }
  filter {
    csv { columns => [#{@csv_columns}]}
  }
  output {
    file { path => "#{@output_file}"}
    stdout { codec => rubydebug }
    kusto {
      ingest_url => "#{@ingest_url}"
      cli_auth => true
      database => "#{@database}"
      table => "#{@table_size_flush}"
      json_mapping => "#{@mapping_name}"
      max_batch_size => 1
      plugin_flush_interval => 300
      max_items => 10000
    }
  }
}

    # Time-based flush: short interval (5 seconds) so the buffer flushes by time,
    # with a large max_batch_size and max_items so they do not trigger first.
    @logstash_config_time = %{
  input {
    file { path => "#{@input_file}"}
  }
  filter {
    csv { columns => [#{@csv_columns}]}
  }
  output {
    file { path => "#{@output_file}"}
    stdout { codec => rubydebug }
    kusto {
      ingest_url => "#{@ingest_url}"
      cli_auth => true
      database => "#{@database}"
      table => "#{@table_time_flush}"
      json_mapping => "#{@mapping_name}"
      max_batch_size => 100
      plugin_flush_interval => 5
      max_items => 10000
    }
  }
}
  end

  def create_table_and_mapping(tables)
    tables.each do |tableop|
      puts "Creating table #{tableop}"
      @query_client.executeMgmt(@database, ".drop table #{tableop} ifexists")
      sleep(1)
      @query_client.executeMgmt(@database, ".create table #{tableop} #{@columns}")
      @query_client.executeMgmt(@database, ".alter table #{tableop} policy ingestionbatching @'{\"MaximumBatchingTimeSpan\":\"00:00:10\", \"MaximumNumberOfItems\": 1, \"MaximumRawDataSizeMB\": 100}'")
      @query_client.executeMgmt(@database, ".create table #{tableop} ingestion json mapping '#{@mapping_name}' '#{File.read("dataset_mapping.json")}'")
    end
  end

  def drop_and_cleanup(tables)
    tables.each do |tableop|
      puts "Dropping table #{tableop}"
      @query_client.executeMgmt(@database, ".drop table #{tableop} ifexists")
      sleep(1)
    end
  end

  def run_logstash(config_content, label)
    config_file = "logstash_#{label}.conf"
    File.write(config_file, config_content)
    logstashpath = File.absolute_path(config_file)
    File.write(@output_file, "")
    File.write(@input_file, "")
    lscommand = "#{@lslocalpath} -f #{logstashpath}"
    puts "[#{label}] Running logstash from config path #{logstashpath}"
    pid = spawn(lscommand)
    sleep(60)
    data = File.read(@csv_file)
    File.open(@input_file, "a") { |f| f.write(data) }
    sleep(60)
    puts "[#{label}] Output:\n#{File.read(@output_file)}"
    # Terminate logstash after the test run
    begin
      Process.kill("TERM", pid)
      Process.wait(pid)
    rescue Errno::ESRCH, Errno::ECHILD
      # already exited
    end
  end

  def assert_data(table)
    max_timeout = 10
    csv_data = CSV.read(@csv_file)
    puts "Validating results for table #{table}"
    (0...max_timeout).each do |_|
      begin
        sleep(5)
        query = @query_client.executeQuery(@database, "#{table} | sort by rownumber asc")
        result = query.getPrimaryResults()
        raise "Wrong count - expected #{csv_data.length}, got #{result.count()} in table #{table}" unless result.count() == csv_data.length
      rescue Exception => e
        puts "Error: #{e}"
        next
      end
      (0...csv_data.length).each do |i|
        result.next()
        puts "Item #{i}"
        (0...@column_count).each do |j|
          csv_item = csv_data[i][j]
          result_item = result.getObject(j) == nil ? "null" : result.getString(j)
          if j == 4 # kusto boolean field
            csv_item = csv_item.to_s == "1" ? "true" : "false"
          elsif j == 12 # date formatting
            csv_item = csv_item.sub(".0000000", "")
          elsif j == 15 # numbers as text
            result_item = i.to_s
          elsif j == 17 # null
            next
          end
          puts "  csv[#{j}] = #{csv_item}"
          puts "  result[#{j}] = #{result_item}"
          raise "Result Doesn't match csv in table #{table}" unless csv_item == result_item
        end
        puts ""
      end
      return
    end
    raise "Failed after timeouts for table #{table}"
  end

  def start
    @query_client = $kusto_java.data.ClientFactory.createClient(
      $kusto_java.data.auth.ConnectionStringBuilder::createWithAzureCli(@engine_url)
    )
    all_tables = [@table_size_flush, @table_time_flush]
    create_table_and_mapping(all_tables)

    puts "=== Test 1: Size-based flush (max_batch_size => 1 KB) ==="
    run_logstash(@logstash_config_size, "size_flush")
    assert_data(@table_size_flush)
    puts "=== Size-based flush test PASSED ==="

    puts "=== Test 2: Time-based flush (plugin_flush_interval => 5s) ==="
    run_logstash(@logstash_config_time, "time_flush")
    assert_data(@table_time_flush)
    puts "=== Time-based flush test PASSED ==="

    drop_and_cleanup(all_tables)
    puts "=== All buffered mode e2e tests PASSED ==="
  end
end

E2EBuffered::new().start
