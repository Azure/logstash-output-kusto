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
    @app_id = ENV["APP_ID"]
    @app_kay = ENV['APP_KEY']
    @tenant_id = ENV['TENANT_ID']
    @database = ENV['TEST_DATABASE']
    @table = "RubyE2E#{Time.now.getutc.to_i}"
    @mapping_name = "test_mapping"
    @csv_file = "dataset.csv"

    @logstash_config = %{
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
    path => "tmp%{+YYYY-MM-dd-HH-mm}.txt"
    ingest_url => "#{@ingest_url}"
    app_id => "#{@app_id}"
    app_key => "#{@app_kay}"
    app_tenant => "#{@tenant_id}"
    database => "#{@database}"
    table => "#{@table}"
    json_mapping => "#{@mapping_name}"
  }
}
}
  end

  def create_table_and_mapping
    puts "Creating table #{@table}"
    @query_client.execute(@database, ".drop table #{@table} ifexists")
    sleep(1)
    @query_client.execute(@database, ".create table #{@table} #{@columns}")
    @query_client.execute(@database, ".alter table #{@table} policy ingestionbatching @'{\"MaximumBatchingTimeSpan\":\"00:00:10\", \"MaximumNumberOfItems\": 1, \"MaximumRawDataSizeMB\": 100}'
")
    @query_client.execute(@database, ".create table #{@table} ingestion json mapping '#{@mapping_name}' '#{File.read("dataset_mapping.json")}'")
  end

  def run_logstash
    File.write("logstash.conf", @logstash_config)

    File.write(@output_file, "")
    File.write(@input_file, "")
    spawn("/usr/share/logstash/bin/logstash -f logstash.conf")
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

    (0...max_timeout).each do |_|
      begin
        sleep(5)
        query = @query_client.execute(@database, "#{@table} | sort by rownumber asc")
        result = query.getPrimaryResults()
        raise "Wrong count - expected #{csv_data.length}, got #{result.count()}" unless result.count() == csv_data.length
      rescue Exception => e
        puts "Error: #{e}"
      end
      (0...csv_data.length).each do |i|
        result.next()
        puts "Item #{i}"
        (0...@column_count).each do |j|
          csv_item = csv_data[i][j]
          result_item = result.getObject(i) == nil ? "" : result.getString(i)
          puts "  csv[#{j}] = #{csv_item}"
          puts "  result[#{j}] = #{result_item}"
          raise "Result Doesn't match csv" unless csv_item == result_item
        end
        puts ""
      end
      return

    end
    raise "Failed after timeouts"

  end

  def start
    @query_client = $kusto_java.data.ClientImpl.new($kusto_java.data.ConnectionStringBuilder::createWithAadApplicationCredentials(@engine_url, @app_id, @app_kay, @tenant_id))
    create_table_and_mapping
    run_logstash
    assert_data
  end

end

E2E::new().start