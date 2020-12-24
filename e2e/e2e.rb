require '../lib/logstash-output-kusto_jars'
$kusto_java = Java::com.microsoft.azure.kusto

class E2E
  @input_file = "input_file.txt"
  @output_file = "output_file.txt"
  @columns = "(rownumber:int, rowguid:string, xdouble:real, xfloat:real, xbool:bool, xint16:int, xint32:int, xint64:long, xuint8:long, xuint16:long, xuint32:long, xuint64:long, xdate:datetime, xsmalltext:string, xtext:string, xnumberAsText:string, xtime:timespan, xtextWithNulls:string, xdynamicWithNulls:dynamic)"
  @csv_columns = 'rownumber", "rowguid", "xdouble", "xfloat", "xbool", "xint16", "xint32", "xint64", "xuint8", "xuint16", "xuint32", "xuint64", "xdate", "xsmalltext", "xtext", "xnumberAsText", "xtime", "xtextWithNulls", "xdynamicWithNulls"'
  @engine_url = ENV["ENGINE_URL"]
  @ingest_url = ENV["INGEST_URL"]
  @app_id = ENV["APP_ID"]
  @app_kay = ENV['APP_KEY']
  @tenant_id = ENV['TENANT_ID']
  @database = ENV['TEST_DATABASE']
  @table = "RubyE2E#{Time.now.getutc}"
  @mapping_name = "test_mapping"

  def create_table_and_mapping
    puts "Creating table #{@table}"
    @query_client.execute(@database, ".drop table #{@table} ifexists")
    sleep(1)
    @query_client.execute(@database, ".create table #{@table} #{@columns}")
    @query_client.execute(@database, ".create table #{@table} ingestion json mapping '#{@mapping_name}' '#{File.read("dataset_mapping.json")}'")
  end

  def initialize
    super
    @logstash_config = %{
input {
  file { path => "#{@input_file}"}
}
filter {
  csv { columns => [#{@csv_columns}]}
}
output {
  file { path => "#{@output_file}"}
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

  def run_logstash
    File.write("logstash.conf", @logstash_config)
    pid = spawn("/usr/share/logstash/bin/logstash -f logstash.conf")

    File.write(@input_file, "dataset.csv")
    sleep(5)
    Process.kill("KILL", pid)
  end

  def start
    @query_client = $kusto_java.data.ClientImpl.new($kusto_java.data.ConnectionStringBuilder::createWithAadApplicationCredentials(@engine_url, @app_id, @app_kay, @tenant_id))
    create_table_and_mapping

  end

end

E2E::new().start