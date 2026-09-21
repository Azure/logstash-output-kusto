require_relative '../lib/logstash-output-kusto_jars'
require 'csv'
require 'tmpdir'
require 'securerandom'
require 'fileutils'

$kusto_java = Java::com.microsoft.azure.kusto

class E2E

  def initialize
    super
    run_id = SecureRandom.hex(8)
    @work_directory = File.join(Dir.tmpdir, "kusto-e2e-#{run_id}").tr('\\', '/')
    @input_file = "#{@work_directory}/input.csv"
    @output_file = "#{@work_directory}/output.json"
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
    @table_with_mapping = "RubyE2E#{run_id}"
    @table_without_mapping = "RubyE2ENoMapping#{run_id}"
    @table_dynamic_odd = "RubyE2EDynamicOdd#{run_id}"
    @table_dynamic_even = "RubyE2EDynamicEven#{run_id}"
    @mapping_name = "test_mapping"
    @odd_mapping = 'odd_mapping'
    @even_mapping = 'even_mapping'
    # Optional pre-provisioned database; the harness never creates databases.
    @even_database = ENV.fetch('TEST_SECOND_DATABASE', @database)
    @csv_file = File.join(__dir__, 'dataset.csv')

    @logstash_config = %{
  input {
    file {
      path => "#{@input_file}"
      start_position => "beginning"
      sincedb_path => "#{@work_directory}/sincedb"
    }
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
        event.set('[@metadata][kusto_database]', rn.odd? ? '#{@database}' : '#{@even_database}')
        event.set('[@metadata][kusto_mapping]', rn.odd? ? '#{@odd_mapping}' : '#{@even_mapping}')
      "
    }
  }
  output {
    file { path => "#{@output_file}"}
    stdout { codec => rubydebug }
    kusto {
      path => "#{@work_directory}/tmp%{+YYYY-MM-dd-HH-mm}.txt"
      stale_cleanup_type => "interval"
      stale_cleanup_interval => 2
      ingest_url => "#{@ingest_url}"
      cli_auth => true
      database => "#{@database}"
      table => "#{@table_with_mapping}"
      json_mapping => "#{@mapping_name}"
    }
    kusto {
      path => "#{@work_directory}/nomaptmp%{+YYYY-MM-dd-HH-mm}.txt"
      stale_cleanup_type => "interval"
      stale_cleanup_interval => 2
      cli_auth => true
      ingest_url => "#{@ingest_url}"
      database => "#{@database}"
      table => "#{@table_without_mapping}"
    }
    # Dynamic routing: a single output resolves database, table AND json_mapping
    # per event from event metadata, fanning events out to two ADX tables by
    # odd/even rownumber. Mapping names differ; TEST_SECOND_DATABASE optionally
    # exercises a second pre-provisioned database as well.
    kusto {
      path => "#{@work_directory}/dyntmp%{+YYYY-MM-dd-HH-mm}.txt"
      stale_cleanup_type => "interval"
      stale_cleanup_interval => 2
      cli_auth => true
      ingest_url => "#{@ingest_url}"
      database => "%{[@metadata][kusto_database]}"
      table => "%{[@metadata][kusto_table]}"
      json_mapping => "%{[@metadata][kusto_mapping]}"
    }
  }
}
  end

  def destinations
    [
      [@database, @table_with_mapping, @mapping_name],
      [@database, @table_without_mapping, nil],
      [@database, @table_dynamic_odd, @odd_mapping],
      [@even_database, @table_dynamic_even, @even_mapping]
    ]
  end

  def create_table_and_mapping
    destinations.each do |database, tableop, mapping|
      puts "Creating table #{tableop}"
      @query_client.executeMgmt(database, ".create table #{tableop} #{@columns}")
      (@created_tables ||= []) << [database, tableop]
      @query_client.executeMgmt(database, ".alter table #{tableop} policy ingestionbatching @'{\"MaximumBatchingTimeSpan\":\"00:00:10\", \"MaximumNumberOfItems\": 1, \"MaximumRawDataSizeMB\": 100}'")
      if mapping
        @query_client.executeMgmt(database, ".create table #{tableop} ingestion json mapping '#{mapping}' '#{File.read(File.join(__dir__, 'dataset_mapping.json'))}'")
      end
    end
  end


  def drop_and_cleanup
    (@created_tables || []).each do |database, tableop|
      puts "Dropping table #{tableop}"
      @query_client.executeMgmt(database, ".drop table #{tableop} ifexists")
    end
  end

  def run_logstash
    FileUtils.mkdir_p(@work_directory)
    logstashpath = File.join(@work_directory, 'logstash.conf')
    File.write(logstashpath, @logstash_config)
    File.write(@output_file, "")
    File.write(@input_file, "")
    lscommand = "#{@lslocalpath} -f #{logstashpath}"
    puts "Running logstash from config path #{logstashpath} and final command #{lscommand}"
    # Isolate the process group on POSIX so CI cleanup includes child processes.
    # Keep the per-run data directory and use PID-only cleanup on Windows.
    @logstash_process_group = !Gem.win_platform?
    process_options = @logstash_process_group ? { pgroup: true } : {}
    @logstash_pid = spawn(@lslocalpath, '-f', logstashpath, '--path.data',
                          File.join(@work_directory, 'data'), **process_options)
    begin
      sleep(60)
      data = File.read(@csv_file)
      File.open(@input_file, 'a') { |file| file.write(data) }
      sleep(60)
      puts File.read(@output_file)
    ensure
      # Stop before querying ADX: shutdown drains queued files and the next
      # streaming E2E test must not inherit a running Logstash process.
      stop_logstash
    end
  end

  # Terminate Logstash (and its POSIX process group). Safe to call repeatedly and
  # when no process was started. Both TERM and KILL waits remain bounded so a
  # hung child cannot prevent test-table cleanup and SDK client closure.
  def stop_logstash
    return if @logstash_pid.nil?
    target = @logstash_process_group ? -@logstash_pid : @logstash_pid
    begin
      Process.kill('TERM', target)
      reaped = wait_for_exit(@logstash_pid, 30)
      unless reaped
        puts "Logstash (pid #{@logstash_pid}) did not exit after TERM; sending KILL."
        Process.kill('KILL', target)
        wait_for_exit(@logstash_pid, 10)
      end
    rescue Errno::ESRCH, Errno::ECHILD
      # Already exited / already reaped.
    rescue => e
      puts "Error stopping logstash (pid #{@logstash_pid}): #{e}"
    ensure
      @logstash_pid = nil
      @logstash_process_group = nil
    end
  end

  # Polls for the child process to be reaped, up to timeout_seconds. Returns true
  # if it exited within the window, false otherwise. Uses a non-blocking wait so a
  # process that ignores TERM cannot block cleanup indefinitely.
  def wait_for_exit(pid, timeout_seconds)
    deadline = Time.now + timeout_seconds
    loop do
      begin
        return true if Process.waitpid(pid, Process::WNOHANG)
      rescue Errno::ECHILD
        return true # already reaped
      end
      return false if Time.now >= deadline
      sleep(0.5)
    end
  end

  def assert_data
    max_timeout = 120
    csv_data = CSV.read(@csv_file)
    # Static tables receive the full dataset and are validated row-by-row.
    Array[@table_with_mapping, @table_without_mapping].each { |tableop|
      puts "Validating results for table #{tableop}"
      validate_table_rows(tableop, csv_data, max_timeout, mapped: tableop == @table_with_mapping)
    }

    # Dynamic routing proof: a single output fanned events out to two tables by
    # odd/even rownumber. Validate that each table received exactly its subset,
    # which proves multiple dynamic destinations from one output.
    odd_rows = csv_data.select { |row| row[0].to_i.odd? }
    even_rows = csv_data.select { |row| row[0].to_i.even? }
    puts "Validating dynamic routing: #{odd_rows.length} odd rows -> #{@table_dynamic_odd}, #{even_rows.length} even rows -> #{@table_dynamic_even}"
    validate_table_rows(@table_dynamic_odd, odd_rows, max_timeout, mapped: true)
    validate_table_rows(@table_dynamic_even, even_rows, max_timeout, mapped: true, database: @even_database)
  end

  # Validates that an ADX table eventually contains exactly the expected rows
  # (retried because ingestion is asynchronous), comparing column by column.
  def validate_table_rows(tableop, expected_rows, max_timeout, mapped: false, database: @database)
    validated = false
    (0...max_timeout).each do |_|
      sleep(5)
      begin
        query = @query_client.executeQuery(database, "#{tableop} | sort by rownumber asc")
        result = query.getPrimaryResults()
      rescue StandardError => e
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
            # dataset_mapping maps this column from rowguid, while the default
            # name-based mapping reads xnumberAsText. Never overwrite the result.
            csv_item = expected_rows[i][1] if mapped
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
    begin
      create_table_and_mapping
      run_logstash
      assert_data
    ensure
      begin
        stop_logstash
        drop_and_cleanup
      ensure
        # Not all SDK query clients expose close.
        @query_client.close if @query_client.respond_to?(:close)
      end
    end
  end
end

E2E.new.start if $PROGRAM_NAME == __FILE__