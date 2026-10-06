require_relative '../lib/logstash-output-kusto_jars'
require 'csv'
require 'tmpdir'
require 'securerandom'
require 'fileutils'
require 'json'

$kusto_java = Java::com.microsoft.azure.kusto

class E2E
  class ShutdownError < StandardError; end
  class ProgressError < StandardError; end

  TERM_TIMEOUT_SECONDS = 30
  KILL_TIMEOUT_SECONDS = 10
  STARTUP_TIMEOUT_SECONDS = 300
  INPUT_TIMEOUT_SECONDS = 120
  INGESTION_TIMEOUT_SECONDS = 600
  QUERY_TIMEOUT_SECONDS = 60

  attr_reader :logstash_status

  def initialize
    super
    run_id = SecureRandom.hex(8)
    @work_directory = File.join(Dir.tmpdir, "kusto-e2e-#{run_id}").tr('\\', '/')
    @input_file = "#{@work_directory}/input.csv"
    @output_file = "#{@work_directory}/output.json"
    @logstash_log = "#{@work_directory}/logstash.log"
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
    @require_second_database = ENV['E2E_REQUIRE_SECOND_DATABASE'] == 'true'
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
    file { path => "#{@output_file}" codec => json_lines flush_interval => 0 }
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

  def validate_test_databases
    distinct = @database && @even_database && !@even_database.strip.empty? &&
               !@even_database.start_with?('$(') && !@even_database.casecmp?(@database)
    if @require_second_database && !distinct
      raise ArgumentError,
            'TEST_SECOND_DATABASE must name a distinct pre-provisioned database when E2E_REQUIRE_SECOND_DATABASE=true.'
    end
    warn 'Single-database smoke test: cross-database routing is NOT covered.' unless distinct
  end

  def create_table_and_mapping
    destinations.each do |database, tableop, mapping|
      puts "Creating table #{tableop}"
      # Track this run's unique name before a create response can be lost.
      (@created_tables ||= []) << [database, tableop]
      @query_client.executeMgmt(database, ".create table #{tableop} #{@columns}")
      @query_client.executeMgmt(database, ".alter table #{tableop} policy ingestionbatching @'{\"MaximumBatchingTimeSpan\":\"00:00:10\", \"MaximumNumberOfItems\": 1, \"MaximumRawDataSizeMB\": 100}'")
      if mapping
        @query_client.executeMgmt(database, ".create table #{tableop} ingestion json mapping '#{mapping}' '#{File.read(File.join(__dir__, 'dataset_mapping.json'))}'")
      end
    end
  end


  def drop_and_cleanup
    if @logstash_pid && !@logstash_terminated
      retained = (@created_tables || []).map { |database, table| "#{database}.#{table}" }.join(', ')
      raise ShutdownError,
            "Logstash (pid #{@logstash_pid}) termination is not confirmed; retaining run-owned tables: #{retained}"
    end

    failures = []
    (@created_tables || []).dup.each do |database, tableop|
      begin
        puts "Dropping table #{tableop}"
        @query_client.executeMgmt(database, ".drop table #{tableop} ifexists")
        @created_tables.delete([database, tableop])
      rescue StandardError => e
        failures << e
        warn "Failed to drop #{database}.#{tableop}: #{e.class}: #{e.message}"
      end
    end
    raise failures.first unless failures.empty?
  end

  def run_logstash
    raise ShutdownError, "Logstash (pid #{@logstash_pid}) is still tracked; cannot start another process" if @logstash_pid

    FileUtils.mkdir_p(@work_directory)
    logstashpath = File.join(@work_directory, 'logstash.conf')
    File.write(logstashpath, @logstash_config)
    File.write(@output_file, "")
    File.write(@input_file, "")
    File.write(@logstash_log, "")
    lscommand = "#{@lslocalpath} -f #{logstashpath}"
    puts "Running logstash from config path #{logstashpath} and final command #{lscommand}"
    # Isolate the process group on POSIX so CI cleanup includes child processes.
    # Keep the per-run data directory and use PID-only cleanup on Windows.
    @logstash_process_group = !Gem.win_platform?
    @logstash_status = nil
    @logstash_reaped = false
    @logstash_terminated = false
    @logstash_term_sent = false
    @logstash_kill_sent = false
    process_options = { out: @logstash_log, err: [:child, :out] }
    process_options[:pgroup] = true if @logstash_process_group
    config_argument = Gem.win_platform? ? logstashpath.tr('/', '\\') : logstashpath
    @logstash_pid = spawn(@lslocalpath, '-f', config_argument, '--path.data',
                          File.join(@work_directory, 'data'), **process_options)
    with_cleanup(:stop_logstash) do
      wait_for_readiness
      data = File.read(@csv_file)
      File.open(@input_file, 'a') { |file| file.write(data) }
      wait_for_input
      puts 'Validating idle ingestion before stopping Logstash'
      assert_data(while_running: true)
    end
  rescue StandardError => e
    report_progress_failure(e)
    raise
  end

  def wait_for_readiness(timeout_seconds = STARTUP_TIMEOUT_SECONDS)
    wait_for_progress('pipeline readiness', timeout_seconds) do
      File.foreach(@logstash_log).any? { |line| line.include?('Pipeline started') }
    end
  end

  def wait_for_input(timeout_seconds = INPUT_TIMEOUT_SECONDS)
    expected = CSV.read(@csv_file).map { |row| row.take(2) }.sort
    wait_for_progress('input consumption', timeout_seconds) do
      actual = File.readlines(@output_file).select { |line| line.end_with?("\n") }.map do |line|
        value = JSON.parse(line)
        [value['rownumber'].to_s, value['rowguid'].to_s]
      end.sort
      @observed_input_rows = actual.length
      if actual.length >= expected.length && actual != expected
        raise ProgressError, 'Logstash file output does not match the input row identities'
      end
      actual == expected
    end
  end

  def wait_for_progress(stage, timeout_seconds)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout_seconds
    loop do
      ensure_logstash_running!
      ready = yield
      ensure_logstash_running!
      return if ready

      remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
      raise ProgressError, "Timed out waiting for #{stage}" if remaining <= 0
      sleep([0.5, remaining].min)
    end
  rescue StandardError => e
    report_progress_failure(e)
    raise
  end
  private :wait_for_progress

  def ensure_logstash_running!
    raise ProgressError, 'Logstash exited before live ingestion validation completed' unless @logstash_pid
    wait_for_exit(@logstash_pid, 0)
    if @logstash_reaped || @logstash_status || @logstash_terminated
      raise ProgressError, 'Logstash exited before live ingestion validation completed'
    end
  end
  private :ensure_logstash_running!

  def logstash_log_tail
    return '' unless File.file?(@logstash_log)
    File.open(@logstash_log, 'rb') do |file|
      file.seek([file.size - 16_384, 0].max)
      file.read
    end
  end
  private :logstash_log_tail

  def report_progress_failure(error)
    return if @reported_progress_error.equal?(error)
    @reported_progress_error = error
    warn "E2E progress failed: #{error.class}: #{error.message}; pid=#{@logstash_pid}; " \
         "observed_rows=#{@observed_input_rows || 0}; input=#{@input_file}; output=#{@output_file}; " \
         "log=#{@logstash_log}\n#{logstash_log_tail}"
  rescue StandardError
    # Diagnostics must not replace the original validation or cleanup error.
  end
  private :report_progress_failure

  # Only clear ownership after reaping the child and confirming its group is
  # gone. A forced shutdown is a test failure even when KILL finishes cleanup.
  def stop_logstash
    return if @logstash_pid.nil?

    pid = @logstash_pid
    target = @logstash_process_group ? -pid : pid
    status = wait_for_exit(pid, 0)
    unless status
      if @logstash_kill_sent
        signal_logstash('KILL', target)
        status = wait_for_exit(pid, KILL_TIMEOUT_SECONDS)
      else
        sent = signal_logstash('TERM', target)
        @logstash_term_sent ||= sent && @logstash_status.nil?
        status = wait_for_exit(pid, TERM_TIMEOUT_SECONDS)
        unless status
          @logstash_kill_sent = signal_logstash('KILL', target)
          status = wait_for_exit(pid, KILL_TIMEOUT_SECONDS)
        end
      end
    end
    raise ShutdownError, "Logstash (pid #{pid}) termination was not confirmed after TERM/KILL" unless status
    raise ShutdownError, "Logstash (pid #{pid}) required KILL to terminate" if @logstash_kill_sent

    term = Signal.list.fetch('TERM')
    # JVMs may report a handled SIGTERM as exit status 128 + TERM.
    expected_term = @logstash_term_sent && (status.termsig == term || status.exitstatus == 128 + term)
    unless status.success? || expected_term
      detail = status.termsig ? "signal #{status.termsig}" : "exit status #{status.exitstatus}"
      raise ShutdownError, "Logstash (pid #{pid}) terminated with #{detail}"
    end
    status
  ensure
    if @logstash_terminated
      @logstash_pid = nil
      @logstash_process_group = nil
    end
  end

  # Return the recorded status only after all members of the owned POSIX group
  # disappear. On Windows only the spawned PID is tracked.
  def wait_for_exit(pid, timeout_seconds)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout_seconds
    target = @logstash_process_group ? -pid : pid
    loop do
      unless @logstash_reaped
        begin
          result = Process.waitpid2(pid, Process::WNOHANG)
          if result
            @logstash_status = result.last
            @logstash_reaped = true
          end
        rescue Errno::ECHILD
          @logstash_reaped = true
        end
      end
      if @logstash_reaped && !logstash_alive?(target)
        @logstash_terminated = true
        raise ShutdownError, "Logstash (pid #{pid}) exit status is unavailable" unless @logstash_status
        return @logstash_status
      end
      remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
      return nil if remaining <= 0
      sleep([remaining, 0.1].min)
    end
  end

  def logstash_alive?(target)
    # Reaping confirms this child is gone; do not probe a potentially reused PID.
    return false if !@logstash_process_group && @logstash_status
    Process.kill(0, target)
    true
  rescue Errno::ESRCH
    false
  end
  private :logstash_alive?

  def signal_logstash(signal, target)
    if !@logstash_process_group && @logstash_reaped
      raise ShutdownError, 'Logstash exit status is unavailable; refusing to signal a reaped PID'
    end
    Process.kill(signal, target)
    true
  rescue Errno::ESRCH
    false
  end
  private :signal_logstash

  def assert_data(while_running: false)
    max_timeout = 120
    polling = { while_running: while_running,
                deadline: Process.clock_gettime(Process::CLOCK_MONOTONIC) + INGESTION_TIMEOUT_SECONDS }
    csv_data = CSV.read(@csv_file)
    # Static tables receive the full dataset and are validated row-by-row.
    Array[@table_with_mapping, @table_without_mapping].each { |tableop|
      puts "Validating results for table #{tableop}"
      validate_table_rows(tableop, csv_data, max_timeout, mapped: tableop == @table_with_mapping, **polling)
    }

    # Dynamic routing proof: a single output fanned events out to two tables by
    # odd/even rownumber. Validate that each table received exactly its subset,
    # which proves multiple dynamic destinations from one output.
    odd_rows = csv_data.select { |row| row[0].to_i.odd? }
    even_rows = csv_data.select { |row| row[0].to_i.even? }
    puts "Validating dynamic routing: #{odd_rows.length} odd rows -> #{@table_dynamic_odd}, #{even_rows.length} even rows -> #{@table_dynamic_even}"
    validate_table_rows(@table_dynamic_odd, odd_rows, max_timeout, mapped: true, **polling)
    validate_table_rows(@table_dynamic_even, even_rows, max_timeout, mapped: true, database: @even_database, **polling)
  end

  # Validates that an ADX table eventually contains exactly the expected rows
  # (retried because ingestion is asynchronous), comparing column by column.
  def validate_table_rows(tableop, expected_rows, max_timeout, mapped: false, database: @database,
                         while_running: false, deadline: nil)
    deadline ||= Process.clock_gettime(Process::CLOCK_MONOTONIC) + INGESTION_TIMEOUT_SECONDS
    validated = false
    (0...max_timeout).each do |attempt|
      ensure_logstash_running! if while_running
      remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
      break if remaining <= 0
      sleep([5, remaining].min) if attempt > 0
      ensure_logstash_running! if while_running
      remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
      break if remaining <= 0
      properties = $kusto_java.data.ClientRequestProperties.new
      properties.setTimeoutInMilliSec(([remaining, QUERY_TIMEOUT_SECONDS].min * 1000).ceil)
      begin
        query = @query_client.executeQuery(database, "#{tableop} | sort by rownumber asc", properties)
        result = query.getPrimaryResults()
      rescue StandardError => e
        puts "Error querying #{tableop}: #{e}"
        next
      end
      ensure_logstash_running! if while_running
      break if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
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
      ensure_logstash_running! if while_running
      puts "Table #{tableop} validated successfully (#{expected_rows.length} rows)"
      validated = true
      break
    end
    raise "Failed after timeouts validating table #{tableop}" unless validated
  end

  def start
    validate_test_databases
    with_cleanup(:stop_logstash, :drop_and_cleanup, :close_query_client) do
      @query_client = $kusto_java.data.ClientFactory.createClient($kusto_java.data.auth.ConnectionStringBuilder::createWithAzureCli(@engine_url))
      create_table_and_mapping
      run_logstash
      assert_data
    end
  end

  def close_query_client
    # Not all SDK query clients expose close.
    @query_client.close if @query_client.respond_to?(:close)
  end
  private :close_query_client

  def with_cleanup(*actions)
    primary_error = nil
    begin
      yield
    rescue Exception => e # Preserve interrupts as well as validation errors.
      primary_error = e
      raise
    ensure
      failures = []
      actions.each do |action|
        begin
          send(action)
        rescue StandardError => e
          failures << e
          warn "Cleanup #{action} failed: #{e.class}: #{e.message}"
        end
      end
      raise failures.first if primary_error.nil? && !failures.empty?
    end
  end
  private :with_cleanup
end

E2E.new.start if $PROGRAM_NAME == __FILE__