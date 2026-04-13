require '../lib/logstash-output-kusto_jars'
require 'csv'
require 'fileutils'

KUSTO_JAVA = Java::com.microsoft.azure.kusto

# Base class for all e2e tests. Provides shared infrastructure for:
# - Kusto client lifecycle (before_all / after_all)
# - Table creation and cleanup
# - Logstash start / stop with single-instance guarantee
# - Data feeding and assertion
# - Sequential test execution with per-test cleanup
class E2EBase
  STARTUP_WAIT = 60
  DATA_WAIT = 60
  STOP_TIMEOUT = 30

  def initialize
    @engine_url = ENV.fetch('ENGINE_URL', nil)
    @ingest_url = ENV.fetch('INGEST_URL', nil)
    @database = ENV.fetch('TEST_DATABASE', nil)
    @lslocalpath = ENV['LS_LOCAL_PATH'] || '/usr/share/logstash/bin/logstash'
    @columns = '(rownumber:int, rowguid:string, xdouble:real, xfloat:real, xbool:bool, ' \
               'xint16:int, xint32:int, xint64:long, xuint8:long, xuint16:long, xuint32:long, ' \
               'xuint64:long, xdate:datetime, xsmalltext:string, xtext:string, ' \
               'xnumberAsText:string, xtime:timespan, xtextWithNulls:string, xdynamicWithNulls:dynamic)'
    @csv_columns = '"rownumber", "rowguid", "xdouble", "xfloat", "xbool", "xint16", "xint32", ' \
                   '"xint64", "xuint8", "xuint16", "xuint32", "xuint64", "xdate", "xsmalltext", ' \
                   '"xtext", "xnumberAsText", "xtime", "xtextWithNulls", "xdynamicWithNulls"'
    @column_count = 19
    @csv_file = File.expand_path('dataset.csv', __dir__)
    @mapping_name = 'test_mapping'
    @logstash_pid = nil
    @tables_created = []
    @test_results = []
    @unique_suffix = "#{Time.now.getutc.to_i}_#{Process.pid}"
  end

  # ── Lifecycle ────────────────────────────────────────────────────

  def before_all
    puts '=== Setting up e2e test suite ==='
    @query_client = KUSTO_JAVA.data.ClientFactory.createClient(
      KUSTO_JAVA.data.auth.ConnectionStringBuilder.createWithAzureCli(@engine_url)
    )
    puts "Kusto client initialised for #{@engine_url}, database: #{@database}"
  end

  def after_all
    puts "\n=== Cleaning up e2e test suite ==="
    stop_logstash
    @tables_created.each do |table|
      puts "Dropping table #{table}"
      @query_client.executeMgmt(@database, ".drop table #{table} ifexists")
    rescue StandardError => e
      puts "Warning: failed to drop #{table}: #{e.message}"
    end
    cleanup_temp_files
  end

  # ── Table management ─────────────────────────────────────────────

  def create_tables(tables, with_mapping: [])
    tables.each do |table|
      puts "Creating table #{table}"
      @query_client.executeMgmt(@database, ".drop table #{table} ifexists")
      sleep(1)
      @query_client.executeMgmt(@database, ".create table #{table} #{@columns}")
      @query_client.executeMgmt(
        @database,
        ".alter table #{table} policy ingestionbatching " \
        "@'{\"MaximumBatchingTimeSpan\":\"00:00:10\", \"MaximumNumberOfItems\": 1, \"MaximumRawDataSizeMB\": 100}'"
      )
      if with_mapping.include?(table)
        mapping_json = File.read(File.expand_path('dataset_mapping.json', __dir__))
        @query_client.executeMgmt(
          @database,
          ".create table #{table} ingestion json mapping '#{@mapping_name}' '#{mapping_json}'"
        )
      end
      @tables_created << table
    end
  end

  # ── Logstash lifecycle ───────────────────────────────────────────

  def start_logstash(config_content, label)
    stop_logstash # guarantee single-instance

    input_file = test_path("input_#{label}.txt")
    output_file = test_path("output_#{label}.txt")
    config_file = test_path("logstash_#{label}.conf")

    File.write(config_file, config_content)
    File.write(output_file, '')
    File.write(input_file, '')

    lscommand = "#{@lslocalpath} -f #{File.absolute_path(config_file)}"
    puts "[#{label}] Starting logstash: #{lscommand}"
    @logstash_pid = spawn(lscommand)
    puts "[#{label}] PID #{@logstash_pid}, waiting #{STARTUP_WAIT}s for startup..."
    sleep(STARTUP_WAIT)

    # Verify process is still alive after startup wait
    begin
      Process.kill(0, @logstash_pid)
      puts "[#{label}] Logstash process #{@logstash_pid} is running"
    rescue Errno::ESRCH
      raise "Logstash process #{@logstash_pid} died during startup"
    end

    # Feed test data
    puts "[#{label}] Writing test data from #{@csv_file}"
    data = File.read(@csv_file)
    File.open(input_file, 'a') { |f| f.write(data) }
    puts "[#{label}] Waiting #{DATA_WAIT}s for data processing..."
    sleep(DATA_WAIT)
    output_content = File.read(output_file)
    line_count = output_content.lines.count
    puts "[#{label}] Output file has #{line_count} lines"
  end

  def stop_logstash
    return unless @logstash_pid

    pid = @logstash_pid
    @logstash_pid = nil
    puts "Stopping logstash (PID #{pid})..."

    begin
      Process.kill('TERM', pid)
    rescue Errno::ESRCH
      puts 'Logstash already exited'
      return
    end

    # Wait with timeout, then SIGKILL as fallback
    deadline = Time.now + STOP_TIMEOUT
    loop do
      Process.waitpid(pid, Process::WNOHANG)
      Process.kill(0, pid) # check still alive
      if Time.now >= deadline
        puts "Logstash did not stop within #{STOP_TIMEOUT}s, sending SIGKILL"
        Process.kill('KILL', pid)
        Process.wait(pid)
        return
      end
      sleep(1)
    rescue Errno::ESRCH, Errno::ECHILD
      puts 'Logstash stopped gracefully'
      return
    end
  end

  # ── Data assertion ───────────────────────────────────────────────

  def assert_data(table, max_attempts: 20)
    csv_data = CSV.read(@csv_file)
    puts "Validating table #{table} (expecting #{csv_data.length} rows)"

    validated = false
    max_attempts.times do |attempt|
      begin
        sleep(5)
        query = @query_client.executeQuery(@database, "#{table} | sort by rownumber asc")
        result = query.getPrimaryResults
        unless result.count == csv_data.length
          puts "  Attempt #{attempt + 1}/#{max_attempts}: got #{result.count}/#{csv_data.length} rows"
          next
        end
      rescue StandardError => e
        puts "  Attempt #{attempt + 1}/#{max_attempts}: #{e.message}"
        next
      end

      validate_rows(csv_data, result, table)
      puts "  All #{csv_data.length} rows validated for #{table}"
      validated = true
      break
    end
    raise "Timed out waiting for data in #{table} after #{max_attempts} attempts" unless validated
  end

  # ── Test runner ──────────────────────────────────────────────────

  def run_test(name)
    puts "\n#{'=' * 60}"
    puts "TEST: #{name}"
    puts '=' * 60
    begin
      yield
      @test_results << { name: name, status: :passed }
      puts "PASSED: #{name}"
    rescue StandardError => e
      @test_results << { name: name, status: :failed, error: e }
      puts "FAILED: #{name}: #{e.message}"
      puts e.backtrace.first(10).join("\n")
    ensure
      stop_logstash
    end
  end

  def print_summary
    puts "\n#{'=' * 60}"
    puts 'TEST SUMMARY'
    puts '=' * 60
    passed = @test_results.count { |r| r[:status] == :passed }
    failed = @test_results.count { |r| r[:status] == :failed }
    @test_results.each do |r|
      icon = r[:status] == :passed ? 'PASS' : 'FAIL'
      line = "  [#{icon}] #{r[:name]}"
      line += " - #{r[:error].message}" if r[:error]
      puts line
    end
    puts "\n#{passed} passed, #{failed} failed out of #{@test_results.length} tests"
    failed.zero?
  end

  # ── Helpers ──────────────────────────────────────────────────────

  def unique_table(prefix)
    "#{prefix}#{@unique_suffix}"
  end

  # Build a file-input block with sincedb disabled so each test gets a clean read
  def file_input_block(input_file)
    %(
    file {
      path => "#{input_file}"
      sincedb_path => "/dev/null"
      start_position => "beginning"
    })
  end

  private

  def test_path(filename)
    File.expand_path(filename, __dir__)
  end

  def cleanup_temp_files
    %w[input_*.txt output_*.txt logstash_*.conf].each do |pattern|
      Dir.glob(File.expand_path(pattern, __dir__)).each do |f|
        File.delete(f)
      rescue StandardError => e
        puts "Warning: could not delete #{f}: #{e.message}"
      end
    end
  end

  def validate_rows(csv_data, result, table)
    csv_data.length.times do |i|
      result.next
      @column_count.times do |j|
        csv_item = csv_data[i][j]
        result_item = result.getObject(j).nil? ? 'null' : result.getString(j)
        case j
        when 4 # kusto boolean
          csv_item = csv_item.to_s == '1' ? 'true' : 'false'
        when 12 # date formatting
          csv_item = csv_item.sub('.0000000', '')
        when 15 # numbers as text
          result_item = i.to_s
        when 17 # null
          next
        end
        next if csv_item == result_item

        raise "Mismatch at row #{i}, col #{j} in #{table}: csv='#{csv_item}' result='#{result_item}'"
      end
    end
  end
end
