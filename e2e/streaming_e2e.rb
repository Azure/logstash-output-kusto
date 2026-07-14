require '../lib/logstash-output-kusto_jars'
require 'fileutils'
require 'json'
require 'open3'
require 'securerandom'
require 'tmpdir'
require 'timeout'

class StreamingE2E
  REQUEST_LIMIT = 1_048_576
  POLICY_FALLBACK_PAYLOAD_BYTES = 7 * 1024 * 1024
  FALLBACK_PAYLOAD_BYTES = 11 * 1024 * 1024
  PROCESS_TIMEOUT_SECONDS = 240
  INGESTION_TIMEOUT_SECONDS = 600
  STREAMING_POLICY_PROPAGATION_SECONDS = 60

  def initialize
    @engine_url = required_env('ENGINE_URL')
    @ingest_url = required_env('INGEST_URL')
    @database = required_env('TEST_DATABASE')
    @logstash_path = ENV.fetch('LS_LOCAL_PATH', '/usr/share/logstash/bin/logstash')
    @run_id = SecureRandom.uuid
    @table = "RubyStreamingE2E#{Time.now.getutc.to_i}"
    @kusto = Java::com.microsoft.azure.kusto
  end

  def start
    @query_client = @kusto.data.ClientFactory.createClient(
      @kusto.data.auth.ConnectionStringBuilder.createWithAzureCli(@engine_url)
    )
    create_table

    events = build_events
    output = run_logstash(events)
    assert_request_outcomes(output)
    assert_ingested_events(events)
  ensure
    drop_table if @query_client
    @query_client.close if @query_client&.respond_to?(:close)
  end

  private

  def required_env(name)
    value = ENV[name]
    raise "#{name} must be set" if value.nil? || value.empty?

    value
  end

  def create_table
    @query_client.executeMgmt(@database, ".drop table #{@table} ifexists")
    @query_client.executeMgmt(
      @database,
      ".create table #{@table} (run_id:string, sequence:long, scenario:string, payload:string)"
    )
    @query_client.executeMgmt(
      @database,
      ".alter column #{@table}.payload policy encoding type='BigObject32'"
    )
    @query_client.executeMgmt(
      @database,
      ".alter table #{@table} policy streamingingestion enable"
    )
    @query_client.executeMgmt(
      @database,
      ".alter table #{@table} policy ingestionbatching " \
      "@'{\"MaximumBatchingTimeSpan\":\"00:00:10\", \"MaximumNumberOfItems\":1, " \
      "\"MaximumRawDataSizeMB\":100}'"
    )
    sleep STREAMING_POLICY_PROPAGATION_SECONDS
  end

  def drop_table
    @query_client.executeMgmt(@database, ".drop table #{@table} ifexists")
  rescue => e
    warn "Failed to drop #{@table}: #{e.message}"
  end

  def build_events
    small_events = 6.times.map do |index|
      event(index, 'chunked', 's' * (400 * 1024))
    end
    above_connector_limit = event(
      small_events.length,
      'single_above_connector_limit',
      'm' * (2 * 1024 * 1024)
    )
    policy_fallback = event(
      small_events.length + 1,
      'streaming_policy_fallback',
      'p' * POLICY_FALLBACK_PAYLOAD_BYTES
    )
    hard_fallback = event(
      small_events.length + 2,
      'streaming_hard_fallback',
      'l' * FALLBACK_PAYLOAD_BYTES
    )

    small_events + [above_connector_limit, policy_fallback, hard_fallback]
  end

  def event(sequence, scenario, payload)
    {
      'run_id' => @run_id,
      'sequence' => sequence,
      'scenario' => scenario,
      'payload' => payload
    }
  end

  def run_logstash(events)
    Dir.mktmpdir('logstash-kusto-streaming-e2e') do |directory|
      config_path = File.join(directory, 'logstash.conf')
      data_path = File.join(directory, 'data')
      File.write(config_path, logstash_config)

      command = [
        @logstash_path,
        '--path.data', data_path,
        '--log.level', 'debug',
        '--pipeline.workers', '1',
        '--pipeline.batch.size', '125',
        '--pipeline.batch.delay', '50',
        '-f', config_path
      ]

      output = +''
      status = nil
      Timeout.timeout(PROCESS_TIMEOUT_SECONDS) do
        Open3.popen2e(*command) do |stdin, stdout_and_stderr, wait_thread|
          reader = Thread.new do
            stdout_and_stderr.each_line { |line| output << line }
          end
          begin
            events.each { |item| stdin.puts(JSON.generate(item)) }
            stdin.close
            status = wait_thread.value
            reader.join
          ensure
            unless wait_thread.join(1)
              Process.kill('TERM', wait_thread.pid)
              unless wait_thread.join(30)
                Process.kill('KILL', wait_thread.pid)
                wait_thread.join
              end
            end
          end
        end
      end

      raise "Logstash failed with status #{status.exitstatus}\n#{output}" unless status.success?

      output
    end
  rescue Timeout::Error
    raise "Logstash did not finish within #{PROCESS_TIMEOUT_SECONDS} seconds"
  end

  def logstash_config
    <<~CONFIG
      input {
        stdin {
          codec => json
        }
      }
      output {
        kusto {
          ingestion_mode => "streaming"
          streaming_max_request_bytes => #{REQUEST_LIMIT}
          streaming_max_retry_attempts => 2
          streaming_retry_backoff_seconds => 1
          streaming_concurrent_requests => 2
          ingest_url => "#{@ingest_url}"
          cli_auth => true
          database => "#{@database}"
          table => "#{@table}"
        }
      }
    CONFIG
  end

  def assert_request_outcomes(output)
    unless output.include?('Streaming request accepted.')
      raise "No streaming success was logged\n#{output}"
    end

    unless output.match?(/status(?:=>|=)"?Succeeded"?/)
      raise "No request completed through streaming ingestion\n#{output}"
    end

    unless output.match?(/status(?:=>|=)"?Queued"?/)
      raise "The oversized event did not exercise queued fallback\n#{output}"
    end
    queued_count = output.scan(/status(?:=>|=)"?Queued"?/).length
    if queued_count < 2
      raise "Both streaming fallback boundaries were not exercised\n#{output}"
    end

    request_sizes = output.scan(/bytes(?:=>|=)(\d+)/).flatten.map(&:to_i)
    unless request_sizes.any? { |bytes| bytes > REQUEST_LIMIT }
      raise "The single event above the connector threshold was not sent intact: #{request_sizes}"
    end
    unless request_sizes.any? { |bytes| bytes > 10 * 1024 * 1024 }
      raise "The fallback request did not exceed the SDK hard streaming limit: #{request_sizes}"
    end
    unless request_sizes.any? { |bytes| bytes > 6 * 1024 * 1024 && bytes <= 10 * 1024 * 1024 }
      raise "The SDK JSON size-estimation fallback was not exercised: #{request_sizes}"
    end
  end

  def assert_ingested_events(events)
    expected_rows = events.map do |item|
      [item.fetch('sequence'), item.fetch('scenario'), item.fetch('payload').bytesize]
    end
    deadline = Time.now + INGESTION_TIMEOUT_SECONDS

    loop do
      result = @query_client.executeQuery(
        @database,
        "#{@table} | where run_id == '#{@run_id}' | project sequence, scenario, " \
        "payload_size=strlen(payload) | order by sequence asc"
      ).getPrimaryResults

      actual_rows = []
      while result.next
        actual_rows << [
          result.getLong('sequence'),
          result.getString('scenario'),
          result.getLong('payload_size')
        ]
      end
      return if actual_rows == expected_rows

      raise "Duplicate events detected: #{actual_rows.inspect}" if actual_rows.length > expected_rows.length
      if actual_rows.length == expected_rows.length
        raise "Ingested events do not match expected sequences: #{actual_rows.inspect}"
      end

      if Time.now >= deadline
        raise "Events did not land before timeout. Observed rows: #{actual_rows.inspect}"
      end

      sleep 2
    end
  end
end

StreamingE2E.new.start
