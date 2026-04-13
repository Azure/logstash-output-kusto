require_relative 'e2e_base'

# Unified e2e test runner.  Executes all test scenarios sequentially
# (only one Logstash instance may run at a time).
#
# Test scenarios:
#   1. File mode   – explicit `path`, with and without json_mapping
#   2. Buffered mode (size)  – small max_batch_size triggers flush
#   3. Buffered mode (time)  – short plugin_flush_interval triggers flush
class E2EAll < E2EBase
  def run_all
    before_all

    run_test('File mode – with and without mapping') { test_file_mode }
    run_test('Buffered mode – size-based flush')     { test_buffered_size_flush }
    run_test('Buffered mode – time-based flush')     { test_buffered_time_flush }

    success = print_summary
  ensure
    after_all
    exit(1) unless success
  end

  private

  # ── Test 1: File mode ────────────────────────────────────────────
  # Sets `path` explicitly on both kusto outputs so the plugin uses
  # the file-based ingestion path (write to disk, then upload).
  def test_file_mode
    table_with_mapping = unique_table('RubyE2EFileMap')
    table_without_mapping = unique_table('RubyE2EFileNoMap')
    create_tables(
      [table_with_mapping, table_without_mapping],
      with_mapping: [table_with_mapping]
    )

    input_file  = test_path('input_file_mode.txt')
    output_file = test_path('output_file_mode.txt')

    config = %(
  input {
    #{file_input_block(input_file)}
  }
  filter {
    csv { columns => [#{@csv_columns}] }
  }
  output {
    file { path => "#{output_file}" }
    kusto {
      path => "/tmp/logstash-kusto-e2e/file_map/%{+YYYY-MM-dd}.log"
      ingest_url => "#{@ingest_url}"
      cli_auth => true
      database => "#{@database}"
      table => "#{table_with_mapping}"
      json_mapping => "#{@mapping_name}"
      stale_cleanup_type => "interval"
      stale_cleanup_interval => 10
    }
    kusto {
      path => "/tmp/logstash-kusto-e2e/file_nomap/%{+YYYY-MM-dd}.log"
      cli_auth => true
      ingest_url => "#{@ingest_url}"
      database => "#{@database}"
      table => "#{table_without_mapping}"
      stale_cleanup_type => "interval"
      stale_cleanup_interval => 10
    }
  }
)

    start_logstash(config, 'file_mode')
    assert_data(table_with_mapping)
    assert_data(table_without_mapping)
  end

  # ── Test 2: Buffered size flush ──────────────────────────────────
  # max_batch_size => 1024 (~1KB) so a few events fill the buffer.
  # Moderate interval as safety net; size should trigger flush first.
  def test_buffered_size_flush
    table = unique_table('RubyE2EBufSize')
    create_tables([table], with_mapping: [table])

    input_file  = test_path('input_buf_size.txt')
    output_file = test_path('output_buf_size.txt')

    config = %(
  input {
    #{file_input_block(input_file)}
  }
  filter {
    csv { columns => [#{@csv_columns}] }
  }
  output {
    file { path => "#{output_file}" }
    kusto {
      ingest_url => "#{@ingest_url}"
      cli_auth => true
      database => "#{@database}"
      table => "#{table}"
      json_mapping => "#{@mapping_name}"
      max_batch_size => 1024
      plugin_flush_interval => 30
      max_items => 10000
    }
  }
)

    start_logstash(config, 'buf_size')
    assert_data(table)
  end

  # ── Test 3: Buffered time flush ──────────────────────────────────
  # plugin_flush_interval => 5 (seconds) so the timer triggers flush.
  # Large batch size (100KB) ensures all 10 events (~5KB total) fit
  # comfortably without triggering a size-based flush.
  def test_buffered_time_flush
    table = unique_table('RubyE2EBufTime')
    create_tables([table], with_mapping: [table])

    input_file  = test_path('input_buf_time.txt')
    output_file = test_path('output_buf_time.txt')

    config = %(
  input {
    #{file_input_block(input_file)}
  }
  filter {
    csv { columns => [#{@csv_columns}] }
  }
  output {
    file { path => "#{output_file}" }
    kusto {
      ingest_url => "#{@ingest_url}"
      cli_auth => true
      database => "#{@database}"
      table => "#{table}"
      json_mapping => "#{@mapping_name}"
      max_batch_size => 102400
      plugin_flush_interval => 5
      max_items => 100000
    }
  }
)

    start_logstash(config, 'buf_time')
    assert_data(table)
  end

  def test_path(filename)
    File.expand_path(filename, __dir__)
  end
end

E2EAll.new.run_all
