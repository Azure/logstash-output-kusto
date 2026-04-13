require_relative 'e2e_base'

# Standalone runner for the original file-mode e2e test.
# Prefer running e2e_all.rb which covers all scenarios.
class E2E < E2EBase
  def start
    before_all

    table_with_mapping = unique_table('RubyE2E')
    table_without_mapping = unique_table('RubyE2ENoMapping')
    create_tables(
      [table_with_mapping, table_without_mapping],
      with_mapping: [table_with_mapping]
    )

    input_file  = File.expand_path('input_file.txt', __dir__)
    output_file = File.expand_path('output_file.txt', __dir__)

    config = %(
  input {
    #{file_input_block(input_file)}
  }
  filter {
    csv { columns => [#{@csv_columns}] }
  }
  output {
    file { path => "#{output_file}" }
    stdout { codec => rubydebug }
    kusto {
      ingest_url => "#{@ingest_url}"
      cli_auth => true
      database => "#{@database}"
      table => "#{table_with_mapping}"
      json_mapping => "#{@mapping_name}"
    }
    kusto {
      cli_auth => true
      ingest_url => "#{@ingest_url}"
      database => "#{@database}"
      table => "#{table_without_mapping}"
    }
  }
)

    start_logstash(config, 'e2e')
    assert_data(table_with_mapping)
    assert_data(table_without_mapping)
  ensure
    after_all
  end
end

E2E.new.start
