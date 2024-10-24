# encoding: utf-8

require 'logstash/outputs/base'
require 'logstash/namespace'
require 'logstash/errors'

require 'logstash/outputs/kusto/ingestor'
require 'logstash/outputs/kusto/interval'
require 'logstash/outputs/kusto/custom_size_based_buffer'

##
# This plugin sends messages to Azure Kusto in batches.
#
class LogStash::Outputs::Kusto < LogStash::Outputs::Base
  config_name 'kusto'
  concurrency :shared

  FIELD_REF = /%\{[^}]+\}/

  attr_reader :failure_path

  # The Kusto endpoint for ingestion related communication. You can see it on the Azure Portal.
  config :ingest_url, validate: :string, required: true

  # The following are the credentials used to connect to the Kusto service
  # application id 
  config :app_id, validate: :string, required: false
  # application key (secret)
  config :app_key, validate: :password, required: false
  # aad tenant id
  config :app_tenant, validate: :string, default: nil
  # managed identity id
  config :managed_identity, validate: :string, default: nil
  # CLI credentials for dev-test
  config :cli_auth, validate: :boolean, default: false
  # The following are the data settings that impact where events are written to
  # Database name
  config :database, validate: :string, required: true
  # Target table name
  config :table, validate: :string, required: true
  # Mapping name - Used by Kusto to map each attribute from incoming event JSON strings to the appropriate column in the table.
  # Note that this must be in JSON format, as this is the interface between Logstash and Kusto
  # Make this optional as name resolution in the JSON mapping can be done based on attribute names in the incoming event JSON strings
  config :json_mapping, validate: :string, default: nil

  # Mapping name - deprecated, use json_mapping
  config :mapping, validate: :string, deprecated: true

  # Path - deprecated
  config :path, validate: :string, deprecated: true

  # TODO: will be used to route events to many tables according to event properties
  config :dynamic_event_routing, validate: :boolean, default: false

  # Specify how many files can be uploaded concurrently
  config :upload_concurrent_count, validate: :number, default: 3

  # Specify how many files can be kept in the upload queue before the main process
  # starts processing them in the main thread (not healthy)
  config :upload_queue_size, validate: :number, default: 30

  # Host of the proxy , is an optional field. Can connect directly
  config :proxy_host, validate: :string, required: false

  # Port where the proxy runs , defaults to 80. Usually a value like 3128
  config :proxy_port, validate: :number, required: false , default: 80

  # Check Proxy URL can be over http or https. Do we need it this way or ignore this & remove this
  config :proxy_protocol, validate: :string, required: false , default: 'http'

  # Maximum size of the buffer before it gets flushed, defaults to 10MB
  config :max_size, validate: :number, default: 10

  # Maximum interval (in seconds) before the buffer gets flushed, defaults to 10
  config :max_interval, validate: :number, default: 10

  default :codec, 'json_lines'

  def register
    # Initialize the custom buffer with size and interval
    @buffer = LogStash::Outputs::CustomSizeBasedBuffer.new(@max_size, @max_interval) do |events|
      flush_buffer(events)
    end
  
    @io_mutex = Mutex.new
  
    final_mapping = json_mapping
    final_mapping = mapping if final_mapping.nil? || final_mapping.empty?
  
    executor = Concurrent::ThreadPoolExecutor.new(min_threads: 1,
                                                  max_threads: upload_concurrent_count,
                                                  max_queue: upload_queue_size,
                                                  fallback_policy: :caller_runs)
  
    @ingestor = Ingestor.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cli_auth, database, table, final_mapping, proxy_host, proxy_port, proxy_protocol, @logger, executor)
    
    # Deprecation warning for path
    if @path
      @logger.warn("The 'path' configuration option is deprecated and will be removed in a future release.")
    end
  end


  public
  def multi_receive_encoded(events_and_encoded)
    events_and_encoded.each do |event, encoded|
      begin
        @buffer << encoded
      rescue => e
        @logger.error("Error processing event: #{e.message}")
      end
    end
  end

  def close
    @logger.info("Closing Kusto output plugin")
  
    begin
      @buffer.shutdown unless @buffer.nil?
      @logger.info("Buffer shutdown") unless @buffer.nil?
    rescue => e
      @logger.error("Error shutting down buffer: #{e.message}")
      @logger.error(e.backtrace.join("\n"))
    end
  
    begin
      @ingestor.stop unless @ingestor.nil?
      @logger.info("Ingestor stopped") unless @ingestor.nil?
    rescue => e
      @logger.error("Error stopping ingestor: #{e.message}")
      @logger.error(e.backtrace.join("\n"))
    end
  
    @logger.info("Kusto output plugin Closed")
  end

  public
  def flush_buffer(events)
    return if events.empty?
    @logger.info("flush_buffer with #{events.size} events")
    begin
      @ingestor.upload_async(events.join)
    rescue => e
      @logger.error("Error during flush: #{e.message}")
      @logger.error(e.backtrace.join("\n"))
      raise e # Exception is raised to trigger the rescue block in buffer_flush
    end
  end

end
