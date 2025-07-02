# encoding: utf-8

require 'logstash/outputs/base'
require 'logstash/namespace'
require 'logstash/errors'

require 'logstash/outputs/kusto/customSizeBasedBuffer'
require 'logstash/outputs/kusto/kustoLogstashConfiguration'
require 'logstash/outputs/kusto/logStashFlushBuffer'
require 'logstash/outputs/kusto/filePersistence'

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
	config :proxy_protocol, validate: :string, required: false , default: 'https'
	# Maximum size of the buffer before it gets flushed, in MB. Defaults to 10.
	config :max_batch_size, validate: :number, required: false, default: 10

	# Interval (in seconds) before the buffer gets flushed, regardless of size. Defaults to 10.
	config :plugin_flush_interval, validate: :number, required: false, default: 10

	# Maximum number of items in the buffer before it gets flushed. Defaults to 1000.
	config :max_items, validate: :number, required: false, default: 1000

	# Process failed batches on startup. Defaults to false.
	config :process_failed_batches_on_startup, validate: :boolean, required: false, default: false

	# Directory to store the failed batches that were not uploaded to Kusto. If the directory does not exist, it will be created, defaults to a temporary directory called "logstash_backout" in Dir.tmpdir
    config :failed_dir_name, validate: :string, required: false, default: nil

	default :codec, 'json_lines'

	def register
		dir = failed_dir_name.nil? || failed_dir_name.empty? ? ::File.join(Dir.tmpdir, "logstash_backout") : failed_dir_name
		@file_persistence = LogStash::Outputs::KustoOutputInternal::FilePersistence.new(dir, @logger)

		kusto_ingest_base =  LogStash::Outputs::KustoInternal::KustoIngestConfiguration.new(ingest_url, database, table, json_mapping) 
		kusto_auth_base   =  LogStash::Outputs::KustoInternal::KustoAuthConfiguration.new(app_id, app_key, app_tenant, managed_identity, cli_auth) 
		kusto_proxy_base  =  LogStash::Outputs::KustoInternal::KustoProxyConfiguration.new(proxy_host , proxy_port , proxy_protocol, false) 
		kusto_flush_config = LogStash::Outputs::KustoInternal::KustoFlushConfiguration.new(max_items, plugin_flush_interval, max_batch_size, process_failed_batches_on_startup)
		kusto_upload_config = LogStash::Outputs::KustoInternal::KustoUploadConfiguration.new(upload_concurrent_count, upload_queue_size)
		kusto_logstash_configuration = LogStash::Outputs::KustoInternal::KustoLogstashConfiguration.new(kusto_ingest_base, kusto_auth_base , kusto_proxy_base, kusto_flush_config, kusto_upload_config, @logger, @file_persistence)
		kusto_logstash_configuration.validate_config
		# Initialize the custom buffer with size and interval
		@buffer = LogStash::Outputs::KustoOutputInternal::LogStashEventsBatcher.new(kusto_logstash_configuration, @logger)
	end


	public
	def multi_receive_encoded(events_and_encoded)
		events_and_encoded.each do |event, encoded|
			begin
				@buffer.batch_event(event.to_hash)
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
end
