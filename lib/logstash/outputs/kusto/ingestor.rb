# encoding: utf-8

require 'logstash/outputs/base' 
require 'logstash/namespace'
require 'logstash/errors'

class LogStash::Outputs::Kusto < LogStash::Outputs::Base
  ##
  # This handles the overall logic and communication with Kusto
  #
  class Ingestor
    require 'kusto/kusto-ingest-1.0.0-BETA-01-jar-with-dependencies.jar'

    RETRY_DELAY_SECONDS = 3
    DEFAULT_THREADPOOL = Concurrent::ThreadPoolExecutor.new(
      min_threads: 1,
      max_threads: 8,
      max_queue: 1,
      fallback_policy: :caller_runs
    )
    LOW_QUEUE_LENGTH = 3
    FIELD_REF = /%\{[^}]+\}/

    def initialize(ingest_url, app_id, app_key, app_tenant, database, table, mapping, delete_local, logger, threadpool = DEFAULT_THREADPOOL)
      @workers_pool = threadpool
      @logger = logger

      validate_config(database, table, mapping)

      @logger.debug('Preparing Kusto resources.')

      kusto_connection_string = Java::com.microsoft.azure.kusto.data.ConnectionStringBuilder.createWithAadApplicationCredentials(ingest_url, app_id, app_key.value, app_tenant)

      @kusto_client = Java::com.microsoft.azure.kusto.ingest.IngestClientFactory.createClient(kusto_connection_string)

      @ingestion_properties = Java::com.microsoft.azure.kusto.ingest.IngestionProperties.new(database, table)
      @ingestion_properties.setJsonMappingName(mapping)

      @delete_local = delete_local

      @logger.debug('Kusto resources are ready.')
    end

    def validate_config(database, table, mapping)
      if database =~ FIELD_REF
        @logger.error('database config value should not be dynamic.', database)
        raise LogStash::ConfigurationError.new('database config value should not be dynamic.')
      end

      if table =~ FIELD_REF
        @logger.error('table config value should not be dynamic.', table)
        raise LogStash::ConfigurationError.new('table config value should not be dynamic.')
      end

      if mapping =~ FIELD_REF
        @logger.error('mapping config value should not be dynamic.', mapping)
        raise LogStash::ConfigurationError.new('mapping config value should not be dynamic.')
      end
    end

    def upload_async(path, delete_on_success)
      if @workers_pool.remaining_capacity <= LOW_QUEUE_LENGTH
        @logger.warn("Ingestor queue capacity is running low with #{@workers_pool.remaining_capacity} free slots.")
      end

      @workers_pool.post do
        LogStash::Util.set_thread_name("Kusto to ingest file: #{path}")
        upload(path, delete_on_success)
      end
    rescue Exception => e
      @logger.error('StandardError.', exception: e.class, message: e.message, path: path, backtrace: e.backtrace)
      raise e
    end

    def upload(path, delete_on_success)
      file_size = File.size(path)
      @logger.debug("Sending file to kusto: #{path}. size: #{file_size}")

      # TODO: dynamic routing
      # file_metadata = path.partition('.kusto.').last
      # file_metadata_parts = file_metadata.split('.')

      # if file_metadata_parts.length == 3
      #   # this is the number we expect - database, table, mapping
      #   database = file_metadata_parts[0]
      #   table = file_metadata_parts[1]
      #   mapping = file_metadata_parts[2]

      #   local_ingestion_properties = Java::KustoIngestionProperties.new(database, table)
      #   local_ingestion_properties.addJsonMappingName(mapping)
      # end

      file_source_info = Java::com.microsoft.azure.kusto.ingest.source.FileSourceInfo.new(path, 0); # 0 - let the sdk figure out the size of the file
      @kusto_client.ingestFromFile(file_source_info, @ingestion_properties)

      File.delete(path) if delete_on_success

      @logger.debug("File #{path} sent to kusto.")
    rescue Java::JavaNioFile::NoSuchFileException => e
      @logger.error("File doesn't exist! Unrecoverable error.", exception: e.class, message: e.message, path: path, backtrace: e.backtrace)
    rescue => e
      # When the retry limit is reached or another error happen we will wait and retry.
      #
      # Thread might be stuck here, but I think its better than losing anything
      # its either a transient errors or something bad really happened.
      @logger.error('Uploading failed, retrying.', exception: e.class, message: e.message, path: path, backtrace: e.backtrace)
      sleep RETRY_DELAY_SECONDS
      retry
    end

    def stop
      @workers_pool.shutdown
      @workers_pool.wait_for_termination(nil) # block until its done
    end
  end
end
