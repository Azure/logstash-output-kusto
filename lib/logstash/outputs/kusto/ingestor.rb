# encoding: utf-8

require 'logstash/outputs/base'
require 'logstash/namespace'
require 'logstash/errors'

class LogStash::Outputs::Kusto < LogStash::Outputs::Base
  ##
  # This handles the overall logic and communication with Kusto
  #
  class Ingestor
    require 'logstash-output-kusto_jars'
    RETRY_DELAY_SECONDS = 3
    DEFAULT_THREADPOOL = Concurrent::ThreadPoolExecutor.new(
      min_threads: 1,
      max_threads: 8,
      max_queue: 1,
      fallback_policy: :caller_runs
    )
    LOW_QUEUE_LENGTH = 3
    FIELD_REF = /%\{[^}]+\}/

    def initialize(ingest_url, app_id, app_key, app_tenant, managed_identity_id, database, table, json_mapping, delete_local, proxy_host , proxy_port , proxy_protocol,proxy_aad_only, logger, threadpool = DEFAULT_THREADPOOL)
      @workers_pool = threadpool
      @logger = logger
      @kustoLogstashConfiguration = LogStash::Outputs::KustoInternal::KustoLogstashConfiguration.new(ingest_url, app_id, app_key, app_tenant, managed_identity_id, database, table, json_mapping, delete_local, proxy_host , proxy_port , proxy_protocol,proxy_aad_only, logger)
      @kustoLogstashConfiguration.validate_config()
      @logger.info('Preparing Kusto resources.')
      @ingestion_properties = get_ingestion_properties()
      if @kustoLogstashConfiguration.proxy_aad_only
        @kustoAadTokenProvider = LogStash::Outputs::KustoInternal::KustoAadTokenProvider.new(@kustoLogstashConfiguration)
      end
      @delete_local = delete_local
      @logger.debug('Kusto resources are ready.')
    end

    def get_kusto_client()
      if @kusto_client.nil?
        kusto_client = create_kusto_client()
      end
      return kusto_client
    end

    def create_kusto_client()
      kusto_java = Java::com.microsoft.azure.kusto
      apache_http = Java::org.apache.http
      # Create a connection string
      kusto_connection_string = if @kustoLogstashConfiguration.is_managed_identity
          if @kustoLogstashConfiguration.is_system_assigned_managed_identity
            @logger.info('Using system managed identity.')
            kusto_java.data.auth.ConnectionStringBuilder.createWithAadManagedIdentity(ingest_url)  
          else
            @logger.info('Using user managed identity.')
            kusto_java.data.auth.ConnectionStringBuilder.createWithAadManagedIdentity(ingest_url, managed_identity_id)
          end
        elsif @kustoLogstashConfiguration.proxy_aad_only
          kusto_java.data.auth.ConnectionStringBuilder.createWithAccessToken(ingest_url, )
        else
          kusto_java.data.auth.ConnectionStringBuilder.createWithAadApplicationCredentials(ingest_url, app_id, app_key.value, app_tenant)
        end
      #
      @logger.debug(Gem.loaded_specs.to_s)
      # Unfortunately there's no way to avoid using the gem/plugin name directly...
      name_for_tracing = "logstash-output-kusto:#{Gem.loaded_specs['logstash-output-kusto']&.version || "unknown"}"
      @logger.debug("Client name for tracing: #{name_for_tracing}")

      tuple_utils = Java::org.apache.commons.lang3.tuple
      # kusto_connection_string.setClientVersionForTracing(name_for_tracing)
      version_for_tracing=Gem.loaded_specs['logstash-output-kusto']&.version || "unknown"
      kusto_connection_string.setConnectorDetails("Logstash",version_for_tracing.to_s,"","",false,"", tuple_utils.Pair.emptyArray());
      
      @kusto_client = begin
        if is_direct_conn
          kusto_java.ingest.IngestClientFactory.createClient(kusto_connection_string)
        else
          kusto_http_client_properties = kusto_java.data.HttpClientProperties.builder().proxy(apache_http.HttpHost.new(proxy_host,proxy_port,proxy_protocol)).build()
          kusto_java.ingest.IngestClientFactory.createClient(kusto_connection_string, kusto_http_client_properties)
        end
      end





    end

    def get_ingestion_properties()
      ingestion_properties = kusto_java.ingest.IngestionProperties.new(@kustoLogstashConfiguration.database, @kustoLogstashConfiguration.table)
      if @kustoLogstashConfiguration.is_mapping_ref_provided
        @logger.debug('Using mapping reference.', json_mapping)
        ingestion_properties.setIngestionMapping(json_mapping, kusto_java.ingest.IngestionMapping::IngestionMappingKind::JSON)
        ingestion_properties.setDataFormat(kusto_java.ingest.IngestionProperties::DataFormat::JSON)
      else
        @logger.debug('No mapping reference provided. Columns will be mapped by names in the logstash output')
      end
      return ingestion_properties
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
      if file_size > 0
        file_source_info = Java::com.microsoft.azure.kusto.ingest.source.FileSourceInfo.new(path, 0); # 0 - let the sdk figure out the size of the file
        @kusto_client.ingestFromFile(file_source_info, @ingestion_properties)
      else
        @logger.warn("File #{path} is an empty file and is not ingested.")
      end
      File.delete(path) if delete_on_success
      @logger.debug("File #{path} sent to kusto.")
    rescue Errno::ENOENT => e
      @logger.error("File doesn't exist! Unrecoverable error.", exception: e.class, message: e.message, path: path, backtrace: e.backtrace)
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
