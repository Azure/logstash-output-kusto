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

    def initialize(kusto_logstash_configuration, logger, latch_timeout = 60, threadpool = DEFAULT_THREADPOOL)
      @workers_pool = threadpool
      @latch_timeout = latch_timeout
      @logger = logger
      #Validate and assign
      kusto_logstash_configuration.validate_config()
      @kusto_logstash_configuration = kusto_logstash_configuration
      @logger.info('Preparing Kusto resources.')

      kusto_java = Java::com.microsoft.azure.kusto
      apache_http = Java::org.apache.http

      is_managed_identity = @kusto_logstash_configuration.kusto_auth.is_managed_identity
      # If it is system managed identity, propagate the system identity
      is_system_assigned_managed_identity = @kusto_logstash_configuration.kusto_auth.is_system_assigned_managed_identity
      # Is it direct connection
      is_direct_conn = @kusto_logstash_configuration.kusto_proxy.is_direct_conn
      # Create a connection string
      kusto_connection_string = if is_managed_identity
          if is_system_assigned_managed_identity
            @logger.info('Using system managed identity.')
            kusto_java.data.auth.ConnectionStringBuilder.createWithAadManagedIdentity(@kusto_logstash_configuration.kusto_ingest.ingest_url)  
          else
            @logger.info('Using user managed identity.')
            kusto_java.data.auth.ConnectionStringBuilder.createWithAadManagedIdentity(@kusto_logstash_configuration.kusto_ingest.ingest_url, @kusto_logstash_configuration.kusto_ingest.managed_identity_id)
          end
        else
          if @kusto_logstash_configuration.kusto_auth.cli_auth
            @logger.warn('*Use of CLI Auth is only for dev-test scenarios. This is ***NOT RECOMMENDED*** for production*')
            kusto_java.data.auth.ConnectionStringBuilder.createWithAzureCli(@kusto_logstash_configuration.kusto_ingest.ingest_url)
          else 
            @logger.info('Using app id and app key.')
            kusto_java.data.auth.ConnectionStringBuilder.createWithAadApplicationCredentials(@kusto_logstash_configuration.kusto_ingest.ingest_url, @kusto_logstash_configuration.kusto_auth.app_id, @kusto_logstash_configuration.kusto_auth.app_key.value, @kusto_logstash_configuration.kusto_auth.app_tenant)
          end
        end
      @logger.debug(Gem.loaded_specs.to_s)
      # Unfortunately there's no way to avoid using the gem/plugin name directly...
      name_for_tracing = "logstash-output-kusto:#{Gem.loaded_specs['logstash-output-kusto']&.version || "unknown"}"
      @logger.debug("Client name for tracing: #{name_for_tracing}")

      tuple_utils = Java::org.apache.commons.lang3.tuple
      # kusto_connection_string.setClientVersionForTracing(name_for_tracing)
      version_for_tracing=Gem.loaded_specs['logstash-output-kusto']&.version || "unknown"
      kusto_connection_string.setConnectorDetails("Logstash",version_for_tracing.to_s,name_for_tracing.to_s,version_for_tracing.to_s,false,"", tuple_utils.Pair.emptyArray());
      
      @kusto_client = begin
        if is_direct_conn
          kusto_java.ingest.IngestClientFactory.createClient(kusto_connection_string)
        else
          kusto_http_client_properties = kusto_java.data.HttpClientProperties.builder().proxy(apache_http.HttpHost.new(@kusto_logstash_configuration.kusto_proxy.proxy_host,@kusto_logstash_configuration.kusto_proxy.proxy_port,@kusto_logstash_configuration.kusto_proxy.proxy_protocol)).build()
          kusto_java.ingest.IngestClientFactory.createClient(kusto_connection_string, kusto_http_client_properties)
        end
      end

      @ingestion_properties = kusto_java.ingest.IngestionProperties.new(@kusto_logstash_configuration.kusto_ingest.database, @kusto_logstash_configuration.kusto_ingest.table)
      if @kusto_logstash_configuration.kusto_ingest.is_mapping_ref_provided
        @logger.debug('Using mapping reference.', @kusto_logstash_configuration.kusto_ingest.json_mapping)
        @ingestion_properties.setIngestionMapping(@kusto_logstash_configuration.kusto_ingest.json_mapping, kusto_java.ingest.IngestionMapping::IngestionMappingKind::JSON)
        @ingestion_properties.setDataFormat(kusto_java.ingest.IngestionProperties::DataFormat::JSON)
      else
        @logger.debug('No mapping reference provided. Columns will be mapped by names in the logstash output')
        @ingestion_properties.setDataFormat(kusto_java.ingest.IngestionProperties::DataFormat::JSON)
      end
      @logger.debug('Kusto resources are ready.')
    end

    def upload_async(data)
      if @workers_pool.remaining_capacity <= LOW_QUEUE_LENGTH
        @logger.warn("Ingestor queue capacity is running low with #{@workers_pool.remaining_capacity} free slots.")
      end
      exception = nil
      @workers_pool.post do
        LogStash::Util.set_thread_name("Kusto to ingest data")
        begin
          upload(data)
        rescue => e
          @logger.error('Error during async upload.', exception: e.class, message: e.message, backtrace: e.backtrace)
          exception = e
        end
      end
      # Wait for the task to complete and check for exceptions
      @workers_pool.shutdown
      @workers_pool.wait_for_termination
    
      if exception
        @logger.error('StandardError in upload_async.', exception: exception.class, message: exception.message, backtrace: exception.backtrace)
        raise exception
      end
    rescue Exception => e
      @logger.error('StandardError in upload_async.', exception: e.class, message: e.message, backtrace: e.backtrace)
      raise e
    end

    def upload(data)
      @logger.info("Sending data to Kusto")
    
      if data.size > 0
        thread_exception = nil
    
        future = java.util.concurrent.CompletableFuture.supplyAsync do
          begin
            data_source_info = Java::com.microsoft.azure.kusto.ingest.source.StreamSourceInfo.new(java.io.ByteArrayInputStream.new(data.to_java_bytes))
            @kusto_client.ingestFromStream(data_source_info, @ingestion_properties)
          rescue => e
            @logger.error('Error during ingestFromStream.', exception: e.class, message: e.message, backtrace: e.backtrace)
            thread_exception = e
          end
        end
    
        begin
          future.get(@latch_timeout, java.util.concurrent.TimeUnit::SECONDS)
        rescue java.util.concurrent.TimeoutException => e
          @logger.error('Ingestion timed out, possible network issue.')
          thread_exception = 'Ingestion timed out, possible network issue.'
        rescue java.util.concurrent.ExecutionException => e
          thread_exception = e.cause
        end
    
        # Raise the exception from the thread if it occurred
        raise thread_exception if thread_exception
      else
        @logger.warn("Data is empty and is not ingested.")
      end
      @logger.info("Data sent to Kusto.")
    rescue => e
      @logger.error('Uploading failed.', exception: e.class, message: e.message, backtrace: e.backtrace)
      raise e # Raise the original error if ingestion fails
    end

    def stop
      @workers_pool.shutdown
      @workers_pool.wait_for_termination(nil) # block until its done
    end
  end
end