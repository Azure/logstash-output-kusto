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

    def initialize(ingest_url, app_id, app_key, app_tenant, managed_identity_id, cli_auth, database, table, json_mapping, proxy_host , proxy_port , proxy_protocol,logger, threadpool = DEFAULT_THREADPOOL)
      @workers_pool = threadpool
      @logger = logger
      validate_config(database, table, json_mapping,proxy_protocol,app_id, app_key, managed_identity_id,cli_auth)
      @logger.info('Preparing Kusto resources.')

      kusto_java = Java::com.microsoft.azure.kusto
      apache_http = Java::org.apache.http
      # kusto_connection_string = kusto_java.data.auth.ConnectionStringBuilder.createWithAadApplicationCredentials(ingest_url, app_id, app_key.value, app_tenant)
      # If there is managed identity, use it. This means the AppId and AppKey are empty/nil
      # If there is CLI Auth, use that instead of managed identity
      is_managed_identity = (app_id.nil? && app_key.nil? && !cli_auth)
      # If it is system managed identity, propagate the system identity
      is_system_assigned_managed_identity = is_managed_identity && 0 == "system".casecmp(managed_identity_id)
      # Is it direct connection
      is_direct_conn = (proxy_host.nil? || proxy_host.empty?)
      # Create a connection string
      kusto_connection_string = if is_managed_identity
          if is_system_assigned_managed_identity
            @logger.info('Using system managed identity.')
            kusto_java.data.auth.ConnectionStringBuilder.createWithAadManagedIdentity(ingest_url)  
          else
            @logger.info('Using user managed identity.')
            kusto_java.data.auth.ConnectionStringBuilder.createWithAadManagedIdentity(ingest_url, managed_identity_id)
          end
        else
          if cli_auth
            @logger.warn('*Use of CLI Auth is only for dev-test scenarios. This is ***NOT RECOMMENDED*** for production*')
            kusto_java.data.auth.ConnectionStringBuilder.createWithAzureCli(ingest_url)
          else 
            @logger.info('Using app id and app key.')
            kusto_java.data.auth.ConnectionStringBuilder.createWithAadApplicationCredentials(ingest_url, app_id, app_key.value, app_tenant)
          end
        end
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

      @ingestion_properties = kusto_java.ingest.IngestionProperties.new(database, table)
      is_mapping_ref_provided = !(json_mapping.nil? || json_mapping.empty?)
      if is_mapping_ref_provided
        @logger.debug('Using mapping reference.', json_mapping)
        @ingestion_properties.setIngestionMapping(json_mapping, kusto_java.ingest.IngestionMapping::IngestionMappingKind::JSON)
        @ingestion_properties.setDataFormat(kusto_java.ingest.IngestionProperties::DataFormat::JSON)
      else
        @logger.debug('No mapping reference provided. Columns will be mapped by names in the logstash output')
        @ingestion_properties.setDataFormat(kusto_java.ingest.IngestionProperties::DataFormat::JSON)
      end
      @logger.debug('Kusto resources are ready.')
    end

    def validate_config(database, table, json_mapping, proxy_protocol, app_id, app_key, managed_identity_id,cli_auth)
      # Add an additional validation and fail this upfront
      if app_id.nil? && app_key.nil? && managed_identity_id.nil?
        if cli_auth
          @logger.info('Using CLI Auth, this is only for dev-test scenarios. This is ***NOT RECOMMENDED*** for production')
        else
          @logger.error('managed_identity_id is not provided and app_id/app_key is empty.')
          raise LogStash::ConfigurationError.new('managed_identity_id is not provided and app_id/app_key is empty.')
        end
      end      
      if database =~ FIELD_REF
        @logger.error('database config value should not be dynamic.', database)
        raise LogStash::ConfigurationError.new('database config value should not be dynamic.')
      end

      if table =~ FIELD_REF
        @logger.error('table config value should not be dynamic.', table)
        raise LogStash::ConfigurationError.new('table config value should not be dynamic.')
      end

      if json_mapping =~ FIELD_REF
        @logger.error('json_mapping config value should not be dynamic.', json_mapping)
        raise LogStash::ConfigurationError.new('json_mapping config value should not be dynamic.')
      end

      if not(["https", "http"].include? proxy_protocol)
        @logger.error('proxy_protocol has to be http or https.', proxy_protocol)
        raise LogStash::ConfigurationError.new('proxy_protocol has to be http or https.')
      end

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
        ingestionLatch = java.util.concurrent.CountDownLatch.new(1)
    
        Thread.new do
          begin
            data_source_info = Java::com.microsoft.azure.kusto.ingest.source.StreamSourceInfo.new(java.io.ByteArrayInputStream.new(data.to_java_bytes))
            ingestion_result = @kusto_client.ingestFromStream(data_source_info, @ingestion_properties)
    
            # Check the ingestion status
            status = ingestion_result.getIngestionStatusCollection.get(0)
            if status.status != Java::com.microsoft.azure.kusto.ingest.result.OperationStatus::Queued
              raise "Failed upload: #{status.errorCodeString}"
            end
            @logger.info("Final ingestion status: #{status.status}")
          rescue => e
            @logger.error('Error during ingestFromStream.', exception: e.class, message: e.message, backtrace: e.backtrace)
            if e.message.include?("network")
              raise e 
            end
          ensure
            ingestionLatch.countDown()
          end
        end
    
        # Wait for the ingestion to complete with a timeout
        if !ingestionLatch.await(30, java.util.concurrent.TimeUnit::SECONDS)
          @logger.error('Ingestion timed out, possible network issue.')
          raise 'Ingestion timed out, possible network issue.'
        end
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