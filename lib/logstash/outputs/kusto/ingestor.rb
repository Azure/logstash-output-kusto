# encoding: utf-8

require 'logstash/outputs/base'
require 'logstash/namespace'
require 'logstash/errors'
require 'concurrent'

module LogStash; module Outputs; class KustoOutputInternal
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

    def initialize(kusto_logstash_configuration, logger, workers_pool)
      @workers_pool = workers_pool
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
        @ingestion_properties.setDataFormat(kusto_java.ingest.IngestionProperties::DataFormat::MULTIJSON)
      else
        @logger.debug('No mapping reference provided. Columns will be mapped by names in the logstash output')
        @ingestion_properties.setDataFormat(kusto_java.ingest.IngestionProperties::DataFormat::MULTIJSON)
      end
      @logger.debug('Kusto resources are ready.')
    end

    def upload(data)
      data_size = data.size
      @logger.info("Ingesting #{data_size} rows to database: #{@ingestion_properties.getDatabaseName} table: #{@ingestion_properties.getTableName}")
      if data_size > 0
        #ingestion_status_futures = Concurrent::Future.execute(executor: @workers_pool) do
        @workers_pool.post {
            begin
              in_bytes = java.io.ByteArrayInputStream.new(data.to_s.to_java_bytes)
              data_source_info = Java::com.microsoft.azure.kusto.ingest.source.StreamSourceInfo.new(in_bytes)
              ingest_result = @kusto_client.ingestFromStream(data_source_info, @ingestion_properties)
              @logger.trace("Ingestion result: #{ingest_result}")
            rescue Exception => e
              @logger.error("General failed: #{e}")
              raise e
            ensure
              in_bytes.close  
            end
         #end # ingestion_status_futures
        }
      else
        @logger.warn("Data is empty and is not ingested.")
      end # if data.size > 0
    end # def upload

    def stop
      @workers_pool.shutdown
      @workers_pool.wait_for_termination(nil) # block until its done
    end # def stop
  end # class Ingestor
end; end; end # module LogStash::Outputs::KustoOutputInternal
