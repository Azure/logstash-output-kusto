# encoding: utf-8

require 'logstash/outputs/base'
require 'logstash/namespace'
require 'logstash/errors'
require 'concurrent'
require 'json'
require 'logstash/outputs/kusto/filePersistence'

module LogStash; module Outputs; class KustoOutputInternal
  ##
  # This handles the overall logic and communication with Kusto
  #
  class Ingestor
    require 'logstash-output-kusto_jars'
    RETRY_DELAY_SECONDS = 3

    FIELD_REF = /%\{[^}]+\}/

    def initialize(kusto_logstash_configuration, logger)
      @kusto_logstash_configuration = kusto_logstash_configuration
      @logger = logger
      @workers_pool = Concurrent::ThreadPoolExecutor.new(min_threads: 1,
            max_threads: kusto_logstash_configuration.kusto_upload_config.upload_concurrent_count,
            max_queue: kusto_logstash_configuration.kusto_upload_config.upload_queue_size,
            fallback_policy: :caller_runs
      ) 
      #Validate and assign
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
        exceptions = Concurrent::Array.new
        promise = Concurrent::Promises.future {
          in_bytes = java.io.ByteArrayInputStream.new(data.to_json.to_java_bytes)
          data_source_info = Java::com.microsoft.azure.kusto.ingest.source.StreamSourceInfo.new(in_bytes)
          ingest_result = @kusto_client.ingestFromStream(data_source_info, @ingestion_properties)
          #@logger.info("Ingestion result: #{ingest_result}")
        }
        .rescue{ |e|
          @logger.error("Ingestion failed: #{e.message}")
          @logger.error("Ingestion failed: #{e.backtrace.join("\n")}")
          LogStash::Outputs::KustoOutputInternal::FilePersistence.persist_batch(data)
          raise e
        }
        .on_resolution do |fulfilled, value, reason, *args|
          @logger.info("******************************************************************************************")
          @logger.info("Future fulfilled: #{fulfilled}, value: #{value}, reason: #{reason}, args: #{args}, class: #{value.class}")
          #@logger.info("Ingestion status: #{value.getIngestionStatusCollection().getStatus}")

          if value.class == Java::ComMicrosoftAzureKustoIngestResult::IngestionStatusResult
            isc = value.getIngestionStatusCollection()&.get(0)&.getStatus()
            @logger.info("Ingestion status: #{isc}")
          else
            @logger.info("Ingestion status is non success status: #{value.class} - #{value}")
          end
          if exceptions.size > 0
            @logger.error("Ingestion failed with exceptions: #{exceptions.map(&:message).join(', ')}")
          end
          @logger.info("******************************************************************************************")
        end
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
