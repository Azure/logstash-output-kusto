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

    def initialize(ingest_url, app_id, app_key, app_tenant, managed_identity_id, cli_auth, database, table, json_mapping, delete_local, proxy_host , proxy_port , proxy_protocol,logger, threadpool = DEFAULT_THREADPOOL)
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
      #
      @logger.debug(Gem.loaded_specs.to_s)
      # Unfortunately there's no way to avoid using the gem/plugin name directly...
      name_for_tracing = "logstash-output-kusto:#{Gem.loaded_specs['logstash-output-kusto']&.version || "unknown"}"
      @logger.debug("Client name for tracing: #{name_for_tracing}")

      java_util = Java::java.util
      # kusto_connection_string.setClientVersionForTracing(name_for_tracing)
      version_for_tracing=Gem.loaded_specs['logstash-output-kusto']&.version || "unknown"
      kusto_connection_string.setConnectorDetails("Logstash",version_for_tracing.to_s,"","",false,"", java_util.Collections.emptyMap());
      @kusto_client = begin
        if is_direct_conn
          kusto_java.ingest.IngestClientFactory.createClient(kusto_connection_string)
        else
          http_kusto = Java::com.microsoft.azure.kusto.data.http
          java_net = Java::java.net
          proxy_inet_server = java_net.InetSocketAddress.new(proxy_host, proxy_port)
          proxy = Java::com.azure.core.http.ProxyOptions.new(Java::com.azure.core.http.ProxyOptions::Type::HTTP, proxy_inet_server)
          http_client_properties = http_kusto.HttpClientProperties.builder().proxy(proxy).build()
          kusto_java.ingest.IngestClientFactory.createClient(kusto_connection_string, http_client_properties)
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
      @delete_local = delete_local
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
      #   # this is the number we expect - database, table, json_mapping
      #   database = file_metadata_parts[0]
      #   table = file_metadata_parts[1]
      #   json_mapping = file_metadata_parts[2]

      #   local_ingestion_properties = Java::KustoIngestionProperties.new(database, table)
      #   local_ingestion_properties.addJsonMappingName(json_mapping)
      # end

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
