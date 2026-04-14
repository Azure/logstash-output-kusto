require 'logstash/outputs/base'
require 'logstash/namespace'
require 'logstash/errors'
require 'concurrent'
require 'json'

module LogStash
  module Outputs
    class KustoOutputInternal
      ##
      # Core Kusto transport layer. Handles both ingestion modes:
      #   - `upload(data)` — streaming ingestion for buffered mode (ingestFromStream)
      #   - `upload_file(path)` — queued ingestion for file mode (ingestFromFile)
      #
      # Both paths use a shared ThreadPoolExecutor for parallel uploads with
      # configurable concurrency (`upload_concurrent_count`) and queue depth
      # (`upload_queue_size`). Backpressure uses caller_runs policy.
      #
      # IMPORTANT: All bare `File`/`Dir` references MUST use `::File`/`::Dir`
      # because this code lives under `LogStash::Outputs::` where `File` resolves
      # to `LogStash::Outputs::File` (the logstash file output plugin class).
      #
      class Ingestor
        require 'logstash-output-kusto_jars'
        RETRY_DELAY_SECONDS = 3
        MAX_RETRIES = 3
        LOW_QUEUE_LENGTH = 3

        FIELD_REF = /%\{[^}]+\}/.freeze

        def initialize(kusto_logstash_configuration, logger)
          @kusto_logstash_configuration = kusto_logstash_configuration
          @logger = logger
          @file_persistence = kusto_logstash_configuration.file_persistence
          @in_flight = []
          @in_flight_mutex = Mutex.new
          @workers_pool = Concurrent::ThreadPoolExecutor.new(min_threads: 1,
                                                             max_threads: kusto_logstash_configuration.kusto_upload_config.upload_concurrent_count,
                                                             max_queue: kusto_logstash_configuration.kusto_upload_config.upload_queue_size,
                                                             fallback_policy: :caller_runs)
          # Validate and assign
          @logger.info('Preparing Kusto resources.')

          kusto_java = Java::com.microsoft.azure.kusto

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
                                        kusto_java.data.auth.ConnectionStringBuilder.createWithAadManagedIdentity(
                                          @kusto_logstash_configuration.kusto_ingest.ingest_url, @kusto_logstash_configuration.kusto_auth.managed_identity_id
                                        )
                                      end
                                    elsif @kusto_logstash_configuration.kusto_auth.cli_auth
                                      @logger.warn('*Use of CLI Auth is only for dev-test scenarios. This is ***NOT RECOMMENDED*** for production*')
                                      kusto_java.data.auth.ConnectionStringBuilder.createWithAzureCli(@kusto_logstash_configuration.kusto_ingest.ingest_url)
                                    else
                                      @logger.info('Using app id and app key.')
                                      kusto_java.data.auth.ConnectionStringBuilder.createWithAadApplicationCredentials(
                                        @kusto_logstash_configuration.kusto_ingest.ingest_url, @kusto_logstash_configuration.kusto_auth.app_id, @kusto_logstash_configuration.kusto_auth.app_key.value, @kusto_logstash_configuration.kusto_auth.app_tenant
                                      )
                                    end
          @logger.debug(Gem.loaded_specs.to_s)
          # Unfortunately there's no way to avoid using the gem/plugin name directly...
          name_for_tracing = "logstash-output-kusto:#{Gem.loaded_specs['logstash-output-kusto']&.version || 'unknown'}"
          @logger.debug("Client name for tracing: #{name_for_tracing}")

          java_util = Java::java.util
          # kusto_connection_string.setClientVersionForTracing(name_for_tracing)
          version_for_tracing = Gem.loaded_specs['logstash-output-kusto']&.version || 'unknown'
          kusto_connection_string.setConnectorDetails('Logstash', version_for_tracing.to_s, '', '', false, '',
                                                      java_util.Collections.emptyMap)

          @kusto_client = if is_direct_conn
                            kusto_java.ingest.IngestClientFactory.createClient(kusto_connection_string)
                          else
                            http_kusto = Java::com.microsoft.azure.kusto.data.http
                            java_net = Java::java.net
                            proxy_inet_server = java_net.InetSocketAddress.new(@kusto_logstash_configuration.kusto_proxy.proxy_host,
                                                                               @kusto_logstash_configuration.kusto_proxy.proxy_port)
                            proxy = Java::com.azure.core.http.ProxyOptions.new(Java::com.azure.core.http.ProxyOptions::Type::HTTP,
                                                                               proxy_inet_server)
                            http_client_properties = http_kusto.HttpClientProperties.builder.proxy(proxy).build
                            kusto_java.ingest.IngestClientFactory.createClient(kusto_connection_string,
                                                                               http_client_properties)
                          end

          @ingestion_properties = kusto_java.ingest.IngestionProperties.new(
            @kusto_logstash_configuration.kusto_ingest.database, @kusto_logstash_configuration.kusto_ingest.table
          )
          if @kusto_logstash_configuration.kusto_ingest.is_mapping_ref_provided
            @logger.debug('Using mapping reference.',
                          @kusto_logstash_configuration.kusto_ingest.json_mapping)
            @ingestion_properties.setIngestionMapping(@kusto_logstash_configuration.kusto_ingest.json_mapping,
                                                      kusto_java.ingest.IngestionMapping::IngestionMappingKind::JSON)
          else
            @logger.debug('No mapping reference provided. Columns will be mapped by names in the logstash output')
          end
          @ingestion_properties.setDataFormat(kusto_java.ingest.IngestionProperties::DataFormat::MULTIJSON)
          @logger.debug('Kusto resources are ready.')
        end

        def upload(data)
          data_size = data.size
          @logger.info("Ingesting #{data_size} rows to database: #{@ingestion_properties.getDatabaseName} table: #{@ingestion_properties.getTableName}")
          if data_size.positive?
            # Serialize as newline-delimited JSON (one object per line) to match
            # the MULTIJSON data format expected by Kusto ingestion.
            json_bytes = data.map(&:to_json).join("\n").to_java_bytes

            promise = Concurrent::Promises.future_on(@workers_pool) do
              ingest_with_retry(data, json_bytes)
            end
                                          .on_resolution do |fulfilled, value, reason|
              if fulfilled
                if value.respond_to?(:getIngestionStatusCollection)
                  isc = value.getIngestionStatusCollection&.get(0)&.getStatus()
                  @logger.info("Ingestion status: #{isc}")
                end
              else
                @logger.warn("Ingestion future rejected: #{reason}")
              end
            end

            @in_flight_mutex.synchronize do
              @in_flight.reject!(&:resolved?)
              @in_flight << promise
            end
          else
            @logger.warn('Data is empty and is not ingested.')
          end
        end

        def upload_file_async(path, delete_on_success)
          @logger.warn("Ingestor queue capacity is running low with #{@workers_pool.remaining_capacity} free slots.") if @workers_pool.remaining_capacity <= LOW_QUEUE_LENGTH

          @workers_pool.post do
            upload_file(path, delete_on_success)
          rescue StandardError => e
            @logger.error('Unhandled error in file upload worker.',
                          exception: e.class, message: e.message, path: path,
                          backtrace: e.backtrace)
          end
        rescue StandardError => e
          @logger.error('Error submitting file upload.', exception: e.class, message: e.message, path: path,
                                                         backtrace: e.backtrace)
          raise e
        end

        def upload_file(path, delete_on_success)
          file_size = ::File.size(path)
          @logger.debug("Sending file to kusto: #{path}. size: #{file_size}")

          unless file_size.positive?
            @logger.warn("File #{path} is an empty file and is not ingested.")
            return
          end

          retries = 0
          max_retries = 10
          begin
            file_source_info = Java::com.microsoft.azure.kusto.ingest.source.FileSourceInfo.new(path)
            @kusto_client.ingestFromFile(file_source_info, @ingestion_properties)
          rescue Errno::ENOENT, Errno::EACCES
            raise # unrecoverable file errors, propagate to outer rescue
          rescue StandardError => e
            retries += 1
            if retries <= max_retries
              delay = RETRY_DELAY_SECONDS * retries
              @logger.error("Uploading failed, retrying (#{retries}/#{max_retries}) in #{delay}s.",
                            exception: e.class, message: e.message, path: path)
              sleep delay
              retry
            else
              @logger.error("Uploading failed after #{max_retries} retries, giving up.",
                            exception: e.class, message: e.message, path: path, backtrace: e.backtrace)
              return
            end
          end

          ::File.delete(path) if delete_on_success
          @logger.debug("File #{path} sent to kusto.")
        rescue Errno::ENOENT => e
          @logger.error("File doesn't exist! Unrecoverable error.", exception: e.class, message: e.message, path: path,
                                                                    backtrace: e.backtrace)
        rescue Java::JavaNioFile::NoSuchFileException => e
          @logger.error("File doesn't exist! Unrecoverable error.", exception: e.class, message: e.message, path: path,
                                                                    backtrace: e.backtrace)
        end

        def stop
          # Wait for all in-flight uploads to complete before shutting down the pool
          promises = @in_flight_mutex.synchronize { @in_flight.dup }
          unless promises.empty?
            @logger.info("Waiting for #{promises.size} in-flight uploads to complete...")
            begin
              Concurrent::Promises.zip(*promises).wait(60)
            rescue StandardError => e
              @logger.warn("Error waiting for in-flight uploads: #{e.message}")
            end
          end
          @workers_pool.shutdown
          # Bounded wait to avoid blocking shutdown indefinitely if a worker is stuck
          return if @workers_pool.wait_for_termination(120)

          @logger.warn('Worker pool did not terminate within 120s, forcing shutdown.')
          @workers_pool.kill
        end

        private

        def ingest_with_retry(data, json_bytes)
          retries = 0
          begin
            in_bytes = java.io.ByteArrayInputStream.new(json_bytes)
            data_source_info = Java::com.microsoft.azure.kusto.ingest.source.StreamSourceInfo.new(in_bytes)
            @kusto_client.ingestFromStream(data_source_info, @ingestion_properties)
          rescue StandardError => e
            retries += 1
            if retries <= MAX_RETRIES
              delay = RETRY_DELAY_SECONDS * (2**(retries - 1))
              @logger.warn("Ingestion attempt #{retries}/#{MAX_RETRIES} failed: #{e.message}. Retrying in #{delay}s...")
              sleep(delay)
              retry
            else
              @logger.error("Ingestion failed after #{MAX_RETRIES} retries: #{e.message}")
              @logger.error(e.backtrace.join("\n"))
              @file_persistence.persist_batch(data)
              raise
            end
          end
        end
      end
    end; end; end # module LogStash::Outputs::KustoOutputInternal
