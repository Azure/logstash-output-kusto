# A class just having all the configurations wrapped into a seperate object
module LogStash
  module Outputs
    module KustoInternal
      class KustoLogstashConfiguration
        FIELD_REF = /%\{[^}]+\}/.freeze
        def initialize(kusto_ingest, kusto_auth, kusto_proxy, kusto_flush_config, kusto_upload_config, logger,
                       file_persistence)
          @logger = logger
          @kusto_ingest = kusto_ingest
          @kusto_auth = kusto_auth
          @kusto_proxy = kusto_proxy
          @kusto_flush_config = kusto_flush_config
          @kusto_upload_config = kusto_upload_config
          @file_persistence = file_persistence
          @logger.info('Kusto configuration initialized.')
        end

        # Configuration
        attr_reader :kusto_ingest

        attr_reader :kusto_auth, :kusto_proxy, :kusto_flush_config, :kusto_upload_config, :file_persistence

        def validate_config
          # Add an additional validation and fail this upfront
          if @kusto_auth.app_id.to_s.empty? && @kusto_auth.managed_identity_id.to_s.empty? && !@kusto_auth.cli_auth
            @logger.error('managed_identity_id is not provided, cli_auth is false and app_id/app_key is empty.')
            raise LogStash::ConfigurationError, 'managed_identity_id is not provided and app_id/app_key is empty.'
          end
          # When using app_id auth, app_key and app_tenant must also be provided
          unless @kusto_auth.app_id.to_s.empty?
            raise LogStash::ConfigurationError, 'app_key is required when app_id is provided.' if @kusto_auth.app_key.nil? || @kusto_auth.app_key.value.to_s.empty?
            raise LogStash::ConfigurationError, 'app_tenant is required when app_id is provided.' if @kusto_auth.app_tenant.to_s.empty?
          end
          # If proxy AAD is required and the proxy configuration is not provided - fail
          if @kusto_proxy.proxy_aad_only && @kusto_proxy.is_direct_conn
            @logger.error('proxy_aad_only can be used only when proxy is configured.', @kusto_proxy.proxy_aad_only)
            raise LogStash::ConfigurationError, 'proxy_aad_only can be used only when proxy is configured.'
          end

          if @kusto_ingest.database =~ FIELD_REF
            @logger.error('database config value should not be dynamic.', @kusto_ingest.database)
            raise LogStash::ConfigurationError, 'database config value should not be dynamic.'
          end
          if @kusto_ingest.table =~ FIELD_REF
            @logger.error('table config value should not be dynamic.', @kusto_ingest.table)
            raise LogStash::ConfigurationError, 'table config value should not be dynamic.'
          end
          if @kusto_ingest.json_mapping =~ FIELD_REF
            @logger.error('json_mapping config value should not be dynamic.', @kusto_ingest.json_mapping)
            raise LogStash::ConfigurationError, 'json_mapping config value should not be dynamic.'
          end
          unless %w[https http].include? @kusto_proxy.proxy_protocol
            @logger.error('proxy_protocol has to be http or https.', @kusto_proxy.proxy_protocol)
            raise LogStash::ConfigurationError, 'proxy_protocol has to be http or https.'
          end

          # If all validation pass then configuration is valid
          true
        end
      end

      class KustoAuthConfiguration
        def initialize(app_id, app_key, app_tenant, managed_identity_id, cli_auth)
          @app_id = app_id
          @app_key = app_key
          @app_tenant = app_tenant
          @managed_identity_id = managed_identity_id
          @cli_auth = cli_auth
          @is_managed_identity = app_id.to_s.empty? && app_key.to_s.empty? && !cli_auth
          @is_system_assigned_managed_identity = @is_managed_identity && managed_identity_id.to_s.strip.length.positive? && 'system'.casecmp(managed_identity_id.to_s).zero?
        end

        # Authentication configuration
        attr_reader :app_id

        attr_reader :app_key, :app_tenant, :managed_identity_id, :is_managed_identity, :cli_auth,
                    :is_system_assigned_managed_identity
      end

      class KustoProxyConfiguration
        def initialize(proxy_host, proxy_port, proxy_protocol, proxy_aad_only)
          @proxy_host = proxy_host
          @proxy_port = proxy_port
          @proxy_protocol = proxy_protocol
          @proxy_aad_only = proxy_aad_only
          # Is it direct connection
          @is_direct_conn = (proxy_host.nil? || proxy_host.empty?)
        end

        # proxy configuration
        attr_reader :proxy_host

        attr_reader :proxy_port, :proxy_protocol, :proxy_aad_only, :is_direct_conn
      end

      class KustoIngestConfiguration
        def initialize(ingest_url, database, table, json_mapping)
          @ingest_url = ingest_url
          @database = database
          @table = table
          @json_mapping = json_mapping
          @is_mapping_ref_provided = !(json_mapping.nil? || json_mapping.empty?)
        end

        # For ingestion
        attr_reader :ingest_url

        attr_reader :database, :table, :json_mapping, :is_mapping_ref_provided
      end

      class KustoFlushConfiguration
        def initialize(max_items, plugin_flush_interval, max_batch_size, process_failed_batches_on_startup)
          @max_items = max_items
          @plugin_flush_interval = plugin_flush_interval
          @max_batch_size = max_batch_size
          @process_failed_batches_on_startup = process_failed_batches_on_startup
        end

        # Flush configuration
        attr_reader :max_items

        attr_reader :plugin_flush_interval, :max_batch_size, :process_failed_batches_on_startup
      end

      class KustoUploadConfiguration
        def initialize(upload_concurrent_count, upload_queue_size)
          @upload_concurrent_count = upload_concurrent_count
          @upload_queue_size = upload_queue_size
        end

        # Upload configuration
        attr_reader :upload_concurrent_count

        attr_reader :upload_queue_size
      end
    end
  end
end
