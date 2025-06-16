# encoding: utf-8
# A class just having all the configurations wrapped into a seperate object
module LogStash
	module Outputs
		module KustoInternal
			class KustoLogstashConfiguration
				FIELD_REF = /%\{[^}]+\}/
				def initialize(kusto_ingest,kusto_auth, kusto_proxy, kusto_flush_config , kusto_upload_config, logger)
					@logger = logger
					@kusto_ingest = kusto_ingest
					@kusto_auth = kusto_auth
					@kusto_proxy = kusto_proxy
					@kusto_flush_config = kusto_flush_config
					@kusto_upload_config = kusto_upload_config
					@logger.info("Kusto configuration initialized.")
				end # def initialize

				# Configuration
				def kusto_ingest
					@kusto_ingest
				end
				def kusto_auth
					@kusto_auth
				end
				def kusto_proxy
					@kusto_proxy
				end
				def kusto_flush_config
					@kusto_flush_config
				end
				def kusto_upload_config
					@kusto_upload_config
				end

				def validate_config()
					# Add an additional validation and fail this upfront
					if @kusto_auth.app_id.to_s.empty? && @kusto_auth.managed_identity_id.to_s.empty? && !@kusto_auth.cli_auth
						@logger.error('managed_identity_id is not provided, cli_auth is false and app_id/app_key is empty.')
						raise LogStash::ConfigurationError.new('managed_identity_id is not provided and app_id/app_key is empty.')
					end
					# If proxy AAD is required and the proxy configuration is not provided - fail
					if @kusto_proxy.proxy_aad_only && @kusto_proxy.is_direct_conn
						@logger.error('proxy_aad_only can be used only when proxy is configured.', @kusto_proxy.proxy_aad_only)
						raise LogStash::ConfigurationError.new('proxy_aad_only can be used only when proxy is configured.')
					end

					if @kusto_ingest.database =~ FIELD_REF
						@logger.error('database config value should not be dynamic.', @kusto_ingest.database)
						raise LogStash::ConfigurationError.new('database config value should not be dynamic.')
					end
					if @kusto_ingest.table =~ FIELD_REF
						@logger.error('table config value should not be dynamic.', @kusto_ingest.table)
						raise LogStash::ConfigurationError.new('table config value should not be dynamic.')
					end
					if @kusto_ingest.json_mapping =~ FIELD_REF
						@logger.error('json_mapping config value should not be dynamic.', @kusto_ingest.json_mapping)
						raise LogStash::ConfigurationError.new('json_mapping config value should not be dynamic.')
					end
					if not(["https", "http"].include? @kusto_proxy.proxy_protocol)
						@logger.error('proxy_protocol has to be http or https.', @kusto_proxy.proxy_protocol)
						raise LogStash::ConfigurationError.new('proxy_protocol has to be http or https.')
					end

					if @kusto_proxy.proxy_aad_only && @kusto_proxy.is_direct_conn
						@logger.error('proxy_aad_only is true, but proxy parameters (Host,Port,Protocol) are missing.',@kusto_proxy.proxy_host,@kusto_proxy.proxy_port,@kusto_proxy.proxy_protocol)
						raise LogStash::ConfigurationError.new('proxy_aad_only is true, but proxy parameters (Host,Port,Protocol) are missing.')
					end
					# If all validation pass then configuration is valid 
					return  true
				end #validate_config()

			end # class KustoLogstashConfiguration
			class KustoAuthConfiguration
				def initialize(app_id, app_key, app_tenant, managed_identity_id, cli_auth)
					@app_id = app_id
					@app_key = app_key
					@app_tenant = app_tenant
					@managed_identity_id = managed_identity_id
					@cli_auth = cli_auth
					@is_managed_identity = app_id.to_s.empty? && app_key.to_s.empty? && !cli_auth
					@is_system_assigned_managed_identity = is_managed_identity && 0 == "system".casecmp(kusto_auth.managed_identity_id)
				end
				# Authentication configuration
				def app_id
					@app_id
				end
				def app_key
					@app_key
				end
				def app_tenant
					@app_tenant
				end
				def managed_identity_id
					@managed_identity_id
				end
				def is_managed_identity
					@is_managed_identity
				end
				def cli_auth
					@cli_auth
				end
				def is_system_assigned_managed_identity
					@is_system_assigned_managed_identity
				end
			end # class KustoAuthConfiguration
			class KustoProxyConfiguration
				def initialize(proxy_host , proxy_port , proxy_protocol, proxy_aad_only)
					@proxy_host = proxy_host
					@proxy_port = proxy_port
					@proxy_protocol = proxy_protocol
					@proxy_aad_only = proxy_aad_only
					# Is it direct connection
					@is_direct_conn = (proxy_host.nil? || proxy_host.empty?)
				end
				# proxy configuration
				def proxy_host
					@proxy_host
				end

				def proxy_port
					@proxy_port
				end

				def proxy_protocol
					@proxy_protocol
				end

				def proxy_aad_only
					@proxy_aad_only
				end

				def is_direct_conn
					@is_direct_conn
				end
			end # class KustoProxyConfiguration
			class KustoIngestConfiguration
				def initialize(ingest_url, database, table, json_mapping)
					@ingest_url = ingest_url
					@database = database
					@table = table
					@json_mapping = json_mapping
					@is_mapping_ref_provided = !(json_mapping.nil? || json_mapping.empty?)
				end
				# For ingestion
				def ingest_url
					@ingest_url
				end
				def database
					@database
				end
				def table
					@table
				end
				def json_mapping
					@json_mapping
				end
				def is_mapping_ref_provided
					@is_mapping_ref_provided
				end                
			end # class KustoIngestionConfiguration
			class KustoFlushConfiguration
				def initialize(max_items, plugin_flush_interval, max_batch_size, process_failed_batches_on_startup)
					@max_items = max_items
					@plugin_flush_interval = plugin_flush_interval
					@max_batch_size = max_batch_size
					@flush_each = flush_each
					@process_failed_batches_on_startup = process_failed_batches_on_startup
				end
				# Flush configuration
				def max_items
					@max_items
				end
				def plugin_flush_interval
					@plugin_flush_interval
				end
				def max_batch_size
					@max_batch_size
				end
				def flush_each
					@flush_each
				end
				def process_failed_batches_on_startup
					@process_failed_batches_on_startup
				end
				
			end # class KustoFlushConfiguration
			class KustoUploadConfiguration
				def initialize(upload_concurrent_count, upload_queue_size)
					@upload_concurrent_count = upload_concurrent_count
					@upload_queue_size = upload_queue_size
				end
				# Upload configuration
				def upload_concurrent_count
					@upload_concurrent_count
				end
				def upload_queue_size
					@upload_queue_size
				end
			end # class KustoUploadConfiguration
		end # module KustoInternal
	end # module Outputs
end # module LogStash
