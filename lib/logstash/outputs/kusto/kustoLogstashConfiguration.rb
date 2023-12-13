# encoding: utf-8
# A class just having all the configurations wrapped into a seperate object

module LogStash
    module Outputs
        module KustoInternal
            class KustoLogstashConfiguration
                FIELD_REF = /%\{[^}]+\}/
                def initialize(ingest_url, app_id, app_key, app_tenant, managed_identity_id, database, table, json_mapping, delete_local, proxy_host , proxy_port , proxy_protocol, proxy_aad_only, logger)
                    @logger = logger
                    # For ingestion
                    @ingest_url = ingest_url
                    @database = database
                    @table = table
                    @json_mapping = json_mapping
                    @is_mapping_ref_provided = !(json_mapping.nil? || json_mapping.empty?)
                    # Authentication configuration
                    @app_id = app_id
                    @app_key = app_key
                    @app_tenant = app_tenant
                    @managed_identity_id = managed_identity_id
                    # proxy configuration
                    @proxy_host = proxy_host
                    @proxy_port = proxy_port
                    @proxy_protocol = proxy_protocol
                    @proxy_aad_only = proxy_aad_only
                    # Fields that are derived from the configuration
                    @is_managed_identity = app_id.to_s.empty? && app_key.to_s.empty?
                    # If it is system managed identity, propagate the system identity
                    @is_system_assigned_managed_identity = is_managed_identity && 0 == "system".casecmp(managed_identity_id)
                    # Is it direct connection
                    @is_direct_conn = (proxy_host.nil? || proxy_host.empty?)
                    @logger.info("Kusto configuration initialized.")
                end # def initialize

                def validate_config()
                    # Add an additional validation and fail this upfront
                    if @app_id.to_s.empty? && @app_key.to_s.empty? && @managed_identity_id.to_s.empty?
                        @logger.error('managed_identity_id is not provided and app_id/app_key is empty.')
                        raise LogStash::ConfigurationError.new('managed_identity_id is not provided and app_id/app_key is empty.')
                    end
                    # If proxy AAD is required and the proxy configuration is not provided - fail
                    if @proxy_aad_only && @is_direct_conn
                        @logger.error('proxy_aad_only can be used only when proxy is configured.', proxy_aad_only)
                        raise LogStash::ConfigurationError.new('proxy_aad_only can be used only when proxy is configured.')
                    end

                    if @database =~ FIELD_REF
                    @logger.error('database config value should not be dynamic.', database)
                    raise LogStash::ConfigurationError.new('database config value should not be dynamic.')
                    end
            
                    if @table =~ FIELD_REF
                    @logger.error('table config value should not be dynamic.', table)
                    raise LogStash::ConfigurationError.new('table config value should not be dynamic.')
                    end
            
                    if @json_mapping =~ FIELD_REF
                    @logger.error('json_mapping config value should not be dynamic.', json_mapping)
                    raise LogStash::ConfigurationError.new('json_mapping config value should not be dynamic.')
                    end
            
                    if not(["https", "http"].include? @proxy_protocol)
                    @logger.error('proxy_protocol has to be http or https.', proxy_protocol)
                    raise LogStash::ConfigurationError.new('proxy_protocol has to be http or https.')
                    end
                    # If all validation pass then configuration is valid 
                    return  true
                end #validate_config()

                # Getters for all the attributes defined in this class
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

                def is_system_assigned_managed_identity
                    @is_system_assigned_managed_identity
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
            end # class KustoLogstashConfiguration
        end # module Kusto
    end # module Outputs
end # module LogStash