# encoding: utf - 8
require "logstash/kusto/kustoLogstashConfiguration"
require 'rest-client'
require 'json'
require 'openssl'
require 'base64'
require 'time'

module LogStash
    module Outputs
        module KustoInternal
            class KustoAadTokenProvider
                def initialize(kustoLogstashConfiguration)
                    @kustoLogstashConfiguration = kustoLogstashConfiguration
                    # Perform the auth initialization
                    scope = CGI.escape(sprintf("%s/.default",kustoLogstashConfiguration.ingest_url))
                    @logger = logger
                    @aad_uri = "https://login.microsoftonline.com"
                    @token_request_body = sprintf("client_id=%s&scope=%s&client_secret=%s&grant_type=client_credentials", kustoLogstashConfiguration.app_id, scope, kustoLogstashConfiguration.app_key)
                    @token_request_uri = sprintf("%s/%s/oauth2/v2.0/token", aad_uri, kustoLogstashConfiguration.app_tenant)
                    @token_state = {
                    : access_token => nil,
                    : expiry_time => nil,
                    : token_details_mutex => Mutex.new,
                    }
                end # def initialize

                # Public methods
                public

                def get_aad_token_bearer()
                    @token_state[: token_details_mutex].synchronize 
                    do
                        if is_saved_token_need_refresh()
                            refresh_saved_token()
                        end
                        return @token_state[: access_token]
                    end
                end # def get_aad_token_bearer

                # Private methods
                private

                def is_saved_token_need_refresh()
                    return @token_state[: access_token].nil ? || @token_state[: expiry_time].nil ? || @token_state[: expiry_time] <= Time.now
                end # def is_saved_token_need_refresh

                def refresh_saved_token()
                    @logger.info("aad token expired - refreshing token.")
                    token_response = post_token_request()
                    @token_state[: access_token] = token_response["access_token"]
                    @token_state[: expiry_time] = get_token_expiry_time(token_response["expires_in"])
                end # def refresh_saved_token

                def get_token_expiry_time(expires_in_seconds)
                    if (expires_in_seconds.nil ? || expires_in_seconds <= 0)
                        return Time.now + (60 * 60 * 24) # Refresh anyway in 24 hours
                    else
                        return Time.now + expires_in_seconds - 1;
                    # Decrease by 1 second to be on the safe side
                    end
                end # def get_token_expiry_time

            # Post the given json to Azure Loganalytics
                def post_token_request()
                # Create REST request header
                    headers = get_header()
                    while true
                        begin
                            proxy_aad = sprintf("%s://%s:%s", @kustoLogstashConfiguration.proxy_protocol, @kustoLogstashConfiguration.proxy_host, @kustoLogstashConfiguration.proxy_port)
                            # Post REST request
                            response = RestClient::Request.execute(method:: post, url: @token_request_uri, payload: @token_request_body, headers: headers,
                            proxy: proxy_aad)

                            if (response.code == 200 || response.code == 201)
                                return JSON.parse(response.body)
                            end
                            rescue RestClient::ExceptionWithResponse => ewr
                            @logger.error("Exception while authenticating with AAD API ['#{ewr.response}']")
                            rescue Exception => ex
                                @logger.trace("Exception while authenticating with AAD API ['#{ex}']")
                        end
                            @logger.error("Error while authenticating with AAD ('#{@aad_uri}'), retrying in 10 seconds.")
                            sleep 10
                    end
                end # def post_token_request
                # Create a header
                def get_header()
                    return {
                        'Content-Type' => 'application/x-www-form-urlencoded',
                    }
                end # def get_header
            end # class KustoAadTokenProvider
        end # module Kusto
    end # module Outputs
end # module LogStash
        
