require 'logstash/outputs/base'
require 'logstash/namespace'
require 'logstash/errors'

require 'logstash/outputs/kusto/customSizeBasedBuffer'
require 'logstash/outputs/kusto/kustoLogstashConfiguration'
require 'logstash/outputs/kusto/logStashFlushBuffer'
require 'logstash/outputs/kusto/filePersistence'
require 'logstash/outputs/kusto/file_mode_handler'

##
# Logstash output plugin for Azure Data Explorer (Kusto).
#
# Supports two ingestion modes:
#   1. **Buffered mode** (default) — events are batched in memory and streamed
#      to Kusto via `ingestFromStream`. Flush is triggered by size, time, or
#      item count thresholds. Failed batches are persisted to disk for recovery.
#   2. **File mode** — events are written to time-rotated temp files on disk,
#      then uploaded to Kusto via `ingestFromFile` (queued ingestion). Activated
#      only when `path` is explicitly set, preserving backward compatibility.
#
# Mode selection: see `use_buffered_mode?` at the bottom of this class.
#
module LogStash
  module Outputs
    class Kusto < LogStash::Outputs::Base
      config_name 'kusto'
      concurrency :shared

      FIELD_REF = /%\{[^}]+\}/.freeze

      attr_reader :failure_path

      # The Kusto endpoint for ingestion related communication. You can see it on the Azure Portal.
      config :ingest_url, validate: :string, required: true
      # The following are the credentials used to connect to the Kusto service
      # application id
      config :app_id, validate: :string, required: false
      # application key (secret)
      config :app_key, validate: :password, required: false
      # aad tenant id
      config :app_tenant, validate: :string, default: nil
      # managed identity id
      config :managed_identity, validate: :string, default: nil
      # CLI credentials for dev-test
      config :cli_auth, validate: :boolean, default: false
      # The following are the data settings that impact where events are written to
      # Database name
      config :database, validate: :string, required: true
      # Target table name
      config :table, validate: :string, required: true
      # Mapping name - Used by Kusto to map each attribute from incoming event JSON strings to the appropriate column in the table.
      # Note that this must be in JSON format, as this is the interface between Logstash and Kusto
      # Make this optional as name resolution in the JSON mapping can be done based on attribute names in the incoming event JSON strings
      config :json_mapping, validate: :string, default: nil

      # Mapping name - deprecated, use json_mapping
      config :mapping, validate: :string, deprecated: true

      # TODO: will be used to route events to many tables according to event properties
      config :dynamic_event_routing, validate: :boolean, default: false

      # Specify how many files can be uploaded concurrently
      config :upload_concurrent_count, validate: :number, default: 3

      # Specify how many files can be kept in the upload queue before the main process
      # starts processing them in the main thread (not healthy)
      config :upload_queue_size, validate: :number, default: 30

      # Host of the proxy , is an optional field. Can connect directly
      config :proxy_host, validate: :string, required: false

      # Port where the proxy runs , defaults to 80. Usually a value like 3128
      config :proxy_port, validate: :number, required: false, default: 80

      # Check Proxy URL can be over http or https. Do we need it this way or ignore this & remove this
      config :proxy_protocol, validate: :string, required: false, default: 'https'

      # --- Buffered mode configs (used by default when path is not set) ---
      # Maximum size of the buffer before it gets flushed, in bytes.
      config :max_batch_size, validate: :number, required: false, default: 10

      # Interval (in seconds) before the buffer gets flushed, regardless of size.
      config :plugin_flush_interval, validate: :number, required: false, default: 10

      # Maximum number of items in the buffer before it gets flushed.
      config :max_items, validate: :number, required: false, default: 1000

      # Process failed batches on startup. Only used in buffered mode.
      config :process_failed_batches_on_startup, validate: :boolean, required: false, default: false

      # Directory to store the failed batches that were not uploaded to Kusto. Only used in buffered mode.
      config :failed_dir_name, validate: :string, required: false, default: nil

      # --- File mode configs (used when path is explicitly set) ---
      # The path to the temporary file. Event fields can be used here for date-based rotation.
      config :path, validate: :string, required: false

      # Flush interval (in seconds) for flushing writes to files. 0 will flush on every message.
      config :flush_interval, validate: :number, default: 2

      # If the generated path is invalid, the events will be saved into this file.
      config :filename_failure, validate: :string, default: '_filepath_failures'

      # If the configured file is deleted, but an event is handled by the plugin, recreate the file.
      config :create_if_deleted, validate: :boolean, default: true

      # Dir access mode to use. Setting it to -1 uses default OS value.
      config :dir_mode, validate: :number, default: -1

      # File access mode to use. Setting it to -1 uses default OS value.
      config :file_mode, validate: :number, default: -1

      # Interval in seconds for stale file cleanup.
      config :stale_cleanup_interval, validate: :number, default: 10
      config :stale_cleanup_type, validate: %w[events interval], default: 'events'

      # Should the plugin recover temp files from past runs?
      config :recovery, validate: :boolean, default: true

      # Should temporary files be deleted after successful upload?
      config :delete_temp_files, validate: :boolean, default: true

      default :codec, 'json_lines'

      def register
        @buffered_mode = use_buffered_mode?
        @logger.info("Kusto output plugin mode: #{@buffered_mode ? 'buffered (in-memory)' : 'file-based'}")

        kusto_ingest_base =  LogStash::Outputs::KustoInternal::KustoIngestConfiguration.new(ingest_url, database, table,
                                                                                            json_mapping)
        kusto_auth_base   =  LogStash::Outputs::KustoInternal::KustoAuthConfiguration.new(app_id, app_key, app_tenant,
                                                                                          managed_identity, cli_auth)
        kusto_proxy_base  =  LogStash::Outputs::KustoInternal::KustoProxyConfiguration.new(proxy_host, proxy_port,
                                                                                           proxy_protocol, false)
        kusto_upload_config = LogStash::Outputs::KustoInternal::KustoUploadConfiguration.new(upload_concurrent_count,
                                                                                             upload_queue_size)

        if @buffered_mode
          dir = if failed_dir_name.nil? || failed_dir_name.empty?
                  ::File.join(Dir.tmpdir,
                              'logstash_backout')
                else
                  failed_dir_name
                end
          @file_persistence = LogStash::Outputs::KustoOutputInternal::FilePersistence.new(dir, @logger)

          kusto_flush_config = LogStash::Outputs::KustoInternal::KustoFlushConfiguration.new(max_items,
                                                                                             plugin_flush_interval, max_batch_size, process_failed_batches_on_startup)
          kusto_logstash_configuration = LogStash::Outputs::KustoInternal::KustoLogstashConfiguration.new(kusto_ingest_base,
                                                                                                          kusto_auth_base, kusto_proxy_base, kusto_flush_config, kusto_upload_config, @logger, @file_persistence)
          kusto_logstash_configuration.validate_config
          @buffer = LogStash::Outputs::KustoOutputInternal::LogStashEventsBatcher.new(kusto_logstash_configuration,
                                                                                      @logger)
        else
          kusto_flush_config = LogStash::Outputs::KustoInternal::KustoFlushConfiguration.new(1000, 10, 10, false)
          kusto_logstash_configuration = LogStash::Outputs::KustoInternal::KustoLogstashConfiguration.new(kusto_ingest_base,
                                                                                                          kusto_auth_base, kusto_proxy_base, kusto_flush_config, kusto_upload_config, @logger, nil)
          kusto_logstash_configuration.validate_config
          file_opts = {
            path: path,
            flush_interval: flush_interval,
            filename_failure: filename_failure,
            create_if_deleted: create_if_deleted,
            dir_mode: dir_mode,
            file_mode: file_mode,
            stale_cleanup_interval: stale_cleanup_interval,
            stale_cleanup_type: stale_cleanup_type,
            recovery: recovery,
            delete_temp_files: delete_temp_files
          }
          @file_handler = LogStash::Outputs::KustoOutputInternal::FileModeHandler.new(kusto_logstash_configuration, file_opts,
                                                                                      @logger)
        end
      end

      def multi_receive_encoded(events_and_encoded)
        if @buffered_mode
          # Buffered mode: events are added to an in-memory buffer (CustomSizeBasedBuffer).
          # The buffer flushes to Kusto automatically when size/time/count thresholds are met.
          # NOTE: We intentionally use event.to_hash (not the codec-encoded payload) because
          # buffered mode sends data via ingestFromStream with MULTIJSON format. The Kusto SDK
          # expects JSON objects, so we serialize them ourselves. User-configured codecs only
          # affect file mode, where the encoded payload is written directly to disk.
          events_and_encoded.each do |event, _encoded|
            @buffer.batch_event(event.to_hash)
          rescue StandardError => e
            @logger.error('Failed to process event, event dropped.',
                          exception: e.class, message: e.message, backtrace: e.backtrace)
          end
        else
          # File mode: events are written to time-rotated temp files on disk.
          # The encoded payload (from the codec) is written directly to the file.
          events_and_encoded.each do |event, encoded|
            @file_handler.receive(event, encoded)
          rescue StandardError => e
            @logger.error('Failed to process event, event dropped.',
                          exception: e.class, message: e.message, backtrace: e.backtrace)
          end
          # Trigger stale-file detection after each batch so that files with no
          # new writes can be closed and uploaded. This restores the behavior that
          # existed in the original single-class implementation on master.
          @file_handler.after_batch
        end
      end

      def close
        @logger.info('Closing Kusto output plugin')
        begin
          if @buffered_mode
            @buffer&.close
            @logger.info('Buffer and ingestor shutdown complete') unless @buffer.nil?
          else
            @file_handler&.close
            @logger.info('File handler shutdown complete') unless @file_handler.nil?
          end
        rescue StandardError => e
          @logger.error("Error shutting down: #{e.message}")
          @logger.error(e.backtrace.join("\n"))
        end
        @logger.info('Kusto output plugin Closed')
      end

      private

      def use_buffered_mode?
        # Backward compatibility: existing users who set `path` continue to use
        # file-based ingestion unchanged. New users (no `path`) get the default
        # buffered in-memory mode with streaming ingestion.
        path.nil? || path.empty?
      end
    end
  end
end
