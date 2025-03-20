# encoding: utf-8

require "logstash/outputs/kusto/kustoLogstashConfiguration"
require "logstash/outputs/kusto/customSizeBasedBuffer"
require "logstash/outputs/kusto/ingestor"
require "logger"

module LogStash; module Outputs; class KustoOutputInternal
class LogStashEventsBatcher
    include CustomSizeBasedBuffer
    def initialize(kustoLogstashConfiguration,logger)
        logger.info("Initializing LogStashEventsBatcher")
        # Initialize the buffer with the configuration
        # The buffer is a custom buffer that extends the LogStash::Outputs::Base#buffer_initialize
        # It is used to buffer the events before sending them to Kusto
        logger.info("Initializing buffer with max_items: #{kustoLogstashConfiguration.kusto_flush_config.max_items}, max_interval: #{kustoLogstashConfiguration.kusto_flush_config.plugin_flush_interval}, flush_each: #{kustoLogstashConfiguration.kusto_flush_config.max_batch_size}")
        buffer_initialize(
            :max_items => kustoLogstashConfiguration.kusto_flush_config.max_items,
            :max_interval => kustoLogstashConfiguration.kusto_flush_config.plugin_flush_interval,
            :logger => logger,
            #todo: There is a small discrepancy between the total size of the documents and the message body 
            :flush_each => kustoLogstashConfiguration.kusto_flush_config.max_batch_size
        )
        @kustoLogstashConfiguration = kustoLogstashConfiguration
        workers_pool = Concurrent::ThreadPoolExecutor.new(min_threads: 1,
            max_threads: @kustoLogstashConfiguration.kusto_upload_config.upload_concurrent_count,
            max_queue: @kustoLogstashConfiguration.kusto_upload_config.upload_queue_size,
            fallback_policy: :caller_runs
        ) if @ingestor.nil?
        @log===ger = logger
        @ingestor = LogStash::Outputs::KustoOutputInternal::Ingestor.new(@kustoLogstashConfiguration, logger, workers_pool)
    end # initialize

    # Public methods
    public

    # Adding an event document into the buffer
    def batch_event(event_document)        
        buffer_receive(event_document)
    end # def batch_event

    # Flushing all buffer content to Azure Loganalytics.
    # Called from Stud::Buffer#buffer_flush when there are events to flush
    def flush (documents, close=false)
        # Skip in case there are no candidate documents to deliver
        if documents.length < 1
            @logger.warn("No documents in batch in the batch. Skipping")
            return
        end
       

        @ingestor.upload(documents)
    end # def flush

    def close
        buffer_flush(:final => true)
    end

end # LogStashAutoResizeBuffer
end ;end ;end 