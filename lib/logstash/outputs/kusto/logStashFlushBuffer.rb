# encoding: utf-8

require "logstash/outputs/kusto/kustoLogstashConfiguration"
require "logstash/outputs/kusto/customSizeBasedBuffer"
require "logstash/outputs/kusto/ingestor"
require "logger"

module LogStash; module Outputs; class KustoOutputInternal
class LogStashEventsBatcher
    include CustomSizeBasedBuffer
    def initialize(kusto_logstash_configuration, logger)
        logger.info("Initializing LogStashEventsBatcher")
        # Initialize the buffer with the configuration
        # The buffer is a custom buffer that extends the LogStash::Outputs::Base#buffer_initialize
        # It is used to buffer the events before sending them to Kusto
        @kusto_logstash_configuration = kusto_logstash_configuration
        @logger = logger
        @ingestor = LogStash::Outputs::KustoOutputInternal::Ingestor.new(@kusto_logstash_configuration, logger)
    
        logger.info("Initializing buffer with max_items: #{kusto_logstash_configuration.kusto_flush_config.max_items}, max_interval: #{kusto_logstash_configuration.kusto_flush_config.plugin_flush_interval}, flush_each: #{kusto_logstash_configuration.kusto_flush_config.max_batch_size}")
        buffer_initialize(
            :max_items => kusto_logstash_configuration.kusto_flush_config.max_items,
            :max_interval => kusto_logstash_configuration.kusto_flush_config.plugin_flush_interval,
            :logger => logger,
            #todo: There is a small discrepancy between the total size of the documents and the message body 
            :flush_each => kusto_logstash_configuration.kusto_flush_config.max_batch_size,
            :process_failed_batches_on_startup => kusto_logstash_configuration.kusto_flush_config.process_failed_batches_on_startup,
            :file_persistence => kusto_logstash_configuration.file_persistence
        )
    end # initialize

    # Public methods
    public

    # Adding an event document into the buffer
    def batch_event(event_document)
        buffer_receive(event_document)
    end # def batch_event

    # Flushing all buffer content to Kusto.
    # Called from Stud::Buffer#buffer_flush when there are events to flush
    def flush (documents, close=false)
        # Skip in case there are no candidate documents to deliver
        if documents.length < 1
            @logger.warn("No documents in batch in the batch. Skipping")
            return
        end
        @logger.info("Uploading batch of documents to Kusto #{documents.length} documents")
        begin
            @ingestor.upload(documents)
        rescue => e
            @logger.error("Error uploading batch to Kusto: #{e.message}")
            raise e # Let the buffer handle persistence of failed batch
        end
    end # def flush

    def close
        @logger.info("Closing LogStashEventsBatcher...")
        shutdown
    end

end # LogStashAutoResizeBuffer
end ;end ;end 