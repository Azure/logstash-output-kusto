require 'logger'

module LogStash
  module Outputs
    class CustomSizeBasedBuffer
      def initialize(max_size, max_interval, &flush_callback)
        @max_size = max_size * 1024 * 1024 # Convert MB to bytes
        @max_interval = max_interval
        @flush_callback = flush_callback
        @buffer = []
        @buffer_size = 0 # Initialize buffer size
        @mutex = Mutex.new
        @last_flush_time = Time.now
        @shutdown = false
        @flusher_condition = ConditionVariable.new

        # Initialize logger
        @logger = Logger.new(STDOUT)
        @logger.level = Logger::DEBUG

        start_flusher_thread
      end

      def <<(event)
        @mutex.synchronize do
          event_size = event.bytesize
          @buffer << event
          @buffer_size += event_size
          if @buffer_size >= @max_size
            @logger.debug("Size-based flush triggered after #{@max_size} bytes was reached")
            flush
          end
        end
      end

      def shutdown
        @mutex.synchronize do
          @shutdown = true
          @flusher_condition.signal # Wake up the flusher thread
        end
        @flusher_thread.join
        flush # Ensure final flush after shutdown
      end

      private

      def start_flusher_thread
        @flusher_thread = Thread.new do
          loop do
            @mutex.synchronize do
              break if @shutdown
              if Time.now - @last_flush_time >= @max_interval
                @logger.debug("Time-based flush triggered after #{@max_interval} seconds")
                flush
              end
              @flusher_condition.wait(@mutex, @max_interval) # Wait for either the interval or shutdown signal
            end
          end
        end
      end


      def flush_if_needed
        @mutex.synchronize do
          if Time.now - @last_flush_time >= @max_interval
            @logger.debug("Time-based flush triggered in flush_if_needed")
            flush
          end
        end
      end

      def flush
        return if @buffer.empty?

        begin
          @logger.debug("Flushing buffer with #{@buffer.size} events and #{@buffer_size} bytes")
          @flush_callback.call(@buffer)
        rescue => e
          # Log the error and continue,
          @logger.error("Error during flush: #{e.message}")
          @logger.error(e.backtrace.join("\n"))
        ensure
          @buffer.clear
          @buffer_size = 0 # Reset buffer size
          @last_flush_time = Time.now
        end
      end
    end
  end
end