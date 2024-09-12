module LogStash
  module Outputs
    class CustomSizeBasedBuffer
      def initialize(max_size, max_interval, &flush_callback)
        @max_size = max_size
        @max_interval = max_interval
        @flush_callback = flush_callback
        @buffer = []
        @mutex = Mutex.new
        @last_flush_time = Time.now
        @shutdown = false
        @flusher_condition = ConditionVariable.new

        start_flusher_thread
      end

      def <<(event)
        @mutex.synchronize do
          @buffer << event
          flush if @buffer.size >= @max_size
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
            flush
          end
        end
      end

      def flush
        return if @buffer.empty?

        begin
          @flush_callback.call(@buffer)
        rescue => e
          # Log the error and continue,
          puts "Error during flush: #{e.message}"
          puts e.backtrace.join("\n")
        ensure
          @buffer.clear
          @last_flush_time = Time.now
        end
      end
    end
  end
end