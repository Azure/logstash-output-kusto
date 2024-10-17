require 'logger'
require 'thread'

module LogStash
  module Outputs
    class CustomSizeBasedBuffer
      def initialize(max_size_mb, max_interval, &flush_callback)
        @buffer_config = {
          max_size: max_size_mb * 1024 * 1024, # Convert MB to bytes
          max_interval: max_interval,
          logger: Logger.new(STDOUT)
        }
        @buffer_state = {
          pending_items: [],
          pending_size: 0,
          last_flush: Time.now.to_i,
          timer: Thread.new do
            loop do
              sleep(@buffer_config[:max_interval])
              buffer_flush(force: true)
            end
          end
        }
        @flush_callback = flush_callback
        @shutdown = false
        @pending_mutex = Mutex.new
        @flush_mutex = Mutex.new
        @buffer_config[:logger].info("CustomSizeBasedBuffer initialized with max_size: #{max_size_mb} MB, max_interval: #{max_interval} seconds")
      end

      def <<(event)
        while buffer_full? do
          sleep 0.1
        end

        @pending_mutex.synchronize do
          @buffer_state[:pending_items] << event
          @buffer_state[:pending_size] += event.bytesize
        end

        buffer_flush
      end

      def shutdown
        @buffer_config[:logger].info("Shutting down buffer")
        @shutdown = true
        @buffer_state[:timer].kill
        buffer_flush(final: true)
      end

      private

      def buffer_full?
        @pending_mutex.synchronize do
          @buffer_state[:pending_size] >= @buffer_config[:max_size]
        end
      end

      def buffer_flush(options = {})
        force = options[:force] || options[:final]
        final = options[:final]

        if final
          @flush_mutex.lock
        elsif !@flush_mutex.try_lock
          return 0
        end

        items_flushed = 0
        max_retries = 5
        retries = 0

        begin
          outgoing_items = []
          outgoing_size = 0

          @pending_mutex.synchronize do
            return 0 if @buffer_state[:pending_size] == 0

            time_since_last_flush = Time.now.to_i - @buffer_state[:last_flush]

            if !force && @buffer_state[:pending_size] < @buffer_config[:max_size] && time_since_last_flush < @buffer_config[:max_interval]
              return 0
            end

            if force
              @buffer_config[:logger].info("Time-based flush triggered after #{@buffer_config[:max_interval]} seconds")
            elsif @buffer_state[:pending_size] >= @buffer_config[:max_size]
              @buffer_config[:logger].info("Size-based flush triggered at #{@buffer_state[:pending_size]} bytes was reached")
            else
              @buffer_config[:logger].info("Flush triggered without specific condition")
            end

            outgoing_items = @buffer_state[:pending_items].dup
            outgoing_size = @buffer_state[:pending_size]
            buffer_initialize
          end

          begin
            @buffer_config[:logger].info("Attempting to flush #{outgoing_items.size} items to the network")
            @flush_callback.call(outgoing_items) # Pass the list of events to the callback
            @buffer_config[:logger].info("Successfully flushed #{outgoing_items.size} items to the network")
          rescue => e
            @buffer_config[:logger].error("Flush failed: #{e.message}")
            @buffer_config[:logger].error(e.backtrace.join("\n"))
            retries += 1
            if retries <= max_retries
              sleep 1
              retry
            else
              @buffer_config[:logger].error("Max retries reached. Data loss may occur.")
              raise e
            end
          end

          @buffer_state[:last_flush] = Time.now.to_i
          @buffer_config[:logger].info("Flush completed. Flushed #{outgoing_items.size} events, #{outgoing_size} bytes")

          items_flushed = outgoing_items.size
        ensure
          @flush_mutex.unlock
        end

        items_flushed
      end

      def buffer_initialize
        @buffer_state[:pending_items] = []
        @buffer_state[:pending_size] = 0
      end
    end
  end
end