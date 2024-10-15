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
        @buffer_config[:logger].info("CustomSizeBasedBuffer initialized with max_size: #{max_size_mb} MB, max_interval: #{max_interval} seconds")
      end

      def <<(event)
        while buffer_full? do
          sleep 0.1
        end

        @buffer_state[:pending_items] << event
        @buffer_state[:pending_size] += event.bytesize

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
        @buffer_state[:pending_size] >= @buffer_config[:max_size]
      end

      def buffer_flush(options = {})
        force = options[:force] || options[:final]
        final = options[:final]

        if @buffer_state[:pending_size] == 0
          return 0
        end

        time_since_last_flush = Time.now.to_i - @buffer_state[:last_flush]

        if !force && @buffer_state[:pending_size] < @buffer_config[:max_size] && time_since_last_flush < @buffer_config[:max_interval]
          return 0
        end

        if force
          @buffer_config[:logger].info("Time-based flush triggered after #{@buffer_config[:max_interval]} seconds")
        elsif @buffer_state[:pending_size] >= @buffer_config[:max_size]
          @buffer_config[:logger].info("Size-based flush triggered at #{@buffer_state[:pending_size]} bytes was reached")
        end

        outgoing_items = @buffer_state[:pending_items]
        outgoing_size = @buffer_state[:pending_size]
        buffer_initialize

        @flush_callback.call(outgoing_items) # Pass the list of events to the callback

        @buffer_state[:last_flush] = Time.now.to_i
        @buffer_config[:logger].info("Flush completed. Flushed #{outgoing_items.size} events")

        outgoing_items.size
      end

      def buffer_initialize
        @buffer_state[:pending_items] = []
        @buffer_state[:pending_size] = 0
      end
    end
  end
end