require 'logger'
require 'thread'
require 'csv'

module LogStash
  module Outputs
    class CustomSizeBasedBuffer
      def initialize(max_size_mb = 10, max_interval = 10, max_retries = 3, failed_items_path = nil, &flush_callback)
        @buffer_config = {
          max_size: max_size_mb * 1024 * 1024, # Convert MB to bytes
          max_interval: max_interval,
          max_retries: max_retries,
          failed_items_path: failed_items_path,
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
        @pending_mutex = Mutex.new
        @flush_mutex = Mutex.new
        @buffer_config[:logger].info("CustomSizeBasedBuffer initialized with max_size: #{max_size_mb} MB, max_interval: #{max_interval} seconds, max_retries: #{max_retries}, failed_items_path: #{failed_items_path}")
      end

      def <<(event)
        while buffer_full? do
          sleep 0.1
        end

        @pending_mutex.synchronize do
          @buffer_state[:pending_items] << event
          @buffer_state[:pending_size] += event.bytesize
        end

        # Trigger a flush if the buffer size exceeds the maximum size
        if buffer_full?
          buffer_flush(force: true)
        end
      end

      def shutdown
        @buffer_config[:logger].info("Shutting down buffer")
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
              if time_since_last_flush >= @buffer_config[:max_interval]
                @buffer_config[:logger].info("Time-based flush triggered after #{@buffer_config[:max_interval]} seconds")
              else
                @buffer_config[:logger].info("Size-based flush triggered when #{@buffer_state[:pending_size]} bytes was reached")
              end
            end

            outgoing_items = @buffer_state[:pending_items].dup
            outgoing_size = @buffer_state[:pending_size]
            buffer_initialize
          end

          retries = 0
          begin
            @buffer_config[:logger].info("Flushing: #{outgoing_items.size} items and #{outgoing_size} bytes to the network")
            @flush_callback.call(outgoing_items) # Pass the list of events to the callback
            @buffer_state[:last_flush] = Time.now.to_i
            @buffer_config[:logger].info("Flush completed. Flushed #{outgoing_items.size} events, #{outgoing_size} bytes")
          rescue => e
            retries += 1
            if retries <= @buffer_config[:max_retries]
              @buffer_config[:logger].error("Flush failed: #{e.message}. \nRetrying (#{retries}/#{@buffer_config[:max_retries]})...")
              sleep 1
              retry
            else
              @buffer_config[:logger].error("Max retries reached. Failed to flush #{outgoing_items.size} items and #{outgoing_size} bytes")
              handle_failed_flush(outgoing_items, e.message)
            end
          end

        ensure
          @flush_mutex.unlock
        end
      end

      def handle_failed_flush(items, error_message)
        if @buffer_config[:failed_items_path]
          begin
            ::File.open(@buffer_config[:failed_items_path], 'a') do |file|
              items.each do |item|
                file.puts(item)
              end
            end
            @buffer_config[:logger].info("Failed items stored in #{@buffer_config[:failed_items_path]}")
          rescue => e
            @buffer_config[:logger].error("Failed to store items: #{e.message}")
          end
        else
          @buffer_config[:logger].warn("No failed_items_path configured. Data loss may occur.")
        end
      end

      def buffer_initialize
        @buffer_state[:pending_items] = []
        @buffer_state[:pending_size] = 0
      end
    end
  end
end