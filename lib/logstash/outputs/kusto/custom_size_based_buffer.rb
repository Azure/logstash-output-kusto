require 'logger'
require 'thread'
require 'fileutils'
require 'securerandom'
require 'net/http'
require 'uri'

module LogStash
  module Outputs
    class CustomSizeBasedBuffer
      def initialize(max_size_mb, max_interval, &flush_callback)
        @buffer_config = {
          max_size: max_size_mb * 1024 * 1024, # Convert MB to bytes
          max_interval: max_interval,
          buffer_dir: './tmp/buffer_storage/',
          logger: Logger.new(STDOUT)
        }
        @buffer_state = {
          pending_items: [],
          pending_size: 0,
          last_flush: Time.now.to_i,
          timer: nil,
          network_down: false
        }
        @flush_callback = flush_callback
        @shutdown = false
        @pending_mutex = Mutex.new
        @flush_mutex = Mutex.new
        load_buffer_from_files
        @buffer_config[:logger].info("CustomSizeBasedBuffer initialized with max_size: #{max_size_mb} MB, max_interval: #{max_interval} seconds")
      
        # Start the timer thread
        @buffer_state[:timer] = Thread.new do
          loop do
            sleep(@buffer_config[:max_interval])
            prepare_flush(force: true)
          end
        end
      end

      def <<(event)
        while buffer_full? do
          prepare_flush(force: true) # Flush when buffer is full
          sleep 0.1
        end

        @pending_mutex.synchronize do
          @buffer_state[:pending_items] << event
          @buffer_state[:pending_size] += event.bytesize
        end
      end

      def shutdown
        @buffer_config[:logger].info("Shutting down buffer")
        @shutdown = true
        @buffer_state[:timer].kill
        prepare_flush(final: true)
        flush_buffer_files
      end

      private

      def buffer_full?
        @pending_mutex.synchronize do
          @buffer_state[:pending_size] >= @buffer_config[:max_size]
        end
      end

      def prepare_flush(options = {})
        force = options[:force] || options[:final]
        final = options[:final]

        outgoing_items = []
        outgoing_size = 0

        @pending_mutex.synchronize do
          return 0 if @buffer_state[:pending_size] == 0
          time_since_last_flush = Time.now.to_i - @buffer_state[:last_flush]

          if !force && @buffer_state[:pending_size] < @buffer_config[:max_size] && time_since_last_flush < @buffer_config[:max_interval]
            return 0
          end

          if time_since_last_flush >= @buffer_config[:max_interval]
            @buffer_config[:logger].info("Time-based flush triggered after #{@buffer_config[:max_interval]} seconds")
          else
            @buffer_config[:logger].info("Size-based flush triggered at #{@buffer_state[:pending_size]} bytes was reached")
          end

          if @buffer_state[:network_down]
            save_buffer_to_file(@buffer_state[:pending_items])
            @buffer_state[:pending_items] = []
            @buffer_state[:pending_size] = 0
            return 0
          end

          outgoing_items = @buffer_state[:pending_items].dup
          outgoing_size = @buffer_state[:pending_size]
          @buffer_state[:pending_items] = []
          @buffer_state[:pending_size] = 0
        end

        if Dir.glob(::File.join(@buffer_config[:buffer_dir], 'buffer_state_*.log')).any?
          @buffer_config[:logger].info("Flushing all buffer state files")
          flush_buffer_files
        end

        Thread.new { perform_flush(outgoing_items) }
      end

      def perform_flush(events, file_path = nil)
        
        @flush_mutex.lock
      
        begin
          if file_path
            unless ::File.exist?(file_path)
              return 0
            end
            begin
              buffer_state = Marshal.load(::File.read(file_path))
              events = buffer_state[:pending_items]
            rescue => e
              @buffer_config[:logger].error("Failed to load buffer from file: #{e.message}")
              return 0
            end
          end
          @buffer_config[:logger].info("Flushing #{events.size} events, #{events.sum(&:bytesize)} bytes")
          @flush_callback.call(events) # Pass the list of events to the callback
          @buffer_state[:network_down] = false # Reset network status after successful flush
          @buffer_state[:last_flush] = Time.now.to_i
          @buffer_config[:logger].info("Flush completed. Flushed #{events.size} events, #{events.sum(&:bytesize)} bytes")
      
          if file_path
            ::File.delete(file_path)
            @buffer_config[:logger].info("Flushed and deleted buffer state file: #{file_path}")
          end
      
        rescue => e
          @buffer_config[:logger].error("Flush failed: #{e.message}")
          @buffer_state[:network_down] = true
      
          while true
            sleep(2) # Wait before checking network availability again
            if network_available?
              @buffer_config[:logger].info("Network is back up. Retrying flush.")
              retry
            end
          end

        ensure
          @flush_mutex.unlock
        end
      end
      
      def network_available?
        begin
          uri = URI('http://www.google.com')
          response = Net::HTTP.get_response(uri)
          response.is_a?(Net::HTTPSuccess)
        rescue
          false
        end
      end

      def save_buffer_to_file(events)
        buffer_state_copy = {
          pending_items: events,
          pending_size: events.sum(&:bytesize)
        }
        begin
          ::FileUtils.mkdir_p(@buffer_config[:buffer_dir]) # Ensure directory exists
          file_path = ::File.join(@buffer_config[:buffer_dir], "buffer_state_#{Time.now.to_i}_#{SecureRandom.uuid}.log")
          ::File.open(file_path, 'w') do |file|
            file.write(Marshal.dump(buffer_state_copy))
          end
          @buffer_config[:logger].info("Saved #{events.size} events to file: #{file_path}")
        rescue => e
          @buffer_config[:logger].error("Failed to save buffer to file: #{e.message}")
        end
      end

      def load_buffer_from_files
        Dir.glob(::File.join(@buffer_config[:buffer_dir], 'buffer_state_*.log')).each do |file_path|
          begin
            buffer_state = Marshal.load(::File.read(file_path))
            @buffer_state[:pending_items].concat(buffer_state[:pending_items])
            @buffer_state[:pending_size] += buffer_state[:pending_size]
            ::File.delete(file_path)
          rescue => e
            @buffer_config[:logger].error("Failed to load buffer from file: #{e.message}")
          end
        end
        @buffer_config[:logger].info("Loaded buffer state from files")
      end

      def flush_buffer_files
        Dir.glob(::File.join(@buffer_config[:buffer_dir], 'buffer_state_*.log')).each do |file_path|
          @buffer_config[:logger].info("Flushing from buffer state file: #{file_path}")
          Thread.new { perform_flush([], file_path) }
        end
      end
    end
  end
end