require 'logger'
require 'thread'
require 'fileutils'
require 'securerandom'

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
      
        # Start the timer thread after a delay to ensure initializations are completed
        Thread.new do
          sleep(10) 
          @buffer_state[:timer] = Thread.new do
            loop do
              sleep(@buffer_config[:max_interval])
              buffer_flush(force: true)
            end
          end
        end
      end

				def <<(event)
					while buffer_full? do
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
        buffer_flush(final: true)
        clear_buffer_files
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
            @buffer_state[:pending_items] = []
            @buffer_state[:pending_size] = 0
          end

          begin
            @flush_callback.call(outgoing_items) # Pass the list of events to the callback
            @buffer_state[:network_down] = false # Reset network status after successful flush
            flush_buffer_files # Flush buffer files if any exist
          rescue => e
            @buffer_config[:logger].error("Flush failed: #{e.message}")
            @buffer_state[:network_down] = true
            save_buffer_to_file(outgoing_items)
          end

          @buffer_state[:last_flush] = Time.now.to_i
          @buffer_config[:logger].info("Flush completed. Flushed #{outgoing_items.size} events, #{outgoing_size} bytes")

          items_flushed = outgoing_items.size
        ensure
          @flush_mutex.unlock
        end

					items_flushed
				end

      def save_buffer_to_file(events)
        buffer_state_copy = {
          pending_items: events,
          pending_size: events.sum(&:bytesize)
        }

        ::FileUtils.mkdir_p(@buffer_config[:buffer_dir]) # Ensure directory exists
        file_path = ::File.join(@buffer_config[:buffer_dir], "buffer_state_#{Time.now.to_i}_#{SecureRandom.uuid}.log")
        ::File.open(file_path, 'w') do |file|
          file.write(Marshal.dump(buffer_state_copy))
        end
        @buffer_config[:logger].info("Saved buffer state to file: #{file_path}")
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
          begin
            buffer_state = Marshal.load(::File.read(file_path))
            @buffer_config[:logger].info("Flushed from file: #{file_path}")
            @flush_callback.call(buffer_state[:pending_items])
            ::File.delete(file_path)
            @buffer_config[:logger].info("Flushed and deleted buffer state file: #{file_path}")
          rescue => e
            @buffer_config[:logger].error("Failed to flush buffer state file: #{e.message}")
            break
          end
        end
      end

      def clear_buffer_files
        Dir.glob(::File.join(@buffer_config[:buffer_dir], 'buffer_state_*.log')).each do |file_path|
          ::File.delete(file_path)
        end
        @buffer_config[:logger].info("Cleared all buffer state files")
      end
    end
  end
end