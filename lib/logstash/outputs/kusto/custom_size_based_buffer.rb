require 'logger'
require 'thread'
require 'fileutils'

module LogStash
  module Outputs
    class CustomSizeBasedBuffer
      def initialize(max_size_mb, max_interval, buffer_file, &flush_callback)
        raise ArgumentError, "buffer_file cannot be nil" if buffer_file.nil?

        @buffer_config = {
          max_size: max_size_mb * 1024 * 1024, # Convert MB to bytes
          max_interval: max_interval,
          buffer_file: buffer_file,
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
        load_buffer_from_file
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
        clear_file_buffer
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
            buffer_initialize
          end

          begin
            @flush_callback.call(outgoing_items) # Pass the list of events to the callback
            clear_flushed_buffer_states(outgoing_items) unless ::File.zero?(@buffer_config[:buffer_file]) # Clear the flushed items from the file
          rescue => e
            @buffer_config[:logger].error("Flush failed: #{e.message}")
            # Save the items to the file buffer in case of failure
            @pending_mutex.synchronize do
              @buffer_state[:pending_items] = outgoing_items + @buffer_state[:pending_items]
              @buffer_state[:pending_size] += outgoing_size
              save_buffer_to_file
            end
            raise e
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

      def clear_flushed_buffer_states(flushed_items)
        remaining_buffer_states = []
        ::File.foreach(@buffer_config[:buffer_file]) do |line|
          begin
            buffer_state = Marshal.load(line)
            buffer_state[:pending_items] -= flushed_items
            buffer_state[:pending_size] = buffer_state[:pending_items].sum(&:bytesize)
            remaining_buffer_states << buffer_state unless buffer_state[:pending_items].empty?
          rescue ArgumentError => e
            @buffer_config[:logger].error("Failed to load buffer state: #{e.message}")
            next
          end
        end
      
        ::File.open(@buffer_config[:buffer_file], 'w') do |file|
          remaining_buffer_states.each do |state|
            file.write(Marshal.dump(state) + "\n")
          end
        end
      end

      def save_buffer_to_file
        buffer_state_copy = @buffer_state.dup
        buffer_state_copy.delete(:timer) # Exclude the Thread object from serialization
      
        ::FileUtils.mkdir_p(::File.dirname(@buffer_config[:buffer_file])) # Ensure directory exists
        ::File.open(@buffer_config[:buffer_file], 'a') do |file|
          file.write(Marshal.dump(buffer_state_copy) + "\n")
        end
        @buffer_config[:logger].info("Saved buffer state to file")
      end
      
      def load_buffer_from_file
        ::FileUtils.mkdir_p(::File.dirname(@buffer_config[:buffer_file])) # Ensure directory exists
        ::File.open(@buffer_config[:buffer_file], 'a') {} # Create the file if it doesn't exist
      
        if ::File.file?(@buffer_config[:buffer_file]) && !::File.zero?(@buffer_config[:buffer_file])
          begin
            @pending_mutex.synchronize do
              buffer_states = []
              ::File.foreach(@buffer_config[:buffer_file]) do |line|
                buffer_states << Marshal.load(line)
              end
              @buffer_state = buffer_states.reduce do |acc, state|
                acc[:pending_items].concat(state[:pending_items])
                acc[:pending_size] += state[:pending_size]
                acc
              end
              @buffer_state[:timer] = Thread.new do
                loop do
                  sleep(@buffer_config[:max_interval])
                  buffer_flush(force: true)
                end
              end
              # Ensure the buffer does not flush immediately upon loading
              @buffer_state[:last_flush] = Time.now.to_i
            end
            @buffer_config[:logger].info("Loaded buffer state from file")
          rescue => e
            @buffer_config[:logger].error("Failed to load buffer from file: #{e.message}")
            buffer_initialize
          end
        else
          buffer_initialize
        end
      end

      def clear_file_buffer
        ::File.open(@buffer_config[:buffer_file], 'w') {} # Truncate the file
        @buffer_config[:logger].info("File buffer cleared on shutdown")
      end
    end
  end
end