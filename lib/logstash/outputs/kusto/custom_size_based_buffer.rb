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
  
          start_flusher_thread
        end
  
        def <<(event)
          @mutex.synchronize do
            @buffer << event
            flush if @buffer.size >= @max_size
          end
        end
  
        private
        def start_flusher_thread
          Thread.new do
            loop do
              sleep @max_interval
              flush_if_needed
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
  
          @flush_callback.call(@buffer)
          @buffer.clear
          @last_flush_time = Time.now
        end
      end
    end
  end
  