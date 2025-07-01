 # This code is from a PR for the official repo of ruby-stud
  # with a small change to calculating the event size in the var_size function
  # https://github.com/jordansissel/ruby-stud/pull/19
  #
  # @author {Alex Dean}[http://github.com/alexdean]
  #
  # Implements a generic framework for accepting events which are later flushed
  # in batches. Flushing occurs whenever +:max_items+ or +:max_interval+ (seconds)
  # has been reached or if the event size outgrows +:flush_each+ (bytes)
  #
  # Including class must implement +flush+, which will be called with all
  # accumulated items either when the output buffer fills (+:max_items+ or
  # +:flush_each+) or when a fixed amount of time (+:max_interval+) passes.
  #
  # == batch_receive and flush
  # General receive/flush can be implemented in one of two ways.
  #
  # === batch_receive(event) / flush(events)
  # +flush+ will receive an array of events which were passed to +buffer_receive+.
  #
  #   batch_receive('one')
  #   batch_receive('two')
  #
  # will cause a flush invocation like
  #
  #   flush(['one', 'two'])
  #
  # === batch_receive(event, group) / flush(events, group)
  # flush() will receive an array of events, plus a grouping key.
  #
  #   batch_receive('one',   :server => 'a')
  #   batch_receive('two',   :server => 'b')
  #   batch_receive('three', :server => 'a')
  #   batch_receive('four',  :server => 'b')
  #
  # will result in the following flush calls
  #
  #   flush(['one', 'three'], {:server => 'a'})
  #   flush(['two', 'four'],  {:server => 'b'})
  #
  # Grouping keys can be anything which are valid Hash keys. (They don't have to
  # be hashes themselves.) Strings or Fixnums work fine. Use anything which you'd
  # like to receive in your +flush+ method to help enable different handling for
  # various groups of events.
  #
  # == on_flush_error
  # Including class may implement +on_flush_error+, which will be called with an
  # Exception instance whenever buffer_flush encounters an error.
  #
  # * +buffer_flush+ will automatically re-try failed flushes, so +on_flush_error+
  #   should not try to implement retry behavior.
  # * Exceptions occurring within +on_flush_error+ are not handled by
  #   +buffer_flush+.
  #
  # == on_full_buffer_receive
  # Including class may implement +on_full_buffer_receive+, which will be called
  # whenever +buffer_receive+ is called while the buffer is full.
  #
  # +on_full_buffer_receive+ will receive a Hash like <code>{:pending => 30,
  # :outgoing => 20}</code> which describes the internal state of the module at
  # the moment.
  #
  # == final flush
  # Including class should call <code>buffer_flush(:final => true)</code>
  # during a teardown/shutdown routine (after the last call to buffer_receive)
  # to ensure that all accumulated messages are flushed.
  module LogStash; module Outputs; class KustoOutputInternal 
  module CustomSizeBasedBuffer

    public
    # Initialize the buffer.
    #
    # Call directly from your constructor if you wish to set some non-default
    # options. Otherwise buffer_initialize will be called automatically during the
    # first buffer_receive call.
    #
    # Options:
    # * :max_items, Max number of items to buffer before flushing. Default 50.
    # * :flush_each, Flush each bytes of buffer. Default 0 (no flushing fired by
    #                a buffer size).
    # * :max_interval, Max number of seconds to wait between flushes. Default 5.
    # * :logger, A logger to write log messages to. No default. Optional.
    #
    # @param [Hash] options
    def buffer_initialize(options={})
      if ! self.class.method_defined?(:flush)
        raise ArgumentError, "Any class including Stud::Buffer must define a flush() method."
      end
      @file_persistence = options[:file_persistence]

      @buffer_config = {
        :max_items => options[:max_items] || 50,
        :flush_each => options[:flush_each].to_i || 0,
        :max_interval => options[:max_interval] || 5,
        :logger => options[:logger] || Logger.new(STDOUT),
        :process_failed_batches_on_startup => options[:process_failed_batches_on_startup] || false,
        :has_on_flush_error => self.class.method_defined?(:on_flush_error),
        :has_on_full_buffer_receive => self.class.method_defined?(:on_full_buffer_receive)
      }

      @shutdown = false

      @buffer_state = {
        # items accepted from including class
        :pending_items => {},
        :pending_count => 0,
        :pending_size => 0,

        # guard access to pending_items & pending_count & pending_size
        :pending_mutex => Mutex.new,

        # items which are currently being flushed
        :outgoing_items => {},
        :outgoing_count => 0,
        :outgoing_size => 0,

        # ensure only 1 flush is operating at once
        :flush_mutex => Mutex.new,

        # data for timed flushes
        :last_flush => Time.now.to_i,
        :timer => Thread.new do
          loop do
            break if @shutdown
            sleep(@buffer_config[:max_interval])
            buffer_flush(:force => true)
          end
        end
      }

      # events we've accumulated
      buffer_clear_pending
      process_failed_batches if options[:process_failed_batches_on_startup]

    end

    # Determine if +:max_items+ or +:flush_each+ has been reached.
    #
    # buffer_receive calls will block while <code>buffer_full? == true</code>.
    #
    # @return [bool] Is the buffer full?
    def buffer_full?

      c1 = (@buffer_state[:pending_count] + @buffer_state[:outgoing_count] >= @buffer_config[:max_items])
      c2 = (@buffer_config[:flush_each] != 0 && @buffer_state[:pending_size] + @buffer_state[:outgoing_size] >= @buffer_config[:flush_each])

      if c1 || c2
        @buffer_config[:logger].debug("---------------Entering buffer_full?-----------------") 
      end


      if c1
        @buffer_config[:logger].debug("Buffer is full: max_items reached")
        @buffer_config[:logger].debug("Pending count: #{@buffer_state[:pending_count]}")
        @buffer_config[:logger].debug("Outgoing count: #{@buffer_state[:outgoing_count]}")
        @buffer_config[:logger].debug("Pending count: #{@buffer_config[:max_items]}")
      end
      if c2
        @buffer_config[:logger].debug("Buffer is full: flush_each reached")
        @buffer_config[:logger].debug("Pending size: #{@buffer_state[:pending_size]}")
        @buffer_config[:logger].debug("Outgoing size: #{@buffer_state[:outgoing_size]}")
        @buffer_config[:logger].debug("Flush each: #{@buffer_config[:flush_each]}")
        @buffer_config[:logger].debug("Max items: #{@buffer_config[:max_items]}")
      end

      if c1 || c2
        @buffer_config[:logger].debug("---------------Exiting buffer_full?-----------------") 
      end

      (@buffer_state[:pending_count] + @buffer_state[:outgoing_count] >= @buffer_config[:max_items]) || \
      (@buffer_config[:flush_each] != 0 && @buffer_state[:pending_size] + @buffer_state[:outgoing_size] >= @buffer_config[:flush_each])
    end

    # Save an event for later delivery
    #
    # Events are grouped by the (optional) group parameter you provide.
    # Groups of events, plus the group name, are later passed to +flush+.
    #
    # This call will block if +:max_items+ or +:flush_each+ has been reached.
    #
    # @see Stud::Buffer The overview has more information on grouping and flushing.
    #
    # @param event An item to buffer for flushing later.
    # @param group Optional grouping key. All events with the same key will be
    #              passed to +flush+ together, along with the grouping key itself.
    def buffer_receive(event, group=nil)
      buffer_initialize if ! @buffer_state
      # block if we've accumulated too many events
      while buffer_full? do
        on_full_buffer_receive(
          :pending => @buffer_state[:pending_count],
          :outgoing => @buffer_state[:outgoing_count]
        ) if @buffer_config[:has_on_full_buffer_receive]
        sleep 0.1
      end
      @buffer_state[:pending_mutex].synchronize do
        @buffer_state[:pending_items][group] << event
        @buffer_state[:pending_count] += 1
        @buffer_state[:pending_size] += var_size(event) if @buffer_config[:flush_each] != 0
      end
      buffer_flush
    end

    # Try to flush events.
    #
    # Returns immediately if flushing is not necessary/possible at the moment:
    # * :max_items or :flush_each have not been accumulated
    # * :max_interval seconds have not elapased since the last flush
    # * another flush is in progress
    #
    # <code>buffer_flush(:force => true)</code> will cause a flush to occur even
    # if +:max_items+ or +:flush_each+ or +:max_interval+ have not been reached. A forced flush
    # will still return immediately (without flushing) if another flush is
    # currently in progress.
    #
    # <code>buffer_flush(:final => true)</code> is identical to <code>buffer_flush(:force => true)</code>,
    # except that if another flush is already in progress, <code>buffer_flush(:final => true)</code>
    # will block/wait for the other flush to finish before proceeding.
    #
    # @param [Hash] options Optional. May be <code>{:force => true}</code> or <code>{:final => true}</code>.
    # @return [Fixnum] The number of items successfully passed to +flush+.
    def buffer_flush(options={})
      force = options[:force] || options[:final]
      final = options[:final]

      # final flush will wait for lock, so we are sure to flush out all buffered events
      if options[:final]
        @buffer_state[:flush_mutex].lock
      elsif ! @buffer_state[:flush_mutex].try_lock # failed to get lock, another flush already in progress
        return 0
      end

      items_flushed = 0

      begin
        return 0 if @buffer_state[:pending_count] == 0

        # compute time_since_last_flush only when some item is pending
        time_since_last_flush = get_time_since_last_flush

        return 0 if (!force) &&
           (@buffer_state[:pending_count] < @buffer_config[:max_items]) &&
           (@buffer_config[:flush_each] == 0 || @buffer_state[:pending_size] < @buffer_config[:flush_each]) &&
           (time_since_last_flush < @buffer_config[:max_interval])

        @buffer_state[:pending_mutex].synchronize do
          @buffer_state[:outgoing_items] = @buffer_state[:pending_items]
          @buffer_state[:outgoing_count] = @buffer_state[:pending_count]
          @buffer_state[:outgoing_size] = @buffer_state[:pending_size]
          buffer_clear_pending
        end
        @buffer_config[:logger].debug("---------------Exiting buffer_flush?-----------------") 
        @buffer_config[:logger].info("Flushing output",
          :outgoing_count => @buffer_state[:outgoing_count],
          :time_since_last_flush => time_since_last_flush,
          :outgoing_events_count => @buffer_state[:outgoing_items].length,
          :batch_timeout => @buffer_config[:max_interval],
          :force => force,
          :final => final
        ) if @buffer_config[:logger]

        @buffer_state[:outgoing_items].each do |group, events|
          begin

            if group.nil?
              flush(events,final)
            else
              flush(events, group, final)
            end

            @buffer_state[:outgoing_items].delete(group)
            events_size = events.size
            @buffer_state[:outgoing_count] -= events_size
            if @buffer_config[:flush_each] != 0
              events_volume = 0
              events.each do |event|
                events_volume += var_size(event)
              end
              @buffer_state[:outgoing_size] -= events_volume
            end
            items_flushed += events_size

          rescue => e
            @buffer_config[:logger].warn("Failed to flush outgoing items",
              :outgoing_count => @buffer_state[:outgoing_count],
              :exception => e,
              :backtrace => e.backtrace
            ) if @buffer_config[:logger]

            if @buffer_config[:has_on_flush_error]
              on_flush_error e
            end

            sleep 1
            retry
          end
          @buffer_state[:last_flush] = Time.now.to_i
          @buffer_config[:logger].debug("---------------Exiting buffer_flush?-----------------")           
        end

      ensure
        @buffer_state[:flush_mutex].unlock
      end

      return items_flushed
    end

    def process_failed_batches
      process_orphaned_processing_files
      process_new_json_files
    end

    def process_orphaned_processing_files
      Dir.glob(::File.join(@file_persistence.failed_dir, "failed_batch_*.json.processing")).each do |processing_file|
        process_failed_batch_file(processing_file)
        sleep(0.1)
      end
    end

    def process_new_json_files
      Dir.glob(::File.join(@file_persistence.failed_dir, "failed_batch_*.json")).each do |file|
        processing_file = file + ".processing"
        begin
          ::File.rename(file, processing_file)
          process_failed_batch_file(processing_file)
        rescue Errno::ENOENT, Errno::EACCES
          # File already claimed or deleted, skip
          next
        end
        sleep(0.1)
      end
    end

    def process_failed_batch_file(processing_file)
      begin
        batch = JSON.load(::File.read(processing_file))
        @buffer_state[:flush_mutex].lock
        begin
          flush(batch, true)
          @file_persistence.delete_batch(processing_file)
          @buffer_config[:logger].info("Successfully flushed and deleted failed batch file: #{processing_file}") if @buffer_config[:logger]
        rescue => e
          @buffer_config[:logger].warn("Failed to flush persisted batch: #{e.message}") if @buffer_config[:logger]
        ensure
          @buffer_state[:flush_mutex].unlock
        end
      rescue Errno::ENOENT
        @buffer_config[:logger].warn("Batch file #{processing_file} was not found when attempting to read. It may have been deleted by another process.") if @buffer_config[:logger]
      rescue => e
        @buffer_config[:logger].warn("Failed to load batch file #{processing_file}: #{e.message}. Moving to quarantine.") if @buffer_config[:logger]
        begin
          quarantine_dir = File.join(@file_persistence.failed_dir, "quarantine")
          FileUtils.mkdir_p(quarantine_dir) unless Dir.exist?(quarantine_dir)
          FileUtils.mv(processing_file, quarantine_dir)
        rescue => del_err
          @buffer_config[:logger].warn("Failed to move corrupted batch file #{processing_file} to quarantine: #{del_err.message}") if @buffer_config[:logger]
        end
      end
    end
    
    def shutdown
      # Graceful shutdown of timer thread
      if @buffer_state && @buffer_state[:timer]
        @shutdown = true
        @buffer_state[:timer].join
        @buffer_state[:timer] = nil
      end
      # Final flush of any remaining in-memory events
      buffer_flush(:final => true) if @buffer_state
    end

    private
    def buffer_clear_pending
      @buffer_state[:pending_items] = Hash.new { |h, k| h[k] = [] }
      @buffer_state[:pending_count] = 0
      @buffer_state[:pending_size] = 0
    end

    private
    def var_size(var)
        # Calculate event size as a json. 
        # assuming event is a hash
       return var.to_json.bytesize + 2
    end

    protected
    def get_time_since_last_flush
      Time.now.to_i - @buffer_state[:last_flush]
    end    

  end
end ;end ;end 