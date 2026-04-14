require 'fileutils'
require 'logstash/outputs/kusto/ingestor'
require 'logstash/outputs/kusto/interval'

module LogStash
  module Outputs
    class KustoOutputInternal
      ##
      # Handles the old file-based staging strategy: events are written to
      # temporary files on disk, then uploaded to Kusto via ingestFromFile.
      #
      class FileModeHandler
        FIELD_REF = /%\{[^}]+\}/.freeze

        def initialize(kusto_logstash_configuration, file_opts, logger)
          @logger = logger
          @kusto_logstash_configuration = kusto_logstash_configuration

          @path = file_opts[:path]
          @flush_interval = file_opts[:flush_interval].to_i
          @filename_failure = file_opts[:filename_failure]
          @create_if_deleted = file_opts[:create_if_deleted]
          @dir_mode = file_opts[:dir_mode]
          @file_mode = file_opts[:file_mode]
          @stale_cleanup_interval = file_opts[:stale_cleanup_interval]
          @stale_cleanup_type = file_opts[:stale_cleanup_type]
          @recovery = file_opts[:recovery]
          @delete_temp_files = file_opts[:delete_temp_files]
          @database = kusto_logstash_configuration.kusto_ingest.database
          @table = kusto_logstash_configuration.kusto_ingest.table

          @files = {}
          @io_mutex = Mutex.new

          @path = ::File.expand_path("#{@path}.#{@database}.#{@table}")

          validate_path

          @file_root = if path_with_field_ref?
                         extract_file_root
                       else
                         ::File.dirname(@path)
                       end
          @failure_path = ::File.join(@file_root, @filename_failure)

          @ingestor = Ingestor.new(kusto_logstash_configuration, logger)

          recover_past_files if @recovery

          @last_stale_cleanup_cycle = Time.now

          @flusher = Interval.start(@flush_interval, -> { flush_pending_files }) if @flush_interval.positive?

          return unless (@stale_cleanup_type == 'interval') && @stale_cleanup_interval.positive?

          @cleaner = Interval.start(@stale_cleanup_interval, -> { close_stale_files })
        end

        def receive(event, encoded)
          file_output_path = event_path(event)

          @io_mutex.synchronize do
            fd = open(file_output_path)
            fd.write(encoded)
            fd.flush unless @flusher&.alive?
          end
        end

        # Called after each batch of events in multi_receive_encoded.
        # Triggers stale file cleanup when stale_cleanup_type is 'events'.
        def after_batch
          close_stale_files if @stale_cleanup_type == 'events'
        end

        def close
          @flusher&.stop
          @cleaner&.stop
          @io_mutex.synchronize do
            @logger.debug('Close: closing files')
            @files.each do |path, fd|
              fd.close
              @logger.debug("Closed file #{path}", fd: fd)
              kusto_send_file(path)
            rescue StandardError => e
              @logger.error('Exception while flushing and closing files.', exception: e)
            end
          end
          @ingestor&.stop
        end

        private

        def validate_path
          if (root_directory =~ FIELD_REF) != nil
            @logger.error('The starting part of the path should not be dynamic.',
                          path: @path)
            raise LogStash::ConfigurationError,
                  'The starting part of the path should not be dynamic.'
          end

          return if path_with_field_ref?

          @logger.error(
            'Path should include some time related fields to allow for file rotation.', path: @path
          )
          raise LogStash::ConfigurationError,
                'Path should include some time related fields to allow for file rotation.'
        end

        def root_directory
          parts = @path.split(::File::SEPARATOR).reject(&:empty?)
          if Gem.win_platform?
            parts[1]
          else
            parts.first
          end
        end

        def event_path(event)
          file_output_path = event.sprintf(@path)
          if path_with_field_ref? && !inside_file_root?(file_output_path)
            @logger.warn('The event tried to write outside the files root, writing the event to the failure file',
                         event: event, filename: @failure_path)
            file_output_path = @failure_path
          elsif !@create_if_deleted && deleted?(file_output_path)
            file_output_path = @failure_path
          end
          @logger.debug('Writing event to tmp file.', filename: file_output_path)
          file_output_path
        end

        def path_with_field_ref?
          @path =~ FIELD_REF
        end

        def extract_file_root
          parts = ::File.expand_path(@path).split(::File::SEPARATOR)
          parts.take_while { |part| part !~ FIELD_REF }.join(::File::SEPARATOR)
        end

        def inside_file_root?(log_path)
          target_file = ::File.expand_path(log_path)
          target_file.start_with?("#{@file_root}/")
        end

        def flush_pending_files
          @io_mutex.synchronize do
            @logger.debug('Starting flush cycle')
            @files.each do |path, fd|
              @logger.debug('Flushing file', path: path, fd: fd)
              fd.flush
            end
          end
        rescue StandardError => e
          @logger.error('Exception flushing files', exception: e.message,
                                                    backtrace: e.backtrace)
        end

        def close_stale_files
          now = Time.now
          return unless now - @last_stale_cleanup_cycle >= @stale_cleanup_interval

          @io_mutex.synchronize do
            @logger.debug('Starting stale files cleanup cycle', files: @files)
            inactive_files = @files.reject { |_path, fd| fd.active }
            @logger.debug("#{inactive_files.count} stale files found",
                          inactive_files: inactive_files)
            inactive_files.each do |path, fd|
              @logger.info("Closing file #{path}")
              fd.close
              @files.delete(path)
              kusto_send_file(path)
            end
            @files.each { |_path, fd| fd.active = false }
            @last_stale_cleanup_cycle = now
          end
        end

        def cached?(path)
          @files.include?(path) && !@files[path].nil?
        end

        def deleted?(path)
          !::File.exist?(path)
        end

        def open(path)
          return @files[path] if !deleted?(path) && cached?(path)

          if deleted?(path)
            if @create_if_deleted
              @logger.debug('Required file does not exist, creating it.', path: path)
              @files.delete(path)
            elsif cached?(path)
              return @files[path]
            end
          end

          @logger.info('Opening file', path: path)

          dir = ::File.dirname(path)
          unless ::Dir.exist?(dir)
            @logger.info('Creating directory', directory: dir)
            if @dir_mode == -1
              ::FileUtils.mkdir_p(dir)
            else
              ::FileUtils.mkdir_p(dir, mode: @dir_mode)
            end
          end

          stat = begin
            ::File.stat(path)
          rescue StandardError
            nil
          end
          fd = if stat && stat.ftype == 'fifo' && LogStash::Environment.jruby?
                 java.io.FileWriter.new(java.io.File.new(path))
               elsif @file_mode != -1
                 ::File.new(path, 'a+', @file_mode)
               else
                 ::File.new(path, 'a+')
               end
          @files[path] = IOWriter.new(fd)
        end

        def kusto_send_file(file_path)
          @ingestor.upload_file_async(file_path, @delete_temp_files)
        end

        def recover_past_files
          require 'find'

          path_last_char = @path.length - 1
          pattern_start = @path.index('%') || path_last_char
          last_folder_before_pattern = @path.rindex('/', pattern_start) || path_last_char
          new_path = @path[0..last_folder_before_pattern]

          begin
            return unless ::Dir.exist?(new_path)

            @logger.info("Going to recover old files in path #{new_path}")

            old_files = ::Find.find(new_path).grep(/.*\.#{@database}\.#{@table}$/)
            @logger.info("Found #{old_files.length} old file(s), sending them now...")

            old_files.each do |file|
              kusto_send_file(file)
            end
          rescue Errno::ENOENT => e
            @logger.warn('No such file or directory', exception: e.class, message: e.message, path: new_path,
                                                      backtrace: e.backtrace)
          end
        end
      end

      # Wrapper class for file IO with activity tracking
      class IOWriter
        def initialize(io)
          @io = io
        end

        def write(*args)
          @io.write(*args)
          @active = true
        end

        def flush
          @io.flush
        end

        def close
          @io.close
        end

        def method_missing(method_name, *args, &block)
          if @io.respond_to?(method_name)
            @io.send(method_name, *args, &block)
          else
            super
          end
        end

        attr_accessor :active
      end
    end; end; end
