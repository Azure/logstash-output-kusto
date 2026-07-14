# encoding: utf-8

require 'logstash/outputs/base'
require 'logstash/namespace'
require 'logstash/errors'

require 'logstash/outputs/kusto/ingestor'
require 'logstash/outputs/kusto/interval'
require 'logstash/outputs/kusto/streaming_chunker'

##
# This plugin sends messages to Azure Kusto in batches.
#
class LogStash::Outputs::Kusto < LogStash::Outputs::Base
  config_name 'kusto'
  concurrency :shared

  FIELD_REF = /%\{[^}]+\}/

  attr_reader :failure_path

  # The path to the file to write. Event fields can be used here,
  # like `/var/log/logstash/%{host}/%{application}`
  # One may also utilize the path option for date-based log
  # rotation via the joda time format. This will use the event
  # timestamp.
  # E.g.: `path => "./test-%{+YYYY-MM-dd}.txt"` to create
  # `./test-2013-05-29.txt`
  #
  # If you use an absolute path you cannot start with a dynamic string.
  # E.g: `/%{myfield}/`, `/test-%{myfield}/` are not valid paths
  config :path, validate: :string, required: false

  # Flush interval (in seconds) for flushing writes to files.
  # 0 will flush on every message. Increase this value to recude IO calls but keep 
  # in mind that events buffered before flush can be lost in case of abrupt failure.
  config :flush_interval, validate: :number, default: 2

  # If the generated path is invalid, the events will be saved
  # into this file and inside the defined path.
  config :filename_failure, validate: :string, default: '_filepath_failures'

  # If the configured file is deleted, but an event is handled by the plugin,
  # the plugin will recreate the file. Default => true
  config :create_if_deleted, validate: :boolean, default: true

  # Dir access mode to use. Note that due to the bug in jruby system umask
  # is ignored on linux: https://github.com/jruby/jruby/issues/3426
  # Setting it to -1 uses default OS value.
  # Example: `"dir_mode" => 0750`
  config :dir_mode, validate: :number, default: -1

  # File access mode to use. Note that due to the bug in jruby system umask
  # is ignored on linux: https://github.com/jruby/jruby/issues/3426
  # Setting it to -1 uses default OS value.
  # Example: `"file_mode" => 0640`
  config :file_mode, validate: :number, default: -1

  # TODO: fix the interval type...
  config :stale_cleanup_interval, validate: :number, default: 10
  config :stale_cleanup_type, validate: %w[events interval], default: 'events'

  # Should the plugin recover from failure?
  #
  # If `true`, the plugin will look for temp files from past runs within the
  # path (before any dynamic pattern is added) and try to process them
  #
  # If `false`, the plugin will disregard temp files found
  config :recovery, validate: :boolean, default: true

  
  # The Kusto endpoint for ingestion related communication. You can see it on the Azure Portal.
  config :ingest_url, validate: :string, required: true

  # The following are the credentails used to connect to the Kusto service
  # application id 
  config :app_id, validate: :string, required: false
  # application key (secret)
  config :app_key, validate: :password, required: false
  # aad tenant id
  config :app_tenant, validate: :string, default: nil
  # managed identity id
  config :managed_identity, validate: :string, default: nil
  # CLI credentials for dev-test
  config :cli_auth, validate: :boolean, default: false
  # The following are the data settings that impact where events are written to
  # Database name
  config :database, validate: :string, required: true
  # Target table name
  config :table, validate: :string, required: true
  # Mapping name - Used by Kusto to map each attribute from incoming event JSON strings to the appropriate column in the table.
  # Note that this must be in JSON format, as this is the interface between Logstash and Kusto
  # Make this optional as name resolution in the JSON mapping can be done based on attribute names in the incoming event JSON strings
  config :json_mapping, validate: :string, default: nil

  # Mapping name - deprecated, use json_mapping
  config :mapping, validate: :string, deprecated: true


  # Determines if local files used for temporary storage will be deleted
  # after upload is successful
  config :delete_temp_files, validate: :boolean, default: true

  # TODO: will be used to route events to many tables according to event properties
  config :dynamic_event_routing, validate: :boolean, default: false

  # Specify how many files can be uploaded concurrently
  config :upload_concurrent_count, validate: :number, default: 3

  # Specify how many files can be kept in the upload queue before the main process
  # starts processing them in the main thread (not healthy)
  config :upload_queue_size, validate: :number, default: 30

  # Queued ingestion is optimized for throughput. Streaming ingestion is
  # optimized for low latency and automatically falls back to queued ingestion.
  config :ingestion_mode, validate: %w[queued streaming], default: 'queued'

  # Maximum encoded bytes in one streaming request. Events are never split.
  config :streaming_max_request_bytes, validate: :number, default: 1_048_576

  # Additional retries for transient errors that escape the streaming client.
  config :streaming_max_retry_attempts, validate: :number, default: 2

  # Initial retry delay. Subsequent streaming retries use exponential backoff.
  config :streaming_retry_backoff_seconds, validate: :number, default: 1

  # Limit concurrent requests issued by shared Logstash pipeline workers.
  config :streaming_concurrent_requests, validate: :number, default: 4

  # Local durable spool for streaming requests. A stable default is
  # derived from the Kusto destination so files can be recovered after restart.
  config :streaming_temp_directory, validate: :string, required: false

  # Host of the proxy , is an optional field. Can connect directly
  config :proxy_host, validate: :string, required: false

  # Port where the proxy runs , defaults to 80. Usually a value like 3128
  config :proxy_port, validate: :number, required: false , default: 80

  # Check Proxy URL can be over http or https. Dowe need it this way or ignore this & remove this
  config :proxy_protocol, validate: :string, required: false , default: 'http'

  default :codec, 'json_lines'

  def register
    require 'fileutils' # For mkdir_p

    @files = {}
    @io_mutex = Mutex.new

    final_mapping = json_mapping
    if final_mapping.nil? || final_mapping.empty?
      final_mapping = mapping
    end

    if ingestion_mode == 'queued'
      raise LogStash::ConfigurationError, 'path is required for queued ingestion.' if path.nil? || path.empty?

      # TODO: add id to the tmp path to support multiple outputs of the same type.
      # TODO: Fix final_mapping when dynamic routing is supported
      @path = if dynamic_event_routing
                File.expand_path("#{path}.%{[@metadata][database]}.%{[@metadata][table]}.%{[@metadata][final_mapping]}")
              else
                File.expand_path("#{path}.#{database}.#{table}")
              end

      validate_path

      @file_root = if path_with_field_ref?
                     extract_file_root
                   else
                     File.dirname(path)
                   end
      @failure_path = File.join(@file_root, @filename_failure)
    else
      validate_streaming_config
      @streaming_chunker = StreamingChunker.new(streaming_max_request_bytes.to_i)
      require 'digest'
      destination_id = Digest::SHA256.hexdigest(
        [ingest_url, database, table, final_mapping].join("\0")
      )[0, 20]
      @streaming_temp_directory = File.expand_path(
        streaming_temp_directory ||
        File.join(
          LogStash::SETTINGS.get('path.data'),
          'plugins',
          'logstash-output-kusto',
          destination_id
        )
      )
      @streaming_dir_mode = @dir_mode == -1 ? 0o700 : @dir_mode.to_i
      @streaming_file_mode = @file_mode == -1 ? 0o600 : @file_mode.to_i
      validate_streaming_modes
      prepare_streaming_spool_directory
      acquire_streaming_spool_lock
      @streaming_metric = metric.namespace(:streaming)
      %i[
        requests
        events
        bytes
        streamed
        queued_fallback
        pending
        final_non_success
        oversized_events
        failures
        retry_cycles
      ].each { |counter| @streaming_metric.increment(counter, 0) }
    end

    begin
      executor = Concurrent::ThreadPoolExecutor.new(
        min_threads: 1,
        max_threads: ingestion_mode == 'queued' ? upload_concurrent_count : streaming_concurrent_requests,
        max_queue: upload_queue_size,
        fallback_policy: :caller_runs
      )

      @ingestor = Ingestor.new(
        ingest_url,
        app_id,
        app_key,
        app_tenant,
        managed_identity,
        cli_auth,
        database,
        table,
        final_mapping,
        delete_temp_files,
        proxy_host,
        proxy_port,
        proxy_protocol,
        @logger,
        executor,
        ingestion_mode,
        streaming_max_retry_attempts.to_i,
        streaming_retry_backoff_seconds.to_f,
        nil,
        nil,
        ingestion_mode == 'streaming' ? @streaming_metric : nil
      )

      if recovery
        ingestion_mode == 'queued' ? recover_past_files : recover_streaming_files
      end

      @last_stale_cleanup_cycle = Time.now

      @flush_interval = @flush_interval.to_i
      if ingestion_mode == 'queued' && @flush_interval > 0
        @flusher = Interval.start(@flush_interval, -> { flush_pending_files })
      end

      if ingestion_mode == 'queued' && (@stale_cleanup_type == 'interval') && (@stale_cleanup_interval > 0)
        @cleaner = Interval.start(stale_cleanup_interval, -> { close_stale_files })
      end
    rescue
      release_streaming_spool_lock
      raise
    end
  end

  private
  def validate_streaming_config
    if streaming_max_request_bytes.to_i <= 0
      raise LogStash::ConfigurationError,
            'streaming_max_request_bytes must be greater than zero.'
    end

    if streaming_max_retry_attempts.to_i < 0
      raise LogStash::ConfigurationError,
            'streaming_max_retry_attempts must be zero or greater.'
    end

    if streaming_retry_backoff_seconds.to_f <= 0
      raise LogStash::ConfigurationError,
            'streaming_retry_backoff_seconds must be greater than zero.'
    end

    if streaming_concurrent_requests.to_i <= 0
      raise LogStash::ConfigurationError,
            'streaming_concurrent_requests must be greater than zero.'
    end
  end

  private
  def validate_streaming_modes
    if (@streaming_dir_mode & 0o022).positive?
      raise LogStash::ConfigurationError,
            'dir_mode must not allow group or world writes for streaming.'
    end

    return unless (@streaming_file_mode & 0o022).positive?

    raise LogStash::ConfigurationError,
          'file_mode must not allow group or world writes for streaming.'
  end

  private
  def prepare_streaming_spool_directory
    if File.symlink?(@streaming_temp_directory)
      raise LogStash::ConfigurationError,
            "Streaming spool directory must not be a symlink: #{@streaming_temp_directory}"
    end

    FileUtils.mkdir_p(@streaming_temp_directory, mode: @streaming_dir_mode)
    File.chmod(@streaming_dir_mode, @streaming_temp_directory)
    @streaming_temp_directory = File.realpath(@streaming_temp_directory)
    validate_streaming_spool_directory
    validate_streaming_spool_parents unless Gem.win_platform?
  end

  private
  def validate_streaming_spool_directory
    stat = File.lstat(@streaming_temp_directory)
    unless stat.directory? && !stat.symlink?
      raise LogStash::ConfigurationError,
            "Streaming spool path is not a directory: #{@streaming_temp_directory}"
    end

    return if Gem.win_platform? || stat.uid == Process.uid

    raise LogStash::ConfigurationError,
          "Streaming spool directory is not owned by the Logstash user: #{@streaming_temp_directory}"
  end

  private
  def validate_streaming_spool_parents
    current = File.dirname(@streaming_temp_directory)
    loop do
      stat = File.lstat(current)
      mode = stat.mode & 0o7777
      owned_by_trusted_user = stat.uid == Process.uid || stat.uid.zero?
      sticky_shared_directory = stat.sticky? && (mode & 0o002).positive?

      unless owned_by_trusted_user || ((mode & 0o200).zero? && (mode & 0o022).zero?)
        raise LogStash::ConfigurationError,
              "Streaming spool parent is owned by an untrusted user: #{current}"
      end
      if (mode & 0o022).positive? && !sticky_shared_directory
        raise LogStash::ConfigurationError,
              "Streaming spool parent is writable by untrusted users: #{current}"
      end

      parent = File.dirname(current)
      break if parent == current

      current = parent
    end
  end

  private
  def validate_path
    if (root_directory =~ FIELD_REF) != nil
      @logger.error('The starting part of the path should not be dynamic.', path: @path)
      raise LogStash::ConfigurationError.new('The starting part of the path should not be dynamic.')
    end

    if !path_with_field_ref?
      @logger.error('Path should include some time related fields to allow for file rotation.', path: @path)
      raise LogStash::ConfigurationError.new('Path should include some time related fields to allow for file rotation.')
    end
  end

  private 
  def root_directory
    parts = @path.split(File::SEPARATOR).reject(&:empty?)
    if Gem.win_platform?
      # First part is the drive letter
      parts[1]
    else
      parts.first
    end
  end

  public
  def multi_receive_encoded(events_and_encoded)
    if ingestion_mode == 'streaming'
      streaming_files = write_streaming_chunks(
        @streaming_chunker.chunks(events_and_encoded.map(&:last))
      )
      streaming_files.each do |file|
        @ingestor.upload_async(file[:path], delete_temp_files)
      end
      return
    end

    encoded_by_path = Hash.new { |h, k| h[k] = [] }

    events_and_encoded.each do |event, encoded|
      file_output_path = event_path(event)
      encoded_by_path[file_output_path] << encoded
    end

    @io_mutex.synchronize do
      encoded_by_path.each do |path, chunks|
        fd = open(path)
        # append to the file
        chunks.each { |chunk| fd.write(chunk) }
        fd.flush unless @flusher && @flusher.alive?
      end

      close_stale_files if @stale_cleanup_type == 'events'
    end
  end

  def close
    @flusher.stop unless @flusher.nil?
    @cleaner.stop unless @cleaner.nil?
    if ingestion_mode == 'queued'
      @io_mutex.synchronize do
        @logger.debug('Close: closing files')

        @files.each do |path, fd|
          begin
            fd.close
            @logger.debug("Closed file #{path}", fd: fd)

            kusto_send_file(path)
          rescue Exception => e
            @logger.error('Exception while flushing and closing files.', exception: e)
          end
        end
      end
    end

    begin
      @ingestor.stop unless @ingestor.nil?
    ensure
      release_streaming_spool_lock
    end
  end

  private
  def inside_file_root?(log_path)
    target_file = File.expand_path(log_path)
    return target_file.start_with?("#{@file_root}/")
  end

  private
  def event_path(event)
    file_output_path = generate_filepath(event)
    if path_with_field_ref? && !inside_file_root?(file_output_path)
      @logger.warn('The event tried to write outside the files root, writing the event to the failure file', event: event, filename: @failure_path)
      file_output_path = @failure_path
    elsif !@create_if_deleted && deleted?(file_output_path)
      file_output_path = @failure_path
    end
    @logger.debug('Writing event to tmp file.', filename: file_output_path)

    file_output_path
  end

  private
  def generate_filepath(event)
    event.sprintf(@path)
  end

  private
  def path_with_field_ref?
    path =~ FIELD_REF
  end

  private
  def extract_file_root
    parts = File.expand_path(path).split(File::SEPARATOR)
    parts.take_while { |part| part !~ FIELD_REF }.join(File::SEPARATOR)
  end

  # the back-bone of @flusher, our periodic-flushing interval.
  private
  def flush_pending_files
    @io_mutex.synchronize do
      @logger.debug('Starting flush cycle')

      @files.each do |path, fd|
        @logger.debug('Flushing file', path: path, fd: fd)
        fd.flush
      end
    end
  rescue Exception => e
    # squash exceptions caught while flushing after logging them
    @logger.error('Exception flushing files', exception: e.message, backtrace: e.backtrace)
  end

  # every 10 seconds or so (triggered by events, but if there are no events there's no point closing files anyway)
  private
  def close_stale_files
    now = Time.now
    return unless now - @last_stale_cleanup_cycle >= @stale_cleanup_interval

    @logger.debug('Starting stale files cleanup cycle', files: @files)
    inactive_files = @files.select { |path, fd| not fd.active }
    @logger.debug("#{inactive_files.count} stale files found", inactive_files: inactive_files)
    inactive_files.each do |path, fd|
      @logger.info("Closing file #{path}")
      fd.close
      @files.delete(path)

      kusto_send_file(path)
    end
    # mark all files as inactive, a call to write will mark them as active again
    @files.each { |path, fd| fd.active = false }
    @last_stale_cleanup_cycle = now
  end

  private
  def cached?(path)
    @files.include?(path) && !@files[path].nil?
  end

  private
  def deleted?(path)
    !File.exist?(path)
  end

  private
  def open(path)
    return @files[path] if !deleted?(path) && cached?(path)

    if deleted?(path)
      if @create_if_deleted
        @logger.debug('Required file does not exist, creating it.', path: path)
        @files.delete(path)
      else
        return @files[path] if cached?(path)
      end
    end

    @logger.info('Opening file', path: path)

    dir = File.dirname(path)
    if !Dir.exist?(dir)
      @logger.info('Creating directory', directory: dir)
      if @dir_mode != -1
        FileUtils.mkdir_p(dir, mode: @dir_mode)
      else
        FileUtils.mkdir_p(dir)
      end
    end

    # work around a bug opening fifos (bug JRUBY-6280)
    stat = begin
             File.stat(path)
           rescue
             nil
           end
    fd =  if stat && stat.ftype == 'fifo' && LogStash::Environment.jruby?
            java.io.FileWriter.new(java.io.File.new(path))
          elsif @file_mode != -1
            File.new(path, 'a+', @file_mode)
          else
            File.new(path, 'a+')
          end
          # fd = if @file_mode != -1
          #         File.new(path, 'a+', @file_mode)
          #       else
          #         File.new(path, 'a+')
          #       end
        #  end
    @files[path] = IOWriter.new(fd)
  end

  private
  def kusto_send_file(file_path)
    @ingestor.upload_async(file_path, delete_temp_files)
  end

  private
  def recover_past_files
    require 'find'

    # we need to find the last "regular" part in the path before any dynamic vars
    path_last_char = @path.length - 1

    pattern_start = @path.index('%') || path_last_char
    last_folder_before_pattern = @path.rindex('/', pattern_start) || path_last_char
    new_path = path[0..last_folder_before_pattern]
    
    begin
      return unless Dir.exist?(new_path)
      @logger.info("Going to recover old files in path #{@new_path}")
      
      old_files = Find.find(new_path).select { |p| /.*\.#{database}\.#{table}$/ =~ p }
      @logger.info("Found #{old_files.length} old file(s), sending them now...")

      old_files.each do |file|
        kusto_send_file(file)
      end
    rescue Errno::ENOENT => e
      @logger.warn('No such file or directory', exception: e.class, message: e.message, path: new_path, backtrace: e.backtrace)
    end
  end

  private
  def write_streaming_chunks(chunks)
    require 'securerandom'

    return [] if chunks.empty?

    batch_id = SecureRandom.uuid
    temporary_directory = File.join(@streaming_temp_directory, ".batch-#{batch_id}.tmp")
    ready_directory = File.join(@streaming_temp_directory, "batch-#{batch_id}.ready")
    files = []
    committed = false
    Dir.mkdir(temporary_directory, @streaming_dir_mode)

    chunks.each_with_index do |chunk, index|
      source_id = SecureRandom.uuid
      path = File.join(
        temporary_directory,
        format('stream-%06d-%s.json', index, source_id)
      )
      bytes = chunk.sum(&:bytesize)
      write_streaming_file(path, chunk)
      files << { path: path, bytes: bytes, events: chunk.length }
    end
    fsync_directory(temporary_directory)
    File.rename(temporary_directory, ready_directory)
    committed = true
    fsync_directory(@streaming_temp_directory)
    files.each do |file|
      file[:path] = File.join(ready_directory, File.basename(file[:path]))
    end

    files.each do |file|
      @streaming_metric.increment(:requests)
      @streaming_metric.increment(:events, file[:events])
      @streaming_metric.increment(:bytes, file[:bytes])
      @streaming_metric.increment(:oversized_events) if
        file[:events] == 1 && file[:bytes] > streaming_max_request_bytes.to_i
    end
    files
  rescue
    FileUtils.rm_rf(temporary_directory) if temporary_directory
    FileUtils.rm_rf(ready_directory) if ready_directory && !committed
    @streaming_metric.increment(:failures)
    raise
  end

  private
  def write_streaming_file(path, encoded_events)
    flags = File::WRONLY | File::CREAT | File::EXCL
    flags |= File::NOFOLLOW if defined?(File::NOFOLLOW)
    file = File.new(path, flags, @streaming_file_mode)
    begin
      encoded_events.each { |encoded| file.write(encoded) }
      file.flush
      file.fsync
    ensure
      file.close
    end
  end

  private
  def recover_streaming_files
    Dir.glob(File.join(@streaming_temp_directory, '.batch-*.tmp')).each do |directory|
      @logger.warn('Discarding incomplete streaming spool batch.', path: directory)
      FileUtils.rm_rf(directory)
    end

    Dir.glob(File.join(@streaming_temp_directory, 'batch-*.ready', 'stream-*.json')).sort.each do |file|
      validate_recovered_streaming_file(file)
      @logger.info('Recovering streaming spool file.', path: file)
      @ingestor.upload_async(file, delete_temp_files)
    end
  end

  private
  def validate_recovered_streaming_file(file)
    batch_directory = File.dirname(file)
    batch_stat = File.lstat(batch_directory)
    file_stat = File.lstat(file)
    safe_batch = batch_stat.directory? && !batch_stat.symlink? &&
                 safe_streaming_spool_stat?(batch_stat)
    safe_file = file_stat.file? && !file_stat.symlink? &&
                safe_streaming_spool_stat?(file_stat)
    return if safe_batch && safe_file

    raise LogStash::ConfigurationError,
          "Streaming recovery rejected an unsafe spool file: #{file}"
  end

  private
  def safe_streaming_spool_stat?(stat)
    owner_is_safe = Gem.win_platform? || stat.uid == Process.uid
    owner_is_safe && (stat.mode & 0o022).zero?
  end

  private
  def fsync_directory(directory)
    return if Gem.win_platform?

    File.open(directory, File::RDONLY) { |file| file.fsync }
  rescue SystemCallError => e
    unsupported_errors = [Errno::EINVAL::Errno]
    unsupported_errors << Errno::EISDIR::Errno if defined?(Errno::EISDIR)
    unsupported_errors << Errno::ENOTSUP::Errno if defined?(Errno::ENOTSUP)
    raise unless unsupported_errors.include?(e.errno)

    @logger.debug('Directory fsync is not supported on this platform.', path: directory)
  end

  private
  def acquire_streaming_spool_lock
    lock_path = File.join(@streaming_temp_directory, '.lock')
    if File.symlink?(lock_path)
      raise LogStash::ConfigurationError,
            "Streaming spool lock must not be a symlink: #{lock_path}"
    end

    flags = File::RDWR | File::CREAT
    flags |= File::NOFOLLOW if defined?(File::NOFOLLOW)
    lock_file = File.open(lock_path, flags, @streaming_file_mode)
    File.chmod(@streaming_file_mode, lock_path)
    unless lock_file.flock(File::LOCK_EX | File::LOCK_NB)
      lock_file.close
      raise LogStash::ConfigurationError,
            "Streaming spool directory is already in use: #{@streaming_temp_directory}"
    end

    @streaming_lock_file = lock_file
  end

  private
  def release_streaming_spool_lock
    return if @streaming_lock_file.nil?

    @streaming_lock_file.flock(File::LOCK_UN)
    @streaming_lock_file.close
    @streaming_lock_file = nil
  end
end

# wrapper class
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

  def method_missing(method_name, *args, &block)
    if @io.respond_to?(method_name)

      @io.send(method_name, *args, &block)
    else
      super
    end
  end
  attr_accessor :active
end
