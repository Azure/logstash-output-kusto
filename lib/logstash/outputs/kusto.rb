# encoding: utf-8

require 'logstash/outputs/base'
require 'logstash/namespace'
require 'logstash/errors'

require 'logstash/outputs/kusto/ingestor'
require 'logstash/outputs/kusto/interval'

##
# This plugin sends messages to Azure Kusto in batches.
#
class LogStash::Outputs::Kusto < LogStash::Outputs::Base
  config_name 'kusto'
  concurrency :shared

  # Possessive quantifier (`++`) prevents catastrophic/quadratic backtracking
  # when scanning attacker- or config-supplied strings such as `%{%{%{...`
  # (CodeQL rb/polynomial-redos). Match semantics are identical to `[^}]+`.
  FIELD_REF = /%\{[^}]++\}/

  # Marker appended to the temp file name (after the user-provided path) to
  # carry the per-event routing target when dynamic routing is active.
  # The `~` separators can never appear inside a resolved identifier because
  # IDENTIFIER_PATTERN forbids them, so decoding back to (database, table,
  # mapping) is unambiguous regardless of dots in the user's path pattern.
  ROUTING_MARKER = '.kusto~'

  # Allowlist for dynamically-resolved database / table / mapping identifiers.
  # Restricting to these characters keeps the routing marker unambiguous and
  # prevents path traversal (no '/', '\\', '.' or '~') in the temp file name.
  IDENTIFIER_PATTERN = /\A[A-Za-z0-9_-]+\z/

  # Human-readable description of IDENTIFIER_PATTERN for user-facing messages
  # (config errors, DLQ reasons). Avoids leaking the raw Ruby regexp dump
  # (e.g. `(?-mix:\A...)`) and matches the allowlist documented in the README.
  IDENTIFIER_PATTERN_DESCRIPTION = '[A-Za-z0-9_-]+ (letters, digits, underscore and hyphen only)'

  # Decodes the (database, table, mapping) routing target encoded into a dynamic
  # temp file name by the output. This is the single source of truth shared by
  # the writer side (validating events before they are written) and the ingestor
  # side (resolving the destination at upload time), so the two can never drift.
  #
  # Returns a hash { database:, table:, mapping: } when the marker is present and
  # both database and table are valid identifiers, or nil otherwise. The mapping
  # segment is optional: an empty value or an unresolved field reference
  # (e.g. `%{[@metadata][mapping]}`, left behind when the field is absent) is
  # normalised to nil (route without a mapping), but a mapping that resolved to a
  # genuinely invalid identifier makes the whole target unroutable so the event
  # is dead-lettered instead of being silently ingested with the wrong mapping.
  def self.decode_routing_target(path)
    return nil if path.nil?

    marker_index = path.rindex(ROUTING_MARKER)
    return nil if marker_index.nil?

    encoded = path[(marker_index + ROUTING_MARKER.length)..-1]
    database_value, table_value, mapping_value = encoded.split('~', 3)

    return nil if database_value.nil? || database_value !~ IDENTIFIER_PATTERN
    return nil if table_value.nil? || table_value !~ IDENTIFIER_PATTERN

    if mapping_value.nil? || mapping_value.empty? || mapping_value =~ FIELD_REF
      # Absent or unresolved mapping field reference -> route without a mapping.
      mapping_value = nil
    elsif mapping_value !~ IDENTIFIER_PATTERN
      # Resolved to a genuinely invalid identifier -> unroutable.
      return nil
    end
    { database: database_value, table: table_value, mapping: mapping_value }
  end

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
  config :path, validate: :string, required: true

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
  # Database name. May contain Logstash field references (e.g. `%{[@metadata][db]}`)
  # to route each event to a different database. When a field reference is used,
  # the resolved value must contain only letters, digits, underscore and hyphen.
  config :database, validate: :string, required: true
  # Target table name. May contain Logstash field references (e.g. `%{table}`)
  # to route each event to a different table, subject to the same identifier
  # restrictions as `database`.
  config :table, validate: :string, required: true
  # Mapping name - Used by Kusto to map each attribute from incoming event JSON strings to the appropriate column in the table.
  # Note that this must be in JSON format, as this is the interface between Logstash and Kusto
  # Make this optional as name resolution in the JSON mapping can be done based on attribute names in the incoming event JSON strings
  # May also contain Logstash field references for dynamic routing.
  config :json_mapping, validate: :string, default: nil

  # Mapping name - deprecated, use json_mapping
  config :mapping, validate: :string, deprecated: true


  # Determines if local files used for temporary storage will be deleted
  # after upload is successful
  config :delete_temp_files, validate: :boolean, default: true

  # When true, force dynamic routing on even if `database`, `table` and
  # `json_mapping` contain no field references. Dynamic routing is also enabled
  # automatically whenever any of those values contains a `%{...}` field
  # reference, so setting this flag explicitly is usually unnecessary.
  config :dynamic_event_routing, validate: :boolean, default: false

  # Specify how many files can be uploaded concurrently
  config :upload_concurrent_count, validate: :number, default: 3

  # Specify how many files can be kept in the upload queue before the main process
  # starts processing them in the main thread (not healthy)
  config :upload_queue_size, validate: :number, default: 30

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

    # Dynamic routing is active when the user explicitly opts in, or whenever any
    # of the routing targets contains a Logstash field reference that must be
    # resolved per event.
    @dynamic_routing = dynamic_event_routing ||
                       value_dynamic?(database) ||
                       value_dynamic?(table) ||
                       value_dynamic?(final_mapping)

    # Fail fast on statically-broken dynamic configs: a non-field-reference
    # database/table/mapping is embedded verbatim into the routing marker, so an
    # invalid literal would make every event unroutable at runtime. database and
    # table are required; json_mapping is optional but, when given as a literal,
    # must still be a valid identifier.
    if @dynamic_routing
      validate_dynamic_literal('database', database)
      validate_dynamic_literal('table', table)
      validate_dynamic_literal('json_mapping', final_mapping, optional: true)
    end

    # The temp file name carries the routing target so the ingestor knows where
    # to send each file. In static mode the (constant) database/table are simply
    # appended as before. In dynamic mode we append a tilde-delimited marker
    # built from the resolved field references; the tilde separators cannot
    # collide with a resolved identifier (see IDENTIFIER_PATTERN) so the ingestor
    # can decode it unambiguously even when `path` itself contains dots.
    @path = if @dynamic_routing
              database_ref = field_ref_or_literal(database)
              table_ref = field_ref_or_literal(table)
              mapping_ref = field_ref_or_literal(final_mapping)
              File.expand_path("#{path}#{ROUTING_MARKER}#{database_ref}~#{table_ref}~#{mapping_ref}")
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

    # Cache the native Logstash dead-letter-queue writer (when DLQ is enabled in
    # logstash.yml). In dynamic mode, events that cannot be routed are sent here;
    # when the DLQ is disabled they are dropped (see handle_unroutable_event).
    @dlq_writer = dlq_enabled? ? execution_context.dlq_writer : nil
    if @dynamic_routing
      if @dlq_writer
        @logger.info('Dynamic event routing is enabled. Events that cannot be routed will be sent to the dead letter queue.')
      else
        @logger.warn('Dynamic event routing is enabled but the Logstash dead letter queue is disabled. Events that cannot be routed (e.g. a missing or invalid database/table field) will be DROPPED. Enable the dead letter queue (dead_letter_queue.enable: true in logstash.yml) to capture them.')
      end
    end

    executor = Concurrent::ThreadPoolExecutor.new(min_threads: 1,
                                                  max_threads: upload_concurrent_count,
                                                  max_queue: upload_queue_size,
                                                  fallback_policy: :caller_runs)

    @ingestor = Ingestor.new(ingest_url, app_id, app_key, app_tenant, managed_identity, cli_auth, database, table, final_mapping, @dynamic_routing, delete_temp_files, proxy_host, proxy_port,proxy_protocol, @logger, executor)

    # send existing files
    recover_past_files if recovery

    @last_stale_cleanup_cycle = Time.now

    @flush_interval = @flush_interval.to_i
    if @flush_interval > 0
      @flusher = Interval.start(@flush_interval, -> { flush_pending_files })
    end

    if (@stale_cleanup_type == 'interval') && (@stale_cleanup_interval > 0)
      @cleaner = Interval.start(stale_cleanup_interval, -> { close_stale_files })
    end
  end

  private

  # True when the value contains a Logstash field reference (e.g. `%{field}`).
  def value_dynamic?(value)
    !value.nil? && value =~ FIELD_REF ? true : false
  end

  # Returns the value unchanged when it carries a field reference (so Logstash
  # sprintf resolves it per event), otherwise returns the static literal (or an
  # empty string for nil, e.g. an absent mapping) for inclusion in the file name.
  def field_ref_or_literal(value)
    return '' if value.nil?
    value
  end

  # Validates a static (non-field-reference) database/table/mapping value used in
  # dynamic mode. Such literals are embedded verbatim into the routing marker, so
  # they must match the identifier allowlist; otherwise every event would fail
  # routing at runtime. Required values (database/table) must also be non-empty;
  # an optional value (json_mapping) may be empty/nil (routed without a mapping).
  def validate_dynamic_literal(name, value, optional: false)
    return if value_dynamic?(value) # field reference: validated per event at write time

    if value.nil? || value.empty?
      return if optional
      @logger.error("#{name} must not be empty when dynamic routing is enabled.")
      raise LogStash::ConfigurationError.new("#{name} must not be empty when dynamic routing is enabled.")
    end

    unless value =~ IDENTIFIER_PATTERN
      @logger.error("#{name} static value '#{value}' must match #{IDENTIFIER_PATTERN_DESCRIPTION} when dynamic routing is enabled.")
      raise LogStash::ConfigurationError.new("#{name} static value '#{value}' must match #{IDENTIFIER_PATTERN_DESCRIPTION} when dynamic routing is enabled.")
    end
  end

  # True when Logstash's native dead-letter queue is enabled for this pipeline.
  # Mirrors the approach used by the official elasticsearch output: Logstash
  # hands plugins a "dummy" writer when the DLQ is disabled in logstash.yml.
  # Defensive (rescues and treats DLQ as disabled) since we are a third-party
  # plugin and the internal writer classes are Logstash-version dependent.
  def dlq_enabled?
    return false unless respond_to?(:execution_context) && execution_context.respond_to?(:dlq_writer)

    writer = execution_context.dlq_writer
    return false if writer.nil?

    # When the DLQ is disabled Logstash hands plugins a dummy writer that silently
    # discards everything. Depending on the Logstash version that dummy may be the
    # writer itself or wrapped behind `inner_writer`, so check BOTH. Treating a
    # dummy as "enabled" would route unroutable events into a no-op (silent data
    # loss) instead of the local failure-file fallback, so we are conservative.
    return false if dummy_dlq_writer?(writer)
    return false if writer.respond_to?(:inner_writer) && dummy_dlq_writer?(writer.inner_writer)

    true
  rescue StandardError => e
    @logger.debug('Could not determine DLQ availability; treating DLQ as disabled.', exception: e.class, message: e.message)
    false
  end

  # Detects Logstash's no-op dead-letter-queue writer across versions. Uses a
  # class-name match rather than `is_a?` because the concrete constant differs
  # between Logstash releases and may not be loadable from a third-party plugin.
  private
  def dummy_dlq_writer?(writer)
    return true if writer.nil?
    writer.class.name.to_s.include?('DummyDeadLetterQueueWriter')
  end

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
    encoded_by_path = Hash.new { |h, k| h[k] = [] }
    unroutable_count = 0

    events_and_encoded.each do |event, encoded|
      file_output_path = event_path(event)
      # A nil path means the event was handled out-of-band (sent to the dead
      # letter queue, or dropped when the DLQ is disabled); it is not written to
      # any temp file.
      if file_output_path.nil?
        unroutable_count += 1
        next
      end

      encoded_by_path[file_output_path] << encoded
    end

    log_unroutable_summary(unroutable_count)

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

  # Emits a single aggregated warning per batch summarising how many events could
  # not be routed, instead of one log line per event. This keeps the logs usable
  # under high volume (mirrors the elasticsearch output's batched DLQ summary).
  def log_unroutable_summary(count)
    return if count.zero?

    if @dlq_writer
      @logger.warn("#{count} event(s) in this batch could not be routed to a Kusto target and were sent to the dead letter queue.")
    else
      @logger.warn("#{count} event(s) in this batch could not be routed to a Kusto target and were DROPPED because the dead letter queue is disabled. Enable the Logstash dead letter queue to capture them.")
    end
  end
  private :log_unroutable_summary

  public
  def close
    @flusher.stop unless @flusher.nil?
    @cleaner.stop unless @cleaner.nil?
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

    @ingestor.stop unless @ingestor.nil?
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
      # The event resolved to a path outside the files root. In dynamic mode this
      # is just another unroutable event, so funnel it through the same handler
      # (DLQ / drop) for one coherent policy; in static mode keep the historical
      # behaviour of writing to the failure file.
      return handle_unroutable_event(event, 'tried to write outside the files root') if @dynamic_routing
      @logger.warn('The event tried to write outside the files root, writing the event to the failure file', event: event, filename: @failure_path)
      file_output_path = @failure_path
    elsif @dynamic_routing && !valid_routing_target?(file_output_path)
      return handle_unroutable_event(event, "did not resolve to a valid Kusto routing target (database/table must match #{IDENTIFIER_PATTERN_DESCRIPTION})")
    elsif !@create_if_deleted && deleted?(file_output_path)
      # The temp file was deleted and we are told not to recreate it. In dynamic
      # mode there is no usable failure file (it carries no routing target and
      # cannot be ingested), so treat this as unroutable (DLQ / drop) to keep the
      # invariant that dynamic mode never writes to @failure_path. Static mode
      # keeps the historical failure-file behaviour.
      return handle_unroutable_event(event, 'temporary file was deleted and create_if_deleted is false') if @dynamic_routing
      file_output_path = @failure_path
    end
    @logger.debug('Writing event to tmp file.', filename: file_output_path)

    file_output_path
  end

  # Handles a dynamic event that could not be routed to a Kusto destination.
  # Sends it to Logstash's native dead letter queue when enabled (idiomatic, and
  # replayable via the dead_letter_queue input). When the DLQ is disabled the
  # event is dropped: this matches the elasticsearch output, avoids an unbounded
  # local file, and is surfaced loudly (a startup warning plus the per-batch
  # count) so it is never a silent loss. Always returns nil so the caller writes
  # nothing to disk for this event.
  private
  def handle_unroutable_event(event, reason)
    if @dlq_writer
      @dlq_writer.write(event, "Event could not be routed to Kusto: #{reason}.")
      @logger.debug('Routed unroutable event to the dead letter queue.', event: event, reason: reason)
    else
      @logger.debug('Dropped unroutable event (dead letter queue disabled).', event: event, reason: reason)
    end
    nil
  end

  # Validates the routing target encoded in a dynamically-generated file path.
  # Delegates to the shared decoder so the writer and ingestor sides stay in
  # lock-step. Returns false when database/table did not resolve to valid
  # identifiers (e.g. the field reference was missing, leaving a literal
  # `%{...}`). The mapping segment is optional and not required here.
  private
  def valid_routing_target?(file_output_path)
    !self.class.decode_routing_target(file_output_path).nil?
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
      @logger.info("Going to recover old files in path #{new_path}")

      # In dynamic mode the database/table are not known up-front, so recover
      # any leftover temp file carrying the routing marker. In static mode keep
      # matching the exact `.database.table` suffix as before. Restrict to regular
      # files so a directory whose name happens to match is never sent to ingest.
      old_files = if @dynamic_routing
                    Find.find(new_path).select { |p| File.file?(p) && p.include?(ROUTING_MARKER) }
                  else
                    Find.find(new_path).select { |p| File.file?(p) && /.*\.#{database}\.#{table}$/ =~ p }
                  end
      @logger.info("Found #{old_files.length} old file(s), sending them now...")

      old_files.each do |file|
        kusto_send_file(file)
      end
    rescue Errno::ENOENT => e
      @logger.warn('No such file or directory', exception: e.class, message: e.message, path: new_path, backtrace: e.backtrace)
    end
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
