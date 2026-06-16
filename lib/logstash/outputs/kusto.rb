# encoding: utf-8

require 'logstash/outputs/base'
require 'logstash/namespace'
require 'logstash/errors'
require 'digest'

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
  # carry the per-event routing target when dynamic routing is active. Each of
  # the three target segments (database, table, mapping) is percent-encoded
  # (see encode_routing_segment), so neither the marker nor the `~` separators
  # can ever appear inside a segment and decoding is always unambiguous.
  ROUTING_MARKER = '.kusto~'

  # Tag inserted into the temp file name (immediately before ROUTING_MARKER) to
  # stamp each dynamic temp file with a stable identifier for the output that
  # wrote it (see register). Crash recovery resends only files carrying this
  # output's identifier, so two Kusto outputs sharing a path root can never pick
  # up each other's leftover files. It sits before the marker so the routing
  # segments stay contiguous and decode_routing_target is unaffected.
  ROUTING_OWNER_MARKER = '.kustoid-'

  # Characters left as-is in an encoded routing segment. Everything else
  # (including '.', ' ', '~', '/', '\\' and '%') is percent-encoded, which keeps
  # the encoded segment free of path separators and of the marker/separator
  # characters, so the file name is always safe and unambiguous to decode.
  ROUTING_SEGMENT_UNSAFE = /[^A-Za-z0-9_-]/n

  # Acceptable resolved database / table / mapping value (after decoding). This
  # follows Azure Data Explorer entity-naming: letters and digits (including
  # non-ASCII), spaces, dots, dashes and underscores. Values outside this set
  # (e.g. containing path separators) are treated as unroutable.
  ROUTING_VALUE_PATTERN = /\A[[:alnum:] ._-]+\z/

  # Human-readable description of ROUTING_VALUE_PATTERN for user-facing messages.
  ROUTING_VALUE_DESCRIPTION = 'letters, digits, spaces, dots, dashes and underscores'

  # Maximum length (in characters) of a resolved database / table / mapping
  # value. Azure Data Explorer entity names are limited to 1-1024 characters, so
  # an overlong value is rejected up front (at register time for static literals,
  # or as unroutable at decode time) instead of failing later on the ADX side.
  ROUTING_VALUE_MAX_LENGTH = 1024

  # Percent-encodes a resolved routing value so it can be embedded as one segment
  # of the routing marker in a temp file name. Operates on bytes, so any value
  # (including non-ASCII and otherwise unsafe characters) round-trips exactly
  # through decode_routing_segment.
  def self.encode_routing_segment(value)
    return '' if value.nil?
    value.to_s.b.gsub(ROUTING_SEGMENT_UNSAFE) { |byte| format('%%%02X', byte.ord) }
  end

  # Reverses encode_routing_segment. Returns the decoded UTF-8 string, or nil if
  # the bytes do not form valid UTF-8 (a corrupt or foreign file name).
  def self.decode_routing_segment(value)
    return '' if value.nil? || value.empty?
    decoded = value.to_s.b.gsub(/%([0-9A-Fa-f]{2})/n) { Regexp.last_match(1).hex.chr }.force_encoding('UTF-8')
    decoded.valid_encoding? ? decoded : nil
  end

  # Decodes the (database, table, mapping) routing target encoded into a dynamic
  # temp file name by the output. This is the single source of truth shared by
  # the writer side (validating events before they are written) and the ingestor
  # side (resolving the destination at upload time), so the two can never drift.
  #
  # Returns a hash { database:, table:, mapping: } when the marker is present and
  # both database and table decode to valid values, or nil otherwise. The mapping
  # segment is optional: an empty value or an unresolved field reference
  # (e.g. `%{[@metadata][mapping]}`, left behind when the field is absent) is
  # normalised to nil (route without a mapping), while a mapping that decoded to a
  # genuinely invalid value makes the whole target unroutable so the event is not
  # silently ingested with the wrong mapping.
  def self.decode_routing_target(path)
    return nil if path.nil?

    marker_index = path.rindex(ROUTING_MARKER)
    return nil if marker_index.nil?

    encoded = path[(marker_index + ROUTING_MARKER.length)..-1]
    database_enc, table_enc, mapping_enc = encoded.split('~', 3)

    database = decode_routing_segment(database_enc)
    table = decode_routing_segment(table_enc)
    return nil if unresolved_or_invalid_routing_value?(database)
    return nil if unresolved_or_invalid_routing_value?(table)

    mapping = decode_routing_segment(mapping_enc)
    if mapping.nil? || mapping.empty? || mapping =~ FIELD_REF
      # Absent or unresolved mapping field reference -> route without a mapping.
      mapping = nil
    elsif mapping !~ ROUTING_VALUE_PATTERN || mapping.length > ROUTING_VALUE_MAX_LENGTH
      # Decoded to a genuinely invalid (or overlong) value -> unroutable.
      return nil
    end
    { database: database, table: table, mapping: mapping }
  end

  # True when a decoded database/table value is missing, empty, an unresolved
  # field reference (the event lacked the field), longer than
  # ROUTING_VALUE_MAX_LENGTH, or outside ROUTING_VALUE_PATTERN.
  def self.unresolved_or_invalid_routing_value?(value)
    return true if value.nil? || value.empty?
    return true if value =~ FIELD_REF
    return true if value.length > ROUTING_VALUE_MAX_LENGTH
    value !~ ROUTING_VALUE_PATTERN
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
  # 0 will flush on every message. Increase this value to reduce IO calls but keep 
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

  # The following are the credentials used to connect to the Kusto service
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
  # to route each event to a different database. A resolved value may contain
  # letters, digits, spaces, dots, dashes and underscores.
  config :database, validate: :string, required: true
  # Target table name. May contain Logstash field references (e.g. `%{table}`)
  # to route each event to a different table, subject to the same value
  # restrictions as `database`.
  config :table, validate: :string, required: true
  # Name of a JSON ingestion mapping already defined on the target table. This is
  # the mapping's reference/name, NOT the mapping JSON itself. Optional: when it
  # is omitted, columns are resolved by the attribute names in the incoming event
  # JSON. May also contain Logstash field references for dynamic routing.
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

  # Logs a warning when dynamic routing is holding at least this many temporary
  # files open at once, as an early signal of high routing cardinality (each
  # distinct time-window x database x table x mapping keeps its own open file and
  # produces its own small ingestion calls). The warning is emitted once until the
  # count drops back below the threshold. Only applies in dynamic mode; set to 0
  # to disable.
  config :dynamic_routing_open_files_warning_threshold, validate: :number, default: 100

  # Specify how many files can be uploaded concurrently
  config :upload_concurrent_count, validate: :number, default: 3

  # Specify how many files can be kept in the upload queue before the main process
  # starts processing them in the main thread (not healthy)
  config :upload_queue_size, validate: :number, default: 30

  # Host of the proxy , is an optional field. Can connect directly
  config :proxy_host, validate: :string, required: false

  # Port where the proxy runs , defaults to 80. Usually a value like 3128
  config :proxy_port, validate: :number, required: false , default: 80

  # Proxy server protocol, one of `http` or `https`. Defaults to `http`.
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
    # database/table is the routing target for every event, so an empty or
    # invalid literal would make every event unroutable at runtime. database and
    # table are required; json_mapping is optional but, when given as a literal,
    # must still be a valid value.
    if @dynamic_routing
      validate_dynamic_literal('database', database)
      validate_dynamic_literal('table', table)
      validate_dynamic_literal('json_mapping', final_mapping, optional: true)
    end

    # The temp file name carries the routing target so the ingestor knows where
    # to send each file. In static mode the (constant) database/table are simply
    # appended as before. In dynamic mode `@path` holds only the user path
    # (resolved per event for time-based rotation); the routing marker is built
    # and percent-encoded per event in generate_filepath, so the routing values
    # are kept verbatim (they are not run through File.expand_path).
    if @dynamic_routing
      @routing_database = database
      @routing_table = table
      @routing_mapping = final_mapping
      @path = File.expand_path(path)
      # Stamp every dynamic temp file with a stable identifier for this output so
      # crash recovery only resends files this output wrote (see recover_past_files).
      # The identifier is derived from the settings that define where this output
      # sends data, so it is stable across restarts yet differs from any other
      # output, even one sharing the same path root.
      @routing_owner_tag = "#{ROUTING_OWNER_MARKER}#{routing_owner_id(final_mapping)}"
    else
      @path = File.expand_path("#{path}.#{database}.#{table}")
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

    # Early-warning latch for high routing cardinality (dynamic mode only; 0 off).
    @open_files_warning_threshold = @dynamic_routing ? dynamic_routing_open_files_warning_threshold : 0
    @open_files_warning_active = false

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

  # Stable per-output identifier embedded in dynamic temp file names so crash
  # recovery only picks up files this output wrote. Derived from the settings that
  # determine where this output sends data (endpoint + routing config + path), so
  # it stays constant across restarts of the same configuration yet differs from
  # any other output. Returns a short hex digest: filename-safe and free of the
  # marker / separator characters.
  def routing_owner_id(final_mapping)
    identity = [ingest_url, path, database, table, (final_mapping || '')].join("\u0000")
    Digest::SHA256.hexdigest(identity)[0, 16]
  end

  # Validates a static (non-field-reference) database/table/mapping value used in
  # dynamic mode. Such literals are the routing target for every event, so an
  # invalid value would make every event unroutable at runtime. Required values
  # (database/table) must be non-empty; an optional value (json_mapping) may be
  # empty/nil (routed without a mapping). Field references are validated per
  # event at write time, so they are skipped here.
  def validate_dynamic_literal(name, value, optional: false)
    return if value_dynamic?(value)

    if value.nil? || value.empty?
      return if optional
      @logger.error("#{name} must not be empty when dynamic routing is enabled.")
      raise LogStash::ConfigurationError.new("#{name} must not be empty when dynamic routing is enabled.")
    end

    unless value =~ ROUTING_VALUE_PATTERN
      @logger.error("#{name} static value '#{value}' must contain only #{ROUTING_VALUE_DESCRIPTION} when dynamic routing is enabled.")
      raise LogStash::ConfigurationError.new("#{name} static value '#{value}' must contain only #{ROUTING_VALUE_DESCRIPTION} when dynamic routing is enabled.")
    end

    if value.length > ROUTING_VALUE_MAX_LENGTH
      @logger.error("#{name} static value is #{value.length} characters; it must be #{ROUTING_VALUE_MAX_LENGTH} characters or fewer when dynamic routing is enabled.")
      raise LogStash::ConfigurationError.new("#{name} static value is #{value.length} characters; it must be #{ROUTING_VALUE_MAX_LENGTH} characters or fewer when dynamic routing is enabled.")
    end
  end

  # True when Logstash's native dead-letter queue is enabled for this pipeline.
  # When the DLQ is disabled Logstash hands plugins a "dummy" no-op writer. This
  # is defensive (rescues and treats the DLQ as disabled) because the internal
  # writer classes vary by Logstash version and may not be loadable here.
  def dlq_enabled?
    return false unless respond_to?(:execution_context) && execution_context.respond_to?(:dlq_writer)

    writer = execution_context.dlq_writer
    return false if writer.nil?

    # When the DLQ is disabled Logstash hands plugins a dummy writer that silently
    # discards everything. Depending on the Logstash version that dummy may be the
    # writer itself or wrapped behind `inner_writer`, so check BOTH. Treating a
    # dummy as "enabled" would report events as DLQ-routed when they would in fact
    # be discarded, bypassing the plugin's explicit drop-with-warning policy, so we
    # are conservative and treat any dummy as disabled.
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

      # Close any files that went stale in previous batches first, then warn on
      # the files still open afterwards, so the high-cardinality signal reflects
      # the genuinely-carried set rather than files about to be closed this batch.
      close_stale_files if @stale_cleanup_type == 'events'
      warn_if_too_many_open_files
    end
  end

  # Emits a single aggregated warning per batch summarising how many events could
  # not be routed, instead of one log line per event, to keep the logs usable
  # under high volume.
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
    # Expand both sides and normalise any backslashes to '/' so the containment
    # check is independent of separator style (Windows/JRuby may produce either)
    # and of '.'/'..' segments. The path is inside the root when it is the root
    # itself or sits beneath it.
    target_file = File.expand_path(log_path).tr('\\', '/')
    root = File.expand_path(@file_root).tr('\\', '/')
    target_file == root || target_file.start_with?("#{root}/")
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
      return handle_unroutable_event(event, "did not resolve to a valid Kusto routing target (database/table must contain only #{ROUTING_VALUE_DESCRIPTION})")
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
  # Sends it to Logstash's native dead letter queue when enabled (where it can be
  # inspected and replayed). When the DLQ is disabled the event is dropped to
  # avoid an unbounded local file; the drop is surfaced loudly (a startup warning
  # plus the per-batch count) so it is never a silent loss. Always returns nil so
  # the caller writes nothing to disk for this event.
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
    return event.sprintf(@path) unless @dynamic_routing

    # Resolve the user path (for time-based rotation) and each routing target
    # separately, then percent-encode each target so the file name is safe and
    # the marker decodes unambiguously. Unresolved field references survive as a
    # literal `%{...}` and are rejected later by decode_routing_target.
    prefix = event.sprintf(@path)
    database = self.class.encode_routing_segment(event.sprintf(@routing_database))
    table = self.class.encode_routing_segment(event.sprintf(@routing_table))
    mapping = self.class.encode_routing_segment(@routing_mapping.nil? ? '' : event.sprintf(@routing_mapping))
    # @routing_owner_tag (before the marker) stamps the file as ours for recovery;
    # the marker and its encoded segments stay contiguous so decoding is unchanged.
    "#{prefix}#{@routing_owner_tag}#{ROUTING_MARKER}#{database}~#{table}~#{mapping}"
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

  # Logs a warning (once, until the count recovers) when dynamic routing is
  # holding many temp files open at the same time — an early signal of high
  # routing cardinality (many open file descriptors and many small ingestion
  # calls). Controlled by dynamic_routing_open_files_warning_threshold; a value of
  # 0 (or static mode) disables it.
  private
  def warn_if_too_many_open_files
    return if @open_files_warning_threshold.nil? || @open_files_warning_threshold <= 0

    open_count = @files.size
    if open_count >= @open_files_warning_threshold
      unless @open_files_warning_active
        @open_files_warning_active = true
        @logger.warn("Dynamic routing currently has #{open_count} temporary files open (threshold #{@open_files_warning_threshold}). High routing cardinality increases open file descriptors and produces many small ingestion calls; consider reducing the number of distinct database/table/mapping targets or increasing flush_interval/stale_cleanup_interval.", open_files: open_count, threshold: @open_files_warning_threshold)
      end
    else
      @open_files_warning_active = false
    end
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
    @files[path] = IOWriter.new(fd)
  end

  private
  def kusto_send_file(file_path)
    @ingestor.upload_async(file_path, delete_temp_files)
  end

  private
  def recover_past_files
    require 'find'

    new_path = recovery_scan_dir

    begin
      return unless Dir.exist?(new_path)
      @logger.info("Going to recover old files in path #{new_path}")

      # In dynamic mode the database/table are not known up-front, so recover any
      # leftover temp file stamped with this output's owner tag (see register). In
      # static mode keep matching the exact `.database.table` suffix as before;
      # database/table are Regexp.escaped so values with metacharacters (e.g. dots)
      # match literally. Restrict to regular files so a directory whose name
      # happens to match is never sent to ingest.
      old_files = if @dynamic_routing
                    # Only resend files this output wrote (owner-stamped), so a
                    # shared path root never causes one output to pick up another's
                    # leftover file (possibly bound for a different cluster/table).
                    Find.find(new_path).select { |p| File.file?(p) && dynamic_temp_file_owned_by_this_output?(p) }
                  else
                    suffix = /\.#{Regexp.escape(database)}\.#{Regexp.escape(table)}\z/
                    Find.find(new_path).select { |p| File.file?(p) && p =~ suffix }
                  end
      @logger.info("Found #{old_files.length} old file(s), sending them now...")

      old_files.each do |file|
        kusto_send_file(file)
      end
    rescue Errno::ENOENT => e
      @logger.warn('No such file or directory', exception: e.class, message: e.message, path: new_path, backtrace: e.backtrace)
    end
  end

  # Computes the directory to scan for leftover temp files on startup: the fixed
  # portion of the (already expanded) @path up to the first dynamic field. Both
  # the index and the slice are taken from @path so relative configured paths
  # resolve correctly (slicing the raw `path` here left `%{...}` in the result
  # and broke recovery for relative paths).
  private
  def recovery_scan_dir
    path_last_char = @path.length - 1
    pattern_start = @path.index('%') || path_last_char
    last_folder_before_pattern = @path.rindex('/', pattern_start) || path_last_char
    @path[0..last_folder_before_pattern]
  end

  # True when `path` is a dynamic temp file written by THIS output. It must carry
  # this output's owner tag immediately followed by the routing marker, exactly
  # as generate_filepath emits it. Requiring the full owner-tag + marker shape
  # (not merely the tag substring) means a stray file that just happens to
  # contain the tag is never queued for ingest — and so never deleted by the
  # ingestor as an invalid routing file.
  private
  def dynamic_temp_file_owned_by_this_output?(path)
    path.include?("#{@routing_owner_tag}#{ROUTING_MARKER}")
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
