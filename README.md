# Logstash Output Plugin for Azure Data Explorer (Kusto)

![build](https://github.com/Azure/logstash-output-kusto/workflows/build/badge.svg)
![build](https://github.com/Azure/logstash-output-kusto/workflows/build/badge.svg?branch=master)
[![Gem](https://img.shields.io/gem/v/logstash-output-kusto.svg)](https://rubygems.org/gems/logstash-output-kusto)
[![Gem](https://img.shields.io/gem/dt/logstash-output-kusto.svg)](https://rubygems.org/gems/logstash-output-kusto)

This is a plugin for [Logstash](https://github.com/elastic/logstash).

It is fully free and open source. The license is Apache 2.0.

This Azure Data Explorer (ADX) Logstash plugin enables you to process events from Logstash into an **Azure Data Explorer** database for later analysis. 

This connector forwards data to
[Azure Data Explorer](https://docs.microsoft.com/en-us/azure/data-explorer),
[Azure Synapse Data Explorer](https://docs.microsoft.com/en-us/azure/synapse-analytics/data-explorer/data-explorer-overview) and
[Real time analytics in Fabric](https://learn.microsoft.com/en-us/fabric/real-time-analytics/overview)

## Requirements

- Logstash version 8.7+. [Installation instructions](https://www.elastic.co/guide/en/logstash/current/installing-logstash.html) 
- Azure Data Explorer cluster with a database. Read [Create a cluster and database](https://docs.microsoft.com/en-us/azure/data-explorer/create-cluster-database-portal) for more information.
- AAD Application credentials with permission to ingest data into Azure Data Explorer. Read [Creating an AAD Application](https://docs.microsoft.com/en-us/azure/kusto/management/access-control/how-to-provision-aad-app) for more information.

## Installation

To make the Azure Data Explorer plugin available in your Logstash environment, run the following command:
```sh
bin/logstash-plugin install logstash-output-kusto
```

## Configuration

Perform configuration before sending events from Logstash to Azure Data Explorer. The following example shows the minimum you need to provide. It should be enough for most use-cases:

```ruby
output {
	kusto {
            path => "/tmp/kusto/%{+YYYY-MM-dd-HH-mm}.txt"
            ingest_url => "https://ingest-<cluster-name>.kusto.windows.net/"
            app_id => "<application id>"
            app_key => "<application key/secret>"
            app_tenant => "<tenant id>"
            database => "<database name>"
            table => "<target table>"
            json_mapping => "<mapping name>"
            proxy_host => "<proxy host>"
            proxy_port => <proxy port>
            proxy_protocol => <"http"|"https">              
	}
}
```
More information about configuring Logstash can be found in the [logstash configuration guide](https://www.elastic.co/guide/en/logstash/current/configuration.html)

### Streaming ingestion

Queued ingestion remains the default and is recommended for high-throughput workloads. For lower ingestion latency, enable streaming mode:

```ruby
output {
  kusto {
    ingestion_mode => "streaming"
    streaming_max_request_bytes => 1048576
    streaming_temp_directory => "/var/lib/logstash/kusto-streaming"
    ingest_url => "https://ingest-<cluster-name>.kusto.windows.net/"
    app_id => "<application id>"
    app_key => "<application key/secret>"
    app_tenant => "<tenant id>"
    database => "<database name>"
    table => "<target table>"
    json_mapping => "<mapping name>"
  }
}
```

Streaming ingestion must be enabled on the target Kusto table. The connector groups encoded events into requests of at most 1 MiB by default, measured in bytes. It never splits one event: an event above the configured target is sent intact, and the Kusto client decides whether to stream it or fall back to queued ingestion.

Before acknowledging a Logstash batch, the connector writes all requests into an atomic local spool batch. Accepted requests are removed, transient failures apply backpressure and retry with interruptible exponential delays, and final non-success statuses are quarantined for investigation without replaying the entire request.

Streaming delivery is **at least once**. A process or host failure after Kusto accepts a request but before the local spool file is removed can cause that request to be recovered and ingested again. The stable source identifier is used for request tracking; it does not provide Kusto-side row deduplication.

The SDK's `Queued` result means the request was transferred to queued ingestion, not that ingestion into the table has completed. Continue monitoring Kusto ingestion failures for later mapping, schema, encoding-policy, or row-size failures.

The default spool is a destination-specific directory under Logstash `path.data`. Streaming mode defaults to private `0700` directories and `0600` files, rejects symlinked or untrusted recovery files, and allows only one active plugin instance per spool. When setting a custom directory, place it on persistent local storage with trusted parent directories. Group- or world-writable `dir_mode` and `file_mode` values are rejected.

### Available Configuration Keys

| Parameter Name | Description | Notes |
| --- | --- | --- |
| **path** | The plugin writes events to temporary files before sending them to ADX. This parameter includes a path where files should be written and a time expression for file rotation to trigger an upload to the ADX service. The example above shows how to rotate the files every minute and check the Logstash docs for more information on time expressions. | Required for queued ingestion |
| **ingest_url** | The Kusto endpoint for ingestion-related communication. See it on the Azure Portal.| Required|
| **app_id, app_key, app_tenant**| Credentials required to connect to the ADX service. Be sure to use an application with 'ingest' privileges. | Optional|
| **managed_identity**| Managed Identity to authenticate. For user-based managed ID, use the Client ID GUID. For system-based, use the value `system`. The ID needs to have 'ingest' privileges on the cluster. | Optional|
| **database**| Database name to place events. Supports Logstash field references (e.g. `%{[@metadata][database]}`) for dynamic routing. | Required |
| **table** | Target table name to place events. Supports Logstash field references (e.g. `%{[@metadata][table]}`) for dynamic routing. | Required |
| **json_mapping** | The **name** of a JSON ingestion mapping already defined on the target table (a mapping reference/name, not the mapping JSON itself). When omitted, columns are resolved by attribute names in the event JSON. Supports Logstash field references for dynamic routing. | Optional |
| **dynamic_event_routing** | Forces dynamic routing even when `database`/`table`/`json_mapping` contain no field reference. Dynamic routing is enabled automatically whenever any of those values contains a `%{...}` field reference, so this flag is usually not needed. Defaults to false. | Optional |
| **dynamic_routing_open_files_warning_threshold** | In dynamic mode, logs a warning when this many temporary files are held open at once, as an early signal of high routing cardinality. Emitted once until the count drops back below the threshold. Defaults to 100; set to 0 to disable. | Optional |
| **dynamic_routing_max_open_files** | In dynamic mode, an optional hard cap on the number of temporary files held open at once. When the cap is reached, events whose route would open another file are sent to the dead letter queue (or dropped, with a warning, when it is disabled) instead of risking file-descriptor exhaustion (`EMFILE`). Defaults to 0 (no cap). **Recommended production hardening:** set this below the process descriptor limit (`ulimit -n`) and enable the dead letter queue so capped events are captured. | Optional |
| **recovery_owner_id** | In dynamic mode, an optional stable identifier that participates in the per-output crash-recovery owner tag. Two outputs identical in `ingest_url`/`path`/`database`/`table`/`json_mapping` otherwise share recovery files; set a distinct `recovery_owner_id` on each (e.g. when they differ only by credentials or a pipeline conditional) to keep their crash recovery separate without using different `path` roots. Logstash's auto-generated `id` is deliberately not used because it changes between runs and would break recovery. | Optional |
| **recovery** | If set to true (default), plugin will attempt to resend pre-existing temp files upon startup. With streaming, disabling recovery can leave committed spool files unprocessed after restart. | |
| **delete_temp_files** | Determines if temp files will be deleted after a successful upload (true is default; set false for debug purposes only)| |
| **flush_interval** | The time (in seconds) for flushing writes to temporary files. Default is 2 seconds, 0 will flush on every event. Increase this value to reduce IO calls but keep in mind that events in the buffer will be lost in case of abrupt failure.| |
| **dir_mode** | Directory permissions. Streaming defaults to `0700` and rejects group- or world-writable values. | |
| **file_mode** | Temporary-file permissions. Streaming defaults to `0600` and rejects group- or world-writable values. | |
| **proxy_host** | The proxy hostname for redirecting traffic to Kusto.| |
| **proxy_port** | The proxy port for the proxy. Defaults to 80.| |
| **proxy_protocol** | The proxy server protocol , is one of http or https.| |
| **ingestion_mode** | `queued` for throughput-oriented ingestion or `streaming` for low-latency ingestion with automatic queued fallback. | Defaults to `queued` |
| **streaming_max_request_bytes** | Target maximum encoded bytes per streaming request. A single event is never split. | Defaults to 1048576 |
| **streaming_max_retry_attempts** | Transient retries before an interruptible cooldown cycle. During an outage, workers remain bounded and backpressure the Logstash pipeline instead of creating an unbounded retry queue. | Defaults to 2 |
| **streaming_retry_backoff_seconds** | Initial delay for connector retries; later retries use exponential backoff. | Defaults to 1 |
| **streaming_concurrent_requests** | Maximum streaming upload worker count. | Defaults to 4 |
| **streaming_temp_directory** | Durable local spool for streaming requests and restart recovery. Only one active output may use a directory. | Defaults below Logstash `path.data/plugins/logstash-output-kusto` |

> Note : LS_JAVA_OPTS can be used to set proxy parameters as well (using export or SET options)

```bash
export  LS_JAVA_OPTS="-Dhttp.proxyHost=1.2.34 -Dhttp.proxyPort=8989 -Dhttps.proxyHost=1.2.3.4 -Dhttps.proxyPort=8989"
```

### Dynamic event routing

Dynamic routing is supported only with `ingestion_mode => "queued"` (the default).
Combining automatic or explicitly enabled dynamic routing with streaming ingestion
is rejected at startup; streaming destinations must be static.

You can route each event to a different database, table and/or JSON mapping by
using Logstash field references in the `database`, `table` or `json_mapping`
settings. This lets a single output block send events to multiple Azure Data
Explorer tables based on event content.

```ruby
filter {
	mutate { add_field => { "[@metadata][table]" => "%{[app]}_%{[event_type]}" } }
}

output {
	kusto {
            path         => "/tmp/kusto/%{+YYYY-MM-dd-HH-mm}.txt"
            ingest_url   => "https://ingest-<cluster-name>.kusto.windows.net/"
            app_id       => "<application id>"
            app_key      => "<application key/secret>"
            app_tenant   => "<tenant id>"
            database     => "<database name>"
            table        => "%{[@metadata][table]}"   # dynamic routing
            json_mapping => "<mapping name>"
	}
}
```

Notes and caveats:

- Dynamic routing turns on automatically when any of `database`, `table` or
  `json_mapping` contains a `%{...}` field reference. You can also force it on
  with `dynamic_event_routing => true`.
- Resolved `database`, `table` and `json_mapping` values may contain letters,
  digits, spaces, dots, dashes and underscores — the common Azure Data Explorer
  entity-naming characters (e.g. `Security.Events`, `App Logs`). The value is
  reversibly encoded into the temp file name, preserving case, dots and spaces.
  Whitespace-only names and invalid UTF-8 are rejected. Uppercase ASCII letters
  are encoded too, so case-distinct tables remain distinct on Windows.
  Values containing other characters (for example a path separator `/`) are
  treated as unroutable. **By design, dynamic mode is stricter than legacy static
  mode:** in pure legacy static mode (no dynamic routing active) a
  `database`/`table`/`json_mapping` literal is passed through to Azure Data
  Explorer as-is, whereas once dynamic routing is active a per-event resolved
  value is validated against this character/length format *before* upload and
  treated as unroutable if it does not match. Note that a static literal used
  *alongside* dynamic routing (including when forced with
  `dynamic_event_routing => true`) is also validated against this format, but at
  startup — the plugin fails fast rather than treating it as unroutable.
- **Length / filename budget.** Azure Data Explorer entity names may be up to
  1024 characters, but in dynamic mode the resolved `database`, `table` and
  `json_mapping` are encoded together into a single temp **file name**. The
  plugin enforces a conservative 255-byte basename budget, including the path
  prefix, owner tag, separators, and a 38-byte generation token. Uppercase ASCII
  costs three encoded bytes; non-ASCII characters can cost more (`é` → 6 bytes).
  The remaining budget is shared by all three encoded values, not 255 bytes per
  field. Over-budget events go to the DLQ or are dropped with a specific reason.
  Filesystems with stricter component/full-path limits can still raise storage
  errors; those errors are propagated rather than silently dropping the batch.
- **File lifetime.** Each active route/time-window writer uses an exclusively
  created, unique physical file. Once closed for upload, its path is never reused
  by a later writer. This prevents a late event from appending to a file already
  being uploaded, including during cap-driven cleanup or restart recovery.
  Static queued filenames and streaming spooling are unchanged.
- Events that cannot be routed — because the referenced field is missing or the
  resolved value is invalid — are **not** ingested into an unintended table.
  When Logstash's
  [Dead Letter Queue](https://www.elastic.co/guide/en/logstash/current/dead-letter-queues.html)
  is enabled in `logstash.yml`, such events are sent there (where they can be
  inspected and replayed via the `dead_letter_queue` input). When the DLQ is
  **disabled, unroutable events are dropped**, which avoids an unbounded local
  file. The drop is never silent: the plugin logs a warning at startup and a
  per-batch count of dropped events. **For production, enable the dead letter
  queue** so unroutable events are captured.
- A persistent per-batch "could not be routed" warning usually means an upstream
  filter is not setting the routing field — fix the pipeline producing the events.
- If a static `database`/`table` is combined with dynamic routing, it is
  validated at startup and the plugin fails fast on an empty or invalid value.
- The `json_mapping` reference is optional: an empty value or a missing field in
  an exact single reference (for example `%{[@metadata][mapping]}`) routes using
  `database`/`table` and maps columns by attribute name. An unresolved composite
  such as `prefix_%{missing}`, a blank name, or invalid encoding is unroutable.
  For an exact single-reference template, missing, null and empty mapping fields
  share the same writer, open-file cap slot and filename budget for the same
  resolved path, database and table. A missing mapping alone does not make an
  event unroutable; other routing checks still apply. If a mapping is required
  for a table, make sure the field is always set upstream.
- Crash recovery scans the temp-file root for leftover files to resend on
  startup. Each dynamic temp file is stamped with a stable identifier derived
  from this output's `ingest_url`, `database`, `table`, `json_mapping` and
  `path`, and recovery only resends files carrying **this** output's identifier.
  This keeps outputs with **different** routing configuration from picking up
  each other's leftover files even when they share the same `path` root. Two
  outputs that are identical in all of those settings (for example differing only
  by credentials, or selected by different upstream pipeline conditionals) share
  the same identifier; set a distinct `recovery_owner_id` on each (or give them
  distinct `path` roots) if they must not recover each other's files. (Changing
  any of those settings also changes the identifier, so temp files written under
  a previous configuration are not auto-recovered; reprocess them with the old
  configuration or resend manually.)
- Use only one active instance per recovery owner, and keep the spool root on
  trusted local storage. Owner tags isolate configuration; they are not process
  locks. Invalid owned files are logged and left untouched for manual recovery,
  not uploaded or deleted. Symlinked dynamic recovery files are not uploaded.
  Both older owner-stamped dynamic filenames and new generations can be recovered.
- **Upgrade caveat (static → dynamic).** Dynamic recovery only resends temp files
  carrying this output's dynamic owner tag; legacy static temp files use a
  `.database.table` suffix instead. If you switch an existing output from static
  to dynamic routing while static temp files are still on disk (for example a
  deploy during a backlog), those leftover static files are **not** auto-recovered
  by the now-dynamic output. Drain the pipeline before switching, or briefly
  redeploy the previous static configuration to flush them, or resend them
  manually.
- **Routing only validates the *format* of the target, not its *existence*.** A
  syntactically valid but non-existent (e.g. mistyped) `database`/`table`/`json_mapping`
  passes local validation. Upload failures may be retried by Logstash; failures
  after queued submission surface **inside Azure Data Explorer** (visible via
  `.show ingestion failures`). Double-check routing values against existing ADX
  objects. Successful queued submission is not confirmation of table ingestion.
- Dynamic routing does not add exactly-once delivery or power-loss durability.
  Queued buffering/retry behavior is unchanged: partial batches can be duplicated
  after a failure, buffered writes can be lost on abrupt termination, and an
  outage can delay shutdown. Files retained with `delete_temp_files => false`
  remain eligible for recovery; use that setting only for debugging.
- **High-cardinality routing has an operational cost.** Dynamic mode keeps one
  open temporary file per distinct *(time window × database × table × mapping)*
  combination, so routing to many destinations means many concurrent file
  descriptors (watch the OS `ulimit -n`) and many small ingestion calls. ADX
  prefers batched ingestion, so rely on the server-side
  [IngestionBatching policy](https://learn.microsoft.com/azure/data-explorer/kusto/management/batchingpolicy)
  and tune `stale_cleanup_interval` rather than routing to an unbounded number of
  tables per pipeline. `flush_interval` only flushes buffers; it does not close
  files or initiate upload. For idle/finite inputs, use
  `stale_cleanup_type => "interval"` with a positive `stale_cleanup_interval`,
  since the default `"events"` cleanup runs only when events arrive.
  As an early signal, the plugin logs a
  warning when the number of open temporary files crosses
  `dynamic_routing_open_files_warning_threshold` (default 100). For a hard limit,
  set `dynamic_routing_max_open_files` (default 0 = no cap): once that many temp
  files are open, events whose route would open another are sent to the dead
  letter queue (or dropped, with a warning, when it is disabled) instead of
  risking file-descriptor exhaustion. The cap is **off by default** (warning
  only), because a default cap combined with the default-disabled dead letter
  queue would silently drop events for a legitimately high-cardinality pipeline.
  **For production, setting `dynamic_routing_max_open_files` together with an
  enabled dead letter queue is recommended hardening:** keep the cap below the
  process descriptor limit (`ulimit -n`, leaving headroom for other
  inputs/outputs) so capped events are captured in the DLQ rather than risking
  file-descriptor exhaustion. The cap and warning threshold must be finite,
  nonnegative integers (0 disables them).


### Release Notes and versions

| Version | Release Date | Notes |
| --- | --- | --- |
| 2.3.0 | Unreleased | Dynamic database/table/mapping routing for queued ingestion, unique per-writer files, case-safe filenames, per-output recovery, invalid-route DLQ handling, and optional open-file limits. Addresses [#92](https://github.com/Azure/logstash-output-kusto/issues/92) and [#3](https://github.com/Azure/logstash-output-kusto/issues/3). See the dynamic routing section for limits and recovery caveats. |
| 2.2.0 | 2026-07-16 | - Add opt-in Kusto streaming ingestion with byte-bounded requests, automatic queued fallback, durable restart recovery, bounded backpressure, secure local spooling, streaming metrics, and production stress coverage. Queued ingestion remains the default. |
| 2.0.8 | 2024-10-23 | - Fix library deprecations, fix issues in the Azure Identity library  |
| 2.0.7 | 2024-01-01 | - Update Kusto JAVA SDK  |
| 2.0.3 | 2023-12-12 | - Make JSON mapping field optional. If not provided logstash output JSON attribute names will be used for column resolution  |
| 2.0.2 | 2023-11-28 | - Bugfix for the scenario where the plugin uses managed identity. Instead of providing the managed identity name as empty in the config,it can completely be skipped  |
| 2.0.0 | 2023-09-19 | - Upgrade to the latest Java SDK version [5.0.2](https://github.com/Azure/azure-kusto-java/releases/tag/v5.0.2). Tests have been performed on **__Logstash 8.5__**  and up (Does not work with 6.x or 7.x versions of Logstash - For these versions use 1.x.x versions of logstash-output-kusto gem) - Fixes CVE's in common-text & outdated Jackson libraries  |
| 1.0.6 | 2022-11-29 | - Upgrade to the latest Java SDK [3.2.1](https://github.com/Azure/azure-kusto-java/releases/tag/v3.2.1) version. Tests have been performed on Logstash 6.x and up.|


## Development Requirements

- Logstash 8.7+ and its compatible JRuby runtime (MRI Ruby is not supported).
- A 64-bit JDK supported by Logstash; the Gradle wrapper requires Java 17+.
- For a local Logstash installation, set `LOGSTASH_SOURCE=1` and `LOGSTASH_PATH`
  to its root, and put its JRuby/JDK on `PATH` with `JAVA_HOME` set appropriately.

Build and run the offline unit/integration suite:

```shell
jruby -S bundle install
./gradlew vendor
jruby -S bundle exec rspec spec
jruby -S gem build logstash-output-kusto.gemspec
```

On Windows, use `gradlew.bat vendor`. The suite is network-free. Dedicated
queued-ingestion integration-style tests replace only the SDK network client
while exercising the real Logstash codec, filesystem, ingestor, executor, and
SDK property objects. Other unit tests also stub writers or ingestors to isolate
specific behaviors and failure paths.

The live harness in [e2e/e2e.rb](e2e/e2e.rb) is separate: it creates and drops test
tables and requires explicit test-cluster credentials (`ENGINE_URL`, `INGEST_URL`,
`TEST_DATABASE`, Azure CLI auth). It checks static ingestion and dynamic table/
mapping fan-out. Set `TEST_SECOND_DATABASE` to an existing test database for
cross-database fan-out too. Do not run it against production resources. Each run
uses independent table names and local paths; local artifacts remain for inspection.
This is a finite smoke test, not a throughput, soak, or fault-recovery qualification.
It requires confirmed shutdown before querying results; forced termination fails
the run. Shutdown checks cover the owned process group on POSIX and only the
spawned PID on Windows. Cleanup attempts all run-owned tables and reports failures
without replacing an earlier validation error.

## Contributing

All contributions are welcome: ideas, patches, documentation, bug reports, and complaints.
Programming is not a required skill. It is more important to the community that you are able to contribute.
For more information about contributing, see the [CONTRIBUTING](https://github.com/elastic/logstash/blob/master/CONTRIBUTING.md) file.
