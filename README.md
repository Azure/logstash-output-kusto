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

### Available Configuration Keys

| Parameter Name | Description | Notes |
| --- | --- | --- |
| **path** | The plugin writes events to temporary files before sending them to ADX. This parameter includes a path where files should be written and a time expression for file rotation to trigger an upload to the ADX service. The example above shows how to rotate the files every minute and check the Logstash docs for more information on time expressions. | Required |
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
| **recovery** | If set to true (default), plugin will attempt to resend pre-existing temp files found in the path upon startup | |
| **delete_temp_files** | Determines if temp files will be deleted after a successful upload (true is default; set false for debug purposes only)| |
| **flush_interval** | The time (in seconds) for flushing writes to temporary files. Default is 2 seconds, 0 will flush on every event. Increase this value to reduce IO calls but keep in mind that events in the buffer will be lost in case of abrupt failure.| |
| **proxy_host** | The proxy hostname for redirecting traffic to Kusto.| |
| **proxy_port** | The proxy port for the proxy. Defaults to 80.| |
| **proxy_protocol** | The proxy server protocol , is one of http or https.| |

> Note : LS_JAVA_OPTS can be used to set proxy parameters as well (using export or SET options)

```bash
export  LS_JAVA_OPTS="-Dhttp.proxyHost=1.2.34 -Dhttp.proxyPort=8989 -Dhttps.proxyHost=1.2.3.4 -Dhttps.proxyPort=8989"
```

### Dynamic event routing

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
  reversibly encoded into the temp file name, so dots and spaces are preserved.
  Values containing other characters (for example a path separator `/`) are
  treated as unroutable. **By design, dynamic mode is stricter than legacy static
  mode:** in pure legacy static mode (no dynamic routing active) a
  `database`/`table`/`json_mapping` literal is passed through to Azure Data
  Explorer as-is, whereas once dynamic routing is active a per-event resolved
  value is validated against this character/length format *before* upload and
  treated as unroutable if it does not match (so a bad per-event value is never
  mis-ingested into an unintended target). Note that a static literal used
  *alongside* dynamic routing (including when forced with
  `dynamic_event_routing => true`) is also validated against this format, but at
  startup — the plugin fails fast rather than treating it as unroutable.
- **Length / filename budget.** Azure Data Explorer entity names may be up to
  1024 characters, but in dynamic mode the resolved `database`, `table` and
  `json_mapping` are encoded together into a single temp **file name**, which the
  filesystem caps at 255 bytes. The practical per-event budget is therefore
  smaller than 1024 and is *shared* across the time-based `path` prefix plus the
  three encoded values. Note that percent-encoding makes non-ASCII and special
  characters cost several bytes each (e.g. `é` → 2 bytes → 6 encoded bytes), so
  the byte length can exceed the character length. As a rule of thumb keep the
  combined `database` + `table` + `json_mapping` comfortably under ~150 bytes for
  a typical short time-prefixed path. A value that is individually over the ADX
  1024-character limit, or whose encoded file name would exceed the filesystem
  limit, is treated as unroutable (sent to the DLQ or dropped) — the DLQ reason
  states which condition was hit.
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
- The `json_mapping` reference is optional: if it does not resolve, the event is
  still routed using `database`/`table` and columns are mapped by attribute name.
  This means a **missing mapping field does not** send the event to the DLQ — if
  a mapping is required for a table, make sure the field is always set upstream.
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
  passes validation and the file is uploaded; because ingestion is asynchronous,
  the failure then surfaces **inside Azure Data Explorer** (visible via
  `.show ingestion failures`), not in Logstash. Double-check routing field values
  against existing ADX objects.
- **High-cardinality routing has an operational cost.** Dynamic mode keeps one
  open temporary file per distinct *(time window × database × table × mapping)*
  combination, so routing to many destinations means many concurrent file
  descriptors (watch the OS `ulimit -n`) and many small ingestion calls. ADX
  prefers batched ingestion, so rely on the server-side
  [IngestionBatching policy](https://learn.microsoft.com/azure/data-explorer/kusto/management/batchingpolicy)
  and tune `flush_interval` / `stale_cleanup_interval` rather than routing to an
  unbounded number of tables per pipeline. As an early signal, the plugin logs a
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
  file-descriptor exhaustion.


### Release Notes and versions

| Version | Release Date | Notes |
| --- | --- | --- |
| 2.2.0 | 2026-06-11 | - Add dynamic event routing: `database`, `table` and `json_mapping` now accept Logstash field references (e.g. `%{[@metadata][table]}`) to route events to different destinations from a single output. Unroutable events are sent to the Dead Letter Queue when it is enabled, otherwise dropped (with startup and per-batch warnings) rather than ingested into an unintended table. Crash recovery is isolated between outputs with *different* routing configuration; outputs identical in `ingest_url`/`path`/`database`/`table`/`json_mapping` share an owner id — set a distinct `recovery_owner_id` (or distinct `path` roots) to separate them. Resolved entity names are validated against the Azure Data Explorer 1-1024 character limit, but because they are encoded together into the temp **file name** the practical per-event budget is the filesystem's 255-byte name limit; over-budget values are treated as unroutable. A warning is logged on high routing cardinality, and an optional `dynamic_routing_max_open_files` cap (off by default) can dead-letter excess routes to bound open file descriptors. See the dynamic routing section above for full caveats.  |
| 2.0.8 | 2024-10-23 | - Fix library deprecations, fix issues in the Azure Identity library  |
| 2.0.7 | 2024-01-01 | - Update Kusto JAVA SDK  |
| 2.0.3 | 2023-12-12 | - Make JSON mapping field optional. If not provided logstash output JSON attribute names will be used for column resolution  |
| 2.0.2 | 2023-11-28 | - Bugfix for the scenario where the plugin uses managed identity. Instead of providing the managed identity name as empty in the config,it can completely be skipped  |
| 2.0.0 | 2023-09-19 | - Upgrade to the latest Java SDK version [5.0.2](https://github.com/Azure/azure-kusto-java/releases/tag/v5.0.2). Tests have been performed on **__Logstash 8.5__**  and up (Does not work with 6.x or 7.x versions of Logstash - For these versions use 1.x.x versions of logstash-output-kusto gem) - Fixes CVE's in common-text & outdated Jackson libraries  |
| 1.0.6 | 2022-11-29 | - Upgrade to the latest Java SDK [3.2.1](https://github.com/Azure/azure-kusto-java/releases/tag/v3.2.1) version. Tests have been performed on Logstash 6.x and up.|


## Development Requirements

- Openjdk **8 64bit** (https://www.openlogic.com/openjdk-downloads)
- JRuby 9.2 or higher, defined with openjdk 8 64bit
- Logstash, defined with openjdk 8 64bit

*It is reccomened to use the bundled jdk and jruby with logstash to avoid compatibility issues.*

To fully build the gem, run: 

```shell
bundle install
lock_jars
gem build
```

## Contributing

All contributions are welcome: ideas, patches, documentation, bug reports, and complaints.
Programming is not a required skill. It is more important to the community that you are able to contribute.
For more information about contributing, see the [CONTRIBUTING](https://github.com/elastic/logstash/blob/master/CONTRIBUTING.md) file.
