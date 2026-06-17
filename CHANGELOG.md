# Changelog


# 2.2.0

- Add dynamic event routing: `database`, `table` and `json_mapping` now accept Logstash field references (e.g. `%{[@metadata][table]}`) so a single output can route events to different Azure Data Explorer destinations. Resolved values may contain letters, digits, spaces, dots, dashes and underscores.
- Unroutable events (missing routing field or a value containing unsupported characters such as a path separator) are sent to Logstash's native Dead Letter Queue when it is enabled, and otherwise dropped with startup and per-batch warnings (rather than mis-ingested into an unintended table). Enable the dead letter queue to capture them.
- Fail fast at startup when a static `database`/`table` used alongside dynamic routing is empty or contains invalid characters.
- Crash recovery is isolated per output: each dynamic temp file is stamped with a stable identifier derived from the output's `ingest_url`/`database`/`table`/`json_mapping`/`path`, and recovery only resends files carrying that identifier, so outputs with a *different* routing configuration sharing a `path` root never pick up each other's leftover files. Outputs identical in all of those settings (e.g. differing only by credentials or pipeline conditionals) share an identifier — give them distinct `path` roots if they must not recover each other's files.
- Enforce the Azure Data Explorer 1-1024 character entity-name limit for dynamic routing: resolved per-event `database`/`table`/`json_mapping` values that are too long are treated as unroutable (decode time), and static `database`/`table`/`json_mapping` literals used alongside dynamic routing are rejected at startup. In addition, because the resolved values are encoded together into the temp **file name**, the practical per-event budget is the filesystem's 255-byte name limit (shared with the `path` prefix), which is smaller than 1024; values whose encoded file name would exceed it are treated as unroutable. (Pure static mode is unchanged.)
- Add `dynamic_routing_open_files_warning_threshold` (default 100, set 0 to disable) to log a warning when dynamic routing holds many temporary files open at once, as an early signal of high routing cardinality.
- Known limitation: routing validates only the *format* of `database`/`table`/`json_mapping`, not their *existence*. A syntactically valid but non-existent (e.g. mistyped) target passes validation and the file is uploaded; because ingestion is asynchronous, the failure then surfaces inside Azure Data Explorer (`.show ingestion failures`), not in Logstash.


# 2.0.3

- Make JSON mapping optional


# 2.0.2

- Bugfix for the scenario where the plugin uses managed identity. Instead of providing the managed identity name as empty in the config,
it can completely be skipped


# 2.0.0

- Use (5.0.2) version of the java sdk, and retrieve it from maven with bundler. Supports logstash 8.6 versions and up
- Upgrade to latest Java SDK fixes [CVE](https://github.com/advisories/GHSA-599f-7c49-w659) and addresses Issue#48

# 1.0.5

- Use (3.1.3) version of the java sdk, and retrieve it from maven with bundler.
- Added support for `proxy_host` `proxy_port` `proxy_protocol` to support proxying ingestion to Kusto

# 1.0.0

- Use stable (2.1.2) version of the java sdk, and retrieve it from maven with bundler.
- Renamed `mapping` to `json_mapping` in order to clarify usage. `mapping` still remains as a deprecated parameter.  

## 0.4.0

- set 'client name for tracing' to identify usage of this plugin on Kusto logs

## 0.3.0

- move to version 1.0.0-BETA-04 of azure-kusto-java sdk
- better support multiple kusto outputs running in parallel

## 0.2.0

- move to version 1.0.0-BETA-01 of azure-kusto-java sdk

## 0.1.7

- fixed app_key (password) bug, include 0.1.7 of the kusto-java-sdk to allow working through a proxy

## 0.1.6

- plugin published to the public. supports ingestion json events into a specific table-database (without dynamic routing currently)


## 0.1.0

- Plugin created with the logstash plugin generator