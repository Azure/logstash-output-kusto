# Changelog


# 3.0.0

## Features
- **Buffered ingestion mode** (default): in-memory buffered ingestion with configurable flush triggers
  - `max_batch_size` — flush when buffered data exceeds a size threshold (bytes)
  - `plugin_flush_interval` — flush on a recurring time interval (seconds)
  - `max_items` — flush when buffered event count reaches a limit
- **Backward-compatible file mode**: setting `path` continues to use the existing file-based ingestion path
- **Disk persistence**: failed buffered batches are persisted to disk and retried on restart
- **Parallel uploads**: configurable thread pool for concurrent Kusto ingestion (`upload_concurrent_count`, `upload_queue_size`)

## Improvements
- Retry with exponential backoff for stream uploads (3 retries) and linear backoff for file uploads (10 retries)
- In-flight upload tracking with graceful drain on shutdown
- Managed identity and Azure CLI authentication support alongside AAD app credentials
- Upgraded to Kusto Java SDK 7.0.5 and Gradle 9.0
- Added comprehensive validation for all configuration parameters
- Added end-to-end tests for file mode, buffered size-based flush, and buffered time-based flush
- Added unit tests for configuration validation error paths
- E2E test performance: poll-based startup and data detection instead of fixed sleeps

## Bug Fixes
- Fixed `upload_file` retry counter reset causing infinite retry loop

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