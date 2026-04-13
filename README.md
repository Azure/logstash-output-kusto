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

The plugin supports two ingestion modes:

- **File mode (default)** — Events are written to temporary files on disk, then uploaded to Kusto via file-based ingestion. This is the original behavior and is used when none of the buffered-mode parameters are set. Requires the `path` parameter.
- **Buffered mode** — Events are buffered in memory and uploaded directly to Kusto via stream ingestion. Activated by setting any of `max_batch_size`, `plugin_flush_interval`, or `max_items`. Supports size-based, time-based, and count-based flush triggers with automatic retry and failed-batch persistence.

### File mode example (default, backward-compatible)

```ruby
output {
  kusto {
    ingest_url => "https://ingest-<cluster-name>.kusto.windows.net/"
    app_id => "<application id>"
    app_key => "<application key/secret>"
    app_tenant => "<tenant id>"
    database => "<database name>"
    table => "<target table>"
    json_mapping => "<mapping name>"
    path => "/tmp/kusto/%{+YYYY-MM-dd-HH-mm}"
  }
}
```

### Buffered mode example

```ruby
output {
  kusto {
    ingest_url => "https://ingest-<cluster-name>.kusto.windows.net/"
    app_id => "<application id>"
    app_key => "<application key/secret>"
    app_tenant => "<tenant id>"
    database => "<database name>"
    table => "<target table>"
    json_mapping => "<mapping name>"
    max_batch_size => 10
    plugin_flush_interval => 10
    max_items => 1000
  }
}
```

More information about configuring Logstash can be found in the [logstash configuration guide](https://www.elastic.co/guide/en/logstash/current/configuration.html)

### Available Configuration Keys

#### Common parameters

| Parameter Name | Description | Notes |
| --- | --- | --- |
| **ingest_url** | The Kusto endpoint for ingestion-related communication. See it on the Azure Portal. | Required |
| **app_id, app_key, app_tenant** | Credentials required to connect to the ADX service. Be sure to use an application with 'ingest' privileges. | Optional |
| **managed_identity** | Managed Identity to authenticate. For user-based managed ID, use the Client ID GUID. For system-based, use the value `system`. The ID needs to have 'ingest' privileges on the cluster. | Optional |
| **cli_auth** | Use Azure CLI credentials for authentication. Only for dev-test scenarios. | Optional. Default: `false` |
| **database** | Database name to place events. | Required |
| **table** | Target table name to place events. | Required |
| **json_mapping** | Maps each attribute from incoming event JSON strings to the appropriate column in the table. Must be in JSON format. | Optional |
| **proxy_host** | The proxy hostname for redirecting traffic to Kusto. | Optional |
| **proxy_port** | The proxy port. | Optional. Default: `80` |
| **proxy_protocol** | The proxy server protocol, one of `http` or `https`. | Optional. Default: `https` |
| **upload_concurrent_count** | Maximum number of concurrent upload threads. | Optional. Default: `3` |
| **upload_queue_size** | Maximum number of uploads queued before backpressure is applied. | Optional. Default: `30` |

#### Buffered mode parameters

Setting **any** of these parameters activates buffered (in-memory) mode. When none are set, the plugin uses file mode.

| Parameter Name | Description | Notes |
| --- | --- | --- |
| **max_batch_size** | Maximum size of the in-memory buffer (in bytes) before it gets flushed. | Optional. Default when active: `10` |
| **plugin_flush_interval** | Interval (in seconds) before the buffer gets flushed regardless of size. | Optional. Default when active: `10` |
| **max_items** | Maximum number of events in the buffer before it gets flushed. | Optional. Default when active: `1000` |
| **process_failed_batches_on_startup** | Retry persisted failed batches when the plugin starts. | Optional. Default: `false` |
| **failed_dir_name** | Directory to store failed batches. If the directory does not exist, it will be created. | Optional. Default: system temp directory |

#### File mode parameters

These parameters are used when buffered mode is **not** active.

| Parameter Name | Description | Notes |
| --- | --- | --- |
| **path** | Path to temporary files. Supports event field references for date-based rotation, e.g. `/tmp/kusto/%{+YYYY-MM-dd-HH-mm}`. | Required in file mode |
| **flush_interval** | Flush interval (in seconds) for writing to files. 0 flushes on every message. | Optional. Default: `2` |
| **delete_temp_files** | Delete temporary files after successful upload. | Optional. Default: `true` |
| **recovery** | Recover and upload temp files from past runs on startup. | Optional. Default: `true` |
| **filename_failure** | Fallback filename when the generated path is invalid. | Optional. Default: `_filepath_failures` |
| **create_if_deleted** | Recreate the file if it has been deleted. | Optional. Default: `true` |
| **dir_mode** | Directory access mode. Set to `-1` for OS default. | Optional. Default: `-1` |
| **file_mode** | File access mode. Set to `-1` for OS default. | Optional. Default: `-1` |
| **stale_cleanup_interval** | Interval in seconds for stale file cleanup. | Optional. Default: `10` |
| **stale_cleanup_type** | Stale cleanup trigger: `events` or `interval`. | Optional. Default: `events` |

> Note: `LS_JAVA_OPTS` can be used to set proxy parameters as well (using export or SET options)

```bash
export  LS_JAVA_OPTS="-Dhttp.proxyHost=1.2.3.4 -Dhttp.proxyPort=8989 -Dhttps.proxyHost=1.2.3.4 -Dhttps.proxyPort=8989"
```


### Release Notes and versions

| Version | Release Date | Notes |
| --- | --- | --- |
| 3.0.0 | 2024-11-01 | Added buffered (in-memory) ingestion mode with size/time/count-based flush triggers, retry with exponential backoff, and failed-batch persistence. File mode preserved for backward compatibility. |
| 2.0.8 | 2024-10-23 | Fix library deprecations, fix issues in the Azure Identity library  |
| 2.0.7 | 2024-01-01 | Update Kusto JAVA SDK  |
| 2.0.3 | 2023-12-12 | Make JSON mapping field optional. If not provided logstash output JSON attribute names will be used for column resolution  |
| 2.0.2 | 2023-11-28 | Bugfix for the scenario where the plugin uses managed identity. Instead of providing the managed identity name as empty in the config,it can completely be skipped  |
| 2.0.0 | 2023-09-19 | Upgrade to the latest Java SDK version [5.0.2](https://github.com/Azure/azure-kusto-java/releases/tag/v5.0.2). Tests have been performed on **__Logstash 8.5__**  and up (Does not work with 6.x or 7.x versions of Logstash - For these versions use 1.x.x versions of logstash-output-kusto gem) - Fixes CVE's in common-text & outdated Jackson libraries  |
| 1.0.6 | 2022-11-29 | Upgrade to the latest Java SDK [3.2.1](https://github.com/Azure/azure-kusto-java/releases/tag/v3.2.1) version. Tests have been performed on Logstash 6.x and up.|


## Development Requirements

- Openjdk **8 64bit** (https://www.openlogic.com/openjdk-downloads)
- JRuby 9.2 or higher, defined with openjdk 8 64bit
- Logstash, defined with openjdk 8 64bit

*It is recommended to use the bundled jdk and jruby with logstash to avoid compatibility issues.*

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
