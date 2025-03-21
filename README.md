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

- Logstash version 6+. [Installation instructions](https://www.elastic.co/guide/en/logstash/current/installing-logstash.html) 
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
    max_size => 10
    max_interval => 10
    latch_timeout => 60
  }
}
```
More information about configuring Logstash can be found in the [logstash configuration guide](https://www.elastic.co/guide/en/logstash/current/configuration.html)

### Available Configuration Keys

| Parameter Name | Description | Notes |
| --- | --- | --- |
| **ingest_url** | The Kusto endpoint for ingestion-related communication. See it on the Azure Portal. | Required |
| **app_id, app_key, app_tenant** | Credentials required to connect to the ADX service. Be sure to use an application with 'ingest' privileges. | Optional |
| **managed_identity** | Managed Identity to authenticate. For user-based managed ID, use the Client ID GUID. For system-based, use the value `system`. The ID needs to have 'ingest' privileges on the cluster. | Optional |
| **database** | Database name to place events | Required |
| **table** | Target table name to place events | Required |
| **json_mapping** | Maps each attribute from incoming event JSON strings to the appropriate column in the table. Note that this must be in JSON format, as this is the interface between Logstash and Kusto | Optional |
| **proxy_host** | The proxy hostname for redirecting traffic to Kusto. | Optional |
| **proxy_port** | The proxy port for the proxy. Defaults to 80. | Optional |
| **proxy_protocol** | The proxy server protocol, is one of http or https. | Optional |
| **max_size** | Maximum size of the buffer before it gets flushed, defaults to 10MB. | Optional |
| **latch_timeout** | Latch timeout in seconds, defaults to 60. This is the maximum wait time after which the flushing attempt is timed out and the network is considered to be down. The system waits for the network to be back to retry flushing the same batch. | Optional |

> Note : LS_JAVA_OPTS can be used to set proxy parameters as well (using export or SET options)

> Note: **path** config parameter is no longer used in the latest release (3.0.0) and will be deprecated in future releases

```bash
export  LS_JAVA_OPTS="-Dhttp.proxyHost=1.2.34 -Dhttp.proxyPort=8989 -Dhttps.proxyHost=1.2.3.4 -Dhttps.proxyPort=8989"
```


### Release Notes and versions

| Version | Release Date | Notes |
| --- | --- | --- |
| 3.0.0 | 2024-11-01 | Updated configuration options |
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
