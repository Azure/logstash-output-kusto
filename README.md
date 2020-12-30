# Logstash Output Plugin for Azure Data Explorer (Kusto)

![build](https://github.com/Azure/logstash-output-kusto/workflows/build/badge.svg)
![build](https://github.com/Azure/logstash-output-kusto/workflows/build/badge.svg?branch=master)
[![Gem](https://img.shields.io/gem/v/logstash-output-kusto.svg)](https://rubygems.org/gems/logstash-output-kusto)
[![Gem](https://img.shields.io/gem/dt/logstash-output-kusto.svg)](https://rubygems.org/gems/logstash-output-kusto)

This is a plugin for [Logstash](https://github.com/elastic/logstash).

It is fully free and open source. The license is Apache 2.0.

This Azure Data Explorer (ADX) Logstash plugin enables you to process events from Logstash into an **Azure Data Explorer** database for later analysis. 

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
            path => "/tmp/kusto/%{+YYYY-MM-dd-HH-mm}.txt"
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
More information about configuring Logstash can be found in the [logstash configuration guide](https://www.elastic.co/guide/en/logstash/current/configuration.html)

### Available Configuration Keys

| Parameter Name | Description | Notes |
| --- | --- | --- |
| **path** | The plugin writes events to temporary files before sending them to ADX. This parameter includes a path where files should be written and a time expression for file rotation to trigger an upload to the ADX service. The example above shows how to rotate the files every minute and check the Logstash docs for more information on time expressions. | Required
| **ingest_url** | The Kusto endpoint for ingestion-related communication. See it on the Azure Portal.| Required|
| **app_id, app_key, app_tenant**| Credentials required to connect to the ADX service. Be sure to use an application with 'ingest' privileges. | Required|
| **database**| Database name to place events | Required |
| **table** | Target table name to place events | Required
| **json_mapping** | Maps each attribute from incoming event JSON strings to the appropriate column in the table. Note that this must be in JSON format, as this is the interface between Logstash and Kusto | Required |
| **recovery** | If set to true (default), plugin will attempt to resend pre-existing temp files found in the path upon startup | |
| **delete_temp_files** | Determines if temp files will be deleted after a successful upload (true is default; set false for debug purposes only)| |
| **flush_interval** | The time (in seconds) for flushing writes to temporary files. Default is 2 seconds, 0 will flush on every event. Increase this value to reduce IO calls but keep in mind that events in the buffer will be lost in case of abrupt failure.| |

## Development Requirements

- Openjdk **8 64bit** (https://www.openlogic.com/openjdk-downloads)
- JRuby 9.2 or higher, defined with openjdk 8 64bit
- Logstash, defined with openjdk 8 64bit

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
