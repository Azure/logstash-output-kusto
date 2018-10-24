# Logstash Output Plugin for Azure Kusto

[![Travis Build Status](https://travis-ci.org/Azure/logstash-output-kusto.svg)](https://travis-ci.org/Azure/logstash-output-kusto)

This is a plugin for [Logstash](https://github.com/elastic/logstash).

It is fully free and fully open source. The license is Apache 2.0, meaning you are pretty much free to use it however you want in whatever way.

This plugin will enable you to process events from Logstash into an **Azure Kusto** database for later analysis. 

## Requirements

- Logstash version 6+. You can find more information on how to install it by reading [this](https://www.elastic.co/guide/en/logstash/current/installing-logstash.html)
- Kusto cluster with a database. You can follow the steps outlined [here](https://docs.microsoft.com/en-us/azure/data-explorer/create-cluster-database-portal) to do this.
- AAD Application credentials with premission to ingest data into Kusto. Read this [document](https://docs.microsoft.com/en-us/azure/kusto/management/access-control/how-to-provision-aad-app) for more info.

## Installation

To make the plugin available in your Logstash environment run the following command:
```sh
bin/logstash-plugin install logstash-output-kusto
```

## Configuration

Before you can start sending events from Logstash to Kusto you need to configure it. The following example shows the bear minimum you should provide, and it would be enough for most use-cases:

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
            mapping => "<mapping name>"
	}
}
```

### Available Configuration Keys

| Parameter Name | Description | Notes |
| --- | --- | --- |
| **path** | The plugin writes events to temporary files before sending them to Kusto. This parameter should include a path where these files should be written and a time expression to note when file rotation should happen and trigger an upload to the Kusto service. The example above shows how to rotate the files every minute, check the Logstash docs for more information on time expressions. | Required.
| **ingest_url** | The Kusto endpoint for ingestion related communication. You can see it on the Azure Portal.| Required.|
| **app_id, app_key, app_tenant**| Those are the credentials required to connect to the Kusto service. Be sure to use an application with 'ingest' privilages. | Required.|
| **database**| Database name where events should go to. | Required. |
| **table** | Target table name where events should be saved | Required.
| **mapping** | A mapping is used by Kusto to map an incoming event json string to the right row format (what property goes into which column) | Required. |
| **recovery** | If this is set to true (the default), the plugin will attempt to resend pre-existing temp files it finds in the path upon startup | |
| **delete_temp_files** | Determines if temp files will be deleted after a successful upload (default is true, set false only for debug purposes)| |
| **flush_interval** | The time (in seconds) for flushing writes to temporary files. Defaults to 2 seconds, 0 will flush on every event. Increase this value to reduce IO calls but keep in mind that events in the buffer will be lost in case of abrupt failure.| |

## Contributing

All contributions are welcome: ideas, patches, documentation, bug reports, complaints, and even something you drew up on a napkin.

Programming is not a required skill. Whatever you've seen about open source and maintainers or community members saying "send patches or die" - you will not see that here.

It is more important to the community that you are able to contribute.

For more information about contributing, see the [CONTRIBUTING](https://github.com/elastic/logstash/blob/master/CONTRIBUTING.md) file.
