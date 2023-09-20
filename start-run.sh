#!/bin/bash
echo "starting script run..."
rm Jars.lock
rm logstash-output-kusto-*-java.gem
rm Gemfile.lock
bundle lock --add-platform java
bundle install
lock_jars
gem build logstash-output-kusto
cd /src/logstash
logstash-plugin uninstall logstash-output-kusto
logstash-plugin install /src/logstash-output-kusto/logstash-output-kusto*.gem
cd /src/logstash-output-kusto
logstash -f /src/logstash-output-kusto/logstash-output-kusto-apache.conf