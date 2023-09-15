cd /src/logstash
logstash-plugin uninstall logstash-output-kusto
logstash-plugin install /src/logstash-output-kusto/logstash-output-kusto*.gem
cd /src/logstash-output-kusto
logstash -f /src/logstash-output-kusto/logstash-kusto-all.conf