ARG logstash_version
FROM docker.elastic.co/logstash/logstash-oss:${logstash_version}
# Install the gem
COPY logstash-output-kusto.gem /tmp/logstash-output-kusto.gem
COPY samples/nginx.log /tmp/nginx.log
COPY conf/logstash-kusto.config /usr/share/logstash/pipeline/logstash-kusto.config
RUN bin/logstash-plugin install /tmp/logstash-output-kusto.gem