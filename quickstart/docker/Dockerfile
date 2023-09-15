FROM mcr.microsoft.com/openjdk/jdk:17-mariner
RUN tdnf update -y && tdnf install -y shadow-utils tar maven wget
# Provide a non-root user to run the process.
RUN groupadd --gid 1000 logstash && \
    adduser --uid 1000 --gid 1000 \
      --home-dir /usr/share/logstash --no-create-home \
      logstash
ENV LOGSTASHFILE logstash-8.9.2-linux-x86_64.tar.gz

RUN curl -LO https://artifacts.elastic.co/downloads/logstash/$LOGSTASHFILE && \
    tar zxf $LOGSTASHFILE -C /usr/share && \
    mv /usr/share/logstash-8.9.2 /usr/share/logstash && \
    chown --recursive logstash:logstash /usr/share/logstash/ && \
    chown -R logstash:root /usr/share/logstash && \
    chmod -R g=u /usr/share/logstash && \
    mkdir /licenses/ && \
    mv /usr/share/logstash/NOTICE.TXT /licenses/NOTICE.TXT && \
    mv /usr/share/logstash/LICENSE.txt /licenses/LICENSE.txt && \
    find /usr/share/logstash -type d -exec chmod g+s {} \; && \
    rm $LOGSTASHFILE

ENV ELASTIC_CONTAINER true

ENV PATH=/usr/share/logstash/bin:$PATH

RUN logstash-plugin install logstash-filter-prune

RUN logstash-plugin install logstash-output-kusto

RUN cd /usr/share/logstash && \
    sed -i -e '/logstash-filter-xml/d' -e '/logstash-devutils/d' Gemfile && \
    rm Gemfile.lock && \
    ./vendor/jruby/lib/ruby/gems/shared/gems/bundler-2.2.33/exe/bundle install --without= development test && \
    ./vendor/jruby/lib/ruby/gems/shared/gems/bundler-2.2.33/exe/bundle clean --force
RUN rm -rf ~/.m2
VOLUME /kusto
ENTRYPOINT /usr/share/logstash/bin/logstash
CMD ["-f","/usr/share/logstash/pipeline/"]

## export TAG="sdktestsacr.azurecr.io/logstash-output-kusto:v5.0.2-Mariner"
##docker build -t $TAG . && docker push $TAG