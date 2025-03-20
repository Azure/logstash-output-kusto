export LOGSTASH_SOURCE=1
export LOGSTASH_PATH=/softwares/logstash
export JRUBY_HOME=$LOGSTASH_PATH/vendor/jruby
export JAVA_HOME=$LOGSTASH_PATH/jdk
export PATH=$PATH:/softwares/logstash/vendor/jruby/bin:/softwares/logstash/bin
jruby -S gem install bundler -v 2.4.19
jruby -S bundle install
gem build *.gemspec
rm Gemfile.lock
/softwares/logstash/bin/logstash-plugin uninstall logstash-output-kusto
/softwares/logstash/bin/logstash-plugin install logstash-output-kusto-3.0.0-java.gem