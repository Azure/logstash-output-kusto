source 'https://rubygems.org'
gemspec



logstash_path = ENV["LOGSTASH_PATH"] || "../../logstash"
use_logstash_source = ENV["LOGSTASH_SOURCE"] && ENV["LOGSTASH_SOURCE"].to_s == "1"

puts "cwd: #{Dir.getwd} ,use_logstash_source: #{use_logstash_source}, logstash_path: #{logstash_path}, exists: #{Dir.exist?(logstash_path)}"

if Dir.exist?(logstash_path) && use_logstash_source
  puts "Using local logstash"
  gem 'logstash-core', :path => "#{logstash_path}/logstash-core"
  gem 'logstash-core-plugin-api', :path => "#{logstash_path}/logstash-core-plugin-api"
else
  puts "using default logstash"
end

gem "rspec"
gem "rspec_junit_formatter"
