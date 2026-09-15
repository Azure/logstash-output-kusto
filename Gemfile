source 'https://rubygems.org'

gemspec

# i18n 1.15 uses Fiber storage APIs absent from the Ruby versions embedded in
# supported Logstash 8.x releases. This constrains development/test resolution,
# not the packaged plugin's runtime dependencies.
gem 'i18n', '~> 1.14.0' if Gem::Version.new(RUBY_VERSION) < Gem::Version.new('3.2')

logstash_path = ENV["LOGSTASH_PATH"] || "../../logstash"
use_logstash_source = ENV["LOGSTASH_SOURCE"] && ENV["LOGSTASH_SOURCE"].to_s == "1"

if Dir.exist?(logstash_path) && use_logstash_source
  gem 'logstash-core', :path => "#{logstash_path}/logstash-core"
  gem 'logstash-core-plugin-api', :path => "#{logstash_path}/logstash-core-plugin-api"
end