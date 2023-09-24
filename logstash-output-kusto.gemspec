Gem::Specification.new do |s|
  s.name          = 'logstash-output-kusto' #WATCH OUT: we hardcoded usage of this name in one of the classes.
  s.version       = '2.0.0'
  s.licenses      = ['Apache-2.0']
  s.summary       = 'Writes events to Azure Data Explorer (Kusto)'
  s.description   = 'This is a logstash output plugin used to write events to an Azure Data Explorer (a.k.a Kusto)'
  s.homepage      = 'https://github.com/Azure/logstash-output-kusto'
  s.authors       = ['Tamir Kamara', 'Asaf Mahlev']
  s.email         = 'nugetkusto@microsoft.com'
  s.require_paths = ['lib']
  s.platform = 'java'

  # Files
  s.files = Dir["lib/**/*","spec/**/*","*.gemspec","*.md","CONTRIBUTORS","Gemfile","LICENSE","NOTICE.TXT", "vendor/jar-dependencies/**/*", "vendor/jar-dependencies/**/*.rb", "version", "docs/**/*"]

  # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "output" }

  # Gem dependencies
  s.add_runtime_dependency 'logstash-core', '>= 8.3.0'
  s.add_runtime_dependency "logstash-core-plugin-api", ">= 1.60", "<= 2.99"  
  s.add_runtime_dependency 'logstash-codec-json_lines'
  s.add_runtime_dependency 'logstash-codec-line'

  s.add_development_dependency 'logstash-devutils'
  s.add_development_dependency 'flores'
  s.add_development_dependency 'logstash-input-generator'
  s.add_development_dependency 'jar-dependencies', '~> 0.4'
  s.add_development_dependency 'rspec_junit_formatter'


end
