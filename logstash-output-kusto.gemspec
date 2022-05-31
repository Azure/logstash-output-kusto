Gem::Specification.new do |s|
  s.name          = 'logstash-output-kusto' #WATCH OUT: we hardcoded usage of this name in one of the classes.
  s.version       = '1.0.4'
  s.licenses      = ['Apache-2.0']
  s.summary       = 'Writes events to Azure Data Explorer (Kusto)'
  s.description   = 'This is a logstash output plugin used to write events to an Azure Data Explorer (a.k.a Kusto)'
  s.homepage      = 'https://github.com/Azure/logstash-output-kusto'
  s.authors       = ['Tamir Kamara', 'Asaf Mahlev']
  s.email         = 'nugetkusto@microsoft.com'
  s.require_paths = ['lib']
  s.platform = 'java'

  # Files
  s.files = Dir['lib/**/*', 'spec/**/*', 'vendor/**/*', '*.gemspec', '*.md', 'CONTRIBUTORS', 'Gemfile', 'LICENSE', 'NOTICE.TXT']

  # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "output" }

  # Gem dependencies
  s.add_runtime_dependency 'logstash-core-plugin-api', '~> 2.0'
  s.add_runtime_dependency 'logstash-codec-json_lines'
  s.add_runtime_dependency 'logstash-codec-line'

  s.add_development_dependency 'logstash-devutils'
  s.add_development_dependency 'flores'
  s.add_development_dependency 'logstash-input-generator'
  s.add_development_dependency 'ruby-maven', '~> 3.3.11'

  # Jar dependencies
  s.requirements << "jar 'com.microsoft.azure.kusto, kusto-ingest, 3.1.1"
  s.add_runtime_dependency 'jar-dependencies'
end
