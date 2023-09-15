@files=[]

task :default do
  system("rake -T")
end

require "logstash/devutils/rake"

begin
  require 'rspec/core/rake_task'
  RSpec::Core::RakeTask.new(:spec)
rescue LoadError
end

RSpec::Core::RakeTask.new(:spec_junit) do |t|
  t.rspec_opts = '--format RspecJunitFormatter --out rspec.xml'
end
