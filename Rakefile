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