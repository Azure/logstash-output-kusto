require 'securerandom'
require 'json'
require 'fileutils'
require 'tmpdir'

module LogStash; module Outputs; class KustoOutputInternal
  class FilePersistence
    attr_reader :failed_dir

    def initialize(dir = nil, logger = nil)
      @failed_dir = dir || ::File.join(Dir.tmpdir, "logstash_backout")
      ::FileUtils.mkdir_p(@failed_dir)
      logger&.info("Backup file directory for failed batches: #{::File.expand_path(@failed_dir)}")
    end

    def persist_batch(batch)
      filename = ::File.join(@failed_dir, "failed_batch_#{Time.now.to_i}_#{SecureRandom.uuid}.json")
      ::File.write(filename, JSON.dump(batch))
    end

    def load_batches
      return [] unless Dir.exist?(@failed_dir)
      Dir.glob(::File.join(@failed_dir, 'failed_batch_*.json')).map do |file|
        [file, JSON.load(::File.read(file))]
      end
    end

    def delete_batch(file)
      ::File.delete(file) if ::File.exist?(file)
    end
  end
end; end; end