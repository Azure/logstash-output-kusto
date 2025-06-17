require 'securerandom'
require 'json'
require 'fileutils'

module LogStash; module Outputs; class KustoOutputInternal
  module FilePersistence
    @failed_dir = '/tmp/buffer_storage/' # default

    def self.failed_dir=(dir)
      @failed_dir = dir
    end

    def self.failed_dir
      @failed_dir
    end

    def self.persist_batch(batch)
      ::FileUtils.mkdir_p(@failed_dir)
      filename = ::File.join(@failed_dir, "failed_batch_#{Time.now.to_i}_#{SecureRandom.uuid}.json")
      ::File.write(filename, JSON.dump(batch))
    end

    def self.load_batches
      return [] unless Dir.exist?(@failed_dir)
      Dir.glob(::File.join(@failed_dir, 'failed_batch_*.json')).map do |file|
        [file, JSON.load(::File.read(file))]
      end
    end

    def self.delete_batch(file)
      ::File.delete(file) if ::File.exist?(file)
    end
  end
end; end; end