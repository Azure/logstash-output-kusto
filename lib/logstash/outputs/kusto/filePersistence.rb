require 'securerandom'
require 'json'
require 'fileutils'

module LogStash; module Outputs; class KustoOutputInternal
  module FilePersistence
    FAILED_DIR = './tmp/buffer_storage/'

    def self.persist_batch(batch)
      ::FileUtils.mkdir_p(FAILED_DIR)
      filename = ::File.join(FAILED_DIR, "failed_batch_#{Time.now.to_i}_#{SecureRandom.uuid}.json")
      ::File.write(filename, JSON.dump(batch))
    end

    def self.load_batches
      return [] unless Dir.exist?(FAILED_DIR)
      Dir.glob(::File.join(FAILED_DIR, 'failed_batch_*.json')).map do |file|
        [file, JSON.load(::File.read(file))]
      end
    end

    def self.delete_batch(file)
      ::File.delete(file) if ::File.exist?(file)
    end
  end
end; end; end