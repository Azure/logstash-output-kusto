require 'securerandom'
require 'json'
require 'fileutils'
require 'tmpdir'
require 'thread'

module LogStash; module Outputs; class KustoOutputInternal
  class FilePersistence
    attr_reader :failed_dir

    def initialize(dir = nil, logger = nil)
      @failed_dir = dir || ::File.join(Dir.tmpdir, "logstash_backout")
      begin
        ::FileUtils.mkdir_p(@failed_dir) unless Dir.exist?(@failed_dir)
      rescue => e
        logger&.fatal("Failed to create backup directory #{@failed_dir}: #{e.message}")
        raise
      end
      @logger = logger
      @write_mutex = Mutex.new
      @logger&.info("Backup file directory for failed batches: #{::File.expand_path(@failed_dir)}")
    end

    def persist_batch(batch, max_retries = 3)
      attempts = 0
      begin
        @write_mutex.synchronize do
          tmpfile = ::File.join(@failed_dir, "tmp_#{SecureRandom.uuid}.json")
          filename = ::File.join(@failed_dir, "failed_batch_#{Time.now.to_i}_#{SecureRandom.uuid}.json")
          begin
            ::File.write(tmpfile, JSON.dump(batch))
            ::File.rename(tmpfile, filename)
            @logger&.info("Persisted failed batch to #{filename}")
            return # Success!
          rescue => e
            @logger&.error("Failed to persist batch to #{filename}: #{e.message}")
            begin
              ::File.delete(tmpfile) if ::File.exist?(tmpfile)
            rescue
              # Ignore cleanup errors
            end
            raise
          end
        end
      rescue => e
        attempts += 1
        if attempts < max_retries
          sleep 0.1 * (2 ** (attempts - 1)) # Exponential backoff
          retry
        else
          @logger&.fatal("Failed to persist batch after #{attempts} attempts. Data loss may occur: #{e.message}")
        end
      end
    end

    def delete_batch(file)
      begin
        ::File.delete(file) if ::File.exist?(file)
      rescue => e
        @logger&.warn("Failed to delete batch file #{file}: #{e.message}")
      end
    end
  end
end; end; end