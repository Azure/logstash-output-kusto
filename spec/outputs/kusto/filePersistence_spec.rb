# encoding: utf-8
require_relative "../../spec_helpers.rb"
require 'logstash/outputs/kusto/filePersistence'
require 'fileutils'

describe LogStash::Outputs::KustoOutputInternal::FilePersistence do
  let(:tmp_dir) { File.expand_path("../../../../tmp/test_buffer_storage", __FILE__) }
  let(:batch) { [{ "foo" => "bar" }, { "baz" => "qux" }] }
  let(:logger) { double("Logger", info: nil) }
  let(:file_persistence) { described_class.new(tmp_dir, logger) }

  before(:each) do
    FileUtils.rm_rf(tmp_dir)
    FileUtils.mkdir_p(tmp_dir)
  end

  after(:each) do
    FileUtils.rm_rf(tmp_dir)
  end

  it 'persists a batch to a file and loads it back' do
    file_persistence.persist_batch(batch)
    files = Dir.glob(File.join(tmp_dir, 'failed_batch_*.json'))
    expect(files.size).to eq(1)
    loaded = file_persistence.load_batches.to_a
    expect(loaded.size).to eq(1)
    expect(loaded.first[1]).to eq(batch)
  end

  it 'deletes a batch file' do
    file_persistence.persist_batch(batch)
    file = Dir.glob(File.join(tmp_dir, 'failed_batch_*.json')).first
    expect(File.exist?(file)).to be true
    file_persistence.delete_batch(file)
    expect(File.exist?(file)).to be false
  end

  it 'does not fail if directory does not exist' do
    FileUtils.rm_rf(tmp_dir)
    expect { file_persistence.persist_batch(batch) }.not_to raise_error
    files = Dir.glob(File.join(tmp_dir, 'failed_batch_*.json'))
    expect(files.size).to eq(1)
  end

  it 'returns empty array if directory does not exist' do
    file_persistence = described_class.new(tmp_dir, logger) # Re-instantiate!
    FileUtils.rm_rf(tmp_dir)
    expect(file_persistence.load_batches).to eq([])
  end
end