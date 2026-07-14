# encoding: utf-8

require_relative '../../spec_helpers'
require 'logstash/outputs/kusto/streaming_chunker'

describe LogStash::Outputs::Kusto::StreamingChunker do
  describe '#chunks' do
    it 'returns no chunks for an empty batch' do
      expect(described_class.new(10).chunks([])).to eq([])
    end

    it 'combines events while their encoded bytes fit within the limit' do
      chunks = described_class.new(10).chunks(%w[1234 567 89])

      expect(chunks).to eq([%w[1234 567 89]])
    end

    it 'starts a new chunk before the next event would exceed the limit' do
      chunks = described_class.new(6).chunks(%w[1234 567 89])

      expect(chunks).to eq([%w[1234], %w[567 89]])
    end

    it 'allows a chunk to be exactly the configured limit' do
      chunks = described_class.new(6).chunks(%w[123 456])

      expect(chunks).to eq([%w[123 456]])
    end

    it 'keeps an oversized event intact and places it in its own chunk' do
      oversized = 'x' * 11
      chunks = described_class.new(10).chunks(['before', oversized, 'after'])

      expect(chunks).to eq([['before'], [oversized], ['after']])
    end

    it 'measures bytes instead of characters' do
      multibyte = "\u20ac\u20ac" # six UTF-8 bytes
      chunks = described_class.new(6).chunks([multibyte, 'x'])

      expect(chunks).to eq([[multibyte], ['x']])
    end

    it 'preserves event order and encoded separators' do
      encoded = ["{\"id\":1}\n", "{\"id\":2}\n", "{\"id\":3}\n"]
      chunks = described_class.new(18).chunks(encoded)

      expect(chunks.flatten).to eq(encoded)
      expect(chunks.map(&:join)).to eq(["{\"id\":1}\n{\"id\":2}\n", "{\"id\":3}\n"])
    end

    it 'rejects a non-positive byte limit' do
      expect { described_class.new(0) }.to raise_error(ArgumentError)
      expect { described_class.new(-1) }.to raise_error(ArgumentError)
    end
  end
end
