# encoding: utf-8

require 'logstash/outputs/base'
require 'logstash/namespace'

class LogStash::Outputs::Kusto < LogStash::Outputs::Base
  class StreamingChunker
    def initialize(max_bytes)
      raise ArgumentError, 'max_bytes must be greater than zero' unless max_bytes.positive?

      @max_bytes = max_bytes
    end

    def chunks(encoded_events)
      chunks = []
      current_chunk = []
      current_bytes = 0

      encoded_events.each do |encoded|
        encoded_bytes = encoded.bytesize

        if !current_chunk.empty? && current_bytes + encoded_bytes > @max_bytes
          chunks << current_chunk
          current_chunk = []
          current_bytes = 0
        end

        current_chunk << encoded
        current_bytes += encoded_bytes

        if encoded_bytes > @max_bytes
          chunks << current_chunk
          current_chunk = []
          current_bytes = 0
        end
      end

      chunks << current_chunk unless current_chunk.empty?
      chunks
    end
  end
end
