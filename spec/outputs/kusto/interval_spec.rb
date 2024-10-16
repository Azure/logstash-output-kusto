# # spec/interval_test.rb
# require 'rspec'
# require 'logstash/outputs/kusto/interval'


# describe LogStash::Outputs::Kusto::Interval do
#   let(:interval_time) { 1 }
#   let(:procsy) { double("procsy", call: true) }

#   describe '#initialize' do
#     it 'initializes with the correct interval and procsy' do
#       interval = described_class.new(interval_time, procsy)
#       expect(interval.instance_variable_get(:@interval)).to eq(interval_time)
#       expect(interval.instance_variable_get(:@procsy)).to eq(procsy)
#     end
#   end

#   describe '#start' do
#     it 'starts the interval thread' do
#       interval = described_class.new(interval_time, procsy)
#       interval.start
#       expect(interval.alive?).to be true
#       interval.stop
#     end

#     it 'does not start a new thread if already started' do
#       interval = described_class.new(interval_time, procsy)
#       interval.start
#       first_thread = interval.instance_variable_get(:@thread)
#       interval.start
#       second_thread = interval.instance_variable_get(:@thread)
#       expect(first_thread).to eq(second_thread)
#       interval.stop
#     end
#   end

#   describe '#stop' do
#     it 'stops the interval thread' do
#       interval = described_class.new(interval_time, procsy)
#       interval.start
#       interval.stop
#       expect(interval.alive?).to be false
#     end
#   end

#   describe '#alive?' do
#     it 'returns true if the thread is alive' do
#       interval = described_class.new(interval_time, procsy)
#       interval.start
#       expect(interval.alive?).to be true
#       interval.stop
#     end

#     it 'returns false if the thread is not alive' do
#       interval = described_class.new(interval_time, procsy)
#       expect(interval.alive?).to be false
#     end
#   end

#   describe 'interval execution' do
#     it 'calls the proc at the specified interval' do
#       interval = described_class.new(interval_time, procsy)
#       expect(procsy).to receive(:call).at_least(:twice)
#       interval.start
#       sleep(2.5)
#       interval.stop
#     end
#   end
# end