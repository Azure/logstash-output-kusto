# encoding: utf-8

require 'logstash/outputs/base'
require 'logstash/namespace'
require 'logstash/errors'

class LogStash::Outputs::Kusto < LogStash::Outputs::Base
	##
	# Bare-bones utility for running a block of code at an interval.
	#
	class Interval
		##
		# Initializes a new Interval with the given arguments and starts it
		# before returning it.
		#
		# @param interval [Integer] (see: Interval#initialize)
		# @param procsy [#call] (see: Interval#initialize)
		#
		# @return [Interval]
		#
		def self.start(interval, procsy)
			new(interval, procsy).tap(&:start)
		end

		##
		# @param interval [Integer]: time in seconds to wait between calling the given proc
		# @param procsy [#call]: proc or lambda to call periodically; must not raise exceptions.
		def initialize(interval, procsy)
			@interval = interval
			@procsy = procsy

			# Mutex, ConditionVariable, etc.
			@mutex = Mutex.new
			@sleeper = ConditionVariable.new
		end

		##
		# Starts the interval, or returns if it has already been started.
		#
		# @return [void]
		def start
			@mutex.synchronize do
				return if @thread && @thread.alive?

				@thread = Thread.new { run }
			end
		end

		##
		# Stop the interval.
		# Does not interrupt if execution is in-progress.
		def stop
			@mutex.synchronize do
				@stopped = true
			end

			@thread && @thread.join
		end

		##
		# @return [Boolean]
		def alive?
			@thread && @thread.alive?
		end

		private

		def run
			@mutex.synchronize do
				loop do
					@sleeper.wait(@mutex, @interval)
					break if @stopped

					@procsy.call
				end
			end
			ensure
			@sleeper.broadcast
		end
	end
end
