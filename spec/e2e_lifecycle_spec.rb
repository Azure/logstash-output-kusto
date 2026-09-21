# encoding: utf-8
require_relative 'spec_helpers'
require_relative '../e2e/e2e'

describe E2E, 'lifecycle failures' do
  let(:harness) { described_class.new }
  let(:pid) { 4242 }

  def exit_status(code = 0, signal = nil)
    instance_double(Process::Status, success?: code == 0 && signal.nil?, exitstatus: code, termsig: signal)
  end

  before do
    allow(harness).to receive(:sleep)
    allow(harness).to receive(:warn)
  end

  context 'shutdown confirmation' do
    before do
      harness.instance_variable_set(:@logstash_pid, pid)
      harness.instance_variable_set(:@logstash_process_group, true)
      allow(Process).to receive(:kill).and_return(1)
      allow(Process).to receive(:waitpid).with(pid, Process::WNOHANG).and_return(pid)
    end

    it 'returns and records the actual exit status instead of a boolean' do
      status = exit_status(23)
      allow(Process).to receive(:waitpid2).with(pid, Process::WNOHANG).and_return([pid, status])
      allow(Process).to receive(:kill).with(0, -pid).and_raise(Errno::ESRCH)

      expect(harness.wait_for_exit(pid, 0)).to equal(status)
      expect(harness.instance_variable_get(:@logstash_status)).to equal(status)
    end

    it 'rejects an abnormal exit even when the process group is gone' do
      status = exit_status(23)
      allow(Process).to receive(:waitpid2).with(pid, Process::WNOHANG).and_return([pid, status])
      allow(Process).to receive(:kill).with(0, -pid).and_raise(Errno::ESRCH)

      expect { harness.stop_logstash }.to raise_error(/exit status 23/)
      expect(harness.instance_variable_get(:@logstash_status)).to equal(status)
      expect(harness.instance_variable_get(:@logstash_pid)).to be_nil
      expect(Process).not_to have_received(:kill).with('TERM', -pid)
    end

    it 'accepts a clean exit without signalling a process that already finished' do
      status = exit_status
      allow(Process).to receive(:waitpid2).with(pid, Process::WNOHANG).and_return([pid, status])
      allow(Process).to receive(:kill).with(0, -pid).and_raise(Errno::ESRCH)

      expect { harness.stop_logstash; harness.stop_logstash }.not_to raise_error
      expect(Process).not_to have_received(:kill).with('TERM', -pid)
    end

    [[nil, 15], [143, nil], [0, nil]].each do |code, signal|
      it "accepts status #{code.inspect}/signal #{signal.inspect} after requesting TERM" do
        status = exit_status(code, signal)
        alive = true
        allow(Process).to receive(:waitpid2).with(pid, Process::WNOHANG) { alive ? nil : [pid, status] }
        allow(Process).to receive(:kill).with('TERM', -pid) { alive = false; 1 }
        allow(Process).to receive(:kill).with(0, -pid) { raise Errno::ESRCH unless alive; 1 }

        expect { harness.stop_logstash }.not_to raise_error
        expect(harness.instance_variable_get(:@logstash_status)).to equal(status)
        expect(harness.instance_variable_get(:@logstash_pid)).to be_nil
        expect(Process).not_to have_received(:kill).with('KILL', -pid)
      end
    end

    [[nil, 15], [143, nil]].each do |code, signal|
      it "rejects status #{code.inspect}/signal #{signal.inspect} when TERM was not requested" do
        status = exit_status(code, signal)
        allow(Process).to receive(:waitpid2).with(pid, Process::WNOHANG).and_return([pid, status])
        allow(Process).to receive(:kill).with(0, -pid).and_raise(Errno::ESRCH)

        expect { harness.stop_logstash }.to raise_error(/Logstash.*(?:exit status 143|signal 15)/)
      end
    end

    it 'does not treat an unavailable exit status as success' do
      allow(Process).to receive(:waitpid2).with(pid, Process::WNOHANG).and_raise(Errno::ECHILD)
      allow(Process).to receive(:kill).with(0, -pid).and_raise(Errno::ESRCH)

      expect { harness.stop_logstash }.to raise_error(/exit status.*unavailable/)
    end

    it 'cleans surviving group members without treating a missing leader status as success' do
      allow(Process).to receive(:waitpid2).with(pid, Process::WNOHANG).and_raise(Errno::ECHILD)
      alive = true
      allow(Process).to receive(:kill).with(0, -pid) { raise Errno::ESRCH unless alive; 1 }
      allow(Process).to receive(:kill).with('TERM', -pid) { alive = false; 1 }

      expect { harness.stop_logstash }.to raise_error(/exit status.*unavailable/)
      expect(Process).to have_received(:kill).with('TERM', -pid).once
      expect(harness.instance_variable_get(:@logstash_pid)).to be_nil
    end

    it 'uses a monotonic deadline while waiting for a running child' do
      allow(Process).to receive(:waitpid2).with(pid, Process::WNOHANG).and_return(nil)
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC).and_return(10.0, 10.5, 12.0)
      expect(Time).not_to receive(:now)

      expect(harness.wait_for_exit(pid, 1)).to be_nil
      expect(harness.instance_variable_get(:@logstash_pid)).to eq(pid)
    end

    it 'does not confirm termination merely because the group leader was reaped' do
      allow(Process).to receive(:waitpid2).with(pid, Process::WNOHANG).and_return([pid, exit_status])

      expect(harness.wait_for_exit(pid, 0)).to be_nil
      expect(harness.instance_variable_get(:@logstash_pid)).to eq(pid)
    end

    it 'does not excuse an earlier abnormal leader exit when TERM only stops remaining group members' do
      allow(Process).to receive(:waitpid2).with(pid, Process::WNOHANG).and_return([pid, exit_status(143)])
      alive = true
      allow(Process).to receive(:kill).with(0, -pid) { raise Errno::ESRCH unless alive; 1 }
      allow(Process).to receive(:kill).with('TERM', -pid) { alive = false; 1 }

      expect { harness.stop_logstash }.to raise_error(/exit status 143/)
      expect(harness.instance_variable_get(:@logstash_pid)).to be_nil
    end

    it 'kills lingering group members but reports the forced shutdown as a failure' do
      status = exit_status
      allow(Process).to receive(:waitpid2).with(pid, Process::WNOHANG).and_return([pid, status])
      now = 0.0
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { now += 10.0 }
      alive = true
      allow(Process).to receive(:kill).with(0, -pid) { raise Errno::ESRCH unless alive; 1 }
      allow(Process).to receive(:kill).with('KILL', -pid) { alive = false; 1 }

      expect { harness.stop_logstash }.to raise_error(/required KILL/)
      expect(Process).to have_received(:kill).with('KILL', -pid).once
      expect(harness.instance_variable_get(:@logstash_pid)).to be_nil
      expect(harness.instance_variable_get(:@logstash_status)).to equal(status)
    end

    it 'retains process ownership and fails when TERM and KILL cannot confirm exit' do
      allow(harness).to receive(:wait_for_exit).and_return(nil)

      expect { harness.stop_logstash }.to raise_error(/termination.*not confirmed/)
      expect(harness.instance_variable_get(:@logstash_pid)).to eq(pid)
      expect(harness.instance_variable_get(:@logstash_process_group)).to be(true)
    end

    it 'retains process ownership and reports a signalling failure' do
      allow(harness).to receive(:wait_for_exit).and_return(nil)
      allow(Process).to receive(:kill).with('TERM', -pid).and_raise(Errno::EPERM)

      expect { harness.stop_logstash }.to raise_error(Errno::EPERM)
      expect(harness.instance_variable_get(:@logstash_pid)).to eq(pid)
    end

    it 'refuses to overwrite ownership by starting another process' do
      expect(harness).not_to receive(:spawn)
      expect { harness.run_logstash }.to raise_error(/still tracked/)
      expect(harness.instance_variable_get(:@logstash_pid)).to eq(pid)
    end
  end

  context 'cleanup failures' do
    let(:client) { double('close-capable client', close: nil) }

    before do
      harness.instance_variable_set(:@engine_url, 'https://test.kusto.windows.net')
      harness.instance_variable_set(:@query_client, client)
      allow($kusto_java.data.ClientFactory).to receive(:createClient).and_return(client)
      allow(harness).to receive(:create_table_and_mapping)
      allow(harness).to receive(:run_logstash)
      allow(harness).to receive(:assert_data)
      allow(harness).to receive(:stop_logstash)
    end

    it 'attempts every owned table and retains failed drops for a later cleanup attempt' do
      tables = [['db', 'one'], ['db', 'two'], ['other_db', 'three']]
      harness.instance_variable_set(:@created_tables, tables.dup)
      allow(client).to receive(:executeMgmt)
      failure = RuntimeError.new('first drop failed')
      allow(client).to receive(:executeMgmt).with('db', '.drop table one ifexists').and_raise(failure)

      expect { harness.drop_and_cleanup }.to raise_error { |e| expect(e).to equal(failure) }
      tables.each do |database, table|
        expect(client).to have_received(:executeMgmt).with(database, ".drop table #{table} ifexists").once
      end
      expect(harness.instance_variable_get(:@created_tables)).to eq([tables.first])
      expect(harness).to have_received(:warn).with(/db\.one.*first drop failed/)

      allow(client).to receive(:executeMgmt).with('db', '.drop table one ifexists').and_return(nil)
      expect { harness.drop_and_cleanup }.not_to raise_error
      expect(client).to have_received(:executeMgmt).with('db', '.drop table one ifexists').twice
      expect(harness.instance_variable_get(:@created_tables)).to be_empty
    end

    it 'preserves the validation exception and backtrace while reporting all cleanup failures' do
      primary = RuntimeError.new('validation failed')
      primary.set_backtrace(['validation-origin'])
      allow(harness).to receive(:assert_data).and_raise(primary)
      allow(harness).to receive(:stop_logstash).and_raise('stop failed')
      allow(harness).to receive(:drop_and_cleanup).and_raise('drop failed')
      allow(client).to receive(:close).and_raise('close failed')

      expect { harness.start }.to raise_error do |e|
        expect(e).to equal(primary)
        expect(e.backtrace).to eq(['validation-origin'])
      end
      expect(harness).to have_received(:drop_and_cleanup).once
      expect(client).to have_received(:close).once
      %w[stop drop close].each { |stage| expect(harness).to have_received(:warn).with(/#{stage} failed/) }
    end

    it 'raises the first cleanup failure when validation succeeded and still attempts remaining cleanup' do
      primary = RuntimeError.new('stop failed')
      allow(harness).to receive(:stop_logstash).and_raise(primary)
      allow(harness).to receive(:drop_and_cleanup).and_raise('drop failed')

      expect { harness.start }.to raise_error { |e| expect(e).to equal(primary) }
      expect(harness).to have_received(:drop_and_cleanup).once
      expect(client).to have_received(:close).once
    end

    it 'keeps an input failure primary when stopping Logstash also fails' do
      allow(harness).to receive(:run_logstash).and_call_original
      allow(harness).to receive(:spawn).and_return(pid)
      allow(File).to receive(:read).and_call_original
      allow(File).to receive(:read).with(harness.instance_variable_get(:@csv_file)).and_raise(IOError, 'input failed')
      allow(harness).to receive(:stop_logstash).and_raise('stop failed')
      allow(harness).to receive(:drop_and_cleanup)

      begin
        expect { harness.start }.to raise_error(IOError, 'input failed')
        expect(harness).to have_received(:warn).with(/stop failed/).at_least(:once)
        expect(harness).to have_received(:drop_and_cleanup).once
        expect(client).to have_received(:close).once
      ensure
        FileUtils.rm_rf(harness.instance_variable_get(:@work_directory))
      end
    end

    it 'preserves an interrupt through cleanup rather than replacing it with a drop error' do
      allow(harness).to receive(:assert_data).and_raise(Interrupt, 'interrupted')
      allow(harness).to receive(:drop_and_cleanup).and_raise('drop failed')

      expect { harness.start }.to raise_error(Interrupt, 'interrupted')
      expect(client).to have_received(:close).once
    end
  end

  context 'real POSIX children' do
    before do
      skip 'POSIX process groups' if Gem.win_platform?
      allow(harness).to receive(:sleep).and_call_original
    end

    after do
      if @child_pid
        begin
          Process.kill('KILL', -@child_pid)
        rescue Errno::ESRCH
        end
        begin
          Process.waitpid(@child_pid)
        rescue Errno::ECHILD
        end
      end
      @input_read.close if @input_read && !@input_read.closed?
      @input_write.close if @input_write && !@input_write.closed?
    end

    it 'records a real child exit status of 23' do
      @child_pid = Process.spawn('/bin/sh', '-c', 'exit 23', pgroup: true)
      harness.instance_variable_set(:@logstash_pid, @child_pid)
      harness.instance_variable_set(:@logstash_process_group, true)

      status = harness.wait_for_exit(@child_pid, 5)
      expect(status).to be_a(Process::Status)
      expect(status.exitstatus).to eq(23)
      expect { harness.stop_logstash }.to raise_error(/exit status 23/)
      expect { Process.kill(0, -@child_pid) }.to raise_error(Errno::ESRCH)
    end

    it 'confirms expected TERM termination and absence of the owned group' do
      @input_read, @input_write = IO.pipe
      @child_pid = Process.spawn('/bin/sh', '-c', 'exec cat', in: @input_read, out: File::NULL, pgroup: true)
      @input_read.close
      harness.instance_variable_set(:@logstash_pid, @child_pid)
      harness.instance_variable_set(:@logstash_process_group, true)

      expect { harness.stop_logstash }.not_to raise_error
      expect(harness.instance_variable_get(:@logstash_status).termsig).to eq(Signal.list.fetch('TERM'))
      expect(harness.instance_variable_get(:@logstash_pid)).to be_nil
      expect { Process.kill(0, -@child_pid) }.to raise_error(Errno::ESRCH)
    end
  end
end