require 'rubygems'
require 'eventmachine'
require 'socket'

module LockServer

  CONNECTION_TIMEOUT = 600 # 10 minutes in seconds

  # Simple resource locking protocol implementation, suitable for EventMachine.
  module ResourceLockServer

    # Returns a queue for specified resource
    #
    # ==== Parameters
    # resource<String>::
    #   A name of resource which query should be returned
    def queue_for(resource)
      p @@resource_queues
      @@resource_queues[resource] ||= []
    end

    # Returns a mutex for specified resource
    #
    # ==== Parameters
    # resource<String>::
    #   A name of resource which mutex should be returned
    def mutex_for(resource)
      @@mutexes[resource] ||= Mutex.new
    end

    # Hash of resource queues. Keys are resource names, values are arrays of connections, trying to acquire resource.
    @@resource_queues = { }
    # Hash of mutexes used for syncronization of queue access. Keys are resource names, values are Mutex instances.
    @@mutexes = { }

    attr_reader :peer_ip
    attr_reader :created_at, :updated_at

    def post_init
      @resource = nil
      @peer_ip = Socket.unpack_sockaddr_in(self.get_peername)[1]
      set_comm_inactivity_timeout(LockServer::CONNECTION_TIMEOUT)
    end

    def send_line(string)
      send_data(string+"\n")
    end

    # A collback of EventMachine, being called when data is received. Used for processing of lock requests.
    # To aquire lock for needed resource name application should send message 'lock resource_name', where resource_name is a
    # name of needed resource.
    # An answer will be either 'ok resource_name' (meaning lock have been obtained and application could proceed) or 'wait resource_name'
    # (there's already lock on this resource, and application should wait).
    # To unlock data, 'unlock' should be sent. This will remove current connection from a queue and a connection will be closed.
    # Sending any other message will result in 'err: unknown message' and connection issued incorrect command will be closed.
    #
    # ==== Parameters
    # data<String>::
    #   Data fetched from EventMachine
    def receive_data(data)
      @updated_at = Time.now
      data.each_line do |line|
        command, resource = line.strip.split(' ')
        case command

        when 'lock'
          obtain_lock(resource)

        when 'unlock'
          send_line("released #{@resource}")
          close_connection_after_writing

        when "status"
          write_server_status
          close_connection_after_writing

        else
          send_line("err: unknown message")
          close_connection_after_writing
        end
      end
    end

    def write_server_status
      @@resource_queues.keys.each{ |k|
        next if queue_for(k).empty?
        send_line("Resource: #{k} (#{queue_for(k).size} connections waiting)")
        send_line(queue_for(k).collect(&:peer_ip).join(", "))
      }
    end

    # Creates a lock for requested resource, acquires mutex to protect resource queue manipulations and
    # either notifies remote side about successful resource aquisition or tells it to wait.
    # First connection in a queue is owning a resource, otherwise a connection will be put on hold for resource aquisition.
    #
    # ==== Parameters
    # resource<String>::
    #   A name of resource to lock.
    def obtain_lock(resource)
      @resource = resource
      queue = queue_for(@resource)
      return if queue.include?(self)
      mutex_for(@resource).synchronize do
        queue << self
        if queue.size == 1
          allow_action
        else
          wait_for_lock_release
        end
      end
    end

    def interrupt_timed_out_connection
      return unless Time.now-@updated_at
    end

    # Releases lock of previously specified resource.
    # Calling this method will remove current connection from @resource aquisitors queue. Also, if current connection is owning a resource (is first in aquisitors queue),
    # it will notify next connection that resource is free now.
    #
    # ==== Parameters
    # none
    def release_lock
      queue = queue_for(@resource)
      mutex_for(@resource).synchronize do
        if queue.first == self
          queue.shift
          queue.first.allow_action unless queue.empty?
        else
          queue.delete self
        end
      end
    end

    # Sends a notification that requested resource is free now.
    def allow_action
      send_line("ok #{@resource}")
    end

    # Sends a notification that requested resource is locked and an application should wait for lock.
    def wait_for_lock_release
      send_line("wait #{@resource}")
    end

    def unbind
      release_lock
    end


  end

  def self.start_server(opts = { })
    opts[:port] ||= 12312
    opts[:host] ||= 'localhost'
    EventMachine::run {
      EventMachine::start_server opts[:host], opts[:port], ResourceLockServer
    }
  end
end

