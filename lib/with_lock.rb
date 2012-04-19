require 'socket'

module WithLock

  class LockTimeOut < Exception
  end

  # Request a lock at remote server, waits for aquisition, and yields a given block.
  # Opens a connection to lock server and performs a communication with it, obtaining lock.
  #
  # ==== Parameters
  # resource<Object>::
  #   A resource to aquire. Will be converted to string with to_str method and sent as a resource name to lock server.
  def with_lock(resource, timeout_period = 20, &block)
    # yield; return

    resource = resource.to_s
    if resource.empty?
      yield; return;
    end
    sock = TCPSocket.new($lock_server_config[:host], $lock_server_config[:port])
    sock.puts "lock #{resource}\n"
    while 1 do
      msg = sock.gets.to_s
      break if msg.strip == "ok #{resource}"
      sleep(0.1)
    end
    Timeout::timeout(timeout_period) { yield }
  ensure
    if sock
      sock.puts("unlock #{resource}\n")
      sock.close
    end
  end
end
