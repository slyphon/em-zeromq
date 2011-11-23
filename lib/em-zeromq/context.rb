#
# different ways to create a socket:
# ctx.bind(:xreq, 'tcp://127.0.0.1:6666')
# ctx.bind('xreq', 'tcp://127.0.0.1:6666')
# ctx.bind(ZMQ::XREQ, 'tcp://127.0.0.1:6666')
#
module EventMachine
  module ZeroMQ
    class Context
      READABLES = [ ZMQ::SUB, ZMQ::PULL, ZMQ::ROUTER, ZMQ::DEALER, ZMQ::REP, ZMQ::REQ ]
      WRITABLES = [ ZMQ::PUB, ZMQ::PUSH, ZMQ::ROUTER, ZMQ::DEALER, ZMQ::REP, ZMQ::REQ ]
      
      def initialize(threads_or_context)
        if threads_or_context.is_a?(ZMQ::Context)
          @context = threads_or_context
        else
          @context = ZMQ::Context.new(threads_or_context)
        end
      end

      def bind(socket_type, address, handler = nil, opts = {}, &blk)
        create(socket_type, :bind, address, handler, opts, &blk)
      end

      def connect(socket_type, address, handler = nil, opts = {}, &blk)
        create(socket_type, :connect, address, handler, opts, &blk)
      end
      
      # block if given will be called with the socket *before* bind or connect is called
      # to allow for setsockopt calls.
      #
      def create(socket_type, bind_or_connect, address, handler, opts = {}, &blk)
        # XXX: raise an EM::ZeroMQ-specific error here
        raise "context closed!" unless @context

        socket_type = find_type(socket_type)
        socket = @context.socket(socket_type)
        
        ident = opts.delete(:identity)
        if ident
          socket.setsockopt(ZMQ::IDENTITY, ident)
        end
        
        unless opts.empty?
          raise "unknown keys: #{opts.keys.join(', ')}"
        end

        blk.call(socket) if blk

        if bind_or_connect == :bind
          socket.bind(address)
        else
          socket.connect(address)
        end
        
        fd = socket.getsockopt(ZMQ::FD)
        conn = EM.watch(fd, EventMachine::ZeroMQ::Connection, socket, socket_type, address, handler)

        if READABLES.include?(socket_type)
          conn.register_readable
        end
        
        if WRITABLES.include?(socket_type)
          conn.register_writable
        end

        conn
      end

      # terminate the underlying ZMQ::Context
      # note that you should shut down all of your sockets BEFORE calling terminate!
      # otherwise BAD THINGS WILL HAPPEN
      #
      # from the guide:
      #
      #   Do not terminate the same context twice. The zmq_term in the main
      #   thread will block until all sockets it knows about are safely closed.
      #
      def terminate
        return unless @context
        ctx, @context = @context, nil
        ctx.terminate
        nil
      end

      # has this context been terminated?
      def closed?
        !@context
      end
      
    private
      def find_type(type)
        if type.is_a?(Symbol) or type.is_a?(String)
          ZMQ.const_get(type.to_s.upcase)
        else
          type
        end
      end
      
    end

  end
end
