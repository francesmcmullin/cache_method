require 'sidekiq'
require 'sidekiq/extensions/class_methods'

module CacheMethod
  class CachedResult #:nodoc: all
    def initialize(obj, method_id, original_method_id, ttl, default, args, async, &blk)
      @obj = obj
      @method_id = method_id
      @method_signature = CacheMethod.method_signature obj, method_id
      @name = @method_signature
      @original_method_id = original_method_id
      @ttl = ttl || CacheMethod.config.default_ttl
      @default = default
      @args = args
      @args_digest = args.empty? ? 'empty' : CacheMethod.digest(args)
      @blk = blk
      @fetch_mutex = ::Mutex.new
      @async = async
    end

    attr_reader :obj
    attr_reader :method_id
    attr_reader :method_signature
    attr_reader :name
    attr_reader :original_method_id
    attr_reader :args
    attr_reader :args_digest
    attr_reader :blk
    attr_reader :ttl
    attr_reader :async

    include Sidekiq::Extensions::Klass

    # Store things wrapped in an Array so that nil is accepted
    def fetch
      wrapped_v = get_wrapped
      if wrapped_v[1] > DateTime.now
        return wrapped_v[0]
      else
        if @fetch_mutex.try_lock
          # i got the lock, so don't bother trying to get first
          begin
            wrapped_v[1] = DateTime.now + ttl.seconds # in the meantime make sure no other process tries to execute it
            CacheMethod.config.storage.set cache_key, wrapped_v, 0
            if @async
                self.delay.set_wrapped #enqueue the refresh job
            else
                wrapped_v = set_wrapped
            end
            wrapped_v[0]
          ensure
            @fetch_mutex.unlock
          end
        else
          # i didn't get the lock, so get in line, and do try to get first
          @fetch_mutex.synchronize do
            get_wrapped.try(:first)
          end
        end
      end
    end

    def exist?
      CacheMethod.config.storage.exist?(cache_key)
    end

    private

    def cache_key
      if obj.is_a?(::Class) or obj.is_a?(::Module)
        [ 'CacheMethod', 'CachedResult', method_signature, current_generation, args_digest ].compact.join CACHE_KEY_JOINER
      else
        [ 'CacheMethod', 'CachedResult', method_signature, CacheMethod.digest(obj), current_generation, args_digest ].compact.join CACHE_KEY_JOINER
      end
    end

    def current_generation
      if CacheMethod.config.generational?
        Generation.new(obj, method_id).fetch
      end
    end

    def get_wrapped
      wrapped_v = CacheMethod.config.storage.get(cache_key) || [@default, DateTime.now]
      wrapped_v
      # if wrapped_v[1] > DateTime.now
      #   wrapped_v
      # else
      #   [@default, DateTime.now]
      # end
    end

    def set_wrapped
      v = obj.send(*([original_method_id]+args), &blk)
      wrapped_v = [v, DateTime.now + ttl.seconds]
      CacheMethod.config.storage.set cache_key, wrapped_v, 0
      wrapped_v
    end

  end
end
