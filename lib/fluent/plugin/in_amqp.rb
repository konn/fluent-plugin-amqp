module Fluent
  class AMQPInput < Input
    Fluent::Plugin.register_input('amqp', self)

    config_param :tag, :string, :default => "hunter.amqp"

    config_param :host, :string, :default => nil
    config_param :user, :string, :default => "guest"
    config_param :pass, :string, :default => "guest"
    config_param :vhost, :string, :default => "/"
    config_param :port, :integer, :default => 5672
    config_param :ssl, :bool, :default => false
    config_param :verify_ssl, :bool, :default => false
    config_param :queue, :string, :default => nil
    config_param :durable, :bool, :default => false
    config_param :exclusive, :bool, :default => false
    config_param :auto_delete, :bool, :default => false
    config_param :passive, :bool, :default => false

    def initialize
      require 'bunny'
      super
    end

    
    def configure(conf)
      super
      @conf = conf
      unless @host && @queue
        raise ConfigError, "'host' and 'queue' must be all specified."
      end
      @bunny = Bunny.new(:host => @host, :port => @port, :vhost => @vhost,
                         :pass => @pass, :user => @user, :ssl => @ssl, :verify_ssl => @verify_ssl)
    end
    
    def start
      super
      @thread = Thread.new(&method(:run))
    end
    
    def shutdown
      @bunny.stop
      @thread.join
      super
    end
    
    def run
      @bunny.start
      q = @bunny.queue(@queue, :passive => @passive, :durable => @durable,
                       :exclusive => @exclusive, :auto_delete => @auto_delete)
      q.subscribe do |msg|
        Engine.emit(@tag, Time.new.to_i, msg[:payload])
      end
    end # AMQPInput#run

  end # class AMQPInput

end # module Fluent
