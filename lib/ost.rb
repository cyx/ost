require "nest"

module Ost
  VERSION = "0.0.3"
  TIMEOUT = ENV["OST_TIMEOUT"] || 2

  class Queue
    attr :ns

    def initialize(name)
      @ns = Nest.new(:ost)[name]
    end

    def push(value)
      redis.lpush(ns, value)
    end

    def each(&block)
      @stopping = false

      loop do
        break if @stopping

        backup = ns[Process.pid][:backup]

        item = redis.brpoplpush(ns, backup, TIMEOUT)

        next if item.nil? or item.empty?

        begin
          yield item
        ensure
          backup.del
        end
      end
    end

    def stop
      @stopping = true
    end

    alias << push
    alias pop each

    def redis
      @redis ||= Redis.connect(Ost.options)
    end
  end

  @queues = Hash.new do |hash, key|
    hash[key] = Queue.new(key)
  end

  def self.[](queue)
    @queues[queue]
  end

  def self.stop
    @queues.each { |_, queue| queue.stop }
  end

  @options = nil

  def self.connect(options = {})
    @options = options
  end

  def self.options
    @options || {}
  end
end
