require File.expand_path("test_helper", File.dirname(__FILE__))

scope do
  def ost(&job)
    thread = Thread.new do
      Ost[:events].each(&job)
    end

    sleep 0.1

    thread.kill
  end

  def enqueue(id)
    Ost[:events].push(id)
  end

  prepare do
    Redis.current.flushall
  end

  setup do
    Ost[:events].redis.quit
    Redis.new
  end

  test "insert items in the queue" do |redis|
    enqueue(1)
    assert_equal ["1"], redis.lrange("ost:events", 0, -1)
  end

  test "process items from the queue" do |redis|
    enqueue(1)

    results = []

    ost do |item|
      results << item
    end

    assert_equal [], redis.lrange("ost:events", 0, -1)
    assert_equal ["1"], results
  end

  test "pushes to a backup queue" do |redis|
    enqueue(1)

    # Let's simulate a server crash by making
    # String#empty? raise.

    class String
      alias :_empty? :empty?

      def empty?
        raise RuntimeError
      end
    end

    begin
      ost do |item|
      end
    rescue
    end

    # Now let's put it back.
    class String
      remove_method :empty?
      alias :empty? :_empty?
    end

    assert_equal ["1"], redis.lrange("ost:events:%s:backup" % Process.pid, 0, -1)
  end

  test "halt processing a queue" do
    Thread.new do
      sleep 0.5
      Ost[:always_empty].stop
    end

    Ost[:always_empty].each { }

    assert true
  end

  test "halt processing all queues" do
    Thread.new do
      sleep 0.5
      Ost.stop
    end

    t1 = Thread.new { Ost[:always_empty].each { } }
    t2 = Thread.new { Ost[:always_empty_too].each { } }

    t1.join
    t2.join

    assert true
  end
end
