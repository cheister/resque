module Resque
  module Failure
    # A Failure backend that stores exceptions in Redis. Very simple but
    # works out of the box, along with support in the Resque web app.
    class Redis < Base
      def save
        data = {
          :failed_at => Time.now.strftime("%Y/%m/%d %H:%M:%S"),
          :payload   => payload,
          :exception => exception.class.to_s,
          :error     => exception.to_s,
          :backtrace => Array(exception.backtrace),
          :worker    => worker.to_s,
          :queue     => queue
        }
        data = Resque.encode(data)
        Resque.redis.rpush("failed:#{queue}", data)
      end

      def self.count(queue)
        Resque.redis.llen("failed:#{queue}").to_i
      end

      def self.all(queue, start = 0, count = 1)
        Resque.list_range("failed:#{queue}", start, count)
      end

      def self.clear(queue)
        Resque.redis.del("failed:#{queue}")
      end

      def self.requeue(queue, index)
        item = all(queue, index)
        item['retried_at'] = Time.now.strftime("%Y/%m/%d %H:%M:%S")
        Resque.redis.lset("failed:#{queue}", index, Resque.encode(item))
        Job.create(item['queue'], item['payload']['class'], *item['payload']['args'])
      end
    end
  end
end
