<?php


use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;

class KafkaQueue extends Queue implements QueueContract
{

    private  $consumer;
    private  $producer;

    public function __construct(\RdKafka\Producer $producer, \RdKafka\KafkaConsumer $consumer)
    {
        $this->consumer = $consumer;
        $this->producer = $producer;
    }

    public function size($queue = null)
    {
        // TODO: Implement size() method.
    }

    public function push($job, $data = '', $queue = null)
    {
        $topic = $this->producer->newTopic($queue ?? env('KAFKA_QUEUE'));
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, serialize($job));
        while ($this->producer->getOutQLen() > 0) {
            $this->producer->poll(0);
        }
    }

    public function pushRaw($payload, $queue = null, array $options = [])
    {
        // TODO: Implement pushRaw() method.
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
        // TODO: Implement later() method.
    }

    /**
     * @throws Exception
     */
    public function pop($queue = null)
    {
        try {
            while (true) {
                $this->consumer->subscribe([$queue]);
                $message = $this->consumer->consume(120 * 1000);

                switch ($message->err) {
                    case RD_KAFKA_RESP_ERR_NO_ERROR:
                        $job = unserialize($message->payload);
                        $job->handle();
                        break;
                    case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                        var_dump("No more messages; will wait for more\n");
                        break;
                    case RD_KAFKA_RESP_ERR__TIMED_OUT:
                        var_dump("Timed out\n");
                        break;
                    default:
                        throw new Exception($message->errstr(), $message->err);
                        break;
                }
            }
        } catch (Exception $e) {
            var_dump($e->getMessage());
        }
    }
}
