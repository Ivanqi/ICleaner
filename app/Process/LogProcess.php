<?php declare(strict_types=1);
/**
 * This file is part of Swoft.
 *
 * @link     https://swoft.org
 * @document https://swoft.org/docs
 * @contact  group@swoft.org
 * @license  https://github.com/swoft-cloud/swoft/blob/master/LICENSE
 */

namespace App\Process;

use Swoft\Log\Helper\CLog;
use Swoft\Process\Annotation\Mapping\Process;
use Swoft\Process\Contract\ProcessInterface;
use Swoole\Coroutine;
use Swoole\Process\Pool;
use Swoft\Redis\Redis;

/**
 * Class LogProcess
 *
 * @since 2.0
 *
 * @Process(workerId={0,1,2,3,4})
 */
class LogProcess implements ProcessInterface
{
    private static $queueName;
    private static $faileQueueName;
    private static $maxTimeout;
    private static $conf;
    private static $consumer;

    public function __construct()
    {
        self::$queueName = config('logjob.queue_name');
        self::$faileQueueName = config('logjob.faile_queue_name');
        self::$maxTimeout = config('logjob.queue_max_timeout');

        self::$conf = new \RdKafka\Conf();
        // Set a rebalance callback to log partition assignments (optional)
        self::$conf->setRebalanceCb(function(\RdKafka\KafkaConsumer $kafka, $err, array $partitions = NULL){
            switch ($err) {
                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    CLog::info("Assign:" . json_encode($partitions));
                    $kafka->assign($partitions);
                    break;
                case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                    CLog::info("Revoke:" . json_encode($partitions));
                    $kafka->assign(NULL);
                    break;
                default:
                    throw new \Exception($err);
            }
        });


        // Configure the group.id. All consumer with the same group.id will come
        // different partitions
        self::$conf->set('group.id', 'myConsumerGroup');

        // Initial list of Kafka brokers
        self::$conf->set('metadata.broker.list', '127.0.0.1');
    }
    /**
     * @param Pool $pool
     * @param int  $workerId
     */
    public function run(Pool $pool, int $workerId): void
    { 
        if (self::$consumer == NULL) {
            self::$consumer = new \RdKafka\KafkaConsumer($conf);
        }
         // Subscribe to topic 'test'
        self::$consumer->subscribe(['test1']);
        while (true) {
            $this->logConsumer(self::$consumer);
            usleep(100);
        }
    }


    private function logConsumer(\RdKafka\KafkaConsumer $consumer): void
    {
        $message = $consumer->consume(120 * 10000);
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                var_dump($message);
                break;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                echo "No more message; will wait for more\n";
                break;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                echo "Timed out\n";
                break;
            default:
                throw new \Exception($message->errstr(), $message->err);
        }
    }
}