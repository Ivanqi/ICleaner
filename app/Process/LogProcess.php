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
use App\Common\Transformation;
use App\Common\SystemUsage;
use App\ProcessRepositories\kafkaConsumerRepositories;

/**
 * Class LogProcess
 *
 * @since 2.0
 *
 * @Process(workerId={0})
 */
class LogProcess implements ProcessInterface
{
    private static $runProject;
    private static $consumerConf;
    private static $kafkakafkaProducer;
    private static $topicNames;
    private static $consumer;

    public function __construct()
    {
        self::$runProject = (int) config('kafka_config.run_project');
    }

    /**
     * @param Pool $pool
     * @param int  $workerId
     */
    public function run(Pool $pool, int $workerId): void
    { 
        $start_time = microtime(true); 
        $kafkakafkaProducer = kafkaConsumerRepositories::getInstance();
        $consumerConf = $kafkakafkaProducer->kafkaConsumerConf();
        $topicNames = $kafkakafkaProducer->getTopicName();

        $consumer = new \RdKafka\KafkaConsumer($consumerConf);

        $consumer->subscribe($topicNames);

        while (self::$runProject > 0) {
            // $kafkakafkaProducer->kafkaConsumer($consumer, $workerId);
            // $syData = SystemUsage::getCpuWithMem();
            $message = $consumer->consume(kafkaConsumerRepositories::$consumerTime);
            // if ($syData['cpu_idle_rate'] > SystemUsage::$defaultMinCpuIdleRate && $syData['mem_usage'] < SystemUsage::$defaultMaxMemUsage) {
                // self::$kafkakafkaProducer->kafkaConsumer(self::$consumer, $workerId);

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $kafkakafkaProducer->handleConsumerMessage($message);
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    CLog::error('No more message; will wait for more');
                    $end_time = microtime(true); 
                    $execution_time = ($end_time - $start_time); 
                    CLog::error(" 脚本执行时间 = ".$execution_time." 秒");
                    Coroutine::sleep(0.1);
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    // CLog::error('Timed out:'. $workerId);
                    Coroutine::sleep(0.1);
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }
            // }
        }
    }
}