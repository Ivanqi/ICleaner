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
        self::$kafkakafkaProducer = kafkaConsumerRepositories::getInstance();
        self::$consumerConf = self::$kafkakafkaProducer->kafkaConsumerConf();
        self::$topicNames = self::$kafkakafkaProducer->getTopicName();
    }

    /**
     * @param Pool $pool
     * @param int  $workerId
     */
    public function run(Pool $pool, int $workerId): void
    { 
        if (self::$consumer == NULL) {
            self::$consumer = new \RdKafka\KafkaConsumer(self::$consumerConf);
        }
        self::$consumer->subscribe(self::$topicNames);

        while (self::$runProject > 0) {
            $syData = SystemUsage::getCpuWithMem();
            if ($syData['cpu_idle_rate'] > SystemUsage::$defaultMinCpuIdleRate && $syData['mem_usage'] < SystemUsage::$defaultMaxMemUsage) {
                self::$kafkakafkaProducer->kafkaConsumer(self::$consumer);
            }
            Coroutine::sleep(0.1);
        }
    }
}