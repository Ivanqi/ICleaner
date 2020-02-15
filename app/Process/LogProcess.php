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

/**
 * Class LogProcess
 *
 * @since 2.0
 *
 * @Process(workerId={0,1,2,3,4})
 */
class LogProcess implements ProcessInterface
{
    private static $runProject;
    private static $kafkaAddr;
    private static $groupId;
    private static $conf;
    private static $consumer;
    private static $topicRule = '';
    private static $topicNames = [];
    private static $tableConfig = [];
    private static $tableRuleConfig = [];
    private static $consumerTime = 0;
    private static $kafkaFailJob = '';
    private static $kafkaPrefix = '';
    private static $callFunc = '';

    public function __construct()
    {
        self::$runProject = (int) config('kafka_config.run_project');
        self::$kafkaAddr = config('kafka_config.kafka_addr');
        self::$groupId = config('kafka_config.kafka_consumer_group');
        self::$tableConfig = config('table_config.' . self::$runProject);
        self::$topicRule = config('kafka_config.kafka_topic_rule');
        self::$consumerTime = config('kafka_config.kafka_consumer_time');
        self::$tableRuleConfig = config('table_info_' . self::$runProject . '_rule')[self::$runProject];
        self::$kafkaFailJob = config('kafka_config.kafka_fail_job');
        self::$kafkaPrefix = config('kafka_config.kafka_topic_prefix');
        self::$callFunc = '\\App\\Common\\Transformation';


        self::$conf = new \RdKafka\Conf();
        
        // Set a rebalance callback to log partition assignments (optional)
        self::$conf->setRebalanceCb(__CLASS__ . '::setRebalanceCb');

        // Configure the group.id. All consumer with the same group.id will come
        // different partitions
        self::$conf->set('group.id', self::$groupId);
        
        // Set where to start consuming messages when there is no initial offset in offset store or the desired offest is out of range.
        // 'smallest': start from the beginning
        self::$conf->set('auto.offset.reset', 'smallest');

        // Initial list of Kafka brokers
        self::$conf->set('metadata.broker.list', self::$kafkaAddr);

        self::$topicNames = self::getTopicName(self::$runProject, self::$tableConfig['topic_list'], self::$kafkaPrefix);
    }


    private static function getTopicName(int $runProject, array $topicList, string $kafkaPrefix) : array
    {
        $topicNameList = [];
        foreach ($topicList as $topic) {
            $topicName = sprintf(self::$topicRule, $kafkaPrefix, $runProject, $topic);
            $topicNameList[] = $topicName;
        }
        return $topicNameList;
    }

    public static function setRebalanceCb(\RdKafka\KafkaConsumer $kafka, $err, array $partitions = NULL): void
    {
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
    }   

    /**
     * @param Pool $pool
     * @param int  $workerId
     */
    public function run(Pool $pool, int $workerId): void
    { 
        if (self::$consumer == NULL) {
            self::$consumer = new \RdKafka\KafkaConsumer(self::$conf);
        }
        self::$consumer->subscribe(self::$topicNames);

        while (self::$runProject > 0) {
            self::kafkaConsumer(self::$consumer);
            Coroutine::sleep(2);
        }
    }

    private static function handleConsumerMessage(\RdKafka\Message $message): void
    {
        try {
            $topicName = $message->topic_name;
            $recordName = substr($topicName, strlen(self::$kafkaPrefix . self::$runProject . '_') + 1);
            if ($message->payload) {
                $recordName = self::$tableConfig['table_prefix'] . $recordName;

                if (!isset(self::$tableRuleConfig[$recordName])) {
                    $failName = sprintf(self::$kafkaFailJob, self::$runProject, $recordName);
                    Redis::PUSH($failName, $message->payload);
                    throw new \Exception($recordName . ': 清洗配置不存在');
                }

                $payload = json_decode($message->payload, true);
                $fieldsRule = self::$tableRuleConfig[$recordName]['fields'];

                $payloadData = [];
                foreach ($payload as $records) {
                    $tmp = [];
                    foreach ($fieldsRule as $fieldsK => $fieldsV) {
                        if (isset($records[$fieldsK])) {
                            $val = \call_user_func_array([self::$callFunc,  $fieldsV['type']], [$records[$fieldsK]]);
                        } else {
                            $val = \call_user_func_array([self::$callFunc, $fieldsV['type']], [Transformation::$defaultVal, $fieldsK]);
                        }
                        $tmp[$fieldsK] = $val;
                    }
                    $payloadData[] = $tmp;
                }
                unset($payload);
                unset($filesRule);

                // 往kafka 重新写入数据
            }
        } catch (\Exception $e) {
            CLog::error($e->getMessage() . '文件：' . $e->getFile(). $e->getLine());
        }
    }

    private static function kafkaProducer()
    {

    }

    private static function kafkaConsumer(\RdKafka\KafkaConsumer $consumer): void
    {
        $message = $consumer->consume(self::$consumerTime);
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                // self::handleConsumerMessage($message);
                break;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                CLog::error('No more message; will wait for more');
                break;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                CLog::error('Timed out');
                break;
            default:
                throw new \Exception($message->errstr(), $message->err);
        }
    }
}