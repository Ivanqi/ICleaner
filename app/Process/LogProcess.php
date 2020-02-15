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

/**
 * Class LogProcess
 *
 * @since 2.0
 *
 * @Process(workerId={0,1})
 */
class LogProcess implements ProcessInterface
{
    private static $runProject;
    private static $kafkaConsumerAddr;
    private static $kafkaProducerAddr;
    private static $groupId;
    private static $consumerConf;
    private static $producerConf;
    private static $consumer;
    private static $producer;
    private static $topicRule = '';
    private static $topicNames = [];
    private static $producerTopic = [];
    private static $tableConfig = [];
    private static $tableRuleConfig = [];
    private static $consumerTime = 0;
    private static $kafkaConsumerFailJob = '';
    private static $kafkaProducerFailJob = '';
    private static $kafkaConsumerPrefix = '';
    private static $kafkaProducerPrefix = '';
    private static $callFunc = '';

    public function __construct()
    {
        self::$runProject = (int) config('kafka_config.run_project');
        self::$kafkaConsumerAddr = config('kafka_config.kafka_consmer_addr');
        self::$kafkaProducerAddr = config('kafka_config.kafka_producer_addr');
        self::$groupId = config('kafka_config.kafka_consumer_group');
        self::$tableConfig = config('table_config.' . self::$runProject);
        self::$topicRule = config('kafka_config.kafka_topic_rule');
        self::$consumerTime = config('kafka_config.kafka_consumer_time');
        self::$tableRuleConfig = config('table_info_' . self::$runProject . '_rule')[self::$runProject];
        self::$kafkaConsumerFailJob = config('kafka_config.kafka_consumer_fail_job');
        self::$kafkaProducerFailJob = config('kafka_config.kafka_producer_fail_job');
        self::$kafkaConsumerPrefix = config('kafka_config.kafka_consumer_topic_prefix');
        self::$kafkaProducerPrefix = config('kafka_config.kafka_producer_topic_prefix');

        self::$callFunc = '\\App\\Common\\Transformation';


        self::$consumerConf = self::kafkaConsumerConf();
        self::$producerConf = self::kafkaProducerConf();
        
        self::$topicNames = self::getTopicName(self::$runProject, self::$tableConfig['topic_list'], self::$kafkaConsumerPrefix);
    }

    private static function kafkaProducerConf(): \RdKafka\Conf
    {
        $conf = new \RdKafka\Conf();
        $conf->set('metadata.broker.list', self::$kafkaProducerAddr);
        $conf->set('socket.keepalive.enable', 'true');
        $conf->set('log.connection.close', 'true');
        return $conf;
    }

    private static function kafkaConsumerConf(): \RdKafka\Conf
    {
        $conf = new \RdKafka\Conf();

        // Set a rebalance callback to log partition assignments (optional)
        $conf->setRebalanceCb(__CLASS__ . '::setRebalanceCb');

        // Configure the group.id. All consumer with the same group.id will come
        // different partitions
        $conf->set('group.id', self::$groupId);
         
        // Set where to start consuming messages when there is no initial offset in offset store or the desired offest is out of range.
        // 'smallest': start from the beginning
        $conf->set('auto.offset.reset', 'smallest');
 
        // Initial list of Kafka brokers
        $conf->set('metadata.broker.list', self::$kafkaConsumerAddr);
        return $conf;
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

    public static function setErrorCb($producer, $err, $reason)
    {
        CLog::error(rd_kafka_err2str($err) . ':' . $reason);
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
            self::$consumer = new \RdKafka\KafkaConsumer(self::$consumerConf);
        }
        self::$consumer->subscribe(self::$topicNames);

        while (self::$runProject > 0) {
            $syData = SystemUsage::getCpuWithMem();
            if ($syData['cpu_idle_rate'] > SystemUsage::$defaultMinCpuIdleRate && $syData['mem_usage'] < SystemUsage::$defaultMaxMemUsage) {
                self::kafkaConsumer(self::$consumer);
            }
            Coroutine::sleep(1);
        }
    }

    private static function handleConsumerMessage(\RdKafka\Message $message): void
    {
        try {
            $topicName = $message->topic_name;
            $recordName = substr($topicName, strlen(self::$kafkaConsumerPrefix . self::$runProject . '_') + 1);
            if ($message->payload) {
                $recordName = self::$tableConfig['table_prefix'] . $recordName;

                if (!isset(self::$tableRuleConfig[$recordName])) {
                    $failName = sprintf(self::$kafkaConsumerFailJob, self::$runProject, $recordName);
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
                $payloadDataJson = json_encode($payloadData);
                unset($payloadData);
                if (!self::kafkaProducer($recordName, $payloadDataJson)) {
                    $failName = sprintf(self::$kafkaProducerFailJob, self::$runProject, $recordName);
                    Redis::PUSH($failName, $payloadDataJson);
                    CLog::info("kafka客户端连接失败！");
                }
                unset($payloadDataJson);
            }
        } catch (\Exception $e) {
            CLog::error($e->getMessage() . '(' . $e->getLine() .')');
        }
    }

    private static function kafkaProducer($recordName, string $data): bool
    {
        if (self::$producer == NULL) {
            self::$producer = new \RdKafka\Producer(self::$producerConf);
        }

        if (empty($data)) return false;

        $topicName = sprintf(self::$topicRule, self::$kafkaProducerPrefix, self::$runProject, $recordName);

        if (!isset(self::$producerTopic[$topicName])) {
            self::$producerTopic[$topicName] = self::$producer->newTopic($topicName);
        } else {
            if (self::$producerTopic[$topicName] == NULL) {
                self::$producerTopic[$topicName] = self::$producer->newTopic($topicName);
            }
        }

        if (!self::$producer->getMetadata(false, self::$producerTopic[$topicName], 2 * 1000)) {
            CLog::error('Failed to get metadata, is broker down?');
        }

        self::$producerTopic[$topicName]->produce(RD_KAFKA_PARTITION_UA, 0, $data);
        self::$producer->poll(0);

        while ((self::$producer->getOutQLen())) {
            self::$producer->poll(20);
        }

       return self::$producer->getOutQLen() > 0 ? false : true;
    }

    private static function kafkaConsumer(\RdKafka\KafkaConsumer $consumer): void
    {
        $message = $consumer->consume(self::$consumerTime);
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                self::handleConsumerMessage($message);
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