<?php declare(strict_types=1);
namespace App\ProcessRepositories;

use Swoft\Redis\Redis;
use Swoft\Log\Helper\CLog;
use App\Common\Transformation;

class kafkaConsumerRepositories
{
    private static $groupId;
    private static $kafkaConsumerAddr;
    private static $kafkaConsumerFailJob = '';
    private static $kafkaConsumerPrefix = '';
    public static $consumerTime = 0;
    private static $runProject;
    private static $tableConfig = [];
    private static $_instance;
    private static $callFunc = '';
    private static $topicRule = '';
    private static $tableRuleConfig = [];
    private static $kafkaProducer;
    private static $kafkaTestEnv = false;
    private static $kafkaTopicJob = '';

    public function __construct()
    {
        self::$runProject = (int) config('kafka_config.run_project');
        self::$groupId = config('kafka_config.kafka_consumer_group');
        self::$kafkaConsumerAddr = config('kafka_config.kafka_consmer_addr');
        self::$kafkaConsumerPrefix = config('kafka_config.kafka_consumer_topic_prefix');
        self::$kafkaConsumerFailJob = config('kafka_config.kafka_consumer_fail_job');
        self::$tableConfig = config('table_config.' . self::$runProject);
        self::$topicRule = config('kafka_config.kafka_topic_rule');
        self::$tableRuleConfig = config('table_info_' . self::$runProject . '_rule')[self::$runProject];
        self::$kafkaProducer = kafkaProducerRepositories::getInstance();
        self::$kafkaTestEnv = config('kafka_config.kafka_test_env');
        self::$kafkaTopicJob = config('kafka_config.kafka_topic_job');


        self::$callFunc = '\\App\\Common\\Transformation';
    }

    public static function getInstance()
    {
        if (!self::$_instance) {
            self::$_instance = new self();
        }
        return self::$_instance;
    }

    public function kafkaConsumerConf(): \RdKafka\Conf
    {
        $conf = new \RdKafka\Conf();
        // Set a rebalance callback to log partition assignments (optional)
        $conf->setRebalanceCb(__CLASS__ . '::setRebalanceCb');

        // Configure the group.id. All consumer with the same group.id will come
        // different partitions
        $conf->set('group.id', self::$groupId . self::$runProject . '106');
         
        // Set where to start consuming messages when there is no initial offset in offset store or the desired offest is out of range.
        // 'smallest': start from the beginning
        if (self::$kafkaTestEnv) {
            $conf->set('auto.offset.reset', 'smallest');
        }
        // Initial list of Kafka brokers
        $conf->set('metadata.broker.list', self::$kafkaConsumerAddr);
        $conf->set('socket.keepalive.enable', 'true');
        $conf->set('log.connection.close', 'false');
        // $conf->set('session.timeout.ms', '400000');
        // $conf->set('max.partition.fetch.bytes', '848576');

        return $conf;
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

    public function getTopicName() : array
    {
        $topicNameList = [];
        foreach (self::$tableConfig['topic_list'] as $topic) {
            $topicName = sprintf(self::$topicRule, self::$kafkaConsumerPrefix, self::$runProject, $topic);
            $topicNameList[] = $topicName;
        }
        return $topicNameList;
    }

    // public function kafkaConsumer(\RdKafka\KafkaConsumer $consumer, $workerId): void
    // {
    //     $message = $consumer->consume(self::$consumerTime);
    //     switch ($message->err) {
    //         case RD_KAFKA_RESP_ERR_NO_ERROR:
    //             // self::handleConsumerMessage($message);
    //             break;
    //         case RD_KAFKA_RESP_ERR__PARTITION_EOF:
    //             CLog::error('No more message; will wait for more');
    //             break;
    //         case RD_KAFKA_RESP_ERR__TIMED_OUT:
    //             CLog::error('Timed out:'. $workerId);
    //             break;
    //         default:
    //             CLog::error($message->errstr() . '(' . $message->err .')');
    //     }
    // }

    public static function handleConsumerMessage(\RdKafka\Message $message): void
    {
        try {
            if ($message->payload) {
                $topicName = $message->topic_name;
                $recordName = substr($topicName, strlen(self::$kafkaConsumerPrefix . self::$runProject . '_') + 1);
                // $tableName = self::$tableConfig['table_prefix'] . $recordName;
                // if (!isset(self::$tableRuleConfig[$tableName])) {
                //     $failName = sprintf(self::$kafkaConsumerFailJob, self::$runProject, $recordName);
                //     Redis::lPush($failName, $message->payload);
                //     throw new \Exception($recordName . ': 清洗配置不存在');
                // }
                $payload = json_decode($message->payload, true);
                
                // $fieldsRule = self::$tableRuleConfig[$tableName]['fields'];

                // $payloadData = [];
                // foreach ($payload as $records) {
                //     $tmp = [];
                //     foreach ($fieldsRule as $fieldsK => $fieldsV) {
                //         if (isset($records[$fieldsK])) {
                //             $val = \call_user_func_array([self::$callFunc,  $fieldsV['type']], [$records[$fieldsK]]);
                //         } else {
                //             $val = \call_user_func_array([self::$callFunc, $fieldsV['type']], [Transformation::$defaultVal, $fieldsK]);
                //         }
                //         $tmp[$fieldsK] = $val;
                //     }
                //     $payloadData[] = $tmp;
                // }
                // unset($payload);
                // unset($filesRule);

                // 往kafka 重新写入数据
                $payloadDataJson = serialize(gzcompress(serialize($payload)));
                $jobName = sprintf(self::$kafkaTopicJob, self::$runProject, $recordName);
                Redis::lPush($jobName, $payloadDataJson);

                // if (!self::$kafkaProducer->kafkaProducer($recordName, $payloadDataJson)) {
                //     $failName = sprintf(self::$kafkaProducerFailJob, self::$runProject, $recordName);
                //     Redis::lPush($failName, $payloadDataJson);
                //     throw new \Exception("kafka客户端连接失败！");
                // }
                unset($payload);
                // unset($payloadData);
                unset($payloadDataJson);
            }
        } catch (\Exception $e) {
            CLog::error($e->getMessage() . '(' . $e->getLine() .')');
        }
    }
}