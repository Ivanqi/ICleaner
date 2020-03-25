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
    private static $consumerTime = 0;
    private static $runProject;
    private static $tableConfig = [];
    private static $_instance;
    
    private static $topicRule = '';
    private static $kafkaTestEnv = false;
    private static $kafkaTopicJob = '';
    private static $rdkafkaConsumerConfig = [];

    public function __construct()
    {
        self::$runProject = (int) config('kafka_config.run_project');
        self::$groupId = config('kafka_config.kafka_consumer_group');
        self::$kafkaConsumerAddr = config('kafka_config.kafka_consmer_addr');
        self::$kafkaConsumerPrefix = config('kafka_config.kafka_consumer_topic_prefix');
        self::$kafkaConsumerFailJob = config('kafka_config.kafka_consumer_fail_job');
        self::$tableConfig = config('table_config.' . self::$runProject);
        self::$topicRule = config('kafka_config.kafka_topic_rule');
        
        self::$kafkaTestEnv = config('kafka_config.kafka_test_env');
        self::$kafkaTopicJob = config('kafka_config.kafka_topic_job');
        self::$consumerTime = config('kafka_config.kafka_consumer_time');
        self::$rdkafkaConsumerConfig = config('kafka_config.rdkafka_consumer_config');
    }

    public static function getInstance()
    {
        if (!self::$_instance) {
            self::$_instance = new self();
        }
        return self::$_instance;
    }

    public function getConsumerTime() {
        return  self::$consumerTime;
    }

    public function kafkaConsumerConf(): \RdKafka\Conf
    {
        $conf = new \RdKafka\Conf();
        // Set a rebalance callback to log partition assignments (optional)
        $conf->setRebalanceCb(__CLASS__ . '::setRebalanceCb');
        foreach(self::$rdkafkaConsumerConfig as $key => $data){
            if (isset($data['func'])) {
                $val = call_user_func([$this, $data['func']]);
            } else {
                $val = $data['val'];
            }
            $conf->set($key, $val);
        }
        return $conf;
    }
    public function getGroupId()
    {
        return self::$groupId . self::$runProject;
    }

    public function getBrokerList()
    {
        return self::$kafkaConsumerAddr;
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

    public static function handleConsumerMessage(\RdKafka\Message $message): void
    {
        try {
            if ($message->payload) {
                $topicName = $message->topic_name;
                $recordName = substr($topicName, strlen(self::$kafkaConsumerPrefix . self::$runProject . '_') + 1);
                
                // 把数据写入redis队列中
                // $payloadDataJson = serialize(gzcompress(serialize($message->payload)));
                $jobName = sprintf(self::$kafkaTopicJob, self::$runProject, $recordName);
                Redis::lPush($jobName, $message->payload);
            }
        } catch (\Exception $e) {
            CLog::error($e->getMessage() . '(' . $e->getLine() .')');
        }
    }
}