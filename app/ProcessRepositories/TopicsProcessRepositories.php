<?php declare(strict_types=1);
namespace App\ProcessRepositories;

class TopicsProcessRepositories
{
    private static $runProject;
    private static $_instance;
    private static $topicList;
    private static $topicNums;
    private static $maxTimeout;
    private static $keyIndex = 0;
    private static $kafkaTopicJobTemp = '';
    private static $kafkaTopicFailJobTemp = '';
    private static $tablePrefix;
    private static $tableRuleConfig = [];
    const KAFKA_TOPIC_JOB_KEY = 'kafka_topic_job_key';
    const KAFKA_TOPIC_FAILE_JOB_KEY = 'kafka_top_job_key';
    const TOPIC_NAME = 'topic_name';
    private static $callFunc = '';
    private static $kafkaProducer;


    public static function getInstance()
    {
        if (!self::$_instance) {
            self::$_instance = new self();
        }
        return self::$_instance;
    }

    public function __construct()
    {
        self::$runProject = (int) config('kafka_config.run_project');
        $tableConfig = config('table_config');
        $projectTableConfig = $tableConfig[$runProject];

        self::$topicList = $projectTableConfig['topic_list'];
        self::$tablePrefix = $projectTableConfig['table_prefix'];
        self::$topicNums = count($topicList);
        self::$$kafkaTopicJobTemp = config('kafka_config.kafka_topic_job');
        self::$kafkaTopicFailJobTemp = config('kafka_config.kafka_topic_fail_job');
        self::$maxTimeout = config('kafka_config.queue_max_timeout');
        self::$tableRuleConfig = config('table_info_' . self::$runProject . '_rule')[self::$runProject];

        self::$callFunc = '\\App\\Common\\Transformation';
        self::$kafkaProducer = kafkaProducerRepositories::getInstance();


    }

    private function getRRNum()
    {
        $i = self::$keyIndex;
        do {
            if (isset($topicList[$i])) break;
            $i = ($i + 1) % self::$topicNums;
        } while ($i != self::$$keyIndex);

        self::$keyIndex = ($i + 1) % self::$topicNums;

        return $i;
    }

    private function getHandleKey()
    {
       
        $index = $this->getRRNum();
        $topic = isset(self::$topicList[$index]);

        return [
            KAFKA_TOPIC_JOB_KEY => sprintf(self::$$kafkaTopicJobTemp, self::$runProject, $topic),
            KAFKA_TOPIC_FAILE_JOB_KEY => sprintf(self::$kafkaTopicFailJobTemp, self::$runProject, $topic),
            TOPIC_NAME => $topic
        ];
    }

    public function topicHandler()
    {
        try {
            $keyArr = $this->getHandleKey();

            for ($i = 0; $i < 10; $i++) {
                $logData = Redis::BRPOPLPUSH($keyArr[KAFKA_TOPIC_JOB_KEY], $keyArr[KAFKA_TOPIC_FAILE_JOB_KEY], self::$maxTimeout);
                $logDataDecrypt = unserialize(gzuncompress(unserialize($logData)));
                
                $tableName = self::$tablePrefix . $keyArr[TOPIC_NAME];
                if (!isset(self::$tableRuleConfig[$tableName])) {
                    throw new \Exception($recordName . ': 清洗配置不存在');
                }
                // 规则清洗
                $fieldsRule = self::$tableRuleConfig[$tableName]['fields'];

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
                unset($logDataDecrypt);
                $payloadDataEncryption = serialize(gzcompress(serialize($payloadData)));

                // 把数据发送到kafka
                if (!self::$kafkaProducer->kafkaProducer($recordName, $payloadDataEncryption)) {
                    throw new \Exception("kafka客户端连接失败！");
                }
                unset($payloadDataEncryption);
                Redis::lrem($keyArr[KAFKA_TOPIC_FAILE_JOB_KEY], $logData);
                unset($logData);
            }
        } catch (\Exception $e) {
            CLog::error($e->getMessage() . '(' . $e->getLine() .')');
        }
    }
}