<?php declare(strict_types=1);
namespace App\ProcessRepositories;
use Swoft\Log\Helper\CLog;
use Swoft\Redis\Redis;
use App\Common\Transformation;

class TopicsProcessRepositories
{
    private static $runProject;
    private static $_instance;
    private static $topicList;
    private static $topicNums;
    private static $maxTimeout;
    private static $maxTimes;
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
        $projectTableConfig = $tableConfig[self::$runProject];

        self::$topicList = $projectTableConfig['topic_list'];
        self::$tablePrefix = $projectTableConfig['table_prefix'];
        self::$topicNums = count(self::$topicList);
        self::$kafkaTopicJobTemp = config('kafka_config.kafka_topic_job');
        self::$kafkaTopicFailJobTemp = config('kafka_config.kafka_topic_fail_job');
        self::$maxTimeout = config('kafka_config.queue_max_timeout');
        self::$tableRuleConfig = config('table_info_' . self::$runProject . '_rule')[self::$runProject];
        self::$maxTimes = config('kafka_config.queue_max_times');

        self::$callFunc = '\\App\\Common\\Transformation';
        self::$kafkaProducer = kafkaProducerRepositories::getInstance();


    }

    private function getRRNum()
    {
        $i = self::$keyIndex;
        do {
            if (isset($topicList[$i])) break;
            $i = ($i + 1) % self::$topicNums;
        } while ($i != self::$keyIndex);

        self::$keyIndex = ($i + 1) % self::$topicNums;

        return $i;
    }

    private function getHandleKey()
    {
       
        $index = $this->getRRNum();
        if (isset(self::$topicList[$index])) {
            $topic = self::$topicList[$index];
        } else {
            $topic = self::$topicList[0];
        }
        
        return [
            self::KAFKA_TOPIC_JOB_KEY => sprintf(self::$kafkaTopicJobTemp, self::$runProject, $topic),
            self::KAFKA_TOPIC_FAILE_JOB_KEY => sprintf(self::$kafkaTopicFailJobTemp, self::$runProject, $topic),
            self::TOPIC_NAME => $topic
        ];
    }

    public function topicHandler()
    {
        try {
            $keyArr = $this->getHandleKey();
            $kafkaData = [];
            $failData = [];
            for ($i = 0; $i < self::$maxTimes; $i++) {
                $logData = Redis::BRPOPLPUSH($keyArr[self::KAFKA_TOPIC_JOB_KEY], $keyArr[self::KAFKA_TOPIC_FAILE_JOB_KEY], self::$maxTimeout);
                if (empty($logData)) continue;
                $failData[] = $logData;
                $logDataDecrypt = unserialize(gzuncompress(unserialize($logData)));
                $tableName = self::$tablePrefix . $keyArr[self::TOPIC_NAME];
                if (!isset(self::$tableRuleConfig[$tableName])) {
                    throw new \Exception($keyArr[self::TOPIC_NAME] . ': 清洗配置不存在');
                }
                // 规则清洗
                $fieldsRule = self::$tableRuleConfig[$tableName]['fields'];

                $palyloadPrev = [];
                $palyloadNext = [];
                $dataNum = count($logDataDecrypt);
                $half = ceil($dataNum  / 2);
                for ($i = 0; $i < $half; $i++) {
                    $palyloadPrev[$i] = $this->ruleCleaning($fieldsRule, $logDataDecrypt[$i]);
                    $next = $half + $i;
                    if ($next < $dataNum) {
                        if (isset($logDataDecrypt[$next])) {
                            $palyloadNext[$next] = $this->ruleCleaning($fieldsRule, $logDataDecrypt[$i]);
                        }
                    }
                }
                unset($logDataDecrypt);
                unset($logData);
                $kafkaData = array_merge($kafkaData, $palyloadPrev, $palyloadNext);
            }
            if (!empty($kafkaData)) {
                $payloadDataEncryption = serialize(gzcompress(serialize($kafkaData)));
                unset($kafkaData);
                // 把数据发送到kafka
                if (!self::$kafkaProducer->kafkaProducer($keyArr[self::TOPIC_NAME], $payloadDataEncryption)) {
                    throw new \Exception("kafka客户端连接失败！");
                }
                unset($payloadDataEncryption);
            }
            if (!empty($failData)) {
                foreach($failData as $failLog) {
                    Redis::lrem($keyArr[self::KAFKA_TOPIC_FAILE_JOB_KEY], $failLog);
                }
                unset($failData);
            }
        } catch (\Exception $e) {
            CLog::error($e->getMessage() . '(' . $e->getLine() .')');
        }
    }

    private function ruleCleaning($fieldsRule, $records)
    {
        $tmp = [];
        foreach ($fieldsRule as $fieldsK => $fieldsV) {
            if (isset($records[$fieldsK])) {
                $val = \call_user_func_array([self::$callFunc,  $fieldsV['type']], [$records[$fieldsK]]);
            } else {
                $val = \call_user_func_array([self::$callFunc, $fieldsV['type']], [Transformation::$defaultVal, $fieldsK]);
            }
            $tmp[$fieldsK] = $val;
        }
        return $tmp;
    }
}