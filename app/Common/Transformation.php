<?php declare(strict_types=1);

namespace App\Common;
use SnowflakeIdWorker\IdWorker;
class Transformation
{
    public static $defaultVal = " ";
    private static $extrac_fields = [
        'pid' , 'client_time'
    ];

    private static $allowFunList = [
        'int' => '_int', 
        'bigint' => '_bigint', 
        'string' => '_string'
    ];

    private static $extrac_fields_check = 'extra_field';
    private static $val_check = 'val';
    private static $dataAdapterList = [
        'val', 'extra_field'
    ];
    private static $_instance;
    private static $workerID;



    public static function getInstance($workerID)
    {
        if (!self::$_instance) {
            self::$_instance = new self($workerID);
        }
        return self::$_instance;
    }

    public function __construct($workerID)
    {
        self::$workerID = $workerID;
    }

    public function __call(string $funcName, $args)
    {
        $funcName = strtolower($funcName);
        if (!isset(self::$allowFunList[$funcName])) {
            throw new \Exception('输入不被允许的函数');
        }
        $args = self::dataAdapter($args);
    
        $val = $args[self::$val_check];
        if (isset($args[self::$extrac_fields_check])) {
            $flipExtracFields = array_flip(self::$extrac_fields);
            $extrac_fields = $args[self::$extrac_fields_check];
            if (isset($flipExtracFields[$extrac_fields])) {
                $val = call_user_func([$this , $extrac_fields]);
            }
        }

        return call_user_func([$this , self::$allowFunList[$funcName]], $val);
    }

    private function dataAdapter(array $args): array
    {
        if (empty($args)) {
            throw new \Exception("参数为空");
        }
        $tmp = [];
        foreach(self::$dataAdapterList as $val) {
            $res = array_shift($args);
            if (!$res) continue;
            $tmp[$val] = $res;
        }

        return $tmp;
    }

    private function _int($val): int
    {
        return (int) $val;
    }

    private function _bigint($val): int
    {
        return (int) $val;
    }

    private function _string($val): string
    {
        return (string) $val;
    }

    private function client_time(): int
    {
        return time();
    }

    private function pid(): int
    {
        $idWorker = IdWorker::getInstance(self::$workerID);
        $pid = $idWorker->nextId();
        return $pid;
    }
}