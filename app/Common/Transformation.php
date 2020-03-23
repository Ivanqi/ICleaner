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

    public static function __callStatic(string $funcName, $args)
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
                $val = call_user_func([__CLASS__ , $extrac_fields]);
            }
        }

        return call_user_func([__CLASS__ , self::$allowFunList[$funcName]], $val);
    }

    private static function dataAdapter(array $args): array
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

    private static function _int($val): int
    {
        return (int) $val;
    }

    private static function _bigint($val): int
    {
        return (int) $val;
    }

    private static function _string($val): string
    {
        return (string) $val;
    }

    private static function client_time(): int
    {
        return time();
    }

    private static function pid(): int
    {
        $idWorker = IdWorker::getInstance();
        $pid = $idWorker->nextId();
        return $pid;
    }
}