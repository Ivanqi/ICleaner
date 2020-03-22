<?php declare(strict_types=1);
namespace App\ProcessRepositories;

use Swoft\Redis\Redis;
class SystemMonitorProcessRepositories
{
    private static $systemMonitorKey = '';
    const SUCCESS_CODE = 1;
    const ERROR_CODE = 0;
    private static $_instance;

    public function __construct()
    {
        $runProject = (int) config('kafka_config.run_project');
        $systemMonitorKeyTemp = config('logjob.system_monitor_key');
        self::$systemMonitorKey = sprintf($systemMonitorKeyTemp, $runProject);
    }

    public static function getInstance()
    {
        if (!self::$_instance) {
            self::$_instance = new self();
        }
        return self::$_instance;
    }

    public function get()
    {
        if (!Redis::EXISTS(self::$systemMonitorKey)){
            $this->set(self::SUCCESS_CODE);
            return self::SUCCESS_CODE;
        }

        return Redis::get(self::$systemMonitorKey);

    }

    public function set($val)
    {
        return Redis::set(self::$systemMonitorKey, $val);
    }
}