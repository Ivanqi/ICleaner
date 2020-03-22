<?php declare(strict_types=1);

namespace App\Process;

use Swoft\Log\Helper\CLog;
use Swoft\Process\Annotation\Mapping\Process;
use Swoft\Process\Contract\ProcessInterface;
use Swoole\Coroutine;
use Swoole\Process\Pool;
use App\Common\SystemUsage;

/**
 * Class SystemMonitorProcess
 *
 * @since 2.0
 *
 * @Process(workerId={1})
 */
class SystemMonitorProcess implements ProcessInterface
{
    /**
     * SIGUSR1: 为异常情况
     * SIGUSR2: 为正常情况
     */
    private static $signo = SIGUSR2;
    public function run(Pool $pool, int $workerId): void
    {
        $pid = \posix_getpid();
        $prevPid = $pid - 1;
        while (true) {
            $syData = SystemUsage::getCpuWithMem();
            if (($syData['cpu_idle_rate'] < SystemUsage::$defaultMinCpuIdleRate || $syData['mem_usage'] > SystemUsage::$defaultMaxMemUsage) && self::$signo == SIGUSR2 ) {
                $ret = \Swoole\Process::kill($prevPid, 0);
                if ($ret) {
                    self::$signo = SIGUSR1;
                    \Swoole\Process::kill($prevPid, SIGUSR1);
                }
            } else if (self::$signo == SIGUSR1){
                $ret = \Swoole\Process::kill($prevPid, 0);
                if ($ret) {
                    self::$signo = SIGUSR2;
                    \Swoole\Process::kill($prevPid , SIGUSR2);
                }
            }
            Coroutine::sleep(2);
        }
    }
}