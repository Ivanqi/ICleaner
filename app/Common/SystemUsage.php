<?php declare(strict_types=1);
namespace App\Common;

// 只适用于 Centos
class SystemUsage
{
    public static $CpuCommandInterpretation = [
        [
            'parameter' => 'us',
            'explain' => 'user(通常缩写us),代表用户态的CPU时间，注意，它不包括下面的nice时间，但是包括了guest时间'
        ],
        [
            'parameter' => 'sy',
            'explain' => 'system(通常缩写为sys)，代表内核态CPU时间'
        ],
        [
            'parameter' => 'ni',
            'explain' => 'nice(通常缩写为ni)，代表低优先级用户态CPU时间，也就是进程的nice值被调整为1-19之间时的CPU时间。
            这里注意，nice可取值范围是-20 到 19 ,数值越大，优先级反而越低'
        ],
        [
            'parameter' => 'ni',
            'explain' => 'nice(通常缩写为ni)，代表低优先级用户态CPU时间，也就是进程的nice值被调整为1-19之间时的CPU时间。
            这里注意，nice可取值范围是-20 到 19 ,数值越大，优先级反而越低'
        ],
        [
            'parameter' => 'id',
            'explain' => 'idle(通常缩写为id),代表空闲时间。注意，它不包括等待I/O的时间(iowait)'
        ],
        [
            'parameter' => 'wa',
            'explain' => 'iowait（通常缩写为 wa），代表等待 I/O 的 CPU 时间'
        ],
        [
            'parameter' => 'hi',
            'explain' => 'irq(通常缩写为hi),代表处理硬中断的CPU时间'
        ],
        [
            'parameter' => 'si',
            'explain' => 'softirq(通常缩写为si)，代表处理软中断的CPU时间'
        ],
        [
            'parameter' => 'st',
            'explain' => 'steal(通常缩写为st),代表当系统运行在虚拟机中的时候，被其他虚拟机占用的CPU时间'
        ]
    ];

    private static $cpuTag = '%Cpu';
    private static $memTag = 'KiB Mem';
    public static $defaultMinCpuIdleRate  = 20;
    public static $defaultMaxMemUsage = 85;

    public static function getCpuWithMem(): array
    {
        $output = shell_exec('top -b -n 1 | grep -E "^(' . self::$cpuTag . '|' . self::$memTag . ')"');
    
        $sys_info = explode("\n", $output);

        // CPU 空闲度
        $cpuIndex = 0;
        $idleRateIndex = 3;
        $cpu_info = explode(",", $sys_info[$cpuIndex]);
        $cpu_idle_rate = (float)trim($cpu_info[$idleRateIndex], self::$CpuCommandInterpretation[$idleRateIndex]['parameter']);

        // 内存占有量
        $memIndex = 1;
        $taotalMemIndex = 0;
        $usedMemIndex = 2;
        $mem_info = explode(",", $sys_info[$memIndex]);
        $mem_total = trim(trim($mem_info[$taotalMemIndex], self::$memTag . ': '),'k total');
        $mem_used = trim($mem_info[$usedMemIndex],'k used');
        $mem_usage = round(100 * intval($mem_used) / intval($mem_total), 2);

        return [
            'cpu_idle_rate' => $cpu_idle_rate,
            'mem_usage' => $mem_usage
        ];
    }

    public static function getCpuIdleRate(): float
    {
        $data = self::getCpuWithMem();
        $tag = 'cpu_idle_rate';
        return isset($data[$tag]) ? $data[$tag] : 0.0;
    }

    public static function getMemUsage(): float
    {
        $data = self::getCpuWithMem();
        $tag = 'mem_usage';
        return isset($data[$tag]) ? $data[$tag] : 0.0;
    }

    public static function getHardDiskUsage(): int
    {
        $output = shell_exec('df -lh | grep -E "^(/)"');
        $rs = preg_replace("/\s{2,}/",' ',$output);  //把多个空格换成 “_”
        $hd = explode(" ",$rs);
        $hd_avail = trim($hd[3],'G'); //磁盘可用空间大小 单位G
        $hd_usage = trim($hd[4],'%'); //挂载点 百分比
        return $hd_usage;

    }

}