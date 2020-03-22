<?php declare(strict_types=1);

namespace App\Process;

use Swoft\Log\Helper\CLog;
use Swoft\Process\Annotation\Mapping\Process;
use Swoft\Process\Contract\ProcessInterface;
use Swoole\Coroutine;
use Swoole\Process\Pool;
use App\ProcessRepositories\TopicsProcessRepositories;
/**
 * Class TopicsProcess
 *
 * @since 2.0
 *
 * @Process(workerId={3})
 */
class TopicsProcess implements ProcessInterface
{
    /**
     * @param Pool $pool
     * @param int  $workerId
     */
    public function run(Pool $pool, int $workerId): void
    { 
        $topicsProcessRepositories = TopicsProcessRepositories::getInstance();
        
        while (true) {
            $topicsProcessRepositories->topicHandler();
            Coroutine::sleep(0.1);
        }
    }
}