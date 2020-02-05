<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Console;

use Illuminate\Queue\Console\WorkCommand;
use Illuminate\Support\Str;
use VladimirYuldashev\LaravelQueueRabbitMQ\Consumer;

class ConsumeCommand extends WorkCommand
{
    protected $signature = 'smsto-queue:work
                            {connection? : The name of the queue connection to work}
                            {--queue= : The names of the queues to work}
                            {--once : Only process the next job on the queue}
                            {--stop-when-empty : Stop when the queue is empty}
                            {--delay=0 : The number of seconds to delay failed jobs}
                            {--force : Force the worker to run even in maintenance mode}
                            {--memory=512 : The memory limit in megabytes}
                            {--sleep=3 : Number of seconds to sleep when no job is available}
                            {--timeout=6000 : The number of seconds a child process can run}
                            {--tries=1 : Number of times to attempt a job before logging it failed}

                            {--consumer-tag}
                            {--prefetch-size=0}
                            {--prefetch-count=10}
                            {--mode=api}
                           ';

    protected $description = 'Consume messages';

    public function handle(): void
    {
        /** @var Consumer $consumer */
        $consumer = $this->worker;

        $consumer->setContainer($this->laravel);
        $consumer->setConsumerTag($this->consumerTag());
        $consumer->setMode($this->getModeOption());
        $consumer->setPrefetchSize((int) $this->option('prefetch-size'));
        $consumer->setPrefetchCount((int) $this->option('prefetch-count'));

        parent::handle();
    }

    protected function consumerTag(): string
    {
        if ($consumerTag = $this->option('consumer-tag')) {
            return $consumerTag;
        }

        return Str::slug(config('app.name', 'laravel'), '_').'_'.getmypid();
    }

    protected function getModeOption(): string
    {
        if ($mode = $this->option('mode')) {
            if($mode === 'web')
            {
                return $mode;
            }
        }

        return '';
    }

    /**
     * Format the status output for the queue worker.
     *
     * @param  \Illuminate\Contracts\Queue\Job  $job
     * @param  string  $status
     * @param  string  $type
     * @return void
     */
    protected function writeStatus($job, $status, $type)
    {
        try
        {
            $command = unserialize($job->payload()['data']['command']);
            $jobName = str_replace('App\\Jobs\\', '', $job->resolveName());
            $tags = [
                'Job' => $jobName,
                'Status' => $status
            ];
            if (property_exists($command, 'tags'))
            {
                $tags = array_merge($tags, $command->tags);
            } else
            {
                $tags['JobId'] = $job->getJobId();
            }
            $output = json_encode($tags);
            $this->output->writeln(sprintf("<{$type}>%s", $output));
        } catch (\Throwable $exc)
        {
            parent::writeStatus($job, $status, $type);
        }
    }
}
