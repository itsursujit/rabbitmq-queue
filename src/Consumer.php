<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ;

use Bugsnag\BugsnagLaravel\Facades\Bugsnag;
use Exception;
use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job;
use Illuminate\Queue\Worker;
use Illuminate\Queue\WorkerOptions;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use Symfony\Component\Debug\Exception\FatalThrowableError;
use Throwable;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Jobs\RabbitMQJob;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\RabbitMQQueue;

class Consumer extends Worker
{
    /** @var Container */
    protected $container;

    /** @var string */
    protected $consumerTag;

    /** @var string */
    protected $mode;

    /** @var int */
    protected $prefetchSize;

    /** @var int */
    protected $prefetchCount;

    /** @var AMQPChannel */
    protected $channel;

    public function setContainer(Container $value): void
    {
        $this->container = $value;
    }

    public function setConsumerTag(string $value): void
    {
        $this->consumerTag = $value;
    }

    public function setPrefetchSize(int $value): void
    {
        $this->prefetchSize = $value;
    }

    public function setPrefetchCount(int $value): void
    {
        $this->prefetchCount = $value;
    }

    public function daemon($connectionName, $queue, WorkerOptions $options): void
    {
        if ($this->supportsAsyncSignals()) {
            $this->listenForSignals();
        }

        $lastRestart = $this->getTimestampOfLastQueueRestart();

        /** @var RabbitMQQueue $connection */
        $connection = $this->manager->connection($connectionName);

        $this->channel = $connection->getChannel();
        $this->channel->basic_qos(
            $this->prefetchSize,
            $this->prefetchCount,
            null
        );
        $queueName = explode(',', $queue);
        if ($this->mode === 'web') {
            $this->mode = 'web-';
        }
        foreach ($queueName as $name) {
            $name = $this->mode . $name;
            $args = [
                'x-dead-letter-exchange' => $name,
                'x-dead-letter-routing-key' => $name,
            ];
            $this->channel->queue_declare(
                $name,
                false,
                true,
                false,
                false,
                false
            );
            $this->channel->basic_consume(
                $name,
                '',
                false,
                false,
                false,
                false,
                function (AMQPMessage $message) use ($connection, $options, $connectionName, $name): void {
                    $job = new RabbitMQJob(
                        $this->container,
                        $connection,
                        $message,
                        $connectionName,
                        $name
                    );

                    if ($this->supportsAsyncSignals()) {
                        $this->registerTimeoutHandler($job, $options);
                    }

                    $this->runJob($job, $connectionName, $options);
                }
            );
        }


        while ($this->channel->is_consuming()) {
            if (!$this->daemonShouldRun($options, $connectionName, $queue)) {
                $this->pauseWorker($options, $lastRestart);
                continue;
            }
            try {
                $this->channel->wait();
            } catch (AMQPRuntimeException $exception) {
                $this->exceptions->report($exception);

                $this->kill(1);
            } catch (Exception $exception) {
                $this->exceptions->report($exception);

                $this->stopWorkerIfLostConnection($exception);
            } catch (Throwable $exception) {
                $this->exceptions->report($exception = new FatalThrowableError($exception));

                $this->stopWorkerIfLostConnection($exception);
            }
            $this->stopIfNecessary($options, $lastRestart, null);
        }
    }

    /**
     * Process the given job.
     *
     * @param \Illuminate\Contracts\Queue\Job $job
     * @param string $connectionName
     * @param \Illuminate\Queue\WorkerOptions $options
     * @return void
     */
    protected function runJob($job, $connectionName, WorkerOptions $options)
    {
        try {
            return $this->process($connectionName, $job, $options);
        } catch (Exception $e) {
            $this->exceptions->report($e);

            $this->stopWorkerIfLostConnection($e);
        } catch (Throwable $e) {
            $this->exceptions->report($e = new FatalThrowableError($e));

            $this->stopWorkerIfLostConnection($e);
        }
    }

    /**
     * Process the given job from the queue.
     *
     * @param string $connectionName
     * @param \Illuminate\Contracts\Queue\Job $job
     * @param \Illuminate\Queue\WorkerOptions $options
     * @return void
     *
     * @throws \Throwable
     */
    public function process($connectionName, $job, WorkerOptions $options)
    {
        try {
            $this->raiseBeforeJobEvent($connectionName, $job);

            $this->markJobAsFailedIfAlreadyExceedsMaxAttempts(
                $connectionName, $job, (int)$options->maxTries
            );

            if ($job->isDeleted()) {
                return $this->raiseAfterJobEvent($connectionName, $job);
            }
            $job->fire();

            $this->raiseAfterJobEvent($connectionName, $job);
        } catch (Exception $e) {
            Bugsnag::notifyException($e);
            $job->fail($e);
        } catch (Throwable $e) {
            Bugsnag::notifyException($e);
            $job->fail($e);
        }
    }

    public function reject(RabbitMQJob $job, bool $requeue = false): void
    {
        $this->channel->basic_reject($job->getRabbitMQMessage()->getDeliveryTag(), $requeue);
    }

    /**
     * Determine if the daemon should process on this iteration.
     *
     * @param WorkerOptions $options
     * @param string $connectionName
     * @param string $queue
     * @return bool
     */
    protected function daemonShouldRun(WorkerOptions $options, $connectionName, $queue): bool
    {
        return !((($this->isDownForMaintenance)() && !$options->force) || $this->paused);
    }

    public function setMode(string $getModeOption)
    {
        $this->mode = $getModeOption;
    }

    /**
     * Mark the given job as failed if it has exceeded the maximum allowed attempts.
     *
     * @param string $connectionName
     * @param Job|RabbitMQJob $job
     * @param int $maxTries
     * @param Exception $e
     */
    protected function markJobAsFailedIfWillExceedMaxAttempts($connectionName, $job, $maxTries, $e): void
    {
        parent::markJobAsFailedIfWillExceedMaxAttempts($connectionName, $job, $maxTries, $e);

        if (!$job->isDeletedOrReleased()) {
            $job->getRabbitMQ()->reject($job);
        }
    }

    /**
     * Stop listening and bail out of the script.
     *
     * @param int $status
     * @return void
     */
    public function stop($status = 0): void
    {
        $this->channel->basic_cancel($this->consumerTag, false, true);

        parent::stop($status);
    }
}
