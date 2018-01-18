<?php
/**
 * Created by IntelliJ IDEA.
 * User: rek
 * Date: 2016/6/7
 * Time: 上午11:19
 */

namespace x2ts\daemon;


use swoole_process;
use x2ts\Component;
use x2ts\ComponentFactory as X;
use x2ts\ExtensionNotLoadedException;


class Daemon extends Component {
    protected static $_conf = [
        'workerNum'     => 1,
        'autoRestart'   => false,
        'daemonize'     => false,
        'name'          => '',
        'onWorkerStart' => null,
        'pidFile'       => X_RUNTIME_ROOT . '/daemon.pid',
        'lockFile'      => X_RUNTIME_ROOT . '/daemon.lock',
        'user'          => '',
        'group'         => '',
    ];

    /**
     * @var int
     */
    private $workerNum;

    /**
     * @var bool
     */
    private $autoRestart;

    /**
     * @var bool
     */
    private $daemonize;

    /**
     * @var string
     */
    private $name;

    /**
     * @var callable
     */
    private $onWorkerStart;

    /**
     * @var array
     */
    private $workers = [];

    /**
     * @var string
     */
    private $pidFile;

    /**
     * @var string
     */
    private $lockFile;

    /**
     * @var bool
     */
    private $isMaster = true;

    /**
     * @var string
     */
    private $user;

    /**
     * @var string
     */
    private $group;

    /**
     * @var array
     */
    private $runtimeConfig;

    /**
     * Daemon constructor.
     *
     * @param array $settings
     *
     * @throws \x2ts\ExtensionNotLoadedException
     */
    public function __construct(array $settings = []) {
        if (!extension_loaded('swoole')) {
            throw new ExtensionNotLoadedException('The x2ts\daemon\Daemon required extension swoole has not been loaded yet');
        }
        $this->runtimeConfig = $settings;
    }

    public function init() {
        foreach ($this->conf as $key => $value) {
            $this->$key = $this->runtimeConfig[$key] ?? $value;
        }
    }

    public function onWorkerStart(callable $callback) {
        $this->onWorkerStart = $callback;
        return $this;
    }

    public function _workerStartDelegate(swoole_process $worker) {
        $this->isMaster = false;

        if ($this->user) {
            X::logger()->trace('Set worker process user to ' . $this->user);
            $info = posix_getpwnam($this->user);
            posix_setuid($info['uid']);
        }
        if ($this->group) {
            X::logger()->trace('Set worker process group to ' . $this->group);
            $info = posix_getgrnam($this->group);
            $gid = $info['gid'];
            posix_setgid($gid);
        }
        if ($this->name) {
            @swoole_set_process_name($this->name . ': worker');
        }
        X::logger()->info($this->name . ' worker start');
        return call_user_func($this->onWorkerStart, $worker);
    }

    public function _signalChild() {
        if (!$this->isMaster) return;
        X::logger()->info('[' . posix_getpid() . '] Receive SIGCHLD');
        /** @noinspection PhpAssignmentInConditionInspection */
        while ($p = swoole_process::wait(false)) {
            unset($this->workers[$p['pid']]);
            if ($this->autoRestart) {
                X::logger()->info('Auto restart');
                $worker = new swoole_process([$this, '_workerStartDelegate']);
                if ($worker->start()) {
                    $this->workers[$worker->pid] = $worker;
                } else {
                    X::logger()->error(swoole_strerror(swoole_errno()));
                }
            } elseif (count($this->workers) === 0) {
                X::logger()->info('All child processes killed, exit');
                exit(0);
            }
        }
    }

    public function _signalTerm() {
        if (!$this->isMaster) return;
        X::logger()->info('Receive SIGTERM');
        $this->autoRestart = false;
        foreach ($this->workers as $worker) {
            swoole_process::kill($worker->pid, SIGQUIT);
        }
        if (is_resource($this->locker)) {
            fclose($this->locker);
            unlink($this->lockFile);
        }
        if ($this->pidFile) {
            unlink($this->pidFile);
        }
    }

    private $locker;

    private function lock() {
        if (false === (bool) $this->lockFile) {
            return true;
        }
        X::logger()->trace('Try to take the lock.');
        if (!is_file($this->lockFile)) {
            $r = touch($this->lockFile);
            if (!$r) {
                $msg = 'Failed to create lock file.';
                X::logger()->error($msg);
                echo $msg, "\n";
                return false;
            }
        }
        $this->locker = @fopen($this->lockFile, 'wb');
        $hasLocked = is_resource($this->locker) ?
            flock($this->locker, LOCK_EX | LOCK_NB) : false;
        if (!$hasLocked) {
            $msg = $this->name . ' master start failed, the lock file ' .
                $this->lockFile . ' has been taken by other process.' . "\n" .
                ' Maybe another daemon is running';
            X::logger()->error($msg);
            echo $msg, "\n";
            return false;
        }
        return true;
    }

    public function run(callable $onWorkerStart = null) {
        if (is_callable($onWorkerStart)) {
            $this->onWorkerStart = $onWorkerStart;
        }
        if (!$this->lock()) return;
        X::logger()->info("{$this->name} master start");
        if ($this->daemonize) {
            swoole_process::daemon(true);
            X::logger()->info('Daemonized');
        }
        if ($this->pidFile) {
            if (false === @file_put_contents($this->pidFile, posix_getpid())) {
                $error = error_get_last()['message'] ?? '';
                X::logger()->warn("Cannot put pid file. $error");
            }
        }
        if ($this->name) {
            @swoole_set_process_name($this->name . ': master');
        }
        swoole_process::signal(SIGTERM, [$this, '_signalTerm']);
        swoole_process::signal(SIGINT, [$this, '_signalTerm']);
        swoole_process::signal(SIGCHLD, [$this, '_signalChild']);
        for ($i = 0; $i < $this->workerNum; $i++) {
            $worker = new swoole_process([$this, '_workerStartDelegate']);
            if ($worker->start())
                $this->workers[$worker->pid] = $worker;
        }
    }
}
