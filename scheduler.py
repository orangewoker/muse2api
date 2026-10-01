"""Bounded FIFO browser scheduler; one worker owns the browser lock.

Queue expiry is independent of the worker. Every terminal path invokes on_done
exactly once. The watchdog interrupts the browser, never releases another
thread's lock and never starts a second worker over a still-running job.
"""
from __future__ import annotations

import logging
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

log = logging.getLogger("muse2api.scheduler")
ST_QUEUED, ST_RUNNING = "queued", "processing"
ST_DONE, ST_FAILED, ST_TIMEOUT = "completed", "failed", "timeout"


class QueueTimeout(TimeoutError):
    pass


class RunTimeout(TimeoutError):
    pass


class SchedulerStopped(RuntimeError):
    pass


@dataclass
class _Job:
    seq: int
    fn: Callable
    label: str
    enqueued_at: float
    deadline: float
    on_done: Callable | None = None
    started: bool = False
    started_at: float = 0.0
    finished: bool = False
    error: BaseException | None = None
    result_value: Any = None
    wait_event: threading.Event = field(default_factory=threading.Event)
    state_lock: Any = field(default_factory=threading.RLock)
    queue_timer: Any = None
    run_timer: Any = None


class Scheduler:
    def __init__(self, lock, max_queue=100, queue_timeout=900, run_timeout=900):
        if max_queue < 1:
            raise ValueError("max_queue 必须大于 0")
        self.lock = lock
        self.max_queue = max_queue
        self.queue_timeout = queue_timeout
        self.run_timeout = run_timeout
        self.on_run_timeout = None
        self._q = queue.Queue(maxsize=max_queue)
        self._state = threading.RLock()
        self._worker = None
        self._stopped = False
        self._seq = 0
        self._jobs = {}
        self._running_job = None
        self.metrics = dict.fromkeys(
            ("submitted", "completed", "failed", "timeout", "rejected", "run_timeout"), 0)

    def start(self):
        with self._state:
            if self._worker and self._worker.is_alive():
                if self._stopped:
                    raise SchedulerStopped("生成调度器正在关闭")
                return
            self._stopped = False
            self._worker = threading.Thread(target=self._loop, name="gen-worker", daemon=True)
            self._worker.start()

    def stop(self):
        with self._state:
            self._stopped = True
            jobs = list(self._jobs.values())
        for job in jobs:
            self.cancel(job, SchedulerStopped("服务正在关闭，任务已中断"))

    @property
    def queue_size(self):
        with self._state:
            return sum(not j.started and not j.finished for j in self._jobs.values())

    @property
    def running(self):
        with self._state:
            job = self._running_job
            if job is None:
                return None
            return {"label": job.label, "started_at": job.started_at,
                    "elapsed": round(time.time() - job.started_at, 1),
                    "enqueued_at": job.enqueued_at,
                    "waited": round(job.started_at - job.enqueued_at, 1),
                    "interrupted": job.finished}

    def stats(self):
        with self._state:
            return {**self.metrics, "queue_size": self.queue_size,
                    "worker_alive": bool(self._worker and self._worker.is_alive()),
                    "running": self.running}

    def submit(self, fn, label="", queue_timeout=None, on_done=None):
        # Admission, sequence and insertion are atomic across concurrent submitters.
        with self._state:
            if self._stopped:
                raise SchedulerStopped("生成调度器已关闭")
            self.start()
            self._seq += 1
            qto = self.queue_timeout if queue_timeout is None else queue_timeout
            now = time.time()
            job = _Job(self._seq, fn, label or f"job#{self._seq}", now,
                       time.monotonic() + qto if qto > 0 else 0, on_done)
            try:
                self._q.put_nowait(job)
            except queue.Full:
                self.metrics["rejected"] += 1
                raise
            self._jobs[job.seq] = job
            self.metrics["submitted"] += 1
            if qto > 0:
                job.queue_timer = threading.Timer(qto, self._expire, args=(job,))
                job.queue_timer.daemon = True
                job.queue_timer.start()
            return job

    def _finish(self, job, error=None, result=None):
        with job.state_lock:
            if job.finished:
                return False
            job.finished, job.error, job.result_value = True, error, result
            for timer in (job.queue_timer, job.run_timer):
                if timer:
                    timer.cancel()
        with self._state:
            self._jobs.pop(job.seq, None)
            metric = ("timeout" if isinstance(error, QueueTimeout) else
                      "run_timeout" if isinstance(error, RunTimeout) else
                      "failed" if error else "completed")
            self.metrics[metric] += 1
        try:
            if job.on_done:
                job.on_done(job)
        except Exception:
            log.exception("Task completion callback failed: %s", job.label)
        finally:
            job.wait_event.set()
        return True

    def _expire(self, job):
        with job.state_lock:
            expired = not job.started and not job.finished
        if expired:
            # Recheck atomically against the worker's start transition.
            self._finish_queued(job, QueueTimeout("任务排队超时，请稍后重新提交"))

    def _finish_queued(self, job, error):
        with self._state:
            with job.state_lock:
                if job.started or job.finished:
                    return
                self._finish(job, error)

    def cancel(self, job, error=None):
        self._finish(job, error or QueueTimeout("调用方已取消任务"))

    def _watchdog(self, job):
        # A late timer must not kill the browser belonging to the next job.
        with self._state:
            if self._running_job is not job or job.finished:
                return
            self._finish(job, RunTimeout("任务执行超时，已中止浏览器会话"))
            if self.on_run_timeout:
                try:
                    self.on_run_timeout(job)
                except Exception:
                    log.exception("Browser watchdog callback failed")

    def _loop(self):
        while True:
            with self._state:
                if self._stopped:
                    return
            try:
                job = self._q.get(timeout=0.1)
            except queue.Empty:
                continue
            acquired = False
            try:
                # Management operations also use this lock. Until we own it,
                # this is queue time, not execution time.
                while not job.finished:
                    if job.deadline and time.monotonic() >= job.deadline:
                        self._expire(job)
                        break
                    if self.lock.acquire(timeout=0.05):
                        acquired = True
                        break
                with self._state:
                    with job.state_lock:
                        if not acquired or job.finished:
                            continue
                        job.started, job.started_at = True, time.time()
                        if job.queue_timer:
                            job.queue_timer.cancel()
                        self._running_job = job
                if self.run_timeout > 0:
                    job.run_timer = threading.Timer(self.run_timeout, self._watchdog, args=(job,))
                    job.run_timer.daemon = True
                    job.run_timer.start()
                try:
                    result = job.fn()
                except BaseException as exc:
                    self._finish(job, exc)
                else:
                    self._finish(job, result=result)
            finally:
                with self._state:
                    if self._running_job is job:
                        self._running_job = None
                if acquired:
                    self.lock.release()
                self._q.task_done()

    def raise_if_interrupted(self):
        if threading.current_thread() is self._worker:
            job = self._running_job
            if job and job.finished and job.error:
                raise job.error

    def run(self, fn):
        with self.lock:
            return fn()

    def run_sync(self, fn, label="", timeout=None, queue_timeout=None):
        job = self.submit(fn, label=label, queue_timeout=queue_timeout)
        if not job.wait_event.wait(timeout if timeout and timeout > 0 else None):
            self.cancel(job, QueueTimeout("等待任务结果超时"))
        if job.error:
            raise job.error
        return job.result_value
