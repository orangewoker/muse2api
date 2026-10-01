"""Offline: python tests/test_scheduler.py"""
import queue
import sys
import threading
import time
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scheduler import Scheduler, QueueTimeout, RunTimeout, SchedulerStopped


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.lock = threading.Lock()
        self.scheduler = Scheduler(self.lock, max_queue=20, queue_timeout=2, run_timeout=2)
        self.release = threading.Event()
        self.entered = threading.Event()

    def tearDown(self):
        self.release.set()
        self.scheduler.stop()
        if self.scheduler._worker:
            self.scheduler._worker.join(2)
            self.assertFalse(self.scheduler._worker.is_alive())

    def block(self):
        self.entered.set()
        self.release.wait(2)

    def blocked(self):
        job = self.scheduler.submit(self.block)
        self.assertTrue(self.entered.wait(1))
        return job

    def test_fifo_lock_and_results(self):
        first = self.blocked()
        order = []
        def run(i):
            self.assertTrue(self.lock.locked())
            order.append(i)
            return i * 2
        jobs = [self.scheduler.submit(lambda i=i: run(i)) for i in range(5)]
        self.release.set()
        for job in [first, *jobs]:
            self.assertTrue(job.wait_event.wait(1))
            self.assertIsNone(job.error)
        self.assertEqual(order, list(range(5)))
        self.assertEqual([j.result_value for j in jobs], [0, 2, 4, 6, 8])
        self.assertEqual(self.scheduler.stats()['completed'], 6)

    def test_queue_expiry_notifies_without_worker(self):
        self.blocked()
        calls = []
        done = []
        job = self.scheduler.submit(lambda: calls.append(1), queue_timeout=0.04,
                                    on_done=lambda j: done.append(j.error))
        self.assertTrue(job.wait_event.wait(0.5))
        self.assertIsInstance(job.error, QueueTimeout)
        self.assertEqual(len(done), 1)
        self.release.set()
        self.scheduler._q.join()
        self.assertEqual(calls, [])
        self.assertEqual(self.scheduler.stats()['timeout'], 1)

    def test_watchdog_terminal_exactly_once(self):
        self.scheduler.run_timeout = 0.04
        interrupted = []
        self.scheduler.on_run_timeout = lambda job: (interrupted.append(job), self.release.set())
        job = self.scheduler.submit(self.block)
        self.assertTrue(job.wait_event.wait(0.5))
        self.assertIsInstance(job.error, RunTimeout)
        self.scheduler._q.join()
        self.assertEqual(interrupted, [job])
        self.assertEqual(self.scheduler.stats()['run_timeout'], 1)
        self.assertEqual(self.scheduler.stats()['completed'], 0)
        self.assertEqual(self.scheduler.run_sync(lambda: 42), 42)

    def test_waiting_for_management_lock_is_queue_time(self):
        self.lock.acquire()
        interrupted = []
        self.scheduler.run_timeout = 0.01
        self.scheduler.on_run_timeout = interrupted.append
        job = self.scheduler.submit(lambda: None, queue_timeout=0.04)
        try:
            self.assertTrue(job.wait_event.wait(0.5))
            self.assertIsInstance(job.error, QueueTimeout)
            self.assertFalse(job.started)
            self.assertEqual(interrupted, [])
        finally:
            self.lock.release()

    def test_full_and_cancelled_jobs_do_not_run(self):
        self.scheduler = Scheduler(self.lock, max_queue=1, queue_timeout=2, run_timeout=2)
        self.blocked()
        ran = []
        job = self.scheduler.submit(lambda: ran.append(1))
        with self.assertRaises(queue.Full):
            self.scheduler.submit(lambda: None)
        self.scheduler.cancel(job)
        self.assertTrue(job.wait_event.is_set())
        self.release.set()
        self.scheduler._q.join()
        self.assertEqual(ran, [])

    def test_shutdown_notifies_and_rejects(self):
        running = self.blocked()
        queued = self.scheduler.submit(lambda: None)
        self.scheduler.stop()
        for job in (running, queued):
            self.assertTrue(job.wait_event.wait(0.5))
            self.assertIsInstance(job.error, SchedulerStopped)
        with self.assertRaises(SchedulerStopped):
            self.scheduler.submit(lambda: None)

    def test_exceptions_do_not_freeze_worker(self):
        def fail():
            raise ValueError('fixture')
        with self.assertRaisesRegex(ValueError, 'fixture'):
            self.scheduler.run_sync(fail)
        self.assertEqual(self.scheduler.run_sync(lambda: 'ok'), 'ok')

    def test_sync_wait_timeout_cancels_queued_job(self):
        self.blocked()
        ran = []
        with self.assertRaises(QueueTimeout):
            self.scheduler.run_sync(lambda: ran.append(1), timeout=0.04)
        self.release.set()
        self.scheduler._q.join()
        self.assertEqual(ran, [])


if __name__ == '__main__':
    unittest.main()
