"""Offline: python tests/test_cdp.py"""
import concurrent.futures
import json
import queue
import sys
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import cdp


class Socket:
    def __init__(self):
        self.incoming = queue.Queue()
        self.sent = queue.Queue()
        self.auto = False

    def settimeout(self, seconds):
        self.timeout = seconds

    def recv(self):
        try:
            return self.incoming.get(timeout=0.02)
        except queue.Empty:
            raise cdp.websocket.WebSocketTimeoutException()

    def send(self, data):
        msg = json.loads(data)
        self.sent.put(msg)
        if self.auto:
            self.reply(msg['id'])

    def reply(self, mid, result=None):
        self.incoming.put(json.dumps({'id': mid, 'result': result or {'value': mid}}))

    def abort(self):
        self.incoming.put('')

    def close(self, timeout=None):
        self.abort()


class CDPTests(unittest.TestCase):
    def setUp(self):
        self.socket = Socket()
        with patch('cdp.websocket.create_connection', return_value=self.socket):
            self.client = cdp.CDP('ws://fixture', timeout=0.5)
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)

    def tearDown(self):
        self.client.close()
        self.pool.shutdown(wait=True)

    def test_out_of_order_response_dispatch(self):
        a = self.pool.submit(self.client.send, 'a')
        b = self.pool.submit(self.client.send, 'b')
        sent = [self.socket.sent.get(timeout=1) for _ in range(2)]
        for msg in reversed(sent):
            self.socket.reply(msg['id'], {'method': msg['method']})
        self.assertEqual(a.result(1)['result']['method'], 'a')
        self.assertEqual(b.result(1)['result']['method'], 'b')

    def test_eof_wakes_all_waiters_and_rejects_send(self):
        futures = [self.pool.submit(self.client.send, 'pending') for _ in range(2)]
        for _ in futures:
            self.socket.sent.get(timeout=1)
        self.socket.incoming.put('')
        for future in futures:
            with self.assertRaises(cdp.CDPError):
                future.result(0.3)
        self.assertFalse(self.client.is_alive())
        with self.assertRaises(cdp.CDPError):
            self.client.send('after-close')

    def test_timeout_and_late_reply_do_not_poison_next_call(self):
        with self.assertRaises(cdp.CDPTimeout):
            self.client.send('slow', timeout=0.03)
        old = self.socket.sent.get(timeout=1)
        self.socket.reply(old['id'])
        self.socket.auto = True
        self.assertEqual(self.client.send('next')['id'], old['id'] + 1)

    def test_send_lock_wait_is_bounded(self):
        self.client._send_lock.acquire()
        started = time.monotonic()
        try:
            with self.assertRaises(cdp.CDPTimeout):
                self.client.send('blocked-write', timeout=0.03)
            self.assertLess(time.monotonic() - started, 0.2)
        finally:
            self.client._send_lock.release()
        self.assertEqual(self.client._pending, {})

    def test_event_callback_can_send_without_blocking_reader(self):
        self.socket.auto = True
        done = threading.Event()
        def callback(msg):
            self.client.send('callback-command')
            done.set()
        self.client.set_event_handler(callback)
        self.socket.incoming.put(json.dumps({'method': 'event'}))
        self.assertTrue(done.wait(0.5))

    def test_close_wakes_pending_requests(self):
        future = self.pool.submit(self.client.send, 'pending')
        self.socket.sent.get(timeout=1)
        self.client.close()
        with self.assertRaises(cdp.CDPError):
            future.result(0.3)

    def test_write_failure_fails_other_pending_requests(self):
        pending = self.pool.submit(self.client.send, 'pending')
        self.socket.sent.get(timeout=1)
        with patch.object(self.socket, 'send', side_effect=OSError('fixture')):
            with self.assertRaises(cdp.CDPError):
                self.client.send('broken-write')
        with self.assertRaises(cdp.CDPError):
            pending.result(0.3)


if __name__ == '__main__':
    unittest.main()
