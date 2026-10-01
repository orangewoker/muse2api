"""Chrome DevTools Protocol client with one reader and bounded requests."""
from __future__ import annotations

import json
import queue
import threading
import time

import requests
import websocket


class CDPError(RuntimeError):
    pass


class CDPTimeout(TimeoutError):
    pass


class CDP:
    def __init__(self, ws_url: str, timeout: float = 90.0, max_size: int = 256 << 20):
        self.ws = websocket.create_connection(ws_url, timeout=timeout, max_size=max_size,
                                              enable_multithread=True)
        # Do not mutate socket timeout while reader/send are in flight: it is shared.
        self.ws.settimeout(min(1.0, max(0.01, timeout)))
        self.timeout = timeout
        self._id = 0
        self._lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._pending = {}
        self._closed = False
        self._reader_dead = False
        self._reader_err = ""
        self._on_event = None
        self._events = queue.Queue(maxsize=256)
        self._reader = threading.Thread(target=self._read_loop, daemon=True, name="cdp-reader")
        self._dispatcher = threading.Thread(target=self._dispatch_loop, daemon=True, name="cdp-events")
        self._reader.start()
        self._dispatcher.start()

    def _fail(self, reason):
        with self._lock:
            self._reader_dead = True
            self._reader_err = reason
            slots = list(self._pending.values())
            self._pending.clear()
            for slot in slots:
                slot["err"] = reason
                slot["event"].set()

    def _read_loop(self):
        try:
            while not self._closed and not self._reader_dead:
                try:
                    raw = self.ws.recv()
                except websocket.WebSocketTimeoutException:
                    continue
                if not raw:
                    raise CDPError("CDP WebSocket 已关闭")
                try:
                    msg = json.loads(raw)
                except (TypeError, ValueError):
                    continue
                if not isinstance(msg, dict):
                    continue
                if "id" in msg:
                    with self._lock:
                        slot = self._pending.pop(msg["id"], None)
                        if slot is not None:
                            slot["msg"] = msg
                            slot["event"].set()
                elif self._on_event is not None:
                    # Callbacks cannot block the only response reader. Event overflow
                    # is bounded; normal engine operations do not depend on events.
                    try:
                        self._events.put_nowait((self._on_event, msg))
                    except queue.Full:
                        pass
        except Exception as exc:
            self._fail(f"CDP 连接中断 ({type(exc).__name__})")
        finally:
            self._fail(self._reader_err or "CDP 读循环已退出")

    def _dispatch_loop(self):
        while not self._closed and not self._reader_dead:
            try:
                callback, msg = self._events.get(timeout=0.1)
            except queue.Empty:
                continue
            try:
                callback(msg)
            except Exception:
                pass

    def send(self, method: str, params: dict | None = None, timeout: float | None = None):
        limit = self.timeout if timeout is None else timeout
        deadline = time.monotonic() + max(0, limit)
        with self._lock:
            if self._closed or self._reader_dead:
                raise CDPError("CDP 连接不可用: " + (self._reader_err or "连接已关闭"))
            self._id += 1
            mid = self._id
            slot = {"event": threading.Event(), "msg": None, "err": None}
            self._pending[mid] = slot
        acquired = False
        try:
            acquired = self._send_lock.acquire(timeout=max(0, deadline - time.monotonic()))
            if not acquired:
                raise CDPTimeout(f"CDP {method} 等待发送超时")
            with self._lock:
                if self._closed or self._reader_dead:
                    raise CDPError("CDP 连接已关闭")
            try:
                self.ws.send(json.dumps({"id": mid, "method": method, "params": params or {}}))
            except Exception as exc:
                self._fail(f"CDP 发送失败 ({type(exc).__name__})")
                raise CDPError(f"{method}: {self._reader_err}") from exc
            finally:
                self._send_lock.release()
                acquired = False
            if not slot["event"].wait(max(0, deadline - time.monotonic())):
                raise CDPTimeout(f"CDP {method} 超时（{limit}s）")
            if slot["err"]:
                raise CDPError(f"{method}: {slot['err']}")
            msg = slot["msg"]
            if "error" in msg:
                raise CDPError(f"{method}: {msg['error']}")
            return msg
        finally:
            if acquired:
                self._send_lock.release()
            with self._lock:
                self._pending.pop(mid, None)

    def js(self, expr: str, await_promise: bool = False, timeout: float | None = None):
        response = self.send("Runtime.evaluate", {"expression": expr, "returnByValue": True,
                                                  "awaitPromise": await_promise}, timeout)
        result = response.get("result", {}).get("result", {})
        return result["value"] if "value" in result else response.get("result")

    def set_event_handler(self, on_event):
        self._on_event = on_event

    def pump(self, seconds: float, on_event=None, sock_timeout: float = 2.0):
        previous = self._on_event
        if on_event is not None:
            self._on_event = on_event
        try:
            deadline = time.monotonic() + seconds
            while self.is_alive() and time.monotonic() < deadline:
                time.sleep(min(0.05, max(0, deadline - time.monotonic())))
        finally:
            self._on_event = previous

    def is_alive(self):
        return not self._closed and not self._reader_dead and self._reader.is_alive()

    def close(self):
        self._closed = True
        self._fail("CDP 连接已关闭")
        try:
            self.ws.abort()
            self.ws.close(timeout=0.1)
        except Exception:
            pass
        if threading.current_thread() is not self._reader:
            self._reader.join(timeout=1.2)


def http_json(url: str, timeout: float = 5.0):
    return requests.get(url, timeout=timeout).json()
