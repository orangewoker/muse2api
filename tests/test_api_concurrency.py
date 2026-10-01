"""Offline integration: python tests/test_api_concurrency.py. No live accounts."""
import asyncio
import concurrent.futures
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_async_images import Client


async def check(home):
    os.environ.update(MUSE2API_HOME=home, MUSE2API_PROFILE_ROOT=home, MUSE2API_KEY='test-only')
    import app
    calls = []
    media = Path(app.CFG.media_dir) / 'fixture.png'
    media.write_bytes(b'fixture')
    def generate(prompt, kind, timeout, **kwargs):
        assert app.GEN_LOCK.locked()
        calls.append(kind)
        return {'filename': media.name, 'size': 7, 'kind': kind, 'path': str(media)}, 'fixture'
    app._run_generation = generate
    client = Client(app.app, {'Authorization': 'Bearer test-only'})

    # Every image entry point validates both size and aspect ratio before queuing.
    for route in ('/v1/images/tasks', '/v1/images/generations', '/v1/images/edits', '/v1/videos'):
        for fields in ({'size': 'garbage'}, {'aspect_ratio': '0:9'}, {'aspect_ratio': 'bad'},
                       {'reference_image': 'data:image/png;base64,???'},
                       {'reference_image': 'http://'}, {'reference_image': 'data:text/plain;base64,eA=='},
                       {'image': 123}, {'image': {'invalid': True}}):
            response = await client.post(route, json={'prompt': 'fixture', **fields})
            assert response.status_code == 400, (route, fields, response.status_code, response.json())
    for duration in (-1, 0, 999):
        assert (await client.post('/v1/videos', json={'prompt': 'fixture', 'duration': duration})).status_code == 400
    for timeout in (-1, 0, 601):
        assert (await client.post('/v1/videos', json={'prompt': 'fixture', 'timeout': timeout})).status_code == 422
    assert (await client.post('/v1/videos', json={'prompt': 'fixture', 'duration': True})).status_code == 422
    assert (await client.post('/v1/images/edits', json={'prompt': {'invalid': True}})).status_code == 422
    assert not calls and not app.store.tasks, 'invalid input must not enqueue work'
    assert (await client.post('/v1/images/edits', data={'prompt': 'fixture', 'timeout': 'abc'},
                              files={'image': ('reference.png', b'fixture', 'image/png')})).status_code == 400
    print('PASS: validation covers all generation endpoints; bad input never enters queue')

    for method, route in [('GET', '/v1/videos'), ('DELETE', '/v1/images/generations'),
                          ('PUT', '/admin/accounts')]:
        assert (await client.request(method, route, headers={'Authorization': 'Bearer wrong'})).status_code == 401
        assert (await client.request(method, route)).status_code == 405
    assert (await client.post('/v1/media/fixture.png', headers={'Authorization': ''})).status_code == 405
    print('PASS: incorrect HTTP method cannot bypass Bearer validation')

    release, entered = threading.Event(), threading.Event()
    def block():
        entered.set()
        assert release.wait(2)
    first = app.SCHED.submit(block)
    assert entered.wait(1)
    video = (await client.post('/v1/videos', json={'prompt': 'fixture', 'duration': 8})).json()
    image = (await client.post('/v1/images/tasks', json={'prompt': 'fixture'})).json()
    for item, route in ((video, '/v1/videos/'), (image, '/v1/images/tasks/')):
        status = (await client.get(route + item['id'])).json()
        assert status['status'] == status['stage'] == 'queued' and status['progress'] == 0
    release.set()
    await asyncio.to_thread(app.SCHED._q.join)
    assert calls == ['video', 'image'], calls
    assert (await client.get('/v1/videos/' + image['id'])).status_code == 404
    stalled = app.store.create_task('video', 'stalled fixture')
    app.store.update_task(stalled['id'], status='processing', progress=17)
    stalled['updated_at'] = time.time() - 130
    state = (await client.get('/v1/videos/' + stalled['id'])).json()
    assert state['status'] == 'stalled' and state['progress'] == 17
    print('PASS: FIFO video/image ordering, truthful queued progress and stall diagnostics')

    release.clear()
    entered.clear()
    app.SCHED.submit(block)
    assert entered.wait(1)
    app.SCHED.queue_timeout = 0.04
    timed = (await client.post('/v1/videos', json={'prompt': 'expires'})).json()
    expired_image = (await client.post('/v1/images/tasks', json={'prompt': 'expires'})).json()
    sync = await client.post('/v1/images/generations', json={'prompt': 'expires'})
    assert sync.status_code == 504, sync.json()
    for task in (timed, expired_image):
        for _ in range(100):
            if app.store.get_task(task['id'])['status'] == 'failed':
                break
            await asyncio.sleep(0.01)
        stored = app.store.get_task(task['id'])
        assert stored['status'] == 'failed' and stored['stage'] == 'timeout', stored
    with concurrent.futures.ThreadPoolExecutor() as pool:
        future = pool.submit(lambda: list(app.safe_chat_stream({}, 'expires', {}, 1, None)))
        try:
            future.result(1)
            raise AssertionError('queued stream must report expiry')
        except app.MuseGenerationError:
            pass
    release.set()
    await asyncio.to_thread(app.SCHED._q.join)
    assert calls == ['video', 'image'], 'expired jobs must not generate later'
    app.SCHED.queue_timeout = 2
    print('PASS: queued video/image/sync/streaming callers all receive terminal timeout')

    # A late successful result/heartbeat must not overwrite watchdog failure.
    release.clear()
    original_callback = app.SCHED.on_run_timeout
    original_timeout = app.SCHED.run_timeout
    app.SCHED.run_timeout = 0.04
    app.SCHED.on_run_timeout = lambda job: release.set()
    def late_generation(*args, **kwargs):
        assert release.wait(1)
        return generate(*args, **kwargs)
    with patch.object(app, '_run_generation', late_generation):
        late = (await client.post('/v1/videos', json={'prompt': 'watchdog'})).json()
        await asyncio.to_thread(app.SCHED._q.join)
    stored = app.store.get_task(late['id'])
    assert stored['status'] == 'failed' and stored['stage'] == 'timeout'
    app.store.update_task(late['id'], progress=99, stage='rendering')
    assert stored['stage'] == 'timeout'
    app.SCHED.run_timeout = original_timeout
    app.SCHED.on_run_timeout = original_callback
    print('PASS: watchdog failures cannot be overwritten by late results or heartbeats')

    # Producer fills the bounded buffer. Closing the consumer must unblock it.
    with patch.object(app.engine, 'start'), patch.object(app.engine, 'reset_thread'), \
         patch.object(app.engine, 'chat_stream', side_effect=lambda *a, **k: iter(['x'] * 1000)):
        stream = app.safe_chat_stream({}, 'stream', {}, 1, None)
        assert await asyncio.to_thread(next, stream) == 'x'
        stream.close()
        assert await asyncio.to_thread(app.SCHED.run_sync, lambda: 'next') == 'next'
    print('PASS: disconnect with full stream buffer releases browser worker')

    pending_video = app.store.create_task('video', 'interrupted')
    pending_image = app.store.create_task('image', 'interrupted')
    async def no_keepalive():
        return
    with patch.object(app, '_keepalive_loop', no_keepalive):
        await app._startup()
        for pending in (pending_video, pending_image):
            assert app.store.get_task(pending['id'])['stage'] == 'interrupted'
        app.CFG.api_key = ''
        await app._startup()
        generated = app.CFG.api_key
        assert Path(app.CFG.api_key_file).read_text().strip() == generated
        app.CFG.api_key = ''
        await app._startup()
        assert app.CFG.api_key == generated
        app.CFG.api_key = 'explicit-env-fixture'
        await app._startup()
        assert app.CFG.api_key == 'explicit-env-fixture'
    assert app.REPO_URL == 'https://github.com/orangewoker/muse2api'
    app._shutdown()
    app.SCHED._worker.join(1)
    print('PASS: restart recovery, persistent automatic key and fork update source')


if __name__ == '__main__':
    with tempfile.TemporaryDirectory(prefix='muse-api-concurrency-') as home:
        asyncio.run(check(home))
