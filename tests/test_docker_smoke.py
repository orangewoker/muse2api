"""Isolated Compose smoke test. Build muse2api:latest first; no Muse account used."""
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]


def check(home):
    env = {k: v for k, v in os.environ.items() if not k.startswith('MUSE2API_')}
    name = 'muse-smoke-' + uuid.uuid4().hex[:8]
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    base = f'http://127.0.0.1:{port}'
    override = Path(home) / 'override.yml'
    override.write_text(f'services:\n  muse2api:\n    container_name: {name}\n', encoding='utf-8')
    dotenv = Path(home) / '.env'
    def write_env(key):
        dotenv.write_text(f'MUSE2API_BIND=127.0.0.1\nMUSE2API_PORT={port}\nMUSE2API_KEY={key}\n'
                           f'MUSE2API_PUBLIC_BASE={base}\n', encoding='utf-8')
    write_env('m2a_compose_smoke_fixture')
    prefix = ['docker', 'compose', '--project-directory', home, '--env-file', str(dotenv),
              '-p', name, '-f', str(ROOT / 'docker-compose.yml'), '-f', str(override)]
    def compose(*args):
        return subprocess.run(prefix + list(args), env=env, text=True, capture_output=True,
                              check=True, timeout=90).stdout
    def request(path, key=None, data=None):
        headers = {'Authorization': 'Bearer ' + key} if key else {}
        if data is not None:
            headers['Content-Type'] = 'application/json'
        req = urllib.request.Request(base + path, headers=headers,
                                     data=json.dumps(data).encode() if data is not None else None)
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()
    def ready():
        for _ in range(80):
            try:
                if request('/healthz')[0] == 200:
                    return
            except OSError:
                pass
            time.sleep(0.25)
        raise AssertionError('container never became ready')
    resolved = json.loads(compose('config', '--format', 'json'))['services']['muse2api']
    assert Path(resolved['volumes'][0]['source']).resolve() == (Path(home) / 'data').resolve(), \
        'smoke test must never mount actual workspace data'
    try:
        compose('up', '-d', '--no-build')
        ready()
        assert request('/v1/models')[0] == 401
        assert request('/v1/models', 'm2a_compose_smoke_fixture')[0] == 200
        assert request('/v1/videos', 'wrong')[0] == 401
        assert request('/v1/videos', 'm2a_compose_smoke_fixture')[0] == 405
        assert request('/v1/videos', 'm2a_compose_smoke_fixture',
                       {'prompt': 'fixture', 'duration': 999})[0] == 400
        scheduler = json.loads(request('/admin/scheduler', 'm2a_compose_smoke_fixture')[1])
        assert scheduler['worker_alive'] and scheduler['submitted'] == 0
        data = Path(home) / 'data'
        media = data / 'media' / 'smoke.webp'
        media.write_bytes(b'offline media fixture')
        (data / 'accounts.json').write_text(json.dumps([
            {'id': 'offline-fixture', 'enabled': False, 'cookies': {}, 'label': 'persistent fixture'}]),
            encoding='utf-8')
        print('PASS: real Compose startup, .env authentication, validation and scheduler health')
        write_env('')
        compose('up', '-d', '--no-build', '--force-recreate')
        ready()
        automatic_key = (data / '.api_key').read_text().strip()
        assert automatic_key.startswith('m2a_')
        assert request('/v1/models', automatic_key)[0] == 200
        compose('up', '-d', '--no-build', '--force-recreate')
        ready()
        assert (data / '.api_key').read_text().strip() == automatic_key
        assert request('/v1/models', automatic_key)[0] == 200
        accounts = json.loads(request('/admin/accounts', automatic_key)[1])['accounts']
        assert accounts[0]['id'] == 'offline-fixture'
        assert request('/v1/media/smoke.webp')[1] == b'offline media fixture'
        print('PASS: container recreation preserves automatic API key, accounts and media')
        for _ in range(40):
            inspect = subprocess.run(['docker', 'inspect', name], check=True,
                                     capture_output=True, text=True)
            if json.loads(inspect.stdout)[0]['State']['Health']['Status'] == 'healthy':
                break
            time.sleep(1)
        else:
            raise AssertionError('Docker healthcheck did not pass')
        print('PASS: Docker healthcheck healthy; isolated smoke resources cleaned up')
    finally:
        compose('down', '--remove-orphans')


if __name__ == '__main__':
    with tempfile.TemporaryDirectory(prefix='muse-docker-smoke-') as home:
        check(home)
