"""Daemon-free Compose regression: python tests/test_compose_config.py"""
import json
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def config(home, env):
    result = subprocess.run(['docker', 'compose', '--project-directory', home,
                             '-f', str(ROOT / 'docker-compose.yml'), 'config', '--format', 'json'],
                            env=env, capture_output=True, text=True, check=True)
    return json.loads(result.stdout)['services']['muse2api']


def check():
    env = {k: v for k, v in os.environ.items() if not k.startswith('MUSE2API_')}
    with tempfile.TemporaryDirectory(prefix='muse-compose-') as home:
        service = config(home, env)
        assert service['environment']['MUSE2API_KEY'] == ''
        dot_env = Path(home) / '.env'
        dot_env.write_text('MUSE2API_KEY=m2a_dotenv_fixture\nMUSE2API_PUBLIC_BASE=https://fixture.example\n'
                           'MUSE2API_PORT=18700\nMUSE2API_IMAGE_TIMEOUT=333\n', encoding='utf-8')
        service = config(home, env)
        assert service['environment']['MUSE2API_KEY'] == 'm2a_dotenv_fixture'
        assert service['environment']['MUSE2API_PUBLIC_BASE'] == 'https://fixture.example'
        assert service['environment']['MUSE2API_IMAGE_TIMEOUT'] == '333'
        assert str(service['ports'][0]['published']) == '18700'
        assert service['environment']['MUSE2API_REPO'] == 'https://github.com/orangewoker/muse2api'
        env['MUSE2API_KEY'] = 'm2a_host_fixture'
        assert config(home, env)['environment']['MUSE2API_KEY'] == 'm2a_host_fixture'
        env['MUSE2API_KEY'] = ''
        assert config(home, env)['environment']['MUSE2API_KEY'] == ''
    print('PASS: Compose .env/host/default precedence, public URL, port and timeout interpolation')


if __name__ == '__main__':
    check()
