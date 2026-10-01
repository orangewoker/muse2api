"""Run each offline regression in its own process; exit nonzero on any failure."""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--skip-browser', action='store_true')
    parser.add_argument('--skip-compose', action='store_true')
    args = parser.parse_args()
    env = dict(os.environ, PYTHONUTF8='1')
    tests = [['test_scheduler.py'], ['test_cdp.py'], ['test_api_concurrency.py'],
             ['test_async_images.py'], ['test_session_health.py'],
             ['test_vm_wait.py', 'engine.py', '--assert']]
    if not args.skip_compose:
        tests.append(['test_compose_config.py'])
    if not args.skip_browser:
        if not env.get('MUSE2API_CHROMIUM'):
            browser = next((shutil.which(n) for n in ('chromium', 'chromium-browser', 'google-chrome')
                            if shutil.which(n)), None)
            if not browser and os.name == 'nt':
                browser = next((p for p in (
                    r'C:\Program Files\Google\Chrome\Application\chrome.exe',
                    r'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe')
                    if Path(p).is_file()), None)
            if browser:
                env['MUSE2API_CHROMIUM'] = browser
        tests.append(['test_media_selection.py'])
    for test in tests:
        command = [sys.executable, str(ROOT / 'tests' / test[0]), *test[1:]]
        print('RUN', ' '.join(command), flush=True)
        subprocess.run(command, cwd=ROOT, env=env, check=True, timeout=180)
    print(f'PASS: {len(tests)} regression suites', flush=True)


if __name__ == '__main__':
    main()
