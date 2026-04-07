import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).parent
INFRA = ROOT / 'infra'

keys = json.loads((ROOT / 'keys.json').read_text())
passphrase = keys.pop('PULUMI_CONFIG_PASSPHRASE', '')
env = {**os.environ, 'PULUMI_CONFIG_PASSPHRASE': passphrase}

for key, value in keys.items():
    if not value:
        print(f'SKIP  k8lh:{key} (empty)')
        continue
    result = subprocess.run(
        ['pulumi', 'config', 'set', '--secret', '--overwrite', f'k8lh:{key}', value, '--stack', 'dev'],
        cwd=INFRA,
        capture_output=True,
        text=True,
        env=env,
    )
    if result.returncode == 0:
        print(f'OK    k8lh:{key}')
    else:
        print(f'FAIL  k8lh:{key}: {result.stderr.strip()}')
