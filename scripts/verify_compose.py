"""Verify this project's disposable fixed-sequencer Compose deployment."""
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
TOKEN = os.environ['RPCACHE_TOKEN']

def compose(*args):
    subprocess.run(['docker', 'compose', *args], cwd=ROOT, check=True, stdout=subprocess.DEVNULL)

def ready():
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            for index in range(1, 4):
                with urllib.request.urlopen(f'http://127.0.0.1:{8000+index}/readyz', timeout=2) as response:
                    assert response.status == 200
            return
        except (OSError, urllib.error.URLError, AssertionError):
            time.sleep(.2)
    raise RuntimeError('configured replicas did not become ready')

def call(node, method, request_id, key, value=''):
    req = urllib.request.Request(f'http://127.0.0.1:{8000+node}/rpc',
        data=json.dumps({'jsonrpc': '2.0', 'id': request_id, 'method': method,
                         'params': {'key': key, 'value': value}}).encode(),
        headers={'Authorization': 'Bearer '+TOKEN, 'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=5) as response:
        result = json.load(response)
    if 'error' in result:
        raise RuntimeError(result['error']['message'])
    return result['result']

def main():
    prefix = 'smoke-' + uuid4().hex
    ready()
    try:
        written = call(2, 'Cache.Set', prefix+'-write', prefix, 'ordered')
        for node in range(1, 4):
            assert call(node, 'Cache.Get', prefix+f'-read-{node}', prefix)['value'] == 'ordered'
        compose('stop', 'n3')
        try:
            call(1, 'Cache.Get', prefix+'-outage', prefix)
        except (RuntimeError, OSError):
            pass
        else:
            raise AssertionError('read succeeded without all configured replicas')
        compose('up', '-d', 'n3')
        ready()
        assert call(3, 'Cache.Get', prefix+'-recovered', prefix)['value'] == 'ordered'
        compose('stop', 'mc1')
        assert call(1, 'Cache.Get', prefix+'-cache-down', prefix)['value'] == 'ordered'
        compose('up', '-d', 'mc1')
        compose('restart', 'n1', 'n2', 'n3')
        ready()
        assert call(3, 'Cache.Get', prefix+'-restart', prefix)['value'] == 'ordered'
        retry = call(2, 'Cache.Set', prefix+'-write', prefix, 'ordered')
        assert retry['revision'] == written['revision']
        assert call(1, 'Cache.Delete', prefix+'-delete', prefix)['found']
        report = dict(timestamp_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            scope='single-host Docker Compose, fixed sequencer n1, all-member acknowledgments, three durable replicas and dedicated Memcached services',
            cross_replica_reads=True, missing_replica_fails_closed=True,
            replica_restart=True, memcached_outage_fallback=True,
            all_nodes_restart=True, durable_retry_receipt=True, test_key_deleted=True)
        (ROOT/'reports/compose-smoke.json').write_text(json.dumps(report, indent=2)+'\n', encoding='utf-8')
        print(json.dumps(report))
    finally:
        compose('up', '-d', 'n1', 'n2', 'n3', 'mc1', 'mc2', 'mc3')

if __name__ == '__main__':
    main()
