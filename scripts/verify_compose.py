"""Exercise an already-started disposable local Compose cluster without deleting volumes."""
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

def leader():
    end = time.monotonic() + 20
    while time.monotonic() < end:
        for index in range(1, 4):
            try:
                with urllib.request.urlopen(f'http://127.0.0.1:{8000+index}/readyz', timeout=1) as response:
                    if response.status == 200:
                        return index
            except (OSError, urllib.error.URLError):
                pass
        time.sleep(.1)
    raise RuntimeError('no ready leader')

def call(node, method, request_id, key, value=''):
    request = urllib.request.Request(f'http://127.0.0.1:{8000+node}/rpc',
        data=json.dumps({'jsonrpc':'2.0','id':request_id,'method':method,'params':{'key':key,'value':value}}).encode(),
        headers={'Authorization':'Bearer '+TOKEN,'Content-Type':'application/json'})
    with urllib.request.urlopen(request, timeout=5) as response:
        result = json.load(response)
    assert 'error' not in result, result
    return result['result']

def main():
    prefix = 'smoke-' + uuid4().hex
    first = leader()
    written = call(first, 'Cache.Set', prefix+'-write', prefix, 'committed')
    compose('stop', f'n{first}')
    second = leader()
    assert second != first
    assert call(second,'Cache.Get',prefix+'-get1',prefix)['value'] == 'committed'
    compose('stop','memcached')
    assert call(second,'Cache.Get',prefix+'-get2',prefix)['value'] == 'committed'
    compose('up','-d')
    compose('down')
    compose('up','-d')
    restarted = leader()
    assert call(restarted,'Cache.Get',prefix+'-get3',prefix)['value'] == 'committed'
    assert call(restarted,'Cache.Set',prefix+'-write',prefix,'committed')['revision'] == written['revision']
    assert call(restarted,'Cache.Delete',prefix+'-delete',prefix)['found']
    report = {'timestamp_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),
        'scope':'Docker Compose on one host; non-root rpcd, three persistent voters and real Memcached',
        'leader_failover':True,'committed_value_retained':True,'cache_outage_read':True,
        'all_nodes_restart_recovery':True,'restart_idempotency_receipt':True,'test_key_deleted':True}
    (ROOT/'reports/compose-smoke.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(report))

if __name__ == '__main__':
    main()
