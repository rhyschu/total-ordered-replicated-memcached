"""Run compiled server/client executables with isolated disposable node state."""
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
SUFFIX = '.exe' if os.name == 'nt' else ''
SERVER = ROOT/'bin'/('server'+SUFFIX)
CLIENT = ROOT/'bin'/('client'+SUFFIX)

def free_port():
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        return listener.getsockname()[1]

def main():
    token = secrets.token_urlsafe(32)
    env = dict(os.environ, RPCACHE_TOKEN=token)
    peers = {f'n{i}': f'127.0.0.1:{free_port()}' for i in range(1, 4)}
    endpoints = {f'n{i}': f'http://127.0.0.1:{free_port()}' for i in range(1, 4)}
    processes, logs = {}, []
    with tempfile.TemporaryDirectory(prefix='ordered-cache-smoke-') as directory:
        def start(node):
            log = open(Path(directory)/(node+f'-{len(logs)}.log'), 'wb')
            logs.append(log)
            processes[node] = subprocess.Popen([str(SERVER), '-id='+node,
                '-sequencer=n1', '-peer='+peers[node], '-http='+endpoints[node][7:],
                '-data='+str(Path(directory)/node), '-peers='+json.dumps(peers)],
                env=env, stdout=log, stderr=log,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)

        def stop(node):
            process = processes.pop(node, None)
            if process:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)

        def ready():
            deadline = time.monotonic()+20
            while time.monotonic() < deadline:
                try:
                    for url in endpoints.values():
                        with urllib.request.urlopen(url+'/readyz', timeout=2) as response:
                            assert response.status == 200
                    return
                except (OSError, AssertionError):
                    time.sleep(.1)
            raise RuntimeError('server processes did not become ready')

        def call(node, method, request_id, value=''):
            req = urllib.request.Request(endpoints[node]+'/rpc',
                data=json.dumps(dict(jsonrpc='2.0', id=request_id, method=method,
                    params=dict(key='smoke-key', value=value))).encode(),
                headers={'Authorization': 'Bearer '+token, 'Content-Type': 'application/json'})
            with urllib.request.urlopen(req, timeout=5) as response:
                result = json.load(response)
            assert result['id'] == request_id
            if 'error' in result:
                raise RuntimeError(result['error']['message'])
            return result['result']

        try:
            for node in peers:
                start(node)
            ready()
            original = call('n2', 'Cache.Set', 'first-write', 'value')
            for node in peers:
                assert call(node, 'Cache.Get', 'read-'+node)['value'] == 'value'
            for failed, reader in [('n3', 'n1'), ('n1', 'n2')]:
                stop(failed)
                try:
                    call(reader, 'Cache.Get', 'down-'+failed)
                except (RuntimeError, OSError):
                    pass
                else:
                    raise AssertionError('missing member did not fail closed')
                start(failed)
                ready()
            for node in peers:
                stop(node)
            for node in peers:
                start(node)
            ready()
            assert call('n3', 'Cache.Get', 'after-restart')['value'] == 'value'
            assert call('n2', 'Cache.Set', 'first-write', 'value')['revision'] == original['revision']
            result = subprocess.run([str(CLIENT), '-targets='+','.join(endpoints.values()),
                '-method=Cache.Get', '-key=smoke-key', '-id=cli-smoke'], env=env,
                capture_output=True, text=True, timeout=10)
            assert result.returncode == 0, result.stderr
            assert json.loads(result.stdout)['value'] == 'value'
            report = dict(timestamp_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                scope='three separate server processes, real TCP and HTTP, dedicated temporary bbolt stores; no Docker, external Memcached or TLS',
                cross_replica_reads=True, missing_replica_fails_closed=True,
                missing_sequencer_fails_closed=True, all_process_restart_recovery=True,
                durable_idempotency=True, command_line_client=True)
            (ROOT/'reports/local-smoke.json').write_text(json.dumps(report, indent=2)+'\n', encoding='utf-8')
            print(json.dumps(report))
        finally:
            for node in list(processes):
                stop(node)
            for log in logs:
                log.close()

if __name__ == '__main__':
    main()
