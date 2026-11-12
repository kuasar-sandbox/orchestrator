"""Run inside the VM as part of the idle-timeout case."""
import http.client
import os
from pathlib import Path
import time


def request(method, path, headers, body=None):
    conn = http.client.HTTPConnection('169.254.169.254', timeout=10)
    try:
        conn.request(method, path, body=body, headers=headers)
        response = conn.getresponse()
        return response.status, response.read().strip()
    finally:
        conn.close()


status, token = request('PUT', '/latest/api/token', {'X-metadata-token-ttl-seconds': '60'})
assert status == 200, status
headers = {'X-metadata-token': token.decode()}
path = '/work/hold'
assert request('PUT', path + '?if-match=0', headers, '1') == (200, b'1')
assert request('PUT', path + '?if-match=0', headers, '2') == (409, b'1')
if os.fork():
    print('registered')
else:
    # Release the exec connection as soon as the parent returns. No inherited
    # stdio or guest business request may keep the VM running during the job.
    os.setsid()
    for fd in (0, 1, 2):
        with open(os.devnull, 'r+b') as stream:
            os.dup2(stream.fileno(), fd)
    time.sleep(8)
    Path('/tmp/idle-hold-result').write_text('completed')
    assert request('PUT', path + '?if-match=1', headers, '0') == (200, b'0')
