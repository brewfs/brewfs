#!/usr/bin/env python3
"""Exercise the actual shared-object Lua against Redis; uses only unique keys.

Example: python3 tests/scripts/test_write_coalesce_redis.py --port 26380 --db 1
"""
import argparse
import concurrent.futures
import json
import pathlib
import re
import subprocess
import time
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=6379)
    parser.add_argument('--db', type=int, default=0)
    args = parser.parse_args()
    source = (pathlib.Path(__file__).resolve().parents[2] / 'src/meta/stores/redis/mod.rs').read_text()

    def script(method):
        section = source.split('async fn ' + method + '(', 1)[1]
        return re.search(r'redis::Script::new\(\s*r#"(.*?)"#', section, re.S).group(1)

    register = script('register_coalesced_group')
    retire = script('retire_coalesced_member')
    forget = script('forget_coalesced_group')
    publish = re.search(r'const WRITE_SLICE_LUA: &str = r#"(.*?)"#;', source, re.S).group(1)
    prefix = 'brewfs-coalesce-test/' + uuid.uuid4().hex + '/'
    owned = set()

    def key(name):
        result = prefix + name
        owned.add(result)
        return result

    def redis(*command):
        return subprocess.check_output([
            'redis-cli', '-h', args.host, '-p', str(args.port), '-n', str(args.db), '--raw',
            *map(str, command),
        ], text=True).strip()

    def evaluate(lua, keys, arguments, client=redis):
        result = client('EVAL', lua, len(keys), *keys, *arguments)
        assert not result.startswith(('ERR', 'WRONGTYPE')), result
        return result

    try:
        ids = [(1 << 52) + 32 + slot for slot in range(2)]
        ledger, pending = key('group/1'), key('pending')
        reservations = [key(f'uc{sid}') for sid in ids]
        registration_keys = [ledger, pending, *reservations]
        registration_args = [int(time.time()), ids[0], 9, 4096, ids[1], 9, 4096]
        assert evaluate(register, registration_keys, registration_args) == '1'
        assert evaluate(register, registration_keys, registration_args) == '0'
        assert evaluate(forget, [ledger], []) == '0'
        node, chunk, version, orphan = key('node'), key('chunk'), key('version'), key('orphan')
        redis('SET', node, json.dumps({'attr': {'size': 0, 'mode': 384}}))

        def commit(slot, client=redis):
            sid = ids[slot]
            return json.loads(evaluate(publish,
                [chunk, version, node, reservations[slot], pending, orphan, ledger],
                [json.dumps({'slice_id': sid, 'chunk_id': 9, 'offset': slot * 4096, 'length': 4096}),
                 8192, 1, 8, 1, slot, sid, 9, 4096], client))

        for malformed_key in [chunk, version]:
            redis('SET', malformed_key, 'malformed')
            assert not commit(0)['ok']
            assert redis('HGET', ledger, 'p0') == '', 'failed publication marked successful'
            assert redis('HGET', reservations[0], 'st') == 'pending'
            redis('DEL', malformed_key)
        # Redis INCR rejects leading zeroes even though Lua tonumber accepts
        # them. A failed publication must not consume its reservation or leave
        # an idempotency marker that makes a later retry report success.
        for malformed_version in ['00', '01', '0001']:
            redis('SET', version, malformed_version)
            try:
                invalid = commit(0)
            except (AssertionError, json.JSONDecodeError):
                invalid = {'ok': False}
            assert not invalid['ok']
            assert redis('HGET', ledger, 'p0') == '', 'invalid INCR marked publication successful'
            assert redis('HGET', reservations[0], 'st') == 'pending'
            assert redis('LLEN', chunk) == '0'
            assert json.loads(redis('GET', node))['attr']['size'] == 0
            redis('DEL', version)
        for malformed_node in ['{"attr":{"size":"0","mode":384}}',
                               '{"attr":{"size":NaN,"mode":384}}',
                               '{"attr":{"size":0,"mode":384},"other":NaN}']:
            redis('SET', node, malformed_node)
            try:
                invalid = commit(0)
            except (AssertionError, json.JSONDecodeError):
                invalid = {'ok': False}
            assert not invalid['ok']
            assert redis('HGET', ledger, 'p0') == '', 'serialization failure marked successful'
            assert redis('HGET', reservations[0], 'st') == 'pending'
        redis('SET', node, json.dumps({'attr': {'size': 0, 'mode': 384}}))
        restricted_user = 'coalesce-' + uuid.uuid4().hex
        redis('ACL', 'SETUSER', restricted_user, 'on', 'nopass', '~' + prefix + '*', '+@all', '-rpush')
        try:
            def restricted(*command):
                return subprocess.check_output([
                    'redis-cli', '-h', args.host, '-p', str(args.port), '-n', str(args.db),
                    '--user', restricted_user, '--pass', '', '--no-auth-warning', '--raw',
                    *map(str, command),
                ], text=True).strip()
            try:
                denied = commit(0, restricted)
            except (AssertionError, json.JSONDecodeError):
                denied = {'ok': False}
            assert not denied['ok'], 'denied RPUSH must fail publication'
            assert redis('HGET', ledger, 'p0') == '', 'ACL failure marked publication successful'
            assert redis('HGET', reservations[0], 'st') == 'pending'
            assert redis('LLEN', chunk) == '0'
            redis('ACL', 'SETUSER', restricted_user, '+rpush', '-zadd')
            acl_keys = [key('acl-group'), key('acl-pending'), key('acl-reservation')]
            denied = restricted('EVAL', register, len(acl_keys), *acl_keys,
                                int(time.time()), ids[0], 9, 4096)
            assert denied.startswith('ERR') or denied == 'coalesced registration permission denied', denied
            assert redis('EXISTS', *acl_keys) == '0', 'ACL failure left a partial registration'
            redis('ACL', 'SETUSER', restricted_user, '+zadd', '-hincrby')
            denied = restricted('EVAL', retire, 1, ledger, 1)
            assert denied.startswith('ERR') or denied == 'coalesced retirement permission denied', denied
            assert redis('HGET', ledger, 'd1') == '', 'ACL failure partially retired a member'
            assert redis('HGET', ledger, 'dead') == '0'
        finally:
            redis('ACL', 'DELUSER', restricted_user)
        redis('SET', version, '0')
        assert commit(0)['ok']
        assert commit(0)['ok']
        assert redis('LLEN', chunk) == '1'
        assert redis('EXISTS', reservations[0]) == '0'
        assert evaluate(retire, [ledger], [1]) == '0'
        assert evaluate(retire, [ledger], [1]) == '0'
        assert commit(1)['error'] == 'coalesced_reservation_lost'
        assert evaluate(forget, [ledger], []) == '0'
        assert evaluate(retire, [ledger], [0]) == '1'
        assert evaluate(forget, [ledger], []) == '1'
        assert evaluate(retire, [ledger], [0]) == '-1'
        assert redis('EXISTS', ledger) == '0'

        concurrent_ledger = key('group/2')
        redis('HSET', concurrent_ledger, 'n', 32, 'dead', 0)
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda slot: evaluate(retire, [concurrent_ledger], [slot]), range(32)))
        assert results.count('1') == 1 and results.count('0') == 31
        assert redis('HGET', concurrent_ledger, 'dead') == '32'
        assert evaluate(forget, [concurrent_ledger], []) == '1'
        print(json.dumps({'passed': True, 'publication_fencing': True,
                          'malformed_metadata_is_atomic': True, 'concurrent_retirement': True}))
    finally:
        if owned:
            redis('DEL', *sorted(owned))


if __name__ == '__main__':
    main()
