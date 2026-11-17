"""Transactional test-only PostgreSQL Registry V2 CAS. No auto migration/connect.

A caller must complete independent object+row+signature verification first.
Database name is restricted, and this module NEVER authorizes production writes.
"""
from __future__ import annotations
import hashlib
import json
import re

class RegistryBlocked(ValueError):
    pass

def check_database(name):
    if type(name) is not str or re.fullmatch(r'viq_s14_test_[a-f0-9]{12}', name) is None:
        raise RegistryBlocked('ISOLATED_DATABASE_NAME_REQUIRED')

def validate_batch(batch):
    if type(batch) is not dict or set(batch) != {'namespace','hour_utc','digest','generation','country_count','window_count','test_only'}:
        raise RegistryBlocked('BATCH_FIELDS')
    if batch['test_only'] is not True: raise RegistryBlocked('TEST_ONLY_REQUIRED')
    if type(batch['namespace']) is not str or re.fullmatch(r's14test_[a-f0-9]{12}',batch['namespace']) is None:
        raise RegistryBlocked('ISOLATED_NAMESPACE_REQUIRED')
    if type(batch['hour_utc']) is not str or re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:00:00Z', batch['hour_utc']) is None:
        raise RegistryBlocked('HOUR_REQUIRED')
    from datetime import datetime
    try: datetime.strptime(batch['hour_utc'], '%Y-%m-%dT%H:%M:%SZ')
    except ValueError: raise RegistryBlocked('INVALID_HOUR')
    if type(batch['digest']) is not str or not re.fullmatch(r'[a-f0-9]{64}', batch['digest']):
        raise RegistryBlocked('BATCH_DIGEST')
    if type(batch['generation']) is not str or not re.fullmatch(r'[a-f0-9]{32}', batch['generation']):
        raise RegistryBlocked('GENERATION')
    if batch['country_count'] != 5 or batch['window_count'] != 60:
        raise RegistryBlocked('FULL_HOUR_REQUIRED')


def compare_and_publish_test_only(conn, database_name, batch, expected_epoch):
    """Single real transaction. Stale CAS => rollback. Replay identical => no-op.

    conn must be an already-open PostgreSQL connection to a separately created
    viq_s14_test_* DB. Schema installed separately with database guard. No SQL
    statements here ever touch other schemas or database names.
    """
    check_database(database_name); validate_batch(batch)
    if type(expected_epoch) is not int or not (0 <= expected_epoch < 1_000_000_000):
        raise RegistryBlocked('EPOCH_RANGE')
    # Server-side verification prevents DSN spoofing; SELECT allowed before write.
    cur = conn.cursor()
    try:
        cur.execute('SELECT current_database()')
        db = cur.fetchone()[0]
        check_database(db)
        if db != database_name: raise RegistryBlocked('DATABASE_MISMATCH')
        cur.execute('SELECT epoch,generation,digest FROM viq_s14test.heads WHERE namespace=%s AND hour_utc=%s FOR UPDATE',
                    (batch['namespace'], batch['hour_utc']))
        row = cur.fetchone()
        if row is not None and row[1] == batch['generation'] and row[2] == batch['digest']:
            conn.commit(); return {'status':'IDEMPOTENT_REPLAY_TEST_ONLY','epoch':row[0]}
        current = 0 if row is None else row[0]
        if current != expected_epoch: raise RegistryBlocked('STALE_CAS')
        # Must not publish an unverified application-supplied digest. Caller
        # constructs this after all 60 independently verified receipts.
        cur.execute('INSERT INTO viq_s14test.generations(namespace,hour_utc,generation,digest,test_only) VALUES (%s,%s,%s,%s,true) ON CONFLICT DO NOTHING',
                    (batch['namespace'],batch['hour_utc'],batch['generation'],batch['digest']))
        cur.execute('SELECT digest FROM viq_s14test.generations WHERE namespace=%s AND hour_utc=%s AND generation=%s',
                    (batch['namespace'],batch['hour_utc'],batch['generation']))
        found = cur.fetchone()
        if found is None or found[0] != batch['digest']:
            raise RegistryBlocked('GENERATION_REPLAY_DRIFT')
        if row is None:
            cur.execute('INSERT INTO viq_s14test.heads(namespace,hour_utc,epoch,generation,digest) VALUES (%s,%s,1,%s,%s)',
                        (batch['namespace'],batch['hour_utc'],batch['generation'],batch['digest']))
        else:
            cur.execute('UPDATE viq_s14test.heads SET epoch=%s,generation=%s,digest=%s WHERE namespace=%s AND hour_utc=%s AND epoch=%s',
                        (current+1,batch['generation'],batch['digest'],batch['namespace'],batch['hour_utc'],current))
            if cur.rowcount != 1: raise RegistryBlocked('CAS_UPDATE_RACE')
        conn.commit()
        return {'status':'COMMITTED_TEST_ONLY','epoch':current+1}
    except BaseException:
        conn.rollback()
        raise
    finally:
        cur.close()
