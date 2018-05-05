"""postgres-stats-publisher

Runs in a infinite loop, fetches numbers from PG
and publishes them to Librato and/or Carbon.

See the README for usage.
"""
from __future__ import division
from __future__ import print_function

import json
import socket
import sys
import time

import librato
import psycopg2


def fetch_pg_version(cur):
    cur.execute("SELECT split_part(version(), ' ', 2)")
    res = cur.fetchall()
    return tuple(map(int, res[0][0].split('.')))


def fetch_backend_states(cur, version):
    if version < (9, 2):
        cur.execute(""" SELECT (case
                                   WHEN current_query = '<IDLE> in transaction'
                                       THEN 'idle_in_transaction'
                                   WHEN current_query = '<IDLE>'
                                       THEN 'idle'
                                   WHEN current_query LIKE 'autovacuum:%'
                                       THEN 'autovacuum'
                                   ELSE 'active'
                               end),
                               COUNT(*)
                        FROM pg_stat_activity
                        GROUP BY 1 """)
    else:
        cur.execute(""" SELECT state,
                               COUNT(*)
                        FROM pg_stat_activity
                        GROUP BY 1 """)

    res = cur.fetchall()
    for state, count in res:
        if state is None:
            state = 'null'
        state = state.replace(' ', '_')
        yield (state, int(count))

def fetch_backend_times(cur, version):
    if version < (9, 2):
        where = """ current_query NOT LIKE '<IDLE>%'
                    AND current_query NOT LIKE '%pg_stat%'
                    AND current_query NOT LIKE 'autovacuum:%' """
    else:
        where = """ state != 'idle'
                    AND query NOT LIKE '%pg_stat%' """

    cur.execute(""" SELECT EXTRACT(epoch FROM GREATEST(NOW() - query_start, '0'))
                               AS runtime
                    FROM pg_stat_activity
                    WHERE %s
                    ORDER BY 1 """ % where)
    res = cur.fetchall()

    times = [row[0] for row in res]
    if times:
        max_time = max(times)
        mean_time = sum(times) / len(times)
        median_time = times[int(len(times) / 2)]

        return [
            ('max_query_time', max_time),
            ('mean_query_time', mean_time),
            ('median_query_time', median_time),
        ]
    else:
        return []

def fetch_cache_hits(cur):
    cur.execute(""" SELECT SUM(heap_blks_hit) / (1 + SUM(heap_blks_hit) + SUM(heap_blks_read))
                               AS ratio
                    FROM pg_statio_user_tables """)
    res = cur.fetchall()
    return float(res[0][0])

def fetch_db_size(cur):
    cur.execute('SELECT pg_database_size(current_database())')
    res = cur.fetchall()
    return int(res[0][0])

def fetch_db_stats(cur, db, version):
    fields = [
        # Number of transactions in this database that have been committed
        ('xact_commit', 'transactions_committed'),
        # Number of transactions in this database that have been rolled back
        ('xact_rollback', 'transactions_rolled_back'),
        # Number of disk blocks read in this database
        ('blks_read', 'disk_blocks_read'),
        # Number of times disk blocks were found already in the buffer cache,
        # so that a read was not necessary (this only includes hits in the
        # PostgreSQL buffer cache, not the operating system's file system
        # cache)
        ('blks_hit', 'disk_blocks_cache_hit'),
        # Number of rows returned by queries in this database
        ('tup_returned', 'rows_returned'),
        # Number of rows fetched by queries in this database
        ('tup_fetched', 'rows_fetched'),
        # Number of rows inserted by queries in this database
        ('tup_inserted', 'rows_inserted'),
        # Number of rows updated by queries in this database
        ('tup_updated', 'rows_updated'),
        # Number of rows deleted by queries in this database
        ('tup_deleted', 'rows_deleted'),
        # Number of active backends
        ('numbackends', 'backends'),
    ]
    if version >= (9, 2):
        fields.extend([
            # Total amount of data written to temporary files by queries in
            # this database. All temporary files are counted, regardless of why
            # the temporary file was created, and regardless of the
            # log_temp_files setting.
            ('temp_bytes', 'temp_file_bytes'),
            # Time spent reading data file blocks by backends in this database,
            # in milliseconds
            ('blk_read_time', 'block_read_time'),
        ])

    cur.execute(""" SELECT %s
                    FROM pg_stat_database
                    WHERE datname = '%s'
                """ % (','.join(f for f, _ in fields), db))
    res = cur.fetchall()
    row = res[0]

    for name, value in zip((name for _, name in fields), row):
        if sys.version_info > (3,):
            yield (name, str(round(value)))
        else:
            yield (name, str(long(round(value))))

def fetch_index_hits(cur):
    cur.execute(""" SELECT SUM(idx_blks_hit) / (1 + SUM(idx_blks_hit + idx_blks_read))
                               AS ratio
                    FROM pg_statio_user_indexes """)
    res = cur.fetchall()
    return float(res[0][0])

def fetch_locks(cur):
    cur.execute(""" SELECT COUNT(*)
                    FROM pg_catalog.pg_locks
                    WHERE NOT pg_catalog.pg_locks.GRANTED """)
    res = cur.fetchall()
    return int(res[0][0])

def fetch_seq_scans(cur):
    cur.execute(""" SELECT sum(seq_scan),
                           sum(idx_scan)
                    FROM pg_stat_user_tables """)
    res = cur.fetchall()

    return [
        ('sequential_scans', str(res[0][0])),
        ('index_scans', str(res[0][1]))
    ]

def fetch_waiting_backends(cur):
    cur.execute(""" SELECT COUNT(*)
                    FROM pg_stat_activity
                    WHERE waiting """)
    res = cur.fetchall()
    return int(res[0][0])

# TODO: Implement fetch_index_sizes
# TODO: Implement fetch_tables_sizes


def dsn_for_db(db):
    creds = 'host=%s port=%d dbname=%s user=%s password=%s' % \
            (db['host'], db['port'], db['database'], db['user'],
             db['password'])
    return creds + \
           ' connect_timeout=2' + \
           ' application_name=postgres-stats-publisher'


def get_stats(config):
    # pylint: disable=too-many-locals
    stats = []
    for db in config['databases']:
        try:
            conn = psycopg2.connect(dsn_for_db(db))
        except psycopg2.OperationalError as e:
            print(repr(e))
            continue

        cur = conn.cursor()
        source = db['source']

        # TODO: Add CLI flag --feedback
        try:
            version = fetch_pg_version(cur)

            backend_states = fetch_backend_states(cur, version)
            for state, count in backend_states:
                stats.append(('backends_' + state, count, source, 'gauge'))

            backend_times = fetch_backend_times(cur, version)
            for metric, secs in backend_times:
                stats.append((metric, secs, source, 'gauge'))

            cache_hits = fetch_cache_hits(cur)
            stats.append(('cache_hits', cache_hits, source, 'gauge'))

            db_size = fetch_db_size(cur)
            stats.append(('db_size', db_size, source, 'gauge'))

            db_stats = fetch_db_stats(cur, db['database'], version)
            for metric, count in db_stats:
                stats.append((metric, count, source, 'counter'))

            index_hits = fetch_index_hits(cur)
            stats.append(('index_hits', index_hits, source, 'gauge'))

            locks = fetch_locks(cur)
            stats.append(('locks', locks, source, 'gauge'))

            seq_scans = fetch_seq_scans(cur)
            for metric, count in seq_scans:
                stats.append((metric, count, source, 'counter'))

            waiting_backends = fetch_waiting_backends(cur)
            stats.append(('backends_waiting', waiting_backends, source,
                          'gauge'))
        except Exception as e:  # pylint: disable=broad-except
            print(repr(e))

        cur.close()
        conn.close()

    return stats

def publish_forever(config, librato_client, carbon_socket):
    while True:
        stats = get_stats(config)

        if librato_client:
            queue = librato_client.new_queue()

            for record in stats:
                stat, data, source, kind = record
                queue.add('postgres.pg_stat.%s' % stat, data, source=source,
                          type=kind)

            queue.submit()

        if carbon_socket:
            sock = socket.socket()
            sock.connect(carbon_socket)

            for record in stats:
                stat, data, source, _ = record
                sock.sendall('postgres.%s.pg_stat.%s %s %d\n' %
                             (source, stat, data, int(time.time())))

            sock.close()

        time.sleep(config['interval'])


def main():
    config_file = 'config.json'
    if len(sys.argv) > 1:
        config_file = sys.argv[1]

    with open(config_file, 'r') as f:
        config = json.load(f)

    librato_client = None
    if 'librato' in config:
        librato_client = librato.connect(config['librato']['user'],
                                         config['librato']['token'])

    carbon_socket = None
    if 'carbon' in config:
        carbon_socket = (config['carbon']['server'], config['carbon']['port'])


    publish_forever(config, librato_client, carbon_socket)

if __name__ == '__main__':
    main()
