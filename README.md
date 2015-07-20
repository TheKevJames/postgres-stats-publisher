# Postgres Stats Publisher

Publishes statistics from PostgreSQL's pg_stat tables.
Supports Postgres &rarr; Librato and Postgres &rarr; Carbon

Originally forked from [postgres-librato](https://github.com/6wunderkinder/postgres-librato), this project's name was changed since it now supports endpoints other than librato. Full credit goes to [Torsten Becker](https://github.com/torsten) for creating the original project.


## Features

Compatible with PostgreSQL v9.1 and greater.

Exposes the following stats:

 - `postgres.pg_stat.backends` — gauge
 - `postgres.pg_stat.backends_active` — gauge
 - `postgres.pg_stat.backends_idle_in_transaction` — gauge
 - `postgres.pg_stat.backends_idle` — gauge
 - `postgres.pg_stat.backends_null` — gauge
 - `postgres.pg_stat.backends_waiting` — gauge
 - `postgres.pg_stat.block_read_time` — counter
 - `postgres.pg_stat.cache_hits` — gauge
 - `postgres.pg_stat.db_size` - guage
 - `postgres.pg_stat.disk_blocks_cache_hit` — counter
 - `postgres.pg_stat.disk_blocks_read` — counter
 - `postgres.pg_stat.index_hits` — gauge
 - `postgres.pg_stat.index_scans` — counter
 - `postgres.pg_stat.locks` - gauge
 - `postgres.pg_stat.max_query_time` — gauge
 - `postgres.pg_stat.mean_query_time` — gauge
 - `postgres.pg_stat.median_query_time` — gauge
 - `postgres.pg_stat.rows_deleted` — counter
 - `postgres.pg_stat.rows_fetched` — counter
 - `postgres.pg_stat.rows_inserted` — counter
 - `postgres.pg_stat.rows_returned` — counter
 - `postgres.pg_stat.rows_updated` — counter
 - `postgres.pg_stat.sequential_scans` — counter
 - `postgres.pg_stat.temp_file_bytes` — counter
 - `postgres.pg_stat.transactions_committed` — counter
 - `postgres.pg_stat.transactions_rolled_back` — counter


## Setup

1&#x2e; Install the dependencies:

    pip install -r requirements.txt

2&#x2e; Create a config file based on [sample-config.json](sample-config.json).

3&#x2e; Setup a upstart script (or something similar)

    start on runlevel [2345]
    stop on runlevel [016]

    setuid app
    setgid app

    # TODO: Edit these paths:
    exec python .../publish.py .../config.json
    respawn


## Related Work

 - [postgres-librato](https://github.com/6wunderkinder/postgres-librato)
 - [PostgreSQL Docs - The Statistics Collector](http://www.postgresql.org/docs/9.3/static/monitoring-stats.html)
 - [pg_stats_aggregator](https://github.com/lmarburger/pg_stats_aggregator/blob/master/pgstats.rb)
 - [heroku-pg-extras](https://github.com/heroku/heroku-pg-extras/blob/master/lib/heroku/command/pg.rb)
 - [High Performance PostgreSQL Tools](https://github.com/gregs1104/hppt/tree/master/perf)
 - [check_postgres](http://bucardo.org/wiki/Check_postgres)
