"""
Copy a snapshot of the pipeline's results from your local database to a cloud
Postgres (for example Neon), so the dashboard can be shown online in demo mode.

The full pipeline (Kafka + Spark) is too heavy for free hosting, so the online
dashboard reads this snapshot of a REAL run instead.

    1. let the stack run normally for a few hours (docker compose up -d)
    2. set the cloud database address (never commit it):
           PowerShell:  $env:TARGET_DATABASE_URL = "postgresql://user:password@host/dbname?sslmode=require"
           bash:        export TARGET_DATABASE_URL="postgresql://user:password@host/dbname?sslmode=require"
    3. python scripts/export_snapshot.py --hours 6

Steps:
  - create the tables on the target (sql/init.sql) and empty them
  - copy the last N hours of every table with Postgres COPY (fast, no CSV files)
  - save the snapshot time in demo_meta (the dashboard counts "last hour" from it)
  - run dbt against the target, so the analytics tables are built there too

Needs: psycopg2 (pip install -r requirements-dev.txt) and the Docker stack running.
"""

import argparse
import io
import os
import subprocess
import sys
from urllib.parse import parse_qs, unquote, urlparse

import psycopg2

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# table -> column used to keep only the last N hours ("" = copy everything)
TABLES = [
    ("fx_rates", ""),
    ("raw_transactions", "event_time"),
    ("bad_events", "processed_at"),
    ("txn_volume_per_minute", "window_start"),
    ("batch_log", "logged_at"),
    ("stream_progress", "batch_timestamp"),
    ("pipeline_events", "logged_at"),
]


def read_env_file():
    """Read the local .env file (Postgres user, password, database name)."""
    values = {}
    with open(os.path.join(ROOT, ".env")) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                values[key] = value
    return values


def local_connection():
    """Connection to the Postgres of the local Docker stack."""
    env = read_env_file()
    return psycopg2.connect(host="localhost", port=5432, dbname=env["POSTGRES_DB"],
                            user=env["POSTGRES_USER"], password=env["POSTGRES_PASSWORD"])


def copy_table(source, target, table, column, hours):
    """Copy one table (only the last `hours` hours) from source to target with COPY."""
    where = f"where {column} >= now() - interval '{int(hours)} hours'" if column else ""
    buffer = io.StringIO()
    with source.cursor() as cur:
        cur.copy_expert(f"copy (select * from {table} {where}) to stdout", buffer)
    buffer.seek(0)
    with target.cursor() as cur:
        cur.copy_expert(f"copy {table} from stdin", buffer)
        cur.execute(f"select count(*) from {table}")
        return cur.fetchone()[0]


def run_dbt(url):
    """Build the dbt models on the target database (dbt runs in its Docker container)."""
    host = url.hostname
    if host in ("localhost", "127.0.0.1"):
        host = "postgres"          # inside Docker, the local database is called "postgres"
    sslmode = parse_qs(url.query).get("sslmode", ["prefer"])[0]
    env_args = ["-e", f"PG_HOST={host}", "-e", f"PG_PORT={url.port or 5432}",
                "-e", f"PG_USER={unquote(url.username)}", "-e", f"PG_PASSWORD={unquote(url.password)}",
                "-e", f"PG_DB={url.path.lstrip('/')}", "-e", f"PG_SSLMODE={sslmode}"]
    cmd = ["docker", "compose", "run", "--rm", "--no-deps", *env_args, "dbt", "dbt run && dbt test"]
    return subprocess.run(cmd, cwd=ROOT).returncode


def main():
    """Export the last N hours to the target database and build dbt there."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=int, default=6, help="how many hours of data to copy")
    args = parser.parse_args()

    target_url = os.getenv("TARGET_DATABASE_URL")
    if not target_url:
        sys.exit("Please set TARGET_DATABASE_URL first (see the top of this file).")
    url = urlparse(target_url)

    source = local_connection()
    target = psycopg2.connect(target_url)
    try:
        with target, target.cursor() as cur:
            print("Creating tables on the target ...")
            with open(os.path.join(ROOT, "sql", "init.sql"), encoding="utf-8") as f:
                cur.execute(f.read())
            cur.execute("create table if not exists demo_meta (snapshot_at timestamptz, source text)")
            cur.execute("truncate demo_meta, " + ", ".join(t for t, _ in TABLES))

        with source, target:
            for table, column in TABLES:
                rows = copy_table(source, target, table, column, args.hours)
                print(f"  {table:24} {rows:>9,} rows")
            with source.cursor() as cur:
                cur.execute("select now()")
                snapshot_at = cur.fetchone()[0]
            with target.cursor() as cur:
                cur.execute("insert into demo_meta values (%s, %s)",
                            (snapshot_at, f"last {args.hours} hours of the local pipeline"))
            # keep the id counter of pipeline_events after the copied rows
            with target.cursor() as cur:
                cur.execute("select setval(pg_get_serial_sequence('pipeline_events', 'id'), "
                            "coalesce((select max(id) from pipeline_events), 1))")
        print(f"Snapshot time: {snapshot_at}")
    finally:
        source.close()
        target.close()

    print("Building the dbt models on the target ...")
    if run_dbt(url) != 0:
        sys.exit("dbt failed on the target database, see the output above.")
    print("Done. The online dashboard (DEMO_MODE=1) can now read this database.")


if __name__ == "__main__":
    main()
