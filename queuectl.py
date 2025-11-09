import os
import sqlite3
import json
import click
import subprocess
import threading
import time
from datetime import datetime
from tabulate import tabulate

DB_PATH = "queue.db"
CONFIG_DEFAULTS = {
    "max_retries": 3,
    "backoff_base": 2,
}


# ---------------- Database ----------------
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        command TEXT,
        state TEXT,
        attempts INTEGER,
        max_retries INTEGER,
        created_at TEXT,
        updated_at TEXT,
        next_run_at REAL
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """)

    # Insert defaults if not present
    for k, v in CONFIG_DEFAULTS.items():
        c.execute("INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)", (k, str(v)))

    conn.commit()
    conn.close()


# ---------------- Helpers ----------------
def get_config(key):
    conn = get_conn()
    cur = conn.execute("SELECT value FROM config WHERE key=?", (key,))
    row = cur.fetchone()
    conn.close()
    return int(row["value"]) if row else CONFIG_DEFAULTS[key]


def set_config(key, value):
    conn = get_conn()
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, str(value)))
    conn.commit()
    conn.close()


def now():
    return datetime.utcnow().isoformat()


# ---------------- Job Operations ----------------
def enqueue_job(job_json):
    job = json.loads(job_json)
    job.setdefault("state", "pending")
    job.setdefault("attempts", 0)
    job.setdefault("max_retries", get_config("max_retries"))
    job.setdefault("created_at", now())
    job.setdefault("updated_at", now())
    job.setdefault("next_run_at", time.time())

    conn = get_conn()
    conn.execute("""
        INSERT OR REPLACE INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at, next_run_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        job["id"], job["command"], job["state"], job["attempts"],
        job["max_retries"], job["created_at"], job["updated_at"], job["next_run_at"]
    ))
    conn.commit()
    conn.close()
    click.echo(f" Job {job['id']} enqueued.")


def get_next_job():
    conn = get_conn()
    cur = conn.execute("""
        SELECT * FROM jobs
        WHERE state='pending' AND next_run_at <= ?
        ORDER BY created_at ASC
        LIMIT 1
    """, (time.time(),))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def update_job_state(job_id, state):
    conn = get_conn()
    conn.execute("UPDATE jobs SET state=?, updated_at=? WHERE id=?", (state, now(), job_id))
    conn.commit()
    conn.close()


def update_job_attempt(job_id, attempts, next_run_at):
    conn = get_conn()
    conn.execute("""
        UPDATE jobs SET attempts=?, next_run_at=?, updated_at=? WHERE id=?
    """, (attempts, next_run_at, now(), job_id))
    conn.commit()
    conn.close()


def move_to_dlq(job_id):
    update_job_state(job_id, "dead")


# ---------------- Worker Logic ----------------
def process_job(job):
    update_job_state(job["id"], "processing")
    click.echo(f"⚙️  Processing job {job['id']} -> {job['command']}")
    try:
        result = subprocess.run(job["command"], shell=True)
        if result.returncode == 0:
            update_job_state(job["id"], "completed")
            click.echo(f" Job {job['id']} completed successfully.")
        else:
            raise Exception("Command failed")
    except Exception as e:
        attempts = job["attempts"] + 1
        if attempts >= job["max_retries"]:
            move_to_dlq(job["id"])
            click.echo(f" Job {job['id']} failed permanently → moved to DLQ.")
        else:
            delay = get_config("backoff_base") ** attempts
            update_job_attempt(job["id"], attempts, time.time() + delay)
            update_job_state(job["id"], "pending")
            click.echo(f"Job {job['id']} failed. Retrying in {delay}s...")


def worker_loop(worker_id, stop_event):
    click.echo(f" Worker-{worker_id} started.")
    while not stop_event.is_set():
        job = get_next_job()
        if job:
            process_job(job)
        else:
            time.sleep(1)
    click.echo(f" Worker-{worker_id} stopped.")


# ---------------- CLI ----------------
@click.group()
def cli():
    """QueueCTL - CLI Background Job Queue"""
    init_db()


@cli.command()
@click.argument("job_json")
def enqueue(job_json):
    """Add a new job to the queue"""
    enqueue_job(job_json)


@cli.group()
def worker():
    """Worker management commands"""
    pass


@worker.command("start")
@click.option("--count", default=1, help="Number of workers to start")
def worker_start(count):
    """Start worker(s)"""
    stop_event = threading.Event()
    threads = []

    for i in range(count):
        t = threading.Thread(target=worker_loop, args=(i + 1, stop_event), daemon=True)
        t.start()
        threads.append(t)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        stop_event.set()
        for t in threads:
            t.join()


@cli.command()
@click.option("--state", default=None, help="Filter by job state")
def list(state):
    """List jobs by state"""
    conn = get_conn()
    if state:
        cur = conn.execute("SELECT * FROM jobs WHERE state=?", (state,))
    else:
        cur = conn.execute("SELECT * FROM jobs")
    rows = cur.fetchall()
    conn.close()
    if rows:
        click.echo(tabulate([dict(r) for r in rows], headers="keys"))
    else:
        click.echo("No jobs found.")


@cli.command()
def status():
    """Show job status summary"""
    conn = get_conn()
    cur = conn.execute("SELECT state, COUNT(*) as count FROM jobs GROUP BY state")
    rows = cur.fetchall()
    conn.close()
    if rows:
        click.echo(tabulate([dict(r) for r in rows], headers="keys"))
    else:
        click.echo("No jobs in queue.")


@cli.group()
def dlq():
    """Dead Letter Queue operations"""
    pass


@dlq.command("list")
def dlq_list():
    """List DLQ jobs"""
    conn = get_conn()
    cur = conn.execute("SELECT * FROM jobs WHERE state='dead'")
    rows = cur.fetchall()
    conn.close()
    if rows:
        click.echo(tabulate([dict(r) for r in rows], headers="keys"))
    else:
        click.echo("No DLQ jobs found.")


@dlq.command("retry")
@click.argument("job_id")
def dlq_retry(job_id):
    """Retry a DLQ job"""
    conn = get_conn()
    conn.execute("""
        UPDATE jobs
        SET state='pending', attempts=0, next_run_at=?, updated_at=?
        WHERE id=? AND state='dead'
    """, (time.time(), now(), job_id))
    conn.commit()
    conn.close()
    click.echo(f"  Job {job_id} moved from DLQ to pending.")


@cli.group()
def config():
    """Configuration commands"""
    pass


@config.command("set")
@click.argument("key")
@click.argument("value")
def config_set(key, value):
    set_config(key, value)
    click.echo(f"🔧 Config {key} = {value}")


@config.command("get")
@click.argument("key")
def config_get(key):
    click.echo(f"{key} = {get_config(key)}")


if __name__ == "__main__":
    cli()
 
