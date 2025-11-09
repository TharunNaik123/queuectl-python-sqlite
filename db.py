# db.py
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime


DB_PATH = 'queue.db'
_conn_lock = threading.Lock()


def init_db():
with _get_conn() as conn:
cur = conn.cursor()
cur.execute('''
CREATE TABLE IF NOT EXISTS jobs (
id TEXT PRIMARY KEY,
command TEXT NOT NULL,
state TEXT NOT NULL,
attempts INTEGER NOT NULL,
max_retries INTEGER NOT NULL,
created_at TEXT NOT NULL,
updated_at TEXT NOT NULL,
next_run_at TEXT
)
''')
cur.execute('''
CREATE TABLE IF NOT EXISTS config (
key TEXT PRIMARY KEY,
value TEXT
)
''')
conn.commit()


@contextmanager
def _get_conn():
# serialize sqlite access at Python level to avoid "database is locked" in simple scenarios
with _conn_lock:
conn = sqlite3.connect(DB_PATH, timeout=30.0, isolation_level=None) # autocommit off
conn.execute('PRAGMA foreign_keys = ON')
try:
yield conn
finally:
conn.close()


def execute(query, params=()):
with _get_conn() as conn:
cur = conn.cursor()
cur.execute('BEGIN IMMEDIATE')
cur.execute(query, params)
conn.commit()
return cur


def query_one(query, params=()):
with _get_conn() as conn:
cur = conn.cursor()
cur.execute(query, params)
return cur.fetchone()


def query_all(query, params=()):
with _get_conn() as conn:
cur = conn.cursor()
cur.execute(query, params)
return cur.fetchall()


def now_iso():
return datetime.utcnow().isoformat() + 'Z'

