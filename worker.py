# worker.py
import threading
import time
import subprocess
from datetime import datetime, timedelta
from models import fetch_and_lock_next_job, set_job_state, move_to_dead, get_job
from config import get_config


_stop_flag = threading.Event()


def _sleep_with_shutdown(seconds):
# sleep in small chunks to react to stop signals
left = seconds
while left > 0 and not _stop_flag.is_set():
t = min(0.5, left)
time.sleep(t)
left -= t


def _run_command(command):
try:
p = subprocess.run(command, shell=True)
return p.returncode == 0
except Exception:
return False


def worker_loop(worker_id):
base = float(get_config('backoff_base', 2))
while not _stop_flag.is_set():
job = fetch_and_lock_next_job()
if not job:
time.sleep(0.5)
continue
print(f'[worker {worker_id}] picked job {job["id"]} (attempt {job["attempts"]})')
success = _run_command(job['command'])
if success:
set_job_state(job['id'], 'completed', attempts=job['attempts'])
print(f'[worker {worker_id}] completed {job["id"]}')
continue
# failed
attempts = job['attempts']
max_retries = job['max_retries']
if attempts <= max_retries:
# schedule next run
delay = base ** attempts
next_run = (datetime.utcnow() + timedelta(seconds=delay)).isoformat() + 'Z'
set_job_state(job['id'], 'pending', attempts=attempts, next_run_at=next_run)
print(f'[worker {worker_id}] job {job["id"]} failed, retrying in {delay}s (attempt {attempts}/{max_retries})')
_sleep_with_shutdown(min(delay, 2)) # backoff is handled by next_run_at, but pause a little to avoid busy loop
else
