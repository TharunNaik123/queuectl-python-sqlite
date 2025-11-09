# models.py
def create_job(jobdict):
job = {
'id': jobdict['id'],
'command': jobdict['command'],
'state': jobdict.get('state', 'pending'),
'attempts': int(jobdict.get('attempts', 0)),
'max_retries': int(jobdict.get('max_retries', 3)),
'created_at': jobdict.get('created_at', now_iso()),
'updated_at': jobdict.get('updated_at', now_iso()),
'next_run_at': jobdict.get('next_run_at')
}
execute('''INSERT INTO jobs(id,command,state,attempts,max_retries,created_at,updated_at,next_run_at)
VALUES (?,?,?,?,?,?,?,?)''',
(job['id'], job['command'], job['state'], job['attempts'], job['max_retries'], job['created_at'], job['updated_at'], job['next_run_at']))
return job


def get_job(jobid):
row = query_one('SELECT id,command,state,attempts,max_retries,created_at,updated_at,next_run_at FROM jobs WHERE id=?', (jobid,))
if not row:
return None
keys = ['id','command','state','attempts','max_retries','created_at','updated_at','next_run_at']
return dict(zip(keys, row))


def list_jobs(state=None):
if state:
rows = query_all('SELECT id,command,state,attempts,max_retries,created_at,updated_at,next_run_at FROM jobs WHERE state=? ORDER BY created_at', (state,))
else:
rows = query_all('SELECT id,command,state,attempts,max_retries,created_at,updated_at,next_run_at FROM jobs ORDER BY created_at')
keys = ['id','command','state','attempts','max_retries','created_at','updated_at','next_run_at']
return [dict(zip(keys, r)) for r in rows]


def move_to_dead(jobid):
execute('UPDATE jobs SET state=?, updated_at=? WHERE id=?', ('dead', now_iso(), jobid))


def set_job_state(jobid, state, attempts=None, next_run_at=None):
parts = []
params = []
parts.append('state=?')
params.append(state)
if attempts is not None:
parts.append('attempts=?')
params.append(attempts)
if next_run_at is not None:
parts.append('next_run_at=?')
params.append(next_run_at)
parts.append('updated_at=?')
params.append(now_iso())
params.append(jobid)
execute('UPDATE jobs SET ' + ','.join(parts) + ' WHERE id=?', tuple(params))


# transactional fetch-and-lock pattern
from db import _get_conn


def fetch_and_lock_next_job():
# return job dict or None
with _get_conn() as conn:
cur = conn.cursor()
cur.execute('BEGIN IMMEDIATE')
# select a pending job with next_run_at <= now or NULL
cur.execute("SELECT id,command,attempts,max_retries FROM jobs WHERE state='pending' AND (next_run_at IS NULL OR next_run_at <= ?) ORDER BY created_at LIMIT 1", (now_iso(),))
row = cur.fetchone()
if not row:
conn.commit()
return None
jobid, command, attempts, max_retries = row
# attempt to lock by updating state to processing and increment attempts
cur.execute('UPDATE jobs SET state=?, attempts=?, updated_at=? WHERE id=? AND state=?', ('processing', attempts+1, now_iso(), jobid, 'pending'))
if cur.rowcount == 0:
conn.commit()
return None
conn.commit()
return get_job(jobid)
