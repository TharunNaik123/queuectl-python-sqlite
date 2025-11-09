# QueueCTL  Python + SQLite CLI Background Job Queue

QueueCTL is a simple command-line tool that I built as part of a backend internship assignment.  
It’s written in Python, uses SQLite for persistent job storage, and supports basic background job execution with retry logic and a Dead Letter Queue.
Quick notes / assumptions (so examiners know what I did):

I used SQLite to keep things simple and persistent across restarts.

Workers are implemented with threads inside a single process for simplicity — this keeps code small and easier to understand. It still supports parallel processing by using multiple worker threads.

To avoid duplicate processing I use a simple transactional grab pattern: select a pending job and then update it to processing inside a transaction.

Backoff uses delay = base ** attempts seconds and jobs have a next_run_at timestamp that prevents them being retried before the delay.

worker start runs in the foreground. To stop, run queuectl worker stop which sets a stop flag the worker checks to gracefully exit after finishing current job.

---

## Features

- Enqueue and process background jobs
- Multiple worker threads
- Retry mechanism with exponential backoff
- Dead Letter Queue (DLQ) for failed jobs
- Persistent job data using SQLite
- Configurable retry and backoff values
- Simple CLI built with [Click](https://pypi.org/project/click/)

---

## Tech Stack

- **Language:** Python 3.8+
- **Database:** SQLite (file-based)
- **Libraries:**  
  - `click` — for CLI commands  
  - `tabulate` — for displaying tables  
  - `subprocess` — for command execution  

---

## Job Structure

Each job in the system follows this structure:

```json
{
  "id": "unique-job-id",
  "command": "echo 'Hello World'",
  "state": "pending",
  "attempts": 0,
  "max_retries": 3,
  "created_at": "2025-11-04T10:30:00Z",
  "updated_at": "2025-11-04T10:30:00Z"
}
| State        | Meaning                              |
| ------------ | ------------------------------------ |
| `pending`    | Waiting to be processed              |
| `processing` | Being executed by a worker           |
| `completed`  | Finished successfully                |
| `failed`     | Temporary failure, will retry        |
| `dead`       | Moved to DLQ after retries exhausted |

Setup Instructions
1️. Clone the repository
git clone https://github.com/<TharunNaik123>/queuectl-python-sqlite.git
cd queuectl-python-sqlite
2.Install dependencies
pip install click tabulate
3.Run the CLI
python queuectl.py

How to run (setup)
1.Make sure you have Python 3.9+ installed.
2.Install dependencies (only click is required):
3.Run CLI (examples below). The DB file queue.db will be created automatically.
Usage examples

Enqueue a job:
python queuectl.py enqueue '{"id":"job1","command":"echo hello","max_retries":3}'
Start 2 workers:
python queuectl.py worker start --count 2
Show status:
python queuectl.py status
List pending jobs:
python queuectl.py list --state pending
List DLQ:
python queuectl.py dlq list
Retry a DLQ job:
python queuectl.py dlq retry job1
Set config (max retries default):
python queuectl.py config set backoff_base 2
python queuectl.py config set default_max_retries 3
