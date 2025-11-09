# QueueCTL  Python + SQLite CLI Background Job Queue

QueueCTL is a simple command-line tool that I built as part of a backend internship assignment.  
It’s written in Python, uses SQLite for persistent job storage, and supports basic background job execution with retry logic and a Dead Letter Queue.

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

## ⚙️ Tech Stack

- **Language:** Python 3.8+
- **Database:** SQLite (file-based)
- **Libraries:**  
  - `click` — for CLI commands  
  - `tabulate` — for displaying tables  
  - `subprocess` — for command execution  

---

## 🧩 Job Structure

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
git clone https://github.com/<your-username>/queuectl-python-sqlite.git
cd queuectl-python-sqlite
2.Install dependencies
pip install click tabulate
3.Run the CLI
python queuectl.py --help


