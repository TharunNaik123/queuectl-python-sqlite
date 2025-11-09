# config.py
from db import query_one, execute, query_all


def set_config(key, value):
existing = query_one('SELECT key FROM config WHERE key=?', (key,))
if existing:
execute('UPDATE config SET value=? WHERE key=?', (str(value), key))
else:
execute('INSERT INTO config(key,value) VALUES (?,?)', (key, str(value)))
def get_config(key, default=None):
row = query_one('SELECT value FROM config WHERE key=?', (key,))
return row[0] if row else default


def all_config():
rows = query_all('SELECT key,value FROM config')
return dict(rows)
