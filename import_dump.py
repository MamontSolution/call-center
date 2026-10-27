"""Импорт MySQL-дампа (Sequel Ace) таблицы postback_proxy в SQLite.

Использование: python3 import_dump.py ../cf_2026-10-23.sql cf.db
"""
import os
import subprocess
import sys

SCHEMA = """
PRAGMA journal_mode=OFF;
PRAGMA synchronous=OFF;
DROP TABLE IF EXISTS postback_proxy;
CREATE TABLE postback_proxy (
  id INTEGER PRIMARY KEY,
  date TEXT, token TEXT, goal_id TEXT, transaction_id TEXT, personalarea_id TEXT,
  uid TEXT, status TEXT, amount TEXT, comment TEXT, resend TEXT, http_code TEXT,
  source TEXT, skm TEXT, fin_product TEXT, departament TEXT, vkm TEXT,
  personal_lk_id TEXT, approved_sum TEXT, updated_at TEXT, user TEXT
);
BEGIN;
"""

INDEXES = """
COMMIT;
CREATE INDEX idx_date ON postback_proxy(date);
CREATE INDEX idx_txn ON postback_proxy(transaction_id, id);
ANALYZE;
"""


def main(src, dst):
    if os.path.exists(dst):
        os.remove(dst)
    proc = subprocess.Popen(["sqlite3", "-bail", dst], stdin=subprocess.PIPE)
    out = proc.stdin
    out.write(SCHEMA.encode())
    in_data = False
    done = 0
    total = os.path.getsize(src)
    with open(src, "rb") as f:
        for line in f:
            done += len(line)
            if line.startswith(b"INSERT INTO"):
                in_data = True
            if in_data:
                # В дампе встречается только экранирование \" — в SQLite оно не нужно
                out.write(line.replace(b'\\"', b'"'))
                if line.rstrip().endswith(b");"):
                    in_data = False
                    sys.stderr.write("\r%5.1f%%" % (done * 100 / total))
    out.write(INDEXES.encode())
    out.close()
    sys.exit(proc.wait())


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
