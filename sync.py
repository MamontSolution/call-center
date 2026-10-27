"""Синхронизация локальной cf.db с удалённой базой MariaDB.

Забирает новые строки postback_proxy (id больше последнего локального) и строки,
изменённые после прошлой синхронизации (по индексу updated_at), а также справочник
партнёров из таблицы lidogen (только id и name).

Доступ к базе читается из .env рядом с этим файлом:
DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME.

Ручной запуск: python3 sync.py [путь_к_cf.db]
"""
import os
import sqlite3
import sys
import threading
import time
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
BATCH = 20000
COLUMNS = ["id", "date", "token", "goal_id", "transaction_id", "personalarea_id", "uid", "status", "amount",
           "comment", "resend", "http_code", "source", "skm", "fin_product", "departament", "vkm",
           "personal_lk_id", "approved_sum", "updated_at", "user"]
SELECT = "SELECT %s FROM postback_proxy" % ", ".join("`%s`" % c for c in COLUMNS)
UPSERT = "INSERT OR REPLACE INTO postback_proxy (%s) VALUES (%s)" % (
    ", ".join('"%s"' % c for c in COLUMNS), ", ".join("?" * len(COLUMNS)))


def load_env(path=os.path.join(HERE, ".env")):
    if not os.path.exists(path):
        return {}
    env = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


def as_text(v):
    # В локальной базе даты хранятся строками, как в дампе
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    return v


class Syncer:
    def __init__(self, db_path, env, interval=60):
        self.db_path = db_path
        self.env = env
        self.interval = interval
        self.partners = {}
        self.last_sync = None
        self.last_error = None
        self.lock = threading.Lock()

    @property
    def enabled(self):
        return bool(self.env.get("DB_HOST"))

    def remote(self):
        import pymysql
        return pymysql.connect(
            host=self.env["DB_HOST"], port=int(self.env.get("DB_PORT", 3306)),
            user=self.env["DB_USER"], password=self.env["DB_PASSWORD"], database=self.env.get("DB_NAME", "cf"),
            charset="utf8mb4", connect_timeout=10, read_timeout=300)

    def run_once(self):
        with self.lock:
            local = sqlite3.connect(self.db_path, timeout=30)
            try:
                local.execute("PRAGMA journal_mode=WAL")
                local.execute("CREATE TABLE IF NOT EXISTS sync_state (key TEXT PRIMARY KEY, value TEXT)")
                state = dict(local.execute("SELECT key, value FROM sync_state"))
                max_id = local.execute("SELECT MAX(id) FROM postback_proxy").fetchone()[0] or 0
                # Изменённые строки ищем с запасом в минуту от последнего известного updated_at
                since = state.get("updated_at") or local.execute(
                    "SELECT MAX(updated_at) FROM postback_proxy WHERE id > ?", (max_id - 50000,)).fetchone()[0]

                remote = self.remote()
                new_rows = updated_rows = 0
                try:
                    cur = remote.cursor()
                    while True:
                        cur.execute(SELECT + " WHERE id > %s ORDER BY id LIMIT %s", (max_id, BATCH))
                        rows = [tuple(as_text(v) for v in r) for r in cur.fetchall()]
                        if not rows:
                            break
                        local.executemany(UPSERT, rows)
                        local.commit()
                        new_rows += len(rows)
                        max_id = rows[-1][0]

                    if since:
                        cur.execute(SELECT + " WHERE updated_at >= %s - INTERVAL 1 MINUTE AND id <= %s", (since, max_id))
                        rows = [tuple(as_text(v) for v in r) for r in cur.fetchall()]
                        local.executemany(UPSERT, rows)
                        updated_rows = len(rows)

                    cur.execute("SELECT id, name FROM lidogen")
                    self.partners = {str(i): n for i, n in cur.fetchall() if n}
                finally:
                    remote.close()

                newest = local.execute(
                    "SELECT MAX(updated_at) FROM postback_proxy WHERE id > ?", (max_id - 50000,)).fetchone()[0]
                local.execute("INSERT OR REPLACE INTO sync_state VALUES ('updated_at', ?)", (newest,))
                local.commit()
            finally:
                local.close()
            self.last_sync = datetime.now()
            self.last_error = None
            return new_rows, updated_rows

    def loop(self):
        while True:
            try:
                new_rows, updated_rows = self.run_once()
                if new_rows or updated_rows:
                    print("%s синхронизация: новых %d, изменённых %d" % (
                        self.last_sync.strftime("%H:%M:%S"), new_rows, updated_rows), flush=True)
            except Exception as e:  # сеть или база недоступны — отчёт продолжает работать по локальным данным
                self.last_error = "%s: %s" % (type(e).__name__, e)
                print("Ошибка синхронизации: " + self.last_error, flush=True)
            time.sleep(self.interval)

    def start(self):
        threading.Thread(target=self.loop, daemon=True).start()


if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "cf.db")
    s = Syncer(db, load_env())
    if not s.enabled:
        sys.exit("Нет доступа к базе: заполните .env")
    t0 = time.time()
    n, u = s.run_once()
    print("Новых строк: %d, изменённых: %d, партнёров: %d, %.1f с" % (n, u, len(s.partners), time.time() - t0))
