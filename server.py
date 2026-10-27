"""Отчёт: время между статусами заявки (по умолчанию 6 → 10)
по сотрудникам (user) и дням.

Запуск: python3 server.py [путь_к_cf.db] [порт]
Открыть: http://localhost:8000

Если в .env указан доступ к удалённой базе, cf.db раз в минуту догружается из неё (см. sync.py).
"""
import csv
import glob
import json
import os
import sqlite3
import sys
import threading
import time
from datetime import date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import geo
from sync import Syncer, load_env

HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "cf.db")
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 8000
HOST = os.environ.get("HOST", "127.0.0.1")  # в Docker — 0.0.0.0
# Справочник партнёров (token = id). По умолчанию — самый свежий cf_*.csv рядом с дампом
PARTNERS_CSV = sys.argv[3] if len(sys.argv) > 3 else None
MAX_DAYS = 93
SYNC = Syncer(DB_PATH, load_env())

# Интервалы, которые можно выбрать в отчёте: (начальный статус, конечный статус)
# Интервал задаётся как "начальный-конечный" статус; можно выбрать любые два статуса из базы
DEFAULT_INTERVAL = ("6", "10")

# Для каждой заявки, получившей конечный статус в периоде:
#   d_start — первое время начального статуса (ищется за всё время, он мог быть раньше периода),
#   d_end   — первое время конечного статуса,
#   user    — сотрудник из строки конечного статуса, а если там пусто — из последней строки заявки, где он указан
#             (в строках статуса 6 user всегда пустой, поэтому связываем по transaction_id),
#   token, skm — из строки конечного статуса.
QUERY = """
WITH e AS (
  SELECT transaction_id AS t, MIN(date) AS d_end, MAX(NULLIF(user, '')) AS user, MAX(token) AS token,
    MAX(NULLIF(skm, '')) AS skm
  FROM postback_proxy INDEXED BY idx_date
  WHERE status = :end AND date >= :from AND date < :to
  GROUP BY transaction_id
)
SELECT
  e.t,
  e.d_end,
  (SELECT MIN(p.date) FROM postback_proxy p
    WHERE p.transaction_id = e.t AND p.status = :start) AS d_start,
  COALESCE(e.user, (SELECT p.user FROM postback_proxy p
    WHERE p.transaction_id = e.t AND p.user <> '' ORDER BY p.id DESC LIMIT 1)) AS user,
  e.token,
  e.skm
FROM e
"""
FMT = "%Y-%m-%d %H:%M:%S"


def partners():
    """{id: name} партнёров: из таблицы lidogen удалённой базы, а если её нет — из CSV.
    Остальные колонки (в т.ч. ключи API) не читаем и не отдаём."""
    if SYNC.partners:
        return SYNC.partners
    found = glob.glob(os.path.join(HERE, "cf_*.csv")) + glob.glob(os.path.join(os.path.dirname(DB_PATH), "cf_*.csv"))
    path = PARTNERS_CSV or max(found, key=os.path.getmtime, default=None)
    if not path or not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8-sig", newline="") as f:
        return {r["id"].strip(): r["name"].strip() for r in csv.DictReader(f) if r.get("id") and r.get("name")}


_status_cache = {"at": 0, "names": {}, "all": []}


def status_names(db, last):
    """Статусы за последние 30 дней данных и их названия: самый частый непустой comment статуса.
    Возвращает ({status: name}, [все статусы]). Кэшируется на 10 минут."""
    if time.time() - _status_cache["at"] > 600 or not _status_cache["all"]:
        since = (datetime.strptime(last, FMT) - timedelta(days=30)).strftime(FMT)
        names, seen = {}, set()
        for status, comment, _ in db.execute(
                """SELECT status, comment, COUNT(*) AS n FROM postback_proxy INDEXED BY idx_date
                   WHERE date >= ? GROUP BY status, comment ORDER BY n""", (since,)):
            seen.add(status)
            if comment:
                names[status] = comment  # по возрастанию частоты — остаётся самый частый
        _status_cache.update(at=time.time(), names=names, all=sorted(seen, key=lambda x: (not x.isdigit(), int(x) if x.isdigit() else 0, x)))
    return _status_cache["names"], _status_cache["all"]


def connect():
    return sqlite3.connect("file:%s?mode=ro" % DB_PATH, uri=True)


def report(date_from, date_to, interval):
    db = connect()
    try:
        last = db.execute("SELECT MAX(date) FROM postback_proxy").fetchone()[0]
        last_day = date.fromisoformat(last[:10])
        try:
            d_to = date.fromisoformat(date_to) if date_to else last_day
        except ValueError:
            d_to = last_day
        try:
            d_from = date.fromisoformat(date_from) if date_from else d_to - timedelta(days=d_to.weekday())
        except ValueError:
            d_from = d_to - timedelta(days=d_to.weekday())
        if d_from > d_to:
            d_from, d_to = d_to, d_from
        d_from = max(d_from, d_to - timedelta(days=MAX_DAYS - 1))
        names, all_statuses = status_names(db, last)
        st_start, _, st_end = (interval or "").partition("-")
        if st_start not in all_statuses or st_end not in all_statuses or st_start == st_end:
            st_start, st_end = DEFAULT_INTERVAL

        rows = db.execute(QUERY, {"start": st_start, "end": st_end, "from": d_from.isoformat(),
                                  "to": (d_to + timedelta(days=1)).isoformat()}).fetchall()
    finally:
        db.close()

    items, no_start, negative = [], 0, 0
    for t, d_end, d_start, user, token, skm in rows:
        if not d_start:
            no_start += 1
            continue
        minutes = (datetime.strptime(d_end, FMT) - datetime.strptime(d_start, FMT)).total_seconds() / 60
        if minutes < 0:
            negative += 1
            continue
        items.append({"t": t, "day": d_end[:10], "user": user or "", "token": token or "", "skm": skm or "",
                      "min": round(minutes, 1), "start_at": d_start, "end_at": d_end})

    days = [(d_from + timedelta(days=i)).isoformat() for i in range((d_to - d_from).days + 1)]
    return {
        "from": d_from.isoformat(),
        "to": d_to.isoformat(),
        "last_event": last,
        "days": days,
        "items": items,
        "partners": partners(),
        "interval": "%s-%s" % (st_start, st_end),
        "statuses": all_statuses,
        "start": {"status": st_start, "name": names.get(st_start, "")},
        "end": {"status": st_end, "name": names.get(st_end, "")},
        "status_names": names,
        "no_start": no_start,
        "negative": negative,
        "max_days": MAX_DAYS,
        "sync": {
            "enabled": SYNC.enabled,
            "last": SYNC.last_sync.strftime(FMT) if SYNC.last_sync else None,
            "error": SYNC.last_error,
        },
    }


def status_times(ids):
    """{transaction_id: {status: время первого появления}} из локальной копии postback_proxy."""
    result = {}
    db = connect()
    try:
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            for t, status, at in db.execute(
                    "SELECT transaction_id, status, MIN(date) FROM postback_proxy WHERE transaction_id IN (%s) "
                    "GROUP BY transaction_id, status" % ",".join("?" * len(chunk)), chunk):
                result.setdefault(t, {})[status] = at
    finally:
        db.close()
    return result


def applications(ids):
    """Данные клиентов из queues по transaction_id: только дата заявки, ФИО и телефон.
    Запрашиваются по клику на ячейку и нигде не сохраняются."""
    if not SYNC.enabled:
        raise RuntimeError("нет доступа к удалённой базе (.env)")
    result = {}
    conn = SYNC.remote()
    try:
        cur = conn.cursor()
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            cur.execute(
                """SELECT transaction_id, date_created, last_name, first_name, middle_name, phone
                   FROM queues WHERE transaction_id IN (%s) ORDER BY id_request""" % ",".join(["%s"] * len(chunk)),
                chunk)
            for t, created, last_name, first_name, middle_name, phone in cur.fetchall():
                result[t] = {
                    "created": created.strftime(FMT) if created else None,
                    "fio": " ".join(x for x in (last_name, first_name, middle_name) if x),
                    "phone": phone or "",
                }
    finally:
        conn.close()
    return result


PAGES = {"/": ("call-centar.html", "text/html; charset=utf-8"), "/call-centar": ("call-centar.html", "text/html; charset=utf-8"), "/map": ("map.html", "text/html; charset=utf-8")}
STATIC = {"/static/russia.json": ("static/russia.json", "application/json"),
          "/static/d3.min.js": ("static/d3.min.js", "text/javascript; charset=utf-8")}


def map_data(date_from, date_to, address):
    """Выдачи по регионам; фильтр — по дате заявки (queues.date_created). По умолчанию — последние 30 дней."""
    db = connect()
    try:
        last_day = date.fromisoformat(db.execute("SELECT MAX(date) FROM postback_proxy").fetchone()[0][:10])
    finally:
        db.close()
    try:
        d_to = date.fromisoformat(date_to) if date_to else last_day
    except ValueError:
        d_to = last_day
    try:
        d_from = date.fromisoformat(date_from) if date_from else d_to - timedelta(days=29)
    except ValueError:
        d_from = d_to - timedelta(days=29)
    if d_from > d_to:
        d_from, d_to = d_to, d_from
    d_from = max(d_from, d_to - timedelta(days=366))
    data = geo.map_report(DB_PATH, SYNC, d_from, d_to, "residential" if address == "residential" else "registration")
    data["last_day"] = last_day.isoformat()
    return data


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if urlparse(self.path).path != "/api/applications":
            self.send_error(404)
            return
        ids = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}").get("ids", [])
        ids = [str(x) for x in ids[:5000]]
        payload, code = {"statuses": status_times(ids), "items": {}}, 200
        try:
            payload["items"] = applications(ids)
        except Exception as e:  # без удалённой базы времена статусов всё равно отдаём
            payload["error"] = "%s: %s" % (type(e).__name__, e)
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        url = urlparse(self.path)
        if url.path == "/api/report":
            q = parse_qs(url.query)
            data = report(q.get("from", [None])[0], q.get("to", [None])[0], q.get("interval", [None])[0])
            body = json.dumps(data, ensure_ascii=False).encode()
            ctype = "application/json; charset=utf-8"
        elif url.path == "/api/map":
            q = parse_qs(url.query)
            body = json.dumps(map_data(q.get("from", [None])[0], q.get("to", [None])[0],
                                       q.get("address", ["registration"])[0]), ensure_ascii=False).encode()
            ctype = "application/json; charset=utf-8"
        elif url.path in PAGES or url.path in STATIC:
            with open(os.path.join(HERE, (PAGES.get(url.path) or STATIC[url.path])[0]), "rb") as f:
                body = f.read()
            ctype = (PAGES.get(url.path) or STATIC[url.path])[1]
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    print("Отчёт: http://localhost:%d  (база: %s)" % (PORT, DB_PATH))
    if SYNC.enabled:
        print("Синхронизация с %s каждые %d с" % (SYNC.env["DB_HOST"], SYNC.interval))
        SYNC.start()
        threading.Thread(target=lambda: print("Кэш регионов: догружено %d заявок" % geo.warm_up(DB_PATH, SYNC), flush=True),
                         daemon=True).start()
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
