"""Выдачи займов по регионам для страницы карты.

Выданный займ — заявка (transaction_id) со статусом 12 в postback_proxy.
Регион, город и дата заявки берутся из queues по transaction_id и кэшируются в cf.db
(таблица queue_geo: только дата заявки, регион и город — без ФИО, телефонов и паспортных данных).
"""
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timedelta

from regions import NAMES, clean_city, region_key

ISSUED = "12"
FMT = "%Y-%m-%d %H:%M:%S"


def _local(db_path):
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("""CREATE TABLE IF NOT EXISTS queue_geo (
        transaction_id TEXT PRIMARY KEY, date_created TEXT,
        reg_region TEXT, reg_city TEXT, res_region TEXT, res_city TEXT)""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_geo_date ON queue_geo(date_created)")
    return conn


def _issued_since(conn, since):
    # Займ выдаётся после создания заявки, поэтому достаточно выдач начиная с первой даты периода
    return conn.execute(
        """SELECT transaction_id, MIN(date)
           FROM postback_proxy INDEXED BY idx_date WHERE status = ? AND date >= ? GROUP BY transaction_id""",
        (ISSUED, since)).fetchall()


def fill_cache(db_path, syncer, since):
    """Догружает из queues регион и дату заявки для выданных займов, которых ещё нет в кэше."""
    conn = _local(db_path)
    try:
        ids = [r[0] for r in _issued_since(conn, since)]
        known = set()
        for i in range(0, len(ids), 900):
            chunk = ids[i:i + 900]
            known.update(r[0] for r in conn.execute(
                "SELECT transaction_id FROM queue_geo WHERE transaction_id IN (%s)" % ",".join("?" * len(chunk)), chunk))
        missing = [t for t in ids if t not in known]
        if not missing or not syncer.enabled:
            return 0
        remote = syncer.remote()
        try:
            cur = remote.cursor()
            for i in range(0, len(missing), 500):
                chunk = missing[i:i + 500]
                cur.execute(
                    """SELECT transaction_id, date_created, registration_region, registration_city,
                              residential_region, residential_city
                       FROM queues FORCE INDEX (transaction_id) WHERE transaction_id IN (%s)
                       ORDER BY id_request""" % ",".join(["%s"] * len(chunk)), chunk)
                found = {r[0]: r for r in cur.fetchall()}
                conn.executemany(
                    "INSERT OR REPLACE INTO queue_geo VALUES (?, ?, ?, ?, ?, ?)",
                    [(t,) + ((found[t][1].strftime(FMT) if found[t][1] else None,) + tuple(found[t][2:])
                             if t in found else (None, None, None, None, None)) for t in chunk])
                conn.commit()
        finally:
            remote.close()
        return len(missing)
    finally:
        conn.close()


def map_report(db_path, syncer, d_from, d_to, address="registration"):
    since = d_from.isoformat()
    until = (d_to + timedelta(days=1)).isoformat()
    error = None
    try:
        fill_cache(db_path, syncer, since)
    except Exception as e:  # без удалённой базы показываем то, что уже в кэше
        error = "%s: %s" % (type(e).__name__, e)

    conn = _local(db_path)
    try:
        issued = _issued_since(conn, since)
        geo = {}
        ids = [r[0] for r in issued]
        for i in range(0, len(ids), 900):
            chunk = ids[i:i + 900]
            for row in conn.execute("SELECT * FROM queue_geo WHERE transaction_id IN (%s)" % ",".join("?" * len(chunk)), chunk):
                geo[row[0]] = row
    finally:
        conn.close()

    regions = defaultdict(lambda: {"count": 0, "cities": Counter()})
    unknown_raw, not_in_queues, total = Counter(), 0, 0
    for t, issued_at in issued:
        g = geo.get(t)
        if not g or not g[1]:
            not_in_queues += 1
            continue
        if not (since <= g[1] < until):
            continue
        reg_region, reg_city, res_region, res_city = g[2:]
        region, city = (reg_region, reg_city) if address == "registration" else (res_region, res_city)
        key = region_key(region)
        if not key:  # если в выбранном адресе регион не распознан — пробуем другой адрес
            alt_region, alt_city = (res_region, res_city) if address == "registration" else (reg_region, reg_city)
            key = region_key(alt_region)
            if key:
                region, city = alt_region, alt_city
        if not key:
            unknown_raw[region or "(пусто)"] += 1
            key = "unknown"
        r = regions[key]
        r["count"] += 1
        r["cities"][clean_city(city) or "(город не указан)"] += 1
        total += 1

    out = []
    for key, r in regions.items():
        out.append({
            "key": key,
            "name": NAMES.get(key, "Регион не определён"),
            "count": r["count"],
            "cities": r["cities"].most_common(15),
            "cities_total": len(r["cities"]),
        })
    out.sort(key=lambda x: -x["count"])
    return {
        "from": d_from.isoformat(),
        "to": d_to.isoformat(),
        "address": address,
        "total": total,
        "regions": out,
        "unknown_raw": unknown_raw.most_common(20),
        "not_in_queues": not_in_queues,
        "error": error,
    }


def warm_up(db_path, syncer, days=93):
    """Заполняет кэш регионов за последние дни, чтобы карта открывалась сразу."""
    since = (datetime.now() - timedelta(days=days)).date().isoformat()
    return fill_cache(db_path, syncer, since)
