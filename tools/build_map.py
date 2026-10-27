"""Сборка static/russia.json — упрощённых контуров субъектов РФ для карты выдач.

Исходники (данные OpenStreetMap, ODbL):
1. Субъекты РФ: https://github.com/timurkanaz/Russia_geojson_OSM, файл Russia_regions.geojson (~52 МБ).
2. ДНР, ЛНР, Запорожская и Херсонская области — контуры из geoBoundaries:
   https://www.geoboundaries.org/api/current/gbOpen/UKR/ADM1/ (geoBoundaries-UKR-ADM1.geojson).

Запуск: python3 tools/build_map.py путь/к/Russia_regions.geojson путь/к/geoBoundaries-UKR-ADM1.geojson
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from regions import NAMES, region_key  # noqa: E402

TOLERANCE = 0.04   # градусы: упрощение Дугласа — Пекера
MIN_AREA = 0.02    # кв. градусы: мелкие острова отбрасываются (самый крупный контур региона остаётся всегда)
PRECISION = 2
# shapeISO в geoBoundaries → ключ субъекта
EXTRA = {"UA-14": "dnr", "UA-09": "lnr", "UA-23": "zaporozhye", "UA-65": "kherson"}


def simplify(points, tol):
    if len(points) < 4:
        return points
    keep = [False] * len(points)
    keep[0] = keep[-1] = True
    stack = [(0, len(points) - 1)]
    tol2 = tol * tol
    while stack:
        a, b = stack.pop()
        ax, ay = points[a]
        bx, by = points[b]
        dx, dy = bx - ax, by - ay
        norm = dx * dx + dy * dy
        best, idx = -1.0, -1
        for i in range(a + 1, b):
            px, py = points[i]
            if norm == 0:
                d = (px - ax) ** 2 + (py - ay) ** 2
            else:
                t = ((px - ax) * dx + (py - ay) * dy) / norm
                t = max(0.0, min(1.0, t))
                d = (px - ax - t * dx) ** 2 + (py - ay - t * dy) ** 2
            if d > best:
                best, idx = d, i
        if best > tol2:
            keep[idx] = True
            stack += [(a, idx), (idx, b)]
    return [p for p, k in zip(points, keep) if k]


def signed_area(ring):
    return sum(x1 * y2 - x2 * y1 for (x1, y1), (x2, y2) in zip(ring, ring[1:])) / 2


def wind(ring, clockwise):
    """d3-geo требует: внешний контур — по часовой стрелке, дыры — против (иначе регион заливается «наизнанку»)."""
    return ring if (signed_area(ring) < 0) == clockwise else ring[::-1]


def area(ring):
    return abs(sum(x1 * y2 - x2 * y1 for (x1, y1), (x2, y2) in zip(ring, ring[1:]))) / 2


def load_sources(src, extra):
    """(ключ, geometry) из обоих исходников."""
    with open(src, encoding="utf-8") as f:
        for feat in json.load(f)["features"]:
            yield region_key(feat["properties"]["region"]), feat["geometry"]
    if extra:
        with open(extra, encoding="utf-8") as f:
            for feat in json.load(f)["features"]:
                key = EXTRA.get(feat["properties"].get("shapeISO"))
                if key:
                    yield key, feat["geometry"]


def main(src, extra, dst):
    features = []
    for key, geom in load_sources(src, extra):
        polys = geom["coordinates"] if geom["type"] == "MultiPolygon" else [geom["coordinates"]]
        largest = max((p[0] for p in polys), key=area)
        out = []
        for poly in polys:
            outer = poly[0]
            if outer is not largest and area(outer) < MIN_AREA:
                continue
            rings = []
            for ring in poly:
                r = [[round(x, PRECISION), round(y, PRECISION)] for x, y in simplify(ring, TOLERANCE)]
                r = [p for i, p in enumerate(r) if i == 0 or p != r[i - 1]]
                if len(r) >= 4:
                    rings.append(r)
                elif ring is outer:
                    rings.append([[round(x, PRECISION + 1), round(y, PRECISION + 1)] for x, y in ring])
            if rings:
                out.append([wind(r, i == 0) for i, r in enumerate(rings)])
        features.append({"type": "Feature", "properties": {"key": key, "name": NAMES[key]},
                         "geometry": {"type": "MultiPolygon", "coordinates": out}})
    with open(dst, "w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f, ensure_ascii=False, separators=(",", ":"))
    print("%d регионов, %.0f КБ" % (len(features), os.path.getsize(dst) / 1024))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "static", "russia.json"))
