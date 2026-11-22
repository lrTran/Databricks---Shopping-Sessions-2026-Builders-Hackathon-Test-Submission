#!/usr/bin/env python3
"""
A slow, literal reference implementation of rules.md, for differential testing.

It shares no code with the Rust pipeline and favours obviousness over speed:
use it on fixtures and small synthetic datasets (up to a few hundred thousand
events), never on the full dev set.

Usage: reference.py DATA_DIR OUTPUT_DIR      (also reads $DATA_DIR / $OUTPUT_DIR)
Writes OUTPUT_DIR/user_daily/part-0.parquet and OUTPUT_DIR/campaign_daily/part-0.parquet.
"""
import calendar
import json
import os
import re
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

DAY = 86_400_000
GAP = 1_800_000
WEEK = 7 * DAY

D = "[0-9]"
F1 = re.compile(rf"({D}{{4}})-({D}{{2}})-({D}{{2}})T({D}{{2}}):({D}{{2}}):({D}{{2}})Z")
F2 = re.compile(rf"({D}{{4}})-({D}{{2}})-({D}{{2}})T({D}{{2}}):({D}{{2}}):({D}{{2}})([+-])({D}{{2}}):({D}{{2}})")
F3 = re.compile(rf"{D}+")
I64_MAX = 2**63 - 1


def civil_ms(y, mo, d, h, mi, s):
    if y < 1:  # year 0000: Python's date can't hold it, and it is outside any window
        return None
    if not (1 <= mo <= 12) or h > 23 or mi > 59 or s > 59:
        return None
    if not (1 <= d <= calendar.monthrange(y, mo)[1]):
        return None
    days = (date(y, mo, d) - date(1970, 1, 1)).days
    return ((days * 24 + h) * 60 + mi) * 60_000 + s * 1000


def parse_ts(s):
    """Epoch ms or None. str.fullmatch with [0-9] keeps digits ASCII-only."""
    if s is None:
        return None
    m = F1.fullmatch(s)
    if m:
        return civil_ms(*map(int, m.groups()))
    m = F2.fullmatch(s)
    if m:
        y, mo, d, h, mi, se, sign, oh, om = m.groups()
        base = civil_ms(int(y), int(mo), int(d), int(h), int(mi), int(se))
        oh, om = int(oh), int(om)
        if base is None or oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60_000
        return base - off if sign == "+" else base + off
    if F3.fullmatch(s):
        v = int(s)
        return v if v <= I64_MAX else None
    return None


def parse_line(line, n):
    """Fields (None for empty) or None if malformed. `line` has no '\\n'."""
    if line == "":
        return None
    parts = line.split(",")
    if len(parts) != n:
        return None
    out = []
    for p in parts:
        if '"' in p:
            if len(p) >= 2 and p[0] == '"' and p[-1] == '"' and '"' not in p[1:-1]:
                p = p[1:-1]
            else:
                return None
        out.append(p if p != "" else None)
    return out


def read_rows(paths, n):
    for path in paths:
        text = Path(path).read_bytes().decode("utf-8", errors="surrogateescape")
        lines = text.split("\n")
        if text.endswith("\n"):
            lines.pop()  # the "line" after the final newline does not exist
        for line in lines[1:]:  # skip header
            row = parse_line(line, n)
            if row is not None:
                yield row


def csvs(folder):
    return sorted(p for p in Path(folder).glob("*.csv") if p.is_file())


def main():
    data = Path(sys.argv[-2] if len(sys.argv) >= 3 else os.environ.get("DATA_DIR", "/data"))
    out = Path(sys.argv[-1] if len(sys.argv) >= 3 else os.environ.get("OUTPUT_DIR", "/output"))
    man = json.loads((data / "manifest.json").read_text())
    ws, we = parse_ts(man["window_start"]), parse_ts(man["window_end"])

    products = {r[0]: r[1] for r in read_rows([data / "products.csv"], 2)}
    campaigns = {r[0]: r[1] for r in read_rows([data / "campaigns.csv"], 2)}

    # Links: union-find, name = smallest ID (Python str order on surrogateescaped
    # UTF-8 equals byte order for valid UTF-8, which the generator writes).
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in read_rows(csvs(data / "links"), 2):
        if a is None or b is None:
            continue
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb
    name_of_root = {}
    for x in list(parent):
        r = find(x)
        if r not in name_of_root or x.encode("utf-8", "surrogateescape") < name_of_root[r].encode("utf-8", "surrogateescape"):
            name_of_root[r] = x

    def person(uid):
        return name_of_root[find(uid)] if uid in parent else uid

    # Events: parse, dedup (min ingest), validate, late.
    best = {}
    for row in read_rows(csvs(data / "events"), 7):
        eid = row[0]
        if eid is None:
            continue
        ing = parse_ts(row[3])
        ing = I64_MAX if ing is None else ing
        if eid not in best or ing < best[eid][0]:
            best[eid] = (ing, row)
    events = defaultdict(list)  # person -> [(ts, price, category)]
    for ing, row in best.values():
        _, uid, ts, _, et, pid, price = row
        if uid is None or pid is None or et not in ("view", "cart", "purchase"):
            continue
        t = parse_ts(ts)
        if t is None or not (ws <= t < we) or ing - t > DAY:
            continue
        p = 0
        if et == "purchase":
            if price is None or not re.fullmatch("[0-9]+", price):
                continue
            p = int(price)
            if not (1 <= p <= 10_000_000):
                continue
        events[person(uid)].append((t, p, products.get(pid, "unknown") if p else None))

    # Clicks: parse, validate (dedup cannot change anything; done anyway).
    clicks = defaultdict(list)  # person -> [(ts, click_id, campaign)]
    seen = set()
    for cid, uid, camp, ts in read_rows(csvs(data / "clicks"), 4):
        if cid is None or cid in seen:
            continue
        seen.add(cid)
        t = parse_ts(ts)
        if uid is None or t is None or not (ws - WEEK <= t < we) or camp not in campaigns:
            continue
        clicks[person(uid)].append((t, cid, camp))

    user = defaultdict(lambda: [0, 0, 0, defaultdict(int)])
    camp = defaultdict(lambda: [0, 0, 0, 0])
    for who, evs in events.items():
        evs.sort(key=lambda e: e[0])
        sessions, cur, prev = [], None, None
        for e in evs:
            if cur is None or e[0] - prev > GAP:
                cur = []
                sessions.append(cur)
            cur.append(e)
            prev = e[0]
        mine = clicks.get(who, [])
        for s in sessions:
            start = s[0][0]
            day = start // DAY
            rev = sum(e[1] for e in s)
            u = user[(who, day)]
            u[0] += 1
            u[1] += len(s)
            u[2] += rev
            for t, p, c in s:
                if p:
                    u[3][c] += p
            window = [k for k in mine if start - WEEK <= k[0] <= start]
            if window:
                latest = max(k[0] for k in window)
                tied = [k for k in window if k[0] == latest]
                credit = min(tied, key=lambda k: k[1].encode("utf-8", "surrogateescape"))[2]
            else:
                credit = "organic"
            a = camp[(credit, day)]
            a[0] += 1
            a[1] += 1 if rev > 0 else 0
            a[2] += rev
            for c in {k[2] for k in window}:
                camp[(c, day)][3] += 1

    import polars as pl

    def as_date(d):
        return date(1970, 1, 1) + timedelta(days=d)

    def top(cs):
        if not cs:
            return None
        return min(cs.items(), key=lambda kv: (-kv[1], kv[0].encode("utf-8", "surrogateescape")))[0]

    ud = pl.DataFrame(
        [(w, as_date(d), v[0], v[1], v[2], top(v[3])) for (w, d), v in user.items()],
        schema={"user_id": pl.String, "day": pl.Date, "sessions": pl.Int64, "events": pl.Int64,
                "revenue_cents": pl.Int64, "top_category": pl.String}, orient="row")
    cd = pl.DataFrame(
        [(c, "organic" if c == "organic" else campaigns[c], as_date(d), *v) for (c, d), v in camp.items()],
        schema={"campaign_id": pl.String, "channel": pl.String, "day": pl.Date, "sessions": pl.Int64,
                "purchasing_sessions": pl.Int64, "revenue_cents": pl.Int64, "assisted_sessions": pl.Int64},
        orient="row")
    for name, df in (("user_daily", ud), ("campaign_daily", cd)):
        (out / name).mkdir(parents=True, exist_ok=True)
        df.write_parquet(out / name / "part-0.parquet")


if __name__ == "__main__":
    main()
