#!/usr/bin/env python3
"""
Synthetic dirty dataset in the /data layout, for differential testing against
scripts/reference.py and for rough timing. It imitates every dirt type listed in
rules.md, more aggressively than the real generator, but it is NOT the official
generator: use generator.zip for `make dev`, and this for extra edge coverage.

Usage: synth.py --seed 7 --events 200000 --out synth/
"""
import argparse
import json
import random
import string
from datetime import datetime, timedelta, timezone
from pathlib import Path

DAY = 86_400_000
B36 = string.digits + string.ascii_lowercase


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--events", type=int, default=200_000)
    ap.add_argument("--out", required=True)
    ap.add_argument("--cr", action="store_true", help="also sprinkle \\r into lines (the real generator never does)")
    a = ap.parse_args()
    R = random.Random(a.seed)
    out = Path(a.out)
    for sub in ("events", "clicks", "links"):
        (out / sub).mkdir(parents=True, exist_ok=True)

    # Window: a random midnight; seed-dependent, sometimes over Feb 29 2028.
    if a.seed % 3 == 0:
        ws_dt = datetime(2028, 2, 20, tzinfo=timezone.utc)
    else:
        ws_dt = datetime(2025, 1, 1, tzinfo=timezone.utc) + timedelta(days=R.randrange(0, 1400))
    we_dt = ws_dt + timedelta(days=14)
    ws, we = int(ws_dt.timestamp() * 1000), int(we_dt.timestamp() * 1000)
    (out / "manifest.json").write_text(json.dumps({
        "window_start": ws_dt.strftime("%Y-%m-%dT%H:%M:%SZ"), "window_end": we_dt.strftime("%Y-%m-%dT%H:%M:%SZ")}))

    used = set()

    def new_id(prefix=""):
        while True:
            n = "".join(R.choice(B36) for _ in range(R.randint(3, 9)))
            if R.random() < 0.125:
                n += "%032x" % R.getrandbits(128)
            s = prefix + n
            if s not in used:
                used.add(s)
                return s

    cats = ["cat_%02d" % i for i in range(20)] + ["Zeta", "unknowx"]
    products = [new_id("p") for _ in range(3000)]
    pcat = {p: R.choice(cats) for p in products}
    with open(out / "products.csv", "w") as f:
        f.write("product_id,category\n")
        for p in products:
            f.write(f"{p},{pcat[p]}\n")
    camps = [f"c_{i}" for i in range(1, 301)]
    with open(out / "campaigns.csv", "w") as f:
        f.write("campaign_id,channel\n")
        for c in camps:
            f.write(f"{c},{R.choice(['search', 'social', 'email', 'display', 'affiliate'])}\n")
    whale_camps = camps[:4]

    # People and IDs.
    n_people = max(10, a.events // 30)
    people = []
    links = []
    for i in range(n_people):
        k = 1 + min(7, int(R.expovariate(1.1)))
        if i < 3 or R.random() < 0.002:
            k = R.randint(50, 200)  # long chains
        ids = [new_id("u") for _ in range(k)]
        if k >= 50:
            order = ids[:]
            R.shuffle(order)
            for x, y in zip(order, order[1:]):
                links.append((x, y))
        else:
            for j in range(1, k):
                links.append((ids[j], ids[R.randrange(j)]))
        if R.random() < 0.05:  # extra linked ID never used
            x = new_id("u")
            links.append((x, R.choice(ids)))
        if R.random() < 0.01:
            links.append((ids[0], ids[0]))
        people.append(ids)
    devices = [new_id("dev") for _ in range(R.randint(3, 6))]
    for ids in people:
        if R.random() < 0.01:
            d = R.choice(devices)
            links.append((d, R.choice(ids)))
            ids.append(d)
    # Link dirt.
    dirty_links = []
    for x, y in links:
        if R.random() < 0.08:
            dirty_links.append((y, x) if R.random() < 0.5 else (x, y))
        if R.random() < 0.002:
            x = ""
        if R.random() < 0.002:
            y = y.upper()
        dirty_links.append((x, y))

    # Events.
    rows = []  # event lines
    weights = [1.0 / (i + 1) ** 0.7 for i in range(n_people)]
    for w in range(5):
        weights[R.randrange(n_people)] = sum(weights) * 0.015
    person_of = R.choices(range(n_people), weights=weights, k=a.events // 6)
    session_starts = []  # (person, start ms)
    eid_n = 0

    def fmt_ts(ms, allow_ms=True):
        r = R.random()
        if ms % 1000 != 0 or r < 0.32:
            return str(ms) if R.random() < 0.9 else "000" + str(ms)
        dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        if r < 0.9:
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        off = R.randint(-10 * 4, 12 * 4) * 15
        local = dt + timedelta(minutes=off)
        sign = "+" if off >= 0 or (off == 0 and R.random() < 0.5) else "-"
        if off == 0 and R.random() < 0.5:
            sign = "-"
        return local.strftime("%Y-%m-%dT%H:%M:%S") + f"{sign}{abs(off) // 60:02d}:{abs(off) % 60:02d}"

    def bad_ts(ms):
        dt = datetime.fromtimestamp(ms // 1000, tz=timezone.utc)
        return R.choice([
            "", dt.strftime("%Y-%m-%dT%H:%M:%S.123Z"), dt.strftime("%Y-%m-%dT%H:%M:%Sz"),
            dt.strftime("%Y-%m-%d %H:%M:%SZ"), dt.strftime("%Y-%m-%dT%H:%M:%S"), f"{ms}.0", "1.79e12", "-5",
            dt.strftime("%Y-13-%dT%H:%M:%SZ"), dt.strftime("%Y-02-30T%H:%M:%SZ"), "2027-02-29T10:00:00Z",
            dt.strftime("%Y-%m-%dT24:%M:%SZ"), dt.strftime("%Y-%m-%dT%H:%M:60Z"), dt.strftime("%Y-%m-%dT%H:%M:%S+24:00"),
            dt.strftime("%Y-%m-%dT%H:%M:%S+05:60"), dt.strftime("%Y-%m-%dT%H:%M:%S+0200"), str(ws - 1), str(we),
            "86400000", "9" * 25, "١٧٩١٠٧١٨٩٠٠٠٠", dt.strftime("%Y-%m-%dT%H:%M:%SZ").replace("2", "２", 1),
        ])

    def iso(ms):
        return datetime.fromtimestamp(ms // 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for pi in person_of:
        ids = people[pi]
        t = R.randrange(ws - 3600_000, we)
        n = R.randint(1, 12)
        start = t
        for j in range(n):
            if j:
                r = R.random()
                if r < 0.01:
                    t += 1_800_000
                elif r < 0.02:
                    t += 1_800_001
                elif r < 0.06:
                    t += 0  # equal timestamps
                else:
                    t += R.randint(1, 600) * (1000 if R.random() < 0.7 else 1) + (R.randrange(1000) if R.random() < 0.2 else 0)
            if j == 0:
                session_starts.append((pi, t))
            uid = ids[0] if R.random() < 0.5 else R.choice(ids)
            et = "purchase" if R.random() < 0.06 else R.choice(["view", "view", "cart"])
            pid = R.choice(products)
            price = str(R.randint(100, 50000)) if et == "purchase" else ""
            ingest = (t // 1000 + R.randint(0, 3600)) * 1000
            eid_n += 1
            eid = new_id("e")
            ts_s = fmt_ts(t)
            # Dirt.
            r = R.random()
            if r < 0.006:
                ts_s = bad_ts(t)
            elif r < 0.016:
                ingest = t // 1000 * 1000 + R.choice([DAY, DAY + 1000, DAY + 5000, 3 * DAY])
            elif r < 0.018:
                uid = ""
            elif r < 0.0185:
                uid = uid.upper()
            elif r < 0.0195:
                et = R.choice(["Purchase", "cart ", "VIEW", "click", ""])
            elif r < 0.0245:
                pid = R.choice([pid.upper(), "", "p_missing", " " + pid])
            if et == "purchase" and R.random() < 0.05:
                price = R.choice(["19.99", "-5", "0", "", "10000001", "+100", "0250", "10000000", "２５０"])
            line = [eid, uid, ts_s, iso(ingest), et, pid, price]
            rows.append(line)
            if R.random() < 0.03:  # duplicates
                for c in range(R.randint(1, 3)):
                    dup = list(line)
                    dup[3] = iso(ingest + R.randint(1, 600) * 1000)
                    if R.random() < 0.02:
                        dup[2] = fmt_ts(t)
                        line[2] = bad_ts(t)
                    if R.random() < 1 / 64:
                        dup[1] = R.choice(people[R.randrange(n_people)])
                    rows.append(dup)

    # Clicks.
    clicks = []
    n_cid = 0
    for pi, s in session_starts:
        ids = people[pi]
        if R.random() < 0.6:
            for _ in range(R.randint(1, 4)):
                r = R.random()
                if r < 0.05:
                    ct = s - 7 * DAY
                elif r < 0.10:
                    ct = s - 7 * DAY - 1
                elif r < 0.15:
                    ct = s
                elif r < 0.20:
                    ct = s + 1
                else:
                    ct = s - R.randrange(7 * DAY)
                    if R.random() < 0.5:
                        ct = ct // 1000 * 1000
                camp = R.choice(whale_camps) if R.random() < 0.3 else R.choice(camps)
                uid = R.choice(ids)
                n_cid += 1
                cid = new_id("k")
                cl = [cid, uid, camp, fmt_ts(ct)]
                rr = R.random()
                if rr < 0.005:
                    cl[3] = bad_ts(ct)
                elif rr < 0.01:
                    cl[2] = R.choice([camp.upper(), "c_99999", " " + camp])
                elif rr < 0.012:
                    cl[1] = ""
                if a.cr and R.random() < 0.003:
                    cl[3] += "\r"  # before duplication: copies of a click stay identical
                clicks.append(cl)
                if R.random() < 0.05:  # same-ms pair, another campaign
                    clicks.append([new_id("k"), uid, R.choice(camps), str(ct)])
                if R.random() < 0.02:
                    clicks.append(list(cl))

    def emit(rows, folder, header, nfiles, cr=True):
        def q(v):
            return f'"{v}"' if R.random() < 0.2 else v
        lines = [",".join(q(v) for v in r) for r in rows]
        # Malformed lines.
        extra = []
        for _ in range(max(3, len(lines) // 2000)):
            base = R.choice(lines) if lines else "x"
            extra.append(R.choice([base + ",x", base.rsplit(",", 1)[0], base.replace(",", ',"', 1), "", '"' + base, base + '"']))
        lines += extra
        if a.cr and cr:
            lines = [l + "\r" if R.random() < 0.002 else l for l in lines]
        R.shuffle(lines)
        sizes = [1.0 / (i + 1) for i in range(nfiles)]
        R.shuffle(sizes)
        sizes[R.randrange(nfiles)] = 0  # header-only file
        tot = sum(sizes)
        pos = 0
        for i, sz in enumerate(sizes):
            k = len(lines) - pos if i == nfiles - 1 else int(len(lines) * sz / tot)
            body = lines[pos:pos + k]
            pos += k
            text = header + "\n" + "".join(l + "\n" for l in body)
            if body and R.random() < 0.3:
                text = text[:-1]  # last line without newline
            (out / folder / f"part-{i:05d}.csv").write_text(text, encoding="utf-8")

    emit(rows, "events", "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents", R.randint(5, 20))
    emit(clicks, "clicks", "click_id,user_id,campaign_id,ts", R.randint(3, 8), cr=False)
    emit(dirty_links, "links", "user_id_a,user_id_b", R.randint(2, 6))
    print(f"{len(rows)} event lines, {len(clicks)} click lines, {len(dirty_links)} link lines -> {out}")


if __name__ == "__main__":
    main()
