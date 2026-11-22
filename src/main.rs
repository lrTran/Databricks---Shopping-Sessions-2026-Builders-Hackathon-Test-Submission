//! Shopping Sessions pipeline (see rules.md).
//!
//! Stages (all in memory, hash-partitioned, one pool of NCPU threads):
//!   0. small inputs: manifest, products, campaigns
//!   1. links  -> union-find -> uid_hash => person_key, person_key => name
//!   2. parse events (into event_id buckets) and clicks (into person buckets)
//!   3. per event_id bucket: dedup (min ingest_ts), keep valid survivors,
//!      scatter them into person buckets
//!   4. per person bucket: sort, sessionize, attribute, aggregate
//!   5. write Parquet
//!
//! A person's key is the 64-bit xxh3 of their name (the byte-wise smallest ID),
//! so an unlinked ID's key is simply the hash of the ID itself and needs no
//! lookup table. Event IDs are deduplicated by their 128-bit xxh3.

mod csv;
mod out;
mod ts;

use std::collections::HashMap;
use std::fs::{self, File};
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;
use xxhash_rust::xxh3::{xxh3_128, xxh3_64};

const DAY_MS: i64 = 86_400_000;
const GAP_MS: i64 = 1_800_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const LATE_MS: i64 = DAY_MS;
const MAX_PRICE: u32 = 10_000_000;

const EB_BITS: u32 = 10; // event_id buckets
const PB_BITS: u32 = 10; // person buckets
const CHUNK: usize = 8 << 20;
const REF_BITS: u32 = 40; // dedup: (worker << 40) | index

// ---------------------------------------------------------------------------
// Hash maps keyed by values that are already good hashes.

#[derive(Default, Clone, Copy)]
struct IdHasher(u64);
impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!("IdHasher only hashes integers")
    }
    fn write_u64(&mut self, x: u64) {
        self.0 = x;
    }
    fn write_u128(&mut self, x: u128) {
        self.0 = x as u64;
    }
}
type IdMap<K, V> = HashMap<K, V, BuildHasherDefault<IdHasher>>;

// ---------------------------------------------------------------------------
// Records

/// One parsed copy of an event, before dedup. Invalid copies carry only eid + ingest.
#[derive(Clone, Copy)]
#[repr(C)]
struct Ev {
    eid: u128,
    ingest: i64,
    ts: i64,
    person: u64,
    uid_ptr: usize,
    uid_len: u32,
    price: u32, // 0 for view/cart, the price (>= 1) for a purchase
    cat: u16,
    valid: bool,
}

/// A surviving event, ready for sessionization.
#[derive(Clone, Copy)]
struct Sv {
    person: u64,
    ts: i64,
    uid_ptr: usize,
    uid_len: u32,
    price: u32,
    cat: u16,
}

/// A valid click.
#[derive(Clone, Copy)]
struct Ck {
    person: u64,
    ts: i64,
    cid_ptr: usize,
    cid_len: u32,
    camp: u32,
}

#[inline(always)]
fn sl(ptr: usize, len: u32) -> &'static [u8] {
    // SAFETY: every pointer refers to a leaked (never freed) mmap or buffer.
    unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) }
}

#[inline(always)]
fn pbucket(person: u64) -> usize {
    (person >> (64 - PB_BITS)) as usize
}

// ---------------------------------------------------------------------------
// Parallel helper: run tasks (in the given order) on `nthreads` threads,
// each thread with its own state, and return the states.

fn run_pool<S, I, F>(nthreads: usize, order: &[usize], init: I, f: F) -> Vec<S>
where
    S: Send,
    I: Fn(usize) -> S + Sync,
    F: Fn(&mut S, usize) + Sync,
{
    let next = AtomicUsize::new(0);
    std::thread::scope(|sc| {
        let handles: Vec<_> = (0..nthreads)
            .map(|w| {
                let (next, init, f) = (&next, &init, &f);
                sc.spawn(move || {
                    let mut st = init(w);
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= order.len() {
                            break;
                        }
                        f(&mut st, order[i]);
                    }
                    st
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().expect("worker panicked")).collect()
    })
}

// ---------------------------------------------------------------------------
// Input files

fn leak_bytes(v: Vec<u8>) -> &'static [u8] {
    Box::leak(v.into_boxed_slice())
}

fn map_file(p: &Path) -> &'static [u8] {
    let f = File::open(p).unwrap_or_else(|e| panic!("open {}: {e}", p.display()));
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    if len == 0 {
        return &[];
    }
    // SAFETY: /data is read-only and nothing modifies the files while we run.
    let m = unsafe { memmap2::Mmap::map(&f) }.unwrap_or_else(|e| panic!("mmap {}: {e}", p.display()));
    let _ = m.advise(memmap2::Advice::Sequential);
    let m: &'static memmap2::Mmap = Box::leak(Box::new(m));
    &m[..]
}

/// Every `*.csv` directly in `dir` (dotfiles excluded, like a shell glob).
fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                !name.starts_with('.') && name.ends_with(".csv") && p.is_file()
            })
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

fn file_chunks(dir: &Path) -> (Vec<&'static [u8]>, usize) {
    let mut chunks = Vec::new();
    let mut bytes = 0;
    for p in csv_files(dir) {
        let data = map_file(&p);
        bytes += data.len();
        csv::chunks(data, CHUNK, &mut chunks);
    }
    (chunks, bytes)
}

fn json_string(text: &str, key: &str) -> String {
    let k = format!("\"{key}\"");
    let at = text.find(&k).unwrap_or_else(|| panic!("manifest has no {key}"));
    let rest = &text[at + k.len()..];
    let rest = &rest[rest.find(':').expect("manifest colon") + 1..];
    let open = rest.find('"').expect("manifest value") + 1;
    let close = rest[open..].find('"').expect("manifest value") + open;
    rest[open..close].to_string()
}

/// Two-column clean file (products, campaigns), header skipped.
fn read_pairs(p: &Path) -> Vec<(&'static [u8], &'static [u8])> {
    let bytes = leak_bytes(fs::read(p).unwrap_or_else(|e| panic!("{}: {e}", p.display())));
    let mut rows = Vec::new();
    let mut f = [&b""[..]; 2];
    for line in csv::body_lines(bytes) {
        if csv::split(line, &mut f) {
            rows.push((f[0], f[1]));
        }
    }
    rows
}

/// Exact-bytes lookup for small tables (products, campaigns).
struct SmallMap {
    map: IdMap<u64, (u32, &'static [u8])>,
}
impl SmallMap {
    fn new<'a>(keys: impl Iterator<Item = (&'static [u8], u32)>) -> Self {
        let mut map = IdMap::default();
        for (k, v) in keys {
            map.insert(xxh3_64(k), (v, k));
        }
        SmallMap { map }
    }
    #[inline(always)]
    fn get(&self, key: &[u8]) -> Option<u32> {
        match self.map.get(&xxh3_64(key)) {
            Some(&(v, k)) if k == key => Some(v),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Links: union-find over IDs.

struct People {
    /// uid hash -> person key, only for linked IDs that are not their person's name.
    link: IdMap<u64, u64>,
    /// person key -> name, for every person that appears in links.
    names: IdMap<u64, &'static [u8]>,
}

impl People {
    #[inline(always)]
    fn person(&self, uid: &[u8]) -> u64 {
        let h = xxh3_64(uid);
        match self.link.get(&h) {
            Some(&p) => p,
            None => h,
        }
    }
}

fn find(parent: &mut [u32], mut x: u32) -> u32 {
    while parent[x as usize] != x {
        let p = parent[x as usize];
        parent[x as usize] = parent[p as usize];
        x = p;
    }
    x
}

fn build_people(nthreads: usize, dir: &Path) -> People {
    let (chunks, _) = file_chunks(dir);
    let order: Vec<usize> = (0..chunks.len()).collect();
    let parts = run_pool(
        nthreads,
        &order,
        |_| Vec::<(u64, &'static [u8], u64, &'static [u8])>::new(),
        |st, i| {
            let mut f = [&b""[..]; 2];
            csv::for_each_line(chunks[i], |line| {
                if csv::split(line, &mut f) && !f[0].is_empty() && !f[1].is_empty() {
                    st.push((xxh3_64(f[0]), f[0], xxh3_64(f[1]), f[1]));
                }
            });
        },
    );
    let n: usize = parts.iter().map(|p| p.len()).sum();
    let mut index: IdMap<u64, u32> = IdMap::with_capacity_and_hasher(n, Default::default());
    let mut ids: Vec<(u64, &'static [u8])> = Vec::with_capacity(n);
    let mut parent: Vec<u32> = Vec::with_capacity(n);
    let mut intern = |h: u64, s: &'static [u8], ids: &mut Vec<(u64, &'static [u8])>, parent: &mut Vec<u32>| -> u32 {
        *index.entry(h).or_insert_with(|| {
            let i = ids.len() as u32;
            ids.push((h, s));
            parent.push(i);
            i
        })
    };
    for part in &parts {
        for &(ha, a, hb, b) in part {
            let ia = intern(ha, a, &mut ids, &mut parent);
            let ib = intern(hb, b, &mut ids, &mut parent);
            let (ra, rb) = (find(&mut parent, ia), find(&mut parent, ib));
            if ra != rb {
                let (lo, hi) = if ra < rb { (ra, rb) } else { (rb, ra) };
                parent[hi as usize] = lo;
            }
        }
    }
    drop(parts);
    // Name each component by its byte-wise smallest ID.
    let m = ids.len();
    let mut root = vec![0u32; m];
    let mut best: Vec<Option<&'static [u8]>> = vec![None; m];
    for i in 0..m {
        let r = find(&mut parent, i as u32);
        root[i] = r;
        let s = ids[i].1;
        let b = &mut best[r as usize];
        if b.map_or(true, |cur| s < cur) {
            *b = Some(s);
        }
    }
    let mut link = IdMap::with_capacity_and_hasher(m, Default::default());
    let mut names = IdMap::with_capacity_and_hasher(m / 2 + 1, Default::default());
    for i in 0..m {
        let name = best[root[i] as usize].unwrap();
        let key = xxh3_64(name);
        if ids[i].1 != name {
            link.insert(ids[i].0, key);
        } else {
            names.insert(key, name);
        }
    }
    People { link, names }
}

// ---------------------------------------------------------------------------

/// ASCII digits only, value 1..=10,000,000 (leading zeros allowed).
#[inline(always)]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u32 = 0;
    for &b in s {
        if !b.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (b - b'0') as u32;
        if v > MAX_PRICE {
            return None; // invalid whether or not a non-digit follows
        }
    }
    if v >= 1 { Some(v) } else { None }
}

struct ParseState {
    ev: Vec<Vec<Ev>>,
    ck: Vec<Vec<Ck>>,
}

#[derive(Clone, Copy, Default)]
struct CampAcc {
    sessions: i64,
    purchasing: i64,
    revenue: i64,
    assisted: i64,
}

struct PersonState {
    camp: Vec<CampAcc>, // (ncamp + 1) * ndays, campaign-major
    users: Vec<out::UserRow>,
    cnt: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u32>,
    catsum: Vec<i64>,
    touched: Vec<u16>,
}

fn env_or(k: &str, d: &str) -> String {
    std::env::var(k).ok().filter(|s| !s.is_empty()).unwrap_or_else(|| d.to_string())
}

fn main() {
    let t0 = Instant::now();
    let lap = |what: &str| eprintln!("[{:>7.3}s] {what}", t0.elapsed().as_secs_f64());

    // run_fixtures.py passes DATA_DIR and OUTPUT_DIR as env vars *and* as the last two args.
    let args: Vec<String> = std::env::args().collect();
    let (data, outdir) = if args.len() >= 3 {
        (PathBuf::from(&args[args.len() - 2]), PathBuf::from(&args[args.len() - 1]))
    } else {
        (PathBuf::from(env_or("DATA_DIR", "/data")), PathBuf::from(env_or("OUTPUT_DIR", "/output")))
    };
    let nthreads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8))
        .max(1);

    // ---- 0. small inputs -------------------------------------------------------
    let manifest = fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let ws = ts::parse(json_string(&manifest, "window_start").as_bytes()).expect("window_start");
    let we = ts::parse(json_string(&manifest, "window_end").as_bytes()).expect("window_end");
    let cws = ws - WEEK_MS;
    let day0 = ws.div_euclid(DAY_MS);
    let ndays = if we > ws { ((we - 1).div_euclid(DAY_MS) - day0 + 1) as usize } else { 1 };

    // Categories sorted byte-wise, so "smallest id" == "byte-wise smallest name".
    let prod_rows = read_pairs(&data.join("products.csv"));
    let mut cats: Vec<&'static [u8]> = prod_rows.iter().map(|r| r.1).collect();
    cats.push(b"unknown");
    cats.sort_unstable();
    cats.dedup();
    let cat_id = |c: &[u8]| cats.binary_search_by(|x| (*x).cmp(c)).unwrap() as u32;
    let unknown_cat = cat_id(b"unknown") as u16;
    let pmap = SmallMap::new(prod_rows.iter().map(|&(p, c)| (p, cat_id(c))));

    let camp_rows = read_pairs(&data.join("campaigns.csv"));
    let ncamp = camp_rows.len();
    let organic = ncamp as u32;
    let cmap = SmallMap::new(camp_rows.iter().enumerate().map(|(i, &(c, _))| (c, i as u32)));
    lap(&format!(
        "small inputs: {} products, {} categories, {ncamp} campaigns, {ndays} days, {nthreads} threads",
        prod_rows.len(),
        cats.len()
    ));

    // ---- 1. links ----------------------------------------------------------------
    let people = build_people(nthreads, &data.join("links"));
    lap(&format!("links: {} linked ids map to another name, {} linked people", people.link.len(), people.names.len()));

    // ---- 2. parse events and clicks ------------------------------------------
    let (ev_chunks, ev_bytes) = file_chunks(&data.join("events"));
    let (ck_chunks, ck_bytes) = file_chunks(&data.join("clicks"));
    let n_ev_chunks = ev_chunks.len();
    let all_chunks: Vec<&'static [u8]> = ev_chunks.into_iter().chain(ck_chunks).collect();
    let mut order: Vec<usize> = (0..all_chunks.len()).collect();
    order.sort_by_key(|&i| std::cmp::Reverse(all_chunks[i].len())); // largest first
    let neb = 1usize << EB_BITS;
    let npb = 1usize << PB_BITS;
    // Over-reserving is cheap: untouched pages are never committed.
    let ev_cap = ev_bytes / 60 / nthreads / neb + 16;
    let ck_cap = ck_bytes / 40 / nthreads / npb + 16;
    let parsed = run_pool(
        nthreads,
        &order,
        |_| ParseState {
            ev: (0..neb).map(|_| Vec::with_capacity(ev_cap)).collect(),
            ck: (0..npb).map(|_| Vec::with_capacity(ck_cap)).collect(),
        },
        |st, i| {
            let chunk = all_chunks[i];
            if i < n_ev_chunks {
                let mut f = [&b""[..]; 7];
                csv::for_each_line(chunk, |line| {
                    if !csv::split(line, &mut f) || f[0].is_empty() {
                        return; // malformed, or no event_id: never a dedup candidate
                    }
                    let eid = xxh3_128(f[0]);
                    let ingest = ts::parse(f[3]).unwrap_or(i64::MAX);
                    let mut e = Ev { eid, ingest, ts: 0, person: 0, uid_ptr: 0, uid_len: 0, price: 0, cat: 0, valid: false };
                    // Validation and the late check depend only on this copy, so they
                    // are decided here; dedup later picks the copy, valid or not.
                    'v: {
                        let uid = f[1];
                        if uid.is_empty() || f[5].is_empty() {
                            break 'v;
                        }
                        let Some(t) = ts::parse(f[2]) else { break 'v };
                        if t < ws || t >= we || ingest - t > LATE_MS {
                            break 'v;
                        }
                        let price = match f[4] {
                            b"view" | b"cart" => 0,
                            b"purchase" => match parse_price(f[6]) {
                                Some(p) => p,
                                None => break 'v,
                            },
                            _ => break 'v,
                        };
                        e.ts = t;
                        e.person = people.person(uid);
                        e.uid_ptr = uid.as_ptr() as usize;
                        e.uid_len = uid.len() as u32;
                        e.price = price;
                        e.cat = if price > 0 { pmap.get(f[5]).map_or(unknown_cat, |c| c as u16) } else { 0 };
                        e.valid = true;
                    }
                    st.ev[(eid >> (128 - EB_BITS)) as usize].push(e);
                });
            } else {
                let mut f = [&b""[..]; 4];
                csv::for_each_line(chunk, |line| {
                    if !csv::split(line, &mut f) || f[0].is_empty() || f[1].is_empty() {
                        return;
                    }
                    let Some(t) = ts::parse(f[3]) else { return };
                    if t < cws || t >= we {
                        return;
                    }
                    let Some(camp) = cmap.get(f[2]) else { return };
                    // Copies of a click have identical values, so deduplicating on
                    // click_id cannot change any output: credit picks one click and
                    // assists count each campaign once. We skip it.
                    let person = people.person(f[1]);
                    st.ck[pbucket(person)].push(Ck { person, ts: t, cid_ptr: f[0].as_ptr() as usize, cid_len: f[0].len() as u32, camp });
                });
            }
        },
    );
    lap(&format!("parsed {} MB of events, {} MB of clicks", ev_bytes >> 20, ck_bytes >> 20));

    // Transpose ownership: bucket -> [one part per worker].
    let mut ev_b: Vec<Mutex<Vec<Vec<Ev>>>> = (0..neb).map(|_| Mutex::new(Vec::with_capacity(nthreads))).collect();
    let mut ck_b: Vec<Vec<Vec<Ck>>> = (0..npb).map(|_| Vec::with_capacity(nthreads)).collect();
    let mut n_clicks = 0usize;
    for st in parsed {
        for (b, v) in st.ev.into_iter().enumerate() {
            ev_b[b].get_mut().unwrap().push(v);
        }
        for (b, v) in st.ck.into_iter().enumerate() {
            n_clicks += v.len();
            ck_b[b].push(v);
        }
    }

    // ---- 3. dedup events -----------------------------------------------------------
    let sizes: Vec<usize> = ev_b.iter_mut().map(|m| m.get_mut().unwrap().iter().map(|v| v.len()).sum()).collect();
    let total_copies: usize = sizes.iter().sum();
    let mut order: Vec<usize> = (0..neb).collect();
    order.sort_by_key(|&b| std::cmp::Reverse(sizes[b]));
    let sv_cap = total_copies / nthreads / npb + 16;
    let deduped = run_pool(
        nthreads,
        &order,
        |_| (IdMap::<u128, u64>::default(), (0..npb).map(|_| Vec::<Sv>::with_capacity(sv_cap)).collect::<Vec<_>>()),
        |(map, outb), b| {
            let parts = std::mem::take(&mut *ev_b[b].lock().unwrap());
            let get = |r: u64| &parts[(r >> REF_BITS) as usize][(r & ((1 << REF_BITS) - 1)) as usize];
            map.clear();
            map.reserve(sizes[b]);
            for (w, part) in parts.iter().enumerate() {
                for (i, e) in part.iter().enumerate() {
                    let r = ((w as u64) << REF_BITS) | i as u64;
                    map.entry(e.eid)
                        .and_modify(|cur| {
                            if e.ingest < get(*cur).ingest {
                                *cur = r;
                            }
                        })
                        .or_insert(r);
                }
            }
            for &r in map.values() {
                let e = get(r);
                if e.valid {
                    outb[pbucket(e.person)].push(Sv { person: e.person, ts: e.ts, uid_ptr: e.uid_ptr, uid_len: e.uid_len, price: e.price, cat: e.cat });
                }
            }
        },
    );
    drop(ev_b);
    let mut sv_b: Vec<Mutex<Vec<Vec<Sv>>>> = (0..npb).map(|_| Mutex::new(Vec::with_capacity(nthreads))).collect();
    for (_, outb) in deduped {
        for (b, v) in outb.into_iter().enumerate() {
            sv_b[b].get_mut().unwrap().push(v);
        }
    }
    let ck_b: Vec<Mutex<Vec<Vec<Ck>>>> = ck_b.into_iter().map(Mutex::new).collect();
    let sizes: Vec<usize> = sv_b.iter_mut().map(|m| m.get_mut().unwrap().iter().map(|v| v.len()).sum()).collect();
    lap(&format!("dedup: {total_copies} event copies -> {} surviving events; {n_clicks} valid clicks", sizes.iter().sum::<usize>()));

    // ---- 4. sessionize + attribute per person bucket ---------------------------------
    let mut order: Vec<usize> = (0..npb).collect();
    order.sort_by_key(|&b| std::cmp::Reverse(sizes[b]));
    let ncol = ncamp + 1;
    let states = run_pool(
        nthreads,
        &order,
        |_| PersonState {
            camp: vec![CampAcc::default(); ncol * ndays],
            users: Vec::new(),
            cnt: vec![0; ncol],
            pos: vec![0; ncol],
            active: Vec::new(),
            catsum: vec![0; cats.len()],
            touched: Vec::new(),
        },
        |st, b| {
            let parts = std::mem::take(&mut *sv_b[b].lock().unwrap());
            let mut evs: Vec<Sv> = Vec::with_capacity(sizes[b]);
            for p in parts {
                evs.extend_from_slice(&p);
            }
            evs.sort_unstable_by_key(|e| ((e.person as u128) << 64) | (e.ts as u64 as u128));
            let parts = std::mem::take(&mut *ck_b[b].lock().unwrap());
            let mut cks: Vec<Ck> = Vec::with_capacity(parts.iter().map(|v| v.len()).sum());
            for p in parts {
                cks.extend_from_slice(&p);
            }
            // (person, ts) ascending, then click_id DEScending: the last click at or
            // before a session start is then the smallest click_id at the latest ts.
            cks.sort_unstable_by(|a, b| {
                (a.person, a.ts).cmp(&(b.person, b.ts)).then_with(|| sl(b.cid_ptr, b.cid_len).cmp(sl(a.cid_ptr, a.cid_len)))
            });
            sessionize(st, &evs, &cks, &people, &cats, day0, ndays, organic);
        },
    );
    drop(sv_b);
    lap("sessionized and attributed");

    // ---- 5. write -------------------------------------------------------------------
    let ud = outdir.join("user_daily");
    let cd = outdir.join("campaign_daily");
    fs::create_dir_all(&ud).expect("mkdir user_daily");
    fs::create_dir_all(&cd).expect("mkdir campaign_daily");

    let mut camp_tot = vec![CampAcc::default(); ncol * ndays];
    let mut user_parts: Vec<Vec<out::UserRow>> = Vec::new();
    for st in states {
        for (t, s) in camp_tot.iter_mut().zip(&st.camp) {
            t.sessions += s.sessions;
            t.purchasing += s.purchasing;
            t.revenue += s.revenue;
            t.assisted += s.assisted;
        }
        user_parts.push(st.users);
    }
    let mut crow = Vec::new();
    for c in 0..ncol {
        let (id, ch): (&'static [u8], &'static [u8]) = if c == ncamp { (b"organic", b"organic") } else { camp_rows[c] };
        for d in 0..ndays {
            let a = camp_tot[c * ndays + d];
            if a.sessions > 0 || a.assisted > 0 {
                crow.push(out::CampaignRow {
                    campaign: id,
                    channel: ch,
                    day: (day0 + d as i64) as i32,
                    sessions: a.sessions,
                    purchasing: a.purchasing,
                    revenue: a.revenue,
                    assisted: a.assisted,
                });
            }
        }
    }
    let nuser: usize = user_parts.iter().map(|v| v.len()).sum();
    std::thread::scope(|sc| {
        sc.spawn(|| out::write_campaign(&cd.join("part-0.parquet"), &crow).expect("write campaign_daily"));
        let last = user_parts.len() - 1;
        let mut wrote = false;
        for (i, rows) in user_parts.iter().enumerate() {
            // Skip empty parts, but always write at least one file.
            if rows.is_empty() && (wrote || i < last) {
                continue;
            }
            wrote = true;
            let path = ud.join(format!("part-{i}.parquet"));
            sc.spawn(move || out::write_user(&path, rows).expect("write user_daily"));
        }
    });
    lap(&format!("wrote {nuser} user_daily rows, {} campaign_daily rows", crow.len()));
}

/// Walk one person bucket: events sorted by (person, ts), clicks by (person, ts, click_id desc).
#[allow(clippy::too_many_arguments)]
fn sessionize(st: &mut PersonState, evs: &[Sv], cks: &[Ck], people: &People, cats: &[&'static [u8]], day0: i64, ndays: usize, organic: u32) {
    let PersonState { camp, users, cnt, pos, active, catsum, touched } = st;
    let mut ci = 0usize;
    let mut i = 0usize;
    while i < evs.len() {
        let p = evs[i].person;
        let mut j = i + 1;
        while j < evs.len() && evs[j].person == p {
            j += 1;
        }
        while ci < cks.len() && cks[ci].person < p {
            ci += 1;
        }
        let c0 = ci;
        while ci < cks.len() && cks[ci].person == p {
            ci += 1;
        }
        let pc = &cks[c0..ci];
        let pe = &evs[i..j];
        i = j;

        // A linked person is named in `names`; an unlinked one by its only ID.
        let name: &'static [u8] = match people.names.get(&p) {
            Some(&n) => n,
            None => sl(pe[0].uid_ptr, pe[0].uid_len),
        };
        let flush = |day: i64, s: i64, e: i64, r: i64, users: &mut Vec<out::UserRow>, catsum: &mut [i64], touched: &mut Vec<u16>| {
            // Highest summed price; ties to the smallest id == byte-wise smallest name.
            let mut top: Option<(u16, i64)> = None;
            for &c in touched.iter() {
                let v = catsum[c as usize];
                top = match top {
                    Some((bc, bv)) if bv > v || (bv == v && bc < c) => Some((bc, bv)),
                    _ => Some((c, v)),
                };
                catsum[c as usize] = 0;
            }
            touched.clear();
            users.push(out::UserRow { name, day: day as i32, sessions: s, events: e, revenue: r, top: top.map(|(c, _)| cats[c as usize]) });
        };

        let mut cur_day = i64::MIN;
        let (mut d_sess, mut d_ev, mut d_rev) = (0i64, 0i64, 0i64);
        let (mut lo, mut hi) = (0usize, 0usize);
        let mut k = 0usize;
        while k < pe.len() {
            let start = pe[k].ts;
            let day = start.div_euclid(DAY_MS);
            if day != cur_day {
                if cur_day != i64::MIN {
                    flush(cur_day, d_sess, d_ev, d_rev, users, catsum, touched);
                }
                cur_day = day;
                d_sess = 0;
                d_ev = 0;
                d_rev = 0;
            }
            // One session: equal timestamps have a gap of 0, so they always join.
            let mut last = start;
            let mut n = 0i64;
            let mut rev = 0i64;
            while k < pe.len() && pe[k].ts - last <= GAP_MS {
                let e = &pe[k];
                last = e.ts;
                n += 1;
                if e.price > 0 {
                    rev += e.price as i64;
                    let c = e.cat as usize;
                    if catsum[c] == 0 {
                        touched.push(e.cat);
                    }
                    catsum[c] += e.price as i64;
                }
                k += 1;
            }
            d_sess += 1;
            d_ev += n;
            d_rev += rev;

            // Clicks in [start - 7d, start] form the window; keep a multiset of their campaigns.
            while hi < pc.len() && pc[hi].ts <= start {
                let c = pc[hi].camp as usize;
                if cnt[c] == 0 {
                    pos[c] = active.len() as u32;
                    active.push(c as u32);
                }
                cnt[c] += 1;
                hi += 1;
            }
            let lim = start - WEEK_MS;
            while lo < hi && pc[lo].ts < lim {
                let c = pc[lo].camp as usize;
                cnt[c] -= 1;
                if cnt[c] == 0 {
                    let at = pos[c] as usize;
                    let moved = active.pop().unwrap();
                    if moved as usize != c {
                        active[at] = moved;
                        pos[moved as usize] = at as u32;
                    }
                }
                lo += 1;
            }
            let credit = if lo < hi { pc[hi - 1].camp } else { organic };
            let di = (day - day0) as usize;
            debug_assert!(di < ndays);
            let a = &mut camp[credit as usize * ndays + di];
            a.sessions += 1;
            a.revenue += rev;
            if rev > 0 {
                a.purchasing += 1;
            }
            for &c in active.iter() {
                camp[c as usize * ndays + di].assisted += 1;
            }
        }
        flush(cur_day, d_sess, d_ev, d_rev, users, catsum, touched);
        for &c in active.iter() {
            cnt[c as usize] = 0;
        }
        active.clear();
    }
}
