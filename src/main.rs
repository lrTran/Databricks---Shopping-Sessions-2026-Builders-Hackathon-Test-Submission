//! Shopping Sessions pipeline (see rules.md).
//!
//! Stages, all in memory and hash-partitioned:
//!   A. On a side thread: links -> union-find -> a uid-hash table of (person, name).
//!      Meanwhile, one pool of $NCPU threads parses events (into event_id buckets)
//!      and clicks (flat per thread). Validation and the late check are decided
//!      at parse time; the person is resolved later.
//!   B. Per event_id bucket: dedup (smallest ingest_ts wins, valid or not), then
//!      resolve each survivor's person with prefetched lookups and scatter it into
//!      person buckets. Clicks are resolved and scattered the same way.
//!   C. Per person bucket: sort, sessionize, attribute, aggregate.
//!   D. Write Parquet.
//!
//! A person's key is the 64-bit xxh3 of their name (the byte-wise smallest ID),
//! so an unlinked ID's key is the hash of the ID itself and needs no table entry.
//! Event IDs are deduplicated by their 128-bit xxh3.

mod arena;
mod csv;
mod out;
mod scan;
mod ts;

use std::collections::HashMap;
use std::fs::{self, File};
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use xxhash_rust::xxh3::{xxh3_128, xxh3_64};

use arena::{Arena, SegVec};

const DAY_MS: i64 = 86_400_000;
const GAP_MS: i64 = 1_800_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const LATE_MS: i64 = DAY_MS;
const MAX_PRICE: u32 = 10_000_000;

const EB_BITS: u32 = 10; // event_id buckets
const PB_BITS: u32 = 10; // person buckets
const CHUNK: usize = 8 << 20;
const CLICK_TASK: usize = 1 << 16; // clicks per resolve task
const INVALID: u32 = u32::MAX; // Ev.ts of a copy that fails validation
const PREFETCH: usize = 16; // lookahead for table lookups

// ---------------------------------------------------------------------------
// Small helpers

/// A string slice of leaked input, packed as pointer (48 bits) | length (16 bits).
#[inline(always)]
fn pack(s: &'static [u8]) -> u64 {
    debug_assert!(s.len() < 1 << 16 && (s.as_ptr() as u64) < 1 << 48);
    (s.as_ptr() as u64) | ((s.len() as u64) << 48)
}
#[inline(always)]
fn unpack(v: u64) -> &'static [u8] {
    // SAFETY: every packed slice points into leaked (never freed) input bytes.
    unsafe { std::slice::from_raw_parts((v & ((1 << 48) - 1)) as *const u8, (v >> 48) as usize) }
}

#[inline(always)]
fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(p as *const i8, std::arch::x86_64::_MM_HINT_T0)
    };
}

#[inline(always)]
fn pbucket(person: u64) -> usize {
    (person >> (64 - PB_BITS)) as usize
}

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
}
type IdMap<K, V> = HashMap<K, V, BuildHasherDefault<IdHasher>>;

// ---------------------------------------------------------------------------
// Records

/// One parsed copy of an event, before dedup (48 bytes).
#[derive(Clone, Copy)]
#[repr(C)]
struct Ev {
    eid: u128,
    ingest: i64,
    uid_hash: u64,
    uid: u64, // packed
    ts: u32,  // ms since window_start, or INVALID
    pc: u32,  // price (0 for view/cart) | category << 24
}

/// A valid click before its person is known (24 bytes).
#[derive(Clone, Copy)]
struct CkRaw {
    uid_hash: u64,
    cid: u64, // packed click_id
    ts: u32,  // ms since window_start - 7 days
    camp: u32,
}

/// A surviving event with its person (24 bytes).
#[derive(Clone, Copy)]
struct Sv {
    person: u64,
    name: u64, // packed person name
    ts: u32,
    pc: u32,
}

/// A valid click with its person (24 bytes).
#[derive(Clone, Copy)]
struct Ck {
    person: u64,
    cid: u64,
    ts: u32,
    camp: u32,
}

// ---------------------------------------------------------------------------
// Parallel helper: run tasks (in the given order) on `nthreads` threads,
// each with its own state, and return the states.

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
    fn new(keys: impl Iterator<Item = (&'static [u8], u32)>) -> Self {
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
// Links: union-find over IDs, then an open-addressing table
// uid_hash -> (person key, packed name) for IDs that are not their own name.

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Slot {
    key: u64, // uid hash; 0 = empty
    person: u64,
    name: u64,
}

struct People {
    slots: Vec<Slot>,
    mask: usize,
    zero: Option<(u64, u64)>, // the (astronomically unlikely) uid hash 0
}

impl People {
    fn new(n: usize) -> Self {
        let cap = (2 * n).next_power_of_two().max(16);
        People { slots: vec![Slot::default(); cap], mask: cap - 1, zero: None }
    }
    fn insert(&mut self, key: u64, person: u64, name: u64) {
        if key == 0 {
            self.zero = Some((person, name));
            return;
        }
        let mut i = key as usize & self.mask;
        while self.slots[i].key != 0 && self.slots[i].key != key {
            i = (i + 1) & self.mask;
        }
        self.slots[i] = Slot { key, person, name };
    }
    #[inline(always)]
    fn prefetch(&self, key: u64) {
        prefetch(unsafe { self.slots.as_ptr().add(key as usize & self.mask) });
    }
    /// (person key, packed name); an ID not in the table is its own person.
    #[inline(always)]
    fn resolve(&self, key: u64, uid: u64) -> (u64, u64) {
        if key == 0 {
            return self.zero.unwrap_or((key, uid));
        }
        let mut i = key as usize & self.mask;
        loop {
            // SAFETY: i is masked to the table size.
            let s = unsafe { self.slots.get_unchecked(i) };
            if s.key == key {
                return (s.person, s.name);
            }
            if s.key == 0 {
                return (key, uid);
            }
            i = (i + 1) & self.mask;
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

fn build_people(dir: &Path) -> People {
    let (chunks, _) = file_chunks(dir);
    let mut index: IdMap<u64, u32> = IdMap::default();
    let mut ids: Vec<(u64, &'static [u8])> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
    let mut intern = |s: &'static [u8], ids: &mut Vec<(u64, &'static [u8])>, parent: &mut Vec<u32>| -> u32 {
        let h = xxh3_64(s);
        *index.entry(h).or_insert_with(|| {
            let i = ids.len() as u32;
            ids.push((h, s));
            parent.push(i);
            i
        })
    };
    for c in chunks {
        scan::for_each_record::<2, _>(c, |f| {
            if f[0].is_empty() || f[1].is_empty() {
                return;
            }
            let ia = intern(f[0], &mut ids, &mut parent);
            let ib = intern(f[1], &mut ids, &mut parent);
            let (ra, rb) = (find(&mut parent, ia), find(&mut parent, ib));
            if ra != rb {
                let (lo, hi) = if ra < rb { (ra, rb) } else { (rb, ra) };
                parent[hi as usize] = lo;
            }
        });
    }
    // Name each component by its byte-wise smallest ID.
    let m = ids.len();
    let mut root = vec![0u32; m];
    let mut best: Vec<Option<&'static [u8]>> = vec![None; m];
    for i in 0..m {
        let r = find(&mut parent, i as u32);
        root[i] = r;
        let s = ids[i].1;
        let b = &mut best[r as usize];
        if b.is_none_or(|cur| s < cur) {
            *b = Some(s);
        }
    }
    let mut people = People::new(m);
    for i in 0..m {
        let name = best[root[i] as usize].unwrap();
        if ids[i].1 != name {
            people.insert(ids[i].0, xxh3_64(name), pack(name));
        }
    }
    people
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
    arena: Arena,
    tsp: ts::Parser,
    ev: Vec<SegVec<Ev>>,
    ck: SegVec<CkRaw>,
}

/// Stage B scratch: a reusable open-addressing table for dedup.
struct DedupState {
    arena: Arena,
    slots: Vec<(u64, u64)>, // (eid low bits, pointer to the Ev); EMPTY for an empty slot
    alive: Vec<(u64, u64, u32, u32)>, // uid_hash, uid, ts, pc
    sv: Vec<SegVec<Sv>>,
    ck: Vec<SegVec<Ck>>,
}
const EMPTY: u64 = u64::MAX;

#[derive(Clone, Copy, Default)]
struct CampAcc {
    sessions: i64,
    purchasing: i64,
    revenue: i64,
    assisted: i64,
}

struct PersonState {
    evs: Vec<Sv>,
    cks: Vec<Ck>,
    grouped_evs: Vec<Sv>,
    grouped_cks: Vec<Ck>,
    table: Vec<(u64, u32)>, // person -> dense id + 1 (0 = empty slot)
    ids: Vec<u32>,
    ev_start: Vec<u32>,
    ck_start: Vec<u32>,
    camp: Vec<CampAcc>, // (ncamp + 1) * ndays, campaign-major
    users: Vec<out::UserRow>,
    cnt: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u32>,
    catsum: Vec<i64>,
    touched: Vec<u16>,
}

struct Window {
    ws: i64,
    day0: i64,
    ndays: usize,
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

    // ---- A0. links on a side thread, overlapping everything until stage B ----
    let links_dir = data.join("links");
    let links = std::thread::spawn(move || build_people(&links_dir));

    // ---- small inputs -------------------------------------------------------------
    let manifest = fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let ws = ts::parse(json_string(&manifest, "window_start").as_bytes()).expect("window_start");
    let we = ts::parse(json_string(&manifest, "window_end").as_bytes()).expect("window_end");
    let cws = ws - WEEK_MS;
    // Timestamps are stored as u32 offsets: window_start - 7 days to window_end must fit.
    assert!(we - cws < INVALID as i64, "window longer than u32 milliseconds");
    let day0 = ws.div_euclid(DAY_MS);
    let ndays = if we > ws { ((we - 1).div_euclid(DAY_MS) - day0 + 1) as usize } else { 1 };
    let win = Window { ws, day0, ndays };

    // Categories sorted byte-wise, so "smallest id" == "byte-wise smallest name".
    let prod_rows = read_pairs(&data.join("products.csv"));
    let mut cats: Vec<&'static [u8]> = prod_rows.iter().map(|r| r.1).collect();
    cats.push(b"unknown");
    cats.sort_unstable();
    cats.dedup();
    assert!(cats.len() <= 256, "more than 255 categories");
    let cat_id = |c: &[u8]| cats.binary_search_by(|x| (*x).cmp(c)).unwrap() as u32;
    let unknown_cat = cat_id(b"unknown");
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

    // ---- A. parse events and clicks ------------------------------------------------
    let (ev_chunks, ev_bytes) = file_chunks(&data.join("events"));
    let (ck_chunks, ck_bytes) = file_chunks(&data.join("clicks"));
    let n_ev_chunks = ev_chunks.len();
    let all_chunks: Vec<&'static [u8]> = ev_chunks.into_iter().chain(ck_chunks).collect();
    let mut order: Vec<usize> = (0..all_chunks.len()).collect();
    order.sort_by_key(|&i| std::cmp::Reverse(all_chunks[i].len())); // largest first
    let neb = 1usize << EB_BITS;
    let npb = 1usize << PB_BITS;
    let parsed = run_pool(
        nthreads,
        &order,
        |_| ParseState { arena: Arena::new(), tsp: ts::Parser::default(), ev: (0..neb).map(|_| SegVec::new()).collect(), ck: SegVec::new() },
        |st, i| {
            let chunk = all_chunks[i];
            if i < n_ev_chunks {
                scan::for_each_record::<7, _>(chunk, |f| {
                    if f[0].is_empty() {
                        return; // no event_id: never a dedup candidate
                    }
                    let eid = xxh3_128(f[0]);
                    let ingest = st.tsp.parse(f[3]).unwrap_or(i64::MAX);
                    let mut e = Ev { eid, ingest, uid_hash: 0, uid: 0, ts: INVALID, pc: 0 };
                    // Validation and the late check depend only on this copy, so they
                    // are decided here; dedup later picks the copy, valid or not.
                    'v: {
                        let uid = f[1];
                        if uid.is_empty() || f[5].is_empty() {
                            break 'v;
                        }
                        let Some(t) = st.tsp.parse(f[2]) else { break 'v };
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
                        let cat = if price > 0 { pmap.get(f[5]).unwrap_or(unknown_cat) } else { 0 };
                        e.ts = (t - ws) as u32;
                        e.uid_hash = xxh3_64(uid);
                        e.uid = pack(uid);
                        e.pc = price | (cat << 24);
                    }
                    st.ev[(eid >> (128 - EB_BITS)) as usize].push(&mut st.arena, e);
                });
            } else {
                scan::for_each_record::<4, _>(chunk, |f| {
                    if f[0].is_empty() || f[1].is_empty() {
                        return;
                    }
                    let Some(t) = st.tsp.parse(f[3]) else { return };
                    if t < cws || t >= we {
                        return;
                    }
                    let Some(camp) = cmap.get(f[2]) else { return };
                    // Copies of a click have identical values, so deduplicating on
                    // click_id cannot change any output: credit picks one click and
                    // assists count each campaign once. We skip it.
                    st.ck.push(&mut st.arena, CkRaw { uid_hash: xxh3_64(f[1]), cid: pack(f[0]), ts: (t - cws) as u32, camp });
                });
            }
        },
    );
    lap(&format!("parsed {} MB of events, {} MB of clicks", ev_bytes >> 20, ck_bytes >> 20));

    // Regroup: event_id bucket -> its segments from every worker; clicks -> tasks.
    let mut ev_b: Vec<Vec<&'static [Ev]>> = (0..neb).map(|_| Vec::new()).collect();
    let mut ck_tasks: Vec<Vec<&'static [CkRaw]>> = vec![Vec::new()];
    let mut n_clicks = 0usize;
    let mut task_len = 0usize;
    for st in &parsed {
        for (b, v) in st.ev.iter().enumerate() {
            ev_b[b].extend(v.slices());
        }
        for sl in st.ck.slices() {
            if task_len >= CLICK_TASK {
                ck_tasks.push(Vec::new());
                task_len = 0;
            }
            ck_tasks.last_mut().unwrap().push(sl);
            task_len += sl.len();
            n_clicks += sl.len();
        }
    }

    let people = links.join().expect("links thread");
    lap("links ready");

    // ---- B. dedup events, resolve persons -------------------------------------------
    let sizes: Vec<usize> = ev_b.iter().map(|v| v.iter().map(|s| s.len()).sum()).collect();
    let total_copies: usize = sizes.iter().sum();
    let task_size = |t: usize| if t < neb { sizes[t] } else { ck_tasks[t - neb].iter().map(|s| s.len()).sum() };
    let mut order: Vec<usize> = (0..neb + ck_tasks.len()).collect();
    order.sort_by_key(|&t| std::cmp::Reverse(task_size(t)));
    let people = &people;
    let resolved = run_pool(
        nthreads,
        &order,
        |_| DedupState {
            arena: Arena::new(),
            slots: Vec::new(),
            alive: Vec::new(),
            sv: (0..npb).map(|_| SegVec::new()).collect(),
            ck: (0..npb).map(|_| SegVec::new()).collect(),
        },
        |st, t| {
            if t >= neb {
                for raw in &ck_tasks[t - neb] {
                    for (j, c) in raw.iter().enumerate() {
                        if let Some(a) = raw.get(j + PREFETCH) {
                            people.prefetch(a.uid_hash);
                        }
                        let (person, _) = people.resolve(c.uid_hash, 0);
                        st.ck[pbucket(person)].push(&mut st.arena, Ck { person, cid: c.cid, ts: c.ts, camp: c.camp });
                    }
                }
                return;
            }
            let cap = (2 * sizes[t]).next_power_of_two().max(16);
            let mask = cap - 1;
            st.slots.clear();
            st.slots.resize(cap, (0, EMPTY));
            for part in &ev_b[t] {
                for e in part.iter() {
                    let r = e as *const Ev as u64;
                    let lo = e.eid as u64;
                    let mut k = lo as usize & mask;
                    loop {
                        // SAFETY: k is masked to the table size; refs point into `parts`.
                        let s = unsafe { st.slots.get_unchecked_mut(k) };
                        if s.1 == EMPTY {
                            *s = (lo, r);
                            break;
                        }
                        if s.0 == lo {
                            // SAFETY: refs point at Ev records in never-freed arena memory.
                            let cur = unsafe { &*(s.1 as *const Ev) };
                            if cur.eid == e.eid {
                                if e.ingest < cur.ingest {
                                    s.1 = r;
                                }
                                break;
                            }
                        }
                        k = (k + 1) & mask;
                    }
                }
            }
            st.alive.clear();
            for &(_, r) in st.slots.iter() {
                if r != EMPTY {
                    let e = unsafe { &*(r as *const Ev) };
                    if e.ts != INVALID {
                        st.alive.push((e.uid_hash, e.uid, e.ts, e.pc));
                    }
                }
            }
            for j in 0..st.alive.len() {
                if let Some(a) = st.alive.get(j + PREFETCH) {
                    people.prefetch(a.0);
                }
                let (h, uid, ts, pc) = st.alive[j];
                let (person, name) = people.resolve(h, uid);
                st.sv[pbucket(person)].push(&mut st.arena, Sv { person, name, ts, pc });
            }
        },
    );
    let mut sv_b: Vec<Vec<&'static [Sv]>> = (0..npb).map(|_| Vec::new()).collect();
    let mut ck_b: Vec<Vec<&'static [Ck]>> = (0..npb).map(|_| Vec::new()).collect();
    for st in &resolved {
        for b in 0..npb {
            sv_b[b].extend(st.sv[b].slices());
            ck_b[b].extend(st.ck[b].slices());
        }
    }
    let sizes: Vec<usize> = sv_b.iter().map(|v| v.iter().map(|s| s.len()).sum()).collect();
    lap(&format!("dedup: {total_copies} event copies -> {} surviving events; {n_clicks} valid clicks", sizes.iter().sum::<usize>()));

    // ---- C. sessionize + attribute per person bucket ---------------------------------
    let mut order: Vec<usize> = (0..npb).collect();
    order.sort_by_key(|&b| std::cmp::Reverse(sizes[b]));
    let ncol = ncamp + 1;
    let states = run_pool(
        nthreads,
        &order,
        |_| PersonState {
            evs: Vec::new(),
            cks: Vec::new(),
            grouped_evs: Vec::new(),
            grouped_cks: Vec::new(),
            table: Vec::new(),
            ids: Vec::new(),
            ev_start: Vec::new(),
            ck_start: Vec::new(),
            camp: vec![CampAcc::default(); ncol * ndays],
            users: Vec::new(),
            cnt: vec![0; ncol],
            pos: vec![0; ncol],
            active: Vec::new(),
            catsum: vec![0; cats.len()],
            touched: Vec::new(),
        },
        |st, b| {
            st.evs.clear();
            for p in &sv_b[b] {
                st.evs.extend_from_slice(p);
            }
            st.cks.clear();
            for p in &ck_b[b] {
                st.cks.extend_from_slice(p);
            }
            group(st);
            sessionize(st, &cats, &win, organic);
        },
    );
    lap("sessionized and attributed");

    // ---- D. write -------------------------------------------------------------------
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
    // Skip destructors of gigabytes of buffers; the files are closed and flushed.
    std::process::exit(0);
}

/// Group one person bucket by person, in linear time: a small hash table gives
/// each person a dense id, and a counting sort places their events (and clicks)
/// together. Then each person's events are sorted by ts, and their clicks by
/// (ts, click_id descending): the last click at or before a session start is
/// then the smallest click_id at the latest ts. Clicks of people with no
/// surviving events are dropped: they cannot credit or assist anything.
fn group(st: &mut PersonState) {
    let PersonState { evs, cks, grouped_evs, grouped_cks, table, ids, ev_start, ck_start, .. } = st;
    let cap = (2 * evs.len()).next_power_of_two().max(16);
    let mask = cap - 1;
    table.clear();
    table.resize(cap, (0, 0));
    ids.clear();
    ev_start.clear();
    ev_start.push(0);
    for e in evs.iter() {
        // The bucket shares its top PB_BITS, so index by the low bits.
        let mut k = e.person as usize & mask;
        let id = loop {
            let slot = &mut table[k];
            if slot.1 == 0 {
                ev_start.push(0);
                *slot = (e.person, ev_start.len() as u32 - 1);
                break slot.1 - 1;
            }
            if slot.0 == e.person {
                break slot.1 - 1;
            }
            k = (k + 1) & mask;
        };
        ids.push(id);
        ev_start[id as usize + 1] += 1;
    }
    let groups = ev_start.len() - 1;
    for g in 0..groups {
        ev_start[g + 1] += ev_start[g];
    }
    // Scatter events by group (ev_start[g] is used as a cursor, then restored).
    grouped_evs.clear();
    grouped_evs.reserve(evs.len());
    // SAFETY: every slot 0..len is written exactly once below before being read.
    unsafe { grouped_evs.set_len(evs.len()) };
    for (e, &id) in evs.iter().zip(ids.iter()) {
        let at = &mut ev_start[id as usize];
        grouped_evs[*at as usize] = *e;
        *at += 1;
    }
    for g in (1..=groups).rev() {
        ev_start[g] = ev_start[g - 1];
    }
    ev_start[0] = 0;
    for g in 0..groups {
        let part = &mut grouped_evs[ev_start[g] as usize..ev_start[g + 1] as usize];
        if part.len() > 1 {
            part.sort_unstable_by_key(|e| e.ts);
        }
    }

    // Clicks: same groups; a click whose person has no events is dropped.
    ids.clear();
    ck_start.clear();
    ck_start.resize(groups + 1, 0);
    let mut kept = 0usize;
    for c in cks.iter() {
        let mut k = c.person as usize & mask;
        let id = loop {
            let slot = &table[k];
            if slot.1 == 0 {
                break u32::MAX;
            }
            if slot.0 == c.person {
                break slot.1 - 1;
            }
            k = (k + 1) & mask;
        };
        ids.push(id);
        if id != u32::MAX {
            ck_start[id as usize + 1] += 1;
            kept += 1;
        }
    }
    for g in 0..groups {
        ck_start[g + 1] += ck_start[g];
    }
    grouped_cks.clear();
    grouped_cks.reserve(kept);
    // SAFETY: as above; exactly `kept` slots are written.
    unsafe { grouped_cks.set_len(kept) };
    for (c, &id) in cks.iter().zip(ids.iter()) {
        if id != u32::MAX {
            let at = &mut ck_start[id as usize];
            grouped_cks[*at as usize] = *c;
            *at += 1;
        }
    }
    for g in (1..=groups).rev() {
        ck_start[g] = ck_start[g - 1];
    }
    ck_start[0] = 0;
    for g in 0..groups {
        let part = &mut grouped_cks[ck_start[g] as usize..ck_start[g + 1] as usize];
        if part.len() > 1 {
            part.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| unpack(b.cid).cmp(unpack(a.cid))));
        }
    }
}

/// Walk one grouped person bucket.
fn sessionize(st: &mut PersonState, cats: &[&'static [u8]], win: &Window, organic: u32) {
    let PersonState { grouped_evs, grouped_cks, ev_start, ck_start, camp, users, cnt, pos, active, catsum, touched, .. } = st;
    let ndays = win.ndays;
    for g in 0..ev_start.len() - 1 {
        let pe = &grouped_evs[ev_start[g] as usize..ev_start[g + 1] as usize];
        let pc = &grouped_cks[ck_start[g] as usize..ck_start[g + 1] as usize];

        // Every event of a person carries the person's name.
        let name = unpack(pe[0].name);
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
            let start = pe[k].ts as i64; // ms since window_start
            let day = (win.ws + start).div_euclid(DAY_MS);
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
            while k < pe.len() && pe[k].ts as i64 - last <= GAP_MS {
                let e = &pe[k];
                last = e.ts as i64;
                n += 1;
                let price = (e.pc & 0x00FF_FFFF) as i64;
                if price > 0 {
                    rev += price;
                    let c = (e.pc >> 24) as usize;
                    if catsum[c] == 0 {
                        touched.push(c as u16);
                    }
                    catsum[c] += price;
                }
                k += 1;
            }
            d_sess += 1;
            d_ev += n;
            d_rev += rev;

            // Clicks in [start - 7d, start] (click ts are ms since window_start - 7d).
            let start_c = start + WEEK_MS;
            while hi < pc.len() && pc[hi].ts as i64 <= start_c {
                let c = pc[hi].camp as usize;
                if cnt[c] == 0 {
                    pos[c] = active.len() as u32;
                    active.push(c as u32);
                }
                cnt[c] += 1;
                hi += 1;
            }
            let lim = start_c - WEEK_MS;
            while lo < hi && (pc[lo].ts as i64) < lim {
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
            let di = (day - win.day0) as usize;
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
