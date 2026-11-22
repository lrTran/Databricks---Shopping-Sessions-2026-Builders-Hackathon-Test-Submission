# Shopping Sessions — Rust pipeline

A complete, correct-first implementation of `rules.md` in Rust.

* Matches the published dev answer exactly (`make check-dev`: 118,107 + 27,449 rows).
* Passes all 36 edge-case fixtures.
* Matches an independent Python reference exactly on official-generator seeds
  2–7 and 15 (seed 15's window holds 2028-02-29), and on 8 seeds of extra-dirty
  synthetic data.

## Why Rust

The score is wall-clock time from container start to exit, including engine
startup and CSV reading, and correctness is all-or-nothing. A single static
binary starts in milliseconds, and a hand-written parser implements the CSV
dialect exactly. Generic CSV readers (Polars, DuckDB, Spark) each have their own
ideas about quotes, `\r` and malformed lines, which rules.md overrides.

## Layout

```
Dockerfile            two-stage build: rust:1.85 builder -> debian slim runtime, runs as uid 1000
Cargo.toml/.lock      memchr, memmap2, xxhash-rust, libc, parquet (no Arrow, no codecs)
src/scan.rs           SIMD scanner: the CSV dialect, 64 bytes at a time (quotes, \r, field counts, empty lines)
src/csv.rs            the same dialect line by line, for the small files, plus file chunking
src/ts.rs             timestamp forms F1/F2/F3: a plain reference parser, and a fast cached one fuzzed against it
src/arena.rs          per-thread huge-page arenas and segmented vectors for the record buffers
src/out.rs            Parquet writer (day = INT32 DATE, strings UTF8)
src/main.rs           the pipeline
scripts/reference.py  slow, literal Python implementation of rules.md, for differential testing
scripts/synth.py      synthetic dirty-data generator exercising every dirt type in rules.md
scripts/run_fixtures.py, compare.py, docker_run.sh   from the starter
fixtures/             the 36 edge cases (excluded from the Docker build context)
rules.md              the spec; it wins over everything else
```

## How the pipeline works

Everything stays in memory: about 5 GB of CSV and well under 24 GB of working
set at final size. One pool of `$NCPU` threads runs each stage:

1. **Links, on a side thread.** The links are parsed and merged with union-find,
   and each component is named by its byte-wise smallest ID. A person's *key* is
   `xxh3_64(name)`, so an unlinked ID's key is just the hash of the ID. Only
   linked IDs whose name is a *different* ID go into an open-addressing table of
   `uid hash -> (person key, name)`. This thread runs while step 2 parses, so
   it costs no wall time.
2. **Parse events and clicks.** The files are memory-mapped and split into ~8 MB
   chunks at line boundaries, largest first. `scan.rs` finds every `,` `"` `\n`
   with SSE2 bitmasks and walks only those positions. Validation and the late
   check depend only on that copy of the event, so they are decided here.
   Timestamps go through a per-thread cache of the ~21 distinct dates. Event
   copies become 48-byte records in 1,024 buckets by `xxh3_128(event_id)`.
   Valid clicks become 24-byte records. Neither needs its person yet.
3. **Dedup and resolve.** For each event-id bucket, an open-addressing table
   keeps the copy with the smallest `ingest_ts`. That copy survives only if its
   own flag says valid, so dedup happens before validation and a later valid
   copy never rescues the event. The survivors' persons are then looked up in
   batches, with memory prefetching to hide cache misses, and the survivors are
   scattered into 1,024 person buckets as 24-byte records. Clicks are resolved
   the same way.
4. **Sessionize and attribute.** In each person bucket, a small hash table and
   a counting sort group events and clicks by person in linear time. Then each
   person's events are sorted by `ts`, and their clicks by `(ts, click_id
   descending)`. For each person:
   * A session breaks when `ts - prev > 30 min`. Equal timestamps have a gap of
     0, so they always join.
   * Two pointers keep the click window `[start - 7d, start]` with a multiset of
     its campaigns. The credit is the last click in the window: the latest `ts`
     and, thanks to the descending sort, the smallest `click_id`. The assists are
     the campaigns currently in the multiset.
   * user_daily is aggregated per (person, day of session start).
   * campaign_daily is aggregated into a dense `[campaign][day]` array per thread.
5. **Write.** One user_daily file per worker, written in parallel, plus one
   campaign_daily file. Then `process::exit` skips gigabytes of destructors.

The record buffers come from per-thread **arenas** (`arena.rs`). Each is a
large virtual reservation marked for transparent huge pages and populated in
4 MB steps with `MADV_POPULATE_WRITE`. This removes most page faults, which
otherwise cost more as threads are added. Nothing is freed mid-run, because
`munmap` interrupts every core. On kernels without these features the
`madvise` calls fail harmlessly.

Progress and timings are logged to stderr.

### Assumptions to know about

* **Hashes as identity.** Event IDs are deduplicated by 128-bit xxh3, so a
  collision is practically impossible. Person keys and the link lookup use 64-bit
  xxh3, which gives roughly a 1-in-a-million chance of merging two people at
  final size. Product and campaign lookups compare the actual bytes.
* **No click dedup.** rules.md guarantees that copies of a click have identical
  values. The credit picks one click and the assists count each campaign once,
  so duplicate copies cannot change any output. The reference does dedup, and
  the two agree.
* An `ingest_ts` that fails to parse (rules.md says it never does) is treated as
  +infinity.
* Compact records assume, and check at startup, that window_start − 7 days to
  window_end fits in u32 milliseconds (49 days; the generator uses 14 + 7), and
  that there are at most 255 categories (the generator has 20). IDs must be
  shorter than 64 KB, because their length is packed into 16 bits.

## Local workflow

You need cargo (Rust 1.85 or newer) and `pip install polars==2.1.0` for the test
scripts. Docker is only needed for the scorer-like targets.

```sh
Build the official generator once from generator.zip (`cargo build --release`)
and put `target/release/generate` on your PATH, or pass `GENERATE=/path/to/generate`.
Unzip dev-answer.zip into the repo root as `dev-answer/`.

```sh
make test            # unit tests: CSV dialect + timestamps
make fixtures-local  # 36/36 fixtures, native binary, about a second
make dev             # the 1M-event dev set (seed 1)
make run-dev-local   # run natively on dev/ and diff against dev-answer/: prints MATCH
make gen-check       # official generator, seeds 2 3 4 5 15: Rust vs Python reference
make synth-check     # extra-dirty synthetic data (\r, odd quoting): Rust vs reference

make build fixtures  # same fixtures through the real image and scorer flags
make run-dev         # image on dev/ with scorer limits, timed
make check-dev       # exact diff of that run's out/ vs dev-answer/
```
```

Only seed 1 has a published answer, so `gen-check` and `synth-check` provide
the other oracles. `synth.py` imitates the dirt in rules.md (duplicate events where the earliest
copy has a bad `ts` or a different user, link chains, shared devices, boundary
clicks, same-millisecond ties, quoting, malformed lines, `\r`). It is not the
official generator, so `check-dev` remains the ground truth. The two test
oracles are deliberately independent. When they disagree, read rules.md and work
out which one is wrong. Once, during development, it was the synthesizer.

## Before you submit

1. `make test fixtures-local run-dev-local gen-check synth-check` all green.
2. `make run-dev check-dev` prints `MATCH` through Docker, well under 15 s.
   The stderr log shows each stage's time.
3. `make build fixtures` passes through Docker on linux/amd64.
4. Push to a public GitHub repo. No data is committed: `.gitignore` excludes
   dev/, out/, synth/ and target/.

## Measured so far

These were measured on official-generator data in a 1-core, 3 GB sandbox,
without Docker. Old and new runs alternated so they saw the same conditions.

| Dataset | First version | Optimized | Peak RSS |
| --- | --- | --- | --- |
| dev, seed 1, 1M events | 436 ms | 298 ms | — |
| seed 9, 5M events | 2,653 ms | 1,530 ms | 1.06 GB |

On 5M events, user CPU went from 2.12 s to 1.41 s and page faults from 189k to
48k. The first version scored about 2.3 s on the practice data.

## Where to find more speed

Callgrind (`valgrind --tool=callgrind`) works on this binary, built with
`CARGO_PROFILE_RELEASE_DEBUG=line-tables-only CARGO_PROFILE_RELEASE_STRIP=false`.
On the dev set, by instructions, the scanner is about a third of the run and
timestamps about a tenth. Ideas, roughly in order:

* **Scanner.** Most of its cost is the per-character loop, not the bitmasks.
  Ideas: process a whole line from its comma mask, or AVX-512 masks with runtime
  detection (the scorers have it).
* **Hashing.** The event ID could use a 64-bit hash, with the bytes compared on
  a match, to shrink `Ev` to 40 bytes.
* **Reuse memory between stages.** Zeroing fresh pages is now most of the kernel
  time. The dedup stage's output arena could reuse the parse stage's memory.
* Measure on the real 8 vCPUs before tuning further: thread scaling there is
  untested.

Re-run `make fixtures-local synth-check check-dev` after every optimization.
