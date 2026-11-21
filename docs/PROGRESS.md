# Progress log

Goal: the top of the Shopping Sessions leaderboard. At the time of the first submission (Sat morning) the fastest practice times were 0.31 s, with the pack around 1.0 to 1.5 s. Final cutoff: Sun 10:00 AM CST.

## Scoreboard

| Version | Commit | Practice score (20M, scorer) | Local best, 20M, data in RAM | Correct |
| --- | --- | --- | --- | --- |
| v1 | `4a02f9d` | **1.95 s** | 1.80 s | 36/36 fixtures, dev MATCH |
| v2 | `88d363d` | not submitted | 0.93 s | 36/36 fixtures + dev MATCH on arm64 and amd64 (AVX2); 20M identical to v1 |
| v2.1 | `451a10f` | not submitted | ~13% faster than v2 in an interleaved A/B | same, plus dev MATCH with 997-byte and 64-byte chunks (boundary stress) |
| v3 | see git log | not submitted yet | **0.80 s** (v2.1: 1.04 s) | unit tests (fast parsers and scanner vs reference), 36/36 fixtures, dev MATCH, boundary stress, on arm64 and amd64; 20M identical to v1 |

Local timings come from a Docker VM on an 8-core Apple-silicon Mac, with the data in `/dev/shm`. The Mac is often loaded, so one run can vary 2 to 3x. Only best-of-N is meaningful, and only the board counts.

## Where the time goes

```mermaid
pie title v1 local profile (20M)
    "stage1 parse + scatter" : 45
    "stage2 dedup" : 17
    "stage3 sessions" : 21
    "links (serial)" : 9
    "write parquet" : 5
    "other" : 3
```

```mermaid
pie title v2 local best (0.93 s, 20M)
    "stage1 parse (links overlapped)" : 60
    "stage2 dedup" : 15
    "stage3 sessions" : 15
    "write parquet" : 7
    "other" : 3
```

v2 profile (perf, CPU samples):
* Stage 1 user code is 47%: the scan loop 22%, event field parsing 17%, click parsing 8%.
* The kernel takes about 25%: page zeroing 8%, page-allocator lock contention 6%, page migration 4.5%.
* Stage 2 is 9%, and the stage 3 sort and walk 8%.

## v1 -> v2: what changed and why

```mermaid
flowchart LR
    subgraph v1["v1 (1.95 s)"]
        a1["links: serial, before parse"] --> b1["parse: memchr per field,<br/>xxh3 keys, hashbrown person lookup"]
        b1 --> c1["Vec buckets that grow<br/>(realloc copies)"]
        c1 --> d1["dedup: gather copy + table"]
        d1 --> e1["40 B / 24 B records,<br/>string pointers for names"]
    end
    subgraph v2["v2"]
        a2["links: union-find overlapped<br/>with event parsing"] --> b2["parse: SIMD 64-byte masks,<br/>exact packed keys"]
        b2 --> c2["block-arena buckets<br/>(no realloc, no free)"]
        c2 --> d2["dedup: insert straight into table,<br/>prefetched person lookups"]
        d2 --> e2["32 B / 16 B records,<br/>names decoded from keys"]
    end
```

## v3: single-thread cost per stage (20M events, data in RAM)

With one worker, the stages run back to back, so each stage's CPU cost is measurable without the noise of the loaded Mac. 8-thread wall time is roughly this divided by 5, because the Mac has 4 performance and 4 efficiency cores; the scorer's 4 cores with hyperthreading scale similarly.

| Stage | Work | Main costs |
| --- | --- | --- |
| A links | 0.16 s | parse, node index, union-find (all parallel in v3) |
| B parse | 2.2 s | page mapping or copying of input ~0.5, SIMD scan + fields ~0.5, field parsing ~0.6, record pushes (fresh memory) ~0.5, clicks ~0.25 |
| C dedup | 0.74 s | table 0.31, person lookups 0.33, pushes 0.10 |
| D sessions | 0.8 s | grouping 0.1, tiny sorts 0.3, session walk 0.4 |
| E write | 0.2 s | |

```mermaid
pie title v3 single-thread work (about 4.1 CPU-s on 20M)
    "B parse" : 2.2
    "C dedup" : 0.74
    "D sessions" : 0.8
    "E write" : 0.2
    "A links" : 0.16
```

## v2.1 -> v3

```mermaid
flowchart LR
    subgraph v21["v2.1"]
        a1["links: serial union-find<br/>on a side thread (0.18 s)"]
        b1["input: pread copy"]
        c1["fields: branch per quote"]
        d1["keys, timestamps: byte loops"]
        e1["clicks: unprefetched lookup"]
        f1["dedup output: fresh memory"]
        g1["sessions: gather + pdqsort"]
        h1["names: one allocation per row"]
    end
    subgraph v3["v3"]
        a2["links: parallel node index +<br/>lock-free union-find (0.05 s)"]
        b2["input: padded mmap +<br/>POPULATE_READ per range"]
        c2["fields: branch-free,<br/>quote count check"]
        d2["keys: SWAR; timestamps:<br/>date cache + SWAR digits"]
        e2["clicks: prefetched lookup"]
        f2["dedup output: recycled blocks"]
        g2["sessions: counting sort into<br/>tiny groups + insertion sort"]
        h2["names: one buffer per file"]
    end
    a1 --> a2
    b1 --> b2
    c1 --> c2
    d1 --> d2
    e1 --> e2
    f1 --> f2
    g1 --> g2
    h1 --> h2
```

## Measurements behind v2.1

Single-thread scanner throughput on memory that is already touched, on one 256 MB event file (best of 5):

| Step | Throughput |
| --- | --- |
| SIMD masks only | 17 GB/s |
| + cut lines and fields | 2.1 GB/s |
| + two ID keys | 1.55 GB/s |
| + two timestamps | 1.2 to 1.4 GB/s |
| (memchr for newlines, reference) | 29 to 35 GB/s |

Inside the pipeline, the same field-splitting step ran at about half that speed. The difference was first-touch page faults on the `mmap`ed input. Hence the switch to `pread`.

## Experiments

| Idea | Result | Kept? |
| --- | --- | --- |
| Block arena instead of growing `Vec` buckets | Less copying and memory churn; parse no longer dominated by reallocation | yes |
| Single-pass SWAR line splitter vs memchr per field | 0.047 s vs 0.056 s per 64 MB (single thread) | superseded by the SIMD scanner |
| `MADV_POPULATE_READ` per input chunk | No measurable change | no |
| Huge pages on the arena (`MADV_HUGEPAGE`) | No change locally, because the VM's THP is already `always`. Scorer setting unknown | env switch `FM_THP`, off |
| Exact packed ID keys | Removes hashing, string pointers and collision risk | yes |
| Union-find on its own thread | Takes about 0.18 s of serial work off the critical path | yes |
| 16-byte person records | Halves stage 3 traffic | yes |
| `pread` ranges into reused buffers instead of `mmap` | ~13% faster end to end in an interleaved A/B; stage 1 no longer pays input page faults | yes |
| Fast path in field split for quote-free lines | Only 21% of lines have no quote (0.8^7), so little gain | replaced by branch-free check in v3 |
| v3 parallel links | 4.4 s at first: the per-partition tables took slot bits from the same hash bits as the partition, so keys piled into 1/64 of each table. Fixed by rotating the hash: 55 ms | yes |
| v3 padded `mmap` + `POPULATE_READ` instead of `pread` | Same single-thread work here (tmpfs), but less memory traffic: best 8-thread run 0.80 s vs 0.97 s | yes |
| `MAP_POPULATE`, `MADV_HUGEPAGE`, `MADV_NOHUGEPAGE` on the arena | No difference beyond noise (single thread, 3 rounds) | no |

## Backlog (ordered by expected gain)

1. Stage 2 person lookups (0.33 s): an in-cache filter of linked IDs in front of the 32 MB map would skip most cache misses.
2. Stage 3 tiny sorts (0.3 s): sort by ts alone when a group holds one person.
3. Stage 1 field parsing (0.6 s) and record pushes (0.5 s).
4. Tune bucket counts and range size for 60M; check whether more threads than `NCPU` helps on the scorer.
