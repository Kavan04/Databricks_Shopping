# Shopping Sessions: Rust pipeline

Single static-ish Rust binary (`src/main.rs`), implementing `rules.md` exactly.

Pipeline: mmap every CSV in newline-aligned 8 MB chunks, then
1. links: parse, union-find over 64-bit xxh3 hashes, person key = hash of the byte-wise smallest id
2. events/clicks: parse + validate in parallel; events scatter by event_id hash, clicks by person hash
3. per event_id bucket: dedup on smallest `ingest_ts` (before validation), scatter survivors by person
4. per person bucket: sort by ts, sessionize (gap > 30 min), credit/assist via a sliding 7-day click window
5. write both tables as uncompressed parquet (one file per worker thread)

Local checks (Docker): `docker build -t shopping-sessions .`, then
`python3 scripts/run_fixtures.py fixtures -- scripts/dev_run.sh` and
`scripts/dev_run.sh dev out && python3 scripts/compare.py out dev-answer`.
