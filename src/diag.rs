//! Diagnostics for the scoring machine, switched on with FM_DIAG=1 (never in a
//! real run). The scorer shows a submission's logs only when its dev-set check
//! fails, so a diagnostic build prints what we need to know and then exits 1:
//!   * system facts: CPU, caches, huge-page policy, kernel, mounts, cgroup limits
//!   * microbenchmarks: page-fault and zeroing cost, bandwidth, random-access latency
//!   * the pipeline, timed per stage (user and kernel time apart), on a 20x tiled
//!     copy of the dev set in /scratch (about the size of the practice data)

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Instant;

/// (user, system) CPU seconds used by the process so far.
pub fn rusage() -> (f64, f64) {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let t = |v: libc::timeval| v.tv_sec as f64 + v.tv_usec as f64 * 1e-6;
    (t(ru.ru_utime), t(ru.ru_stime))
}

fn read(path: &str) -> String {
    std::fs::read_to_string(path).map(|s| s.trim().to_string()).unwrap_or_else(|_| "?".into())
}

fn vmstat() -> HashMap<String, u64> {
    read("/proc/vmstat")
        .lines()
        .filter_map(|l| {
            let mut it = l.split_whitespace();
            Some((it.next()?.to_string(), it.next()?.parse().ok()?))
        })
        .collect()
}

fn vm_delta(a: &HashMap<String, u64>, b: &HashMap<String, u64>) -> String {
    ["pgfault", "pgmajfault", "thp_fault_alloc", "thp_fault_fallback", "compact_stall", "pgfree", "numa_hint_faults"]
        .iter()
        .map(|k| format!("{}={}", k, b.get(*k).unwrap_or(&0).saturating_sub(*a.get(*k).unwrap_or(&0))))
        .collect::<Vec<_>>()
        .join(" ")
}

fn system_info() {
    eprintln!("== system");
    eprintln!("kernel: {}", read("/proc/version"));
    let cpu = read("/proc/cpuinfo");
    let first = |k: &str| cpu.lines().find(|l| l.starts_with(k)).unwrap_or("?").to_string();
    eprintln!("{} | {} | processors={}", first("model name"), first("cpu MHz"), cpu.lines().filter(|l| l.starts_with("processor")).count());
    let flags = first("flags");
    eprintln!("flags: avx2={} avx512bw={} bmi2={} erms={} fsrm={}", flags.contains(" avx2"), flags.contains("avx512bw"), flags.contains("bmi2"), flags.contains(" erms"), flags.contains("fsrm"));
    eprintln!("siblings/cores: {} / {}", first("siblings"), first("cpu cores"));
    eprintln!("available_parallelism={:?}", std::thread::available_parallelism().map(|n| n.get()));
    for i in 0..5 {
        let p = format!("/sys/devices/system/cpu/cpu0/cache/index{}", i);
        if Path::new(&p).exists() {
            eprintln!("cache L{} {} {} shared_with={}", read(&format!("{}/level", p)), read(&format!("{}/type", p)), read(&format!("{}/size", p)), read(&format!("{}/shared_cpu_list", p)));
        }
    }
    eprintln!("thp enabled: {}", read("/sys/kernel/mm/transparent_hugepage/enabled"));
    eprintln!("thp defrag: {}", read("/sys/kernel/mm/transparent_hugepage/defrag"));
    let mi = read("/proc/meminfo");
    for k in ["MemTotal", "MemFree", "MemAvailable", "Cached", "AnonHugePages", "Hugepagesize"] {
        eprintln!("{}", mi.lines().find(|l| l.starts_with(k)).unwrap_or("?"));
    }
    eprintln!("cgroup cpu.max={} memory.max={}", read("/sys/fs/cgroup/cpu.max"), read("/sys/fs/cgroup/memory.max"));
    for l in read("/proc/mounts").lines() {
        let f: Vec<&str> = l.split_whitespace().collect();
        if f.len() > 2 && ["/", "/data", "/scratch", "/output", "/tmp"].contains(&f[1]) {
            eprintln!("mount {} {} {} {}", f[1], f[2], f[0], f.get(3).unwrap_or(&""));
        }
    }
    eprintln!("env NCPU={:?} MEM_GB={:?}", std::env::var("NCPU").ok(), std::env::var("MEM_GB").ok());
}

fn map(n: usize, huge: bool) -> *mut u8 {
    unsafe {
        let p = libc::mmap(std::ptr::null_mut(), n, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE, -1, 0);
        assert!(p != libc::MAP_FAILED);
        libc::madvise(p, n, if huge { libc::MADV_HUGEPAGE } else { libc::MADV_NOHUGEPAGE });
        p as *mut u8
    }
}

fn timed<R>(f: impl FnOnce() -> R) -> (R, f64, f64, f64) {
    let (u0, s0) = rusage();
    let t = Instant::now();
    let r = f();
    let e = t.elapsed().as_secs_f64();
    let (u1, s1) = rusage();
    (r, e, u1 - u0, s1 - s0)
}

fn microbench(nt: usize) {
    eprintln!("== microbenchmarks");
    const GB: usize = 1 << 30;
    for huge in [false, true] {
        let p = map(GB, huge);
        let v0 = vmstat();
        let (_, e, u, s) = timed(|| {
            for i in (0..GB).step_by(4096) {
                unsafe { p.add(i).write_volatile(1) };
            }
        });
        eprintln!("first touch 1 GiB, 1 thread, huge={}: {:.3}s (user {:.3} sys {:.3}) [{}]", huge, e, u, s, vm_delta(&v0, &vmstat()));
        let (_, e, _, _) = timed(|| unsafe { std::ptr::write_bytes(p, 7, GB) });
        eprintln!("  memset same 1 GiB (warm): {:.3}s = {:.1} GB/s", e, 1.0 / e);
        unsafe { libc::munmap(p as *mut libc::c_void, GB) };
    }
    // parallel first touch: the page allocator under 8 threads
    for huge in [false, true] {
        let p = map(GB, huge) as usize;
        let (_, e, u, s) = timed(|| {
            std::thread::scope(|sc| {
                for t in 0..nt {
                    sc.spawn(move || {
                        let per = GB / nt;
                        for i in (t * per..(t + 1) * per).step_by(4096) {
                            unsafe { (p as *mut u8).add(i).write_volatile(1) };
                        }
                    });
                }
            })
        });
        eprintln!("first touch 1 GiB, {} threads, huge={}: {:.3}s (user {:.3} sys {:.3})", nt, huge, e, u, s);
        // parallel read bandwidth on warm memory
        let (sum, e, _, _) = timed(|| {
            std::thread::scope(|sc| {
                let hs: Vec<_> = (0..nt)
                    .map(|t| {
                        sc.spawn(move || {
                            let per = GB / nt / 8;
                            let q = unsafe { std::slice::from_raw_parts((p as *const u64).add(t * per), per) };
                            q.iter().fold(0u64, |a, &x| a.wrapping_add(x))
                        })
                    })
                    .collect();
                hs.into_iter().map(|h| h.join().unwrap()).fold(0u64, |a, x| a.wrapping_add(x))
            })
        });
        eprintln!("  read 1 GiB with {} threads: {:.3}s = {:.1} GB/s (sum {})", nt, e, 1.0 / e, sum & 1);
        unsafe { libc::munmap(p as *mut libc::c_void, GB) };
    }
    // random access latency (dependent chain) and throughput (independent loads)
    for huge in [false, true] {
        let n = 64 << 20; // 64M u32 = 256 MiB
        let p = map(n * 4, huge) as *mut u32;
        let a = unsafe { std::slice::from_raw_parts_mut(p, n) };
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        for i in 0..n {
            a[i] = i as u32;
        }
        for i in (1..n).rev() {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let j = (x % i as u64) as usize;
            a.swap(i, j);
        }
        let steps = 2_000_000;
        let (end, e, _, _) = timed(|| {
            let mut k = 0usize;
            for _ in 0..steps {
                k = a[k] as usize;
            }
            k
        });
        let (sum, e2, _, _) = timed(|| {
            let mut s = 0u64;
            let mut k = 1u64;
            for _ in 0..steps {
                k = k.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                s = s.wrapping_add(a[(k >> 38) as usize] as u64);
            }
            s
        });
        eprintln!("random 256 MiB, huge={}: dependent {:.1} ns/load, independent {:.1} ns/load ({} {})", huge, e * 1e9 / steps as f64, e2 * 1e9 / steps as f64, end & 1, sum & 1);
        unsafe { libc::munmap(p as *mut libc::c_void, n * 4) };
    }
}

const ID_FIELDS: [usize; 2] = [0, 1];

/// Shift the last character of each ID field by `r` positions in base 36, so
/// every replica has its own events, users and clicks in the same format.
fn shift_line(line: &mut [u8], r: usize) {
    let mut s = 0;
    let mut k = 0;
    let n = line.len();
    for i in 0..=n {
        if i == n || line[i] == b',' {
            if ID_FIELDS.contains(&k) {
                let mut e = i;
                if e > s + 1 && line[e - 1] == b'"' {
                    e -= 1;
                }
                if e >= s + 2 {
                    let c = line[e - 1];
                    let v = match c {
                        b'0'..=b'9' => Some(c - b'0'),
                        b'a'..=b'z' => Some(c - b'a' + 10),
                        _ => None,
                    };
                    if let Some(v) = v {
                        let nv = (v as usize + r) % 36;
                        line[e - 1] = if nv < 10 { b'0' + nv as u8 } else { b'a' + nv as u8 - 10 };
                    }
                }
            }
            k += 1;
            s = i + 1;
        }
    }
}

fn tile(src: &Path, dst: &Path, reps: usize, nt: usize) -> usize {
    let _ = std::fs::remove_dir_all(dst);
    for d in ["events", "clicks", "links"] {
        std::fs::create_dir_all(dst.join(d)).unwrap();
    }
    for f in ["products.csv", "campaigns.csv", "manifest.json"] {
        std::fs::copy(src.join(f), dst.join(f)).unwrap();
    }
    let mut jobs = Vec::new();
    for d in ["events", "clicks", "links"] {
        if let Ok(rd) = std::fs::read_dir(src.join(d)) {
            for e in rd.flatten() {
                let p = e.path();
                if p.extension().map_or(false, |x| x == "csv") {
                    for r in 0..reps {
                        jobs.push((p.clone(), dst.join(d).join(format!("r{:02}-{}", r, p.file_name().unwrap().to_string_lossy())), r));
                    }
                }
            }
        }
    }
    let next = AtomicUsize::new(0);
    let bytes = AtomicUsize::new(0);
    std::thread::scope(|s| {
        for _ in 0..nt {
            s.spawn(|| loop {
                let i = next.fetch_add(1, Relaxed);
                let Some((from, to, r)) = jobs.get(i) else { break };
                let mut out = std::fs::read(from).unwrap();
                let mut s = match memchr::memchr(b'\n', &out) {
                    Some(i) => i + 1,
                    None => out.len(),
                };
                while s < out.len() {
                    let e = memchr::memchr(b'\n', &out[s..]).map_or(out.len(), |i| s + i);
                    shift_line(&mut out[s..e], *r);
                    s = e + 1;
                }
                bytes.fetch_add(out.len(), Relaxed);
                std::fs::File::create(to).unwrap().write_all(&out).unwrap();
            });
        }
    });
    bytes.into_inner()
}

/// How fast can the page-cached input be brought in: copied with pread, mapped
/// with one populate call per file, or mapped page by page through faults.
fn input_access(dir: &Path, nt: usize) {
    let files: Vec<std::path::PathBuf> = std::fs::read_dir(dir).unwrap().flatten().map(|e| e.path()).collect();
    for mode in ["pread 4 MiB", "mmap + POPULATE_READ", "mmap + page faults"] {
        let next = AtomicUsize::new(0);
        let total = AtomicUsize::new(0);
        let (_, e, u, s) = timed(|| {
            std::thread::scope(|sc| {
                for _ in 0..nt {
                    sc.spawn(|| {
                        let mut buf = vec![0u8; 4 << 20];
                        let mut acc = 0u64;
                        loop {
                            let i = next.fetch_add(1, Relaxed);
                            let Some(p) = files.get(i) else { break };
                            let f = std::fs::File::open(p).unwrap();
                            let n = f.metadata().unwrap().len() as usize;
                            total.fetch_add(n, Relaxed);
                            if mode.starts_with("pread") {
                                use std::os::unix::fs::FileExt;
                                let mut off = 0;
                                while off < n {
                                    let k = f.read_at(&mut buf, off as u64).unwrap();
                                    for j in (0..k).step_by(4096) {
                                        acc = acc.wrapping_add(buf[j] as u64);
                                    }
                                    off += k;
                                }
                            } else {
                                use std::os::fd::AsRawFd;
                                unsafe {
                                    let m = libc::mmap(std::ptr::null_mut(), n, libc::PROT_READ, libc::MAP_PRIVATE, f.as_raw_fd(), 0);
                                    if mode.contains("POPULATE") {
                                        libc::madvise(m, n, 22);
                                    }
                                    let q = m as *const u8;
                                    for j in (0..n).step_by(4096) {
                                        acc = acc.wrapping_add(*q.add(j) as u64);
                                    }
                                    libc::munmap(m, n);
                                }
                            }
                        }
                        std::hint::black_box(acc);
                    });
                }
            })
        });
        let gb = total.into_inner() as f64 / 1e9;
        eprintln!("input {}: {:.2} GB in {:.3}s = {:.1} GB/s (user {:.3} sys {:.3})", mode, gb, e, gb / e, u, s);
    }
}

pub fn main(data_dir: &Path, nt: usize, run: &dyn Fn(&Path, &Path, usize)) {
    let t0 = Instant::now();
    system_info();
    microbench(nt);
    let scratch = std::path::PathBuf::from(std::env::var("SCRATCH_DIR").unwrap_or("/scratch".into()));
    eprintln!("== dev set (1M), {} threads", nt);
    run(data_dir, &scratch.join("out-dev"), nt);
    let reps: usize = std::env::var("FM_DIAG_REPS").ok().and_then(|v| v.parse().ok()).unwrap_or(20);
    let tiled = scratch.join("tiled");
    let (bytes, e, _, _) = timed(|| tile(data_dir, &tiled, reps, nt));
    eprintln!("== tiled the dev set {}x into {:?}: {} MB in {:.2}s", reps, tiled, bytes >> 20, e);
    input_access(&tiled.join("events"), nt);
    let variants: [(&str, usize, Option<&str>); 6] = [
        ("default", nt, None),
        ("default again", nt, None),
        ("arena MADV_HUGEPAGE", nt, Some("FM_THP")),
        ("default third", nt, None),
        ("half the threads", (nt / 2).max(1), None),
        ("single thread", 1, None),
    ];
    for (i, (name, n, env)) in variants.iter().enumerate() {
        if let Some(k) = env {
            std::env::set_var(k, "1");
        }
        eprintln!("== run {}: {} ({} threads)", i, name, n);
        let v0 = vmstat();
        let (_, e, u, s) = timed(|| run(&tiled, &scratch.join(format!("out-{}", i)), *n));
        eprintln!("   total {:.3}s user {:.3} sys {:.3} [{}]", e, u, s, vm_delta(&v0, &vmstat()));
        if let Some(k) = env {
            std::env::remove_var(k);
        }
        let _ = std::fs::remove_dir_all(scratch.join(format!("out-{}", i)));
    }
    eprintln!("== diagnostics done in {:.1}s; exiting 1 on purpose so the logs are shown", t0.elapsed().as_secs_f64());
}
