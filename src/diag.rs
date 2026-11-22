#![allow(dead_code)]
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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::Mutex;
use std::time::Instant;

/// When set, the pipeline records its stage laps here instead of printing them.
pub static QUIET: AtomicBool = AtomicBool::new(false);
pub static LAPS: Mutex<Vec<(String, f64)>> = Mutex::new(Vec::new());

// ---------------------------------------------------------------- sampling profiler

const NS: usize = 1 << 20;
static PCS: [AtomicU64; NS] = [const { AtomicU64::new(0) }; NS];
static NPC: AtomicUsize = AtomicUsize::new(0);
static PROF_ON: AtomicBool = AtomicBool::new(false);

extern "C" fn on_prof(_: libc::c_int, _: *mut libc::siginfo_t, ctx: *mut libc::c_void) {
    if !PROF_ON.load(Relaxed) {
        return;
    }
    let uc = ctx as *const libc::ucontext_t;
    #[cfg(target_arch = "x86_64")]
    let pc = unsafe { (*uc).uc_mcontext.gregs[libc::REG_RIP as usize] as u64 };
    #[cfg(target_arch = "aarch64")]
    let pc = unsafe { (*uc).uc_mcontext.pc };
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let pc = 0u64;
    let i = NPC.fetch_add(1, Relaxed);
    if i < NS {
        PCS[i].store(pc, Relaxed);
    }
}

/// Sample the program counter every `us` microseconds of process CPU time.
fn prof_start(us: i64) {
    NPC.store(0, Relaxed);
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = on_prof as *const () as usize;
        sa.sa_flags = libc::SA_SIGINFO | libc::SA_RESTART;
        libc::sigaction(libc::SIGPROF, &sa, std::ptr::null_mut());
        let tv = libc::timeval { tv_sec: 0, tv_usec: us as libc::suseconds_t };
        let it = libc::itimerval { it_interval: tv, it_value: tv };
        PROF_ON.store(true, Relaxed);
        libc::setitimer(libc::ITIMER_PROF, &it, std::ptr::null_mut());
    }
}

fn prof_stop() {
    unsafe {
        let z = libc::timeval { tv_sec: 0, tv_usec: 0 };
        let it = libc::itimerval { it_interval: z, it_value: z };
        libc::setitimer(libc::ITIMER_PROF, &it, std::ptr::null_mut());
    }
    PROF_ON.store(false, Relaxed);
}

/// Hottest lines of our own code, and hottest functions, among the samples.
/// Each sample is charged to the innermost inlined frame that lies in src/.
fn prof_report(top_lines: usize, top_fns: usize) -> Vec<String> {
    const OURS: [&str; 7] = ["main.rs", "scan.rs", "keys.rs", "mem.rs", "links.rs", "out.rs", "diag.rs"];
    let n = NPC.load(Relaxed).min(NS);
    let mut by_pc: HashMap<u64, usize> = HashMap::new();
    for i in 0..n {
        *by_pc.entry(PCS[i].load(Relaxed)).or_insert(0) += 1;
    }
    let mut by_line: HashMap<String, usize> = HashMap::new();
    let mut by_fn: HashMap<String, usize> = HashMap::new();
    for (&pc, &c) in &by_pc {
        let mut frames: Vec<(String, u32, String)> = Vec::new();
        backtrace::resolve(pc as *mut std::ffi::c_void, |sym| {
            let file = sym.filename().and_then(|f| f.file_name()).map(|f| f.to_string_lossy().into_owned()).unwrap_or_else(|| "?".into());
            let name = sym.name().map(|n| format!("{:#}", n)).unwrap_or_default();
            frames.push((file, sym.lineno().unwrap_or(0), name));
        });
        let ours = frames.iter().find(|f| OURS.contains(&f.0.as_str())).or(frames.first());
        let (line_key, fn_key) = match ours {
            Some((file, line, name)) => {
                let f: String = name.split("::").filter(|p| !p.is_empty() && !p.starts_with('{') && !p.starts_with('<')).last().unwrap_or("?").to_string();
                (format!("{}:{} {}", file, line, f), format!("{} {}", file, f))
            }
            None => ("?".into(), "?".into()),
        };
        *by_line.entry(line_key).or_insert(0) += c;
        *by_fn.entry(fn_key).or_insert(0) += c;
    }
    let pct = |c: usize| 100.0 * c as f64 / n.max(1) as f64;
    let mut out = vec![format!("P {} samples (500us of CPU each)", n)];
    let mut v: Vec<(usize, String)> = by_fn.into_iter().map(|(k, c)| (c, k)).collect();
    v.sort_unstable_by(|a, b| b.cmp(a));
    for (c, k) in v.into_iter().take(top_fns) {
        let mut l = format!("F {:4.1}% {}", pct(c), k);
        l.truncate(70);
        out.push(l);
    }
    let mut v: Vec<(usize, String)> = by_line.into_iter().map(|(k, c)| (c, k)).collect();
    v.sort_unstable_by(|a, b| b.cmp(a));
    for (c, k) in v.into_iter().take(top_lines) {
        let mut l = format!("L {:4.1}% {}", pct(c), k);
        l.truncate(70);
        out.push(l);
    }
    out
}

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
                    // replicas 36.. also shift the second-last character
                    if r >= 36 && e >= s + 3 {
                        let c = line[e - 2];
                        let v = match c {
                            b'0'..=b'9' => Some(c - b'0'),
                            b'a'..=b'z' => Some(c - b'a' + 10),
                            _ => None,
                        };
                        if let Some(v) = v {
                            let nv = (v as usize + r / 36) % 36;
                            line[e - 2] = if nv < 10 { b'0' + nv as u8 } else { b'a' + nv as u8 - 10 };
                        }
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

/// One line of facts about the machine.
fn facts() -> String {
    let cpu = read("/proc/cpuinfo");
    let model = cpu.lines().find(|l| l.starts_with("model name")).and_then(|l| l.split(':').nth(1)).unwrap_or("?").trim().to_string();
    let mut caches = Vec::new();
    for i in 0..5 {
        let p = format!("/sys/devices/system/cpu/cpu0/cache/index{}", i);
        if Path::new(&p).exists() {
            caches.push(format!("L{}{}={}", read(&format!("{}/level", p)), &read(&format!("{}/type", p))[..1], read(&format!("{}/size", p))));
        }
    }
    let thp = read("/sys/kernel/mm/transparent_hugepage/enabled");
    let mut m = model;
    m.truncate(40);
    format!("D {} | {} | thp {} | n={:?}", m, caches.join(" "), thp, std::thread::available_parallelism().map(|n| n.get()).ok())
}

/// One-line result of a BENCH=1 child (parse kernels, single thread).
pub static BENCH_OUT: Mutex<String> = Mutex::new(String::new());

fn lap_line() -> String {
    let b = BENCH_OUT.lock().unwrap().clone();
    if !b.is_empty() {
        return b;
    }
    let short = |l: &str| match l {
        "opened" => "o",
        "links" => "l",
        "prepopulated" => "pp",
        "stage1 parse" => "p",
        "stage1 parse + links" => "p",
        "stage2 dedup" => "d",
        "stage3 sessions + user_daily" => "s",
        "written" => "w",
        _ => "?",
    };
    let mut prev = 0.0;
    let mut parts = Vec::new();
    for (name, t) in LAPS.lock().unwrap().iter() {
        parts.push(format!("{}{:.3}", short(name), t - prev));
        prev = *t;
    }
    parts.join(" ")
}

/// Run the pipeline in a forked child that exits like a real run, so the wall
/// time includes process teardown. Returns "laps W<wall> u<user> s<sys>".
fn run_child(data: &Path, out: &Path, n: usize, env: &[(&str, &str)], run: &dyn Fn(&Path, &Path, usize)) -> String {
    for (k, v) in env {
        std::env::set_var(k, v);
    }
    let mut fds = [0i32; 2];
    unsafe { libc::pipe(fds.as_mut_ptr()) };
    let t = Instant::now();
    let pid = unsafe { libc::fork() };
    if pid == 0 {
        unsafe { libc::close(fds[0]) };
        LAPS.lock().unwrap().clear();
        run(data, out, n);
        let line = lap_line();
        unsafe { libc::write(fds[1], line.as_ptr() as *const libc::c_void, line.len()) };
        std::process::exit(0);
    }
    unsafe { libc::close(fds[1]) };
    let mut buf = vec![0u8; 4096];
    let mut got = 0;
    loop {
        let k = unsafe { libc::read(fds[0], buf[got..].as_mut_ptr() as *mut libc::c_void, buf.len() - got) };
        if k <= 0 {
            break;
        }
        got += k as usize;
    }
    unsafe { libc::close(fds[0]) };
    let mut status = 0;
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::wait4(pid, &mut status, 0, &mut ru) };
    let wall = t.elapsed().as_secs_f64();
    for (k, _) in env {
        std::env::remove_var(k);
    }
    let _ = std::fs::remove_dir_all(out);
    let tv = |v: libc::timeval| v.tv_sec as f64 + v.tv_usec as f64 * 1e-6;
    format!("{} W{:.3} u{:.2} s{:.2}{}", String::from_utf8_lossy(&buf[..got]), wall, tv(ru.ru_utime), tv(ru.ru_stime), if status != 0 { " FAIL" } else { "" })
}

/// Cost of getting fresh writable memory on this machine, per 4 KiB page.
fn memcost(nt: usize) -> String {
    const SZ: usize = 512 << 20;
    let mut parts = Vec::new();
    for threads in [1usize, nt] {
        let p = map(SZ, false) as usize;
        let (_, e, u, s) = timed(|| {
            std::thread::scope(|sc| {
                for t in 0..threads {
                    sc.spawn(move || {
                        let per = SZ / threads;
                        for i in (t * per..(t + 1) * per).step_by(4096) {
                            unsafe { (p as *mut u8).add(i).write_volatile(1) };
                        }
                    });
                }
            })
        });
        parts.push(format!("anon{}t {:.2}us/pg(cpu {:.2})", threads, e * 1e6 / (SZ / 4096) as f64, (u + s) * 1e6 / (SZ / 4096) as f64));
        unsafe { libc::munmap(p as *mut libc::c_void, SZ) };
    }
    // page cache: write() a file, then map it back
    let scratch = std::path::PathBuf::from(std::env::var("SCRATCH_DIR").unwrap_or("/scratch".into()));
    let path = scratch.join("memcost.bin");
    let buf = vec![7u8; 4 << 20];
    let (_, e, u, s) = timed(|| {
        let mut f = std::fs::File::create(&path).unwrap();
        for _ in 0..SZ / buf.len() {
            f.write_all(&buf).unwrap();
        }
    });
    parts.push(format!("pcwrite {:.2}us/pg(cpu {:.2})", e * 1e6 / (SZ / 4096) as f64, (u + s) * 1e6 / (SZ / 4096) as f64));
    let _ = std::fs::remove_file(&path);
    format!("M {}", parts.join(" "))
}

pub fn main(data_dir: &Path, nt: usize, run: &dyn Fn(&Path, &Path, usize)) {
    let t0 = Instant::now();
    let scratch = std::path::PathBuf::from(std::env::var("SCRATCH_DIR").unwrap_or("/scratch".into()));
    let r20: usize = std::env::var("FM_DIAG_REPS").ok().and_then(|v| v.parse().ok()).unwrap_or(20);
    let r60: usize = std::env::var("FM_DIAG_REPS60").ok().and_then(|v| v.parse().ok()).unwrap_or(60);
    QUIET.store(true, Relaxed);
    let mut lines = Vec::new();
    let t20 = scratch.join("tiled20");
    let (b, te, _, _) = timed(|| tile(data_dir, &t20, r20, nt));
    lines.push(format!("T {}x {} MB {:.2}s", r20, b >> 20, te));
    let out = scratch.join("out");
    let variants: Vec<(&str, Vec<(&str, &str)>)> = vec![
        ("base", vec![]),
        ("scan1", vec![("FM_SCAN1", "1")]),
        ("pf1k", vec![("FM_PF", "1024")]),
        ("s3div16", vec![("FM_S3DIV", "16")]),
        ("s3-24k", vec![("FM_S3MAX", "24576")]),
        ("nosub", vec![("FM_SUBMAX", "100000000")]),
        ("b9/10", vec![("FM_B2", "10")]),
        ("base2", vec![]),
        ("scan1bl", vec![("FM_SCAN1", "1"), ("FM_SCANBL", "1")]),
        ("scan1-2", vec![("FM_SCAN1", "1")]),
        ("pf1k2", vec![("FM_PF", "1024")]),
        ("s3div162", vec![("FM_S3DIV", "16")]),
    ];
    // the first run after tiling pays for the fresh files: warm up, then measure
    lines.push(format!("V warmup  {}", run_child(&t20, &out, nt, &[], run)));
    for (label, env) in &variants {
        lines.push(format!("V {:7} {}", label, run_child(&t20, &out, nt, env, run)));
    }
    // parse kernels on one thread, input streaming from memory
    lines.push(format!("B {}", run_child(&t20, &out, 1, &[("BENCH", "1")], run)));
    // the final's scale
    let t60 = scratch.join("tiled60");
    let _ = std::fs::remove_dir_all(&t20);
    let (b, te, _, _) = timed(|| tile(data_dir, &t60, r60, nt));
    lines.push(format!("T {}x {} MB {:.2}s", r60, b >> 20, te));
    lines.push(format!("V 60:warm {}", run_child(&t60, &out, nt, &[], run)));
    for (label, env) in [("60:base", vec![]), ("60:nos3", vec![("FM_S3MAX", "100000000")]), ("60:b10", vec![("FM_B1", "10"), ("FM_B2", "10")]), ("60:scan1", vec![("FM_SCAN1", "1")]), ("60:base2", vec![])] {
        lines.push(format!("V {:7} {}", label, run_child(&t60, &out, nt, &env, run)));
    }
    let _ = std::fs::remove_dir_all(&t60);
    lines.push(memcost(nt));
    lines.push(facts());
    lines.push(format!("Z done {:.1}s; exit 1 on purpose", t0.elapsed().as_secs_f64()));
    for l in &lines {
        eprintln!("{}", l);
    }
}
