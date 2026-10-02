// Turso 0.8 through its native Rust crate: the workload shapes of the Turso 0.8 post (100-row INSERT per transaction on disjoint keys with
// BEGIN CONCURRENT; closed-loop throughput, or open-loop Poisson arrivals with latency measured from the scheduled arrival to the commit).
// Written for this comparison; it is not Turso's own benchmark suite.
//   turso_bench throughput|latency CONNS [duration_s] [tps]
use std::sync::Arc;
use std::time::{Duration, Instant};
use turso::Builder;

const PAYLOAD: &str = "0123456789012345678901234567890123456789012345678901234567890123";

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 { let mut x = self.0; x ^= x << 13; x ^= x >> 7; x ^= x << 17; self.0 = x; x }
    fn exp(&mut self, mean_s: f64) -> f64 { let u = ((self.next() >> 11) as f64 + 1.0) / 9007199254740993.0; -mean_s * u.ln() }
}

fn is_conflict(e: &str) -> bool { let m = e.to_lowercase(); m.contains("conflict") || m.contains("busy") || m.contains("locked") || m.contains("snapshot") }

async fn drain(conn: &turso::Connection, sql: &str) -> turso::Result<Vec<String>> {
    let mut rows = conn.query(sql, ()).await?;
    let mut out = vec![];
    while let Some(r) = rows.next().await? { out.push(format!("{:?}", r.get_value(0)?)); }
    Ok(out)
}

// macOS coalesces the timers of idle threads (a sleeping thread wakes up 2 ms late at the median, 10 ms at p99); a real-time time-constraint policy removes it.
#[cfg(target_os = "macos")]
fn set_realtime() {
    #[repr(C)] struct Tb { numer: u32, denom: u32 }
    #[repr(C)] struct Tc { period: u32, computation: u32, constraint: u32, preemptible: i32 }
    extern "C" { fn mach_thread_self() -> u32; fn mach_timebase_info(i: *mut Tb) -> i32; fn thread_policy_set(t: u32, flavor: u32, policy: *mut i32, count: u32) -> i32; }
    unsafe {
        let mut tb = Tb { numer: 0, denom: 0 };
        mach_timebase_info(&mut tb);
        let c = tb.denom as f64 / tb.numer as f64 * 1e6;
        let mut p = Tc { period: (1.0 * c) as u32, computation: (0.1 * c) as u32, constraint: (2.0 * c) as u32, preemptible: 1 };
        thread_policy_set(mach_thread_self(), 2, &mut p as *mut Tc as *mut i32, 4);      // THREAD_TIME_CONSTRAINT_POLICY
    }
}
#[cfg(not(target_os = "macos"))]
fn set_realtime() {}

fn main() {
    let latency = std::env::args().nth(1).as_deref() == Some("latency") && std::env::var("TURSO_RT").as_deref() != Ok("0");
    let mut b = tokio::runtime::Builder::new_multi_thread();
    b.enable_all();
    if latency { b.on_thread_start(set_realtime); }
    b.build().unwrap().block_on(async_main());
}

async fn async_main() {
    let a: Vec<String> = std::env::args().collect();
    let mode = a[1].clone();
    let conns: usize = a[2].parse().unwrap();
    let duration: f64 = a.get(3).map(|s| s.parse().unwrap()).unwrap_or(3.0);
    let tps: f64 = a.get(4).map(|s| s.parse().unwrap()).unwrap_or(1000.0);
    let dir = std::env::temp_dir().join(format!("turso_rs_{}_{}", std::process::id(), conns));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("bench.db");
    let db = Arc::new(Builder::new_local(path.to_str().unwrap()).build().await.unwrap());
    {
        let c = db.connect().unwrap();
        drain(&c, "PRAGMA journal_mode=mvcc").await.unwrap();
        drain(&c, "PRAGMA synchronous=FULL").await.unwrap();
        c.execute("CREATE TABLE bk(id INTEGER PRIMARY KEY, v TEXT)", ()).await.unwrap();
    }
    let open_loop = mode == "latency";
    let warm = Duration::from_secs_f64(1.0);
    let t_start = Instant::now();
    let t_meas = t_start + warm;
    let t_end = t_meas + Duration::from_secs_f64(duration);
    let mut handles = vec![];
    for k in 0..conns {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            let conn = db.connect().unwrap();
            drain(&conn, "PRAGMA synchronous=FULL").await.unwrap();
            let mut rng = Rng(0x9E3779B97F4A7C15u64.wrapping_mul(k as u64 + 1));
            let (mut commits, mut retries, mut max_retries) = (0u64, 0u64, 0u64);
            let mut lat: Vec<f64> = vec![];
            let mut next = Instant::now() + Duration::from_secs_f64(if open_loop { rng.exp(conns as f64 / tps) } else { 0.0 });
            let mut n: u64 = 0;
            loop {
                let sched = if open_loop { next } else { Instant::now() };
                if sched >= t_end { break; }
                if open_loop {
                    if sched > Instant::now() {
                        match std::env::var("TURSO_WAIT").as_deref() {
                            Ok("spin") => { while Instant::now() < sched { std::hint::spin_loop(); } }
                            Ok("block") => { let d = sched.saturating_duration_since(Instant::now()); tokio::task::block_in_place(|| std::thread::sleep(d)); }
                            _ => { tokio::time::sleep_until(tokio::time::Instant::from_std(sched)).await; }
                        }
                    }
                    next = sched + Duration::from_secs_f64(rng.exp(conns as f64 / tps));
                }
                let base = ((k as u64) << 32) + n * 1000;
                let mut sql = String::from("INSERT INTO bk(id,v) VALUES");
                let nrows: u64 = std::env::var("ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(100);
                for i in 0..nrows { if i > 0 { sql.push(','); } sql.push_str(&format!("({},'{}')", base + i, PAYLOAD)); }
                let mut tries = 0u64;
                loop {
                    let r: turso::Result<()> = async {
                        conn.execute("BEGIN CONCURRENT", ()).await?;
                        conn.execute(&sql, ()).await?;
                        conn.execute("COMMIT", ()).await?;
                        Ok(())
                    }.await;
                    match r {
                        Ok(()) => break,
                        Err(e) => {
                            if !is_conflict(&e.to_string()) { panic!("unexpected error: {e}"); }
                            tries += 1;
                            let _ = conn.execute("ROLLBACK", ()).await;
                            tokio::time::sleep(Duration::from_micros(200)).await;
                        }
                    }
                }
                n += 1;
                let end = Instant::now();
                if end >= t_meas && sched >= t_meas - Duration::from_millis(0) {
                    commits += 1; retries += tries; if tries > max_retries { max_retries = tries; }
                    lat.push(end.duration_since(sched).as_secs_f64());
                }
            }
            (commits, retries, max_retries, lat)
        }));
    }
    let mut commits = 0u64; let mut retries = 0u64; let mut maxr = 0u64; let mut lat: Vec<f64> = vec![];
    for h in handles { let (c, r, m, l) = h.await.unwrap(); commits += c; retries += r; if m > maxr { maxr = m; } lat.extend(l); }
    lat.sort_by(|x, y| x.partial_cmp(y).unwrap());
    let pct = |p: f64| if lat.is_empty() { 0.0 } else { lat[((p * lat.len() as f64) as usize).min(lat.len() - 1)] * 1e6 };
    let secs = duration;
    let v = db.connect().unwrap();
    let total_rows: i64 = drain(&v, "SELECT count(*) FROM bk").await.unwrap()[0].trim_start_matches("Integer(").trim_end_matches(')').parse().unwrap_or(-1);
    let ic = drain(&v, "PRAGMA integrity_check").await.unwrap();
    let ic_ok = ic.len() == 1 && ic[0].to_lowercase().contains("ok");
    let valid = ic_ok && total_rows >= (commits as i64);
    println!("JSON {{\"engine\":\"turso-rust\",\"mode\":\"{}\",\"conns\":{},\"tps_target\":{},\"tx_per_s\":{:.1},\"commits\":{},\"retries\":{},\"max_retries\":{},\"p50_us\":{:.1},\"p99_us\":{:.1},\"p999_us\":{:.1},\"max_us\":{:.1},\"rows_in_table\":{},\"integrity_ok\":{},\"valid\":{}}}",
        mode, conns, if open_loop { format!("{}", tps) } else { "null".to_string() }, commits as f64 / secs, commits, retries, maxr, pct(0.5), pct(0.99), pct(0.999), lat.last().copied().unwrap_or(0.0) * 1e6, total_rows, ic_ok, valid);
    let _ = std::fs::remove_dir_all(&dir);
}
