//! Production HTTP source, with a persistent receiver in a separate process.
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::{Child, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};

use fast_telemetry::CompletedSpan;
use fast_telemetry::otlp::{
    build_resource, build_trace_export_request, encode_native_trace_export_request_chunks,
};
use flate2::read::GzDecoder;
use prost::Message;

#[allow(dead_code)]
#[path = "main.rs"]
mod capture;
#[allow(dead_code)]
mod transport;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn cpu_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: initialized writable timespec; clock_gettime retains no pointer.
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) },
        0
    );
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

fn read_u64(input: &mut impl Read) -> std::io::Result<u64> {
    let mut bytes = [0; 8];
    input.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn serve() -> Result<()> {
    if let Ok(cpu) = std::env::var("EXPORT_PROBE_RECEIVER_CPU") {
        let result = Command::new("taskset")
            .args(["-pc", &cpu, &std::process::id().to_string()])
            .output()?;
        if !result.status.success() {
            return Err("receiver affinity failed".into());
        }
    }
    let mut input = std::io::stdin().lock();
    let cycles = read_u64(&mut input)?;
    let delay_us = read_u64(&mut input)?;
    let gzip = read_u64(&mut input)? != 0;
    let count = read_u64(&mut input)? as usize;
    let mut expected = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_u64(&mut input)? as usize;
        let mut body = vec![0; len];
        input.read_exact(&mut body)?;
        expected.push(body);
    }
    drop(input);
    let listener = TcpListener::bind("127.0.0.1:0")?;
    println!("http://{}", listener.local_addr()?);
    std::io::stdout().flush()?;
    let (mut socket, _) = listener.accept()?;
    socket.set_nodelay(true)?;
    socket.set_read_timeout(Some(Duration::from_secs(10)))?;
    let mut input = BufReader::new(socket.try_clone()?);
    let mut wire_bytes = 0;
    for cycle in 0..cycles {
        for golden in &expected {
            let mut line = String::new();
            input.read_line(&mut line)?;
            assert!(line.starts_with("POST /v1/traces HTTP/1.1"), "{line:?}");
            let mut length = None;
            let mut compressed = false;
            loop {
                line.clear();
                input.read_line(&mut line)?;
                if line == "\r\n" {
                    break;
                }
                let (key, value) = line.split_once(':').ok_or("invalid header")?;
                if key.eq_ignore_ascii_case("content-length") {
                    length = Some(value.trim().parse::<usize>()?);
                }
                if key.eq_ignore_ascii_case("content-encoding") {
                    assert_eq!(value.trim(), "gzip");
                    compressed = true;
                }
            }
            let length = length.ok_or("missing content length")?;
            assert!(length <= 16 * 1024 * 1024);
            let mut wire = vec![0; length];
            input.read_exact(&mut wire)?;
            assert_eq!(compressed, gzip);
            let body = if compressed {
                let mut body = Vec::new();
                GzDecoder::new(wire.as_slice()).read_to_end(&mut body)?;
                body
            } else {
                wire
            };
            assert_eq!(
                &body, golden,
                "collector checks complete protobuf bytes before acknowledging"
            );
            if cycle >= 8 {
                wire_bytes += length;
            }
            if delay_us > 0 {
                std::thread::sleep(Duration::from_micros(delay_us));
            }
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/x-protobuf\r\nContent-Length: 0\r\n\r\n")?;
        }
    }
    println!("verified,{},{wire_bytes}", cycles as usize * count);
    Ok(())
}

struct Server {
    child: Option<Child>,
    output: BufReader<ChildStdout>,
    endpoint: String,
}

impl Server {
    fn start(cycles: usize, delay_us: u64, gzip: bool, expected: &[Vec<u8>]) -> Result<Self> {
        let mut child = Command::new(std::env::current_exe()?)
            .arg("serve")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let mut input = child.stdin.take().ok_or("missing child stdin")?;
        for value in [
            cycles as u64,
            delay_us,
            u64::from(gzip),
            expected.len() as u64,
        ] {
            input.write_all(&value.to_le_bytes())?;
        }
        for body in expected {
            input.write_all(&(body.len() as u64).to_le_bytes())?;
            input.write_all(body)?;
        }
        drop(input);
        let mut output = BufReader::new(child.stdout.take().ok_or("missing child stdout")?);
        let mut endpoint = String::new();
        output.read_line(&mut endpoint)?;
        if !endpoint.starts_with("http://127.0.0.1:") {
            return Err("receiver did not start".into());
        }
        Ok(Self {
            child: Some(child),
            output,
            endpoint: endpoint.trim().into(),
        })
    }

    fn finish(mut self, expected_requests: usize) -> Result<usize> {
        let mut line = String::new();
        self.output.read_line(&mut line)?;
        let parts = line.trim().split(',').collect::<Vec<_>>();
        if parts.len() != 3
            || parts[0] != "verified"
            || parts[1].parse::<usize>()? != expected_requests
        {
            return Err("receiver count mismatch".into());
        }
        let bytes = parts[2].parse()?;
        if !self
            .child
            .take()
            .ok_or("missing receiver")?
            .wait()?
            .success()
        {
            return Err("receiver failed".into());
        }
        Ok(bytes)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

async fn run() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 6 {
        return Err("usage: static|attributes SPANS CAP|all CYCLES plain|gzip ACK_DELAY_US".into());
    }
    let workload = &args[0];
    let count: usize = args[1].parse()?;
    let cap = if args[2] == "all" {
        usize::MAX
    } else {
        args[2].parse::<usize>()?
    };
    let cycles: usize = args[3].parse()?;
    let compression = &args[4];
    let delay_us: u64 = args[5].parse()?;
    let caps = [usize::MAX, 512, 2048];
    if !matches!(workload.as_str(), "static" | "attributes")
        || count == 0
        || cycles == 0
        || !caps.contains(&cap)
        || !matches!(compression.as_str(), "plain" | "gzip")
        || count.checked_mul(cycles).is_none()
        || cycles.checked_add(8).is_none()
    {
        return Err("invalid HTTP probe options".into());
    }
    let batch = capture::fixture(workload, count);
    let resource = build_resource("chunk-probe", &[("version", "1")]);
    let golden_sets = caps
        .iter()
        .map(|limit| {
            let mut offset = 0;
            let mut bodies = Vec::new();
            while offset < count {
                let n = (*limit).min(count - offset);
                bodies.push(
                    build_trace_export_request(
                        &resource,
                        "scope",
                        batch
                            .iter()
                            .skip(offset)
                            .take(n)
                            .map(CompletedSpan::to_otlp)
                            .collect(),
                    )
                    .encode_to_vec(),
                );
                offset += n;
            }
            bodies
        })
        .collect::<Vec<_>>();
    let expected = &golden_sets[caps.iter().position(|&limit| limit == cap).expect("cap")];
    let server = Server::start(cycles + 8, delay_us, compression == "gzip", expected)?;
    let client = transport::OtlpHttpClient::new(
        transport::OtlpHttpConfig::new(&server.endpoint).with_gzip_threshold(
            if compression == "gzip" {
                1024
            } else {
                usize::MAX
            },
        ),
    )?;
    let mut requests = Vec::new();
    let (mut encoding, mut transport_cpu, mut wall, mut verified) = (0, 0, 0, 0);
    for cycle in 0..cycles + 8 {
        let wall_start = Instant::now();
        let start = cpu_ns();
        requests.reserve(count.div_ceil(cap));
        requests.extend(encode_native_trace_export_request_chunks(
            &resource, "scope", &batch, cap,
        ));
        let encode_ns = cpu_ns() - start;
        let start = cpu_ns();
        let mut accepted = 0;
        for request in requests.drain(..) {
            let outcome = client.export_native_traces(request).await?;
            accepted += outcome.accepted;
            assert_eq!(outcome.rejected, 0);
        }
        let transport_ns = cpu_ns() - start;
        let elapsed = wall_start.elapsed().as_nanos();
        assert_eq!(accepted, count as u64);
        if cycle >= 8 {
            encoding += encode_ns;
            transport_cpu += transport_ns;
            wall += elapsed;
            verified += count;
        }
    }
    let wire = server.finish((cycles + 8) * expected.len())?;
    let protobuf_bytes: usize = expected.iter().map(Vec::len).sum();
    println!(
        "workload,batch,cap,cycles,compression,ack_delay_us,verified_spans,requests_per_cycle,protobuf_bytes_per_cycle,wire_bytes,client_cpu_ns_per_span,encode_cpu_ns_per_span,transport_cpu_ns_per_span,wall_ns_per_span"
    );
    println!(
        "{workload},{count},{cap},{cycles},{compression},{delay_us},{verified},{},{protobuf_bytes},{wire},{:.3},{:.3},{:.3},{:.3}",
        expected.len(),
        (encoding + transport_cpu) as f64 / verified as f64,
        encoding as f64 / verified as f64,
        transport_cpu as f64 / verified as f64,
        wall as f64 / verified as f64
    );
    Ok(())
}

fn main() -> Result<()> {
    if std::env::args().nth(1).as_deref() == Some("serve") {
        return serve();
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run())
}
