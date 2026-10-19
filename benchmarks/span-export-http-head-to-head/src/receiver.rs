//! Shared persistent HTTP/1.1 receiver; validates complete bytes before ACK.
use crate::Result;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::{Child, ChildStdout, Command, Stdio};
use std::time::Duration;

fn read_u64(input: &mut impl Read) -> std::io::Result<u64> {
    let mut bytes = [0; 8];
    input.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

pub(crate) fn serve() -> Result<()> {
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
            assert!(!compressed, "compression disabled for both exporters");
            let body = wire;
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

pub(crate) struct Server {
    child: Option<Child>,
    output: BufReader<ChildStdout>,
    pub(crate) endpoint: String,
}

impl Server {
    pub(crate) fn start(cycles: usize, delay_us: u64, expected: &[Vec<u8>]) -> Result<Self> {
        let mut child = Command::new(std::env::current_exe()?)
            .arg("serve")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let mut input = child.stdin.take().ok_or("missing child stdin")?;
        for value in [cycles as u64, delay_us, expected.len() as u64] {
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

    pub(crate) fn finish(mut self, expected_requests: usize) -> Result<usize> {
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
