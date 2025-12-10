use std::env;
use std::error::Error;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;

fn main() -> Result<(), Box<dyn Error>> {
    let mut stream = TcpStream::connect("0.0.0.0:8777")?;
    let args: Vec<String> = env::args().collect();
    let command = args[1..].join(" ");

    writeln!(stream, "{}", command)?;

    let mut reader = BufReader::new(stream.try_clone()?);
    let mut response = String::new();
    reader.read_line(&mut response)?;
    let response = response.trim();

    println!("Server: {}", response);

    Ok(())
}
