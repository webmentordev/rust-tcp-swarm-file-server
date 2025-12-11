use rusqlite::{Connection, Result as DBResult};
use std::error::Error;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::{env, thread};

struct Server {
    master_key: String,
    listener: TcpListener,
    database: Arc<Mutex<Connection>>,
}

struct Config {
    master_ip_address: String,
    master_port: u32,
    slave_port: u32,
}

impl Server {
    fn connect() -> Result<Self, Box<dyn Error>> {
        match Self::verify_config() {
            Some(config) => Self::connect_slave(config),
            None => Self::connect_master(),
        }
    }

    fn connect_master() -> Result<Self, Box<dyn Error>> {
        let listener = match TcpListener::bind("127.0.0.1:8777") {
            Ok(listener) => listener,
            Err(_) => TcpListener::bind("127.0.0.1:0").expect("Can't connect to any port."),
        };
        let database = Arc::new(Mutex::new(Self::build_db()?));
        let master_key =
            std::env::var("MASTER_KEY").expect("MASTER_KEY environment variable not set");
        let info = listener.local_addr()?;
        println!(
            "👑 Master Listening at: http://{}:{}",
            info.ip(),
            info.port()
        );
        Ok(Server {
            listener,
            database,
            master_key,
        })
    }

    fn connect_slave(config: Config) -> Result<Self, Box<dyn Error>> {
        let listener = match TcpListener::bind(format!("127.0.0.1:{}", config.slave_port)) {
            Ok(listener) => listener,
            Err(_) => TcpListener::bind("127.0.0.1:0").expect("Can't connect to any port."),
        };
        let database = Arc::new(Mutex::new(Self::build_db()?));
        let master_key =
            std::env::var("MASTER_KEY").expect("MASTER_KEY environment variable not set");
        let info = listener.local_addr()?;
        println!(
            "🧑‍🌾 Listening as slave at: http://{}:{}",
            info.ip(),
            info.port()
        );
        Ok(Server {
            listener,
            database,
            master_key,
        })
    }

    fn join(ip_addr: &str) -> Result<(), Box<dyn Error>> {
        match Self::verify_config() {
            Some(_) => {
                println!("You are already part of a swarm. Type --help for more.")
            }
            None => {
                let mut stream = TcpStream::connect(ip_addr)?;
                let socket = stream.local_addr()?;
                let master_key =
                    std::env::var("MASTER_KEY").expect("MASTER_KEY environment variable not set");

                let command = format!("JOIN {}", master_key);
                writeln!(stream, "{}", command)?;

                let mut response = String::new();
                let mut reader = BufReader::new(stream.try_clone()?);
                reader.read_line(&mut response)?;
                let response = response.trim();
                println!("Server: {}", response);

                if response.contains("joined") {
                    let parts: Vec<&str> = ip_addr.split(":").collect();
                    let master_ip = parts[0].to_string();
                    let master_port = parts[1].to_string();
                    Self::create_config(master_ip, master_port, socket.port().to_string())?;
                }
            }
        }
        Ok(())
    }

    fn leave() -> Result<(), Box<dyn Error>> {
        match Self::verify_config() {
            Some(config) => {
                let mut stream = TcpStream::connect(format!(
                    "{}:{}",
                    config.master_ip_address, config.master_port
                ))?;
                writeln!(stream, "LEAVE")?;
                let mut response = String::new();
                let mut reader = BufReader::new(stream.try_clone()?);
                reader.read_line(&mut response)?;
                let response = response.trim();
                println!("Server: {}", response);

                if response.contains("left") {
                    fs::remove_file("config.txt")?;
                    println!("Config file has been deleted!")
                }
            }
            None => {
                eprintln!("You are not a part of any swarm! no action required.");
            }
        }
        Ok(())
    }

    fn list() -> Result<(), Box<dyn Error>> {
        match Self::verify_config() {
            Some(config) => {
                let mut stream = TcpStream::connect(format!(
                    "{}:{}",
                    config.master_ip_address, config.master_port
                ))?;
                writeln!(stream, "LIST")?;
                let mut response = String::new();
                let mut reader = BufReader::new(stream.try_clone()?);
                reader.read_line(&mut response)?;
                println!("Server: {}", response.trim());

                let mut files: Vec<String> = Vec::new();
                loop {
                    response.clear();
                    let bytes_read = reader.read_line(&mut response)?;
                    if bytes_read == 0 {
                        break;
                    }
                    let line = response.trim();
                    if line.contains("done") {
                        break;
                    }
                    if line.is_empty() {
                        continue;
                    }
                    files.push(line.to_string());
                }

                for filename in files {
                    println!("📄 {filename}");
                }
            }
            None => {
                eprintln!(
                    "You can not fetch files from any server becasue you are not a part of a swarm."
                );
            }
        }
        Ok(())
    }

    fn create_config(
        ip: String,
        master_port: String,
        slave_port: String,
    ) -> Result<(), Box<dyn Error>> {
        let filename = "config.txt";
        if !Path::new(filename).exists() {
            let data = format!(
                "master_ip_address={}\nmaster_port={}\nslave_port={}",
                ip, master_port, slave_port
            );
            fs::write(filename, data)?;
            println!("Config file has been created!");
        } else {
            println!("Config file already exist!");
        }
        Ok(())
    }

    fn verify_config() -> Option<Config> {
        let filename = "config.txt";
        if Path::new(filename).exists() {
            let content =
                fs::read_to_string(filename).expect("Config file exist but no read permissions.");
            let mut master_ip_address = String::new();
            let mut master_port: u32 = 8777;
            let mut slave_port: u32 = 8777;
            for line in content.lines() {
                if let Some((key, value)) = line.split_once('=') {
                    match key {
                        "master_ip_address" => master_ip_address = value.to_string(),
                        "master_port" => {
                            if let Ok(port) = value.parse::<u32>() {
                                master_port = port;
                            }
                        }
                        "slave_port" => {
                            if let Ok(port) = value.parse::<u32>() {
                                slave_port = port;
                            }
                        }
                        _ => {}
                    }
                }
            }
            return Some(Config {
                master_ip_address,
                master_port,
                slave_port,
            });
        }
        return None;
    }

    fn build_db() -> DBResult<Connection> {
        let conn = Connection::open("master_node.db")?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS servers (
                    id INTEGER PRIMARY KEY,
                    ip_address VARCHAR NOT NULL,
                    port VARCHAR NOT NULL,
                    is_active BOOLEAN DEFAULT 1,
                    has_left BOOLEAN DEFAULT 0
                )",
            [],
        )?;
        Ok(conn)
    }

    fn handle_connection(
        mut stream: TcpStream,
        master_key: String,
        address: SocketAddr,
        db: Arc<Mutex<Connection>>,
    ) -> Result<(), Box<dyn Error>> {
        let mut reader = BufReader::new(stream.try_clone()?);
        let mut command = String::new();
        reader.read_line(&mut command)?;
        let command = command.trim();
        println!("{}", command);

        let commands: Vec<&str> = command.split(" ").collect();

        match commands[0] {
            "JOIN" => {
                if master_key == commands[1].to_string() {
                    let db = db.lock().unwrap();
                    let mut stmt = db.prepare(
                        "SELECT COUNT(*) FROM servers WHERE ip_address = ?1 AND port = ?2",
                    )?;
                    let exists: i64 = stmt.query_row(
                        [address.ip().to_string(), address.port().to_string()],
                        |row| row.get(0),
                    )?;
                    if exists > 0 {
                        writeln!(stream, "Server already exists!")?;
                    } else {
                        db.execute(
                            "INSERT INTO servers (ip_address, port) VALUES (?1, ?2)",
                            [address.ip().to_string(), address.port().to_string()],
                        )?;
                        writeln!(stream, "Swam has been joined!")?;
                    }
                } else {
                    writeln!(stream, "Authentication failed!")?;
                }
            }
            "LEAVE" => {
                let db = db.lock().unwrap();
                let mut stmt =
                    db.prepare("SELECT has_left FROM servers WHERE ip_address = ?1 AND port= ?2")?;
                let result = stmt.query_row(
                    [address.ip().to_string(), address.port().to_string()],
                    |row| row.get::<_, bool>(0),
                );
                match result {
                    Ok(has_left) => {
                        if has_left {
                            writeln!(stream, "You have already left!")?;
                        } else {
                            db.execute(
                                "UPDATE servers SET has_left = true WHERE ip_address = ?1 AND port= ?2",
                                [address.ip().to_string(), address.port().to_string()],
                            )?;
                            writeln!(stream, "Swam has been left!")?;
                        }
                    }
                    Err(_) => {
                        db.execute(
                            "INSERT INTO servers (ip_address, has_left) VALUES (?1, ?2)",
                            rusqlite::params![address.ip().to_string(), false],
                        )?;
                        writeln!(stream, "Swam has been joined!")?;
                    }
                }
            }

            "LIST" => {
                writeln!(stream, "Collecting data from servers...")?;
                let db = db.lock().unwrap();
                let mut stmt =
                    db.prepare("SELECT ip_address, port FROM servers WHERE has_left = 0")?;
                let servers = stmt.query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?;
                let mut active_servers: Vec<(String, String)> = Vec::new();
                for server in servers {
                    match server {
                        Ok((ip, port)) => {
                            active_servers.push((ip, port));
                        }
                        Err(e) => {
                            eprintln!("Error reading server: {}", e);
                        }
                    }
                }
                writeln!(stream, "Active servers: {}", active_servers.len())?;
                for (ip, port) in &active_servers {
                    println!("{ip}:{port}");
                    // writeln!(stream, "  {} : {}", ip, port)?;
                }
            }
            _ => {
                writeln!(stream, "Unknown command!")?;
            }
        }
        Ok(())
    }

    fn run(&self) -> Result<(), Box<dyn Error>> {
        loop {
            match self.listener.accept() {
                Ok((stream, address)) => {
                    let master_key = self.master_key.clone();
                    let database = self.database.clone();
                    thread::spawn(move || {
                        if let Err(e) =
                            Self::handle_connection(stream, master_key, address, database)
                        {
                            eprintln!("Error handling connection: {}", e);
                        };
                    });
                }
                Err(e) => {
                    eprintln!("Connection error: {}", e);
                }
            }
        }
    }
}

// Main Functions
fn main() -> Result<(), Box<dyn Error>> {
    dotenv::dotenv().ok();
    let args: Vec<String> = env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("join") => {
            if args.len() != 3 {
                println!("Not enough arguments!");
                print_usage();
            } else {
                join_handler(&args[2])?;
            }
        }
        Some("list") => {
            list_handler()?;
        }
        Some("leave") => {
            leave_handler()?;
        }
        Some("serve") => {
            run_server()?;
        }
        Some("help") | Some("--help") => {
            print_usage();
        }
        _ => print_usage(),
    }
    Ok(())
}

fn run_server() -> Result<(), Box<dyn Error>> {
    let server = Server::connect()?;
    server.run()?;
    Ok(())
}

fn join_handler(ip_addr: &str) -> Result<(), Box<dyn Error>> {
    Server::join(ip_addr)?;
    Ok(())
}

fn list_handler() -> Result<(), Box<dyn Error>> {
    Server::list()?;
    Ok(())
}

fn leave_handler() -> Result<(), Box<dyn Error>> {
    Server::leave()?;
    Ok(())
}

fn print_usage() {
    println!("\n||=======================================================================");
    println!("|| Usage:");
    println!("||=======================================================================");
    println!("||  * serve                                  - Start the server");
    println!("||  * join <ip_address:port> <master_key>    - Join the running network");
    println!("||  * leave                                  - Leave the network");
    println!("||  * list                                   - List all active servers");
    println!("||  * status <ip_address>                    - Check server status");
    println!("||  * help                                   - Show this message");
    println!("=========================================================================");
}
