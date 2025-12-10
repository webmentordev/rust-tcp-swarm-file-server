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
                    Self::create_config(socket.ip().to_string(), socket.port().to_string())?;
                }
            }
        }
        Ok(())
    }

    fn create_config(ip: String, port: String) -> Result<(), Box<dyn Error>> {
        let filename = "config.txt";
        if !Path::new(filename).exists() {
            let data = format!("master_ip_address={}\nslave_port={}", ip, port);
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
            let mut slave_port: u32 = 8777;
            for line in content.lines() {
                if let Some((key, value)) = line.split_once('=') {
                    match key {
                        "master_ip_address" => master_ip_address = value.to_string(),
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
                slave_port,
            });
        }
        return None;
    }

    // fn config() -> Result<Config, Box<dyn Error>> {
    //     let filename = "config.txt";
    //     if !Path::new(filename).exists() {
    //         let data = "is_in_swarm=true\nip_address=192.167.1.88999\nlistening=8777";
    //         fs::write(filename, data)?;
    //         println!("File created with default data");
    //     }
    //     let content = fs::read_to_string(filename)?;
    //     let mut is_in_swarm = String::new();
    //     let mut ip_address = String::new();
    //     let mut join_port: u32 = 8777;
    //     for line in content.lines() {
    //         if let Some((key, value)) = line.split_once('=') {
    //             match key {
    //                 "is_in_swarm" => is_in_swarm = value.to_string(),
    //                 "is_master" => {
    //                     is_master = {
    //                         if let Ok(master) = value.parse::<bool>() {
    //                             is_master = master;
    //                         }
    //                     }
    //                 }
    //                 "is_slave" => {
    //                     is_slave = {
    //                         if let Ok(slave) = value.parse::<bool>() {
    //                             is_slave = slave;
    //                         }
    //                     }
    //                 }
    //                 "ip_address" => ip_address = value.to_string(),
    //                 "join_port" => {
    //                     if let Ok(port) = value.parse::<u32>() {
    //                         join_port = port;
    //                     }
    //                 }
    //                 _ => {}
    //             }
    //         }
    //     }
    //     Ok(Config {
    //         is_in_swarm,
    //         is_master,
    //         is_slave,
    //         ip_address,
    //         join_port,
    //     })
    // }

    fn build_db() -> DBResult<Connection> {
        let conn = Connection::open("master_node.db")?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS servers (
                    id INTEGER PRIMARY KEY,
                    ip_address VARCHAR NOT NULL,
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
                    let mut stmt =
                        db.prepare("SELECT COUNT(*) FROM servers WHERE ip_address = ?1")?;
                    let exists: i64 = stmt.query_row([address.to_string()], |row| row.get(0))?;

                    if exists > 0 {
                        writeln!(stream, "Server already exists!")?;
                    } else {
                        db.execute(
                            "INSERT INTO servers (ip_address) VALUES (?1)",
                            [address.to_string()],
                        )?;
                        writeln!(stream, "Swam has been joined!")?;
                    }
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

// fn config() -> Option<Config> {
//     let filename = "config.txt";
//     if !Path::new(filename).exists() {
//         return None;
//     }
//     let content = fs::read_to_string(filename)?;
//     let mut master_ip_address = String::new();
//     let mut slave_port: u32 = 8777;
//     for line in content.lines() {
//         if let Some((key, value)) = line.split_once('=') {
//             match key {
//                 "master_ip_address" => master_ip_address = value.to_string(),
//                 "slave_port" => {
//                     if let Ok(port) = value.parse::<u32>() {
//                         slave_port = port;
//                     }
//                 }
//                 _ => {}
//             }
//         }
//     }
//     Some(Config {
//         master_ip_address,
//         slave_port
//     })
// }
