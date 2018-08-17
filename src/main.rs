use std::collections::HashMap;
use std::io::prelude::*;
use std::io::Read;
use std::net::*;
use std::str;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;

mod lexer;
mod parser;

use parser::{Command, Command::*, Parser, Value, Value::*};

type Key = String;

struct Entry {
    value: Value,
    expiration: Option<usize>,
    subscribers: Option<Vec<Sender<Vec<u8>>>>,
}

struct Database {
    data: HashMap<Key, Entry>,
}

impl Database {
    pub fn new() -> Self {
        Database {
            data: HashMap::new(),
        }
    }

    pub fn create(&mut self, key: Key, value: Value) -> Option<Value> {
        println!("create {}->{}", key, &value);
        self.data
            .insert(
                key,
                Entry {
                    value: value,
                    expiration: None,
                    subscribers: None,
                },
            ).map(|e| e.value)
    }

    pub fn read(&self, key: &Key) -> Option<&Value> {
        println!("read {}", key,);
        self.data.get(key).map(|e| &e.value)
    }

    pub fn update(
        &mut self,
        key: &Key,
        value: Value,
    ) -> Result<Option<Value>, Box<std::error::Error>> {
        // println!(
        //     "update {:?}->{:?}",
        //     str::from_utf8(&key)?,
        //     str::from_utf8(&value)?
        // );
        if self.data.contains_key(key) {
            if let Some(exist) = self.data.get_mut(key) {
                if let Some(ref subscribers) = exist.subscribers {
                    for sub in subscribers.iter() {
                        let response = format!("update {}->{}\r\n\r\n", key, value);
                        sub.send(Vec::from(response.as_bytes()))?;
                    }
                }
                Ok(Some(std::mem::replace(&mut exist.value, value)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn delete(&mut self, key: &Key) -> Option<Value> {
        self.data.remove(key).map(|e| e.value)
    }

    pub fn subscribe(&mut self, key: &Key, sender: Sender<Vec<u8>>) -> usize {
        let mut nsub = 0;
        if self.data.contains_key(key) {
            if let Some(exist) = self.data.get_mut(key) {
                let response = format!("update {}->{}\r\n\r\n", key, &exist.value);
                sender.send(Vec::from(response.as_bytes()));
                if let Some(ref mut subs) = exist.subscribers {
                    subs.push(sender);
                    nsub = subs.len();
                } else {
                    exist.subscribers = Some(vec![sender]);
                }
            }
        }
        nsub
    }
}

struct Client {
    stream: TcpStream,
    db: Arc<Mutex<Database>>,
}

impl Client {
    pub fn spawn(stream: TcpStream, db: Arc<Mutex<Database>>) -> Self {
        Client { stream, db }
    }

    pub fn run(mut self) {
        println!("Client {} connected", self.stream.peer_addr().unwrap());

        self.stream.write(b"connect to kv\r\n").unwrap();
        self.stream.set_read_timeout(None).unwrap();

        let (tx, rx) = channel::<Vec<u8>>();
        let mut stream = self.stream.try_clone().unwrap();

        // Spawn the writing stream
        thread::spawn(move || {
            loop {
                if let Ok(message) = rx.recv() {
                    stream.write(&message[..]);
                } else {
                    break;
                }
            }
            println!("Closing sender");
        });

        // Spawn the reading stream
        thread::spawn(move || {
            let mut buffer = [0u8; 1024];
            'outer: loop {
                let read_bytes = match self.stream.read(&mut buffer) {
                    Ok(r) => r,
                    Err(_) => {
                        println!("Error reading from stream {:?}", self.stream.peer_addr());
                        break;
                    }
                };

                if read_bytes == 0 {
                    break 'outer;
                }

                //println!("read {} bytes", read_bytes);

                if let Some(mut parser) = Parser::from(&buffer[0..read_bytes]) {
                    if let Some(commands) = parser.parse() {
                        let mut db = match self.db.lock() {
                            Ok(db) => db,
                            Err(_) => {
                                println!(
                                    "Poisoned lock on thread connected to {:?}",
                                    self.stream.peer_addr()
                                );
                                break;
                            }
                        };
                        for cmd in commands {
                            let mut response: Option<Value> = match cmd {
                                Command::Disconnect => {
                                    println!(
                                        "Client {} requesting disconnect",
                                        self.stream.peer_addr().unwrap()
                                    );
                                    self.stream.shutdown(Shutdown::Both).unwrap();
                                    break 'outer;
                                }
                                Command::Create(key, val) => db.create(key, val).map(|v| v.clone()),
                                Command::Delete(key) => db.delete(&key).map(|v| v.clone()),
                                Command::Read(key) => db.read(&key).map(|v| v.clone()),
                                Command::Update(key, val) => {
                                    match db.update(&key, val).map(|v| v.clone()) {
                                        Ok(r) => r,
                                        e => {
                                            println!(
                                                "Error writing to stream {:?}",
                                                self.stream.peer_addr()
                                            );
                                            break;
                                        }
                                    }
                                }
                                Command::Subscribe(key) => {
                                    db.subscribe(&key, tx.clone());
                                    None
                                }
                            };

                            let result = if let Some(mut r) = response {
                                if let Err(_) = tx.send(Vec::from(format!("{}", r).as_bytes())) {
                                    println!(
                                        "Error writing to stream {:?}",
                                        self.stream.peer_addr()
                                    );
                                    break;
                                }
                            };
                        }
                        drop(db);
                    } else {
                        // if let Err(_) = tx.send(b"Error parsing") {
                        //             println!("Error writing to stream {:#?}", self.stream.peer_addr());
                        //             break;
                        //         }
                    }
                };
            }
            println!("Dropped connection to {:?}", self.stream.peer_addr());
        });
    }
}

struct Server {
    db: Arc<Mutex<Database>>,
    listener: TcpListener,
}

impl Server {
    pub fn listen<A: ToSocketAddrs>(addr: A) -> Result<(), std::io::Error> {
        let server = Server {
            db: Arc::new(Mutex::new(Database::new())),
            listener: TcpListener::bind(addr)?,
        };
        for stream in server.listener.incoming() {
            match stream {
                Ok(stream) => Client::spawn(stream, server.db.clone()).run(),
                Err(e) => {
                    println!("Error connecting to stream {:?}", e);
                }
            }
        }
        Ok(())
    }
}

fn main() {
    println!("kv listening on 1122");
    Server::listen("0.0.0.0:1122").unwrap();
}
