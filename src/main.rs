#![allow(dead_code)]
use std::collections::HashMap;
use std::io::prelude::*;
use std::io::Read;
use std::net::*;
use std::str;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

mod lexer;
mod parser;

use parser::{Command, Parser, Value};

type Key = String;

struct Entry {
    value: Value,
    expiration: Option<usize>,
    subscribers: Option<Vec<Sender<Vec<u8>>>>,
}

struct Database {
    data: HashMap<Key, Entry>,
    next_tx_id: usize,
}

// struct Transaction<'d> {
//     tx_id: usize,
//     db: &'d Database,
//     val: Option<Value>,
// }

// enum Error {
//     NonexistantKey,
//     CreateExistingKey,
// }

impl Database {
    pub fn new() -> Self {
        Database {
            data: HashMap::new(),
            next_tx_id: 0,
        }
    }

    // fn transaction(&mut self, val: Option<Value>) -> Transaction {
    //     let tx_id = self.next_tx_id;
    //     self.next_tx_id += 1;
    //     Transaction {
    //         tx_id,
    //         db: self,
    //         val,
    //     }
    // }

    // pub fn execute(&mut self, command: Command, sender: Option<Sender<Vec<u8>>>) -> Result<Transaction, Error> {
    //     match command {
    //         Command::Disconnect => Ok(self.transaction(None)),
    //         Command::Create(key, val) => Ok(self.transaction(self.create(key, val))),
    //         Command::Delete(key) => Ok(self.transaction(self.delete(&key))),
    //         Command::Read(key) => match self.read(&key) {
    //             Some(val) => Ok(self.transaction(Some(val))),
    //             None => Err(Error::NonexistantKey),
    //         }
    //         Command::Update(key, val) => match db.update(&key, val).map(|v| v.clone()) {
    //             Ok(r) => r,
    //             e => {
    //                 println!("Error writing to stream {:?}", self.stream.peer_addr());
    //                 break;
    //             }
    //         },
    //         Command::Subscribe(key) => {
    //             match db.subscribe(&key, sender) {
    //                 Ok(self.transaction(None))
    //             }

    //         }
    //     }
    //     //Err(Error::NonexistantKey)
    //     }

    pub fn create(&mut self, key: Key, value: Value) -> Option<Value> {
        println!("create {}->{}", key, &value);
        self.data
            .insert(
                key,
                Entry {
                    value,
                    expiration: None,
                    subscribers: None,
                },
            ).map(|e| e.value)
    }

    pub fn read(&self, key: &str) -> Option<&Value> {
        println!("read {}", key,);
        self.data.get(key).map(|e| &e.value)
    }

    pub fn update(
        &mut self,
        key: &str,
        value: Value,
    ) -> Result<Option<Value>, Box<std::error::Error>> {
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

    pub fn delete(&mut self, key: &str) -> Option<Value> {
        self.data.remove(key).map(|e| e.value)
    }

    pub fn subscribe(&mut self, key: &str, sender: Sender<Vec<u8>>) -> usize {
        let mut nsub = 0;
        if self.data.contains_key(key) {
            if let Some(exist) = self.data.get_mut(key) {
                let response = format!("update {}->{}\r\n\r\n", key, &exist.value);
                let _ = sender.send(Vec::from(response.as_bytes()));
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
        println!("Client {:?} connected", self.stream.peer_addr());

        match self.stream.write(b"connect to kv\r\n") {
            Ok(0) | Err(_) => {
                println!("Error writing to stream {:?}", self.stream.peer_addr());
                return;
            }
            Ok(_) => (),
        }

        let (tx, rx) = channel::<Vec<u8>>();
        let mut stream = self
            .stream
            .try_clone()
            .expect("Error cloning client stream");

        // Spawn the writing stream
        thread::spawn(move || {
            while let Ok(message) = rx.recv() {
                stream
                    .write_all(&message[..])
                    .expect("Error writing to stream");
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

                match Parser::from(&buffer[0..read_bytes]) {
                    Err(e) => {
                        println!("Error constructing parser {:?}", e);
                    }
                    Ok(mut parser) => match parser.parse() {
                        Ok(commands) => {
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
                                    Command::Create(key, val) => db.create(key, val),
                                    Command::Delete(key) => db.delete(&key),
                                    Command::Read(key) => db.read(&key).cloned(),
                                    Command::Update(key, val) => {
                                        match db.update(&key, val).map(|v| v.clone()) {
                                            Ok(r) => r,
                                            _ => {
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

                                if let Some(mut r) = response {
                                    if tx.send(Vec::from(r.encode().as_bytes())).is_err() {
                                        println!(
                                            "Error writing to stream {:?}",
                                            self.stream.peer_addr()
                                        );
                                        break;
                                    }
                                };
                            }
                            drop(db);
                        }
                        Err(e) => println!("Parser error {:?}", e),
                    },
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
