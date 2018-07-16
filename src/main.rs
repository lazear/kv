use std::io;
use std::io::prelude::*;
use std::net::*;
use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::mpsc::{Sender, Receiver, channel};
use std::thread;
use std::collections::HashMap;

type Key = Vec<u8>;
type Value = Vec<u8>;

struct Entry {
    value: Value,
    expiration: Option<usize>,
    subscribers: Option<Vec<Sender<Value>>>,
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
        println!("create {:?}->{:?}", String::from_utf8_lossy(&key), String::from_utf8_lossy(&value) );
        self.data.insert(key, Entry { value: value, expiration: None, subscribers: None}).map(|e| e.value)
    }

    pub fn read(&self, key: &Key) -> Option<&Value> {
        println!("read {:?}", String::from_utf8_lossy(key));
        self.data.get(key).map(|e| &e.value)
    }

    pub fn update(&mut self, key: &Key, value: Value) -> Option<Value> {
        if self.data.contains_key(key) {
            if let Some(exist) = self.data.get_mut(key) {
                if let Some(ref subscribers) = exist.subscribers {
                    for sub in subscribers.iter() {
                        sub.send(value.clone());
                    }
                }
                Some(std::mem::replace(&mut exist.value, value))
            } else {
                None
            }            
        } else {
            None
        }
    }

    pub fn delete(&mut self, key: &Key) -> Option<Value> {
        self.data.remove(key).map(|e| e.value)
    }

    pub fn subscribe(&mut self, key: &Key, sender: Sender<Value>) -> usize {
        if self.data.contains_key(key) {
            if let Some(exist) = self.data.get_mut(key) {
                if let Some(ref mut subs) = exist.subscribers {
                    subs.push(sender);
                } else {
                    exist.subscribers = Some(vec![sender]);
                }
            }
        }
        0
    }
}

enum Command {
    Create(Key, Value),
    Read(Key),
    Update(Key, Value),
    Delete(Key),
}

fn parse(input: &[u8]) -> Option<Command> {
    let s = String::from_utf8_lossy(input);
    let mut sp = s.split(' ');
    let cmd = sp.next()?;
    match cmd {
        "create" => Some(Command::Create(sp.next()?.as_bytes().into(), sp.next()?.as_bytes().into())),
        "read" => Some(Command::Read(sp.next()?.as_bytes().into())),
        "update" => Some(Command::Update(sp.next()?.as_bytes().into(), sp.next()?.as_bytes().into())),
        "delete" => Some(Command::Delete(sp.next()?.as_bytes().into())),
        _ => None
    }
}

struct Client {
    stream: TcpStream,
    db: Arc<Mutex<Database>>,
}

impl Client {
    pub fn run(&mut self) {
        let mut buffer: Vec<u8> = Vec::new();
        
        self.stream.write(b"connect to mq\r\n").unwrap();
        loop {
            buffer.extend(std::iter::repeat(0).take(16));
            match self.stream.read(&mut buffer[0..]) {
                Ok(r) => r,
                Err(e) => {
                    println!("error reading from stream {:#?}", self.stream.peer_addr());
                    break;
                }
            };

            let mut db = match self.db.lock() {
                Ok(db) => db,
                Err(_) => panic!("lock"),
            };

            match buffer[0] as char {
                'c' => {
                    let key = parse(&buffer[1..5]);
                    let val = parse(&buffer[5..]);
                    let output = match db.create(key, val) {
                        Some(exist) => self.stream.write(&exist).unwrap(),
                        None => self.stream.write(b"new key\r\n").unwrap(),
                    };
                },
                'r' => {
                    match db.read(&parse(&buffer[1..])) {
                        Some(val) => self.stream.write(&val).unwrap(),
                        None => self.stream.write(b"no value\r\n").unwrap(),
                    };
                },
                'u' => {

                },
                'd' => {

                },
                _ => {
                    self.stream.write(b"unrecognized command\r\n").unwrap();
                }
            }

            drop(db);
            buffer.clear();

        }
        println!("dropped connection");
    }
}

struct Server {
    db: Arc<Mutex<Database>>,

}

// fn handle(mut stream: TcpStream, db: Arc<Mutex<Database>>) -> io::Result<()> {
//     Client::run
//     Ok(())
// }

fn main() {

    let listener = TcpListener::bind("0.0.0.0:1122").unwrap();

    let db = Arc::new(Mutex::new(Database::new()));
    for stream in listener.incoming() {
        let c = db.clone();
        thread::spawn(move || Client { stream: stream.unwrap(), db: c}.run());
    }


    println!("Hello, world!");
}
