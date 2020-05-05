use std::cell::RefCell;
use std::fs;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::net::TcpStream;
use std::ptr;

use super::writer::{AfterAdd, MessageWriter};

pub struct Replica {
    #[cfg(feature = "replica_local")]
    pub writer: BufWriter<File>,
    #[cfg(feature = "replica_tcp")]
    pub writer: BufWriter<TcpStream>,
}

impl AfterAdd for Replica {
    fn apply(&mut self, row_index: usize, message: *const u8, length: usize) -> () {
        unsafe {
            let buff = ptr::slice_from_raw_parts(message, length);
            self.writer.write(buff.as_ref().unwrap()).unwrap();
            self.writer.write(b"\n").unwrap();
        }
    }
}

impl Replica {
    #[cfg(feature = "replica_local")]
    pub fn new() -> Box<Replica> {
        let inner = File::create("data/backup.dump").unwrap();
        Box::new(Replica {
            writer: BufWriter::new(inner),
        })
    }

    #[cfg(feature = "replica_tcp")]
    pub fn new() -> Box<Replica> {
        let inner = TcpStream::connect("127.0.0.1:123456").unwrap();
        Box::new(Replica {
            writer: BufWriter::new(inner),
        })
    }
}

pub fn setup(writer: &mut MessageWriter) {
    if cfg!(feature = "replica_local") || cfg!(feature = "replica_tcp") {
        writer.callback_after_add.push(RefCell::new(Replica::new()));
    }
}
