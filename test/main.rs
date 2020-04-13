use std::sync::{Mutex, Arc};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

fn main() {
    let c = Arc::new(Mutex::new(0 as u8));
    let (tx, rx) = mpsc::channel();

    let counter = Arc::clone(&c);
    let handle = thread::spawn(move || {
        let vals = vec![
            String::from("hi"),
            String::from("from"),
            String::from("the"),
            String::from("thread"),
        ];

        let mut num = counter.lock().unwrap();
        *num += 1;

        for val in vals {
            tx.send(val).unwrap();
            thread::sleep(Duration::from_millis(10));
        }
    });

    for i in 1..5 {
        println!("hi number {} from the main thread!", i);
        thread::sleep(Duration::from_millis(1));
    }
    for received in rx {
        println!("Got: {}", received);
    }
    handle.join().unwrap();

    println!("m = {:?}", *c.lock().unwrap());
}
