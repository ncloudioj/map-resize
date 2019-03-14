use rkv::{
    Manager,
    Rkv,
    StoreOptions,
    Value,
};
use ctrlc;

use std::{
    fs,
    path::Path,
    sync::{Arc, Mutex},
    thread,
    time,
};

fn main() {
    let map_size: usize = 40960;
    let done = Arc::new(Mutex::new(false));
    let done_clone = Arc::clone(&done);
    ctrlc::set_handler(move || {
        let mut done = done_clone.lock().unwrap();
        *done = true;
    }).expect("set up signal handler");

    let root = Path::new("/tmp/remap-db");
    fs::create_dir_all(root).unwrap();

    let created_arc = Manager::singleton().write().unwrap().get_or_create(root, Rkv::new).unwrap();
    let mut k = created_arc.write().unwrap();
    k.set_map_size(map_size).unwrap();

    let store = k.open_single("store", StoreOptions::create()).unwrap();

    let mut counter = 0;
    println!("Inserting into {:?}", root);
    while !*done.lock().unwrap() {
        let mut failed = false;
        {
            let writer = k.write();
            match writer {
                Ok(mut writer) => {
                    counter += 1;
                    let key = format!("key {}", counter);
                    match store.put(&mut writer, &key, &Value::Str(&"Apple".repeat(2000))) {
                        Ok(_) => {
                            println!("Inserting {}...", key);
                            match writer.commit() {
                                Err(error) => {
                                    println!("Failed to commit: {:?}", error);
                                    failed = true;
                                },
                                _ => {}
                            }
                        },
                        Err(error) => {
                            println!("Failed to put: {:?}", error);
                            writer.abort();
                            failed = true;
                        }
                    }
                },
                Err(error) => {
                    println!("Failed to open transaction: {:?}", error);
                    failed = true;
                }
            }
        }
        if failed {
            println!("Reloading env ...{}", k.stat().unwrap().entries());
            k.set_map_size(0).unwrap();
        }
        thread::sleep(time::Duration::from_millis(200));
    }
}
