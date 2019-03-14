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
    let done = Arc::new(Mutex::new(false));
    let done_clone = Arc::clone(&done);
    ctrlc::set_handler(move || {
        let mut done = done_clone.lock().unwrap();
        *done = true;
    }).expect("set up signal handler");

    let root = Path::new("/tmp/remap-db");
    fs::create_dir_all(root).unwrap();

    let created_arc = Manager::singleton().write().unwrap().get_or_create(root, Rkv::new).unwrap();
    let mut counter = 0;

    let mut size = 1048576;
    while !*done.lock().unwrap() {
        counter += 1;
        size *= 2;
        println!("Round {}, resizing to {}", counter, size);
        let mut rkv = created_arc.write().expect("guard");
        let mut info = rkv.info().unwrap();
        println!("Old size {}", info.map_size());
        rkv.set_map_size(size).unwrap();

        // Need to mutate the env to take the resize effect. rkv.sync(true) alone won't work.
        let store = rkv.open_single("store", StoreOptions::create()).unwrap();
        let mut writer = rkv.write().unwrap();
        store.put(&mut writer, "foo", &Value::Str(&"Apple".repeat(2000))).unwrap();
        writer.commit().unwrap();

        info = rkv.info().unwrap();
        println!("New size {}", info.map_size());
        thread::sleep(time::Duration::from_millis(20_000));
    }
}
