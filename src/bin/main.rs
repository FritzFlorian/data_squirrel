extern crate core;

use std::path::PathBuf;

fn main() {
    let data_store = core::data_store::DefaultDataStore::create(
        &PathBuf::from("/Users/florianfritz/Documents/projects"),
        "XYZ",
        "XYZ",
        "local",
    )
    .unwrap();
    let result = data_store.perform_full_scan().unwrap();
    println!("Scanned: {:?}", result);
}
