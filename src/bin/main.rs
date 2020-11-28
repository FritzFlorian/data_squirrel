extern crate core;

use std::path::PathBuf;

fn main() {
    core::data_store::DefaultDataStore::create(&PathBuf::from("./"), "XYZ", "XYZ", "local")
        .unwrap();
}
