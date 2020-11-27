extern crate core;

use std::path::PathBuf;

fn main() {
    core::data_store::DefaultDataStore::open(&PathBuf::from("./"), true).unwrap();
}
