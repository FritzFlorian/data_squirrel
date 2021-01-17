extern crate clap;
extern crate core;
use clap::{App, Arg, ArgMatches, SubCommand};
use core::fs_interaction::relative_path::RelativePath;
use std::path::PathBuf;

fn main() {
    let data_set_name_arg = Arg::with_name("name")
        .long("name")
        .short("n")
        .help("The unique name of the data set this store will belong to.")
        .required(true)
        .takes_value(true);
    let create_cmd = SubCommand::with_name("create")
        .about("inits a directory to be a data_store")
        .arg(data_set_name_arg);

    let scan_cmd = SubCommand::with_name("scan")
        .about("performs a scan of the given data store, indexing any changed hard drive content");

    let remote_path_arg = Arg::with_name("REMOTE_PATH")
        .required(true)
        .index(1)
        .help("Path of the remote data store on disk");
    let sync_from_cmd = SubCommand::with_name("sync-from")
        .about("syncs from the remote store to the local store (local <- remote)")
        .arg(remote_path_arg);

    let local_path_arg = Arg::with_name("LOCAL_PATH")
        .required(true)
        .index(1)
        .help("Path of the local data store on disk");
    let cli = App::new("DataSquirrel")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about("Allows to synchronize directories p2p without restrictions on the sync order")
        .arg(local_path_arg)
        .subcommand(create_cmd)
        .subcommand(scan_cmd)
        .subcommand(sync_from_cmd)
        .get_matches();

    let local_path = cli.value_of("LOCAL_PATH").unwrap();
    if let Some(create_cli) = cli.subcommand_matches("create") {
        create_data_store(&local_path, &create_cli);
    } else if let Some(scan_cli) = cli.subcommand_matches("scan") {
        scan_data_store(&local_path, &scan_cli);
    } else if let Some(sync_from_cli) = cli.subcommand_matches("sync-from") {
        sync_from_remote(&local_path, &sync_from_cli);
    } else {
        println!("Please specify the command you want to perform on the data store.");
        println!("See --help for more information.");
    }
}

fn create_data_store(local_path: &str, cmd_cli: &ArgMatches) {
    let data_set_name = cmd_cli.value_of("name").unwrap();

    println!("Creating new data store at '{}'...", local_path);
    let result =
        core::data_store::DefaultDataStore::create(local_path, data_set_name, "default", "default");

    match result {
        Ok(data_store) => println!(
            "Created new store (data_set_name: '{}', data_store_name: '{}', data_store_desc: '{}')!",
            data_store.data_set_name().unwrap(),
            data_store.local_data_store_name().unwrap(),
            data_store.local_data_store_desc().unwrap()
        ),
        Err(core::data_store::DataStoreError::FSInteractionError {
            source: core::fs_interaction::FSInteractionError::MetadataDirAlreadyExists,
        }) => eprintln!("A data store already exists on the given path!"),
        Err(err) => eprintln!("{:?}", err),
    }
}

fn scan_data_store(local_path: &str, _cmd_cli: &ArgMatches) {
    println!("Performing full scan on data store...");
    let local_data_store =
        core::data_store::DefaultDataStore::open(&PathBuf::from(local_path)).unwrap();
    let result = local_data_store.perform_full_scan().unwrap();
    println!("Scan Complete: {:?}", result);
}

fn sync_from_remote(local_path: &str, cmd_cli: &ArgMatches) {
    println!("Syncing new changes FROM remote TO local data store...");
    let local_data_store =
        core::data_store::DefaultDataStore::open(&PathBuf::from(local_path)).unwrap();
    let remote_path = cmd_cli.value_of("REMOTE_PATH").unwrap();
    let remote_data_store =
        core::data_store::DefaultDataStore::open(&PathBuf::from(remote_path)).unwrap();

    local_data_store
        .sync_from_other_store(&remote_data_store, &RelativePath::from_path(""))
        .unwrap();
    println!("Sync Complete!");
}
