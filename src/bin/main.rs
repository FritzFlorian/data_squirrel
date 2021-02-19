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

    let optimize_cmd = SubCommand::with_name("optimize")
        .about("optimizes the underlying SQLite database (can save space and speed up operations)");

    let remote_path_arg = Arg::with_name("REMOTE_PATH")
        .required(true)
        .index(1)
        .help("Path of the remote data store on disk");
    let conflict_choose_local = Arg::with_name("choose-local")
        .long("choose-local")
        .short("l")
        .help("Instructs the sync algorithm to choose the local over the remote item on conflicts.")
        .required(false)
        .takes_value(false);
    let conflict_choose_remote = Arg::with_name("choose-remote")
        .long("choose-remote")
        .short("r")
        .help("Instructs the sync algorithm to choose the remote over the local item on conflicts.")
        .required(false)
        .takes_value(false);
    let sync_from_cmd = SubCommand::with_name("sync-from")
        .about("syncs from the remote store to the local store (local <- remote)")
        .arg(remote_path_arg)
        .arg(conflict_choose_local)
        .arg(conflict_choose_remote);

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
        .subcommand(optimize_cmd)
        .get_matches();

    let local_path = cli.value_of("LOCAL_PATH").unwrap();
    if let Some(create_cli) = cli.subcommand_matches("create") {
        create_data_store(&local_path, &create_cli);
    } else if let Some(scan_cli) = cli.subcommand_matches("scan") {
        scan_data_store(&local_path, &scan_cli);
    } else if let Some(sync_from_cli) = cli.subcommand_matches("sync-from") {
        sync_from_remote(&local_path, &sync_from_cli);
    } else if let Some(cleanup_cli) = cli.subcommand_matches("optimize") {
        optimize_data_store(&local_path, &cleanup_cli);
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
    let choose_local = cmd_cli.is_present("choose-local");
    let choose_remote = cmd_cli.is_present("choose-remote");
    if choose_local && choose_remote {
        panic!("Must not choose both local and remote items on sync (use either --choose-local or --choose-remote or none)");
    }

    let local_data_store =
        core::data_store::DefaultDataStore::open(&PathBuf::from(local_path)).unwrap();
    let remote_path = cmd_cli.value_of("REMOTE_PATH").unwrap();
    let remote_data_store =
        core::data_store::DefaultDataStore::open(&PathBuf::from(remote_path)).unwrap();

    use core::data_store::SyncConflictEvent::*;
    use core::data_store::SyncConflictResolution;
    local_data_store
        .sync_from_other_store(
            &remote_data_store,
            &RelativePath::from_path(""),
            &mut |conflict| match conflict {
                LocalDeletionRemoteFolder(db_item, _)
                | LocalFileRemoteFolder(db_item, _)
                | LocalDeletionRemoteFile(db_item, _)
                | LocalItemRemoteFile(db_item, _)
                | LocalItemRemoteDeletion(db_item, _) => {
                    println!("Conflict: {:?}", db_item.path.to_path_buf());
                    if choose_local {
                        println!("Choosing local version over remote!");
                        SyncConflictResolution::ChooseLocalItem
                    } else if choose_remote {
                        println!("Choosing remote version over local!");
                        SyncConflictResolution::ChooseRemoteItem
                    } else {
                        println!("Do not resolve the conflict (re-run sync with --choose-local or --choose-remote)");
                        SyncConflictResolution::DoNotResolve
                    }
                }
            },
        )
        .unwrap();
    println!("Sync Complete!");
}

fn optimize_data_store(local_path: &str, _cmd_cli: &ArgMatches) {
    println!("Optimizing database file...");
    let local_data_store =
        core::data_store::DefaultDataStore::open(&PathBuf::from(local_path)).unwrap();
    local_data_store.optimize_database().unwrap();
    println!("Optimization done!");
}
