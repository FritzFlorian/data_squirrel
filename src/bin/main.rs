extern crate clap;
extern crate core;
use clap::{App, Arg, ArgMatches, SubCommand};
use core::fs_interaction::relative_path::RelativePath;
use std::path::PathBuf;

fn main() {
    let local_path_arg = Arg::with_name("LOCAL_PATH")
        .required(true)
        .index(1)
        .help("Path of the local data store on disk");
    let cli = App::new("DataSquirrel")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about("Allows to synchronize directories p2p without restrictions on the sync order")
        .arg(local_path_arg)
        .subcommand(create_cmd())
        .subcommand(scan_cmd())
        .subcommand(sync_from_cmd())
        .subcommand(optimize_cmd())
        .subcommand(rules_cmd())
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
    } else if let Some(inclusion_cli) = cli.subcommand_matches("rules") {
        manage_inclusion_rules(&local_path, inclusion_cli);
    } else {
        println!("Please specify the command you want to perform on the data store.");
        println!("See --help for more information.");
    }
}

fn create_cmd<'a, 'b>() -> App<'a, 'b> {
    let data_set_name_arg = Arg::with_name("name")
        .long("name")
        .short("n")
        .help("The unique name of the data set this store will belong to.")
        .required(true)
        .takes_value(true);
    let transfer_store_arg = Arg::with_name("transfer-store")
        .long("transfer-store")
        .short("t")
        .required(false)
        .takes_value(false)
        .help("Marks the store to be a transfer store. It does not index data itself, but carries it to other stores that need it.");
    let create_cmd = SubCommand::with_name("create")
        .about("inits a directory to be a data_store")
        .arg(data_set_name_arg)
        .arg(transfer_store_arg);

    create_cmd
}

fn create_data_store(local_path: &str, cmd_cli: &ArgMatches) {
    let data_set_name = cmd_cli.value_of("name").unwrap();

    println!("Creating new data store at '{}'...", local_path);
    let result =
        core::data_store::DefaultDataStore::create(local_path, data_set_name, "default", "default");

    match result {
        Ok(data_store) => {
            println!(
            "Created new store (data_set_name: '{}', data_store_name: '{}', data_store_desc: '{}')!",
            data_store.data_set_name().unwrap(),
            data_store.local_data_store_name().unwrap(),
            data_store.local_data_store_desc().unwrap()
            );
            if cmd_cli.is_present("transfer-store") {
                println!(
                    "Marking store as transfer store (it will carry data to all synced stores)."
                );
                data_store.mark_as_transfer_store().unwrap();
            }
        }
        Err(core::data_store::DataStoreError::FSInteractionError {
            source: core::fs_interaction::FSInteractionError::MetadataDirAlreadyExists,
        }) => panic!("A data store already exists on the given path!"),
        Err(err) => panic!("{:?}", err),
    }
}

fn scan_cmd<'a, 'b>() -> App<'a, 'b> {
    let scan_cmd = SubCommand::with_name("scan")
        .about("performs a scan of the given data store, indexing any changed hard drive content");

    scan_cmd
}

fn scan_data_store(local_path: &str, _cmd_cli: &ArgMatches) {
    println!("Performing full scan on data store...");
    let local_data_store =
        core::data_store::DefaultDataStore::open(&PathBuf::from(local_path)).unwrap();
    let result = local_data_store.perform_full_scan().unwrap();
    println!("Scan Complete: {:?}", result);
}

fn sync_from_cmd<'a, 'b>() -> App<'a, 'b> {
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

    sync_from_cmd
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

fn optimize_cmd<'a, 'b>() -> App<'a, 'b> {
    let optimize_cmd = SubCommand::with_name("optimize")
        .about("optimizes the underlying SQLite database (can save space and speed up operations)");

    optimize_cmd
}

fn optimize_data_store(local_path: &str, _cmd_cli: &ArgMatches) {
    println!("Optimizing database file...");
    let local_data_store =
        core::data_store::DefaultDataStore::open(&PathBuf::from(local_path)).unwrap();
    local_data_store.optimize_database().unwrap();
    println!("Optimization done!");
}

fn rules_cmd<'a, 'b>() -> App<'a, 'b> {
    let dry_run_arg = Arg::with_name("dry-run")
        .long("dry-run")
        .help("Executes the command as a DRY run, not performing any actual changes.")
        .required(false)
        .takes_value(false);
    let remove_rule_arg = Arg::with_name("remove-rule")
        .long("remove-rule")
        .takes_value(true)
        .multiple(true);
    let ignore_rule_arg = Arg::with_name("ignore-rule")
        .long("ignore-rule")
        .takes_value(true)
        .multiple(true);
    let inclusion_rule_arg = Arg::with_name("inclusion-rule")
        .long("inclusion-rule")
        .takes_value(true)
        .multiple(true);
    let print_rule_arg = Arg::with_name("print")
        .long("print")
        .required(false)
        .takes_value(false);
    let inclusion_rule_cmd = SubCommand::with_name("rules")
        .about("Manipulates the inclusion and ignore rules of the data store.")
        .arg(dry_run_arg)
        .arg(remove_rule_arg)
        .arg(inclusion_rule_arg)
        .arg(ignore_rule_arg)
        .arg(print_rule_arg);

    inclusion_rule_cmd
}

fn manage_inclusion_rules(local_path: &str, cmd_cli: &ArgMatches) {
    println!("Changing inclusion/ignore rules of data_store...");
    let mut local_data_store =
        core::data_store::DefaultDataStore::open(&PathBuf::from(local_path)).unwrap();
    let mut rules = local_data_store.get_inclusion_rules().clone();

    if cmd_cli.is_present("print") {
        println!("Inclusion rules of store:");
        for rule in rules.iter() {
            if rule.include {
                println!("+ {}", rule.rule.as_str());
            } else {
                println!("- {}", rule.rule.as_str());
            }
        }
    }

    if let Some(removed_rules) = cmd_cli.values_of("remove-rule") {
        println!("Removing rules: ");
        for removed_rule in removed_rules {
            println!("{}", removed_rule);
            rules.remove_rule(&removed_rule);
        }
    }
    if let Some(ignore_rules) = cmd_cli.values_of("ignore-rule") {
        println!("Adding ignore rules: ");
        for ignore_rule in ignore_rules {
            println!("{}", ignore_rule);
            rules.add_ignore_rule(glob::Pattern::new(&ignore_rule).unwrap());
        }
    }
    if let Some(inclusion_rules) = cmd_cli.values_of("inclusion-rule") {
        println!("Adding inclusion rules: ");
        for inclusion_rule in inclusion_rules {
            println!("{}", inclusion_rule);
            rules.add_inclusion_rule(glob::Pattern::new(&inclusion_rule).unwrap());
        }
    }

    let dry_run = cmd_cli.is_present("dry-run");
    let (added_items, removed_items) = local_data_store
        .update_inclusion_rules(rules, dry_run)
        .unwrap();
    if dry_run {
        println!("DRY RUN - NO ACTUAL CHANGES TO DB");
    }
    println!("No longer ignored items:");
    for added_item in added_items {
        println!("{}", added_item.path.to_path_buf().to_str().unwrap());
    }
    println!("Newly ignored items:");
    for removed_item in removed_items {
        println!("{}", removed_item.path.to_path_buf().to_str().unwrap());
    }
}
