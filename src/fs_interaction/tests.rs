use self::virtual_fs::{InMemoryFS, FS};
use super::*;
use filetime::FileTime;
use std::fs;

#[test]
fn create_data_store_in_empty_folder() {
    let test_dir = tempfile::tempdir().unwrap();

    let data_store = DefaultFSInteraction::create(test_dir.path()).unwrap();
    assert_eq!(
        data_store.root_path,
        test_dir.path().canonicalize().unwrap()
    );

    assert!(
        test_dir.path().join(METADATA_DIR).is_dir(),
        "Must have created a special data_squirrel metadata folder."
    );
    assert!(
        test_dir
            .path()
            .join(METADATA_DIR)
            .join(PENDING_FILES_DIR)
            .is_dir(),
        "Must have created a special metadata/pending_files folder."
    );
    assert!(
        test_dir
            .path()
            .join(METADATA_DIR)
            .join(SNAPSHOT_DIR)
            .is_dir(),
        "Must have created a special metadata/snapshots folder."
    );
}

#[test]
fn data_store_creates_and_releases_locks() {
    let test_dir = tempfile::tempdir().unwrap();

    let data_store = DefaultFSInteraction::create(test_dir.path()).unwrap();
    assert!(
        test_dir.path().join(METADATA_DIR).join(LOCK_FILE).is_file(),
        "Must create lock file when having an open data_store."
    );

    drop(data_store);
    assert!(
        !test_dir.path().join(METADATA_DIR).join(LOCK_FILE).is_file(),
        "Must delete the lock file when closing a data_store."
    );
}

#[test]
fn can_not_open_data_store_multiple_times() {
    let test_dir = tempfile::tempdir().unwrap();

    // Create and close
    let data_store_1 = DefaultFSInteraction::create(test_dir.path()).unwrap();
    drop(data_store_1);

    // Open first instance
    let _data_store_2 = DefaultFSInteraction::open(test_dir.path()).unwrap();

    // Opening second instance should fail
    match DefaultFSInteraction::open(test_dir.path()) {
        Err(FSInteractionError::AlreadyOpened) => (),
        _ => panic!("Must report error that data_store is in use."),
    };
}

fn has_data_item(items: &Vec<DataItem>, name: &str) -> bool {
    items
        .iter()
        .any(|item| item.relative_path == RelativePath::from_path(name))
}

#[test]
fn can_index_root_directory() {
    let test_dir = tempfile::tempdir().unwrap();
    let data_store = DefaultFSInteraction::create(test_dir.path()).unwrap();

    // Create some test content
    fs::File::create(test_dir.path().join("a.txt")).unwrap();
    fs::File::create(test_dir.path().join("b.txt")).unwrap();
    fs::create_dir(test_dir.path().join("a")).unwrap();
    fs::create_dir(test_dir.path().join("b")).unwrap();

    // Query for that test content
    let content = data_store.index(&RelativePath::from_path("")).unwrap();

    assert!(has_data_item(&content, "a.txt"));
    assert!(has_data_item(&content, "b.txt"));
    assert!(has_data_item(&content, "a"));
    assert!(has_data_item(&content, "b"));

    assert!(!has_data_item(&content, METADATA_DIR));
}

#[test]
fn can_index_sub_directory() {
    let test_dir = tempfile::tempdir().unwrap();
    let data_store = DefaultFSInteraction::create(test_dir.path()).unwrap();

    // Create some test content
    fs::create_dir(test_dir.path().join("sub")).unwrap();
    fs::create_dir(test_dir.path().join("sub/a")).unwrap();
    fs::File::create(test_dir.path().join("sub/a.txt")).unwrap();

    // Query for that test content
    let content = data_store.index(&RelativePath::from_path("sub")).unwrap();

    assert!(has_data_item(&content, "sub/a.txt"));
    assert!(has_data_item(&content, "sub/a"));
}

#[test]
fn detects_duplicates() {
    // Create some test content
    let test_fs = InMemoryFS::default();
    test_fs.create_dir(&PathBuf::from("/AbC"), false).unwrap();
    test_fs.create_dir(&PathBuf::from("/aBc"), false).unwrap();
    test_fs.create_file(&PathBuf::from("/abC")).unwrap();

    test_fs.create_dir(&PathBuf::from("/other"), false).unwrap();
    test_fs.create_file(&PathBuf::from("/file")).unwrap();

    let data_store =
        FSInteraction::<InMemoryFS>::create_with_fs(&PathBuf::from("/"), test_fs).unwrap();

    // Query for that test content
    let content = data_store.index(&RelativePath::from_path("")).unwrap();
    assert_eq!(content.len(), 5);
    content.iter().for_each(|item| {
        if item.relative_path.name().to_lowercase() == "abc" {
            assert_eq!(item.issues, vec![Issue::Duplicate]);
        }
    });
}

#[test]
fn calculates_hash_correctly() {
    const STRING_A: &str = "hello world!";
    const HASH_A: [u8; 32] = [
        117, 9, 229, 189, 160, 199, 98, 210, 186, 199, 249, 13, 117, 139, 91, 34, 99, 250, 1,
        204, 188, 84, 42, 181, 227, 223, 22, 59, 224, 142, 108, 169,
    ];
    const STRING_B: &str = "whoo!";
    const HASH_B: [u8; 32] = [
        151, 254, 64, 101, 229, 147, 199, 192, 195, 195, 188, 8, 124, 186, 196, 35, 235, 157,
        84, 215, 226, 136, 93, 24, 67, 133, 176, 243, 247, 96, 139, 176,
    ];

    let test_fs = InMemoryFS::default();
    test_fs.create_file("/a.txt").unwrap();
    test_fs
        .test_set_file_content("/a.txt", STRING_A.to_string().into_bytes())
        .unwrap();
    test_fs.create_file("/b.txt").unwrap();
    test_fs
        .test_set_file_content("/b.txt", STRING_B.to_string().into_bytes())
        .unwrap();

    let data_store =
        FSInteraction::<InMemoryFS>::create_with_fs(&PathBuf::from("/"), test_fs).unwrap();

    assert_eq!(
        data_store
            .calculate_hash(&RelativePath::from_path("/a.txt"))
            .unwrap()
            .as_ref(),
        HASH_A
    );
    assert_eq!(
        data_store
            .calculate_hash(&RelativePath::from_path("/b.txt"))
            .unwrap()
            .as_ref(),
        HASH_B
    );
}

#[test]
fn modifies_data_correctly_in_memory() {
    modifies_data_correctly::<virtual_fs::InMemoryFS>(&PathBuf::new());
}

#[test]
fn modifies_data_correctly_wrapper() {
    let test_dir = tempfile::tempdir().unwrap();
    modifies_data_correctly::<virtual_fs::WrapperFS>(test_dir.path());
}

fn modifies_data_correctly<FS: virtual_fs::FS>(root_dir: &Path) {
    // Create some test content
    let test_fs = FS::default();
    test_fs.create_file(&root_dir.join("file")).unwrap();

    let data_store = FSInteraction::<FS>::create_with_fs(&root_dir, test_fs).unwrap();

    // Query metadata...
    let file_metadata = data_store
        .metadata(&RelativePath::from_path("file"))
        .unwrap();

    // ...change it...
    let new_mod_time =
        FileTime::from_unix_time(10 + file_metadata.last_mod_time().unix_seconds(), 0);
    data_store
        .set_metadata(&RelativePath::from_path("file"), new_mod_time, true)
        .unwrap();

    // ...re-load and test it.
    let file_metadata = data_store
        .metadata(&RelativePath::from_path("file"))
        .unwrap();
    assert_eq!(file_metadata.read_only(), true);
    assert_eq!(file_metadata.last_mod_time(), new_mod_time,);
}

#[test]
fn moves_data_correctly_in_memory() {
    moves_data_correctly::<virtual_fs::InMemoryFS>(&PathBuf::new());
}

#[test]
fn moves_data_correctly_wrapper() {
    let test_dir = tempfile::tempdir().unwrap();
    moves_data_correctly::<virtual_fs::WrapperFS>(test_dir.path());
}

fn moves_data_correctly<FS: virtual_fs::FS>(root_dir: &Path) {
    // Create some test content
    let test_fs = FS::default();
    test_fs.create_dir(&root_dir.join("dir"), false).unwrap();
    test_fs.create_file(&root_dir.join("dir/file")).unwrap();

    let data_store = FSInteraction::<FS>::create_with_fs(&root_dir, test_fs.clone()).unwrap();

    data_store
        .rename_file_or_directory(
            &RelativePath::from_path("dir"),
            &RelativePath::from_path("new-dir"),
        )
        .unwrap();
    let root_entries = test_fs.list_dir(&root_dir).unwrap();
    root_entries.iter().any(|item| item.file_name == "new-dir");
    assert_eq!(root_entries.len(), 2);
    assert!(root_entries.iter().any(|item| item.file_name == "new-dir"));

    data_store
        .rename_file_or_directory(
            &RelativePath::from_path("new-dir/file"),
            &RelativePath::from_path("file"),
        )
        .unwrap();
    let root_entries = test_fs.list_dir(&root_dir).unwrap();
    assert_eq!(root_entries.len(), 3);
    assert!(root_entries.iter().any(|item| item.file_name == "new-dir"));
    assert!(root_entries.iter().any(|item| item.file_name == "file"));
}