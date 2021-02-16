use super::*;
use fs_interaction::virtual_fs::{InMemoryFS, FS};
use std::fs::File;
use std::io::Write;
use std::thread::sleep;
use std::time::Duration;

#[test]
fn create_data_store() {
    let test_dir = tempfile::tempdir().unwrap();
    let data_store =
        DefaultDataStore::create(test_dir.path(), "XYZ-123", "XYZ", "local-data-store").unwrap();

    let data_set = data_store.db_access.get_data_set().unwrap();
    assert_eq!(data_set.unique_name, "XYZ-123");
    assert_eq!(data_set.human_name, "XYZ");

    let this_data_store = data_store.db_access.get_local_data_store().unwrap();
    assert_eq!(
        this_data_store.path_on_device,
        test_dir.path().canonicalize().unwrap().to_str().unwrap()
    );
    assert_eq!(this_data_store.human_name, "local-data-store");
}

#[test]
fn re_open_data_store() {
    let test_dir = tempfile::tempdir().unwrap();

    // Should succeed in creating a new data-store in the empty directory.
    let data_store_1 =
        DefaultDataStore::create(&test_dir.path(), "XYZ", "XYZ", "local-data-store").unwrap();
    drop(data_store_1);

    // Should fail because we can not re-create in this directory.
    assert!(DefaultDataStore::create(&test_dir.path(), "XYZ", "XYZ", "local-data-store").is_err());

    // Should succeed to open the just opened data-store.
    let _data_store_2 = DefaultDataStore::open(test_dir.path()).unwrap();

    // Should fail, as the data store is already opened.
    assert!(DefaultDataStore::open(test_dir.path()).is_err());
}

#[test]
fn scan_data_store_directory() {
    let in_memory_fs = virtual_fs::InMemoryFS::new();
    let data_store_1 =
        DataStore::create_with_fs("", "XYZ", "XYZ", "local-data-store", in_memory_fs.clone())
            .unwrap();

    // Initial data set
    in_memory_fs.create_dir("sUb-1", false).unwrap();
    in_memory_fs.create_dir("sUb-1/sub-1-1", false).unwrap();
    in_memory_fs.create_dir("sUb-2", false).unwrap();

    in_memory_fs.create_file("file-1").unwrap();
    in_memory_fs.create_file("file-2").unwrap();
    in_memory_fs.create_file("sUb-1/file-1").unwrap();

    let changes = data_store_1.perform_full_scan().unwrap();
    assert_eq!(
        changes,
        ScanResult {
            indexed_items: 6,
            changed_items: 0,
            new_items: 6,
            deleted_items: 0
        }
    );
    assert_eq!(data_store_1.local_time().unwrap(), 6);

    // Detect new and changed files
    in_memory_fs.create_file("file-3").unwrap();
    in_memory_fs
        .test_set_file_content("file-1", "hello", true)
        .unwrap();

    let changes = data_store_1.perform_full_scan().unwrap();
    assert_eq!(
        changes,
        ScanResult {
            indexed_items: 7,
            changed_items: 1,
            new_items: 1,
            deleted_items: 0
        }
    );
    assert_eq!(data_store_1.local_time().unwrap(), 8);

    // Detect deleted files and directories
    in_memory_fs.remove_file("file-1").unwrap();
    in_memory_fs.remove_file("sUb-1/file-1").unwrap();
    in_memory_fs.remove_dir_recursive("sUb-1/sub-1-1").unwrap();
    in_memory_fs.remove_dir_recursive("sUb-1").unwrap();

    let changes = data_store_1.perform_full_scan().unwrap();
    assert_eq!(
        changes,
        ScanResult {
            indexed_items: 5,
            changed_items: 0,
            new_items: 0,
            deleted_items: 2,
        }
    );
    assert_eq!(data_store_1.local_time().unwrap(), 10);

    // Re-add some
    in_memory_fs.create_file("file-1").unwrap();
    in_memory_fs.create_dir("sUb-1", false).unwrap();
    in_memory_fs.create_file("sUb-1/file-1").unwrap();
    let changes = data_store_1.perform_full_scan().unwrap();
    assert_eq!(
        changes,
        ScanResult {
            indexed_items: 6,
            changed_items: 0,
            new_items: 3,
            deleted_items: 0
        }
    );
    assert_eq!(data_store_1.local_time().unwrap(), 13);

    // Changes in capitalization should be recognized as metadata changes
    in_memory_fs.rename("file-1", "FILE-1").unwrap();
    in_memory_fs.rename("sUb-1", "SUB-1").unwrap();
    in_memory_fs.rename("SUB-1/file-1", "SUB-1/FILE-1").unwrap();

    let changes = data_store_1.perform_full_scan().unwrap();
    assert_eq!(
        changes,
        ScanResult {
            indexed_items: 6,
            changed_items: 3,
            new_items: 0,
            deleted_items: 0
        }
    );
    assert_eq!(data_store_1.local_time().unwrap(), 16);
    let changes = data_store_1.perform_full_scan().unwrap();
    assert_eq!(
        changes,
        ScanResult {
            indexed_items: 6,
            changed_items: 0,
            new_items: 0,
            deleted_items: 0
        }
    );
    assert_eq!(data_store_1.local_time().unwrap(), 16);
}

#[test]
fn exclude_ignored_files_during_scan() {
    let in_memory_fs = virtual_fs::InMemoryFS::new();
    let mut data_store_1 =
        DataStore::create_with_fs("", "XYZ", "XYZ", "local-data-store", in_memory_fs.clone())
            .unwrap();

    // Initial data set
    in_memory_fs.create_dir("sub-1", false).unwrap();
    in_memory_fs.create_dir("sub-2", false).unwrap();
    in_memory_fs.create_dir("sub-1/sub-3", false).unwrap();

    in_memory_fs.create_file("file-1").unwrap();
    in_memory_fs.create_file("file-2").unwrap();
    in_memory_fs.create_file("sub-1/file-1").unwrap();
    in_memory_fs.create_file("sub-1/sub-3/file-3").unwrap();

    // Ignore just sub-3 for now.
    data_store_1
        .add_scan_ignore_rule("**/sub-3", false)
        .unwrap();
    let changes = data_store_1.perform_full_scan().unwrap();
    assert_eq!(
        changes,
        ScanResult {
            indexed_items: 6, // Note that we do not even 'see' file-3
            changed_items: 0,
            new_items: 5, // Note that we ignore one of the scanned items
            deleted_items: 0
        }
    );

    // Detect we do not want to detect these changes, both should fall under the new ignore rules.
    in_memory_fs
        .test_increase_file_mod_time("sub-1/sub-3/file-3")
        .unwrap();
    in_memory_fs.create_file("sub-1/file-2").unwrap();
    // This change on the other hand should be detected.
    in_memory_fs.test_increase_file_mod_time("file-2").unwrap();

    // NOTE: we do NOT EXPECT to remove existing indexed files. We just never want to add new ones
    //       when we added them to the scan ignore rules.
    //       Think of this a lot like git ignore files: once you staged files they wont go away.
    data_store_1
        .add_scan_ignore_rule("**/file-2", false)
        .unwrap();
    let changes = data_store_1.perform_full_scan().unwrap();
    assert_eq!(
        changes,
        ScanResult {
            indexed_items: 7, // We expect to 'see' the ignored file-2
            changed_items: 1,
            new_items: 0,
            deleted_items: 0
        }
    );

    // Deleting and re-creating an file should 'clear' it from the db and then
    // the next re-index will ignore it.
    in_memory_fs.remove_file("file-2").unwrap();
    let changes = data_store_1.perform_full_scan().unwrap();
    assert_eq!(
        changes,
        ScanResult {
            indexed_items: 7,
            changed_items: 0,
            new_items: 0,
            deleted_items: 1,
        }
    );
    in_memory_fs.create_file("file-2").unwrap();
    let changes = data_store_1.perform_full_scan().unwrap();
    assert_eq!(
        changes,
        ScanResult {
            indexed_items: 7,
            changed_items: 0,
            new_items: 0,
            deleted_items: 0
        }
    );

    // Forget the scan ignore rules. Now we should see all the files again.
    data_store_1.remove_temporary_ignore_rule().unwrap();
    let changes = data_store_1.perform_full_scan().unwrap();
    assert_eq!(
        changes,
        ScanResult {
            indexed_items: 8,
            changed_items: 0,
            new_items: 4,
            deleted_items: 0
        }
    );
}

fn dir_should_contain<FS: virtual_fs::FS>(fs: &FS, path: &str, expected_content: Vec<&str>) {
    let dir_entries = fs.list_dir(path).unwrap();
    for expected_item in expected_content {
        assert!(dir_entries
            .iter()
            .any(|item| item.file_name == expected_item));
    }
}

fn dir_should_not_contain<FS: virtual_fs::FS>(
    fs: &FS,
    path: &str,
    not_expected_content: Vec<&str>,
) {
    let dir_entries = fs.list_dir(path).unwrap();
    for not_expected_item in not_expected_content {
        assert!(!dir_entries
            .iter()
            .any(|item| item.file_name == not_expected_item));
    }
}

fn create_in_memory_store() -> (InMemoryFS, DataStore<InMemoryFS>) {
    let fs = virtual_fs::InMemoryFS::new();
    let data_store =
        DataStore::create_with_fs("", "XYZ", "XYZ", "source-data-store", fs.clone()).unwrap();

    (fs, data_store)
}

#[test]
fn unidirectional_sync() {
    let (fs_1, data_store_1) = create_in_memory_store();
    let (fs_2, data_store_2) = create_in_memory_store();

    // Initial Data Set - Local Data Store
    fs_1.create_dir("sub-1", false).unwrap();
    fs_1.create_dir("sub-1/sub-1-1", false).unwrap();
    fs_1.create_dir("sub-2", false).unwrap();
    fs_1.create_file("file-1").unwrap();
    fs_1.create_file("file-2").unwrap();
    fs_1.create_file("sub-1/file-1").unwrap();

    // Index it and sync it to the remote data store
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();

    // We should have the files on the second store
    dir_should_contain(&fs_2, "", vec!["sub-1", "sub-2", "file-1", "file-2"]);
    let changes = data_store_2.perform_full_scan().unwrap();
    assert_eq!(
        changes,
        ScanResult {
            indexed_items: 6,
            changed_items: 0,
            new_items: 0,
            deleted_items: 0
        }
    );

    // Lets do some non-conflicting changes in both stores
    fs_2.test_set_file_content("file-2", "testing", true)
        .unwrap();
    fs_1.create_file("file-3").unwrap();
    fs_1.remove_file("file-1").unwrap();

    // Fully scan and sync them
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
    data_store_1
        .sync_from_other_store_panic_conflicts(&data_store_2, &RelativePath::from_path(""))
        .unwrap();

    // The contents should now match without any conflicts
    dir_should_contain(&fs_1, "", vec!["sub-1", "sub-2", "file-2", "file-3"]);
    dir_should_contain(&fs_2, "", vec!["sub-1", "sub-2", "file-2", "file-3"]);

    // Lastly, lets see about permission changes (for now only read-only bits).
    let old_metadata = fs_1.metadata("file-2").unwrap();
    fs_1.update_metadata("file-2", old_metadata.last_mod_time(), true)
        .unwrap();

    data_store_1.perform_full_scan().unwrap();
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();

    let other_metadata = fs_2.metadata("file-2").unwrap();
    assert_eq!(other_metadata.read_only(), true);
}

#[test]
#[should_panic(expected = "Must not sync if disk content is not correctly indexed in DB")]
fn panics_when_trying_to_sync_without_index_1() {
    let (fs_1, data_store_1) = create_in_memory_store();
    let (fs_2, data_store_2) = create_in_memory_store();

    // Initial Data Set - Local Data Store
    fs_1.create_dir("sub-1", false).unwrap();
    fs_1.create_file("file-1").unwrap();
    fs_2.create_file("file-1").unwrap();

    // Index it and sync it to the remote data store
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();
    // Rename item on receiving data store after scan operation
    fs_2.rename("file-1", "FILE-1").unwrap();
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
}

#[test]
#[should_panic(expected = "Must not sync if disk content is not correctly indexed in DB")]
fn panics_when_trying_to_sync_without_index_2() {
    let (fs_1, data_store_1) = create_in_memory_store();
    let (fs_2, data_store_2) = create_in_memory_store();

    // Initial Data Set - Local Data Store
    fs_1.create_dir("sub-1", false).unwrap();
    fs_1.create_file("file-1").unwrap();
    fs_2.create_file("file-1").unwrap();

    // Index it and sync it to the remote data store
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();
    // Delete item on receiving data store after scan operation
    fs_2.remove_file("file-1").unwrap();
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
}

#[test]
#[should_panic(expected = "Must not sync if disk content is not correctly indexed in DB")]
fn panics_when_trying_to_sync_without_index_3() {
    let (fs_1, data_store_1) = create_in_memory_store();
    let (fs_2, data_store_2) = create_in_memory_store();

    // Initial Data Set - Local Data Store
    fs_1.create_dir("sub-1", false).unwrap();
    fs_1.create_file("file-1").unwrap();
    fs_2.create_file("file-1").unwrap();

    // Index it and sync it to the remote data store
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();
    // Delete item on sending data store after scan operation
    fs_1.remove_file("file-1").unwrap();
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
}

#[test]
#[should_panic(expected = "Must not sync if disk content is not correctly indexed in DB")]
fn panics_when_trying_to_sync_without_index_4() {
    let (fs_1, data_store_1) = create_in_memory_store();
    let (fs_2, data_store_2) = create_in_memory_store();

    // Initial Data Set - Local Data Store
    fs_1.create_dir("sub-1", false).unwrap();
    fs_1.create_file("file-1").unwrap();
    fs_2.create_file("file-1").unwrap();

    // Index it and sync it to the remote data store
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();
    // Modify item on sending data store after scan operation
    fs_1.test_set_file_content("file-1", "test", true).unwrap();
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
}

#[test]
fn metadata_set_correctly_after_sync() {
    // We experienced a bug where after sycing a file from A -> B the transmitted file is
    // detected to have a change when then scanning the disk content on B.
    // For example, say you create test.txt on A, sync it to B, then the next scan on B will
    // detect a change in test.txt, even though it was not changed locally.

    let test_dir_1 = tempfile::tempdir().unwrap();
    let test_dir_2 = tempfile::tempdir().unwrap();

    let data_store_1 =
        DefaultDataStore::create(test_dir_1.path(), "XYZ", "XYZ", "source-data-store").unwrap();
    let data_store_2 =
        DefaultDataStore::create(test_dir_2.path(), "XYZ", "XYZ", "source-data-store").unwrap();

    // Create file in store 1 and sync it to store 2
    File::create(test_dir_1.path().join("test.txt"))
        .unwrap()
        .write_all(b"hello!")
        .unwrap();
    data_store_1.perform_full_scan().unwrap();

    // ..give some time for the time stamps of the newly created file to be different.
    sleep(Duration::from_millis(10));
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();

    // A scan on store 2 should now NOT result in any changes
    let changes = data_store_2.perform_full_scan().unwrap();
    assert_eq!(
        changes,
        ScanResult {
            indexed_items: 1,
            changed_items: 0,
            new_items: 0,
            deleted_items: 0
        }
    );
}

#[test]
fn can_sync_read_only_files() {
    // Better do this on the real FS
    let test_dir_1 = tempfile::tempdir().unwrap();
    let test_dir_2 = tempfile::tempdir().unwrap();

    let data_store_1 =
        DefaultDataStore::create(test_dir_1.path(), "XYZ", "XYZ", "source-data-store").unwrap();
    let data_store_2 =
        DefaultDataStore::create(test_dir_2.path(), "XYZ", "XYZ", "source-data-store").unwrap();

    // Create read-only file in store 1 and sync it to store 2
    let file_path = test_dir_1.path().join("test.txt");
    File::create(&file_path)
        .unwrap()
        .write_all(b"hello!")
        .unwrap();
    let mut permissions = std::fs::symlink_metadata(&file_path).unwrap().permissions();
    permissions.set_readonly(true);
    std::fs::set_permissions(&file_path, permissions).unwrap();

    data_store_1.perform_full_scan().unwrap();
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();

    // Now change it's content and try to sync it again.
    let mut permissions = std::fs::symlink_metadata(&file_path).unwrap().permissions();
    permissions.set_readonly(false);
    std::fs::set_permissions(&file_path, permissions).unwrap();
    std::fs::write(&file_path, b"other content").unwrap();
    let mut permissions = std::fs::symlink_metadata(&file_path).unwrap().permissions();
    permissions.set_readonly(true);
    std::fs::set_permissions(&file_path, permissions).unwrap();

    data_store_1.perform_full_scan().unwrap();
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();

    // We expect the target content to have changed but still be a read-only file.
    let file_path = test_dir_2.path().join("test.txt");
    let permissions = std::fs::symlink_metadata(&file_path).unwrap().permissions();
    assert_eq!(permissions.readonly(), true);
    assert_eq!(std::fs::read(&file_path).unwrap(), b"other content");
}

#[test]
fn sync_changes_in_file_name_case() {
    let (fs_1, data_store_1) = create_in_memory_store();
    let (fs_2, data_store_2) = create_in_memory_store();

    // Initial Data Set - Local Data Store
    fs_1.create_dir("sub-1", false).unwrap();
    fs_1.create_file("file-1").unwrap();
    fs_1.create_file("sub-1/file-1").unwrap();

    // Index it and sync it to the remote data store
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();

    // Change the case of files in the local data store
    fs_1.rename("sub-1", "SUB-1").unwrap();
    fs_1.rename("file-1", "File-1").unwrap();
    fs_1.rename("SUB-1/file-1", "SUB-1/FILE-1").unwrap();
    // ...also mix in some file contents.
    fs_1.test_set_file_content("File-1", "hello there!", true)
        .unwrap();

    // Index it and sync it to the remote data store
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();

    // We expect the case changes to be propagated
    dir_should_contain(&fs_2, "", vec!["SUB-1", "File-1"]);
    dir_should_not_contain(&fs_2, "", vec!["sub-1", "file-1"]);
    dir_should_contain(&fs_2, "SUB-1", vec!["FILE-1"]);
    dir_should_not_contain(&fs_2, "SUB-1", vec!["file-1"]);

    // Try some more changes, especially with parent dir/child interactions, maybe even change
    // the from folder to file at the same time.
    fs_1.remove_file("File-1").unwrap();
    fs_1.create_dir("FILE-1", false).unwrap();
    fs_1.create_file("SUB-1/file-2").unwrap();
    fs_1.rename("SUB-1", "sub-1").unwrap();

    // Index it and sync it to the remote data store
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();

    // We expect the case changes to be propagated
    dir_should_contain(&fs_2, "", vec!["sub-1", "FILE-1"]);
    dir_should_not_contain(&fs_2, "", vec!["File-1", "SUB-1"]);
    dir_should_contain(&fs_2, "sub-1", vec!["FILE-1", "file-2"]);
}

#[test]
fn multi_target_sync() {
    let (fs_1, data_store_1) = create_in_memory_store();
    let (fs_2, data_store_2) = create_in_memory_store();
    let (fs_3, data_store_3) = create_in_memory_store();

    // Initial Data Set
    fs_1.create_dir("sub-1", false).unwrap();
    fs_1.create_file("sub-1/file-1").unwrap();

    fs_2.create_dir("sub-2", false).unwrap();
    fs_2.create_file("sub-2/file-1").unwrap();

    // Index all
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();
    data_store_3.perform_full_scan().unwrap();

    // Sync from 1 to 3...
    data_store_3
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
    dir_should_contain(&fs_3, "", vec!["sub-1"]);
    dir_should_contain(&fs_3, "sub-1", vec!["file-1"]);
    // ...then from 3 to 2 (so effectively from 1 to 2)
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_3, &RelativePath::from_path(""))
        .unwrap();
    dir_should_contain(&fs_2, "", vec!["sub-1", "sub-2"]);
    dir_should_contain(&fs_2, "sub-1", vec!["file-1"]);
    dir_should_contain(&fs_2, "sub-2", vec!["file-1"]);

    // Finally, finish the sync-circle (from 2 to 1)
    data_store_1
        .sync_from_other_store_panic_conflicts(&data_store_2, &RelativePath::from_path(""))
        .unwrap();
    dir_should_contain(&fs_1, "", vec!["sub-1", "sub-2"]);
    dir_should_contain(&fs_1, "sub-1", vec!["file-1"]);
    dir_should_contain(&fs_1, "sub-2", vec!["file-1"]);

    // Let's do some more complicated changes
    fs_1.create_file("sub-1/file-2").unwrap();
    fs_2.remove_file("sub-1/file-1").unwrap();
    fs_2.remove_file("sub-2/file-1").unwrap();

    // Index all
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();
    data_store_3.perform_full_scan().unwrap();

    // Get all changes to store 3
    data_store_3
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
    data_store_3
        .sync_from_other_store_panic_conflicts(&data_store_2, &RelativePath::from_path(""))
        .unwrap();
    dir_should_contain(&fs_3, "", vec!["sub-1", "sub-2"]);
    dir_should_contain(&fs_3, "sub-1", vec!["file-2"]);
    dir_should_not_contain(&fs_3, "sub-1", vec!["file-1"]);
    dir_should_not_contain(&fs_3, "sub-2", vec!["file-1"]);

    // Re-create an independent 'sub-1/file-1' on store 2
    fs_2.create_file("sub-1/file-1").unwrap();
    data_store_2.perform_full_scan().unwrap();

    // Sync back (we expect to have all changes from store 2 and the file-1 still exists)
    data_store_3
        .sync_from_other_store_panic_conflicts(&data_store_2, &RelativePath::from_path(""))
        .unwrap();
    dir_should_contain(&fs_3, "", vec!["sub-1", "sub-2"]);
    dir_should_contain(&fs_3, "sub-1", vec!["file-2", "file-1"]);
    dir_should_not_contain(&fs_3, "sub-2", vec!["file-1"]);
}

#[test]
fn multi_target_sync_with_ignores() {
    let (fs_1, mut data_store_1) = create_in_memory_store();
    let (fs_2, mut data_store_2) = create_in_memory_store();
    let (fs_3, data_store_3) = create_in_memory_store();

    // Two stores have a 'split' data set, i.e. they contain exclusive files (think sharding data).
    fs_1.create_file("file-1").unwrap();
    fs_1.create_file("file-2").unwrap();
    fs_2.create_file("file-3").unwrap();
    fs_2.create_file("file-4").unwrap();

    // Now comes the interesting part. The two stores enforce th sharding, the ignore the other data.
    data_store_1.add_scan_ignore_rule("/file-3", false).unwrap();
    data_store_1.add_scan_ignore_rule("/file-4", false).unwrap();
    data_store_2.add_scan_ignore_rule("/file-1", false).unwrap();
    data_store_2.add_scan_ignore_rule("/file-2", false).unwrap();

    // Index all.
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();
    data_store_3.perform_full_scan().unwrap();

    // Syncing 1 and 2 up should leave both with only their initial data, but ignore notices about
    // the other stores data.
    data_store_1
        .sync_from_other_store_panic_conflicts(&data_store_2, &RelativePath::from_path(""))
        .unwrap();
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
    dir_should_not_contain(&fs_1, "", vec!["file-3", "file-4"]);
    dir_should_not_contain(&fs_2, "", vec!["file-1", "file-2"]);

    // Here comes the 'tricky' part:
    // When we now sync from 2 -> 3, 2 will only send the two local files it holds to 3,
    // as it can not provide the ignored files. The sync algorithm must be sure to NOT update
    // the parent sync time at this point, as this could prevent a future sync from 1 -> 3.
    data_store_3
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
    dir_should_contain(&fs_3, "", vec!["file-1", "file-2"]);
    data_store_3
        .sync_from_other_store_panic_conflicts(&data_store_2, &RelativePath::from_path(""))
        .unwrap();
    dir_should_contain(&fs_3, "", vec!["file-1", "file-2", "file-3", "file-4"]);

    // Syncing back out from 3 should work.
    fs_3.create_file("file-5").unwrap();
    fs_3.remove_file("file-1").unwrap();
    fs_3.remove_file("file-3").unwrap();
    data_store_3.perform_full_scan().unwrap();

    // 3 -> 1 should propagate ALL changes (also deletion of file-3).
    data_store_1
        .sync_from_other_store_panic_conflicts(&data_store_3, &RelativePath::from_path(""))
        .unwrap();
    dir_should_contain(&fs_1, "", vec!["file-2", "file-5"]);
    dir_should_not_contain(&fs_1, "", vec!["file-1", "file-3", "file-4"]);

    // 1 -> 2 should thus propagate the deletion of file-3.
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
    dir_should_contain(&fs_2, "", vec!["file-4", "file-5"]);
    dir_should_not_contain(&fs_2, "", vec!["file-1", "file-2", "file-3"]);

    // TODO: We still have a problematic state.
    //       If we sync from 1 -> 3 multiple times, without ever syncing from 2 -> 3,
    //       store 3 will never update its sync time of the folder, thus always try to re-fetch
    //       data from store 1.
    //       This is mostly about performance, thus we postpone it for now.
    //       In the future, we need a scheme to partially update the parents sync time for
    //       ignored items. However, this opens a whole new discussion on how to handle ignores, ...
    //       The hard part about all these operations are implicit deletion notices.
    //       We must NEVER increase the parent sync time without having all entries in it,
    //       as a missing entry with a higher sync time creates an implicit deletion.
}

fn create_synced_base_state() -> (
    (InMemoryFS, DataStore<InMemoryFS>),
    (InMemoryFS, DataStore<InMemoryFS>),
) {
    let (fs_1, data_store_1) = create_in_memory_store();
    let (fs_2, data_store_2) = create_in_memory_store();

    fs_1.create_file("file-1").unwrap();
    fs_1.test_set_file_content("file-1", "start", true).unwrap();

    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();

    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();

    ((fs_1, data_store_1), (fs_2, data_store_2))
}

fn create_synced_base_state_folders() -> (
    (InMemoryFS, DataStore<InMemoryFS>),
    (InMemoryFS, DataStore<InMemoryFS>),
) {
    let (fs_1, data_store_1) = create_in_memory_store();
    let (fs_2, data_store_2) = create_in_memory_store();

    fs_1.create_dir("sub", true).unwrap();
    fs_1.create_file("sub/file-1").unwrap();
    fs_1.test_set_file_content("sub/file-1", "start", true)
        .unwrap();

    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();

    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();

    ((fs_1, data_store_1), (fs_2, data_store_2))
}

// CASE 1: Two different, concurrently changed file versions on both data stores.
//         Choose the remote item.
#[test]
fn sync_with_conflicts_01() {
    let ((fs_1, data_store_1), (fs_2, data_store_2)) = create_synced_base_state();

    // Two independent changes -> this should result in conflict on sync
    fs_1.test_set_file_content("file-1", "fs_1", true).unwrap();
    fs_2.test_set_file_content("file-1", "fs_2", true).unwrap();
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();

    // Sync from 1 -> 2. No resolution, nothing should happen.
    let mut conflict_happened = false;
    data_store_2
        .sync_from_other_store(&data_store_1, &RelativePath::from_path(""), &mut |event| {
            conflict_happened = matches!(event, SyncConflictEvent::LocalItemRemoteFile(_, _));
            SyncConflictResolution::DoNotResolve
        })
        .unwrap();
    assert!(conflict_happened);
    assert_eq!(fs_2.test_get_file_content("file-1").unwrap(), "fs_2");

    // Sync from 1 -> 2. Resolution, we should choose the remote item now.
    let mut conflict_happened = false;
    data_store_2
        .sync_from_other_store(&data_store_1, &RelativePath::from_path(""), &mut |event| {
            conflict_happened = matches!(event, SyncConflictEvent::LocalItemRemoteFile(_, _));
            SyncConflictResolution::ChooseRemoteItem
        })
        .unwrap();
    assert!(conflict_happened);
    assert_eq!(fs_2.test_get_file_content("file-1").unwrap(), "fs_1");

    // A second sync SHOULD NOT be a conflict.
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();

    // Sync from 2 -> 1 should result in 2 propagating the its choice of keeping the fs_1 version.
    data_store_1
        .sync_from_other_store_panic_conflicts(&data_store_2, &RelativePath::from_path(""))
        .unwrap();
    assert_eq!(fs_1.test_get_file_content("file-1").unwrap(), "fs_1");
}

// CASE 2: Two different, concurrently changed file versions on both data stores.
//         Choose the local item.
#[test]
fn sync_with_conflicts_02() {
    let ((fs_1, data_store_1), (fs_2, data_store_2)) = create_synced_base_state();

    // Two independent changes -> this should result in conflict on sync.
    fs_1.test_set_file_content("file-1", "fs_1", true).unwrap();
    fs_2.test_set_file_content("file-1", "fs_2", true).unwrap();
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();

    // Sync from 1 -> 2. Choose the local item.
    let mut conflict_happened = false;
    data_store_2
        .sync_from_other_store(&data_store_1, &RelativePath::from_path(""), &mut |event| {
            conflict_happened = matches!(event, SyncConflictEvent::LocalItemRemoteFile(_, _));
            SyncConflictResolution::ChooseLocalItem
        })
        .unwrap();
    assert!(conflict_happened);
    assert_eq!(fs_2.test_get_file_content("file-1").unwrap(), "fs_2");

    // We chose our local file. Make a further change and move the data back to store 1.
    // This should work, as we know about all changes in 1 and then deliberately change it.
    fs_2.test_set_file_content("file-1", "change_after_resolution", true)
        .unwrap();
    data_store_2.perform_full_scan().unwrap();

    // Sync from 2 -> 1. Should work without conflict.
    data_store_1
        .sync_from_other_store_panic_conflicts(&data_store_2, &RelativePath::from_path(""))
        .unwrap();
    assert_eq!(
        fs_1.test_get_file_content("file-1").unwrap(),
        "change_after_resolution"
    );
}

// CASE 3: The local store deletes a file, the remote concurrently modifies it.
//         Choose the remote item.
#[test]
fn sync_with_conflicts_03() {
    let ((fs_1, data_store_1), (fs_2, data_store_2)) = create_synced_base_state();

    // Now lets remove the file on one store 1 and modify it on store 2. This should conflict.
    fs_1.remove_file("file-1").unwrap();
    fs_2.test_set_file_content("file-1", "fs_2", true).unwrap();
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();

    // Sync form 2 -> 1. We choose the remote item on fs_2.
    // Resolution, we should choose the remote item on fs_2.
    let mut conflict_happened = false;
    data_store_1
        .sync_from_other_store(&data_store_2, &RelativePath::from_path(""), &mut |event| {
            conflict_happened = matches!(event, SyncConflictEvent::LocalDeletionRemoteFile(_, _));
            SyncConflictResolution::ChooseRemoteItem
        })
        .unwrap();
    assert!(conflict_happened);
    assert_eq!(fs_1.test_get_file_content("file-1").unwrap(), "fs_2");

    // Further syncs should just work fine.
    data_store_1
        .sync_from_other_store_panic_conflicts(&data_store_2, &RelativePath::from_path(""))
        .unwrap();

    fs_1.test_set_file_content("file-1", "fs_1", true).unwrap();
    data_store_1.perform_full_scan().unwrap();
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
    assert_eq!(fs_2.test_get_file_content("file-1").unwrap(), "fs_1");
}

// CASE 4: The local store deletes a file, the remote concurrently modifies it.
//         Choose the local item.
#[test]
fn sync_with_conflicts_04() {
    let ((fs_1, data_store_1), (fs_2, data_store_2)) = create_synced_base_state();

    // Now lets remove the file on one store 1 and modify it on store 2. This should conflict.
    fs_1.remove_file("file-1").unwrap();
    fs_2.test_set_file_content("file-1", "fs_2", true).unwrap();
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();

    // Sync form 2 -> 1, should have a conflict.
    // Resolution, we should choose the local deletion on fs_1.
    let mut conflict_happened = false;
    data_store_1
        .sync_from_other_store(&data_store_2, &RelativePath::from_path(""), &mut |event| {
            conflict_happened = matches!(event, SyncConflictEvent::LocalDeletionRemoteFile(_, _));
            SyncConflictResolution::ChooseLocalItem
        })
        .unwrap();
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
    assert!(conflict_happened);
    dir_should_not_contain(&fs_1, "", vec!["file-1"]);
    dir_should_not_contain(&fs_2, "", vec!["file-1"]);
}

// CASE 5: The local store modifies a file, the remote concurrently deletes it.
//         Choose the local item.
#[test]
fn sync_with_conflicts_05() {
    let ((fs_1, data_store_1), (fs_2, data_store_2)) = create_synced_base_state();

    // Now lets remove the file on one store 1 and modify it on store 2. This should conflict.
    fs_1.test_set_file_content("file-1", "fs_1", true).unwrap();
    fs_2.remove_file("file-1").unwrap();
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();

    // Sync form 2 -> 1, should have a conflict.
    // Resolution, we should choose the local change on fs_1.
    let mut conflict_happened = false;
    data_store_1
        .sync_from_other_store(&data_store_2, &RelativePath::from_path(""), &mut |event| {
            conflict_happened = matches!(event, SyncConflictEvent::LocalItemRemoteDeletion(_, _));
            SyncConflictResolution::ChooseLocalItem
        })
        .unwrap();
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
    assert!(conflict_happened);
    assert_eq!(fs_1.test_get_file_content("file-1").unwrap(), "fs_1");
    assert_eq!(fs_2.test_get_file_content("file-1").unwrap(), "fs_1");
}

// CASE 6: The local store modifies a file, the remote concurrently deletes it.
//         Choose the remote item.
#[test]
fn sync_with_conflicts_06() {
    let ((fs_1, data_store_1), (fs_2, data_store_2)) = create_synced_base_state();

    // Now lets remove the file on one store 1 and modify it on store 2. This should conflict.
    fs_1.test_set_file_content("file-1", "fs_1", true).unwrap();
    fs_2.remove_file("file-1").unwrap();
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();

    // Sync form 2 -> 1, should have a conflict.
    // Resolution, we should choose the local change on fs_1.
    let mut conflict_happened = false;
    data_store_1
        .sync_from_other_store(&data_store_2, &RelativePath::from_path(""), &mut |event| {
            conflict_happened = matches!(event, SyncConflictEvent::LocalItemRemoteDeletion(_, _));
            SyncConflictResolution::ChooseRemoteItem
        })
        .unwrap();
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
    assert!(conflict_happened);
    dir_should_not_contain(&fs_1, "", vec!["file-1"]);
    dir_should_not_contain(&fs_2, "", vec!["file-1"]);
}

// CASE 7: The local store deletes a folder, the remote concurrently modifies it.
//         Choose the local item.
#[test]
fn sync_with_conflicts_07() {
    let ((fs_1, data_store_1), (fs_2, data_store_2)) = create_synced_base_state_folders();

    // Now lets remove the folder on one store 1 and modify it on store 2. This should conflict.
    fs_1.remove_dir_recursive("sub").unwrap();
    fs_2.test_set_file_content("sub/file-1", "fs_2", true)
        .unwrap();
    fs_2.create_file("sub/file-2").unwrap();
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();

    // Sync form 2 -> 1, should have a conflict.
    // Resolution, we should choose the local deletion on fs_1.
    let mut conflict_happened = false;
    data_store_1
        .sync_from_other_store(&data_store_2, &RelativePath::from_path(""), &mut |event| {
            conflict_happened = matches!(event, SyncConflictEvent::LocalDeletionRemoteFolder(_, _));
            SyncConflictResolution::ChooseLocalItem
        })
        .unwrap();
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
    assert!(conflict_happened);
    dir_should_not_contain(&fs_1, "", vec!["sub-1"]);
    dir_should_not_contain(&fs_2, "", vec!["sub-1"]);
}

// CASE 8: The local store modifies a folder, the remote concurrently deletes it.
//         Choose the remote item.
#[test]
fn sync_with_conflicts_08() {
    let ((fs_1, data_store_1), (fs_2, data_store_2)) = create_synced_base_state_folders();

    // Now lets remove the folder on one store 1 and modify it on store 2. This should conflict.
    fs_1.remove_dir_recursive("sub").unwrap();
    fs_2.test_set_file_content("sub/file-1", "fs_2", true)
        .unwrap();
    fs_2.create_file("sub/file-2").unwrap();
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();

    // Sync form 2 -> 1, should have a conflict.
    // Resolution, we should choose the remote change on fs_2.
    let mut conflict_happened = false;
    data_store_1
        .sync_from_other_store(&data_store_2, &RelativePath::from_path(""), &mut |event| {
            conflict_happened |=
                matches!(event, SyncConflictEvent::LocalDeletionRemoteFolder(_, _));
            SyncConflictResolution::ChooseRemoteItem
        })
        .unwrap();
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
    assert!(conflict_happened);
    dir_should_contain(&fs_1, "", vec!["sub"]);
    dir_should_contain(&fs_2, "", vec!["sub"]);
    dir_should_contain(&fs_1, "sub", vec!["file-1", "file-2"]);
    dir_should_contain(&fs_2, "sub", vec!["file-1", "file-2"]);
    assert_eq!(fs_1.test_get_file_content("sub/file-1").unwrap(), "fs_2");
    assert_eq!(fs_2.test_get_file_content("sub/file-1").unwrap(), "fs_2");
}

// CASE 9: The local store has a file, the remote concurrently modifies it to be a folder.
//         Choose the local item.
#[test]
fn sync_with_conflicts_09() {
    let ((fs_1, data_store_1), (fs_2, data_store_2)) = create_synced_base_state();

    // Now lets modify the item on one store 1 and  change it to a folder on store 2. This should conflict.
    fs_1.test_set_file_content("file-1", "fs_1", true).unwrap();
    fs_2.remove_file("file-1").unwrap();
    fs_2.create_dir("file-1", false).unwrap();
    fs_2.create_file("file-1/file-2").unwrap();
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();

    // Sync form 2 -> 1, should have a conflict.
    // Resolution, we should choose the local deletion on fs_1.
    let mut conflict_happened = false;
    data_store_1
        .sync_from_other_store(&data_store_2, &RelativePath::from_path(""), &mut |event| {
            conflict_happened = matches!(event, SyncConflictEvent::LocalFileRemoteFolder(_, _));
            SyncConflictResolution::ChooseLocalItem
        })
        .unwrap();
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
    assert!(conflict_happened);
    assert_eq!(fs_1.test_get_file_content("file-1").unwrap(), "fs_1");
    assert_eq!(fs_2.test_get_file_content("file-1").unwrap(), "fs_1");
}

// CASE 10: The local store has a file, the remote concurrently modifies it to be a folder.
//         Choose the remote item.
#[test]
fn sync_with_conflicts_10() {
    let ((fs_1, data_store_1), (fs_2, data_store_2)) = create_synced_base_state();

    // Now lets modify the item on one store 1 and  change it to a folder on store 2. This should conflict.
    fs_1.test_set_file_content("file-1", "fs_1", true).unwrap();
    fs_2.remove_file("file-1").unwrap();
    fs_2.create_dir("file-1", false).unwrap();
    fs_2.create_file("file-1/file-2").unwrap();
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();

    // Sync form 2 -> 1, should have a conflict.
    // Resolution, we should choose the remote change on fs_2.
    let mut conflict_happened = false;
    data_store_1
        .sync_from_other_store(&data_store_2, &RelativePath::from_path(""), &mut |event| {
            conflict_happened = matches!(event, SyncConflictEvent::LocalFileRemoteFolder(_, _));
            SyncConflictResolution::ChooseRemoteItem
        })
        .unwrap();
    data_store_2
        .sync_from_other_store_panic_conflicts(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
    assert!(conflict_happened);
    dir_should_contain(&fs_1, "", vec!["file-1"]);
    dir_should_contain(&fs_1, "file-1", vec!["file-2"]);
    dir_should_contain(&fs_2, "", vec!["file-1"]);
    dir_should_contain(&fs_2, "file-1", vec!["file-2"]);
}

#[test]
fn convert_from_and_to_external_version_vectors() {
    let (_fs_1, data_store_1) = create_in_memory_store();
    let (_fs_2, data_store_2) = create_in_memory_store();

    let data_store_1_name = data_store_1
        .db_access
        .get_local_data_store()
        .unwrap()
        .unique_name;

    let (mapper_1, mapper_2) = data_store_1.sync_data_store_lists(&data_store_2).unwrap();

    // Create a vector local to store 1
    let data_store_1_id = data_store_1
        .db_access
        .get_data_store(&data_store_1_name)
        .unwrap()
        .unwrap()
        .id;
    let mut vector_on_store_1 = VersionVector::new();
    vector_on_store_1[&data_store_1_id] = 42;

    // Simulate the 'externalize and internalize' procedure to transfer it to store 2.
    let internalized_vector_on_store_2 = mapper_2.external_to_internal(&vector_on_store_1);

    // Check it...
    let data_store_2_id = data_store_2
        .db_access
        .get_data_store(&data_store_1_name)
        .unwrap()
        .unwrap()
        .id;
    assert_eq!(internalized_vector_on_store_2[&data_store_2_id], 42);

    // Transfer it back
    let internalized_vector_on_store_1 =
        mapper_1.external_to_internal(&internalized_vector_on_store_2);
    assert_eq!(internalized_vector_on_store_1[&data_store_1_id], 42);
}
