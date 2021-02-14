use data_store::synchronization_messages::*;
use metadata_db::DBItem;

pub enum SyncConflictResolution {
    ChooseLocalItem,
    ChooseRemoteItem,
    DoNotResolve,
}

pub enum SyncConflictEvent<'a> {
    // Remote has changes on an item that was deleted locally.
    LocalDeletionRemoteFolder(&'a DBItem, &'a IntFolderSyncContent),
    LocalFileRemoteFolder(&'a DBItem, &'a IntFolderSyncContent),
    LocalDeletionRemoteFile(&'a DBItem, &'a IntFileSyncContent),
    LocalFileRemoteFile(&'a DBItem, &'a IntFileSyncContent),
    LocalFileRemoteDeletion(&'a DBItem, &'a IntDeletionSyncContent),
}
