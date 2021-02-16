use data_store::synchronization_messages::*;
use metadata_db::DBItem;

pub enum SyncConflictResolution {
    ChooseLocalItem,
    ChooseRemoteItem,
    DoNotResolve,
}

pub enum SyncConflictEvent<'a> {
    LocalFileRemoteFolder(&'a DBItem, &'a IntFolderSyncContent),

    LocalDeletionRemoteFile(&'a DBItem, &'a IntFileSyncContent),
    LocalDeletionRemoteFolder(&'a DBItem, &'a IntFolderSyncContent),

    LocalItemRemoteFile(&'a DBItem, &'a IntFileSyncContent),
    LocalItemRemoteDeletion(&'a DBItem, &'a IntDeletionSyncContent),
}
