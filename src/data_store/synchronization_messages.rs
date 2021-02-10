use crate::version_vector::VersionVector;
use fs_interaction::relative_path::RelativePath;

use metadata_db;
use metadata_db::ItemFSMetadata;
use metadata_db::MetadataDB;
use std::collections::HashMap;

/// Handshake message before the actual sync procedure starts running.
pub struct SyncHandshake {
    pub data_set_name: String,
    pub data_stores: Vec<metadata_db::DataStore>,
}
/// Mapper to translate remote data store IDs into local data store IDs.
/// This is required to understand the sync and version vectors given by the other store.
pub struct DataStoreIDMapper {
    ext_to_int: HashMap<i64, i64>,
}
impl DataStoreIDMapper {
    pub fn create_mapper(local_db: &MetadataDB, remote: SyncHandshake) -> super::Result<Self> {
        let mut ext_to_int = HashMap::with_capacity(remote.data_stores.len());

        for remote_data_store in remote.data_stores {
            let local_data_store = local_db
                .get_data_store(&remote_data_store.unique_name)?
                .unwrap();
            ext_to_int.insert(remote_data_store.id, local_data_store.id);
        }

        Ok(Self { ext_to_int })
    }

    pub fn external_to_internal(&self, ext_vector: &VersionVector<i64>) -> VersionVector<i64> {
        let mut result = VersionVector::new();
        for (id, time) in ext_vector.iter() {
            result[&self.ext_to_int[id]] = *time;
        }

        result
    }
}

/// Send this request to synchronize an item with a target data store.
/// It will answer appropriately depending on it's local DB entries, i.e. for a file it only
/// answers with information on the individual file, for a folder it includes it's contents.
pub struct ExtSyncRequest {
    pub item_path: RelativePath,
    pub item_sync_time: VersionVector<i64>,
}

pub struct IntSyncRequest {
    pub item_path: RelativePath,
    pub item_sync_time: VersionVector<i64>,
}

/// Response to a SyncRequest.
/// The answer depends on the type found on the remote end and if it requires synchronization.
pub struct ExtSyncResponse {
    pub sync_time: VersionVector<i64>,
    pub action: ExtSyncAction,
}
pub enum ExtSyncAction {
    UpToDate,
    UpdateRequired(ExtSyncContent),
}
pub enum ExtSyncContent {
    Deletion(ExtDeletionSyncContent),
    File(ExtFileSyncContent),
    Folder(ExtFolderSyncContent),
    Ignore(ExtIgnoreSyncContent),
}
pub struct ExtDeletionSyncContent {}
pub struct ExtFileSyncContent {
    pub last_mod_time: VersionVector<i64>,
    pub creation_time: VersionVector<i64>,

    pub fs_metadata: ItemFSMetadata,
}
pub struct ExtFolderSyncContent {
    pub last_mod_time: VersionVector<i64>,
    pub creation_time: VersionVector<i64>,

    pub fs_metadata: ItemFSMetadata,
    pub child_items: Vec<String>,
}
pub struct ExtIgnoreSyncContent {
    pub creation_time: VersionVector<i64>,

    pub last_mod_time: VersionVector<i64>,
    pub mod_time: VersionVector<i64>,
}

pub struct IntSyncResponse {
    pub sync_time: VersionVector<i64>,
    pub action: IntSyncAction,
}
pub enum IntSyncAction {
    UpToDate,
    UpdateRequired(IntSyncContent),
}
pub enum IntSyncContent {
    Deletion(IntDeletionSyncContent),
    File(IntFileSyncContent),
    Folder(IntFolderSyncContent),
    Ignore(IntIgnoreSyncContent),
}
pub struct IntDeletionSyncContent {}
pub struct IntFileSyncContent {
    pub last_mod_time: VersionVector<i64>,
    pub creation_time: VersionVector<i64>,

    pub fs_metadata: ItemFSMetadata,
}
pub struct IntFolderSyncContent {
    pub last_mod_time: VersionVector<i64>,
    pub creation_time: VersionVector<i64>,

    pub fs_metadata: ItemFSMetadata,
    pub child_items: Vec<String>,
}
pub struct IntIgnoreSyncContent {
    pub creation_time: VersionVector<i64>,

    pub last_mod_time: VersionVector<i64>,
    pub mod_time: VersionVector<i64>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Boilerplate conversions from internal to external representations.
////////////////////////////////////////////////////////////////////////////////////////////////////

// Sync Request Implementation
impl ExtSyncRequest {
    pub fn internalize(self, mapper: &DataStoreIDMapper) -> IntSyncRequest {
        IntSyncRequest {
            item_path: self.item_path,
            item_sync_time: mapper.external_to_internal(&self.item_sync_time),
        }
    }
}
impl IntSyncRequest {
    pub fn externalize(self, _mapper: &DataStoreIDMapper) -> ExtSyncRequest {
        ExtSyncRequest {
            item_path: self.item_path,
            item_sync_time: self.item_sync_time,
        }
    }
}

// Sync Response Implementation (from local to external)
impl ExtSyncResponse {
    pub fn internalize(self, mapper: &DataStoreIDMapper) -> IntSyncResponse {
        IntSyncResponse {
            sync_time: mapper.external_to_internal(&self.sync_time),
            action: self.action.internalize(&mapper),
        }
    }
}
impl ExtSyncAction {
    pub fn internalize(self, mapper: &DataStoreIDMapper) -> IntSyncAction {
        match self {
            Self::UpToDate => IntSyncAction::UpToDate,
            Self::UpdateRequired(content) => {
                IntSyncAction::UpdateRequired(content.internalize(&mapper))
            }
        }
    }
}
impl ExtSyncContent {
    pub fn internalize(self, mapper: &DataStoreIDMapper) -> IntSyncContent {
        match self {
            Self::Deletion(_content) => IntSyncContent::Deletion(IntDeletionSyncContent {}),
            Self::File(content) => IntSyncContent::File(IntFileSyncContent {
                last_mod_time: mapper.external_to_internal(&content.last_mod_time),
                creation_time: mapper.external_to_internal(&content.creation_time),
                fs_metadata: content.fs_metadata,
            }),
            Self::Folder(content) => IntSyncContent::Folder(IntFolderSyncContent {
                last_mod_time: mapper.external_to_internal(&content.last_mod_time),
                creation_time: mapper.external_to_internal(&content.creation_time),
                fs_metadata: content.fs_metadata,
                child_items: content.child_items,
            }),
            Self::Ignore(content) => IntSyncContent::Ignore(IntIgnoreSyncContent {
                creation_time: mapper.external_to_internal(&content.creation_time),
                last_mod_time: mapper.external_to_internal(&content.last_mod_time),
                mod_time: mapper.external_to_internal(&content.mod_time),
            }),
        }
    }
}

// Sync Response Implementation (from external to local)
impl IntSyncResponse {
    pub fn externalize(self, mapper: &DataStoreIDMapper) -> ExtSyncResponse {
        ExtSyncResponse {
            sync_time: self.sync_time,
            action: self.action.externalize(&mapper),
        }
    }
}
impl IntSyncAction {
    pub fn externalize(self, mapper: &DataStoreIDMapper) -> ExtSyncAction {
        match self {
            Self::UpToDate => ExtSyncAction::UpToDate,
            Self::UpdateRequired(content) => {
                ExtSyncAction::UpdateRequired(content.externalize(&mapper))
            }
        }
    }
}
impl IntSyncContent {
    pub fn externalize(self, _mapper: &DataStoreIDMapper) -> ExtSyncContent {
        match self {
            Self::Deletion(_content) => ExtSyncContent::Deletion(ExtDeletionSyncContent {}),
            Self::File(content) => ExtSyncContent::File(ExtFileSyncContent {
                last_mod_time: content.last_mod_time,
                creation_time: content.creation_time,
                fs_metadata: content.fs_metadata,
            }),
            Self::Folder(content) => ExtSyncContent::Folder(ExtFolderSyncContent {
                last_mod_time: content.last_mod_time,
                creation_time: content.creation_time,
                fs_metadata: content.fs_metadata,
                child_items: content.child_items,
            }),
            Self::Ignore(content) => ExtSyncContent::Ignore(ExtIgnoreSyncContent {
                creation_time: content.creation_time,
                last_mod_time: content.last_mod_time,
                mod_time: content.mod_time,
            }),
        }
    }
}
