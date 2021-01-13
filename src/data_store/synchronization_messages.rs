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
    Deletion,
    File {
        last_mod_time: VersionVector<i64>,
        creation_time: VersionVector<i64>,

        fs_metadata: ItemFSMetadata,
    },
    Folder {
        last_mod_time: VersionVector<i64>,
        creation_time: VersionVector<i64>,

        fs_metadata: ItemFSMetadata,
        child_items: Vec<String>,
    },
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
    Deletion,
    File {
        last_mod_time: VersionVector<i64>,
        creation_time: VersionVector<i64>,

        fs_metadata: ItemFSMetadata,
    },
    Folder {
        last_mod_time: VersionVector<i64>,
        creation_time: VersionVector<i64>,

        fs_metadata: ItemFSMetadata,
        child_items: Vec<String>,
    },
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
            Self::Deletion => IntSyncContent::Deletion,
            Self::File {
                last_mod_time,
                creation_time,
                fs_metadata,
            } => IntSyncContent::File {
                last_mod_time: mapper.external_to_internal(&last_mod_time),
                creation_time: mapper.external_to_internal(&creation_time),
                fs_metadata,
            },
            Self::Folder {
                last_mod_time,
                creation_time,
                fs_metadata,
                child_items,
            } => IntSyncContent::Folder {
                last_mod_time: mapper.external_to_internal(&last_mod_time),
                creation_time: mapper.external_to_internal(&creation_time),
                fs_metadata,
                child_items,
            },
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
            Self::Deletion => ExtSyncContent::Deletion,
            Self::File {
                last_mod_time,
                creation_time,
                fs_metadata,
            } => ExtSyncContent::File {
                last_mod_time: last_mod_time,
                creation_time: creation_time,
                fs_metadata,
            },
            Self::Folder {
                last_mod_time,
                creation_time,
                fs_metadata,
                child_items,
            } => ExtSyncContent::Folder {
                last_mod_time: last_mod_time,
                creation_time: creation_time,
                fs_metadata,
                child_items,
            },
        }
    }
}
