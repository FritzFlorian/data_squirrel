use crate::version_vector::VersionVector;
use fs_interaction::relative_path::RelativePath;

use metadata_db;
use metadata_db::ItemFSMetadata;
use metadata_db::MetadataDB;

// TODO: In general, if we really push this protocol over the network later on optimize out all
//       the not needed strings (especially the ID's in the version vectors can be simplified).
//       The main point to do this is when we go from a local to an external representation and
//       vice versa.
// TODO: Can optimize transferred data in many ways (not full path's, compress sync vectors, ...).
// TODO: On an over-the-wire protocol we probably also want to batch send this stuff, e.g.
//       always include the sync/mod times of directory content.

/// VersionVector used during synchronization.
/// It uses full unique string identifiers for each data_store involved, as the database ID's might
/// differ depending on the exact DB layout. The data_store that uses the pool can then convert
/// this universal/over the wire representation to its local DB equivalent.
///
/// All sync types have a local/internal representation, i.e. they contain ids from our local db,
/// and an external representation, i.e. they contain full, unique string identifiers.
pub type SyncVersionVector = VersionVector<String>;

/// Send this request to synchronize an item with a target data store.
/// It will answer appropriately depending on it's local DB entries, i.e. for a file it only
/// answers with information on the individual file, for a folder it includes it's contents.
pub struct ExtSyncRequest {
    pub item_path: RelativePath,
    pub item_sync_time: SyncVersionVector,
}

pub struct IntSyncRequest {
    pub item_path: RelativePath,
    pub item_sync_time: VersionVector<i64>,
}

/// Response to a SyncRequest.
/// The answer depends on the type found on the remote end and if it requires synchronization.
pub struct ExtSyncResponse {
    pub sync_time: SyncVersionVector,
    pub action: ExtSyncAction,
}
pub enum ExtSyncAction {
    UpToDate,
    UpdateRequired(ExtSyncContent),
}
pub enum ExtSyncContent {
    Deletion,
    File {
        last_mod_time: SyncVersionVector,
        creation_time: SyncVersionVector,

        fs_metadata: ItemFSMetadata,
    },
    Folder {
        last_mod_time: SyncVersionVector,
        creation_time: SyncVersionVector,

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
    pub fn internalize(self, db_access: &MetadataDB) -> metadata_db::Result<IntSyncRequest> {
        Ok(IntSyncRequest {
            item_path: self.item_path,
            item_sync_time: db_access.named_to_id_version_vector(&self.item_sync_time)?,
        })
    }
}
impl IntSyncRequest {
    pub fn externalize(self, db_access: &MetadataDB) -> metadata_db::Result<ExtSyncRequest> {
        Ok(ExtSyncRequest {
            item_path: self.item_path,
            item_sync_time: db_access.id_to_named_version_vector(&self.item_sync_time)?,
        })
    }
}

// Sync Response Implementation (from local to external)
impl ExtSyncResponse {
    pub fn internalize(self, db_access: &MetadataDB) -> metadata_db::Result<IntSyncResponse> {
        Ok(IntSyncResponse {
            sync_time: db_access.named_to_id_version_vector(&self.sync_time)?,
            action: self.action.internalize(&db_access)?,
        })
    }
}
impl ExtSyncAction {
    pub fn internalize(self, db_access: &MetadataDB) -> metadata_db::Result<IntSyncAction> {
        match self {
            Self::UpToDate => Ok(IntSyncAction::UpToDate),
            Self::UpdateRequired(content) => Ok(IntSyncAction::UpdateRequired(
                content.internalize(&db_access)?,
            )),
        }
    }
}
impl ExtSyncContent {
    pub fn internalize(self, db_access: &MetadataDB) -> metadata_db::Result<IntSyncContent> {
        match self {
            Self::Deletion => Ok(IntSyncContent::Deletion),
            Self::File {
                last_mod_time,
                creation_time,
                fs_metadata,
            } => Ok(IntSyncContent::File {
                last_mod_time: db_access.named_to_id_version_vector(&last_mod_time)?,
                creation_time: db_access.named_to_id_version_vector(&creation_time)?,
                fs_metadata,
            }),
            Self::Folder {
                last_mod_time,
                creation_time,
                fs_metadata,
                child_items,
            } => Ok(IntSyncContent::Folder {
                last_mod_time: db_access.named_to_id_version_vector(&last_mod_time)?,
                creation_time: db_access.named_to_id_version_vector(&creation_time)?,
                fs_metadata,
                child_items,
            }),
        }
    }
}

// Sync Response Implementation (from external to local)
impl IntSyncResponse {
    pub fn externalize(self, db_access: &MetadataDB) -> metadata_db::Result<ExtSyncResponse> {
        Ok(ExtSyncResponse {
            sync_time: db_access.id_to_named_version_vector(&self.sync_time)?,
            action: self.action.externalize(&db_access)?,
        })
    }
}
impl IntSyncAction {
    pub fn externalize(self, db_access: &MetadataDB) -> metadata_db::Result<ExtSyncAction> {
        match self {
            Self::UpToDate => Ok(ExtSyncAction::UpToDate),
            Self::UpdateRequired(content) => Ok(ExtSyncAction::UpdateRequired(
                content.externalize(&db_access)?,
            )),
        }
    }
}
impl IntSyncContent {
    pub fn externalize(self, db_access: &MetadataDB) -> metadata_db::Result<ExtSyncContent> {
        match self {
            Self::Deletion => Ok(ExtSyncContent::Deletion),
            Self::File {
                last_mod_time,
                creation_time,
                fs_metadata,
            } => Ok(ExtSyncContent::File {
                last_mod_time: db_access.id_to_named_version_vector(&last_mod_time)?,
                creation_time: db_access.id_to_named_version_vector(&creation_time)?,
                fs_metadata,
            }),
            Self::Folder {
                last_mod_time,
                creation_time,
                fs_metadata,
                child_items,
            } => Ok(ExtSyncContent::Folder {
                last_mod_time: db_access.id_to_named_version_vector(&last_mod_time)?,
                creation_time: db_access.id_to_named_version_vector(&creation_time)?,
                fs_metadata,
                child_items,
            }),
        }
    }
}
