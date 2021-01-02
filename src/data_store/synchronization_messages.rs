use crate::version_vector::VersionVector;
use fs_interaction::relative_path::RelativePath;
use metadata_db;

// TODO: In general, if we really push this protocol over the network later on optimize out all
//       the not needed strings (especially the ID's in the version vectors can be simplified).
//       For now we keep them to keep the system clearer and simpler to debug.
// TODO: Can optimize transferred data in many ways (not full path's, compress sync vectors, ...).
// TODO: On an over-the-wire protocol we probably also want to batch send this stuff, e.g.
//       always include the sync/mod times of directory content.

/// VersionVector used during synchronization.
/// It uses full unique string identifiers for each data_store involved, as the database ID's might
/// differ depending on the exact DB layout. The data_store that uses the pool can then convert
/// this universal/over the wire representation to its local DB equivalent.
pub type SyncVersionVector = VersionVector<String>;

/// Send this request to synchronize an item directory with a target data store.
/// It will answer appropriately depending on it's local DB entries, i.e. for a file it only
/// answers with information on the individual file, for a folder it includes it's contents.
pub struct SyncRequest {
    pub item_path: RelativePath,
    pub dir_sync_time: SyncVersionVector,
}

/// Response to a SyncRequest.
/// The answer depends on the type found on the remote end and if it requires synchronization.
pub struct SyncResponse {
    pub item_path: RelativePath, // We can easily spare this later on, its just nice for debugging.
    pub sync_time: SyncVersionVector,
    pub action: SyncResponseAction,
}
pub enum SyncResponseAction {
    UpToDate,
    UpdateRequired(SyncUpdateContent),
}
pub enum SyncUpdateContent {
    Deletion,
    File {
        mod_time: SyncVersionVector,
        metadata: metadata_db::Metadata,
    },
    Folder {
        mod_time: SyncVersionVector,
        metadata: metadata_db::Metadata,
        child_items: Vec<String>,
    },
}
