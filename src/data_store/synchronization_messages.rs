use crate::version_vector::VersionVector;
use std::path::PathBuf;

// TODO: In general, if we really push this protocol over the network later on optimize out all
//       the not needed strings (especially the ID's in the version vectors can be simplified).
//       For now we keep them to keep the system clearer and simpler to debug.
// TODO: Can optimize transferred data in many ways (not full path's, compress sync vectors, ...).

/// VersionVector used during synchronization.
/// It uses full unique string identifiers for each data_store involved, as the database ID's might
/// differ depending on the exact DB layout. The data_store that uses the pool can then convert
/// this universal/over the wire representation to its local DB equivalent.
pub type SyncVersionVector = VersionVector<String>;

/// Send this request to synchronize a directory with a target data store.
/// It will answer with the required information on which items should be synced according to the
/// transferred information on local synchronization times.
pub struct SyncRequest {
    pub dir_path: PathBuf,
    pub dir_sync_time: SyncVersionVector,
    pub dir_items: Vec<SyncRequestItem>,
}
pub struct SyncRequestItem {
    pub item_path: PathBuf,
    pub item_sync_time: SyncVersionVector,
}

/// Response to a SyncRequest.
/// Lists for each item in the directory if it requires any updates.
/// In case updates are required, all required metadata to proceed with the sync is transferred.
pub struct SyncResponse {
    pub dir_path: PathBuf,
    pub dir_items: Vec<SyncResponseItem>,
}
pub struct SyncResponseItem {
    pub item_path: PathBuf,
    pub sync_time: SyncVersionVector,
    pub sync_action: SyncResponseAction,
}
pub enum SyncResponseAction {
    UpToDate,
    // TODO: add more descriptive metadata to display 'nice' conflict messages.
    // TODO: add hash (or even sub-hashes) of the file, to allow for more efficient fetching.
    Changed {
        is_file: bool,
        mod_time: SyncVersionVector,
    },
    Deleted,
    Created {
        is_file: bool,
        creator: SyncVersionVector,
        mod_time: SyncVersionVector,
    },
}
