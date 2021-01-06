use super::DataItem;
use super::Metadata;
use super::OwnerInformation;

use version_vector::VersionVector;

/// DB-Internal representation of an entry loaded from the DB.
/// Depending on the synchronization/deletion status, this might,
/// e.g. not have any metadata assigned to it.
/// The 'defining' factor for an db entry to be valid is that we have an owner information.
pub struct ItemInternal {
    pub data_item: DataItem,
    pub owner_info: OwnerInformation,
    pub metadata: Option<Metadata>,
    pub mod_time: Option<VersionVector<i64>>,
    pub sync_time: Option<VersionVector<i64>>,
}
impl ItemInternal {
    pub fn from_db_query(item: DataItem, owner: OwnerInformation, meta: Option<Metadata>) -> Self {
        Self {
            data_item: item,
            owner_info: owner,
            metadata: meta,
            mod_time: None,
            sync_time: None,
        }
    }
}

// Represents a local item stored in the DB.
// We ONLY return this to external actors in a fully loaded state.
#[derive(Clone)]
pub struct Item {
    pub path_component: String,
    pub sync_time: VersionVector<i64>,

    pub content: ItemType,
    // TODO: add ignore status
}
#[derive(Clone)]
pub enum ItemType {
    DELETION,
    FILE {
        metadata: ItemFSMetadata,
        creation_time: VersionVector<i64>,
        last_mod_time: VersionVector<i64>,
    },
    FOLDER {
        metadata: ItemFSMetadata,
        creation_time: VersionVector<i64>,
        last_mod_time: VersionVector<i64>,

        max_mod_time: VersionVector<i64>,
    },
}
#[derive(Clone)]
pub struct ItemFSMetadata {
    pub case_sensitive_name: String,
    pub creation_time: chrono::NaiveDateTime,
    pub mod_time: chrono::NaiveDateTime,
    pub hash: String,
}

impl Item {
    pub fn is_deletion(&self) -> bool {
        matches!(self.content, ItemType::DELETION { .. })
    }

    pub fn is_file(&self) -> bool {
        matches!(self.content, ItemType::FILE { .. })
    }

    pub fn is_folder(&self) -> bool {
        matches!(self.content, ItemType::FOLDER{ .. })
    }

    pub fn last_mod_time(&self) -> &VersionVector<i64> {
        match &self.content {
            ItemType::FILE { last_mod_time, .. } => last_mod_time,
            ItemType::FOLDER { last_mod_time, .. } => last_mod_time,
            ItemType::DELETION { .. } => panic!("Must not query mod_time of deletion notice!"),
        }
    }

    pub fn max_mod_time(&self) -> &VersionVector<i64> {
        match &self.content {
            ItemType::FILE { last_mod_time, .. } => last_mod_time,
            ItemType::FOLDER { max_mod_time, .. } => max_mod_time,
            ItemType::DELETION { .. } => panic!("Must not query mod_time of deletion notice!"),
        }
    }

    pub fn creation_time(&self) -> &VersionVector<i64> {
        match &self.content {
            ItemType::FILE { creation_time, .. } => creation_time,
            ItemType::FOLDER { creation_time, .. } => creation_time,
            ItemType::DELETION { .. } => panic!("Must not query creation time of deletion notice!"),
        }
    }

    pub fn creation_store_id(&self) -> i64 {
        *self.creation_time().iter().next().as_ref().unwrap().0
    }
    pub fn creation_store_time(&self) -> i64 {
        *self.creation_time().iter().next().as_ref().unwrap().1
    }

    pub fn metadata(&self) -> &ItemFSMetadata {
        match &self.content {
            ItemType::FILE { metadata, .. } => metadata,
            ItemType::FOLDER { metadata, .. } => metadata,
            _ => panic!("Must not query metadata of deletion notice!"),
        }
    }
    pub fn metadata_mut(&mut self) -> &mut ItemFSMetadata {
        match &mut self.content {
            ItemType::FILE { metadata, .. } => metadata,
            ItemType::FOLDER { metadata, .. } => metadata,
            _ => panic!("Must not query metadata of deletion notice!"),
        }
    }
}
