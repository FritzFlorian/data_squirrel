use super::FileSystemMetadata;
use super::Item;
use super::ModMetadata;
use super::PathComponent;

use version_vector::VersionVector;

/// DB-Internal representation of an entry loaded from the DB.
/// Depending on the synchronization/deletion status, this might,
/// e.g. not have any metadata assigned to it.
/// The 'defining' factor for an db entry to be valid is that we have an owner information.
#[derive(Clone)]
pub struct DBItemInternal {
    pub path_component: PathComponent,
    pub item: Item,

    pub fs_metadata: Option<FileSystemMetadata>,
    pub mod_metadata: Option<ModMetadata>,

    pub mod_time: Option<VersionVector<i64>>,
    pub sync_time: Option<VersionVector<i64>>,
}
impl DBItemInternal {
    pub fn from_db_query(
        path: PathComponent,
        item: Item,
        fs_metadata: Option<FileSystemMetadata>,
        mod_metadata: Option<ModMetadata>,
    ) -> Self {
        Self {
            path_component: path,
            item: item,
            fs_metadata: fs_metadata,
            mod_metadata: mod_metadata,
            mod_time: None,
            sync_time: None,
        }
    }
}

// Represents a local item stored in the DB.
// We ONLY return this to external actors in a fully loaded state.
#[derive(Clone)]
pub struct DBItem {
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
        mod_time: VersionVector<i64>,
    },
}
#[derive(Clone)]
pub struct ItemFSMetadata {
    pub case_sensitive_name: String,
    pub creation_time: chrono::NaiveDateTime,
    pub mod_time: chrono::NaiveDateTime,
    pub hash: String,
}

impl DBItem {
    pub fn from_internal_item(item: DBItemInternal) -> Self {
        let item_type = if item.item.is_deleted {
            ItemType::DELETION
        } else {
            // Query the creation and last modification info from the metadata.
            // (NOTE: this function expects a FULL item, i.e. all info should be present)
            let mut meta_creation_time = VersionVector::new();
            meta_creation_time[&item.mod_metadata.as_ref().unwrap().creator_store_id] =
                item.mod_metadata.as_ref().unwrap().creator_store_time;
            let mut meta_last_mod_time = VersionVector::new();
            meta_last_mod_time[&item.mod_metadata.as_ref().unwrap().last_mod_store_id] =
                item.mod_metadata.as_ref().unwrap().last_mod_store_time;

            if item.item.is_file {
                ItemType::FILE {
                    metadata: Self::internal_to_external_metadata(item.fs_metadata.unwrap()),
                    creation_time: meta_creation_time,
                    last_mod_time: meta_last_mod_time,
                }
            } else {
                // Only folders have a max_mod_time attribute.
                ItemType::FOLDER {
                    metadata: Self::internal_to_external_metadata(item.fs_metadata.unwrap()),
                    creation_time: meta_creation_time,
                    mod_time: item.mod_time.unwrap(),
                    last_mod_time: meta_last_mod_time,
                }
            }
        };

        Self {
            path_component: item.path_component.path_component.to_owned(),
            sync_time: item.sync_time.unwrap(),
            content: item_type,
        }
    }
    fn internal_to_external_metadata(metadata: FileSystemMetadata) -> ItemFSMetadata {
        ItemFSMetadata {
            case_sensitive_name: metadata.case_sensitive_name,

            mod_time: metadata.mod_time,
            creation_time: metadata.creation_time,
            hash: metadata.hash,
        }
    }

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

    pub fn mod_time(&self) -> &VersionVector<i64> {
        match &self.content {
            ItemType::FILE { last_mod_time, .. } => last_mod_time,
            ItemType::FOLDER { mod_time, .. } => mod_time,
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

    pub fn last_mod_store_id(&self) -> i64 {
        *self.last_mod_time().iter().next().as_ref().unwrap().0
    }
    pub fn last_mod_store_time(&self) -> i64 {
        *self.last_mod_time().iter().next().as_ref().unwrap().1
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
