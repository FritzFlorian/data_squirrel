use metadata_db::Metadata;
use version_vector::VersionVector;

pub struct ItemInternal {
    pub data_item: super::DataItem,
    pub owner_info: super::OwnerInformation,
    pub metadata: Option<super::Metadata>,
    pub mod_time: Option<VersionVector<i64>>,
    pub sync_time: Option<VersionVector<i64>>,
}

pub struct Item {
    pub path_component: String,
    pub content: ItemType,
    // TODO: add ignore/sync status
}
pub enum ItemType {
    FILE {
        metadata: Option<super::Metadata>,
        mod_time: VersionVector<i64>,
        sync_time: VersionVector<i64>,
    },
    FOLDER {
        metadata: Option<super::Metadata>,
        mod_time: VersionVector<i64>,
        sync_time: VersionVector<i64>,
    },
    DELETION {
        sync_time: VersionVector<i64>,
    },
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

    pub fn mod_time(&self) -> &VersionVector<i64> {
        match &self.content {
            ItemType::FILE { mod_time, .. } => mod_time,
            ItemType::FOLDER { mod_time, .. } => mod_time,
            ItemType::DELETION { .. } => panic!("Must not query mod_time of deletion notice!"),
        }
    }

    pub fn creation_time(&self) -> VersionVector<i64> {
        match &self.content {
            ItemType::FILE {
                metadata: Some(metadata),
                ..
            } => VersionVector::from_initial_values(vec![(
                &metadata.creator_store_id,
                metadata.creator_store_time,
            )]),
            ItemType::FOLDER {
                metadata: Some(metadata),
                ..
            } => VersionVector::from_initial_values(vec![(
                &metadata.creator_store_id,
                metadata.creator_store_time,
            )]),
            ItemType::DELETION { .. } => panic!("Must not query creation time of deletion notice!"),
            _ => panic!("Must not query metadata on items that do not have it loaded."),
        }
    }

    pub fn sync_time(&self) -> &VersionVector<i64> {
        match &self.content {
            ItemType::FILE { sync_time, .. } => sync_time,
            ItemType::FOLDER { sync_time, .. } => sync_time,
            ItemType::DELETION { sync_time, .. } => sync_time,
        }
    }

    pub fn metadata(&self) -> &Option<Metadata> {
        match &self.content {
            ItemType::FILE { metadata, .. } => metadata,
            ItemType::FOLDER { metadata, .. } => metadata,
            _ => panic!("Must not query metadata of deletion notice!"),
        }
    }
}

impl ItemInternal {
    pub fn from_join_tuple(
        item: super::DataItem,
        owner: super::OwnerInformation,
        meta: Option<super::Metadata>,
    ) -> Self {
        Self {
            data_item: item,
            owner_info: owner,
            metadata: meta,
            mod_time: None,
            sync_time: None,
        }
    }
}
