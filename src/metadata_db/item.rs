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
