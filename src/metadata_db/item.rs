pub struct Item {
    pub data_item: super::DataItem,
    pub owner_info: super::OwnerInformation,
    pub metadata: super::Metadata,
}

impl Item {
    pub fn from_join_tuple(
        item: super::DataItem,
        owner: super::OwnerInformation,
        metadata: super::Metadata,
    ) -> Self {
        Self {
            data_item: item,
            owner_info: owner,
            metadata: metadata,
        }
    }

    pub fn path(&self) -> &str {
        &self.data_item.path
    }

    pub fn creator_store_id(&self) -> i64 {
        self.metadata.creator_store_id
    }
    pub fn creator_store_time(&self) -> i64 {
        self.metadata.creator_store_time
    }

    pub fn is_file(&self) -> bool {
        self.metadata.is_file
    }
    pub fn creation_time(&self) -> chrono::NaiveDateTime {
        self.metadata.creation_time
    }
    pub fn mod_time(&self) -> chrono::NaiveDateTime {
        self.metadata.mod_time
    }
    pub fn hash(&self) -> &str {
        &self.metadata.hash
    }
}
