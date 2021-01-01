table! {
    data_sets (id) {
        id -> BigInt,
        unique_name -> Text,
        human_name -> Text,
    }
}

table! {
    data_stores (id) {
        id -> BigInt,
        data_set_id -> BigInt,

        unique_name -> Text,
        human_name -> Text,
        creation_date -> Timestamp,
        path_on_device -> Text,
        location_note -> Text,

        is_this_store -> Bool,
        time -> BigInt,
    }
}

table! {
    data_items (id) {
        id -> BigInt,

        parent_item_id -> Nullable<BigInt>,
        path_component -> Text,
    }
}

table! {
    owner_informations (id) {
        id -> BigInt,

        data_store_id -> BigInt,
        data_item_id -> BigInt,

        is_file -> Bool,
        is_deleted -> Bool,
    }
}

table! {
    metadatas (id) {
        id -> BigInt,
        owner_information_id -> BigInt,

        creator_store_id -> BigInt,
        creator_store_time -> BigInt,

        case_sensitive_name -> Text,
        creation_time -> Timestamp,
        mod_time -> Timestamp,
        hash -> Text,
    }
}

table! {
    mod_times (id) {
        id -> BigInt,
        owner_information_id -> BigInt,
        data_store_id -> BigInt,
        time -> BigInt,
    }
}

table! {
    sync_times (id) {
        id -> BigInt,
        owner_information_id -> BigInt,
        data_store_id -> BigInt,
        time -> BigInt,
    }
}

allow_tables_to_appear_in_same_query!(
    data_items,
    data_sets,
    data_stores,
    metadatas,
    mod_times,
    owner_informations,
    sync_times,
);

joinable!(data_stores -> data_sets(data_set_id));
// Can not use implicit self joins for data_items -> parent_item_id
joinable!(owner_informations -> data_stores(data_store_id));
joinable!(owner_informations -> data_items(data_item_id));
joinable!(metadatas -> owner_informations(owner_information_id));
joinable!(metadatas -> data_stores(creator_store_id));
joinable!(mod_times -> owner_informations(owner_information_id));
joinable!(sync_times -> owner_informations(owner_information_id));
