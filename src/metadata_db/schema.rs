table! {
    data_items (id) {
        id -> BigInt,
        creator_store_id -> Integer,
        creator_version -> Integer,
        parent_item_id -> Nullable<Integer>,
        path -> Text,
        is_file -> Integer,
    }
}

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
        version -> BigInt,
    }
}

table! {
    item_metadatas (id) {
        id -> BigInt,
        data_store_id -> Integer,
        creation_time -> Text,
        mod_time -> Text,
        hash -> Text,
    }
}

table! {
    mod_times (id) {
        id -> BigInt,
        owner_information_id -> Integer,
        data_store_id -> Integer,
        time -> Integer,
    }
}

table! {
    owner_informations (id) {
        id -> BigInt,
        data_store_id -> Integer,
        data_item_id -> Integer,
    }
}

table! {
    sync_times (id) {
        id -> BigInt,
        owner_information_id -> Integer,
        data_store_id -> Integer,
        time -> Integer,
    }
}

allow_tables_to_appear_in_same_query!(
    data_items,
    data_sets,
    data_stores,
    item_metadatas,
    mod_times,
    owner_informations,
    sync_times,
);
