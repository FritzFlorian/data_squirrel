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
    path_components (id) {
        id -> BigInt,
        parent_id -> Nullable<BigInt>,
        full_path -> Text,
    }
}

table! {
    items (id) {
        id -> BigInt,

        data_store_id -> BigInt,
        path_component_id -> BigInt,

        is_file -> Bool,
        is_deleted -> Bool,
    }
}

table! {
    file_system_metadatas (id) {
        id -> BigInt,

        case_sensitive_name -> Text,
        creation_time -> Timestamp,
        mod_time -> Timestamp,
        hash -> Text,
    }
}

table! {
    mod_metadatas (id) {
        id -> BigInt,

        creator_store_id -> BigInt,
        creator_store_time -> BigInt,

        last_mod_store_id -> BigInt,
        last_mod_store_time -> BigInt,
    }
}

table! {
    mod_times (id) {
        id -> BigInt,
        mod_metadata_id -> BigInt,
        data_store_id -> BigInt,
        time -> BigInt,
    }
}

table! {
    sync_times (id) {
        id -> BigInt,
        item_id -> BigInt,
        data_store_id -> BigInt,
        time -> BigInt,
    }
}

allow_tables_to_appear_in_same_query!(
    path_components,
    data_sets,
    data_stores,
    file_system_metadatas,
    mod_metadatas,
    mod_times,
    items,
    sync_times,
);

joinable!(data_stores -> data_sets(data_set_id));

joinable!(items -> data_stores(data_store_id));
joinable!(items -> path_components(path_component_id));

joinable!(file_system_metadatas -> items(id));

joinable!(mod_metadatas -> items(id));
// Must be done with explicit joins, as both reference the same other table.
joinable!(mod_metadatas -> data_stores(last_mod_store_id));
// joinable!(mod_metadatas -> data_stores(creator_store_id));

joinable!(mod_times -> mod_metadatas(mod_metadata_id));
joinable!(sync_times -> items(item_id));
