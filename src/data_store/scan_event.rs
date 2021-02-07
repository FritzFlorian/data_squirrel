use fs_interaction::{DataItem, Issue};
use metadata_db::DBItem;

pub enum ScanEvent<'a> {
    UnchangedFile(&'a DataItem, &'a DBItem),
    UnchangedFolder(&'a DataItem, &'a DBItem),

    NewFile(&'a DataItem),
    NewFolder(&'a DataItem),

    ChangedFile(&'a DataItem, &'a DBItem),
    ChangedFolder(&'a DataItem, &'a DBItem),

    ChangedFolderToFile(&'a DataItem, &'a DBItem),
    ChangedFileToFolder(&'a DataItem, &'a DBItem),

    DeletedItem(&'a DBItem),
    IgnoredNewItem(&'a DataItem),
    IgnoredExistingItem(&'a DataItem),

    IssueBitRot {
        fs_item: &'a DataItem,
        db_hash: &'a str,
        fs_hash: &'a str,
    },
    IssueSkipLink(&'a DataItem),
    IssueOther(&'a DataItem, &'a Issue),
}
