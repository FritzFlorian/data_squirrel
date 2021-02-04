use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::serialize::{self, Output, ToSql};
use diesel::sql_types::*;
use std::io::Write;

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, FromSqlRow, AsExpression)]
#[sql_type = "Integer"]
pub enum FileType {
    FILE = 1,
    DIRECTORY = 2,
    DELETED = 3,
}

impl<DB> FromSql<Integer, DB> for FileType
where
    DB: Backend,
    i32: FromSql<Integer, DB>,
{
    fn from_sql(bytes: Option<&DB::RawValue>) -> deserialize::Result<Self> {
        match i32::from_sql(bytes)? {
            x if x == Self::FILE as i32 => Ok(Self::FILE),
            x if x == Self::DIRECTORY as i32 => Ok(Self::DIRECTORY),
            x if x == Self::DELETED as i32 => Ok(Self::DELETED),
            x => Err(format!("Unrecognized variant {}", x).into()),
        }
    }
}

impl<DB> ToSql<Integer, DB> for FileType
where
    DB: Backend,
    i32: ToSql<Integer, DB>,
{
    fn to_sql<W: Write>(&self, out: &mut Output<W, DB>) -> serialize::Result {
        (*self as i32).to_sql(out)
    }
}
