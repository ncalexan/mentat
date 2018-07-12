// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

/// Low-level functions for testing.

use std::cmp::{
    Ordering,
};
use std::io::{
    Write,
};
use std::ops::{
    Deref,
};

use itertools::Itertools;
use rusqlite;
use rusqlite::types::{ToSql};
use tabwriter::TabWriter;

use bootstrap;
use db::TypedSQLValue;
use edn::{
    self,
    ValueRc,
};
use edn::entities::{
    EntidOrIdent,
};
use entids;
use errors::Result;
use mentat_core::{
    Entid,
    HasSchema,
    SQLValueType,
    TypedValue,
    ValueType,
};
use schema::{
    SchemaBuilding,
};
use types::Schema;

/// Represents a *datom* (assertion) in the store.
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub(crate) struct Datom {
    pub(crate) e: Entid,
    pub(crate) a: Entid,
    pub(crate) v: TypedValue,
    pub(crate) tx: i64,
    pub(crate) added: Option<bool>,
}

/// Represents a set of datoms (assertions) in the store.
///
/// To make comparision easier, we deterministically order.  The ordering is the ascending tuple
/// ordering determined by `(e, a, (value_type_tag, v), tx)`, where `value_type_tag` is an internal
/// value that is not exposed but is deterministic.
pub(crate) struct Datoms {
    pub schema: ValueRc<Schema>,
    pub datoms: Vec<Datom>,
}

/// Sort datoms by `[e a v]`, grouping by `tx` first if `added` is present.
fn datom_cmp(x: &Datom, y: &Datom) -> Ordering {
    match x.added.is_some() {
        true =>
            (&x.tx, &x.e, &x.a, x.v.value_type().value_type_tag(), &x.v, &x.added).cmp(
           &(&y.tx, &y.e, &y.a, y.v.value_type().value_type_tag(), &y.v, &y.added)),
        false =>
            (&x.e, &x.a, x.v.value_type().value_type_tag(), &x.v, &x.tx).cmp(
           &(&y.e, &y.a, y.v.value_type().value_type_tag(), &y.v, &y.tx)),
    }
}

impl Datoms {
    pub fn new<I>(schema: I, datoms: Vec<Datom>) -> Self where I: Into<ValueRc<Schema>> {
        let schema = schema.into();

        let mut datoms = datoms;
        datoms[..].sort_unstable_by(datom_cmp);

        Datoms {
            schema,
            datoms,
        }
    }
}

impl Deref for Datoms {
    type Target = [Datom];

    fn deref(&self) -> &Self::Target {
        self.datoms.deref()
    }
}

/// Represents an ordered sequence of transactions in the store.
///
/// To make comparision easier, we deterministically order.  The ordering is the ascending tuple
/// ordering determined by `(e, a, (value_type_tag, v), tx, added)`, where `value_type_tag` is an
/// internal value that is not exposed but is deterministic, and `added` is ordered such that
/// retracted assertions appear before added assertions.
pub(crate) struct Transactions(pub Vec<Datoms>);

impl Deref for Transactions {
    type Target = [Datoms];

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

/// Represents the fulltext values in the store.
pub(crate) struct FulltextValues(pub Vec<(i64, String)>);

impl Datom {
    pub(crate) fn to_edn(&self, schema: &Schema) -> edn::Value {
        let f = |entid: &EntidOrIdent| -> edn::Value {
            match *entid {
                EntidOrIdent::Entid(ref y) => edn::Value::Integer(y.clone()),
                EntidOrIdent::Ident(ref y) => edn::Value::Keyword(y.clone()),
            }
        };

        let mut v = vec![edn::Value::Integer(self.e),
                         f(&to_entid_or_ident(schema, self.a)),
                         self.v.clone().map_ident(schema).to_edn_value_pair().0];
        if let Some(added) = self.added {
            v.push(edn::Value::Integer(self.tx));
            v.push(edn::Value::Boolean(added));
        }

        edn::Value::Vector(v)
    }
}

impl Datoms {
    pub(crate) fn to_edn(&self) -> edn::Value {
        edn::Value::Vector((&self.datoms).into_iter().map(|x| x.to_edn(&self.schema)).collect())
    }
}

impl Transactions {
    pub(crate) fn to_edn(&self) -> edn::Value {
        edn::Value::Vector((&self.0).into_iter().map(|x| x.to_edn()).collect())
    }
}

impl FulltextValues {
    pub(crate) fn to_edn(&self) -> edn::Value {
        edn::Value::Vector((&self.0).into_iter().map(|&(x, ref y)| edn::Value::Vector(vec![edn::Value::Integer(x), edn::Value::Text(y.clone())])).collect())
    }
}

/// Turn TypedValue::Ref into TypedValue::Keyword when it is possible.
trait ToIdent {
  fn map_ident(self, schema: &Schema) -> Self;
}

impl ToIdent for TypedValue {
    fn map_ident(self, schema: &Schema) -> Self {
        if let TypedValue::Ref(e) = self {
            schema.get_ident(e).cloned().map(|i| i.into()).unwrap_or(TypedValue::Ref(e))
        } else {
            self
        }
    }
}

/// Convert a numeric entid to an ident `Entid` if possible, otherwise a numeric `Entid`.
fn to_entid_or_ident(schema: &Schema, entid: i64) -> EntidOrIdent {
    schema.get_ident(entid).map_or(EntidOrIdent::Entid(entid), |ident| EntidOrIdent::Ident(ident.clone()))
}

/// Return the set of datoms in the store, ordered by (e, a, v, tx), but not including any datoms of
/// the form [... :db/txInstant ...].
pub(crate) fn datoms(conn: &rusqlite::Connection, schema: &Schema) -> Result<Datoms> {
    datoms_after(conn, schema, bootstrap::TX0 - 1)
}

/// Turn a row like `SELECT e, a, v, value_type_tag, tx[, added]?` into a `Datom`, optionally
/// filtering `:db/txInstant` datoms out.
fn row_to_datom(schema: &Schema, filter_tx_instant: bool, row: &rusqlite::Row) -> Result<Option<Datom>> {
    let e: i64 = row.get_checked(0)?;
    let a: i64 = row.get_checked(1)?;

    if filter_tx_instant && a == entids::DB_TX_INSTANT {
        return Ok(None);
    }

    let v: rusqlite::types::Value = row.get_checked(2)?;
    let value_type_tag: i32 = row.get_checked(3)?;

    let attribute = schema.require_attribute_for_entid(a)?;
    let value_type_tag = if !attribute.fulltext { value_type_tag } else { ValueType::Long.value_type_tag() };

    let typed_value = TypedValue::from_sql_value_pair(v, value_type_tag)?;

    let tx: i64 = row.get_checked(4)?;
    let added: Option<bool> = row.get_checked(5).ok();

    Ok(Some(Datom {
        e,
        a,
        v: typed_value,
        tx,
        added,
    }))
}

/// Return the set of datoms in the store with transaction ID strictly greater than the given `tx`,
/// ordered by (e, a, v, tx).
///
/// The datom set returned does not include any datoms of the form [... :db/txInstant ...].
pub(crate) fn datoms_after(conn: &rusqlite::Connection, schema: &Schema, tx: i64) -> Result<Datoms> {
    let mut stmt: rusqlite::Statement = conn.prepare("SELECT e, a, v, value_type_tag, tx FROM datoms WHERE tx > ?")?;

    let r: Result<Vec<_>> = stmt.query_and_then(&[&tx], |row| row_to_datom(schema, true, row))?.collect();

    Ok(Datoms::new(schema.clone(), r?.into_iter().filter_map(|x| x).collect()))
}

/// Return the sequence of transactions in the store with transaction ID strictly greater than the
/// given `tx`, ordered by (tx, e, a, v).
///
/// Each transaction returned includes the [(transaction-tx) :db/txInstant ...] datom.
pub(crate) fn transactions_after(conn: &rusqlite::Connection, schema: &Schema, tx: i64) -> Result<Transactions> {
    let mut stmt: rusqlite::Statement = conn.prepare("SELECT e, a, v, value_type_tag, tx, added FROM transactions WHERE tx > ?")?;

    let r: Result<Vec<_>> = stmt.query_and_then(&[&tx], |row| row_to_datom(schema, false, row))?.collect();

    let schema_rc: ValueRc<Schema> = schema.clone().into();

    // Group by tx.
    let r: Vec<Datoms> = r?.into_iter().filter_map(|x| x).group_by(|x| x.tx).into_iter().map(|(_key, group)| Datoms::new(schema_rc.clone(), group.collect())).collect();

    Ok(Transactions(r))
}

/// Return the set of fulltext values in the store, ordered by rowid.
pub(crate) fn fulltext_values(conn: &rusqlite::Connection) -> Result<FulltextValues> {
    let mut stmt: rusqlite::Statement = conn.prepare("SELECT rowid, text FROM fulltext_values ORDER BY rowid")?;

    let r: Result<Vec<_>> = stmt.query_and_then(&[], |row| {
        let rowid: i64 = row.get_checked(0)?;
        let text: String = row.get_checked(1)?;
        Ok((rowid, text))
    })?.collect();

    r.map(FulltextValues)
}

/// Execute the given `sql` query with the given `params` and format the results as a
/// tab-and-newline formatted string suitable for debug printing.
///
/// The query is printed followed by a newline, then the returned columns followed by a newline, and
/// then the data rows and columns.  All columns are aligned.
pub(crate) fn dump_sql_query(conn: &rusqlite::Connection, sql: &str, params: &[&ToSql]) -> Result<String> {
    let mut stmt: rusqlite::Statement = conn.prepare(sql)?;

    let mut tw = TabWriter::new(Vec::new()).padding(2);
    write!(&mut tw, "{}\n", sql).unwrap();

    for column_name in stmt.column_names() {
        write!(&mut tw, "{}\t", column_name).unwrap();
    }
    write!(&mut tw, "\n").unwrap();

    let r: Result<Vec<_>> = stmt.query_and_then(params, |row| {
        for i in 0..row.column_count() {
            let value: rusqlite::types::Value = row.get_checked(i)?;
            write!(&mut tw, "{:?}\t", value).unwrap();
        }
        write!(&mut tw, "\n").unwrap();
        Ok(())
    })?.collect();
    r?;

    let dump = String::from_utf8(tw.into_inner().unwrap()).unwrap();
    Ok(dump)
}
