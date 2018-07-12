// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use rusqlite;

use errors::{
    DbErrorKind,
    Result,
};

use mentat_core::{
    Entid,
    Schema,
    TypedValue,
    KnownEntid,
};

use edn::{
    InternSet,
};

use edn::entities::OpType;

use db::{
    TypedSQLValue,
};
use tx;

use types::{
    PartitionMap,
};

use internal_types::{
    Term,
    TermWithoutTempIds,
};

use watcher::{
    NullWatcher,
};

pub static MAIN_TIMELINE: Entid = 0;

fn current_timeline(conn: &rusqlite::Connection, smallest_tx: Entid, tx_ids: &[Entid]) -> Result<Entid> {
    let mut stmt = conn.prepare("SELECT tx, timeline FROM timelined_transactions WHERE tx >= ?")?;
    let mut rows = stmt.query_and_then(&[&smallest_tx], |row: &rusqlite::Row| -> Result<(Entid, Entid)>{
        Ok((row.get_checked(0)?, row.get_checked(1)?))
    })?;

    // Ensure that tx_ids are a consistent block at the tail end of the transactions table.
    // TODO do this in SQL? e.g. SELECT tx FROM timelined_transactions WHERE tx >= ? AND tx NOT IN [tx_ids]...
    let timeline = match rows.next() {
        Some(t) => {
            let t = t?;
            if !tx_ids.contains(&t.0) {
                bail!(DbErrorKind::TimelinesNotOnTail);
            }
            t.1
        },
        None => bail!(DbErrorKind::TimelinesInvalidTransactionIds)
    };

    while let Some(t) = rows.next() {
        let t = t?;
        if !tx_ids.contains(&t.0) {
            bail!(DbErrorKind::TimelinesNotOnTail);
        }
        if t.1 != timeline {
            bail!(DbErrorKind::TimelinesMixed);
        }
    }

    Ok(timeline)
}

fn move_transactions_to(conn: &rusqlite::Connection, tx_ids: &[Entid], new_timeline: Entid) -> Result<()> {
    // Move specified transactions over to a specified timeline.
    conn.execute(&format!(
        "UPDATE timelined_transactions SET timeline = {} WHERE tx IN ({})",
            new_timeline,
            ::repeat_values(tx_ids.len(), 1)
        ), &(tx_ids.iter().map(|x| x as &rusqlite::types::ToSql).collect::<Vec<_>>())
    )?;
    Ok(())
}

fn eradicate_transaction(conn: &rusqlite::Connection, tx: Entid) -> Result<()> {
    conn.execute("DELETE FROM timelined_transactions WHERE tx = ?", &[&tx])?;
    Ok(())
}

/// Get terms after tx_id, and reverse them both in order (DESC tx) and in meaning (swap add & retract).
fn reversed_terms_after(conn: &rusqlite::Connection, tx_id: Entid) -> Result<Vec<TermWithoutTempIds>> {
    let mut stmt = conn.prepare("SELECT e, a, v, value_type_tag, tx, added FROM timelined_transactions WHERE tx > ? AND timeline = ? ORDER BY tx DESC")?;
    let mut rows = stmt.query_and_then(&[&tx_id, &MAIN_TIMELINE], |row| -> Result<TermWithoutTempIds> {
        let op = match row.get_checked(5)? {
            true => OpType::Retract,
            false => OpType::Add
        };
        Ok(Term::AddOrRetract(
            op,
            KnownEntid(row.get_checked(0)?),
            row.get_checked(1)?,
            TypedValue::from_sql_value_pair(row.get_checked(2)?, row.get_checked(3)?)?,
        ))
    })?;

    let mut terms = vec![];

    while let Some(row) = rows.next() {
        terms.push(row?);
    }
    Ok(terms)
}

pub fn move_from_main_timeline(conn: &rusqlite::Connection, schema: &Schema,
    mut partition_map: PartitionMap, tx_ids: &[Entid], new_timeline: Entid
    ) -> Result<(PartitionMap, Option<Schema>)> {

    if new_timeline == MAIN_TIMELINE {
        bail!(DbErrorKind::NotYetImplemented(format!("Can't move transactions to main timeline")));
    }

    let smallest_tx = match tx_ids.iter().min() {
        Some(e) => *e,
        None => bail!(DbErrorKind::TimelinesNoTransactionsSupplied)
    };

    let timeline = current_timeline(conn, smallest_tx, tx_ids)?;

    // TODO stil need to return schema and partitionmap...
    if timeline != MAIN_TIMELINE {
        bail!(DbErrorKind::TimelinesNotOnMain);
    }

    let reversed_terms = reversed_terms_after(conn, smallest_tx - 1)?;
    let lowest_e = match reversed_terms.last() {
        Some(Term::AddOrRetract(_, e, _, _)) => e.0,
        None => unreachable!()
    };

    // TODO comment why we're filtering.
    let filtered_reversed_terms = reversed_terms.into_iter()
        .filter(|Term::AddOrRetract(_, _, a, _)| *a != 1)
        .map(|t| t.rewrap());

    // Rewind schema and datoms.
    let (tx_report, _, new_schema, _) = tx::transact_terms(conn, partition_map.clone(), schema, schema, NullWatcher(), filtered_reversed_terms, InternSet::new())?;

    // 'transact_terms' resulted in a "rewind transaction", which we don't need. Delete it.
    eradicate_transaction(conn, tx_report.tx_id)?;

    // Move transactions over to the target timeline.
    move_transactions_to(conn, tx_ids, new_timeline)?;

    // We need to deallocate rewound entids from the user partition.
    match partition_map.get_mut(":db.part/user") {
        Some(p) => p.set_index(lowest_e),
        None => unreachable!()
    };

    // We need to deallocate rewound tx from the tx partiton.
    match partition_map.get_mut(":db.part/tx") {
        Some(p) => p.set_index(smallest_tx),
        None => unreachable!()
    };

    Ok((partition_map, new_schema))
}
