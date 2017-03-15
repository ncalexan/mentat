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

//! Most transactions can mutate the Mentat metadata by transacting assertions:
//!
//! - they can add (and, eventually, retract and alter) recognized idents using the `:db/ident`
//!   attribute;
//!
//! - they can add (and, eventually, retract and alter) schema attributes using
//!   `:db.install/attribute` and various `:db/*` attributes;
//!
//! - eventually, they will be able to add (and possibly retract) entid partitions using the
//!   `:db.install/partition` attribute.
//!
//! This module recognizes, validates, applies, and reports on these mutations.

use std::collections::{BTreeMap, BTreeSet};
use std::collections::btree_map::Entry;

use edn::symbols;
use entids;
use errors::{
    ErrorKind,
    Result,
};
use mentat_core::{
    Entid,
    SchemaMap,
    TypedValue,
    ValueType,
};
use schema::{
    AttributeBuilder,
};

/// An alteration to an attribute.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum AttributeAlteration {
    /// From http://blog.datomic.com/2014/01/schema-alteration.html:
    /// - rename attributes
    /// - rename your own programmatic identities (uses of :db/ident)
    /// - add or remove indexes
    Index,
    /// - add or remove uniqueness constraints
    UniqueValue,
    UniqueIdentity,
    /// - change attribute cardinality
    Cardinality,
    /// - change whether history is retained for an attribute
    NoHistory,
    /// - change whether an attribute is treated as a component
    IsComponent,
}

/// An alteration to an ident.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum IdentAlteration {
    Ident(symbols::NamespacedKeyword),
}

/// Summarizes changes to metadata such as a a `Schema` and (in the future) a `PartitionMap`.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct MetadataReport {
    // Entids that were not present in the original `SchemaMap` that was mutated.
    pub attributes_installed: BTreeSet<Entid>,

    // Entids that were present in the original `SchemaMap` that was mutated, together with a
    // representation of the mutations that were applied.
    pub attributes_altered: BTreeMap<Entid, Vec<AttributeAlteration>>,

    // Idents that were installed into the `SchemaMap`.
    pub idents_altered: BTreeMap<Entid, IdentAlteration>,
}

/// Update a `SchemaMap` in place from the given `[e a typed_value]` triples.
///
/// This is suitable for producing a `SchemaMap` from the `schema` materialized view, which does not
/// contain install and alter markers.
///
/// Returns a report summarizing the mutations that were applied.
pub fn update_schema_map_from_entid_triples<U>(schema_map: &mut SchemaMap, assertions: U) -> Result<MetadataReport>
    where U: IntoIterator<Item=(Entid, Entid, TypedValue)> {

    // Group mutations by impacted entid.
    let mut builders: BTreeMap<Entid, AttributeBuilder> = BTreeMap::new();

    for (entid, attr, ref value) in assertions.into_iter() {
        let builder = builders.entry(entid).or_insert(AttributeBuilder::default());

        // TODO: improve error messages throughout.
        match attr {
            entids::DB_DOC => {
                match *value {
                    TypedValue::String(_) => {},
                    _ => bail!(ErrorKind::BadSchemaAssertion(format!("Expected [... :db/doc \"string value\"] but got [... :db/doc {:?}] for entid '{}' and attribute '{}'", value, entid, attr)))
                }
            },

            entids::DB_VALUE_TYPE => {
                match *value {
                    TypedValue::Ref(entids::DB_TYPE_REF) => { builder.value_type(ValueType::Ref); },
                    TypedValue::Ref(entids::DB_TYPE_BOOLEAN) => { builder.value_type(ValueType::Boolean); },
                    TypedValue::Ref(entids::DB_TYPE_DOUBLE) => { builder.value_type(ValueType::Double); },
                    TypedValue::Ref(entids::DB_TYPE_LONG) => { builder.value_type(ValueType::Long); },
                    TypedValue::Ref(entids::DB_TYPE_STRING) => { builder.value_type(ValueType::String); },
                    TypedValue::Ref(entids::DB_TYPE_KEYWORD) => { builder.value_type(ValueType::Keyword); },
                    _ => bail!(ErrorKind::BadSchemaAssertion(format!("Expected [... :db/valueType :db.type/*] but got [... :db/valueType {:?}] for entid '{}' and attribute '{}'", value, entid, attr)))
                }
            },

            entids::DB_CARDINALITY => {
                match *value {
                    TypedValue::Ref(entids::DB_CARDINALITY_MANY) => { builder.multival(true); },
                    TypedValue::Ref(entids::DB_CARDINALITY_ONE) => { builder.multival(false); },
                    _ => bail!(ErrorKind::BadSchemaAssertion(format!("Expected [... :db/cardinality :db.cardinality/many|:db.cardinality/one] but got [... :db/cardinality {:?}]", value)))
                }
            },

            entids::DB_UNIQUE => {
                match *value {
                    // TODO: accept nil in some form.
                    // TypedValue::Nil => {
                    //     builder.unique_value(false);
                    //     builder.unique_identity(false);
                    // },
                    TypedValue::Ref(entids::DB_UNIQUE_VALUE) => { builder.unique_value(true); },
                    TypedValue::Ref(entids::DB_UNIQUE_IDENTITY) => { builder.unique_identity(true); },
                    _ => bail!(ErrorKind::BadSchemaAssertion(format!("Expected [... :db/unique :db.unique/value|:db.unique/identity] but got [... :db/unique {:?}]", value)))
                }
            },

            entids::DB_INDEX => {
                match *value {
                    TypedValue::Boolean(x) => { builder.index(x); },
                    _ => bail!(ErrorKind::BadSchemaAssertion(format!("Expected [... :db/index true|false] but got [... :db/index {:?}]", value)))
                }
            },

            entids::DB_FULLTEXT => {
                match *value {
                    TypedValue::Boolean(x) => { builder.fulltext(x); },
                    _ => bail!(ErrorKind::BadSchemaAssertion(format!("Expected [... :db/fulltext true|false] but got [... :db/fulltext {:?}]", value)))
                }
            },

            entids::DB_IS_COMPONENT => {
                match *value {
                    TypedValue::Boolean(x) => { builder.component(x); },
                    _ => bail!(ErrorKind::BadSchemaAssertion(format!("Expected [... :db/isComponent true|false] but got [... :db/isComponent {:?}]", value)))
                }
            },

            _ => {
                bail!(ErrorKind::BadSchemaAssertion(format!("Do not recognize attribute '{}' for entid '{}'", attr, entid)))
            }
        }
    };

    let mut attributes_installed: BTreeSet<Entid> = BTreeSet::default();
    let mut attributes_altered: BTreeMap<Entid, Vec<AttributeAlteration>> = BTreeMap::default();

    for (entid, builder) in builders.into_iter() {
        match schema_map.entry(entid) {
            Entry::Vacant(entry) => {
                if !builder.is_valid_install_attribute() {
                    bail!(ErrorKind::BadSchemaAssertion(format!("Schema attribute for :db.install/attribute with entid '{}' does not set :db/valueType", entid)));
                }
                entry.insert(builder.build());
                attributes_installed.insert(entid);
            },
            Entry::Occupied(mut entry) => {
                if !builder.is_valid_alter_attribute() {
                    bail!(ErrorKind::BadSchemaAssertion(format!("Schema attribute for :db.alter/attribute with entid '{}' must not set :db/valueType", entid)));
                }
                let mutations = builder.mutate(entry.get_mut());
                attributes_altered.insert(entid, mutations);
            },
        }
    }

    Ok(MetadataReport {
        attributes_installed: attributes_installed,
        attributes_altered: attributes_altered,
        idents_altered: BTreeMap::default(),
    })
}
