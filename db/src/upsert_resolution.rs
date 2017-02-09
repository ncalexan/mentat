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

//! This module implements the upsert resolution algorithm described at
//! https://github.com/mozilla/mentat/wiki/Transacting:-upsert-resolution-algorithm.

use std::iter::{once};

use mentat_tx::entities::OpType;
use errors;
use types::{Attribute, AVPair, DB, Entid, TypedValue};
use internal_types::*;

// /// XXX Population type models 
// #[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
// enum PopulationType {
//     /// "Simple upserts" that look like [:db/add TEMPID a v], where a is :db.unique/identity.
//     UpsertsE,

//     /// "Complex upserts" that look like [:db/add TEMPID a OTHERID], where a is :db.unique/identity
//     UpsertsEV,

//     /// Entities that look like:
//     /// - [:db/add TEMPID b OTHERID], where b is not :db.unique/identity;
//     /// - [:db/add TEMPID b v], where b is not :db.unique/identity.
//     /// - [:db/add e b OTHERID].
//     Allocations,

//     /// Entities that do not reference temp IDs.
//     Inert,
// }

// /// A "Simple upsert" that looks like [:db/add TEMPID a v], where a is :db.unique/identity.
// #[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
// struct UpsertE(TempId, Attribute, Entid, TypedValue);

// /// A "Complex upsert" that looks like [:db/add TEMPID a OTHERID], where a is :db.unique/identity
// #[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
// struct UpsertEV(TempId, Attribute, Entid, TempId);

// #[derive(Clone,Debug,Default,Eq,Hash,Ord,PartialOrd,PartialEq)]
// struct Generation {
//     /// "Simple upserts" that look like [:db/add TEMPID a v], where a is :db.unique/identity.
//     upserts_e: Vec<UpsertE>,

//     /// "Complex upserts" that look like [:db/add TEMPID a OTHERID], where a is :db.unique/identity
//     upserts_ev: Vec<UpsertEV>,

//     /// Entities that look like:
//     /// - [:db/add TEMPID b OTHERID], where b is not :db.unique/identity;
//     /// - [:db/add TEMPID b v], where b is not :db.unique/identity.
//     /// - [:db/add e b OTHERID].
//     allocations: Vec<TermWithTempIds>,

//     /// Entities that upserted and no longer reference temp IDs.  These assertions are guaranteed to
//     /// be in the store.
//     upserted: Vec<TermWithoutTempIds>,

//     /// Entities that resolved due to other upserts and no longer reference temp IDs.  These
//     /// assertions may or may not be in the store.
//     resolved: Vec<TermWithoutTempIds>,

//     // /// Entities that look like [:db/add TEMPID b OTHERID], where b is not :db.unique/identity.
//     // allocations_ev: Population,

//     // /// Entities that look like [:db/add TEMPID b OTHERID], where b is not :db.unique/identity.
//     // allocations_e: Population,

//     // /// Entities that look like [:db/add e b OTHERID], where b is not :db.unique/identity
//     // allocations_v: Population,

//     // /// Upserts that upserted.
//     // upserted: Population,

//     // /// Allocations that resolved due to other upserts.
//     // resolved: Population,
// }

// impl Generation {
//     /// Return true if it's possible to evolve this generation further.
//     fn can_evolve(&self) -> bool {
//         !self.upserts_e.is_empty()
//     }

//     /// Evolve this generation one step further by rewriting the existing :db/add entities using the
//     /// given temporary IDs.
//     ///
//     /// TODO: Considering doing this in place; the function already consumes `self`.
//     fn evolve_one_step(self, temp_id_map: &TempIdMap) -> Generation {
//         let mut next = Generation::default();
//         next.allocations = self.allocations;
//         next.upserted = self.upserted;
//         next.resolved = self.resolved;

//         for UpsertE(t, _, a, v) in self.upserts_e {
//             match temp_id_map.get(&*t) {
//                 Some(&n) => next.upserted.push(Term::AddOrRetract(OpType::Add, n, a, v)),
//                 None => next.allocations.push(Term::AddOrRetract(OpType::Add, Err(t), a, Ok(v))),
//             }
//         }

//         for UpsertEV(t1, attribute, a, t2) in self.upserts_ev {
//             match (temp_id_map.get(&*t1), temp_id_map.get(&*t2)) {
//                 (Some(&n1), Some(&n2)) => next.resolved.push(Term::AddOrRetract(OpType::Add, n1, a, TypedValue::Ref(n2))),
//                 (None, Some(&n2)) => next.upserts_e.push(UpsertE(t1, attribute, a, TypedValue::Ref(n2))),
//                 (Some(&n1), None) => next.allocations.push(Term::AddOrRetract(OpType::Add, Ok(n1), a, Err(t2))),
//                 (None, None) => next.allocations.push(Term::AddOrRetract(OpType::Add, Err(t1), a, Err(t2))),
//             }
//         }

//         next
//     }

//     /// Collect id->[a v] pairs.
//     ///
//     /// Note: the return type is Box<> since `impl Trait` is not yet stable.
//     fn av_pairs<'a>(&'a self) -> Box<Iterator<Item=(TempId, AVPair)> + 'a> {
//         let i = self.upserts_e.iter().map(|&UpsertE(ref t, _, ref a, ref v)| { // in &self.upserts_e {
//             // TODO: figure out how to make this less expensive, i.e., don't require clone() of an
//             // arbitrary TypedValue.
//             (t.clone(), (*a, v.clone()))
//         });
//         Box::new(i)
//     }

//     // /// Iterate any temporary IDs present in entities still requiring allocation.
//     // ///
//     // /// Note: the return type is Box<> since `impl Trait` is not yet stable.
//     // fn temp_ids_iter<'a>(&'a self) -> Box<Iterator<Item=TempId> + 'a> {
//     //     // fn collect<B: FromIterator<Self::Item>>(self) -> B
//     //     for term in &self.allocations {
//     //         match term {
//     //             &Term::AddOrRetract(_, Err(ref t1), _, Err(ref t2)) => once(t1).chain(once(t2)) as &Iterator<Item=TempId>,
//     //             &Term::AddOrRetract(_, Err(ref t1), _, Ok(_)) => once(t1) as &Iterator<Item=TempId>,
//     //             &Term::AddOrRetract(_, Ok(_), _, Err(ref t2)) => once(t2) as &Iterator<Item=TempId>,
//     //             &Term::AddOrRetract(_, Ok(_), _, Ok(_)) => empty() as &Iterator<Item=TempId>,
//     //         }
//     //     });

//     //     Box::new(i)
//     // }
// }



//         // // TODO: handle all allocations uniformly? // .into_iter().chain(self.allocations_e.into_iter()).chain(self.allocations_v.into_iter()) {
//         // for term in self.allocations_ev {
//         //     match term {
//         //         Term::AddOrRetract(op, Err(t1), a, Err(t2)) => {
//         //             match (temp_id_map.get(&*t1), temp_id_map.get(&*t2)) {
//         //                 (Some(&n1), Some(&n2)) => next.resolved.push(Term::AddOrRetract(op, Ok(n1), a, Ok(TypedValue::Ref(n2)))),
//         //                 (None, Some(&n2)) => next.allocations_e.push(Term::AddOrRetract(op, Err(t1), a, Ok(TypedValue::Ref(n2)))),
//         //                 (Some(&n1), None) => next.allocations_v.push(Term::AddOrRetract(op, Ok(n1), a, Err(t2))),
//         //                 (None, None) => next.allocations_ev.push(Term::AddOrRetract(op, Err(t1), a, Err(t2))),
//         //             }
//         //         },
//         //         _ => panic!("At the disco"),
//         //     }
//         // }

//         // // TODO: same as upserts_e!
//         // for term in self.allocations_e {
//         //     match term {
//         //         Term::AddOrRetract(op, Err(t), a, v) => {
//         //             match temp_id_map.get(&*t) {
//         //                 Some(&n) => next.resolved.push(Term::AddOrRetract(op, Ok(n), a, v)),
//         //                 None => next.allocations_e.push(Term::AddOrRetract(op, Err(t), a, v)),
//         //             }
//         //         },
//         //         _ => panic!("At the disco"),
//         //     }
//         // }

//         // for term in self.allocations_v {
//         //     match term {
//         //         Term::AddOrRetract(op, e, a, Err(t)) => {
//         //             match temp_id_map.get(&*t) {
//         //                 Some(&n) => next.resolved.push(Term::AddOrRetract(op, e, a, Ok(TypedValue::Ref(n)))),
//         //                 None => next.allocations_v.push(Term::AddOrRetract(op, e, a, Err(t))),
//         //             }
//         //         },
//         //         _ => panic!("At the disco"),
//         //     }
//         // }

//         // next.inert = self.inert;

//     //     next

//     //     // AddOrRetract(OpType, E, Entid, V),
//     //     // RetractAttribute(E, Entid),
//     //     // RetractEntity(E)
//     // }


#[derive(Clone,Debug,Default,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct Generation {
    /// "Simple upserts" that look like [:db/add TEMPID a v], where a is :db.unique/identity.
    upserts_e: Population,

    /// "Complex upserts" that look like [:db/add TEMPID a OTHERID], where a is :db.unique/identity
    upserts_ev: Population,

    /// Entities that look like [:db/add TEMPID b OTHERID], where b is not :db.unique/identity.
    allocations_ev: Population,

    /// Entities that look like [:db/add TEMPID b OTHERID], where b is not :db.unique/identity.
    allocations_e: Population,

    /// Entities that look like [:db/add e b OTHERID], where b is not :db.unique/identity
    allocations_v: Population,

    /// Upserts that upserted.
    upserted: Population,

    /// Allocations that resolved due to other upserts.
    resolved: Population,

    /// Entities that do not reference temp IDs.
    inert: Population,
}

#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
enum PopulationType {
    /// "Simple upserts" that look like [:db/add TEMPID a v], where a is :db.unique/identity.
    UpsertsE,

    /// "Complex upserts" that look like [:db/add TEMPID a OTHERID], where a is :db.unique/identity
    UpsertsEV,

    /// Entities that look like [:db/add TEMPID b OTHERID], where b is not :db.unique/identity.
    AllocationsEV,

    /// Entities that look like [:db/add TEMPID b v], where b is not :db.unique/identity.
    AllocationsE,

    /// Entities that look like [:db/add e b OTHERID].
    AllocationsV,

    // TODO: ensure :db/retract{Entity,Attribute} are pushed into Allocations* correctly.

    /// Entities that do not reference temp IDs.
    Inert,
}

impl TermWithTempIds {
    fn population_type(&self, db: &DB) -> errors::Result<PopulationType> {
        let is_unique = |a: &Entid| -> errors::Result<bool> {
            let attribute: &Attribute = db.schema.require_attribute_for_entid(a)?;
            Ok(attribute.unique_identity)
        };

        match self {
            &Term::AddOrRetract(ref op, Err(_), ref a, Err(_)) => if op == &OpType::Add && is_unique(a)? { Ok(PopulationType::UpsertsEV) } else { Ok(PopulationType::AllocationsEV) },
            &Term::AddOrRetract(ref op, Err(_), ref a, Ok(_)) => if op == &OpType::Add && is_unique(a)? { Ok(PopulationType::UpsertsE) } else { Ok(PopulationType::AllocationsE) },
            &Term::AddOrRetract(_, Ok(_), _, Err(_)) => Ok(PopulationType::AllocationsV),
            _ => Ok(PopulationType::Inert),
        }
    }
}

impl Generation {
    pub fn from<I>(terms: I, db: &DB) -> errors::Result<Generation> where I: IntoIterator<Item=TermWithTempIds> {
        let mut generation = Generation::default();

        for term in terms.into_iter() {
            match term.population_type(db)? {
                PopulationType::UpsertsEV => generation.upserts_ev.push(term),
                PopulationType::UpsertsE => generation.upserts_e.push(term),
                PopulationType::AllocationsEV => generation.allocations_ev.push(term),
                PopulationType::AllocationsE => generation.allocations_e.push(term),
                PopulationType::AllocationsV => generation.allocations_v.push(term),
                PopulationType::Inert => generation.inert.push(term),
            }
        }

        Ok(generation)
    }

    pub fn can_evolve(&self) -> bool {
        !self.upserts_e.is_empty()
    }

    pub fn evolve_one_step(self, temp_id_map: &TempIdMap) -> Generation {
        let mut next = Generation::default();

        for term in self.upserts_e {
            match term {
                Term::AddOrRetract(op, Err(t), a, v) => {
                    match temp_id_map.get(&*t) {
                        Some(&n) => next.upserted.push(Term::AddOrRetract(op, Ok(n), a, v)),
                        None => next.allocations_e.push(Term::AddOrRetract(op, Err(t), a, v)),
                    }
                },
                _ => panic!("At the disco"),
            }
        }

        for term in self.upserts_ev {
            match term {
                Term::AddOrRetract(op, Err(t1), a, Err(t2)) => {
                    match (temp_id_map.get(&*t1), temp_id_map.get(&*t2)) {
                        (Some(&n1), Some(&n2)) => next.resolved.push(Term::AddOrRetract(op, Ok(n1), a, Ok(TypedValue::Ref(n2)))),
                        (None, Some(&n2)) => next.upserts_e.push(Term::AddOrRetract(op, Err(t1), a, Ok(TypedValue::Ref(n2)))),
                        (Some(&n1), None) => next.allocations_v.push(Term::AddOrRetract(op, Ok(n1), a, Err(t2))),
                        (None, None) => next.allocations_ev.push(Term::AddOrRetract(op, Err(t1), a, Err(t2))),
                    }
                },
                _ => panic!("At the disco"),
            }
        }

        // TODO: handle all allocations uniformly? // .into_iter().chain(self.allocations_e.into_iter()).chain(self.allocations_v.into_iter()) {
        for term in self.allocations_ev {
            match term {
                Term::AddOrRetract(op, Err(t1), a, Err(t2)) => {
                    match (temp_id_map.get(&*t1), temp_id_map.get(&*t2)) {
                        (Some(&n1), Some(&n2)) => next.resolved.push(Term::AddOrRetract(op, Ok(n1), a, Ok(TypedValue::Ref(n2)))),
                        (None, Some(&n2)) => next.allocations_e.push(Term::AddOrRetract(op, Err(t1), a, Ok(TypedValue::Ref(n2)))),
                        (Some(&n1), None) => next.allocations_v.push(Term::AddOrRetract(op, Ok(n1), a, Err(t2))),
                        (None, None) => next.allocations_ev.push(Term::AddOrRetract(op, Err(t1), a, Err(t2))),
                    }
                },
                _ => panic!("At the disco"),
            }
        }

        // TODO: same as upserts_e!
        for term in self.allocations_e {
            match term {
                Term::AddOrRetract(op, Err(t), a, v) => {
                    match temp_id_map.get(&*t) {
                        Some(&n) => next.resolved.push(Term::AddOrRetract(op, Ok(n), a, v)),
                        None => next.allocations_e.push(Term::AddOrRetract(op, Err(t), a, v)),
                    }
                },
                _ => panic!("At the disco"),
            }
        }

        for term in self.allocations_v {
            match term {
                Term::AddOrRetract(op, e, a, Err(t)) => {
                    match temp_id_map.get(&*t) {
                        Some(&n) => next.resolved.push(Term::AddOrRetract(op, e, a, Ok(TypedValue::Ref(n)))),
                        None => next.allocations_v.push(Term::AddOrRetract(op, e, a, Err(t))),
                    }
                },
                _ => panic!("At the disco"),
            }
        }

        next.inert = self.inert;

        next

        // AddOrRetract(OpType, E, Entid, V),
        // RetractAttribute(E, Entid),
        // RetractEntity(E)
    }

    // TODO: assert invariants all around the joint.


    // Collect id->[a v] pairs.
    pub fn temp_id_avs<'a>(&'a self) -> Vec<(TempId, AVPair)> {
        let mut temp_id_avs: Vec<(TempId, AVPair)> = vec![];
        for term in &self.upserts_e {
            match term {
                &Term::AddOrRetract(_, Err(ref t), ref a, Ok(ref v)) => {

                    // TODO: figure out how to make this less expensive, i.e., don't require
                    // clone() of an arbitrary value.
                    temp_id_avs.push((t.clone(), (*a, v.clone())));
                },
                _ => panic!("At the disco"),
            }
        }
        temp_id_avs
    }

    /// Return type is Box<> since `impl Trait` is not yet stable.
    pub fn temp_ids_iter<'a>(&'a self) -> Box<Iterator<Item=TempId> + 'a> {
        let i1 = self.upserts_e.iter().chain(self.allocations_e.iter()).map(|term| {
            match term {
                &Term::AddOrRetract(_, Err(ref t), _, _) => t.clone(),
                _ => panic!("At the disco"),
            }
        });

        let i2 = self.upserts_ev.iter().chain(self.allocations_ev.iter()).flat_map(|term| {
            match term {
                &Term::AddOrRetract(_, Err(ref t1), _, Err(ref t2)) => {
                    once(t1.clone()).chain(once(t2.clone()))
                },
                _ => panic!("At the disco"),
            }
        });

        let i3 = self.allocations_v.iter().map(|term| {
            match term {
                &Term::AddOrRetract(_, _, _, Err(ref t)) => t.clone(),
                _ => panic!("At the disco"),
            }
        });

        Box::new(i1.chain(i2).chain(i3))
    }

    pub fn into_allocated_iter<'a>(self, temp_id_map: TempIdMap) -> Box<Iterator<Item=TermWithoutTempIds> + 'a> {
        assert!(self.upserts_e.is_empty());
        assert!(self.upserts_ev.is_empty());

        let i1 = self.allocations_ev.into_iter()
            .chain(self.allocations_e.into_iter())
            .chain(self.allocations_v.into_iter())
            .map(move |term| {
                match term {
                    Term::AddOrRetract(op, Err(t1), a, Err(t2)) => {
                        // TODO: consider require on temp_id_map.
                        match (temp_id_map.get(&*t1), temp_id_map.get(&*t2)) {
                            (Some(&n1), Some(&n2)) => Term::AddOrRetract(op, n1, a, TypedValue::Ref(n2)),
                            _ => panic!("At the disco"),
                        }
                    },
                    Term::AddOrRetract(op, Err(t), a, Ok(v)) => {
                        match temp_id_map.get(&*t) {
                            Some(&n) => (Term::AddOrRetract(op, n, a, v)),
                            _ => panic!("At the disco"),
                        }
                    },
                    Term::AddOrRetract(op, Ok(e), a, Err(t)) => {
                        match temp_id_map.get(&*t) {
                            Some(&n) => Term::AddOrRetract(op, e, a, TypedValue::Ref(n)),
                            _ => panic!("At the disco"),
                        }
                    },
                    _ => panic!("At the disco"),
                }
            });

        // These have no temp IDs by definition, and just need to be lowered.
        let i2 = self.resolved.into_iter()
            .chain(self.inert.into_iter())
            .map(|term| {
                match term {
                    Term::AddOrRetract(op, Ok(n), a, Ok(v)) => {
                        Term::AddOrRetract(op, n, a, v)
                    },
                    _ => panic!("At the disco"),
                }
            });

        Box::new(i1.chain(i2))
    }
}
