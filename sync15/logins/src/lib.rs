// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![crate_name = "mentat_sync15_logins"]

extern crate chrono;
#[macro_use] extern crate log;
#[macro_use] extern crate lazy_static;
#[macro_use] extern crate error_chain;
extern crate uuid;

extern crate edn;
#[macro_use] extern crate mentat;
extern crate mentat_core;
extern crate mentat_db;
extern crate mentat_query;

mod types;
pub use types::{
    ServerPassword,
};
pub mod logins;
