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
#[macro_use] extern crate error_chain;
#[macro_use] extern crate log;
#[macro_use] extern crate lazy_static;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate uuid;

extern crate edn;
#[macro_use] extern crate mentat;
extern crate mentat_core;
extern crate mentat_db;
extern crate mentat_query;

mod types;
pub use types::{
    Credential,
    CredentialId,
    FormTarget,
    ServerPassword,
    SyncGuid,
};
pub mod credentials;
pub mod logins;

#[cfg(test)]
mod tests {
    use mentat::{
        Store,
    };

    use mentat::vocabulary::{
        VersionedStore,
        VocabularyOutcome,
    };

    use credentials::{
        CREDENTIAL_VOCAB,
        LOGIN_VOCAB,
    };

    use logins::{
        FORM_VOCAB,
        SYNC_PASSWORD_VOCAB,
    };

    pub(crate) fn testing_store() -> Store {
        let mut store = Store::open("").expect("opened");

        // Scoped borrow of `store`.
        {
            let mut in_progress = store.begin_transaction().expect("begun successfully");

            assert!(in_progress.verify_core_schema().is_ok());

            assert_eq!(VocabularyOutcome::Installed, in_progress.ensure_vocabulary(&CREDENTIAL_VOCAB).expect("ensure succeeded"));
            assert_eq!(VocabularyOutcome::Installed, in_progress.ensure_vocabulary(&LOGIN_VOCAB).expect("ensure succeeded"));
            assert_eq!(VocabularyOutcome::Installed, in_progress.ensure_vocabulary(&FORM_VOCAB).expect("ensure succeeded"));
            assert_eq!(VocabularyOutcome::Installed, in_progress.ensure_vocabulary(&SYNC_PASSWORD_VOCAB).expect("ensure succeeded"));

            in_progress.commit().expect("commit succeeded");
        }

        store
    }
}
