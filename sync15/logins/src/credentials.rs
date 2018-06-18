// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use mentat_core::util::Either;

pub use edn::{FromMicros, ToMicros /* Utc, */};

use mentat;
use mentat::{
    Binding,
    Keyword,
    QueryInputs,
    QueryResults,
    Queryable,
    Store,
};

use mentat::conn::{
    Dumpable,
    InProgress,
};

use mentat_core;
use mentat_core::{
    Cloned,
    DateTime,
    Entid,
    KnownEntid,
    TypedValue,
    Utc,
    ValueType,
};

use mentat::query_builder::QueryBuilder;

use edn::entity_builder::{
    BuildEntities,
    Builder,
};

use mentat::vocabulary;

use mentat::vocabulary::{
    Definition
};

use types::{
    Credential,
    CredentialId,
};

lazy_static! {
    // [:credential/username       :db.type/string  :db.cardinality/one]
    // [:credential/password       :db.type/string  :db.cardinality/one]
    // [:credential/created        :db.type/instant :db.cardinality/one]
    // An application might allow users to name their credentials; e.g., "My LDAP".
    // [:credential/title          :db.type/string  :db.cardinality/one]

    pub static ref CREDENTIAL_ID: Keyword = {
        kw!(:credential/id)
    };

    pub static ref CREDENTIAL_USERNAME: Keyword = {
        kw!(:credential/username)
    };

    pub static ref CREDENTIAL_PASSWORD: Keyword = {
        kw!(:credential/password)
    };

    pub static ref CREDENTIAL_CREATED_AT: Keyword = {
        kw!(:credential/createdAt)
    };

    pub static ref CREDENTIAL_TITLE: Keyword = {
        kw!(:credential/title)
    };

    pub static ref CREDENTIAL_VOCAB: vocabulary::Definition = {
        vocabulary::Definition {
            name: kw!(:org.mozilla/credential),
            version: 1,
            attributes: vec![
                (CREDENTIAL_ID.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::String)
                 .unique(vocabulary::attribute::Unique::Identity)
                 .multival(false)
                 .build()),
                (CREDENTIAL_USERNAME.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::String)
                 .multival(false)
                 .build()),
                (CREDENTIAL_PASSWORD.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::String)
                 .multival(false)
                 .build()),
                (CREDENTIAL_CREATED_AT.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::Instant)
                 .multival(false)
                 .build()),
                (CREDENTIAL_TITLE.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::String)
                 .multival(false)
                 .build()),
            ],
            pre: Definition::no_op,
            post: Definition::no_op,
        }
    };

    // This is metadata recording user behavior.
    // [:login/at                  :db.type/instant :db.cardinality/one]
    // [:login/device              :db.type/ref     :db.cardinality/one]
    // [:login/url                 :db.type/string  :db.cardinality/one]
    // [:login/credential          :db.type/ref     :db.cardinality/one]
    // [:login/form                :db.type/ref     :db.cardinality/one]
    pub static ref LOGIN_AT: Keyword = {
        kw!(:login/at)
    };

    pub static ref LOGIN_DEVICE: Keyword = {
        kw!(:login/device)
    };

    pub static ref LOGIN_URL: Keyword = {
        kw!(:login/url)
    };

    pub static ref LOGIN_CREDENTIAL: Keyword = {
        kw!(:login/credential)
    };

    pub static ref LOGIN_FORM: Keyword = {
        kw!(:login/form)
    };

    pub static ref LOGIN_VOCAB: vocabulary::Definition = {
        vocabulary::Definition {
            name: kw!(:org.mozilla/login),
            version: 1,
            attributes: vec![
                (LOGIN_AT.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::Instant)
                 .multival(false)
                 .build()),
                (LOGIN_DEVICE.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::Ref)
                 .multival(false)
                 .build()),
                (LOGIN_URL.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::String)
                 .multival(false)
                 .build()),
                (LOGIN_CREDENTIAL.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::Ref)
                 .multival(false)
                 .build()),
                (LOGIN_FORM.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::Ref)
                 .multival(false)
                 .build()),
            ],
            pre: Definition::no_op,
            post: Definition::no_op,
        }
    };
}


pub fn add_credential<I, J>(builder: &mut Builder<TypedValue>,
                  id: CredentialId,
                  username: Option<String>,
                  password: String,
                  created: I,
                  title: J)
                  -> mentat::errors::Result<()>
    where I: Into<Option<DateTime<Utc>>>, J: Into<Option<String>>,
{
    let c = Builder::tempid("c");

    builder.add(c.clone(),
                CREDENTIAL_ID.clone(),
                TypedValue::typed_string(id));
    if let Some(username) = username {
        builder.add(c.clone(),
                    CREDENTIAL_USERNAME.clone(),
                    TypedValue::String(username.into()));
    }
    builder.add(c.clone(),
                CREDENTIAL_PASSWORD.clone(),
                TypedValue::String(password.into()));
    // TODO: set created to the transaction timestamp.  This might require implementing
    // (transaction-instant), which requires some thought because it is a "delayed binding".
    created.into().map(|created| {
        builder.add(c.clone(),
                    CREDENTIAL_CREATED_AT.clone(),
                    TypedValue::Instant(created));
    });
    title.into().map(|title| {
        builder.add(c.clone(),
                    CREDENTIAL_TITLE.clone(),
                    TypedValue::typed_string(title));
    });

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use tests::{
        testing_store,
    };

    extern crate env_logger;

    #[test]
    fn test_get_modified_sync_password_uuids_to_upload() {
        // env_logger::init();

        let mut store = testing_store();

        // // Scoped borrow of `store`.
        // {
        //     let mut in_progress = store.begin_transaction().expect("begun successfully");

        //     apply_changed_login(&mut in_progress, LOGIN1.clone()).expect("to apply");
        //     apply_changed_login(&mut in_progress, LOGIN2.clone()).expect("to apply");

        //     // But if there are no local changes, we shouldn't propose any records to re-upload.
        //     let sp = get_modified_sync_password_uuids_to_upload(&in_progress).expect("to get_sync_password");
        //     assert_eq!(sp, vec![]);

        //     // Now, let's modify locally an existing credential connected to a Sync 1.5 record.
        //     let mut builder = Builder::<TypedValue>::new();
        //     add_credential(&mut builder,
        //                    CredentialId(LOGIN1.uuid.0.clone()),
        //                    Some("us3rnam3@mockymid.com".into()),
        //                    "pa33w3rd".into(),
        //                    None,
        //                    None)
        //         .expect("to update credential");
        //     in_progress.transact_entity_builder(builder).expect("to transact");

        //     // Just for our peace of mind.
        //     let t = in_progress.dump_last_transaction().expect("transaction");
        //     assert_eq!(t.into_vector().expect("vector").len(), 5); // One add and one retract per field, and the :db/txInstant.

        //     // Our local change results in a record needing to be uploaded remotely.
        //     let sp = get_modified_sync_password_uuids_to_upload(&in_progress).expect("to get_sync_password");
        //     assert_eq!(sp, vec![LOGIN1.uuid.clone()]);

        //     // Suppose we disconnect, so that the last sync tx is TX0, and then reconnect.  We'll
        //     // have Sync 1.5 data in the store, and we'll need to upload it all.
        //     reset_client(&mut in_progress).expect("to reset_client");

        //     let sp = get_modified_sync_password_uuids_to_upload(&in_progress).expect("to get_sync_password");
        //     assert_eq!(sp, vec![LOGIN1.uuid.clone(), LOGIN2.uuid.clone()]);
        // }
    }
}
