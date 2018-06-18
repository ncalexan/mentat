// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

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
    // [:login/credential          :db.type/ref     :db.cardinality/one]
    // [:login/form                :db.type/ref     :db.cardinality/one]
    pub static ref LOGIN_AT: Keyword = {
        kw!(:login/at)
    };

    pub static ref LOGIN_DEVICE: Keyword = {
        kw!(:login/device)
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


pub fn add_credential(builder: &mut Builder<TypedValue>,
                      credential: Credential)
                      -> mentat::errors::Result<()>
{
    let c = Builder::tempid("c");

    builder.add(c.clone(),
                CREDENTIAL_ID.clone(),
                TypedValue::typed_string(credential.id));
    if let Some(username) = credential.username {
        builder.add(c.clone(),
                    CREDENTIAL_USERNAME.clone(),
                    TypedValue::String(username.into()));
    }
    builder.add(c.clone(),
                CREDENTIAL_PASSWORD.clone(),
                TypedValue::String(credential.password.into()));
    // TODO: set created to the transaction timestamp.  This might require implementing
    // (transaction-instant), which requires some thought because it is a "delayed binding".
    builder.add(c.clone(),
                CREDENTIAL_CREATED_AT.clone(),
                TypedValue::Instant(credential.created_at));
    if let Some(title) = credential.title {
        builder.add(c.clone(),
                    CREDENTIAL_TITLE.clone(),
                    TypedValue::String(title.into()));
    }

    Ok(())
}

pub fn get_credential<Q>(queryable: &Q,
                         id: CredentialId)
                         -> mentat::errors::Result<Option<Credential>>
where Q: Queryable {
    let q = r#"[:find
                (pull ?c [:credential/id :credential/username :credential/password :credential/createdAt :credential/title]) .
                :in
                ?id
                :where
                [?c :credential/id ?id]
               ]"#;

    let inputs = QueryInputs::with_value_sequence(vec![
        (var!(?id), TypedValue::typed_string(&id)),
    ]);

    let scalar = queryable.q_once(q, inputs)?.into_scalar()?;
    let credential = match scalar {
        Some(Binding::Map(cm)) => {
            let username = cm.get(&*CREDENTIAL_USERNAME).and_then(|username| username.as_string()).cloned().map(|x| x.cloned()); // XXX
            let password = cm[CREDENTIAL_PASSWORD.clone()].as_string().cloned().map(|x| x.cloned()).unwrap(); // XXX
            let created_at = cm[CREDENTIAL_CREATED_AT.clone()].as_instant().cloned().map(|x| x.clone()).unwrap(); // XXX
            let title = cm.get(&*CREDENTIAL_TITLE).and_then(|username| username.as_string()).cloned().map(|x| x.cloned()); // XXX
            // TODO: device.
            // TODO: form.

            Ok(Some(Credential {
                id: id,
                created_at,
                username,
                password,
                title,
            }))
        },
        Some(_) => bail!("bad query result types in get_credential"),
        None => Ok(None),
    };

    credential
}

pub fn get_all_credentials<Q>(queryable: &Q)
                              -> mentat::errors::Result<Vec<Credential>>
where Q: Queryable {
    let q = r#"[
:find
 [?id ...]
:where
 [_ :credential/id ?id]
:order
 (asc ?id)
]"#;

    let ids: mentat::errors::Result<Vec<_>> = queryable.q_once(q, None)?
        .into_coll()?
        .into_iter()
        .map(|id| {
            match id {
                Binding::Scalar(TypedValue::String(id)) => Ok(CredentialId((*id).clone())),
                _ => bail!("bad query result types in get_all_credentials"),
            }
        })
        .collect();
    let ids = ids?;

    let mut cs = Vec::with_capacity(ids.len());

    for id in ids {
        get_credential(queryable, id)?.map(|c| cs.push(c));
    }

    Ok(cs)
}

pub fn touch_by_id(builder: &mut Builder<TypedValue>,
               id: CredentialId,
               at: Option<DateTime<Utc>>,
               // TODO: device,
               // TOOD: form,
              )
               -> mentat::errors::Result<()> {
    let l = Builder::tempid("l");

    // New login.
    builder.add(l.clone(),
                LOGIN_AT.clone(),
                TypedValue::Instant(at.unwrap_or_else(|| mentat_core::now())));
    builder.add(l.clone(),
                LOGIN_CREDENTIAL.clone(),
                Builder::lookup_ref(CREDENTIAL_ID.clone(), TypedValue::typed_string(id)));

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use tests::{
        testing_store,
    };

    extern crate env_logger;

    lazy_static! {
        static ref CREDENTIAL1: Credential = {
            Credential {
                id: CredentialId("1".into()),
                username: Some("user1@mockymid.com".into()),
                password: "password1".into(),
                created_at: DateTime::<Utc>::from_micros(1523908112453),
                title: None,
            }
        };

        static ref CREDENTIAL2: Credential = {
            Credential {
                id: CredentialId("2".into()),
                username: Some("user2@mockymid.com".into()),
                password: "password2".into(),
                created_at: DateTime::<Utc>::from_micros(1523909000000),
                title: Some("marché".into()),  // Observe accented character.
            }
        };
    }

    #[test]
    fn test_credentials() {
        // env_logger::init();

        let mut store = testing_store();

        // Scoped borrow of `store`.
        {
            let mut in_progress = store.begin_transaction().expect("begun successfully");

            // First, let's add a single credential.
            let mut builder = Builder::<TypedValue>::new();
            add_credential(&mut builder, CREDENTIAL1.clone()).expect("to add_credential 1");
            in_progress.transact_entity_builder(builder).expect("to transact");

            let c = get_credential(&in_progress, CREDENTIAL1.id.clone()).expect("to get_credential 1");
            assert_eq!(Some(CREDENTIAL1.clone()), c);

            let cs = get_all_credentials(&in_progress).expect("to get_all_credentials 1");
            assert_eq!(vec![CREDENTIAL1.clone()], cs);
            
            // Now a second one.
            let mut builder = Builder::<TypedValue>::new();
            add_credential(&mut builder, CREDENTIAL2.clone()).expect("to add_credential 2");
            in_progress.transact_entity_builder(builder).expect("to transact");

            let c = get_credential(&in_progress, CREDENTIAL1.id.clone()).expect("to get_credential 1");
            assert_eq!(Some(CREDENTIAL1.clone()), c);

            let c = get_credential(&in_progress, CREDENTIAL2.id.clone()).expect("to get_credential 2");
            assert_eq!(Some(CREDENTIAL2.clone()), c);

            let cs = get_all_credentials(&in_progress).expect("to get_all_credentials 2");
            assert_eq!(vec![CREDENTIAL1.clone(), CREDENTIAL2.clone()], cs);
        }
    }
}
