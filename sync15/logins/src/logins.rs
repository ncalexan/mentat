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

use mentat_db;

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
    CredentialId,
    FormTarget,
    ServerPassword,
    SyncGuid,
};

lazy_static! {
    // [:credential/username       :db.type/string  :db.cardinality/one]
    // [:credential/password       :db.type/string  :db.cardinality/one]
    // [:credential/created        :db.type/instant :db.cardinality/one]
    // An application might allow users to name their credentials; e.g., "My LDAP".
    // [:credential/title          :db.type/string  :db.cardinality/one]

    static ref CREDENTIAL_ID: Keyword = {
        kw!(:credential/id)
    };

    static ref CREDENTIAL_USERNAME: Keyword = {
        kw!(:credential/username)
    };

    static ref CREDENTIAL_PASSWORD: Keyword = {
        kw!(:credential/password)
    };

    static ref CREDENTIAL_CREATED_AT: Keyword = {
        kw!(:credential/createdAt)
    };

    static ref CREDENTIAL_TITLE: Keyword = {
        kw!(:credential/title)
    };

    static ref CREDENTIAL_VOCAB: vocabulary::Definition = {
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


    // A 'form' is either an HTTP login box _or_ a web form.
    // [:http/realm                :db.type/string  :db.cardinality/one]
    // It's possible that hostname or submitOrigin are unique-identity attributes.
    // [:form/hostname             :db.type/string  :db.cardinality/one]
    // [:form/submitOrigin         :db.type/string  :db.cardinality/one]
    // [:form/usernameField        :db.type/string  :db.cardinality/one]
    // [:form/passwordField        :db.type/string  :db.cardinality/one]
    // This is our many-to-many relation between forms and credentials.
    // [:form/credential           :db.type/ref     :db.cardinality/many]
    static ref FORM_HOSTNAME: Keyword = {
        kw!(:form/hostname)
    };

    static ref FORM_SUBMIT_URL: Keyword = {
        kw!(:form/submitUrl)
    };

    static ref FORM_USERNAME_FIELD: Keyword = {
        kw!(:form/usernameField)
    };

    static ref FORM_PASSWORD_FIELD: Keyword = {
        kw!(:form/passwordField)
    };

    static ref FORM_CREDENTIAL: Keyword = {
        kw!(:form/credential)
    };

    static ref FORM_HTTP_REALM: Keyword = {
        kw!(:form/httpRealm)
    };

    // This is arguably backwards.  In the future, we'd like forms to be independent of Sync 1.5
    // password records, in the way that we're making credentials independent of password records.
    // For now, however, we don't want to add an identifier and identify forms by content, so we're
    // linking a form to a unique Sync password.  Having the link go in this direction lets us
    // upsert the form.
    static ref FORM_SYNC_PASSWORD: Keyword = {
        kw!(:form/syncPassword)
    };

    static ref FORM_VOCAB: vocabulary::Definition = {
        vocabulary::Definition {
            name: kw!(:org.mozilla/form),
            version: 1,
            attributes: vec![
                (FORM_SYNC_PASSWORD.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::Ref)
                 .multival(false)
                 .unique(vocabulary::attribute::Unique::Identity)
                 .build()),
                (FORM_HOSTNAME.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::String)
                 .multival(false)
                 .build()),
                (FORM_SUBMIT_URL.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::String)
                 .multival(false)
                 .build()),
                (FORM_USERNAME_FIELD.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::String)
                 .multival(false)
                 .build()),
                (FORM_PASSWORD_FIELD.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::String)
                 .multival(false)
                 .build()),
                (FORM_CREDENTIAL.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::Ref)
                 .multival(true)
                 .build()),
                (FORM_HTTP_REALM.clone(),
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
    // [:login/url                 :db.type/string  :db.cardinality/one]
    // [:login/credential          :db.type/ref     :db.cardinality/one]
    // [:login/form                :db.type/ref     :db.cardinality/one]
    static ref LOGIN_AT: Keyword = {
        kw!(:login/at)
    };

    static ref LOGIN_URL: Keyword = {
        kw!(:login/url)
    };

    static ref LOGIN_CREDENTIAL: Keyword = {
        kw!(:login/credential)
    };

    static ref LOGIN_FORM: Keyword = {
        kw!(:login/form)
    };

    static ref LOGIN_VOCAB: vocabulary::Definition = {
        vocabulary::Definition {
            name: kw!(:org.mozilla/login),
            version: 1,
            attributes: vec![
                (LOGIN_AT.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::Instant)
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

    static ref SYNC_PASSWORD_CREDENTIAL: Keyword = {
        kw!(:sync.password/credential)
    };

    static ref SYNC_PASSWORD_UUID: Keyword = {
        kw!(:sync.password/uuid)
    };

    // Use materialTx for material change comparisons, metadataTx for metadata change
    // comparisons.  Downloading updates materialTx only.  We only use materialTx to
    // determine whether or not to upload.  Uploaded records are built using metadataTx,
    // however.  Successful upload sets both materialTx and metadataTx.
    static ref SYNC_PASSWORD_MATERIAL_TX: Keyword = {
        kw!(:sync.password/materialTx)
    };

    static ref SYNC_PASSWORD_METADATA_TX: Keyword = {
        kw!(:sync.password/metadataTx)
    };

    static ref SYNC_PASSWORD_SERVER_MODIFIED: Keyword = {
        kw!(:sync.password/serverModified)
    };

    static ref SYNC_PASSWORD_TIMES_USED: Keyword = {
        kw!(:sync.password/timesUsed)
    };

    static ref SYNC_PASSWORD_TIME_CREATED: Keyword = {
        kw!(:sync.password/timeCreated)
    };

    static ref SYNC_PASSWORD_TIME_LAST_USED: Keyword = {
        kw!(:sync.password/timeLastUsed)
    };

    static ref SYNC_PASSWORD_TIME_PASSWORD_CHANGED: Keyword = {
        kw!(:sync.password/timePasswordChanged)
    };

    static ref SYNC_PASSWORD_VOCAB: vocabulary::Definition = {
        vocabulary::Definition {
            name: kw!(:org.mozilla.sync/login),
            version: 1,
            attributes: vec![
                (SYNC_PASSWORD_CREDENTIAL.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::Ref)
                 .multival(false)
                 .unique(vocabulary::attribute::Unique::Identity)
                 .build()),
                (SYNC_PASSWORD_UUID.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::String)
                 .multival(false)
                 .unique(vocabulary::attribute::Unique::Identity)
                 .build()),
                (SYNC_PASSWORD_MATERIAL_TX.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::Ref)
                 .multival(false)
                 .build()),
                (SYNC_PASSWORD_METADATA_TX.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::Ref)
                 .multival(false)
                 .build()),
                (SYNC_PASSWORD_SERVER_MODIFIED.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::Instant)
                 .multival(false)
                 .build()),
                (SYNC_PASSWORD_TIMES_USED.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::Long)
                 .multival(false)
                 .build()),
                (SYNC_PASSWORD_TIME_CREATED.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::Instant)
                 .multival(false)
                 .build()),
                (SYNC_PASSWORD_TIME_LAST_USED.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::Instant)
                 .multival(false)
                 .build()),
                (SYNC_PASSWORD_TIME_PASSWORD_CHANGED.clone(),
                 vocabulary::AttributeBuilder::helpful()
                 .value_type(ValueType::Instant)
                 .multival(false)
                 .build()),
            ],
            pre: Definition::no_op,
            post: Definition::no_op,
        }
    };

}

// Note that in a Mentat-native world there's no need for a GUID.

// This data model can be used to derive a record for Firefox Sync by aggregating login events into counts and timestamps, synthesizing a single record for each (credential, form) pair.

// To manage this in Sync, a relation similar to :form/credential can be reified and given a stable GUID:

// func getLogin(_ record: Record<LoginPayload>) -> ServerPassword {
//     let guid = record.id
//     let payload = record.payload
//     let modified = record.modified

//     let login = ServerPassword(guid: guid, hostname: payload.hostname, username: payload.username, password: payload.password, modified: modified)
//     login.formSubmitURL = payload.formSubmitURL
//     login.httpRealm = payload.httpRealm
//     login.usernameField = payload.usernameField
//     login.passwordField = payload.passwordField

//     // Microseconds locally, milliseconds remotely. We should clean this up.
//     login.timeCreated = 1000 * (payload.timeCreated ?? 0)
//     login.timeLastUsed = 1000 * (payload.timeLastUsed ?? 0)
//     login.timePasswordChanged = 1000 * (payload.timePasswordChanged ?? 0)
//     login.timesUsed = payload.timesUsed ?? 0
//     return login
// }


// fn transact_sync_password(in_progress: &mut InProgress,
//                        c: Option<KnownEntid>,
//                        f: Option<KnownEntid>,
//                        login: ServerPassword)
//                        -> mentat::errors::Result<()> {
//     let mut builder = Builder::new();
//     let c: KnownEntidOr<TempIdHandle> = match c {
//         Some(c) => Either::Left(c),
//         None => Either::Right(builder.named_tempid("c".into())),
//     };
//     builder.add(c.clone(),
//              in_progress.get_entid(&CREDENTIAL_CREATED).expect(":credential"),
//              TypedValue::Instant(login.time_created))?;
//     if let Some(username) = login.username {
//         builder.add(c.clone(),
//                  in_progress.get_entid(&CREDENTIAL_USERNAME).expect(":credential"),
//                  TypedValue::String(username.into()))?;
//     }
//     builder.add(c.clone(),
//              in_progress.get_entid(&CREDENTIAL_PASSWORD).expect(":credential"),
//              TypedValue::String(login.password.into()))?;

//     let f: KnownEntidOr<TempIdHandle> = match f {
//         Some(f) => Either::Left(f),
//         None => Either::Right(builder.named_tempid("f".into())),
//     };
//     builder.add(f.clone(),
//              in_progress.get_entid(&FORM_HOSTNAME).expect(":form"),
//              TypedValue::String(login.hostname.into()))?;

//     match login.target {
//         FormTarget::FormSubmitURL(form_submit_url) => {
//             builder.add(f.clone(),
//                      in_progress.get_entid(&FORM_SUBMIT_ORIGIN).expect(":form"),
//                      TypedValue::String(form_submit_url.into()))?;
//         }
//         FormTarget::HttpRealm(http_realm) => {
//             builder.add(f.clone(),
//                      in_progress.get_entid(&FORM_HTTP_REALM).expect(":form"),
//                      TypedValue::String(http_realm.into()))?;
//         }
//     }

//     if let Some(username_field) = login.username_field {
//         builder.add(f.clone(),
//                  in_progress.get_entid(&FORM_USERNAME_FIELD).expect(":form"),
//                  TypedValue::String(username_field.into()))?;
//     }
//     if let Some(password_field) = login.password_field {
//         builder.add(f.clone(),
//                  in_progress.get_entid(&FORM_PASSWORD_FIELD).expect(":form"),
//                  TypedValue::String(password_field.into()))?;
//     }

//     let sl = builder.named_tempid("sl".into());
//     builder.add(sl.clone(),
//              in_progress.get_entid(&SYNC_PASSWORD_UUID).expect(":sync.password"),
//              TypedValue::String(login.uuid.into()))?;

//     // let cv: TypedValueOr<TempIdHandle> = match c {
//     //     Some(c) => {
//     //         Either::Left(c.into())
//     //     },
//     //     None => {
//     //         Either::Right(builder.named_tempid("c".into()))
//     //     },
//     // };

//     builder.add(sl.clone(),
//              in_progress.get_entid(&SYNC_PASSWORD_CREDENTIAL).expect(":sync.password"),
//              c)?;
//     builder.add(sl.clone(),
//              in_progress.get_entid(&SYNC_PASSWORD_FORM).expect(":sync.password"),
//              f)?;

//     in_progress.transact_builder(builder).and(Ok(()))

//     // [:sync.password/form                   :db.type/ref    :db.cardinality/one]
//     // [:sync.password/credential             :db.type/ref    :db.cardinality/one]
//     // [:sync.password/uuid                   :db.type/string :db.cardinality/one :db.unique/identity]
// }

// fn add_sync_password(in_progress: &mut InProgress, login: ServerPassword) -> mentat::errors::Result<()> {
//     transact_sync_password(in_progress, None, None, login)
// }

fn find_sync_password_credential_and_form
    (store: &mut Store,
     uuid: String)
     -> mentat::errors::Result<Option<(KnownEntid, KnownEntid)>> {
    let vs = QueryBuilder::new(store,
                               "[:find [?c ?f] :where [?login :sync.password/uuid ?uuid] [?login \
                                :sync.password/credential ?c] [?login :sync.password/form ?f]]")
        .bind_value("?uuid", uuid.clone())
        .execute_tuple()
        .expect("to execute query");

    Ok(vs.map(|vs| {
        match (vs.len(), vs.get(0), vs.get(1)) {
            (2, Some(c), Some(f)) => {
                ((c.clone().into_known_entid().expect("c")),
                 (f.clone().into_known_entid().expect("f")))
            }
            _ => unreachable!("Boom"),
        }
    }))
}

// fn update_sync_password(store: &mut Store, login: ServerPassword) -> mentat::errors::Result<()> {
//     let (c, f) = find_sync_password_credential_and_form(store, login.uuid.clone())
//         ?
//         .expect("to find sync login");

//     let mut in_progress = store.begin_transaction().expect("begun successfully");

//     transact_sync_password(&mut in_progress, Some(c), Some(f), login)?;

//     // If we commit, it'll stick around.
//     in_progress.commit().and(Ok(()))
// }

// TODO: Into<Option<DateTime<Utc>>>.
fn touch_by_id(builder: &mut Builder<TypedValue>,
               id: CredentialId,
               at: Option<DateTime<Utc>>)
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

// fn touch_sync_password_by_uuid(builder: &mut Builder<TypedValue>,
//                             uuid: String,
//                             at: Option<DateTime<Utc>>)
//                             -> mentat::errors::Result<()> {
//     // let (c, _f) =
//     //     find_sync_password_credential_and_form(store, uuid.clone())?.expect("to find sync login");

//     // let t = builder.named_tempid("t".into());
//     let t = Builder::tempid("t")

//     builder.add(t.clone(),
//                 LOGIN_AT.clone(),
//                 TypedValue::Instant(at.unwrap_or_else(|| mentat_core::now())))?;
//     // builder.add(t.clone(), in_progress.get_entid(&LOGIN_URL).expect(":login"), TypedValue::String(login.time_created))?;
//     builder.add(t.clone(),
//                 LOGIN_CREDENTIAL.clone(),
//                 "c")?; XXX
//     // builder.add(t.clone(), in_progress.get_entid(&LOGIN_FORM).expect(":login"), f)?;

//     in_progress.transact_builder(builder)?;

//     in_progress.commit().and(Ok(()))
// }

// fn add_credential(in_progress: &mut InProgress, username: String, password: Option<String>, title: Option<String>) -> mentat::errors::Result<()> {
//     // [:credential/username       :db.type/string  :db.cardinality/one]
//     // [:credential/password       :db.type/string  :db.cardinality/one]
//     // [:credential/created        :db.type/instant :db.cardinality/one]
//     // An application might allow users to name their credentials; e.g., "My LDAP".
//     // [:credential/title          :db.type/string  :db.cardinality/one]

//     let mut builder = TermBuilder::new();
//     let c = builder.named_tempid("c".into());
//     builder.add(c.clone(), in_progress.get_entid(&CREDENTIAL_CREATED).expect(":credential"), TypedValue::Instant(mentat_core::now()))?;
//     builder.add(c.clone(), in_progress.get_entid(&CREDENTIAL_USERNAME).expect(":credential"), TypedValue::String(username.into()))?;
//     if let Some(password) = password {
//         builder.add(c.clone(), in_progress.get_entid(&CREDENTIAL_PASSWORD).expect(":credential"), TypedValue::String(password.into()))?;
//     }
//     if let Some(title) = title {
//         builder.add(c.clone(), in_progress.get_entid(&CREDENTIAL_TITLE).expect(":credential"), TypedValue::String(title.into()))?;
//     }

//     in_progress.transact_builder(builder).and(Ok(()))
// }

// fn add_form(in_progress: &mut InProgress, hostname: String, submit_origin: String, username_field: Option<String>, password_field: Option<String>, http_realm: Option<String>) -> mentat::errors::Result<()> {
//     // A 'form' is either an HTTP login box _or_ a web form.
//     // [:http/realm                :db.type/string  :db.cardinality/one]
//     // It's possible that hostname or submitOrigin are unique-identity attributes.
//     // [:form/hostname             :db.type/string  :db.cardinality/one]
//     // [:form/submitOrigin         :db.type/string  :db.cardinality/one]
//     // [:form/usernameField        :db.type/string  :db.cardinality/one]
//     // [:form/passwordField        :db.type/string  :db.cardinality/one]
//     // This is our many-to-many relation between forms and credentials.
//     // [:form/credential           :db.type/ref     :db.cardinality/many]
//     let mut builder = TermBuilder::new();
//     let c = builder.named_tempid("c".into());
//     builder.add(c.clone(), in_progress.get_entid(&FORM_HOSTNAME).expect(":form"), TypedValue::String(hostname.into()))?;
//     builder.add(c.clone(), in_progress.get_entid(&FORM_SUBMIT_ORIGIN).expect(":form"), TypedValue::String(submit_origin.into()))?;
//     if let Some(username_field) = username_field {
//         builder.add(c.clone(), in_progress.get_entid(&FORM_USERNAME_FIELD).expect(":form"), TypedValue::String(username_field.into()))?;
//     }
//     if let Some(password_field) = password_field {
//         builder.add(c.clone(), in_progress.get_entid(&FORM_PASSWORD_FIELD).expect(":form"), TypedValue::String(password_field.into()))?;
//     }
//     if let Some(http_realm) = http_realm {
//         builder.add(c.clone(), in_progress.get_entid(&FORM_HTTP_REALM).expect(":form"), TypedValue::String(http_realm.into()))?;
//     }

//     in_progress.transact_builder(builder).and(Ok(()))
// }

// fn add_login_usage(store: &mut Store, id: String, ) -> mentat::errors::Result<mentat_db::TxReport> {
//     store.transact(
//         r#"[{:login/at :db/txInstant
//              :login/url 1
//              :login/credential 2
//              :login/form 3}]"#)
// }

pub fn find_sync_password_by_uuid<T>(queryable: &T,
                                  uuid: SyncGuid)
                                  -> mentat::errors::Result<Option<(String, String)>>
    where T: Queryable
{
    // TODO: this will be much easier to express with the pull API, tracked by
    // https://github.com/mozilla/mentat/issues/110.
    match queryable.q_once("[:find [?username ?password] :in ?uuid :where [?login :sync.password/uuid ?uuid] [?login :sync.password/credential ?credential] [?credential :credential/username ?username] [?credential :credential/password ?password]]",
                           QueryInputs::with_value_sequence(vec![(var!(?uuid), TypedValue::typed_string(uuid))]))?.results {
        QueryResults::Tuple(Some(vs)) => {
            match (vs.len(), vs.get(0), vs.get(1)) {
                (2, Some(&Binding::Scalar(TypedValue::String(ref username))), Some(&Binding::Scalar(TypedValue::String(ref password)))) => Ok(Some(((**username).clone(), (**password).clone()))),
                _ => unreachable!("bad query result types in find_sync_password_by_uuid"),
            }
        },
        QueryResults::Tuple(None) => {
            Ok(None)
        },
        _ => unreachable!("bad query in find_sync_password_by_uuid"),
    }
}

pub fn find_sync_password_by_uuid_deltas<Q>
    (in_progress: &Q,
     uuid: String)
     -> mentat::errors::Result<Option<(String, Entid, DateTime<Utc>, String, Entid, DateTime<Utc>)>>
    where Q: Queryable
{
    // TODO: this will be much easier to express with the pull API, tracked by
    // https://github.com/mozilla/mentat/issues/110.

    let q = r#"[:find
                [?username ?username-tx ?username-txInstant ?password ?password-tx ?password-txInstant]
                :in
                ?uuid
                :where
                [?login :sync.password/uuid ?uuid]
                [?login :sync.password/credential ?credential]
                [?credential :credential/username ?username ?username-tx]
                [?username-tx :db/txInstant ?username-txInstant]
                [?credential :credential/password ?password ?password-tx]
                [?password-tx :db/txInstant ?password-txInstant]]"#;
    let inputs =
        QueryInputs::with_value_sequence(vec![(var!(?uuid), TypedValue::String(uuid.into()))]);

    match in_progress.q_once(q, inputs)?.into_tuple()? {
        Some(vs) => {
            match (vs.len(), (vs.get(0), vs.get(1), vs.get(2), vs.get(3), vs.get(4), vs.get(5))) {
                (6,
                 (Some(&Binding::Scalar(TypedValue::String(ref username))),
                  Some(&Binding::Scalar(TypedValue::Ref(username_tx))),
                  Some(&Binding::Scalar(TypedValue::Instant(ref username_tx_instant))),
                  Some(&Binding::Scalar(TypedValue::String(ref password))),
                  Some(&Binding::Scalar(TypedValue::Ref(password_tx))),
                  Some(&Binding::Scalar(TypedValue::Instant(password_tx_instant))))) => {
                    Ok(Some(((**username).clone(),
                             username_tx,
                             username_tx_instant.clone(),
                             (**password).clone(),
                             password_tx,
                             password_tx_instant.clone())))
                }
                _ => unreachable!("bad query result types in find_sync_password_by_uuid_deltas"),
            }
        }
        None => Ok(None),
        // _ => unreachable!("bad query in find_sync_password_by_uuid_deltas"),
    }
}

pub fn credential_deltas<Q>
    (in_progress: &Q,
     id: CredentialId)
     -> mentat::errors::Result<Option<(String, Entid, DateTime<Utc>, String, Entid, DateTime<Utc>)>>
    where Q: Queryable
{
    let q = r#"[:find
                [?username ?username-tx ?username-txInstant ?password ?password-tx ?password-txInstant]
                :in
                ?id
                :where
                [?credential :credential/id ?id]
                [?credential :credential/username ?username ?username-tx]
                [?username-tx :db/txInstant ?username-txInstant]
                [?credential :credential/password ?password ?password-tx]
                [?password-tx :db/txInstant ?password-txInstant]]"#;
    let inputs = QueryInputs::with_value_sequence(vec![(var!(?id), TypedValue::typed_string(id))]);

    match in_progress.q_once(q, inputs)?.into_tuple()? {
        Some(vs) => {
            match (vs.len(), (vs.get(0), vs.get(1), vs.get(2), vs.get(3), vs.get(4), vs.get(5))) {
                (6,
                 (Some(&Binding::Scalar(TypedValue::String(ref username))),
                  Some(&Binding::Scalar(TypedValue::Ref(username_tx))),
                  Some(&Binding::Scalar(TypedValue::Instant(ref username_tx_instant))),
                  Some(&Binding::Scalar(TypedValue::String(ref password))),
                  Some(&Binding::Scalar(TypedValue::Ref(password_tx))),
                  Some(&Binding::Scalar(TypedValue::Instant(password_tx_instant))))) => {
                    Ok(Some(((**username).clone(),
                             username_tx,
                             username_tx_instant.clone(),
                             (**password).clone(),
                             password_tx,
                             password_tx_instant.clone())))
                }
                _ => unreachable!("bad query result types in find_sync_password_by_uuid_deltas"),
            }
        }
        None => Ok(None),
    }
}

pub fn find_sync_password_by_content(queryable: &mut Store,
                                  login: &ServerPassword)
                                  -> mentat::errors::Result<Option<String>> {
    // where T: Queryable {
    // TODO: this will be much easier to express with the pull API, tracked by
    // https://github.com/mozilla/mentat/issues/110.

    let q = r#"[:find [?uuid]
                :in
                ?hostname ?username ?target
                :where
                [?login :sync.password/uuid ?uuid]
                [?form :form/syncPassword ?login]
                [?form :form/hostname ?hostname]
                [?login :sync.password/credential ?credential]
]"#;

    let mut qb = QueryBuilder::new(queryable, q);
    // qb.
    //     .bind_value("?v", true)
    //     .execute_tuple()
    // .expect("CollResult");
    //     let entid = results.get(1).map_or(None, |t| t.to_owned().into_entid()).expect("entid");

    // let (target_fragment, target_binding) =
    match login.target {
        FormTarget::FormSubmitURL(ref form_submit_url) => {
            qb.add_where("[?form :form/submitUrl ?target]");
            qb.add_where("(not [?form :form/httpRealm _])");
            qb.bind_value("?target", form_submit_url.clone());
            // "[?form :form/submitOrigin ?target]"
        }
        FormTarget::HttpRealm(ref http_realm) => {
            qb.add_where("[?form :form/httpRealm ?target]");
            qb.add_where("(not [?form :form/submitUrl _])");
            qb.bind_value("?target", http_realm.clone());
            // "[?form :form/httpRealm ?target]"
        }
    };

    qb.bind_value("?hostname", login.hostname.clone());

    match login.username {
        Some(ref username) => {
            qb.add_where("[?credential :credential/username ?username]");
            qb.bind_value("?username", username.clone());
        }
        None => {
            qb.add_where("(not [?credential :credential/username _])");
        }
    }

    match qb.execute_tuple()? {
        Some(vs) => {
            match (vs.len(), vs.get(0)) { // , vs.get(1), vs.get(2)) {
                (1, Some(&Binding::Scalar(TypedValue::String(ref uuid)))) => Ok(Some((**uuid).clone())),
                _ => unreachable!("bad query result types in find_sync_password_by_content"),
            }
        }
        None => Ok(None),
    }

    // match queryable.q_once("[:find [?uuid] :in ?hostname ?httpRealm ?username :where [?login :sync.password/uuid ?uuid] [?login :sync.password/form ?form] [?form :form/hostname ?hostname] [?form :form/httpRealm ?httpRealm] [?login :sync.password/credential ?credential] [?credential :credential/username ?username]]",
    //                        QueryInputs::with_value_sequence(vec![(var!(?hostname),  TypedValue::String(login.hostname.clone().into())),
    //                                                              (var!(?httpRealm), TypedValue::String(login.http_realm.clone().expect("XXX").into())),
    //                                                              (var!(?username),  TypedValue::String(login.username.clone().expect("YYY").into())),
    //                        ]))?.results {
    //     QueryResults::Tuple(Some(vs)) => {
    //         match (vs.len(), vs.get(0)) { // , vs.get(1), vs.get(2)) {
    //             (1, Some(&Binding::Scalar(TypedValue::String(ref uuid))) => Ok(Some(((**uuid).clone()))),
    //             _ => unreachable!("bad query result types in find_sync_password_by_content"),
    //         }
    //     },
    //     QueryResults::Tuple(None) => {
    //         Ok(None)
    //     },
    //     _ => unreachable!("bad query in find_sync_password_by_content"),
}

pub fn time_sync_password_modified<Q>(queryable: &Q, uuid: SyncGuid) -> mentat::errors::Result<Option<DateTime<Utc>>>
    where Q: Queryable
{

    let remote_time_sync_password_modified = {
        let q = r#"[:find
                ?t .
                :in
                ?uuid
                :where
                [?sp :sync.password/uuid ?uuid]
                [?sp :sync.password/serverModified ?t]
               ]"#;

        let result = queryable.q_once(q, QueryInputs::with_value_sequence(vec![(var!(?uuid), TypedValue::typed_string(&uuid))]))?.into_scalar()?;

        match result {
            Some(Binding::Scalar(TypedValue::Instant(t))) => t,
            Some(_) => bail!("bad query result types in XXX"),
            None => return Ok(None),
        }
    };

    info!("time_sync_password_modified: remote_time_sync_password_modified: {:?}", remote_time_sync_password_modified);

    let local_time_sync_password_modified = {
        let q = r#"[:find
                 (max ?txI) .
                :in
                ?uuid
                :where
                [?sp :sync.password/uuid ?uuid]
                [?sp :sync.password/materialTx ?materialTx]

                (or-join [?sp ?a ?tx]
                 (and
                  [?sp :sync.password/credential ?c]
                  [?c ?a _ ?tx]
                  [(ground [:credential/id :credential/username :credential/password]) [?a ...]])
                 (and
                  [?f :form/syncPassword ?sp]
                  [?f ?a _ ?tx]
                  [(ground [:form/hostname :form/usernameField :form/passwordField :form/submitUrl :form/httpRealm]) [?a ...]]))

               [(tx-after ?tx ?materialTx)]
               [?tx :db/txInstant ?txI]

               ]"#;

        let result = queryable.q_once(q, QueryInputs::with_value_sequence(vec![(var!(?uuid), TypedValue::typed_string(&uuid))]))?.into_scalar()?;

        match result {
            Some(Binding::Scalar(TypedValue::Instant(t))) => Some(t),
            Some(_) => bail!("bad query result types in XXX"),
            None => None,
        }
    };

    info!("time_sync_password_modified: local_time_sync_password_modified: {:?}", local_time_sync_password_modified);

    let mut is = vec![];
    is.push(remote_time_sync_password_modified);

    local_time_sync_password_modified.map(|t| is.push(t));

    Ok(is.into_iter().max())
}

pub fn get_sync_password<Q>(queryable: &Q,
                            id: SyncGuid)
                            -> mentat::errors::Result<Option<ServerPassword>>
where Q: Queryable + Dumpable {
    let q = r#"[:find
                [?c
                 (pull ?c [:credential/id :credential/username :credential/password :credential/createdAt])
                 (pull ?f [:form/hostname :form/usernameField :form/passwordField :form/submitUrl :form/httpRealm])]
                :in
                ?uuid
                :where
                [?sp :sync.password/uuid ?uuid]
                [?sp :sync.password/credential ?c]
                [?f :form/syncPassword ?sp]
               ]"#;

    let inputs = QueryInputs::with_value_sequence(vec![
        (var!(?uuid), TypedValue::typed_string(&id)),
    ]);

    // debug!("{}", queryable.dump_sql_query("SELECT e, a, v, tx FROM datoms ORDER BY e, a, v, tx", &[]).expect("debug"));

    let tuple = queryable.q_once(q, inputs.clone())?.into_tuple()?;
    let server_password = match tuple {
        Some(bindings) => {
            let mut it = bindings.into_iter();
            let c: Entid = it.next().unwrap().into_entid().unwrap();

            let cm = it.next().unwrap().into_map().unwrap();
            let cid = CredentialId(cm[CREDENTIAL_ID.clone()].as_string().cloned().map(|x| x.cloned()).unwrap()); // XXX
            let username = cm[CREDENTIAL_USERNAME.clone()].as_string().cloned().map(|x| x.cloned()); // XXX
            let password = cm[CREDENTIAL_PASSWORD.clone()].as_string().cloned().map(|x| x.cloned()).unwrap(); // XXX
            let time_created = cm[CREDENTIAL_CREATED_AT.clone()].as_instant().cloned().map(|x| x.clone()).unwrap(); // XXX
            let fm = it.next().unwrap().into_map().unwrap();
            let hostname = fm.0.get(&FORM_HOSTNAME.clone()).and_then(|x| x.as_string()).cloned().map(|x| x.cloned()).unwrap(); // XXX
            let username_field = fm.0.get(&FORM_USERNAME_FIELD.clone()).and_then(|x| x.as_string()).cloned().map(|x| x.cloned()); // XXX
            let password_field = fm.0.get(&FORM_PASSWORD_FIELD.clone()).and_then(|x| x.as_string()).cloned().map(|x| x.cloned()); // XXX

            let form_submit_url = fm.0.get(&FORM_SUBMIT_URL.clone()).and_then(|x| x.as_string()).cloned().map(|x| x.cloned()); // XXX
            let http_realm = fm.0.get(&FORM_HTTP_REALM.clone()).and_then(|x| x.as_string()).cloned().map(|x| x.cloned()); // XXX

            let target = match (form_submit_url, http_realm) {
                // Logins with both a formSubmitURL and httpRealm are not valid.
                (Some(_), Some(_)) => bail!("bad target"),
                (Some(form_submit_url), _) => FormTarget::FormSubmitURL(form_submit_url),
                (_, Some(http_realm)) => FormTarget::HttpRealm(http_realm),
                // Login must have at least a formSubmitURL or httpRealm.
                _ => bail!("no target"),
            };

            Ok(Some(ServerPassword {
                modified: time_sync_password_modified(queryable, id.clone())?.expect("time_sync_password_modified"),
                uuid: id.clone(),
                hostname: hostname,
                target: target,
                username,
                password,
                username_field,
                password_field,
                time_created,
                time_password_changed: time_password_changed(queryable, id.clone())?.expect("time_password_changed"),
                time_last_used: time_last_used(queryable, cid.clone())?,
                times_used: times_used(queryable, cid.clone())? as usize,
            }))
        },
        None => Ok(None),
    };

    server_password
}


// ;; This is metadata recording user behavior.
// [:login/at                  :db.type/instant :db.cardinality/one]
// [:login/url                 :db.type/string  :db.cardinality/one]
// [:login/credential          :db.type/ref     :db.cardinality/one]
// [:login/form                :db.type/ref     :db.cardinality/one]

pub fn find_recent_sync_passwords(queryable: &mut Store,
                               limit: Option<usize>)
                               -> mentat::errors::Result<Vec<DateTime<Utc>>> {
    // TODO: this will be much easier to express with the pull API, tracked by
    // https://github.com/mozilla/mentat/issues/110.
    let q = r#"[:find ?at
                :where
                [?login :login/credential ?c]
                [?login :login/at ?at]
                :order
                ?at
]"#;

    let mut qb = QueryBuilder::new(queryable, q);
    if let Some(limit) = limit {
        qb.add_limit(&format!("{}", limit));
    }

    let logins = qb.execute_rel()?
        .into_iter()
        .map(|vs| {
            match (vs.len(), vs.get(0)) {
                (1, Some(&Binding::Scalar(TypedValue::Instant(ref inst)))) => Ok(inst.clone()),
                _ => bail!("bad query result types in find_recent_sync_passwords"),
            }
        })
        .collect::<mentat::errors::Result<Vec<_>>>()?;

    Ok(logins)
}

fn add_credential<I>(builder: &mut Builder<TypedValue>,
                  id: CredentialId,
                  username: Option<String>,
                  password: String,
                  created: I)
                  -> mentat::errors::Result<()>
    where I: Into<Option<DateTime<Utc>>>
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

    Ok(())
}

// TODO: figure out the API that allows to express optional typed results, where None is okay but Some(bad) is not OK, like:
// Option<TypedValue::String> -> Result<String, E>::Ok(...)
// Option<TypedValue::_> -> Result<String, E>::Err(...)

pub fn find_credential_id_by_sync_password_uuid<Q>(queryable: &Q,
                                                uuid: SyncGuid)
                                                -> mentat::errors::Result<Option<CredentialId>>
    where Q: Queryable
{
    let q = r#"[:find ?id .
                :in
                ?uuid
                :where
                [?c :credential/id ?id]
                [?l :sync.password/credential ?c]
                [?l :sync.password/uuid ?uuid]
]"#;

    let inputs = QueryInputs::with_value_sequence(vec![(var!(?uuid), TypedValue::typed_string(uuid))]);
    match queryable.q_once(q, inputs)?.into_scalar()? {
        Some(x) => {
            match x.into_string() {
                Some(x) => Ok(Some(CredentialId((*x).clone()))),
                None => bail!("bad query result type"),
            }
        }
        None => Ok(None),
    }
}

pub fn find_credential_id_by_content<Q>(queryable: &Q,
                                        username: String,
                                        password: String)
                                        -> mentat::errors::Result<Option<CredentialId>>
    where Q: Queryable
{
    // TODO: this will be much easier to express with the pull API, tracked by
    // https://github.com/mozilla/mentat/issues/110.

    let q = r#"[:find ?id .
                :in
                ?username ?password
                :where
                [?c :credential/id ?id]
                [?c :credential/username ?username]
                [?c :credential/password ?password]
]"#;

    let inputs = QueryInputs::with_value_sequence(vec![(var!(?username), TypedValue::String(username.clone().into())),
                                                       (var!(?password), TypedValue::String(password.clone().into()))]);
    match queryable.q_once(q, inputs)?.into_scalar()? {
        Some(x) => {
            match x.into_string() {
                Some(x) => Ok(Some(CredentialId((*x).clone()))),
                None => bail!("bad query result type"),
            }
        }
        None => Ok(None),
    }
}

// TODO: u64.
pub fn times_used<Q>(queryable: &Q, id: CredentialId) -> mentat::errors::Result<i64>
    where Q: Queryable
{
    // TODO: this will be much easier to express with the pull API, tracked by
    // https://github.com/mozilla/mentat/issues/110.

    let sync_mirror = {
        // Scope borrow of store.
        let q = r#"[:find
                [?sl ?timesUsed ?tx]
                :in
                ?id
                :where
                [?c :credential/id ?id]
                [?sl :sync.password/credential ?c]
                [?sl :sync.password/timesUsed ?timesUsed]
                [?sl :sync.password/metadataTx ?tx]
               ]"#;

        // let results = self.execute()?;
        // results.into_tuple().map_err(|e| e.into())

        // let mut qb = QueryBuilder::new(store, q);
        // qb.bind_value("?id", id.clone());

        let sync_mirror: mentat::errors::Result<_> = match queryable.q_once(q, QueryInputs::with_value_sequence(vec![(var!(?id), TypedValue::typed_string(&id))]))?.into_tuple()? {
            Some(vs) => {
                match (vs.len(), vs.get(0), vs.get(1), vs.get(2)) {
                    (3, Some(&Binding::Scalar(TypedValue::Ref(sl))), Some(&Binding::Scalar(TypedValue::Long(times_used))), Some(&Binding::Scalar(TypedValue::Ref(tx)))) => {
                        Ok(Some((KnownEntid(sl), times_used, KnownEntid(tx))))
                    },
                    _ => bail!("bad query result types in find_recent_sync_passwords"),
                }
            },
            None => Ok(None),
        };
        sync_mirror?
    };

    info!("times_used: sync_mirror: {:?}", sync_mirror);

    // timesUsed id => [
    // :find
    // ?remoteTimesUsed (count ?login)
    // :in
    // ?id
    // :where
    // [?credential :credential/id ?id]
    // [?login :login/credential ?credential]
    // [?login :login/at _ ?login-tx]
    // (tx-after ?login-tx ?sync-tx)
    // [?sync.password :sync.password/credential ?credential]
    // [?sync.password :sync.password/timesUsed ?remoteTimesUsed]
    // [?sync.password :sync.password/tx ?sync-tx]
    // ]


    // let mut qb = QueryBuilder::new(store, q);
    // qb.bind_value("?id", id);

    // TODO: use `when_some` instead?  I'm not clear which is more clear.
    let (q, sync_tx) = if let Some((_, _, KnownEntid(sync_tx))) = sync_mirror {
        let q = r#"[:find
                (count ?l) .
                :in
                ?id ?sync_tx
                :where
                [?c :credential/id ?id]
                [?l :login/credential ?c]
                [?l :login/at _ ?login-tx]
                [(tx-after ?login-tx ?sync_tx)]
               ]"#;
        (q, sync_tx)

        // qb.add_where("(tx-after ?login-tx ?sync_tx)");
        // qb.bind_value("?sync_tx", sync_tx);
    } else {
        let q = r#"[:find
                (count ?l) .
                :in
                ?id ?sync_tx
                :where
                [?c :credential/id ?id]
                [?l :login/credential ?c]
                [?l :login/at _]
               ]"#;
        (q, 0)
        // Work around a bug?
        // qb.bind_value("?sync_tx", 0);
    };

    let values =
        QueryInputs::with_value_sequence(vec![(var!(?id), TypedValue::typed_string(&id)),
                                              (var!(?sync_tx), TypedValue::Ref(sync_tx))]);

    let local_times_used: mentat::errors::Result<_> = match queryable.q_once(q, values)?
        .into_scalar()? {
        Some(Binding::Scalar(TypedValue::Long(times_used))) => Ok(times_used), // TODO: work out overflow.
        None => Ok(0),
        _ => bail!("bad query result types in find_recent_sync_passwords"),
    };
    let local_times_used = local_times_used?;

    info!("times_used: local_times_used: {:?}", local_times_used);

    let times_used = if let Some((_, remote_times_used, _)) = sync_mirror {
        remote_times_used + local_times_used
    } else {
        local_times_used
    };

    Ok(times_used)
}

use chrono::TimeZone;

// TODO: u64.
pub fn time_last_used<Q>(queryable: &Q, id: CredentialId) -> mentat::errors::Result<DateTime<Utc>>
    where Q: Queryable
{
    // TODO: this will be much easier to express with the pull API, tracked by
    // https://github.com/mozilla/mentat/issues/110.


    // # We only care about local usages after the last tx we uploaded.

    // timeLastUsed id => [
    // :find
    // ?remoteTimeLastUsed (max ?login-at)
    // :in
    // ?id
    // :where
    // [?credential :credential/id ?id]
    // [?login :login/credential ?credential]
    // [?login :login/at ?login-at ?login-tx]
    // (tx-after ?login-tx ?sync-tx)
    // [?sync.password :sync.password/credential ?credential]
    // [?sync.password :sync.password/timeLastUsed ?remoteTimeLastUsed]
    // [?sync.password :sync.password/tx ?sync-tx]
    // ]

    let sync_mirror = {
        // Scope borrow of store.
        let q = r#"[:find
                [?sl ?timeLastUsed ?tx]
                :in
                ?id
                :where
                [?c :credential/id ?id]
                [?sl :sync.password/credential ?c]
                [?sl :sync.password/timeLastUsed ?timeLastUsed]
                [?sl :sync.password/metadataTx ?tx]
               ]"#;

        let sync_mirror: mentat::errors::Result<_> = match queryable.q_once(q, QueryInputs::with_value_sequence(vec![(var!(?id), TypedValue::typed_string(id.clone()))]))?.into_tuple()? {
            Some(vs) => {
                match (vs.len(), vs.get(0), vs.get(1), vs.get(2)) {
                    (3, Some(&Binding::Scalar(TypedValue::Ref(sl))), Some(&Binding::Scalar(TypedValue::Instant(ref time_last_used))), Some(&Binding::Scalar(TypedValue::Ref(tx)))) => {
                        Ok(Some((KnownEntid(sl), time_last_used.clone(), KnownEntid(tx))))
                    },
                    _ => bail!("bad query result types in find_recent_sync_passwords"),
                }
            },
            None => Ok(None),
        };
        sync_mirror?
    };

    info!("time_last_used: sync_mirror: {:?}", sync_mirror);

    // TODO: use `when_some` instead?  I'm not clear which is more clear.
    let (q, sync_tx) = if let Some((_, _, KnownEntid(sync_tx))) = sync_mirror {
        let q = r#"[:find
                (max ?at) .
                :in
                ?id ?sync_tx
                :where
                [?c :credential/id ?id]
                [?l :login/credential ?c]
                [?l :login/at ?at ?login-tx]
                [(tx-after ?login-tx ?sync_tx)]
               ]"#;
        (q, sync_tx)
    } else {
        let q = r#"[:find
                (max ?at) .
                :in
                ?id ?sync_tx
                :where
                [?c :credential/id ?id]
                [?l :login/credential ?c]
                [?l :login/at ?at]
               ]"#;
        (q, 0)
    };

    let values =
        QueryInputs::with_value_sequence(vec![(var!(?id), TypedValue::typed_string(id)),
                                              (var!(?sync_tx), TypedValue::Ref(sync_tx))]);

    info!("time_last_used: values: {:?}", values);

    let local_time_last_used: mentat::errors::Result<_> = match queryable.q_once(q, values)?
        .into_scalar()? {
        Some(Binding::Scalar(TypedValue::Instant(time_last_used))) => Ok(Some(time_last_used)),
        None => Ok(None),
        _ => bail!("bad query result types in find_recent_sync_passwords XXX"),
    };

    let local_time_last_used = local_time_last_used?.unwrap_or_else(|| Utc.timestamp(0, 0));

    let time_last_used = if let Some((_, remote_time_last_used, _)) = sync_mirror {
        remote_time_last_used.max(local_time_last_used)
    } else {
        local_time_last_used
    };

    Ok(time_last_used)
}

// TODO: u64.
pub fn new_credential_ids<Q>(queryable: &Q) -> mentat::errors::Result<Vec<CredentialId>>
    where Q: Queryable
{
    // TODO: this will be much easier to express with the pull API, tracked by
    // https://github.com/mozilla/mentat/issues/110.

    // # New records don't have a corresponding Sync login.

    // newRecords => [
    // :find
    // ?id
    // :where
    // [?credential :credential/id ?id]
    // (not [?sync.password :sync.password/credential ?credential])
    // ]

    // TODO: narrow by tx?  We only care about credentials created after the last sync tx; if we
    // index on creation we can find just those credentials more efficiently.
    let q = r#"[:find
                [?id ...]
                :where
                [?c :credential/id ?id]
                (not [_ :sync.password/credential ?c])
                :order ; TODO: don't order?
                ?id
               ]"#;

    let vs = queryable.q_once(q, None)?.into_coll()?;
    let new_ids: mentat::errors::Result<Vec<_>> = vs.into_iter()
        .map(|id| match id {
            Binding::Scalar(TypedValue::String(id)) => Ok(CredentialId((*id).clone())),
            _ => bail!("bad query result types in new_credential_ids"),
        })
        .collect();
    new_ids
}

pub fn time_password_changed<Q>(queryable: &Q, uuid: SyncGuid) -> mentat::errors::Result<Option<DateTime<Utc>>>
    where Q: Queryable
{

    let remote_time_password_changed = {
        let q = r#"[:find
                ?timePasswordChanged .
                :in
                ?id
                :where
                [?sl :sync.password/uuid ?id]
                [?sl :sync.password/timePasswordChanged ?timePasswordChanged]
               ]"#;

        let remote_time_password_changed = queryable.q_once(q, QueryInputs::with_value_sequence(vec![(var!(?id), TypedValue::typed_string(&uuid))]))?.into_scalar()?;

        let remote_time_password_changed = match remote_time_password_changed {
            Some(Binding::Scalar(TypedValue::Instant(time_password_changed))) => time_password_changed,
            Some(_) => bail!("bad query result types in find_recent_sync_passwords"),
            None => return Ok(None),
        };

        remote_time_password_changed
    };

    info!("time_last_used: remote_time_password_changed: {:?}", remote_time_password_changed);

    // This is basically credential_deltas, but keyed by Sync uuid rather than credential id.
    let local_time_password_changed = {
        let q = r#"[:find
                [?materialTx ?username-tx ?username-txInstant ?password-tx ?password-txInstant]
                :in
                ?id
                :where
                [?sl :sync.password/uuid ?id]
                [?sl :sync.password/materialTx ?materialTx]
                [?sl :sync.password/credential ?credential]
                [?credential :credential/username ?username ?username-tx]
                [?username-tx :db/txInstant ?username-txInstant]
                [?credential :credential/password ?password ?password-tx]
                [?password-tx :db/txInstant ?password-txInstant]]"#;

        match queryable.q_once(q, QueryInputs::with_value_sequence(vec![(var!(?id), TypedValue::typed_string(&uuid))]))?.into_tuple()? {
            Some(vs) => {
                match (vs.len(), (vs.get(0), vs.get(1), vs.get(2), vs.get(3), vs.get(4))) {
                    (5,
                     (Some(&Binding::Scalar(TypedValue::Ref(material_tx))),
                      Some(&Binding::Scalar(TypedValue::Ref(username_tx))),
                      Some(&Binding::Scalar(TypedValue::Instant(ref username_tx_instant))),
                      Some(&Binding::Scalar(TypedValue::Ref(password_tx))),
                      Some(&Binding::Scalar(TypedValue::Instant(password_tx_instant))))) => {
                        Some((material_tx,
                                 username_tx,
                                 username_tx_instant.clone(),
                                 password_tx,
                                 password_tx_instant.clone()))
                    },
                    _ => bail!("bad query result types in find_sync_password_by_uuid_deltas"),
                }
            },
            None => {
                None
            },
        }
    };

    info!("time_last_used: local_time_password_changed: {:?}", local_time_password_changed);

    let mut is = vec![];
    is.push(remote_time_password_changed);

    match local_time_password_changed {
        Some((material_tx, utx, utxi, ptx, ptxi)) => {
            if utx > material_tx {
                is.push(utxi);
            }
            if ptx > material_tx {
                is.push(ptxi);
            }
        },
        None => (),
    }

    Ok(is.into_iter().max())
}

pub fn get_deleted_sync_password_uuids_to_upload<Q>(queryable: &Q) -> mentat::errors::Result<Vec<SyncGuid>>
    where Q: Queryable
{
    // TODO: this will be much easier to express with the pull API, tracked by
    // https://github.com/mozilla/mentat/issues/110.

    // # Deleted records either don't have a linked credential, or have a dangling reference to a
    // # credential with no id.

    // deletedRecords => [
    // :find
    // ?uuid
    // :where
    // [?sync.password :sync.password/uuid ?uuid]
    // (not (and [?sync.password :sync.password/credential ?credential]
    //           [?credential :credential/id _]))
    // ]

    // TODO: is there a way to narrow by tx?  Probably not here, since we're talking about records
    // that have been removed.  We could walk tx-data instead of searching datoms; who knows if that
    // is faster in practice.
    let q = r#"[:find
                [?uuid ...]
                :where
                [?sl :sync.password/uuid ?uuid]
                (not-join [?sl] [?sl :sync.password/credential ?credential] [?credential :credential/id _])
                :order ; TODO: don't order?
                ?uuid
               ]"#;

    let vs = queryable.q_once(q, None)?.into_coll()?;
    let deleted_uuids: mentat::errors::Result<Vec<_>> = vs.into_iter()
        .map(|id| match id {
            Binding::Scalar(TypedValue::String(id)) => Ok(SyncGuid((*id).clone())),
            _ => bail!("bad query result types in get_deleted_sync_password_uuids_to_upload"),
        })
        .collect();
    deleted_uuids
}

pub fn reset_client(in_progress: &mut InProgress) -> mentat::errors::Result<()> {
    // Need to delete Sync data, credential data, form data, and usage data.  So version of
    // `:db/retractEntity` is looking pretty good right now!
    let q = r#"[
:find
 [?e ...]
:where
 [?e :sync.password/uuid _]
]"#;

    let tx = TypedValue::Ref(mentat_db::TX0);

    let mut builder = Builder::<TypedValue>::new();

    let results = in_progress.q_once(q, None)?.results;

    match results {
        QueryResults::Coll(es) => {
            for e in es {
                match e {
                    Binding::Scalar(TypedValue::Ref(e)) => {
                        builder.add(e, SYNC_PASSWORD_MATERIAL_TX.clone(), tx.clone());
                        builder.add(e, SYNC_PASSWORD_METADATA_TX.clone(), tx.clone());
                    },
                    _ => unreachable!("bad query in find_sync_password_by_content"),
                }
            }
        },
        _ => unreachable!("bad query in find_sync_password_by_content"),
    }

    in_progress.transact_entity_builder(builder).and(Ok(()))
}

pub fn get_modified_sync_password_uuids_to_upload<Q>(queryable: &Q) -> mentat::errors::Result<Vec<SyncGuid>>
    where Q: Queryable
{
    let modified = {
        let q = r#"[:find
                ;(max ?txI) ; Useful for debugging.
                [?uuid ...]
                :order
                ?uuid ; TODO: don't order?
                :with
                ?sp
                :where
                [?sp :sync.password/uuid ?uuid]
                [?sp :sync.password/materialTx ?materialTx]

                (or-join [?sp ?a ?tx]
                 (and
                  [?sp :sync.password/credential ?c]
                  [?c ?a _ ?tx]
                  [(ground [:credential/id :credential/username :credential/password]) [?a ...]])
                 (and
                  [?f :form/syncPassword ?sp]
                  [?f ?a _ ?tx]
                  [(ground [:form/hostname :form/usernameField :form/passwordField :form/submitUrl :form/httpRealm]) [?a ...]]))

                [(tx-after ?tx ?materialTx)]
               ;[?tx :db/txInstant ?txI] ; Useful for debugging.
               ]"#;

        queryable.q_once(q, None)?
            .into_coll()?
            .into_iter()
            .map(|b| { b
                       .into_scalar()
                       .and_then(|s| s.into_string())
                       .map(|s| SyncGuid((*s).clone()))
                       .expect("sync guid") })
            .collect()
    };

    Ok(modified)
}

fn transact_sync_password_metadata(builder: &mut Builder<TypedValue>,
                                login: &ServerPassword,
                                credential_id: CredentialId)
                                -> mentat::errors::Result<()> {
    let c = Builder::tempid("c");
    builder.add(c.clone(),
                CREDENTIAL_ID.clone(),
                TypedValue::typed_string(credential_id));

    let sl = Builder::tempid("sl");
    builder.add(sl.clone(),
                SYNC_PASSWORD_UUID.clone(),
                TypedValue::typed_string(&login.uuid));
    builder.add(sl.clone(),
                SYNC_PASSWORD_CREDENTIAL.clone(),
                TypedValue::typed_string("c")); // TODO
    builder.add(sl.clone(),
                SYNC_PASSWORD_SERVER_MODIFIED.clone(),
                TypedValue::Instant(login.modified.clone()));
    builder.add(sl.clone(),
                SYNC_PASSWORD_TIMES_USED.clone(),
                TypedValue::Long(login.times_used as i64)); // XXX.
    builder.add(sl.clone(),
                SYNC_PASSWORD_TIME_CREATED.clone(),
                TypedValue::Instant(login.time_created.clone()));
    builder.add(sl.clone(),
                SYNC_PASSWORD_TIME_LAST_USED.clone(),
                TypedValue::Instant(login.time_last_used.clone()));
    builder.add(sl.clone(),
                SYNC_PASSWORD_TIME_PASSWORD_CHANGED.clone(),
                TypedValue::Instant(login.time_password_changed.clone()));

    let f = Builder::tempid("f");
    builder.add(f.clone(),
                FORM_SYNC_PASSWORD.clone(),
                TypedValue::typed_string("sl")); // TODO
    builder.add(f.clone(),
                FORM_HOSTNAME.clone(),
                TypedValue::typed_string(&login.hostname));
    if let Some(ref username_field) = login.username_field {
        builder.add(f.clone(),
                    FORM_USERNAME_FIELD.clone(),
                    TypedValue::typed_string(&username_field));
    }
    if let Some(ref password_field) = login.password_field {
        builder.add(f.clone(),
                    FORM_PASSWORD_FIELD.clone(),
                    TypedValue::typed_string(&password_field));
    }

    match login.target {
        FormTarget::FormSubmitURL(ref form_submit_url) => {
            builder.add(f.clone(),
                        FORM_SUBMIT_URL.clone(),
                        TypedValue::typed_string(form_submit_url));
        },
        FormTarget::HttpRealm(ref http_realm) => {
            builder.add(f.clone(),
                        FORM_HTTP_REALM.clone(),
                        TypedValue::typed_string(http_realm));
        },
    }

    // builder.add(sl.clone(), in_progress.get_entid(&SYNC_PASSWORD_TIME_PASSWORD_CHANGED).expect(":sync.password"), login.last_time_used)?;

    // in_progress.transact_builder(builder).and(Ok(()))
    Ok(())
}

// pub fn update_sync_password_metadata(in_progress: &mut InProgress, login: &ServerPassword) -> mentat::errors::Result<()> {
//     Ok(())
// }

pub fn merge_into_credential(in_progress: &InProgress,
                             builder: &mut Builder<TypedValue>,
                             id: CredentialId,
                             modified: DateTime<Utc>,
                             username: Option<String>,
                             password: String)
                             -> mentat::errors::Result<()> {
    let x = credential_deltas(in_progress, id.clone())?;
    let (_u, _utx, utxi, _p, _ptx, ptxi) = match x {
        Some(x) => x,
        None => bail!("Expected credentials to exist"),
    };

    debug!("modified {}, utxi {}, ptxi {}", modified, utxi, ptxi);

    // let c = builder.named_tempid("c".into());
    let c = Builder::tempid("c");
    builder.add(c.clone(),
                CREDENTIAL_ID.clone(),
                // in_progress.get_entid(*CREDENTIAL_ID).expect(":credential"),
                TypedValue::typed_string(id));

    // if modified > utxi {
    //     builder.add(c.clone(),
    //                 CREDENTIAL_USERNAME.clone(),
    //                 // in_progress.get_entid(&CREDENTIAL_USERNAME).expect(":credential"),
    //                 TypedValue::String(username.unwrap().into()));
    // }
    // if modified > ptxi {
    //     builder.add(c.clone(),
    //                 CREDENTIAL_PASSWORD.clone(),
    //                 // in_progress.get_entid(&CREDENTIAL_PASSWORD).expect(":credential"),
    //                 TypedValue::String(password.into()));
    // }

    // We either accept all the remote material changes or we keep some local material changes.
    //
    // If we accept all the remote changes there are no local material changes to upload, and we
    // advance materialTx, which means this login won't be considered materially changed when we
    // check for logins to upload.
    //
    // If we keep at least one local material change, then we need to upload the merged login.  We
    // don't advance materialTx at all, which means this login will be considered materially changed
    // when we check for logins to upload.
    let remote_later = modified > utxi && modified > ptxi;
    if remote_later  {
        info!("setting username {}, password {}", username.clone().unwrap(), password);

        builder.add(c.clone(),
                    CREDENTIAL_USERNAME.clone(),
                    TypedValue::String(username.unwrap().into()));

        builder.add(c.clone(),
                    CREDENTIAL_PASSWORD.clone(),
                    TypedValue::String(password.into()));

        let sl = Builder::tempid("sl");
        builder.add(sl.clone(),
                    SYNC_PASSWORD_CREDENTIAL.clone(),
                    TypedValue::typed_string("c"));
        // XXX typechecking?
        builder.add(sl.clone(),
                    SYNC_PASSWORD_MATERIAL_TX.clone(),
                    Builder::tx_function("transaction-tx"));
    }

    // TODO: what do we do with timeCreated and timePasswordChanged?

    Ok(())
}

pub fn apply_changed_login(in_progress: &mut InProgress,
                           login: ServerPassword)
                           -> mentat::errors::Result<()> {
    // let mut in_progress = store.begin_transaction()?;

    let id = match find_credential_id_by_sync_password_uuid(in_progress, login.uuid.clone())? {
        Some(id) => Some(Either::Left(id)),
        None => {
            find_credential_id_by_content(in_progress,
                                          login.username.clone().unwrap(),
                                          login.password.clone())?
                .map(Either::Right)
        } // TODO: handle optional usernames.
    };

    let mut builder = Builder::<TypedValue>::new();

    match id {
        None => {
            info!("apply_changed_login: no existing credential for sync uuid {:?}", login.uuid);

            // Nothing found locally.  Add the credential and the sync login directly to the store, and
            // commit the sync tx at the same time.
            let id = CredentialId(login.uuid.0.clone()); // CredentialId::random();

            add_credential(&mut builder,
                           id.clone(),
                           login.username.clone(),
                           login.password.clone(),
                           login.time_created.clone())?;
            transact_sync_password_metadata(&mut builder, &login, id.clone())?;

            // Set metadataTx and materialTx to :db/tx.
            let c = Builder::tempid("c"); // This is fun!  We could have collision of tempids across uses.
            builder.add(c.clone(),
                        CREDENTIAL_ID.clone(),
                        TypedValue::typed_string(id));
            let sl = Builder::tempid("sl");
            builder.add(sl.clone(),
                        SYNC_PASSWORD_CREDENTIAL.clone(),
                        TypedValue::typed_string("c"));
            builder.add(sl.clone(),
                        SYNC_PASSWORD_MATERIAL_TX.clone(),
                        Builder::tx_function("transaction-tx"));
            builder.add(sl.clone(),
                        SYNC_PASSWORD_METADATA_TX.clone(),
                        Builder::tx_function("transaction-tx"));
        }

        Some(Either::Left(id)) => {
            info!("apply_changed_login: existing credential {:?} associated with sync password for sync uuid {:?}", id, login.uuid);

            // We have an existing Sync login.  We need to merge the new changes into the credential
            // based on timestamps; we can't do better.
            transact_sync_password_metadata(&mut builder, &login, id.clone())?;
            // Sets at most materialTx.
            merge_into_credential(&in_progress,
                                  &mut builder,
                                  id.clone(),
                                  login.modified,
                                  login.username.clone(),
                                  login.password.clone())?;
        }

        Some(Either::Right(id)) => {
            info!("apply_changed_login: existing credential {:?} content matched for sync uuid {:?}", id, login.uuid);

            // We content matched.  We need to merge the new changes into the credential based on
            // timestamps; we can't do better.
            transact_sync_password_metadata(&mut builder, &login, id.clone())?;
            // Sets at most materialTx.
            merge_into_credential(&in_progress,
                                  &mut builder,
                                  id.clone(),
                                  login.modified,
                                  login.username.clone(),
                                  login.password.clone())?;
        }
    }

    in_progress.transact_entity_builder(builder).and(Ok(()))
}

// timesUsed id => [
// :find
// ?remoteTimesUsed (count ?login)
// :in
// ?id
// :where
// [?credential :credential/id ?id]
// [?login :login/credential ?credential]
// [?login :login/at _ ?login-tx]
// (tx-after ?login-tx ?sync-tx)
// [?sync.password :sync.password/credential ?credential]
// [?sync.password :sync.password/timesUsed ?remoteTimesUsed]
// [?sync.password :sync.password/tx ?sync-tx]
// ]

// :credential/id (Lockbox specific, could be local entid if we're not concerned about leaking)
// :credential/title (Lockbox specific)
// :credential/username (optional)
// :credential/password
//
// :login/at :db.type/instant
// :login/credential :db.type/ref
//
// :sync.password/uuid
// :sync.password/credential
// :sync.password/tx
// :sync.password/lastModified
// :sync.password/timesUsed
// :sync.password/timeCreated
// :sync.password/timeModified
// :sync.password/timeLastUsed
//
// # We only count local usages after the last tx we uploaded.
//
// timesUsed id => [
// :find
// ?remoteTimesUsed (count ?login)
// :in
// ?id
// :where
// [?credential :credential/id ?id]
// [?login :login/credential ?credential]
// [?login :login/at _ ?login-tx]
// (tx-after ?login-tx ?sync-tx)
// [?sync.password :sync.password/credential ?credential]
// [?sync.password :sync.password/timesUsed ?remoteTimesUsed]
// [?sync.password :sync.password/tx ?sync-tx]
// ]
//
// # We only care about the local time of creation if the record was around before the remote record
// # was downloaded.
//
// timeCreated id => [
// :find
// ?remoteTimeCreated ?created ?credential-tx
// :in
// ?id
// :where
// [?credential :credential/id ?id ?credential-tx]
// [?credential-tx :db/txInstant ?created]
// (tx-before ?credential-tx ?sync-tx)
// [?sync.password :sync.password/credential ?credential]
// [?sync.password :sync.password/timeCreated ?remoteTimeCreated]
// [?sync.password :sync.password/tx ?sync-tx]
// ]
//
// # We only care about creation and modification after the last tx we uploaded.
//
// timeModified id => [
// :find
// ?remoteTimeModified ?created ?title-modified ?username-modified ?password-modified
// :in
// ?id
// :where
// [?credential :credential/id ?id ?credential-tx]
// [?credential-tx :db/txInstant ?created]
// (tx-after ?credential-tx ?sync-tx)
// [?credential :credential/title _ ?credential-title-tx]
// [?credential-title-tx :db/txInstant ?title-modified]
// (tx-after ?credential-title-tx ?sync-tx)
// [?credential :credential/username _ ?credential-username-tx]
// [?credential-username-tx :db/txInstant ?username-modified]
// (tx-after ?credential-username-tx ?sync-tx)
// [?credential :credential/password _ ?credential-password-tx]
// [?credential-password-tx :db/txInstant ?password-modified]
// (tx-after ?credential-password-tx ?sync-tx)
// [?sync.password :sync.password/credential ?credential]
// [?sync.password :sync.password/timeModified ?remoteTimeModified]
// [?sync.password :sync.password/tx ?sync-tx]
// ]
//
// # We only care about local usages after the last tx we uploaded.
//
// timeLastUsed id => [
// :find
// ?remoteTimeLastUsed (max ?login-at)
// :in
// ?id
// :where
// [?credential :credential/id ?id]
// [?login :login/credential ?credential]
// [?login :login/at ?login-at ?login-tx]
// (tx-after ?login-tx ?sync-tx)
// [?sync.password :sync.password/credential ?credential]
// [?sync.password :sync.password/timeLastUsed ?remoteTimeLastUsed]
// [?sync.password :sync.password/tx ?sync-tx]
// ]
//
// # New records don't have a corresponding Sync login.
//
// newRecords => [
// :find
// ?id
// :where
// [?credential :credential/id ?id]
// (not [?sync.password :sync.password/credential ?credential])
// ]
//
// # Deleted records either don't have a linked credential, or have a dangling reference to a
// # credential with no id.
//
// deletedRecords => [
// :find
// ?uuid
// :where
// [?sync.password :sync.password/uuid ?uuid]
// (not (and [?sync.password :sync.password/credential ?credential]
// [?credential :credential/id _]))
// ]
//
//

pub fn find_frequent_sync_passwords(queryable: &mut Store,
                                 limit: Option<usize>)
                                 -> mentat::errors::Result<Vec<(i64, String)>> {
    // TODO: this will be much easier to express with the pull API, tracked by
    // https://github.com/mozilla/mentat/issues/110.
    let q = r#"[:find (count ?at) ?uuid
                :where
                [?e :sync.password/credential ?c]
                [?e :sync.password/uuid ?uuid]
                [?login :login/credential ?c]
                [?login :login/at ?at]
                ; :order
                ; (count ?at)
]"#;

    let mut qb = QueryBuilder::new(queryable, q);
    if let Some(limit) = limit {
        qb.add_limit(&format!("{}", limit));
    }

    let logins = qb.execute_rel()?
        .into_iter()
        .map(|vs| {
            match (vs.len(), vs.get(0), vs.get(1)) {
                (2, Some(&Binding::Scalar(TypedValue::Long(x))), Some(&Binding::Scalar(TypedValue::String(ref y)))) => {
                    Ok((x, (**y).clone()))
                }
                _ => bail!("bad query result types in find_frequent_sync_passwords"),
            }
        })
        .collect::<mentat::errors::Result<Vec<_>>>()?;

    Ok(logins)
}

fn delete_by_sync_uuid(in_progress: &mut InProgress,
                       uuid: SyncGuid)
                       -> mentat::errors::Result<()> {
    delete_by_sync_uuids(in_progress, ::std::iter::once(uuid))
}

fn delete_by_sync_uuids<I>(in_progress: &mut InProgress,
                           uuids: I)
                           -> mentat::errors::Result<()>
where I: IntoIterator<Item=SyncGuid> {

    // Need to delete Sync data, credential data, form data, and usage data.  So version of
    // `:db/retractEntity` is looking pretty good right now!
    let q = r#"[
:find
 ?e ?a ?v
:in
 ?uuid
:where
 (or-join [?e ?a ?v ?uuid]
  (and
   [?e :sync.password/uuid ?uuid]
   [?e ?a ?v])
  (and
   [?p :sync.password/uuid ?uuid]
   [?p :sync.password/credential ?e]
   [?e ?a ?v])
  (and
   [?p :sync.password/uuid ?uuid]
   [?p :sync.password/credential ?c]
   [?e :login/credential ?c]
   [?e ?a ?v])
  (and
   [?p :sync.password/uuid ?uuid]
   [?e :form/syncPassword ?p]
   [?e ?a ?v]))
]"#;

    let mut builder = Builder::<TypedValue>::new();

    for uuid in uuids {
        let inputs = QueryInputs::with_value_sequence(vec![(var!(?uuid), TypedValue::typed_string(uuid))]);
        let results = in_progress.q_once(q, inputs)?.results;

        match results {
            QueryResults::Rel(vals) => {
                vals.into_iter().for_each(|vs| {
                    match (vs.len(), vs.get(0), vs.get(1), vs.get(2)) {
                        (3, Some(&Binding::Scalar(TypedValue::Ref(e))), Some(&Binding::Scalar(TypedValue::Ref(a))), Some(&Binding::Scalar(ref v))) => {
                            builder.retract(e, a, v.clone()); // TODO: don't clone.
                        }
                        _ => unreachable!("bad query result types in delete_by_sync_uuid"),
                    }
                });
            },
            _ => unreachable!("bad query in find_sync_password_by_content"),
        }
    }

    in_progress.transact_entity_builder(builder).and(Ok(()))
}

fn mark_synced_by_sync_uuids<I>(
    in_progress: &mut InProgress,
    uuids: I)
    -> mentat::errors::Result<()>
where I: IntoIterator<Item=SyncGuid> {

    // Need to delete Sync data, credential data, form data, and usage data.  So version of
    // `:db/retractEntity` is looking pretty good right now!
    let q = r#"[
:find
 ?e .
:in
 ?uuid
:where
 [?e :sync.password/uuid ?uuid]
]"#;

    let tx = TypedValue::Ref(in_progress.last_tx_id());

    let mut builder = Builder::<TypedValue>::new();

    for uuid in uuids {
        let inputs = QueryInputs::with_value_sequence(vec![(var!(?uuid), TypedValue::typed_string(uuid))]);
        let results = in_progress.q_once(q, inputs)?.results;

        match results {
            QueryResults::Scalar(Some(Binding::Scalar(TypedValue::Ref(e)))) => {
                builder.add(e, SYNC_PASSWORD_MATERIAL_TX.clone(), tx.clone()); // TODO: don't clone.
                builder.add(e, SYNC_PASSWORD_METADATA_TX.clone(), tx.clone()); // TODO: don't clone.
            },
            _ => unreachable!("bad query in mark_synced_by_sync_uuids"),
        }
    }

    in_progress.transact_entity_builder(builder).and(Ok(()))
}

#[cfg(test)]
mod tests {
    use super::*;

    extern crate env_logger;

    use chrono;

    use mentat::vocabulary::{
        VersionedStore,
        VocabularyOutcome,
    };

    lazy_static! {
        static ref LOGIN1: ServerPassword = {
            ServerPassword {
                modified: DateTime::<Utc>::from_micros(1523908142550),
                uuid: SyncGuid("{c5144948-fba1-594b-8148-ff70c85ee19a}".into()),
                hostname: "https://oauth-sync.dev.lcip.org".into(),
                target: FormTarget::FormSubmitURL("https://oauth-sync.dev.lcip.org/post".into()),
                username: Some("username@mockmyid.com".into()),
                password: "password".into(),
                username_field: Some("email".into()),
                password_field: None,
                time_created: DateTime::<Utc>::from_micros(1523908112453),
                time_password_changed: DateTime::<Utc>::from_micros(1523908112453),
                time_last_used: DateTime::<Utc>::from_micros(1000),
                times_used: 12,
            }
        };

        static ref LOGIN2: ServerPassword = {
            ServerPassword {
                modified: DateTime::<Utc>::from_micros(1523909142550),
                uuid: SyncGuid("{d2c78792-1528-4026-afcb-6bd927f36a45}".into()),
                hostname: "https://totally-different.com".into(),
                target: FormTarget::FormSubmitURL("https://auth.totally-different.com".into()),
                username: Some("username@mockmyid.com".into()),
                password: "totally-different-password".into(),
                username_field: Some("auth_username".into()),
                password_field: Some("auth_password".into()),
                time_created: DateTime::<Utc>::from_micros(1523909141550),
                time_password_changed: DateTime::<Utc>::from_micros(1523909142550),
                time_last_used: DateTime::<Utc>::from_micros(1523909142550),
                times_used: 1,
            }
        };
    }

    fn testing_store() -> Store {
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

    #[test]
    fn test_roundtrip() {
        // Verify that applying a password and then immediately reading it back yields the original
        // data.

        let mut store = testing_store();

        // Scoped borrow of `store`.
        {
            let mut in_progress = store.begin_transaction().expect("begun successfully");

            let login = ServerPassword {
                modified: DateTime::<Utc>::from_micros(1523908142550),
                uuid: SyncGuid("{c5144948-fba1-594b-8148-ff70c85ee19a}".into()),
                hostname: "https://oauth-sync.dev.lcip.org".into(),
                target: FormTarget::FormSubmitURL("https://oauth-sync.dev.lcip.org/post".into()),
                username: Some("username@mockmyid.com".into()),
                password: "password".into(),
                username_field: Some("email".into()),
                password_field: None,
                time_created: DateTime::<Utc>::from_micros(1523908112453),
                time_password_changed: DateTime::<Utc>::from_micros(1523908112453),
                time_last_used: DateTime::<Utc>::from_micros(1000),
                times_used: 12,
            };
            // info!("{}", in_progress.dump_sql_query("SELECT e, a, v, tx FROM datoms ORDER BY e, a, v, tx", &[]).expect("debug"));

            apply_changed_login(&mut in_progress, login.clone()).expect("to apply");

            // info!("{}", in_progress.dump_sql_query("SELECT e, a, v, tx FROM datoms ORDER BY e, a, v, tx", &[]).expect("debug"));

            let sp = get_sync_password(&in_progress,
                                       SyncGuid("{c5144948-fba1-594b-8148-ff70c85ee19a}".into())).expect("to get_sync_password");
            assert_eq!(sp, Some(login.clone()));
        }
    }

    #[test]
    fn test_apply_twice() {
        // Verify that applying a password twice doesn't do anything the second time through.

        let mut store = testing_store();

        // Scoped borrow of `store`.
        {
            let mut in_progress = store.begin_transaction().expect("begun successfully");

            let login = ServerPassword {
                modified: DateTime::<Utc>::from_micros(1523908142550),
                uuid: SyncGuid("{c5144948-fba1-594b-8148-ff70c85ee19a}".into()),
                hostname: "https://oauth-sync.dev.lcip.org".into(),
                target: FormTarget::FormSubmitURL("https://oauth-sync.dev.lcip.org/post".into()),
                username: Some("username@mockmyid.com".into()),
                password: "password".into(),
                username_field: Some("email".into()),
                password_field: None,
                time_created: DateTime::<Utc>::from_micros(1523908112453),
                time_password_changed: DateTime::<Utc>::from_micros(1523908112453),
                time_last_used: DateTime::<Utc>::from_micros(1000),
                times_used: 12,
            };
            apply_changed_login(&mut in_progress, login.clone()).expect("to apply");

            apply_changed_login(&mut in_progress, login.clone()).expect("to apply");

            let t = in_progress.dump_last_transaction().expect("transaction");
            assert_eq!(t.into_vector().expect("vector").len(), 1); // Just the :db/txInstant.
        }
    }

    #[test]
    fn test_remote_evolved() {
        // Verify that when there are no local changes, applying a remote record that has evolved
        // takes the remote changes.

        let mut store = testing_store();

        // Scoped borrow of `store`.
        {
            let mut in_progress = store.begin_transaction().expect("begun successfully");

            let mut login = ServerPassword {
                modified: DateTime::<Utc>::from_micros(1523908142550),
                uuid: SyncGuid("{c5144948-fba1-594b-8148-ff70c85ee19a}".into()),
                hostname: "https://oauth-sync.dev.lcip.org".into(),
                target: FormTarget::FormSubmitURL("https://oauth-sync.dev.lcip.org/post".into()),
                username: Some("username@mockmyid.com".into()),
                password: "password".into(),
                username_field: Some("email".into()),
                password_field: None,
                time_created: DateTime::<Utc>::from_micros(1523908112453),
                time_password_changed: DateTime::<Utc>::from_micros(1523908112453),
                time_last_used: DateTime::<Utc>::from_micros(1000),
                times_used: 12,
            };
            apply_changed_login(&mut in_progress, login.clone()).expect("to apply");

            login.modified = mentat_core::now();
            login.password = "password2".into();
            login.password_field = Some("password".into());
            login.times_used = 13;

            apply_changed_login(&mut in_progress, login.clone()).expect("to apply");

            let sp = get_sync_password(&in_progress,
                                       login.uuid.clone()).expect("to get_sync_password");
            assert_eq!(sp, Some(login.clone()));

            let t = in_progress.dump_last_transaction().expect("transaction");
            // assert_eq!(t.into_vector().expect("vector").len()); // Just the :db/txInstant.
        }
    }

    #[test]
    fn test_get_modified_sync_password_uuids_to_upload() {
        // env_logger::init();

        let mut store = testing_store();

        // Scoped borrow of `store`.
        {
            let mut in_progress = store.begin_transaction().expect("begun successfully");

            apply_changed_login(&mut in_progress, LOGIN1.clone()).expect("to apply");
            apply_changed_login(&mut in_progress, LOGIN2.clone()).expect("to apply");

            // But if there are no local changes, we shouldn't propose any records to re-upload.
            let sp = get_modified_sync_password_uuids_to_upload(&in_progress).expect("to get_sync_password");
            assert_eq!(sp, vec![]);

            // Now, let's modify locally an existing credential connected to a Sync 1.5 record.
            let mut builder = Builder::<TypedValue>::new();
            add_credential(&mut builder,
                           CredentialId(LOGIN1.uuid.0.clone()),
                           Some("us3rnam3@mockymid.com".into()),
                           "pa33w3rd".into(),
                           None)
                .expect("to update credential");
            in_progress.transact_entity_builder(builder).expect("to transact");

            // Just for our peace of mind.
            let t = in_progress.dump_last_transaction().expect("transaction");
            assert_eq!(t.into_vector().expect("vector").len(), 5); // One add and one retract per field, and the :db/txInstant.

            // Our local change results in a record needing to be uploaded remotely.
            let sp = get_modified_sync_password_uuids_to_upload(&in_progress).expect("to get_sync_password");
            assert_eq!(sp, vec![LOGIN1.uuid.clone()]);

            // Suppose we disconnect, so that the last sync tx is TX0, and then reconnect.  We'll
            // have Sync 1.5 data in the store, and we'll need to upload it all.
            reset_client(&mut in_progress).expect("to reset_client");

            let sp = get_modified_sync_password_uuids_to_upload(&in_progress).expect("to get_sync_password");
            assert_eq!(sp, vec![LOGIN1.uuid.clone(), LOGIN2.uuid.clone()]);
        }
    }

    #[test]
    fn test_get_deleted_sync_uuids_to_upload() {
        //env_logger::init();

        let mut store = testing_store();

        // Scoped borrow of `store`.
        {
            let mut in_progress = store.begin_transaction().expect("begun successfully");

            apply_changed_login(&mut in_progress, LOGIN1.clone()).expect("to apply");
            apply_changed_login(&mut in_progress, LOGIN2.clone()).expect("to apply");

            // Deletion is a global operation in our Sync 1.5 data model, meaning that we don't take
            // into account the current Sync tx when determining if something has been deleted:
            // absence is all that matters.
            let sp = get_deleted_sync_password_uuids_to_upload(&in_progress).expect("to get_sync_password");
            assert_eq!(sp, vec![]);

            // Now, let's delete an existing credential connected to a Sync 1.5 record.  Right now
            // is when we want to be able to :db/retractEntity a lookup-ref; see
            // https://github.com/mozilla/mentat/issues/378.
            //
            // Here we're using that the credential uuid and the Sync 1.5 uuid are the same; that's
            // not a stable assumption.
            let mut builder = Builder::<TypedValue>::new();
            builder.retract(Builder::lookup_ref(CREDENTIAL_ID.clone(), TypedValue::String(LOGIN1.uuid.0.clone().into())),
                            CREDENTIAL_ID.clone(),
                            TypedValue::String(LOGIN1.uuid.0.clone().into()));
            in_progress.transact_entity_builder(builder).expect("to transact");

            // Just for our peace of mind.
            let t = in_progress.dump_last_transaction().expect("transaction");
            assert_eq!(t.into_vector().expect("vector").len(), 2); // One retract, and the :db/txInstant.

            // The record's gone, Jim!
            let sp = get_deleted_sync_password_uuids_to_upload(&in_progress).expect("to get_sync_password");
            assert_eq!(sp, vec![LOGIN1.uuid.clone()]);

            // We can also sever the link between the Sync 1.5 record and the underlying credential.
            let mut builder = Builder::<TypedValue>::new();
            builder.retract(Builder::lookup_ref(SYNC_PASSWORD_UUID.clone(), TypedValue::String(LOGIN2.uuid.0.clone().into())),
                            SYNC_PASSWORD_CREDENTIAL.clone(),
                            Builder::lookup_ref(CREDENTIAL_ID.clone(), TypedValue::String(LOGIN2.uuid.0.clone().into())));
            in_progress.transact_entity_builder(builder).expect("to transact");

            let t = in_progress.dump_last_transaction().expect("transaction");
            assert_eq!(t.into_vector().expect("vector").len(), 2); // One retract, and the :db/txInstant.

            // Now both records are gone.
            let sp = get_deleted_sync_password_uuids_to_upload(&in_progress).expect("to get_sync_password");
            assert_eq!(sp, vec![LOGIN1.uuid.clone(), LOGIN2.uuid.clone()]);
        }
    }

    #[test]
    fn test_mark_synced_by_sync_uuids() {
        env_logger::init();

        let mut store = testing_store();

        // Scoped borrow of `store`.
        {
            let mut in_progress = store.begin_transaction().expect("begun successfully");

            apply_changed_login(&mut in_progress, LOGIN1.clone()).expect("to apply");
            apply_changed_login(&mut in_progress, LOGIN2.clone()).expect("to apply");

            in_progress.commit().expect("commit succeeded");
        }

        // Scoped borrow of `store`.
        {
            let mut in_progress = store.begin_transaction().expect("begun successfully");

            let sp = get_modified_sync_password_uuids_to_upload(&in_progress).expect("to get_sync_password");
            assert_eq!(sp, vec![]);

            // Suppose we disconnect, so that the last sync tx is TX0, and then reconnect.  We'll
            // have Sync 1.5 data in the store, and we'll need to upload it all.
            reset_client(&mut in_progress).expect("to reset_client");

            let sp = get_modified_sync_password_uuids_to_upload(&in_progress).expect("to get_sync_password");
            assert_eq!(sp, vec![LOGIN1.uuid.clone(), LOGIN2.uuid.clone()]);

            // Mark one password synced, and the other one will need to be uploaded.
            let iters = ::std::iter::once(LOGIN1.uuid.clone());
            mark_synced_by_sync_uuids(&mut in_progress, iters.clone()).expect("to mark synced by sync uuids");

            let sp = get_modified_sync_password_uuids_to_upload(&in_progress).expect("to get_sync_password");
            assert_eq!(sp, vec![LOGIN2.uuid.clone()]);

            // Mark everything unsynced, and then everything synced, and we won't upload anything.
            reset_client(&mut in_progress).expect("to reset_client");

            let iters = ::std::iter::once(LOGIN1.uuid.clone()).chain(::std::iter::once(LOGIN2.uuid.clone()));
            mark_synced_by_sync_uuids(&mut in_progress, iters.clone()).expect("to mark synced by sync uuids");

            let sp = get_modified_sync_password_uuids_to_upload(&in_progress).expect("to get_sync_password");
            assert_eq!(sp, vec![]);
        }
    }

    #[test]
    fn test_delete_by_sync_uuid() {
        let mut store = testing_store();

        // Scoped borrow of `store`.
        {
            let mut in_progress = store.begin_transaction().expect("begun successfully");

            apply_changed_login(&mut in_progress, LOGIN1.clone()).expect("to apply");
            apply_changed_login(&mut in_progress, LOGIN2.clone()).expect("to apply");

            delete_by_sync_uuid(&mut in_progress, LOGIN1.uuid.clone()).expect("to delete by sync uuid");

            // The record's gone.
            let sp = get_sync_password(&in_progress,
                                       LOGIN1.uuid.clone()).expect("to get_sync_password");
            assert_eq!(sp, None);

            // And moreover, we won't try to upload a tombstone.
            let sp = get_deleted_sync_password_uuids_to_upload(&in_progress).expect("to get_sync_password");
            assert_eq!(sp, vec![]);

            // If we try to delete again, that's okay.
            delete_by_sync_uuid(&mut in_progress, LOGIN1.uuid.clone()).expect("to delete by sync uuid");


            let sp = get_sync_password(&in_progress,
                                       LOGIN1.uuid.clone()).expect("to get_sync_password");
            assert_eq!(sp, None);

            // The other password wasn't deleted.
            let sp = get_sync_password(&in_progress,
                                       LOGIN2.uuid.clone()).expect("to get_sync_password");
            assert_eq!(sp, Some(LOGIN2.clone()));
        }
    }

    #[test]
    fn test_delete_by_sync_uuids() {
        let mut store = testing_store();

        // Scoped borrow of `store`.
        {
            let mut in_progress = store.begin_transaction().expect("begun successfully");

            apply_changed_login(&mut in_progress, LOGIN1.clone()).expect("to apply");
            apply_changed_login(&mut in_progress, LOGIN2.clone()).expect("to apply");

            let iters = ::std::iter::once(LOGIN1.uuid.clone()).chain(::std::iter::once(LOGIN2.uuid.clone()));
            delete_by_sync_uuids(&mut in_progress, iters.clone()).expect("to delete by sync uuids");

            // The record's gone.
            let sp = get_sync_password(&in_progress,
                                       LOGIN1.uuid.clone()).expect("to get_sync_password");
            assert_eq!(sp, None);

            let sp = get_sync_password(&in_progress,
                                       LOGIN2.uuid.clone()).expect("to get_sync_password");
            assert_eq!(sp, None);

            // And moreover, we won't try to upload a tombstone.
            let sp = get_deleted_sync_password_uuids_to_upload(&in_progress).expect("to get_sync_password");
            assert_eq!(sp, vec![]);

            // If we try to delete again, that's okay.
            delete_by_sync_uuids(&mut in_progress, iters.clone()).expect("to delete by sync uuid");
        }
    }

    #[test]
    fn test_lockbox_logins() {
        let cid = CredentialId("id1".to_string());

        let mut store = Store::open("").expect("opened");

        // Scoped borrow of `store`.
        {
            let mut in_progress = store.begin_transaction().expect("begun successfully");

            assert!(in_progress.verify_core_schema().is_ok());

            assert_eq!(VocabularyOutcome::Installed, in_progress.ensure_vocabulary(&CREDENTIAL_VOCAB).expect("ensure succeeded"));
            assert_eq!(VocabularyOutcome::Installed, in_progress.ensure_vocabulary(&LOGIN_VOCAB).expect("ensure succeeded"));
            assert_eq!(VocabularyOutcome::Installed, in_progress.ensure_vocabulary(&FORM_VOCAB).expect("ensure succeeded"));
            assert_eq!(VocabularyOutcome::Installed, in_progress.ensure_vocabulary(&SYNC_PASSWORD_VOCAB).expect("ensure succeeded"));

            // If we commit, it'll stick around.
            in_progress.commit().expect("commit succeeded");
        }

        // Scoped borrow of `store`.
        {
            let mut in_progress = store.begin_transaction().expect("begun successfully");
            let mut builder = Builder::<TypedValue>::new();

            add_credential(&mut builder,
                           cid.clone(),
                           Some("user1".to_string()),
                           "pass1".to_string(),
                           None)
                .expect("to add credential 1");
            in_progress.transact_entity_builder(builder).expect("to transact");
            in_progress.commit().expect("to commit");
        };

        // Scoped borrow of `store`.
        {
            let mut in_progress = store.begin_transaction().expect("begun successfully");
            let mut builder = Builder::<TypedValue>::new();

            touch_by_id(&mut builder, cid.clone(), Some(Utc.timestamp(1, 0))).expect("to touch id1 1");

            in_progress.transact_entity_builder(builder).expect("to transact");
            in_progress.commit().expect("to commit");
        }

        // Scoped borrow of `store`.
        let tx_id = {
            let mut in_progress = store.begin_transaction().expect("begun successfully");

            let tx_id = in_progress.transact(r#"[]"#).expect("to transact empty").tx_id;

            in_progress.commit().expect("to commit");
            tx_id
        };

        // Scoped borrow of `store`.
        {
            let mut in_progress = store.begin_transaction().expect("begun successfully");
            let mut builder = Builder::<TypedValue>::new();

            touch_by_id(&mut builder, cid.clone(), Some(Utc.timestamp(3, 0))).expect("to touch id1 2");

            in_progress.transact_entity_builder(builder).expect("to transact");
            in_progress.commit().expect("to commit");
        }

        // Scoped borrow of `store`.
        {
            let in_progress = store.begin_read().expect("begun successfully");

            assert_eq!(2, times_used(&in_progress, cid.clone()).expect("to fetch local_times_used"));
            assert_eq!(Utc.timestamp(3, 0), time_last_used(&in_progress, cid.clone()).expect("to fetch local_times_used"));
            assert_eq!(vec![cid.clone()], new_credential_ids(&in_progress).expect("to fetch new_credentials_ids"));
            assert_eq!(Vec::<SyncGuid>::new(), get_deleted_sync_password_uuids_to_upload(&in_progress).expect("to fetch get_deleted_sync_password_uuids_to_upload"));
        }

        // Scoped borrow of `store`.
        {
            let mut in_progress = store.begin_transaction().expect("begun successfully");

            let x = format!("[
    {{:sync.password/credential (lookup-ref :credential/id \"id1\")
     :sync.password/uuid \"uuid1\"
     :sync.password/timesUsed 3
     :sync.password/timeLastUsed #inst \"{}\"
     :sync.password/metadataTx {}}}
    ]", Utc.timestamp(5, 0).to_rfc3339(), 0);
            // assert_eq!("", x);
            in_progress.transact(x).expect("to transact 1");

            // 3 remote visits, 2 local visits after the given :sync.password/tx.
            assert_eq!(5, times_used(&in_progress, cid.clone()).expect("to fetch local_times_used + remote_times_used"));
            // Remote lastUsed is after all of our local usages.
            assert_eq!(Utc.timestamp(5, 0), time_last_used(&in_progress, cid.clone()).expect("to fetch time_last_used"));
            assert_eq!(Vec::<CredentialId>::new(), new_credential_ids(&in_progress).expect("to fetch new_credentials_ids"));
            assert_eq!(Vec::<SyncGuid>::new(), get_deleted_sync_password_uuids_to_upload(&in_progress).expect("to fetch get_deleted_sync_password_uuids_to_upload"));

            // in_progress.commit()
        }

        // Scoped borrow of `store`.
        {
            let mut in_progress = store.begin_transaction().expect("begun successfully");

            in_progress.transact(format!("[
    {{:sync.password/credential (lookup-ref :credential/id \"id1\")
     :sync.password/uuid \"uuid1\"
     :sync.password/timesUsed 3
     :sync.password/timeLastUsed #inst \"{}\"
     :sync.password/metadataTx {}}}
    ]", Utc.timestamp(2, 0).to_rfc3339(), tx_id))
                .expect("to transact 2");

            // 3 remote visits, 1 local visit after the given :sync.password/tx.
            assert_eq!(4, times_used(&in_progress, cid.clone()).expect("to fetch local_times_used + remote_times_used"));
            // Remote lastUsed is between our local usages, so the latest local usage wins.
            assert_eq!(Utc.timestamp(3, 0), time_last_used(&in_progress, cid.clone()).expect("to fetch time_last_used"));
            assert_eq!(Vec::<CredentialId>::new(), new_credential_ids(&in_progress).expect("to fetch new_credentials_ids"));
            assert_eq!(Vec::<SyncGuid>::new(), get_deleted_sync_password_uuids_to_upload(&in_progress).expect("to fetch get_deleted_sync_password_uuids_to_upload"));

            // in_progress.commit()
        }

        // Scoped borrow of `store`.
        {
            let mut in_progress = store.begin_transaction().expect("begun successfully");

            in_progress.transact(format!("[
    [:db/retract (lookup-ref :credential/id \"id1\") :credential/id \"id1\"]
    {{:sync.password/uuid \"uuid2\"
    }}
    {{:sync.password/uuid \"uuid3\"
      :sync.password/credential (lookup-ref :credential/id \"id1\")
    }}
    ]"))
                .expect("to transact 3");

            assert_eq!(vec![SyncGuid("uuid2".to_string()), SyncGuid("uuid3".to_string())], get_deleted_sync_password_uuids_to_upload(&in_progress).expect("to fetch get_deleted_sync_password_uuids_to_upload"));

            // in_progress.commit()
        }

        // Scoped borrow of `store`.
        {
            let mut in_progress = store.begin_transaction().expect("begun successfully");
            let last_tx = in_progress.last_tx_id();
            // assert_eq!(1, 2);

            let login = ServerPassword {
                modified: DateTime::<Utc>::from_micros(1523908142550),
                uuid: SyncGuid("{c5144948-fba1-594b-8148-ff70c85ee19a}".into()),
                hostname: "https://oauth-sync.dev.lcip.org".into(),
                target: FormTarget::FormSubmitURL("https://oauth-sync.dev.lcip.org/post".into()),
                username: Some("username@mockmyid.com".into()),
                password: "password".into(),
                username_field: Some("email".into()),
                password_field: None,
                time_created: DateTime::<Utc>::from_micros(1523908112453),
                time_password_changed: DateTime::<Utc>::from_micros(1523908112453),
                time_last_used: DateTime::<Utc>::from_micros(1000),
                times_used: 12,
            };
            apply_changed_login(&mut in_progress, login).expect("to apply 1");

            // let vs = queryable.q_once(q, None)?.into_edn()?;
            // assert_eq!("", format!("{:?}", in_progress.dump_datoms_after(last_tx).expect("datoms")));

            // let s = in_progress.dump_sql_query("SELECT e, a, v FROM datoms ORDER BY e, a, v, tx", &[]).expect("debug");
            let s = in_progress.dump_datoms_after(last_tx-1).expect("datoms").to_pretty(120).unwrap();
            println!("last_tx {}:\n{}", last_tx, s);

            in_progress.commit().expect("to commit")
        }

        // Scoped borrow of `store`.
        {
            let mut in_progress = store.begin_transaction().expect("begun successfully");
            let last_tx = in_progress.last_tx_id();

            let mut login = ServerPassword {
                modified: DateTime::<Utc>::from_micros(1523908142550),
                uuid: SyncGuid("{c5144948-fba1-594b-8148-ff70c85ee19a}".into()),
                hostname: "https://oauth-sync.dev.lcip.org".into(),
                target: FormTarget::FormSubmitURL("https://oauth-sync.dev.lcip.org/post".into()),
                username: Some("username@mockmyid.com".into()),
                password: "password".into(),
                username_field: Some("email".into()),
                password_field: None,
                time_created: DateTime::<Utc>::from_micros(1523908112453),
                time_password_changed: DateTime::<Utc>::from_micros(1523908112453),
                time_last_used: DateTime::<Utc>::from_micros(1000),
                times_used: 12,
            };
            login.password = "password2".into();
            login.modified = mentat_core::now() + chrono::Duration::seconds(1);
            apply_changed_login(&mut in_progress, login).expect("to apply 2");

            // let vs = queryable.q_once(q, None)?.into_edn()?;
            // assert_eq!("", format!("{:?}", in_progress.dump_datoms_after(last_tx).expect("datoms")));

            // let s = in_progress.dump_sql_query("SELECT e, a, v FROM datoms ORDER BY e, a, v, tx", &[]).expect("debug");
            let s = in_progress.dump_datoms_after(last_tx).expect("datoms").to_pretty(120).unwrap();
            println!("last_tx {}:\n{}", last_tx, s);

            // assert_eq!("", s);

            // in_progress.commit().expect("to commit")
        }

        // Scoped borrow of `store`.
        {
            let mut in_progress = store.begin_transaction().expect("begun successfully");
            let last_tx = in_progress.last_tx_id();

            let mut login = ServerPassword {
                modified: DateTime::<Utc>::from_micros(1523908142550),
                uuid: SyncGuid("{abcdabcd-fba1-594b-8148-ff70c85ee19a}".into()),
                hostname: "https://oauth-sync.dev.lcip.org".into(),
                target: FormTarget::FormSubmitURL("https://oauth-sync.dev.lcip.org/post".into()),
                username: Some("x@mockmyid.com".into()),
                password: "y".into(),
                username_field: Some("email".into()),
                password_field: None,
                time_created: DateTime::<Utc>::from_micros(1523908112453),
                time_password_changed: DateTime::<Utc>::from_micros(1523908112453),
                time_last_used: DateTime::<Utc>::from_micros(1000),
                times_used: 12,
            };
            login.modified = mentat_core::now();

            let mut builder = Builder::<TypedValue>::new();
            add_credential(&mut builder, "a-credential-id".into(), login.username.clone(), login.password.clone(), None).expect("to add credential");
            in_progress.transact_entity_builder(builder).expect("to transact");

            let id = find_credential_id_by_content(&in_progress,
                                                   login.username.clone().unwrap(),
                                                   login.password.clone()).expect("to find");
            assert_eq!(Some("a-credential-id".into()), id);

            apply_changed_login(&mut in_progress, login).expect("to apply 3");

            // let vs = queryable.q_once(q, None)?.into_edn()?;
            // assert_eq!("", format!("{:?}", in_progress.dump_datoms_after(last_tx).expect("datoms")));

            // let s = in_progress.dump_sql_query("SELECT e, a, v FROM datoms ORDER BY e, a, v, tx", &[]).expect("debug");
            let s = in_progress.dump_datoms_after(last_tx).expect("datoms").to_pretty(120).unwrap();
            println!("last_tx {}:\n{}", last_tx, s);

                                        // assert_eq!("", s);

            // in_progress.commit().expect("to commit")
        }

        // Scoped borrow of `store`.
        {
            let mut in_progress = store.begin_transaction().expect("begun successfully");
            let last_tx = in_progress.last_tx_id();

            assert_eq!(get_sync_password(&in_progress, SyncGuid("{c5144948-fba1-594b-8148-ff70c85ee19a}".into())).expect("to get_sync_password"),
                       None);
        }


        // // Scoped borrow of `store`.
        // {
        //     assert_eq!(5, times_used(store.begin_read().expect("to begin read"), "id1".to_string()).expect("to fetch local_times_used + remote_times_used"));
        // }

        //     let q = r#"[:find
        //             [?sl ?timesUsed ?tx]
        //             :in
        //             ?id
        //             :where
        //             [?c :credential/id ?id]
        //             [?sl :sync.password/credential ?c]
        //             [?sl :sync.password/timesUsed ?timesUsed]
        //             [?sl :sync.password/tx ?tx]
        //            ]"#;

        //     let mut qb = QueryBuilder::new(store, q);
        //     qb.bind_value("?id", id.clone());
        // }


        // let z = store.into_debug_conn();

        // assert_matches!(z.last_transaction(),
        //                 "[[100 :db.schema/version 1 ?tx true]
        //                   [101 :db.schema/version 2 ?tx true]]");
    }


    // #[test]
    // fn test_logins() {
    //     // let mut sqlite = mentat_db::db::new_connection("").unwrap();
    //     // let mut conn = Conn::connect(&mut sqlite).unwrap();

    //     let mut store = Store::open("").expect("opened");

    //     // Scoped borrow of `store`.
    //     {
    //         let mut in_progress = store.begin_transaction().expect("begun successfully");

    //         assert!(in_progress.verify_core_schema().is_ok());
    //         // assert_eq!(VocabularyCheck::NotPresent, in_progress.check_vocabulary(&foo_v1_a).expect("check completed"));
    //         // assert_eq!(VocabularyCheck::NotPresent, in_progress.check_vocabulary(&foo_v1_b).expect("check completed"));

    //         assert_eq!(VocabularyOutcome::Installed, in_progress.ensure_vocabulary(&CREDENTIAL_VOCAB).expect("ensure succeeded"));
    //         assert_eq!(VocabularyOutcome::Installed, in_progress.ensure_vocabulary(&FORM_VOCAB).expect("ensure succeeded"));
    //         assert_eq!(VocabularyOutcome::Installed, in_progress.ensure_vocabulary(&LOGIN_VOCAB).expect("ensure succeeded"));
    //         assert_eq!(VocabularyOutcome::Installed, in_progress.ensure_vocabulary(&SYNC_PASSWORD_VOCAB).expect("ensure succeeded"));

    //         // // Now we can query to get the vocab.
    //         // let ver_attr =
    //         //     in_progress.q_once(foo_version_query, None)
    //         //                .into_tuple_result()
    //         //                .expect("query returns")
    //         //                .expect("a result");
    //         // assert_eq!(ver_attr[0], TypedValue::Long(1));
    //         // assert_eq!(ver_attr[1], TypedValue::typed_ns_keyword("foo", "bar"));

    //         // If we commit, it'll stick around.
    //         in_progress.commit().expect("commit succeeded");
    //     }


    //     // {"id":"{c5144948-fba1-594b-8148-ff70c85ee19a}","modified":1523908142.55,"payload":"{\"ciphertext\":\"fZnNYWb3K51j82N+rhm0Mv0p9egXZC35Wv/SLTdAsETU4MJLkDBVtxTTq2TYEc0TLvCbKwOCcuf/FT2svI1xyIZMJ7s2Gm3bbsM7ghBxpkeNdt3G5N7mu+t0q4StWHbee0exWv9t6W2vyiF8uBVQ3tm/ZXjjfjGZ4CHL16sgXe6RQnWBjjE4qF7RgxSEwOq796EjAJPsFzlEpFrfNfpOuoOdBmb6HWvOmC4AKLY/fkW+Pq2c6FqhJ17Mz9jM9GjiK63viVgSK7cU1vGIiK4FczrTrVnXvinkH6vzSF3wsP8liWoR7N06IWmdty/kzW1WZ5bqqACQVglf7sR/uLdQPl8DQfTsBIVaFw1VATcgOrWlx+IDvNsIxnW8//7iQXM+QPNqN8wbCcG9FJD1d4vyI4dYmxYHK9E4JcyI87rqsEwHXjfu99iiNAEYZeNR0W/fXpX/zOF5Ul+mtz1uVo0ePw==\",\"IV\":\"wAy1kDAIsTOD+HYsp8j07w==\",\"hmac\":\"029a9201cf46e05c6e58d87258db5724ccce8569cad8a4d57a90effe8ddcc8ae\"}"}
    //     // {"id":"{c5144948-fba1-594b-8148-ff70c85ee19a}","hostname":"https://oauth-sync.dev.lcip.org","formSubmitURL":"https://oauth-sync.dev.lcip.org/post","httpRealm":null,"username":"testboxlocker@mockmyid.com","password":"4two!testboxlocker","usernameField":"email","passwordField":"","timeCreated":1523908112453,"timePasswordChanged":1523908112453}
    //     let mut login = ServerPassword {
    //         modified: DateTime::<Utc>::from_micros(1523908142550),
    //         uuid: "{c5144948-fba1-594b-8148-ff70c85ee19a}".into(),
    //         hostname: "https://oauth-sync.dev.lcip.org".into(),
    //         target: FormTarget::FormSubmitURL("https://oauth-sync.dev.lcip.org/post".into()),
    //         username: Some("username@mockmyid.com".into()),
    //         password: "password".into(),
    //         username_field: Some("email".into()),
    //         password_field: Some("".into()),
    //         time_created: DateTime::<Utc>::from_micros(1523908112453),
    //         time_password_changed: DateTime::<Utc>::from_micros(1523908112453),
    //         time_last_used: DateTime::<Utc>::from_micros(0),
    //         times_used: 0,
    //     };

    //     // Scoped borrow of `store`.
    //     {
    //         let mut in_progress = store.begin_transaction().expect("begun successfully");

    //         add_sync_password(&mut in_progress, login.clone()).expect("to add :sync.password");

    //         in_progress.commit().expect("commit succeeded");
    //     }

    //     let present = find_sync_password_by_uuid(&store, "{c5144948-fba1-594b-8148-ff70c85ee19a}".into())
    //         .expect("to find_sync_password_by_uuid when present");
    //     assert_eq!(present, Some(("username@mockmyid.com".into(), "password".into())));

    //     let missing = find_sync_password_by_uuid(&store, "{missingx-fba1-594b-8148-ff70c85ee19a}".into())
    //         .expect("to find_sync_password_by_uuid when missing");
    //     assert_eq!(missing, None);

    //     login.password = "password2".into();

    //     {
    //         // let mut in_progress = store.begin_transaction().expect("begun successfully");

    //         // // r#"[:db/add [:sync.password/_credential [:sync.password/uuid ?uuid]] :credential/password ?password]"#;

    //         // [:retract (?person :person/name "Bill")
    //         //  :assert (?person :person/name "William")
    //         //  :where [?person :person/name "Bill"]]

    //         // [:transact
    //         //  [:db/add ?c :credential/password ?password]
    //         //  [:db/add ?f :form/usernameField ?username-field]
    //         //  :where
    //         //  [?login :sync.password/uuid ?uuid]
    //         //  [?login :sync.password/credential ?c]
    //         //  [?login :sync.password/form ?f]]

    //         update_sync_password(&mut store, login.clone()).expect("to update :sync.password");

    //         // let mut builder = TermBuilder::new();
    //         // builder.add(c, in_progress.get_entid(&CREDENTIAL_PASSWORD).expect(":credential"), TypedValue::String(login.password.into()))?;

    //         // // let c = builder.named_tempid("c".into());
    //         // // builder.add(c.clone(), in_progress.get_entid(&CREDENTIAL_PASSWORD).expect(":credential"), TypedValue::String(login.password.into()))?;

    //         // in_progress.transact_builder(builder).and(Ok(()))?;
    //         // // in_progress.transact_builder(builder)?;

    //         // in_progress.commit().expect("commit succeeded");
    //     }

    //     // let present = find_sync_password_by_uuid_deltas(&mut store, "{c5144948-fba1-594b-8148-ff70c85ee19a}".into()).expect("to find_sync_password_by_uuid when present").expect("XXX");
    //     // // assert_eq!(present, Some(("username@mockmyid.com".into(), 0, "password2".into(), 0)));

    //     // assert_eq!(present.0, "username@mockmyid.com".to_string());
    //     // assert_eq!(present.2, "password2".to_string());
    //     // assert_eq!(present.1, present.3 - 1);
    //     // // , 0, "password2".into(), 0)));

    //     let missing = find_sync_password_by_uuid_deltas(&mut store,
    //                                                  "{missingx-fba1-594b-8148-ff70c85ee19a}".into())
    //         .expect("to find_sync_password_by_uuid when missing");
    //     assert_eq!(missing, None);

    //     let present = find_sync_password_by_content(&mut store, &login)
    //         .expect("to find_sync_password_by_content when present");
    //     assert_eq!(present, Some("{c5144948-fba1-594b-8148-ff70c85ee19a}".into()));

    //     login.username = Some("missing@mockmyid.com".into());

    //     let missing = find_sync_password_by_content(&mut store, &login)
    //         .expect("to find_sync_password_by_content when missing");
    //     assert_eq!(missing, None);

    //     // Note reversed order; we want to test ordering of the results.
    //     let t2 = mentat_core::now();
    //     let t1 = mentat_core::now();

    //     touch_sync_password_by_uuid(&mut store, login.uuid.clone(), Some(t1))
    //         .expect("to touch_sync_password_by_uuid 1");
    //     touch_sync_password_by_uuid(&mut store, login.uuid.clone(), Some(t2))
    //         .expect("to touch_sync_password_by_uuid 2");

    //     let recent = find_recent_sync_passwords(&mut store, None).expect("to find_recent_sync_passwords");
    //     assert_eq!(recent, vec![t2, t1]);

    //     let frequent = find_frequent_sync_passwords(&mut store, None)
    //         .expect("to find_frequent_sync_passwords");
    //     assert_eq!(frequent, vec![(2, "{c5144948-fba1-594b-8148-ff70c85ee19a}".to_string())]);


    //     // let z = store.into_debug_conn();

    //     // assert_matches!(z.datoms(),
    //     //                 "[[100 :db.schema/version 1 ?tx true]
    //     //                   [101 :db.schema/version 2 ?tx true]]");


    //     // // let merged =
    //     // // "[:find [?a ?v] where [?le ?la ?lv ?ltx] [(ground values) [[?re ?ra ?rv ?rtx]]] (= ?la ?ra) (


    //     // // let y = conn.current_schema();
    //     // // let last_tx_id = conn.metadata.lock().expect("metadata").partition_map[":db.part/tx"].index;

    //     // // let metadata = conn.metadata.lock().expect("metadata");
    //     // // let z = TestConn {
    //     // //     sqlite: sqlite,
    //     // //     partition_map: metadata.partition_map,
    //     // //     schema: metadata.schema,
    //     // // };

    //     // // let x = mentat_db::debug::
    //     // // transactions_after(&mut sqlite, conn.current_schema(), last_tx_id).expect("last_transaction").0[0].into_edn();

    //     // let z = store.into_debug_conn();

    //     // assert_matches!(z.last_transaction(),
    //     //                 "[[100 :db.schema/version 1 ?tx true]
    //     //                   [101 :db.schema/version 2 ?tx true]]");

    //     // // assert_eq!(z.last_tx_id(), 10);


    //     // assert_eq!(Some((z.last_tx_id(), z.transactions())), None);
    //     //                 // "[[100 :db.schema/version 1 ?tx true]
    //     //                 //   [101 :db.schema/version 2 ?tx true]]");

    //     // store.transact(r#"[
    //     //     [:db/add "a" :db/ident :foo/term]
    //     //     [:db/add "a" :db/valueType :db.type/string]
    //     //     [:db/add "a" :db/fulltext false]
    //     //     [:db/add "a" :db/cardinality :db.cardinality/many]
    //     // ]"#).unwrap();

    //     // let tx1 = store.transact(r#"[
    //     //     [:db/add "e" :foo/term "1"]
    //     // ]"#).expect("tx1 to apply");

    //     // let tx2 = store.transact(r#"[
    //     //     [:db/add "e" :foo/term "2"]
    //     // ]"#).expect("tx2 to apply");

    //     // fn assert_tx_data(store: &Store, tx: &TxReport, value: TypedValue) {
    //     //     // TODO: after https://github.com/mozilla/mentat/issues/641, use q_prepare with inputs bound
    //     //     // at execution time.
    //     //     let r = store.q_once(r#"[:find ?e ?a-name ?v ?tx ?op
    //     //                          :in ?tx-in
    //     //                          :where
    //     //                          [(tx-data $ ?tx-in) [[?e ?a ?v ?tx ?op]]]
    //     //                          [?a :db/ident ?a-name]
    //     //                          :order ?e
    //     //                         ]"#,
    //     //                          QueryInputs::with_value_sequence(vec![
    //     //                              (Variable::from_valid_name("?tx-in"),  TypedValue::Ref(tx.tx_id)),
    //     //                          ]))
    //     //         .expect("results")
    //     //         .into();

    //     //     let e = tx.tempids.get("e").cloned().expect("tempid");

    //     //     match r {
    //     //         QueryResults::Rel(vals) => {
    //     //             assert_eq!(vals,
    //     //                        vec![
    //     //                            vec![TypedValue::Ref(e),
    //     //                                 TypedValue::typed_ns_keyword("foo", "term"),
    //     //                                 value,
    //     //                                 TypedValue::Ref(tx.tx_id),
    //     //                                 TypedValue::Boolean(true)],
    //     //                            vec![TypedValue::Ref(tx.tx_id),
    //     //                                 TypedValue::typed_ns_keyword("db", "txInstant"),
    //     //                                 TypedValue::Instant(tx.tx_instant),
    //     //                                 TypedValue::Ref(tx.tx_id),
    //     //                                 TypedValue::Boolean(true)],
    //     //                        ]);
    //     //         },
    //     //         x => panic!("Got unexpected results {:?}", x),
    //     //     }
    //     // };

    //     // assert_tx_data(&store, &tx1, TypedValue::String("1".to_string().into()));
    //     // assert_tx_data(&store, &tx2, TypedValue::String("2".to_string().into()));
    // }
}
