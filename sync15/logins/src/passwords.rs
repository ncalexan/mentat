// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use mentat::{
    Binding,
    // Cloned,
    DateTime,
    Entid,
    QueryInputs,
    QueryResults,
    Queryable,
    TxReport,
    TypedValue,
    Utc,
};

use mentat::conn::{
    InProgress,
};

use mentat::entity_builder::{
    BuildTerms,
    TermBuilder,
};

use credentials::{
    // LOGIN_AT,
    // LOGIN_CREDENTIAL,
    // LOGIN_DEVICE,
    // LOGIN_FORM,
    build_credential,
    find_credential_by_content,
};
use errors::{
    Error,
    Result,
};
use types::{
    Credential,
    CredentialId,
    FormTarget,
    ServerPassword,
    SyncGuid,
};

use vocab::{
    CREDENTIAL_CREATED_AT,
    CREDENTIAL_ID,
    CREDENTIAL_PASSWORD,
    CREDENTIAL_USERNAME,
    FORM_HOSTNAME,
    FORM_HTTP_REALM,
    FORM_PASSWORD_FIELD,
    FORM_SUBMIT_URL,
    FORM_SYNC_PASSWORD,
    FORM_USERNAME_FIELD,
    SYNC_PASSWORD_CREDENTIAL,
    SYNC_PASSWORD_MATERIAL_TX,
    SYNC_PASSWORD_METADATA_TX,
    SYNC_PASSWORD_SERVER_MODIFIED,
    SYNC_PASSWORD_TIMES_USED,
    SYNC_PASSWORD_TIME_CREATED,
    SYNC_PASSWORD_TIME_LAST_USED,
    SYNC_PASSWORD_TIME_PASSWORD_CHANGED,
    SYNC_PASSWORD_UUID,
    // CREDENTIAL_VOCAB,
    // FORM_VOCAB,
    // // LOGIN_VOCAB,
    // SYNC_PASSWORD_VOCAB,
};

pub fn credential_deltas<Q>
    (in_progress: &Q,
     id: CredentialId)
     -> Result<Option<(String, Entid, DateTime<Utc>, String, Entid, DateTime<Utc>)>>
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
        Some((Binding::Scalar(TypedValue::String(username)),
              Binding::Scalar(TypedValue::Ref(username_tx)),
              Binding::Scalar(TypedValue::Instant(username_tx_instant)),
              Binding::Scalar(TypedValue::String(password)),
              Binding::Scalar(TypedValue::Ref(password_tx)),
              Binding::Scalar(TypedValue::Instant(password_tx_instant)))) =>
            Ok(Some(((*username).clone(),
                     username_tx,
                     username_tx_instant,
                     (*password).clone(),
                     password_tx,
                     password_tx_instant))),
        None => Ok(None),
        _ => bail!(Error::BadQueryResultType),
    }
}

pub fn time_sync_password_modified<Q>(queryable: &Q, uuid: SyncGuid) -> Result<Option<DateTime<Utc>>>
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
            Some(_) => bail!(Error::BadQueryResultType),
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
            Some(_) => bail!(Error::BadQueryResultType),
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
                            -> Result<Option<ServerPassword>>
where Q: Queryable {
    let q = r#"[:find
                [(pull ?c [:credential/id :credential/username :credential/password :credential/createdAt])
                 (pull ?f [:form/hostname :form/usernameField :form/passwordField :form/submitUrl :form/httpRealm])]
                :in
                ?uuid
                :where
                [?sp :sync.password/uuid ?uuid]
                [?sp :sync.password/credential ?c]
                [?c :credential/id _] ; Deleted credentials produce dangling :sync.password/credential refs; ignore them.
                [?f :form/syncPassword ?sp]
               ]"#;

    let inputs = QueryInputs::with_value_sequence(vec![
        (var!(?uuid), TypedValue::typed_string(&id)),
    ]);

    let server_password = match queryable.q_once(q, inputs)?.into_tuple()? {
        Some((Binding::Map(cm), Binding::Map(fm))) => {
            let cid = CredentialId(cm[CREDENTIAL_ID.clone()].as_string().map(|x| (**x).clone()).unwrap()); // XXX
            let username = cm[CREDENTIAL_USERNAME.clone()].as_string().map(|x| (**x).clone()); // XXX
            let password = cm[CREDENTIAL_PASSWORD.clone()].as_string().map(|x| (**x).clone()).unwrap(); // XXX
            let time_created = cm[CREDENTIAL_CREATED_AT.clone()].as_instant().map(|x| (*x).clone()).unwrap(); // XXX

            let hostname = fm.0.get(&FORM_HOSTNAME.clone()).and_then(|x| x.as_string()).map(|x| (**x).clone()).unwrap(); // XXX
            let username_field = fm.0.get(&FORM_USERNAME_FIELD.clone()).and_then(|x| x.as_string()).map(|x| (**x).clone()); // XXX
            let password_field = fm.0.get(&FORM_PASSWORD_FIELD.clone()).and_then(|x| x.as_string()).map(|x| (**x).clone()); // XXX

            let form_submit_url = fm.0.get(&FORM_SUBMIT_URL.clone()).and_then(|x| x.as_string()).map(|x| (**x).clone()); // XXX
            let http_realm = fm.0.get(&FORM_HTTP_REALM.clone()).and_then(|x| x.as_string()).map(|x| (**x).clone()); // XXX

            // TODO: produce a more informative error in this situation.
            let target = match (form_submit_url, http_realm) {
                // Logins with both a formSubmitURL and httpRealm are not valid.
                (Some(_), Some(_)) => bail!(Error::BadQueryResultType),
                (Some(form_submit_url), _) => FormTarget::FormSubmitURL(form_submit_url),
                (_, Some(http_realm)) => FormTarget::HttpRealm(http_realm),
                // Login must have at least a formSubmitURL or httpRealm.
                _ => bail!(Error::BadQueryResultType),
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
        _ => bail!(Error::BadQueryResultType),
    };

    server_password
}

pub fn get_all_sync_passwords<Q>(queryable: &Q)
                                 -> Result<Vec<ServerPassword>>
where Q: Queryable {
    let q = r#"[
:find
 [?uuid ...]
:where
 [_ :sync.password/uuid ?uuid]
:order
 (asc ?uuid)
]"#;

    let uuids: Result<Vec<_>> = queryable.q_once(q, None)?
        .into_coll()?
        .into_iter()
        .map(|uuid| {
            match uuid {
                Binding::Scalar(TypedValue::String(uuid)) => Ok(SyncGuid((*uuid).clone())),
                _ => bail!(Error::BadQueryResultType),
            }
        })
        .collect();
    let uuids = uuids?;

    let mut ps = Vec::with_capacity(uuids.len());

    for uuid in uuids {
        get_sync_password(queryable, uuid)?.map(|p| ps.push(p));
    }

    Ok(ps)
}

pub fn find_credential_id_by_sync_password_uuid<Q>(queryable: &Q,
                                                uuid: SyncGuid)
                                                -> Result<Option<CredentialId>>
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
                None => bail!(Error::BadQueryResultType),
            }
        }
        None => Ok(None),
    }
}

// TODO: u64.
pub fn times_used<Q>(queryable: &Q, id: CredentialId) -> Result<i64>
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

        let sync_mirror: Result<_> = match queryable.q_once(q, QueryInputs::with_value_sequence(vec![(var!(?id), TypedValue::typed_string(&id))]))?.into_tuple()? {
            Some((Binding::Scalar(TypedValue::Ref(sl)), Binding::Scalar(TypedValue::Long(times_used)), Binding::Scalar(TypedValue::Ref(tx)))) => {
                Ok(Some((sl, times_used, tx)))
            },
            None => Ok(None),
            _ => bail!(Error::BadQueryResultType),
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
    let (q, sync_tx) = if let Some((_, _, sync_tx)) = sync_mirror {
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

    let local_times_used: Result<_> = match queryable.q_once(q, values)?
        .into_scalar()? {
        Some(Binding::Scalar(TypedValue::Long(times_used))) => Ok(times_used), // TODO: work out overflow.
        None => Ok(0),
            _ => bail!(Error::BadQueryResultType),
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
pub fn time_last_used<Q>(queryable: &Q, id: CredentialId) -> Result<DateTime<Utc>>
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

        let sync_mirror: Result<_> = match queryable.q_once(q, QueryInputs::with_value_sequence(vec![(var!(?id), TypedValue::typed_string(id.clone()))]))?.into_tuple()? {
            Some((Binding::Scalar(TypedValue::Ref(sl)), Binding::Scalar(TypedValue::Instant(time_last_used)), Binding::Scalar(TypedValue::Ref(tx)))) =>
                Ok(Some((sl, time_last_used, tx))),
            None => Ok(None),
            _ => bail!(Error::BadQueryResultType),
        };

        sync_mirror?
    };

    info!("time_last_used: sync_mirror: {:?}", sync_mirror);

    // TODO: use `when_some` instead?  I'm not clear which is more clear.
    let (q, sync_tx) = if let Some((_, _, sync_tx)) = sync_mirror {
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

    // info!("time_last_used: values: {:?}", values);

    let local_time_last_used: Result<_> = match queryable.q_once(q, values)?
        .into_scalar()? {
        Some(Binding::Scalar(TypedValue::Instant(time_last_used))) => Ok(Some(time_last_used)),
        None => Ok(None),
            _ => bail!(Error::BadQueryResultType),
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
pub fn new_credential_ids<Q>(queryable: &Q) -> Result<Vec<CredentialId>>
    where Q: Queryable
{
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
    let new_ids: Result<Vec<_>> = vs.into_iter()
        .map(|id| match id {
            Binding::Scalar(TypedValue::String(id)) => Ok(CredentialId((*id).clone())),
            _ => bail!(Error::BadQueryResultType),
        })
        .collect();
    new_ids
}

pub fn time_password_changed<Q>(queryable: &Q, uuid: SyncGuid) -> Result<Option<DateTime<Utc>>>
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
            Some(_) => bail!(Error::BadQueryResultType),
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
            Some((Binding::Scalar(TypedValue::Ref(material_tx)),
                  Binding::Scalar(TypedValue::Ref(username_tx)),
                  Binding::Scalar(TypedValue::Instant(username_tx_instant)),
                  Binding::Scalar(TypedValue::Ref(password_tx)),
                  Binding::Scalar(TypedValue::Instant(password_tx_instant)))) => {
                Some((material_tx,
                      username_tx,
                      username_tx_instant.clone(),
                      password_tx,
                      password_tx_instant.clone()))
            },
            None => None,
            _ => bail!(Error::BadQueryResultType),
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

pub fn get_deleted_sync_password_uuids_to_upload<Q>(queryable: &Q) -> Result<Vec<SyncGuid>>
    where Q: Queryable
{
    // Deleted records either don't have a linked credential, or have a dangling reference to a
    // credential with no id.

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
    let deleted_uuids: Result<Vec<_>> = vs.into_iter()
        .map(|id| match id {
            Binding::Scalar(TypedValue::String(id)) => Ok(SyncGuid((*id).clone())),
            _ => bail!(Error::BadQueryResultType),
        })
        .collect();
    deleted_uuids
}

pub fn reset_client(in_progress: &mut InProgress) -> Result<()> {
    let q = r#"[
:find
 [?e ...]
:where
 [?e :sync.password/uuid _]
]"#;

    let tx = TypedValue::Ref(0);

    let mut builder = TermBuilder::new();

    let results = in_progress.q_once(q, None)?.results;

    match results {
        QueryResults::Coll(es) => {
            for e in es {
                match e {
                    Binding::Scalar(TypedValue::Ref(e)) => {
                        builder.add(e, SYNC_PASSWORD_MATERIAL_TX.clone(), tx.clone())?;
                        builder.add(e, SYNC_PASSWORD_METADATA_TX.clone(), tx.clone())?;
                    },
                    _ => bail!(Error::BadQueryResultType),
                }
            }
        },
        _ => bail!(Error::BadQueryResultType),
    }

    in_progress.transact_builder(builder).map_err(|e| e.into()).and(Ok(()))
}

pub fn get_modified_sync_passwords_to_upload<Q>(queryable: &Q) -> Result<Vec<ServerPassword>>
where Q: Queryable
{
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

    let uuids: Result<Vec<_>> = queryable.q_once(q, None)?
        .into_coll()?
        .into_iter()
        .map(|uuid| {
            match uuid {
                Binding::Scalar(TypedValue::String(uuid)) => Ok(SyncGuid((*uuid).clone())),
                _ => bail!(Error::BadQueryResultType),
            }
        })
        .collect();
    let uuids = uuids?;

    let mut ps = Vec::with_capacity(uuids.len());

    for uuid in uuids {
        get_sync_password(queryable, uuid)?.map(|p| ps.push(p));
    }

    Ok(ps)
}

fn transact_sync_password_metadata(builder: &mut TermBuilder,
                                login: &ServerPassword,
                                credential_id: CredentialId)
                                -> Result<()> {
    let c = builder.named_tempid("c");
    builder.add(c.clone(),
                CREDENTIAL_ID.clone(),
                TypedValue::typed_string(credential_id))?;

    let sl = builder.named_tempid("sl");
    builder.add(sl.clone(),
                SYNC_PASSWORD_UUID.clone(),
                TypedValue::typed_string(&login.uuid))?;
    builder.add(sl.clone(),
                SYNC_PASSWORD_CREDENTIAL.clone(),
                TypedValue::typed_string("c"))?; // TODO
    builder.add(sl.clone(),
                SYNC_PASSWORD_SERVER_MODIFIED.clone(),
                TypedValue::Instant(login.modified.clone()))?;
    builder.add(sl.clone(),
                SYNC_PASSWORD_TIMES_USED.clone(),
                TypedValue::Long(login.times_used as i64))?; // XXX.
    builder.add(sl.clone(),
                SYNC_PASSWORD_TIME_CREATED.clone(),
                TypedValue::Instant(login.time_created.clone()))?;
    builder.add(sl.clone(),
                SYNC_PASSWORD_TIME_LAST_USED.clone(),
                TypedValue::Instant(login.time_last_used.clone()))?;
    builder.add(sl.clone(),
                SYNC_PASSWORD_TIME_PASSWORD_CHANGED.clone(),
                TypedValue::Instant(login.time_password_changed.clone()))?;

    let f = builder.named_tempid("f");
    builder.add(f.clone(),
                FORM_SYNC_PASSWORD.clone(),
                TypedValue::typed_string("sl"))?; // TODO
    builder.add(f.clone(),
                FORM_HOSTNAME.clone(),
                TypedValue::typed_string(&login.hostname))?;
    if let Some(ref username_field) = login.username_field {
        builder.add(f.clone(),
                    FORM_USERNAME_FIELD.clone(),
                    TypedValue::typed_string(&username_field))?;
    }
    if let Some(ref password_field) = login.password_field {
        builder.add(f.clone(),
                    FORM_PASSWORD_FIELD.clone(),
                    TypedValue::typed_string(&password_field))?;
    }

    match login.target {
        FormTarget::FormSubmitURL(ref form_submit_url) => {
            builder.add(f.clone(),
                        FORM_SUBMIT_URL.clone(),
                        TypedValue::typed_string(form_submit_url))?;
        },
        FormTarget::HttpRealm(ref http_realm) => {
            builder.add(f.clone(),
                        FORM_HTTP_REALM.clone(),
                        TypedValue::typed_string(http_realm))?;
        },
    }

    // builder.add(sl.clone(), in_progress.get_entid(&SYNC_PASSWORD_TIME_PASSWORD_CHANGED).expect(":sync.password"), login.last_time_used)??;

    // in_progress.transact_builder(builder).map_err(|e| e.into()).and(Ok(()))
    Ok(())
}

pub fn merge_into_credential(in_progress: &InProgress,
                             builder: &mut TermBuilder,
                             id: CredentialId,
                             modified: DateTime<Utc>,
                             username: Option<String>,
                             password: String)
                             -> Result<()> {
    let x = credential_deltas(in_progress, id.clone())?;
    let (_u, _utx, utxi, _p, _ptx, ptxi) = match x {
        Some(x) => x,
        None => bail!(Error::BadQueryResultType),
    };

    debug!("merge_into_credential({}): remote modified {}, local username modified {}, local password modified {}",
           &id.0, modified, utxi, ptxi);

    let c = builder.named_tempid("c");
    builder.add(c.clone(),
                CREDENTIAL_ID.clone(),
                TypedValue::typed_string(id.clone()))?;

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
        info!("merge_into_credential({}): remote modified later than both local username and password; setting username {}, password {}",
              &id.0, username.clone().unwrap(), password);

        builder.add(c.clone(),
                    CREDENTIAL_USERNAME.clone(),
                    TypedValue::String(username.unwrap().into()))?; // XXX.

        builder.add(c.clone(),
                    CREDENTIAL_PASSWORD.clone(),
                    TypedValue::String(password.into()))?;

        let sl = builder.named_tempid("sl");
        builder.add(sl.clone(),
                    SYNC_PASSWORD_CREDENTIAL.clone(),
                    TypedValue::typed_string("c"))?;
        builder.add(sl.clone(),
                    SYNC_PASSWORD_MATERIAL_TX.clone(),
                    TermBuilder::tx_function("transaction-tx"))?;
    } else {
        info!("merge_into_credential({}): local modified later than either remote username or password; keeping (and uploading) local modifications",
              &id.0);
    }

    // TODO: what do we do with timeCreated and timePasswordChanged?

    Ok(())
}

enum Either<A, B> {
    Left(A),
    Right(B),
}

pub fn apply_changed_login(in_progress: &mut InProgress,
                           login: ServerPassword)
                           -> Result<TxReport> {
    // let mut in_progress = store.begin_transaction()?;

    let id = match find_credential_id_by_sync_password_uuid(in_progress, login.uuid.clone())? {
        Some(id) => Some(Either::Left(id)),
        None => {
            find_credential_by_content(in_progress,
                                       login.username.clone().unwrap(),
                                       login.password.clone())?
                .map(|c| Either::Right(c.id))
        } // TODO: handle optional usernames.
    };

    let mut builder = TermBuilder::new();

    match id {
        None => {
            info!("apply_changed_login: no existing credential for sync uuid {:?}", login.uuid);

            // Nothing found locally.  Add the credential and the sync login directly to the store, and
            // commit the sync tx at the same time.
            let id = CredentialId(login.uuid.0.clone()); // CredentialId::random();

            let credential = Credential {
                id: id.clone(),
                username: login.username.clone(),
                password: login.password.clone(),
                created_at: login.time_created.clone(),
                title: None,
            };

            build_credential(&mut builder, credential)?;
            transact_sync_password_metadata(&mut builder, &login, id.clone())?;

            // Set metadataTx and materialTx to :db/tx.
            let c = builder.named_tempid("c"); // This is fun!  We could have collision of tempids across uses.
            builder.add(c.clone(),
                        CREDENTIAL_ID.clone(),
                        TypedValue::typed_string(id))?;
            let sl = builder.named_tempid("sl");
            builder.add(sl.clone(),
                        SYNC_PASSWORD_CREDENTIAL.clone(),
                        TypedValue::typed_string("c"))?;
            builder.add(sl.clone(),
                        SYNC_PASSWORD_MATERIAL_TX.clone(),
                        TermBuilder::tx_function("transaction-tx"))?;
            builder.add(sl.clone(),
                        SYNC_PASSWORD_METADATA_TX.clone(),
                        TermBuilder::tx_function("transaction-tx"))?;
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

    in_progress.transact_builder(builder).map_err(|e| e.into())
}

pub fn delete_by_sync_uuid(in_progress: &mut InProgress,
                       uuid: SyncGuid)
                       -> Result<()> {
    delete_by_sync_uuids(in_progress, ::std::iter::once(uuid))
}

pub fn delete_by_sync_uuids<I>(in_progress: &mut InProgress,
                           uuids: I)
                           -> Result<()>
where I: IntoIterator<Item=SyncGuid> {

    // TODO: use `:db/retractEntity` to make this less onerous and avoid cloning.
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

    let mut builder = TermBuilder::new();

    // TODO: do this in one query.  It's awkward because Mentat doesn't support binding non-scalar
    // inputs yet; see https://github.com/mozilla/mentat/issues/714.
    for uuid in uuids {
        let inputs = QueryInputs::with_value_sequence(vec![(var!(?uuid), TypedValue::typed_string(uuid))]);
        let results = in_progress.q_once(q, inputs)?.results;

        match results {
            QueryResults::Rel(vals) => {
                for vs in vals {
                    match (vs.len(), vs.get(0), vs.get(1), vs.get(2)) {
                        (3, Some(&Binding::Scalar(TypedValue::Ref(e))), Some(&Binding::Scalar(TypedValue::Ref(a))), Some(&Binding::Scalar(ref v))) => {
                            builder.retract(e, a, v.clone())?; // TODO: don't clone.
                        }
                        _ => unreachable!("bad query result types in delete_by_sync_uuid"),
                    }
                }
            },
            _ => bail!(Error::BadQueryResultType),
        }
    }

    in_progress.transact_builder(builder).map_err(|e| e.into()).and(Ok(()))
}

pub fn mark_synced_by_sync_uuids<I>(in_progress: &mut InProgress, uuids: I, tx_id: Entid) -> Result<()>
where I: IntoIterator<Item=SyncGuid> {

    let q = r#"[
        :find
         ?e .
        :in
         ?uuid
        :where
         [?e :sync.password/uuid ?uuid]
        ]"#;

    let tx = TypedValue::Ref(tx_id);

    let mut builder = TermBuilder::new();

    // TODO: do this in one query (or transaction).  It's awkward because Mentat doesn't support
    // binding non-scalar inputs yet; see https://github.com/mozilla/mentat/issues/714.
    for uuid in uuids {
        let inputs = QueryInputs::with_value_sequence(vec![(var!(?uuid), TypedValue::typed_string(uuid))]);
        match in_progress.q_once(q, inputs)?.results {
            QueryResults::Scalar(Some(Binding::Scalar(TypedValue::Ref(e)))) => {
                builder.add(e, SYNC_PASSWORD_MATERIAL_TX.clone(), tx.clone())?;
                builder.add(e, SYNC_PASSWORD_METADATA_TX.clone(), tx.clone())?;
            },
            _ => bail!(Error::BadQueryResultType),
        }
    }

    in_progress.transact_builder(builder).map_err(|e| e.into()).and(Ok(()))
}

#[cfg(test)]
mod tests {
    use mentat::{
        FromMicros,
    };

    use mentat::conn::{
        Dumpable,
    };

    use super::*;

    use credentials::{
        // add_credential,
        delete_by_id,
        // touch_by_id,
    };

    use tests::{
        testing_store,
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

    #[test]
    fn test_get_sync_password() {
        // Verify that applying a password and then immediately reading it back yields the original
        // data.

        let mut store = testing_store();
        let mut in_progress = store.begin_transaction().expect("begun successfully");

        apply_changed_login(&mut in_progress, LOGIN1.clone()).expect("to apply");
        apply_changed_login(&mut in_progress, LOGIN2.clone()).expect("to apply");

        let sp = get_sync_password(&in_progress,
                                   LOGIN1.uuid.clone()).expect("to get_sync_password");
        assert_eq!(sp, Some(LOGIN1.clone()));

        let sp = get_sync_password(&in_progress,
                                   LOGIN2.uuid.clone()).expect("to get_sync_password");
        assert_eq!(sp, Some(LOGIN2.clone()));

        let sp = get_sync_password(&in_progress,
                                   "nonexistent id".into()).expect("to get_sync_password");
        assert_eq!(sp, None);
    }

    #[test]
    fn test_get_sync_password_with_deleted_credential() {
        // Verify that applying a password, deleting its underlying credential, and then immediately
        // reading it back doesn't return the Sync 1.5 password.  This is one of many possible
        // choices for representing local deletion.

        let mut store = testing_store();
        let mut in_progress = store.begin_transaction().expect("begun successfully");

        apply_changed_login(&mut in_progress, LOGIN1.clone()).expect("to apply");

        let sp = get_sync_password(&in_progress,
                                   LOGIN1.uuid.clone()).expect("to get_sync_password");
        assert_eq!(sp, Some(LOGIN1.clone()));

        // Here we're using that the credential uuid and the Sync 1.5 uuid are the same; that's
        // not a stable assumption.
        delete_by_id(&mut in_progress, LOGIN1.uuid.0.clone().into()).expect("to delete_by_id");

        let sp = get_sync_password(&in_progress,
                                   LOGIN1.uuid.clone()).expect("to get_sync_password");
        assert_eq!(sp, None);
    }

    #[test]
    fn test_get_all_sync_passwords() {
        // Verify that applying passwords and then immediately reading them all back yields the
        // original data.

        let mut store = testing_store();
        let mut in_progress = store.begin_transaction().expect("begun successfully");

        apply_changed_login(&mut in_progress, LOGIN1.clone()).expect("to apply 1");
        apply_changed_login(&mut in_progress, LOGIN2.clone()).expect("to apply 2");

        let sps = get_all_sync_passwords(&in_progress).expect("to get_all_sync_passwords");
        assert_eq!(sps, vec![LOGIN1.clone(), LOGIN2.clone()]);
    }

    #[test]
    fn test_apply_twice() {
        // Verify that applying a password twice doesn't do anything the second time through.

        let mut store = testing_store();
        let mut in_progress = store.begin_transaction().expect("begun successfully");

        apply_changed_login(&mut in_progress, LOGIN1.clone()).expect("to apply");

        apply_changed_login(&mut in_progress, LOGIN1.clone()).expect("to apply");

        let t = in_progress.dump_last_transaction().expect("transaction");
        assert_eq!(t.into_vector().expect("vector").len(), 1); // Just the :db/txInstant.
    }

    #[test]
    fn test_remote_evolved() {
        // Verify that when there are no local changes, applying a remote record that has evolved
        // takes the remote changes.

        let mut store = testing_store();
        let mut in_progress = store.begin_transaction().expect("begun successfully");

        let mut login = LOGIN1.clone();

        apply_changed_login(&mut in_progress, login.clone()).expect("to apply");

        login.modified = ::mentat::now();
        login.password = "password2".into();
        login.password_field = Some("password".into());
        login.times_used = 13;

        apply_changed_login(&mut in_progress, login.clone()).expect("to apply");

        let sp = get_sync_password(&in_progress,
                                   login.uuid.clone()).expect("to get_sync_password");
        assert_eq!(sp, Some(login.clone()));
    }

    #[test]
    fn test_get_modified_sync_passwords_to_upload() {
        let mut store = testing_store();
        let mut in_progress = store.begin_transaction().expect("begun successfully");

        apply_changed_login(&mut in_progress, LOGIN1.clone()).expect("to apply");
        let report2 = apply_changed_login(&mut in_progress, LOGIN2.clone()).expect("to apply");

        // But if there are no local changes, we shouldn't propose any records to re-upload.
        let sp = get_modified_sync_passwords_to_upload(&in_progress).expect("to get_sync_password");
        assert_eq!(sp, vec![]);

        // Now, let's modify locally an existing credential connected to a Sync 1.5 record.
        //
        // Here we're using that the credential uuid and the Sync 1.5 uuid are the same; that's
        // not a stable assumption.
        let mut builder = TermBuilder::new();
        builder.add(TermBuilder::lookup_ref(CREDENTIAL_ID.clone(), TypedValue::String(LOGIN1.uuid.0.clone().into())),
                    CREDENTIAL_USERNAME.clone(),
                    TypedValue::typed_string("us3rnam3@mockymid.com")).expect("add");
        builder.add(TermBuilder::lookup_ref(CREDENTIAL_ID.clone(), TypedValue::String(LOGIN1.uuid.0.clone().into())),
                    CREDENTIAL_PASSWORD.clone(),
                    TypedValue::typed_string("pa33w3rd")).expect("add");
        let report1 = in_progress.transact_builder(builder).expect("to transact");

        // Just for our peace of mind.  One add and one retract per
        // {username,password}, and the :db/txInstant.
        let t = in_progress.dump_last_transaction().expect("transaction");
        assert_eq!(t.into_vector().expect("vector").len(), 5);

        // Our local change results in a record needing to be uploaded remotely.
        let sp = get_modified_sync_passwords_to_upload(&in_progress).expect("to get_sync_password");

        let mut login1 = LOGIN1.clone();
        login1.username = Some("us3rnam3@mockymid.com".into());
        login1.password = "pa33w3rd".into();
        login1.modified = report1.tx_instant;
        login1.time_password_changed = report1.tx_instant;
        assert_eq!(sp, vec![login1.clone()]);

        // Suppose we disconnect, so that the last materialTx is TX0 (and the last metadataTx is
        // also TX0), and then reconnect.  We'll have Sync 1.5 data in the store, and we'll need
        // to upload it all.
        reset_client(&mut in_progress).expect("to reset_client");

        let sp = get_modified_sync_passwords_to_upload(&in_progress).expect("to get_sync_password");

        // The credential in the store postdates the materialTx (== TX0), so we'll re-upload
        // these as records with timestamps that aren't identical to upstream.  I think it would
        // be better to re-populate the server with records identical to the earlier records,
        // but I don't think it's necessary to do so, so for now I'm avoiding that hassle.
        let mut login2 = LOGIN2.clone();
        login2.modified = report2.tx_instant;
        login2.time_password_changed = report2.tx_instant;
        assert_eq!(sp.len(), 2);
        assert_eq!(sp[0], login1);
        assert_eq!(sp[1], login2);
        assert_eq!(sp, vec![login1.clone(), login2.clone()]);
    }

    #[test]
    fn test_get_deleted_sync_uuids_to_upload() {
        let mut store = testing_store();
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
        let mut builder = TermBuilder::new();
        builder.retract(TermBuilder::lookup_ref(CREDENTIAL_ID.clone(), TypedValue::String(LOGIN1.uuid.0.clone().into())),
                        CREDENTIAL_ID.clone(),
                        TypedValue::String(LOGIN1.uuid.0.clone().into())).expect("add");
        in_progress.transact_builder(builder).expect("to transact");

        // Just for our peace of mind.
        let t = in_progress.dump_last_transaction().expect("transaction");
        assert_eq!(t.into_vector().expect("vector").len(), 2); // One retract, and the :db/txInstant.

        // The record's gone, Jim!
        let sp = get_deleted_sync_password_uuids_to_upload(&in_progress).expect("to get_sync_password");
        assert_eq!(sp, vec![LOGIN1.uuid.clone()]);

        // We can also sever the link between the Sync 1.5 record and the underlying credential.
        let mut builder = TermBuilder::new();
        builder.retract(TermBuilder::lookup_ref(SYNC_PASSWORD_UUID.clone(), TypedValue::String(LOGIN2.uuid.0.clone().into())),
                        SYNC_PASSWORD_CREDENTIAL.clone(),
                        TermBuilder::lookup_ref(CREDENTIAL_ID.clone(), TypedValue::String(LOGIN2.uuid.0.clone().into()))).expect("add");
        in_progress.transact_builder(builder).expect("to transact");

        let t = in_progress.dump_last_transaction().expect("transaction");
        assert_eq!(t.into_vector().expect("vector").len(), 2); // One retract, and the :db/txInstant.

        // Now both records are gone.
        let sp = get_deleted_sync_password_uuids_to_upload(&in_progress).expect("to get_sync_password");
        assert_eq!(sp, vec![LOGIN1.uuid.clone(), LOGIN2.uuid.clone()]);
    }

    #[test]
    fn test_mark_synced_by_sync_uuids() {
        let mut store = testing_store();
        let mut in_progress = store.begin_transaction().expect("begun successfully");

        let report1 = apply_changed_login(&mut in_progress, LOGIN1.clone()).expect("to apply");
        let report2 = apply_changed_login(&mut in_progress, LOGIN2.clone()).expect("to apply");

        let sp = get_modified_sync_passwords_to_upload(&in_progress).expect("to get_sync_password");
        assert_eq!(sp, vec![]);

        // Suppose we disconnect, so that the last sync tx is TX0, and then reconnect.  We'll
        // have Sync 1.5 data in the store, and we'll need to upload it all.
        reset_client(&mut in_progress).expect("to reset_client");

        let sp = get_modified_sync_passwords_to_upload(&in_progress).expect("to get_sync_password");

        let mut login1 = LOGIN1.clone();
        login1.modified = report1.tx_instant;
        login1.time_password_changed = report1.tx_instant;
        let mut login2 = LOGIN2.clone();
        login2.modified = report2.tx_instant;
        login2.time_password_changed = report2.tx_instant;
        assert_eq!(sp, vec![login1.clone(), login2.clone()]);

        // Mark one password synced, and the other one will need to be uploaded.
        let synced_tx_id = in_progress.last_tx_id();
        let iters = ::std::iter::once(LOGIN1.uuid.clone());
        mark_synced_by_sync_uuids(&mut in_progress, iters.clone(), synced_tx_id).expect("to mark synced by sync uuids");

        let sp = get_modified_sync_passwords_to_upload(&in_progress).expect("to get_sync_password");
        assert_eq!(sp, vec![login2.clone()]);

        // Mark everything unsynced, and then everything synced, and we won't upload anything.
        reset_client(&mut in_progress).expect("to reset_client");

        let synced_tx_id = in_progress.last_tx_id();
        let iters = ::std::iter::once(LOGIN1.uuid.clone()).chain(::std::iter::once(LOGIN2.uuid.clone()));
        mark_synced_by_sync_uuids(&mut in_progress, iters.clone(), synced_tx_id).expect("to mark synced by sync uuids");

        let sp = get_modified_sync_passwords_to_upload(&in_progress).expect("to get_sync_password");
        assert_eq!(sp, vec![]);
    }

    #[test]
    fn test_delete_by_sync_uuid() {
        let mut store = testing_store();
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

    #[test]
    fn test_delete_by_sync_uuids() {
        let mut store = testing_store();
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

    // #[test]
    // fn test_lockbox_logins() {
    //     let cid = CredentialId("id1".to_string());

    //     let mut store = testing_store();

    //     // Scoped borrow of `store`.
    //     {
    //         let mut in_progress = store.begin_transaction().expect("begun successfully");

    //         let credential = Credential {
    //             id: cid.clone(),
    //             username: Some("user1".to_string()),
    //             password: "pass1".to_string(),
    //             created_at: ::mentat::now(),
    //             title: None,
    //         };
    //         add_credential(&mut in_progress, credential)
    //             .expect("to add credential 1");
    //         in_progress.commit().expect("to commit");
    //     };

    //     // Scoped borrow of `store`.
    //     {
    //         let mut in_progress = store.begin_transaction().expect("begun successfully");
    //         touch_by_id(&mut in_progress, cid.clone(), Some(Utc.timestamp(1, 0))).expect("to touch id1 1");
    //         in_progress.commit().expect("to commit");
    //     }

    //     // Scoped borrow of `store`.
    //     let tx_id = {
    //         let mut in_progress = store.begin_transaction().expect("begun successfully");

    //         let tx_id = in_progress.transact(r#"[]"#).expect("to transact empty").tx_id;

    //         in_progress.commit().expect("to commit");
    //         tx_id
    //     };

    //     // Scoped borrow of `store`.
    //     {
    //         let mut in_progress = store.begin_transaction().expect("begun successfully");
    //         touch_by_id(&mut in_progress, cid.clone(), Some(Utc.timestamp(3, 0))).expect("to touch id1 2");
    //         in_progress.commit().expect("to commit");
    //     }

    //     // Scoped borrow of `store`.
    //     {
    //         let in_progress = store.begin_read().expect("begun successfully");

    //         assert_eq!(2, times_used(&in_progress, cid.clone()).expect("to fetch local_times_used"));
    //         assert_eq!(Utc.timestamp(3, 0), time_last_used(&in_progress, cid.clone()).expect("to fetch local_times_used"));
    //         assert_eq!(vec![cid.clone()], new_credential_ids(&in_progress).expect("to fetch new_credentials_ids"));
    //         assert_eq!(Vec::<SyncGuid>::new(), get_deleted_sync_password_uuids_to_upload(&in_progress).expect("to fetch get_deleted_sync_password_uuids_to_upload"));
    //     }

    //     // Scoped borrow of `store`.
    //     {
    //         let mut in_progress = store.begin_transaction().expect("begun successfully");

    //         let x = format!("[
    // {{:sync.password/credential (lookup-ref :credential/id \"id1\")
    //  :sync.password/uuid \"uuid1\"
    //  :sync.password/timesUsed 3
    //  :sync.password/timeLastUsed #inst \"{}\"
    //  :sync.password/metadataTx {}}}
    // ]", Utc.timestamp(5, 0).to_rfc3339(), 0);
    //         // assert_eq!("", x);
    //         in_progress.transact(x).expect("to transact 1");

    //         // 3 remote visits, 2 local visits after the given :sync.password/tx.
    //         assert_eq!(5, times_used(&in_progress, cid.clone()).expect("to fetch local_times_used + remote_times_used"));
    //         // Remote lastUsed is after all of our local usages.
    //         assert_eq!(Utc.timestamp(5, 0), time_last_used(&in_progress, cid.clone()).expect("to fetch time_last_used"));
    //         assert_eq!(Vec::<CredentialId>::new(), new_credential_ids(&in_progress).expect("to fetch new_credentials_ids"));
    //         assert_eq!(Vec::<SyncGuid>::new(), get_deleted_sync_password_uuids_to_upload(&in_progress).expect("to fetch get_deleted_sync_password_uuids_to_upload"));

    //         // in_progress.commit()
    //     }

    //     // Scoped borrow of `store`.
    //     {
    //         let mut in_progress = store.begin_transaction().expect("begun successfully");

    //         in_progress.transact(format!("[
    // {{:sync.password/credential (lookup-ref :credential/id \"id1\")
    //  :sync.password/uuid \"uuid1\"
    //  :sync.password/timesUsed 3
    //  :sync.password/timeLastUsed #inst \"{}\"
    //  :sync.password/metadataTx {}}}
    // ]", Utc.timestamp(2, 0).to_rfc3339(), tx_id))
    //             .expect("to transact 2");

    //         // 3 remote visits, 1 local visit after the given :sync.password/tx.
    //         assert_eq!(4, times_used(&in_progress, cid.clone()).expect("to fetch local_times_used + remote_times_used"));
    //         // Remote lastUsed is between our local usages, so the latest local usage wins.
    //         assert_eq!(Utc.timestamp(3, 0), time_last_used(&in_progress, cid.clone()).expect("to fetch time_last_used"));
    //         assert_eq!(Vec::<CredentialId>::new(), new_credential_ids(&in_progress).expect("to fetch new_credentials_ids"));
    //         assert_eq!(Vec::<SyncGuid>::new(), get_deleted_sync_password_uuids_to_upload(&in_progress).expect("to fetch get_deleted_sync_password_uuids_to_upload"));

    //         // in_progress.commit()
    //     }

    //     // Scoped borrow of `store`.
    //     {
    //         let mut in_progress = store.begin_transaction().expect("begun successfully");

    //         in_progress.transact(format!("[
    // [:db/retract (lookup-ref :credential/id \"id1\") :credential/id \"id1\"]
    // {{:sync.password/uuid \"uuid2\"
    // }}
    // {{:sync.password/uuid \"uuid3\"
    //   :sync.password/credential (lookup-ref :credential/id \"id1\")
    // }}
    // ]"))
    //             .expect("to transact 3");

    //         assert_eq!(vec![SyncGuid("uuid2".to_string()), SyncGuid("uuid3".to_string())], get_deleted_sync_password_uuids_to_upload(&in_progress).expect("to fetch get_deleted_sync_password_uuids_to_upload"));

    //         // in_progress.commit()
    //     }

    //     // Scoped borrow of `store`.
    //     {
    //         let mut in_progress = store.begin_transaction().expect("begun successfully");
    //         let last_tx = in_progress.last_tx_id();
    //         // assert_eq!(1, 2);

    //         apply_changed_login(&mut in_progress, LOGIN1.clone()).expect("to apply 1");

    //         let s = in_progress.dump_datoms_after(last_tx-1).expect("datoms").to_pretty(120).unwrap();
    //         println!("last_tx {}:\n{}", last_tx, s);

    //         in_progress.commit().expect("to commit")
    //     }

    //     // Scoped borrow of `store`.
    //     {
    //         let mut in_progress = store.begin_transaction().expect("begun successfully");
    //         let last_tx = in_progress.last_tx_id();

    //         let mut login = LOGIN1.clone();

    //         login.password = "password2".into();
    //         login.modified = ::mentat::now() + chrono::Duration::seconds(1);
    //         apply_changed_login(&mut in_progress, login).expect("to apply 2");

    //         // let vs = queryable.q_once(q, None)?.into_edn()?;
    //         // assert_eq!("", format!("{:?}", in_progress.dump_datoms_after(last_tx).expect("datoms")));

    //         // let s = in_progress.dump_sql_query("SELECT e, a, v FROM datoms ORDER BY e, a, v, tx", &[]).expect("debug");
    //         let s = in_progress.dump_datoms_after(last_tx).expect("datoms").to_pretty(120).unwrap();
    //         println!("last_tx {}:\n{}", last_tx, s);

    //         // assert_eq!("", s);

    //         // in_progress.commit().expect("to commit")
    //     }

    //     // Scoped borrow of `store`.
    //     {
    //         let mut in_progress = store.begin_transaction().expect("begun successfully");
    //         let last_tx = in_progress.last_tx_id();

    //         let mut login = ServerPassword {
    //             modified: DateTime::<Utc>::from_micros(1523908142550),
    //             uuid: SyncGuid("{abcdabcd-fba1-594b-8148-ff70c85ee19a}".into()),
    //             hostname: "https://oauth-sync.dev.lcip.org".into(),
    //             target: FormTarget::FormSubmitURL("https://oauth-sync.dev.lcip.org/post".into()),
    //             username: Some("x@mockmyid.com".into()),
    //             password: "y".into(),
    //             username_field: Some("email".into()),
    //             password_field: None,
    //             time_created: DateTime::<Utc>::from_micros(1523908112453),
    //             time_password_changed: DateTime::<Utc>::from_micros(1523908112453),
    //             time_last_used: DateTime::<Utc>::from_micros(1000),
    //             times_used: 12,
    //         };
    //         login.modified = ::mentat::now();

    //         let mut credential = Credential {
    //             id: "a-credential-id".into(),
    //             username: login.username.clone(),
    //             password: login.password.clone(),
    //             created_at: login.time_created.clone(),
    //             title: None,
    //         };
    //         add_credential(&mut in_progress, credential).expect("to add credential");

    //         let id = find_credential_id_by_content(&in_progress,
    //                                                login.username.clone().unwrap(),
    //                                                login.password.clone()).expect("to find");
    //         assert_eq!(Some("a-credential-id".into()), id);

    //         apply_changed_login(&mut in_progress, login).expect("to apply 3");

    //         // let vs = queryable.q_once(q, None)?.into_edn()?;
    //         // assert_eq!("", format!("{:?}", in_progress.dump_datoms_after(last_tx).expect("datoms")));

    //         // let s = in_progress.dump_sql_query("SELECT e, a, v FROM datoms ORDER BY e, a, v, tx", &[]).expect("debug");
    //         let s = in_progress.dump_datoms_after(last_tx).expect("datoms").to_pretty(120).unwrap();
    //         println!("last_tx {}:\n{}", last_tx, s);

    //                                     // assert_eq!("", s);

    //         // in_progress.commit().expect("to commit")
    //     }

    //     // // Scoped borrow of `store`.
    //     // {
    //     //     let mut in_progress = store.begin_transaction().expect("begun successfully");
    //     //     let last_tx = in_progress.last_tx_id();

    //     //     assert_eq!(get_sync_password(&in_progress, SyncGuid("{c5144948-fba1-594b-8148-ff70c85ee19a}".into())).expect("to get_sync_password"),
    //     //                None);
    //     // }


    //     // // Scoped borrow of `store`.
    //     // {
    //     //     assert_eq!(5, times_used(store.begin_read().expect("to begin read"), "id1".to_string()).expect("to fetch local_times_used + remote_times_used"));
    //     // }

    //     //     let q = r#"[:find
    //     //             [?sl ?timesUsed ?tx]
    //     //             :in
    //     //             ?id
    //     //             :where
    //     //             [?c :credential/id ?id]
    //     //             [?sl :sync.password/credential ?c]
    //     //             [?sl :sync.password/timesUsed ?timesUsed]
    //     //             [?sl :sync.password/tx ?tx]
    //     //            ]"#;

    //     //     let mut qb = QueryBuilder::new(store, q);
    //     //     qb.bind_value("?id", id.clone());
    //     // }


    //     // let z = store.into_debug_conn();

    //     // assert_matches!(z.last_transaction(),
    //     //                 "[[100 :db.schema/version 1 ?tx true]
    //     //                   [101 :db.schema/version 2 ?tx true]]");
    // }


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

    //         // in_progress.transact_builder(builder).map_err(|e| e.into()).and(Ok(()))?;
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
    //     let t2 = ::mentat::now();
    //     let t1 = ::mentat::now();

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
