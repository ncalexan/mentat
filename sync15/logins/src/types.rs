// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

///! This module defines some core types that support Sync 1.5 passwords and arbitrary logins.
///!
///! We use "passwords" or "password records" to talk about Sync 1.5's object format stored in the
///! "passwords" collection.  We use "logins" to talk about local credentials, which might be more
///! general than Sync 1.5's limited object format.
///!
///! Throughout, we reference the somewhat out-dated but still useful client documentation at
///! https://mozilla-services.readthedocs.io/en/latest/sync/objectformats.html#passwords

use std::convert::{
    AsRef,
};

use chrono::{
    DateTime,
    Utc,
};

use serde::{
    Deserializer,
    Serializer,
};

use uuid::{
    Uuid,
};

use edn::{
    FromMillis,
    ToMillis,
};

/// Firefox Sync password records must have at least a formSubmitURL or httpRealm, but not both.
#[derive(PartialEq, Eq, Hash, Clone, Debug, Serialize, Deserialize)]
pub enum FormTarget {
    #[serde(rename = "httpRealm")]
    HttpRealm(String),

    #[serde(rename = "formSubmitURL")]
    FormSubmitURL(String),
}

#[derive(PartialEq, Eq, Hash, Clone, Debug, Serialize, Deserialize)]
pub struct SyncGuid(pub(crate) String);

impl AsRef<str> for SyncGuid {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl<T> From<T> for SyncGuid where T: Into<String> {
    fn from(x: T) -> SyncGuid {
        SyncGuid(x.into())
    }
}

fn zero_timestamp() -> DateTime<Utc> {
    DateTime::<Utc>::from_millis(0)
}

/// A Sync 1.5 password record.
#[derive(PartialEq, Eq, Hash, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerPassword {
    /// The UUID of this record, returned by the remote server as part of this record's envelope.
    ///
    /// For historical reasons, Sync 1.5 passwords use a UUID rather than a (9 character) GUID like
    /// other collections.
    #[serde(rename = "id")]
    pub uuid: SyncGuid,

    /// The time last modified, returned by the remote server as part of this record's envelope.
    #[serde(skip_serializing, default = "zero_timestamp")]
    pub modified: DateTime<Utc>,

    /// Material fields.  A password without a username corresponds to an XXX.
    pub hostname: String,
    pub username: Option<String>,
    pub password: String,

    #[serde(flatten)]
    pub target: FormTarget,

    /// Metadata.  Unfortunately, not all clients pass-through (let alone collect and propagate!)
    /// metadata correctly.
    #[serde(default)]
    pub times_used: usize,

    #[serde(serialize_with = "ServerPassword::serialize_timestamp",
            deserialize_with = "ServerPassword::deserialize_timestamp",
            default = "zero_timestamp")]
    pub time_created: DateTime<Utc>,

    #[serde(serialize_with = "ServerPassword::serialize_timestamp",
            deserialize_with = "ServerPassword::deserialize_timestamp",
            default = "zero_timestamp")]
    pub time_last_used: DateTime<Utc>,

    #[serde(serialize_with = "ServerPassword::serialize_timestamp",
            deserialize_with = "ServerPassword::deserialize_timestamp",
            default = "zero_timestamp")]
    pub time_password_changed: DateTime<Utc>,

    /// Mostly deprecated: these fields were once used to help with form fill.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username_field: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub password_field: Option<String>,
}

impl ServerPassword {
    fn serialize_timestamp<S>(x: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error> where S: Serializer {
        s.serialize_u64(x.to_millis() as u64)
    }

    fn deserialize_timestamp<'de, D>(d: D) -> Result<DateTime<Utc>, D::Error> where D: Deserializer<'de> {
        struct Visitor;

        impl<'de> ::serde::de::Visitor<'de> for Visitor {
            type Value = DateTime<Utc>;

            fn expecting(&self, formatter: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
                formatter.write_str("timestamp in millis")
            }

            fn visit_u64<E>(self, value: u64) -> Result<DateTime<Utc>, E> where E: ::serde::de::Error
            {
                Ok(DateTime::<Utc>::from_millis(value as i64))
            }
        }

        d.deserialize_u64(Visitor)
    }
}

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct CredentialId(pub(crate) String);

impl AsRef<str> for CredentialId {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl CredentialId {
    pub fn random() -> Self {
        CredentialId(Uuid::new_v4().hyphenated().to_string())
    }
}

impl<T> From<T> for CredentialId where T: Into<String> {
    fn from(x: T) -> CredentialId {
        CredentialId(x.into())
    }
}

/// A Sync.next credential.
///
/// A credential is a username/password pair, optionally decorated with a user-specified
/// title.
///
/// A credential is uniquely identified by its `id`.
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct Credential {
    /// A stable opaque identifier uniquely naming this credential.
    pub id: CredentialId,

    // The username associated to this credential.
    pub username: Option<String>,

    // The password associated to this credential.
    pub password: String,

    // When the credential was created.  This is best-effort: it's the timestamp observed by the
    // device on which the credential was created, which is incomparable with timestamps observed by
    // other devices in the constellation (including any servers).
    pub created_at: DateTime<Utc>,

    /// An optional user-specified title of this credential, like `My LDAP`.
    pub title: Option<String>,
}
