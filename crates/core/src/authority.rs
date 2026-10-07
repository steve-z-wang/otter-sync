//! Pure record authority evidence, independent of wire carriers and storage.
use crate::{Result, counter, invalid};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
fn nonblank(value: &str) -> Result<()> {
    if value.trim().is_empty() {
        Err(invalid("nonblank identifier required"))
    } else {
        Ok(())
    }
}
fn position(value: u64) -> Result<()> {
    counter(value)?;
    if value == 0 {
        Err(invalid("positive position required"))
    } else {
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityProtection {
    pub materialization: String,
    pub cursor: u64,
    pub deleted: bool,
}
/// Data-independent admission evidence. Persist this with the corresponding
/// base change; this helper deliberately owns neither rows nor pending overlays.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordEvidence {
    pub membership: Option<MembershipPosition>,
    pub history: BTreeMap<String, u64>,
    pub current: Option<AuthorityProtection>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipPosition {
    pub cursor: u64,
    pub live: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthorityAdmission {
    Duplicate,
    Newer,
    Rematerialize,
}
impl RecordEvidence {
    pub fn validate(&self) -> Result<()> {
        for (context, cursor) in &self.history {
            nonblank(context)?;
            position(*cursor)?;
        }
        if let Some(member) = &self.membership {
            position(member.cursor)?;
        }
        if let Some(current) = &self.current {
            nonblank(&current.materialization)?;
            position(current.cursor)?;
            if self.history.get(&current.materialization) != Some(&current.cursor)
                || self.history.values().any(|cursor| *cursor > current.cursor)
            {
                return Err(invalid(
                    "current protection lacks latest historical evidence",
                ));
            }
            if !current.deleted && self.membership.as_ref().is_some_and(|member| !member.live) {
                return Err(invalid("removed membership cannot protect live content"));
            }
        }
        Ok(())
    }
}
impl RecordEvidence {
    pub fn allows_cache(&self) -> bool {
        self.current.is_none()
    }
    /// Local changes clear content authority, never an existing Stream tombstone.
    pub fn direct_write(&mut self) {
        if self.current.as_ref().is_some_and(|value| !value.deleted) {
            self.current = None;
        }
    }
    /// The caller must first admit the active direct/Stream context. A new
    /// context can adapt the same base position, but never an older position.
    pub fn admission(&self, materialization: &str, cursor: u64) -> Result<AuthorityAdmission> {
        self.validate()?;
        nonblank(materialization)?;
        position(cursor)?;
        if self
            .history
            .get(materialization)
            .is_some_and(|old| cursor <= *old)
            || self
                .membership
                .as_ref()
                .is_some_and(|member| !member.live && cursor <= member.cursor)
        {
            return Ok(AuthorityAdmission::Duplicate);
        }
        let previous = self.history.values().copied().max().unwrap_or(0);
        Ok(if cursor < previous {
            AuthorityAdmission::Duplicate
        } else if cursor == previous {
            AuthorityAdmission::Rematerialize
        } else {
            AuthorityAdmission::Newer
        })
    }
    /// Installs evidence only. Rematerialize requires consumers to adapt base
    /// fields and replay surviving direct/pending operations in this commit.
    pub fn install(&mut self, materialization: &str, cursor: u64, deleted: bool) -> Result<bool> {
        let admission = self.admission(materialization, cursor)?;
        if admission == AuthorityAdmission::Duplicate {
            return Ok(false);
        }
        let preserve_direct =
            admission == AuthorityAdmission::Rematerialize && self.current.is_none();
        self.history.insert(materialization.into(), cursor);
        if !preserve_direct || deleted {
            self.current = Some(AuthorityProtection {
                materialization: materialization.into(),
                cursor,
                deleted,
            });
        }
        if self
            .membership
            .as_ref()
            .is_none_or(|member| member.cursor < cursor)
        {
            self.membership = Some(MembershipPosition { cursor, live: true });
        }
        Ok(true)
    }
    /// Remove releases live content protection, preserving content, historical
    /// authority and real absence protection. Old removes cannot undo re-track.
    pub fn remove(&mut self, cursor: u64) -> Result<bool> {
        self.validate()?;
        position(cursor)?;
        let old = self
            .membership
            .as_ref()
            .map_or(0, |member| member.cursor)
            .max(self.history.values().copied().max().unwrap_or(0));
        if cursor <= old {
            return Ok(false);
        }
        self.membership = Some(MembershipPosition {
            cursor,
            live: false,
        });
        if self.current.as_ref().is_some_and(|value| !value.deleted) {
            self.current = None;
        }
        Ok(true)
    }
    pub fn covers(&self, materialization: &str, cursor: u64) -> bool {
        self.history
            .get(materialization)
            .is_some_and(|held| *held >= cursor)
    }
}
