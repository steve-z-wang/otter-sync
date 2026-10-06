//! Select the public operation kind from each name's latest retained version.
use crate::action_names::kind;
use crate::emit::{arr, s};
use axton_core::CallKind;
use serde_json::Value;
use std::collections::BTreeMap;

pub(crate) struct CurrentOperations<'a>(BTreeMap<&'a str, &'a Value>);

impl<'a> CurrentOperations<'a> {
    pub(crate) fn new(config: &'a Value) -> Self {
        let mut latest = BTreeMap::<&str, &Value>::new();
        for action in arr(config, "actions") {
            let version = action["version"].as_u64().unwrap();
            latest
                .entry(s(action, "name"))
                .and_modify(|current| {
                    if version > current["version"].as_u64().unwrap() {
                        *current = action;
                    }
                })
                .or_insert(action);
        }
        Self(latest)
    }

    pub(crate) fn version(&self, name: &str) -> u64 {
        self.0[name]["version"].as_u64().unwrap()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&'a str, u64, &'a Value)> + '_ {
        self.0
            .iter()
            .map(|(name, action)| (*name, action["version"].as_u64().unwrap(), *action))
    }

    pub(crate) fn of_kind(
        &self,
        expected: CallKind,
    ) -> impl Iterator<Item = (&'a str, u64, &'a Value)> + '_ {
        self.iter()
            .filter(move |(_, _, action)| kind(action) == expected)
    }

    pub(crate) fn has_mutations(&self) -> bool {
        self.of_kind(CallKind::Mutation).next().is_some()
    }
}
