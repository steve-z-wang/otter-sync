//! Bounded membership-label predicates. No business fields or client state.
use crate::scope_members::declared_tags;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

pub const PREDICATE_DEPTH: usize = 16;
pub const PREDICATE_NODES: usize = 128;
pub const PREDICATE_BYTES: usize = 65_536;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ScopePredicate {
    #[serde(skip_serializing_if = "Option::is_none")]
    tags: Option<TagPredicate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    and: Option<Vec<ScopePredicate>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    or: Option<Vec<ScopePredicate>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    not: Option<Box<ScopePredicate>>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct TagPredicate {
    #[serde(skip_serializing_if = "Option::is_none")]
    all: Option<BTreeSet<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    any: Option<BTreeSet<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    none: Option<BTreeSet<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    only: Option<BTreeSet<String>>,
}
impl<'de> Deserialize<'de> for ScopePredicate {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        if serde_json::to_vec(&value)
            .map_err(serde::de::Error::custom)?
            .len()
            > PREDICATE_BYTES
        {
            return Err(serde::de::Error::custom(
                "predicate exceeds 65536 UTF-8 JSON bytes",
            ));
        }
        Self::parse(&value, 1, &mut 0).map_err(serde::de::Error::custom)
    }
}
impl ScopePredicate {
    fn parse(value: &Value, depth: usize, nodes: &mut usize) -> Result<Self, String> {
        *nodes += 1;
        if depth > PREDICATE_DEPTH || *nodes > PREDICATE_NODES {
            return Err("predicate exceeds depth 16 or 128 nodes".into());
        }
        let object = value.as_object().ok_or("predicate must be an object")?;
        if object.is_empty()
            || object
                .keys()
                .any(|key| !["tags", "and", "or", "not"].contains(&key.as_str()))
        {
            return Err("predicate is empty or has unknown keys".into());
        }
        let tags = object
            .get("tags")
            .map(|value| -> Result<TagPredicate, String> {
                let leaf = value.as_object().ok_or("tags must be an object")?;
                if leaf.is_empty()
                    || leaf
                        .keys()
                        .any(|key| !["all", "any", "none", "only"].contains(&key.as_str()))
                {
                    return Err("tags is empty or has unknown keys".into());
                }
                let labels = |key: &str| -> Result<Option<BTreeSet<String>>, String> {
                    leaf.get(key)
                        .map(|value| {
                            let labels: Vec<String> = serde_json::from_value(value.clone())
                                .map_err(|_| format!("tags.{key} must be a string array"))?;
                            if labels.is_empty() && key != "only" {
                                return Err(format!("tags.{key} must not be empty"));
                            }
                            declared_tags(&labels)
                        })
                        .transpose()
                };
                Ok(TagPredicate {
                    all: labels("all")?,
                    any: labels("any")?,
                    none: labels("none")?,
                    only: labels("only")?,
                })
            })
            .transpose()?;
        let mut group = |key: &str| -> Result<Option<Vec<Self>>, String> {
            object
                .get(key)
                .map(|value| {
                    let children = value
                        .as_array()
                        .ok_or_else(|| format!("{key} must be an array"))?;
                    if children.is_empty() {
                        return Err(format!("{key} must not be empty"));
                    }
                    children
                        .iter()
                        .map(|child| Self::parse(child, depth + 1, nodes))
                        .collect()
                })
                .transpose()
        };
        let and = group("and")?;
        let or = group("or")?;
        let not = object
            .get("not")
            .map(|child| Self::parse(child, depth + 1, nodes).map(Box::new))
            .transpose()?;
        Ok(Self { tags, and, or, not })
    }
    pub fn matches(&self, tags: &BTreeSet<String>) -> bool {
        self.tags.as_ref().is_none_or(|leaf| {
            leaf.all
                .as_ref()
                .is_none_or(|wanted| wanted.is_subset(tags))
                && leaf
                    .any
                    .as_ref()
                    .is_none_or(|wanted| !wanted.is_disjoint(tags))
                && leaf
                    .none
                    .as_ref()
                    .is_none_or(|wanted| wanted.is_disjoint(tags))
                && leaf.only.as_ref().is_none_or(|wanted| wanted == tags)
        }) && self
            .and
            .as_ref()
            .is_none_or(|children| children.iter().all(|child| child.matches(tags)))
            && self
                .or
                .as_ref()
                .is_none_or(|children| children.iter().any(|child| child.matches(tags)))
            && self.not.as_ref().is_none_or(|child| !child.matches(tags))
    }
    /// Labels whose union bounds positive selection candidates.
    pub fn labels(&self) -> BTreeSet<String> {
        let mut labels = BTreeSet::new();
        if let Some(leaf) = &self.tags {
            for set in [&leaf.all, &leaf.any, &leaf.none, &leaf.only]
                .into_iter()
                .flatten()
            {
                labels.extend(set.iter().cloned());
            }
        }
        for child in self
            .and
            .iter()
            .chain(self.or.iter())
            .flatten()
            .chain(self.not.iter().map(Box::as_ref))
        {
            labels.extend(child.labels());
        }
        labels
    }
    /// Negative conditions conservatively require the complete current Scope.
    pub fn requires_all(&self) -> bool {
        self.not.is_some()
            || self.tags.as_ref().is_some_and(|leaf| {
                leaf.none.is_some() || leaf.only.as_ref().is_some_and(BTreeSet::is_empty)
            })
            || self
                .and
                .iter()
                .chain(self.or.iter())
                .flatten()
                .any(Self::requires_all)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn predicate(value: Value) -> ScopePredicate {
        serde_json::from_value(value).unwrap()
    }
    #[test]
    fn siblings_and_boolean_groups_match_complete_sets() {
        let x = BTreeSet::from(["X".into()]);
        let xy = BTreeSet::from(["X".into(), "Y".into()]);
        for (value, yes, no) in [
            (json!({"tags":{"all":["X"],"none":["Y"]}}), &x, &xy),
            (json!({"tags":{"only":["X", "X"]}}), &x, &xy),
            (
                json!({"and":[{"tags":{"any":["X", "Z"]}},{"not":{"tags":{"all":["Y"]}}}]}),
                &x,
                &xy,
            ),
            (
                json!({"or":[{"tags":{"all":["Y"]}},{"tags":{"only":[]}}]}),
                &xy,
                &x,
            ),
        ] {
            let p = predicate(value);
            assert!(p.matches(yes));
            assert!(!p.matches(no));
        }
        assert!(predicate(json!({"tags":{"only":[]}})).matches(&BTreeSet::new()));
    }
    #[test]
    fn candidate_reads_are_conservative_for_negative_predicates() {
        for value in [
            json!({"tags":{"none":["X"]}}),
            json!({"not":{"tags":{"all":["X"]}}}),
            json!({"tags":{"only":[]}}),
            json!({"and":[{"tags":{"all":["Y"]}},{"tags":{"none":["X"]}}]}),
        ] {
            assert!(predicate(value).requires_all());
        }
        let p = predicate(json!({"or":[{"tags":{"all":["X"]}},{"tags":{"only":["Y"]}}]}));
        assert!(!p.requires_all());
        assert_eq!(p.labels(), BTreeSet::from(["X".into(), "Y".into()]));
    }
    #[test]
    fn strict_shapes_and_all_limits_are_enforced() {
        for value in [
            json!({}),
            json!([]),
            json!({"unknown":true}),
            json!({"tags":{}}),
            json!({"tags":{"what":["X"]}}),
            json!({"tags":{"all":[]}}),
            json!({"tags":{"any":[]}}),
            json!({"tags":{"none":[]}}),
            json!({"tags":{"only":null}}),
            json!({"tags":{"all":[1]}}),
            json!({"and":[]}),
            json!({"or":[]}),
            json!({"not":null}),
            json!({"tags":{"all":[" "]}}),
            json!({"tags":{"all":["é".repeat(129)]}}),
        ] {
            assert!(
                serde_json::from_value::<ScopePredicate>(value.clone()).is_err(),
                "{value}"
            );
        }
        let leaf = json!({"tags":{"all":["X"]}});
        let mut deep = leaf.clone();
        for _ in 1..16 {
            deep = json!({"not": deep});
        }
        assert!(serde_json::from_value::<ScopePredicate>(deep.clone()).is_ok());
        assert!(serde_json::from_value::<ScopePredicate>(json!({"not":deep})).is_err());
        assert!(
            serde_json::from_value::<ScopePredicate>(json!({"and":vec![leaf.clone();127]})).is_ok()
        );
        assert!(serde_json::from_value::<ScopePredicate>(json!({"and":vec![leaf;128]})).is_err());
        let labels: Vec<String> = (0..64).map(|i| format!("{i}")).collect();
        assert!(serde_json::from_value::<ScopePredicate>(json!({"tags":{"all":labels}})).is_ok());
        assert!(
            serde_json::from_value::<ScopePredicate>(
                json!({"tags":{"all":(0..65).map(|i| i.to_string()).collect::<Vec<_>>()}})
            )
            .is_err()
        );
        assert!(
            serde_json::from_value::<ScopePredicate>(json!({"tags":{"all":vec!["X";17000]}}))
                .is_err()
        );
        let mut labels = vec!["X".to_string(); 16379];
        labels[0] = "é".into();
        let boundary = json!({"tags":{"only":labels}});
        assert_eq!(
            serde_json::to_vec(&boundary).unwrap().len(),
            PREDICATE_BYTES
        );
        assert!(serde_json::from_value::<ScopePredicate>(boundary).is_ok());
        labels[0] = "éX".into();
        let over = json!({"tags":{"only":labels}});
        assert_eq!(
            serde_json::to_vec(&over).unwrap().len(),
            PREDICATE_BYTES + 1
        );
        assert!(serde_json::from_value::<ScopePredicate>(over).is_err());
    }
}
