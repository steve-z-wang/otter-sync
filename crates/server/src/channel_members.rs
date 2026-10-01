//! The final membership of each Channel/record pair after one settlement's
//! ordered declarations ([Publish](../../../docs/engineering/architecture/server/engine/publish.md)).
//!
//! Pure: settlement reads the members its declarations and touches can reach
//! (`readChannelMembers`), [`reduce`] folds the declarations over that
//! transaction-local view in order, and the [`MemberDelta`]s it answers are
//! what `applyChannelMembers` persists. The wire shapes of the three types
//! belong to the host contract ([`crate::host`]).
use crate::scope_predicate::ScopePredicate;
use crate::{Result, internal};
use axton_core::RecordKey;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// The most UTF-8 bytes one tag may take.
pub const TAG_BYTES: usize = 256;
/// The most distinct tags one add may declare.
pub const TAGS_PER_ADD: usize = 64;

/// Whether `tag` is blank. The TypeScript collector refuses a tag that JS
/// `trim()` empties; Rust's `char::is_whitespace` differs from it (it adds
/// U+0085 and lacks U+FEFF), so the engine refuses the union of both. Nothing
/// the collector refuses gets through, and a tag only Rust calls blank is
/// refused with its declaration, never dropped.
fn blank(tag: &str) -> bool {
    tag.chars().all(|c| c.is_whitespace() || c == '\u{feff}')
}

/// A tag's spelling rule: not blank and at most [`TAG_BYTES`] UTF-8 bytes. An
/// accepted tag is kept exactly as spelled.
pub fn check_tag(tag: &str) -> std::result::Result<(), String> {
    if blank(tag) {
        return Err(format!("tag {tag:?} must not be blank"));
    }
    if tag.len() > TAG_BYTES {
        return Err(format!(
            "a tag must be at most {TAG_BYTES} UTF-8 bytes; one has {}",
            tag.len()
        ));
    }
    Ok(())
}

/// One add's tags as a set: each passes [`check_tag`], and there are at most
/// [`TAGS_PER_ADD`] distinct ones.
pub fn declared_tags(tags: &[String]) -> std::result::Result<BTreeSet<String>, String> {
    let mut distinct = BTreeSet::new();
    for tag in tags {
        check_tag(tag)?;
        distinct.insert(tag.clone());
    }
    if distinct.len() > TAGS_PER_ADD {
        return Err(format!(
            "an add declares {} distinct tags; at most {TAGS_PER_ADD} are allowed",
            distinct.len()
        ));
    }
    Ok(distinct)
}

/// One live member as `readChannelMembers` answers it: the record and its
/// complete current tags.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    try_from = "crate::host::MemberStateWire",
    into = "crate::host::MemberStateWire"
)]
pub struct MemberState {
    pub key: RecordKey,
    pub tags: BTreeSet<String>,
}

/// One pair's final state, as `applyChannelMembers` persists it. `present`
/// with its complete final `tags`, or absent with none. `publish` allocates a
/// new position (`upsert` when present, `remove` when not); without it a
/// present member keeps its existing position and only its tags may change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    try_from = "crate::host::MemberDeltaWire",
    into = "crate::host::MemberDeltaWire"
)]
pub struct MemberDelta {
    pub channel: String,
    pub key: RecordKey,
    pub present: bool,
    pub tags: BTreeSet<String>,
    pub publish: bool,
}

/// What a position says about its pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PositionKind {
    Upsert,
    Remove,
}

/// The latest position of one pair after `applyChannelMembers`: new for a
/// published delta, the existing one otherwise.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    try_from = "crate::host::MemberPositionWire",
    into = "crate::host::MemberPositionWire"
)]
pub struct MemberPosition {
    pub channel: String,
    pub key: RecordKey,
    pub cursor: u64,
    pub kind: PositionKind,
}

/// One declaration against one Channel, its record resolved to a canonical key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Declaration {
    /// Ensure membership and union `tags` into its labels.
    Add {
        key: RecordKey,
        tags: BTreeSet<String>,
    },
    /// Release the record's whole membership.
    Remove {
        key: RecordKey,
    },
    /// Release every member carrying `tag` as the preceding declarations left it.
    RemoveTag {
        tag: String,
    },
    TagAdd {
        key: RecordKey,
        tags: BTreeSet<String>,
    },
    TagRemove {
        key: RecordKey,
        tags: BTreeSet<String>,
    },
    DetachTags {
        tags: BTreeSet<String>,
    },
    Select {
        model: Option<String>,
        predicate: ScopePredicate,
        action: SelectionAction,
    },
}

impl Declaration {
    /// The record an add or remove names; a selector names none.
    pub fn key(&self) -> Option<&RecordKey> {
        match self {
            Self::Add { key, .. }
            | Self::Remove { key }
            | Self::TagAdd { key, .. }
            | Self::TagRemove { key, .. } => Some(key),
            Self::RemoveTag { .. } | Self::DetachTags { .. } | Self::Select { .. } => None,
        }
    }
}

/// An action applies to a selection's current members.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum SelectionAction {
    Remove,
    TagAdd { tags: BTreeSet<String> },
    TagRemove { tags: BTreeSet<String> },
}

/// One pair's life inside the settlement.
struct Pair {
    key: RecordKey,
    initial: Option<BTreeSet<String>>,
    current: Option<BTreeSet<String>>,
    /// An initially present membership was released at some point, so a
    /// final membership is a new one even when its tags match.
    released: bool,
    /// An add named the pair: an unchanged member still answers its position.
    added: bool,
}

impl Pair {
    fn release(&mut self) {
        if self.current.take().is_some() && self.initial.is_some() {
            self.released = true;
        }
    }
}

/// Reduce `channel`'s declarations, in order, over the members read for it,
/// to one delta per pair that changes, is touched while present, or is named
/// by an add, in canonical record key order.
///
/// `initial` must hold every member a declaration names or a selector
/// matches, with complete tags; `touched` holds the canonical keys of the
/// records this settlement changed. A pair absent at both ends yields
/// nothing. Absent to present, or present to absent, publishes. Present at
/// both ends publishes when the member was released and re-added or its
/// record is touched; otherwise the delta only carries its final tags.
pub fn reduce(
    channel: &str,
    initial: Vec<MemberState>,
    declarations: &[Declaration],
    touched: &BTreeSet<String>,
) -> Result<Vec<MemberDelta>> {
    let mut pairs: BTreeMap<String, Pair> = BTreeMap::new();
    for member in initial {
        pairs.insert(
            member.key.encoded().map_err(internal)?,
            Pair {
                key: member.key,
                initial: Some(member.tags.clone()),
                current: Some(member.tags),
                released: false,
                added: false,
            },
        );
    }
    for declaration in declarations {
        match declaration {
            Declaration::Add { key, tags } => {
                let pair = pairs
                    .entry(key.encoded().map_err(internal)?)
                    .or_insert_with(|| Pair {
                        key: key.clone(),
                        initial: None,
                        current: None,
                        released: false,
                        added: false,
                    });
                pair.current
                    .get_or_insert_with(BTreeSet::new)
                    .extend(tags.iter().cloned());
                pair.added = true;
            }
            Declaration::Remove { key } => {
                if let Some(pair) = pairs.get_mut(&key.encoded().map_err(internal)?) {
                    pair.release();
                }
            }
            Declaration::TagAdd { key, tags } => {
                let pair = pairs
                    .get_mut(&key.encoded().map_err(internal)?)
                    .filter(|pair| pair.current.is_some())
                    .ok_or_else(|| {
                        crate::settlement::invalid_tags(
                            channel,
                            format!(
                                "cannot add labels to absent member {} {}",
                                key.model, key.identity
                            ),
                        )
                    })?;
                pair.current.as_mut().unwrap().extend(tags.iter().cloned());
            }
            Declaration::TagRemove { key, tags } => {
                if let Some(current) = pairs
                    .get_mut(&key.encoded().map_err(internal)?)
                    .and_then(|pair| pair.current.as_mut())
                {
                    current.retain(|tag| !tags.contains(tag));
                }
            }
            Declaration::DetachTags { tags } => {
                for pair in pairs.values_mut() {
                    if let Some(current) = &mut pair.current {
                        current.retain(|tag| !tags.contains(tag));
                    }
                }
            }
            Declaration::Select {
                model,
                predicate,
                action,
            } => {
                for pair in pairs.values_mut() {
                    if model.as_ref().is_none_or(|model| *model == pair.key.model)
                        && pair
                            .current
                            .as_ref()
                            .is_some_and(|tags| predicate.matches(tags))
                    {
                        match action {
                            SelectionAction::Remove => pair.release(),
                            SelectionAction::TagAdd { tags } => {
                                pair.current.as_mut().unwrap().extend(tags.iter().cloned())
                            }
                            SelectionAction::TagRemove { tags } => pair
                                .current
                                .as_mut()
                                .unwrap()
                                .retain(|tag| !tags.contains(tag)),
                        }
                    }
                }
            }
            Declaration::RemoveTag { tag } => {
                for pair in pairs.values_mut() {
                    if pair.current.as_ref().is_some_and(|tags| tags.contains(tag)) {
                        pair.release();
                    }
                }
            }
        }
    }
    let mut deltas = vec![];
    for (encoded, pair) in pairs {
        let (present, tags, publish) = match (&pair.initial, pair.current) {
            (None, None) => continue,
            (Some(_), None) => (false, BTreeSet::new(), true),
            (None, Some(tags)) => (true, tags, true),
            (Some(initial), Some(tags)) => {
                let publish = pair.released || touched.contains(&encoded);
                if !publish && !pair.added && *initial == tags {
                    continue;
                }
                (true, tags, publish)
            }
        };
        deltas.push(MemberDelta {
            channel: channel.into(),
            key: pair.key,
            present,
            tags,
            publish,
        });
    }
    Ok(deltas)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn key(id: &str) -> RecordKey {
        RecordKey {
            model: "Todo".into(),
            identity: json!({ "id": id }),
        }
    }
    fn tags(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|name| name.to_string()).collect()
    }
    fn member(id: &str, names: &[&str]) -> MemberState {
        MemberState {
            key: key(id),
            tags: tags(names),
        }
    }
    fn add(id: &str, names: &[&str]) -> Declaration {
        Declaration::Add {
            key: key(id),
            tags: tags(names),
        }
    }
    fn remove(id: &str) -> Declaration {
        Declaration::Remove { key: key(id) }
    }
    fn remove_tag(tag: &str) -> Declaration {
        Declaration::RemoveTag { tag: tag.into() }
    }
    fn delta(id: &str, present: bool, names: &[&str], publish: bool) -> MemberDelta {
        MemberDelta {
            channel: "U".into(),
            key: key(id),
            present,
            tags: tags(names),
            publish,
        }
    }
    fn touched(ids: &[&str]) -> BTreeSet<String> {
        ids.iter().map(|id| key(id).encoded().unwrap()).collect()
    }

    /// A label, the initial members, the declarations, the touched records and
    /// the exact deltas.
    type Case = (
        &'static str,
        Vec<MemberState>,
        Vec<Declaration>,
        &'static [&'static str],
        Vec<MemberDelta>,
    );

    /// The plan's six cases: initial members, declarations (and touches), and
    /// the exact deltas; "no event" is a delta without `publish`, or none.
    #[test]
    fn the_ordered_declaration_table_reduces_to_its_final_deltas() {
        let cases: [Case; 6] = [
            (
                "{} ; add A/X, removeTag X => {} ; no event",
                vec![],
                vec![add("A", &["X"]), remove_tag("X")],
                &[],
                vec![],
            ),
            (
                "A:{X,Y} ; removeTag X => {} ; remove A",
                vec![member("A", &["X", "Y"])],
                vec![remove_tag("X")],
                &[],
                vec![delta("A", false, &[], true)],
            ),
            (
                "A:{X} ; add A/Y => A:{X,Y} ; no event",
                vec![member("A", &["X"])],
                vec![add("A", &["Y"])],
                &[],
                vec![delta("A", true, &["X", "Y"], false)],
            ),
            (
                "A:{X} ; remove A, add A/Y => A:{Y} ; upsert A",
                vec![member("A", &["X"])],
                vec![remove("A"), add("A", &["Y"])],
                &[],
                vec![delta("A", true, &["Y"], true)],
            ),
            (
                "{} ; removeTag X, add A/X => A:{X} ; upsert A",
                vec![],
                vec![remove_tag("X"), add("A", &["X"])],
                &[],
                vec![delta("A", true, &["X"], true)],
            ),
            (
                "A:{X},B:{Y} ; removeTag X, touch B => B:{Y} ; remove A, upsert B",
                vec![member("A", &["X"]), member("B", &["Y"])],
                vec![remove_tag("X")],
                &["B"],
                vec![delta("A", false, &[], true), delta("B", true, &["Y"], true)],
            ),
        ];
        for (label, initial, declarations, touches, expected) in cases {
            let deltas = reduce("U", initial, &declarations, &touched(touches)).unwrap();
            assert_eq!(deltas, expected, "{label}");
        }
    }

    /// Removing an absent record or a tag nobody carries changes nothing; a
    /// member neither named, selected nor touched yields no delta.
    #[test]
    fn declarations_that_reach_no_member_yield_nothing() {
        let initial = vec![member("A", &["X"]), member("C", &[])];
        let deltas = reduce(
            "U",
            initial,
            &[remove("B"), remove_tag("Z"), add("B", &[]), remove("B")],
            &BTreeSet::new(),
        )
        .unwrap();
        assert!(deltas.is_empty(), "{deltas:?}");
    }

    /// A removed membership discards its tags: a re-add starts from its own.
    /// A selector sees an add's union; an untagged add keeps existing tags.
    #[test]
    fn a_removal_discards_tags_and_a_selector_sees_the_union_so_far() {
        let deltas = reduce(
            "U",
            vec![member("A", &["X", "Y"]), member("B", &["Y"])],
            &[
                remove("A"),
                add("A", &["Z"]),
                add("B", &[]),
                add("B", &["W"]),
                remove_tag("Y"),
                add("C", &["Y"]),
            ],
            &BTreeSet::new(),
        )
        .unwrap();
        assert_eq!(
            deltas,
            [
                delta("A", true, &["Z"], true),
                delta("B", false, &[], true),
                delta("C", true, &["Y"], true),
            ]
        );
    }

    /// A touched member publishes once however it is also declared; a
    /// touched record that leaves the Channel publishes its removal instead;
    /// a touched non-member of this Channel is not in it.
    #[test]
    fn a_touch_publishes_each_final_member_once() {
        let deltas = reduce(
            "U",
            vec![member("A", &["X"]), member("B", &[])],
            &[
                add("A", &["Y"]),
                add("A", &["X"]),
                remove("B"),
                add("C", &[]),
            ],
            &touched(&["A", "B", "C", "D"]),
        )
        .unwrap();
        assert_eq!(
            deltas,
            [
                delta("A", true, &["X", "Y"], true),
                delta("B", false, &[], true),
                delta("C", true, &[], true),
            ]
        );
    }

    #[test]
    fn tags_are_nonblank_bounded_and_counted_distinct() {
        assert!(check_tag("Journal:1").is_ok());
        assert!(check_tag(" padded ").is_ok(), "kept as spelled");
        assert!(check_tag(&"é".repeat(128)).is_ok(), "256 bytes");
        assert!(check_tag(&"é".repeat(128).replacen('é', "ée", 1)).is_err());
        for blank in ["", " ", "\t\n", "\u{feff}", "\u{85}", "\u{3000}\u{2028}"] {
            assert!(check_tag(blank).is_err(), "{blank:?}");
        }
        let many: Vec<String> = (0..64).map(|n| format!("t{n}")).collect();
        assert_eq!(declared_tags(&many).unwrap().len(), 64);
        let mut repeated = many.clone();
        repeated.push("t0".into());
        assert_eq!(declared_tags(&repeated).unwrap().len(), 64, "distinct");
        let mut past = many;
        past.push("t64".into());
        assert!(declared_tags(&past).unwrap_err().contains("64"));
    }
    #[test]
    fn exact_selection_and_detachment_preserve_overlapping_members() {
        let predicate = serde_json::from_value(json!({"tags":{"only":["X"]}})).unwrap();
        let declarations = vec![
            Declaration::Select {
                model: None,
                predicate,
                action: SelectionAction::Remove,
            },
            Declaration::DetachTags { tags: tags(&["X"]) },
        ];
        assert_eq!(
            reduce(
                "U",
                vec![
                    member("A", &["X", "Y"]),
                    member("B", &["X"]),
                    member("C", &["Y"]),
                    member("D", &["X", "Z"])
                ],
                &declarations,
                &BTreeSet::new()
            )
            .unwrap(),
            vec![
                delta("A", true, &["Y"], false),
                delta("B", false, &[], true),
                delta("D", true, &["Z"], false)
            ]
        );
    }
    #[test]
    fn label_edits_require_membership_and_never_remove_it() {
        let add = Declaration::TagAdd {
            key: key("A"),
            tags: tags(&["X"]),
        };
        assert_eq!(
            reduce("U", vec![], &[add.clone()], &BTreeSet::new())
                .unwrap_err()
                .code,
            crate::code::HANDLER_INVALID
        );
        assert_eq!(
            reduce(
                "U",
                vec![member("A", &["X"])],
                &[
                    add,
                    Declaration::TagRemove {
                        key: key("A"),
                        tags: tags(&["X"])
                    }
                ],
                &BTreeSet::new()
            )
            .unwrap(),
            vec![delta("A", true, &[], false)]
        );
        assert!(
            reduce(
                "U",
                vec![member("A", &["X"])],
                &[Declaration::TagRemove {
                    key: key("A"),
                    tags: tags(&["Z"])
                }],
                &BTreeSet::new()
            )
            .unwrap()
            .is_empty()
        );
        assert!(
            reduce(
                "U",
                vec![member("A", &["X"])],
                &[
                    remove("A"),
                    Declaration::TagAdd {
                        key: key("A"),
                        tags: tags(&["Y"])
                    }
                ],
                &BTreeSet::new()
            )
            .is_err()
        );
    }
    #[test]
    fn selection_uses_current_labels_and_model() {
        let select = Declaration::Select {
            model: Some("Other".into()),
            predicate: serde_json::from_value(json!({"tags":{"only":[]}})).unwrap(),
            action: SelectionAction::Remove,
        };
        assert!(
            reduce("U", vec![member("A", &[])], &[select], &BTreeSet::new())
                .unwrap()
                .is_empty()
        );
        let select = Declaration::Select {
            model: None,
            predicate: serde_json::from_value(json!({"tags":{"all":["Y"]}})).unwrap(),
            action: SelectionAction::TagRemove {
                tags: tags(&["X", "Y"]),
            },
        };
        assert_eq!(
            reduce(
                "U",
                vec![member("A", &["X"])],
                &[
                    Declaration::TagAdd {
                        key: key("A"),
                        tags: tags(&["Y"])
                    },
                    select
                ],
                &BTreeSet::new()
            )
            .unwrap(),
            vec![delta("A", true, &[], false)]
        );
    }
}
