//! Canonical server Stream declaration types; all settlement remains runtime-owned.
use crate::emit::{arr, lower, s};
use serde_json::Value;
use std::fmt::Write;

pub(crate) fn backend(models: &[Value], o: &mut String) {
    writeln!(
        o,
        "export interface RecordDeclaration {{\n (records: RecordRef | readonly RecordRef[]): void;"
    )
    .unwrap();
    for m in models {
        let n = s(m, "name");
        let ids = arr(m, "identity");
        let operand = if ids.len() == 1 {
            format!("{n}Identity | {n}Identity[{}]", ids[0])
        } else {
            format!("{n}Identity")
        };
        writeln!(
            o,
            " {}(ids: {operand} | readonly ({operand})[]): void;",
            lower(n)
        )
        .unwrap();
    }
    o.push_str("}\n");
    o.push_str("export interface Stream { readonly track: RecordDeclaration; readonly invalidate: RecordDeclaration }\nexport interface LoadStream { readonly track: RecordDeclaration }\n");
}
