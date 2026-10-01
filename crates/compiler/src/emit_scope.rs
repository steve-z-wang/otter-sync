//! Canonical server Scope declaration types; all settlement remains runtime-owned.
use crate::emit::{arr, lower, s};
use serde_json::Value;
use std::fmt::Write;

pub(crate) fn backend(models: &[Value], o: &mut String) {
    o.push_str("export interface AddDeclaration { tag(labels: string | readonly string[]): AddDeclaration }\nexport type ScopePredicate = { readonly tags?: { readonly all?: readonly string[]; readonly any?: readonly string[]; readonly none?: readonly string[]; readonly only?: readonly string[] }; readonly and?: readonly ScopePredicate[]; readonly or?: readonly ScopePredicate[]; readonly not?: ScopePredicate };\nexport interface ScopeSelection { remove(): void; tag(labels: string | readonly string[]): { add(): void; remove(): void } }\n");
    for (name, result) in [
        ("ScopeAdd", "AddDeclaration"),
        ("ScopeRecords", "void"),
        ("Touch", "void"),
    ] {
        writeln!(
            o,
            "export interface {name} {{\n (records: RecordRef | readonly RecordRef[]): {result};"
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
                " {}(ids: {operand} | readonly ({operand})[]): {result};",
                lower(n)
            )
            .unwrap();
        }
        o.push_str("}\n");
    }
    o.push_str("export interface ScopeTagRemoval extends ScopeRecords { (): void }\nexport interface ScopeWhere { (predicate: ScopePredicate): ScopeSelection;\n");
    for m in models {
        writeln!(
            o,
            " {}(predicate: ScopePredicate): ScopeSelection;",
            lower(s(m, "name"))
        )
        .unwrap();
    }
    o.push_str("}\nexport interface Scope { readonly add: ScopeAdd; readonly remove: ScopeRecords; tag(labels: string | readonly string[]): { readonly add: ScopeRecords; readonly remove: ScopeTagRemoval }; readonly where: ScopeWhere }\nexport interface LoadScope { readonly add: ScopeAdd; tag(labels: string | readonly string[]): { readonly add: ScopeRecords } }\n");
}
