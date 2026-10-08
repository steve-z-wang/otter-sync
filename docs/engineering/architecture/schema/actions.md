# Mutations and Queries

## 1. Introduction and Goals

An operation is a versioned named backend call with typed inputs and explicit outputs. A Mutation may change business state; a Query reads without business side effects. The declaration supplies normalization, retained contract history and local Mutation optimism; application handlers supply business logic.

## 3. Context and Scope

```text
mutation AddTodo(todo Todo.create)

mutation UpdateTodoAndRead(todo Todo.update) {
  todo Todo
}

query SearchTodos(text String, cursor String?) {
  todos Todo[]
  nextCursor String?
}
```

Mutations accept ordinary values and Model create/update/delete inputs, including optional and list operands. Omitted optional Model input normalizes to null. Model operands imply no output. Model outputs are identities returned explicitly by the Handler and resolved through the selected retained Loader contract; scalar outputs come from the Handler. Input and output names may coincide without referring to the same record. No output falls back to an input.

Queries accept ordinary inputs and cannot declare Model mutations or sequence dependencies. Their Model outputs do not track records automatically. Current Query contexts provide explicit track-only initiating and multi-Stream handles; they provide no invalidation. Fetch reads one identity through the same viewer Loader authority.

| Generated call | Returns | Completion |
| --- | --- | --- |
| `mutations.<name>` | `Call<Output>` | Local durable acceptance of input, optimism and companions |
| `queries.<name>` | `Output` | Fresh backend invocation snapshot |
| `Call.wait()` | Final outcome | Required accepted settlement has committed locally, or refusal is known |

Queries and Fetch choose request-level `{store?: boolean}`, default true. Storage may populate unprotected cache through null-cursor records; it cannot replace protected Stream authority or advance progress. `store: false` returns the invocation snapshot without storing those records. There is no direct-Mutation route, queued Query, per-output store map, generic Load job or once cache in the current application API.

## 5. Building Block View

[Parser](../../../../crates/compiler/src/parse.rs), [validation](../../../../crates/compiler/src/validate.rs) and [history](../../../../crates/compiler/src/history.rs) own declarations and retained contracts. [Core actions](../../../../crates/core/src/actions.rs) normalizes inputs and results; [server delivery plans](../../../../crates/server/src/delivery_plan.rs) executes fresh reads and explicit tracking; [server Batches](../../../../crates/server/src/mutation_batch.rs) owns durable Mutation execution. [Generated typed APIs](../sdks/typed-api/README.md) own language interfaces.

## 9. Architecture Decisions

`@version(n)` fixes each operation's kind, inputs and outputs. `history/actions.json` retains every published version and the selected Model output contracts. Incompatible shape or kind changes require a new version. Keep retained handlers/Loaders while serving those versions. This history preserves format-5 queued intent; it supplies no old-framework file conversion.

A Query's business contract is read-only. The engine rejects forbidden framework effects but cannot inspect arbitrary application SQL or a captured external client. Ordinary Handler/Loader awaits hold neither Store progress nor the global publication fence. Explicit tracking is published through a fenced Serializable read transaction and may retry the whole invocation; save external business effects for Mutation transactions.

Member deprecation is a generated notice only. Required fields and binding rules remain enforced. Creation defaults are evaluated once for fresh Model creates; they do not regenerate during replay ([Models](models.md)).

## 10. Quality Requirements

[Compiler/history tests](../../../../crates/compiler/tests/history.rs) cover retained operation contracts; [operation contract fixtures](../../../../integration/action-contract/schema.model) exercise generated interfaces. Current [server carrier tests](../../../../crates/server/tests/protocol_v05.rs), [delivery-plan tests](../../../../crates/server/tests/delivery_plan.rs) and [SDK host runner](../../../../integration/v05-sdk/run-host.sh) cover execution versus settlement, explicit tracking and null-cursor snapshots. These identify maintained proof; joined execution and installed/mobile evidence are separate gates.
