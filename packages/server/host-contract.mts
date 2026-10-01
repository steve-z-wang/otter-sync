/**
 * The host operation contract, mirroring `crates/server/src/host.rs`.
 *
 * Hand-written: the two languages have no shared code generator today, so
 * `fixtures/protocol/host-operations.json` is what keeps them in step. A change
 * on either side belongs in the fixture too, and the round-trip tests
 * (`crates/server/tests/host_contract.rs`,
 * `integration/persistence/server/host-contract.test.mjs`) fail on a one-sided one.
 *
 * `handle` and `load` may answer a refusal or a failure: a refusal rolls that
 * mutation back to its savepoint and records the code as its rejection; a
 * failure carries a thrown application error as data. Every other thrown
 * host error still aborts the whole delivery.
 */

/** Lock this client's row and report its last accepted batch. */
export type ClaimRequest = { op: "claim"; owner: string; clientId: string };
/** Record the receipt for an accepted batch. */
export type SaveReceiptRequest = {
  op: "saveReceipt";
  owner: string;
  clientId: string;
  sequence: number;
  receipt: string;
};
/** Lock one call's immutable intent and its completed response. */
export type ClaimCallRequest = {
  op: "claimCall";
  owner: string;
  callId: string;
  request: string;
};
/** Save a complete response for a fresh claim in the same transaction. */
export type SaveCallRequest = {
  op: "saveCall";
  owner: string;
  callId: string;
  response: string;
};
/** The scope's current head cursor. */
export type HeadRequest = { op: "head"; scope: string };
/**
 * Retained log rows after `after`, including removals, at most `limit`
 * in cursor order. Legacy projection happens only in the engine.
 */
export type ScanRequest = {
  op: "scan";
  scope: string;
  after: number;
  limit: number;
};
/** Open the savepoint that isolates one mutation. */
export type SavepointRequest = { op: "savepoint"; ordinal: number };
/** Undo one mutation's effects back to its savepoint. */
export type RollbackRequest = { op: "rollback"; ordinal: number };
/** Discard one mutation's savepoint, keeping its effects. */
export type ReleaseRequest = { op: "release"; ordinal: number };
/** Run one mutation's handler. `arguments` carries the decoded slots verbatim. */
export type HandleRequest = {
  op: "handle";
  name: string;
  version: number;
  arguments: Record<string, unknown>;
  owner: string;
  ordinal: number;
};
export type HandleActionRequest = {
  op: "handleAction";
  name: string;
  version: number;
  arguments: Record<string, unknown>;
  owner: string;
  callId: string;
  ordinal: number;
};
/** Portable JSON: what a Load continuation state may hold. */
export type JsonValue =
  null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };
/** `null` on a first page (or at the end of a traversal); `{state}` otherwise, even when `state` is `null`. */
export type LoadNext = null | { state: JsonValue };
/**
 * Run one Load handler for one page. Its context declares no changes: the
 * answer names identities, the next continuation and the Scope additions
 * its add-only handles declared.
 */
export type HandleLoadRequest = {
  op: "handleLoad";
  name: string;
  version: number;
  arguments: Record<string, unknown>;
  continuation: LoadNext;
  owner: string;
  callId: string;
  loadId: string;
};
/**
 * Load the current state of these identities as records of one retained model
 * read contract, for this caller. Loads name no scope: the same identity,
 * version and stamp describe the same content on every delivery path.
 */
export type LoadRequest = {
  op: "load";
  model: string;
  version: number;
  identities: Record<string, unknown>[];
  owner: string;
};
/** Allocate the next stamp of one record: initialize it at 1 or increment it. */
export type AdvanceStampRequest = {
  op: "advanceStamp";
  model: string;
  identityKey: string;
};
/** The record's current stamp, initialized at 1 only when it has none. */
export type EnsureStampRequest = {
  op: "ensureStamp";
  model: string;
  identityKey: string;
};
/**
 * The current stamps of these records of one model, one per key in request
 * order: an existing stamp is read and never rewritten; only a record without
 * one is initialized at 1.
 */
export type ReadStampsRequest = {
  op: "readStamps";
  model: string;
  identityKeys: string[];
};
/**
 * Write-lock one existing record row without changing its stamp, so a
 * concurrent writer of the row whose snapshot predates this commit restarts
 * instead of acting on it, whether the transaction is serializable or a
 * caller-owned one at Repeatable Read. Never creates a row: an absent record
 * answers `null`.
 */
export type LockRecordRequest = {
  op: "lockRecord";
  model: string;
  identityKey: string;
};
/** The Scopes this record is a persistent member of: a touch's recipients. */
export type MembershipsRequest = {
  op: "memberships";
  model: string;
  identityKey: string;
};
/**
 * Serialize membership changes on these Scopes: lock each existing Scope
 * row, in exactly this order (distinct, canonical byte order), until the
 * transaction ends. Creates no Scope. Every settlement takes its Scopes
 * this way before any record guard.
 */
export type LockScopesRequest = { op: "lockScopes"; scopes: string[] };
/** A record as the Scope operations name it: its Model and canonical identity key. */
export type MemberKey = { model: string; identityKey: string };
/**
 * The live members of the locked `scope` that `explicitKeys` names or that
 * carry one of `tags` (distinct, canonical byte order), each once, with its
 * complete current tags. `all: true` additionally selects every present
 * member; absent `all` defaults to false. Reads only.
 */
export type ReadScopeMembersRequest = {
  op: "readScopeMembers";
  scope: string;
  explicitKeys: MemberKey[];
  tags: string[];
  all?: boolean;
};
/**
 * One pair's final state. Present with exactly `tags`, or absent with none.
 * `publish` takes the Scope's next position (`upsert` when present,
 * `remove` when not; a removal always publishes); without it the member keeps
 * its existing position and only its tags may change.
 */
export type MemberDelta = {
  scope: string;
  model: string;
  identity: Record<string, unknown>;
  identityKey: string;
  present: boolean;
  tags: string[];
  publish: boolean;
};
/**
 * Persist final member states in the caller's transaction, without
 * re-evaluating any selector or opening a transaction. A present delta needs
 * the record's metadata row; a missing Scope starts at head zero. Published
 * deltas take consecutive positions per Scope in delta order. Answers one
 * position per delta, in delta order.
 */
export type ApplyScopeMembersRequest = {
  op: "applyScopeMembers";
  deltas: MemberDelta[];
};

export type HostRequest =
  | ClaimRequest
  | SaveReceiptRequest
  | ClaimCallRequest
  | SaveCallRequest
  | HeadRequest
  | ScanRequest
  | SavepointRequest
  | RollbackRequest
  | ReleaseRequest
  | HandleRequest
  | HandleActionRequest
  | HandleLoadRequest
  | LoadRequest
  | AdvanceStampRequest
  | EnsureStampRequest
  | ReadStampsRequest
  | LockRecordRequest
  | MembershipsRequest
  | LockScopesRequest
  | ReadScopeMembersRequest
  | ApplyScopeMembersRequest;

export type HostOperation = HostRequest["op"];

/** The subset a [Persistence] answers: everything that is not application code. */
export type PersistenceRequest = Exclude<
  HostRequest,
  HandleRequest | HandleActionRequest | HandleLoadRequest | LoadRequest
>;

/** The answer to an operation whose only answer is "done". */
export type Acknowledged = null;
/** The answer to `claim`. */
export type Claimed = {
  clientId: string;
  owner: string;
  sequence: number;
  receipt: string | null;
};
/** An existing ID returns its original request, even when the incoming intent differs. */
export type ClaimedCall = {
  fresh: boolean;
  request: string;
  response: string | null;
};
/** The answer to `head`: a bare counter. */
export type Head = number;
/**
 * One retained scope position and centralized identity. Only an upsert
 * carries the current content stamp from the same snapshot as its Loader.
 */
export type Invalidation = {
  /** Omitted only by legacy hosts; new scans include retained removals. */
  kind?: "upsert" | "remove";
  scope: string;
  cursor: number;
  model: string;
  identity: Record<string, unknown>;
  identityKey: string;
  /** Required on upserts; removals carry identity only. */
  stamp?: number;
};
/** The answer to `advanceStamp` and `ensureStamp`: the record's stamp. */
export type Stamped = number;
/** The answer to `readStamps`: one stamp per requested key, in request order. */
export type Stamps = number[];
/** The answer to `lockRecord`: the locked record's unchanged stamp, or `null` when it has no row. */
export type Locked = number | null;
/** The answer to `memberships`: unique Scope names, sorted by the database. */
export type Memberships = string[];
/** One member `readScopeMembers` answers: its complete current tags, each once, in any order. */
export type MemberState = MemberKey & { tags: string[] };
/** The latest position of one pair: new for a published delta, the existing one otherwise. */
export type MemberPosition = MemberKey & {
  scope: string;
  cursor: number;
  kind: "upsert" | "remove";
};
/** A record a handler names: an additional changed record. */
export type HostRecordRef = {
  model: string;
  identity: Record<string, unknown>;
};
/**
 * One persistent Scope membership declaration, in declaration order: `add`
 * makes the record a member of `scope` and unions `tags` (distinct, as
 * spelled; `[]` adds none) with its labels; `remove` releases the record's
 * whole membership; predicate selections operate on the preceding declarations. The engine reduces the list
 * in order to its final state.
 */
export type ScopeIntent =
  | {
      kind: "add";
      scope: string;
      record: HostRecordRef;
      tags: readonly string[];
    }
  | { kind: "remove"; scope: string; record: HostRecordRef }
  | {
      kind: "tagAdd" | "tagRemove";
      scope: string;
      record: HostRecordRef;
      tags: readonly string[];
    }
  | { kind: "detachTags"; scope: string; tags: readonly string[] }
  | {
      kind: "select";
      scope: string;
      model?: string;
      predicate: ScopePredicate;
      action: SelectionAction;
    };
export type ScopePredicate = {
  tags?: {
    all?: readonly string[];
    any?: readonly string[];
    none?: readonly string[];
    only?: readonly string[];
  };
  and?: readonly ScopePredicate[];
  or?: readonly ScopePredicate[];
  not?: ScopePredicate;
};
export type SelectionAction =
  | { kind: "remove" }
  | { kind: "tagAdd" | "tagRemove"; tags: readonly string[] };
/**
 * The effects one settlement carries, shared by Mutation handlers, legacy
 * handlers and `backend.transaction`: changed records beyond any input
 * targets and ordered Scope intents. There is no implicit publication.
 */
export type SettlementEffects = {
  changes: HostRecordRef[];
  memberships: ScopeIntent[];
};
/**
 * The answer to `handle`: the records the handler changed beyond the uploaded
 * operations and its membership intents, a rejection code, or a failure
 * carrying a thrown handler error — never more than one of these.
 *
 * "Never more than one" is not something this union can enforce. TypeScript
 * only applies its excess-property check to object literals, so a value that
 * reaches here through a variable satisfies the union with several keys set.
 * Rust enforces it on decode (`HandledWire` in crates/server/src/host.rs),
 * which refuses such an answer with `handler.invalid` rather than reading it
 * as a rejection or a failure.
 */
export type Handled =
  SettlementEffects | { rejection: string } | { error: string };
export type HandledAction =
  | ({ outputs: Record<string, unknown> } & SettlementEffects)
  | { rejection: string }
  | { error: string };
/**
 * The answer to `handleLoad`: the page's identity lists, next continuation
 * and the membership additions the handler declared through its add-only
 * Scope handles, a rejection code, or a failure carrying a thrown handler
 * error. `memberships` is omitted when there are none (an older host never
 * sends it); `null`, `changes`, or memberships beside a rejection or failure
 * are refused. The engine, not this type, refuses a removal, a tag selector
 * or a record the page did not return.
 */
export type HandledLoad =
  | {
      data: Record<string, unknown>;
      next: LoadNext;
      memberships?: ScopeIntent[];
    }
  | { rejection: string }
  | { error: string };
/**
 * The answer to `load`: one entry per identity, `null` for a record that does
 * not exist for this caller, a refusal the engine records as the mutation's
 * rejection (push) or reports for the page (pull), or a failure carrying a
 * thrown loader error.
 */
export type Loaded =
  | (Record<string, unknown> | null)[]
  | { rejection: string }
  | { error: string };

/** The answer each operation owes, keyed by `op`. */
export type HostResponse = {
  claim: Claimed;
  saveReceipt: Acknowledged;
  claimCall: ClaimedCall;
  saveCall: Acknowledged;
  head: Head;
  scan: Invalidation[];
  savepoint: Acknowledged;
  rollback: Acknowledged;
  release: Acknowledged;
  handle: Handled;
  handleAction: HandledAction;
  handleLoad: HandledLoad;
  load: Loaded;
  advanceStamp: Stamped;
  ensureStamp: Stamped;
  readStamps: Stamps;
  lockRecord: Locked;
  memberships: Memberships;
  lockScopes: Acknowledged;
  readScopeMembers: MemberState[];
  applyScopeMembers: MemberPosition[];
};

/**
 * Every operation, checked against the union in both directions: a missing key
 * and an extra one are both compile errors here.
 */
const OPERATIONS: Record<HostOperation, true> = {
  claim: true,
  saveReceipt: true,
  claimCall: true,
  saveCall: true,
  head: true,
  scan: true,
  savepoint: true,
  rollback: true,
  release: true,
  handle: true,
  handleAction: true,
  handleLoad: true,
  load: true,
  advanceStamp: true,
  ensureStamp: true,
  readStamps: true,
  lockRecord: true,
  memberships: true,
  lockScopes: true,
  readScopeMembers: true,
  applyScopeMembers: true,
};

export const HOST_OPERATIONS: readonly HostOperation[] = Object.keys(
  OPERATIONS,
) as HostOperation[];
