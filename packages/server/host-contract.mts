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
/** The channel's current head cursor. */
export type HeadRequest = { op: "head"; channel: string };
/**
 * Invalidation rows after `after` whose record is still a member of the
 * channel, at most `limit` of them, in cursor order. Membership filters before
 * the limit; a removed record's row stays but is not answered.
 */
export type ScanRequest = {
  op: "scan";
  channel: string;
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
/**
 * Load the current state of these identities as records of one retained model
 * read contract, for this caller. Loads name no channel: the same identity,
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
 * Invalidate one record on one channel at this stamp, allocating only the
 * channel cursor. `stamp` must be the record's current stamp.
 */
export type PublishRequest = {
  op: "publish";
  channel: string;
  model: string;
  identity: Record<string, unknown>;
  identityKey: string;
  stamp: number;
};
/**
 * Write-lock one existing record row without changing its stamp, so a
 * concurrent Repeatable Read writer of the row restarts instead of acting on
 * a stale snapshot. Never creates a row: an absent record answers `null`.
 */
export type LockRecordRequest = {
  op: "lockRecord";
  model: string;
  identityKey: string;
};
/** The Channels this record is a persistent member of. */
export type MembershipsRequest = {
  op: "memberships";
  model: string;
  identityKey: string;
};
/**
 * Make the record a member of `channel` (`present: true`, creating the Channel
 * at head zero if needed) or not (`false`). Idempotent both ways; never
 * allocates a cursor. The record's metadata must exist to add it.
 */
export type SetMembershipRequest = {
  op: "setMembership";
  channel: string;
  model: string;
  identityKey: string;
  present: boolean;
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
  | LoadRequest
  | AdvanceStampRequest
  | EnsureStampRequest
  | PublishRequest
  | LockRecordRequest
  | MembershipsRequest
  | SetMembershipRequest;

export type HostOperation = HostRequest["op"];

/** The subset a [Persistence] answers: everything that is not application code. */
export type PersistenceRequest = Exclude<
  HostRequest,
  HandleRequest | HandleActionRequest | LoadRequest
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
 * One row of the answer to `scan`: the invalidation's own cursor with the
 * record's *current* stamp, read from the record metadata in the same snapshot
 * the loader will read.
 */
export type Invalidation = {
  channel: string;
  cursor: number;
  model: string;
  identity: Record<string, unknown>;
  identityKey: string;
  stamp: number;
};
/** The answer to `advanceStamp` and `ensureStamp`: the record's stamp. */
export type Stamped = number;
/** The answer to `publish`: the allocated cursor and the stamp the request named. */
export type Published = { cursor: number; stamp: number };
/** The answer to `lockRecord`: the locked record's unchanged stamp, or `null` when it has no row. */
export type Locked = number | null;
/** The answer to `memberships`: unique Channel names, sorted by the database. */
export type Memberships = string[];
/** A record a handler names: an additional changed record. */
export type HostRecordRef = {
  model: string;
  identity: Record<string, unknown>;
};
/**
 * One persistent Channel membership declaration: the record should
 * (`present`) or should not be a member of `channel`. Intents are ordered; the
 * last one per Channel/record pair is the desired state.
 */
export type MembershipIntent = {
  channel: string;
  model: string;
  identity: Record<string, unknown>;
  present: boolean;
};
/**
 * The effects one settlement carries, shared by Mutation handlers, legacy
 * handlers and `backend.transaction`: changed records beyond any input
 * targets and ordered membership intents. There is no implicit publication.
 */
export type SettlementEffects = {
  changes: HostRecordRef[];
  memberships: MembershipIntent[];
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
  load: Loaded;
  advanceStamp: Stamped;
  ensureStamp: Stamped;
  publish: Published;
  lockRecord: Locked;
  memberships: Memberships;
  setMembership: Acknowledged;
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
  load: true,
  advanceStamp: true,
  ensureStamp: true,
  publish: true,
  lockRecord: true,
  memberships: true,
  setMembership: true,
};

export const HOST_OPERATIONS: readonly HostOperation[] = Object.keys(
  OPERATIONS,
) as HostOperation[];
