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
/** The stream's current head cursor. */
export type HeadRequest = { op: "head"; stream: string };
/**
 * Retained log rows after `after`, including removals, at most `limit`
 * in cursor order. Legacy projection happens only in the engine.
 */
export type ScanRequest = {
  op: "scan";
  stream: string;
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
  context?: HandlerContext;
};
/** Portable JSON: what a Load continuation state may hold. */
export type JsonValue =
  null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };
/** `null` on a first page (or at the end of a traversal); `{state}` otherwise, even when `state` is `null`. */
export type LoadNext = null | { state: JsonValue };
/**
 * Run one Load handler for one page. Its context declares no changes: the
 * answer names identities, the next continuation and the tracking
 * its handler declared.
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
 * read contract, for this caller. Loads name no stream: the same identity,
 * version and stamp describe the same content on every delivery path.
 */
export type LoadRequest = {
  op: "load";
  /** Prepare only (empty success rows), or read after completed preparation. */
  mode?: "prepare" | "canonical";
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
export type MemberKey = { model: string; identityKey: string };
export type TrackingPair = MemberKey & { stream: string };
export type ReadTrackingRequest = {
  op: "readTracking";
  records: MemberKey[];
  pairs: TrackingPair[];
};
export type ReadTrackingResponse = TrackingPair[];
export type GuardRecordsRequest = {
  op: "guardRecords";
  records: (MemberKey & { mode: "advance" | "ensure" | "lock" })[];
};
export type GuardRecordsResponse = (number | null)[];
/** Distinct, strictly ordered UTF-8 names; lock before canonical record guards. */
export type LockStreamsRequest = { op: "lockStreams"; streams: string[] };
export type TrackingDelta = TrackingPair & {
  identity: Record<string, unknown>;
  publish: boolean;
};
export type ApplyStreamMembersRequest = {
  op: "applyStreamMembers";
  deltas: TrackingDelta[];
};

/** Protocol-5 persistence/application seam. Every call uses the retained transaction. */
export type Protocol05Context = {
  protocol: 5;
  storeId: string;
  stream: string;
  materialization: string;
};
export type Protocol05Operation =
  | { op: "bootstrapState"; storeId: string }
  | { op: "handleBootstrap05"; owner: string; storeId: string; stream: string }
  | { op: "finishBootstrap"; storeId: string }
  | { op: "deliveryHead"; stream: string }
  | { op: "deliveryNow" }
  | {
      op: "deliveryCandidates";
      stream: string;
      after: number;
      models: string[] | null;
      keys: MemberKey[] | null;
      capacity: number;
    }
  | {
      op: "saveDelivery";
      owner: string;
      intent: string;
      header: JsonValue;
      parts: JsonValue[];
    }
  | {
      op: "readDelivery";
      owner: string;
      context: Protocol05Context;
      intent: string;
      continuation: {
        planId: string;
        digest: string;
        unit: number;
        part: number;
      };
    }
  | { op: "admit"; owner: string; context: Protocol05Context }
  | { op: "claimStore"; storeId: string; principal: string; stream: string }
  | {
      op: "beginBatch";
      storeId: string;
      batchId: number;
      digest: string;
      count: number;
    }
  | { op: "readResult"; storeId: string; batchId: number; ordinal: number }
  | { op: "readResults"; storeId: string; batchId: number }
  | {
      op: "saveResult";
      storeId: string;
      batchId: number;
      ordinal: number;
      count: number;
      result: JsonValue;
    }
  | ReadTrackingRequest
  | GuardRecordsRequest
  | ReadPositionsRequest
  | { op: "targetPositions"; stream: string; records: MemberKey[] }
  | ApplyStreamMembersRequest;
export type Protocol05Request = {
  op: "protocol05";
  request: Protocol05Operation;
};
export type HostRequest =
  | Protocol05Request
  | AdmitContextRequest
  | PublicationFenceRequest
  | SavePublicationGroupsRequest
  | ReadPublicationGroupsRequest
  | ReadPositionsRequest
  | HandleBootstrapRequest
  | ReadCallRequest
  | CreateManifestRequest
  | ReadManifestRequest
  | CaptureTailRequest
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
  | ReadTrackingRequest
  | GuardRecordsRequest
  | LockStreamsRequest
  | ApplyStreamMembersRequest;

export type HostOperation = HostRequest["op"];

/** The subset a [Persistence] answers: everything that is not application code. */
export type PersistenceRequest = Exclude<
  HostRequest,
  | HandleRequest
  | HandleActionRequest
  | HandleLoadRequest
  | LoadRequest
  | AdmitContextRequest
  | HandleBootstrapRequest
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
 * One retained stream position and centralized identity. Only an upsert
 * carries the current content stamp from the same snapshot as its Loader.
 */
export type Invalidation = {
  /** Omitted only by legacy hosts; new scans include retained removals. */
  kind?: "upsert" | "remove";
  stream: string;
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
export type MemberPosition = TrackingPair & {
  cursor: number;
  kind: "upsert" | "remove";
};
/** A record a handler names: an additional changed record. */
export type HostRecordRef = {
  model: string;
  identity: Record<string, unknown>;
};
export type TrackIntent = {
  kind: "track";
  stream: string;
  record: HostRecordRef;
};
export type StreamIntent =
  | TrackIntent
  | { kind: "invalidate"; streams: string[] | null; record: HostRecordRef };
/**
 * The effects one settlement carries, shared by Mutation handlers, legacy
 * handlers and `backend.transaction`: changed records beyond any input
 * targets and combined Stream declarations. There is no implicit publication.
 */
export type SettlementEffects = {
  changes: HostRecordRef[];
  declarations: StreamIntent[];
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
/** Native Loads declare only tracking of records their successful page returns. */
export type HandledLoad =
  | {
      data: Record<string, unknown>;
      next: LoadNext;
      tracking?: TrackIntent[];
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
  admitContext: boolean;
  protocol05: unknown;
  publicationFence: Acknowledged;
  savePublicationGroups: Acknowledged;
  readPublicationGroups: PublicationGroup[];
  readPositions: MemberPosition[];
  handleBootstrap: { declarations: TrackIntent[] };
  readCall: string | null;
  createManifest: ManifestSlice;
  readManifest: ManifestSlice;
  captureTail: number;
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
  readTracking: ReadTrackingResponse;
  guardRecords: GuardRecordsResponse;
  lockStreams: Acknowledged;
  applyStreamMembers: MemberPosition[];
};

/**
 * Every operation, checked against the union in both directions: a missing key
 * and an extra one are both compile errors here.
 */
const OPERATIONS: Record<HostOperation, true> = {
  protocol05: true,
  admitContext: true,
  publicationFence: true,
  handleBootstrap: true,
  createManifest: true,
  readCall: true,
  readManifest: true,
  captureTail: true,
  savePublicationGroups: true,
  readPublicationGroups: true,
  readPositions: true,
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
  readTracking: true,
  guardRecords: true,
  lockStreams: true,
  applyStreamMembers: true,
};

export const HOST_OPERATIONS: readonly HostOperation[] = Object.keys(
  OPERATIONS,
) as HostOperation[];

export type RequestContext = {
  protocol: 4;
  binding: {
    backend: string;
    viewer: string;
    stream: string;
    contract: string;
  };
  materialization: string;
  incarnation: string;
};
export type AdmitContextRequest = {
  op: "admitContext";
  owner: string;
  context: RequestContext;
  durable: boolean;
};
export type PublicationFenceRequest = { op: "publicationFence" };

export type PublicationGroup = {
  from: number;
  through: number;
  keys: MemberKey[];
};
export type SavePublicationGroupsRequest = {
  op: "savePublicationGroups";
  positions: MemberPosition[];
};
export type ReadPublicationGroupsRequest = {
  op: "readPublicationGroups";
  stream: string;
  after: number;
  limit: number;
};
export type ReadPositionsRequest = {
  op: "readPositions";
  stream: string;
  records: MemberKey[];
};

export type HandleBootstrapRequest = {
  op: "handleBootstrap";
  owner: string;
  callId: string;
  context: RequestContext;
};
export type CreateManifestRequest = {
  op: "createManifest";
  owner: string;
  manifestId: string;
  context: RequestContext;
  start: number;
  models: Record<string, number>;
  selected: string[];
  held: MemberKey[];
  budget: number;
};
export type ReadManifestRequest = {
  op: "readManifest";
  owner: string;
  manifestId: string;
  context: RequestContext;
  from: number;
  limit: number;
  uniqueModels: string[];
};
export type CaptureTailRequest = {
  op: "captureTail";
  owner: string;
  manifestId: string;
  context: RequestContext;
  head: number;
};
export type ManifestSlice = {
  start: number;
  total: number;
  models: Record<string, number>;
  from: number;
  to: number;
  keys: MemberKey[];
  companions?: MemberKey[];
};

export type ReadCallRequest = { op: "readCall"; owner: string; callId: string };

/** Authenticated internal handler context; never supplied by a public caller. */
export type HandlerContext = { owner: string; stream: string; storeId: string; materialization: string };
