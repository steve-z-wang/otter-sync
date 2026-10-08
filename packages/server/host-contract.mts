/** The stream's current head cursor. */
export type HeadRequest = { op: "head"; stream: string };
/** Open the savepoint that isolates one mutation. */
export type SavepointRequest = { op: "savepoint"; ordinal: number };
/** Undo one mutation's effects back to its savepoint. */
export type RollbackRequest = { op: "rollback"; ordinal: number };
/** Discard one mutation's savepoint, keeping its effects. */
export type ReleaseRequest = { op: "release"; ordinal: number };
/** Run one mutation's handler. `arguments` carries the decoded slots verbatim. */

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
/**
 * Load the current state of these identities as records of one retained model
 * read contract, for this caller. Loads name no stream: the same identity,
 * version describe the same content on every delivery path.
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
  records: (MemberKey & { mode: "ensure" | "lock" })[];
};
export type GuardRecordsResponse = boolean[];
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
  | { op: "inspectStore"; storeId: string }
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
  | { op: "targetPositions"; stream: string; records: MemberKey[] }
  | ApplyStreamMembersRequest;
export type Protocol05Request = {
  op: "protocol05";
  request: Protocol05Operation;
};
export type HostRequest =
  | Protocol05Request
  | PublicationFenceRequest
  | HeadRequest
  | SavepointRequest
  | RollbackRequest
  | ReleaseRequest
  | HandleActionRequest
  | LoadRequest
  | ReadTrackingRequest
  | GuardRecordsRequest
  | LockStreamsRequest
  | ApplyStreamMembersRequest;

export type HostOperation = HostRequest["op"];

/** The subset a [Persistence] answers: everything that is not application code. */
export type PersistenceRequest = Exclude<
  HostRequest,
  HandleActionRequest | LoadRequest
>;

/** The answer to an operation whose only answer is "done". */
export type Acknowledged = null;
/** The answer to `head`: a bare counter. */
export type Head = number;
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
 * The effects one settlement carries, shared by Mutation handlers and `backend.transaction`: changed records beyond any input
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
  protocol05: unknown;
  publicationFence: Acknowledged;
  head: Head;
  savepoint: Acknowledged;
  rollback: Acknowledged;
  release: Acknowledged;
  handleAction: HandledAction;
  load: Loaded;
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
  publicationFence: true,
  head: true,
  savepoint: true,
  rollback: true,
  release: true,
  handleAction: true,
  load: true,
  readTracking: true,
  guardRecords: true,
  lockStreams: true,
  applyStreamMembers: true,
};

export const HOST_OPERATIONS: readonly HostOperation[] = Object.keys(
  OPERATIONS,
) as HostOperation[];

export type PublicationFenceRequest = { op: "publicationFence" };

/** Authenticated internal handler context; never supplied by a public caller. */
export type HandlerContext = {
  owner: string;
  stream: string;
  storeId: string;
  materialization: string;
};
