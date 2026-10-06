import { randomUUID } from "expo-crypto";
import { databasePath } from "@axton/client-react-native";
import {
  GeneratedClient,
  type Todo,
  type User,
} from "../../generated/mobile/client";

export const scope = "todo:demo";

export interface TodoSession {
  watch(
    listener: (rows: Todo[]) => void,
    onError: (error: unknown) => void,
  ): () => void;
  add(title: string): Promise<void>;
  setDone(id: string, done: boolean): Promise<void>;
  close(): Promise<void>;
}

export interface Rejection {
  ordinal: number;
  code: string;
}

/** Sorted by code-unit id order so both phones render the same list independent of locale. */
export function sortTodos(rows: Todo[]): Todo[] {
  return [...rows].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
}

export interface OpenedSession {
  session: TodoSession;
  client: GeneratedClient;
  user: () => Promise<User | null>;
  /** Server refusals of this phone's own writes; dismissed once shown. */
  rejections: () => Promise<Rejection[]>;
  dismiss: (ordinal: number) => Promise<void>;
}

/** One client per installation: the database name is fixed per configured user, so the client identity persists across launches. */
export async function openTodoSession(options: {
  user: string;
  url: string;
  onConnectionError: (error: unknown) => void;
}): Promise<OpenedSession> {
  const client = await GeneratedClient.open({
    path: await databasePath(`todo-${options.user}.sqlite`),
    stream: scope,
    connection: { url: options.url, token: options.user, identity: { backend: "todo-demo", viewer: options.user, contract: "todo-v04" }, options: { onError: options.onConnectionError } },
  });
  client.bootstrap().catch(options.onConnectionError);
  const session: TodoSession = {
    watch(listener, onError) {
      return client.models.todo.watch(
        {},
        (rows) => listener(sortTodos(rows)),
        onError,
      );
    },
    async add(title) {
      const value = title.trim();
      if (!value) throw Error("Enter a task title");
      await client.mutations.addTodo({
        todo: {
          id: randomUUID(),
          title: value,
          done: false,
          createdById: options.user,
        },
      });
    },
    async setDone(id, done) {
      await client.mutations.setTodoDone({
        todo: { id, done },
      });
    },
    close() {
      return client.close();
    },
  };
  return {
    session,
    client,
    user: () => client.models.user.get({ id: options.user }),
    rejections: async () =>
      (await client.syncState()).rejections as Rejection[],
    dismiss: (ordinal) =>
      client.dismissRejection(ordinal).then(() => undefined),
  };
}

/** Short user-facing text for a server refusal; internal codes stay in logs. */
export function describeRejection(code: string): string {
  switch (code) {
    case "todo.title_empty":
      return "The server needs a task title";
    case "todo.missing":
      return "That task no longer exists";
    case "todo.creator_invalid":
    case "todo.initial_state_invalid":
    case "todo.id_conflict":
      return "The server did not accept that task";
    default:
      return "The server rejected a change";
  }
}
