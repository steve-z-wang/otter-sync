import { createServer, type IncomingMessage } from "node:http";
import { connect, type Socket } from "node:net";

/** One HTTP request through the proxy and what became of it. */
export type Exchange = {
  path: string;
  body: string;
  status?: number;
  response?: string;
  /** `request`: cut before the backend saw it; `response`: the backend answered, the client never got it. */
  dropped?: "request" | "response";
};
type Rule = {
  match: (exchange: Exchange) => boolean;
  used: boolean;
  run: (exchange: Exchange, socket: Socket) => Promise<void>;
};
export type Proxy = Awaited<ReturnType<typeof createProxy>>;

/**
 * Forwards `/sync/*` HTTP and the live WebSocket to the backend. Rules act on
 * answered exchanges; `down()` cuts every connection and answers nothing
 * until `up()`.
 */
export async function createProxy(target: string) {
  const upstream = new URL(target);
  const exchanges: Exchange[] = [];
  const sockets = new Set<Socket>();
  const rules: Rule[] = [];
  const waiters = new Set<() => void>();
  const requestHolds: {
    match: (exchange: Exchange) => boolean;
    used: boolean;
    arrive: () => void;
    released: Promise<void>;
  }[] = [];
  let down = false;
  const changed = () => {
    for (const wake of [...waiters]) wake();
  };
  const cut = () => {
    for (const socket of sockets) socket.destroy();
    sockets.clear();
  };
  const track = (socket: Socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
  };
  const server = createServer(async (request: IncomingMessage, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    const exchange: Exchange = {
      path: request.url?.split("?")[0] ?? "",
      body: Buffer.concat(chunks).toString("utf8"),
    };
    exchanges.push(exchange);
    const held = requestHolds.find(
      (candidate) => !candidate.used && candidate.match(exchange),
    );
    if (held) {
      held.used = true;
      held.arrive();
      await held.released;
    }
    if (down) {
      exchange.dropped = "request";
      request.socket.destroy();
      changed();
      return;
    }
    try {
      const answer = await fetch(new URL(request.url ?? "/", upstream), {
        method: request.method,
        headers: {
          authorization: String(request.headers.authorization ?? ""),
          "content-type": String(
            request.headers["content-type"] ?? "application/json",
          ),
        },
        body: exchange.body,
      });
      exchange.status = answer.status;
      exchange.response = await answer.text();
    } catch {
      exchange.dropped = "request";
      request.socket.destroy();
      changed();
      return;
    }
    const rule = rules.find(
      (candidate) => !candidate.used && candidate.match(exchange),
    );
    if (rule) {
      rule.used = true;
      await rule.run(exchange, request.socket);
    }
    if (down || request.socket.destroyed) {
      exchange.dropped = "response";
      request.socket.destroy();
      changed();
      return;
    }
    response.writeHead(exchange.status!, {
      "content-type": "application/json; charset=utf-8",
    });
    response.end(exchange.response);
    changed();
  });
  server.on("connection", track);
  server.on(
    "upgrade",
    (request: IncomingMessage, socket: Socket, head: Buffer) => {
      if (down) {
        socket.destroy();
        return;
      }
      const backend = connect(Number(upstream.port), upstream.hostname, () => {
        const lines = [`${request.method} ${request.url} HTTP/1.1`];
        for (let n = 0; n < request.rawHeaders.length; n += 2)
          lines.push(`${request.rawHeaders[n]}: ${request.rawHeaders[n + 1]}`);
        backend.write(`${lines.join("\r\n")}\r\n\r\n`);
        if (head.length) backend.write(head);
        socket.pipe(backend).pipe(socket);
      });
      track(backend);
      backend.on("error", () => socket.destroy());
      socket.on("error", () => backend.destroy());
      socket.on("close", () => backend.destroy());
    },
  );
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address() as { port: number };
  const rule = (match: (exchange: Exchange) => boolean, run: Rule["run"]) => {
    rules.push({ match, used: false, run });
  };
  const releases = new Set<() => void>();
  return {
    url: `http://127.0.0.1:${address.port}`,
    exchanges,
    requests(path: string) {
      return exchanges
        .filter((exchange) => exchange.path === path)
        .map((exchange) => ({
          ...exchange,
          request: JSON.parse(exchange.body),
          answer:
            exchange.response === undefined
              ? undefined
              : JSON.parse(exchange.response),
        }));
    },
    /** Resolves once `predicate` holds over the exchanges so far. */
    until(
      predicate: () => boolean,
      label: string,
      timeout = 20_000,
    ): Promise<void> {
      if (predicate()) return Promise.resolve();
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          waiters.delete(check);
          reject(Error(`Timed out waiting for ${label}`));
        }, timeout);
        const check = () => {
          if (predicate()) {
            clearTimeout(timer);
            waiters.delete(check);
            resolve();
          }
        };
        waiters.add(check);
      });
    },
    /**
     * Hold the first matching answered exchange's response until released;
     * `clientGone` resolves when the client's connection has closed.
     */
    holdResponse(match: (exchange: Exchange) => boolean) {
      let arrive!: (exchange: Exchange) => void;
      let release!: () => void;
      let gone!: () => void;
      const arrived = new Promise<Exchange>((resolve) => {
        arrive = resolve;
      });
      const released = new Promise<void>((resolve) => {
        release = resolve;
      });
      const clientGone = new Promise<void>((resolve) => {
        gone = resolve;
      });
      releases.add(release);
      rule(match, async (exchange, socket) => {
        if (socket.destroyed) gone();
        else socket.once("close", gone);
        arrive(exchange);
        await released;
      });
      return { arrived, release, clientGone };
    },
    /** Hold the first matching request before the backend sees it, until released. */
    holdRequest(match: (exchange: Exchange) => boolean) {
      let arrive!: () => void;
      let release!: () => void;
      const arrived = new Promise<void>((resolve) => {
        arrive = resolve;
      });
      const released = new Promise<void>((resolve) => {
        release = resolve;
      });
      releases.add(release);
      requestHolds.push({ match, used: false, arrive, released });
      return { arrived, release };
    },
    /** Answer nothing for the first matching exchange and cut the network (`down()`). */
    dropResponseAndGoDown(match: (exchange: Exchange) => boolean) {
      rule(match, async () => {
        down = true;
        cut();
      });
    },
    down() {
      down = true;
      cut();
    },
    up() {
      down = false;
    },
    releaseAll() {
      for (const release of releases) release();
    },
    async close() {
      cut();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}
