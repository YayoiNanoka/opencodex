import { afterEach, beforeEach, expect, test } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { saveConfig } from "../../src/config";
import { codexWebSocketAdmissionMetrics, MAX_TRACKED_CODEX_WEBSOCKETS } from "../../src/codex/websocket-registry";
import { startServer } from "../../src/server";
import type { OcxConfig } from "../../src/types";
import { installIsolatedCodexHome, type IsolatedCodexHome } from "../helpers/isolated-codex-home";
import { removeTreeWithRetry } from "../helpers/remove-tree";

const originalFetch = globalThis.fetch;
const previousHome = process.env.OPENCODEX_HOME;
const MODEL = "fixture/capacity-model";
type Frame = Record<string, any>;
type Client = { socket: WebSocket; frames: Frame[]; closed?: CloseEvent; error?: Event };
let home: string;
let codex: IsolatedCodexHome;
let proxy: ReturnType<typeof startServer> | undefined;
let upstream: ReturnType<typeof Bun.serve> | undefined;
let finishStream: (() => void) | undefined;
let upstreamRequests = 0;
const clients: Client[] = [];

async function until(condition: () => boolean, description: string): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (!condition()) {
    if (Date.now() >= deadline) throw new Error(`Timed out: ${description}`);
    await Bun.sleep(2);
  }
}

function sse(event: Frame): Uint8Array {
  return new TextEncoder().encode(`event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`);
}

beforeEach(() => {
  home = mkdtempSync(join(tmpdir(), "ocx-ws-capacity-"));
  process.env.OPENCODEX_HOME = home;
  codex = installIsolatedCodexHome("ocx-ws-capacity-codex-");
  upstreamRequests = 0;
  finishStream = undefined;
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(input instanceof Request ? input.url : String(input));
    if (url.hostname !== "127.0.0.1" && url.hostname !== "localhost") {
      throw new Error("Capacity fixture forbids external requests");
    }
    return originalFetch(input, init);
  }) as typeof fetch;
  upstream = Bun.serve({
    hostname: "127.0.0.1", port: 0,
    async fetch(request) {
      const body = await request.json() as Frame;
      upstreamRequests++;
      const id = `resp_capacity_${upstreamRequests}`;
      const tool = JSON.stringify(body.input).includes("request-fixture-tool");
      const hold = JSON.stringify(body.input).includes("hold-fixture-stream");
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(sse({ type: "response.created", response: { id, status: "in_progress", output: [] } }));
          const output = tool
            ? [{ type: "function_call", id: "fc_capacity", call_id: "call_capacity", name: "fixture_tool", arguments: "{}" }]
            : [{ type: "message", id: "msg_capacity", role: "assistant", content: [{ type: "output_text", text: "capacity fixture completed" }] }];
          const finish = () => {
            if (tool) controller.enqueue(sse({ type: "response.output_item.done", output_index: 0, item: output[0] }));
            controller.enqueue(sse({ type: "response.completed", response: { id, status: "completed", output,
              usage: { input_tokens: 1, output_tokens: 1, total_tokens: 2 } } }));
            controller.close();
          };
          if (hold) finishStream = finish; else finish();
        },
      });
      return new Response(stream, { headers: { "content-type": "text/event-stream" } });
    },
  });
  saveConfig({
    port: 0, hostname: "127.0.0.1", codexAutoStart: false, websockets: true,
    defaultProvider: "fixture", providers: { fixture: {
      adapter: "openai-responses", baseUrl: `${upstream.url}v1`, apiKey: "sk-capacity-fixture", upstreamWebsocket: false,
      allowPrivateNetwork: true,
    } },
  } as OcxConfig);
  expect(codexWebSocketAdmissionMetrics().active).toBe(0);
  proxy = startServer(0);
});

afterEach(async () => {
  try {
    for (const client of clients) client.socket.close();
    await proxy?.stop(true);
    await upstream?.stop(true);
    await until(() => codexWebSocketAdmissionMetrics().active === 0, "all socket reservations released");
  } finally {
    clients.length = 0;
    proxy = undefined;
    upstream = undefined;
    globalThis.fetch = originalFetch;
    codex.restore();
    removeTreeWithRetry(home);
    if (previousHome === undefined) delete process.env.OPENCODEX_HOME;
    else process.env.OPENCODEX_HOME = previousHome;
  }
});

async function connect(): Promise<Client> {
  const url = new URL("/v1/responses", proxy!.url);
  url.protocol = "ws:";
  const client: Client = { socket: new WebSocket(url), frames: [] };
  clients.push(client);
  client.socket.addEventListener("message", event => client.frames.push(JSON.parse(String(event.data))));
  client.socket.addEventListener("close", event => { client.closed = event; });
  client.socket.addEventListener("error", event => { client.error = event; });
  await until(() => client.socket.readyState === WebSocket.OPEN || Boolean(client.error || client.closed), "socket handshake");
  if (client.socket.readyState !== WebSocket.OPEN) throw new Error("Capacity fixture socket handshake failed");
  return client;
}

async function fill(): Promise<Client[]> {
  const opened: Client[] = [];
  for (let i = 0; i < MAX_TRACKED_CODEX_WEBSOCKETS; i++) opened.push(await connect());
  return opened;
}

async function exchange(client: Client, fields: Frame, terminal = "response.completed"): Promise<Frame> {
  const start = client.frames.length;
  client.socket.send(JSON.stringify({ type: "response.create", model: MODEL, input: "fixture input", ...fields }));
  await until(() => client.frames.slice(start).some(frame => frame.type === terminal || frame.type === "error")
    || Boolean(client.closed), `socket ${terminal}`);
  const result = client.frames.slice(start).find(frame => frame.type === terminal || frame.type === "error");
  expect(result?.type).toBe(terminal);
  return result!;
}

function expectBounded(): void {
  const metrics = codexWebSocketAdmissionMetrics();
  expect(metrics.active).toBe(MAX_TRACKED_CODEX_WEBSOCKETS);
  expect(metrics.peak).toBe(MAX_TRACKED_CODEX_WEBSOCKETS);
  expect(metrics.releaseMisses).toBe(0);
}

test("the 129th handshake reclaims the least recently used idle socket and reconnects cleanly", async () => {
  const opened = await fill();
  expect(opened.every(client => client.socket.readyState === WebSocket.OPEN)).toBe(true);
  await exchange(opened[0]!, { generate: false });
  expect(upstreamRequests).toBe(0);
  const replacement = await connect();
  await until(() => Boolean(opened[1]!.closed), "least recently used idle socket closed");
  expect(opened[1]!.closed?.code).toBe(1001);
  expect(opened[0]!.socket.readyState).toBe(WebSocket.OPEN);
  expectBounded();
  await exchange(replacement, { input: "fresh request after capacity recovery" });
  expect(upstreamRequests).toBe(1);
  const reconnected = await connect();
  await exchange(reconnected, { input: "resubmitted full conversation" });
  expect(upstreamRequests).toBe(2);
  expectBounded();
}, 15_000);

test("concurrent handshakes replace idle sockets without over-admitting or leaking leases", async () => {
  const opened = await fill();
  const added = await Promise.all(Array.from({ length: 16 }, () => connect()));
  await until(() => opened.filter(client => client.closed).length === 16, "all reclaimed clients closed");
  expect(opened.slice(0, 16).every(client => client.closed?.code === 1001)).toBe(true);
  expect(opened.slice(16).every(client => client.socket.readyState === WebSocket.OPEN)).toBe(true);
  expect(added.every(client => client.socket.readyState === WebSocket.OPEN)).toBe(true);
  expectBounded();
}, 15_000);

test("capacity pressure preserves a streaming response until it completes", async () => {
  const opened = await fill();
  await exchange(opened[0]!, { input: "hold-fixture-stream" }, "response.created");
  // Refresh every idle socket so the busy connection is the oldest by activity too.
  for (const client of opened.slice(1)) await exchange(client, { generate: false });
  await connect();
  await until(() => Boolean(opened[1]!.closed), "oldest eligible idle socket closed");
  expect(opened[0]!.socket.readyState).toBe(WebSocket.OPEN);
  finishStream!();
  await until(() => opened[0]!.frames.some(frame => frame.type === "response.completed"), "protected stream completes");
  expect(opened[0]!.frames.some(frame => frame.type === "error")).toBe(false);
  expectBounded();
}, 15_000);

test("client tool execution stays protected, and all-busy saturation keeps the 503 refusal", async () => {
  const opened = await fill();
  for (const client of opened) {
    await exchange(client, { input: "request-fixture-tool", tools: [{ type: "function", name: "fixture_tool",
      parameters: { type: "object", properties: {} } }] });
  }
  const rejectedBefore = codexWebSocketAdmissionMetrics().rejected;
  const response = await fetch(new URL("/v1/responses", proxy!.url), { headers: {
    connection: "upgrade", upgrade: "websocket", "sec-websocket-version": "13",
    "sec-websocket-key": Buffer.from("capacity-fixture").toString("base64"),
  } });
  expect(response.status).toBe(503);
  expect(response.headers.get("retry-after")).toBe("1");
  expect(await response.text()).toContain("Codex WebSockets capacity reached");
  expect(codexWebSocketAdmissionMetrics().rejected).toBe(rejectedBefore + 1);
  expect(opened.every(client => client.socket.readyState === WebSocket.OPEN)).toBe(true);
  await exchange(opened[0]!, { input: [{ type: "function_call_output", call_id: "call_capacity", output: "fixture tool result" }] });
  await connect();
  await until(() => Boolean(opened[0]!.closed), "completed tool workflow becomes reclaimable");
  expect(opened.slice(1).every(client => client.socket.readyState === WebSocket.OPEN)).toBe(true);
  expectBounded();
}, 20_000);
