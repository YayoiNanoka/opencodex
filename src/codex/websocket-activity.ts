import { createHash } from "node:crypto";

// Logical recency avoids clock adjustments and ties between messages in one millisecond.
let activityOrder = 0;
const MAX_PENDING_RESULTS = 1024;
const MAX_RESULT_ID_LENGTH = 1024;
const resultTypes: Readonly<Record<string, string>> = {
  function_call: "function_call_output",
  custom_tool_call: "custom_tool_call_output",
  local_shell_call: "function_call_output",
  shell_call: "shell_call_output",
  computer_call: "computer_call_output",
  apply_patch_call: "apply_patch_call_output",
  tool_search_call: "tool_search_output",
  mcp_approval_request: "mcp_approval_response",
};

function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function resultKey(type: string, id: string): string {
  return createHash("sha256").update(JSON.stringify([type, id])).digest("hex");
}

/** Content-free activity owned by one admitted incoming Responses socket. */
export class CodexWebSocketActivity {
  lastUsed = ++activityOrder;
  closing = false;
  private turns = 0;
  private pendingResults = new Set<string>();
  private returnedResults = new Set<string>();
  // Unknown/oversized identities cannot safely prove that client work finished.
  private untrackedResults = false;

  get idle(): boolean {
    return !this.closing && this.turns === 0 && this.pendingResults.size === 0 && !this.untrackedResults;
  }

  touch(): void {
    this.lastUsed = ++activityOrder;
  }

  /** Each turn retains its own busy reference, including superseded async cleanup. */
  beginTurn(input: unknown): () => void {
    this.turns += 1;
    this.touch();
    this.acceptResults(input);
    let finished = false;
    return () => {
      if (finished) return;
      finished = true;
      this.turns -= 1;
      if (this.turns === 0) this.touch();
    };
  }

  /** Called only after admission or native control accepts the client's results. */
  acceptResults(input: unknown): void {
    if (this.untrackedResults) return;
    if (Array.isArray(input)) {
      for (const item of input) {
        if (!record(item) || typeof item.type !== "string") continue;
        const id = item.type === "mcp_approval_response" ? item.approval_request_id : item.call_id;
        if (typeof id === "string" && id.length <= MAX_RESULT_ID_LENGTH) {
          // Ordinary Codex shell results use function_call_output; native steering
          // also accepts its explicit local_shell_call_output continuation stub.
          const type = item.type === "local_shell_call_output" ? "function_call_output" : item.type;
          const key = resultKey(type, id);
          if (this.pendingResults.delete(key)) {
            if (this.returnedResults.size >= MAX_PENDING_RESULTS) { this.protectUntracked(); return; }
            else this.returnedResults.add(key);
          }
        }
      }
    }
  }

  /** Preserve tool waits even when the response terminal omits its output array. */
  observePayload(payload: string): void {
    if (this.untrackedResults) return;
    let frame: unknown;
    try { frame = JSON.parse(payload); } catch { return; }
    if (!record(frame)) return;
    // Native injection can return a result before the same terminal repeats its call.
    if (frame.type === "response.created") this.returnedResults.clear();
    if (frame.type === "response.output_item.done") this.observeOutput(frame.item);
    if (["response.completed", "response.incomplete", "response.failed"].includes(String(frame.type))
      && record(frame.response) && Array.isArray(frame.response.output)) {
      for (const item of frame.response.output) this.observeOutput(item);
    }
  }

  private observeOutput(item: unknown): void {
    if (this.untrackedResults) return;
    if (!record(item) || typeof item.type !== "string") return;
    // Hosted execution may publish both the call and its result in the output stream.
    this.acceptResults([item]);
    if (!Object.hasOwn(resultTypes, item.type)) return;
    if (item.type === "tool_search_call" && item.execution !== "client") return;
    const id = item.type === "mcp_approval_request" ? item.id
      : item.type === "local_shell_call" || item.type === "tool_search_call" ? item.call_id ?? item.id : item.call_id;
    if (typeof id !== "string" || !id.length || id.length > MAX_RESULT_ID_LENGTH) {
      this.protectUntracked();
      return;
    }
    const key = resultKey(resultTypes[item.type]!, id);
    if (this.pendingResults.has(key) || this.returnedResults.has(key)) return;
    if (this.pendingResults.size >= MAX_PENDING_RESULTS) this.protectUntracked();
    else this.pendingResults.add(key);
  }

  private protectUntracked(): void {
    this.untrackedResults = true;
    this.pendingResults.clear();
    this.returnedResults.clear();
  }
}
