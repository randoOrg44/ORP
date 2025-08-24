// src/index.js
// Assumes wrangler.toml has a binding:
// [durable_objects]
// bindings = [{ name = "MY_DURABLE_OBJECT", class_name = "MyDurableObject" }]

export default {
  async fetch(req, env) {
    const url = new URL(req.url);

    // WebSocket entrypoint; path includes a "room" or "thread" id you choose.
    // e.g. wss://<your-domain>/ws/foo
    if (url.pathname.startsWith("/ws/")) {
      const name = url.pathname.split("/").pop() || "default";
      const id = env.MY_DURABLE_OBJECT.idFromName(name);
      const stub = env.MY_DURABLE_OBJECT.get(id);
      return stub.fetch(req);
    }

    // (Optional) health
    if (url.pathname === "/health") return new Response("ok");

    return new Response("not found", { status: 404 });
  }
}

/**
 * Durable Object that:
 * - Accepts WebSocket connections
 * - Starts an OpenRouter streaming run exactly once per runId ("begin")
 * - Buffers every delta with an incremental seq
 * - Replays backlog on "resume"
 *
 * Messages over WS (JSON):
 *  Client -> DO:
 *    {type:"begin", runId, after?:number, model, messages, apiKey}
 *    {type:"resume", runId, after:number}
 *  DO -> Client:
 *    {type:"delta", runId, seq:number, text:string}
 *    {type:"done",  runId}
 *    {type:"err",   runId, message}
 */
export class MyDurableObject {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    /** Map<runId, Run> */
    this.runs = new Map();
  }

  async fetch(req) {
    // Only WS here.
    if (req.headers.get("Upgrade") !== "websocket")
      return new Response("Expected websocket", { status: 426 });

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();

    // Light ping/pong to keep connections healthy (helps some proxies)
    const ping = setInterval(() => {
      try { server.send(JSON.stringify({ type: "ping" })); } catch {}
    }, 30000);

    server.addEventListener("message", (evt) => this.#onMessage(server, evt));
    server.addEventListener("close", () => clearInterval(ping));
    server.addEventListener("error", () => clearInterval(ping));

    return new Response(null, { status: 101, webSocket: client });
  }

  async #onMessage(ws, evt) {
    let msg;
    try { msg = JSON.parse(String(evt.data || "")); }
    catch { return ws.send(JSON.stringify({ type: "err", message: "bad json" })); }

    const t = msg?.type;
    if (t !== "begin" && t !== "resume") {
      return ws.send(JSON.stringify({ type: "err", message: "bad type" }));
    }

    const runId = String(msg.runId || "");
    if (!runId) return ws.send(JSON.stringify({ type: "err", message: "missing runId" }));

    // Get or create the run record
    let run = this.runs.get(runId);
    if (!run) {
      run = {
        buffer: [],          // [{seq, text}]
        seq: 0,
        sockets: new Set(),  // live clients
        started: false,
        finished: false,
        error: null,
        // (Optional) persist some metadata in storage if you want cross-restart resume
      };
      this.runs.set(runId, run);
    }

    // Track this ws in the run
    run.sockets.add(ws);
    ws.addEventListener("close", () => run.sockets.delete(ws));

    const after = Number.isFinite(+msg.after) ? +msg.after : -1;

    if (t === "begin") {
      if (run.started) {
        // Already running. Just replay backlog from 'after' and follow the stream.
        this.#replay(run, ws, after);
        if (run.finished) ws.send(JSON.stringify({ type: "done", runId }));
        return;
      }

      // Start upstream stream once
      run.started = true;
      const { model, messages, apiKey } = msg;

      // Simple guards
      if (!apiKey) return this.#failRun(runId, run, "Missing apiKey");
      if (!model) return this.#failRun(runId, run, "Missing model");
      if (!Array.isArray(messages) || !messages.length) {
        return this.#failRun(runId, run, "Missing messages");
      }

      // Kick off upstream streaming (no await; fire-and-forget with catch)
      this.#replay(run, ws, after);
      this.#streamFromOpenRouter(runId, run, { apiKey, model, messages })
        .catch(err => this.#failRun(runId, run, err?.message || "stream failed"));
    } else if (t === "resume") {
      // Just replay from 'after'. If still streaming, the client will keep receiving.
      this.#replay(run, ws, after);
      if (run.finished) ws.send(JSON.stringify({ type: "done", runId }));
      if (run.error) ws.send(JSON.stringify({ type: "err", runId, message: run.error }));
    }
  }

  #replay(run, ws, after) {
    // Send buffered deltas with seq > after
    for (const item of run.buffer) {
      if (item.seq > after) {
        try { ws.send(JSON.stringify({ type: "delta", runId: item.runId, seq: item.seq, text: item.text })); }
        catch {}
      }
    }
  }

  async #streamFromOpenRouter(runId, run, { apiKey, model, messages }) {
    const body = JSON.stringify({
      model,
      messages,
      stream: true
    });

    const res = await fetch("https://openrouter.ai/api/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + apiKey
      },
      body
    });

    if (!res.ok || !res.body) {
      const errText = await res.text().catch(() => "");
      this.#failRun(runId, run, errText || `HTTP ${res.status}`);
      return;
    }

    const dec = new TextDecoder();
    const reader = res.body.getReader();
    let buffer = "";

    // Parse SSE: "\n\n" delimited lines, "data: {...}"
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += dec.decode(value, { stream: true });

      let idx;
      while ((idx = buffer.indexOf("\n\n")) !== -1) {
        const raw = buffer.slice(0, idx).trim();
        buffer = buffer.slice(idx + 2);

        if (!raw) continue;
        if (!raw.startsWith("data:")) continue;

        const payload = raw.slice(5).trim();
        if (payload === "[DONE]") {
          this.#finishRun(runId, run);
          break;
        }

        try {
          const json = JSON.parse(payload);
          const delta = json?.choices?.[0]?.delta?.content ?? "";
          if (delta) {
            const seq = ++run.seq;
            // Buffer the delta
            run.buffer.push({ runId, seq, text: delta });
            // (Optional) Persist buffer in storage for cross-restart resume:
            // await this.state.storage.put(`${runId}:${seq}`, delta);

            // Broadcast to all connected sockets for this run
            const msg = JSON.stringify({ type: "delta", runId, seq, text: delta });
            for (const sock of run.sockets) {
              try { sock.send(msg); } catch {}
            }
          }

          const finish = json?.choices?.[0]?.finish_reason;
          if (finish) {
            this.#finishRun(runId, run);
            break;
          }
        } catch (e) {
          // ignore parse errors of stray lines
        }
      }
    }

    // safety close if stream ended without explicit finish
    if (!run.finished && !run.error) this.#finishRun(runId, run);
  }

  #finishRun(runId, run) {
    if (run.finished) return;
    run.finished = true;
    const msg = JSON.stringify({ type: "done", runId });
    for (const sock of run.sockets) {
      try { sock.send(msg); } catch {}
    }
    // You can compact buffer or persist final here if desired.
  }

  #failRun(runId, run, message) {
    run.error = String(message || "error");
    const msg = JSON.stringify({ type: "err", runId, message: run.error });
    for (const sock of run.sockets) {
      try { sock.send(msg); } catch {}
    }
  }
}
