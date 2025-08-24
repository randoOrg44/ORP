export default {
  async fetch(req, env) {
    const url = new URL(req.url);

    if (url.pathname === "/ws") {
      const id = env.MY_DURABLE_OBJECT.idFromName("singleton");
      const stub = env.MY_DURABLE_OBJECT.get(id);
      return stub.fetch(req);
    }

    return new Response("not found", { status: 404 });
  }
}

/**
 * Single-run Durable Object:
 * - buffers deltas with seq++
 * - begin(apiKey, model, messages)
 * - resume(after)
 */
export class MyDurableObject {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    this.buffer = [];       // [{seq, text}]
    this.seq = -1;
    this.started = false;
    this.finished = false;
    this.error = null;
    this.sockets = new Set();
  }

  async fetch(req) {
    if (req.headers.get("Upgrade") !== "websocket")
      return new Response("Expected websocket", { status: 426 });

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();

    this.sockets.add(server);
    server.addEventListener("close", () => this.sockets.delete(server));
    server.addEventListener("message", (evt) => this.#onMessage(server, evt));

    return new Response(null, { status: 101, webSocket: client });
  }

  #send(ws, obj) { try { ws.send(JSON.stringify(obj)); } catch {} }
  #broadcast(obj) { const s = JSON.stringify(obj); for (const ws of this.sockets) { try { ws.send(s); } catch {} } }

  #replay(ws, after) {
    for (const it of this.buffer) if (it.seq > after) this.#send(ws, { type: "delta", seq: it.seq, text: it.text });
    if (this.finished) this.#send(ws, { type: "done" });
    if (this.error) this.#send(ws, { type: "err", message: this.error });
  }

  async #onMessage(ws, evt) {
    let msg; try { msg = JSON.parse(String(evt.data || "")); } catch { return this.#send(ws, { type: "err", message: "bad json" }); }
    if (msg?.type === "resume") {
      const after = Number.isFinite(+msg.after) ? +msg.after : -1;
      return this.#replay(ws, after);
    }
    if (msg?.type !== "begin") return this.#send(ws, { type: "err", message: "bad type" });

    const { apiKey, model, messages, after } = msg;
    if (!apiKey || !model || !Array.isArray(messages) || !messages.length)
      return this.#send(ws, { type: "err", message: "missing fields" });

    // If already running, just replay and tail.
    this.#replay(ws, Number.isFinite(+after) ? +after : -1);
    if (this.started) return;

    // Reset state for a fresh run.
    this.buffer = [];
    this.seq = -1;
    this.finished = false;
    this.error = null;
    this.started = true;

    // Stream from OpenRouter
    this.#streamOpenRouter({ apiKey, model, messages }).catch((e) => this.#fail(String(e?.message || "stream failed")));
  }

  async #streamOpenRouter({ apiKey, model, messages }) {
    const res = await fetch("https://openrouter.ai/api/v1/chat/completions", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": "Bearer " + apiKey },
      body: JSON.stringify({ model, messages, stream: true })
    });
    if (!res.ok || !res.body) {
      const t = await res.text().catch(() => "");
      return this.#fail(t || `HTTP ${res.status}`);
    }

    const dec = new TextDecoder();
    const reader = res.body.getReader();
    let buf = "";

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });

      let i;
      while ((i = buf.indexOf("\n\n")) !== -1) {
        const chunk = buf.slice(0, i).trim();
        buf = buf.slice(i + 2);
        if (!chunk || !chunk.startsWith("data:")) continue;

        const data = chunk.slice(5).trim();
        if (data === "[DONE]") return this.#finish();
        try {
          const j = JSON.parse(data);
          const delta = j?.choices?.[0]?.delta?.content ?? "";
          if (delta) {
            const seq = ++this.seq;
            this.buffer.push({ seq, text: delta });
            this.#broadcast({ type: "delta", seq, text: delta });
          }
          const doneReason = j?.choices?.[0]?.finish_reason;
          if (doneReason) return this.#finish();
        } catch { /* ignore */ }
      }
    }
    this.#finish();
  }

  #finish() {
    if (this.finished) return;
    this.finished = true;
    this.#broadcast({ type: "done" });
  }

  #fail(message) {
    this.error = message;
    this.#broadcast({ type: "err", message });
    this.finished = true;
  }
}
