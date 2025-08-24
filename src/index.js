const TTL_MS = 20601000; // ~5.72h

const CORS_HEADERS = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET,OPTIONS",
  "access-control-allow-headers": "*",
};

function json(data, status = 200, extraHeaders = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
      ...CORS_HEADERS,
      ...extraHeaders
    }
  });
}

function text(s, status = 200, extraHeaders = {}) {
  return new Response(String(s), {
    status,
    headers: {
      "content-type": "text/plain; charset=utf-8",
      "cache-control": "no-store",
      ...CORS_HEADERS,
      ...extraHeaders
    }
  });
}

export default {
  async fetch(req, env) {
    const u = new URL(req.url);

    if (req.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    if (u.pathname === "/ws") {
      const raw = u.searchParams.get("uid") || "anon";
      const uid = String(raw).slice(0, 64).replace(/[^a-zA-Z0-9_-]/g, "") || "anon";
      const id = env.MY_DURABLE_OBJECT.idFromName(uid);
      return env.MY_DURABLE_OBJECT.get(id).fetch(req);
    }

    if (u.pathname.startsWith("/chat/") && req.method === "GET") {
      const raw = u.pathname.split("/")[2] || "";
      const uid = String(raw).slice(0, 64).replace(/[^a-zA-Z0-9_-]/g, "") || "anon";
      const id = env.MY_DURABLE_OBJECT.idFromName(uid);
      const r = new Request(new URL("https://do/snapshot"), { method: "GET", headers: req.headers });
      return env.MY_DURABLE_OBJECT.get(id).fetch(r);
    }

    const parts = u.pathname.split("/").filter(Boolean);
    if (req.method === "GET" && parts.length === 1 && parts[0] !== "ws" && parts[0] !== "chat") {
      const raw = parts[0] || "";
      const uid = String(raw).slice(0, 64).replace(/[^a-zA-Z0-9_-]/g, "") || "anon";
      const id = env.MY_DURABLE_OBJECT.idFromName(uid);
      const r = new Request(new URL("https://do/snapshot"), { method: "GET", headers: req.headers });
      return env.MY_DURABLE_OBJECT.get(id).fetch(r);
    }

    if (u.pathname === "/healthz") return text("ok");
    return text("not found", 404);
  }
};

export class MyDurableObject {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sockets = new Set();
    this.reset();
  }

  reset() {
    this.rid = null;
    this.buffer = [];       // [{seq, text}]
    this.full = "";
    this.seq = -1;
    this.phase = "idle";    // idle | running | done | error
    this.error = null;
    this.controller = null;
    this.savedAt = 0;
    this.requestMessages = [];
    this.lastUpdateAt = 0;
  }

  async restoreIfCold() {
    if (this.rid) return;
    const snap = await this.state.storage.get("run").catch(() => null);
    const fresh = snap && Date.now() - (snap.savedAt || 0) < TTL_MS;
    if (!fresh) {
      if (snap) await this.state.storage.delete("run").catch(() => {});
      return;
    }
    this.rid = snap.rid || null;
    this.buffer = Array.isArray(snap.buffer) ? snap.buffer : [];
    this.full = typeof snap.full === "string" ? snap.full : (this.buffer.map(b => b.text).join("") || "");
    this.seq = Number.isFinite(+snap.seq) ? +snap.seq : -1;
    this.phase = snap.phase || "done";
    this.error = snap.error || null;
    this.savedAt = Number.isFinite(+snap.savedAt) ? +snap.savedAt : Date.now();
    this.requestMessages = Array.isArray(snap.requestMessages) ? snap.requestMessages : [];
    this.lastUpdateAt = Number.isFinite(+snap.lastUpdateAt) ? +snap.lastUpdateAt : this.savedAt;
  }

  saveSnapshot() {
    const data = {
      rid: this.rid,
      buffer: this.buffer,
      full: this.full,
      seq: this.seq,
      phase: this.phase,
      error: this.error,
      requestMessages: this.requestMessages,
      lastUpdateAt: this.lastUpdateAt,
      savedAt: Date.now()
    };
    this.savedAt = data.savedAt;
    this.state.storage.put("run", data).catch(() => {});
  }

  bcast(o) {
    const s = JSON.stringify(o);
    for (const ws of this.sockets) {
      try { ws.send(s); } catch {}
    }
  }

  send(ws, o) {
    try { ws.send(JSON.stringify(o)); } catch {}
  }

  replay(ws, after) {
    for (const it of this.buffer) {
      if (it.seq > after) this.send(ws, { type: "delta", seq: it.seq, text: it.text });
    }
    if (this.phase === "done") this.send(ws, { type: "done" });
    if (this.phase === "error") this.send(ws, { type: "err", message: this.error });
  }

  async fetch(req) {
    const u = new URL(req.url);

    if (req.method === "GET" && u.pathname === "/snapshot" && req.headers.get("Upgrade") !== "websocket") {
      await this.restoreIfCold();
      const ttlRemaining = Math.max(0, (this.savedAt || 0) + TTL_MS - Date.now());
      return json({
        rid: this.rid,
        phase: this.phase,
        seq: this.seq,
        error: this.error,
        full: this.full || "",
        buffer_len: Array.isArray(this.buffer) ? this.buffer.length : 0,
        messages: this.requestMessages || [],
        savedAt: this.savedAt || 0,
        lastUpdateAt: this.lastUpdateAt || 0,
        ttlRemaining
      });
    }

    if (req.method !== "GET" || req.headers.get("Upgrade") !== "websocket") {
      return text("Expected websocket", 426);
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    server.accept();
    this.sockets.add(server);

    const onCloseOrError = () => {
      this.sockets.delete(server);
      server.removeEventListener("close", onCloseOrError);
      server.removeEventListener("error", onCloseOrError);
    };

    server.addEventListener("close", onCloseOrError);
    server.addEventListener("error", onCloseOrError);

    server.addEventListener("message", (e) => {
      Promise.resolve(this.onMessage(server, e)).catch(() => {
        this.send(server, { type: "err", message: "internal_error" });
      });
    });

    return new Response(null, { status: 101, webSocket: client });
  }

  async onMessage(ws, evt) {
    await this.restoreIfCold();

    let m;
    try { m = JSON.parse(String(evt.data || "")); }
    catch { return this.send(ws, { type: "err", message: "bad_json" }); }

    if (m.type === "resume") {
      if (!m.rid || m.rid !== this.rid) return this.send(ws, { type: "err", message: "stale_run" });
      const a = Number.isFinite(+m.after) ? +m.after : -1;
      return this.replay(ws, a);
    }

    if (m.type === "stop") {
      if (m.rid && m.rid === this.rid) this.stop("client");
      return;
    }

    if (m.type !== "begin") return this.send(ws, { type: "err", message: "bad_type" });

    const { rid, apiKey, or_body, model, messages, after } = m;
    const body = or_body || (model && Array.isArray(messages) ? { model, messages, stream: true } : null);

    if (!rid || !apiKey || !body || !Array.isArray(body.messages) || !body.messages.length) {
      return this.send(ws, { type: "err", message: "missing_fields" });
    }

    if (this.phase === "running" && rid !== this.rid) {
      return this.send(ws, { type: "err", message: "busy" });
    }

    if (rid === this.rid && this.phase !== "idle") {
      const a = Number.isFinite(+after) ? +after : -1;
      return this.replay(ws, a);
    }

    // Fresh run
    this.reset();
    this.rid = rid;
    this.phase = "running";
    this.controller = new AbortController();
    this.requestMessages = Array.isArray(body.messages) ? body.messages : [];
    this.full = "";
    this.lastUpdateAt = Date.now();
    this.saveSnapshot();

    this.stream({ apiKey, body }).catch((e) => this.fail(String(e?.message || "stream_failed")));
  }

  async stream({ apiKey, body }) {
    const res = await fetch("https://openrouter.ai/api/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: "Bearer " + apiKey
      },
      body: JSON.stringify({ ...body, stream: true }),
      signal: this.controller.signal
    }).catch((e) => ({
      ok: false,
      status: 0,
      body: null,
      text: async () => String(e?.message || "fetch_failed")
    }));

    if (!res.ok || !res.body) {
      const t = await res.text().catch(() => "");
      return this.fail(t || "HTTP " + res.status);
    }

    const dec = new TextDecoder();
    const reader = res.body.getReader();

    let buf = "";
    let doneSignal = false;

    const push = (d) => {
      const seq = ++this.seq;
      this.buffer.push({ seq, text: d });
      this.full += d;
      this.lastUpdateAt = Date.now();
      this.bcast({ type: "delta", seq, text: d });
      this.saveSnapshot();
    };

    try {
      while (this.phase === "running") {
        const { done, value } = await reader.read().catch(() => ({ done: true }));
        if (done) break;

        buf += dec.decode(value, { stream: true });

        // Process complete lines
        // Per OpenRouter docs: parse by newline; lines start with "data: " or ":" comments; [DONE] terminates.
        while (true) {
          const idx = buf.indexOf("\n");
          if (idx === -1) break;

          const rawLine = buf.slice(0, idx);
          buf = buf.slice(idx + 1);

          const line = rawLine.trimEnd(); // keep leading spaces for "data: "
          if (!line) continue;

          if (line.startsWith(":")) {
            // SSE comment keep-alive, ignore.
            continue;
          }

          if (line.startsWith("data:")) {
            const data = line.slice(5).trimStart();

            if (data === "[DONE]") {
              doneSignal = true;
              break; // exit inner; outer will break on next iteration
            }

            try {
              const j = JSON.parse(data);
              const d = j?.choices?.[0]?.delta?.content ?? "";
              if (d) push(d);
              const fr = j?.choices?.[0]?.finish_reason;
              if (fr && fr !== null) {
                doneSignal = true;
                break;
              }
            } catch {
              // ignore partial or non-JSON payloads
            }
          }
        }

        if (doneSignal) break;
      }
    } finally {
      try { await reader.cancel(); } catch {}
    }

    // After stream closes, decide terminal state
    if (doneSignal) {
      this.finish();
    } else {
      // No explicit [DONE] or finish_reason → treat as incomplete
      this.fail("stream_incomplete");
    }
  }

  finish() {
    if (this.phase !== "running") return;
    this.phase = "done";
    try { this.controller?.abort(); } catch {}
    this.lastUpdateAt = Date.now();
    this.saveSnapshot();
    this.bcast({ type: "done" });
  }

  stop() {
    if (this.phase !== "running") return;
    this.phase = "done";
    try { this.controller?.abort(); } catch {}
    this.lastUpdateAt = Date.now();
    this.saveSnapshot();
    this.bcast({ type: "done" });
  }

  fail(message) {
    if (this.phase === "error") return;
    this.phase = "error";
    this.error = message;
    try { this.controller?.abort(); } catch {}
    this.lastUpdateAt = Date.now();
    this.saveSnapshot();
    this.bcast({ type: "err", message });
  }
}
