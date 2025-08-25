const TTL_MS = 20601000; // ~5.72h
const FLUSH_MS = 50;     // broadcast cadence
const SNAPSHOT_MS = 250; // storage write throttle
const LOG_PREFIX = "do-stream";

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

    if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS_HEADERS });

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
    this.buffer = [];
    this.full = "";
    this.seq = -1;
    this.phase = "idle";
    this.error = null;
    this.controller = null;
    this.savedAt = 0;
    this.requestMessages = [];
    this.lastUpdateAt = 0;

    // batching
    this.pendingText = "";
    this.flushTimer = null;
    this.flushing = false;

    // metrics for debugging
    this.metrics = {
      createdAt: Date.now(),
      deltas: 0,
      bytesIn: 0,
      linesParsed: 0,
      keepAlives: 0,
      jsonParses: 0,
      parseErrors: 0,
      snapshots: 0,
      wsBroadcasts: 0,
      flushes: 0,
      lastFlushAt: 0,
      readerReads: 0,
      readerDone: false,
      upstream: { status: null, reqId: null, model: null },
      terminal: { reason: null, at: 0 },
    };
  }

  log(ev, extra = {}) {
    try {
      console.log(JSON.stringify({ p: LOG_PREFIX, ev, rid: this.rid, phase: this.phase, t: Date.now(), ...extra }));
    } catch {}
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
    this.log("restore", { savedAt: this.savedAt, seq: this.seq, phase: this.phase });
  }

  maybeSaveSnapshot(force = false) {
    const now = Date.now();
    if (!force && now - this.savedAt < SNAPSHOT_MS) return;
    const data = {
      rid: this.rid,
      buffer: this.buffer,
      full: this.full,
      seq: this.seq,
      phase: this.phase,
      error: this.error,
      requestMessages: this.requestMessages,
      lastUpdateAt: this.lastUpdateAt,
      savedAt: now,
      // expose metrics to snapshot to help debugging
      metrics: this.metrics
    };
    this.savedAt = now;
    this.metrics.snapshots++;
    this.state.storage.put("run", data).catch(() => {});
  }

  async flushNow() {
    if (this.flushing) return;
    if (!this.pendingText) return;

    this.flushing = true;
    const d = this.pendingText;
    this.pendingText = "";

    try {
      const seq = ++this.seq;
      this.buffer.push({ seq, text: d });
      this.full += d;
      this.lastUpdateAt = Date.now();

      const msg = JSON.stringify({ type: "delta", seq, text: d });
      let fanout = 0;
      for (const ws of this.sockets) {
        try { ws.send(msg); fanout++; } catch {}
      }
      this.metrics.wsBroadcasts += fanout;
      this.metrics.flushes++;
      this.metrics.lastFlushAt = Date.now();
      this.log("flush", { seq, len: d.length, fanout });
      this.maybeSaveSnapshot(false);
    } finally {
      this.flushing = false;
    }
  }

  scheduleFlush() {
    if (this.flushTimer) return;
    this.flushTimer = setTimeout(() => {
      this.flushTimer = null;
      this.flushNow().catch(() => {});
      if (this.pendingText) this.scheduleFlush();
    }, FLUSH_MS);
  }

  async flushFinal() {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    await this.flushNow();
    this.maybeSaveSnapshot(true);
  }

  bcast(o) {
    const s = JSON.stringify(o);
    let n = 0;
    for (const ws of this.sockets) {
      try { ws.send(s); n++; } catch {}
    }
    if (o?.type === "done" || o?.type === "err") this.log("terminal_bcast", { type: o.type, fanout: n });
  }

  send(ws, o) {
    try { ws.send(JSON.stringify(o)); } catch {}
  }

  replay(ws, after) {
    let n = 0;
    for (const it of this.buffer) {
      if (it.seq > after) { this.send(ws, { type: "delta", seq: it.seq, text: it.text }); n++; }
    }
    if (this.phase === "done") this.send(ws, { type: "done" });
    if (this.phase === "error") this.send(ws, { type: "err", message: this.error });
    this.log("replay", { after, sent: n, phase: this.phase });
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
        ttlRemaining,
        metrics: this.metrics
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
      Promise.resolve(this.onMessage(server, e)).catch((err) => {
        this.log("onMessage_error", { err: String(err) });
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
    this.maybeSaveSnapshot(true);
    this.log("begin", { bodyExists: true });

    this.stream({ apiKey, body }).catch((e) => this.fail(String(e?.message || "stream_failed")));
  }

  async stream({ apiKey, body }) {
    const startedAt = Date.now();
    let res;
    try {
      res = await fetch("https://openrouter.ai/api/v1/chat/completions", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: "Bearer " + apiKey
        },
        body: JSON.stringify({ ...body, stream: true }),
        signal: this.controller.signal
      });
    } catch (e) {
      this.log("upstream_fetch_error", { err: String(e) });
      return this.fail("fetch_failed");
    }

    const reqId = res.headers.get("x-request-id") || res.headers.get("openrouter-request-id") || null;
    this.metrics.upstream.status = res.status;
    this.metrics.upstream.reqId = reqId;
    this.metrics.upstream.model = body.model || null;
    this.log("upstream_headers", { status: res.status, reqId });

    if (!res.ok || !res.body) {
      const t = await res.text().catch(() => "");
      this.log("upstream_http_error", { status: res.status, body: t.slice(0, 500) });
      return this.fail(t || ("HTTP " + res.status));
    }

    const dec = new TextDecoder();
    const reader = res.body.getReader();

    let buf = "";
    let doneSignal = false;

    const push = (d) => {
      this.pendingText += d;
      this.metrics.deltas++;
      this.scheduleFlush();
    };

    try {
      while (this.phase === "running") {
        const r = await reader.read().catch((e) => ({ done: true, err: e }));
        this.metrics.readerReads++;
        if (r.err) this.log("reader_read_error", { err: String(r.err) });
        if (r.done) { this.metrics.readerDone = true; break; }

        this.metrics.bytesIn += r.value?.byteLength ?? 0;
        buf += dec.decode(r.value, { stream: true });

        // Process complete lines (SSE)
        while (true) {
          const idx = buf.indexOf("\n");
          if (idx === -1) break;

          const rawLine = buf.slice(0, idx);
          buf = buf.slice(idx + 1);

          const line = rawLine.trimEnd();
          if (!line) continue;
          this.metrics.linesParsed++;

          if (line.startsWith(":")) { this.metrics.keepAlives++; continue; }

          if (line.startsWith("data:")) {
            const data = line.slice(5).trimStart();

            if (data === "[DONE]") {
              doneSignal = true;
              this.log("done_signal", {});
              break;
            }

            try {
              const j = JSON.parse(data);
              this.metrics.jsonParses++;
              const d = j?.choices?.[0]?.delta?.content ?? "";
              if (d) push(d);
              const fr = j?.choices?.[0]?.finish_reason;
              if (fr && fr !== null) {
                doneSignal = true;
                this.log("finish_reason", { finish_reason: fr });
                break;
              }
            } catch (e) {
              this.metrics.parseErrors++;
              this.log("json_parse_error", { msg: String(e), dataSample: data.slice(0, 120) });
            }
          }
        }

        if (doneSignal) break;
      }
    } finally {
      try { await reader.cancel(); } catch {}
    }

    await this.flushFinal();

    if (doneSignal) {
      this.finishInternal("DONE_OR_FINISH_REASON", startedAt);
    } else {
      this.failInternal("stream_incomplete", startedAt);
    }
  }

  finishInternal(reason, startedAt) {
    if (this.phase !== "running") return;
    this.phase = "done";
    try { this.controller?.abort(); } catch {}
    this.lastUpdateAt = Date.now();
    this.metrics.terminal = { reason, at: Date.now() };
    this.maybeSaveSnapshot(true);
    this.log("finish", {
      reason,
      elapsedMs: Date.now() - startedAt,
      deltas: this.metrics.deltas,
      bytesIn: this.metrics.bytesIn
    });
    this.bcast({ type: "done" });
  }

  async finish() { /* not used directly; keep for compatibility */ }

  stop() {
    if (this.phase !== "running") return;
    this.flushFinal().catch(() => {});
    this.phase = "done";
    try { this.controller?.abort(); } catch {}
    this.lastUpdateAt = Date.now();
    this.metrics.terminal = { reason: "CLIENT_STOP", at: Date.now() };
    this.maybeSaveSnapshot(true);
    this.bcast({ type: "done" });
  }

  failInternal(message, startedAt) {
    if (this.phase === "error") return;
    this.phase = "error";
    this.error = message;
    try { this.controller?.abort(); } catch {}
    this.lastUpdateAt = Date.now();
    this.metrics.terminal = { reason: message, at: Date.now() };
    this.maybeSaveSnapshot(true);
    this.log("fail", {
      message,
      elapsedMs: Date.now() - startedAt,
      deltas: this.metrics.deltas,
      bytesIn: this.metrics.bytesIn,
      lastFlushAt: this.metrics.lastFlushAt,
      readerReads: this.metrics.readerReads
    });
    this.bcast({ type: "err", message });
  }

  async fail(message) { this.failInternal(message, Date.now()); }
}
