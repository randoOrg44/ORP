// src/index.js
import OpenAI from 'openai';

// How long a saved run snapshot is considered fresh (for cold restore)
const TTL_MS = 20 * 60 * 1000;

// Batching/Throttling
const BATCH_MS = 60;            // how often to flush accumulated deltas (ms)
const BATCH_BYTES = 2048;       // force flush if pending exceeds this many bytes
const SNAPSHOT_MIN_MS = 1500;   // minimum spacing between snapshots (ms)

const corsHeaders = () => ({
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Max-Age': '86400',
});

function withCORS(resp) {
  const hdrs = new Headers(resp.headers || {});
  for (const [k, v] of Object.entries(corsHeaders())) hdrs.set(k, v);
  return new Response(resp.body, { status: resp.status, statusText: resp.statusText, headers: hdrs });
}

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const method = req.method.toUpperCase();

    // CORS preflight support
    if (method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders() });
    }

    if (url.pathname === '/ws') {
      const raw = url.searchParams.get('uid') || 'anon';
      const uid = String(raw).slice(0, 64).replace(/[^a-zA-Z0-9_-]/g, '') || 'anon';
      const id = env.MY_DURABLE_OBJECT.idFromName(uid);
      const stub = env.MY_DURABLE_OBJECT.get(id);

      // Forward both websocket upgrades and HTTP GET to the DO.
      if (req.headers.get('Upgrade') === 'websocket') {
        return stub.fetch(req);
      }
      if (method === 'GET') {
        const r = await stub.fetch(req);
        return withCORS(r);
      }
      return withCORS(new Response('method not allowed', { status: 405 }));
    }

    return withCORS(new Response('not found', { status: 404 }));
  }
}

/**
 * Durable Object with provider-aware routing (OpenRouter or OpenAI).
 *
 * The WebSocket "begin" message should include:
 * - provider: "openrouter" | "openai" (defaults to "openrouter" if missing/invalid)
 * - apiKey: string
 * - or_body: the chat completions body built by the client (model, messages, stream, sampling)
 *   (for backward compatibility, we also accept "body" or { model, messages } combo)
 *
 * Parameter handling:
 * - Shared and passed to BOTH OpenRouter and OpenAI:
 *   - temperature
 *   - top_p
 *   - frequency_penalty
 *   - presence_penalty
 *   - max_tokens
 *   - reasoning.effort (if present)
 *   - verbosity (if present)
 *
 * - OpenRouter-only (removed for OpenAI, passed through for OpenRouter):
 *   - top_k
 *   - repetition_penalty
 *   - min_p
 *   - top_a
 *
 * Anything else provided by the client is passed through unchanged for OpenRouter.
 */
export class MyDurableObject {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    this.sockets = new Set();
    this.reset();
  }

  reset() {
    this.rid = null;
    this.provider = 'openrouter'; // default unless begin specifies openai
    this.buffer = [];        // [{ seq, text }]
    this.seq = -1;
    this.phase = 'idle';     // 'idle' | 'running' | 'done' | 'error'
    this.error = null;
    this.controller = null;

    // Batching/throttling
    this.pending = '';        // text waiting to be flushed/broadcasted
    this.flushTimer = null;
    this.lastSavedAt = 0;
    this.lastFlushedAt = 0;
  }

  corsJSON(obj, status = 200) {
    return new Response(JSON.stringify(obj), {
      status,
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-store',
        ...corsHeaders(),
      }
    });
  }

  send(ws, obj) {
    try { ws.send(JSON.stringify(obj)); } catch {}
  }

  bcast(obj) {
    const s = JSON.stringify(obj);
    for (const ws of this.sockets) {
      try { ws.send(s); } catch {}
    }
  }

  async restoreIfCold() {
    if (this.rid) return;
    const snap = await this.state.storage.get('run').catch(() => null);
    const fresh = snap && (Date.now() - (snap.savedAt || 0) < TTL_MS);
    if (!fresh) {
      if (snap) await this.state.storage.delete('run').catch(() => {});
      return;
    }
    this.rid = snap.rid || null;
    this.provider = snap.provider || 'openrouter';
    this.buffer = Array.isArray(snap.buffer) ? snap.buffer : [];
    this.seq = Number.isFinite(+snap.seq) ? +snap.seq : -1;
    this.phase = snap.phase || 'done';
    this.error = snap.error || null;
    this.pending = ''; // pending is never persisted
  }

  saveSnapshot() {
    const data = {
      rid: this.rid,
      provider: this.provider,
      buffer: this.buffer,
      seq: this.seq,
      phase: this.phase,
      error: this.error,
      savedAt: Date.now()
    };
    this.lastSavedAt = Date.now();
    this.state.storage.put('run', data).catch(() => {});
  }

  saveSnapshotThrottled() {
    const now = Date.now();
    if (now - this.lastSavedAt >= SNAPSHOT_MIN_MS) {
      this.saveSnapshot();
    }
  }

  replay(ws, after) {
    for (const it of this.buffer) {
      if (it.seq > after) this.send(ws, { type: 'delta', seq: it.seq, text: it.text });
    }
    if (this.phase === 'done') this.send(ws, { type: 'done' });
    if (this.phase === 'error') this.send(ws, { type: 'err', message: this.error });
  }

  flush(force = false) {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }

    if (this.pending) {
      const text = this.pending;
      this.pending = '';
      const seq = ++this.seq;
      const item = { seq, text };
      this.buffer.push(item);
      this.bcast({ type: 'delta', seq, text });
      this.lastFlushedAt = Date.now();
    }

    if (force) {
      this.saveSnapshot();
    } else {
      this.saveSnapshotThrottled();
    }
  }

  queueDelta(text) {
    if (!text) return;
    this.pending += text;

    if (this.pending.length >= BATCH_BYTES) {
      this.flush(false);
      return;
    }

    if (!this.flushTimer) {
      this.flushTimer = setTimeout(() => this.flush(false), BATCH_MS);
    }
  }

  // Append an end reason to the last chat (always as a delta at the end)
  appendEndReason(reason) {
    const clean = String(reason || 'unknown').replace(/\s+/g, ' ').trim().slice(0, 128);
    this.queueDelta(` [end:${clean}]`);
  }

  async fetch(req) {
    const url = new URL(req.url);
    const upgrade = req.headers.get('Upgrade');

    // CORS preflight
    if (req.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders() });
    }

    // WebSocket path
    if (url.pathname === '/ws' && upgrade === 'websocket') {
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      server.accept();

      this.sockets.add(server);
      server.addEventListener('close', () => this.sockets.delete(server));
      server.addEventListener('message', (e) => this.onMessage(server, e));

      return new Response(null, { status: 101, webSocket: client });
    }

    // HTTP GET: aggregated transcript for this DO (uid)
    if (req.method === 'GET') {
      await this.restoreIfCold();
      const text = (this.buffer.map(it => it.text).join('') + (this.pending || ''));
      const payload = {
        rid: this.rid,
        provider: this.provider,
        seq: this.seq,
        phase: this.phase,
        done: this.phase === 'done',
        error: this.phase === 'error' ? this.error : null,
        text
      };
      return this.corsJSON(payload, 200);
    }

    return this.corsJSON({ error: 'not allowed' }, 405);
  }

  normalizeProvider(p) {
    const v = String(p || '').toLowerCase();
    return v === 'openai' ? 'openai' : 'openrouter';
  }

  /**
   * Sanitize/shape the body for the selected provider.
   * - Always enforces stream: true
   * - OpenAI: strips params it doesn't accept (top_k, repetition_penalty, min_p, top_a)
   */
  sanitizeBodyForProvider(provider, input) {
    const body = { ...(input || {}) };

    // Ensure stream is on
    body.stream = true;

    if (provider === 'openai') {
      // Remove OpenRouter-only sampling params not accepted by OpenAI Chat Completions:
      //   - top_k
      //   - repetition_penalty
      //   - min_p
      //   - top_a
      delete body.top_k;
      delete body.repetition_penalty;
      delete body.min_p;
      delete body.top_a;

      // Allowed/passed-through to OpenAI (from what we use):
      // - model
      // - messages
      // - stream
      // - temperature
      // - top_p
      // - frequency_penalty
      // - presence_penalty
      // - max_tokens
      // - reasoning: { effort }  (OAI accepts effort)
      // - verbosity              (OAI accepts)
      //
      // We leave other common fields alone if present (tools, tool_choice, stop, etc.).
    } else {
      // provider === 'openrouter'
      // Pass-through all parameters — OpenRouter supports the extended set:
      // - temperature, top_p, top_k, frequency_penalty, presence_penalty,
      //   repetition_penalty, min_p, top_a, max_tokens, reasoning.effort, verbosity, etc.
    }

    return body;
  }

  async onMessage(ws, evt) {
    await this.restoreIfCold();

    let m;
    try {
      m = JSON.parse(String(evt.data || ''));
    } catch {
      return this.send(ws, { type: 'err', message: 'bad_json' });
    }

    if (m.type === 'resume') {
      if (!m.rid || m.rid !== this.rid) return this.send(ws, { type: 'err', message: 'stale_run' });
      const after = Number.isFinite(+m.after) ? +m.after : -1;
      return this.replay(ws, after);
    }

    if (m.type === 'stop') {
      if (m.rid && m.rid === this.rid) this.stop('client');
      return;
    }

    if (m.type !== 'begin') return this.send(ws, { type: 'err', message: 'bad_type' });

    const {
      rid,
      apiKey,
      provider: provRaw,
      or_body,              // frontend sends 'or_body'
      body,                 // also accept 'body' if present
      model,
      messages,
      after
    } = m;

    const provider = this.normalizeProvider(provRaw);
    // Accept or_body, or generic body, or last-resort fallback { model, messages }
    const rawBody = or_body || body || (model && Array.isArray(messages) ? { model, messages, stream: true } : null);

    if (!rid || !apiKey || !rawBody || !Array.isArray(rawBody.messages) || rawBody.messages.length === 0) {
      return this.send(ws, { type: 'err', message: 'missing_fields' });
    }

    if (this.phase === 'running' && rid !== this.rid) {
      return this.send(ws, { type: 'err', message: 'busy' });
    }

    if (rid === this.rid && this.phase !== 'idle') {
      const a = Number.isFinite(+after) ? +after : -1;
      return this.replay(ws, a);
    }

    // New run
    this.reset();
    this.rid = rid;
    this.provider = provider;
    this.phase = 'running';
    this.controller = new AbortController();
    this.saveSnapshot();

    const cleanBody = this.sanitizeBodyForProvider(provider, rawBody);
    this.stream({ provider, apiKey, body: cleanBody }).catch(e => this.fail(String(e?.message || 'stream_failed')));
  }

  async stream({ provider, apiKey, body }) {
    const client = new OpenAI({
      apiKey,
      baseURL: provider === 'openai' ? 'https://api.openai.com/v1' : 'https://openrouter.ai/api/v1',
    });

    let finishReason = null;

    try {
      const params = { ...body, stream: true };
      const stream = await client.chat.completions.create(params, { signal: this.controller.signal });

      for await (const chunk of stream) {
        if (this.phase !== 'running') {
          // likely stopped elsewhere; just break
          break;
        }
        const delta = chunk?.choices?.[0]?.delta?.content ?? '';
        if (delta) this.queueDelta(delta);
        const fr = chunk?.choices?.[0]?.finish_reason;
        if (fr) finishReason = fr;
      }
    } catch (e) {
      // If aborted due to stop(), do nothing here; stop() handles appending reason and finishing.
      if (this.phase === 'running') {
        const msg = String(e?.message || 'stream_failed');
        if ((e && e.name === 'AbortError') || /abort/i.test(msg)) {
          // stop() will append reason and finalize
          return;
        }
        return this.fail(msg);
      }
      return;
    }

    // Normal finish via streaming end
    if (this.phase === 'running') {
      this.finish(finishReason || 'done');
    }
  }

  finish(reason = 'done') {
    if (this.phase !== 'running') return;
    this.appendEndReason(reason);
    this.flush(true);
    this.phase = 'done';
    try { this.controller?.abort(); } catch {}
    this.saveSnapshot();
    this.bcast({ type: 'done' });
  }

  stop(origin = 'client') {
    if (this.phase !== 'running') return;
    // Append reason and finalize
    this.appendEndReason(`stop:${origin}`);
    this.flush(true);
    this.phase = 'done';
    try { this.controller?.abort(); } catch {}
    this.saveSnapshot();
    this.bcast({ type: 'done' });
  }

  fail(message) {
    if (this.phase === 'error') return;
    const reason = `error:${String(message || 'unknown')}`.replace(/\s+/g, ' ').trim().slice(0, 200);

    if (this.phase === 'running') {
      // Append reason to the last chat if stream was active
      this.appendEndReason(reason);
      this.flush(true);
    } else {
      // Ensure any pending is flushed so GET shows current text
      this.flush(true);
    }

    this.phase = 'error';
    this.error = String(message || 'stream_failed');
    try { this.controller?.abort(); } catch {}
    this.saveSnapshot();
    this.bcast({ type: 'err', message: this.error });
  }
}
