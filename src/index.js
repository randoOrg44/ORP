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

export class MyDurableObject {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    this.sockets = new Set();
    this.reset();
  }

  reset() {
    this.rid = null;
    this.buffer = [];        // [{ seq, text }]
    this.seq = -1;
    this.phase = 'idle';     // 'idle' | 'running' | 'done' | 'error'
    this.error = null;
    this.controller = null;

    // OpenAI stream handle (to allow abort)
    this.oaStream = null;

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
    this.buffer = Array.isArray(snap.buffer) ? snap.buffer : [];
    this.seq = Number.isFinite(+snap.seq) ? +snap.seq : -1;
    this.phase = snap.phase || 'done';
    this.error = snap.error || null;
    this.pending = ''; // pending is never persisted
  }

  saveSnapshot() {
    const data = {
      rid: this.rid,
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

    const { rid, apiKey, or_body, model, messages, after, provider } = m;
    const body = or_body || (model && Array.isArray(messages) ? { model, messages, stream: true } : null);

    if (!rid || !apiKey || !body || !Array.isArray(body.messages) || body.messages.length === 0) {
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
    this.phase = 'running';
    this.controller = new AbortController();
    this.saveSnapshot();

    this.stream({ apiKey, body, provider: (provider || 'openrouter') }).catch(e => this.fail(String(e?.message || 'stream_failed')));
  }

  // provider: 'openai' | 'openrouter' (default)
  async stream({ apiKey, body, provider }) {
    // NOTE: no end-reason is appended to output anymore
    if (provider === 'openai') {
      await this.streamOpenAI({ apiKey, body });
      return;
    }

    // Default: OpenRouter (unchanged)
    await this.streamOpenRouter({ apiKey, body });
  }

  // Map chat-style message parts to OpenAI Responses API input content
  mapContentPartToResponses(part) {
    const t = (part && part.type) || 'text';

    if (t === 'text') {
      // Convert to input_text
      return { type: 'input_text', text: String(part.text || '') };
    }

    if (t === 'image_url') {
      const url = part?.image_url?.url || part?.image_url || '';
      if (url) return { type: 'input_image', image_url: String(url) };
      // Fallback to text if no URL
      return { type: 'input_text', text: '' };
    }

    // Unsupported inline for Responses without file upload: degrade to text
    if (t === 'file') {
      const fname = part?.file?.filename || 'file';
      return { type: 'input_text', text: `[file:${fname}]` };
    }

    if (t === 'input_audio') {
      const fmt = part?.input_audio?.format || 'audio';
      return { type: 'input_text', text: `[audio:${fmt}]` };
    }

    // Any unknown -> input_text fallback
    return { type: 'input_text', text: '' };
  }

  // New: Direct OpenAI Responses API streaming path
  async streamOpenAI({ apiKey, body }) {
    const client = new OpenAI({ apiKey });

    // Translate chat.completions-style body to Responses API (roles + content[])
    const input = Array.isArray(body.messages)
      ? body.messages.map(m => ({
          role: m.role,
          content: Array.isArray(m.content)
            ? m.content.map(p => this.mapContentPartToResponses(p))
            : [{ type: 'input_text', text: String(m.content || '') }]
        }))
      : [];

    // Build params (include only supported/common controls)
    const params = {
      model: body.model,
      input,
      temperature: body.temperature,
      //top_p: body.top_p,
      stream: true,
    };
    if (Number.isFinite(+body.max_tokens) && +body.max_tokens > 0) {
      // Responses API uses max_output_tokens
      params.max_output_tokens = +body.max_tokens;
    }
    if (body.reasoning && body.reasoning.effort) {
      params.reasoning = { effort: body.reasoning.effort };
    }
    if (body.verbosity) {
      params.verbosity = body.verbosity;
    }

    let stream;
    try {
      // Stream and iterate events
      stream = await client.responses.stream(params);
      this.oaStream = stream;

      for await (const event of stream) {
        if (this.phase !== 'running') break;

        // Accumulate output text/refusal deltas
        if (event.type === 'response.output_text.delta' && event.delta) {
          this.queueDelta(event.delta);
        } else if (event.type === 'response.refusal.delta' && event.delta) {
          this.queueDelta(event.delta);
        }
      }
    } catch (e) {
      if (this.phase === 'running') {
        const msg = String(e?.message || 'stream_failed');
        if ((e && e.name === 'AbortError') || /abort/i.test(msg)) {
          // Stopped elsewhere; just exit
          return;
        }
        return this.fail(msg);
      }
      return;
    } finally {
      // Best-effort close
      try { stream?.controller?.abort(); } catch {}
      this.oaStream = null;
    }

    // Normal finish
    if (this.phase === 'running') {
      this.finish();
    }
  }

  // Existing: OpenRouter (kept intact)
  async streamOpenRouter({ apiKey, body }) {
    const client = new OpenAI({
      apiKey,
      baseURL: 'https://openrouter.ai/api/v1',
    });

    try {
      const params = { ...body, stream: true };
      const stream = await client.chat.completions.create(params, { signal: this.controller.signal });

      for await (const chunk of stream) {
        if (this.phase !== 'running') {
          break;
        }
        const delta = chunk?.choices?.[0]?.delta?.content ?? '';
        if (delta) this.queueDelta(delta);
        // finish_reason deliberately ignored; we no longer append it
      }
    } catch (e) {
      if (this.phase === 'running') {
        const msg = String(e?.message || 'stream_failed');
        if ((e && e.name === 'AbortError') || /abort/i.test(msg)) {
          return;
        }
        return this.fail(msg);
      }
      return;
    }

    if (this.phase === 'running') {
      this.finish();
    }
  }

  finish() {
    if (this.phase !== 'running') return;
    // Do not append any end reason; just finalize
    this.flush(true);
    this.phase = 'done';
    try { this.controller?.abort(); } catch {}
    try { this.oaStream?.controller?.abort(); } catch {}
    this.saveSnapshot();
    this.bcast({ type: 'done' });
  }

  stop(origin = 'client') {
    if (this.phase !== 'running') return;
    // Do not append any end reason; just finalize
    this.flush(true);
    this.phase = 'done';
    try { this.controller?.abort(); } catch {}
    try { this.oaStream?.controller?.abort(); } catch {}
    this.saveSnapshot();
    this.bcast({ type: 'done' });
  }

  fail(message) {
    if (this.phase === 'error') return;
    const reason = String(message || 'stream_failed');

    if (this.phase === 'running') {
      // Do not append any end reason; ensure pending is flushed
      this.flush(true);
    } else {
      this.flush(true);
    }

    this.phase = 'error';
    this.error = reason;
    try { this.controller?.abort(); } catch {}
    try { this.oaStream?.controller?.abort(); } catch {}
    this.saveSnapshot();
    this.bcast({ type: 'err', message: this.error });
  }
}
