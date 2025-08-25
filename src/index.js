// src/index.js
import OpenAI from 'openai';

const TTL_MS = 20 * 60 * 1000;
const BATCH_MS = 60;
const BATCH_BYTES = 2048;
const SNAPSHOT_MIN_MS = 1500;

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
    if (method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders() });
    }
    if (url.pathname === '/ws') {
      const raw = url.searchParams.get('uid') || 'anon';
      const uid = String(raw).slice(0, 64).replace(/[^a-zA-Z0-9_-]/g, '') || 'anon';
      const id = env.MY_DURABLE_OBJECT.idFromName(uid);
      const stub = env.MY_DURABLE_OBJECT.get(id);
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
    this.provider = 'openrouter';
    this.buffer = [];
    this.seq = -1;
    this.phase = 'idle';
    this.error = null;
    this.controller = null;
    this.pending = '';
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
    this.pending = '';
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

  appendEndReason(reason) {
    const clean = String(reason || 'unknown').replace(/\s+/g, ' ').trim().slice(0, 128);
    this.queueDelta(`[end:${clean}]`);
  }

  async fetch(req) {
    const url = new URL(req.url);
    const upgrade = req.headers.get('Upgrade');
    if (req.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders() });
    }
    if (url.pathname === '/ws' && upgrade === 'websocket') {
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      server.accept();
      this.sockets.add(server);
      server.addEventListener('close', () => this.sockets.delete(server));
      server.addEventListener('message', (e) => this.onMessage(server, e));
      return new Response(null, { status: 101, webSocket: client });
    }
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

  mapParts(provider, parts) {
    if (!Array.isArray(parts)) {
      const t = String(parts ?? '');
      return provider === 'openai'
        ? [{ type: 'input_text', text: t }]
        : [{ type: 'text', text: t }];
    }
    if (provider !== 'openai') {
      return parts;
    }
    const out = [];
    for (const p of parts) {
      if (!p || typeof p !== 'object') continue;
      if (p.type === 'text' && typeof p.text === 'string') {
        out.push({ type: 'input_text', text: p.text });
      } else if (p.type === 'input_text' && typeof p.text === 'string') {
        out.push({ type: 'input_text', text: p.text });
      } else if (p.type === 'image_url') {
        const url = typeof p.image_url === 'string' ? p.image_url : (p.image_url && p.image_url.url);
        if (url) out.push({ type: 'input_image', image_url: url });
      } else if (p.type === 'input_image' && p.image_url) {
        const url = typeof p.image_url === 'string' ? p.image_url : (p.image_url && p.image_url.url);
        if (url) out.push({ type: 'input_image', image_url: url });
      } else if (p.type === 'input_audio' && p.input_audio && p.input_audio.data && p.input_audio.format) {
        out.push({ type: 'input_audio', data: p.input_audio.data, format: p.input_audio.format });
      } else if (p.type === 'file') {
        const name = p.file?.filename || 'file';
        out.push({ type: 'input_text', text: `(file attached: ${name})` });
      } else if (p.type === 'output_text' && typeof p.text === 'string') {
        out.push({ type: 'input_text', text: p.text });
      } else if (typeof p === 'string') {
        out.push({ type: 'input_text', text: p });
      } else if (p.text) {
        out.push({ type: 'input_text', text: String(p.text) });
      }
    }
    if (!out.length) out.push({ type: 'input_text', text: '' });
    return out;
  }

  sanitizeBodyForProvider(provider, input) {
    const body = { ...(input || {}) };
    if (!body.input) {
      if (Array.isArray(body.messages)) {
        body.input = body.messages.map(m => ({
          role: m.role || 'user',
          content: this.mapParts(provider, m.content ?? (m.text != null ? String(m.text) : ''))
        }));
        delete body.messages;
      } else if (typeof body.prompt === 'string') {
        body.input = [{ role: 'user', content: this.mapParts(provider, [{ type: 'text', text: body.prompt }]) }];
        delete body.prompt;
      }
    } else if (Array.isArray(body.input)) {
      body.input = body.input.map(msg => ({
        role: msg.role || 'user',
        content: this.mapParts(provider, msg.content)
      }));
    }
    body.stream = true;
    if (body.max_tokens != null && body.max_output_tokens == null) {
      body.max_output_tokens = body.max_tokens;
      delete body.max_tokens;
    }
    if (provider === 'openai') {
      delete body.top_k;
      delete body.repetition_penalty;
      delete body.min_p;
      delete body.top_a;
      delete body.frequency_penalty;
      delete body.presence_penalty;
      if (!Array.isArray(body.input)) {
        body.input = [{ role: 'user', content: [{ type: 'input_text', text: '' }] }];
      } else {
        body.input = body.input.map(msg => ({
          role: msg.role || 'user',
          content: this.mapParts('openai', msg.content)
        }));
      }
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
      or_body,
      body,
      model,
      messages,
      after
    } = m;
    const provider = this.normalizeProvider(provRaw);
    let rawBody = or_body || body || null;
    if (!rawBody && model && Array.isArray(messages)) {
      rawBody = { model, messages, stream: true };
    }
    const hasMessages = rawBody && Array.isArray(rawBody.messages) && rawBody.messages.length > 0;
    const hasInput = rawBody && (rawBody.input || rawBody.prompt);
    const hasModel = rawBody && rawBody.model;
    if (!rid || !apiKey || !rawBody || !hasModel || (!hasMessages && !hasInput)) {
      return this.send(ws, { type: 'err', message: 'missing_fields' });
    }
    if (this.phase === 'running' && rid !== this.rid) {
      return this.send(ws, { type: 'err', message: 'busy' });
    }
    if (rid === this.rid && this.phase !== 'idle') {
      const a = Number.isFinite(+after) ? +after : -1;
      return this.replay(ws, a);
    }
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
    try {
      const stream = await client.responses.stream({ ...body }, { signal: this.controller.signal });
      for await (const event of stream) {
        if (this.phase !== 'running') break;
        if (event.type === 'response.output_text.delta') {
          const delta = event.delta || '';
          if (delta) this.queueDelta(delta);
        }
        if (event.type === 'response.message.delta') {
          const parts = event.delta?.content || [];
          for (const p of parts) {
            if (p.type === 'output_text' && p.text) this.queueDelta(p.text);
          }
        }
        if (event.type === 'response.completed') {
          this.finish('completed');
        }
        if (event.type === 'response.error') {
          const msg = event.error?.message || 'stream_failed';
          return this.fail(msg);
        }
      }
      if (this.phase === 'running') {
        this.finish('done');
      }
    } catch (e) {
      if (this.phase === 'running') {
        const msg = String(e?.message || 'stream_failed');
        if ((e && e.name === 'AbortError') || /abort/i.test(msg)) {
          return;
        }
        return this.fail(msg);
      }
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
      this.appendEndReason(reason);
      this.flush(true);
    } else {
      this.flush(true);
    }
    this.phase = 'error';
    this.error = String(message || 'stream_failed');
    try { this.controller?.abort(); } catch {}
    this.saveSnapshot();
    this.bcast({ type: 'err', message: this.error });
  }
}
