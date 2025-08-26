import OpenAI from 'openai';

const TTL_MS = 20 * 60 * 1000;
const BATCH_MS = 60;
const BATCH_BYTES = 2048;
const SNAPSHOT_MIN_MS = 1500;

// Heartbeat configuration: emulate two alarms per 4s by a 2s cadence
const HB_TOTAL_MS = 4000;
const HB_OFFSET_MS = 2000;
const HB_TICK_MS = HB_OFFSET_MS; // single DO alarm -> tick every 2s

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Max-Age': '86400',
};

const withCORS = (resp) => {
  const headers = new Headers(resp.headers);
  Object.entries(CORS_HEADERS).forEach(([k, v]) => headers.set(k, v));
  return new Response(resp.body, { ...resp, headers });
};

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const method = req.method.toUpperCase();

    if (method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    if (url.pathname === '/ws') {
      const isGet = method === 'GET';
      const isWs = req.headers.get('Upgrade') === 'websocket';
      if (!isGet && !isWs) {
        return withCORS(new Response('method not allowed', { status: 405 }));
      }

      const rawUid = url.searchParams.get('uid') || 'anon';
      const uid = String(rawUid).slice(0, 64).replace(/[^a-zA-Z0-9_-]/g, '') || 'anon';
      const id = env.MY_DURABLE_OBJECT.idFromName(uid);
      const stub = env.MY_DURABLE_OBJECT.get(id);
      
      const resp = await stub.fetch(req);
      return isWs ? resp : withCORS(resp);
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
    this.buffer = [];
    this.seq = -1;
    this.phase = 'idle';
    this.error = null;
    this.controller = null;
    this.oaStream = null;
    this.pending = '';
    this.flushTimer = null;
    this.lastSavedAt = 0;
    this.lastFlushedAt = 0;

    // Heartbeat state
    this.hbActive = false;
    this.hbSlot = 0; // virtual 2-slot heartbeat (0/1) to represent 2 alarms per 4s
  }

  corsJSON(obj, status = 200) {
    return new Response(JSON.stringify(obj), {
      status,
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-store',
        ...CORS_HEADERS,
      }
    });
  }

  send(ws, obj) {
    try { ws.send(JSON.stringify(obj)); } catch {}
  }

  bcast(obj) {
    const message = JSON.stringify(obj);
    this.sockets.forEach(ws => { try { ws.send(message); } catch {} });
  }

  async restoreIfCold() {
    if (this.rid) return; // Already restored, object is "warm".
    
    const snap = await this.state.storage.get('run').catch(() => null);
    if (!snap || (Date.now() - (snap.savedAt || 0) >= TTL_MS)) {
      if (snap) await this.state.storage.delete('run').catch(() => {});
      return;
    }

    // Restore properties from the snapshot
    this.rid = snap.rid || null;
    this.buffer = Array.isArray(snap.buffer) ? snap.buffer : [];
    this.seq = Number.isFinite(+snap.seq) ? +snap.seq : -1;
    this.phase = snap.phase || 'done';
    this.error = snap.error || null;
    this.pending = '';

    // As per Master's instruction: if the DO boots up and its last phase was 'running',
    // it means the process was interrupted, likely by runtime eviction.
    // We change the phase to 'evicted' to mark it as a terminated, non-successful run.
    if (this.phase === 'running') {
      this.phase = 'evicted';
      this.error = 'The run was interrupted due to system eviction.';
      // Persist this updated terminal state immediately.
      this.saveSnapshot();
      // Ensure heartbeat is off for terminal state
      this.stopHeartbeat().catch(() => {});
    }
  }

  saveSnapshot() {
    this.lastSavedAt = Date.now();
    const data = {
      rid: this.rid,
      buffer: this.buffer,
      seq: this.seq,
      phase: this.phase,
      error: this.error,
      savedAt: this.lastSavedAt,
    };
    this.state.storage.put('run', data).catch(() => {});
  }

  saveSnapshotThrottled() {
    if (Date.now() - this.lastSavedAt >= SNAPSHOT_MIN_MS) {
      this.saveSnapshot();
    }
  }

  replay(ws, after) {
    this.buffer.forEach(it => {
      if (it.seq > after) this.send(ws, { type: 'delta', seq: it.seq, text: it.text });
    });
    
    // Notify client of terminal state
    if (this.phase === 'done') {
      this.send(ws, { type: 'done' });
    } else if (this.phase === 'error' || this.phase === 'evicted') {
      this.send(ws, { type: 'err', message: this.error || 'The run was terminated unexpectedly.' });
    }
  }

  flush(force = false) {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    if (this.pending) {
      this.buffer.push({ seq: ++this.seq, text: this.pending });
      this.bcast({ type: 'delta', seq: this.seq, text: this.pending });
      this.pending = '';
      this.lastFlushedAt = Date.now();
    }
    force ? this.saveSnapshot() : this.saveSnapshotThrottled();
  }

  queueDelta(text) {
    if (!text) return;
    this.pending += text;
    if (this.pending.length >= BATCH_BYTES) {
      this.flush(false);
    } else if (!this.flushTimer) {
      this.flushTimer = setTimeout(() => this.flush(false), BATCH_MS);
    }
  }

  async fetch(req) {
    if (req.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }
    if (req.headers.get('Upgrade') === 'websocket') {
      const [client, server] = Object.values(new WebSocketPair());
      server.accept();
      this.sockets.add(server);
      server.addEventListener('close', () => this.sockets.delete(server));
      server.addEventListener('message', e => this.state.waitUntil(this.onMessage(server, e)));
      return new Response(null, { status: 101, webSocket: client });
    }
    if (req.method === 'GET') {
      await this.restoreIfCold();
      const text = this.buffer.map(it => it.text).join('') + this.pending;

      const isTerminal = ['done', 'error', 'evicted'].includes(this.phase);
      const isError = ['error', 'evicted'].includes(this.phase);

      return this.corsJSON({
        rid: this.rid,
        seq: this.seq,
        phase: this.phase,
        done: isTerminal,
        error: isError ? (this.error || 'The run was terminated unexpectedly.') : null,
        text,
      });
    }
    return this.corsJSON({ error: 'not allowed' }, 405);
  }

  async onMessage(ws, evt) {
    await this.restoreIfCold();
    let msg;
    try {
      msg = JSON.parse(String(evt.data || ''));
    } catch {
      return this.send(ws, { type: 'err', message: 'bad_json' });
    }

    if (msg.type === 'resume') {
      if (!msg.rid || msg.rid !== this.rid) return this.send(ws, { type: 'err', message: 'stale_run' });
      return this.replay(ws, Number.isFinite(+msg.after) ? +msg.after : -1);
    }

    if (msg.type === 'stop') {
      if (msg.rid && msg.rid === this.rid) this.stop();
      return;
    }

    if (msg.type !== 'begin') return this.send(ws, { type: 'err', message: 'bad_type' });

    const { rid, apiKey, or_body, model, messages, after, provider } = msg;
    const body = or_body || (model && Array.isArray(messages) ? { model, messages, stream: true } : null);

    if (!rid || !apiKey || !body || !Array.isArray(body.messages) || body.messages.length === 0) {
      return this.send(ws, { type: 'err', message: 'missing_fields' });
    }
    if (this.phase === 'running' && rid !== this.rid) {
      return this.send(ws, { type: 'err', message: 'busy' });
    }
    if (rid === this.rid && this.phase !== 'idle') {
      return this.replay(ws, Number.isFinite(+after) ? +after : -1);
    }

    this.reset();
    this.rid = rid;
    this.phase = 'running';
    this.controller = new AbortController();
    this.saveSnapshot();

    // Start heartbeat while running
    this.state.waitUntil(this.startHeartbeat());

    this.state.waitUntil(this.stream({ apiKey, body, provider: provider || 'openrouter' }));
  }

  async stream({ apiKey, body, provider }) {
    try {
      if (provider === 'openai') {
        await this.streamOpenAI({ apiKey, body });
      } else {
        await this.streamOpenRouter({ apiKey, body });
      }
      if (this.phase === 'running') this.stop();
    } catch (e) {
      if (this.phase === 'running') {
        const msg = String(e?.message || 'stream_failed');
        if (!((e && e.name === 'AbortError') || /abort/i.test(msg))) {
          this.fail(msg);
        }
      }
    }
  }

  isMultimodalMessage(m) {
    if (!m || !Array.isArray(m.content)) return false;
    return m.content.some(p => {
      const type = p?.type || '';
      return type && type !== 'text' && type !== 'input_text';
    });
  }

  extractTextFromMessage(m) {
    if (!m) return '';
    if (typeof m.content === 'string') return String(m.content);
    if (!Array.isArray(m.content)) return '';
    return m.content
      .filter(p => p && (p.type === 'text' || p.type === 'input_text'))
      .map(p => String((p.type === 'text') ? (p.text ?? p.content ?? '') : (p.text ?? '')))
      .join('');
  }

  mapContentPartToResponses(part) {
    const type = part?.type || 'text';
    if (type === 'image_url' || type === 'input_image') {
      const url = part?.image_url?.url || part?.image_url || '';
      return url ? { type: 'input_image', image_url: String(url) } : null;
    }
    if (type === 'text' || type === 'input_text') {
      const text = (type === 'text') ? (part.text ?? part.content ?? '') : (part.text ?? '');
      return { type: 'input_text', text: String(text) };
    }
    return { type: 'input_text', text: `[${type}:${part?.file?.filename || 'file'}]` };
  }

  buildInputForResponses(messages) {
    if (!Array.isArray(messages) || messages.length === 0) return '';
    const isMulti = messages.some(m => this.isMultimodalMessage(m));

    if (!isMulti) {
      if (messages.length === 1) return this.extractTextFromMessage(messages[0]);
      return messages.map(m => ({ role: m.role, content: this.extractTextFromMessage(m) }));
    }
    return messages.map(m => {
      let content = [{ type: 'input_text', text: String(m.content || '') }];
      if (Array.isArray(m.content)) {
        content = m.content.map(p => this.mapContentPartToResponses(p)).filter(Boolean);
      }
      return { role: m.role, content };
    });
  }

  async streamOpenAI({ apiKey, body }) {
    const client = new OpenAI({ apiKey });
    const params = {
      model: body.model,
      input: this.buildInputForResponses(body.messages || []),
      temperature: body.temperature,
      stream: true,
    };
    if (Number.isFinite(+body.max_tokens) && +body.max_tokens > 0) {
      params.max_output_tokens = +body.max_tokens;
    }
    if (body.reasoning?.effort) params.reasoning = { effort: body.reasoning.effort };
    if (body.verbosity) params.verbosity = body.verbosity;

    this.oaStream = await client.responses.stream(params);
    try {
      for await (const event of this.oaStream) {
        if (this.phase !== 'running') break;
        const delta = event.delta;
        if ((event.type.endsWith('.delta')) && delta) {
          this.queueDelta(delta);
        }
      }
    } finally {
      try { this.oaStream?.controller?.abort(); } catch {}
      this.oaStream = null;
    }
  }

  async streamOpenRouter({ apiKey, body }) {
    const client = new OpenAI({ apiKey, baseURL: 'https://openrouter.ai/api/v1' });
    const stream = await client.chat.completions.create(
      { ...body, stream: true },
      { signal: this.controller.signal }
    );
    for await (const chunk of stream) {
      if (this.phase !== 'running') break;
      const delta = chunk?.choices?.[0]?.delta?.content ?? '';
      if (delta) this.queueDelta(delta);
    }
  }

  stop() {
    if (this.phase !== 'running') return;
    this.flush(true);
    this.phase = 'done';
    this.error = null;
    try { this.controller?.abort(); } catch {}
    try { this.oaStream?.controller?.abort(); } catch {}
    this.saveSnapshot();
    this.bcast({ type: 'done' });
    // Stop heartbeat on terminal state
    this.state.waitUntil(this.stopHeartbeat());
  }

  fail(message) {
    if (this.phase === 'error') return;
    this.flush(true);
    this.phase = 'error';
    this.error = String(message || 'stream_failed');
    try { this.controller?.abort(); } catch {}
    try { this.oaStream?.controller?.abort(); } catch {}
    this.saveSnapshot();
    this.bcast({ type: 'err', message: this.error });
    // Stop heartbeat on terminal state
    this.state.waitUntil(this.stopHeartbeat());
  }

  // Heartbeat: emulate two alarms per 4s via single DO alarm every 2s
  async startHeartbeat() {
    if (this.hbActive || this.phase !== 'running') return;
    this.hbActive = true;
    this.hbSlot = 0;
    try {
      await this.state.storage.setAlarm(Date.now() + HB_OFFSET_MS);
    } catch {}
  }

  async stopHeartbeat() {
    this.hbActive = false;
    this.hbSlot = 0;
    try {
      await this.state.storage.setAlarm(null); // clear any pending alarm
    } catch {}
  }

  // Alarm handler
  async alarm() {
    await this.restoreIfCold();
    if (this.phase !== 'running' || !this.hbActive) {
      // not running or heartbeat disabled -> clear any alarm
      try { await this.state.storage.setAlarm(null); } catch {}
      return;
    }

    // Heartbeat tick: persist occasionally to keep the DO active
    this.saveSnapshotThrottled();

    // Flip virtual slot (0/1) to represent two alarms per 4s offset by 2s
    this.hbSlot = (this.hbSlot ^ 1);

    // Re-arm next tick after 2s to keep the 2s cadence (two per 4s)
    try {
      await this.state.storage.setAlarm(Date.now() + HB_TICK_MS);
    } catch {}
  }
}
