import OpenAI from 'openai';

const TTL_MS = 20 * 60 * 1000; // 20 minutes
const BATCH_MS = 100; // Flush buffer after this many ms of inactivity
const BATCH_BYTES = 2048; // Flush buffer when pending text reaches this size
const SNAPSHOT_MIN_MS = 1500; // Throttle saves to storage to at most once per this period
const HEARTBEAT_MS = 1 * 1000; // Heartbeat to prevent eviction during a stream. Set to 1 second for testing.

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
    this.reset();
    this.state.waitUntil(this.restoreIfCold());
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
    this.lastSavedAt = 0;
    this.lastFlushedAt = 0;
    this.initialized = false;
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

  send(ws, obj) { try { ws.send(JSON.stringify(obj)); } catch (e) {} }

  bcast(obj) {
    const message = JSON.stringify(obj);
    for (const ws of this.state.getWebSockets()) { try { ws.send(message); } catch (e) {} }
  }

  async restoreIfCold() {
    if (this.initialized) return;
    this.initialized = true;

    const snap = await this.state.storage.get('run').catch(() => null);
    if (!snap || (Date.now() - (snap.savedAt || 0) >= TTL_MS)) {
      if (snap) await this.state.storage.delete('run').catch(() => {});
      return;
    }

    this.rid = snap.rid || null;
    this.buffer = Array.isArray(snap.buffer) ? snap.buffer : [];
    this.seq = Number.isFinite(+snap.seq) ? +snap.seq : -1;
    this.phase = snap.phase || 'done';
    this.error = snap.error || null;
    this.pending = '';

    if (this.phase === 'running') {
      this.phase = 'evicted';
      this.error = 'The run was interrupted due to system eviction.';
      await this.saveSnapshot();
    }
  }

  async saveSnapshot() {
    this.lastSavedAt = Date.now();
    const data = { rid: this.rid, buffer: this.buffer, seq: this.seq, phase: this.phase, error: this.error, savedAt: this.lastSavedAt, };
    await this.state.storage.put('run', data).catch(() => {});
  }

  async saveSnapshotThrottled() {
    if (Date.now() - this.lastSavedAt >= SNAPSHOT_MIN_MS) { await this.saveSnapshot(); }
  }

  replay(ws, after) {
    this.buffer.forEach(it => {
      if (it.seq > after) this.send(ws, { type: 'delta', seq: it.seq, text: it.text });
    });
    if (this.phase === 'done') {
      this.send(ws, { type: 'done' });
    } else if (this.phase === 'error' || this.phase === 'evicted') {
      this.send(ws, { type: 'err', message: this.error || 'The run was terminated unexpectedly.' });
    }
  }

  async flush(force = false) {
    if (this.pending) {
      this.buffer.push({ seq: ++this.seq, text: this.pending });
      this.bcast({ type: 'delta', seq: this.seq, text: this.pending });
      this.pending = '';
      this.lastFlushedAt = Date.now();
    }
    await (force ? this.saveSnapshot() : this.saveSnapshotThrottled());
  }

  async queueDelta(text) {
    if (!text) return;
    this.pending += text;
    if (this.pending.length >= BATCH_BYTES) {
      await this.flush(false);
      // After a forced flush, ensure the next heartbeat is scheduled correctly.
      await this.state.storage.setAlarm(Date.now() + HEARTBEAT_MS);
    } else {
      const currentAlarm = await this.state.storage.getAlarm();
      // Schedule a batching alarm only if there isn't a heartbeat coming sooner.
      if (currentAlarm === null || currentAlarm > Date.now() + BATCH_MS) {
        await this.state.storage.setAlarm(Date.now() + BATCH_MS);
      }
    }
  }

  async alarm() {
    await this.restoreIfCold();
    // The alarm serves a dual purpose: flushing the text buffer and acting as a heartbeat.
    await this.flush(false);

    // If the stream is still running, perpetuate the heartbeat.
    if (this.phase === 'running') {
      await this.state.storage.setAlarm(Date.now() + HEARTBEAT_MS);
    }
  }

  async startHeartbeat() {
    await this.state.storage.setAlarm(Date.now() + HEARTBEAT_MS);
  }

  async stopHeartbeat() {
    await this.state.storage.deleteAlarm();
  }

  async fetch(req) { /* ... no changes ... */ }
  async webSocketMessage(ws, message) { /* ... no changes ... */ }
  async webSocketClose(ws, code, reason, wasClean) { /* ... no changes ... */ }
  async webSocketError(ws, error) { /* ... no changes ... */ }

  async stream({ apiKey, body, provider }) {
    await this.startHeartbeat();
    try {
      if (provider === 'openai') {
        await this.streamOpenAI({ apiKey, body });
      } else {
        await this.streamOpenRouter({ apiKey, body });
      }
      if (this.phase === 'running') await this.stop();
    } catch (e) {
      if (this.phase === 'running') {
        const msg = String(e?.message || 'stream_failed');
        if (!((e && e.name === 'AbortError') || /abort/i.test(msg))) {
          await this.fail(msg);
        } else {
          await this.stopHeartbeat();
        }
      }
    }
  }

  /* isMultimodalMessage and other helpers are unchanged */
  isMultimodalMessage(m) {
    if (!m || !Array.isArray(m.content)) return false;
    return m.content.some(p => { const type = p?.type || ''; return type && type !== 'text' && type !== 'input_text'; });
  }
  extractTextFromMessage(m) {
    if (!m) return '';
    if (typeof m.content === 'string') return String(m.content);
    if (!Array.isArray(m.content)) return '';
    return m.content.filter(p => p && (p.type === 'text' || p.type === 'input_text')).map(p => String((p.type === 'text') ? (p.text ?? p.content ?? '') : (p.text ?? ''))).join('');
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
      if (Array.isArray(m.content)) { content = m.content.map(p => this.mapContentPartToResponses(p)).filter(Boolean); }
      return { role: m.role, content };
    });
  }

  async streamOpenAI({ apiKey, body }) { /* ... no changes ... */ }
  async streamOpenRouter({ apiKey, body }) { /* ... no changes ... */ }

  async stop() {
    if (this.phase !== 'running') return;
    await this.stopHeartbeat();
    await this.flush(true);
    this.phase = 'done';
    this.error = null;
    try { this.controller?.abort(); } catch {}
    try { this.oaStream?.controller?.abort(); } catch {}
    await this.saveSnapshot();
    this.bcast({ type: 'done' });
  }

  async fail(message) {
    if (this.phase === 'error') return;
    await this.stopHeartbeat();
    await this.flush(true);
    this.phase = 'error';
    this.error = String(message || 'stream_failed');
    try { this.controller?.abort(); } catch {}
    try { this.oaStream?.controller?.abort(); } catch {}
    await this.saveSnapshot();
    this.bcast({ type: 'err', message: this.error });
  }
}
