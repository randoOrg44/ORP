const TTL_MS = 20 * 60 * 1000; // allow 10 min resumes

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/ws") {
      const id = env.MY_DURABLE_OBJECT.idFromName("singleton");
      return env.MY_DURABLE_OBJECT.get(id).fetch(req);
    }
    return new Response("not found", { status: 404 });
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
    this.buffer = [];   // [{seq,text}]
    this.seq = -1;
    this.phase = "idle"; // idle|running|done|error
    this.error = null;
    this.controller = null;
  }

  async fetch(req) {
    if (req.headers.get("Upgrade") !== "websocket")
      return new Response("Expected websocket", { status: 426 });

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();

    this.sockets.add(server);
    server.addEventListener("close", () => this.sockets.delete(server));
    server.addEventListener("message", (e) => this.onMessage(server, e));

    return new Response(null, { status: 101, webSocket: client });
  }

  send(ws, obj){ try{ ws.send(JSON.stringify(obj)) }catch{} }
  bcast(obj){ const s=JSON.stringify(obj); for(const ws of this.sockets){ try{ ws.send(s) }catch{} } }

  async restoreIfCold(){
    if (this.rid) return; // already warm
    const snap = await this.state.storage.get("run").catch(()=>null);
    const fresh = snap && (Date.now() - (snap.savedAt||0) < TTL_MS);
    if (!fresh) { if (snap) await this.state.storage.delete("run").catch(()=>{}); return; }
    this.rid = snap.rid || null;
    this.buffer = Array.isArray(snap.buffer) ? snap.buffer : [];
    this.seq = Number.isFinite(+snap.seq) ? +snap.seq : -1;
    this.phase = snap.phase || "done";
    this.error = snap.error || null;
  }

  saveSnapshot(){
    const data = { rid:this.rid, buffer:this.buffer, seq:this.seq, phase:this.phase, error:this.error, savedAt:Date.now() };
    this.state.storage.put("run", data).catch(()=>{});
  }

  replay(ws, after){
    for (const it of this.buffer) if (it.seq > after) this.send(ws, { type:"delta", seq:it.seq, text:it.text });
    if (this.phase === "done")  this.send(ws, { type:"done" });
    if (this.phase === "error") this.send(ws, { type:"err", message:this.error });
  }

  async onMessage(ws, evt){
    await this.restoreIfCold();

    let m; try { m = JSON.parse(String(evt.data||"")); } catch { return this.send(ws,{type:"err",message:"bad_json"}) }

    if (m.type === "resume"){
      if (!m.rid || m.rid !== this.rid) return this.send(ws,{type:"err",message:"stale_run"});
      const after = Number.isFinite(+m.after) ? +m.after : -1;
      return this.replay(ws, after);
    }

    if (m.type === "stop"){
      if (m.rid && m.rid === this.rid) this.stop("client");
      return;
    }

    if (m.type !== "begin") return this.send(ws,{type:"err",message:"bad_type"});

    const { rid, apiKey, or_body, model, messages, after } = m;
    const body = or_body || (model && Array.isArray(messages) ? { model, messages, stream:true } : null);
    if (!rid || !apiKey || !body || !Array.isArray(body.messages) || body.messages.length===0)
      return this.send(ws,{type:"err",message:"missing_fields"});

    if (this.phase === "running" && rid !== this.rid)
      return this.send(ws,{type:"err",message:"busy"});

    if (rid === this.rid && this.phase !== "idle"){
      const a = Number.isFinite(+after) ? +after : -1;
      return this.replay(ws, a);
    }

    this.reset();
    this.rid = rid;
    this.phase = "running";
    this.controller = new AbortController();
    this.saveSnapshot();
    this.stream({ apiKey, body }).catch(e => this.fail(String(e?.message || "stream_failed")));
  }

  async stream({ apiKey, body }){
    const res = await fetch("https://openrouter.ai/api/v1/chat/completions",{
      method:"POST",
      headers:{ "Content-Type":"application/json", "Authorization":"Bearer "+apiKey },
      body:JSON.stringify(body),
      signal:this.controller.signal
    }).catch(e=>({ ok:false, status:0, body:null, text:async()=>String(e?.message||"fetch_failed") }));

    if (!res.ok || !res.body){
      const t = await res.text().catch(()=> "");
      return this.fail(t || ("HTTP "+res.status));
    }

    const dec = new TextDecoder(), reader = res.body.getReader();
    let buf = "";
    while (this.phase === "running"){
      const { value, done } = await reader.read().catch(()=>({done:true}));
      if (done) break;
      buf += dec.decode(value,{stream:true});

      let i;
      while ((i = buf.indexOf("\n\n")) !== -1){
        const chunk = buf.slice(0,i).trim(); buf = buf.slice(i+2);
        if (!chunk || !chunk.startsWith("data:")) continue;

        const data = chunk.slice(5).trim();
        if (data === "[DONE]") return this.finish();

        try{
          const j = JSON.parse(data);
          const delta = j?.choices?.[0]?.delta?.content ?? "";
          if (delta){
            const seq = ++this.seq;
            this.buffer.push({ seq, text: delta });
            this.bcast({ type:"delta", seq, text: delta });
            this.saveSnapshot(); // persist progress for late resumes
          }
          if (j?.choices?.[0]?.finish_reason) return this.finish();
        }catch{}
      }
    }
    this.finish();
  }

  finish(){
    if (this.phase !== "running") return;
    this.phase = "done";
    try{ this.controller?.abort() }catch{}
    this.saveSnapshot();
    this.bcast({ type:"done" });
  }

  stop(){
    if (this.phase !== "running") return;
    this.phase = "done";
    try{ this.controller?.abort() }catch{}
    this.saveSnapshot();
    this.bcast({ type:"done" });
  }

  fail(message){
    if (this.phase === "error") return;
    this.phase = "error";
    this.error = message;
    try{ this.controller?.abort() }catch{}
    this.saveSnapshot();
    this.bcast({ type:"err", message });
  }
}
