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
    this.buffer = [];
    this.seq = -1;
    this.phase = "idle"; // idle | running | done | error
    this.error = null;
    this.sockets = new Set();
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
    server.addEventListener("message", e => this.#onMessage(server, e));

    return new Response(null, { status: 101, webSocket: client });
  }

  #send(ws, obj) { try { ws.send(JSON.stringify(obj)); } catch {} }
  #broadcast(obj) {
    const s = JSON.stringify(obj);
    for (const ws of this.sockets) { try { ws.send(s) } catch {} }
  }
  #replay(ws, after) {
    for (const it of this.buffer) if (it.seq > after) this.#send(ws, { type:"delta", ...it });
    if (this.phase === "done") this.#send(ws, { type:"done" });
    if (this.phase === "error") this.#send(ws, { type:"err", message:this.error });
  }

  async #onMessage(ws, e) {
    let msg; try { msg = JSON.parse(String(e.data||"")); } catch { return this.#send(ws,{type:"err",message:"bad_json"}) }
    if (msg.type === "resume") return this.#replay(ws, +msg.after||-1);
    if (msg.type === "stop") return this.#stop("client");
    if (msg.type !== "begin") return this.#send(ws,{type:"err",message:"bad_type"});

    const { apiKey, model, messages, after } = msg;
    if (!apiKey || !model || !Array.isArray(messages) || !messages.length)
      return this.#send(ws,{type:"err",message:"missing_fields"});

    this.#replay(ws, +after||-1);
    if (this.phase === "running") return;

    this.buffer=[]; this.seq=-1; this.error=null;
    this.phase="running"; this.controller=new AbortController();
    this.#stream({apiKey,model,messages}).catch(e=>this.#fail(String(e?.message||"stream_failed")));
  }

  async #stream({apiKey,model,messages}) {
    const res = await fetch("https://openrouter.ai/api/v1/chat/completions", {
      method:"POST",
      headers:{ "Content-Type":"application/json", "Authorization":"Bearer "+apiKey },
      body: JSON.stringify({ model, messages, stream:true }),
      signal: this.controller.signal
    });
    if (!res.ok || !res.body) return this.#fail("HTTP "+res.status);

    const dec=new TextDecoder(), reader=res.body.getReader();
    let buf="";
    while (this.phase==="running") {
      const {value,done}=await reader.read().catch(()=>({done:true}));
      if (done) break;
      buf+=dec.decode(value,{stream:true});
      let i;
      while((i=buf.indexOf("\\n\\n"))!==-1){
        const chunk=buf.slice(0,i).trim(); buf=buf.slice(i+2);
        if(!chunk||!chunk.startsWith("data:"))continue;
        const data=chunk.slice(5).trim();
        if(data==="[DONE]") return this.#finish();
        try{
          const j=JSON.parse(data);
          const delta=j?.choices?.[0]?.delta?.content??"";
          if(delta){
            const seq=++this.seq;
            this.buffer.push({seq,text:delta});
            this.#broadcast({type:"delta",seq,text:delta});
          }
          if(j?.choices?.[0]?.finish_reason) return this.#finish();
        }catch{}
      }
    }
    this.#finish();
  }

  #finish(){ if(this.phase!=="running")return; this.phase="done"; this.controller?.abort(); this.#broadcast({type:"done"}); }
  #stop(){ if(this.phase!=="running")return; this.phase="done"; this.controller?.abort(); this.#broadcast({type:"done"}); }
  #fail(msg){ if(this.phase==="done"||this.phase==="error")return; this.phase="error"; this.error=msg; this.controller?.abort(); this.#broadcast({type:"err",message:msg}); }
}
