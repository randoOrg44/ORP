import OpenAI from 'openai'
import { DurableObject } from 'cloudflare:workers'

const TTL_MS = 20 * 60 * 1000
const BATCH_MS = 60
const BATCH_BYTES = 2048
const SNAPSHOT_MIN_MS = 1500

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Max-Age': '86400',
}

const withCORS = r => {
  const h = new Headers(r.headers); Object.entries(CORS_HEADERS).forEach(([k,v])=>h.set(k,v))
  return new Response(r.body,{...r,headers:h})
}

export default {
  async fetch(req, env) {
    const u = new URL(req.url), m = req.method.toUpperCase()
    if (m === 'OPTIONS') return new Response(null,{status:204,headers:CORS_HEADERS})
    if (u.pathname === '/ws') {
      if (m !== 'GET' || req.headers.get('Upgrade') !== 'websocket') return withCORS(new Response('method not allowed',{status:405}))
      const uid = (u.searchParams.get('uid')||'anon').slice(0,64).replace(/[^a-zA-Z0-9_-]/g,'')||'anon'
      return env.MY_DURABLE_OBJECT.getByName(uid).fetch(req)
    }
    return withCORS(new Response('not found',{status:404}))
  }
}

export class MyDurableObject extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env)
    this.env = env
    this.reset()
  }

  reset() {
    this.rid = null
    this.buffer = []
    this.seq = -1
    this.phase = 'idle'
    this.error = null
    this.controller = null
    this.oaStream = null
    this.pending = ''
    this.flushTimer = null
    this.lastSavedAt = 0
    this.lastFlushedAt = 0
  }

  corsJSON(obj, status=200) {
    return new Response(JSON.stringify(obj),{status,headers:{'Content-Type':'application/json','Cache-Control':'no-store',...CORS_HEADERS}})
  }

  send(ws,obj){ try{ ws.send(JSON.stringify(obj)) }catch{} }
  bcast(obj){ const msg=JSON.stringify(obj); this.ctx.getWebSockets().forEach(ws=>{ try{ ws.send(msg) }catch{} }) }

  async restoreIfCold() {
    if (this.rid) return
    const snap = await this.ctx.storage.get('run').catch(()=>null)
    if (!snap || (Date.now() - (snap.savedAt||0) >= TTL_MS)) { if (snap) await this.ctx.storage.delete('run').catch(()=>{}); return }
    this.rid = snap.rid||null
    this.buffer = Array.isArray(snap.buffer)?snap.buffer:[]
    this.seq = Number.isFinite(+snap.seq)?+snap.seq:-1
    this.phase = snap.phase||'done'
    this.error = snap.error||null
    this.pending = ''
  }

  saveSnapshot(){
    this.lastSavedAt=Date.now()
    this.ctx.storage.put('run',{rid:this.rid,buffer:this.buffer,seq:this.seq,phase:this.phase,error:this.error,savedAt:this.lastSavedAt}).catch(()=>{})
  }
  saveSnapshotThrottled(){ if (Date.now()-this.lastSavedAt>=SNAPSHOT_MIN_MS) this.saveSnapshot() }

  replay(ws, after){
    this.buffer.forEach(it=>{ if (it.seq>after) this.send(ws,{type:'delta',seq:it.seq,text:it.text}) })
    if (this.pending) this.send(ws,{type:'delta',seq:this.seq+1,text:this.pending})
    if (this.phase==='done') this.send(ws,{type:'done'})
    if (this.phase==='error') this.send(ws,{type:'err',message:this.error})
  }

  flush(force=false){
    if (this.flushTimer){ clearTimeout(this.flushTimer); this.flushTimer=null }
    if (this.pending){
      this.buffer.push({seq:++this.seq,text:this.pending})
      this.bcast({type:'delta',seq:this.seq,text:this.pending})
      this.pending=''
      this.lastFlushedAt=Date.now()
    }
    force?this.saveSnapshot():this.saveSnapshotThrottled()
  }

  queueDelta(text){
    if (!text) return
    this.pending+=text
    if (this.pending.length>=BATCH_BYTES) this.flush(false)
    else if (!this.flushTimer) this.flushTimer=setTimeout(()=>this.flush(false),BATCH_MS)
  }

  async fetch(req){
    if (req.method==='OPTIONS') return new Response(null,{status:204,headers:CORS_HEADERS})
    if (req.headers.get('Upgrade')==='websocket'){
      const pair=new WebSocketPair(); const [client,server]=Object.values(pair)
      this.ctx.acceptWebSocket(server)
      return new Response(null,{status:101,webSocket:client})
    }
    if (req.method==='GET'){
      await this.restoreIfCold()
      const text=this.buffer.map(it=>it.text).join('')+this.pending
      return this.corsJSON({rid:this.rid,seq:this.seq,phase:this.phase,done:this.phase==='done',error:this.phase==='error'?this.error:null,text})
    }
    return this.corsJSON({error:'not allowed'},405)
  }

  async webSocketMessage(ws, msgEvt){
    await this.restoreIfCold()
    let msg; try{ msg=JSON.parse(String(msgEvt)) }catch{ return this.send(ws,{type:'err',message:'bad_json'}) }

    if (msg.type==='resume'){ if (!msg.rid||msg.rid!==this.rid) return this.send(ws,{type:'err',message:'stale_run'}); return this.replay(ws,Number.isFinite(+msg.after)?+msg.after:-1) }
    if (msg.type==='stop'){ if (msg.rid&&msg.rid===this.rid) this.stop(); return }
    if (msg.type!=='begin') return this.send(ws,{type:'err',message:'bad_type'})

    const { rid, apiKey, or_body, model, messages, after, provider } = msg
    const body = or_body || (model&&Array.isArray(messages)?{model,messages,stream:true}:null)
    if (!rid||!apiKey||!body||!Array.isArray(body.messages)||!body.messages.length) return this.send(ws,{type:'err',message:'missing_fields'})
    if (this.phase==='running'&&rid!==this.rid) return this.send(ws,{type:'err',message:'busy'})
    if (rid===this.rid&&this.phase!=='idle') return this.replay(ws,Number.isFinite(+after)?+after:-1)

    this.reset()
    this.rid=rid
    this.phase='running'
    this.controller=new AbortController()
    this.saveSnapshot()
    this.ctx.waitUntil(this.stream({apiKey,body,provider:provider||'openrouter'}))
  }

  async stream({apiKey,body,provider}){
    try{
      if (provider==='openai') await this.streamOpenAI({apiKey,body}); else await this.streamOpenRouter({apiKey,body})
      if (this.phase==='running') this.stop()
    }catch(e){
      if (this.phase==='running'){
        const m=String(e?.message||'stream_failed')
        if (!((e&&e.name==='AbortError')||/abort/i.test(m))) this.fail(m)
      }
    }
  }

  isMultimodalMessage(m){ return !!(m&&Array.isArray(m.content)&&m.content.some(p=>{const t=p?.type||'';return t&&t!=='text'&&t!=='input_text'})) }
  extractTextFromMessage(m){ if(!m) return ''; if(typeof m.content==='string') return String(m.content); if(!Array.isArray(m.content)) return ''; return m.content.filter(p=>p&&(p.type==='text'||p.type==='input_text')).map(p=>String(p.type==='text'?(p.text??p.content??''):(p.text??''))).join('') }
  mapContentPartToResponses(part){ const t=part?.type||'text'; if(t==='image_url'||t==='input_image'){ const url=part?.image_url?.url||part?.image_url||''; return url?{type:'input_image',image_url:String(url)}:null } if(t==='text'||t==='input_text'){ const text=t==='text'?(part.text??part.content??''):(part.text??''); return {type:'input_text',text:String(text)} } return {type:'input_text',text:`[${t}:${part?.file?.filename||'file'}]`} }
  buildInputForResponses(messages){
    if (!Array.isArray(messages)||!messages.length) return ''
    const multi=messages.some(m=>this.isMultimodalMessage(m))
    if(!multi){ return messages.length===1?this.extractTextFromMessage(messages[0]):messages.map(m=>({role:m.role,content:this.extractTextFromMessage(m)})) }
    return messages.map(m=>{ let content=[{type:'input_text',text:String(m.content||'')}]; if(Array.isArray(m.content)) content=m.content.map(p=>this.mapContentPartToResponses(p)).filter(Boolean); return {role:m.role,content} })
  }

  async streamOpenAI({apiKey,body}){
    const client=new OpenAI({apiKey})
    const params={model:body.model,input:this.buildInputForResponses(body.messages||[]),temperature:body.temperature,stream:true}
    if (Number.isFinite(+body.max_tokens)&&+body.max_tokens>0) params.max_output_tokens=+body.max_tokens
    if (body.reasoning?.effort) params.reasoning={effort:body.reasoning.effort}
    if (body.verbosity) params.verbosity=body.verbosity
    this.oaStream=await client.responses.stream(params)
    try{
      for await (const ev of this.oaStream){
        if (this.phase!=='running') break
        const d=ev.delta
        if (ev.type.endsWith('.delta')&&d) this.queueDelta(d)
      }
    }finally{
      try{ this.oaStream?.controller?.abort() }catch{}
      this.oaStream=null
    }
  }

  async streamOpenRouter({apiKey,body}){
    const client=new OpenAI({apiKey,baseURL:'https://openrouter.ai/api/v1'})
    const stream=await client.chat.completions.create({...body,stream:true},{signal:this.controller.signal})
    for await (const chunk of stream){
      if (this.phase!=='running') break
      const d=chunk?.choices?.[0]?.delta?.content??''
      if (d) this.queueDelta(d)
    }
  }

  stop(){
    if (this.phase!=='running') return
    this.flush(true)
    this.phase='done'
    this.error=null
    try{ this.controller?.abort() }catch{}
    try{ this.oaStream?.controller?.abort() }catch{}
    this.saveSnapshot()
    this.bcast({type:'done'})
  }

  fail(message){
    if (this.phase==='error') return
    this.flush(true)
    this.phase='error'
    this.error=String(message||'stream_failed')
    try{ this.controller?.abort() }catch{}
    try{ this.oaStream?.controller?.abort() }catch{}
    this.saveSnapshot()
    this.bcast({type:'err',message:this.error})
  }

  async webSocketClose(){}

  async webSocketError(){}
}
