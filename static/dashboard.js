/* GRAEAE Hive Mind dashboard script (Phase 1c: extracted from dashboard.html) */
'use strict';
const BUS = location.origin;
let _myUrn = null;
let _editingJobId = null;
let _submitOpen = false;
let _sessionPromise = null;

// ── helpers ──────────────────────────────────────────────────────────
function esc(v){return String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
// Per-panel change detection: JSON signature of a panel's data. If a refresh
// produced the SAME data, the panel skips its innerHTML rebuild entirely (no
// flicker, no lost scroll/expansion, no needless reflow).
const _panelSig={};
function unchanged(key,data){const s=JSON.stringify(data);if(_panelSig[key]===s)return true;_panelSig[key]=s;return false;}
function td(html,cls=''){const t=document.createElement('td');t.className=cls;if(html instanceof Node)t.appendChild(html);else t.innerHTML=html;return t;}
async function establishSession(){
  // LAN-trusted bus (operator 2026-10-15): the fleet is local-LAN only and the
  // bus requires NO auth, so there is no administrator token to enter and no
  // /v1/auth/session route. This is a no-op kept so api() callers don't change.
  return {};
}
async function api(method,path,body,retry=true){
  const opts={method,credentials:'same-origin',headers:{'Content-Type':'application/json'}};
  if(body)opts.body=JSON.stringify(body);
  const r=await fetch(BUS+path,opts);
  const j=await r.json().catch(()=>({}));
  if(r.status===401&&retry){await establishSession();return api(method,path,body,false);}
  if(!r.ok)throw new Error(j.detail||JSON.stringify(j));
  return j;
}
function relTime(ts){if(!ts)return '';const d=(Date.now()/1000-ts);if(d<60)return Math.round(d)+'s ago';if(d<3600)return Math.round(d/60)+'m ago';return Math.round(d/3600)+'h ago';}
function pbar(p){return `<span class="pbar"><span class="pbar-fill" style="width:${Math.min(100,p)}%"></span></span>`;}
function badgeCls(s){return 'badge badge-'+(s||'queued');}

// ── registration & heartbeat ──────────────────────────────────────────
async function registerAgent(){
  try{
    const r=await api('POST','/v1/agents/register',{
      host:'dashboard-browser',runtime:'human',kind:'human',
      autonomy_level:'interactive',auth_method:'unknown',
      capabilities:['job-submit','triage','cancel'],
      version:'dashboard-2026-05',
    });
    _myUrn=r.urn;
    const el=document.getElementById('meta-reg');
    el.textContent='agent: '+r.urn.split(':').slice(2,4).join('@');
    el.className='pill online';
    document.getElementById('submit-btn').disabled=false;
    setInterval(()=>api('POST','/v1/agents/heartbeat',{urn:_myUrn,status:'online'}).catch(()=>{}),25000);
  }catch(e){
    const el=document.getElementById('meta-reg');
    el.textContent='agent: reg failed — '+e.message.slice(0,40);
    el.className='pill err';
  }
}

// ── submit form ───────────────────────────────────────────────────────
function toggleSubmit(){
  _submitOpen=!_submitOpen;
  document.getElementById('submit-body').className=_submitOpen?'open':'';
  document.getElementById('submit-toggle').textContent=_submitOpen?'▲ collapse':'▼ expand';
}
function setStatus(msg,isErr=false){
  const el=document.getElementById('submit-status');
  el.textContent=msg;el.className=isErr?'err':'';
}
function clearForm(){
  ['f-desc','f-project'].forEach(id=>document.getElementById(id).value='');
  document.getElementById('f-priority').value=50;
  document.getElementById('f-kind').value='investigation';
  document.getElementById('f-tier').value='B';
  document.getElementById('f-eligible').value='';
  cancelEdit();
  setStatus('');
}
function cancelEdit(){
  _editingJobId=null;
  document.getElementById('edit-banner').className='';
  document.getElementById('submit-title').textContent='New Job';
}
function setSelectOrAdd(selectEl, value){
  if(!value)return;
  let found=false;
  for(const o of selectEl.options){if(o.value===value){found=true;break;}}
  if(!found){
    const o=document.createElement('option');
    o.value=o.textContent=value;
    selectEl.appendChild(o);
  }
  selectEl.value=value;
}
function fillFormForEdit(job){
  _editingJobId=job.id;
  document.getElementById('f-desc').value=job.description||'';
  document.getElementById('f-priority').value=job.priority||50;
  document.getElementById('f-project').value=job.project||'';
  setSelectOrAdd(document.getElementById('f-kind'),job.kind||'investigation');
  document.getElementById('f-tier').value=(job.max_cost_tier||'B').toUpperCase();
  const ek=(job.eligible_kinds||[]);
  document.getElementById('f-eligible').value=ek.join(', ');
  document.getElementById('edit-banner').className='show';
  document.getElementById('submit-title').textContent='Edit Job';
  if(!_submitOpen)toggleSubmit();
  document.getElementById('f-desc').focus();
  closeModalBtn();
  setStatus('');
}
async function submitJob(){
  if(!_myUrn){setStatus('Not registered yet — wait a moment',true);return;}
  const desc=document.getElementById('f-desc').value.trim();
  if(!desc){setStatus('Description required',true);return;}
  const eligible=document.getElementById('f-eligible').value
    .split(',').map(s=>s.trim()).filter(Boolean);
  const body={
    submitter_urn:_myUrn,
    kind:document.getElementById('f-kind').value,
    description:desc,
    priority:parseInt(document.getElementById('f-priority').value,10)||50,
    max_cost_tier:document.getElementById('f-tier').value,
    eligible_kinds:eligible.length?eligible:undefined,
    project:document.getElementById('f-project').value.trim()||undefined,
  };
  const btn=document.getElementById('submit-btn');
  btn.disabled=true;
  setStatus('Submitting…');
  try{
    const oldId=_editingJobId;
    cancelEdit();
    const r=await api('POST','/v1/jobs',body);
    if(oldId){
      try{await api('PATCH','/v1/jobs/'+oldId,{status:'cancelled'});}catch(_){}
    }
    const shortId=r.id.slice(0,8);
    if(r.cache_hit){
      setStatus(`✓ Cache hit — job ${shortId} done instantly`);
    } else {
      setStatus(`✓ Job ${shortId} queued — watching for agent pickup…`);
    }
    clearForm();
    await refreshAll();
  }catch(e){
    setStatus('Error: '+e.message,true);
    _editingJobId=_editingJobId; // restore if edit was set
  }finally{
    btn.disabled=false;
  }
}

// ── cancel job ────────────────────────────────────────────────────────
async function cancelJob(id,e){
  if(e)e.stopPropagation();
  if(!confirm('Cancel job '+id.slice(0,8)+'?'))return;
  try{
    await api('PATCH','/v1/jobs/'+id,{status:'cancelled'});
    await refreshAll();
  }catch(err){alert('Cancel failed: '+err.message);}
}

// ── job detail modal ──────────────────────────────────────────────────
let _lastJobs={};
function showJobModal(job){
  _lastJobs[job.id]=job;
  document.getElementById('modal-title').textContent=
    `Job ${job.id.slice(0,12)}… · ${job.kind} · P${job.priority}`;
  const meta=document.getElementById('modal-meta');
  meta.innerHTML=[
    `<span class="${badgeCls(job.status)}">${esc(job.status)}</span>`,
    job.claimed_by?`<span>claimed by ${esc(job.claimed_by.split(':').slice(2,4).join('@'))}</span>`:'',
    `<span>created ${relTime(job.started_at)}</span>`,
    job.ended_at?`<span>ended ${relTime(job.ended_at)}</span>`:'',
    job.estimated_cost_usd?`<span>$${job.estimated_cost_usd.toFixed(4)}</span>`:'',
    (job.eligible_kinds||[]).length?`<span>eligible: ${esc((job.eligible_kinds).join(', '))}</span>`:'',
  ].filter(Boolean).join('');
  document.getElementById('modal-desc').textContent=job.description||'(no description)';
  // result display
  const resSec=document.getElementById('modal-result-sec');
  const rawSec=document.getElementById('modal-raw-sec');
  const resEl=document.getElementById('modal-result');
  const rawEl=document.getElementById('modal-raw');
  if(job.result){
    const r=job.result;
    const texts=[];
    if(r.summary)texts.push('SUMMARY\n'+r.summary);
    if(r.text)texts.push(r.text);
    if(r.output)texts.push('OUTPUT\n'+r.output);
    if(r.stdout)texts.push('STDOUT\n'+r.stdout);
    if(r.stderr)texts.push('STDERR\n'+r.stderr);
    if(r.files_changed&&r.files_changed.length)texts.push('FILES CHANGED\n'+r.files_changed.join('\n'));
    if(r.commits&&r.commits.length)texts.push('COMMITS\n'+r.commits.map(c=>(c&&c.sha)?(c.sha.slice(0,10)+' '+(c.subject||'')):c).join('\n'));
    if(r.review)texts.push('REVIEW: '+(r.review.verdict||'?')+(r.review.model?' ['+r.review.model+']':'')+'\n'+(r.review.text||''));
    if(r.unreviewed)texts.push('\u26a0 UNREVIEWED: '+r.unreviewed);
    if(r.pushed_branch)texts.push('PUSHED BRANCH\n'+r.pushed_branch+(r.pushed_remote?(' @ '+r.pushed_remote):''));
    if(r.failure_reason)texts.push('FAILURE\n'+r.failure_reason);
    if(r.exit_code!==undefined)texts.push('exit_code: '+r.exit_code);
    if(texts.length){
      resEl.textContent=texts.join('\n\n──────\n\n');
      resSec.style.display='';
    } else {
      resSec.style.display='none';
    }
    rawEl.textContent=JSON.stringify(r,null,2);
    rawSec.style.display='';
  } else {
    resSec.style.display='none';
    rawSec.style.display='none';
    rawEl.textContent='';
  }
  // modal actions
  const acts=document.getElementById('modal-actions');
  acts.innerHTML='';
  if(job.status==='queued'||job.status==='offered'){
    const editBtn=document.createElement('button');
    editBtn.className='btn btn-ghost';
    editBtn.textContent='✏️ Edit & Resubmit';
    editBtn.onclick=()=>fillFormForEdit(job);
    acts.appendChild(editBtn);
    const cancelBtn=document.createElement('button');
    cancelBtn.className='btn btn-danger';
    cancelBtn.textContent='🗑 Cancel Job';
    cancelBtn.onclick=()=>cancelJob(job.id).then(closeModalBtn);
    acts.appendChild(cancelBtn);
  }
  const closeBtn=document.createElement('button');
  closeBtn.className='btn btn-ghost';
  closeBtn.textContent='Close';
  closeBtn.onclick=closeModalBtn;
  acts.appendChild(closeBtn);
  document.getElementById('modal-backdrop').className='open';
}
function closeModal(e){if(e&&e.target===document.getElementById('modal-backdrop'))closeModalBtn();}
function closeModalBtn(){document.getElementById('modal-backdrop').className='';}

// ── refresh panels ────────────────────────────────────────────────────
async function refreshAgents(){
  const d=await api('GET','/v1/agents?status=online');
  const groups=new Map();
  for(const a of d.agents){
    const k=`${a.kind}|${a.host}|${a.model||''}`;
    const g=groups.get(k);
    if(g){g.count++;if((a.last_heartbeat||0)>(g.last_heartbeat||0))g.last_heartbeat=a.last_heartbeat;}
    else groups.set(k,{...a,count:1});
  }
  const rows=[...groups.values()].sort((a,b)=>(a.kind+a.host).localeCompare(b.kind+b.host));
  if(unchanged('agents',[rows,d.count]))return;
  document.getElementById('agent-count').textContent=`${rows.length} pools · ${d.count} instances`;
  const tb=document.querySelector('#agent-tbl tbody');tb.innerHTML='';
  const AKC={claude:'#ffa657',zeroclaw:'#79c0ff',openclaw:'#d2a8ff',opencode:'#7ee787',codex:'#e3b341',doctor:'#f85149','dream-walker':'#ff7bd5',system:'#8b949e',human:'#c9d1d9',hermes:'#56d4dd'};
  for(const a of rows){
    const r=tb.insertRow();
    const _kc=AKC[a.kind]||'#8b949e';const kindCell=`<span style="color:${_kc}">●</span> <b style="color:${_kc}">${esc(a.kind)}</b>`+(a.count>1?` <small class="dim">×${a.count}</small>`:'');
    r.appendChild(td(kindCell));
    r.appendChild(td(esc(a.host)));
    r.appendChild(td(`<small class="dim">${esc(a.model||'')}</small>`));
    const tt=td('');tt.appendChild(Object.assign(document.createElement('span'),{className:'tag t-'+(a.cost_tier||'C'),textContent:a.cost_tier||'?'}));r.appendChild(tt);
  }
}

async function refreshJobs(){
  const [q,offered]=await Promise.all([
    api('GET','/v1/jobs?status=queued&limit=250'),
    api('GET','/v1/jobs?status=offered&limit=25').catch(()=>({jobs:[]})),
  ]);
  const all=[...(q.jobs||[]),...(offered.jobs||[])].sort((a,b)=>b.priority-a.priority);
  if(unchanged('jobs',all))return;
  const cnt=all.length;
  document.getElementById('queue-cnt').textContent=`(${cnt})`;
  document.getElementById('meta-queue').textContent=cnt?cnt+' queued':'queue clear';
  const tb=document.querySelector('#jobs-tbl tbody');tb.innerHTML='';
  for(const j of all){
    const r=tb.insertRow();
    r.className='row-clickable';
    r.onclick=()=>showJobModal(j);
    r.appendChild(td(`${pbar(j.priority)} <b>${j.priority}</b>`));
    r.appendChild(td(`<span style="color:#a5d6ff">${esc(j.kind)}</span><br><span class="s-${esc(j.status)} dim" style="font-size:.78em">${esc(j.status)}</span>`));
    r.appendChild(td(`<small class="dim">${esc((j.description||'').slice(0,55))}</small>`));
    r.appendChild(td(j.estimated_cost_usd?'$'+j.estimated_cost_usd.toFixed(4):'-'));
    const actCell=td('');
    actCell.onclick=e=>e.stopPropagation();
    const acts=document.createElement('div');acts.className='act-btns';
    if(j.status==='queued'||j.status==='offered'){
      const eb=document.createElement('button');
      eb.className='btn btn-ghost btn-sm';eb.title='Edit & Resubmit';eb.textContent='✏️';
      eb.onclick=()=>fillFormForEdit(j);
      acts.appendChild(eb);
      const cb=document.createElement('button');
      cb.className='btn btn-danger btn-sm';cb.title='Cancel job';cb.textContent='✕';
      cb.onclick=(e)=>cancelJob(j.id,e);
      acts.appendChild(cb);
    }
    actCell.appendChild(acts);
    r.appendChild(actCell);
  }
}

async function refreshRunning(){
  // Worker POOL grouped BY HOST. A host runs N worker instances (e.g. ULTRA x6,
  // hydra x4); show one card per host with "busy/total active" + the running job
  // kinds, NOT one card per instance. kind==zeroclaw is the build fan-out pool.
  // Maps use Object.create(null) because keys are untrusted URN/host strings
  // (avoids __proto__/constructor prototype-pollution + Object.values drops).
  const [aj,jj]=await Promise.all([
    api('GET','/v1/agents?status=online').catch(()=>({agents:null})),
    api('GET','/v1/jobs?status=running&limit=200').catch(()=>({jobs:[]})),
  ]);
  const agents=(Array.isArray(aj.agents)?aj.agents:[]).filter(a=>a&&a.kind==='zeroclaw');
  const jobs=jj.jobs||[];
  const byClaimer=Object.create(null);
  for(const j of jobs){ if(j.claimed_by) byClaimer[j.claimed_by]=j; }
  const hosts=Object.create(null);
  for(const a of agents){
    const host=(a.urn||'').split(':')[3]||'?';
    const h=hosts[host]||(hosts[host]={host,total:0,busy:0,jobs:[]});
    h.total++;
    const j=byClaimer[a.urn];
    if(j){ h.busy++; h.jobs.push(j); }
  }
  let list=Object.values(hosts);
  let degraded=false;
  if(!list.length && jobs.length){
    degraded=true;
    const hg=Object.create(null);
    for(const j of jobs){
      const parts=(j.claimed_by||'').split(':');
      if(parts[2]!=='zeroclaw') continue;
      const host=parts[3]||'?';
      const h=hg[host]||(hg[host]={host,total:0,busy:0,jobs:[]});
      h.total++; h.busy++; h.jobs.push(j);
    }
    list=Object.values(hg);
  }
  if(unchanged('running',[list,degraded]))return;
  const grid=document.getElementById('workers-grid');
  const empty=document.getElementById('workers-empty');
  const totW=list.reduce((n,h)=>n+h.total,0);
  const totB=list.reduce((n,h)=>n+h.busy,0);
  document.getElementById('workers-cnt').textContent=
    list.length?`(${list.length} hosts \u00b7 ${totW} workers \u00b7 ${totB} busy${degraded?' \u00b7 degraded':''})`:'';
  grid.innerHTML='';
  if(!list.length){empty.style.display='';return;}
  empty.style.display='none';
  list.sort((a,b)=>(b.busy-a.busy)||a.host.localeCompare(b.host));
  for(const h of list){
    const card=document.createElement('div');
    card.className='worker-card'+(h.busy?'':' idle');
    if(h.busy){ const j0=h.jobs[0]; card.onclick=()=>showJobModal(j0); }
    const jobKinds=h.jobs.map(j=>esc(j.kind||'?')).join(', ');
    card.innerHTML=`
      <div class="wc-agent"><span class="wc-dot"></span>
        <span class="wc-kind-badge">zeroclaw</span> ${esc(h.host)}</div>
      <div class="wc-job">${h.busy}/${h.total} active</div>
      <div class="wc-desc${h.busy?'':' dim'}">${h.busy?jobKinds:'all idle \u00b7 waiting for work'}</div>
      <div class="wc-meta"><span>${h.total} worker${h.total===1?'':'s'}</span><span>${h.busy} busy</span></div>`;
    grid.appendChild(card);
  }
}


async function refreshResults(){
  const d=await api('GET','/v1/jobs?status=done&limit=20');
  const jobs=(d.jobs||[]).sort((a,b)=>(b.ended_at||0)-(a.ended_at||0));
  if(unchanged('results',jobs))return;
  const rtotal=d.total||jobs.length;
  document.getElementById('results-cnt').textContent=`(${rtotal})`;
  const el=document.getElementById('results-list');
  el.innerHTML='';
  if(!jobs.length){el.innerHTML='<div class="dim" style="font-size:.82em;padding:.3em">No completed jobs yet</div>';return;}
  for(const j of jobs.slice(0,15)){
    const card=document.createElement('div');
    const r=j.result||{};
    const isBuild=String(j.kind||'').startsWith('build:')||r.head_sha||r.committed!==undefined;
    const hasCode=(r.commits&&r.commits.length)||r.head_sha||r.committed;
    card.className='result-card done '+(hasCode?'rc-code':'rc-text');
    let preview=r.summary||r.text||r.output;
    if(!preview&&isBuild){
      const parts=[];
      if(r.repo)parts.push('repo='+r.repo);
      if(r.committed)parts.push('committed');
      if(r.pushed)parts.push('pushed→'+(r.pushed_branch||r.local_branch||'?'));
      else if(r.committed)parts.push('not pushed');
      if(r.files_changed)parts.push((r.files_changed.length||0)+' files');
      if(r.review&&r.review.verdict)parts.push('review='+r.review.verdict);
      preview=parts.length?parts.join('  ·  '):'(build, no detail)';
    }
    if(!preview)preview='(no text result)';
    const _at=(r.alias_tried&&r.alias_tried.length)?r.alias_tried[r.alias_tried.length-1]:'';const prov=_at||r.gateway_provider_reported||r.gateway_model||r.agent_alias||'';
    const isCodex=/codex/i.test(j.kind||'')||/codex/i.test(prov)||/codex/i.test(r.via||'')||/codex/i.test(r.gateway_provider||'')||r.runtime==='codex';
    let agentType=r.runtime||String(j.kind||'').split(':')[0]||'agent';
    const wkHost=(String(j.claimed_by||'').match(/:zeroclaw:([^:]+):/)||[])[1]||(String(j.claimed_by||'').split(':')[3])||'';
    if(isCodex)agentType='codex';
    if(isCodex)card.classList.add('rc-codexcard');
    const commits=(r.commits&&r.commits.length)?`<div style="font-size:.74em;color:#7ee787;margin-top:.2em">commits: ${esc(r.commits.slice(0,3).join(', '))}</div>`:'';
    const _dur=r.duration_sec||(r.drive&&r.drive.duration_sec);
    const _rev=(r.review&&r.review.verdict)?r.review.verdict:(typeof r.review==='string'?r.review:'');
    const buildChips=isBuild?`<div style="font-size:.74em;margin-top:.2em;display:flex;gap:.45em;flex-wrap:wrap">`
      +(r.head_sha?`<span style="color:#a5d6ff;font-family:monospace" title="head commit">${esc(String(r.head_sha).slice(0,10))}</span>`:'')
      +(r.committed?`<span style="color:#7ee787">✓ committed</span>`:'')
      +(r.pushed?`<span style="color:#7ee787">✓ pushed ${esc(r.pushed_branch||r.local_branch||'')}</span>`:(r.committed?`<span style="color:#d29922">⚠ not pushed${r.local_branch?' ('+esc(r.local_branch)+')':''}</span>`:''))
      +(r.files_changed?`<span class="dim">${(r.files_changed.length||0)} files</span>`:'')
      +(_rev?`<span style="color:#ffb454" title="review verdict">review:${esc(_rev)}</span>`:'')
      +((r.drive&&r.drive.tool_call_count!==undefined)?`<span class="dim">${r.drive.tool_call_count} tools</span>`:'')
      +(_dur?`<span class="dim">${Math.round(_dur)}s</span>`:'')
      +`</div>`:'';
    card.innerHTML=`
      <div class="rc-head">
        <span class="rc-kind">${esc(j.kind)}</span>
        <span class="rc-agent${isCodex?' codex':''}" title="agent type">${esc(agentType)}</span>
        ${wkHost?`<span class="rc-host" title="host that ran the job">\ud83d\udda5\ufe0f ${esc(wkHost)}</span>`:''}
        <span class="${badgeCls(j.status)}">${esc(j.status)}</span>
        ${prov?`<span class="rc-model" title="model/provider that did this job">${esc(prov)}</span>`:''}
        <span class="rc-id">${esc(j.id.slice(0,10))}</span>
        <span class="dim" style="font-size:.75em;margin-left:auto">${relTime(j.ended_at)}</span>
      </div>
      <div class="dim" style="font-size:.78em;margin-bottom:.25em">${esc((j.description||'').slice(0,80))}</div>
      <div class="rc-body">${esc(String(preview).slice(0,200))}</div>
      ${buildChips}
      ${commits}
    `;
    card.onclick=()=>showJobModal(j);
    el.appendChild(card);
  }
}

let _failedJobs={};
async function refreshDoctor(){
  // Pull jobs across all relevant statuses; filter doctor-eligible OR claimed-by-doctor
  const [q,c,r,off]=await Promise.all([
    api('GET','/v1/jobs?status=queued&limit=500').catch(()=>({jobs:[]})),
    api('GET','/v1/jobs?status=claimed&limit=200').catch(()=>({jobs:[]})),
    api('GET','/v1/jobs?status=running&limit=200').catch(()=>({jobs:[]})),
    api('GET','/v1/jobs?status=offered&limit=50').catch(()=>({jobs:[]})),
  ]);
  const all=[...(q.jobs||[]),...(c.jobs||[]),...(r.jobs||[]),...(off.jobs||[])];
  const doctorJobs=all.filter(j=>{
    const ek=j.eligible_kinds||[];
    if(ek.includes('doctor')) return true;
    const cb=j.claimed_by||'';
    if(cb.startsWith('urn:agent:doctor:')) return true;
    return false;
  }).sort((a,b)=>{
    // Group by status: claimed/running first (active), then queued
    const order={running:0,claimed:1,offered:2,queued:3};
    const ao=order[a.status]??9, bo=order[b.status]??9;
    if(ao!==bo) return ao-bo;
    return (b.priority||0)-(a.priority||0);
  });
  document.getElementById('doctor-cnt').textContent=`(${doctorJobs.length})`;
  // Active doctor agents
  const ag=await api('GET','/v1/agents').catch(()=>({agents:[]}));
  const docs=(ag.agents||[]).filter(a=>a.kind==='doctor');
  const onlineDocs=docs.filter(a=>a.status==='online'||a.status==='idle');
  document.getElementById('doctor-workers').textContent=
    `${onlineDocs.length}/${docs.length} doctor agents online`;
  // Render table
  const tb=document.querySelector('#doctor-tbl tbody');tb.innerHTML='';
  const now=Date.now()/1000;
  for(const j of doctorJobs.slice(0,80)){
    const row=tb.insertRow();
    row.className='row-clickable';
    row.onclick=()=>showJobModal(j);
    row.appendChild(td(`<b>${j.priority||0}</b>`));
    row.appendChild(td(`<span style="color:#a5d6ff">${esc(j.kind||'')}</span>`));
    const statusColor={running:'#7ee787',claimed:'#ffa657',queued:'#c9d1d9',offered:'#a5d6ff'}[j.status]||'#c9d1d9';
    row.appendChild(td(`<span style="color:${statusColor}">${esc(j.status||'?')}</span>`));
    row.appendChild(td(`<small class="dim">${esc((j.description||'').slice(0,60))}</small>`));
    const cb=j.claimed_by||'';
    const cbShort=cb?cb.split(':').slice(2,4).join('@'):'<i class="dim">unclaimed</i>';
    row.appendChild(td(`<small>${esc(cbShort)}</small>`));
    const ref=j.started_at||j.claimed_at||0;
    const age=ref?Math.floor(now-ref):null;
    row.appendChild(td(age!==null?`<small class="dim">${age}s</small>`:'<small class="dim">-</small>'));
  }
}

async function refreshDoctor(){ /* merged into unified Triage Queue (refreshFailed) */ }
  async function refreshFailed(){
    // UNIFIED TRIAGE QUEUE (operator 2026-06-04): one queue, all non-done/non-cancelled
    // statuses, color-coded + priority. Replaces the separate Failed + Doctor panels.
    const sts=['dead-letter','failed','running','claimed','offered','queued'];
    const res=await Promise.all(sts.map(st=>api('GET',`/v1/jobs?status=${st}&limit=${st==='queued'?300:100}`).catch(()=>({jobs:[]}))));
    let jobs=[];
    sts.forEach((st,i)=>{(res[i].jobs||[]).forEach(j=>{if(!j.status)j.status=st;jobs.push(j);});});
    _failedJobs=Object.fromEntries(jobs.map(j=>[j.id,j]));
    const order={'dead-letter':0,failed:1,running:2,claimed:3,offered:4,queued:5};
    jobs.sort((a,b)=>{const ao=order[a.status]??9,bo=order[b.status]??9;if(ao!==bo)return ao-bo;return (b.priority||0)-(a.priority||0);});
    if(unchanged('failed',jobs))return;
    const cntEl=document.getElementById('failed-cnt');
    if(cntEl)cntEl.textContent=`(${jobs.length})`;
    const el=document.getElementById('failed-list');
    if(!el)return;
    el.innerHTML='';
    if(!jobs.length){ el.innerHTML='<div class="failed-panel-empty">Queue empty — nothing in flight ✓</div>'; return; }
    for(const j of jobs.slice(0,150)){
      const r=j.result||{};
      const reason=r.failure_reason||r.stderr||r.error||r.note||r.worker_error||r.summary||'';
      const card=document.createElement('div');
      card.className='failed-card tq-'+(j.status||'queued');
      card.onclick=()=>showJobModal(j);
      const jid=j.id, prio=j.priority||0;
      const pcls=prio>=80?'pri-hi':(prio>=40?'pri-mid':'pri-lo');
      const isTerminal=(j.status==='failed'||j.status==='dead-letter');
      const acts=isTerminal?`<div class="fc-acts" onclick="event.stopPropagation()">
          <button class="btn btn-ghost btn-sm" onclick="resubmitFailed('${jid}')">🔁 Resubmit</button>
          <button class="btn btn-primary btn-sm" onclick="triageAndResubmit('${jid}')">🔍 Triage &amp; Resubmit</button>
          <button class="btn btn-danger btn-sm" title="Dismiss" onclick="dismissFailed('${jid}',event)">✕</button>
        </div>`:'';
      card.innerHTML=`
        <div class="fc-head">
          <span class="tq-pri ${pcls}" title="priority level">P${prio}</span>
          <span class="fc-kind">${esc(j.kind)}</span><span class="${badgeCls(j.status)}" title="job status" style="margin-left:.3em">${esc(j.status)}</span>
          ${j.claimed_by?`<span class="dim" style="font-size:.75em">via ${esc(j.claimed_by.split(':').slice(2,4).join('@').slice(0,20))}</span>`:''}
          <span class="dim" style="font-size:.75em;margin-left:auto">${relTime(j.ended_at||j.started_at||j.claimed_at)}</span>
        </div>
        <div class="fc-desc">${esc((j.description||'').slice(0,100))}</div>
        ${reason?`<div class="fc-reason">${esc(String(reason).slice(0,250))}</div>`:''}
        ${acts}`;
      el.appendChild(card);
    }
  }
  function resubmitFailed(id){
  const j=_failedJobs[id];
  if(!j)return;
  // Pre-fill form but don't cancel the failed job on submit (it's already done)
  _editingJobId=null;
  document.getElementById('f-desc').value=j.description||'';
  document.getElementById('f-priority').value=j.priority||50;
  document.getElementById('f-project').value=j.project||'';
  setSelectOrAdd(document.getElementById('f-kind'),j.kind||'investigation');
  document.getElementById('f-tier').value=(j.max_cost_tier||'B').toUpperCase();
  document.getElementById('f-eligible').value=(j.eligible_kinds||[]).join(', ');
  document.getElementById('edit-banner').className='';
  document.getElementById('submit-title').textContent='Resubmit Job';
  if(!_submitOpen)toggleSubmit();
  document.getElementById('f-desc').focus();
  setStatus('Review and submit — failed job will NOT be cancelled (already in terminal state)');
}

async function triageAndResubmit(id){
  const j=_failedJobs[id];
  if(!j||!_myUrn){setStatus('Not registered or job not found',true);return;}
  const r=j.result||{};
  const reason=r.failure_reason||r.stderr||r.error||r.summary||'(no failure detail)';
  const triageDesc=`Triage this failed Hive job and recommend a corrected resubmit.\n\nFailed kind: ${j.kind}\nPriority: ${j.priority}\nEligible: ${(j.eligible_kinds||[]).join(', ')}\n\nOriginal description:\n${j.description||''}\n\nFailure output:\n${String(reason).slice(0,800)}`;
  try{
    const tr=await api('POST','/v1/jobs',{
      submitter_urn:_myUrn,
      kind:'triage',
      description:triageDesc,
      priority:Math.min((j.priority||50)+10,100),
      eligible_kinds:['codex','claude'],
      project:j.project||undefined,
    });
    setStatus(`✓ Triage job ${tr.id.slice(0,8)} queued — review form below then submit corrected job`);
    // Pre-fill form for the corrected resubmit
    _editingJobId=null;
    document.getElementById('f-desc').value=j.description||'';
    document.getElementById('f-priority').value=j.priority||50;
    document.getElementById('f-project').value=j.project||'';
    setSelectOrAdd(document.getElementById('f-kind'),j.kind||'investigation');
    document.getElementById('f-tier').value=(j.max_cost_tier||'B').toUpperCase();
    document.getElementById('f-eligible').value=(j.eligible_kinds||[]).join(', ');
    document.getElementById('edit-banner').className='';
    document.getElementById('submit-title').textContent='Corrected Resubmit (after triage)';
    if(!_submitOpen)toggleSubmit();
    document.getElementById('f-desc').focus();
    await refreshAll();
  }catch(e){setStatus('Triage error: '+e.message,true);}
}

async function dismissFailed(id,e){
  if(e)e.stopPropagation();
  try{
    await api('PATCH','/v1/jobs/'+id,{status:'cancelled'}).catch(()=>{});
    delete _failedJobs[id];
    await refreshFailed();
  }catch(_){}
}

async function refreshCosts(){
  const d=await api('GET','/v1/stats/costs?group_by=provider&since_hours=24');
  if(unchanged('costs',d))return;
  const tot=d.totals||{};
  document.getElementById('cost-totals').innerHTML=
    `<span class="stat">jobs <b>${tot.job_count||0}</b></span>`+
    `<span class="stat">spent <b class="savings">$${(tot.estimated_cost_usd||0).toFixed(4)}</b></span>`;
  const tb=document.querySelector('#cost-tbl tbody');tb.innerHTML='';
  for(const b of (d.buckets||[]).slice(0,8)){
    const r=tb.insertRow();
    r.appendChild(td(esc(b.bucket)));
    r.appendChild(td(b.job_count));
    r.appendChild(td(`<small class="dim">${(b.tokens_in||0).toLocaleString()}/${(b.tokens_out||0).toLocaleString()}</small>`));
    r.appendChild(td('$'+(b.estimated_cost_usd||0).toFixed(4)));
  }
}

async function refreshWorkers(){
  const d=await api('GET','/v1/stats/workers?top_n=20');
  if(unchanged('workers',d))return;
  const tb=document.querySelector('#wstats-tbl tbody');tb.innerHTML='';
  for(const w of (d.workers||[]).slice(0,15)){
    const parts=(w.urn||'').split(':');
    const lab=parts.length>=4?`${parts[2]}@${parts[3].slice(-10)}`:w.urn;
    const r=tb.insertRow();
    r.appendChild(td(`<small class="dim">${esc(lab)}</small>`));
    r.appendChild(td(esc(w.kind)));
    r.appendChild(td(`<span class="s-done">${w.success_count}</span>`));
    r.appendChild(td(`<span class="s-failed">${w.fail_count}</span>`));
    r.appendChild(td((w.success_pct||0)+'%'));
    r.appendChild(td(w.avg_duration_sec||0));
  }
}

async function refreshSchedules(){
  const d=await api('GET','/v1/schedules');
  if(unchanged('schedules',d))return;
  const tb=document.querySelector('#sched-tbl tbody');tb.innerHTML='';
  for(const s of (d.schedules||[])){
    const r=tb.insertRow();
    r.appendChild(td(esc(s.name)));
    r.appendChild(td(s.interval_seconds+'s'));
    r.appendChild(td(s.enabled?'<span class="s-online">on</span>':'<span class="s-offline">off</span>'));
    r.appendChild(td(s.fire_count));
    r.appendChild(td(s.next_fire_at?new Date(s.next_fire_at*1000).toLocaleTimeString():'-'));
  }
}

function loadBar(pct){
  if(pct==null)return '<span class="dim">-</span>';
  const cls=pct<70?'load-ok':pct<90?'load-warn':'load-crit';
  return `<span class="load-bar"><span class="load-bar-fill ${cls}" style="width:${Math.min(100,pct)}%"></span></span>${pct.toFixed(0)}%`;
}

async function refreshGPU(){
  const d = await api('GET','/v1/hosts?include_stale=false').catch(()=>({hosts:[]}));
  const hosts = d.hosts || [];
  const rows = [];
  for (const h of hosts) {
    const gpus = h.gpus_runtime || [];
    if (!gpus.length) continue;
    for (const g of gpus) {
      rows.push({host: h.host, gpu: g});
    }
  }
  document.getElementById('gpu-cnt').textContent = `(${rows.length} GPU${rows.length===1?'':'s'} reporting)`;
  if(unchanged('gpu',rows))return;
  const tb = document.querySelector('#gpu-tbl tbody'); tb.innerHTML = '';
  for (const r of rows) {
    const tr = tb.insertRow();
    const g = r.gpu;
    const util = g.util_pct;
    const utilCls = util==null?'load-ok':util<70?'load-ok':util<90?'load-warn':'load-crit';
    const utilCell = `<span class="load-bar" style="width:60px"><span class="load-bar-fill ${utilCls}" style="width:${util||0}%"></span></span> ${util==null?'-':util.toFixed(0)+'%'}`;
    const mu = g.mem_used_mib, mt = g.mem_total_mib;
    const memPct = (mu!=null && mt && mt>0) ? (100*mu/mt) : null;
    const memCls = memPct==null?'load-ok':memPct<70?'load-ok':memPct<90?'load-warn':'load-crit';
    const memCell = (mu!=null && mt!=null) ? `<span class="load-bar" style="width:60px"><span class="load-bar-fill ${memCls}" style="width:${memPct||0}%"></span></span> ${(mu/1024).toFixed(1)}/${(mt/1024).toFixed(1)} GiB` : '-';
    const temp = g.temp_c, tempColor = temp==null?'#8b949e':temp<70?'#7ee787':temp<85?'#9e6a03':'#da3633';
    const tempCell = temp==null ? '-' : `<span style="color:${tempColor}">${temp.toFixed(0)}°C</span>`;
    const power = g.power_w, powerCell = power==null||power===0 ? '-' : `${power.toFixed(0)}W`;
    const vendorIcon = g.vendor==='nvidia'?'🟢':g.vendor==='amd'?'🔴':'⚪';
    tr.appendChild(td(`<b>${esc(r.host)}</b>`));
    tr.appendChild(td(`${vendorIcon} <small class="dim">${esc(g.name||g.vendor||'?')}</small>`));
    tr.appendChild(td(utilCell));
    tr.appendChild(td(memCell));
    tr.appendChild(td(tempCell));
    tr.appendChild(td(powerCell));
  }
}

async function refreshHosts(){
  const d=await api('GET','/v1/hosts?include_stale=true').catch(()=>({hosts:[]}));
  const hosts=d.hosts||[];
  document.getElementById('hosts-cnt').textContent=`(${hosts.filter(h=>!h.stale).length} live · ${hosts.length} total)`;
  if(unchanged('hosts',hosts))return;
  const tb=document.querySelector('#hosts-tbl tbody');tb.innerHTML='';
  for(const h of hosts){
    const r=tb.insertRow();
    if(h.stale)r.className='host-row-stale';
    // host name + OS
    const osTag=h.os==='Darwin'?'🍎':h.arch==='aarch64'||h.arch==='arm64'?'🦾':'🐧';
    r.appendChild(td(`${osTag} <b>${esc(h.host)}</b>${h.stale?` <small class="dim">(stale ${h.age_s}s)</small>`:''}`));
    // cpu
    const cpuStr=h.cpu_model
      ?(h.cpu_model.replace(/\s+/g,' ').replace(/Intel\(R\)|AMD |Apple /gi,'').slice(0,28)+(h.cpu_threads?` ×${h.cpu_threads}`:''))
      :(h.cpu_threads?`${h.cpu_threads} threads`:'-');
    r.appendChild(td(`<small class="dim">${esc(cpuStr)}</small>`));
    // RAM
    r.appendChild(td(h.ram_gb!=null?h.ram_gb+'GB':'-'));
    // GPU
    const gpus=(h.gpus||[]);
    const gpuStr=gpus.length?gpus.map(g=>g.name||(g.vendor+' gpu')).join(', ').slice(0,30):'-';
    r.appendChild(td(`<small class="dim">${esc(gpuStr)}</small>`));
    // load avg
    const load=h.load_1min;
    const threads=h.cpu_threads||h.cpu_count||1;
    const loadPct=load!=null?Math.round(100*load/threads):null;
    r.appendChild(td(load!=null?`${load.toFixed(2)}${loadPct!=null?' ('+loadBar(loadPct)+')':''}`:'-'));
    // CPU%
    r.appendChild(td(loadBar(h.cpu_used_pct)));
    // RAM%
    r.appendChild(td(loadBar(h.ram_used_pct)));
    // Volumes (multi-mount support)
    const vols=h.volumes||[];
    if(vols.length){
      const cell=document.createElement('td');
      cell.style.fontSize='.78em';
      // Horizontal chip rendering — eliminates vertical list jitter.
      // Tightened color thresholds: red<5GB free, orange<10GB free, green≥10GB.
      const CHIP_CAP = 4;
      const shown = vols.slice(0, CHIP_CAP);
      const overflow = vols.length - shown.length;
      const chips = shown.map(v=>{
        const used = v.used_pct || 0;
        const free = v.free_gb || 0;
        const total = v.total_gb || 0;
        const barCls = used<70?'load-ok':used<90?'load-warn':'load-crit';
        // Smart color: small system partitions (/boot, /boot/efi, /efi) are 1-2GB
        // total — never trigger "low free GB" alerts on them. Use percentage instead.
        const isSysMount = /^\/boot(\/efi)?$|^\/efi$|^\/sys|^\/run/.test(v.mount||'');
        const dColor = isSysMount
          ? (used>90?'#da3633':used>75?'#9e6a03':'#7ee787')
          : (free<5?'#da3633':free<10?'#9e6a03':'#7ee787');
        const mount = (v.mount||'').length > 18 ? '…'+v.mount.slice(-17) : (v.mount||'');
        return `<span title="${esc(v.mount)} — ${used.toFixed(0)}% used, ${free.toFixed(1)} GB free of ${total.toFixed(0)} GB" style="display:inline-block;border:1px solid #30363d;border-radius:3px;padding:.1em .35em;margin:.05em .15em .05em 0;background:#0d1117;white-space:nowrap"><span style="color:#a5d6ff">${esc(mount)}</span> <span style="color:${dColor}">${free.toFixed(1)}/${total.toFixed(0)}GB</span></span>`;
      }).join('');
      const moreTag = overflow > 0 ? `<span title="${overflow} more mounts not shown" class="dim" style="font-size:.85em">+${overflow}</span>` : '';
      cell.innerHTML = chips + moreTag;
      cell.style.lineHeight = '1.8';
      cell.style.whiteSpace = 'normal';
      r.appendChild(cell);
    } else {
      // Fallback to legacy single-volume field if volumes[] empty
      var dfg=h.srv_free_gb; var dColor=dfg==null?'#8b949e':dfg<10?'#da3633':dfg<25?'#9e6a03':'#7ee787';
      r.appendChild(td(dfg!=null?'<span style="color:'+dColor+'">'+dfg.toFixed(1)+'GB free</span>':'-'));
    }
    // caps (brief)
    const caps=(h.capabilities||[]).filter(c=>!['system','host'].includes(c)).slice(0,6);
    r.appendChild(td(caps.map(c=>`<span class="dim" style="font-size:.72em;border:1px solid #30363d;border-radius:3px;padding:.05em .25em">${esc(c)}</span>`).join(' ')));
    // heartbeat age
    const ageClass=h.age_s>60?'s-failed':h.age_s>30?'s-claimed':'s-online';
    r.appendChild(td(`<span class="${ageClass}">${h.age_s}s</span>`));
  }
}

let _capState=null;
async function refreshCapBanner(){
  try{const d=await api('GET','/v1/knemon/cap');_capState=d.capped?d:null;renderCapBanner();}catch(_){}
}
function renderCapBanner(){
  const el=document.getElementById('cap-banner');if(!el)return;
  const rem=Math.max(0,Math.round(((_capState&&_capState.capped_until)||0)-Date.now()/1000));
  if(rem<=0){el.style.display='none';return;}
  const h=Math.floor(rem/3600),m=Math.floor((rem%3600)/60),sc=rem%60;
  // Build the static text ONCE per state change; the 1s tick only updates the
  // countdown node (was: full innerHTML rebuild every second).
  const key=(_capState&&_capState.capped_until)+'|'+(_capState&&_capState.model)+'|'+(_capState&&_capState.reporter)+'|'+(_capState&&_capState.hit_at);
  if(el.dataset.capkey!==key){
    const hit=_capState.hit_at?new Date(_capState.hit_at*1000).toLocaleTimeString():'unknown time';
    const model=_capState.model?esc(_capState.model):'unknown model';
    const reporter=_capState.reporter?(' by '+esc(_capState.reporter)):'';
    el.innerHTML='\u23f3 <b>OpenAI OAuth cap reported, unverified</b> \u2014 '+model+reporter+' reported usage_limit at <b>'+esc(hit)+'</b>. Most jobs route fallback; ~5% are OAuth probes. Auto-clear in <b id="cap-countdown"></b> unless a success report clears it first.';
    el.dataset.capkey=key;
  }
  const cd=document.getElementById('cap-countdown');
  if(cd) cd.textContent=h+'h '+String(m).padStart(2,'0')+'m '+String(sc).padStart(2,'0')+'s';
  el.style.display='block';
}
setInterval(renderCapBanner,1000);

// ── refresh scheduling ────────────────────────────────────────────────
// ONE in-flight refreshAll at a time; SSE events and the poll both funnel
// through scheduleRefresh() which coalesces bursts into a single run.
let _refreshing=false, _refreshQueued=false, _refreshTimer=null;
function scheduleRefresh(delay=350){
  if(_refreshTimer)clearTimeout(_refreshTimer);
  _refreshTimer=setTimeout(()=>{_refreshTimer=null;refreshAll();},delay);
}
async function refreshAll(){
  if(_refreshing){_refreshQueued=true;return;}
  _refreshing=true;
  document.getElementById('meta-time').textContent=`last refresh ${new Date().toLocaleTimeString()} · auto 8s`;
  try{
    await Promise.all([
      refreshRunning(),refreshAgents(),refreshJobs(),refreshResults(),refreshCapBanner(),
      refreshFailed(),refreshCosts(),refreshWorkers(),refreshSchedules(),
      refreshHosts(),refreshGPU(),
    ]);
  }catch(e){document.getElementById('meta-time').textContent='error: '+e;}
  finally{
    _refreshing=false;
    if(_refreshQueued){_refreshQueued=false;scheduleRefresh(150);}
  }
}

// ── SSE live events ───────────────────────────────────────────────────
const evDiv=document.getElementById('events');
function evLine(s){
  const d=document.createElement('div');d.className='ev';d.innerHTML=s;
  evDiv.insertBefore(d,evDiv.firstChild);
  while(evDiv.children.length>40)evDiv.removeChild(evDiv.lastChild);
}
function initSSE(){
  try{
    const es=new EventSource(BUS+'/v1/events');
    es.onmessage=ev=>{
      try{
        const d=JSON.parse(ev.data);
        const p=d.payload||{};
        const short=(p.id||'').slice(0,8);
        let detail='';
        if(p.claimed_by)detail+=` by ${p.claimed_by.split(':').slice(2,4).join('@')}`;
        if(p.cost_saved_usd)detail+=` (cache saved $${p.cost_saved_usd.toFixed(4)})`;
        evLine(`<span class="ek">${esc(d.kind)}</span> ${short?esc(short)+'…':''} ${esc(detail)}`);
        // Coalesce: ANY job-lifecycle event funnels into ONE debounced refresh
        // (was: up to 3 separate refreshX() fetches per event -> storm).
        if(String(d.kind||'').startsWith('job.') || d.kind==='message') scheduleRefresh();
      }catch(e){}
    };
    es.onerror=()=>evLine('<span style="color:#f85149">SSE disconnected — retrying</span>');
  }catch(e){}
}

// ── init ──────────────────────────────────────────────────────────────
async function init(){
  try{await establishSession();}catch(e){
    document.getElementById('meta-time').textContent='authentication required: '+e.message;
    return;
  }
  await registerAgent();
  await refreshAll();
  // SSE drives live refreshes; this is a slow safety net for a dropped stream.
  setInterval(refreshAll, 15000);
  initSSE();
}
init();
