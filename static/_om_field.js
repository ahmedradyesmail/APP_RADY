// ── التشيك (قديم الفرز): ملفان + Match + GPS
let omCheckLargeFile=null,omCheckSmallFile=null,omCheckResultBlob=null;
let omCheckDetected={large:null,small:null},omCheckLargeHasGps=false,omGpsResultBlob=null;
let omPostgresLargeEnabled=false,omUseStoredLarge=false;
let omImportLargeBusy=false;
let omLargeNeedsPassword=false;
let omTempCheckSessionToken='';
let omTempCheckPingTimer=null;
let omTempLargeFingerprint='';
let omTempLargeReady=false;
let omHasStoredImports=false;

function omGetSmallPlatesTextLines(){
  var ta=document.getElementById('omSmallPlatesText');
  if(!ta) return [];
  return String(ta.value||'').split(/\r?\n/).map(function(x){ return x.trim().replace(/\s+/g,' '); }).filter(Boolean);
}
function omUsingSmallText(){
  return omGetSmallPlatesTextLines().length>0;
}
function omIsPasswordRelatedError(msg){
  var t=String(msg||'');
  return t.indexOf('فك تشفير')!==-1 || t.indexOf('كلمة المرور')!==-1 || t.toLowerCase().indexOf('password')!==-1;
}
function omLargeFingerprint(file){
  if(!file) return '';
  return [String(file.name||''),String(file.size||0),String(file.lastModified||0)].join('|');
}
async function omEnsureTempCheckSession(){
  if(!omPostgresLargeEnabled) return '';
  if(omTempCheckSessionToken) return omTempCheckSessionToken;
  var r=await fetch('/api/check/temp/session/start',{method:'POST'});
  var j=await r.json().catch(function(){return{};});
  if(!r.ok) throw new Error(j.detail||r.statusText||'فشل بدء جلسة التشيك');
  omTempCheckSessionToken=String(j.session_token||'');
  try{ localStorage.setItem('omTempCheckSessionToken',omTempCheckSessionToken); }catch(_){}
  if(omTempCheckPingTimer) clearInterval(omTempCheckPingTimer);
  omTempCheckPingTimer=setInterval(async function(){
    if(!omTempCheckSessionToken) return;
    const fd=new FormData();
    fd.append('session_token',omTempCheckSessionToken);
    try{ await fetch('/api/check/temp/session/ping',{method:'POST',body:fd}); }catch(_){}
  },60000);
  return omTempCheckSessionToken;
}
function omOnSmallTextInput(){
  var lines=omGetSmallPlatesTextLines();
  if(lines.length){
    omSetBadge('small','found','✔ نصي ('+String(lines.length)+' لوحة)');
    omRebuildCheckExportChoices('small',['رقم اللوحة'],true);
    if(!document.getElementById('omSmallCol').value.trim()) document.getElementById('omSmallCol').value='رقم اللوحة';
  }else if(!omCheckSmallFile){
    omSetBadge('small','pending','—');
  }
  omCheckRunReady();
}

async function omRenderStoredImportsList(){
  var el=document.getElementById('omStoredImportsList');
  if(!el||!omPostgresLargeEnabled) return;
  try{
    var r=await fetch('/api/check/stored-imports');
    var j=await r.json().catch(function(){return{};});
    if(!r.ok){
      el.innerHTML='<span style="color:var(--dim2)">\u26A0 \u0644\u0627 \u064A\u0645\u0643\u0646 \u062A\u062D\u0645\u064A\u0644 \u0642\u0627\u0626\u0645\u0629 \u0627\u0644\u0627\u0633\u062A\u064A\u0631\u0627\u062F\u0627\u062A</span>';
      return;
    }
    var arr=j.imports||[];
    if(!arr.length){
      omHasStoredImports=false;
      el.innerHTML='<span style="color:var(--dim2)">\u2014 \u0644\u0627 \u062A\u0648\u062C\u062F \u0627\u0633\u062A\u064A\u0631\u0627\u062F\u0627\u062A \u0645\u062E\u0632\u0651\u0646\u0629 \u0628\u0639\u062F</span>';
      return;
    }
    omHasStoredImports=true;
    if(!omCheckLargeFile){
      var cb=document.getElementById('omUseStoredLargeCb');
      if(cb && !cb.checked){
        cb.checked=true;
        omOnToggleStoredLarge();
      }
    }
    var totalRows=arr.reduce(function(sum,it){ return sum+Number(it.row_count||0); },0);
    el.innerHTML='<div style="display:flex;flex-wrap:wrap;gap:.4rem;margin-bottom:.45rem">'+
      '<span style="padding:.2rem .5rem;border:1px solid var(--brd2);border-radius:999px;background:var(--s2);font-family:var(--mono)">\uD83D\uDCC1 \u0627\u0644\u0645\u0644\u0641\u0627\u062A: '+String(arr.length)+'</span>'+
      '<span style="padding:.2rem .5rem;border:1px solid var(--brd2);border-radius:999px;background:var(--s2);font-family:var(--mono)">\uD83E\uDDEE \u0627\u0644\u0635\u0641\u0648\u0641: '+String(totalRows)+'</span>'+
      '</div>'+
      '<div style="font-weight:600;margin-bottom:.35rem;color:var(--dim)">\u0627\u0644\u0645\u0644\u0641\u0627\u062A \u0627\u0644\u0645\u062E\u0632\u0651\u0646\u0629</div>'+arr.map(function(it){
      var nm=(it.filename||'').replace(/</g,'&lt;');
      return '<div style="display:flex;flex-wrap:wrap;align-items:center;gap:.45rem;margin:.3rem 0;padding:.35rem .5rem;background:var(--s2);border:1px solid var(--brd2);border-radius:8px">'+
        '<span style="flex:1;min-width:8rem;font-family:var(--mono);font-size:.72rem">'+nm+' <span style="color:var(--dim2)">('+String(it.row_count||0)+' \u0635\u0641)</span></span>'+
        '<button type="button" class="tb-btn outline" style="padding:.2rem .55rem;font-size:.72rem" onclick="omDeleteStoredImport('+Number(it.id)+')">\u062D\u0630\u0641</button></div>';
    }).join('');
  }catch(_e){
    omHasStoredImports=false;
    el.innerHTML='';
  }
}
async function omDeleteStoredImport(id){
  if(!id||!confirm('\u062D\u0630\u0641 \u0647\u0630\u0627 \u0627\u0644\u0627\u0633\u062A\u064A\u0631\u0627\u062F \u0645\u0646 \u0627\u0644\u062E\u0627\u062F\u0645\u061F')) return;
  try{
    var r=await fetch('/api/check/stored-imports/'+encodeURIComponent(String(id)),{method:'DELETE'});
    var j=await r.json().catch(function(){return{};});
    if(!r.ok) throw new Error(j.detail||r.statusText||'\u0641\u0634\u0644');
    omShowFieldStatus('ok','\u2705 \u062A\u0645 \u0627\u0644\u062D\u0630\u0641');
    await omRenderStoredImportsList();
    if(omUseStoredLarge) await omDetectColForSide('large');
  }catch(e){ omShowFieldStatus('err','\u062E\u0637\u0623: '+(e.message||'')); }
}

async function omFetchCheckCapabilities(){
  try{
    const r=await fetch('/api/check/capabilities');
    if(!r.ok){ omPostgresLargeEnabled=false; return; }
    const j=await r.json();
    omPostgresLargeEnabled=!!j.postgres_large_enabled;
    const panel=document.getElementById('omPgStoragePanel');
    if(panel) panel.style.display=omPostgresLargeEnabled?'':'none';
    if(!omPostgresLargeEnabled){ omUseStoredLarge=false; var cb=document.getElementById('omUseStoredLargeCb'); if(cb) cb.checked=false; }
    else{ omRenderStoredImportsList(); }
  }catch(_e){ omPostgresLargeEnabled=false; }
}

function omOnToggleStoredLarge(){
  var c=document.getElementById('omUseStoredLargeCb');
  omUseStoredLarge=!!(c&&c.checked);
  if(omUseStoredLarge){
    omClearPersistedCheckFile('large');
    omCheckLargeFile=null;
    var fn=document.getElementById('omLargeFname'); if(fn) fn.textContent='';
    var rb=document.getElementById('omRemoveLargeBtn'); if(rb) rb.classList.remove('show');
    var fi=document.getElementById('omLargeFileIn'); if(fi) fi.value='';
    var lp=document.getElementById('omLargePw'); if(lp) lp.value='';
    omResetColDropdown('large');
    omSetBadge('large','pending','\u2014');
    omClearCheckExportList('large');
  }
  omResetCheckDetect();
  omDetectColForSide('large');
  if(omUseStoredLarge) omRenderStoredImportsList();
  omCheckRunReady();
}

async function omImportLargeToServer(){
  if(!omCheckLargeFile){ omShowFieldStatus('err','\u0627\u0631\u0641\u0639 \u0627\u0644\u0645\u0644\u0641 \u0627\u0644\u0643\u0628\u064A\u0631 \u062B\u0645 \u0627\u0636\u063A\u0637 \u0627\u0633\u062A\u064A\u0631\u0627\u062F \u0625\u0644\u0649 \u0627\u0644\u062E\u0627\u062F\u0645'); return; }
  if(omImportLargeBusy) return;
  omImportLargeBusy=true;
  var importBtn=document.getElementById('omImportLargeBtn');
  if(importBtn) importBtn.disabled=true;
  var hi=document.getElementById('omPgImportHint');
  if(hi) hi.textContent='';
  omShowFieldStatus('proc','\u062C\u0627\u0631\u064A \u0627\u0633\u062A\u064A\u0631\u0627\u062F \u0627\u0644\u0645\u0644\u0641 \u0627\u0644\u0643\u0628\u064A\u0631 \u2026',true);
  try{
    var fd=new FormData();
    fd.append('large_file',omCheckLargeFile);
    var pw=document.getElementById('omLargePw').value.trim(); if(pw) fd.append('password',pw);
    var lc=document.getElementById('omLargeCol').value.trim(); if(lc) fd.append('large_col',lc);
    var r=await fetch('/api/check/import-large',{method:'POST',body:fd});
    var j=await r.json().catch(function(){return{};});
    if(!r.ok) throw new Error(j.detail||r.statusText||'\u0641\u0634\u0644 \u0627\u0644\u0627\u0633\u062A\u064A\u0631\u0627\u062F');
    if(hi) hi.textContent='\u2714 \u0627\u0633\u062A\u064A\u0631\u0627\u062F #'+String(j.import_id||'')+': '+String(j.row_count||0)+' \u0635\u0641\u060C \u0639\u0645\u0648\u062F '+String(j.large_col_used||'')+' \u2014 '+String(j.sheet_name||'');
    omShowFieldStatus('ok','\u2705 \u062A\u0645 \u062D\u0641\u0638 \u0627\u0644\u0645\u0644\u0641 \u0627\u0644\u0643\u0628\u064A\u0631 \u0639\u0644\u0649 \u0627\u0644\u062E\u0627\u062F\u0645');
    var cb=document.getElementById('omUseStoredLargeCb');
    if(cb && !cb.checked){
      cb.checked=true;
      omOnToggleStoredLarge();
    }
    await omRenderStoredImportsList();
    if(omUseStoredLarge) await omDetectColForSide('large');
  }catch(e){ omShowFieldStatus('err','\u062E\u0637\u0623: '+(e.message||'')); }
  finally{
    omImportLargeBusy=false;
    if(importBtn) importBtn.disabled=false;
  }
}
async function omUploadLargeToTempIfNeeded(){
  if(!omPostgresLargeEnabled) throw new Error('تخزين التشيك غير متاح');
  if(!omCheckLargeFile) throw new Error('ارفع الملف الكبير أولاً');
  const fp=omLargeFingerprint(omCheckLargeFile);
  if(omTempLargeReady && fp && fp===omTempLargeFingerprint) return;
  const token=await omEnsureTempCheckSession();
  const fd=new FormData();
  fd.append('session_token',token);
  fd.append('large_file',omCheckLargeFile);
  const pw=document.getElementById('omLargePw').value.trim(); if(pw) fd.append('password',pw);
  const lc=document.getElementById('omLargeCol').value.trim(); if(lc) fd.append('large_col',lc);
  const r=await fetch('/api/check/temp/upload-large',{method:'POST',body:fd});
  const j=await r.json().catch(function(){return{};});
  if(!r.ok) throw new Error(j.detail||r.statusText||'فشل رفع الملف الكبير المؤقت');
  omTempLargeReady=true;
  omTempLargeFingerprint=fp;
  omShowFieldStatus('ok','✅ تم تجهيز '+String(j.stored_count||0)+' لوحة للتشيك السريع');
}
async function omRunTempTextCheck(){
  const txt=(document.getElementById('omSmallPlatesText').value||'').trim();
  if(!txt) throw new Error('أدخل اللوحات النصية أولاً');
  await omUploadLargeToTempIfNeeded();
  const token=await omEnsureTempCheckSession();
  const fd=new FormData();
  fd.append('session_token',token);
  fd.append('plates_text',txt);
  const r=await fetch('/api/check/temp/query',{method:'POST',body:fd});
  const j=await r.json().catch(function(){return{};});
  if(!r.ok) throw new Error(j.detail||r.statusText||'فشل فحص اللوحات');
  const rows=(j.rows||[]).map(function(it){ return [String(it.plate||''), it.exists?'موجودة':'غير موجودة']; });
  const found=Number(j.found||0), total=Number(j.total||rows.length), miss=Number(j.not_found||Math.max(0,total-found));
  document.getElementById('omRMatched').textContent=String(found);
  document.getElementById('omRPlates').textContent=String(total);
  document.getElementById('omRUnmatched').textContent=String(miss);
  document.getElementById('omRLargeCol').textContent='تشيك مؤقت (لوحات فقط)';
  document.getElementById('omRSmallCol').textContent=document.getElementById('omSmallCol').value.trim()||'رقم اللوحة';
  document.getElementById('omDlBtn').style.display='none';
  document.getElementById('omResultBox').classList.add('show');
  omRenderCheckMatchPreview({headers:['اللوحة','الحالة'],col_sources:['small','small'],rows:rows,total_rows:rows.length,truncated:false});
  omShowFieldStatus('ok','✅ تم فحص '+String(total)+' لوحة (موجود: '+String(found)+')');
}

document.addEventListener('DOMContentLoaded',async function(){
  omFetchCheckCapabilities();
  try{
    var tok=localStorage.getItem('omTempCheckSessionToken')||'';
    if(tok) omTempCheckSessionToken=tok;
  }catch(_){}
  try{ await omEnsureTempCheckSession(); }catch(_){}
});
window.addEventListener('om-auth-state-changed',function(e){
  var authed=!!(e && e.detail && e.detail.authenticated);
  if(!authed){
    omPostgresLargeEnabled=false;
    omUseStoredLarge=false;
    omHasStoredImports=false;
    return;
  }
  omFetchCheckCapabilities();
});
function omColId(side){ return side==='large'?'omLargeCol':'omSmallCol'; }
function omColBadgeId(side){ return side==='large'?'omLargeColBadge':'omSmallColBadge'; }
function omShowFieldStatus(type,txt,spin=false){
  const bar=document.getElementById('omFieldStatus');
  if(!bar) return;
  bar.className='status '+(type==='proc'?'proc':type==='ok'?'ok':type==='warn'?'warn':'err');
  const sp=document.getElementById('omFieldSpin');
  if(sp) sp.style.display=spin?'block':'none';
  const tx=document.getElementById('omFieldStatusTxt');
  if(tx) tx.textContent=txt||'';
  if(type!=='proc') setTimeout(function(){ bar.className='status'; },9000);
}
async function omPollCheckStatus(jobId){
  const deadline=Date.now()+15*60*1000;
  while(Date.now()<deadline){
    const r=await fetch('/api/check/status/'+encodeURIComponent(jobId));
    const payload=await r.json().catch(()=>({}));
    if(!r.ok) throw new Error(payload.detail||r.statusText);
    if(payload.status==='done'&&payload.data) return payload.data;
    if(payload.status==='error') throw new Error(payload.detail||'خطأ في المطابقة');
    await new Promise(res=>setTimeout(res,1200));
  }
  throw new Error('انتهت مهلة انتظار المطابقة');
}
async function omPersistCheckFile(side, file){
  if(side!=='large') return;
  try{
    const b64=await fileToB64(file);
    const meta=JSON.stringify({name:file.name,type:file.type,size:file.size});
    localStorage.setItem('omCheckFile_large_b64',b64);
    localStorage.setItem('omCheckFile_large_meta',meta);
  }catch(e){}
}
function omClearPersistedCheckFile(side){
  localStorage.removeItem('omCheckFile_'+side+'_b64');
  localStorage.removeItem('omCheckFile_'+side+'_meta');
}
async function loadOmPersistedCheckFiles(){
  try{
    const b64=localStorage.getItem('omCheckFile_large_b64');
    const meta=localStorage.getItem('omCheckFile_large_meta');
    if(!b64||!meta) return;
    const m=JSON.parse(meta);
    const file=b64ToFile(b64,m.name,m.type);
    omCheckLargeFile=file;
    const el=document.getElementById('omLargeFname');
    if(el) el.textContent='📎 '+m.name;
    const rb=document.getElementById('omRemoveLargeBtn');
    if(rb) rb.classList.add('show');
    await omDetectColForSide('large');
    omCheckRunReady();
  }catch(e){}
}
async function omOnCheckFileChange(file,side){
  if(!file)return;
  if(side==='large'){
    omLargeNeedsPassword=false;
    omTempLargeReady=false;
    omTempLargeFingerprint='';
    omCheckLargeFile=file;
    document.getElementById('omLargeFname').textContent='📎 '+file.name;
    document.getElementById('omRemoveLargeBtn').classList.add('show');
    if(!document.getElementById('omLargeCol').value.trim())omSetBadge('large','pending','...');
    omCheckResultBlob=null;
    document.getElementById('omResultBox').classList.remove('show');
    if(typeof omRenderCheckMatchPreview==='function') omRenderCheckMatchPreview(null);
    document.getElementById('omFieldHeadersHint').style.display='none';
    await omDetectColForSide('large');
    omPersistCheckFile('large',file);
    if(omPostgresLargeEnabled){
      var pw0=document.getElementById('omLargePw').value.trim();
      if(omLargeNeedsPassword && !pw0){
        omShowFieldStatus('warn','الملف الكبير مشفّر. أدخل كلمة المرور ثم اضغط «تأكيد» ليتم الاستيراد.');
      }else{
        omImportLargeToServer();
      }
    }
  } else {
    omCheckSmallFile=file;
    var ta=document.getElementById('omSmallPlatesText'); if(ta) ta.value='';
    document.getElementById('omSmallFname').textContent='📎 '+file.name;
    document.getElementById('omRemoveSmallBtn').classList.add('show');
    if(!document.getElementById('omSmallCol').value.trim())omSetBadge('small','pending','...');
    omCheckResultBlob=null;
    document.getElementById('omResultBox').classList.remove('show');
    if(typeof omRenderCheckMatchPreview==='function') omRenderCheckMatchPreview(null);
    omDetectColForSide('small');
  }
  omCheckRunReady();
}
function omHandleDropCheck(e,side){e.preventDefault();document.getElementById(side==='large'?'omDropLarge':'omDropSmall').classList.remove('drag');const f=e.dataTransfer.files[0];if(f)omOnCheckFileChange(f,side);}
function omClearCheckExportList(side){
  const id=side==='large'?'omLargeExportList':'omSmallExportList';
  const el=document.getElementById(id);
  if(el) el.innerHTML='';
}
/** مراجع عربية لأعمدة المركبة الأربعة + صيغ قريبة (لون أساسي، نوع تسجيل لوحة، طراز، صانع). */
var OM_VEHICLE_EXPORT_SEEDS=[
  '\u0644\u0648\u0646 \u0627\u0644\u0645\u0631\u0643\u0628\u0629 \u0627\u0644\u0623\u0633\u0627\u0633\u064A','\u0644\u0648\u0646 \u0627\u0644\u0645\u0631\u0643\u0628\u0647 \u0627\u0644\u0627\u0633\u0627\u0633\u064A','\u0644\u0648\u0646 \u0627\u0644\u0645\u0631\u0643\u0628\u0629 \u0627\u0644\u0627\u0633\u0627\u0633\u064A','\u0644\u0648\u0646 \u0627\u0644\u0645\u0631\u0643\u0628\u0647 \u0627\u0644\u0623\u0633\u0627\u0633\u064A',
  '\u0646\u0648\u0639 \u062A\u0633\u062C\u064A\u0644 \u0627\u0644\u0644\u0648\u062D\u0629','\u0646\u0648\u0639 \u062A\u0633\u062C\u064A\u0644 \u0627\u0644\u0644\u0648\u062D\u0647',
  '\u0637\u0631\u0627\u0632 \u0627\u0644\u0645\u0631\u0643\u0628\u0629','\u0637\u0631\u0627\u0632 \u0627\u0644\u0645\u0631\u0643\u0628\u0647',
  '\u0635\u0627\u0646\u0639 \u0627\u0644\u0645\u0631\u0643\u0628\u0629','\u0635\u0627\u0646\u0639 \u0627\u0644\u0645\u0631\u0643\u0628\u0647',
  '\u0644\u0648\u0646 \u0627\u0644\u0645\u0631\u0643\u0628\u0629','\u0644\u0648\u0646 \u0627\u0644\u0645\u0631\u0643\u0628\u0647'
];
var OM_COLOR_TOKENS=['ابيض','أبيض','ازرق','أزرق','احمر','أحمر','اسود','أسود','فضي','رمادي','رصاصي','ذهبي','اخضر','أخضر','بني'];
var OM_MAKER_TOKENS=['تويوتا','مرسيدس','هيونداي','كيا','نيسان','هوندا','فورد','شيفروليه','جيب','بي ام دبليو','bmw','audi','لكزس'];
var OM_MODEL_TOKENS=['اكسنت','كريتا','كامري','يارس','النترا','سوناتا','كورولا','اوبتيما','سيراتو','cx5','tucson','rio'];
var OM_REGTYPE_TOKENS=['خاص','نقل','اجرة','أجرة','دبلوماسي','تجاري'];
function omNormHeaderForSim(s){
  return String(s||'').normalize('NFKC')
    .replace(/[\u0640\u061c\u200c-\u200f\ufeff]/g,'')
    .replace(/[\u0623\u0625\u0622]/g,'\u0627')
    .replace(/\u0629/g,'\u0647')
    .replace(/\u0649/g,'\u064a')
    .replace(/\s+/g,'')
    .toLowerCase();
}
function omLevRatio(a,b){
  if(a===b) return 1;
  const m=a.length,n=b.length;
  if(!m||!n) return 0;
  let prev=new Uint16Array(n+1);
  for(let j=0;j<=n;j++) prev[j]=j;
  for(let i=1;i<=m;i++){
    const cur=new Uint16Array(n+1);
    cur[0]=i;
    const ca=a.charCodeAt(i-1);
    for(let j=1;j<=n;j++){
      const cost=ca===b.charCodeAt(j-1)?0:1;
      cur[j]=Math.min(prev[j]+1,cur[j-1]+1,prev[j-1]+cost);
    }
    prev=cur;
  }
  const d=prev[n];
  return 1-d/Math.max(m,n);
}
function omVehicleExportTokenMatch(nh){
  if(!nh) return false;
  if(nh.indexOf('\u0644\u0648\u0646')!==-1&&nh.indexOf('\u0645\u0631\u0643\u0628\u0647')!==-1&&(nh.indexOf('\u0627\u0633\u0627\u0633')!==-1||nh.indexOf('\u0623\u0633\u0627\u0633')!==-1)) return true;
  if(nh.indexOf('\u0646\u0648\u0639')!==-1&&nh.indexOf('\u062A\u0633\u062C\u064A\u0644')!==-1&&nh.indexOf('\u0644\u0648\u062D')!==-1) return true;
  if(nh.indexOf('\u0637\u0631\u0627\u0632')!==-1&&nh.indexOf('\u0645\u0631\u0643\u0628\u0647')!==-1) return true;
  if(nh.indexOf('\u0635\u0627\u0646\u0639')!==-1&&nh.indexOf('\u0645\u0631\u0643\u0628\u0647')!==-1) return true;
  return false;
}
function omVehicleExportHeaderAutoPick(header){
  const h=String(header||'');
  if(!h) return false;
  const nh=omNormHeaderForSim(h);
  if(omVehicleExportTokenMatch(nh)) return true;
  const SIM=0.8;
  for(let i=0;i<OM_VEHICLE_EXPORT_SEEDS.length;i++){
    const ns=omNormHeaderForSim(OM_VEHICLE_EXPORT_SEEDS[i]);
    if(!ns) continue;
    if(nh===ns) return true;
    if(omLevRatio(nh,ns)>=SIM) return true;
  }
  return false;
}
function omHasAnyToken(text,tokens){
  const t=omNormHeaderForSim(text);
  if(!t) return false;
  for(let i=0;i<tokens.length;i++){
    if(t.indexOf(omNormHeaderForSim(tokens[i]))!==-1) return true;
  }
  return false;
}
function omVehicleExportContentAutoPick(header,samplesMap){
  const vals=(samplesMap&&samplesMap[header])||[];
  if(!vals.length) return false;
  let hits=0;
  const total=Math.min(vals.length,25);
  for(let i=0;i<total;i++){
    const v=String(vals[i]||'');
    if(
      omHasAnyToken(v,OM_COLOR_TOKENS) ||
      omHasAnyToken(v,OM_MAKER_TOKENS) ||
      omHasAnyToken(v,OM_MODEL_TOKENS) ||
      omHasAnyToken(v,OM_REGTYPE_TOKENS)
    ){
      hits++;
    }
  }
  return hits>=2 && (hits/Math.max(1,total))>=0.2;
}
function omRebuildCheckExportChoices(side,headers,defaultChecked,samplesMap){
  const id=side==='large'?'omLargeExportList':'omSmallExportList';
  const el=document.getElementById(id);
  if(!el) return;
  el.innerHTML='';
  const allOn=defaultChecked!==false;
  (headers||[]).forEach(h=>{
    if(!h) return;
    const lab=document.createElement('label');
    lab.style.cssText=side==='small'
      ?'display:flex;align-items:center;gap:.45rem;font-size:.78rem;cursor:pointer;padding:.2rem 0 .2rem .4rem;border-left:3px solid rgba(34,197,94,.85);margin-left:.1rem'
      :'display:flex;align-items:center;gap:.45rem;font-size:.78rem;cursor:pointer;padding:.2rem 0';
    const cb=document.createElement('input');
    cb.type='checkbox';
    cb.value=h;
    cb.checked=allOn
      ? (omVehicleExportHeaderAutoPick(h) || (side==='small' && omVehicleExportContentAutoPick(h,samplesMap)))
      : false;
    cb.className='check-export-cb check-export-'+side;
    const sp=document.createElement('span');
    sp.textContent=h;
    sp.style.fontFamily='var(--mono)';
    lab.appendChild(cb);
    lab.appendChild(sp);
    el.appendChild(lab);
  });
}
function omGetCheckExportColsJson(side){
  const boxes=document.querySelectorAll('.check-export-cb.check-export-'+side+':checked');
  if(!boxes.length) return '[]';
  return JSON.stringify([...boxes].map(b=>b.value));
}
function omRemoveCheckFile(side){
  if(side==='large'){omLargeNeedsPassword=false;omTempLargeReady=false;omTempLargeFingerprint='';omCheckLargeFile=null;document.getElementById('omLargeFname').textContent='';document.getElementById('omRemoveLargeBtn').classList.remove('show');document.getElementById('omLargeFileIn').value='';const lp=document.getElementById('omLargePw');if(lp)lp.value='';omResetColDropdown('large');omSetBadge('large','pending','\u2014');omClearPersistedCheckFile('large');omClearCheckExportList('large');}
  else{omCheckSmallFile=null;document.getElementById('omSmallFname').textContent='';document.getElementById('omRemoveSmallBtn').classList.remove('show');document.getElementById('omSmallFileIn').value='';omResetColDropdown('small');omSetBadge('small','pending','\u2014');omClearPersistedCheckFile('small');omClearCheckExportList('small');}
  omResetCheckDetect();omCheckRunReady();
}
function omResetColDropdown(side){
  const sel=document.getElementById(omColId(side));
  if(!sel)return;
  sel.innerHTML='<option value="">اختر عموداً…</option>';
}
function omFillColDropdown(side,headers,selectedVal){
  const sel=document.getElementById(omColId(side));
  if(!sel)return;
  const prev=(selectedVal||sel.value||'').trim();
  sel.innerHTML='<option value="">اختر عموداً…</option>';
  (headers||[]).forEach(h=>{
    if(!h)return;
    const opt=document.createElement('option');
    opt.value=h;
    opt.textContent=h;
    sel.appendChild(opt);
  });
  if(prev && (headers||[]).includes(prev)) sel.value=prev;
}
function omResetCheckDetect(){
  omCheckLargeHasGps=false;omGpsResultBlob=null;
  document.getElementById('omGpsMatchSection').style.display='none';
  document.getElementById('omGpsResultBox').classList.remove('show');
  omCheckResultBlob=null;
  if(typeof omRenderCheckMatchPreview==='function') omRenderCheckMatchPreview(null);
  document.getElementById('omResultBox').classList.remove('show');
  document.getElementById('omFieldHeadersHint').style.display='none';
  ['large','small'].forEach(s=>{if(!document.getElementById(omColId(s)).value.trim())omSetBadge(s,'pending','\u2014');});
}
function omOnManualColInput(side){const val=document.getElementById(omColId(side)).value.trim();omSetBadge(side,val?'found':'pending',val?'\u25BE \u0645\u062E\u062A\u0627\u0631':'\u2014');if(!val)omAutoDetectCols();omCheckRunReady();}
function omSetBadge(side,state,text){const b=document.getElementById(omColBadgeId(side));b.className='detect-badge '+state;b.textContent=text;}
async function omAutoDetectCols(){
  if(omCheckLargeFile || (omPostgresLargeEnabled && omUseStoredLarge)) omDetectColForSide('large');
  if(omCheckSmallFile || omUsingSmallText()) omDetectColForSide('small');
}

async function omDetectColForSide(side){
  var usingSmallText=omUsingSmallText();
  if(side==='large' && omPostgresLargeEnabled && omUseStoredLarge){
    omSetBadge('large','pending','...');
    try{
      const res=await fetch('/api/check/stored-large-meta');
      const j=await res.json().catch(function(){return{};});
      if(!res.ok){ omSetBadge('large','notfound','\u26A0 '+(j.detail||res.statusText)); return; }
      if(!j.has_data||!j.headers||!j.headers.length){
        omSetBadge('large','notfound','\u0644\u0627 \u0628\u064A\u0627\u0646\u0627\u062A');
        omRebuildCheckExportChoices('large',[],true);
        return;
      }
      omFillColDropdown('large',j.headers,document.getElementById('omLargeCol').value.trim());
      document.getElementById('omLargeCol').value='';
      omSetBadge('large','found','\u2714 \u0645\u062E\u0632\u0651\u0646 ('+String(j.row_count||0)+' \u0635\u0641)');
      omRebuildCheckExportChoices('large',j.headers||[],true);
      omCheckLargeHasGps=!!(j.headers&&j.headers.indexOf('GPS')!==-1);
      const gs=document.getElementById('omGpsMatchSection');
      if(omCheckLargeHasGps){ gs.style.display=''; if(!document.getElementById('omGpsMyLat').value.trim())omRefreshCheckLoc(); }
      else{ gs.style.display='none'; }
    }catch(e){ omSetBadge('large','notfound','\u26A0'); }
    omCheckRunReady();
    return;
  }
  if(side==='small' && omUsingSmallText()){
    var lines=omGetSmallPlatesTextLines();
    omFillColDropdown('small',['رقم اللوحة'],document.getElementById('omSmallCol').value.trim());
    if(!document.getElementById('omSmallCol').value.trim()) document.getElementById('omSmallCol').value='رقم اللوحة';
    omSetBadge('small','found','✔ نصي ('+String(lines.length)+' لوحة)');
    omRebuildCheckExportChoices('small',['رقم اللوحة'],true);
    omCheckRunReady();
    return;
  }
  const fd=new FormData();
  if(side==='large'){
    if(!omCheckLargeFile){ omSetBadge('large','pending','\u2014'); return; }
    fd.append('large_file',omCheckLargeFile);
    const pw=document.getElementById('omLargePw').value.trim();
    if(pw)fd.append('password',pw);
  } else {
    if(usingSmallText){
      fd.append('small_plates_text',document.getElementById('omSmallPlatesText').value||'');
    }else{
      fd.append('small_file',omCheckSmallFile);
    }
  }
  omSetBadge(side,'pending','...');
  try{
    const res=await fetch('/api/check-headers',{method:'POST',body:fd});
    if(!res.ok){omSetBadge(side,'notfound','⚠ خطأ');return;}
    const data=await res.json();
    const info=side==='large'?data.large:data.small;
    if(!info){return;}
    if(info.error){
      if(side==='large') omLargeNeedsPassword=omIsPasswordRelatedError(info.error);
      omSetBadge(side,'notfound','⚠ '+info.error.slice(0,30));
      return;
    }
    if(side==='large') omLargeNeedsPassword=false;
    omFillColDropdown(side,info.headers||[],document.getElementById(omColId(side)).value.trim());
    if(info.detected&&!document.getElementById(omColId(side)).value.trim()){
      document.getElementById(omColId(side)).value=info.detected;
      if(side==='large')omCheckDetected.large=info.detected;
      else omCheckDetected.small=info.detected;
      omSetBadge(side,'found','✔ تلقائي');
    } else if(document.getElementById(omColId(side)).value.trim()){
      omSetBadge(side,'found','▾ مختار');
    } else if(!info.detected){
      omSetBadge(side,'notfound','؟ غير مكتشف');
      if(info.headers&&info.headers.length)omShowHeadersHint(side,info.headers);
    }
    if((side==='large'||side==='small')&&!info.error){
      omRebuildCheckExportChoices(side,info.headers||[],true,info.column_samples||null);
    }
    if(side==='large'&&!info.error){
      omCheckLargeHasGps=!!(info.headers&&info.headers.includes('GPS'));
      const gs=document.getElementById('omGpsMatchSection');
      if(omCheckLargeHasGps){
        gs.style.display='';
        if(!document.getElementById('omGpsMyLat').value.trim())omRefreshCheckLoc();
      } else {
        gs.style.display='none';
      }
    }
    omCheckRunReady();
  }catch(e){omSetBadge(side,'notfound','⚠');}
}
function omShowHeadersHint(side,headers){
  const box=document.getElementById('omFieldHeadersHint');
  const content=document.getElementById('omFieldHeadersContent');
  box.style.display='';
  const label=side==='large'?'\u0627\u0644\u0645\u0644\u0641 \u0627\u0644\u0643\u0628\u064A\u0631':'\u0627\u0644\u0645\u0644\u0641 \u0627\u0644\u0635\u063A\u064A\u0631';
  const ti=omColId(side);
  content.innerHTML+='<p style="margin-bottom:.5rem"><strong style="color:var(--violet)">'+label+':</strong> \u0627\u0646\u0642\u0631 \u0644\u0627\u062E\u062A\u064A\u0627\u0631 \u0639\u0645\u0648\u062F \u0627\u0644\u0644\u0648\u062D\u0629:</p><div style="display:flex;flex-wrap:wrap;gap:.4rem;margin-bottom:.8rem">'+
    headers.map(h=>h?'<button onclick="document.getElementById(\''+ti+'\').value=\''+esc(h)+'\';omSetBadge(\''+side+'\',\'found\',\'\u270E \u064A\u062F\u0648\u064A\');omCheckRunReady();document.getElementById(\'omFieldHeadersHint\').style.display=\'none\'" style="padding:.3rem .7rem;background:var(--s2);border:1px solid var(--brd2);border-radius:6px;cursor:pointer;font-family:var(--mono);font-size:.75rem;color:var(--text);transition:all .18s" onmouseover="this.style.borderColor=\'var(--violet)\';this.style.color=\'var(--violet)\'" onmouseout="this.style.borderColor=\'var(--brd2)\';this.style.color=\'var(--text)\'">'+esc(h)+'</button>':'').join('')+'</div>';
}
function omCheckRunReady(){
  var hasSmall=!!omCheckSmallFile || omUsingSmallText();
  var hasLarge=!!omCheckLargeFile;
  var ok=hasSmall && (hasLarge || (omPostgresLargeEnabled && omUseStoredLarge && omHasStoredImports));
  document.getElementById('omMatchBtn').disabled=!ok;
}
async function omRunMatch(){
  var usingSmallText=omUsingSmallText();
  if(!omCheckSmallFile && !usingSmallText){
    omShowFieldStatus('err','\u064A\u0631\u062C\u0649 \u0631\u0641\u0639 \u0627\u0644\u0645\u0644\u0641 \u0627\u0644\u0635\u063A\u064A\u0631 \u0623\u0648 \u0625\u062F\u062E\u0627\u0644 \u0644\u0648\u062D\u0627\u062A \u0646\u0635\u064A\u0629');
    return;
  }
  var useStored=omPostgresLargeEnabled && omUseStoredLarge;
  if(omPostgresLargeEnabled && usingSmallText && !useStored){
    document.getElementById('omFieldHeadersHint').style.display='none';document.getElementById('omFieldHeadersContent').innerHTML='';
    document.getElementById('omGpsResultBox').classList.remove('show');
    document.getElementById('omGpsProgress').style.display='none';
    omShowFieldStatus('proc','جاري فحص اللوحات النصية …',true);document.getElementById('omMatchBtn').disabled=true;
    try{
      await omRunTempTextCheck();
    }catch(e){
      omShowFieldStatus('err','خطأ: '+(e.message||e));
    }finally{
      document.getElementById('omMatchBtn').disabled=false;
    }
    return;
  }
  if(!omCheckLargeFile && !useStored){
    omShowFieldStatus('err','\u064A\u0631\u062C\u0649 \u0631\u0641\u0639 \u0627\u0644\u0645\u0644\u0641 \u0627\u0644\u0643\u0628\u064A\u0631\u060C \u0623\u0648 \u0641\u0639\u0651\u0644 \u00ab\u0627\u0633\u062A\u062E\u062F\u0627\u0645 \u0627\u0644\u0628\u064A\u0627\u0646\u0627\u062A \u0627\u0644\u0645\u062E\u0632\u0651\u0646\u0629\u00bb \u0628\u0639\u062F \u0627\u0633\u062A\u064A\u0631\u0627\u062F \u0627\u0644\u0643\u0628\u064A\u0631 \u0625\u0644\u0649 \u0627\u0644\u062E\u0627\u062F\u0645');
    return;
  }
  omCheckResultBlob=null;document.getElementById('omResultBox').classList.remove('show');
  omRenderCheckMatchPreview(null);
  document.getElementById('omFieldHeadersHint').style.display='none';document.getElementById('omFieldHeadersContent').innerHTML='';
  omGpsResultBlob=null;document.getElementById('omGpsResultBox').classList.remove('show');
  document.getElementById('omGpsResultTableWrap').style.display='none';
  document.getElementById('omGpsResultTableBody').innerHTML='';
  omShowFieldStatus('proc','\u062C\u0627\u0631\u064A \u0627\u0644\u0645\u0637\u0627\u0628\u0642\u0629 \u2026',true);document.getElementById('omMatchBtn').disabled=true;
  try{
    const fd=new FormData();
    if(!(omPostgresLargeEnabled && omUseStoredLarge)){
      fd.append('large_file',omCheckLargeFile);
    } else {
      fd.append('use_stored_large','true');
    }
    if(usingSmallText){
      fd.append('small_plates_text',document.getElementById('omSmallPlatesText').value||'');
    }else{
      fd.append('small_file',omCheckSmallFile);
    }
    const pw=document.getElementById('omLargePw').value.trim();if(pw)fd.append('password',pw);
    const lc=document.getElementById('omLargeCol').value.trim();if(lc)fd.append('large_col',lc);
    const sc=document.getElementById('omSmallCol').value.trim();if(sc)fd.append('small_col',sc);
    fd.append('large_export_cols_json',omGetCheckExportColsJson('large'));
    fd.append('small_export_cols_json',omGetCheckExportColsJson('small'));
    const res=await fetch('/api/check',{method:'POST',body:fd});
    if(!res.ok){
      const errData=await res.json().catch(()=>({detail:res.statusText}));
      throw new Error(errData.detail||res.statusText);
    }
    const start=await res.json();
    const jobId=start.job_id;
    if(!jobId) throw new Error('\u0644\u0645 \u064A\u064F\u0631\u062C\u0639 job_id');
    const result=await omPollCheckStatus(jobId);
    if(result.kind==='xlsx'){
      let bytes;
      if(result.storage==='file'||!result.content_b64){
        const fr=await fetch('/api/check/result/'+encodeURIComponent(jobId));
        if(!fr.ok){
          const ej=await fr.json().catch(()=>({}));
          throw new Error(ej.detail||fr.statusText||'تعذّر تحميل ملف النتيجة');
        }
        bytes=new Uint8Array(await fr.arrayBuffer());
      }else{
        const bin=atob(result.content_b64);
        bytes=new Uint8Array(bin.length);
        for(let i=0;i<bin.length;i++) bytes[i]=bin.charCodeAt(i);
      }
      omCheckResultBlob=new Blob([bytes],{type:'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'});
      omShowFieldStatus('ok','\u2705 \u062A\u0645\u062A \u0627\u0644\u0645\u0637\u0627\u0628\u0642\u0629 \u2014 \u0627\u0644\u0646\u062A\u0627\u0626\u062C \u0623\u062F\u0646\u0627\u0647 \u0648\u064A\u0645\u0643\u0646 \u0641\u062A\u062D\u0647\u0627 \u0641\u064A Excel');
      const lu=document.getElementById('omLargeCol').value.trim()||omCheckDetected.large||'\u2014';
      const su=document.getElementById('omSmallCol').value.trim()||omCheckDetected.small||'\u2014';
      document.getElementById('omRLargeCol').textContent=lu;document.getElementById('omRSmallCol').textContent=su;
      const pv=result.preview;
      if(pv&&pv.stats){
        document.getElementById('omRMatched').textContent=String(pv.stats.matched_rows!=null?pv.stats.matched_rows:'\u2014');
        document.getElementById('omRPlates').textContent=String(pv.stats.matched_plate_hits!=null?pv.stats.matched_plate_hits:'\u2014');
        document.getElementById('omRUnmatched').textContent=String(pv.stats.unmatched_plates!=null?pv.stats.unmatched_plates:'\u2014');
      }else{
        document.getElementById('omRMatched').textContent='\u2714';document.getElementById('omRPlates').textContent='\u2714';document.getElementById('omRUnmatched').textContent='\u2014';
      }
      omRenderCheckMatchPreview(pv||null);
      document.getElementById('omDlBtn').style.display='';
      document.getElementById('omResultBox').classList.add('show');
      if(omCheckLargeHasGps) await omRunGpsNearestAfterMatch();
    }else if(result.kind==='json'){
      const data=result.body;
      const statusCode=result.status_code;
      if(statusCode===200&&data.matched===0){
        omShowFieldStatus('warn','\u0644\u0627 \u062A\u0648\u062C\u062F \u062A\u0637\u0627\u0628\u0642\u0627\u062A \u2014 \u062A\u062D\u0642\u0642 \u0645\u0646 \u0623\u0633\u0645\u0627\u0621 \u0627\u0644\u0623\u0639\u0645\u062F\u0629 \u0623\u0648 \u0627\u0644\u0645\u062D\u062A\u0648\u0649');
        document.getElementById('omRMatched').textContent='0';document.getElementById('omRPlates').textContent='0';
        document.getElementById('omRUnmatched').textContent=data.unmatched??'\u2014';
        document.getElementById('omRLargeCol').textContent=data.large_col_used??'\u2014';document.getElementById('omRSmallCol').textContent=data.small_col_used??'\u2014';
        document.getElementById('omDlBtn').style.display='none';document.getElementById('omResultBox').classList.add('show');
      }else if(statusCode===422){
        omShowFieldStatus('err',data.detail||'\u062E\u0637\u0623 \u063A\u064A\u0631 \u0645\u0639\u0631\u0648\u0641');
        if(data.headers){omShowHeadersHint(data.code==='COL_NOT_FOUND_SMALL'?'small':'large',data.headers);}
      }else{
        omShowFieldStatus('err',data.detail||'\u062E\u0637\u0623 \u063A\u064A\u0631 \u0645\u0639\u0631\u0648\u0641');
        if(data.headers){omShowHeadersHint(data.code==='COL_NOT_FOUND_SMALL'?'small':'large',data.headers);}
      }
    }else throw new Error('\u0627\u0633\u062A\u062C\u0627\u0628\u0629 \u063A\u064A\u0631 \u0645\u062A\u0648\u0642\u0639\u0629');
  }catch(e){omShowFieldStatus('err','\u062E\u0637\u0623: '+e.message);}
  finally{
    document.getElementById('omGpsProgress').style.display='none';
    document.getElementById('omMatchBtn').disabled=false;
  }
}
function omCheckIsGpsHeader(h){
  const t=String(h||'').trim();
  if(/^GPS$/i.test(t)) return true;
  if(/^صغير\s*[—-]\s*GPS$/i.test(t)) return true;
  return false;
}
function omCheckParseGpsCoords(val){
  if(val==null||val==='') return null;
  const t=String(val).trim();
  if(!t.includes(',')) return null;
  const parts=t.split(',');
  if(parts.length<2) return null;
  const lat=parseFloat(parts[0].trim());
  const lng=parseFloat(parts[1].trim());
  if(isNaN(lat)||isNaN(lng)) return null;
  if(Math.abs(lat)>90||Math.abs(lng)>180) return null;
  return {lat:lat,lng:lng};
}
function omCheckCellDisplayHtml(header,val){
  const raw=val==null?'':String(val);
  const coords=omCheckParseGpsCoords(raw);
  if(coords&&omCheckIsGpsHeader(header)){
    const href='https://www.google.com/maps/dir/?api=1&destination='+encodeURIComponent(coords.lat+','+coords.lng);
    return '<a class="maps-link" href="'+href+'" target="_blank" rel="noopener" title="فتح الخريطة">&#x1F4CD; فتح الخريطة</a>';
  }
  return esc(raw);
}
function omRenderCheckMatchPreview(preview){
  const wrap=document.getElementById('omMatchTableWrap');
  const thead=document.getElementById('omMatchThead');
  const tbody=document.getElementById('omMatchTbody');
  const note=document.getElementById('omMatchTruncNote');
  if(!wrap||!thead||!tbody) return;
  if(!preview||!preview.headers||!preview.headers.length){
    wrap.style.display='none';
    if(note){ note.style.display='none'; note.textContent=''; }
    thead.innerHTML='';
    tbody.innerHTML='';
    return;
  }
  const srcs=preview.col_sources||[];
  function thCls(i){ return srcs[i]==='small'?' class="om-match-col-small"':''; }
  if(!preview.rows||!preview.rows.length){
    thead.innerHTML='<tr>'+preview.headers.map(function(h,i){ return '<th'+thCls(i)+'>'+esc(h)+'</th>'; }).join('')+'</tr>';
    tbody.innerHTML='';
    wrap.style.display='';
    if(note) note.style.display='none';
    return;
  }
  thead.innerHTML='<tr>'+preview.headers.map(function(h,i){ return '<th'+thCls(i)+'>'+esc(h)+'</th>'; }).join('')+'</tr>';
  tbody.innerHTML='';
  preview.rows.forEach(function(row){
    const tr=document.createElement('tr');
    row.forEach(function(cell,idx){
      const td=document.createElement('td');
      td.className='td-gps'+(srcs[idx]==='small'?' om-match-col-small':'');
      td.innerHTML=omCheckCellDisplayHtml(preview.headers[idx],cell);
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  wrap.style.display='';
  if(note){
    if(preview.truncated){
      note.textContent='عرض أول '+preview.rows.length+' صف من أصل '+preview.total_rows+' — الملف الكامل عند الفتح في Excel.';
      note.style.display='';
    }else{
      note.style.display='none';
      note.textContent='';
    }
  }
}
function omGetCheckMatchExcelFilename(){
  const ts=new Date().toISOString().slice(0,16).replace('T','_').replace(':','-');
  return '\u0627\u0644\u062A\u0637\u0627\u0628\u0642\u0627\u062A_'+ts+'.xlsx';
}
async function omOpenExcelResult(){
  if(!omCheckResultBlob) return;
  const filename=omGetCheckMatchExcelFilename();
  const mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
  const file=new File([omCheckResultBlob],filename,{type:mime});
  try{
    if(navigator.canShare&&navigator.canShare({files:[file]})){
      await navigator.share({files:[file],title:filename});
      return;
    }
  }catch(e){
    if(e&&e.name==='AbortError') return;
  }
  const url=URL.createObjectURL(omCheckResultBlob);
  try{
    const win=window.open(url,'_blank','noopener,noreferrer');
    if(win){
      setTimeout(function(){ try{ URL.revokeObjectURL(url); }catch(_){} },120000);
      return;
    }
  }catch(_){}
  try{
    if(window.showSaveFilePicker){
      const base=filename.replace(/\.xlsx$/i,'');
      const h=await window.showSaveFilePicker({
        suggestedName:base||'التطابقات',
        types:[{description:'Excel',accept:{[mime]:['.xlsx']}}]
      });
      const w=await h.createWritable();
      await w.write(omCheckResultBlob);
      await w.close();
      try{ URL.revokeObjectURL(url); }catch(_){}
      omShowFieldStatus('ok','\u2705 \u062A\u0645 \u062D\u0641\u0638 \u0627\u0644\u0645\u0644\u0641. \u0627\u0641\u062A\u062D\u0647 \u0641\u064A Excel \u0645\u0646 \u0627\u0644\u0645\u0648\u0636\u0639 \u0627\u0644\u0630\u064A \u0627\u062E\u062A\u0631\u062A\u0647.');
      return;
    }
  }catch(e){
    if(e&&e.name==='AbortError'){ try{ URL.revokeObjectURL(url); }catch(_){} return; }
  }
  try{ URL.revokeObjectURL(url); }catch(_){}
  triggerDownload(omCheckResultBlob,filename);
  omShowFieldStatus('warn','\u062A\u0645 \u062A\u0646\u0632\u064A\u0644 \u0627\u0644\u0645\u0644\u0641 \u2014 \u0627\u0641\u062A\u062D\u0647 \u0645\u0646 \u0627\u0644\u0645\u062C\u0644\u062F \u0623\u0648 \u0627\u0644\u062A\u0646\u0632\u064A\u0644\u0627\u062A \u0641\u064A Excel.');
}
function omTogglePw(inputId,btn){const inp=document.getElementById(inputId);inp.type=inp.type==='password'?'text':'password';btn.textContent=inp.type==='password'?'\uD83D\uDC41':'\uD83D\uDE48';}
function omConfirmLargePw(){
  if(omPostgresLargeEnabled && omUseStoredLarge){
    omResetCheckDetect();
    omDetectColForSide('large');
    return;
  }
  if(!omCheckLargeFile){omShowFieldStatus('err','يرجى رفع الملف الكبير أولاً');return;}
  omResetCheckDetect();
  omDetectColForSide('large').then(function(){
    if(omPostgresLargeEnabled && !omUseStoredLarge){
      var pw=document.getElementById('omLargePw').value.trim();
      if(omLargeNeedsPassword && !pw){
        omShowFieldStatus('err','يرجى إدخال كلمة مرور الملف الكبير أولاً.');
        return;
      }
      omImportLargeToServer();
    }
  });
}
function omRefreshCheckLoc(){
  const btn=document.getElementById('omGpsLocBtn');btn.disabled=true;omSetCheckLocDot('','\u062C\u0627\u0631\u064A...');
  if(!navigator.geolocation){omSetCheckLocDot('err','GPS \u063A\u064A\u0631 \u0645\u062F\u0639\u0648\u0645');btn.disabled=false;return;}
  navigator.geolocation.getCurrentPosition(
    pos=>{document.getElementById('omGpsMyLat').value=pos.coords.latitude.toFixed(6);document.getElementById('omGpsMyLon').value=pos.coords.longitude.toFixed(6);omSetCheckLocDot('on','\u2713 \u0645\u062D\u062F\u062F \xB1'+Math.round(pos.coords.accuracy)+'\u0645');btn.disabled=false;},
    ()=>{omSetCheckLocDot('err','\u0641\u0634\u0644 GPS');btn.disabled=false;},
    {enableHighAccuracy:true,timeout:12000,maximumAge:0}
  );
}
function omSetCheckLocDot(state,txt){const d=document.getElementById('omGpsLocDot');d.className='dot'+(state==='on'?' on':state==='err'?' err':'');document.getElementById('omGpsLocTxt').textContent=txt;}
async function omRunGpsNearestAfterMatch(){
  const orsKey=getOrsKey().trim();
  const myLat=document.getElementById('omGpsMyLat').value.trim();
  const myLon=document.getElementById('omGpsMyLon').value.trim();
  if(!orsKey||!myLat||!myLon){
    omShowFieldStatus('ok','\u2705 \u062A\u0645\u062A \u0627\u0644\u0645\u0637\u0627\u0628\u0642\u0629 \u2014 \u0644\u0645 \u064A\u064F\u062D\u0633\u0628 \u0623\u0642\u0631\u0628 \u0627\u0644\u0645\u0631\u0643\u0628\u0627\u062A (\u0623\u0636\u0641 \u0645\u0641\u062A\u0627\u062D ORS \u0645\u0646 \u2699 \u0627\u0644\u0625\u0639\u062F\u0627\u062F\u0627\u062A \u0648\u062D\u062F\u062F \u0645\u0648\u0642\u0639\u0643).');
    return;
  }
  document.getElementById('omGpsProgress').style.display='';
  document.getElementById('omGpsProgFill').style.width='0%';
  document.getElementById('omGpsProgLbl').textContent='';
  omShowFieldStatus('proc','\u062C\u0627\u0631\u064A \u0645\u0637\u0627\u0628\u0642\u0629 GPS (\u0623\u0642\u0631\u0628 \u0627\u0644\u0645\u0631\u0643\u0628\u0627\u062A) \u2026',true);
  let vehicles=[];
  try{
    const fd=new FormData();
    let res;
    if(omPostgresLargeEnabled && omUseStoredLarge){
      if(usingSmallText){
      fd.append('small_plates_text',document.getElementById('omSmallPlatesText').value||'');
    }else{
      fd.append('small_file',omCheckSmallFile);
    }
      const sc2=document.getElementById('omSmallCol').value.trim();if(sc2)fd.append('small_col',sc2);
      res=await fetch('/api/check/gps-stored',{method:'POST',body:fd});
    } else {
      fd.append('large_file',omCheckLargeFile);fd.append('small_file',omCheckSmallFile);
      const pw=document.getElementById('omLargePw').value.trim();if(pw)fd.append('password',pw);
      const lc=document.getElementById('omLargeCol').value.trim();if(lc)fd.append('large_col',lc);
      const sc=document.getElementById('omSmallCol').value.trim();if(sc)fd.append('small_col',sc);
      res=await fetch('/api/check-gps-data',{method:'POST',body:fd});
    }
    const data=await res.json();
    if(!res.ok){omShowFieldStatus('warn','\u2705 \u062A\u0645\u062A \u0645\u0637\u0627\u0628\u0642\u0629 Excel. GPS: '+String(data.detail||'\u062E\u0637\u0623'));omGpsReset();return;}
    if(!data.vehicles||!data.vehicles.length){
      omShowFieldStatus('ok','\u2705 \u062A\u0645\u062A \u0645\u0637\u0627\u0628\u0642\u0629 Excel. \u0644\u0627 \u062A\u0648\u062C\u062F \u0645\u0631\u0643\u0628\u0627\u062A \u0628\u0625\u062D\u062F\u0627\u062B\u064A\u0627\u062A \u0644\u0644\u0645\u0637\u0627\u0628\u0642\u0629.');
      omGpsReset();
      return;
    }
    vehicles=data.vehicles;
    if(data.skipped>0)omShowFieldStatus('proc','\u062A\u0645 \u062A\u062E\u0637\u064A '+data.skipped+' \u0628\u062F\u0648\u0646 GPS \u2026',true);
  }catch(e){
    omShowFieldStatus('warn','\u2705 \u062A\u0645\u062A \u0645\u0637\u0627\u0628\u0642\u0629 Excel. GPS: '+e.message);
    omGpsReset();
    return;
  }
  const ORS='https://api.openrouteservice.org/v2/directions/driving-car';
  const results=[],failed=[];let quotaExceeded=false;
  for(let i=0;i<vehicles.length&&!quotaExceeded;i++){
    const v=vehicles[i];
    const pct=Math.round(((i+1)/vehicles.length)*100);
    document.getElementById('omGpsProgFill').style.width=pct+'%';
    document.getElementById('omGpsProgLbl').textContent=v.plate+' ('+(i+1)+'/'+vehicles.length+')';
    omShowFieldStatus('proc','\u062C\u0627\u0631\u064A \u062D\u0633\u0627\u0628 \u0627\u0644\u0645\u0633\u0627\u0631\u0627\u062A... '+pct+'%',true);
    const parts=v.gps.split(',');const vLat=parseFloat(parts[0].trim()),vLon=parseFloat(parts[1].trim());
    if(isNaN(vLat)||isNaN(vLon)){failed.push({...v,reason:'\u0625\u062D\u062F\u0627\u062B\u064A\u0627\u062A \u063A\u064A\u0631 \u0635\u062D\u064A\u062D\u0629'});continue;}
    if(Math.hypot(parseFloat(myLat)-vLat,parseFloat(myLon)-vLon)<0.0001){results.push({...v,distance_km:0,duration_min:0});continue;}
    let routed=false;
    for(let att=0;att<=2&&!routed;att++){
      try{
        const r=await fetch(ORS,{method:'POST',headers:{'Authorization':orsKey,'Content-Type':'application/json'},body:JSON.stringify({coordinates:[[parseFloat(myLon),parseFloat(myLat)],[vLon,vLat]],units:'km'})});
        const rd=await r.json();
        if(r.status===429){omShowFieldStatus('warn','\u2705 Excel \u062C\u0627\u0647\u0632. GPS: \u062A\u062C\u0627\u0648\u0632\u062A \u062D\u062F ORS');quotaExceeded=true;break;}
        if(r.ok&&rd.routes?.length){const s=rd.routes[0].summary;results.push({...v,distance_km:+s.distance.toFixed(2),duration_min:+(s.duration/60).toFixed(1)});routed=true;}
        else{if(att<2)await new Promise(r=>setTimeout(r,3000));else failed.push({...v,reason:rd?.error?.message||'\u0644\u0627 \u064A\u0648\u062C\u062F \u0645\u0633\u0627\u0631'});}
      }catch(e2){if(att<2)await new Promise(r=>setTimeout(r,3000));else failed.push({...v,reason:e2.message});}
    }
    if(!quotaExceeded&&i<vehicles.length-1)await new Promise(r=>setTimeout(r,1200));
  }
  document.getElementById('omGpsProgress').style.display='none';
  if(!results.length){
    omShowFieldStatus('warn','\u2705 Excel \u062C\u0627\u0647\u0632. \u0644\u0645 \u064A\u062A\u0645 \u062D\u0633\u0627\u0628 \u0645\u0633\u0627\u0631 GPS.');
    omGpsReset();
    return;
  }
  results.sort((a,b)=>a.duration_min-b.duration_min);results.forEach((r,i)=>r.rank=i+1);
  const tbody=document.getElementById('omGpsResultTableBody');
  tbody.innerHTML='';
  results.forEach((r,i)=>{
    const gps=r.gps||'';
    let mapsUrl='';
    if(gps&&gps.includes(',')&&myLat&&myLon){
      const parts=gps.split(',');
      try{
        const vLat=parseFloat(parts[0].trim()),vLng=parseFloat(parts[1].trim());
        if(!isNaN(vLat)&&!isNaN(vLng))
          mapsUrl='https://www.google.com/maps/dir/'+myLat+','+myLon+'/'+vLat+','+vLng;
      }catch(e2){}
    }
    const mapCell=mapsUrl
      ?'<a class="maps-link" href="'+mapsUrl+'" target="_blank" rel="noopener">&#x1F4CD; فتح الخريطة</a>'
      :'—';
    const isFirst=i===0;
    const tr=document.createElement('tr');
    if(isFirst)tr.style.background='rgba(34,197,94,.1)';
    tr.innerHTML=
      '<td class="td-num">'+r.rank+'</td>'+
      '<td class="td-plate" style="'+(isFirst?'color:var(--green);font-weight:800':'')+'">'+esc(r.plate||'')+'</td>'+
      '<td>'+mapCell+'</td>'+
      '<td>'+esc(r.vehicle_type||'')+'</td>'+
      '<td style="font-size:.78rem;color:var(--dim2)">'+esc(r.notes||'')+'</td>'+
      '<td class="td-num" style="color:var(--sky)">'+r.distance_km+'</td>'+
      '<td class="td-num" style="color:var(--sky)">'+r.duration_min+'</td>'+
      '<td style="font-family:var(--mono);font-size:.75rem">'+esc(r.date||'')+'</td>';
    tbody.appendChild(tr);
  });
  document.getElementById('omGpsResultTableWrap').style.display='';
  document.getElementById('omGpsRSucc').textContent=results.length;
  document.getElementById('omGpsRFail').textContent=failed.length||'0';
  document.getElementById('omGpsRNearest').textContent=results[0].plate;
  document.getElementById('omGpsResultBox').classList.add('show');
  omShowFieldStatus('proc','\u062C\u0627\u0631\u064A \u0625\u0646\u0634\u0627\u0621 Excel GPS \u2026',true);
  try{
    const fd2=new FormData();fd2.append('results_json',JSON.stringify(results));fd2.append('failed_json',JSON.stringify(failed));fd2.append('my_lat',myLat);fd2.append('my_lon',myLon);
    const er=await fetch('/api/export-gps-excel',{method:'POST',body:fd2});
    if(!er.ok){const e=await er.json().catch(()=>({detail:er.statusText}));throw new Error(e.detail);}
    omGpsResultBlob=await er.blob();
    omShowFieldStatus('ok','\u2705 \u062A\u0645\u062A \u0627\u0644\u0645\u0637\u0627\u0628\u0642\u0629 \u0648 GPS: '+results.length+' \u0645\u0631\u0643\u0628\u0629'+(failed.length?' \u2014 '+failed.length+' \u0641\u0634\u0644\u062A':''));
  }catch(e){omShowFieldStatus('warn','\u2705 Excel \u0627\u0644\u062A\u0637\u0627\u0628\u0642 \u062C\u0627\u0647\u0632. \u062E\u0637\u0623 GPS: '+e.message);}
}
function omGpsReset(){document.getElementById('omGpsProgress').style.display='none';}
function omDownloadGpsResult(){if(!omGpsResultBlob)return;const ts=new Date().toISOString().slice(0,16).replace('T','_').replace(':','-');triggerDownload(omGpsResultBlob,'\u0623\u0642\u0631\u0628_\u0627\u0644\u0645\u0631\u0643\u0628\u0627\u062A_'+ts+'.xlsx');}
