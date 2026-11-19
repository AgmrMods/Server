// friends-store.js — Stickman Combat arkadaşlık deposu (sunucuya ekle)
// Kullanım (clan-store.js'in yanına):
//   const fx = require('./friends-store');
//   // HTTP isteği işleyicisinde, diğer yollardan önce:
//   if (fx.handle(req, res)) return;
// Ayrıca CORS için (yoksa): Access-Control-Allow-Origin: *  (handle bunu kendisi ekler)
const fs = require('fs');
const FILE = process.env.FRIENDS_FILE || './friends-data.json';
const OK = /^(pl|frq|chl)\/[A-Za-z0-9_\-]{1,40}$/;
// Kalıcı depo: UPSTASH_REDIS_REST_URL + UPSTASH_REDIS_REST_TOKEN ortam değişkenleri varsa Upstash Redis, yoksa dosya.
const RU = (process.env.UPSTASH_REDIS_REST_URL || '').replace(/\/+$/, ''), RT = process.env.UPSTASH_REDIS_REST_TOKEN || '';
const redis = cmd => fetch(RU, { method: 'POST', headers: { Authorization: 'Bearer ' + RT, 'Content-Type': 'application/json' }, body: JSON.stringify(cmd) }).then(r => r.json());
let db = { tk: {}, kv: {} };
let ready;
if (RU && RT) {
  ready = redis(['GET', 'stk_friends']).then(j => { if (j && j.result) db = JSON.parse(j.result); }).catch(e => console.error('friends-store: Upstash okunamadı', e)).then(() => {});
} else {
  try { db = JSON.parse(fs.readFileSync(FILE, 'utf8')); } catch (e) {}
  ready = Promise.resolve();
}
ready = ready.then(() => { db.tk = db.tk || {}; db.kv = db.kv || {}; });
let st = 0;
const save = () => { clearTimeout(st); st = setTimeout(() => {
  try {
    if (RU && RT) redis(['SET', 'stk_friends', JSON.stringify(db)]).catch(e => console.error('friends-store: Upstash yazılamadı', e));
    else fs.writeFileSync(FILE, JSON.stringify(db));
  } catch (e) {}
}, 1500); };
const now = () => Date.now();
function prune() {
  const t = now();
  for (const p in db.kv) {
    const e = db.kv[p], c = p.split('/')[0];
    if (c === 'chl' && t - e.t > 600000) delete db.kv[p];
    else if (e.d === null && t - e.t > 7 * 864e5) delete db.kv[p];
    else if (c === 'pl' && t - e.t > 60 * 864e5) delete db.kv[p];
  }
}
setInterval(() => { prune(); save(); }, 600000);
const involves = (p, id) => { const c = p.split('/')[0], i = p.slice(c.length + 1); return i === id || ((c === 'frq' || c === 'chl') && i.split('_').indexOf(id) >= 0); };
// yazma yetkisi: pl/<id> sadece sahibi; frq/chl çiftinden biri
const canWrite = (p, id) => involves(p, id);
function handle(req, res) {
  const url = req.url || '';
  if (url.split('?')[0] !== '/kv') return false;
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return true; }
  if (req.method !== 'POST') { res.writeHead(405); res.end(); return true; }
  let body = '';
  req.on('data', c => { body += c; if (body.length > 40000) req.destroy(); });
  req.on('end', () => ready.then(() => {
    let m; try { m = JSON.parse(body); } catch (e) { res.writeHead(400); res.end(); return; }
    const id = String(m.id || ''), tk = String(m.tk || '');
    if (!/^[A-Za-z0-9_\-]{4,40}$/.test(id) || tk.length < 20 || tk.length > 120) { res.writeHead(400); res.end(); return; }
    if (!db.tk[id]) { db.tk[id] = tk; save(); } else if (db.tk[id] !== tk) { res.writeHead(403); res.end(); return; }
    const out = o => { res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(o)); };
    if (m.op === 'set') {
      const p = String(m.p || ''), t = +m.t;
      if (!OK.test(p) || !canWrite(p, id) || !isFinite(t) || t > now() + 600000) { res.writeHead(400); res.end(); return; }
      if (m.d !== null && (typeof m.d !== 'object' || JSON.stringify(m.d).length > 14000)) { res.writeHead(400); res.end(); return; }
      const e = db.kv[p];
      if (!e || e.t < t) { db.kv[p] = { d: m.d, t }; save(); }
      return out({ ok: 1 });
    }
    if (m.op === 'get') {
      const p = String(m.p || ''), e = OK.test(p) ? db.kv[p] : null;
      return out({ item: e && e.d ? { p, d: e.d, t: e.t } : null });
    }
    if (m.op === 'list') {
      const items = [], pl = [];
      for (const p in db.kv) {
        const e = db.kv[p], c = p.split('/')[0];
        if (c === 'pl') { if (e.d) pl.push({ p, d: e.d, t: e.t }); }
        else if (involves(p, id)) items.push({ p, d: e.d, t: e.t });
      }
      pl.sort((a, b) => b.t - a.t);
      return out({ items: items.concat(pl.slice(0, 100)) });
    }
    res.writeHead(400); res.end();
  }));
  return true;
}
module.exports = { handle };
