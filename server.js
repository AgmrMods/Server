/*
  STICKMAN COMBAT — gerçek zamanlı oda sunucusu (Nitroxine Studios)
  Bağımlılık YOK:  node server.js   (Node 16+)   ·   PORT ortam değişkeni ile port seçilir (varsayılan 8080)

  Tasarım (düşük ping):
   - Saf WebSocket, TCP_NODELAY açık, sıkıştırma kapalı, mesaj başına minimum iş
   - Sunucu sadece "röle": oyun simülasyonunu odanın sahibi (host) çalıştırır, sunucu paketleri anında iletir
   - Yavaş alıcıya birikmiş eski snapshot gönderilmez (backpressure → paket atılır, gecikme birikmez)
   - Kısa kopmalarda (mobil ağ geçişi) 6 sn içinde aynı id ile geri bağlanma (oda ve maç bozulmaz)
   - Odalar: "lobby*" = oda listesi (kalabalık), diğerleri = 2 kişilik maç odası, 3. kişi "full" alır
*/
'use strict';
const http = require('http');
const crypto = require('crypto');

const PORT = +process.env.PORT || 8080;
const MAX_MATCH = 2, MAX_LOBBY = 300;
const MAX_MSG = 8192;                 // tek mesaj üst sınırı (snapshot ~600 B)
const RATE_MAX = 400;                 // bir bağlantı için saniyedeki en çok mesaj
const GRACE_MS = 6000;                // kopan oyuncuyu bekleme süresi
const DEAD_MS = 35000;                // hiç ses yoksa bağlantıyı at
const SNAP_DROP_BYTES = 32768;        // alıcıda bu kadar birikmişse snapshot atla
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

const rooms = new Map();              // ad -> { name, peers: Map(id -> peer) }
let totalConn = 0;

/* ---------------- HTTP (sağlık kontrolü; Render/Railway uyandırma) ---------------- */
const server = http.createServer((req, res) => {
  const h = { 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-store' };
  if (req.url === '/health' || req.url === '/') {
    let p = 0, r = 0;
    rooms.forEach(rm => { r++; p += rm.peers.size; });
    res.writeHead(200, { ...h, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: 1, game: 'stickman-combat', rooms: r, players: p, conns: totalConn, up: Math.round(process.uptime()) }));
  } else { res.writeHead(404, h); res.end(); }
});
server.keepAliveTimeout = 5000;

/* ---------------- WebSocket çerçeveleme (RFC 6455) ---------------- */
function frame(op, payload) {
  const len = payload.length;
  let head;
  if (len < 126) { head = Buffer.allocUnsafe(2); head[1] = len; }
  else if (len < 65536) { head = Buffer.allocUnsafe(4); head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.allocUnsafe(10); head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  head[0] = 0x80 | op;
  return Buffer.concat([head, payload]);
}
const textFrame = s => frame(1, Buffer.from(s, 'utf8'));

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  socket.setKeepAlive(true, 15000);
  socket.setTimeout(0);
  new Conn(socket);
});

class Conn {
  constructor(sock) {
    this.sock = sock; this.buf = Buffer.alloc(0); this.alive = true; this.lastRx = Date.now();
    this.peer = null; this.frag = null; this.fragOp = 0; this.cnt = 0; this.cntT = Date.now();
    totalConn++;
    sock.on('data', d => this.onData(d));
    sock.on('close', () => this.onClose());
    sock.on('error', () => { try { sock.destroy(); } catch (e) {} });
  }
  onData(d) {
    this.lastRx = Date.now();
    this.buf = this.buf.length ? Buffer.concat([this.buf, d]) : d;
    if (this.buf.length > MAX_MSG * 4) { this.kill(); return; }
    for (;;) {
      const b = this.buf;
      if (b.length < 2) return;
      const fin = (b[0] & 0x80) !== 0, op = b[0] & 0x0f, masked = (b[1] & 0x80) !== 0;
      let len = b[1] & 0x7f, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; const L = b.readBigUInt64BE(2); if (L > BigInt(MAX_MSG)) { this.kill(); return; } len = Number(L); off = 10; }
      if (len > MAX_MSG) { this.kill(); return; }
      if (!masked) { this.kill(); return; }               // tarayıcı istemcileri her zaman maskeler
      if (b.length < off + 4 + len) return;
      const mask = b.subarray(off, off + 4), pl = Buffer.allocUnsafe(len);
      for (let i = 0; i < len; i++) pl[i] = b[off + 4 + i] ^ mask[i & 3];
      this.buf = b.subarray(off + 4 + len);
      this.onFrame(fin, op, pl);
      if (!this.alive) return;
    }
  }
  onFrame(fin, op, pl) {
    if (op === 8) { this.sendRaw(frame(8, pl.subarray(0, 2))); this.kill(); return; }
    if (op === 9) { this.sendRaw(frame(10, pl)); return; }        // ping -> pong
    if (op === 10) return;
    if (op === 0) {                                                // devam çerçevesi
      if (!this.frag) return;
      this.frag.push(pl);
      if (fin) { const all = Buffer.concat(this.frag); this.frag = null; if (this.fragOp === 1) this.onText(all.toString('utf8')); }
      return;
    }
    if (op === 1) { if (fin) this.onText(pl.toString('utf8')); else { this.frag = [pl]; this.fragOp = 1; } }
  }
  sendRaw(buf) { if (this.alive && !this.sock.destroyed) { try { this.sock.write(buf); } catch (e) {} } }
  send(s) { this.sendRaw(textFrame(s)); }
  sendDroppable(s) {                                               // eski snapshot biriktirme
    if (this.sock.writableLength > SNAP_DROP_BYTES) return;
    this.send(s);
  }
  kill() { if (!this.alive) return; this.alive = false; try { this.sock.destroy(); } catch (e) {} this.onClose(); }
  onClose() {
    if (this._closed) return; this._closed = true; this.alive = false; totalConn--;
    const p = this.peer; if (!p || p.conn !== this) return;
    p.conn = null;
    p.timer = setTimeout(() => dropPeer(p), GRACE_MS);              // kısa kopmada yerini koru
  }
  onText(s) {
    const now = Date.now();
    if (now - this.cntT > 1000) { this.cnt = 0; this.cntT = now; }
    if (++this.cnt > RATE_MAX) return;                              // taşkın koruması
    let m; try { m = JSON.parse(s); } catch (e) { return; }
    if (!m || typeof m.k !== 'string') return;
    if (!this.peer) { if (m.k === 'j') join(this, m); else this.kill(); return; }
    const p = this.peer, rm = p.room;
    switch (m.k) {
      case 'ev': {                                                  // olay: odadaki diğerlerine ilet
        if (typeof m.t !== 'string' || m.t.length > 16) return;
        const out = JSON.stringify({ k: 'ev', f: p.id, t: m.t, d: m.d });
        const drop = m.t === 'snap';
        rm.peers.forEach(o => { if (o !== p && o.conn) drop ? o.conn.sendDroppable(out) : o.conn.send(out); });
        break;
      }
      case 'pr': {                                                  // varlık bilgisi (takma ad, karakter, oda ilanı…)
        const js = JSON.stringify(m.p || {});
        if (js.length > 1500 || js === p.prJs) return;              // değişmediyse yayma
        p.prJs = js; p.pr = m.p || {};
        const out = JSON.stringify({ k: 'pr', f: p.id, p: p.pr });
        rm.peers.forEach(o => { if (o !== p && o.conn) o.conn.send(out); });
        break;
      }
      case 'hb': break;                                             // canlı tutma
    }
  }
}

function rid() { return crypto.randomBytes(4).toString('hex'); }

function join(c, m) {
  const name = String(m.r || '').slice(0, 48);
  if (!/^[\w\-:.]{1,48}$/.test(name)) { c.send('{"k":"err","e":"bad"}'); c.kill(); return; }
  let rm = rooms.get(name);
  if (!rm) { rm = { name, peers: new Map(), lobby: name.startsWith('lobby') || name.startsWith('sc-lobby') }; rooms.set(name, rm); }
  const cap = rm.lobby ? MAX_LOBBY : MAX_MATCH;
  let p = null;
  const wantId = typeof m.id === 'string' && /^[0-9a-f]{8}$/.test(m.id) ? m.id : null;
  if (wantId && rm.peers.has(wantId)) {                             // geri bağlanma
    p = rm.peers.get(wantId);
    if (p.conn && p.conn !== c) { const old = p.conn; p.conn = null; old.alive = false; old._closed = true; totalConn--; try { old.sock.destroy(); } catch (e) {} }
    clearTimeout(p.timer); p.timer = null;
  } else {
    if (rm.peers.size >= cap) { c.send('{"k":"err","e":"full"}'); setTimeout(() => c.kill(), 100); return; }
    p = { id: rid(), room: rm, conn: null, pr: {}, prJs: '', timer: null };
    while (rm.peers.has(p.id)) p.id = rid();
    rm.peers.set(p.id, p);
  }
  p.conn = c; c.peer = p;
  const list = [];
  rm.peers.forEach(o => { if (o !== p) list.push([o.id, o.pr]); });
  c.send(JSON.stringify({ k: 'w', id: p.id, peers: list, t: Date.now() }));
  if (!wantId || !p.prJs) {
    const out = JSON.stringify({ k: 'pr', f: p.id, p: p.pr });        // yeni geldiğini duyur
    rm.peers.forEach(o => { if (o !== p && o.conn) o.conn.send(out); });
  }
}

function dropPeer(p) {
  const rm = p.room;
  if (p.conn) return;
  rm.peers.delete(p.id);
  const out = JSON.stringify({ k: 'bye', f: p.id });
  rm.peers.forEach(o => { if (o.conn) o.conn.send(out); });
  if (!rm.peers.size) rooms.delete(rm.name);
}

/* ölü bağlantı temizliği + WS ping */
setInterval(() => {
  const now = Date.now();
  rooms.forEach(rm => rm.peers.forEach(p => {
    const c = p.conn; if (!c) return;
    if (now - c.lastRx > DEAD_MS) { c.kill(); return; }
    c.sendRaw(frame(9, Buffer.alloc(0)));
  }));
}, 15000).unref();

server.listen(PORT, '0.0.0.0', () => console.log('Stickman Combat sunucusu hazır :' + PORT));
