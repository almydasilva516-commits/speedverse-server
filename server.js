/* SPEEDVERSE — servidor WebSocket (até 50 jogadores) · ETAPA 10: mundo compartilhado
   Protocolo (JSON em TEXTO):
     cliente → servidor  {type:"state", x,y,z, r, g, u, k, s}        posição do jogador (s = velocidade em m/s)
                         {type:"vs",  id, s:{x,y,z,r,s,h,t,a,d,u,k,b}}  estado do vilão (SÓ a autoridade)
                         {type:"vhit", id, n, dir:[x,z]}             golpe num vilão (quem não é a autoridade)
                         {type:"vev", id, e:"imp", k, x,y,z}         flash de impacto (só a autoridade)
     servidor → cliente  {type:"welcome", id, max, count, players:[{id,x,y,z,r,g,u,k,s}]}
                         {type:"players", players:[...]}  ·  {type:"leave", id}  ·  {type:"count", count}  ·  {type:"hb"}
                         {type:"villains", vl:[{id,k,a,s}]}   estado atual dos vilões ao entrar (a = id da autoridade)
                         {type:"vauth", id, a, s}             a autoridade do vilão mudou
                         {type:"vs", id, s}                   estado do vilão (para quem NÃO é a autoridade)
                         {type:"vhit", id, n, dir, by}        golpe repassado à autoridade
                         {type:"vev", id, e:"down"|"back"|"imp", ...}   vilão derrotado · voltou à caça · impacto
   Vilões: cada um tem id único (hoje "rf-1" = Flash Reverso) e existe UMA vez. A "autoridade" é o jogador mais próximo
   dele: só o aparelho dela simula a IA; o servidor guarda o último estado, repassa a todos e, se a autoridade sair ou
   ficar parada, passa o comando a outro jogador, que continua do último estado.
   Render: Start Command "node server.js" · escuta process.env.PORT */
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = 50;
const TICK_MS = 100;          // envio em lote: cada cliente recebe só os jogadores que mudaram
const HB_MS = 15000;          // batimento: mantém a conexão viva no Render
const IDLE_MS = 30000;        // sem nenhuma mensagem do cliente (o cliente manda algo a cada 3 s)
const MAX_RATE = 40;          // mensagens por segundo aceitas de um mesmo cliente
const VIL_MS = 1000;          // revisão da autoridade dos vilões
const VIL_STALE = 4000;       // autoridade sem enviar o estado do vilão há tanto tempo → passa o comando
const VIL_SWITCH_MS = 6000;   // tempo mínimo entre trocas de autoridade (evita vai-e-volta)
const VIL_HIT_RANGE = 40;     // golpe só vale se o jogador está perto do vilão (tolerância de rede)

const players = new Map();    // id -> estado
const socks = new Map();      // id -> websocket
const dirty = new Set();

/* vilões compartilhados: um registro por vilão, com id único */
const villains = new Map([
  ['rf-1', { id: 'rf-1', kind: 'rf', auth: null, snap: null, at: 0, switchAt: 0, defeated: false }],
]);

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  const v = [...villains.values()].map(x => `${x.id}:${x.auth ? 'autoridade ' + x.auth : 'sem autoridade'}`).join(' · ');
  res.end(`SPEEDVERSE server OK · jogadores: ${players.size}/${MAX_PLAYERS} · vilões: ${v}`);
});
const wss = new WebSocketServer({ server, maxPayload: 2048, perMessageDeflate: false });

const send = (ws, obj) => { if (ws.readyState === 1) { try { ws.send(JSON.stringify(obj)); } catch (e) {} } };
const broadcast = (obj, except) => { const s = JSON.stringify(obj); for (const c of wss.clients) if (c.readyState === 1 && c.pid !== except) { try { c.send(s); } catch (e) {} } };
const pub = (p) => ({ id: p.id, x: p.x, y: p.y, z: p.z, r: p.r, g: p.g, u: p.u, k: p.k, s: p.s });
const fin = (v, lim) => (typeof v === 'number' && isFinite(v) && Math.abs(v) <= lim);
const int = (v, a, b) => (typeof v === 'number' && isFinite(v)) ? Math.max(a, Math.min(b, Math.round(v))) : null;

/* ---------- vilões compartilhados ---------- */
const vpub = (v) => ({ id: v.id, k: v.kind, a: v.auth, s: v.snap });

function cleanSnap(s) {
  if (!s || typeof s !== 'object') return null;
  if (!fin(s.x, 20000) || !fin(s.y, 20000) || !fin(s.z, 20000) || !fin(s.r, 100) || !fin(s.s, 200) || !fin(s.h, 1000)) return null;
  const t = int(s.t, 0, 5), a = int(s.a, 0, 3), k = int(s.k, 0, 2);
  if (t === null || a === null || k === null) return null;
  return {
    x: s.x, y: s.y, z: s.z, r: s.r, s: Math.max(0, s.s), h: Math.max(0, Math.min(100, s.h)),
    t, a, d: s.d ? 1 : 0, u: s.u ? 1 : 0, k,
    b: (typeof s.b === 'string' && players.has(s.b)) ? s.b : 0, // quem deu o último golpe (para o aviso "derrotou")
  };
}

/* autoridade = jogador mais próximo do vilão (quem já mandou posição tem prioridade sobre quem acabou de entrar) */
function pickAuth(v, exclude) {
  let best = null, bd = Infinity, any = null;
  for (const [id, p] of players) {
    if (id === exclude) continue;
    if (!any) any = id;
    if (!p.known) continue;
    const d = v.snap ? Math.hypot(p.x - v.snap.x, p.z - v.snap.z) : 0;
    if (d < bd) { bd = d; best = id; }
  }
  return best || any;
}
function setAuth(v, id) {
  v.auth = id; v.switchAt = Date.now(); v.at = Date.now();
  console.log(`~ ${v.id} autoridade → ${id}`);
  if (id) broadcast({ type: 'vauth', id: v.id, a: id, s: v.snap });
}

function onVillainState(ws, m) {
  const v = villains.get(m.id);
  if (!v || v.auth !== ws.pid) return;            // só a autoridade atual atualiza o vilão
  const s = cleanSnap(m.s); if (!s) return;
  const prev = v.snap; v.snap = s; v.at = Date.now();
  if (s.a === 3 && (!prev || prev.a !== 3)) {       // caiu derrotado: todos veem e são avisados
    v.defeated = true;
    broadcast({ type: 'vev', id: v.id, e: 'down', b: s.b || null });
  } else if (v.defeated && prev && prev.t === 0 && s.t !== 0 && s.a !== 3) { // saiu do esconderijo depois de derrotado
    v.defeated = false;
    broadcast({ type: 'vev', id: v.id, e: 'back' });
  }
  const msg = JSON.stringify({ type: 'vs', id: v.id, s });
  for (const c of wss.clients) if (c.readyState === 1 && c.pid !== ws.pid) { try { c.send(msg); } catch (e) {} }
}

function onVillainHit(ws, m) {
  const v = villains.get(m.id);
  if (!v || !v.auth || v.auth === ws.pid) return;   // a autoridade aplica os próprios golpes localmente
  const n = m.n;
  if (!(typeof n === 'number' && isFinite(n) && n > 0 && n <= 30)) return;
  const now = Date.now(); if (now - (ws.lastHit || 0) < 150) return; ws.lastHit = now;
  const p = players.get(ws.pid);
  if (v.snap && p && p.known && Math.hypot(p.x - v.snap.x, p.z - v.snap.z) > VIL_HIT_RANGE) return;
  const d = Array.isArray(m.dir) && fin(m.dir[0], 2) && fin(m.dir[1], 2) ? [m.dir[0], m.dir[1]] : [0, 0];
  const a = socks.get(v.auth);
  if (a) send(a, { type: 'vhit', id: v.id, n, dir: d, by: ws.pid });
}

function onVillainEvent(ws, m) {
  const v = villains.get(m.id);
  if (!v || v.auth !== ws.pid || m.e !== 'imp') return;
  if (!fin(m.x, 20000) || !fin(m.y, 20000) || !fin(m.z, 20000)) return;
  broadcast({ type: 'vev', id: v.id, e: 'imp', k: m.k === 'hit' ? 'hit' : 'block', x: m.x, y: m.y, z: m.z }, ws.pid);
}

/* revisão periódica: autoridade que saiu, que parou de responder ou que ficou longe demais */
function villainTick() {
  const now = Date.now();
  for (const v of villains.values()) {
    if (!players.size) continue;
    if (!v.auth || !players.has(v.auth)) { const n = pickAuth(v, null); if (n) setAuth(v, n); continue; }
    if (players.size < 2) continue;
    if (now - v.at > VIL_STALE) { const n = pickAuth(v, v.auth); if (n) setAuth(v, n); continue; } // app em segundo plano, sem rede…
    if (now - v.switchAt < VIL_SWITCH_MS || !v.snap) continue;
    const n = pickAuth(v, null);
    if (!n || n === v.auth) continue;
    const a = players.get(v.auth), p = players.get(n);
    if (!p.known) continue;
    const da = Math.hypot(a.x - v.snap.x, a.z - v.snap.z), dn = Math.hypot(p.x - v.snap.x, p.z - v.snap.z);
    if (dn < da * 0.6 && da - dn > 60) setAuth(v, n); // só troca se o outro jogador estiver bem mais perto
  }
}
setInterval(villainTick, VIL_MS);

/* ---------- jogadores ---------- */
wss.on('connection', (ws) => {
  if (players.size >= MAX_PLAYERS) { ws.close(4001, 'Servidor cheio (' + MAX_PLAYERS + ')'); return; }
  const id = crypto.randomBytes(4).toString('hex');
  const p = { id, x: 0, y: 0, z: 0, r: 0, g: 1, u: 0, k: 0, s: 0, known: false };
  players.set(id, p); socks.set(id, ws);
  ws.pid = id; ws.alive = true; ws.last = Date.now(); ws.win = Date.now(); ws.n = 0; ws.lastHit = 0;

  send(ws, { type: 'welcome', id, max: MAX_PLAYERS, count: players.size, players: [...players.values()].filter(q => q.known && q.id !== id).map(pub) });
  broadcast({ type: 'count', count: players.size });
  /* mundo compartilhado: quem entra recebe o vilão (e quem manda nele) do jeito que ele está agora */
  for (const v of villains.values()) if (!v.auth || !players.has(v.auth)) { v.auth = id; v.switchAt = Date.now(); v.at = Date.now(); }
  send(ws, { type: 'villains', vl: [...villains.values()].map(vpub) });
  console.log(`+ ${id} (${players.size}/${MAX_PLAYERS})`);

  ws.on('pong', () => { ws.alive = true; ws.last = Date.now(); });
  ws.on('error', (e) => console.error('ws erro', id, e.message));
  ws.on('message', (data) => {
    try {
      ws.alive = true; ws.last = Date.now();
      const now = ws.last;
      if (now - ws.win >= 1000) { ws.win = now; ws.n = 0; }
      if (++ws.n > MAX_RATE) return;                    // excesso de mensagens: descarta
      const m = JSON.parse(data.toString());
      if (!m || typeof m !== 'object') return;
      switch (m.type) {
        case 'state': {
          if (!fin(m.x, 20000) || !fin(m.y, 20000) || !fin(m.z, 20000)) return;
          p.x = m.x; p.y = m.y; p.z = m.z;
          p.r = fin(m.r, 100) ? m.r : p.r;
          p.g = m.g === 0 ? 0 : 1;
          p.u = fin(m.u, 10) ? Math.max(0, Math.round(m.u)) : 0;
          p.k = (m.k === 1 || m.k === 2) ? m.k : 0;
          p.s = fin(m.s, 200) ? Math.max(0, Math.round(m.s * 10) / 10) : 0;
          p.known = true; dirty.add(id);
          break;
        }
        case 'ping': send(ws, { type: 'pong', t: m.t }); break;
        case 'vs': onVillainState(ws, m); break;
        case 'vhit': onVillainHit(ws, m); break;
        case 'vev': onVillainEvent(ws, m); break;
      }
    } catch (e) { /* mensagem inválida nunca derruba o servidor */ }
  });
  ws.on('close', () => {
    players.delete(id); dirty.delete(id); socks.delete(id);
    broadcast({ type: 'leave', id });
    broadcast({ type: 'count', count: players.size });
    for (const v of villains.values()) if (v.auth === id) { v.auth = null; const n = pickAuth(v, id); if (n) setAuth(v, n); } // o vilão continua do último estado
    console.log(`- ${id} (${players.size}/${MAX_PLAYERS})`);
  });
});

// envio em lote (10x/s): cada cliente recebe só os jogadores que mudaram
setInterval(() => {
  if (!dirty.size) return;
  const ids = [...dirty]; dirty.clear();
  for (const c of wss.clients) {
    if (c.readyState !== 1) continue;
    const list = [];
    for (const id of ids) { if (id === c.pid) continue; const q = players.get(id); if (q && q.known) list.push(pub(q)); }
    if (list.length) send(c, { type: 'players', players: list });
  }
}, TICK_MS);

// batimento + limpeza de conexões mortas
setInterval(() => {
  const now = Date.now();
  for (const c of wss.clients) {
    if (!c.alive || now - c.last > IDLE_MS) { console.log('x derrubando', c.pid); try { c.terminate(); } catch (e) {} continue; }
    c.alive = false;
    try { c.ping(); } catch (e) {}
    send(c, { type: 'hb' });
  }
}, HB_MS);

process.on('uncaughtException', (e) => console.error('uncaughtException', e));
process.on('unhandledRejection', (e) => console.error('unhandledRejection', e));
server.listen(PORT, '0.0.0.0', () => console.log('SPEEDVERSE server na porta ' + PORT));
