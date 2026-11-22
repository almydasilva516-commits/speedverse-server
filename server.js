/* SPEEDVERSE — servidor WebSocket (até 50 jogadores)
   Protocolo (JSON em TEXTO):
     cliente → servidor  {type:"state", x,y,z, r, g, u, k}
     servidor → cliente  {type:"welcome", id, max, players:[{id,x,y,z,r,g,u,k}]}   ao conectar
                         {type:"players", players:[{id,x,y,z,r,g,u,k}, ...]}        10x/s, só quem mudou
                         {type:"leave", id}  ·  {type:"count", count}  ·  {type:"hb"} a cada 15 s
   Render: Start Command "node server.js" · escuta process.env.PORT */
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = 50;
const TICK_MS = 100;          // envio em lote: cada cliente recebe no máx. 10 pacotes/s
const HB_MS = 15000;          // batimento: mantém a conexão viva no Render e derruba conexões mortas
const IDLE_MS = 30000;        // sem nenhuma mensagem do cliente (o cliente envia a cada 3 s)
const MAX_RATE = 40;          // mensagens por segundo aceitas de um mesmo cliente

const players = new Map();    // id -> estado
const dirty = new Set();

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end(`SPEEDVERSE server OK · jogadores: ${players.size}/${MAX_PLAYERS}\n`);
});
const wss = new WebSocketServer({ server, maxPayload: 2048, perMessageDeflate: false });

const send = (ws, obj) => { if (ws.readyState === 1) { try { ws.send(JSON.stringify(obj)); } catch (e) {} } }; // sempre TEXTO
const broadcast = (obj, except) => { const s = JSON.stringify(obj); for (const c of wss.clients) if (c !== except && c.readyState === 1) { try { c.send(s); } catch (e) {} } };
const pub = (p) => ({ id: p.id, x: p.x, y: p.y, z: p.z, r: p.r, g: p.g, u: p.u, k: p.k });
const fin = (v, lim) => (typeof v === 'number' && isFinite(v) && Math.abs(v) <= lim);

wss.on('connection', (ws) => {
  if (players.size >= MAX_PLAYERS) { ws.close(4001, 'Servidor cheio (50/50)'); return; }
  const id = crypto.randomBytes(4).toString('hex');
  const p = { id, x: 0, y: 0, z: 0, r: 0, g: 1, u: 0, k: 0, known: false };
  players.set(id, p);
  ws.pid = id; ws.alive = true; ws.last = Date.now(); ws.win = Date.now(); ws.cnt = 0;

  send(ws, { type: 'welcome', id, max: MAX_PLAYERS, players: [...players.values()].filter(q => q.known && q.id !== id).map(pub) });
  broadcast({ type: 'count', count: players.size });
  console.log(`+ ${id} (${players.size}/${MAX_PLAYERS})`);

  ws.on('pong', () => { ws.alive = true; ws.last = Date.now(); });
  ws.on('error', (e) => console.error('ws erro', id, e.message));
  ws.on('message', (data, isBinary) => {
    try {
      ws.alive = true; ws.last = Date.now();
      const now = Date.now();
      if (now - ws.win > 1000) { ws.win = now; ws.cnt = 0; }
      if (++ws.cnt > MAX_RATE) return;                     // limite de taxa: ignora excesso, não derruba
      if (isBinary) return;
      const m = JSON.parse(data.toString());
      if (!m || typeof m !== 'object') return;
      if (m.type === 'state') {
        if (!fin(m.x, 20000) || !fin(m.y, 20000) || !fin(m.z, 20000)) return;
        p.x = m.x; p.y = m.y; p.z = m.z;
        p.r = fin(m.r, 100) ? m.r : p.r;
        p.g = m.g === 0 ? 0 : 1;
        p.u = fin(m.u, 10) ? Math.max(0, Math.round(m.u)) : 0;
        p.k = (m.k === 1 || m.k === 2) ? m.k : 0;
        p.known = true; dirty.add(id);
      } else if (m.type === 'ping') {
        send(ws, { type: 'pong', t: m.t });
      }
    } catch (e) { /* mensagem inválida nunca derruba o servidor */ }
  });
  ws.on('close', () => {
    players.delete(id); dirty.delete(id);
    broadcast({ type: 'leave', id });
    broadcast({ type: 'count', count: players.size });
    console.log(`- ${id} (${players.size}/${MAX_PLAYERS})`);
  });
});

// envio em lote (10x/s): cada cliente recebe só os jogadores que mudaram, sem ele mesmo
setInterval(() => {
  if (!dirty.size) return;
  const ids = [...dirty]; dirty.clear();
  for (const c of wss.clients) {
    if (c.readyState !== 1) continue;
    const list = [];
    for (const id of ids) { if (id === c.pid) continue; const q = players.get(id); if (q) list.push(pub(q)); }
    if (list.length) send(c, { type: 'players', players: list });
  }
}, TICK_MS);

// batimento + limpeza de conexões mortas
setInterval(() => {
  const now = Date.now();
  for (const c of wss.clients) {
    if (!c.alive || now - c.last > IDLE_MS) { console.log('x derrubando conexão morta', c.pid); c.terminate(); continue; }
    c.alive = false;
    try { c.ping(); } catch (e) {}
    send(c, { type: 'hb' });
  }
}, HB_MS);

process.on('uncaughtException', (e) => console.error('uncaughtException', e));
process.on('unhandledRejection', (e) => console.error('unhandledRejection', e));
server.listen(PORT, '0.0.0.0', () => console.log('SPEEDVERSE server na porta ' + PORT));
