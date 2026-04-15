require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const qrcode = require('qrcode');
const { Client, LocalAuth } = require('whatsapp-web.js');
const Groq = require('groq-sdk');
const path = require('path');
const fs = require('fs');

// ─── Per-session scan file helpers ────────────────────────────────────────────
function getScanFile(sessionId) {
  return path.join(__dirname, `last-scan-${sessionId}.json`);
}

function readLastScan(sessionId) {
  try {
    const file = getScanFile(sessionId);
    if (fs.existsSync(file)) return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch {}
  return null;
}

function writeLastScan(sessionId, data) {
  try {
    fs.writeFileSync(getScanFile(sessionId), JSON.stringify(data), 'utf8');
  } catch (err) {
    console.error('Failed to write scan file:', err);
  }
}

function readPendingTasks(sessionId) {
  try {
    const data = readLastScan(sessionId);
    return Array.isArray(data?.pendingTasks) ? data.pendingTasks : [];
  } catch { return []; }
}

function writePendingTasks(sessionId, tasks) {
  const data = readLastScan(sessionId) || {};
  data.pendingTasks = tasks;
  writeLastScan(sessionId, data);
}

function readDoneIds(sessionId) {
  try {
    const data = readLastScan(sessionId);
    return new Set(Array.isArray(data?.doneIds) ? data.doneIds : []);
  } catch { return new Set(); }
}

function writeDoneIds(sessionId, ids) {
  const data = readLastScan(sessionId) || {};
  data.doneIds = [...ids];
  writeLastScan(sessionId, data);
}

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

if (!process.env.GROQ_API_KEY) console.error('⚠️  GROQ_API_KEY is not set — AI analysis will fail!');
const groq = process.env.GROQ_API_KEY ? new Groq({ apiKey: process.env.GROQ_API_KEY }) : null;

// ─── Session Management ────────────────────────────────────────────────────
// sessions: Map<sessionId, { client, isReady, lastQrImage, myWhatsAppNumber, contactCache, cleanupTimer }>
const sessions = new Map();

function getOrCreateSession(sessionId) {
  if (!sessions.has(sessionId)) {
    sessions.set(sessionId, {
      client: null,
      isReady: false,
      lastQrImage: null,
      myWhatsAppNumber: null,
      contactCache: new Map(),
      cleanupTimer: null
    });
  }
  return sessions.get(sessionId);
}

function initClient(sessionId) {
  const session = getOrCreateSession(sessionId);
  if (session.client) return session; // already initialized

  const client = new Client({
    authStrategy: new LocalAuth({
      clientId: sessionId,
      dataPath: path.join(__dirname, '.wwebjs_auth')
    }),
    puppeteer: {
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
      headless: true
    }
  });

  session.client = client;

  client.on('qr', async (qr) => {
    try {
      const qrImage = await qrcode.toDataURL(qr);
      session.lastQrImage = qrImage;
      io.to(sessionId).emit('qr', { image: qrImage });
      io.to(sessionId).emit('loading', { message: 'Scan the QR code with your WhatsApp' });
      console.log(`[${sessionId.slice(0, 8)}] QR code generated`);
    } catch (err) {
      console.error('QR generation error:', err);
    }
  });

  client.on('loading_screen', (percent, message) => {
    io.to(sessionId).emit('loading', { message: `Loading: ${message} (${percent}%)` });
  });

  client.on('authenticated', () => {
    io.to(sessionId).emit('loading', { message: 'Authenticated! Finishing setup...' });
    console.log(`[${sessionId.slice(0, 8)}] Authenticated`);
  });

  client.on('auth_failure', (msg) => {
    io.to(sessionId).emit('auth_failure', { message: msg });
    console.error(`[${sessionId.slice(0, 8)}] Auth failure:`, msg);
  });

  client.on('ready', () => {
    session.isReady = true;
    try { session.myWhatsAppNumber = client.info.wid._serialized.split('@')[0]; } catch {}
    if (session.cleanupTimer) {
      clearTimeout(session.cleanupTimer);
      session.cleanupTimer = null;
    }
    io.to(sessionId).emit('ready', { status: 'connected' });
    console.log(`[${sessionId.slice(0, 8)}] Ready, number:`, session.myWhatsAppNumber);
  });

  client.on('disconnected', (reason) => {
    session.isReady = false;
    io.to(sessionId).emit('disconnected', { reason });
    console.log(`[${sessionId.slice(0, 8)}] Disconnected:`, reason);
  });

  io.to(sessionId).emit('loading', { message: 'Initializing WhatsApp...' });
  client.initialize().catch(err => {
    console.error(`[${sessionId.slice(0, 8)}] Init error:`, err);
    io.to(sessionId).emit('loading', { message: 'Initialization failed. Please restart.' });
  });

  return session;
}

// ─── Socket.IO ─────────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  const sessionId = socket.handshake.query.sessionId;
  if (!sessionId) {
    socket.emit('loading', { message: 'Error: missing session ID. Please refresh.' });
    return;
  }

  socket.join(sessionId);
  console.log(`Browser connected: session=${sessionId.slice(0, 8)}`);

  const session = sessions.get(sessionId);

  // Cancel any pending cleanup — user reconnected
  if (session?.cleanupTimer) {
    clearTimeout(session.cleanupTimer);
    session.cleanupTimer = null;
  }

  if (session?.isReady) {
    socket.emit('ready', { status: 'connected' });
  } else if (session?.lastQrImage) {
    socket.emit('qr', { image: session.lastQrImage });
    socket.emit('loading', { message: 'Scan the QR code with your WhatsApp' });
  } else if (session?.client) {
    // Client initializing — events will arrive via the room
    socket.emit('loading', { message: 'Initializing WhatsApp...' });
  } else {
    // No session yet — create and initialize
    initClient(sessionId);
  }

  socket.on('disconnect', () => {
    console.log(`Browser disconnected: session=${sessionId.slice(0, 8)}`);
    const s = sessions.get(sessionId);
    if (s && !s.isReady && s.client) {
      // Destroy unauthenticated client after 5 min if no sockets reconnect
      s.cleanupTimer = setTimeout(async () => {
        const roomSockets = await io.in(sessionId).fetchSockets();
        if (roomSockets.length === 0) {
          const still = sessions.get(sessionId);
          if (still && !still.isReady) {
            console.log(`Cleaning up unauthenticated session ${sessionId.slice(0, 8)}`);
            try { await still.client.destroy(); } catch {}
            sessions.delete(sessionId);
          }
        }
      }, 5 * 60 * 1000);
    }
  });
});

// ─── API Routes ────────────────────────────────────────────────────────────

// GET /api/status?sessionId=xxx — check if session is already authenticated
app.get('/api/status', (req, res) => {
  const { sessionId } = req.query;
  if (!sessionId) return res.status(400).json({ error: 'sessionId required' });
  const session = sessions.get(sessionId);
  if (!session) return res.json({ status: 'new' });
  if (session.isReady) return res.json({ status: 'authenticated' });
  return res.json({ status: 'disconnected' });
});

app.get('/api/scan-status', (req, res) => {
  const { sessionId } = req.query;
  if (!sessionId) return res.status(400).json({ error: 'sessionId required' });
  const lastScan = readLastScan(sessionId);
  res.json({
    isFirstScan: !lastScan || !lastScan.lastScanTime,
    lastScanTime: lastScan ? lastScan.lastScanTime : null,
    pendingCount: Array.isArray(lastScan?.pendingTasks) ? lastScan.pendingTasks.length : 0
  });
});

app.delete('/api/reset-scan', (req, res) => {
  const sessionId = req.query.sessionId || req.body?.sessionId;
  if (!sessionId) return res.status(400).json({ error: 'sessionId required' });
  try {
    const file = getScanFile(sessionId);
    if (fs.existsSync(file)) fs.unlinkSync(file);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/chats', async (req, res) => {
  const { sessionId } = req.query;
  if (!sessionId) return res.status(400).json({ error: 'sessionId required' });
  const session = sessions.get(sessionId);
  if (!session || !session.isReady) return res.status(503).json({ error: 'WhatsApp not ready' });
  try {
    const chats = await session.client.getChats();
    const result = chats
      .sort((a, b) => {
        const ta = a.lastMessage ? a.lastMessage.timestamp : 0;
        const tb = b.lastMessage ? b.lastMessage.timestamp : 0;
        return tb - ta;
      })
      .map(chat => ({
        id: chat.id._serialized,
        name: chat.name || chat.id.user,
        isGroup: chat.isGroup,
        unreadCount: chat.unreadCount || 0
      }));
    res.json(result);
  } catch (err) {
    console.error('Get chats error:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/logout — destroy client and clear session
app.post('/api/logout', async (req, res) => {
  const { sessionId } = req.body;
  if (!sessionId) return res.status(400).json({ error: 'sessionId required' });
  const session = sessions.get(sessionId);
  if (session?.client) {
    try { await session.client.logout(); } catch {}
    try { await session.client.destroy(); } catch {}
  }
  sessions.delete(sessionId);
  res.json({ success: true });
});

// POST /api/analyze — starts batched scan, streams results via socket events
app.post('/api/analyze', (req, res) => {
  const { sessionId, chatIds = [], userName = '', userDesignation = '' } = req.body;
  if (!sessionId) return res.status(400).json({ error: 'sessionId required' });
  const session = sessions.get(sessionId);
  if (!session || !session.isReady) return res.status(503).json({ error: 'WhatsApp not ready' });
  if (!userName.trim()) return res.status(400).json({ error: 'userName is required' });
  if (!chatIds.length)  return res.status(400).json({ error: 'No chats selected' });

  res.json({ started: true, totalChats: chatIds.length });
  processChatsBatched(sessionId, chatIds, userName, userDesignation).catch(err => {
    console.error('Analyze error:', err);
    io.to(sessionId).emit('analyze_error', { message: err.message });
  });
});

// POST /api/done — remove task from pending store
app.post('/api/done', async (req, res) => {
  const { sessionId, chatId, taskText, assignedBy, messageId, taskUid } = req.body;
  if (!sessionId) return res.status(400).json({ error: 'sessionId required' });
  const session = sessions.get(sessionId);
  if (!session || !session.isReady) return res.status(503).json({ error: 'WhatsApp not ready' });
  if (!chatId) return res.status(400).json({ error: 'chatId required' });

  try {
    const pending = readPendingTasks(sessionId);
    const updated = pending.filter(t => {
      if (messageId && t.messageId === messageId) return false;
      if (taskUid && t._uid === taskUid) return false;
      return true;
    });
    if (updated.length !== pending.length) writePendingTasks(sessionId, updated);

    if (messageId) {
      const doneIds = readDoneIds(sessionId);
      doneIds.add(messageId);
      writeDoneIds(sessionId, doneIds);
    }

    res.json({ success: true });
  } catch (err) {
    console.error('Done error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ─── Helpers ───────────────────────────────────────────────────────────────

function buildPrompt(userName, userDesignation, chunkText) {
  return `You are an expert WhatsApp task extractor. Your job is to find EVERY task or action item directed at "${userName}"${userDesignation.trim() ? ` (role: ${userDesignation.trim()})` : ''}.

MESSAGE FORMAT: ChatName|Sender|Date|MessageID|MessageBody
- [TAGGED] prefix means the user was directly @mentioned — ALWAYS extract as a task.
- [NAMED] prefix means the user's name appears in the message — extract ONLY if it contains a task/request for them.
- [DIRECT] prefix means this is a personal 1-on-1 chat — extract ONLY if the message contains a real task, request, reminder, or action item. Skip casual conversation, greetings, FYI messages, or acknowledgements (ok, done, noted, 👍, thanks, etc.).

EXTRACT a task if ANY of the following is true:
1. Message has [TAGGED] or [DIRECT] prefix → matchType: "tagged"
2. Message contains the name "${userName}" with any request or action → matchType: "named"
${userDesignation.trim() ? `3. Message mentions role/designation "${userDesignation.trim()}" with any action → matchType: "role"` : ''}
4. Message contains action words directed at someone (karo, bhejo, check karo, send, share, call, bata, dekho, dedo, confirm, update, fix, review, submit, upload, reply, approve, complete) and the sender seems to be asking ${userName} — matchType: "named"
5. Message has words like "please", "can you", "could you", "aap", "tumhe", "aapko", "bhai", "yaar" with any task/request → matchType: "named"

INCLUDE tasks in ANY language: English, Hindi, Hinglish, Urdu, Marathi, etc.
INCLUDE: reminders, follow-ups, deadlines, requests to share files/reports/data
INCLUDE: even gentle requests like "please have a look", "ek baar dekho", "thoda check karo"
BE INCLUSIVE — it is far better to return extra tasks than to miss any

For EACH task output EXACTLY this JSON (one object per task):
{"task":"short clear description of what needs to be done","assignedBy":"sender name","date":"date from message","originalMessage":"first 150 chars of message body","priority":"urgent|normal|someday","chatName":"chat name from the line","chatId":"","messageId":"the MessageID field value","matchType":"tagged|named|role"}

Priority: urgent=asap/urgent/today/abhi/jaldi/deadline/immediately; someday=later/kabhi/when free/no rush; normal=everything else

Return ONLY a raw JSON array — no markdown fences, no explanation, no extra text.
Return [] if truly no tasks found.

MESSAGES TO ANALYZE:
${chunkText}`;
}

async function resolveContactName(session, author, fallback) {
  if (session.contactCache.has(author)) return session.contactCache.get(author);
  const name = (await session.client.getContactById(author).catch(() => null))?.pushname || fallback;
  session.contactCache.set(author, name);
  return name;
}

// ── Groq global rate-limiter ─────────────────────────────────────────────
// Free tier: ~30 req/min. Enforce minimum 2.5 s between any two Groq calls
// so parallel lanes never collide and trigger 429s.
let _groqLastCall = 0;
const GROQ_MIN_GAP_MS = 2500;

async function callGroq(prompt, retries = 4) {
  if (!groq) throw new Error('GROQ_API_KEY is not set');
  for (let attempt = 0; attempt < retries; attempt++) {
    const gap = Date.now() - _groqLastCall;
    if (gap < GROQ_MIN_GAP_MS) {
      await new Promise(r => setTimeout(r, GROQ_MIN_GAP_MS - gap));
    }
    _groqLastCall = Date.now();

    try {
      const completion = await groq.chat.completions.create({
        model: 'llama-3.3-70b-versatile',
        messages: [{ role: 'user', content: prompt }],
        temperature: 0.1,
        max_tokens: 4096
      });
      return completion.choices[0]?.message?.content || '[]';
    } catch (err) {
      const wait = Math.pow(2, attempt + 1) * 2000; // 4s, 8s, 16s, 32s
      console.error(`Groq attempt ${attempt + 1}/${retries} failed (retry in ${wait}ms):`, err.message);
      if (attempt === retries - 1) throw err;
      await new Promise(r => setTimeout(r, wait));
    }
  }
}

function parseTasks(raw) {
  try {
    const cleaned = raw.replace(/```(?:json)?/gi, '').replace(/```/g, '').trim();
    const parsed = JSON.parse(cleaned);
    if (!Array.isArray(parsed)) {
      console.error('parseTasks: got non-array:', typeof parsed, cleaned.slice(0, 100));
      return [];
    }
    return parsed;
  } catch (e) {
    const match = raw.match(/\[[\s\S]*\]/);
    if (match) {
      try { const p = JSON.parse(match[0]); if (Array.isArray(p)) return p; } catch {}
    }
    console.error('parseTasks: JSON parse failed:', e.message, '| raw:', raw.slice(0, 300));
    return [];
  }
}

// Run fn on every item in arr with at most `lanes` running simultaneously
async function parallel(arr, lanes, fn) {
  const queue = [...arr];
  await Promise.all(
    Array.from({ length: Math.min(lanes, arr.length) }, async () => {
      while (queue.length > 0) {
        const item = queue.shift();
        if (item !== undefined) await fn(item);
      }
    })
  );
}

async function processChatsBatched(sessionId, chatIds, userName, userDesignation) {
  const session = sessions.get(sessionId);
  if (!session || !session.isReady) throw new Error('Session not ready');

  // Pipeline 1: fetch messages — MUST be 1 (sequential).
  // Pipeline 2: AI analysis — 2 Groq lanes (global rate-limiter keeps them safe)
  const FETCH_LANES   = 1;
  const GROQ_LANES    = 2;
  const totalChats    = chatIds.length;
  let   totalMessages = 0;

  // ── Time window ──────────────────────────────────────────────────────────
  const THREE_DAYS_AGO  = Math.floor(Date.now() / 1000) - 3 * 24 * 60 * 60;
  const lastScan        = readLastScan(sessionId);
  const lastScanTimeSec = lastScan?.lastScanTime
    ? Math.floor(lastScan.lastScanTime / 1000)
    : 0;
  const messageCutoff   = Math.max(lastScanTimeSec, THREE_DAYS_AGO);
  const isIncremental   = lastScanTimeSec > THREE_DAYS_AGO;

  // ── Done IDs — never re-surface tasks the user already marked done ────────
  const doneIds = readDoneIds(sessionId);

  console.log(
    `\n=== [${sessionId.slice(0, 8)}] SCAN START: ${totalChats} chats, user="${userName}", role="${userDesignation}"`,
    `| mode=${isIncremental ? 'INCREMENTAL (since ' + new Date(messageCutoff * 1000).toLocaleString() + ')' : 'FULL 3-DAY'} ===`
  );

  // ── Previous tasks: emit immediately so UI loads them right away ──────────
  const oldPending = readPendingTasks(sessionId);
  if (oldPending.length > 0) {
    const visiblePrev = oldPending.filter(t => !t.messageId || !doneIds.has(t.messageId));
    if (visiblePrev.length > 0) {
      io.to(sessionId).emit('pending_tasks', { tasks: visiblePrev.map(t => ({ ...t, fromPrevious: true })) });
    }
  }

  const allFoundTasks = [];
  const seenIds       = new Set();
  const seenPrefixes  = new Set();

  io.to(sessionId).emit('loading', { message: `Reading messages from ${totalChats} chats…` });
  io.to(sessionId).emit('scan_mode', {
    isIncremental,
    messageCutoff: messageCutoff * 1000,
    lastScanTime: lastScan?.lastScanTime || null
  });

  // ══ PIPELINE 1 — read messages directly from WhatsApp Web's in-memory store ══
  const chatDataList = [];
  let fetchedCount   = 0;

  // First collect all chat metadata (name, isGroup) without fetching messages
  const chatMeta = {};
  const allChats = await session.client.getChats().catch(() => []);
  for (const c of allChats) {
    chatMeta[c.id._serialized] = { name: c.name || c.id.user, isGroup: c.isGroup };
  }

  // Read ALL selected chats' messages from the in-memory store in ONE evaluate call.
  let storeResults = {};
  try {
    storeResults = await session.client.pupPage.evaluate((chatIds, cutoff, myNum) => {
      const out = {};
      for (const chatId of chatIds) {
        try {
          const wid  = window.Store.WidFactory.createWid(chatId);
          const chat = window.Store.Chat.get(wid);
          if (!chat) { out[chatId] = { ok: false, reason: 'not_in_store', msgs: [] }; continue; }

          const all = chat.msgs.getModelsArray ? chat.msgs.getModelsArray() : [];
          const msgs = all
            .filter(m => m.t >= cutoff && m.body && !m.isNotification && !m.isSentByMe)
            .map(m => {
              const jids = (m.mentionedJidList || []).map(j =>
                typeof j === 'string' ? j : (j._serialized || '')
              );
              const isMentioned = !!myNum && jids.some(j => j.split('@')[0] === myNum);
              return {
                id:               m.id._serialized,
                body:             m.body || '',
                timestamp:        m.t,
                author:           m.author ? m.author._serialized : null,
                isMentioned,
                mentionedJidList: jids
              };
            });
          out[chatId] = { ok: true, msgs, total: all.length };
        } catch(e) {
          out[chatId] = { ok: false, reason: e.message, msgs: [] };
        }
      }
      return out;
    }, chatIds, messageCutoff, session.myWhatsAppNumber);
  } catch(e) {
    console.error('Store evaluate failed:', e.message);
  }

  // Process each chat
  for (const chatId of chatIds) {
    const meta   = chatMeta[chatId] || { name: chatId, isGroup: false };
    const result = storeResults[chatId] || { ok: false, reason: 'no_result', msgs: [] };
    const chatDisplayName = meta.name;
    const isGroup = meta.isGroup;
    fetchedCount++;

    io.to(sessionId).emit('progress', {
      message: `Reading ${fetchedCount}/${totalChats}: "${chatDisplayName}"`,
      percent: Math.round((fetchedCount / totalChats) * 50)
    });

    if (!result.ok) {
      console.log(`  [SKIP] "${chatDisplayName}": ${result.reason}`);
      continue;
    }

    const rawMsgs = result.msgs;
    console.log(`  [FETCH] "${chatDisplayName}" (${isGroup ? 'group' : 'DM'}): ${rawMsgs.length} msgs in last 3 days`);
    if (rawMsgs.length === 0) continue;

    totalMessages += rawMsgs.length;

    const uniqueAuthors = [...new Set(rawMsgs.map(m => m.author).filter(Boolean))];
    await Promise.all(uniqueAuthors.map(a => resolveContactName(session, a, a)));

    const taggedLines     = [];
    const otherLines      = [];
    const guaranteedTasks = [];

    for (const msg of rawMsgs) {
      if (doneIds.has(msg.id)) continue;

      const date   = new Date(msg.timestamp * 1000).toLocaleDateString('en-IN',
        { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit' });
      const sender = msg.author
        ? (session.contactCache.get(msg.author) || msg.author)
        : chatDisplayName;

      const isMentioned = msg.isMentioned ||
        (!session.myWhatsAppNumber && (msg.mentionedJidList || []).length > 0);
      const nameInText  = userName.length >= 2 &&
        msg.body.toLowerCase().includes(userName.toLowerCase());
      const isDirectChat = !isGroup;

      const lower    = msg.body.toLowerCase();
      const priority = /urgent|asap|today|deadline|immediately|abhi|jaldi/.test(lower)
        ? 'urgent' : 'normal';

      if (isMentioned) {
        guaranteedTasks.push({
          task: msg.body.slice(0, 120), assignedBy: sender, date,
          originalMessage: msg.body.slice(0, 150), priority, isGroup,
          chatName: chatDisplayName, chatId, messageId: msg.id, matchType: 'tagged'
        });
        taggedLines.push({ line: `${chatDisplayName}|${sender}|${date}|${msg.id}|[TAGGED] ${msg.body.slice(0, 400)}` });
      } else if (nameInText) {
        taggedLines.push({ line: `${chatDisplayName}|${sender}|${date}|${msg.id}|[NAMED] ${msg.body.slice(0, 400)}` });
      } else if (isDirectChat) {
        otherLines.push({ line: `${chatDisplayName}|${sender}|${date}|${msg.id}|[DIRECT] ${msg.body.slice(0, 400)}` });
      } else {
        otherLines.push({ line: `${chatDisplayName}|${sender}|${date}|${msg.id}|${msg.body.slice(0, 400)}` });
      }
    }

    console.log(
      `  [FILTER] "${chatDisplayName}": guaranteed=${guaranteedTasks.length},`,
      `tagged/named=${taggedLines.length}, other=${otherLines.length}`
    );

    const lines = [...taggedLines, ...otherLines];
    if (lines.length === 0 && guaranteedTasks.length === 0) continue;

    chatDataList.push({ chatId, chatDisplayName, isGroup, lines, guaranteedTasks });
  }

  const analyzable = chatDataList.length;
  console.log(`\n=== [${sessionId.slice(0, 8)}] FETCH DONE: ${analyzable} chats have messages, totalMessages=${totalMessages} ===`);

  if (analyzable === 0) {
    io.to(sessionId).emit('tasks_batch', { tasks: [], chatsComplete: 0, totalChats, totalMessages, done: true, isIncremental });
    return;
  }

  io.to(sessionId).emit('loading', { message: `Analyzing ${analyzable} chats with AI (${GROQ_LANES} parallel lanes)…` });

  // ══ PIPELINE 2 — AI analysis (2 Groq lanes + global rate limiter) ══════
  let chatsComplete = 0;

  await parallel(chatDataList, GROQ_LANES, async ({ chatId: cid, chatDisplayName, isGroup, lines, guaranteedTasks }) => {
    let chatTasks = [...(guaranteedTasks || [])];
    const guaranteedMsgIds = new Set(chatTasks.map(t => t.messageId).filter(Boolean));

    if (lines.length > 0) {
      const CHUNK_LIMIT = 15000;
      const chunks = [];
      let current  = '';
      for (const item of lines) {
        if (current.length + item.line.length + 1 > CHUNK_LIMIT) {
          chunks.push(current);
          current = item.line + '\n';
        } else {
          current += item.line + '\n';
        }
      }
      if (current.trim()) chunks.push(current);

      console.log(`  [AI] "${chatDisplayName}": ${lines.length} lines → ${chunks.length} chunk(s), guaranteed=${chatTasks.length}`);

      for (let ci = 0; ci < chunks.length; ci++) {
        try {
          const raw    = await callGroq(buildPrompt(userName, userDesignation, chunks[ci]));
          console.log(`    chunk ${ci + 1}/${chunks.length} raw (first 200): ${raw.slice(0, 200)}`);
          const parsed = parseTasks(raw);
          console.log(`    chunk ${ci + 1}/${chunks.length} parsed: ${parsed.length} tasks`);
          for (const t of parsed) {
            t.chatId = cid;
            t.isGroup = isGroup;
            if (!t.messageId || !guaranteedMsgIds.has(t.messageId)) {
              chatTasks.push(t);
            }
          }
        } catch (err) {
          console.error(`  [AI ERROR] "${chatDisplayName}" chunk ${ci + 1}:`, err.message);
        }
      }
    } else {
      console.log(`  [AI SKIP] "${chatDisplayName}": no non-guaranteed lines, ${chatTasks.length} guaranteed tasks`);
    }

    // Deduplicate + filter done
    chatTasks = chatTasks.filter(t => {
      if (t.messageId && doneIds.has(t.messageId)) return false;
      if (t.messageId) {
        if (seenIds.has(t.messageId)) return false;
        seenIds.add(t.messageId);
      }
      const pfx = (t.task || '').trim().slice(0, 30).toLowerCase();
      if (pfx && seenPrefixes.has(pfx)) return false;
      if (pfx) seenPrefixes.add(pfx);
      return true;
    });

    console.log(`  [DONE] "${chatDisplayName}": ${chatTasks.length} tasks (guaranteed + AI, after dedup)`);

    allFoundTasks.push(...chatTasks);
    chatsComplete++;
    const done = chatsComplete === analyzable;

    io.to(sessionId).emit('progress', {
      message: `AI: ${chatsComplete}/${analyzable} chats — "${chatDisplayName}"`,
      percent: 50 + Math.round((chatsComplete / analyzable) * 50)
    });
    io.to(sessionId).emit('tasks_batch', {
      tasks: chatTasks,
      chatsComplete,
      totalChats: analyzable,
      totalMessages,
      done,
      isIncremental
    });
  });

  console.log(`\n=== [${sessionId.slice(0, 8)}] SCAN COMPLETE: ${allFoundTasks.length} total tasks found ===\n`);

  // ── Persist: merge new tasks with old incomplete ones ─────────────────────
  const newMsgIds   = new Set(allFoundTasks.map(t => t.messageId).filter(Boolean));
  const carriedOver = oldPending.filter(t => {
    if (t.messageId && doneIds.has(t.messageId)) return false;
    if (t.messageId && newMsgIds.has(t.messageId)) return false;
    return true;
  });

  writeLastScan(sessionId, {
    lastScanTime: Date.now(),
    chatTimes:    {},
    doneIds:      [...doneIds],
    pendingTasks: [
      ...allFoundTasks.map(t => ({ ...t, _savedAt: Date.now() })),
      ...carriedOver
    ]
  });
}

const PORT = process.env.PORT || 3002;
server.listen(PORT, () => {
  console.log(`\n🚀 WhatsApp Task Finder running on http://localhost:${PORT}\n`);
});
