/**
 * FTT Signal Worker v4.1 — Multi-Timeframe Binary Trading (Candle-Based Expiry)
 *
 * ✅ Each timeframe shows its OWN candle-based duration:
 *    1min  → X candles × 1min  = Y minutes
 *    5min  → X candles × 5min  = Y minutes
 *    15min → X candles × 15min = Y minutes
 *
 * ✅ All 15+ indicators per timeframe
 * ✅ Best timeframe recommendation
 * ✅ Per-TF entry/expiry details
 * ✅ Multi-TF alignment scoring
 * ✅ Signal quality grading (A/B/C/D)
 * ✅ Market condition detection
 * ✅ Candlestick patterns + Divergence detection
 * ✅ KV caching + Rate limiting + API key rotation
 */

// ============================================
// CONFIG
// ============================================

const CONFIG = {
  API_BASE_URL: 'https://api.twelvedata.com',
  REFRESH_INTERVAL: 60000,
  REQUEST_TIMEOUT: 12000,
  MAX_RETRIES: 3,

  MIN_SCORE_THRESHOLD: 3.5,
  MIN_CONFLUENCE: 3,

  CACHE_TTL: {
    '1min': 60,
    '5min': 300,
    '15min': 900,
  },

  RATE_LIMIT_MAX_REQUESTS: 30,
  RATE_LIMIT_WINDOW_SECONDS: 60,

  // Indicator periods
  ATR_PERIOD: 14,
  RSI_PERIOD: 14,
  STOCH_PERIOD: 14,
  STOCH_SMOOTH_K: 3,
  STOCH_SMOOTH_D: 3,
  ADX_PERIOD: 14,
  CCI_PERIOD: 20,
  MFI_PERIOD: 14,
  WILLIAMS_PERIOD: 14,
  BB_PERIOD: 20,
  BB_STD_DEV: 2,

  DIVERGENCE_LOOKBACK: 30,
  DIVERGENCE_MIN_BARS: 5,
};

// ── Candle duration in minutes per timeframe ──
const CANDLE_MINUTES = {
  '1min': 1,
  '5min': 5,
  '15min': 15,
};

// ── Duration ranges per timeframe (in candles) ──
const DURATION_CONFIG = {
  '1min':  { base: 5, min: 2, max: 15 },  // 2–15 candles → 2–15 min
  '5min':  { base: 3, min: 1, max: 8 },   // 1–8 candles  → 5–40 min
  '15min': { base: 2, min: 1, max: 4 },   // 1–4 candles  → 15–60 min
};

const TIMEFRAME_MAP = {
  '1min': '1min', '5min': '5min', '15min': '15min',
  '1m': '1min', '5m': '5min', '15m': '15min',
};

const VALID_CURRENCIES = [
  'EUR','USD','GBP','JPY','AUD','NZD',
  'CAD','CHF','HKD','SGD','SEK','NOK',
  'DKK','ZAR','MXN','TRY','BRL','INR',
];

// ============================================
// MAIN HANDLER
// ============================================

export default {
  async fetch(request, env, ctx) {
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization',
      'Access-Control-Max-Age': '86400',
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders });
    }

    try {
      const url = new URL(request.url);
      const path = url.pathname;

      if (path === '/api/signal' || path === '/signal') {
        const rl = await checkRateLimit(request, env);
        if (rl) return applyCors(rl, corsHeaders);
      }

      let response;

      if (path === '/' || path === '/health') {
        response = handleHealth(env);
      } else if (path === '/api/signal' || path === '/signal') {
        const rawPair = url.searchParams.get('pair') || 'EUR/USD';
        const pair = sanitizePair(rawPair);
        if (!pair) {
          response = jsonResponse({
            error: true,
            message: `Invalid pair: "${rawPair}". Use EUR/USD or EURUSD`,
            validCurrencies: VALID_CURRENCIES,
          }, 400);
        } else {
          response = await handleSignal(pair, env, ctx);
        }
      } else if (path === '/api/pairs') {
        response = handlePairs();
      } else {
        response = jsonResponse({
          status: 'ok',
          message: 'FTT Signal Worker v4.1 — Multi-Timeframe',
          endpoints: { health:'/', signal:'/api/signal?pair=EUR/USD', pairs:'/api/pairs' },
          timestamp: new Date().toISOString(),
        });
      }

      return applyCors(response, corsHeaders);
    } catch (error) {
      console.error('Fatal:', error);
      return applyCors(jsonResponse({ error: true, message: 'Internal server error' }, 500), corsHeaders);
    }
  },
};

// ============================================
// CORS
// ============================================

function applyCors(response, corsHeaders) {
  const h = new Headers(response.headers);
  for (const [k, v] of Object.entries(corsHeaders)) h.set(k, v);
  return new Response(response.body, { status: response.status, statusText: response.statusText, headers: h });
}

// ============================================
// RATE LIMITING
// ============================================

async function checkRateLimit(request, env) {
  const ip = request.headers.get('CF-Connecting-IP') || 'unknown';

  if (env.RATE_LIMITER) {
    try {
      const { success } = await env.RATE_LIMITER.limit({ key: ip });
      if (!success) return jsonResponse({ error: true, message: 'Rate limit exceeded.', retryAfter: CONFIG.RATE_LIMIT_WINDOW_SECONDS }, 429);
      return null;
    } catch (e) { console.warn('Rate limiter err:', e.message); }
  }

  if (env.SIGNAL_CACHE) {
    try {
      const kvKey = `rl:${ip}`;
      const now = Math.floor(Date.now() / 1000);
      const stored = await env.SIGNAL_CACHE.get(kvKey, 'json');
      let reqs = (stored && Array.isArray(stored)) ? stored.filter(t => t > now - CONFIG.RATE_LIMIT_WINDOW_SECONDS) : [];
      if (reqs.length >= CONFIG.RATE_LIMIT_MAX_REQUESTS) {
        return jsonResponse({ error: true, message: 'Rate limit exceeded.', retryAfter: CONFIG.RATE_LIMIT_WINDOW_SECONDS }, 429);
      }
      reqs.push(now);
      await env.SIGNAL_CACHE.put(kvKey, JSON.stringify(reqs), { expirationTtl: CONFIG.RATE_LIMIT_WINDOW_SECONDS + 10 });
      return null;
    } catch (e) { console.warn('KV RL err:', e.message); return null; }
  }
  return null;
}

// ============================================
// INPUT SANITIZATION
// ============================================

function sanitizePair(input) {
  if (!input || typeof input !== 'string') return null;
  const c = input.replace(/[^A-Za-z/]/g, '').toUpperCase();
  if (/^[A-Z]{3}\/[A-Z]{3}$/.test(c)) {
    const b = c.slice(0,3), q = c.slice(4,7);
    if (VALID_CURRENCIES.includes(b) && VALID_CURRENCIES.includes(q) && b !== q) return c;
  }
  if (/^[A-Z]{6}$/.test(c)) {
    const b = c.slice(0,3), q = c.slice(3,6);
    if (VALID_CURRENCIES.includes(b) && VALID_CURRENCIES.includes(q) && b !== q) return `${b}/${q}`;
  }
  return null;
}

// ============================================
// HANDLERS
// ============================================

function handleHealth(env) {
  const keyCount = getApiKeys(env).length;
  return jsonResponse({
    status: 'healthy', version: '4.1.0',
    timestamp: new Date().toISOString(),
    apiKeys: { configured: keyCount, status: keyCount > 0 ? 'ready' : 'NO KEYS' },
    bindings: {
      kvCache: env.SIGNAL_CACHE ? 'ready' : 'NOT CONFIGURED',
      rateLimiter: env.RATE_LIMITER ? 'ready' : 'KV fallback',
    },
    indicators: [
      'EMA(5/10/20)','SMA(50)','RSI(14)','MACD(12,26,9)',
      'Stochastic(14,3,3)','ADX(14)+DI','Williams%R(14)',
      'CCI(20)','MFI(14)','ATR(14)','Bollinger(20,2)',
      'PivotPoints','CandlestickPatterns','RSI/MACD Divergence',
    ],
    multiTimeframe: {
      '1min': 'Duration in 1-min candles (1–15 min)',
      '5min': 'Duration in 5-min candles (5–40 min)',
      '15min': 'Duration in 15-min candles (15–60 min)',
    },
  });
}

function handlePairs() {
  const pairs = [];
  for (const b of VALID_CURRENCIES) for (const q of VALID_CURRENCIES) if (b !== q) pairs.push(`${b}/${q}`);
  return jsonResponse({ count: pairs.length, currencies: VALID_CURRENCIES, examplePairs: pairs.slice(0,20) });
}

async function handleSignal(pair, env, ctx) {
  const timeframes = ['1min', '5min', '15min'];
  const candleData = {};
  const errors = {};
  let totalFailures = 0, cacheHits = 0;

  for (const tf of timeframes) {
    const data = await fetchCandlesWithCache(pair, tf, 100, env, ctx);
    if (data.error) { errors[tf] = data.error; totalFailures++; }
    else { if (data._fromCache) cacheHits++; candleData[tf] = data.candles || data; }
  }

  if (totalFailures === timeframes.length) {
    return jsonResponse({
      pair, signal: generateDummySignal(pair),
      source: 'DUMMY_FALLBACK', errors, timestamp: new Date().toISOString(),
    });
  }

  const signal = buildMultiTimeframeSignal(candleData, pair);

  return jsonResponse({
    pair, signal,
    source: totalFailures > 0 ? 'PARTIAL_DATA' : 'FULL_DATA',
    timestamp: new Date().toISOString(),
    nextRefresh: new Date(Date.now() + CONFIG.REFRESH_INTERVAL).toISOString(),
    cacheHits,
    dataStatus: Object.fromEntries(timeframes.map(tf => [
      tf, candleData[tf] ? `${candleData[tf].length} candles` : `FAILED: ${errors[tf]}`
    ])),
  });
}

// ============================================
// API KEYS
// ============================================

function getApiKeys(env) {
  const keys = [];
  for (let i = 1; i <= 10; i++) {
    const k = env[`TWELVEDATA_API_KEY_${i}`];
    if (k && typeof k === 'string' && k.trim().length > 0) keys.push(k.trim());
  }
  if (keys.length === 0 && env.TWELVEDATA_API_KEY) keys.push(env.TWELVEDATA_API_KEY.trim());
  return keys;
}

// ============================================
// KV CACHING
// ============================================

async function fetchCandlesWithCache(pair, tf, limit, env, ctx) {
  const cacheKey = `c:${pair}:${tf}:${limit}`;
  const ttl = CONFIG.CACHE_TTL[tf] || 60;

  if (env.SIGNAL_CACHE) {
    try {
      const cached = await env.SIGNAL_CACHE.get(cacheKey, 'json');
      if (cached && Array.isArray(cached) && cached.length > 0) return { candles: cached, _fromCache: true };
    } catch (e) { console.warn('Cache read err:', e.message); }
  }

  const result = await fetchCandles(pair, tf, limit, env);
  if (result.error) return result;

  if (env.SIGNAL_CACHE && ctx && Array.isArray(result) && result.length > 0) {
    ctx.waitUntil(
      env.SIGNAL_CACHE.put(cacheKey, JSON.stringify(result), { expirationTtl: Math.max(60, ttl) })
        .catch(e => console.warn('Cache write err:', e.message))
    );
  }
  return { candles: result, _fromCache: false };
}

// ============================================
// DATA FETCHING
// ============================================

async function fetchCandles(pair, tf, limit, env) {
  const apiKeys = getApiKeys(env);
  if (apiKeys.length === 0) return { error: 'No API keys configured.' };

  const symbol = pair.includes('/') ? pair : `${pair.slice(0,3)}/${pair.slice(3)}`;
  const interval = TIMEFRAME_MAP[tf] || tf;
  const maxAttempts = Math.min(CONFIG.MAX_RETRIES, apiKeys.length);
  const startIdx = Math.floor(Date.now() / 1000) % apiKeys.length;
  let lastError = '';

  for (let a = 0; a < maxAttempts; a++) {
    const ki = (startIdx + a) % apiKeys.length;
    try {
      const u = new URL('/time_series', CONFIG.API_BASE_URL);
      u.searchParams.set('symbol', symbol);
      u.searchParams.set('interval', interval);
      u.searchParams.set('outputsize', String(limit));
      u.searchParams.set('apikey', apiKeys[ki]);
      u.searchParams.set('format', 'JSON');

      const res = await fetch(u.toString(), {
        signal: AbortSignal.timeout(CONFIG.REQUEST_TIMEOUT),
        headers: { Accept: 'application/json' },
      });

      if (!res.ok) {
        if (res.status === 429) { lastError = 'TwelveData rate limited'; continue; }
        lastError = `HTTP ${res.status}`;
        continue;
      }

      const data = await res.json();
      if (data.status === 'error') { lastError = data.message || 'API error'; continue; }
      if (!data.values || !Array.isArray(data.values) || data.values.length === 0) { lastError = 'No data'; continue; }

      const candles = data.values.map(c => ({
        datetime: c.datetime,
        open: parseFloat(c.open), high: parseFloat(c.high),
        low: parseFloat(c.low), close: parseFloat(c.close),
        volume: parseFloat(c.volume || 0),
      })).reverse();

      if (!candles.every(c => isFinite(c.open) && isFinite(c.high) && isFinite(c.low) && isFinite(c.close))) {
        lastError = 'Invalid data'; continue;
      }
      return candles;
    } catch (e) {
      lastError = e.name === 'TimeoutError' ? 'Timeout' : e.message;
      continue;
    }
  }
  return { error: `All ${maxAttempts} attempts failed: ${lastError}` };
}

// =====================================================================
//  TECHNICAL INDICATORS LIBRARY
// =====================================================================

function calculateSMA(data, period) {
  if (!data || data.length < period) return new Array(data ? data.length : 0).fill(null);
  const r = new Array(period - 1).fill(null);
  let s = 0;
  for (let i = 0; i < period; i++) s += data[i];
  r.push(s / period);
  for (let i = period; i < data.length; i++) { s += data[i] - data[i - period]; r.push(s / period); }
  return r;
}

function calculateEMA(data, period) {
  if (!data || data.length === 0) return [];
  if (data.length < period) return new Array(data.length).fill(null);
  const k = 2 / (period + 1);
  const r = new Array(period - 1).fill(null);
  let s = 0;
  for (let i = 0; i < period; i++) s += data[i];
  let ema = s / period;
  r.push(ema);
  for (let i = period; i < data.length; i++) { ema = data[i] * k + ema * (1 - k); r.push(ema); }
  return r;
}

function calculateRSI(data, period = 14) {
  if (!data || data.length < period + 1) return new Array(data ? data.length : 0).fill(null);
  const ch = [];
  for (let i = 1; i < data.length; i++) ch.push(data[i] - data[i - 1]);
  let ag = 0, al = 0;
  for (let i = 0; i < period; i++) { if (ch[i] > 0) ag += ch[i]; else al += Math.abs(ch[i]); }
  ag /= period; al /= period;
  const rsi = [al === 0 ? 100 : 100 - 100 / (1 + ag / al)];
  for (let i = period; i < ch.length; i++) {
    const g = ch[i] > 0 ? ch[i] : 0, l = ch[i] < 0 ? Math.abs(ch[i]) : 0;
    ag = (ag * (period - 1) + g) / period;
    al = (al * (period - 1) + l) / period;
    rsi.push(al === 0 ? 100 : 100 - 100 / (1 + ag / al));
  }
  return new Array(data.length - rsi.length).fill(null).concat(rsi);
}

function calculateMACD(data) {
  if (!data || data.length === 0) return { macdLine: [], signalLine: [], histogram: [] };
  const e12 = calculateEMA(data, 12), e26 = calculateEMA(data, 26);
  const ml = e12.map((v, i) => (v === null || e26[i] === null) ? null : v - e26[i]);
  const vals = [], idxs = [];
  ml.forEach((v, i) => { if (v !== null) { vals.push(v); idxs.push(i); } });
  const se = calculateEMA(vals, 9);
  const sl = new Array(ml.length).fill(null);
  idxs.forEach((idx, j) => { sl[idx] = se[j]; });
  const hist = ml.map((v, i) => (v === null || sl[i] === null) ? null : v - sl[i]);
  return { macdLine: ml, signalLine: sl, histogram: hist };
}

function calculateATR(candles, period = 14) {
  if (!candles || candles.length < period + 1) return new Array(candles ? candles.length : 0).fill(null);
  const tr = [null];
  for (let i = 1; i < candles.length; i++) {
    const h = candles[i].high, l = candles[i].low, pc = candles[i - 1].close;
    tr.push(Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc)));
  }
  let s = 0;
  for (let i = 1; i <= period; i++) s += tr[i];
  let atr = s / period;
  const r = new Array(period).fill(null);
  r.push(atr);
  for (let i = period + 1; i < candles.length; i++) { atr = (atr * (period - 1) + tr[i]) / period; r.push(atr); }
  return r;
}

function calculateBollingerBands(data, period = 20, mult = 2) {
  if (!data || data.length === 0) return { upper:[], middle:[], lower:[], bandwidth:[], percentB:[] };
  const n = data.length;
  const u = new Array(n).fill(null), m = new Array(n).fill(null), l = new Array(n).fill(null);
  const bw = new Array(n).fill(null), pb = new Array(n).fill(null);
  for (let i = period - 1; i < n; i++) {
    let s = 0;
    for (let j = i - period + 1; j <= i; j++) s += data[j];
    const sma = s / period;
    let sq = 0;
    for (let j = i - period + 1; j <= i; j++) sq += (data[j] - sma) ** 2;
    const sd = Math.sqrt(sq / period);
    m[i] = sma; u[i] = sma + mult * sd; l[i] = sma - mult * sd;
    bw[i] = sma > 0 ? ((u[i] - l[i]) / sma) * 100 : 0;
    const rng = u[i] - l[i];
    pb[i] = rng > 0 ? (data[i] - l[i]) / rng : 0.5;
  }
  return { upper: u, middle: m, lower: l, bandwidth: bw, percentB: pb };
}

function calculateStochastic(candles, kP = 14, sK = 3, sD = 3) {
  if (!candles || candles.length < kP) return { k: new Array(candles ? candles.length : 0).fill(null), d: [] };
  const rawK = new Array(kP - 1).fill(null);
  for (let i = kP - 1; i < candles.length; i++) {
    let hi = -Infinity, lo = Infinity;
    for (let j = i - kP + 1; j <= i; j++) { if (candles[j].high > hi) hi = candles[j].high; if (candles[j].low < lo) lo = candles[j].low; }
    const rng = hi - lo;
    rawK.push(rng > 0 ? ((candles[i].close - lo) / rng) * 100 : 50);
  }
  const k = calculateSMA(rawK.map(v => v === null ? 0 : v), sK);
  for (let i = 0; i < kP - 1; i++) k[i] = null;
  const d = calculateSMA(k.map(v => v === null ? 0 : v), sD);
  for (let i = 0; i < kP - 1 + sK - 1; i++) if (i < d.length) d[i] = null;
  return { k, d };
}

function calculateADX(candles, period = 14) {
  const n = candles ? candles.length : 0;
  if (n < period * 2 + 1) return { adx: new Array(n).fill(null), plusDI: new Array(n).fill(null), minusDI: new Array(n).fill(null) };

  const pDM = [0], mDM = [0], tr = [0];
  for (let i = 1; i < n; i++) {
    const up = candles[i].high - candles[i-1].high, dn = candles[i-1].low - candles[i].low;
    pDM.push(up > dn && up > 0 ? up : 0);
    mDM.push(dn > up && dn > 0 ? dn : 0);
    const h = candles[i].high, l = candles[i].low, pc = candles[i-1].close;
    tr.push(Math.max(h-l, Math.abs(h-pc), Math.abs(l-pc)));
  }

  function ws(arr, p) {
    const r = new Array(arr.length).fill(null);
    let s = 0; for (let i = 1; i <= p; i++) s += arr[i]; r[p] = s;
    for (let i = p+1; i < arr.length; i++) r[i] = r[i-1] - r[i-1]/p + arr[i];
    return r;
  }

  const sTR = ws(tr, period), sPDM = ws(pDM, period), sMDM = ws(mDM, period);
  const plusDI = new Array(n).fill(null), minusDI = new Array(n).fill(null), dx = new Array(n).fill(null);

  for (let i = period; i < n; i++) {
    if (sTR[i] && sTR[i] > 0) {
      plusDI[i] = (sPDM[i] / sTR[i]) * 100;
      minusDI[i] = (sMDM[i] / sTR[i]) * 100;
      const ds = plusDI[i] + minusDI[i];
      dx[i] = ds > 0 ? (Math.abs(plusDI[i] - minusDI[i]) / ds) * 100 : 0;
    }
  }

  const adx = new Array(n).fill(null);
  let adxS = 0, adxC = 0, adxI = -1;
  for (let i = period; i < n; i++) {
    if (dx[i] !== null) { adxS += dx[i]; adxC++; if (adxC === period) { adx[i] = adxS / period; adxI = i; break; } }
  }
  if (adxI > 0) for (let i = adxI + 1; i < n; i++) { if (dx[i] !== null && adx[i-1] !== null) adx[i] = (adx[i-1] * (period-1) + dx[i]) / period; }

  return { adx, plusDI, minusDI };
}

function calculateWilliamsR(candles, period = 14) {
  if (!candles || candles.length < period) return new Array(candles ? candles.length : 0).fill(null);
  const r = new Array(period - 1).fill(null);
  for (let i = period - 1; i < candles.length; i++) {
    let hi = -Infinity, lo = Infinity;
    for (let j = i - period + 1; j <= i; j++) { if (candles[j].high > hi) hi = candles[j].high; if (candles[j].low < lo) lo = candles[j].low; }
    const rng = hi - lo;
    r.push(rng > 0 ? ((hi - candles[i].close) / rng) * -100 : -50);
  }
  return r;
}

function calculateCCI(candles, period = 20) {
  if (!candles || candles.length < period) return new Array(candles ? candles.length : 0).fill(null);
  const tp = candles.map(c => (c.high + c.low + c.close) / 3);
  const r = new Array(period - 1).fill(null);
  for (let i = period - 1; i < tp.length; i++) {
    let s = 0; for (let j = i - period + 1; j <= i; j++) s += tp[j]; const mean = s / period;
    let mad = 0; for (let j = i - period + 1; j <= i; j++) mad += Math.abs(tp[j] - mean); mad /= period;
    r.push(mad > 0 ? (tp[i] - mean) / (0.015 * mad) : 0);
  }
  return r;
}

function calculateMFI(candles, period = 14) {
  if (!candles || candles.length < period + 1) return new Array(candles ? candles.length : 0).fill(null);
  const tp = candles.map(c => (c.high + c.low + c.close) / 3);
  const mf = candles.map((c, i) => tp[i] * c.volume);
  const r = new Array(period).fill(null);
  for (let i = period; i < candles.length; i++) {
    let pos = 0, neg = 0;
    for (let j = i - period + 1; j <= i; j++) {
      if (tp[j] > tp[j-1]) pos += mf[j]; else if (tp[j] < tp[j-1]) neg += mf[j];
    }
    r.push(neg > 0 ? 100 - 100 / (1 + pos / neg) : 100);
  }
  return r;
}

function calculatePivotPoints(candles) {
  if (!candles || candles.length < 2) return { pivot:null, r1:null, r2:null, r3:null, s1:null, s2:null, s3:null };
  const lb = Math.min(20, candles.length - 1);
  const sc = candles.slice(-lb - 1, -1);
  let sh = -Infinity, sl = Infinity;
  const scl = sc[sc.length - 1].close;
  for (const c of sc) { if (c.high > sh) sh = c.high; if (c.low < sl) sl = c.low; }
  const p = (sh + sl + scl) / 3, rng = sh - sl;
  return { pivot: p, r1: 2*p - sl, r2: p + rng, r3: sh + 2*(p - sl), s1: 2*p - sh, s2: p - rng, s3: sl - 2*(sh - p) };
}

// ─── Candlestick Pattern Detection ───
function detectCandlestickPatterns(candles) {
  const patterns = [];
  if (!candles || candles.length < 3) return patterns;
  const n = candles.length;
  const c0 = candles[n-1], c1 = candles[n-2], c2 = candles[n-3];
  const b0 = c0.close - c0.open, b1 = c1.close - c1.open, b2 = c2.close - c2.open;
  const ab0 = Math.abs(b0), ab1 = Math.abs(b1);
  const r0 = c0.high - c0.low || 0.00001, r1 = c1.high - c1.low || 0.00001;
  const bp0 = ab0 / r0, bp1 = ab1 / r1;
  const uw0 = c0.high - Math.max(c0.open, c0.close);
  const lw0 = Math.min(c0.open, c0.close) - c0.low;

  if (b1 < 0 && b0 > 0 && c0.open <= c1.close && c0.close >= c1.open && ab0 > ab1)
    patterns.push({ name: 'BULLISH_ENGULFING', direction: 'BUY', strength: 2.0 });
  if (b1 > 0 && b0 < 0 && c0.open >= c1.close && c0.close <= c1.open && ab0 > ab1)
    patterns.push({ name: 'BEARISH_ENGULFING', direction: 'SELL', strength: 2.0 });
  if (bp0 < 0.35 && lw0 > ab0 * 2 && uw0 < ab0 * 0.5)
    patterns.push({ name: 'HAMMER', direction: 'BUY', strength: 1.5 });
  if (bp0 < 0.35 && uw0 > ab0 * 2 && lw0 < ab0 * 0.5)
    patterns.push({ name: 'SHOOTING_STAR', direction: 'SELL', strength: 1.5 });
  if (bp0 < 0.1)
    patterns.push({ name: 'DOJI', direction: 'NEUTRAL', strength: 0.5 });
  if (lw0 > r0 * 0.6 && uw0 < r0 * 0.15 && bp0 < 0.3)
    patterns.push({ name: 'PIN_BAR_BULLISH', direction: 'BUY', strength: 1.8 });
  if (uw0 > r0 * 0.6 && lw0 < r0 * 0.15 && bp0 < 0.3)
    patterns.push({ name: 'PIN_BAR_BEARISH', direction: 'SELL', strength: 1.8 });

  const r2 = c2.high - c2.low || 0.00001;
  if (b2 < 0 && Math.abs(b2)/r2 > 0.5 && bp1 < 0.2 && b0 > 0 && bp0 > 0.5 && c0.close > (c2.open+c2.close)/2)
    patterns.push({ name: 'MORNING_STAR', direction: 'BUY', strength: 2.5 });
  if (b2 > 0 && Math.abs(b2)/r2 > 0.5 && bp1 < 0.2 && b0 < 0 && bp0 > 0.5 && c0.close < (c2.open+c2.close)/2)
    patterns.push({ name: 'EVENING_STAR', direction: 'SELL', strength: 2.5 });
  if (b2 > 0 && b1 > 0 && b0 > 0 && c1.close > c2.close && c0.close > c1.close && bp0 > 0.5 && bp1 > 0.5)
    patterns.push({ name: 'THREE_WHITE_SOLDIERS', direction: 'BUY', strength: 2.0 });
  if (b2 < 0 && b1 < 0 && b0 < 0 && c1.close < c2.close && c0.close < c1.close && bp0 > 0.5 && bp1 > 0.5)
    patterns.push({ name: 'THREE_BLACK_CROWS', direction: 'SELL', strength: 2.0 });

  return patterns;
}

// ─── Divergence Detection ───
function detectRSIDivergence(candles, rsiVals, lookback = 30) {
  if (!candles || !rsiVals || candles.length < lookback) return null;
  const n = candles.length, st = n - lookback;
  const pL = [], pH = [];
  for (let i = st + 2; i < n - 2; i++) {
    if (rsiVals[i] === null) continue;
    if (candles[i].low <= candles[i-1].low && candles[i].low <= candles[i-2].low && candles[i].low <= candles[i+1].low && candles[i].low <= candles[i+2].low)
      pL.push({ idx: i, price: candles[i].low, rsi: rsiVals[i] });
    if (candles[i].high >= candles[i-1].high && candles[i].high >= candles[i-2].high && candles[i].high >= candles[i+1].high && candles[i].high >= candles[i+2].high)
      pH.push({ idx: i, price: candles[i].high, rsi: rsiVals[i] });
  }
  if (pL.length >= 2) {
    const r = pL[pL.length-1], p = pL[pL.length-2];
    if (r.price < p.price && r.rsi > p.rsi && r.idx - p.idx >= CONFIG.DIVERGENCE_MIN_BARS)
      return { type: 'BULLISH_RSI_DIVERGENCE', direction: 'BUY', strength: 2.0 };
  }
  if (pH.length >= 2) {
    const r = pH[pH.length-1], p = pH[pH.length-2];
    if (r.price > p.price && r.rsi < p.rsi && r.idx - p.idx >= CONFIG.DIVERGENCE_MIN_BARS)
      return { type: 'BEARISH_RSI_DIVERGENCE', direction: 'SELL', strength: 2.0 };
  }
  return null;
}

function detectMACDDivergence(candles, hist, lookback = 30) {
  if (!candles || !hist || candles.length < lookback) return null;
  const n = candles.length, st = n - lookback;
  const pL = [], pH = [];
  for (let i = st + 2; i < n - 2; i++) {
    if (hist[i] === null) continue;
    if (candles[i].low <= candles[i-1].low && candles[i].low <= candles[i+1].low)
      pL.push({ idx: i, price: candles[i].low, macd: hist[i] });
    if (candles[i].high >= candles[i-1].high && candles[i].high >= candles[i+1].high)
      pH.push({ idx: i, price: candles[i].high, macd: hist[i] });
  }
  if (pL.length >= 2) {
    const r = pL[pL.length-1], p = pL[pL.length-2];
    if (r.price < p.price && r.macd > p.macd) return { type: 'BULLISH_MACD_DIV', direction: 'BUY', strength: 1.5 };
  }
  if (pH.length >= 2) {
    const r = pH[pH.length-1], p = pH[pH.length-2];
    if (r.price > p.price && r.macd < p.macd) return { type: 'BEARISH_MACD_DIV', direction: 'SELL', strength: 1.5 };
  }
  return null;
}

function detectMarketCondition(adxVal, bbBW, atr, lastClose) {
  const cond = [];
  if (adxVal !== null) { cond.push(adxVal >= 25 ? 'TRENDING' : 'RANGING'); if (adxVal >= 40) cond.push('STRONG_TREND'); }
  if (bbBW !== null) { if (bbBW < 0.05) cond.push('SQUEEZE'); else if (bbBW > 0.5) cond.push('HIGH_VOLATILITY'); }
  if (atr !== null && lastClose > 0) { const ap = (atr/lastClose)*100; if (ap > 0.2) cond.push('VOLATILE'); else if (ap < 0.02) cond.push('DEAD_MARKET'); }
  return cond.length === 0 ? ['NORMAL'] : cond;
}

// ============================================
// CALCULATE ALL INDICATORS
// ============================================

function calculateAllIndicators(candles) {
  const closes = candles.map(c => c.close);
  return {
    ema5: calculateEMA(closes, 5),
    ema10: calculateEMA(closes, 10),
    ema20: calculateEMA(closes, 20),
    sma50: calculateSMA(closes, 50),
    rsi: calculateRSI(closes, CONFIG.RSI_PERIOD),
    macd: calculateMACD(closes),
    atr: calculateATR(candles, CONFIG.ATR_PERIOD),
    bollinger: calculateBollingerBands(closes, CONFIG.BB_PERIOD, CONFIG.BB_STD_DEV),
    stochastic: calculateStochastic(candles, CONFIG.STOCH_PERIOD, CONFIG.STOCH_SMOOTH_K, CONFIG.STOCH_SMOOTH_D),
    adx: calculateADX(candles, CONFIG.ADX_PERIOD),
    williamsR: calculateWilliamsR(candles, CONFIG.WILLIAMS_PERIOD),
    cci: calculateCCI(candles, CONFIG.CCI_PERIOD),
    mfi: calculateMFI(candles, CONFIG.MFI_PERIOD),
    pivots: calculatePivotPoints(candles),
    patterns: detectCandlestickPatterns(candles),
  };
}

// ============================================
// SAFE VALUE HELPERS
// ============================================

function safeLastValue(arr) {
  if (!arr || arr.length === 0) return null;
  for (let i = arr.length - 1; i >= 0; i--) {
    if (arr[i] !== null && arr[i] !== undefined && !isNaN(arr[i])) return arr[i];
  }
  return null;
}

function safeLastTwo(arr) {
  if (!arr || arr.length === 0) return { last: null, prev: null };
  let last = null, prev = null;
  for (let i = arr.length - 1; i >= 0; i--) {
    if (arr[i] !== null && arr[i] !== undefined && !isNaN(arr[i])) {
      if (last === null) last = arr[i]; else { prev = arr[i]; break; }
    }
  }
  return { last, prev };
}

// ============================================
// ★★★ BUILD MULTI-TIMEFRAME SIGNAL ★★★
// ============================================

function buildMultiTimeframeSignal(candleData, pair) {
  const now = new Date();
  const tfResults = {};
  const votes = [];

  // ── Step 1: Analyze EACH timeframe independently ──
  for (const [tf, candles] of Object.entries(candleData)) {
    const indicators = calculateAllIndicators(candles);
    const analysis = analyzeTimeframe(indicators, candles, tf);

    // Calculate duration IN CANDLES for this timeframe
    const durationCandles = calculateCandleDuration(indicators, analysis.direction, candles, tf);
    const candleMin = CANDLE_MINUTES[tf] || 1;
    const durationMinutes = durationCandles * candleMin;

    // Calculate expiry time
    const expiryTime = new Date(now.getTime() + durationMinutes * 60000);

    // Next candle close time
    const nextCandleClose = getNextCandleClose(now, candleMin);

    analysis.expiry = {
      candles: durationCandles,
      candleSize: `${candleMin}min`,
      totalMinutes: durationMinutes,
      expiryTime: expiryTime.toISOString(),
      humanReadable: formatDuration(durationMinutes),
      nextCandleClose: nextCandleClose.toISOString(),
    };

    // Entry price info
    const lastCandle = candles[candles.length - 1];
    analysis.entry = {
      price: lastCandle.close,
      candleTime: lastCandle.datetime,
      candleDirection: lastCandle.close >= lastCandle.open ? 'BULLISH' : 'BEARISH',
    };

    tfResults[tf] = analysis;
    votes.push(analysis);
  }

  // ── Step 2: Multi-TF Alignment Check ──
  const activeDirs = votes.filter(v => v.direction !== 'NO_TRADE').map(v => v.direction);
  const allBuy = activeDirs.length > 0 && activeDirs.every(d => d === 'BUY');
  const allSell = activeDirs.length > 0 && activeDirs.every(d => d === 'SELL');
  let alignment = 'MIXED';
  let alignmentBonus = 0;
  if (allBuy)  { alignment = 'ALL_BULLISH';  alignmentBonus = 12; }
  if (allSell) { alignment = 'ALL_BEARISH';  alignmentBonus = 12; }
  if (!allBuy && !allSell && activeDirs.length >= 2) {
    const bc = activeDirs.filter(d => d === 'BUY').length;
    const sc = activeDirs.filter(d => d === 'SELL').length;
    if (bc > sc) { alignment = 'MOSTLY_BULLISH'; alignmentBonus = 5; }
    if (sc > bc) { alignment = 'MOSTLY_BEARISH'; alignmentBonus = 5; }
  }

  // ── Step 3: Majority Vote for Final Signal ──
  const upVotes = votes.filter(v => v.direction === 'BUY').length;
  const downVotes = votes.filter(v => v.direction === 'SELL').length;
  const totalVotes = votes.length;

  let finalDirection, confidence;
  if (upVotes > downVotes && upVotes >= Math.ceil(totalVotes / 2)) {
    finalDirection = 'BUY';
    confidence = Math.round((upVotes / totalVotes) * 100);
  } else if (downVotes > upVotes && downVotes >= Math.ceil(totalVotes / 2)) {
    finalDirection = 'SELL';
    confidence = Math.round((downVotes / totalVotes) * 100);
  } else {
    const tie = resolveTieWithTolerance(tfResults);
    finalDirection = tie.direction;
    confidence = tie.confidence;
  }

  confidence = Math.min(99, confidence + alignmentBonus);

  // ── Step 4: Signal Grade ──
  const avgConf = votes.reduce((s, v) => s + (v.confluence || 0), 0) / Math.max(votes.length, 1);
  const grade = getSignalGrade(confidence, avgConf, alignment);

  // ── Step 5: Market Condition (from highest TF) ──
  const htf = candleData['15min'] || candleData['5min'] || candleData['1min'];
  let marketCondition = ['UNKNOWN'];
  if (htf) {
    const hi = calculateAllIndicators(htf);
    marketCondition = detectMarketCondition(
      safeLastValue(hi.adx.adx), safeLastValue(hi.bollinger.bandwidth),
      safeLastValue(hi.atr), htf[htf.length - 1].close
    );
  }

  // ── Step 6: Best Timeframe Recommendation ──
  const best = findBestTimeframe(tfResults, finalDirection);

  // ── Step 7: Per-TF Recommendations ──
  const recommendations = {};
  for (const [tf, r] of Object.entries(tfResults)) {
    recommendations[tf] = {
      direction: r.direction,
      score: r.score,
      confluence: `${r.confluence}/10 categories`,
      expiry: r.expiry,
      entry: r.entry,
      patterns: r.categoryScores?.patterns?.detected || [],
      divergence: {
        rsi: r.categoryScores?.divergence?.rsi || 'NONE',
        macd: r.categoryScores?.divergence?.macd || 'NONE',
      },
    };
  }

  return {
    finalSignal: finalDirection,
    confidence: `${confidence}%`,
    grade,
    marketCondition,
    alignment,

    // ★ Per-timeframe recommendations with candle-based expiry
    recommendations,

    // ★ Best timeframe to trade
    bestTimeframe: best,

    votes: {
      BUY: upVotes, SELL: downVotes,
      NO_TRADE: totalVotes - upVotes - downVotes,
      total: totalVotes,
    },
    averageConfluence: Math.round(avgConf * 10) / 10,

    // Full details per TF (all indicators)
    timeframeAnalysis: tfResults,

    method: 'MULTI_TF_CANDLE_VOTE_v4.1',
    generatedAt: now.toISOString(),
  };
}

// ============================================
// ★ FIND BEST TIMEFRAME
// ============================================

function findBestTimeframe(tfResults, finalDirection) {
  let bestTF = null, bestScore = -1, bestConf = -1;

  for (const [tf, r] of Object.entries(tfResults)) {
    if (r.direction === finalDirection || finalDirection === 'NO_TRADE') {
      const score = r.direction === 'BUY' ? r.score.up : r.direction === 'SELL' ? r.score.down : 0;
      if (r.confluence > bestConf || (r.confluence === bestConf && score > bestScore)) {
        bestTF = tf;
        bestScore = score;
        bestConf = r.confluence;
      }
    }
  }

  if (!bestTF) {
    // Pick highest score regardless
    for (const [tf, r] of Object.entries(tfResults)) {
      const score = Math.max(r.score.up, r.score.down);
      if (score > bestScore) { bestTF = tf; bestScore = score; bestConf = r.confluence; }
    }
  }

  if (!bestTF) return { timeframe: 'N/A', reason: 'No analyzable timeframe' };

  const best = tfResults[bestTF];
  return {
    timeframe: bestTF,
    direction: best.direction,
    score: bestScore,
    confluence: bestConf,
    expiry: best.expiry,
    reason: `Strongest ${best.direction} signal with ${bestConf}/10 category confluence`,
  };
}

// ============================================
// ★ CANDLE-BASED DURATION PER TIMEFRAME
// ============================================

function calculateCandleDuration(indicators, direction, candles, timeframe) {
  const cfg = DURATION_CONFIG[timeframe] || { base: 3, min: 1, max: 10 };
  let dur = cfg.base;

  // Active signal gets more time
  if (direction === 'BUY' || direction === 'SELL') dur += 1;

  // ── RSI extremes → shorter (quick reversal expected) ──
  const rsi = safeLastValue(indicators.rsi);
  if (rsi !== null) {
    if (rsi > 80 || rsi < 20) dur -= 2;
    else if (rsi > 70 || rsi < 30) dur -= 1;
  }

  // ── Stochastic extremes ──
  const stochK = safeLastValue(indicators.stochastic.k);
  if (stochK !== null) {
    if (stochK > 90 || stochK < 10) dur -= 1;
  }

  // ── ATR volatility ──
  const atr = safeLastValue(indicators.atr);
  if (atr !== null && candles.length > 0) {
    const lastClose = candles[candles.length - 1].close;
    const atrPct = (atr / lastClose) * 100;
    if (atrPct > 0.2)      dur -= 2;  // Very high vol → fast moves
    else if (atrPct > 0.1) dur -= 1;
    else if (atrPct < 0.02) dur += 2; // Dead market → slow
    else if (atrPct < 0.05) dur += 1;
  }

  // ── ADX trend strength ──
  const adxVal = safeLastValue(indicators.adx.adx);
  if (adxVal !== null) {
    if (adxVal >= 40) dur += 1;  // Strong trend → hold longer
    else if (adxVal < 15) dur -= 1; // No trend → shorter
  }

  // ── Bollinger squeeze ──
  const bbBW = safeLastValue(indicators.bollinger.bandwidth);
  if (bbBW !== null && bbBW < 0.05) dur += 1;

  // ── Strong patterns → hold longer ──
  if (indicators.patterns) {
    const strongP = indicators.patterns.filter(p =>
      ['MORNING_STAR','EVENING_STAR','THREE_WHITE_SOLDIERS','THREE_BLACK_CROWS','BULLISH_ENGULFING','BEARISH_ENGULFING'].includes(p.name)
    );
    if (strongP.length > 0) dur += 1;
  }

  // ── Timeframe-specific adjustments ──
  // Higher timeframes: signal needs fewer candles but more certainty
  if (timeframe === '15min' && adxVal !== null && adxVal < 20) {
    dur -= 1; // Ranging on 15min → very short
  }
  if (timeframe === '1min' && adxVal !== null && adxVal >= 30) {
    dur += 1; // Trending on 1min → ride the trend
  }

  return Math.max(cfg.min, Math.min(cfg.max, Math.round(dur)));
}

// ============================================
// NEXT CANDLE CLOSE TIME
// ============================================

function getNextCandleClose(now, candleMinutes) {
  const ms = candleMinutes * 60000;
  const currentSlot = Math.floor(now.getTime() / ms);
  return new Date((currentSlot + 1) * ms);
}

function formatDuration(minutes) {
  if (minutes < 60) return `${minutes} min`;
  const h = Math.floor(minutes / 60);
  const m = minutes % 60;
  return m > 0 ? `${h}h ${m}min` : `${h}h`;
}

// ============================================
// TIMEFRAME ANALYSIS (10 CATEGORIES)
// ============================================

function analyzeTimeframe(indicators, candles, timeframe) {
  const ema5 = safeLastValue(indicators.ema5);
  const ema10 = safeLastValue(indicators.ema10);
  const ema20 = safeLastValue(indicators.ema20);
  const sma50 = safeLastValue(indicators.sma50);
  const rsi = safeLastValue(indicators.rsi);
  const { last: macdHist, prev: prevMacdHist } = safeLastTwo(indicators.macd.histogram);
  const { last: macdLine } = safeLastTwo(indicators.macd.macdLine);
  const { last: macdSignal } = safeLastTwo(indicators.macd.signalLine);
  const atr = safeLastValue(indicators.atr);
  const bbUpper = safeLastValue(indicators.bollinger.upper);
  const bbLower = safeLastValue(indicators.bollinger.lower);
  const bbMiddle = safeLastValue(indicators.bollinger.middle);
  const bbBandwidth = safeLastValue(indicators.bollinger.bandwidth);
  const bbPercentB = safeLastValue(indicators.bollinger.percentB);
  const stochK = safeLastValue(indicators.stochastic.k);
  const stochD = safeLastValue(indicators.stochastic.d);
  const { prev: prevStochK } = safeLastTwo(indicators.stochastic.k);
  const adxVal = safeLastValue(indicators.adx.adx);
  const plusDI = safeLastValue(indicators.adx.plusDI);
  const minusDI = safeLastValue(indicators.adx.minusDI);
  const williamsR = safeLastValue(indicators.williamsR);
  const cci = safeLastValue(indicators.cci);
  const mfi = safeLastValue(indicators.mfi);
  const pivots = indicators.pivots;
  const patterns = indicators.patterns;

  if (ema5 === null || ema20 === null) {
    return {
      direction: 'NO_TRADE', score: { up:0, down:0, diff:0 },
      confluence: 0, reason: 'Insufficient data', timeframe,
    };
  }

  const lastCandle = candles[candles.length - 1];
  const lastClose = lastCandle.close;
  let upScore = 0, downScore = 0;
  let upCat = 0, downCat = 0;
  const catScores = {};

  // ═══ CAT 1: TREND (max ~3 pts) ═══
  {
    let u = 0, d = 0;
    if (ema5 > ema20) u += 1; else if (ema5 < ema20) d += 1;
    if (ema10 !== null) { if (ema10 > ema20) u += 0.5; else if (ema10 < ema20) d += 0.5; }
    if (sma50 !== null) { if (lastClose > sma50) u += 0.75; else if (lastClose < sma50) d += 0.75; }
    if (ema10 !== null) {
      if (ema5 > ema10 && ema10 > ema20) u += 0.75;
      else if (ema5 < ema10 && ema10 < ema20) d += 0.75;
    }
    upScore += u; downScore += d;
    if (u > d) upCat++; else if (d > u) downCat++;
    catScores.trend = { up: r2(u), down: r2(d) };
  }

  // ═══ CAT 2: MOMENTUM RSI/Williams/MFI (max ~3 pts) ═══
  {
    let u = 0, d = 0;
    if (rsi !== null) {
      if (rsi >= 75) d += 1.5; else if (rsi >= 55) u += 0.75;
      else if (rsi > 45) { } else if (rsi > 25) d += 0.75; else u += 1.5;
    }
    if (williamsR !== null) {
      if (williamsR > -20) d += 0.5; else if (williamsR < -80) u += 0.5;
      else if (williamsR > -50) u += 0.25; else d += 0.25;
    }
    if (mfi !== null) {
      if (mfi >= 80) d += 0.5; else if (mfi <= 20) u += 0.5;
      else if (mfi >= 55) u += 0.25; else if (mfi <= 45) d += 0.25;
    }
    upScore += u; downScore += d;
    if (u > d) upCat++; else if (d > u) downCat++;
    catScores.momentum = { up: r2(u), down: r2(d) };
  }

  // ═══ CAT 3: MACD (max ~2 pts) ═══
  {
    let u = 0, d = 0;
    if (macdHist !== null) {
      if (macdHist > 0) u += 0.75; else if (macdHist < 0) d += 0.75;
      if (prevMacdHist !== null) { if (macdHist > prevMacdHist) u += 0.5; else if (macdHist < prevMacdHist) d += 0.5; }
    }
    if (macdLine !== null && macdSignal !== null) { if (macdLine > macdSignal) u += 0.5; else if (macdLine < macdSignal) d += 0.5; }
    upScore += u; downScore += d;
    if (u > d) upCat++; else if (d > u) downCat++;
    catScores.macd = { up: r2(u), down: r2(d) };
  }

  // ═══ CAT 4: STOCHASTIC (max ~2 pts) ═══
  {
    let u = 0, d = 0;
    if (stochK !== null && stochD !== null) {
      if (stochK > 80 && stochD > 80) d += 0.75;
      else if (stochK < 20 && stochD < 20) u += 0.75;
      if (stochK > stochD) u += 0.5; else if (stochK < stochD) d += 0.5;
      if (prevStochK !== null) { if (stochK > prevStochK) u += 0.25; else if (stochK < prevStochK) d += 0.25; }
      if (stochK < 20 && stochK > stochD) u += 0.5;
      if (stochK > 80 && stochK < stochD) d += 0.5;
    }
    upScore += u; downScore += d;
    if (u > d) upCat++; else if (d > u) downCat++;
    catScores.stochastic = { up: r2(u), down: r2(d) };
  }

  // ═══ CAT 5: BOLLINGER + CCI (max ~3 pts) ═══
  {
    let u = 0, d = 0;
    if (bbUpper !== null && bbLower !== null && bbMiddle !== null) {
      if (lastClose >= bbUpper) d += 1.0; else if (lastClose <= bbLower) u += 1.0;
      else if (lastClose > bbMiddle) u += 0.25; else if (lastClose < bbMiddle) d += 0.25;
      if (bbPercentB !== null) { if (bbPercentB > 1.0) d += 0.5; else if (bbPercentB < 0.0) u += 0.5; }
    }
    if (cci !== null) {
      if (cci > 200) d += 0.75;
      else if (cci > 100) u += 0.5;
      else if (cci < -200) u += 0.75;
      else if (cci < -100) d += 0.5;
    }
    upScore += u; downScore += d;
    if (u > d) upCat++; else if (d > u) downCat++;
    catScores.bands = { up: r2(u), down: r2(d) };
  }

  // ═══ CAT 6: ADX (max ~2 pts) ═══
  {
    let u = 0, d = 0;
    if (adxVal !== null && plusDI !== null && minusDI !== null) {
      if (plusDI > minusDI) u += 0.75; else if (minusDI > plusDI) d += 0.75;
      if (adxVal >= 25) { if (plusDI > minusDI) u += 0.75; else d += 0.75; }
      const { last: al, prev: ap } = safeLastTwo(indicators.adx.adx);
      if (al !== null && ap !== null && al > ap && al >= 20) { if (plusDI > minusDI) u += 0.5; else d += 0.5; }
    }
    upScore += u; downScore += d;
    if (u > d) upCat++; else if (d > u) downCat++;
    catScores.adx = { up: r2(u), down: r2(d) };
  }

  // ═══ CAT 7: CANDLESTICK PATTERNS (max 2.5 pts) ═══
  {
    let u = 0, d = 0;
    if (patterns && patterns.length > 0) {
      for (const p of patterns) {
        if (p.direction === 'BUY') u += p.strength; else if (p.direction === 'SELL') d += p.strength;
      }
    }
    const bodySize = Math.abs(lastCandle.close - lastCandle.open);
    const totalRange = lastCandle.high - lastCandle.low || 0.00001;
    if (bodySize / totalRange > 0.5) {
      if (lastCandle.close > lastCandle.open) u += 0.5; else d += 0.5;
    }
    u = Math.min(u, 2.5); d = Math.min(d, 2.5);
    upScore += u; downScore += d;
    if (u > d) upCat++; else if (d > u) downCat++;
    catScores.patterns = { up: r2(u), down: r2(d), detected: patterns.map(p => p.name) };
  }

  // ═══ CAT 8: DIVERGENCE (max 2 pts) ═══
  {
    let u = 0, d = 0;
    const rDiv = detectRSIDivergence(candles, indicators.rsi);
    const mDiv = detectMACDDivergence(candles, indicators.macd.histogram);
    if (rDiv) { if (rDiv.direction === 'BUY') u += rDiv.strength; else d += rDiv.strength; }
    if (mDiv) { if (mDiv.direction === 'BUY') u += mDiv.strength; else d += mDiv.strength; }
    u = Math.min(u, 2.0); d = Math.min(d, 2.0);
    upScore += u; downScore += d;
    if (u > d) upCat++; else if (d > u) downCat++;
    catScores.divergence = { up: r2(u), down: r2(d), rsi: rDiv ? rDiv.type : 'NONE', macd: mDiv ? mDiv.type : 'NONE' };
  }

  // ═══ CAT 9: PIVOT POINTS (max 2 pts) ═══
  {
    let u = 0, d = 0;
    if (pivots && pivots.pivot !== null) {
      if (lastClose > pivots.pivot) u += 0.5; else if (lastClose < pivots.pivot) d += 0.5;
      if (pivots.s1 && Math.abs(lastClose - pivots.s1) / pivots.s1 < 0.001) u += 0.75;
      if (pivots.s2 && Math.abs(lastClose - pivots.s2) / pivots.s2 < 0.001) u += 1.0;
      if (pivots.r1 && Math.abs(lastClose - pivots.r1) / pivots.r1 < 0.001) d += 0.75;
      if (pivots.r2 && Math.abs(lastClose - pivots.r2) / pivots.r2 < 0.001) d += 1.0;
    }
    u = Math.min(u, 2.0); d = Math.min(d, 2.0);
    upScore += u; downScore += d;
    if (u > d) upCat++; else if (d > u) downCat++;
    catScores.pivots = { up: r2(u), down: r2(d) };
  }

  // ═══ CAT 10: VOLUME (max 1 pt) ═══
  {
    let u = 0, d = 0;
    if (candles.length >= 20) {
      const rv = candles.slice(-20).map(c => c.volume);
      const av = rv.reduce((a, b) => a + b, 0) / rv.length;
      if (av > 0 && lastCandle.volume > av * 1.5) {
        if (lastCandle.close > lastCandle.open) u += 0.75; else if (lastCandle.close < lastCandle.open) d += 0.75;
      }
      if (candles.length >= 5) {
        const lv = candles.slice(-5).map(c => c.volume);
        if (lv[4] > lv[0] && lv[3] > lv[1]) {
          if (lastCandle.close > candles[candles.length - 5].close) u += 0.25; else d += 0.25;
        }
      }
    }
    upScore += u; downScore += d;
    if (u > d) upCat++; else if (d > u) downCat++;
    catScores.volume = { up: r2(u), down: r2(d) };
  }

  // ═══ VOLATILITY FILTER ═══
  let volMult = 1.0;
  if (bbBandwidth !== null) {
    if (bbBandwidth < 0.03) volMult = 0.4;
    else if (bbBandwidth < 0.05) volMult = 0.6;
    else if (bbBandwidth < 0.08) volMult = 0.8;
  }
  upScore *= volMult;
  downScore *= volMult;

  // ═══ DECISION ═══
  const scoreDiff = Math.abs(upScore - downScore);
  const confluence = Math.max(upCat, downCat);
  let direction;

  if (upScore >= CONFIG.MIN_SCORE_THRESHOLD && upScore > downScore && upCat >= CONFIG.MIN_CONFLUENCE) {
    direction = 'BUY';
  } else if (downScore >= CONFIG.MIN_SCORE_THRESHOLD && downScore > upScore && downCat >= CONFIG.MIN_CONFLUENCE) {
    direction = 'SELL';
  } else if (scoreDiff >= 2.0 && confluence >= 2) {
    direction = upScore > downScore ? 'BUY' : 'SELL';
  } else {
    direction = 'NO_TRADE';
  }

  return {
    direction,
    score: { up: r2(upScore), down: r2(downScore), diff: r2(scoreDiff) },
    confluence,
    confluenceDetail: { bullish: upCat, bearish: downCat, total: 10 },
    categoryScores: catScores,
    volatilityMultiplier: volMult,
    indicators: {
      ema5: fmt(ema5), ema10: fmt(ema10), ema20: fmt(ema20), sma50: fmt(sma50),
      emaAlignment: (ema10 !== null && ema5 > ema10 && ema10 > ema20) ? 'BULLISH' :
                    (ema10 !== null && ema5 < ema10 && ema10 < ema20) ? 'BEARISH' : 'MIXED',
      rsi: fmt(rsi, 2), stochK: fmt(stochK, 2), stochD: fmt(stochD, 2),
      macdHist: fmt(macdHist, 6), macdLine: fmt(macdLine, 6), macdSignal: fmt(macdSignal, 6),
      adx: fmt(adxVal, 2), plusDI: fmt(plusDI, 2), minusDI: fmt(minusDI, 2),
      williamsR: fmt(williamsR, 2), cci: fmt(cci, 2), mfi: fmt(mfi, 2),
      atr: fmt(atr, 6),
      bbUpper: fmt(bbUpper), bbMiddle: fmt(bbMiddle), bbLower: fmt(bbLower),
      bbBandwidth: bbBandwidth !== null ? bbBandwidth.toFixed(4) + '%' : 'N/A',
      bbPercentB: fmt(bbPercentB, 4),
      pivot: pivots.pivot !== null ? pivots.pivot.toFixed(5) : 'N/A',
      r1: pivots.r1 !== null ? pivots.r1.toFixed(5) : 'N/A',
      r2: pivots.r2 !== null ? pivots.r2.toFixed(5) : 'N/A',
      s1: pivots.s1 !== null ? pivots.s1.toFixed(5) : 'N/A',
      s2: pivots.s2 !== null ? pivots.s2.toFixed(5) : 'N/A',
      patterns: patterns.map(p => p.name),
    },
    timeframe,
  };
}

function r2(v) { return Math.round(v * 100) / 100; }
function fmt(v, d = 5) { return v !== null ? v.toFixed(d) : 'N/A'; }

// ============================================
// TIE RESOLUTION
// ============================================

function resolveTieWithTolerance(details) {
  let tU = 0, tD = 0, cU = 0, cD = 0;
  for (const s of Object.values(details)) {
    tU += s.score.up; tD += s.score.down;
    cU += s.confluenceDetail?.bullish || 0;
    cD += s.confluenceDetail?.bearish || 0;
  }
  const total = tU + tD;
  if (tU > tD && cU >= cD) return { direction: 'BUY', confidence: total > 0 ? Math.round((tU/total)*100) : 50 };
  if (tD > tU && cD >= cU) return { direction: 'SELL', confidence: total > 0 ? Math.round((tD/total)*100) : 50 };
  if (tU > tD) return { direction: 'BUY', confidence: total > 0 ? Math.round((tU/total)*100) : 50 };
  if (tD > tU) return { direction: 'SELL', confidence: total > 0 ? Math.round((tD/total)*100) : 50 };
  return { direction: 'NO_TRADE', confidence: 50 };
}

// ============================================
// SIGNAL GRADE
// ============================================

function getSignalGrade(confidence, avgConf, alignment) {
  let sc = 0;
  sc += Math.min(40, confidence * 0.4);
  sc += Math.min(35, avgConf * 5);
  if (alignment === 'ALL_BULLISH' || alignment === 'ALL_BEARISH') sc += 25;
  else if (alignment.startsWith('MOSTLY')) sc += 12;

  if (sc >= 80) return { grade:'A', label:'STRONG', description:'High probability — multiple confirmations aligned across timeframes.' };
  if (sc >= 60) return { grade:'B', label:'GOOD', description:'Solid setup with good confirmation.' };
  if (sc >= 40) return { grade:'C', label:'MODERATE', description:'Some conflicting signals. Trade with caution.' };
  return { grade:'D', label:'WEAK', description:'Low confidence. Consider skipping.' };
}

// ============================================
// DUMMY FALLBACK
// ============================================

function generateDummySignal(pair) {
  const seed = (new Date().getMinutes() + pair.split('').reduce((a,c) => a + c.charCodeAt(0), 0)) % 10;
  const dir = seed < 4 ? 'BUY' : seed < 8 ? 'SELL' : 'NO_TRADE';
  return {
    finalSignal: dir, confidence: '0%',
    grade: { grade:'D', label:'DUMMY', description:'Fallback — no real data.' },
    marketCondition: ['UNKNOWN'], alignment: 'NONE',
    recommendations: {}, bestTimeframe: { timeframe: 'N/A' },
    votes: { BUY:0, SELL:0, NO_TRADE:0, total:0 },
    timeframeAnalysis: {}, method: 'DUMMY_FALLBACK',
    warning: 'All API calls failed. Fallback signal.',
  };
}

// ============================================
// JSON RESPONSE
// ============================================

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { 'Content-Type':'application/json', 'Cache-Control':'no-cache, no-store, must-revalidate' },
  });
}