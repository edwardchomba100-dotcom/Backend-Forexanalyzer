/**
 * ForexAnalyzer Pro — Backend Server v5.1.0
 *
 * CHANGES OVER v5.0.0  (aligns with EA v5.1.0):
 *
 *  SETTINGS POLICY CHANGES:
 *   • IncludeHistory and MaxHistoryDays are now LOCKED on the server side.
 *     The server never sends these fields — the EA always runs with history
 *     ON and MaxHistoryDays = 36500 (effectively unlimited). Removed from
 *     DEFAULT_EA_SETTINGS and stripped from PUT /api/accounts/:id/settings
 *     validation to avoid misleading the dashboard.
 *
 *   • EnablePriceAlerts is also LOCKED to true on the EA side.
 *     The server still stores it (for UI display) but the EA ignores any
 *     false value coming from the server.
 *
 *   • EARole and MasterAccountId are SERVER-ONLY fields — derived from
 *     copy-pair configuration in the dashboard.  The EA's user input panel
 *     no longer exposes these.  The server's /ea/settings endpoint already
 *     handled this correctly; no endpoint change required.
 *
 *   • Timing fields (LivePushIntervalMs, StatusPushIntervalMs,
 *     StaticPushIntervalMs, CommandPollMs), retry fields (MaxRetries,
 *     RetryDelayMs), and AlertReloadMs are now "internal" — they can be
 *     remotely tuned via the server but are NOT shown to the end user.
 *     The PUT /api/accounts/:id/settings endpoint still accepts and saves
 *     them so advanced operators can tune from the dashboard if needed.
 *
 *  VERSION STRINGS updated to 5.1.0 throughout (startup banner,
 *  /health endpoint, /api/debug endpoint).
 *
 *  All v5.0.0 logic (copy queue, risk manager, analytics, journal,
 *  commands, alerts, settings endpoints) preserved verbatim.
 *
 *  BACKWARD COMPATIBILITY:
 *   v5.0.0 EAs continue to work.  v4.1.0 EAs continue to work.
 */

const express    = require('express');
const cors       = require('cors');
require('dotenv').config();
const fs         = require('fs');
const path       = require('path');
const http       = require('http');
const https      = require('https');
const { Server } = require('socket.io');
const crypto     = require('crypto');
const WebSocket  = require('ws');

class SimpleHeaders {
  constructor(headers = {}) {
    this.map = new Map();
    for (const [key, value] of Object.entries(headers)) {
      this.map.set(key.toLowerCase(), Array.isArray(value) ? value.join(', ') : String(value ?? ''));
    }
  }
  get(name) {
    return this.map.get(String(name).toLowerCase()) || null;
  }
  has(name) {
    return this.map.has(String(name).toLowerCase());
  }
  forEach(callback) {
    for (const [key, value] of this.map.entries()) callback(value, key, this);
  }
}

const normalizeFetchHeaders = (headers) => {
  const out = {};
  if (!headers) return out;
  if (typeof headers.forEach === 'function') {
    headers.forEach((value, key) => { out[key] = value; });
  } else if (Array.isArray(headers)) {
    for (const [key, value] of headers) out[key] = value;
  } else {
    Object.assign(out, headers);
  }
  return out;
};

const makeSimpleResponse = (url, res, buffer) => ({
  ok: res.statusCode >= 200 && res.statusCode < 300,
  status: res.statusCode,
  statusText: res.statusMessage || '',
  url,
  headers: new SimpleHeaders(res.headers),
  text: async () => buffer.toString('utf8'),
  json: async () => {
    const text = buffer.toString('utf8');
    return text ? JSON.parse(text) : null;
  },
  arrayBuffer: async () => buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength),
  clone: () => makeSimpleResponse(url, res, buffer),
});

// Avoid Node 20's bundled undici fetch on shared hosts where its Wasm parser can
// fail under CloudLinux/LVE memory limits.
const nodeHttpsFetch = (input, init = {}) => new Promise((resolve, reject) => {
  const inputUrl = typeof input === 'string' || input instanceof URL ? String(input) : input?.url;
  const url = new URL(inputUrl);
  if (url.protocol !== 'https:') return reject(new Error(`Unsupported fetch protocol: ${url.protocol}`));

  const body = init.body ?? input?.body;
  const req = https.request({
    protocol: url.protocol,
    hostname: url.hostname,
    port: url.port || 443,
    path: `${url.pathname}${url.search}`,
    method: init.method || input?.method || (body ? 'POST' : 'GET'),
    headers: normalizeFetchHeaders(init.headers || input?.headers),
  }, (res) => {
    const chunks = [];
    res.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
    res.on('end', () => resolve(makeSimpleResponse(inputUrl, res, Buffer.concat(chunks))));
  });

  req.on('error', reject);
  if (init.signal) {
    if (init.signal.aborted) req.destroy(new Error('The operation was aborted'));
    init.signal.addEventListener('abort', () => req.destroy(new Error('The operation was aborted')), { once: true });
  }
  if (body) req.write(body);
  req.end();
});

globalThis.fetch = nodeHttpsFetch;

const makeSupabaseError = (status, payload) => {
  if (!status || status < 400) return null;
  return {
    status,
    message: payload?.message || payload?.msg || payload?.error_description || payload?.error || `Supabase request failed (${status})`,
    details: payload?.details,
    hint: payload?.hint,
    code: payload?.code,
  };
};

const encodeFilterValue = (value) => encodeURIComponent(value === null ? 'null' : String(value));

class SupabaseRestQuery {
  constructor(client, schema, table) {
    this.client = client;
    this.schema = schema || 'public';
    this.table = table;
    this.method = 'GET';
    this.body = undefined;
    this.params = new URLSearchParams();
    this.selectColumns = null;
    this.singleMode = null;
    this.prefer = [];
  }

  select(columns = '*') {
    this.selectColumns = columns;
    this.params.set('select', columns);
    return this;
  }

  upsert(body, options = {}) {
    this.method = 'POST';
    this.body = body;
    this.prefer.push('resolution=merge-duplicates');
    if (options.onConflict) this.params.set('on_conflict', options.onConflict);
    return this;
  }

  insert(body) {
    this.method = 'POST';
    this.body = body;
    return this;
  }

  update(body) {
    this.method = 'PATCH';
    this.body = body;
    return this;
  }

  delete() {
    this.method = 'DELETE';
    return this;
  }

  eq(column, value) {
    this.params.set(column, `eq.${encodeFilterValue(value)}`);
    return this;
  }

  is(column, value) {
    this.params.set(column, `is.${encodeFilterValue(value)}`);
    return this;
  }

  order(column, options = {}) {
    this.params.set('order', `${column}.${options.ascending === false ? 'desc' : 'asc'}`);
    return this;
  }

  limit(count) {
    this.params.set('limit', String(count));
    return this;
  }

  single() {
    this.singleMode = 'single';
    return this.execute();
  }

  maybeSingle() {
    this.singleMode = 'maybeSingle';
    return this.execute();
  }

  then(resolve, reject) {
    return this.execute().then(resolve, reject);
  }

  catch(reject) {
    return this.execute().catch(reject);
  }

  async execute() {
    const query = this.params.toString();
    const url = `${this.client.url}/rest/v1/${encodeURIComponent(this.table)}${query ? `?${query}` : ''}`;
    const headers = {
      apikey: this.client.key,
      authorization: `Bearer ${this.client.key}`,
      accept: 'application/json',
    };

    if (this.schema !== 'public') {
      headers['accept-profile'] = this.schema;
      headers['content-profile'] = this.schema;
    }

    if (this.body !== undefined) headers['content-type'] = 'application/json';
    if (this.method !== 'GET') {
      this.prefer.push(this.selectColumns ? 'return=representation' : 'return=minimal');
    }
    if (this.prefer.length) headers.prefer = [...new Set(this.prefer)].join(',');

    const res = await nodeHttpsFetch(url, {
      method: this.method,
      headers,
      body: this.body === undefined ? undefined : JSON.stringify(this.body),
    });

    const text = await res.text();
    let payload = null;
    if (text) {
      try { payload = JSON.parse(text); } catch { payload = text; }
    }

    const error = makeSupabaseError(res.status, payload);
    if (error) return { data: null, error };

    let data = payload;
    if (this.singleMode) {
      if (Array.isArray(payload)) data = payload[0] || null;
      if (this.singleMode === 'single' && !data) {
        return { data: null, error: { message: 'No rows returned', status: 406 } };
      }
    }

    return { data, error: null };
  }
}

const createSupabaseRestClient = (url, key) => ({
  url: String(url || '').replace(/\/+$/, ''),
  key,
  from(table) {
    return new SupabaseRestQuery(this, 'public', table);
  },
  schema(schemaName) {
    return {
      from: (table) => new SupabaseRestQuery(this, schemaName, table),
    };
  },
  auth: {
    async getUser(token) {
      const res = await nodeHttpsFetch(`${String(url || '').replace(/\/+$/, '')}/auth/v1/user`, {
        method: 'GET',
        headers: {
          apikey: key,
          authorization: `Bearer ${token}`,
          accept: 'application/json',
        },
      });
      const text = await res.text();
      let payload = null;
      if (text) {
        try { payload = JSON.parse(text); } catch { payload = text; }
      }
      const error = makeSupabaseError(res.status, payload);
      return { data: error ? null : { user: payload }, error };
    },
  },
});

// ─── Configuration ────────────────────────────────────────────────────────────
const CONFIG = {
  PORT: +(process.env.PORT || 3000),
  PUBLIC_BASE_URL: (process.env.PUBLIC_BASE_URL || 'https://api.forexanalyzerpro.com').replace(/\/+$/, ''),

  MT5_COMMON_DIR: process.env.MT5_COMMON_DIR ||
    'C:\\Users\\IAN\\AppData\\Roaming\\MetaQuotes\\Terminal\\Common\\Files',

  STORAGE_DIR:            path.join(__dirname, 'data'),
  JOURNAL_FILE:           path.join(__dirname, 'data', 'journal.json'),
  EQUITY_HISTORY_FILE:    path.join(__dirname, 'data', 'equity_history.json'),
  INSIGHTS_FILE:          path.join(__dirname, 'data', 'insights.json'),
  ACCOUNTS_FILE:          path.join(__dirname, 'data', 'accounts.json'),
  COPY_PAIRS_FILE:        path.join(__dirname, 'data', 'copy_pairs.json'),
  NEWS_FILE:              path.join(__dirname, 'data', 'news_events.json'),
  COMMAND_LOG_FILE:       path.join(__dirname, 'data', 'command_log.json'),
  ALERTS_DIR:             path.join(__dirname, 'data', 'alerts'),
  SETTINGS_DIR:           path.join(__dirname, 'data', 'settings'),
  SETTINGS_DEFAULTS_FILE: path.join(__dirname, 'data', 'settings_defaults.json'),

  EA_OFFLINE_THRESHOLD_S: 15,
  API_KEY: process.env.API_KEY || 'forexanalyzer-local-key',

  MAX_QUEUE_EVENTS: 500,
  EVENT_ID_LENGTH:  16,

  SUPABASE_URL:              process.env.SUPABASE_URL || '',
  SUPABASE_SERVICE_ROLE_KEY: process.env.SUPABASE_SERVICE_ROLE_KEY || '',
  SUPABASE_SCHEMA:           process.env.SUPABASE_SCHEMA || 'public',
  DB_WRITE_DEBOUNCE_MS:      +(process.env.DB_WRITE_DEBOUNCE_MS || 750),
  ALLOW_LEGACY_EA_KEY:       process.env.ALLOW_LEGACY_EA_KEY !== 'false',

  METAAPI_TOKEN:             process.env.METAAPI_TOKEN || '',
  METAAPI_PROVISIONING_URL:  (process.env.METAAPI_PROVISIONING_URL || 'https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai').replace(/\/+$/, ''),
  METAAPI_CLIENT_API_URL:    (process.env.METAAPI_CLIENT_API_URL || 'https://mt-client-api-v1.new-york.agiliumtrade.ai').replace(/\/+$/, ''),
  METAAPI_SYNC_INTERVAL_MS:  +(process.env.METAAPI_SYNC_INTERVAL_MS || 15000),
  METAAPI_HISTORY_DAYS:      +(process.env.METAAPI_HISTORY_DAYS || 180),
  METAAPI_HISTORY_LIMIT:     +(process.env.METAAPI_HISTORY_LIMIT || 1000),
};

const DATABASE_ENABLED = !!(CONFIG.SUPABASE_URL && CONFIG.SUPABASE_SERVICE_ROLE_KEY);

const supabase = DATABASE_ENABLED
  ? createSupabaseRestClient(CONFIG.SUPABASE_URL, CONFIG.SUPABASE_SERVICE_ROLE_KEY)
  : null;

const g_kvCache                 = new Map();
const g_kvMeta                  = new Map();
const g_settingsCache           = new Map();
const g_settingsMeta            = new Map();
const g_alertsCache             = new Map();
const g_alertsMeta              = new Map();
const g_snapshotPersistTimers   = new Map();
const g_accountOwners           = new Map();
const g_eaKeyOwners             = new Map();
const g_authTokenCache          = new Map();
const g_profileUpsertAt         = new Map();
const g_historySignatures       = new Map();
const g_directAccounts          = new Map();
const g_directSyncTimers        = new Map();
const g_directSyncInFlight      = new Set();
const g_directCreateTransactions= new Map();

const cloneJSON = (value) =>
  value === undefined ? undefined : JSON.parse(JSON.stringify(value));

const dbFrom = (table) =>
  CONFIG.SUPABASE_SCHEMA && CONFIG.SUPABASE_SCHEMA !== 'public'
    ? supabase.schema(CONFIG.SUPABASE_SCHEMA).from(table)
    : supabase.from(table);

const logDbError = (label, err) => {
  const message = err?.message || err?.details || String(err);
  console.error(`[Supabase] ${label}: ${message}`);
};

const runDbTask = (label, task) => {
  if (!supabase) return;
  Promise.resolve()
    .then(task)
    .catch(err => logDbError(label, err));
};

const safeWriteJSONFile = (filePath, payload) => {
  try {
    const dir = path.dirname(filePath);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(filePath, JSON.stringify(payload, null, 2));
  } catch (e) {
    console.warn(`[Storage] Could not mirror ${filePath}: ${e.message}`);
  }
};

const hashSecret = (secret) =>
  crypto.createHash('sha256').update(String(secret || '')).digest('hex');

const publicHash = (value, length = 10) =>
  hashSecret(value).slice(0, length);

const safeJsonFromResponse = async (res) => {
  const text = await res.text();
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return { message: text };
  }
};

const externalJsonRequest = async (url, options = {}) => {
  const headers = {
    Accept: 'application/json',
    ...(options.body ? { 'Content-Type': 'application/json' } : {}),
    ...(options.headers || {}),
  };
  const res = await nodeHttpsFetch(url, {
    method: options.method || (options.body ? 'POST' : 'GET'),
    headers,
    body: options.body ? JSON.stringify(options.body) : undefined,
  });
  const payload = await safeJsonFromResponse(res);
  if (!res.ok) {
    const err = new Error(payload?.message || payload?.error || `Request failed (${res.status})`);
    err.statusCode = res.status;
    err.payload = payload;
    err.retryAfter = res.headers.get('retry-after');
    throw err;
  }
  return { status: res.status, payload, headers: res.headers };
};

const metaApiRequest = (baseUrl, pathName, options = {}) => {
  if (!CONFIG.METAAPI_TOKEN) {
    const err = new Error('MetaApi is not configured. Set METAAPI_TOKEN on the backend.');
    err.statusCode = 503;
    throw err;
  }
  const url = `${baseUrl}${pathName}`;
  return externalJsonRequest(url, {
    ...options,
    headers: {
      'auth-token': CONFIG.METAAPI_TOKEN,
      ...(options.transactionId ? { 'transaction-id': options.transactionId } : {}),
      ...(options.headers || {}),
    },
  });
};

const decodeJwtPayload = (token) => {
  try {
    const [, payload] = String(token || '').split('.');
    if (!payload) return {};
    const normalized = payload.replace(/-/g, '+').replace(/_/g, '/');
    return JSON.parse(Buffer.from(normalized, 'base64').toString('utf8'));
  } catch {
    return {};
  }
};

const getBearerToken = (req) => {
  const header = req.headers.authorization || '';
  const match = header.match(/^Bearer\s+(.+)$/i);
  return match ? match[1] : null;
};

const sanitizeUser = (user) => ({
  id: user.id,
  email: user.email || '',
  name: user.user_metadata?.full_name || user.user_metadata?.name || user.email || 'Trader',
  avatar: user.user_metadata?.avatar_url || user.user_metadata?.picture || '',
});

const getAuthenticatedUser = async (token) => {
  if (!supabase || !token) return null;
  const cacheKey = hashSecret(token);
  const cached = g_authTokenCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now()) return cached.user;

  const { data, error } = await supabase.auth.getUser(token);
  if (error || !data?.user) return null;
  const payload = decodeJwtPayload(token);
  const tokenExpiresAt = payload.exp ? payload.exp * 1000 : Date.now() + 5 * 60 * 1000;
  g_authTokenCache.set(cacheKey, {
    user: data.user,
    expiresAt: Math.min(tokenExpiresAt, Date.now() + 10 * 60 * 1000),
  });
  return data.user;
};

const upsertUserProfile = (user) => {
  if (!supabase || !user?.id) return;
  const last = g_profileUpsertAt.get(user.id) || 0;
  if (Date.now() - last < 6 * 60 * 60 * 1000) return;
  g_profileUpsertAt.set(user.id, Date.now());
  const clean = sanitizeUser(user);
  runDbTask(`profile:${user.id}`, async () => {
    const { error } = await dbFrom('tradevault_user_profiles').upsert({
      user_id: clean.id,
      email: clean.email,
      full_name: clean.name,
      avatar_url: clean.avatar,
      updated_at: new Date().toISOString(),
    }, { onConflict: 'user_id' });
    if (error) throw error;
  });
};

const requireUser = async (req, res, next) => {
  if (!DATABASE_ENABLED) {
    return res.status(503).json({ error: 'Supabase is not configured. Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY.' });
  }

  const user = await getAuthenticatedUser(getBearerToken(req));
  if (!user) return res.status(401).json({ error: 'Authentication required' });

  req.user = sanitizeUser(user);
  upsertUserProfile(user);
  return next();
};

// ─── Default EA Settings ──────────────────────────────────────────────────────
//  These mirror SetDefaultSettings() in the v5.1.0 EA.
//  They are used whenever an account has no custom settings saved,
//  and also returned by GET /api/settings/defaults.
//
//  NOTE (v5.1.0): IncludeHistory and MaxHistoryDays are intentionally
//  absent here — the EA v5.1.0 locks these to ON / unlimited internally
//  and ignores any server-supplied values.  EnablePriceAlerts is stored
//  for UI display but the EA always forces it to true.

const DEFAULT_EA_SETTINGS = {
  // Identity — set by server via copy-pair config, never by user input
  EARole:                    'STANDALONE',
  MasterAccountId:           '',

  // Timing — internal; server may remote-tune, not shown to end user
  LivePushIntervalMs:        200,
  StatusPushIntervalMs:      2000,
  StaticPushIntervalMs:      30000,
  CommandPollMs:             100,

  // Copy Trading
  LotMultiplier:             1.0,
  UseFixedLot:               false,
  FixedLotSize:              0.01,
  CopyStopLoss:              true,
  CopyTakeProfit:            true,

  // Risk Management
  EnableRiskManager:         true,
  MaxLotSize:                5.0,
  MaxOpenTrades:             10,
  MaxDrawdownPct:            20.0,
  MaxDailyLossPct:           5.0,
  EquityProtectionPct:       15.0,
  AutoResumeDaily:           true,

  // Session Filters
  FilterBySession:           false,
  TradeAsia:                 true,
  TradeLondon:               true,
  TradeNewYork:              true,
  TradeOverlap:              true,

  // News Filter
  EnableNewsFilter:          false,
  NewsBufferMinutes:         30,

  // Execution — internal; server may remote-tune, not shown to end user
  MaxRetries:                1,
  RetryDelayMs:              200,
  SlippagePoints:            30,
  MagicNumberBase:           77000,

  // Manual Trade Detection
  DetectManualTrades:        true,

  // Price Alerts — EnablePriceAlerts is stored for display only;
  // the EA v5.1.0 always forces it to true regardless of this value.
  EnablePriceAlerts:         true,
  EnablePushNotifications:   true,
  EnablePCAlerts:            true,
  AlertReloadMs:             5000,  // internal — server may remote-tune

  // Command fallback
  UseFileCommandFallback:    true,

  // Copy queue behaviour
  MaxQueueEventsPerPoll:     20,
  SyncAfterMissedMs:         5000,
};

// ─── Initialize storage ───────────────────────────────────────────────────────
[
  CONFIG.STORAGE_DIR,
  CONFIG.ALERTS_DIR,
  CONFIG.SETTINGS_DIR,
].forEach(dir => {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

const initFile = (p, d) => {
  if (!fs.existsSync(p)) fs.writeFileSync(p, JSON.stringify(d, null, 2));
};

initFile(CONFIG.JOURNAL_FILE,         { entries: [] });
initFile(CONFIG.EQUITY_HISTORY_FILE,  { snapshots: [] });
initFile(CONFIG.INSIGHTS_FILE,        { generated_at: null, insights: [] });
initFile(CONFIG.ACCOUNTS_FILE,        { accounts: [] });
initFile(CONFIG.COPY_PAIRS_FILE,      { pairs: [] });
initFile(CONFIG.NEWS_FILE,            { events: [] });
initFile(CONFIG.COMMAND_LOG_FILE,     { commands: [] });
initFile(CONFIG.SETTINGS_DEFAULTS_FILE, DEFAULT_EA_SETTINGS);  // v5.1.0: no IncludeHistory/MaxHistoryDays

const KV_FILE_KEYS = new Map([
  [path.resolve(CONFIG.JOURNAL_FILE).toLowerCase(),          'journal'],
  [path.resolve(CONFIG.EQUITY_HISTORY_FILE).toLowerCase(),   'equity_history'],
  [path.resolve(CONFIG.INSIGHTS_FILE).toLowerCase(),         'insights'],
  [path.resolve(CONFIG.ACCOUNTS_FILE).toLowerCase(),         'accounts'],
  [path.resolve(CONFIG.COPY_PAIRS_FILE).toLowerCase(),       'copy_pairs'],
  [path.resolve(CONFIG.NEWS_FILE).toLowerCase(),             'news_events'],
  [path.resolve(CONFIG.COMMAND_LOG_FILE).toLowerCase(),      'command_log'],
  [path.resolve(CONFIG.SETTINGS_DEFAULTS_FILE).toLowerCase(),'settings_defaults'],
]);

const KV_KEY_FILES = new Map([
  ['journal',           CONFIG.JOURNAL_FILE],
  ['equity_history',    CONFIG.EQUITY_HISTORY_FILE],
  ['insights',          CONFIG.INSIGHTS_FILE],
  ['accounts',          CONFIG.ACCOUNTS_FILE],
  ['copy_pairs',        CONFIG.COPY_PAIRS_FILE],
  ['news_events',       CONFIG.NEWS_FILE],
  ['command_log',       CONFIG.COMMAND_LOG_FILE],
  ['settings_defaults', CONFIG.SETTINGS_DEFAULTS_FILE],
]);

const kvKeyForFile = (filePath) =>
  KV_FILE_KEYS.get(path.resolve(filePath).toLowerCase()) || null;

const persistKV = (key, value) => {
  runDbTask(`persist kv:${key}`, async () => {
    const { error } = await dbFrom('tradevault_kv_store').upsert({
      key,
      value: cloneJSON(value),
      updated_at: new Date().toISOString(),
    }, { onConflict: 'key' });
    if (error) throw error;
  });
};

// ─── Settings Helpers ─────────────────────────────────────────────────────────

/**
 * Returns the settings file path for a given account.
 */
const settingsFilePath = (accountId) =>
  path.join(CONFIG.SETTINGS_DIR, `settings_${accountId}.json`);

const settingsFileExists = (accountId) =>
  g_settingsCache.has(accountId) || fs.existsSync(settingsFilePath(accountId));

const settingsSavedAt = (accountId) => {
  const meta = g_settingsMeta.get(accountId);
  if (meta?.updated_at) return meta.updated_at;
  const filePath = settingsFilePath(accountId);
  return fs.existsSync(filePath) ? fs.statSync(filePath).mtime : null;
};

/**
 * Load settings for an account.
 * Merges stored overrides on top of defaults so every key is always present.
 * Returns a complete EASettings object safe to send directly to the EA.
 */
const loadAccountSettings = (accountId) => {
  const defaults = { ...DEFAULT_EA_SETTINGS };
  if (g_settingsCache.has(accountId)) {
    return { ...defaults, ...cloneJSON(g_settingsCache.get(accountId)) };
  }

  const filePath = settingsFilePath(accountId);
  if (!fs.existsSync(filePath)) return defaults;
  try {
    const stored = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    g_settingsCache.set(accountId, cloneJSON(stored));
    g_settingsMeta.set(accountId, { updated_at: fs.statSync(filePath).mtime });
    return { ...defaults, ...stored };
  } catch (e) {
    console.error(`[Settings] Failed to parse settings for ${accountId}:`, e.message);
    return defaults;
  }
};

/**
 * Persist settings overrides for an account.
 * Only stores keys that differ from defaults to keep the file lean.
 * Returns the merged (full) settings object.
 */
const saveAccountSettings = (accountId, incoming) => {
  const merged    = { ...DEFAULT_EA_SETTINGS, ...incoming };
  const filePath  = settingsFilePath(accountId);
  g_settingsCache.set(accountId, cloneJSON(merged));
  g_settingsMeta.set(accountId, { updated_at: new Date().toISOString() });
  safeWriteJSONFile(filePath, merged);
  runDbTask(`persist settings:${accountId}`, async () => {
    const { error } = await dbFrom('tradevault_account_settings').upsert({
      account_id: accountId,
      settings: cloneJSON(merged),
      updated_at: new Date().toISOString(),
    }, { onConflict: 'account_id' });
    if (error) throw error;
  });
  console.log(`[Settings] Saved settings for ${accountId}`);
  return merged;
};

/**
 * Delete the per-account settings file (resets to defaults).
 */
const resetAccountSettings = (accountId) => {
  const filePath = settingsFilePath(accountId);
  if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
  g_settingsCache.delete(accountId);
  g_settingsMeta.delete(accountId);
  runDbTask(`delete settings:${accountId}`, async () => {
    const { error } = await dbFrom('tradevault_account_settings')
      .delete()
      .eq('account_id', accountId);
    if (error) throw error;
  });
  console.log(`[Settings] Reset settings for ${accountId} to defaults`);
};

const alertsFilePath = (accountId) =>
  path.join(CONFIG.ALERTS_DIR, `alerts_${accountId}.json`);

const loadAccountAlerts = (accountId) => {
  if (g_alertsCache.has(accountId)) return cloneJSON(g_alertsCache.get(accountId));
  const filePath = alertsFilePath(accountId);
  if (!fs.existsSync(filePath)) return { alerts: [] };
  try {
    const payload = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    g_alertsCache.set(accountId, cloneJSON(payload));
    g_alertsMeta.set(accountId, { updated_at: fs.statSync(filePath).mtime });
    return payload;
  } catch (e) {
    console.error(`[Alerts] Failed to parse alerts for ${accountId}:`, e.message);
    return { alerts: [] };
  }
};

const saveAccountAlerts = (accountId, payload) => {
  g_alertsCache.set(accountId, cloneJSON(payload));
  g_alertsMeta.set(accountId, { updated_at: new Date().toISOString() });
  safeWriteJSONFile(alertsFilePath(accountId), payload);

  const commonFile = path.join(CONFIG.MT5_COMMON_DIR, `alerts_${accountId}.json`);
  if (CONFIG.MT5_COMMON_DIR && fs.existsSync(CONFIG.MT5_COMMON_DIR)) {
    safeWriteJSONFile(commonFile, payload);
  }

  runDbTask(`persist alerts:${accountId}`, async () => {
    const { error } = await dbFrom('tradevault_account_alerts').upsert({
      account_id: accountId,
      payload: cloneJSON(payload),
      updated_at: new Date().toISOString(),
    }, { onConflict: 'account_id' });
    if (error) throw error;
  });
};

// ─── State ────────────────────────────────────────────────────────────────────
const g_accounts = new Map();

// ─── Copy Event Queue System ──────────────────────────────────────────────────
const g_copyQueues  = new Map();
const g_copyLatency = new Map();

const newEventId = () =>
  crypto.randomBytes(CONFIG.EVENT_ID_LENGTH / 2).toString('hex');

const getCopyQueue = (slaveId) => {
  if (!g_copyQueues.has(slaveId)) {
    g_copyQueues.set(slaveId, {
      events:        [],
      lastPositions: new Map(),
      seenTickets:   new Set(),
      lastEventId:   null,
      pendingClose:  new Set(),
    });
  }
  return g_copyQueues.get(slaveId);
};

const pushToQueue = (queue, event) => {
  event.id        = newEventId();
  event.server_ts = Date.now();
  queue.events.push(event);
  if (queue.events.length > CONFIG.MAX_QUEUE_EVENTS) {
    queue.events.splice(0, queue.events.length - CONFIG.MAX_QUEUE_EVENTS);
    console.warn(`[CopyQueue] Queue cap hit — evicting oldest events`);
  }
};

const drainQueue = (queue, afterEventId) => {
  if (!afterEventId) return [...queue.events];
  const idx = queue.events.findIndex(e => e.id === afterEventId);
  if (idx < 0) return [...queue.events];
  return queue.events.slice(idx + 1);
};

const recordLatency = (slaveId, eventTs) => {
  if (!eventTs) return;
  const rtt = Date.now() - eventTs;
  if (!g_copyLatency.has(slaveId)) {
    g_copyLatency.set(slaveId, {
      samples: [], avg: 0, min: Infinity, max: 0, last: 0, count: 0,
    });
  }
  const s = g_copyLatency.get(slaveId);
  s.last = rtt;
  s.count++;
  s.min = Math.min(s.min, rtt);
  s.max = Math.max(s.max, rtt);
  s.samples.push(rtt);
  if (s.samples.length > 100) s.samples.shift();
  s.avg = Math.round(s.samples.reduce((a, b) => a + b, 0) / s.samples.length);
};

// ─── Diff master positions → populate slave queues ────────────────────────────
const diffMasterPositions = (masterState) => {
  const masterAccId    = masterState.accountId;
  const openPositions  = masterState.processedData?.open_positions || [];
  const currentTickets = new Set(openPositions.map(p => String(p.ticket)));

  g_accounts.forEach((slaveState, slaveId) => {
    if (slaveState.config.role !== 'SLAVE')                return;
    if (slaveState.config.masterAccountId !== masterAccId) return;
    if (slaveState.config.active === false)                return;

    const queue = getCopyQueue(slaveId);

    // OPEN and MODIFY events
    for (const pos of openPositions) {
      const ticketStr = String(pos.ticket);

      if (!queue.seenTickets.has(ticketStr)) {
        pushToQueue(queue, {
          event:      'OPEN',
          ticket:     pos.ticket,
          symbol:     pos.symbol,
          type:       pos.type,
          lots:       pos.lots,
          open_price: pos.open_price,
          sl:         pos.sl,
          tp:         pos.tp,
        });
        queue.seenTickets.add(ticketStr);
        queue.lastPositions.set(ticketStr, { sl: pos.sl, tp: pos.tp });

        console.log(`\n[COPY-QUEUE] *** OPEN EVENT QUEUED ***`);
        console.log(`  Master  : ${masterAccId}`);
        console.log(`  Slave   : ${slaveId}`);
        console.log(`  Ticket  : ${pos.ticket}  Symbol: ${pos.symbol}  Lots: ${pos.lots}`);
        console.log(`  Queue depth: ${queue.events.length}`);

      } else {
        const prev = queue.lastPositions.get(ticketStr) || {};
        if (prev.sl !== pos.sl || prev.tp !== pos.tp) {
          pushToQueue(queue, {
            event:  'MODIFY',
            ticket: pos.ticket,
            sl:     pos.sl,
            tp:     pos.tp,
          });
          queue.lastPositions.set(ticketStr, { sl: pos.sl, tp: pos.tp });

          console.log(`\n[COPY-QUEUE] MODIFY EVENT QUEUED`);
          console.log(`  Ticket: ${pos.ticket}  old SL/TP: ${prev.sl}/${prev.tp}  new: ${pos.sl}/${pos.tp}`);
        }
      }
    }

    // CLOSE events
    for (const ticketStr of queue.seenTickets) {
      if (!currentTickets.has(ticketStr) && !queue.pendingClose.has(ticketStr)) {
        pushToQueue(queue, {
          event:  'CLOSE',
          ticket: parseInt(ticketStr, 10),
        });
        queue.pendingClose.add(ticketStr);

        console.log(`\n[COPY-QUEUE] *** CLOSE EVENT QUEUED ***`);
        console.log(`  Master: ${masterAccId}  Slave: ${slaveId}  Ticket: ${ticketStr}`);
        console.log(`  Queue depth: ${queue.events.length}`);
      }
    }
  });
};

// ─── AccountState ─────────────────────────────────────────────────────────────
class AccountState {
  constructor(accountId, config = {}) {
    this.accountId       = accountId;
    this.config          = {
      alias:           config.alias           || accountId,
      source:          config.source          || 'ea',
      connectionMethod: config.connectionMethod || (config.source === 'metaapi' ? 'direct' : 'ea'),
      platform:        config.platform        || 'MT5',
      broker:          config.broker          || '',
      server:          config.server          || '',
      login:           config.login           || accountId,
      metaapiAccountId: config.metaapiAccountId || null,
      passwordType:    config.passwordType    || '',
      role:            config.role            || 'STANDALONE',
      masterAccountId: config.masterAccountId || null,
      lotMultiplier:   config.lotMultiplier   || 1.0,
      useFixedLot:     config.useFixedLot     || false,
      fixedLotSize:    config.fixedLotSize    || 0.01,
      copySL:          config.copySL          !== false,
      copyTP:          config.copyTP          !== false,
      active:          config.active          !== false,
      commandFile: config.commandFile ||
        path.join(CONFIG.MT5_COMMON_DIR, `commands_${accountId}.json`),
    };
    this.rawLiveData     = null;
    this.rawStaticData   = null;
    this.processedData   = null;
    this.eaStatus        = null;
    this.lastSeen        = null;
    this.online          = false;
    this.pendingCommands = new Map();
    this._offlineTimer   = null;
    this._pendingCommand = null;
    this.pushCounts      = { live: 0, static: 0, status: 0 };
    this.lastError       = null;

    // Track when settings were last fetched by the EA
    this.lastSettingsFetch = null;
  }
}

// ─── App Setup ────────────────────────────────────────────────────────────────
const getAccountOwner = (accountId) =>
  g_accountOwners.get(String(accountId)) || null;

const setAccountOwner = async (accountId, userId) => {
  if (!accountId || !userId) return null;
  const id = String(accountId);
  const existing = getAccountOwner(id);
  if (existing && existing !== userId) {
    const err = new Error(`Account ${id} is already linked to another user`);
    err.statusCode = 409;
    throw err;
  }
  if (existing === userId) return userId;

  g_accountOwners.set(id, userId);

  if (supabase) {
    const { error } = await dbFrom('tradevault_account_owners').upsert({
      account_id: id,
      user_id: userId,
      claimed_at: new Date().toISOString(),
    }, { onConflict: 'account_id' });
    if (error) throw error;
  }

  return userId;
};

const clearAccountOwner = (accountId) => {
  const id = String(accountId);
  g_accountOwners.delete(id);
  runDbTask(`delete account owner:${id}`, async () => {
    const { error } = await dbFrom('tradevault_account_owners')
      .delete()
      .eq('account_id', id);
    if (error) throw error;
  });
};

const ensureAccountOwner = async (req, res, accountId) => {
  const owner = getAccountOwner(accountId);
  if (!owner) {
    res.status(403).json({
      error: `Account ${accountId} is not linked to your user yet. Open Connect Account and claim/register it first.`,
    });
    return false;
  }
  if (owner !== req.user.id) {
    res.status(403).json({ error: `Account ${accountId} belongs to another user` });
    return false;
  }
  return true;
};

const userOwnsAccount = (userId, accountId) =>
  !!userId && getAccountOwner(accountId) === userId;

const getVisibleAccountIds = (userId) =>
  new Set([...g_accountOwners.entries()]
    .filter(([, owner]) => owner === userId)
    .map(([accountId]) => accountId));

const shareExpiryToDate = (expiry) => {
  const now = Date.now();
  if (expiry === '24h') return new Date(now + 24 * 60 * 60 * 1000).toISOString();
  if (expiry === '7d') return new Date(now + 7 * 24 * 60 * 60 * 1000).toISOString();
  return null;
};

const serializeShareLink = (row) => ({
  token: row.token,
  accountId: row.account_id,
  label: row.label || '',
  createdAt: row.created_at,
  expiresAt: row.expires_at,
  revokedAt: row.revoked_at,
});

const buildSharePayload = (accountId, fallbackSnapshot = null, label = '') => {
  const state = g_accounts.get(String(accountId));
  const snapshot = state?.processedData || fallbackSnapshot || null;
  const account = snapshot?.account || {};
  return {
    accountId: String(accountId),
    accountAlias:
      label ||
      state?.config?.alias ||
      account.name ||
      account.login ||
      String(accountId),
    online: !!state?.online,
    lastSeen: state?.lastSeen || null,
    generatedAt: Date.now(),
    snapshot,
  };
};

const app = express();
app.use(cors({ origin: '*' }));
app.use(express.json({ limit: '10mb' }));

const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  pingInterval: 5000,
  pingTimeout:  3000,
});

io.use(async (socket, next) => {
  if (!DATABASE_ENABLED) return next(new Error('Supabase is not configured'));
  const token = socket.handshake.auth?.token || socket.handshake.headers?.authorization?.replace(/^Bearer\s+/i, '');
  const user = await getAuthenticatedUser(token);
  if (!user) return next(new Error('Authentication required'));
  socket.user = sanitizeUser(user);
  upsertUserProfile(user);
  return next();
});

io.on('connection', (socket) => {
  const userId = socket.user.id;
  socket.join(`user:${userId}`);
  console.log(`[Socket.io] Client connected: ${socket.id} user=${userId}`);
  socket.emit('INIT', {
    type:      'INIT',
    data:      buildMultiAccountSnapshot(userId),
    timestamp: Date.now(),
  });
  socket.on('disconnect', () => {
    console.log(`[Socket.io] Client disconnected: ${socket.id}`);
  });
});

const broadcast = (type, data, accountId = null, userId = null) => {
  const payload = { type, data, accountId, timestamp: Date.now() };
  const ownerId = userId || (accountId ? getAccountOwner(accountId) : null);
  if (ownerId) return io.to(`user:${ownerId}`).emit(type, payload);
  if (accountId) return;
  io.emit(type, payload);
};

// ─── Account Registration ─────────────────────────────────────────────────────
const registerAccount = (accountId, config = {}) => {
  if (!g_accounts.has(accountId)) {
    const state = new AccountState(accountId, config);
    g_accounts.set(accountId, state);
    startOfflineDetector(state);
    console.log(`[Account] Registered: ${accountId} (${config.role || 'STANDALONE'})`);
  } else {
    const existing = g_accounts.get(accountId);
    const { role: incomingRole, ...rest } = config;
    Object.assign(existing.config, rest);
    // FIX A: Only update role if the account is not already MASTER or SLAVE
    if (incomingRole && existing.config.role === 'STANDALONE') {
      existing.config.role = incomingRole;
    }
  }
  persistAccountRegistry();
};

const loadAccountRegistry = () => {
  const store = loadJSON(CONFIG.ACCOUNTS_FILE);
  (store.accounts || []).forEach(acc => registerAccount(acc.accountId, acc.config));
};

const persistAccountRegistry = () => {
  const accounts = [];
  g_accounts.forEach((state, id) => {
    accounts.push({ accountId: id, config: state.config });
  });
  saveJSON(CONFIG.ACCOUNTS_FILE, { accounts });
};

// ─── Copy Pairs ───────────────────────────────────────────────────────────────
const loadCopyPairs = () => loadJSON(CONFIG.COPY_PAIRS_FILE).pairs || [];
const saveCopyPairs = (pairs) => saveJSON(CONFIG.COPY_PAIRS_FILE, { pairs });

const buildPairObject = (masterAccountId, slaveAccountId) => {
  const slaveState = g_accounts.get(slaveAccountId);
  if (!slaveState) return null;
  const cfg     = slaveState.config;
  const latency = g_copyLatency.get(slaveAccountId) || null;
  return {
    masterAccountId,
    slaveAccountId,
    lotMultiplier: cfg.lotMultiplier,
    copySL:        cfg.copySL,
    copyTP:        cfg.copyTP,
    active:        cfg.active !== false,
    createdAt:     cfg.pairCreatedAt || new Date().toISOString(),
    latency: latency ? {
      avg_ms:  latency.avg,
      min_ms:  latency.min === Infinity ? null : latency.min,
      max_ms:  latency.max,
      last_ms: latency.last,
      count:   latency.count,
    } : null,
  };
};

// ─── Offline Detector ─────────────────────────────────────────────────────────
const startOfflineDetector = (state) => {
  if (state._offlineTimer) clearInterval(state._offlineTimer);
  state._offlineTimer = setInterval(() => {
    if (!state.lastSeen) return;
    const ageMs = Date.now() - state.lastSeen;
    const thresholdMs = state.config?.source === 'metaapi'
      ? Math.max(CONFIG.EA_OFFLINE_THRESHOLD_S * 1000, CONFIG.METAAPI_SYNC_INTERVAL_MS * 3)
      : CONFIG.EA_OFFLINE_THRESHOLD_S * 1000;
    if (ageMs > thresholdMs && state.online) {
      state.online = false;
      scheduleAccountSnapshotPersist(state, 0);
      broadcast('EA_OFFLINE', { accountId: state.accountId }, state.accountId);
      console.log(`[Account] ${state.accountId} went offline (${(ageMs / 1000).toFixed(0)}s silent)`);
    }
  }, 2000);
};

const markEaOnline = (state) => {
  state.lastSeen = Date.now();
  if (!state.online) {
    state.online = true;
    broadcast('EA_ONLINE', { accountId: state.accountId }, state.accountId);
    console.log(`[Account] ${state.accountId} came online`);
  }
};

// ─── EA Router ────────────────────────────────────────────────────────────────
const persistAccountSnapshotNow = (state) => {
  if (!state || !supabase) return;
  const row = {
    account_id: state.accountId,
    owner_user_id: getAccountOwner(state.accountId),
    config: cloneJSON(state.config),
    raw_live_data: cloneJSON(state.rawLiveData),
    raw_static_data: cloneJSON(state.rawStaticData),
    processed_data: cloneJSON(state.processedData),
    ea_status: cloneJSON(state.eaStatus),
    push_counts: cloneJSON(state.pushCounts),
    last_seen_ms: state.lastSeen,
    last_settings_fetch_ms: state.lastSettingsFetch,
    last_error: cloneJSON(state.lastError),
    updated_at: new Date().toISOString(),
  };

  runDbTask(`persist account snapshot:${state.accountId}`, async () => {
    const { error } = await dbFrom('tradevault_account_snapshots')
      .upsert(row, { onConflict: 'account_id' });
    if (error) throw error;
  });
};

const buildHistorySignature = (processedData) => {
  const history = processedData?.trade_history || [];
  const openTickets = (processedData?.open_positions || [])
    .map(p => String(p.ticket || ''))
    .filter(Boolean)
    .sort()
    .join(',');
  if (!history.length) return `history-empty|open:${openTickets}`;
  const last = history.reduce((best, trade) => {
    const exitTime = Number(trade.exit_time || 0);
    const ticket = String(trade.deal_ticket || trade.position_id || trade.ticket || '');
    if (!best) return { exitTime, ticket };
    if (exitTime > best.exitTime) return { exitTime, ticket };
    if (exitTime === best.exitTime && ticket > best.ticket) return { exitTime, ticket };
    return best;
  }, null);
  return `history:${history.length}:${last?.exitTime || 0}:${last?.ticket || ''}|open:${openTickets}`;
};

const persistDurableTradingDataIfNeeded = (state, reason = 'live') => {
  if (!state?.processedData) return false;
  const accountId = state.accountId;
  const nextSig = buildHistorySignature(state.processedData);
  const prevSig = g_historySignatures.get(accountId);
  if (prevSig === nextSig) return false;

  g_historySignatures.set(accountId, nextSig);
  persistEquitySnapshot(accountId, state.processedData.account || {});
  saveJSON(CONFIG.INSIGHTS_FILE, {
    generated_at: Date.now(),
    account_id: accountId,
    insights: state.processedData.insights || [],
  });
  scheduleAccountSnapshotPersist(state, 0);
  console.log(`[Storage] Durable snapshot saved for ${accountId} (${reason}, history=${nextSig})`);
  return true;
};

const scheduleAccountSnapshotPersist = (state, delayMs = CONFIG.DB_WRITE_DEBOUNCE_MS) => {
  if (!state || !supabase) return;
  const existing = g_snapshotPersistTimers.get(state.accountId);
  if (existing) clearTimeout(existing);
  const timer = setTimeout(() => {
    g_snapshotPersistTimers.delete(state.accountId);
    persistAccountSnapshotNow(state);
  }, Math.max(0, delayMs));
  g_snapshotPersistTimers.set(state.accountId, timer);
};

const deleteAccountSnapshot = (accountId) => {
  if (!supabase) return;
  const existing = g_snapshotPersistTimers.get(accountId);
  if (existing) clearTimeout(existing);
  g_snapshotPersistTimers.delete(accountId);
  runDbTask(`delete account snapshot:${accountId}`, async () => {
    const { error } = await dbFrom('tradevault_account_snapshots')
      .delete()
      .eq('account_id', accountId);
    if (error) throw error;
  });
};

const eaRouter = express.Router();

const lookupEaKeyOwner = async (apiKey) => {
  if (!apiKey || !supabase) return null;
  const keyHash = hashSecret(apiKey);
  if (g_eaKeyOwners.has(keyHash)) return g_eaKeyOwners.get(keyHash);

  const { data, error } = await dbFrom('tradevault_user_ea_keys')
    .select('user_id')
    .eq('key_hash', keyHash)
    .is('revoked_at', null)
    .maybeSingle();
  if (error) {
    logDbError('lookup EA key', error);
    return null;
  }
  if (data?.user_id) {
    g_eaKeyOwners.set(keyHash, data.user_id);
    return data.user_id;
  }
  return null;
};

const ensureEaCanUseAccount = async (req, res, accountId) => {
  if (!req.eaUserId) return true;
  try {
    await setAccountOwner(accountId, req.eaUserId);
    return true;
  } catch (e) {
    res.status(e.statusCode || 403).json({ error: e.message });
    return false;
  }
};

eaRouter.use(async (req, res, next) => {
  const key = req.headers['x-api-key'] || req.query.apiKey;
  if (CONFIG.ALLOW_LEGACY_EA_KEY && key === CONFIG.API_KEY) {
    req.eaAuthType = 'legacy';
    req.eaUserId = null;
    return next();
  }

  const ownerId = await lookupEaKeyOwner(key);
  if (!ownerId) return res.status(401).json({ error: 'Unauthorized EA key' });
  req.eaAuthType = 'user_key';
  req.eaUserId = ownerId;
  next();
});

// ─── GET /ea/settings/:accountId ─────────────────────────────────────────────
//
//  EA calls this on init and every SettingsRefreshMs.
//
//  v5.1.0 policy enforced here:
//   • IncludeHistory / MaxHistoryDays are NOT sent — EA locks these itself.
//   • EnablePriceAlerts is sent as true always — EA also locks it, but we
//     stay consistent.
//   • EARole / MasterAccountId come exclusively from copy-pair config,
//     never from per-account settings file.
//
//  The server always auto-registers the account on first contact so the
//  settings fetch can succeed even before the first /ea/live push.

eaRouter.get('/settings/:accountId', async (req, res) => {
  const { accountId } = req.params;

  // Auto-register fresh accounts so the settings fetch always succeeds
  if (!g_accounts.has(accountId)) {
    registerAccount(accountId, { role: 'STANDALONE' });
  }
  if (!(await ensureEaCanUseAccount(req, res, accountId))) return;

  const state    = g_accounts.get(accountId);
  const settings = loadAccountSettings(accountId);

  // ── v5.1.0: enforce locked fields ────────────────────────────────────────
  // IncludeHistory and MaxHistoryDays are locked in the EA — omit them from
  // the response so old settings files don't accidentally override the lock.
  delete settings.IncludeHistory;
  delete settings.MaxHistoryDays;

  // Price alerts are always on in the EA; keep the value true for consistency.
  settings.EnablePriceAlerts = true;

  // Sync copy-trading fields from account config into settings response
  // so dashboard-managed copy pairs are reflected in settings automatically
  if (state.config.role === 'SLAVE' && state.config.masterAccountId) {
    settings.EARole          = 'SLAVE';
    settings.MasterAccountId = state.config.masterAccountId;
    settings.LotMultiplier   = state.config.lotMultiplier   || settings.LotMultiplier;
    settings.UseFixedLot     = state.config.useFixedLot     !== undefined ? state.config.useFixedLot : settings.UseFixedLot;
    settings.FixedLotSize    = state.config.fixedLotSize    || settings.FixedLotSize;
    settings.CopyStopLoss    = state.config.copySL          !== undefined ? state.config.copySL      : settings.CopyStopLoss;
    settings.CopyTakeProfit  = state.config.copyTP          !== undefined ? state.config.copyTP      : settings.CopyTakeProfit;
  } else if (state.config.role === 'MASTER') {
    settings.EARole = 'MASTER';
  } else {
    settings.EARole          = 'STANDALONE';
    settings.MasterAccountId = '';
  }

  // Record when the EA last fetched settings
  state.lastSettingsFetch = Date.now();

  console.log(`[Settings] Fetched by EA — account: ${accountId} | role: ${settings.EARole}`);

  res.json(settings);
});

// POST /ea/live
eaRouter.post('/live', async (req, res) => {
  const { accountId } = req.query;
  if (!accountId) return res.status(400).json({ error: 'accountId required' });

  if (!g_accounts.has(accountId)) registerAccount(accountId, { role: 'STANDALONE' });
  if (!(await ensureEaCanUseAccount(req, res, accountId))) return;

  const state = g_accounts.get(accountId);
  markEaOnline(state);
  state.rawLiveData = req.body;
  state.pushCounts.live++;

  const merged = {
    ...(state.rawStaticData || {}),
    ...req.body,
    meta: {
      ...((state.rawStaticData || {}).meta || {}),
      ...(req.body.meta || {}),
    },
  };

  try {
    state.processedData = processData(merged, accountId);
    broadcast('FULL_UPDATE', state.processedData, accountId);

    if (state.config.role === 'MASTER') {
      diffMasterPositions(state);
    }

    persistDurableTradingDataIfNeeded(state, 'live');
    res.json({ ok: true, push_count: state.pushCounts.live });
  } catch (err) {
    state.lastError = { context: 'live', message: err.message, time: Date.now() };
    console.error(`[EA /live] Error for ${accountId}:`, err.message);
    res.status(500).json({ error: 'Processing error', message: err.message });
  }
});

// POST /ea/static
eaRouter.post('/static', async (req, res) => {
  const { accountId } = req.query;
  if (!accountId) return res.status(400).json({ error: 'accountId required' });

  if (!g_accounts.has(accountId)) registerAccount(accountId, { role: 'STANDALONE' });
  if (!(await ensureEaCanUseAccount(req, res, accountId))) return;

  const state = g_accounts.get(accountId);
  markEaOnline(state);
  state.rawStaticData = req.body;
  state.pushCounts.static++;

  if (state.rawLiveData) {
    try {
      const merged = {
        ...req.body,
        ...state.rawLiveData,
        meta: { ...(req.body.meta || {}), ...(state.rawLiveData.meta || {}) },
      };
      state.processedData = processData(merged, accountId);
      broadcast('STATIC_UPDATE', state.processedData, accountId);
    } catch (err) {
      state.lastError = { context: 'static', message: err.message, time: Date.now() };
    }
  }

  persistDurableTradingDataIfNeeded(state, 'static');
  res.json({ ok: true, push_count: state.pushCounts.static });
});

// POST /ea/status
eaRouter.post('/status', async (req, res) => {
  const { accountId } = req.query;
  if (!accountId) return res.status(400).json({ error: 'accountId required' });

  if (!g_accounts.has(accountId)) registerAccount(accountId, { role: 'STANDALONE' });
  if (!(await ensureEaCanUseAccount(req, res, accountId))) return;

  const state = g_accounts.get(accountId);
  markEaOnline(state);
  state.eaStatus = req.body;
  state.pushCounts.status++;
  broadcast('STATUS_UPDATE', { accountId, status: req.body }, accountId);
  res.json({ ok: true, push_count: state.pushCounts.status });
});

// POST /ea/result
eaRouter.post('/result', async (req, res) => {
  const { accountId } = req.query;
  if (!accountId) return res.status(400).json({ error: 'accountId required' });

  const state = g_accounts.get(accountId);
  if (!state) return res.status(404).json({ error: 'Account not found' });
  if (!(await ensureEaCanUseAccount(req, res, accountId))) return;

  const result = req.body;
  if (result.command_id) {
    broadcast('COMMAND_RESULT', { accountId, result }, accountId);
    state.pendingCommands.delete(result.command_id);

    // Log RELOAD_SETTINGS outcomes so the dashboard can observe them
    if (result.action === 'RELOAD_SETTINGS') {
      console.log(`[Settings] EA reload result — account: ${accountId} | success: ${result.success} | message: ${result.message}`);
      broadcast('SETTINGS_RELOADED', { accountId, success: result.success, message: result.message }, accountId);
    }
  }

  const eventTs = parseInt(req.headers['x-event-ts'] || '0', 10);
  if (eventTs && state.config.role === 'SLAVE') {
    recordLatency(accountId, eventTs);
  }

  res.json({ ok: true });
});

// POST /ea/alert-triggered
eaRouter.post('/alert-triggered', async (req, res) => {
  const { accountId } = req.query;
  if (!accountId) return res.status(400).json({ error: 'accountId required' });
  if (!(await ensureEaCanUseAccount(req, res, accountId))) return;
  const { triggered = [] } = req.body;
  triggered.forEach(payload => {
    broadcast('alert_triggered', { accountId, payload });
  });
  res.json({ ok: true });
});

// POST /ea/missing-symbol
eaRouter.post('/missing-symbol', async (req, res) => {
  const { accountId } = req.query;
  if (!accountId) return res.status(400).json({ error: 'accountId required' });
  if (!(await ensureEaCanUseAccount(req, res, accountId))) return;
  const { symbol } = req.body;
  if (symbol) broadcast('symbol_missing', { accountId, symbol });
  res.json({ ok: true });
});

// GET /ea/commands/:accountId
eaRouter.get('/commands/:accountId', async (req, res) => {
  const { accountId } = req.params;
  const state = g_accounts.get(accountId);
  if (!state) return res.status(404).json({ error: 'Account not found' });
  if (!(await ensureEaCanUseAccount(req, res, accountId))) return;

  if (state._pendingCommand) {
    const cmd = state._pendingCommand;
    state._pendingCommand = null;
    return res.json(cmd);
  }
  res.json({ action: 'NONE' });
});

// GET /ea/copy-queue/:masterAccountId
eaRouter.get('/copy-queue/:masterAccountId', async (req, res) => {
  const { masterAccountId }               = req.params;
  const { slaveAccountId, lastEventId, echoTs } = req.query;

  if (!slaveAccountId) {
    return res.status(400).json({ error: 'slaveAccountId query param required' });
  }
  if (!(await ensureEaCanUseAccount(req, res, slaveAccountId))) return;
  if (req.eaUserId && !userOwnsAccount(req.eaUserId, masterAccountId)) {
    return res.status(403).json({ error: `Master account ${masterAccountId} belongs to another user` });
  }

  const masterState = g_accounts.get(masterAccountId);
  if (!masterState) {
    return res.status(404).json({ error: `Master account ${masterAccountId} not found` });
  }

  const slaveState = g_accounts.get(slaveAccountId);
  if (!slaveState) {
    return res.status(404).json({ error: `Slave account ${slaveAccountId} not found` });
  }

  if (echoTs) recordLatency(slaveAccountId, parseInt(echoTs, 10));

  if (slaveState.config.active === false) {
    return res.json({ events: [], serverTime: Date.now(), queueDepth: 0 });
  }

  const queue  = getCopyQueue(slaveAccountId);
  const events = drainQueue(queue, lastEventId || null);

  if (lastEventId) {
    const ackIdx = queue.events.findIndex(e => e.id === lastEventId);
    if (ackIdx >= 0) {
      const acked = queue.events.slice(0, ackIdx + 1);
      for (const ev of acked) {
        if (ev.event === 'CLOSE') {
          queue.seenTickets.delete(String(ev.ticket));
          queue.pendingClose.delete(String(ev.ticket));
          queue.lastPositions.delete(String(ev.ticket));
        }
      }
    }
  }

  if (events.length > 0) {
    console.log(`\n[COPY-QUEUE] Slave polled — ${events.length} event(s) | slave: ${slaveAccountId}`);
    events.forEach((ev, i) => {
      console.log(`    [${i}] id=${ev.id}  event=${ev.event}  ticket=${ev.ticket}`);
    });
  }

  res.json({ events, serverTime: Date.now(), queueDepth: queue.events.length });
});

// GET /ea/copy-sync/:masterAccountId
eaRouter.get('/copy-sync/:masterAccountId', async (req, res) => {
  const { masterAccountId } = req.params;
  const { slaveAccountId }  = req.query;

  if (!slaveAccountId) {
    return res.status(400).json({ error: 'slaveAccountId query param required' });
  }
  if (!(await ensureEaCanUseAccount(req, res, slaveAccountId))) return;
  if (req.eaUserId && !userOwnsAccount(req.eaUserId, masterAccountId)) {
    return res.status(403).json({ error: `Master account ${masterAccountId} belongs to another user` });
  }

  const masterState = g_accounts.get(masterAccountId);
  if (!masterState) {
    return res.status(404).json({ error: `Master account ${masterAccountId} not found` });
  }

  const slaveState = g_accounts.get(slaveAccountId);
  if (!slaveState) {
    return res.status(404).json({ error: `Slave account ${slaveAccountId} not found` });
  }

  g_copyQueues.delete(slaveAccountId);

  const positions = masterState.processedData?.open_positions || [];
  console.log(`[CopySync] Emergency sync — slave: ${slaveAccountId} | positions: ${positions.length}`);

  res.json({
    positions,
    serverTime: Date.now(),
    note: 'Queue reset. Slave should rebuild copyMap from this position set.',
  });
});

// DEPRECATED: GET /ea/copy-events/:masterAccountId
eaRouter.get('/copy-events/:masterAccountId', async (req, res) => {
  const { masterAccountId } = req.params;
  const { slaveAccountId }  = req.query;
  if (!slaveAccountId) return res.status(400).json({ error: 'slaveAccountId required' });
  if (!(await ensureEaCanUseAccount(req, res, slaveAccountId))) return;
  if (req.eaUserId && !userOwnsAccount(req.eaUserId, masterAccountId)) {
    return res.status(403).json({ error: `Master account ${masterAccountId} belongs to another user` });
  }
  const masterState = g_accounts.get(masterAccountId);
  if (!masterState)  return res.status(404).json({ error: `Master ${masterAccountId} not found` });
  const slaveState  = g_accounts.get(slaveAccountId);
  if (!slaveState)   return res.status(404).json({ error: `Slave ${slaveAccountId} not found` });
  if (slaveState.config.active === false) return res.json({ events: [], deprecated: true });
  const queue  = getCopyQueue(slaveAccountId);
  const events = drainQueue(queue, null);
  res.json({ events, deprecated: true, hint: 'Upgrade EA to use /ea/copy-queue' });
});

app.use('/ea', eaRouter);

// ─── Command Dispatcher ───────────────────────────────────────────────────────
const sendCommand = (accountId, command) => {
  const state = g_accounts.get(accountId);
  if (!state) return { error: `Account ${accountId} not registered` };
  if (state.config?.source === 'metaapi') {
    return { error: 'Direct MetaTrader accounts are read-only in this version. Use the EA connection for website trading commands.' };
  }

  const commandId   = `cmd_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
  const fullCommand = {
    command_id: commandId,
    account_id: accountId,
    timestamp:  Math.floor(Date.now() / 1000),
    ...command,
  };

  state._pendingCommand = fullCommand;

  if (CONFIG.MT5_COMMON_DIR && fs.existsSync(CONFIG.MT5_COMMON_DIR)) {
    try {
      fs.writeFileSync(state.config.commandFile, JSON.stringify(fullCommand, null, 2));
    } catch (e) {
      console.warn(`[CMD] Could not write command file for ${accountId}: ${e.message}`);
    }
  }

  logCommand(fullCommand);
  console.log(`[CMD] → ${accountId}: ${command.action} (${commandId})`);
  return { success: true, command_id: commandId, command: fullCommand };
};

const logCommand = (cmd) => {
  const store    = loadJSON(CONFIG.COMMAND_LOG_FILE);
  const commands = store.commands || [];
  commands.unshift({ ...cmd, logged_at: Date.now() });
  if (commands.length > 500) commands.splice(500);
  saveJSON(CONFIG.COMMAND_LOG_FILE, { commands });
};

// ─── Data Processing Pipeline ─────────────────────────────────────────────────
const processData = (raw, accountId) => {
  if (!raw) return null;

  const history       = raw.trade_history  || [];
  const openPositions = raw.open_positions || [];
  const account       = raw.account        || {};
  const meta          = raw.meta           || {};

  const journal       = loadJSON(CONFIG.JOURNAL_FILE);
  const accountOwner  = getAccountOwner(accountId);
  const journalEntries= accountOwner
    ? (journal.entries || []).filter(e => e.user_id === accountOwner)
    : [];
  const coreAnalytics = computeCoreAnalytics(history);
  const equityCurve   = buildEquityCurve(history, account.balance);
  const sessionStats  = computeSessionStats(history);
  const symbolStats   = computeSymbolStats(history);
  const dowStats      = computeDayOfWeekStats(history);
  const streakData    = computeStreakData(history);
  const periodReturns = computePeriodReturns(history);
  const drawdownCurve = buildDrawdownCurve(history, account.balance);
  const tradeDistrib  = buildTradeDistribution(history);
  const enriched      = enrichHistoryWithJournal(history, journalEntries);
  const insights      = generateInsights({ history, sessionStats, symbolStats, coreAnalytics, dowStats, streakData, account });

  const accountConfig = g_accounts.get(accountId)?.config || {};
  const state         = g_accounts.get(accountId);

  return {
    meta: {
      ...meta,
      account_config: {
        role:           accountConfig.role || 'STANDALONE',
        master_account: accountConfig.masterAccountId || null,
        lot_multiplier: accountConfig.lotMultiplier || 1.0,
        copy_sl:        accountConfig.copySL,
        copy_tp:        accountConfig.copyTP,
      },
    },
    account,
    open_positions:  openPositions,
    pending_orders:  raw.pending_orders || [],
    trade_history:   enriched,
    analytics: {
      ...coreAnalytics,
      equity_curve:       equityCurve,
      drawdown_curve:     drawdownCurve,
      session_stats:      sessionStats,
      symbol_stats:       symbolStats,
      day_of_week_stats:  dowStats,
      streak_data:        streakData,
      period_returns:     periodReturns,
      trade_distribution: tradeDistrib,
    },
    insights,
    symbols:         raw.symbols      || [],
    risk_config:     raw.risk_config  || {},
    copy_config:     raw.copy_config  || {},
    journal_pending: getPendingJournalEntries(openPositions, journalEntries),
    ea_status:       state?.eaStatus  || null,
    server_meta: {
      processed_at:      Date.now(),
      data_age_seconds:  meta.timestamp ? Math.floor(Date.now() / 1000) - meta.timestamp : 0,
      ws_clients:        io.engine.clientsCount,
      ea_online:         state?.online || false,
      settings_loaded:   meta.settings_loaded || false,
      last_settings_fetch: state?.lastSettingsFetch || null,
    },
  };
};

// ─── Analytics ────────────────────────────────────────────────────────────────
// Direct MetaTrader login via MetaApi. This keeps the rest of the app on the
// same processedData shape used by EA-connected accounts.
const makeDirectAccountId = (platform, server, login) =>
  `direct_${String(platform || 'mt5').toLowerCase()}_${String(login || '').replace(/[^a-zA-Z0-9_-]/g, '')}_${publicHash(String(server || '').toLowerCase(), 8)}`;

const isoFromMetaApiTime = (value) => {
  if (!value) return new Date().toISOString();
  if (typeof value === 'string') return value;
  if (value instanceof Date) return value.toISOString();
  if (value.date) return value.date;
  return String(value);
};

const secondsFromTime = (value) => {
  const ts = Date.parse(isoFromMetaApiTime(value));
  return Number.isFinite(ts) ? Math.floor(ts / 1000) : Math.floor(Date.now() / 1000);
};

const directSessionFromTime = (value) => {
  const hour = new Date(secondsFromTime(value) * 1000).getUTCHours();
  if (hour >= 0 && hour < 7) return 'Asian';
  if (hour >= 7 && hour < 13) return 'London';
  if (hour >= 13 && hour < 20) return 'New York';
  return 'Off-Hours';
};

const directDayFromTime = (value) =>
  ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'][new Date(secondsFromTime(value) * 1000).getUTCDay()];

const directPositionType = (type) =>
  String(type || '').toUpperCase().includes('SELL') ? 'SELL' : 'BUY';

const normalizeDirectPosition = (position) => ({
  ticket: position.id || position.ticket || position.positionId,
  symbol: position.symbol || '',
  type: directPositionType(position.type),
  lots: Number(position.volume ?? position.lots ?? 0),
  open_price: Number(position.openPrice ?? position.open_price ?? 0),
  current_price: Number(position.currentPrice ?? position.current_price ?? position.openPrice ?? 0),
  sl: Number(position.stopLoss ?? position.sl ?? 0),
  tp: Number(position.takeProfit ?? position.tp ?? 0),
  profit: Number(position.profit ?? position.unrealizedProfit ?? 0),
  swap: Number(position.swap ?? 0),
  commission: Number(position.commission ?? 0),
  open_time_human: isoFromMetaApiTime(position.time || position.openTime),
  session: directSessionFromTime(position.time || position.openTime),
  magic: Number(position.magic ?? 0),
  comment: position.comment || '',
  is_copy_trade: false,
});

const normalizeDirectHistory = (deals) => {
  const byPosition = new Map();
  for (const deal of deals || []) {
    const positionId = String(deal.positionId || deal.position_id || deal.id || '');
    if (!positionId) continue;
    if (!byPosition.has(positionId)) byPosition.set(positionId, []);
    byPosition.get(positionId).push(deal);
  }

  const history = [];
  for (const [positionId, items] of byPosition.entries()) {
    const sorted = [...items].sort((a, b) => secondsFromTime(a.time) - secondsFromTime(b.time));
    const entry = sorted.find((d) => String(d.entryType || d.entry || '').toUpperCase().includes('IN')) || sorted[0];
    const exits = sorted.filter((d) => String(d.entryType || d.entry || '').toUpperCase().includes('OUT'));
    const exit = exits[exits.length - 1] || sorted[sorted.length - 1];
    const netProfit = sorted.reduce((sum, d) =>
      sum + Number(d.profit || 0) + Number(d.swap || 0) + Number(d.commission || 0), 0);
    if (!exit || exit === entry || !Number.isFinite(netProfit)) continue;

    const entrySec = secondsFromTime(entry.time);
    const exitSec = secondsFromTime(exit.time);
    history.push({
      deal_ticket: exit.id || exit.ticket || positionId,
      position_id: positionId,
      order_ticket: exit.orderId || entry.orderId || '',
      symbol: exit.symbol || entry.symbol || '',
      type: directPositionType(entry.type),
      lots: Number(entry.volume ?? exit.volume ?? 0),
      entry_price: Number(entry.price ?? entry.openPrice ?? 0),
      exit_price: Number(exit.price ?? exit.closePrice ?? 0),
      entry_time: entrySec,
      exit_time: exitSec,
      entry_time_human: isoFromMetaApiTime(entry.time),
      exit_time_human: isoFromMetaApiTime(exit.time),
      duration_minutes: Math.max(0, Math.round((exitSec - entrySec) / 60)),
      duration_human: `${Math.max(0, Math.round((exitSec - entrySec) / 60))}m`,
      net_profit: +netProfit.toFixed(2),
      commission: Number(exit.commission || 0),
      swap: Number(exit.swap || 0),
      pips: 0,
      is_win: netProfit > 0,
      session: directSessionFromTime(entry.time),
      day_of_week: directDayFromTime(exit.time),
      magic: Number(entry.magic ?? exit.magic ?? 0),
      comment: exit.comment || entry.comment || '',
      is_copy_trade: false,
    });
  }

  return history.sort((a, b) => b.exit_time - a.exit_time).slice(0, CONFIG.METAAPI_HISTORY_LIMIT);
};

const normalizeDirectAccountInfo = (info, row) => ({
  login: String(info.login ?? row.login),
  name: info.name || row.account_name || String(row.login),
  broker: info.broker || row.server,
  server: row.server,
  platform: String(row.platform || 'mt5').toUpperCase(),
  currency: info.currency || '',
  balance: Number(info.balance ?? 0),
  equity: Number(info.equity ?? info.balance ?? 0),
  margin: Number(info.margin ?? 0),
  free_margin: Number(info.freeMargin ?? info.free_margin ?? 0),
  leverage: info.leverage ? `1:${info.leverage}` : '',
  profit: Number(info.profit ?? 0),
});

const directClientPath = (metaapiAccountId, endpoint, query = '') =>
  `/users/current/accounts/${encodeURIComponent(metaapiAccountId)}${endpoint}${query}`;

const fetchDirectSnapshot = async (row) => {
  const accountId = row.metaapi_account_id;
  const nowSec = Math.floor(Date.now() / 1000);
  const start = new Date(Date.now() - CONFIG.METAAPI_HISTORY_DAYS * 24 * 60 * 60 * 1000).toISOString();
  const end = new Date().toISOString();

  const [{ payload: accountInfo }, { payload: positions }, { payload: deals }] = await Promise.all([
    metaApiRequest(CONFIG.METAAPI_CLIENT_API_URL, directClientPath(accountId, '/account-information')),
    metaApiRequest(CONFIG.METAAPI_CLIENT_API_URL, directClientPath(accountId, '/positions')),
    metaApiRequest(
      CONFIG.METAAPI_CLIENT_API_URL,
      directClientPath(
        accountId,
        '/history-deals/time',
        `/${encodeURIComponent(start)}/${encodeURIComponent(end)}?offset=0&limit=${CONFIG.METAAPI_HISTORY_LIMIT}`,
      ),
    ),
  ]);

  const dealItems = Array.isArray(deals) ? deals : (deals?.deals || deals?.items || []);
  const positionItems = Array.isArray(positions) ? positions : (positions?.positions || positions?.items || []);

  return {
    account: normalizeDirectAccountInfo(accountInfo || {}, row),
    open_positions: positionItems.map(normalizeDirectPosition),
    pending_orders: [],
    trade_history: normalizeDirectHistory(dealItems),
    symbols: [],
    risk_config: {},
    copy_config: {},
    meta: {
      timestamp: nowSec,
      source: 'metaapi',
      connection_method: 'direct',
      settings_loaded: true,
    },
  };
};

const persistDirectAccountRow = (row, patch = {}) => {
  const next = { ...row, ...patch, updated_at: new Date().toISOString() };
  g_directAccounts.set(next.account_id, next);
  runDbTask(`persist direct account:${next.account_id}`, async () => {
    const { error } = await dbFrom('tradevault_direct_mt_accounts')
      .upsert(next, { onConflict: 'account_id' });
    if (error) throw error;
  });
  return next;
};

const syncDirectAccount = async (accountId, options = {}) => {
  const id = String(accountId);
  const row = g_directAccounts.get(id);
  const state = g_accounts.get(id);
  if (!row || !state || !CONFIG.METAAPI_TOKEN) return null;
  if (g_directSyncInFlight.has(id)) return state.processedData;

  g_directSyncInFlight.add(id);
  const started = Date.now();
  try {
    const raw = await fetchDirectSnapshot(row);
    state.rawLiveData = raw;
    state.rawStaticData = null;
    state.processedData = processData(raw, id);
    state.eaStatus = {
      connected: true,
      last_heartbeat: new Date().toISOString(),
      latency_ms: Date.now() - started,
      trading_paused: false,
      execution_avg_ms: Date.now() - started,
      uptime: 'Cloud',
    };
    state.lastSeen = Date.now();
    state.online = true;
    state.lastError = null;
    state.pushCounts.live++;
    persistDurableTradingDataIfNeeded(state, 'direct');
    if (options.broadcast !== false) {
      broadcast('FULL_UPDATE', state.processedData, id);
      broadcast('EA_ONLINE', { accountId: id, status: state.eaStatus }, id);
    }
    persistDirectAccountRow(row, {
      connection_status: 'connected',
      state: 'DEPLOYED',
      last_sync_at: new Date().toISOString(),
      last_error: null,
    });
    return state.processedData;
  } catch (err) {
    state.lastError = { context: 'direct-sync', message: err.message, time: Date.now() };
    if (state.online) {
      state.online = false;
      broadcast('EA_OFFLINE', { accountId: id, status: { connected: false, last_error: err.message } }, id);
    }
    persistDirectAccountRow(row, {
      connection_status: 'error',
      last_error: err.message,
    });
    throw err;
  } finally {
    g_directSyncInFlight.delete(id);
  }
};

const scheduleDirectAccountSync = (accountId, delayMs = CONFIG.METAAPI_SYNC_INTERVAL_MS) => {
  const id = String(accountId);
  if (g_directSyncTimers.has(id)) clearTimeout(g_directSyncTimers.get(id));
  if (!CONFIG.METAAPI_TOKEN || !g_directAccounts.has(id)) return;
  const timer = setTimeout(async () => {
    try {
      await syncDirectAccount(id, { broadcast: true });
    } catch (err) {
      console.warn(`[Direct MT] Sync failed for ${id}: ${err.message}`);
    } finally {
      if (g_directAccounts.has(id)) scheduleDirectAccountSync(id);
    }
  }, Math.max(2000, delayMs));
  g_directSyncTimers.set(id, timer);
};

const stopDirectAccountSync = (accountId) => {
  const id = String(accountId);
  const timer = g_directSyncTimers.get(id);
  if (timer) clearTimeout(timer);
  g_directSyncTimers.delete(id);
  g_directSyncInFlight.delete(id);
};

const createMetaApiAccount = async ({ accountId, userId, platform, login, password, server, name }) => {
  const transactionId = g_directCreateTransactions.get(accountId) || crypto.randomBytes(16).toString('hex');
  g_directCreateTransactions.set(accountId, transactionId);
  const payload = {
    name,
    type: 'cloud-g2',
    login,
    password,
    server,
    platform: String(platform || 'mt5').toLowerCase(),
    magic: 0,
    manualTrades: true,
    metadata: {
      forexAnalyzerUserId: userId,
      createdBy: 'ForexAnalyzer Pro',
    },
  };
  const result = await metaApiRequest(CONFIG.METAAPI_PROVISIONING_URL, '/users/current/accounts', {
    method: 'POST',
    body: payload,
    transactionId,
  });
  if (result.payload?.id || result.payload?._id || result.payload?.accountId) {
    g_directCreateTransactions.delete(accountId);
  }
  return result.payload;
};

const deployMetaApiAccount = async (metaapiAccountId) => {
  await metaApiRequest(
    CONFIG.METAAPI_PROVISIONING_URL,
    `/users/current/accounts/${encodeURIComponent(metaapiAccountId)}/deploy`,
    { method: 'POST' },
  );
};

const removeMetaApiAccount = async (metaapiAccountId) => {
  if (!metaapiAccountId || !CONFIG.METAAPI_TOKEN) return;
  try {
    await metaApiRequest(
      CONFIG.METAAPI_PROVISIONING_URL,
      `/users/current/accounts/${encodeURIComponent(metaapiAccountId)}`,
      { method: 'DELETE' },
    );
  } catch (err) {
    console.warn(`[Direct MT] Could not remove MetaApi account ${metaapiAccountId}: ${err.message}`);
  }
};

const computeCoreAnalytics = (history) => {
  if (!history.length) return getEmptyAnalytics();
  let wins = 0, losses = 0, grossProfit = 0, grossLoss = 0;
  let bestTrade = null, worstTrade = null;
  let totalPips = 0, totalDuration = 0, totalRR = 0, rrCount = 0;

  for (const t of history) {
    const p = t.net_profit;
    if (p > 0) {
      wins++; grossProfit += p;
      if (!bestTrade || p > bestTrade.net_profit) bestTrade = t;
    } else {
      losses++; grossLoss += Math.abs(p);
      if (!worstTrade || p < worstTrade.net_profit) worstTrade = t;
    }
    totalPips     += t.pips || 0;
    totalDuration += t.duration_minutes || 0;
    if (t.risk_reward && t.risk_reward > 0) { totalRR += t.risk_reward; rrCount++; }
  }

  const total  = history.length;
  const avgWin = wins > 0 ? grossProfit / wins : 0;
  const avgLoss= losses > 0 ? grossLoss / losses : 0;

  return {
    total_trades: total, wins, losses,
    win_rate:          +((wins / total) * 100).toFixed(2),
    gross_profit:      +grossProfit.toFixed(2),
    gross_loss:        +grossLoss.toFixed(2),
    net_profit:        +(grossProfit - grossLoss).toFixed(2),
    profit_factor:     grossLoss > 0 ? +(grossProfit / grossLoss).toFixed(4) : 0,
    expectancy:        +((grossProfit - grossLoss) / total).toFixed(2),
    avg_win:           +avgWin.toFixed(2),
    avg_loss:          +avgLoss.toFixed(2),
    risk_reward_ratio: avgLoss > 0 ? +(avgWin / avgLoss).toFixed(2) : 0,
    avg_rr_planned:    rrCount > 0 ? +(totalRR / rrCount).toFixed(2) : 0,
    avg_pips:          total > 0 ? +(totalPips / total).toFixed(1) : 0,
    avg_duration_minutes: total > 0 ? Math.round(totalDuration / total) : 0,
    best_trade: bestTrade, worst_trade: worstTrade,
  };
};

const buildEquityCurve = (history, currentBalance) => {
  if (!history.length) return [];
  const sorted = [...history].sort((a, b) => a.exit_time - b.exit_time);
  let cumulative = 0;
  return sorted.map((t, i) => {
    cumulative += t.net_profit || 0;
    return {
      index:            i + 1,
      timestamp:        (t.exit_time || 0) * 1000,
      timestamp_human:  t.exit_time_human || '',
      balance:          +((currentBalance || 0) - (sorted.slice(-1)[0]?.net_profit || 0) + cumulative).toFixed(2),
      profit:           +(t.net_profit || 0).toFixed(2),
      symbol:           t.symbol,
      is_win:           t.is_win,
      cumulative_profit: +cumulative.toFixed(2),
    };
  });
};

const buildDrawdownCurve = (history, currentBalance) => {
  const curve = buildEquityCurve(history, currentBalance);
  let peak = curve[0]?.balance || 0;
  return curve.map(p => {
    if (p.balance > peak) peak = p.balance;
    const dd = peak > 0 ? ((peak - p.balance) / peak) * 100 : 0;
    return {
      ...p,
      peak:         +peak.toFixed(2),
      drawdown_pct: +dd.toFixed(4),
      drawdown_abs: +(peak - p.balance).toFixed(2),
    };
  });
};

const computeSessionStats = (history) => {
  const sessions = {};
  for (const t of history) {
    const s = t.session || 'Off-Hours';
    if (!sessions[s]) sessions[s] = { session: s, trades: 0, wins: 0, losses: 0, gross_profit: 0, gross_loss: 0, net_profit: 0 };
    sessions[s].trades++;
    const p = t.net_profit || 0;
    if (p > 0) { sessions[s].wins++; sessions[s].gross_profit += p; }
    else { sessions[s].losses++; sessions[s].gross_loss += Math.abs(p); }
    sessions[s].net_profit += p;
  }
  return Object.values(sessions).map(s => ({
    ...s,
    gross_profit:  +s.gross_profit.toFixed(2),
    gross_loss:    +s.gross_loss.toFixed(2),
    net_profit:    +s.net_profit.toFixed(2),
    win_rate:      s.trades > 0 ? +((s.wins / s.trades) * 100).toFixed(2) : 0,
    profit_factor: s.gross_loss > 0 ? +(s.gross_profit / s.gross_loss).toFixed(4) : 0,
  })).sort((a, b) => b.net_profit - a.net_profit);
};

const computeSymbolStats = (history) => {
  const symbols = {};
  for (const t of history) {
    const s = t.symbol;
    if (!symbols[s]) symbols[s] = { symbol: s, trades: 0, wins: 0, losses: 0, gross_profit: 0, gross_loss: 0, net_profit: 0, total_pips: 0 };
    symbols[s].trades++;
    const p = t.net_profit || 0;
    if (p > 0) { symbols[s].wins++; symbols[s].gross_profit += p; }
    else { symbols[s].losses++; symbols[s].gross_loss += Math.abs(p); }
    symbols[s].net_profit += p;
    symbols[s].total_pips += t.pips || 0;
  }
  return Object.values(symbols).map(s => ({
    ...s,
    gross_profit:  +s.gross_profit.toFixed(2),
    gross_loss:    +s.gross_loss.toFixed(2),
    net_profit:    +s.net_profit.toFixed(2),
    win_rate:      s.trades > 0 ? +((s.wins / s.trades) * 100).toFixed(2) : 0,
    avg_pips:      s.trades > 0 ? +(s.total_pips / s.trades).toFixed(1) : 0,
    profit_factor: s.gross_loss > 0 ? +(s.gross_profit / s.gross_loss).toFixed(4) : 0,
  })).sort((a, b) => b.net_profit - a.net_profit);
};

const computeDayOfWeekStats = (history) => {
  const days  = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'];
  const stats = {};
  days.forEach(d => { stats[d] = { day: d, trades: 0, wins: 0, losses: 0, net_profit: 0 }; });
  for (const t of history) {
    const d = t.day_of_week;
    if (!stats[d]) continue;
    stats[d].trades++;
    const p = t.net_profit || 0;
    if (p > 0) stats[d].wins++; else stats[d].losses++;
    stats[d].net_profit += p;
  }
  return days.map(d => ({
    ...stats[d],
    net_profit: +stats[d].net_profit.toFixed(2),
    win_rate:   stats[d].trades > 0 ? +((stats[d].wins / stats[d].trades) * 100).toFixed(2) : 0,
  }));
};

const computeStreakData = (history) => {
  const sorted = [...history].sort((a, b) => a.exit_time - b.exit_time);
  if (!sorted.length) return { streaks: [], max_win_streak: 0, max_loss_streak: 0, current_streak: null };
  const streaks = [];
  let curType = null, curCount = 0, curProfit = 0, curStart = 0;
  let maxWin = 0, maxLoss = 0;
  for (const t of sorted) {
    const type = t.is_win ? 'win' : 'loss';
    if (type !== curType) {
      if (curCount > 0) streaks.push({ type: curType, count: curCount, profit: +curProfit.toFixed(2), start: curStart });
      curType = type; curCount = 1; curProfit = t.net_profit || 0; curStart = t.exit_time;
    } else { curCount++; curProfit += t.net_profit || 0; }
    if (type === 'win' && curCount > maxWin) maxWin = curCount;
    if (type === 'loss' && curCount > maxLoss) maxLoss = curCount;
  }
  if (curCount > 0) streaks.push({ type: curType, count: curCount, profit: +curProfit.toFixed(2), start: curStart });
  return {
    streaks:         streaks.slice(-20),
    max_win_streak:  maxWin,
    max_loss_streak: maxLoss,
    current_streak:  streaks[streaks.length - 1] || null,
  };
};

const computePeriodReturns = (history) => {
  const monthly = {}, weekly = {};
  for (const t of history) {
    const d    = new Date((t.exit_time || 0) * 1000);
    const mKey = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
    const wKey = getISOWeek(d);
    if (!monthly[mKey]) monthly[mKey] = { period: mKey, profit: 0, trades: 0, wins: 0 };
    if (!weekly[wKey])  weekly[wKey]  = { period: wKey,  profit: 0, trades: 0, wins: 0 };
    monthly[mKey].profit += t.net_profit || 0; monthly[mKey].trades++; if (t.is_win) monthly[mKey].wins++;
    weekly[wKey].profit  += t.net_profit || 0; weekly[wKey].trades++;  if (t.is_win) weekly[wKey].wins++;
  }
  const toArr = (obj) =>
    Object.values(obj).map(p => ({
      ...p,
      profit:   +p.profit.toFixed(2),
      win_rate: p.trades > 0 ? +((p.wins / p.trades) * 100).toFixed(2) : 0,
    })).sort((a, b) => a.period.localeCompare(b.period));
  return { monthly: toArr(monthly), weekly: toArr(weekly) };
};

const buildTradeDistribution = (history) => {
  const buckets = [
    { label: 'Big Win (>100)',    count: 0 },
    { label: 'Win (20-100)',      count: 0 },
    { label: 'Small Win (0-20)',  count: 0 },
    { label: 'Small Loss (0-20)', count: 0 },
    { label: 'Loss (20-100)',     count: 0 },
    { label: 'Big Loss (>100)',   count: 0 },
  ];
  for (const t of history) {
    const p = t.net_profit || 0;
    if (p > 100) buckets[0].count++;
    else if (p > 20) buckets[1].count++;
    else if (p > 0)  buckets[2].count++;
    else if (p > -20)  buckets[3].count++;
    else if (p > -100) buckets[4].count++;
    else               buckets[5].count++;
  }
  return buckets;
};

const generateInsights = ({ history, sessionStats, symbolStats, coreAnalytics, dowStats, streakData, account }) => {
  const insights = [];
  const ts = Date.now();

  if (sessionStats.length > 0) {
    const best  = sessionStats[0];
    const worst = sessionStats[sessionStats.length - 1];
    insights.push({ id:`session_best_${ts}`, type:'session_best', severity:'success', message:`Best session: ${best.session} — $${best.net_profit} net (${best.win_rate}% WR)`, context:{session:best.session}, generated_at:ts });
    if (worst.net_profit < -100) insights.push({ id:`session_avoid_${ts}`, type:'session_avoid', severity:'warning', message:`Avoid ${worst.session}: $${worst.net_profit} net loss`, context:{session:worst.session}, generated_at:ts });
  }
  if (symbolStats.length > 0) {
    const best  = symbolStats[0];
    const worst = symbolStats[symbolStats.length - 1];
    insights.push({ id:`sym_best_${ts}`, type:'symbol_best', severity:'success', message:`Top symbol: ${best.symbol} — $${best.net_profit} net (${best.win_rate}% WR)`, context:{symbol:best.symbol}, generated_at:ts });
    if (worst.net_profit < -100) insights.push({ id:`sym_avoid_${ts}`, type:'symbol_avoid', severity:'warning', message:`${worst.symbol} is draining your account: $${worst.net_profit} net`, context:{symbol:worst.symbol}, generated_at:ts });
  }
  const a = coreAnalytics;
  if (a.win_rate > 60) insights.push({ id:`wr_high_${ts}`, type:'win_rate_high', severity:'success', message:`Win rate is strong at ${a.win_rate}%`, generated_at:ts });
  if (a.win_rate < 40 && a.total_trades > 10) insights.push({ id:`wr_low_${ts}`, type:'win_rate_low', severity:'danger', message:`Win rate is low at ${a.win_rate}%. Review your entries.`, generated_at:ts });
  if (a.profit_factor >= 2) insights.push({ id:`pf_exc_${ts}`, type:'pf_excellent', severity:'success', message:`Excellent profit factor: ${a.profit_factor}`, generated_at:ts });
  if (a.profit_factor > 0 && a.profit_factor < 1) insights.push({ id:`pf_neg_${ts}`, type:'pf_negative', severity:'danger', message:`Profit factor below 1.0 (${a.profit_factor}) — losing strategy`, generated_at:ts });
  const dd = account.current_drawdown_pct || 0;
  if (dd > 15) insights.push({ id:`dd_high_${ts}`, type:'drawdown_high', severity:'danger', message:`High drawdown: ${dd.toFixed(2)}%`, generated_at:ts });
  else if (dd > 8) insights.push({ id:`dd_mod_${ts}`, type:'drawdown_moderate', severity:'warning', message:`Moderate drawdown: ${dd.toFixed(2)}%`, generated_at:ts });
  const streak = streakData.current_streak;
  if (streak?.type === 'loss' && streak.count >= 3) insights.push({ id:`streak_${ts}`, type:'consec_losses', severity:'warning', message:`On a ${streak.count}-trade losing streak. Consider reducing size.`, generated_at:ts });
  if (dowStats.length > 0) {
    const bestDay = [...dowStats].sort((a, b) => b.net_profit - a.net_profit)[0];
    insights.push({ id:`bestday_${ts}`, type:'best_day', severity:'info', message:`Best day: ${bestDay.day} with $${bestDay.net_profit} avg profit`, generated_at:ts });
  }
  return insights;
};

// ─── Journal Helpers ──────────────────────────────────────────────────────────
const enrichHistoryWithJournal = (history, journalEntries) => {
  const jMap = {};
  for (const e of journalEntries) jMap[String(e.ticket)] = e;
  return history.map(t => ({
    ...t,
    journal: jMap[String(t.deal_ticket)] || jMap[String(t.position_id)] || null,
  }));
};

const getPendingJournalEntries = (openPositions, journalEntries) => {
  const journaledTickets = new Set(journalEntries.map(e => String(e.ticket)));
  return openPositions.filter(p => !journaledTickets.has(String(p.ticket)));
};

// ─── Equity Persistence ───────────────────────────────────────────────────────
const persistEquitySnapshot = (accountId, account) => {
  const store     = loadJSON(CONFIG.EQUITY_HISTORY_FILE);
  const snapshots = store.snapshots || [];
  const last      = snapshots[snapshots.length - 1];
  if (last && last.balance === account.balance && last.equity === account.equity) return;
  snapshots.push({
    account_id:      accountId,
    timestamp:       Math.floor(Date.now() / 1000),
    timestamp_ms:    Date.now(),
    balance:         account.balance,
    equity:          account.equity,
    profit:          account.profit,
    drawdown_pct:    account.current_drawdown_pct,
  });
  if (snapshots.length > 10000) snapshots.splice(0, snapshots.length - 10000);
  saveJSON(CONFIG.EQUITY_HISTORY_FILE, { snapshots });
};

// ─── Multi-Account Snapshot ───────────────────────────────────────────────────
const buildMultiAccountSnapshot = (userId = null) => {
  const result = {};
  const visible = userId ? getVisibleAccountIds(userId) : null;
  g_accounts.forEach((state, id) => {
    if (visible && !visible.has(id)) return;
    result[id] = {
      accountId: id,
      config:    state.config,
      online:    state.online,
      lastSeen:  state.lastSeen,
      data:      state.processedData,
      eaStatus:  state.eaStatus,
    };
  });
  return result;
};

// ─── REST: Health & Multi-Account ────────────────────────────────────────────
app.get('/health', (req, res) => res.json({
  status:     'ok',
  version:    '5.1.0',
  accounts:   g_accounts.size,
  ws_clients: io.engine.clientsCount,
  timestamp:  Date.now(),
  copy_queues: g_copyQueues.size,
  features: {
    web_settings:       true,
    copy_queue:         true,
    copy_sync:          true,
    latency_tracking:   true,
    locked_history:     true,   // v5.1.0: history always on, always unlimited
    locked_alerts:      true,   // v5.1.0: price alerts always on
    server_only_role:   true,   // v5.1.0: EARole set exclusively by server
    supabase_database:  DATABASE_ENABLED,
    direct_mt_login:    !!CONFIG.METAAPI_TOKEN,
  },
  ea_whitelist_instruction: `MT5: Tools → Options → Expert Advisors → Allow WebRequest → ${CONFIG.PUBLIC_BASE_URL}`,
}));

app.get('/api/health', (req, res) => res.json({
  status: 'ok',
  version: '5.1.0',
  accounts: g_accounts.size,
  supabase_database: DATABASE_ENABLED,
  timestamp: Date.now(),
}));

app.get('/public/share/:token', async (req, res) => {
  if (!supabase) return res.status(503).json({ error: 'Sharing is not configured' });

  const token = String(req.params.token || '');
  const { data: row, error } = await dbFrom('tradevault_share_links')
    .select('token,user_id,account_id,label,snapshot,created_at,expires_at,revoked_at')
    .eq('token', token)
    .maybeSingle();

  if (error) return res.status(500).json({ error: error.message });
  if (!row || row.revoked_at) return res.status(404).json({ error: 'Share link not found' });
  if (row.expires_at && new Date(row.expires_at).getTime() <= Date.now()) {
    return res.status(410).json({ error: 'Share link expired' });
  }

  res.json({
    link: serializeShareLink(row),
    payload: buildSharePayload(row.account_id, row.snapshot, row.label),
  });
});

app.use('/api', requireUser);

app.get('/api/auth/me', async (req, res) => {
  const keyRows = supabase
    ? await dbFrom('tradevault_user_ea_keys')
        .select('key_prefix,created_at,revoked_at')
        .eq('user_id', req.user.id)
        .is('revoked_at', null)
        .order('created_at', { ascending: false })
        .limit(1)
    : { data: [], error: null };

  if (keyRows.error) return res.status(500).json({ error: keyRows.error.message });
  res.json({
    user: req.user,
    eaKey: keyRows.data?.[0] || null,
  });
});

app.post('/api/auth/ea-key/rotate', async (req, res) => {
  const secret = `tvp_ea_${crypto.randomBytes(32).toString('hex')}`;
  const keyHash = hashSecret(secret);
  const keyPrefix = `${secret.slice(0, 12)}...${secret.slice(-6)}`;

  const { error: revokeError } = await dbFrom('tradevault_user_ea_keys')
    .update({ revoked_at: new Date().toISOString() })
    .eq('user_id', req.user.id)
    .is('revoked_at', null);
  if (revokeError) return res.status(500).json({ error: revokeError.message });

  const { error } = await dbFrom('tradevault_user_ea_keys').insert({
    user_id: req.user.id,
    key_hash: keyHash,
    key_prefix: keyPrefix,
    name: req.body?.name || 'Default EA key',
  });
  if (error) return res.status(500).json({ error: error.message });

  g_eaKeyOwners.set(keyHash, req.user.id);
  res.json({
    apiKey: secret,
    keyPrefix,
    message: 'Copy this key into the EAApiKey input. It is shown only once.',
  });
});

app.get('/api/share-links', async (req, res) => {
  const accountId = String(req.query.accountId || '');
  if (accountId && !userOwnsAccount(req.user.id, accountId)) {
    return res.status(403).json({ error: `Account ${accountId} is not linked to your user` });
  }

  let query = dbFrom('tradevault_share_links')
    .select('token,account_id,label,created_at,expires_at,revoked_at')
    .eq('user_id', req.user.id)
    .is('revoked_at', null)
    .order('created_at', { ascending: false });

  if (accountId) query = query.eq('account_id', accountId);

  const { data, error } = await query;
  if (error) return res.status(500).json({ error: error.message });
  res.json({ links: (data || []).map(serializeShareLink) });
});

app.post('/api/share-links', async (req, res) => {
  const { accountId, expiry = '7d', label = '' } = req.body || {};
  if (!accountId) return res.status(400).json({ error: 'accountId required' });
  if (!userOwnsAccount(req.user.id, accountId)) {
    return res.status(403).json({ error: `Account ${accountId} is not linked to your user` });
  }

  const state = g_accounts.get(String(accountId));
  const snapshot = state?.processedData || null;
  if (!snapshot) {
    return res.status(409).json({ error: 'This account has no dashboard data yet. Start the EA once, then create the share link.' });
  }

  const token = crypto.randomBytes(24).toString('hex');
  const row = {
    token,
    user_id: req.user.id,
    account_id: String(accountId),
    label: String(label || state?.config?.alias || accountId),
    snapshot: cloneJSON(snapshot),
    expires_at: shareExpiryToDate(expiry),
  };

  const { data, error } = await dbFrom('tradevault_share_links')
    .insert(row)
    .select('token,account_id,label,created_at,expires_at,revoked_at')
    .single();
  if (error) return res.status(500).json({ error: error.message });
  res.status(201).json({ link: serializeShareLink(data) });
});

app.delete('/api/share-links/:token', async (req, res) => {
  const token = String(req.params.token || '');
  const { data, error } = await dbFrom('tradevault_share_links')
    .update({ revoked_at: new Date().toISOString() })
    .eq('token', token)
    .eq('user_id', req.user.id)
    .select('token')
    .maybeSingle();
  if (error) return res.status(500).json({ error: error.message });
  if (!data) return res.status(404).json({ error: 'Share link not found' });
  res.json({ success: true });
});

app.get('/api/accounts', (req, res) => res.json(buildMultiAccountSnapshot(req.user.id)));

app.get('/api/direct-accounts', async (req, res) => {
  const rows = [...g_directAccounts.values()]
    .filter(row => row.user_id === req.user.id && !row.revoked_at)
    .map(row => ({
      accountId: row.account_id,
      metaapiAccountId: row.metaapi_account_id,
      platform: row.platform,
      login: row.login,
      server: row.server,
      accountName: row.account_name,
      passwordType: row.password_type,
      connectionStatus: row.connection_status,
      state: row.state,
      lastSyncAt: row.last_sync_at,
      lastError: row.last_error,
      createdAt: row.created_at,
    }));
  res.json({ accounts: rows, metaapiConfigured: !!CONFIG.METAAPI_TOKEN });
});

app.post('/api/direct-accounts/connect', async (req, res) => {
  if (!CONFIG.METAAPI_TOKEN) {
    return res.status(503).json({ error: 'Direct MT login is not configured yet. Set METAAPI_TOKEN on the backend.' });
  }

  const platform = String(req.body?.platform || 'mt5').toLowerCase();
  const login = String(req.body?.login || '').trim();
  const password = String(req.body?.password || '');
  const serverName = String(req.body?.server || '').trim();
  const accountName = String(req.body?.accountName || `${serverName} ${login}`).trim();
  const passwordType = String(req.body?.passwordType || 'investor').toLowerCase();
  const role = String(req.body?.role || 'STANDALONE').toUpperCase();

  if (!['mt4', 'mt5'].includes(platform)) return res.status(400).json({ error: 'platform must be mt4 or mt5' });
  if (!login) return res.status(400).json({ error: 'MetaTrader account number is required' });
  if (!serverName) return res.status(400).json({ error: 'Broker server name is required' });
  if (!password) return res.status(400).json({ error: 'MetaTrader password is required' });

  const accountId = makeDirectAccountId(platform, serverName, login);
  const owner = getAccountOwner(accountId);
  if (owner && owner !== req.user.id) {
    return res.status(409).json({ error: `Account ${login} on ${serverName} is already linked to another user` });
  }

  try {
    let row = g_directAccounts.get(accountId);
    if (!row) {
      const created = await createMetaApiAccount({
        accountId,
        userId: req.user.id,
        platform,
        login,
        password,
        server: serverName,
        name: accountName || `${serverName} ${login}`,
      });
      const metaapiAccountId = created?.id || created?._id || created?.accountId;
      if (!metaapiAccountId) {
        return res.status(202).json({
          pending: true,
          message: 'MetaApi accepted the account creation request. Try again in a minute if no account appears.',
        });
      }
      await deployMetaApiAccount(metaapiAccountId);

      row = {
        account_id: accountId,
        user_id: req.user.id,
        metaapi_account_id: metaapiAccountId,
        platform,
        login,
        server: serverName,
        account_name: accountName || `${serverName} ${login}`,
        password_type: passwordType,
        connection_status: 'deploying',
        state: 'DEPLOYING',
        last_sync_at: null,
        last_error: null,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
        revoked_at: null,
      };
      persistDirectAccountRow(row);
    }

    registerAccount(accountId, {
      alias: accountName || login,
      source: 'metaapi',
      connectionMethod: 'direct',
      platform: platform.toUpperCase(),
      broker: serverName,
      server: serverName,
      login,
      passwordType,
      metaapiAccountId: row.metaapi_account_id,
      role: ['MASTER', 'SLAVE', 'STANDALONE'].includes(role) ? role : 'STANDALONE',
    });
    await setAccountOwner(accountId, req.user.id);
    scheduleAccountSnapshotPersist(g_accounts.get(accountId), 0);

    let snapshot = null;
    try {
      snapshot = await syncDirectAccount(accountId, { broadcast: true });
    } catch (syncErr) {
      // MetaApi can need a little time after deploy before terminal state is readable.
      console.warn(`[Direct MT] Initial sync pending for ${accountId}: ${syncErr.message}`);
    }
    scheduleDirectAccountSync(accountId);

    res.status(snapshot ? 201 : 202).json({
      success: true,
      pending: !snapshot,
      accountId,
      account: {
        accountId,
        platform,
        login,
        server: serverName,
        connectionStatus: g_directAccounts.get(accountId)?.connection_status || 'deploying',
      },
      message: snapshot
        ? 'Direct MetaTrader account connected.'
        : 'Account saved. MetaApi is still deploying the terminal; data will appear shortly.',
    });
  } catch (err) {
    console.error(`[Direct MT] Connect failed for ${login}@${serverName}: ${err.message}`);
    return res.status(err.statusCode || 500).json({
      error: err.message,
      details: err.payload || null,
    });
  }
});

app.post('/api/direct-accounts/:accountId/sync', async (req, res) => {
  const { accountId } = req.params;
  if (!(await ensureAccountOwner(req, res, accountId))) return;
  if (!g_directAccounts.has(accountId)) return res.status(404).json({ error: 'Direct account not found' });
  try {
    const snapshot = await syncDirectAccount(accountId, { broadcast: true });
    scheduleDirectAccountSync(accountId);
    res.json({ success: true, accountId, snapshot });
  } catch (err) {
    res.status(err.statusCode || 500).json({ error: err.message, details: err.payload || null });
  }
});

app.post('/api/accounts/register', async (req, res) => {
  const { accountId, config } = req.body;
  if (!accountId) return res.status(400).json({ error: 'accountId required' });
  const owner = getAccountOwner(accountId);
  if (owner && owner !== req.user.id) {
    return res.status(409).json({ error: `Account ${accountId} is already linked to another user` });
  }
  registerAccount(accountId, config || {});
  try {
    await setAccountOwner(accountId, req.user.id);
  } catch (e) {
    return res.status(e.statusCode || 500).json({ error: e.message });
  }
  scheduleAccountSnapshotPersist(g_accounts.get(accountId), 0);
  res.json({ success: true, accountId });
});

app.put('/api/accounts/:accountId/config', (req, res) => {
  const state = getAccountState(req, res); if (!state) return;
  Object.assign(state.config, req.body);
  persistAccountRegistry();
  scheduleAccountSnapshotPersist(state, 0);
  res.json({ success: true, config: state.config });
});

app.post('/api/accounts/:accountId/reset', (req, res) => {
  const state = getAccountState(req, res); if (!state) return;
  state.rawLiveData   = null;
  state.rawStaticData = null;
  state.processedData = null;
  state.eaStatus      = null;
  state.lastSeen      = null;
  state.online        = false;
  state.pushCounts    = { live: 0, static: 0, status: 0 };
  state.lastError     = null;
  scheduleAccountSnapshotPersist(state, 0);
  res.json({ success: true });
});

app.delete('/api/accounts/:accountId', async (req, res) => {
  const { accountId } = req.params;
  if (!g_accounts.has(accountId)) return res.status(404).json({ error: 'Account not found' });
  if (!userOwnsAccount(req.user.id, accountId)) {
    return res.status(403).json({ error: `Account ${accountId} is not linked to your user` });
  }
  const state = g_accounts.get(accountId);
  if (state.config?.source === 'metaapi') {
    const row = g_directAccounts.get(accountId);
    stopDirectAccountSync(accountId);
    if (row) {
      await removeMetaApiAccount(row.metaapi_account_id);
      persistDirectAccountRow(row, {
        revoked_at: new Date().toISOString(),
        connection_status: 'disconnected',
      });
      g_directAccounts.delete(accountId);
    }
  }
  if (state._offlineTimer) clearInterval(state._offlineTimer);
  g_accounts.delete(accountId);
  persistAccountRegistry();
  clearAccountOwner(accountId);
  deleteAccountSnapshot(accountId);
  res.json({ success: true });
});

// ─── REST: Settings (NEW in v5.0.0) ──────────────────────────────────────────

/**
 * GET /api/settings/defaults
 * Returns the system-wide default EASettings object.
 * Useful for pre-populating the dashboard settings form.
 */
app.get('/api/settings/defaults', (req, res) => {
  res.json({ settings: DEFAULT_EA_SETTINGS });
});

/**
 * GET /api/accounts/:accountId/settings
 * Returns the current effective settings for an account
 * (stored overrides merged with defaults).
 */
app.get('/api/accounts/:accountId/settings', (req, res) => {
  const state = getAccountState(req, res);
  if (!state) return;
  const settings = loadAccountSettings(req.params.accountId);
  res.json({
    accountId:         req.params.accountId,
    settings,
    isDefault:         !settingsFileExists(req.params.accountId),
    lastSettingsFetch: state.lastSettingsFetch,
    savedAt:           settingsSavedAt(req.params.accountId),
  });
});

/**
 * PUT /api/accounts/:accountId/settings
 * Save settings for an account and immediately signal the EA to reload.
 *
 * Body: partial or full EASettings JSON.
 * Any fields omitted fall back to defaults on the EA side.
 *
 * After saving, a RELOAD_SETTINGS command is queued for the EA so changes
 * take effect within the next CommandPollMs cycle (~100 ms by default).
 */
app.put('/api/accounts/:accountId/settings', (req, res) => {
  const state = getAccountState(req, res);
  if (!state) return;

  const { accountId } = req.params;
  const incoming      = req.body;

  if (!incoming || typeof incoming !== 'object') {
    return res.status(400).json({ error: 'Request body must be a JSON settings object' });
  }

  // v5.1.0: silently strip locked fields — the EA ignores them anyway,
  // and storing them would create confusion in the dashboard.
  delete incoming.IncludeHistory;
  delete incoming.MaxHistoryDays;
  // EnablePriceAlerts is stored for display but the EA always forces true.

  // Validate numeric types for key safety-critical fields
  const numericFields = [
    'MaxDrawdownPct', 'MaxDailyLossPct', 'EquityProtectionPct',
    'MaxLotSize', 'MaxOpenTrades', 'LotMultiplier',
    'LivePushIntervalMs', 'CommandPollMs',
  ];
  for (const field of numericFields) {
    if (incoming[field] !== undefined && isNaN(Number(incoming[field]))) {
      return res.status(400).json({ error: `Field ${field} must be numeric` });
    }
  }

  const merged = saveAccountSettings(accountId, incoming);

  // Broadcast to dashboard so all open tabs see the change immediately
  broadcast('SETTINGS_UPDATED', { accountId, settings: merged }, accountId);

  // Signal the EA to reload settings from the server immediately
  const cmdResult = sendCommand(accountId, { action: 'RELOAD_SETTINGS' });

  console.log(`[Settings] Updated for ${accountId} — RELOAD_SETTINGS queued (${cmdResult.command_id})`);

  res.json({
    success:    true,
    accountId,
    settings:   merged,
    command_id: cmdResult.command_id,
    message:    'Settings saved. EA will reload within the next poll cycle.',
  });
});

/**
 * POST /api/accounts/:accountId/settings/reset
 * Deletes the per-account settings file, reverting the EA to defaults.
 * Also queues a RELOAD_SETTINGS command.
 */
app.post('/api/accounts/:accountId/settings/reset', (req, res) => {
  const state = getAccountState(req, res);
  if (!state) return;

  const { accountId } = req.params;
  resetAccountSettings(accountId);

  broadcast('SETTINGS_UPDATED', { accountId, settings: DEFAULT_EA_SETTINGS }, accountId);

  const cmdResult = sendCommand(accountId, { action: 'RELOAD_SETTINGS' });

  res.json({
    success:    true,
    accountId,
    settings:   DEFAULT_EA_SETTINGS,
    command_id: cmdResult.command_id,
    message:    'Settings reset to defaults. EA will reload within the next poll cycle.',
  });
});

// ─── REST: Dashboard & Data ───────────────────────────────────────────────────
const getAccountState = (req, res) => {
  const accountId = req.params.accountId || req.query.accountId;
  if (!accountId) { res.status(400).json({ error: 'accountId required' }); return null; }
  const state = g_accounts.get(accountId);
  if (!state) { res.status(404).json({ error: `Account ${accountId} not found` }); return null; }
  if (req.user && !userOwnsAccount(req.user.id, accountId)) {
    res.status(403).json({ error: `Account ${accountId} is not linked to your user` });
    return null;
  }
  return state;
};

app.get('/api/accounts/:accountId/dashboard', (req, res) => {
  const state = getAccountState(req, res); if (!state) return;
  if (!state.processedData) return res.status(503).json({
    error:     'No data yet — EA has not pushed any data.',
    diagnosis: { account_online: state.online, last_seen: state.lastSeen, push_counts: state.pushCounts, last_error: state.lastError },
  });
  res.json(state.processedData);
});

app.get('/api/accounts/:accountId/status',    (req, res) => { const s = getAccountState(req,res); if(!s) return; res.json({ online:s.online, lastSeen:s.lastSeen, eaStatus:s.eaStatus, pushCounts:s.pushCounts, lastError:s.lastError, lastSettingsFetch:s.lastSettingsFetch }); });
app.get('/api/accounts/:accountId/positions', (req, res) => { const s = getAccountState(req,res); if(!s) return; res.json(s.processedData?.open_positions || []); });
app.get('/api/accounts/:accountId/analytics', (req, res) => { const s = getAccountState(req,res); if(!s) return; res.json(s.processedData?.analytics || {}); });

app.get('/api/accounts/:accountId/history', (req, res) => {
  const state = getAccountState(req, res); if (!state) return;
  let history  = state.processedData?.trade_history || [];
  const { symbol, session, day, limit = 100, offset = 0 } = req.query;
  if (symbol)  history = history.filter(t => t.symbol      === symbol);
  if (session) history = history.filter(t => t.session     === session);
  if (day)     history = history.filter(t => t.day_of_week === day);
  const sorted = [...history].sort((a, b) => b.exit_time - a.exit_time);
  res.json({ total: sorted.length, data: sorted.slice(+offset, +offset + +limit) });
});

// ─── REST: Commands ───────────────────────────────────────────────────────────
app.post('/api/accounts/:accountId/command',               (req, res) => { const s = getAccountState(req,res); if(!s) return; const r = sendCommand(req.params.accountId, req.body); if(r.error) return res.status(500).json(r); res.json(r); });
app.post('/api/accounts/:accountId/open',                  (req, res) => { const s = getAccountState(req,res); if(!s) return; res.json(sendCommand(req.params.accountId, { action:'OPEN_TRADE',    ...req.body })); });
app.post('/api/accounts/:accountId/close/:ticket',         (req, res) => { const s = getAccountState(req,res); if(!s) return; res.json(sendCommand(req.params.accountId, { action:'CLOSE_TRADE',   ticket:req.params.ticket })); });
app.post('/api/accounts/:accountId/close-all',             (req, res) => { const s = getAccountState(req,res); if(!s) return; res.json(sendCommand(req.params.accountId, { action:'CLOSE_ALL' })); });
app.post('/api/accounts/:accountId/modify/:ticket',        (req, res) => { const s = getAccountState(req,res); if(!s) return; res.json(sendCommand(req.params.accountId, { action:'MODIFY_TRADE',  ticket:req.params.ticket, ...req.body })); });
app.post('/api/accounts/:accountId/partial-close/:ticket', (req, res) => { const s = getAccountState(req,res); if(!s) return; res.json(sendCommand(req.params.accountId, { action:'PARTIAL_CLOSE', ticket:req.params.ticket, ...req.body })); });
app.post('/api/accounts/:accountId/pause',                 (req, res) => { const s = getAccountState(req,res); if(!s) return; res.json(sendCommand(req.params.accountId, { action:'PAUSE_TRADING' })); });
app.post('/api/accounts/:accountId/resume',                (req, res) => { const s = getAccountState(req,res); if(!s) return; res.json(sendCommand(req.params.accountId, { action:'RESUME_TRADING' })); });
app.post('/api/accounts/:accountId/breakeven/:ticket',     (req, res) => { const s = getAccountState(req,res); if(!s) return; res.json(sendCommand(req.params.accountId, { action:'SET_BREAKEVEN', ticket:req.params.ticket })); });
app.post('/api/accounts/:accountId/trail/:ticket',         (req, res) => { const s = getAccountState(req,res); if(!s) return; res.json(sendCommand(req.params.accountId, { action:'TRAIL_STOP',    ticket:req.params.ticket, sl:req.body.trail_pips })); });
app.get('/api/commands', (req, res) => {
  const store = loadJSON(CONFIG.COMMAND_LOG_FILE);
  const commands = (store.commands || [])
    .filter(cmd => !cmd.account_id || userOwnsAccount(req.user.id, cmd.account_id));
  res.json({ commands: commands.slice(0, +(req.query.limit || 50)) });
});

// ─── REST: Copy Trading ───────────────────────────────────────────────────────
app.get('/api/copy/pairs', (req, res) => {
  const pairs = [];
  g_accounts.forEach((state, id) => {
    if (!userOwnsAccount(req.user.id, id)) return;
    if (state.config.role === 'SLAVE' && state.config.masterAccountId) {
      pairs.push(buildPairObject(state.config.masterAccountId, id));
    }
  });
  res.json({ pairs: pairs.filter(Boolean) });
});

app.get('/api/copy/latency', (req, res) => {
  const result = {};
  g_copyLatency.forEach((stats, slaveId) => {
    if (!userOwnsAccount(req.user.id, slaveId)) return;
    result[slaveId] = {
      avg_ms:  stats.avg,
      min_ms:  stats.min === Infinity ? null : stats.min,
      max_ms:  stats.max,
      last_ms: stats.last,
      samples: stats.count,
    };
  });
  res.json({ latency: result });
});

app.get('/api/copy/queue-stats', (req, res) => {
  const result = {};
  g_copyQueues.forEach((queue, slaveId) => {
    if (!userOwnsAccount(req.user.id, slaveId)) return;
    result[slaveId] = {
      depth:         queue.events.length,
      seen_tickets:  queue.seenTickets.size,
      pending_close: queue.pendingClose.size,
    };
  });
  res.json({ queues: result });
});

app.post('/api/copy/pairs', (req, res) => {
  const { masterAccountId, slaveAccountId, lotMultiplier = 1.0, copySL = true, copyTP = true } = req.body;

  if (!masterAccountId || !slaveAccountId) return res.status(400).json({ error: 'masterAccountId and slaveAccountId required' });
  if (masterAccountId === slaveAccountId)   return res.status(400).json({ error: 'masterAccountId and slaveAccountId must differ' });
  if (!g_accounts.has(masterAccountId))     return res.status(400).json({ error: `Master account ${masterAccountId} not registered` });
  if (!g_accounts.has(slaveAccountId))      return res.status(400).json({ error: `Slave account ${slaveAccountId} not registered` });
  if (!userOwnsAccount(req.user.id, masterAccountId) || !userOwnsAccount(req.user.id, slaveAccountId)) {
    return res.status(403).json({ error: 'Both copy-pair accounts must belong to your user' });
  }

  const masterState = g_accounts.get(masterAccountId);
  const slaveState  = g_accounts.get(slaveAccountId);

  masterState.config.role             = 'MASTER';
  slaveState.config.role              = 'SLAVE';
  slaveState.config.masterAccountId   = masterAccountId;
  slaveState.config.lotMultiplier     = lotMultiplier;
  slaveState.config.copySL            = copySL;
  slaveState.config.copyTP            = copyTP;
  slaveState.config.active            = true;
  slaveState.config.pairCreatedAt     = new Date().toISOString();

  persistAccountRegistry();
  scheduleAccountSnapshotPersist(masterState, 0);
  scheduleAccountSnapshotPersist(slaveState, 0);
  g_copyQueues.delete(slaveAccountId);
  g_copyLatency.delete(slaveAccountId);

  // Sync copy settings into the slave's settings file
  const existingSettings = loadAccountSettings(slaveAccountId);
  saveAccountSettings(slaveAccountId, {
    ...existingSettings,
    EARole:          'SLAVE',
    MasterAccountId: masterAccountId,
    LotMultiplier:   lotMultiplier,
    CopyStopLoss:    copySL,
    CopyTakeProfit:  copyTP,
  });

  if (slaveState.processedData) broadcast('STATIC_UPDATE', slaveState.processedData, slaveAccountId);
  sendCommand(slaveAccountId, { action: 'RESUME_TRADING' });
  sendCommand(slaveAccountId, { action: 'RELOAD_SETTINGS' });

  return res.status(201).json({ ok: true, pair: buildPairObject(masterAccountId, slaveAccountId) });
});

app.put('/api/copy/pairs/:slaveAccountId', (req, res) => {
  const { slaveAccountId } = req.params;
  const slaveState = g_accounts.get(slaveAccountId);
  if (!slaveState) return res.status(404).json({ error: `Slave account ${slaveAccountId} not found` });
  if (!userOwnsAccount(req.user.id, slaveAccountId)) return res.status(403).json({ error: `Account ${slaveAccountId} is not linked to your user` });
  if (slaveState.config.role !== 'SLAVE') return res.status(400).json({ error: `Account ${slaveAccountId} is not a SLAVE` });

  const { lotMultiplier, copySL, copyTP, active } = req.body;
  const wasActive = slaveState.config.active !== false;

  if (lotMultiplier !== undefined) slaveState.config.lotMultiplier = lotMultiplier;
  if (copySL        !== undefined) slaveState.config.copySL        = copySL;
  if (copyTP        !== undefined) slaveState.config.copyTP        = copyTP;
  if (active        !== undefined) slaveState.config.active        = active;

  persistAccountRegistry();
  scheduleAccountSnapshotPersist(slaveState, 0);

  // Keep settings file in sync
  const existing = loadAccountSettings(slaveAccountId);
  const patch    = {};
  if (lotMultiplier !== undefined) patch.LotMultiplier  = lotMultiplier;
  if (copySL        !== undefined) patch.CopyStopLoss   = copySL;
  if (copyTP        !== undefined) patch.CopyTakeProfit = copyTP;
  if (Object.keys(patch).length) {
    saveAccountSettings(slaveAccountId, { ...existing, ...patch });
    sendCommand(slaveAccountId, { action: 'RELOAD_SETTINGS' });
  }

  if (active !== undefined) {
    const nowActive = active === true || active === 'true';
    if (wasActive && !nowActive) {
      sendCommand(slaveAccountId, { action: 'PAUSE_TRADING' });
      console.log(`[Copy] Pair paused — slave: ${slaveAccountId}`);
    } else if (!wasActive && nowActive) {
      sendCommand(slaveAccountId, { action: 'RESUME_TRADING' });
      console.log(`[Copy] Pair resumed — slave: ${slaveAccountId}`);
    }
  }

  if (slaveState.processedData) broadcast('STATIC_UPDATE', slaveState.processedData, slaveAccountId);
  return res.json({ ok: true, pair: buildPairObject(slaveState.config.masterAccountId, slaveAccountId) });
});

app.delete('/api/copy/pairs/:slaveAccountId', (req, res) => {
  const { slaveAccountId } = req.params;
  const slaveState = g_accounts.get(slaveAccountId);
  if (!slaveState) return res.status(404).json({ error: `Slave account ${slaveAccountId} not found` });
  if (!userOwnsAccount(req.user.id, slaveAccountId)) return res.status(403).json({ error: `Account ${slaveAccountId} is not linked to your user` });

  slaveState.config.role            = 'STANDALONE';
  slaveState.config.masterAccountId = null;
  slaveState.config.active          = true;
  slaveState.config.pairCreatedAt   = null;

  // Update settings file to reflect the role change
  const existing = loadAccountSettings(slaveAccountId);
  saveAccountSettings(slaveAccountId, { ...existing, EARole: 'STANDALONE', MasterAccountId: '' });

  g_copyQueues.delete(slaveAccountId);
  g_copyLatency.delete(slaveAccountId);
  persistAccountRegistry();
  scheduleAccountSnapshotPersist(slaveState, 0);

  sendCommand(slaveAccountId, { action: 'RELOAD_SETTINGS' });

  if (slaveState.processedData) broadcast('STATIC_UPDATE', slaveState.processedData, slaveAccountId);
  return res.json({ ok: true });
});

// ─── REST: Alerts ─────────────────────────────────────────────────────────────
app.get('/api/alerts/:accountId', (req, res) => {
  if (!userOwnsAccount(req.user.id, req.params.accountId)) {
    return res.status(403).json({ error: `Account ${req.params.accountId} is not linked to your user` });
  }
  res.json(loadAccountAlerts(req.params.accountId));
});

app.post('/api/alerts/:accountId', (req, res) => {
  if (!userOwnsAccount(req.user.id, req.params.accountId)) {
    return res.status(403).json({ error: `Account ${req.params.accountId} is not linked to your user` });
  }
  const { alerts } = req.body;
  if (!Array.isArray(alerts)) return res.status(400).json({ error: 'alerts must be an array' });
  const payload    = { alerts, updatedAt: new Date().toISOString() };
  try {
    saveAccountAlerts(req.params.accountId, payload);
    broadcast('ALERTS_UPDATED', { accountId: req.params.accountId, alerts }, req.params.accountId);
    res.json({ ok: true, count: alerts.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── REST: Journal ────────────────────────────────────────────────────────────
app.get('/api/journal', (req, res) => {
  const journal = loadJSON(CONFIG.JOURNAL_FILE);
  const { symbol, strategy, limit = 50, offset = 0 } = req.query;
  let entries = (journal.entries || []).filter(e => e.user_id === req.user.id);
  if (symbol)   entries = entries.filter(e => e.symbol       === symbol);
  if (strategy) entries = entries.filter(e => e.strategy_tag === strategy);
  const sorted = [...entries].sort((a, b) => b.created_at - a.created_at);
  res.json({ total: sorted.length, data: sorted.slice(+offset, +offset + +limit) });
});

app.post('/api/journal', (req, res) => {
  const { ticket, symbol, reason, strategy_tag, confidence, notes } = req.body;
  if (!ticket) return res.status(400).json({ error: 'ticket required' });
  const journal = loadJSON(CONFIG.JOURNAL_FILE);
  const entries = journal.entries || [];
  const idx     = entries.findIndex(e => e.user_id === req.user.id && String(e.ticket) === String(ticket));
  const entry   = {
    id:           idx >= 0 ? entries[idx].id : `j_${Date.now()}`,
    ticket:       String(ticket),
    symbol:       symbol       || '',
    reason:       reason       || '',
    strategy_tag: strategy_tag || '',
    confidence:   confidence   || null,
    notes:        notes        || '',
    user_id:      req.user.id,
    created_at:   idx >= 0 ? entries[idx].created_at : Date.now(),
    updated_at:   Date.now(),
  };
  if (idx >= 0) entries[idx] = entry; else entries.push(entry);
  saveJSON(CONFIG.JOURNAL_FILE, { entries });
  g_accounts.forEach(state => {
    if (state.rawLiveData && userOwnsAccount(req.user.id, state.accountId)) {
      state.processedData = processData({ ...(state.rawStaticData || {}), ...state.rawLiveData }, state.accountId);
    }
  });
  broadcast('JOURNAL_UPDATE', { entry }, null, req.user.id);
  res.json({ success: true, entry });
});

app.delete('/api/journal/:ticket', (req, res) => {
  const journal  = loadJSON(CONFIG.JOURNAL_FILE);
  journal.entries= (journal.entries || []).filter(e => !(e.user_id === req.user.id && String(e.ticket) === req.params.ticket));
  saveJSON(CONFIG.JOURNAL_FILE, journal);
  res.json({ success: true });
});

// ─── REST: Legacy single-account ─────────────────────────────────────────────
const getFirstLiveAccount = (userId = null) => {
  for (const s of g_accounts.values()) {
    if (userId && !userOwnsAccount(userId, s.accountId)) continue;
    if (s.processedData) return s;
  }
  return null;
};
app.get('/api/dashboard',     (req, res) => { const s = getFirstLiveAccount(req.user.id); if (!s) return res.status(503).json({ error: 'No data yet.' }); res.json(s.processedData); });
app.get('/api/positions',     (req, res) => { const s = getFirstLiveAccount(req.user.id); res.json(s?.processedData?.open_positions || []); });
app.get('/api/analytics',     (req, res) => { const s = getFirstLiveAccount(req.user.id); res.json(s?.processedData?.analytics || {}); });
app.get('/api/insights',      (req, res) => { const s = getFirstLiveAccount(req.user.id); res.json(s?.processedData?.insights || []); });
app.get('/api/equity-curve',  (req, res) => { const s = getFirstLiveAccount(req.user.id); res.json(s?.processedData?.analytics?.equity_curve || []); });
app.get('/api/equity-history',(req, res) => {
  const snapshots = (loadJSON(CONFIG.EQUITY_HISTORY_FILE).snapshots || [])
    .filter(s => userOwnsAccount(req.user.id, s.account_id));
  res.json(snapshots);
});

// ─── REST: Debug ──────────────────────────────────────────────────────────────
app.get('/api/debug', (req, res) => {
  const result = [];
  g_accounts.forEach((state, accountId) => {
    if (!userOwnsAccount(req.user.id, accountId)) return;
    const queue   = g_copyQueues.get(accountId);
    const latency = g_copyLatency.get(accountId);
    result.push({
      accountId,
      online:              state.online,
      lastSeen:            state.lastSeen,
      lastSeenAgoMs:       state.lastSeen ? Date.now() - state.lastSeen : null,
      hasProcessedData:    !!state.processedData,
      hasLiveData:         !!state.rawLiveData,
      hasStaticData:       !!state.rawStaticData,
      eaStatus:            state.eaStatus,
      pushCounts:          state.pushCounts,
      lastError:           state.lastError,
      lastSettingsFetch:   state.lastSettingsFetch,
      settingsSaved:       settingsFileExists(accountId),
      copyQueue:  queue   ? { depth: queue.events.length, seenTickets: queue.seenTickets.size, pendingClose: queue.pendingClose.size } : null,
      latency:    latency ? { avg_ms: latency.avg, min_ms: latency.min, max_ms: latency.max, last_ms: latency.last } : null,
      diagnosis:  !state.rawLiveData ? 'EA never pushed to /ea/live.' : !state.processedData ? 'Data received but processing failed.' : 'OK',
    });
  });
  res.json({
    server_version:   '5.1.0',
    accounts:          result,
    socketio_clients:  io.engine.clientsCount,
    uptime_seconds:    Math.floor(process.uptime()),
    copy_queue_count:  g_copyQueues.size,
    database:           {
      enabled: DATABASE_ENABLED,
      schema:  CONFIG.SUPABASE_SCHEMA,
      url:     CONFIG.SUPABASE_URL ? CONFIG.SUPABASE_URL.replace(/\/\/([^./]+)/, '//***') : null,
    },
    settings_dir:      CONFIG.SETTINGS_DIR,
    settings_files:    fs.existsSync(CONFIG.SETTINGS_DIR)
                         ? fs.readdirSync(CONFIG.SETTINGS_DIR)
                         : [],
  });
});

// ─── Utility ──────────────────────────────────────────────────────────────────
const loadJSON = (p) => {
  const key = kvKeyForFile(p);
  if (key && g_kvCache.has(key)) return cloneJSON(g_kvCache.get(key));
  try {
    const payload = JSON.parse(fs.readFileSync(p, 'utf8'));
    if (key) {
      g_kvCache.set(key, cloneJSON(payload));
      g_kvMeta.set(key, { updated_at: fs.statSync(p).mtime });
    }
    return payload;
  } catch {
    return {};
  }
};
const saveJSON = (p, d) => {
  const key = kvKeyForFile(p);
  if (key) {
    g_kvCache.set(key, cloneJSON(d));
    g_kvMeta.set(key, { updated_at: new Date().toISOString() });
    persistKV(key, d);
  }
  safeWriteJSONFile(p, d);
};
const getEmptyAnalytics = () => ({
  total_trades: 0, wins: 0, losses: 0, win_rate: 0, gross_profit: 0, gross_loss: 0,
  net_profit: 0, profit_factor: 0, expectancy: 0, avg_win: 0, avg_loss: 0,
  risk_reward_ratio: 0, avg_rr_planned: 0, avg_pips: 0, avg_duration_minutes: 0,
  best_trade: null, worst_trade: null,
});
const getISOWeek = (date) => {
  const d  = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
  const dn = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - dn);
  const ys = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  return `${d.getUTCFullYear()}-W${String(Math.ceil(((d - ys) / 86400000 + 1) / 7)).padStart(2, '0')}`;
};

// ─── Startup ──────────────────────────────────────────────────────────────────
const hydrateDatabase = async () => {
  if (!supabase) {
    console.log('[Supabase] Disabled. Using local JSON files only.');
    return;
  }

  console.log(`[Supabase] Connecting to ${CONFIG.SUPABASE_URL} (schema: ${CONFIG.SUPABASE_SCHEMA})`);

  try {
    const { data, error } = await dbFrom('tradevault_kv_store').select('key,value,updated_at');
    if (error) throw error;
    for (const row of data || []) {
      g_kvCache.set(row.key, cloneJSON(row.value));
      g_kvMeta.set(row.key, { updated_at: row.updated_at });
      const filePath = KV_KEY_FILES.get(row.key);
      if (filePath) safeWriteJSONFile(filePath, row.value);
    }

    for (const [key, filePath] of KV_KEY_FILES.entries()) {
      if (g_kvCache.has(key) || !fs.existsSync(filePath)) continue;
      try {
        const payload = JSON.parse(fs.readFileSync(filePath, 'utf8'));
        g_kvCache.set(key, cloneJSON(payload));
        g_kvMeta.set(key, { updated_at: fs.statSync(filePath).mtime });
        persistKV(key, payload);
      } catch (e) {
        console.warn(`[Supabase] Could not seed ${key} from local file: ${e.message}`);
      }
    }
    console.log(`[Supabase] Loaded ${data?.length || 0} key/value document(s).`);
  } catch (e) {
    logDbError('hydrate key/value documents', e);
  }

  try {
    const { data, error } = await dbFrom('tradevault_account_settings')
      .select('account_id,settings,updated_at');
    if (error) throw error;
    for (const row of data || []) {
      g_settingsCache.set(row.account_id, cloneJSON(row.settings || {}));
      g_settingsMeta.set(row.account_id, { updated_at: row.updated_at });
      safeWriteJSONFile(settingsFilePath(row.account_id), row.settings || {});
    }

    if (fs.existsSync(CONFIG.SETTINGS_DIR)) {
      for (const file of fs.readdirSync(CONFIG.SETTINGS_DIR)) {
        const match = file.match(/^settings_(.+)\.json$/);
        if (!match || g_settingsCache.has(match[1])) continue;
        const accountId = match[1];
        try {
          const payload = JSON.parse(fs.readFileSync(path.join(CONFIG.SETTINGS_DIR, file), 'utf8'));
          g_settingsCache.set(accountId, cloneJSON(payload));
          g_settingsMeta.set(accountId, { updated_at: fs.statSync(path.join(CONFIG.SETTINGS_DIR, file)).mtime });
          saveAccountSettings(accountId, payload);
        } catch (e) {
          console.warn(`[Supabase] Could not seed settings for ${accountId}: ${e.message}`);
        }
      }
    }
    console.log(`[Supabase] Loaded ${data?.length || 0} account setting row(s).`);
  } catch (e) {
    logDbError('hydrate account settings', e);
  }

  try {
    const { data, error } = await dbFrom('tradevault_account_alerts')
      .select('account_id,payload,updated_at');
    if (error) throw error;
    for (const row of data || []) {
      g_alertsCache.set(row.account_id, cloneJSON(row.payload || { alerts: [] }));
      g_alertsMeta.set(row.account_id, { updated_at: row.updated_at });
      safeWriteJSONFile(alertsFilePath(row.account_id), row.payload || { alerts: [] });
    }

    if (fs.existsSync(CONFIG.ALERTS_DIR)) {
      for (const file of fs.readdirSync(CONFIG.ALERTS_DIR)) {
        const match = file.match(/^alerts_(.+)\.json$/);
        if (!match || g_alertsCache.has(match[1])) continue;
        const accountId = match[1];
        try {
          const payload = JSON.parse(fs.readFileSync(path.join(CONFIG.ALERTS_DIR, file), 'utf8'));
          saveAccountAlerts(accountId, payload);
        } catch (e) {
          console.warn(`[Supabase] Could not seed alerts for ${accountId}: ${e.message}`);
        }
      }
    }
    console.log(`[Supabase] Loaded ${data?.length || 0} alert row(s).`);
  } catch (e) {
    logDbError('hydrate account alerts', e);
  }

  try {
    const { data, error } = await dbFrom('tradevault_account_owners')
      .select('account_id,user_id,claimed_at');
    if (error) throw error;
    for (const row of data || []) {
      g_accountOwners.set(row.account_id, row.user_id);
    }
    console.log(`[Supabase] Loaded ${data?.length || 0} account owner row(s).`);
  } catch (e) {
    logDbError('hydrate account owners', e);
  }

  try {
    const { data, error } = await dbFrom('tradevault_user_ea_keys')
      .select('key_hash,user_id')
      .is('revoked_at', null);
    if (error) throw error;
    for (const row of data || []) {
      g_eaKeyOwners.set(row.key_hash, row.user_id);
    }
    console.log(`[Supabase] Loaded ${data?.length || 0} active EA key row(s).`);
  } catch (e) {
    logDbError('hydrate EA keys', e);
  }
};

const hydrateDirectAccounts = async () => {
  if (!supabase) return;

  try {
    const { data, error } = await dbFrom('tradevault_direct_mt_accounts')
      .select('account_id,user_id,metaapi_account_id,platform,login,server,account_name,password_type,connection_status,state,last_sync_at,last_error,created_at,updated_at,revoked_at')
      .is('revoked_at', null);
    if (error) throw error;

    for (const row of data || []) {
      g_directAccounts.set(row.account_id, row);
      registerAccount(row.account_id, {
        alias: row.account_name || row.login,
        source: 'metaapi',
        connectionMethod: 'direct',
        platform: String(row.platform || 'mt5').toUpperCase(),
        broker: row.server,
        server: row.server,
        login: row.login,
        passwordType: row.password_type,
        metaapiAccountId: row.metaapi_account_id,
        role: 'STANDALONE',
      });
      if (row.user_id && !g_accountOwners.has(row.account_id)) {
        g_accountOwners.set(row.account_id, row.user_id);
      }
      scheduleDirectAccountSync(row.account_id, 5000);
    }

    console.log(`[Supabase] Loaded ${data?.length || 0} direct MetaTrader account row(s).`);
  } catch (e) {
    logDbError('hydrate direct MetaTrader accounts', e);
  }
};

const hydrateAccountSnapshots = async () => {
  if (!supabase) return;

  try {
    const { data, error } = await dbFrom('tradevault_account_snapshots')
      .select('account_id,owner_user_id,config,raw_live_data,raw_static_data,processed_data,ea_status,push_counts,last_seen_ms,last_settings_fetch_ms,last_error,updated_at');
    if (error) throw error;

    for (const row of data || []) {
      if (!g_accounts.has(row.account_id)) {
        registerAccount(row.account_id, row.config || {});
      }
      if (row.owner_user_id && !g_accountOwners.has(row.account_id)) {
        g_accountOwners.set(row.account_id, row.owner_user_id);
      }
      const state = g_accounts.get(row.account_id);
      Object.assign(state.config, row.config || {});
      state.rawLiveData       = row.raw_live_data || null;
      state.rawStaticData     = row.raw_static_data || null;
      state.processedData     = row.processed_data || null;
      state.eaStatus          = row.ea_status || null;
      state.pushCounts        = row.push_counts || { live: 0, static: 0, status: 0 };
      state.lastSeen          = row.last_seen_ms || null;
      state.lastSettingsFetch = row.last_settings_fetch_ms || null;
      state.lastError         = row.last_error || null;
      state.online            = false;
    }

    console.log(`[Supabase] Restored ${data?.length || 0} latest account snapshot(s).`);
  } catch (e) {
    logDbError('hydrate account snapshots', e);
  }
};

const startServer = async () => {
  await hydrateDatabase();
  await hydrateDirectAccounts();
  loadAccountRegistry();
  await hydrateAccountSnapshots();

  server.listen(CONFIG.PORT, () => {
  console.log('');
  console.log('╔══════════════════════════════════════════════════════╗');
  console.log('║    ForexAnalyzer Pro — Backend Server v5.1.0         ║');
  console.log('║    Web-Managed EA Settings + Copy Trading Engine     ║');
  console.log('╚══════════════════════════════════════════════════════╝');
  console.log('');
  console.log(`  REST API:         ${CONFIG.PUBLIC_BASE_URL}/api/dashboard`);
  console.log(`  Debug:            ${CONFIG.PUBLIC_BASE_URL}/api/debug`);
  console.log(`  Settings (EA):    GET ${CONFIG.PUBLIC_BASE_URL}/ea/settings/<accountId>?apiKey=...`);
  console.log(`  Settings (API):   GET/PUT ${CONFIG.PUBLIC_BASE_URL}/api/accounts/<id>/settings`);
  console.log(`  Settings Defaults:GET ${CONFIG.PUBLIC_BASE_URL}/api/settings/defaults`);
  console.log(`  Copy Queue:       GET ${CONFIG.PUBLIC_BASE_URL}/ea/copy-queue/<masterId>?slaveAccountId=<id>`);
  console.log(`  Copy Sync:        GET ${CONFIG.PUBLIC_BASE_URL}/ea/copy-sync/<masterId>?slaveAccountId=<id>`);
  console.log(`  Latency Stats:    GET ${CONFIG.PUBLIC_BASE_URL}/api/copy/latency`);
  console.log(`  Accounts:         ${g_accounts.size} registered`);
  console.log('');
  console.log('  v5.1.0 changes (EA alignment):');
  console.log('    • IncludeHistory + MaxHistoryDays removed — EA locks to unlimited history');
  console.log('    • EnablePriceAlerts locked to true — EA always runs alerts');
  console.log('    • EARole + MasterAccountId set exclusively by copy-pair config on server');
  console.log('    • Timing/retry/AlertReloadMs are internal — tunable but not user-facing');
  console.log('    • GET /ea/settings now strips IncludeHistory/MaxHistoryDays from response');
  console.log('    • PUT /api/accounts/:id/settings silently drops locked fields');
  console.log('');
  });
};

startServer().catch(err => {
  console.error('[Startup] Failed to start server:', err);
  process.exit(1);
});

module.exports = app;
