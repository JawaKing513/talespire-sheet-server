const http = require("http");
const { WebSocketServer, WebSocket } = require("ws");
const fs = require("fs");
const path = require("path");

const PORT = process.env.PORT || 8787;

// ✅ Define the HTTP server (so `server` exists)
const server = http.createServer((req, res) => {
  // simple health check so Render is happy
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("ok");
});

// ✅ Attach WebSocket server to the HTTP server
const wss = new WebSocketServer({ server });

// ✅ Listen on Render’s port
server.listen(PORT, "0.0.0.0", () => {
  console.log("[NET] listening on", PORT);
});

let host = null;
const clients = new Set();

// Connected user presence (ephemeral; not a campaign table)
const USERNAMES = new Map(); // ws -> username
const NAMESET = new Set();

function listUsernames() {
  return Array.from(NAMESET.values()).sort((a, b) => String(a).localeCompare(String(b)));
}

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function sendSafe(ws, obj) {
  try {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(obj));
      return true;
    }
  } catch (_) {}
  return false;
}

// Track delivery of campaign-table broadcasts (ephemeral)
// ackId -> { tableName, key, senderName, expected:Set<ws>, received:Set<ws>, startedAt, timer }
const PENDING_ACKS = new Map();

function usernameOf(ws) {
  return USERNAMES.get(ws) || "(unknown)";
}
function finalizeAck(ackId, reason = "done") {
  const rec = PENDING_ACKS.get(ackId);
  if (!rec) return;
  PENDING_ACKS.delete(ackId);
  try { clearTimeout(rec.timer); } catch (_) {}

  const expectedNames = Array.from(rec.expected).map(usernameOf).sort();
  const receivedNames = Array.from(rec.received).map(usernameOf).sort();
  const missingNames = expectedNames.filter((n) => !receivedNames.includes(n));

  const ms = Date.now() - (rec.startedAt || Date.now());
  const t = String(rec.tableName || "items");
  console.log(
    `[NET] broadcast ${t} '${rec.key}' (${reason}, ${ms}ms) sender=${rec.senderName} ` +
    `received=[${receivedNames.join(", ") || "(none)"}] missing=[${missingNames.join(", ") || "(none)"}]`
  );
}




//// ⚔️🛡💍🧪🧪📜🛠️💰📦🌑🕰️🌠🧿🔗👑


function broadcast(obj) {
  for (const c of clients) send(c, obj);
}

// =========================================================
// Campaign Tables
//   items      -> serialized items
//   statuses   -> serialized status effects
//   feats      -> serialized feats
//   abilities  -> serialized abilities/prayers/spells
// =========================================================
const TABLES = {
  // Stored as: [ [id, payloadBase64], ... ]
  items: [],
  statuses: [],
  feats: [],
  abilities: [],
  sheets: [],
  loot_tables: [],
  shops: [],
};

// Per-player sheet visibility map: username -> [sheetId,...]
let PLAYER_INDEXS = {};

// Per-player loot table visibility: username -> [lootTableId,...]
let PLAYER_LOOT_INDEXS = {};


// =========================================================
// PERSISTENCE (server-side)
// Each campaign table is stored on disk as JSON so a crash/restart
// doesn't lose data.
// Files:
//   data/items.json, data/statuses.json, data/feats.json, data/abilities.json
// Format:
//   [ [id, payloadBase64], ... ]
// =========================================================
const DATA_DIR = path.join(__dirname, "data");
const TABLE_FILES = {
  items: path.join(DATA_DIR, "items.json"),
  statuses: path.join(DATA_DIR, "statuses.json"),
  feats: path.join(DATA_DIR, "feats.json"),
  abilities: path.join(DATA_DIR, "abilities.json"),
  sheets: path.join(DATA_DIR, "sheets.json"),
  loot_tables: path.join(DATA_DIR, "loot_tables.json"),
  shops: path.join(DATA_DIR, "shops.json"),
  player_indexs: path.join(DATA_DIR, "player_indexs.json"),
  player_loot_indexs: path.join(DATA_DIR, "player_loot_indexs.json"),
};

function ensureDataDir() {
  try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch (_) {}
}

function safeReadJson(file, fallback) {
  try {
    if (!fs.existsSync(file)) return fallback;
    const raw = fs.readFileSync(file, "utf8");
    if (!raw.trim()) return fallback;
    const parsed = JSON.parse(raw);
    return parsed;
  } catch (e) {
    console.log(`[PERSIST] failed reading ${file}: ${e?.message || e}`);
    return fallback;
  }
}

function atomicWriteJson(file, dataObj) {
  const tmp = file + ".tmp";
  const raw = JSON.stringify(dataObj, null, 2);
  fs.writeFileSync(tmp, raw, "utf8");
  fs.renameSync(tmp, file);
}

function loadAllTables() {
  ensureDataDir();

  const items = safeReadJson(TABLE_FILES.items, []);
  const statuses = safeReadJson(TABLE_FILES.statuses, []);
  const feats = safeReadJson(TABLE_FILES.feats, []);
  const abilities = safeReadJson(TABLE_FILES.abilities, []);
  const sheets = safeReadJson(TABLE_FILES.sheets, []);
  const player_indexs = safeReadJson(TABLE_FILES.player_indexs, {});

  TABLES.items = Array.isArray(items) ? items.filter(r => Array.isArray(r) && r.length === 2) : [];
  TABLES.statuses = Array.isArray(statuses) ? statuses.filter(r => Array.isArray(r) && r.length === 2) : [];
  TABLES.feats = Array.isArray(feats) ? feats.filter(r => Array.isArray(r) && r.length === 2) : [];
  TABLES.abilities = Array.isArray(abilities) ? abilities.filter(r => Array.isArray(r) && r.length === 2) : [];
  TABLES.sheets = Array.isArray(sheets) ? sheets.filter(r => Array.isArray(r) && r.length === 2) : [];

  PLAYER_INDEXS = (player_indexs && typeof player_indexs === "object") ? player_indexs : {};

  console.log(`[PERSIST] loaded: items=${TABLES.items.length}, statuses=${TABLES.statuses.length}, feats=${TABLES.feats.length}, abilities=${TABLES.abilities.length}, sheets=${TABLES.sheets.length}, player_indexs=${Object.keys(PLAYER_INDEXS||{}).length}`);
}

const SAVE_TIMERS = new Map(); // tableName -> timeout

function saveTableNow(tableName) {
  const t = normalizeTableName(tableName);
  ensureDataDir();
  try {
    const arr =
      (t === "items") ? TABLES.items :
      (t === "statuses") ? TABLES.statuses :
      (t === "feats") ? TABLES.feats :
      (t === "abilities") ? TABLES.abilities :
      (t === "loot_tables") ? TABLES.loot_tables :
      (t === "shops") ? TABLES.shops :
      TABLES.sheets;

    atomicWriteJson(TABLE_FILES[t], arr);
  } catch (e) {
    console.log(`[PERSIST] failed saving ${t}: ${e?.message || e}`);
  }
}

function scheduleSave(tableName) {
  const t = normalizeTableName(tableName);
  const prev = SAVE_TIMERS.get(t);
  if (prev) { try { clearTimeout(prev); } catch (_) {} }
  const timer = setTimeout(() => {
    SAVE_TIMERS.delete(t);
    saveTableNow(t);
  }, 150);
  SAVE_TIMERS.set(t, timer);
}

function normalizeUser(name) {
  return String(name || "").trim();
}

function getUserSheetIds(name) {
  const n = normalizeUser(name);
  const arr = PLAYER_INDEXS?.[n];
  return Array.isArray(arr) ? arr.map(String).filter(Boolean) : [];
}

function setUserSheetIds(name, ids) {
  const n = normalizeUser(name);
  if (!n) return;
  const out = Array.isArray(ids) ? ids.map(String).filter(Boolean) : [];
  PLAYER_INDEXS[n] = Array.from(new Set(out));
  // persist
  try {
    ensureDataDir();
    atomicWriteJson(TABLE_FILES.player_indexs, PLAYER_INDEXS);
  } catch (e) {
    console.log(`[PERSIST] failed saving player_indexs: ${e?.message || e}`);
  }
}


// ===== Loot table visibility helpers =====
function getUserLootIds(name) {
  const n = normalizeUser(name);
  const arr = PLAYER_LOOT_INDEXS?.[n];
  return Array.isArray(arr) ? arr.map(String).filter(Boolean) : [];
}
function setUserLootIds(name, ids) {
  const n = normalizeUser(name);
  if (!n) return;
  const out = Array.isArray(ids) ? ids.map(String).filter(Boolean) : [];
  PLAYER_LOOT_INDEXS[n] = Array.from(new Set(out));
  try {
    ensureDataDir();
    atomicWriteJson(TABLE_FILES.player_loot_indexs, PLAYER_LOOT_INDEXS);
  } catch (e) {
    console.log(`[PERSIST] failed saving player_loot_indexs: ${e?.message || e}`);
  }
}
function addUserLootId(name, lootId) {
  const n = normalizeUser(name);
  const id = String(lootId || "").trim();
  if (!n || !id) return;
  const before = getUserLootIds(n);
  if (before.includes(id)) return;
  before.push(id);
  setUserLootIds(n, before);
}
function removeUserLootId(name, lootId) {
  const n = normalizeUser(name);
  const id = String(lootId || "").trim();
  if (!n || !id) return;
  const before = getUserLootIds(n);
  const after = before.filter((x) => x !== id);
  setUserLootIds(n, after);
}

function addUserSheetId(name, sheetId) {
  const n = normalizeUser(name);
  const sid = String(sheetId || "").trim();
  if (!n || !sid) return;
  const cur = getUserSheetIds(n);
  if (!cur.includes(sid)) cur.push(sid);
  setUserSheetIds(n, cur);
}

function removeUserSheetId(name, sheetId) {
  const n = normalizeUser(name);
  const sid = String(sheetId || "").trim();
  if (!n || !sid) return;
  const cur = getUserSheetIds(n).filter((x) => x !== sid);
  setUserSheetIds(n, cur);
}

function sendToUser(user, obj) {
  const name = normalizeUser(user);
  if (!name) return false;
  for (const ws of clients) {
    if (usernameOf(ws) === name) {
      sendSafe(ws, obj);
      return true;
    }
  }
  // host can also be target if they are connected with same username
  if (host && usernameOf(host) === name) {
    sendSafe(host, obj);
    return true;
  }
  return false;
}


// =========================================================
// Sheet push helpers (server -> specific client)
// ---------------------------------------------------------
// When a user is granted sheets, we also push the sheet payloads
// so their client can immediately populate its local sheet table.
// =========================================================
function findSheetPayload(sheetId) {
  const sid = String(sheetId || "").trim();
  if (!sid) return null;
  const row = (Array.isArray(TABLES.sheets) ? TABLES.sheets : []).find((r) => Array.isArray(r) && r[0] === sid);
  return row ? String(row[1] || "") : null;
}

function sendSheetToWs(ws, sheetId) {
  const payload = findSheetPayload(sheetId);
  if (!payload) return false;
  // Use existing sheet_updated message so client reuses the same handler.
  // ackId is optional; clients may still send sheet_ack with undefined, which we ignore.
  return sendSafe(ws, { t: "sheet_updated", key: String(sheetId), value: String(payload), ackId: null });
}

function sendSheetsToWs(ws, sheetIds) {
  const ids = Array.isArray(sheetIds) ? sheetIds : [];
  for (const sid of ids) sendSheetToWs(ws, sid);
}

// Load persisted campaign data on startup
loadAllTables();


function getTable(name) {
  const k = String(name || "items").toLowerCase();
  if (k === "item" || k === "items") return TABLES.items;
  if (k === "status" || k === "statuses" || k === "statuseffects" || k === "status_effects") return TABLES.statuses;
  if (k === "feat" || k === "feats") return TABLES.feats;
  if (k === "ability" || k === "abilities" || k === "prayer" || k === "prayers" || k === "spell" || k === "spells") return TABLES.abilities;
  if (k === "sheet" || k === "sheets" || k === "characters" || k === "chars") return TABLES.sheets;
  return TABLES.items; // default/back-compat
}

function normalizeTableName(name) {
  const k = String(name || "items").toLowerCase();
  if (k === "item" || k === "items") return "items";
  if (k === "status" || k === "statuses" || k === "statuseffects" || k === "status_effects") return "statuses";
  if (k === "feat" || k === "feats") return "feats";
  if (k === "ability" || k === "abilities" || k === "prayer" || k === "prayers" || k === "spell" || k === "spells") return "abilities";
  if (k === "sheet" || k === "sheets" || k === "characters" || k === "chars") return "sheets";
  return "items";
}

wss.on("connection", (ws, req) => {
  ws.role = "unknown";

  ws.on("message", (buf) => {
    let msg;
    try { msg = JSON.parse(String(buf)); } catch { return; }
	
	
	if (msg.t === "join") {
      const desiredName = String(msg.name || "").trim();
      if (!desiredName) {
        send(ws, { t: "welcome", ok: false, err: "missing_name" });
        return;
      }

      // Collision check: cannot connect with a duplicate name.
      if (NAMESET.has(desiredName)) {
        send(ws, { t: "welcome", ok: false, err: "name_taken" });
        return;
      }

      ws.role = msg.role === "host" ? "host" : "client";
      if (ws.role === "host") host = ws;
      else clients.add(ws);

      USERNAMES.set(ws, desiredName);
      NAMESET.add(desiredName);

      // Log connection by username (not IP)
      console.log(`[NET] joined: ${desiredName}`);
      console.log(`[NET] connected users: ${listUsernames().join(", ") || "(none)"}`);

      send(ws, {
        t: "welcome",
        ok: true,
        role: ws.role,
        name: desiredName
      });

      // Push current sheet visibility for this user (server authoritative)
      try {
        sendSafe(ws, { t: "player_indexs_set", sheetIds: getUserSheetIds(desiredName) });
      // Also push the actual sheet payloads for the sheets this user is allowed to see
      try { sendSheetsToWs(ws, getUserSheetIds(desiredName)); } catch (_) {}
      } catch (_) {}

      return;
    }


// =========================================================
// Sheet visibility (server-authoritative) : player_indexs
// =========================================================
if (msg.t === "who") {
  return send(ws, {
    t: "whoReply",
    replyTo: msg.id,
    ok: true,
    users: listUsernames()
  });
}

if (msg.t === "player_indexs_get") {
  const id = msg.id;
  const me = usernameOf(ws);
  return send(ws, {
    t: "player_indexs_getReply",
    replyTo: id,
    ok: true,
    user: me,
    sheetIds: getUserSheetIds(me)
  });
}

if (msg.t === "player_indexs_ensure") {
  const id = msg.id;
  const me = usernameOf(ws);
  const sheetId = String(msg.sheetId || "").trim();
  if (!sheetId) return send(ws, { t: "player_indexs_ensureReply", replyTo: id, ok: false, err: "missing_sheetId" });

  addUserSheetId(me, sheetId);

  // Push updated list to the user
  sendSafe(ws, { t: "player_indexs_set", sheetIds: getUserSheetIds(me) });

  // Ensure the client also has the sheet payload locally
  try { sendSheetToWs(ws, sheetId); } catch (_) {}

  return send(ws, { t: "player_indexs_ensureReply", replyTo: id, ok: true });
}

if (msg.t === "sheet_perm_query") {
  const id = msg.id;
  const sheetId = String(msg.sheetId || "").trim();
  const users = listUsernames();
  const out = users.map((name) => ({
    name,
    has: getUserSheetIds(name).includes(sheetId),
  }));
  return send(ws, {
    t: "sheet_perm_queryReply",
    replyTo: id,
    ok: true,
    sheetId,
    users: out
  });
}

if (msg.t === "sheet_perm_set") {
  const id = msg.id;
  const sheetId = String(msg.sheetId || "").trim();
  const user = String(msg.user || "").trim();
  const allow = !!msg.allow;

  if (!sheetId || !user) return send(ws, { t: "sheet_perm_setReply", replyTo: id, ok: false, err: "missing_args" });

  if (allow) addUserSheetId(user, sheetId);
  else removeUserSheetId(user, sheetId);

  // Push updated visibility to target if connected
  sendToUser(user, { t: "player_indexs_set", sheetIds: getUserSheetIds(user) });

  if (allow) {
    // If the user is connected, also push the sheet payload so they can load it immediately
    for (const ws2 of clients) {
      if (usernameOf(ws2) === normalizeUser(user)) { try { sendSheetToWs(ws2, sheetId); } catch (_) {} }
    }
    if (host && usernameOf(host) === normalizeUser(user)) { try { sendSheetToWs(host, sheetId); } catch (_) {} }
  }

  

// =========================================================
// Loot table visibility (server-authoritative) : player_loot_indexs
// =========================================================
if (msg.t === "player_loot_indexs_get") {
  const id = msg.id;
  const me = usernameOf(ws);
  return send(ws, {
    t: "player_loot_indexs_getReply",
    replyTo: id,
    ok: true,
    user: me,
    lootIds: getUserLootIds(me)
  });
}

if (msg.t === "player_loot_indexs_ensure") {
  const id = msg.id;
  const me = usernameOf(ws);
  const lootId = String(msg.lootId || "").trim();
  if (!lootId) return send(ws, { t: "player_loot_indexs_ensureReply", replyTo: id, ok: false, err: "missing_lootId" });

  addUserLootId(me, lootId);
  sendSafe(ws, { t: "player_loot_indexs_set", lootIds: getUserLootIds(me) });
  return send(ws, { t: "player_loot_indexs_ensureReply", replyTo: id, ok: true });
}

if (msg.t === "loot_perm_query") {
  const id = msg.id;
  const lootId = String(msg.lootId || "").trim();
  const users = listUsernames();
  const out = users.map((name) => ({
    name,
    has: getUserLootIds(name).includes(lootId),
  }));
  return send(ws, {
    t: "loot_perm_queryReply",
    replyTo: id,
    ok: true,
    lootId,
    users: out
  });
}

if (msg.t === "loot_perm_set") {
  const id = msg.id;
  const lootId = String(msg.lootId || "").trim();
  const user = String(msg.user || "").trim();
  const allow = !!msg.allow;

  if (!lootId || !user) return send(ws, { t: "loot_perm_setReply", replyTo: id, ok: false, err: "missing_args" });

  if (allow) addUserLootId(user, lootId);
  else removeUserLootId(user, lootId);

  sendToUser(user, { t: "player_loot_indexs_set", lootIds: getUserLootIds(user) });
  return send(ws, { t: "loot_perm_setReply", replyTo: id, ok: true });
}
return send(ws, { t: "sheet_perm_setReply", replyTo: id, ok: true });
}

    // =========================================================
    // Dice roll broadcast (no server console spam)
    // client -> server -> (host + all other clients)
    // payload from client is already formatted; we just relay.
    // =========================================================
    if (msg.t === "roll") {
      const line = String(msg.line || "").trim();
      if (!line) return;

      const payload = {
        t: "roll",
        line,
        from: usernameOf(ws),
        at: Date.now(),
      };

      // send to host (if any)
      if (host && host !== ws) sendSafe(host, payload);

      // send to all other clients
      for (const c of clients) {
        if (c === ws) continue;
        sendSafe(c, payload);
      }
      return;
    }
    
    // Generic ACK handler for any table broadcast (items/statuses/feats/abilities)
    if (typeof msg.t === "string" && msg.t.endsWith("_ack")) {
      const ackId = String(msg.ackId || "");
      const rec = PENDING_ACKS.get(ackId);
      if (rec) {
        rec.received.add(ws);
        if (rec.received.size >= rec.expected.size) {
          finalizeAck(ackId, "all-acked");
        }
      }
      return;
    }

    if (msg.t === "tableRequest") {
      const id = msg.id;
      const key = String(msg.key || "");
      const table = getTable(msg.table);
      const idx = table.findIndex(row => row[0] === key);
    
      if (idx === -1) {
        return send(ws, {
          t: "tableReply",
          replyTo: id,
          ok: false,
          err: "bad key",
          key
        });
      }
    
      const value = table[idx][1];
    
      return send(ws, {
        t: "tableReply",
        replyTo: id,
        ok: true,
        key,
        value
      });
    }


    // =========================================================
    // Sheet Import (client sends payload, server assigns new id)
    // =========================================================
    if (msg.t === "sheet_import") {
      const id = msg.id;
      const payload = String(msg.payload ?? msg.value ?? "").trim();
      if (!payload) return send(ws, { t: "sheet_importReply", replyTo: id, ok: false, err: "empty_payload" });

      // basic safety cap (~1.5MB base64)
      if (payload.length > 1_500_000) {
        return send(ws, { t: "sheet_importReply", replyTo: id, ok: false, err: "payload_too_large" });
      }

      const makeId = () => {
        const rand = Math.random().toString(36).slice(2, 8);
        return `sh_${Date.now().toString(36)}_${rand}`;
      };

      let key = makeId();
      // ensure unique key
      const has = (k) => TABLES.sheets.some((row) => row && row[0] === k);
      for (let i = 0; i < 8 && has(key); i++) key = makeId();
      if (has(key)) {
        return send(ws, { t: "sheet_importReply", replyTo: id, ok: false, err: "id_collision" });
      }

      TABLES.sheets.push([key, payload]);
      try { scheduleSave("sheets"); } catch (_) {}

      // Grant to the importing user (players need visibility to see it)
      try {
        const who = usernameOf(ws);
        if (who && who !== "(unknown)") {
          addUserSheetId(who, key);
          send(ws, { t: "player_indexs_set", sheetIds: getUserSheetIds(who) });
        }
      } catch (_) {}

      // Broadcast to other connected clients (ACK tracked)
      try {
        const senderName = usernameOf(ws);
        const ackId = `sheet:create:${key}:${Date.now().toString(36)}:${Math.random().toString(36).slice(2, 8)}`;
        const expected = new Set();
        const received = new Set();
        for (const c of clients) {
          if (c === ws) continue;
          expected.add(c);
        }
        for (const c of expected) {
          sendSafe(c, { t: "sheet_created", ackId, key, value: payload });
        }
        const timer = setTimeout(() => finalizeAck(ackId, "timeout"), 2000);
        PENDING_ACKS.set(ackId, { tableName: "sheets", key, senderName, expected, received, startedAt: Date.now(), timer });
        if (expected.size === 0) finalizeAck(ackId, "no-recipients");
      } catch (_) {}

      // Audit log
      try {
        const who = USERNAMES.get(ws) || "(unknown)";
        console.log(`[NET] sheets imported by ${who}: ${key}`);
      } catch (_) {}

      return send(ws, { t: "sheet_importReply", replyTo: id, ok: true, key, value: payload });
    }

	if (msg.t === "tableAppend") {
      const id = msg.id; // request id (from netRequest)
      const key = String(msg.key || "").trim();
      const value = String(msg.value ?? "");

      const table = getTable(msg.table);
    
      if (!key) {
        return send(ws, { t: "tableAppendReply", replyTo: id, ok: false, err: "empty key" });
      }
    
      // disallow duplicates (safer)
      const exists = table.some(row => row && row[0] === key);
      if (exists) {
        return send(ws, { t: "tableAppendReply", replyTo: id, ok: false, err: "key exists", key });
      }
    
      table.push([key, value]);

      // Persist
      try { scheduleSave(normalizeTableName(msg.table)); } catch (_) {}

      // Persist
      try { scheduleSave(normalizeTableName(msg.table)); } catch (_) {}

      // ✅ Broadcast newly created rows to all OTHER connected clients.
      // Clients will ACK receipt so we can log who got it.
      try {
        const tableName2 = String(msg.table || "items").toLowerCase();
        const short = (tableName2 === "item") ? "items" : tableName2;
        const kind = (short === "statuses" || short === "status") ? "status"
          : (short === "feats" || short === "feat") ? "feat"
          : (short === "abilities" || short === "ability" || short === "prayers" || short === "prayer" || short === "spells" || short === "spell") ? "ability"
          : (short === "sheets" || short === "sheet" || short === "characters" || short === "chars") ? "sheet"
          : (short === "loot_tables" || short === "loot" || short === "loottables" || short === "loot-tables") ? "loot"
          : (short === "shops" || short === "shop") ? "shop"
          : "item";
        const tableNorm = (kind === "status") ? "statuses" : (kind === "feat") ? "feats" : (kind === "ability") ? "abilities" : (kind === "sheet") ? "sheets" : (kind === "loot") ? "loot_tables" : (kind === "shop") ? "shops" : "items";

        const senderName = usernameOf(ws);
        const ackId = `${kind}:create:${key}:${Date.now().toString(36)}:${Math.random().toString(36).slice(2, 8)}`;

        const expected = new Set();
        const received = new Set();

        for (const c of clients) {
          if (c === ws) continue;
          expected.add(c);
        }

        const tmsg = (kind === "status") ? "status_created"
          : (kind === "feat") ? "feat_created"
          : (kind === "ability") ? "ability_created"
          : (kind === "sheet") ? "sheet_created"
          : (kind === "loot") ? "loot_created"
          : (kind === "shop") ? "shop_created"
          : "item_created";

        for (const c of expected) {
          sendSafe(c, { t: tmsg, ackId, key, value });
        }

        const timer = setTimeout(() => finalizeAck(ackId, "timeout"), 2000);
        PENDING_ACKS.set(ackId, { tableName: tableNorm, key, senderName, expected, received, startedAt: Date.now(), timer });

        if (expected.size === 0) finalizeAck(ackId, "no-recipients");
      } catch (_) {}


      // Audit log: creation
      try {
        const tname = String(msg.table || "items").toLowerCase();
        const who = USERNAMES.get(ws) || "(unknown)";
        console.log(`[NET] ${tname} created by ${who}: ${key}`);
      } catch (_) {}
    
      return send(ws, {
        t: "tableAppendReply",
        replyTo: id,
        ok: true,
        key,
        value,
        len: table.length
      });
    }

    if (msg.t === "tableUpdate") {
      const id = msg.id; // request id (from netRequest)
      const key = String(msg.key || "").trim();
      const value = String(msg.value ?? "");

      const table = getTable(msg.table);

      if (!key) {
        return send(ws, { t: "tableUpdateReply", replyTo: id, ok: false, err: "empty key" });
      }

      const idx = table.findIndex(row => row && row[0] === key);
      if (idx < 0) {
        return send(ws, { t: "tableUpdateReply", replyTo: id, ok: false, err: "missing key", key });
      }

      table[idx] = [key, value];

      // Persist
      try { scheduleSave(normalizeTableName(msg.table)); } catch (_) {}

      // Persist
      try { scheduleSave(normalizeTableName(msg.table)); } catch (_) {}

      // ✅ Broadcast updated rows to all OTHER connected clients.
      // Clients will ACK receipt so we can log who got it.
      try {
        const tableName2 = String(msg.table || "items").toLowerCase();
        const short = (tableName2 === "item") ? "items" : tableName2;
        const kind = (short === "statuses" || short === "status") ? "status"
          : (short === "feats" || short === "feat") ? "feat"
          : (short === "abilities" || short === "ability" || short === "prayers" || short === "prayer" || short === "spells" || short === "spell") ? "ability"
          : (short === "sheets" || short === "sheet" || short === "characters" || short === "chars") ? "sheet"
          : (short === "loot_tables" || short === "loot" || short === "loottables" || short === "loot-tables") ? "loot"
          : (short === "shops" || short === "shop") ? "shop"
          : "item";
        const tableNorm = (kind === "status") ? "statuses" : (kind === "feat") ? "feats" : (kind === "ability") ? "abilities" : (kind === "sheet") ? "sheets" : (kind === "loot") ? "loot_tables" : (kind === "shop") ? "shops" : "items";

        const senderName = usernameOf(ws);
        const ackId = `${kind}:update:${key}:${Date.now().toString(36)}:${Math.random().toString(36).slice(2, 8)}`;

        const expected = new Set();
        const received = new Set();
        for (const c of clients) {
          if (c === ws) continue;
          expected.add(c);
        }

        const tmsg = (kind === "status") ? "status_updated"
          : (kind === "feat") ? "feat_updated"
          : (kind === "ability") ? "ability_updated"
          : (kind === "sheet") ? "sheet_updated"
          : (kind === "loot") ? "loot_updated"
          : (kind === "shop") ? "shop_updated"
          : "item_updated";

        for (const c of expected) {
          sendSafe(c, { t: tmsg, ackId, key, value });
        }

        const timer = setTimeout(() => finalizeAck(ackId, "timeout"), 2000);
        PENDING_ACKS.set(ackId, { tableName: tableNorm, key, senderName, expected, received, startedAt: Date.now(), timer });
        if (expected.size === 0) finalizeAck(ackId, "no-recipients");
      } catch (_) {}

      // Audit log: update
      try {
        const tname = String(msg.table || "items").toLowerCase();
        const who = USERNAMES.get(ws) || "(unknown)";
        console.log(`[NET] ${tname} updated by ${who}: ${key}`);
      } catch (_) {}

      return send(ws, {
        t: "tableUpdateReply",
        replyTo: id,
        ok: true,
        key,
        value,
        len: table.length
      });
    }

    if (msg.t === "tableDelete") {
      const id = msg.id; // request id (from netRequest)
      const key = String(msg.key || "").trim();
      const table = getTable(msg.table);

      if (!key) {
        return send(ws, { t: "tableDeleteReply", replyTo: id, ok: false, err: "empty key" });
      }

      const idx = table.findIndex((row) => row && row[0] === key);
      if (idx < 0) {
        return send(ws, { t: "tableDeleteReply", replyTo: id, ok: false, err: "missing key", key });
      }


table.splice(idx, 1);

// If a sheet is deleted, also remove it from ALL player visibility lists.
// (Otherwise clients can end up with permissions pointing at missing sheets.)
try {
  const tableNorm = normalizeTableName(msg.table);
  if (tableNorm === "sheets") {
    let touched = false;
    const sid = key;
    for (const user of Object.keys(PLAYER_INDEXS || {})) {
      const before = getUserSheetIds(user);
      const after = before.filter((x) => String(x || "") !== sid);
      if (after.length !== before.length) {
        setUserSheetIds(user, after);
        touched = true;
        // Push updated visibility to target if connected
        sendToUser(user, { t: "player_indexs_set", sheetIds: getUserSheetIds(user) });
      }
    }
    if (touched) {
      try { console.log(`[NET] sheet '${sid}' removed from player_indexs for all users`); } catch (_) {}
    }
  }
} catch (_) {}

      // ✅ Broadcast deletion to all OTHER connected clients.
      // Clients will ACK receipt so we can log who got it.
      try {
        const tableNorm = normalizeTableName(msg.table);
        const kind = (tableNorm === "statuses") ? "status"
          : (tableNorm === "feats") ? "feat"
          : (tableNorm === "abilities") ? "ability"
          : (tableNorm === "sheets") ? "sheet"
          : (tableNorm === "loot_tables") ? "loot"
          : (tableNorm === "shops") ? "shop"
          : "item";

        const senderName = usernameOf(ws);
        const ackId = `${kind}:delete:${key}:${Date.now().toString(36)}:${Math.random().toString(36).slice(2, 8)}`;

        const expected = new Set();
        const received = new Set();
        for (const c of clients) {
          if (c === ws) continue;
          expected.add(c);
        }

        const tmsg = (kind === "status") ? "status_deleted"
          : (kind === "feat") ? "feat_deleted"
          : (kind === "ability") ? "ability_deleted"
          : (kind === "sheet") ? "sheet_deleted"
          : (kind === "loot") ? "loot_deleted"
          : (kind === "shop") ? "shop_deleted"
          : "item_deleted";

        for (const c of expected) {
          sendSafe(c, { t: tmsg, ackId, key });
        }

        const timer = setTimeout(() => finalizeAck(ackId, "timeout"), 2000);
        PENDING_ACKS.set(ackId, { tableName: tableNorm, key, senderName, expected, received, startedAt: Date.now(), timer });
        if (expected.size === 0) finalizeAck(ackId, "no-recipients");
      } catch (_) {}

      // Audit log: delete
      try {
        const tname = normalizeTableName(msg.table);
        const who = USERNAMES.get(ws) || "(unknown)";
        console.log(`[NET] ${tname} deleted by ${who}: ${key}`);
      } catch (_) {}

      return send(ws, {
        t: "tableDeleteReply",
        replyTo: id,
        ok: true,
        key,
        len: table.length,
      });
    }

	if (msg.t === "tableManifest") {
	const id = msg.id;
	const table = getTable(msg.table);
	const ids = table.map(row => row[0]); // first element of each row
	
	return send(ws, {
		t: "tableManifestReply",
		replyTo: id,
		ok: true,
		table: String(msg.table || "items"),
		ids
	});
	}
	
    // client -> host
    if (msg.t === "get_snapshot" && host) {
      msg.from = ws.role;
      return send(host, msg);
    }
    if (msg.t === "op" && host) return send(host, msg);

    // host -> clients
	if (msg.t === "snapshot" && msg.replyTo) {
		return broadcast(msg);
	}
    if (msg.t === "snapshot") return broadcast(msg);

    if (msg.t === "op_applied") return broadcast(msg);
  });

  ws.on("close", () => {
    clients.delete(ws);

    // Remove from any pending broadcast expectations (so 'missing' is accurate).
    for (const [ackId, rec] of PENDING_ACKS.entries()) {
      if (rec?.expected?.has(ws)) {
        rec.expected.delete(ws);
        // If everyone else has already ACKed, finalize now.
        if (rec.received.size >= rec.expected.size) finalizeAck(ackId, "client-left");
      }
    }
    if (host === ws) host = null;

    const name = USERNAMES.get(ws);
    if (name) {
      USERNAMES.delete(ws);
      NAMESET.delete(name);
      console.log(`[NET] left: ${name}`);
      console.log(`[NET] connected users: ${listUsernames().join(", ") || "(none)"}`);
    }
  });
});

console.log(`WS server listening on ws://127.0.0.1:${PORT}`);