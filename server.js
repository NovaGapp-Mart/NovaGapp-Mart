const fsNative = require("fs");
const path = require("path");

function loadDotEnvFile(filePath){
  try{
    const raw = fsNative.readFileSync(filePath, "utf8");
    raw.split(/\r?\n/).forEach(line => {
      const trimmed = String(line || "").trim();
      if(!trimmed || trimmed.startsWith("#")) return;
      const eqIdx = trimmed.indexOf("=");
      if(eqIdx <= 0) return;
      const key = trimmed.slice(0, eqIdx).trim();
      if(!key) return;
      const existing = process.env[key];
      if(existing !== undefined && String(existing).trim() !== "") return;
      let value = trimmed.slice(eqIdx + 1).trim();
      if((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))){
        value = value.slice(1, -1);
      }
      process.env[key] = value;
    });
  }catch(_){ }
}

loadDotEnvFile(path.join(__dirname, ".env"));

console.log("OPENAI API KEY PRESENT:", !!process.env.OPENAI_API_KEY);
const express = require("express");
const cors = require("cors");
const multer = require("multer");
const fs = require("fs/promises");
const crypto = require("crypto");
const fetch = global.fetch
  ? global.fetch.bind(global)
  : (...args) => import("node-fetch").then(mod => (mod.default || mod)(...args));

let FormDataCtor = global.FormData;
if (!FormDataCtor) {
  try {
    FormDataCtor = require("form-data");
  } catch (err) {
    console.error("FormData not available. Install 'form-data' or use Node 18+.");
  }
}
const BlobCtor = global.Blob;

const app = express();
const upload = multer();
const PORT = process.env.PORT || 3000;
const UPLOADS_DIR = path.join(__dirname, "uploads");
const MANUAL_DIR = path.join(UPLOADS_DIR, "manual-requests");
const MANUAL_JSON_PATH = path.join(MANUAL_DIR, "requests.json");
const SYSTEM_DIR = path.join(UPLOADS_DIR, "system");
const SYSTEM_STATE_PATH = path.join(SYSTEM_DIR, "state.json");
const FREE_PLAN_DAILY_LIMIT = 2;
const PRO_PLAN_DAILY_LIMIT = 30;
const TRYON_PRICE_PER_IMAGE_USD = 1;
const PLAN_40_USD = 40;
const PLAN_4000_USD = 4000;
const PLAN_BILLING_DAYS = 30;
const PLAN_4000_SEAT_LIMIT = 5;
const VERIFIED_MIN_FOLLOWERS = 5000;
const VERIFIED_TRUST_BONUS = 25;
const VERIFIED_SEARCH_BOOST = 250;
const DEFAULT_TRUST_FLOW = ["discovery", "chat", "trust", "ai_tryon", "checkout"];
const ADMIN_DM_EMAIL = String(process.env.TRYON_ADMIN_EMAIL || "prashikbhalerao0208@gmail.com").trim();
const ADMIN_DM_WEBHOOK_URL = String(process.env.TRYON_ADMIN_DM_WEBHOOK || "").trim();
const ADMIN_AUTOMATION_EMAIL = String(process.env.ADMIN_AUTOMATION_EMAIL || "novagapp2026@gmail.com").trim();
const ADMIN_AUTOMATION_TOKEN = String(process.env.ADMIN_AUTOMATION_TOKEN || "").trim();
const RESEND_API_KEY = String(process.env.RESEND_API_KEY || "").trim();
const RESEND_FROM_EMAIL = String(process.env.RESEND_FROM_EMAIL || "onboarding@resend.dev").trim();
const RAZORPAY_WEBHOOK_SECRET = String(
  process.env.RAZORPAY_WEBHOOK_SECRET ||
  process.env.WEBHOOK_SECRET ||
  ""
).trim();

const OPENAI_API_KEY = String(process.env.OPENAI_API_KEY || "").trim();

app.use(express.json({
  limit: "15mb",
  verify: (req, _res, buf) => {
    try{
      req.rawBody = Buffer.from(buf || []);
    }catch(_){
      req.rawBody = Buffer.alloc(0);
    }
  }
}));
app.use(cors());
app.use("/uploads", express.static(UPLOADS_DIR));
app.use(express.static(__dirname, {
  dotfiles: "deny",
  etag: true,
  maxAge: "1h",
  setHeaders: (res, servedPath) => {
    if(/\.html?$/i.test(String(servedPath || ""))){
      res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate");
      res.setHeader("Pragma", "no-cache");
      res.setHeader("Expires", "0");
    }
  }
}));

const PUBLIC_SUPABASE_URL = String(
  process.env.SUPABASE_URL ||
  process.env.CONTEST_SUPABASE_URL ||
  ""
).trim();
const PUBLIC_SUPABASE_ANON_KEY = String(
  process.env.SUPABASE_ANON_KEY ||
  process.env.CONTEST_SUPABASE_ANON_KEY ||
  process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ||
  ""
).trim();
const PUBLIC_RAZORPAY_KEY_ID = String(
  process.env.RAZORPAY_KEY_ID ||
  process.env.RAZORPAY_PUBLIC_KEY ||
  process.env.API_KEY ||
  process.env.RAZORPAY_API_KEY ||
  ""
).trim();
const PUBLIC_FIREBASE_API_KEY = String(process.env.FIREBASE_API_KEY || "").trim();
const PUBLIC_FIREBASE_AUTH_DOMAIN = String(process.env.FIREBASE_AUTH_DOMAIN || "").trim();
const PUBLIC_FIREBASE_PROJECT_ID = String(process.env.FIREBASE_PROJECT_ID || "").trim();
const PUBLIC_FIREBASE_STORAGE_BUCKET = String(process.env.FIREBASE_STORAGE_BUCKET || "").trim();
const PUBLIC_FIREBASE_MESSAGING_SENDER_ID = String(process.env.FIREBASE_MESSAGING_SENDER_ID || "").trim();
const PUBLIC_FIREBASE_APP_ID = String(process.env.FIREBASE_APP_ID || "").trim();
const PUBLIC_FIREBASE_MEASUREMENT_ID = String(process.env.FIREBASE_MEASUREMENT_ID || "").trim();
const PUBLIC_FIREBASE_VAPID_KEY = String(process.env.FIREBASE_VAPID_KEY || "").trim();
const FIREBASE_SERVER_KEY = String(process.env.FIREBASE_SERVER_KEY || "").trim();

app.get("/api/public/config", (req, res) => {
  res.setHeader("Cache-Control", "public, max-age=120");
  res.json({
    supabaseUrl: PUBLIC_SUPABASE_URL,
    supabaseAnonKey: PUBLIC_SUPABASE_ANON_KEY,
    razorpayKeyId: PUBLIC_RAZORPAY_KEY_ID,
    firebaseApiKey: PUBLIC_FIREBASE_API_KEY,
    firebaseAuthDomain: PUBLIC_FIREBASE_AUTH_DOMAIN,
    firebaseProjectId: PUBLIC_FIREBASE_PROJECT_ID,
    firebaseStorageBucket: PUBLIC_FIREBASE_STORAGE_BUCKET,
    firebaseMessagingSenderId: PUBLIC_FIREBASE_MESSAGING_SENDER_ID,
    firebaseAppId: PUBLIC_FIREBASE_APP_ID,
    firebaseMeasurementId: PUBLIC_FIREBASE_MEASUREMENT_ID,
    firebaseVapidKey: PUBLIC_FIREBASE_VAPID_KEY
  });
});

app.post("/api/push/register", async (req, res) => {
  const userId = sanitizePushUserId(req.body?.user_id);
  const token = sanitizePushToken(req.body?.token);
  if(!userId || !token){
    return res.status(400).json({ ok:false, error:"user_id_and_token_required" });
  }
  try{
    await mutateSystemState((state) => {
      setPushTokenForUser(state, userId, token, {
        platform: req.body?.platform,
        user_agent: req.headers["user-agent"] || req.body?.user_agent || ""
      });
      return true;
    });
    return res.json({ ok:true });
  }catch(err){
    return res.status(500).json({
      ok:false,
      error:"push_register_failed",
      message:String(err?.message || "Unable to register push token.")
    });
  }
});

app.post("/api/push/notify", async (req, res) => {
  const toUserId = sanitizePushUserId(req.body?.to_user_id);
  const title = String(req.body?.title || "NOVAGAPP").trim().slice(0, 120);
  const body = String(req.body?.body || "").trim().slice(0, 300);
  if(!toUserId || !title){
    return res.status(400).json({ ok:false, error:"to_user_id_and_title_required" });
  }
  try{
    const tokens = await mutateSystemState((state) => getPushTokensForUser(state, toUserId));
    if(!Array.isArray(tokens) || tokens.length === 0){
      return res.json({ ok:true, sent:0, failed:0, skipped:"no_tokens" });
    }
    const result = await sendFcmNotificationToTokens(tokens, {
      title,
      body,
      data: req.body?.data || {}
    });
    return res.json({
      ok: !!result?.ok,
      sent: Number(result?.sent || 0),
      failed: Number(result?.failed || 0),
      error: result?.error || ""
    });
  }catch(err){
    return res.status(500).json({
      ok:false,
      error:"push_notify_failed",
      message:String(err?.message || "Unable to send push notification.")
    });
  }
});

function getSocialSupabaseBase(){
  return String(PUBLIC_SUPABASE_URL || CONTEST_SUPABASE_URL || "").trim().replace(/\/+$/g, "");
}

function getSocialSupabaseServiceRoleKey(){
  const candidates = [
    CONTEST_SUPABASE_SERVICE_ROLE_KEY,
    process.env.SUPABASE_SERVICE_ROLE_KEY,
    process.env.CONTEST_SUPABASE_SERVICE_ROLE_KEY
  ];
  for(const value of candidates){
    const key = String(value || "").trim();
    if(key) return key;
  }
  return "";
}

function getSocialSupabaseReadKey(){
  const candidates = [
    CONTEST_SUPABASE_SERVICE_ROLE_KEY,
    process.env.SUPABASE_SERVICE_ROLE_KEY,
    process.env.CONTEST_SUPABASE_SERVICE_ROLE_KEY,
    PUBLIC_SUPABASE_ANON_KEY
  ];
  for(const value of candidates){
    const key = String(value || "").trim();
    if(key) return key;
  }
  return "";
}

function detectMissingColumnName(rawText){
  const text = String(rawText || "");
  if(!text) return "";
  const pgRestMatch = text.match(/'([a-zA-Z0-9_]+)' column/i);
  if(pgRestMatch && pgRestMatch[1]){
    return String(pgRestMatch[1]).toLowerCase();
  }
  const pgMatch = text.match(/column\s+\"?([a-zA-Z0-9_.]+)\"?\s+does not exist/i);
  if(pgMatch && pgMatch[1]){
    return String(pgMatch[1]).replace(/\"/g, "").toLowerCase().split(".").pop() || "";
  }
  return "";
}

async function fetchPublicLongVideos(params){
  const base = getSocialSupabaseBase();
  const key = getSocialSupabaseReadKey();
  if(!base || !key){
    throw new Error("social_supabase_unconfigured");
  }

  const page = Math.max(0, Math.floor(Number(params?.page) || 0));
  const size = Math.max(1, Math.min(20, Math.floor(Number(params?.size) || 8)));
  const focus = sanitizeToken(params?.focus, 80);
  const searchRaw = String(params?.q || "").trim().replace(/[\r\n]/g, " ");
  const search = searchRaw.replace(/[*%]/g, "").slice(0, 120);

  const endpoint = new URL("/rest/v1/long_videos", base + "/");
  endpoint.searchParams.set("select", "id,user_id,video_url,thumb_url,title,description,created_at");
  endpoint.searchParams.set("order", "created_at.desc");
  if(focus){
    endpoint.searchParams.set("id", "eq." + focus);
  }else{
    if(search){
      endpoint.searchParams.set("title", "ilike.*" + search + "*");
    }
  }

  const headers = {
    apikey: key,
    Authorization: `Bearer ${key}`
  };
  if(!focus){
    const from = page * size;
    const to = from + size - 1;
    headers.Range = `${from}-${to}`;
  }else{
    headers.Range = "0-0";
  }

  const response = await fetch(endpoint.toString(), {
    method: "GET",
    headers
  });
  if(!response.ok){
    const body = await response.text().catch(() => "");
    throw new Error(`social_long_videos_fetch_failed_${response.status}:${body.slice(0, 200)}`);
  }
  const rows = await response.json().catch(() => []);
  return Array.isArray(rows) ? rows : [];
}

function isAutomationSupabaseConfigured(){
  return Boolean(getSocialSupabaseBase() && getSocialSupabaseServiceRoleKey());
}

async function automationSupabaseUpsert(tableName, rows, onConflict){
  if(!isAutomationSupabaseConfigured()) return;
  const table = String(tableName || "").trim();
  const conflict = String(onConflict || "").trim();
  const list = Array.isArray(rows) ? rows.filter(Boolean) : [];
  if(!table || !conflict || !list.length) return;

  const base = getSocialSupabaseBase();
  const key = getSocialSupabaseServiceRoleKey();
  const endpoint = `${base}/rest/v1/${table}?on_conflict=${encodeURIComponent(conflict)}`;
  const response = await fetch(endpoint, {
    method: "POST",
    headers: {
      apikey: key,
      Authorization: `Bearer ${key}`,
      "Content-Type": "application/json",
      Prefer: "resolution=merge-duplicates,return=minimal"
    },
    body: JSON.stringify(list)
  });
  if(!response.ok){
    const body = await response.text().catch(() => "");
    throw new Error(`automation_supabase_upsert_failed_${table}_${response.status}:${body.slice(0, 220)}`);
  }
}

async function fetchRemoteSubscriptionState(userId){
  const uid = sanitizeUserId(userId);
  if(!uid || !isAutomationSupabaseConfigured()){
    return null;
  }
  const base = getSocialSupabaseBase();
  const key = getSocialSupabaseServiceRoleKey();
  const endpoint = new URL("/rest/v1/subscription_state", base + "/");
  endpoint.searchParams.set("select", "user_id,plan,status,expires_at,source,payment_id,order_id,amount_paise,currency,updated_at");
  endpoint.searchParams.set("user_id", `eq.${uid}`);
  endpoint.searchParams.set("limit", "1");
  const response = await fetch(endpoint.toString(), {
    method: "GET",
    headers: {
      apikey: key,
      Authorization: `Bearer ${key}`
    }
  });
  if(!response.ok){
    return null;
  }
  const rows = await response.json().catch(() => []);
  if(!Array.isArray(rows) || !rows[0]){
    return null;
  }
  return rows[0];
}

async function fetchRemoteWebhookEvent(eventId){
  const id = String(eventId || "").trim().slice(0, 120);
  if(!id || !isAutomationSupabaseConfigured()){
    return null;
  }
  const base = getSocialSupabaseBase();
  const key = getSocialSupabaseServiceRoleKey();
  const endpoint = new URL("/rest/v1/subscription_webhook_events", base + "/");
  endpoint.searchParams.set("select", "event_id,event_name,payment_id,order_id,created_at");
  endpoint.searchParams.set("event_id", `eq.${id}`);
  endpoint.searchParams.set("limit", "1");
  const response = await fetch(endpoint.toString(), {
    method: "GET",
    headers: {
      apikey: key,
      Authorization: `Bearer ${key}`
    }
  });
  if(!response.ok){
    return null;
  }
  const rows = await response.json().catch(() => []);
  if(!Array.isArray(rows) || !rows[0]){
    return null;
  }
  return rows[0];
}

function normalizeLongVideoPublishPayload(raw){
  const userId = sanitizeUserId(raw?.user_id);
  const videoUrl = String(raw?.video_url || "").trim().slice(0, 3000);
  if(!userId || !videoUrl){
    return null;
  }
  const thumbUrl = String(raw?.thumb_url || "").trim().slice(0, 3000);
  const title = String(raw?.title || "").trim().slice(0, 180);
  const description = String(raw?.description || "").trim().slice(0, 2500);
  const keywordsRaw = raw?.keywords;
  const keywordsText = Array.isArray(keywordsRaw)
    ? keywordsRaw.map(item => String(item || "").trim()).filter(Boolean).join(", ")
    : String(keywordsRaw || "").trim();
  const durationValue = safeNumber(raw?.duration);
  const widthValue = safeNumber(raw?.width);
  const heightValue = safeNumber(raw?.height);

  return {
    user_id: userId,
    video_url: videoUrl,
    thumb_url: thumbUrl || null,
    title: title || null,
    description: description || null,
    keywords: keywordsText ? keywordsText.slice(0, 1500) : null,
    duration: Number.isFinite(durationValue) && durationValue > 0 ? Math.round(durationValue) : null,
    width: Number.isFinite(widthValue) && widthValue > 0 ? Math.round(widthValue) : null,
    height: Number.isFinite(heightValue) && heightValue > 0 ? Math.round(heightValue) : null,
    is_public: true,
    visibility: "public",
    public: true
  };
}

async function insertPublicLongVideoRow(rawPayload){
  const base = getSocialSupabaseBase();
  const key = getSocialSupabaseServiceRoleKey();
  if(!base || !key){
    throw new Error("social_supabase_service_unconfigured");
  }

  const payload = normalizeLongVideoPublishPayload(rawPayload);
  if(!payload){
    throw new Error("long_video_payload_invalid");
  }

  const endpoint = new URL("/rest/v1/long_videos", base + "/");
  const headers = {
    apikey: key,
    Authorization: `Bearer ${key}`,
    "Content-Type": "application/json",
    Prefer: "return=representation"
  };

  const requiredColumns = new Set(["user_id", "video_url"]);
  const row = { ...payload };
  while(true){
    const response = await fetch(endpoint.toString(), {
      method: "POST",
      headers,
      body: JSON.stringify([row])
    });
    if(response.ok){
      const inserted = await response.json().catch(() => []);
      if(Array.isArray(inserted) && inserted[0]){
        return inserted[0];
      }
      return row;
    }

    const body = await response.text().catch(() => "");
    const missingColumn = detectMissingColumnName(body);
    if(
      missingColumn &&
      Object.prototype.hasOwnProperty.call(row, missingColumn) &&
      !requiredColumns.has(missingColumn)
    ){
      delete row[missingColumn];
      continue;
    }
    throw new Error(`social_long_videos_insert_failed_${response.status}:${body.slice(0, 220)}`);
  }
}

app.get("/api/public/long-videos", async (req, res) => {
  try{
    const rows = await fetchPublicLongVideos(req.query || {});
    res.setHeader("Cache-Control", "no-store");
    return res.json({ ok:true, data:rows });
  }catch(err){
    const message = String(err?.message || "public_long_videos_failed");
    const status = message.includes("unconfigured") ? 503 : 500;
    return res.status(status).json({
      ok:false,
      error:"public_long_videos_failed",
      message
    });
  }
});

app.post("/api/public/long-videos/publish", async (req, res) => {
  try{
    const inserted = await insertPublicLongVideoRow(req.body || {});
    return res.json({ ok:true, data:inserted || null });
  }catch(err){
    const message = String(err?.message || "public_long_videos_publish_failed");
    const code = message.includes("payload_invalid")
      ? 400
      : (message.includes("service_unconfigured") ? 503 : 500);
    return res.status(code).json({
      ok:false,
      error:"public_long_videos_publish_failed",
      message
    });
  }
});

async function ensureDir(dir){
  await fs.mkdir(dir, { recursive: true });
}

async function readManualRequests(){
  try{
    const raw = await fs.readFile(MANUAL_JSON_PATH, "utf8");
    const parsed = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed) ? parsed : [];
  }catch(_){
    return [];
  }
}

async function writeManualRequests(items){
  await ensureDir(MANUAL_DIR);
  await fs.writeFile(MANUAL_JSON_PATH, JSON.stringify(items, null, 2), "utf8");
}

function inferExtFromMime(mime){
  const m = String(mime || "").toLowerCase();
  if(m.includes("png")) return "png";
  if(m.includes("webp")) return "webp";
  if(m.includes("jpg") || m.includes("jpeg")) return "jpg";
  if(m.includes("gif")) return "gif";
  return "jpg";
}

function normalizePlanCode(planInput){
  const raw = String(planInput || "free").trim().toLowerCase();
  if(
    raw === "4000" ||
    raw === "enterprise_4000" ||
    raw === "enterprise" ||
    raw === "business" ||
    raw === "business_999"
  ) return "4000";
  if(
    raw === "40" ||
    raw === "pro_40" ||
    raw === "pro" ||
    raw === "plus" ||
    raw === "creator" ||
    raw === "plus_99" ||
    raw === "creator_299"
  ) return "40";
  return "free";
}

function getPlanDailyLimit(planCode){
  if(planCode === "4000") return Infinity;
  if(planCode === "40") return PRO_PLAN_DAILY_LIMIT;
  return FREE_PLAN_DAILY_LIMIT;
}

function manualProcessingResponse(planCode, extras = {}){
  const safePlan = normalizePlanCode(planCode);
  const limit = getPlanDailyLimit(safePlan);
  return {
    status: "manual_processing",
    plan: safePlan,
    limit: limit === Infinity ? null : limit,
    waiting_message: "Just wait for moment your images will be ready.",
    dm_delivery_message: "Technical issues are active right now, so your image cannot be generated at this moment. Your image will be delivered automatically in DM within 24 hours.",
    pricing_info: `Per image price is $${TRYON_PRICE_PER_IMAGE_USD}, but due to this issue your image is free.`,
    free_delivery: true,
    download_allowed: true,
    ...extras
  };
}

function buildAdminNotificationSummary(requestItem){
  const product = requestItem?.product_info || {};
  return {
    request_id: requestItem?.request_id || "",
    admin_dm_email: ADMIN_DM_EMAIL,
    user_id: requestItem?.user_id || "guest",
    username: requestItem?.username || "guest",
    user_email: requestItem?.user_email || "",
    plan: normalizePlanCode(requestItem?.plan),
    product_id: String(product.id || ""),
    product_name: String(product.name || ""),
    product_category: String(product.category || ""),
    uploaded_image_path: requestItem?.uploaded_image_path || "",
    timestamp: requestItem?.timestamp || new Date().toISOString()
  };
}

async function sendAdminWebhook(summary){
  if(!ADMIN_DM_WEBHOOK_URL) return;
  const webhookRes = await fetch(ADMIN_DM_WEBHOOK_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      event: "tryon_manual_request",
      admin_dm_email: ADMIN_DM_EMAIL,
      payload: summary
    })
  });
  if(!webhookRes.ok){
    const body = await webhookRes.text().catch(()=>"");
    throw new Error(`admin_webhook_failed_${webhookRes.status}:${body.slice(0, 220)}`);
  }
}

async function sendAdminEmail(summary){
  if(!RESEND_API_KEY) return;

  const lines = [
    "New manual try-on request",
    `Request ID: ${summary.request_id}`,
    `Admin DM Email: ${summary.admin_dm_email}`,
    `User ID: ${summary.user_id}`,
    `Username: ${summary.username}`,
    `User Email: ${summary.user_email || "-"}`,
    `Plan: ${summary.plan}`,
    `Product ID: ${summary.product_id || "-"}`,
    `Product Name: ${summary.product_name || "-"}`,
    `Product Category: ${summary.product_category || "-"}`,
    `Uploaded Image Path: ${summary.uploaded_image_path || "-"}`,
    `Timestamp: ${summary.timestamp}`
  ];

  const emailRes = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${RESEND_API_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      from: RESEND_FROM_EMAIL,
      to: [ADMIN_DM_EMAIL],
      subject: `Manual Try-On Request: ${summary.request_id || "new"}`,
      text: lines.join("\n")
    })
  });

  if(!emailRes.ok){
    const body = await emailRes.text().catch(()=>"");
    throw new Error(`admin_email_failed_${emailRes.status}:${body.slice(0, 220)}`);
  }
}

async function notifyAdminManualRequest(requestItem){
  const summary = buildAdminNotificationSummary(requestItem);
  const tasks = [];
  if(ADMIN_DM_WEBHOOK_URL) tasks.push(sendAdminWebhook(summary));
  if(RESEND_API_KEY) tasks.push(sendAdminEmail(summary));
  if(tasks.length === 0){
    return;
  }
  const results = await Promise.allSettled(tasks);
  results.forEach(result => {
    if(result.status === "rejected"){
      console.error("Manual request notify failed:", result.reason?.stack || result.reason);
    }
  });
}

async function saveManualRequest(payload){
  await ensureDir(MANUAL_DIR);

  const ext = inferExtFromMime(payload.userImageMime || "image/jpeg");
  const imageFile = `${Date.now()}_${crypto.randomBytes(4).toString("hex")}.${ext}`;
  const imageAbsPath = path.join(MANUAL_DIR, imageFile);
  const imageRelPath = path.posix.join("uploads", "manual-requests", imageFile);

  if(payload.userImageBuffer && payload.userImageBuffer.length){
    await fs.writeFile(imageAbsPath, payload.userImageBuffer);
  }

  const list = await readManualRequests();
  const requestId = typeof crypto.randomUUID === "function"
    ? crypto.randomUUID()
    : `${Date.now()}_${crypto.randomBytes(4).toString("hex")}`;
  const requestItem = {
    request_id: requestId,
    user_id: payload.userId || "guest",
    username: payload.username || "guest",
    user_email: payload.userEmail || "",
    admin_dm_email: ADMIN_DM_EMAIL,
    plan: normalizePlanCode(payload.plan),
    language: payload.language || "en",
    uploaded_image_path: imageRelPath,
    product_info: payload.productInfo || {},
    timestamp: new Date().toISOString()
  };
  list.push(requestItem);
  await writeManualRequests(list);
  await notifyAdminManualRequest(requestItem);
  return requestItem;
}

async function fetchImageBuffer(imageUrl){
  if(!imageUrl) return { ok:false, status:0, statusText:"Missing image URL", contentType:"" };

  if(imageUrl.startsWith("data:")){
    const m = imageUrl.match(/^data:(.+?);base64,(.+)$/);
    if(!m || !m[2]){
      return { ok:false, status:0, statusText:"Invalid data URL", contentType:"" };
    }
    return {
      ok: true,
      status: 200,
      statusText: "OK",
      contentType: m[1] || "image/jpeg",
      buffer: Buffer.from(m[2], "base64")
    };
  }

  const res = await fetch(imageUrl, {
    headers: {
      "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
      "Accept":"image/*"
    }
  });
  const contentType = res.headers.get("content-type") || "";
  if(!res.ok){
    return { ok:false, status:res.status, statusText:res.statusText, contentType };
  }
  return {
    ok: true,
    status: res.status,
    statusText: res.statusText,
    contentType,
    buffer: Buffer.from(await res.arrayBuffer())
  };
}

function isPrivateOfflineProxyHost(hostname){
  const host = String(hostname || "").trim().toLowerCase();
  if(!host) return true;
  if(host === "localhost" || host === "127.0.0.1" || host === "::1" || host === "0.0.0.0"){
    return true;
  }
  if(host.startsWith("10.") || host.startsWith("192.168.")){
    return true;
  }
  const m172 = host.match(/^172\.(\d+)\./);
  if(m172){
    const second = Number(m172[1]);
    if(Number.isFinite(second) && second >= 16 && second <= 31){
      return true;
    }
  }
  return false;
}

function parseSafeOfflineVideoUrl(urlInput){
  const raw = String(urlInput || "").trim();
  if(!raw || raw.length > 3000){
    return null;
  }
  try{
    const parsed = new URL(raw);
    if(parsed.protocol !== "http:" && parsed.protocol !== "https:"){
      return null;
    }
    if(isPrivateOfflineProxyHost(parsed.hostname)){
      return null;
    }
    return parsed.toString();
  }catch(_){
    return null;
  }
}

let systemStateQueue = Promise.resolve();

function createDefaultSystemState(){
  const nowIso = new Date().toISOString();
  return {
    version: 1,
    created_at: nowIso,
    updated_at: nowIso,
    identities: {},
    subscriptions: {
      users: {},
      payments: {},
      webhooks: {},
      transitions: []
    },
    premium: {
      seats: {},
      invites: {}
    },
    verification: {
      by_user: {},
      requests: {},
      history: []
    },
    automation: {
      funnel_events: [],
      media_events: [],
      notifications: [],
      followups: [],
      reports: {}
    },
    tryon: {
      abuse: {},
      lane_metrics: {
        standard_queued: 0,
        priority_queued: 0,
        completed: 0,
        failed: 0
      }
    }
  };
}

function normalizeSystemState(input){
  const state = input && typeof input === "object"
    ? input
    : createDefaultSystemState();

  state.version = Math.max(1, Math.floor(safeNumber(state.version) || 1));
  if(!state.created_at) state.created_at = new Date().toISOString();
  if(!state.updated_at) state.updated_at = new Date().toISOString();

  if(!state.identities || typeof state.identities !== "object"){
    state.identities = {};
  }
  if(!state.subscriptions || typeof state.subscriptions !== "object"){
    state.subscriptions = {};
  }
  if(!state.subscriptions.users || typeof state.subscriptions.users !== "object"){
    state.subscriptions.users = {};
  }
  if(!state.subscriptions.payments || typeof state.subscriptions.payments !== "object"){
    state.subscriptions.payments = {};
  }
  if(!state.subscriptions.webhooks || typeof state.subscriptions.webhooks !== "object"){
    state.subscriptions.webhooks = {};
  }
  if(!Array.isArray(state.subscriptions.transitions)){
    state.subscriptions.transitions = [];
  }
  if(!state.premium || typeof state.premium !== "object"){
    state.premium = {};
  }
  if(!state.premium.seats || typeof state.premium.seats !== "object"){
    state.premium.seats = {};
  }
  if(!state.premium.invites || typeof state.premium.invites !== "object"){
    state.premium.invites = {};
  }
  if(!state.verification || typeof state.verification !== "object"){
    state.verification = {};
  }
  if(!state.verification.by_user || typeof state.verification.by_user !== "object"){
    state.verification.by_user = {};
  }
  if(!state.verification.requests || typeof state.verification.requests !== "object"){
    state.verification.requests = {};
  }
  if(!Array.isArray(state.verification.history)){
    state.verification.history = [];
  }
  if(!state.automation || typeof state.automation !== "object"){
    state.automation = {};
  }
  if(!Array.isArray(state.automation.funnel_events)){
    state.automation.funnel_events = [];
  }
  if(!Array.isArray(state.automation.media_events)){
    state.automation.media_events = [];
  }
  if(!Array.isArray(state.automation.notifications)){
    state.automation.notifications = [];
  }
  if(!Array.isArray(state.automation.followups)){
    state.automation.followups = [];
  }
  if(!state.automation.reports || typeof state.automation.reports !== "object"){
    state.automation.reports = {};
  }
  if(!state.push || typeof state.push !== "object"){
    state.push = {};
  }
  if(!state.push.tokens_by_user || typeof state.push.tokens_by_user !== "object"){
    state.push.tokens_by_user = {};
  }
  if(!state.push.token_index || typeof state.push.token_index !== "object"){
    state.push.token_index = {};
  }
  if(!state.tryon || typeof state.tryon !== "object"){
    state.tryon = {};
  }
  if(!state.tryon.abuse || typeof state.tryon.abuse !== "object"){
    state.tryon.abuse = {};
  }
  if(!state.tryon.lane_metrics || typeof state.tryon.lane_metrics !== "object"){
    state.tryon.lane_metrics = {};
  }
  state.tryon.lane_metrics.standard_queued = safeNumber(state.tryon.lane_metrics.standard_queued);
  state.tryon.lane_metrics.priority_queued = safeNumber(state.tryon.lane_metrics.priority_queued);
  state.tryon.lane_metrics.completed = safeNumber(state.tryon.lane_metrics.completed);
  state.tryon.lane_metrics.failed = safeNumber(state.tryon.lane_metrics.failed);

  return state;
}

function sanitizePushToken(value){
  const token = String(value || "").trim();
  if(!token) return "";
  if(token.length < 20 || token.length > 4096) return "";
  return token;
}

function sanitizePushUserId(value){
  return sanitizeUserId(value);
}

function sanitizePushData(input){
  const out = {};
  if(!input || typeof input !== "object") return out;
  Object.keys(input).slice(0, 30).forEach((key) => {
    const safeKey = String(key || "").trim().slice(0, 60);
    if(!safeKey) return;
    out[safeKey] = String(input[key] ?? "").slice(0, 600);
  });
  return out;
}

function setPushTokenForUser(state, userIdInput, tokenInput, meta){
  const userId = sanitizePushUserId(userIdInput);
  const token = sanitizePushToken(tokenInput);
  if(!userId || !token) return false;
  if(!state.push || typeof state.push !== "object"){
    state.push = { tokens_by_user:{}, token_index:{} };
  }
  if(!state.push.tokens_by_user || typeof state.push.tokens_by_user !== "object"){
    state.push.tokens_by_user = {};
  }
  if(!state.push.token_index || typeof state.push.token_index !== "object"){
    state.push.token_index = {};
  }

  const previousOwner = String(state.push.token_index[token] || "").trim();
  if(previousOwner && previousOwner !== userId){
    const prevMap = state.push.tokens_by_user[previousOwner];
    if(prevMap && typeof prevMap === "object"){
      delete prevMap[token];
      if(Object.keys(prevMap).length === 0){
        delete state.push.tokens_by_user[previousOwner];
      }
    }
  }

  if(!state.push.tokens_by_user[userId] || typeof state.push.tokens_by_user[userId] !== "object"){
    state.push.tokens_by_user[userId] = {};
  }
  state.push.tokens_by_user[userId][token] = {
    token,
    platform: String(meta?.platform || "").trim().slice(0, 40),
    user_agent: String(meta?.user_agent || "").trim().slice(0, 240),
    updated_at: new Date().toISOString()
  };
  state.push.token_index[token] = userId;
  return true;
}

function getPushTokensForUser(state, userIdInput){
  const userId = sanitizePushUserId(userIdInput);
  if(!userId) return [];
  const map = state?.push?.tokens_by_user?.[userId];
  if(!map || typeof map !== "object") return [];
  return Object.keys(map).map(sanitizePushToken).filter(Boolean).slice(0, 100);
}

async function sendFcmNotificationToTokens(tokens, payload){
  const unique = Array.from(new Set((tokens || []).map(sanitizePushToken).filter(Boolean))).slice(0, 100);
  if(!unique.length){
    return { ok:true, sent:0, failed:0 };
  }
  if(!FIREBASE_SERVER_KEY){
    return { ok:false, sent:0, failed:unique.length, error:"firebase_server_key_missing" };
  }
  const body = {
    registration_ids: unique,
    priority: "high",
    notification: {
      title: String(payload?.title || "NOVAGAPP").slice(0, 120),
      body: String(payload?.body || "").slice(0, 300)
    },
    data: sanitizePushData(payload?.data || {})
  };
  const response = await fetch("https://fcm.googleapis.com/fcm/send", {
    method: "POST",
    headers: {
      Authorization: `key=${FIREBASE_SERVER_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  });
  const raw = await response.text().catch(() => "");
  let parsed = null;
  try{
    parsed = raw ? JSON.parse(raw) : null;
  }catch(_){
    parsed = null;
  }
  if(!response.ok){
    return {
      ok:false,
      sent:0,
      failed:unique.length,
      error: `firebase_send_failed_${response.status}`,
      details: String(raw || "").slice(0, 280)
    };
  }
  return {
    ok:true,
    sent: Math.max(0, Number(parsed?.success || 0)),
    failed: Math.max(0, Number(parsed?.failure || 0))
  };
}

async function readSystemState(){
  try{
    const raw = await fs.readFile(SYSTEM_STATE_PATH, "utf8");
    const parsed = raw ? JSON.parse(raw) : null;
    return normalizeSystemState(parsed);
  }catch(_){
    return createDefaultSystemState();
  }
}

async function writeSystemState(state){
  await ensureDir(SYSTEM_DIR);
  await fs.writeFile(SYSTEM_STATE_PATH, JSON.stringify(state, null, 2), "utf8");
}

async function mutateSystemState(mutator){
  const run = systemStateQueue.then(async () => {
    const state = await readSystemState();
    const result = await mutator(state);
    state.updated_at = new Date().toISOString();
    await writeSystemState(state);
    return result;
  });
  systemStateQueue = run.catch(() => {});
  return run;
}

function pushBounded(list, item, max){
  if(!Array.isArray(list)) return;
  list.push(item);
  const cap = Math.max(1, Math.floor(safeNumber(max) || 1000));
  while(list.length > cap){
    list.shift();
  }
}

function titleCaseWords(text){
  return String(text || "")
    .split(/\s+/)
    .map(part => {
      const token = String(part || "").trim();
      if(!token) return "";
      return token.slice(0, 1).toUpperCase() + token.slice(1).toLowerCase();
    })
    .filter(Boolean)
    .join(" ")
    .slice(0, 64);
}

const IDENTITY_PLACEHOLDER_NAMES = new Set([
  "user",
  "u",
  "guest",
  "unknown",
  "member",
  "unique",
  "new",
  "newuser",
  "test",
  "undefined",
  "null"
]);

function isIdentityPlaceholderName(value){
  const normalized = sanitizeDisplayName(value).toLowerCase();
  if(!normalized) return true;
  if(IDENTITY_PLACEHOLDER_NAMES.has(normalized)) return true;
  if(
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(normalized)
  ){
    return true;
  }
  if(/^member[\s._-]*[a-z0-9]{4,}$/i.test(normalized)) return true;
  if(/^user[\s._-]*[a-z0-9]{4,}$/i.test(normalized)) return true;
  return false;
}

function isLikelyGeneratedIdentityHandle(value){
  const raw = sanitizeDisplayName(value).toLowerCase();
  if(!raw) return false;
  if(raw.startsWith("@")) return true;
  if(/^memer[\s._-]*[a-z0-9]{1,}$/i.test(raw)) return true;
  if(/^member[\s._-]*[a-z0-9]{1,}$/i.test(raw)) return true;
  if(/^user[\s._-]*[a-z0-9]{1,}$/i.test(raw)) return true;
  if(/^unique[\s._-]*[a-z0-9]{0,}$/i.test(raw)) return true;
  const hasSpace = /\s/.test(raw);
  const hasDigits = /\d/.test(raw);
  const hasSeparators = /[._-]/.test(raw);
  if(!hasSpace && (hasDigits || hasSeparators)){
    return true;
  }
  return false;
}

function splitCompactIdentityToken(token){
  const clean = String(token || "").toLowerCase().replace(/[^a-z]+/g, "");
  if(clean.length < 9){
    return clean ? [clean] : [];
  }
  const mid = Math.floor(clean.length / 2);
  const minSide = 3;
  let bestIdx = -1;
  let bestWeight = -Infinity;
  for(let i = minSide; i <= clean.length - minSide; i += 1){
    const prev = clean[i - 1];
    const next = clean[i];
    const prevVowel = /[aeiou]/.test(prev);
    const nextVowel = /[aeiou]/.test(next);
    const dist = Math.abs(i - mid);
    let weight = 0;
    if(prevVowel && !nextVowel) weight += 4;
    if(!prevVowel && nextVowel) weight += 3;
    if(dist <= 1) weight += 3;
    else if(dist <= 2) weight += 2;
    else if(dist <= 4) weight += 1;
    if(i >= 4 && clean.length - i >= 4) weight += 1;
    if(weight > bestWeight){
      bestWeight = weight;
      bestIdx = i;
    }
  }
  if(bestIdx <= 0){
    return [clean];
  }
  const left = clean.slice(0, bestIdx).trim();
  const right = clean.slice(bestIdx).trim();
  if(!left || !right){
    return [clean];
  }
  return [left, right];
}

function parseEmailLocalToWords(localPart){
  const cleaned = String(localPart || "")
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, " ")
    .replace(/[._-]+/g, " ")
    .replace(/\d+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if(!cleaned){
    return "";
  }
  const expanded = [];
  cleaned.split(/\s+/).forEach(piece => {
    if(!piece) return;
    splitCompactIdentityToken(piece).forEach(part => {
      const token = String(part || "").trim();
      if(token) expanded.push(token);
    });
  });
  return expanded.join(" ").trim().slice(0, 80);
}

function tokenizeIdentityText(value){
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9@._ -]+/g, " ")
    .split(/[\s@._-]+/)
    .map(part => part.trim())
    .filter(Boolean)
    .slice(0, 40);
}

function deriveIdentityDefaults(input){
  const userId = sanitizeUserId(input?.user_id);
  const email = String(input?.email || "").trim().toLowerCase().slice(0, 180);
  const providedName = sanitizeDisplayName(
    input?.display_name ||
    input?.full_name ||
    input?.username ||
    input?.user_name ||
    ""
  );
  const rawName = isIdentityPlaceholderName(providedName) ? "" : providedName;
  const emailLocal = String((email.split("@")[0] || "")).trim();
  const emailLocalWords = parseEmailLocalToWords(emailLocal);
  const displaySeed = rawName || emailLocalWords || emailLocal || "Member";
  const displayName = titleCaseWords(displaySeed) || "Member";
  const providedUsername = sanitizeDisplayName(input?.username || "");
  const usernameSeed = isIdentityPlaceholderName(providedUsername)
    ? (emailLocal || emailLocalWords || rawName || displayName || "member")
    : (providedUsername || emailLocal || emailLocalWords || rawName || displayName || "member");
  const userSeed = String(usernameSeed)
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 24);
  const username = userSeed || `member_${String(userId || "").replace(/[^a-z0-9]/gi, "").slice(0, 6) || "user"}`;
  const photo = String(input?.photo || input?.avatar_url || "").trim().slice(0, 3000);
  const searchTokens = Array.from(new Set([
    ...tokenizeIdentityText(displayName),
    ...tokenizeIdentityText(username),
    ...tokenizeIdentityText(emailLocalWords),
    ...tokenizeIdentityText(emailLocal),
    ...tokenizeIdentityText(email)
  ])).slice(0, 48);

  return {
    user_id: userId,
    email,
    email_local: emailLocal.slice(0, 120),
    display_name: displayName,
    username,
    photo,
    search_tokens: searchTokens,
    updated_at: new Date().toISOString()
  };
}

function safePublicNameFromIdentity(identity, userId){
  const primaryName = sanitizeDisplayName(
    identity?.display_name ||
    identity?.full_name ||
    ""
  );
  if(
    primaryName &&
    !isIdentityPlaceholderName(primaryName) &&
    !isLikelyGeneratedIdentityHandle(primaryName)
  ){
    return titleCaseWords(primaryName) || primaryName;
  }
  const fromEmailLocal = parseEmailLocalToWords(
    identity?.email_local ||
    String(identity?.email || "").split("@")[0] ||
    ""
  );
  if(fromEmailLocal){
    return titleCaseWords(fromEmailLocal) || "Member";
  }
  const fromHandle = parseEmailLocalToWords(identity?.username || primaryName || "");
  if(fromHandle && !isIdentityPlaceholderName(fromHandle)){
    return titleCaseWords(fromHandle) || "Member";
  }
  const usernamePreferred = sanitizeDisplayName(identity?.username || "");
  if(
    usernamePreferred &&
    !isIdentityPlaceholderName(usernamePreferred) &&
    !isLikelyGeneratedIdentityHandle(usernamePreferred)
  ){
    return titleCaseWords(usernamePreferred) || usernamePreferred;
  }
  const shortId = String(userId || "")
    .replace(/[^a-z0-9]/gi, "")
    .slice(0, 6)
    .toUpperCase();
  return shortId ? `Member ${shortId}` : "Member";
}

function getPlanConfig(planCode){
  const code = normalizePlanCode(planCode);
  if(code === "4000"){
    return {
      code,
      billing_days: PLAN_BILLING_DAYS,
      monthly_price_usd: PLAN_4000_USD,
      seat_limit: PLAN_4000_SEAT_LIMIT,
      tryon_daily_limit: Infinity,
      tryon_lane: "priority",
      features: {
        chat: true,
        ai_tryon: true,
        checkout: true,
        analytics: true,
        conversion_tracking: true,
        smart_dm_assistant: true,
        auto_followup: true,
        monthly_report: true,
        dedicated_processing_lane: true,
        advanced_revenue_assistant: true,
        behavioral_insights: true,
        elite_badge: true,
        invite_only: true,
        limited_seats: true,
        premium_visibility_boost: true
      }
    };
  }
  if(code === "40"){
    return {
      code,
      billing_days: PLAN_BILLING_DAYS,
      monthly_price_usd: PLAN_40_USD,
      seat_limit: 1,
      tryon_daily_limit: PRO_PLAN_DAILY_LIMIT,
      tryon_lane: "standard",
      features: {
        chat: true,
        ai_tryon: true,
        checkout: true,
        analytics: true,
        conversion_tracking: true,
        smart_dm_assistant: true,
        auto_followup: true,
        monthly_report: true,
        priority_support: true
      }
    };
  }
  return {
    code: "free",
    billing_days: 0,
    monthly_price_usd: 0,
    seat_limit: 1,
    tryon_daily_limit: FREE_PLAN_DAILY_LIMIT,
    tryon_lane: "standard",
    features: {
      chat: true,
      ai_tryon: true,
      checkout: true
    }
  };
}

function getSubscriptionExpiryIso(planCode, startedAtIso){
  const cfg = getPlanConfig(planCode);
  if(!cfg.billing_days || cfg.billing_days <= 0){
    return null;
  }
  const started = new Date(startedAtIso || Date.now());
  const expires = new Date(started.getTime() + (cfg.billing_days * 24 * 60 * 60 * 1000));
  return expires.toISOString();
}

function isIsoFuture(iso){
  if(!iso) return false;
  const t = new Date(iso).getTime();
  return Number.isFinite(t) && t > Date.now();
}

function normalizeSubscriptionRecord(userId, raw){
  const nowIso = new Date().toISOString();
  const normalizedUserId = sanitizeUserId(userId || raw?.user_id);
  const plan = normalizePlanCode(raw?.plan);
  const statusRaw = String(raw?.status || "").trim().toLowerCase();
  let status = statusRaw || "free";
  if(plan === "free" && status === "active"){
    status = "free";
  }
  const startedAt = String(raw?.started_at || raw?.created_at || nowIso);
  const expiresAt = raw?.expires_at ? String(raw.expires_at) : getSubscriptionExpiryIso(plan, startedAt);
  if(plan !== "free" && status === "active" && expiresAt && !isIsoFuture(expiresAt)){
    status = "expired";
  }
  if(plan === "free" && (status === "expired" || status === "downgraded")){
    status = "free";
  }
  return {
    user_id: normalizedUserId,
    plan,
    status,
    started_at: startedAt,
    expires_at: expiresAt,
    source: String(raw?.source || "system").trim().slice(0, 40) || "system",
    amount_paise: Math.max(0, Math.round(safeNumber(raw?.amount_paise))),
    currency: sanitizeCurrencyCode(raw?.currency) || "INR",
    last_payment_id: String(raw?.last_payment_id || raw?.payment_id || "").trim().slice(0, 120),
    last_order_id: String(raw?.last_order_id || raw?.order_id || "").trim().slice(0, 120),
    metadata: raw?.metadata && typeof raw.metadata === "object" ? raw.metadata : {},
    created_at: String(raw?.created_at || nowIso),
    updated_at: nowIso
  };
}

function buildFreeSubscription(userId, reason){
  const nowIso = new Date().toISOString();
  return {
    user_id: sanitizeUserId(userId),
    plan: "free",
    status: "free",
    started_at: nowIso,
    expires_at: null,
    source: reason || "fallback",
    amount_paise: 0,
    currency: "INR",
    last_payment_id: "",
    last_order_id: "",
    metadata: {},
    created_at: nowIso,
    updated_at: nowIso
  };
}

function ensureActiveSeatState(state, ownerUserId){
  const ownerId = sanitizeUserId(ownerUserId);
  if(!ownerId) return null;
  const existing = state.premium.seats[ownerId];
  const nowIso = new Date().toISOString();
  const row = existing && typeof existing === "object"
    ? existing
    : {
      owner_user_id: ownerId,
      seat_limit: PLAN_4000_SEAT_LIMIT,
      members: [ownerId],
      created_at: nowIso,
      updated_at: nowIso
    };
  row.owner_user_id = ownerId;
  row.seat_limit = Math.max(1, Math.floor(safeNumber(row.seat_limit) || PLAN_4000_SEAT_LIMIT));
  row.members = Array.isArray(row.members)
    ? Array.from(new Set(row.members.map(sanitizeUserId).filter(Boolean)))
    : [ownerId];
  if(!row.members.includes(ownerId)){
    row.members.unshift(ownerId);
  }
  if(!row.created_at) row.created_at = nowIso;
  row.updated_at = nowIso;
  state.premium.seats[ownerId] = row;
  return row;
}

function getSeatOwnerForMember(state, memberUserId){
  const targetId = sanitizeUserId(memberUserId);
  if(!targetId) return "";
  const entries = Object.entries(state.premium?.seats || {});
  for(const [ownerId, seatRow] of entries){
    const members = Array.isArray(seatRow?.members) ? seatRow.members : [];
    if(members.includes(targetId)){
      return sanitizeUserId(ownerId);
    }
  }
  return "";
}

function getEffectiveSubscription(state, userId){
  const normalizedUserId = sanitizeUserId(userId);
  if(!normalizedUserId){
    return buildFreeSubscription("", "guest");
  }

  const direct = normalizeSubscriptionRecord(
    normalizedUserId,
    state.subscriptions.users[normalizedUserId] || buildFreeSubscription(normalizedUserId, "missing")
  );
  if(direct.plan !== "free" && direct.status === "active"){
    return direct;
  }

  const ownerId = getSeatOwnerForMember(state, normalizedUserId);
  if(ownerId && ownerId !== normalizedUserId){
    const owner = normalizeSubscriptionRecord(ownerId, state.subscriptions.users[ownerId] || {});
    if(owner.plan === "4000" && owner.status === "active" && isIsoFuture(owner.expires_at)){
      return {
        ...owner,
        user_id: normalizedUserId,
        source: "seat_invite",
        metadata: {
          ...(owner.metadata || {}),
          seat_owner_id: ownerId
        }
      };
    }
  }

  return buildFreeSubscription(normalizedUserId, "inactive");
}

function buildFeatureGateForSubscription(subRecord){
  const record = normalizeSubscriptionRecord(subRecord?.user_id, subRecord || {});
  const config = getPlanConfig(record.plan);
  const active = record.plan === "free"
    ? true
    : (record.status === "active" && isIsoFuture(record.expires_at));
  const features = active ? { ...config.features } : { chat:true, ai_tryon:true, checkout:true };
  const plan = active ? record.plan : "free";
  const paidActive = plan !== "free";
  const queueLane = active ? config.tryon_lane : "standard";
  const premiumBadge = paidActive;
  const autoActivationMessage = paidActive
    ? "Payment verified. Plan auto-activated. No manual activation required."
    : "Free plan active.";

  return {
    user_id: record.user_id,
    plan,
    status: active ? record.status : "free",
    expires_at: active ? record.expires_at : null,
    source: record.source,
    seat_limit: config.seat_limit,
    tryon_lane: queueLane,
    tryon_daily_limit: active ? config.tryon_daily_limit : FREE_PLAN_DAILY_LIMIT,
    plan_active: paidActive,
    premium_badge: premiumBadge,
    activation_mode: "automatic",
    auto_activation_message: autoActivationMessage,
    features
  };
}

function ensureSubscriptionExpirySweep(state){
  const nowIso = new Date().toISOString();
  Object.entries(state.subscriptions.users || {}).forEach(([userId, raw]) => {
    const normalized = normalizeSubscriptionRecord(userId, raw);
    if(normalized.plan !== "free" && normalized.status === "active" && normalized.expires_at && !isIsoFuture(normalized.expires_at)){
      const downgraded = buildFreeSubscription(userId, "auto_expiry_downgrade");
      downgraded.status = "downgraded";
      downgraded.metadata = {
        previous_plan: normalized.plan,
        expired_at: normalized.expires_at || nowIso
      };
      state.subscriptions.users[userId] = downgraded;
      pushBounded(state.subscriptions.transitions, {
        type: "auto_downgrade",
        user_id: userId,
        previous_plan: normalized.plan,
        next_plan: "free",
        expired_at: normalized.expires_at || nowIso,
        at: nowIso
      }, 4000);
    }
  });
}

function activateSubscriptionInState(state, payload){
  const userId = sanitizeUserId(payload?.user_id);
  if(!userId){
    return { ok:false, error:"user_id_required", subscription:buildFreeSubscription("", "invalid") };
  }

  ensureSubscriptionExpirySweep(state);
  const nowIso = new Date().toISOString();
  const plan = normalizePlanCode(payload?.plan);
  const existing = normalizeSubscriptionRecord(userId, state.subscriptions.users[userId] || {});
  const paymentId = String(payload?.payment_id || payload?.razorpay_payment_id || "").trim();
  const orderId = String(payload?.order_id || payload?.razorpay_order_id || "").trim();

  if(paymentId && state.subscriptions.payments[paymentId]){
    const known = normalizeSubscriptionRecord(userId, state.subscriptions.users[userId] || existing);
    return { ok:true, idempotent:true, subscription:known };
  }

  let next = buildFreeSubscription(userId, payload?.source || "activation");
  if(plan !== "free"){
    next = normalizeSubscriptionRecord(userId, {
      ...existing,
      user_id: userId,
      plan,
      status: "active",
      started_at: nowIso,
      expires_at: getSubscriptionExpiryIso(plan, nowIso),
      source: String(payload?.source || "payment").trim().slice(0, 40) || "payment",
      amount_paise: Math.max(0, Math.round(safeNumber(payload?.amount_paise || payload?.payment_amount_paise))),
      currency: sanitizeCurrencyCode(payload?.currency || payload?.payment_currency) || "INR",
      payment_id: paymentId,
      order_id: orderId,
      metadata: payload?.metadata && typeof payload.metadata === "object"
        ? payload.metadata
        : {}
    });
  }else{
    next.source = String(payload?.source || "downgrade").trim().slice(0, 40) || "downgrade";
  }

  state.subscriptions.users[userId] = next;
  if(paymentId){
    state.subscriptions.payments[paymentId] = {
      user_id: userId,
      plan: next.plan,
      order_id: orderId,
      activated_at: nowIso
    };
  }
  pushBounded(state.subscriptions.transitions, {
    type: "activation",
    user_id: userId,
    previous_plan: existing.plan || "free",
    next_plan: next.plan,
    payment_id: paymentId,
    order_id: orderId,
    at: nowIso
  }, 4000);

  if(next.plan === "4000" && next.status === "active"){
    ensureActiveSeatState(state, userId);
  }
  if(next.plan !== "4000"){
    const seat = state.premium.seats[userId];
    if(seat && Array.isArray(seat.members)){
      seat.members = [userId];
      seat.updated_at = nowIso;
    }
  }

  return { ok:true, idempotent:false, subscription:next };
}

async function notifyAutomationAdmin(subject, payload){
  const nowIso = new Date().toISOString();
  const safeSubject = String(subject || "system_event").trim().slice(0, 120) || "system_event";
  const details = payload && typeof payload === "object" ? payload : {};
  const lines = [
    `Event: ${safeSubject}`,
    `Timestamp: ${nowIso}`,
    "",
    JSON.stringify(details, null, 2)
  ];
  if(ADMIN_DM_WEBHOOK_URL){
    try{
      await fetch(ADMIN_DM_WEBHOOK_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          event: "system_automation",
          subject: safeSubject,
          timestamp: nowIso,
          payload: details
        })
      });
      return { ok:true, channel:"webhook" };
    }catch(_){ }
  }
  if(RESEND_API_KEY){
    try{
      await fetch("https://api.resend.com/emails", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${RESEND_API_KEY}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          from: RESEND_FROM_EMAIL,
          to: [ADMIN_AUTOMATION_EMAIL],
          subject: `NOVAGAPP Automation: ${safeSubject}`,
          text: lines.join("\n")
        })
      });
      return { ok:true, channel:"email" };
    }catch(_){ }
  }
  return { ok:false, channel:"none" };
}

function sanitizeTrustStep(step){
  const normalized = String(step || "").trim().toLowerCase().replace(/[^a-z0-9_ -]/g, "").replace(/\s+/g, "_");
  if(DEFAULT_TRUST_FLOW.includes(normalized)){
    return normalized;
  }
  return "discovery";
}

function sanitizeUserSearchQuery(text){
  return String(text || "")
    .trim()
    .replace(/\s+/g, " ")
    .replace(/[^\w@._ -]/g, "")
    .slice(0, 120);
}

function buildUserSearchTokens(text){
  return Array.from(new Set(tokenizeIdentityText(sanitizeUserSearchQuery(text))))
    .slice(0, 8);
}

function scoreUserSearchMatch(user, tokens){
  const hay = [
    String(user?.display_name || "").toLowerCase(),
    String(user?.full_name || "").toLowerCase(),
    String(user?.username || "").toLowerCase(),
    String(user?.email_local || "").toLowerCase(),
    String(user?.email || "").toLowerCase(),
    Array.isArray(user?.search_tokens) ? user.search_tokens.join(" ").toLowerCase() : ""
  ].join(" ");
  if(!tokens.length){
    return 1;
  }
  let score = 0;
  tokens.forEach((token, idx) => {
    if(!token) return;
    if(hay === token){
      score += 300 - idx;
      return;
    }
    if(hay.startsWith(token)){
      score += 200 - idx;
      return;
    }
    if(hay.includes(token)){
      score += 120 - idx;
      return;
    }
    const compactToken = token.replace(/\s+/g, "");
    if(compactToken && hay.includes(compactToken)){
      score += 80 - idx;
    }
  });
  return score;
}

function parseBooleanFlag(value){
  const raw = String(value === undefined || value === null ? "" : value).trim().toLowerCase();
  if(!raw) return false;
  return raw === "true" || raw === "1" || raw === "yes" || raw === "y" || raw === "on";
}

function normalizeVerificationStatus(value){
  const raw = String(value || "").trim().toLowerCase();
  if(
    raw === "requirements_not_met" ||
    raw === "pending_admin" ||
    raw === "approved" ||
    raw === "rejected" ||
    raw === "not_requested"
  ){
    return raw;
  }
  return "not_requested";
}

function normalizeVerificationRecord(userIdInput, raw){
  const userId = sanitizeUserId(userIdInput || raw?.user_id);
  const followersCount = Math.max(0, Math.floor(safeNumber(raw?.followers_count)));
  const kycCompleted = parseBooleanFlag(raw?.kyc_completed);
  const approvedByAdmin = parseBooleanFlag(raw?.approved_by_admin);
  const eligible = followersCount >= VERIFIED_MIN_FOLLOWERS && kycCompleted;
  let status = normalizeVerificationStatus(raw?.status);
  if(status === "not_requested" && raw?.requested_at){
    status = "requirements_not_met";
  }
  if(status === "approved" && !eligible){
    status = "requirements_not_met";
  }
  const verified = approvedByAdmin && eligible && status === "approved";
  const baseTrust = Math.max(0, Math.floor(safeNumber(raw?.trust_score)));
  const trustScore = verified
    ? Math.max(baseTrust, 70 + VERIFIED_TRUST_BONUS)
    : Math.max(baseTrust, eligible ? 65 : 0);
  const searchBoost = verified ? VERIFIED_SEARCH_BOOST : 0;
  return {
    user_id: userId,
    request_id: sanitizeToken(raw?.request_id, 80),
    followers_count: followersCount,
    kyc_completed: kycCompleted,
    kyc_reference: String(raw?.kyc_reference || "").trim().slice(0, 120),
    status,
    approved_by_admin: approvedByAdmin,
    verified,
    trust_score: trustScore,
    search_boost: searchBoost,
    requested_at: raw?.requested_at ? String(raw.requested_at) : "",
    approved_at: raw?.approved_at ? String(raw.approved_at) : "",
    rejected_at: raw?.rejected_at ? String(raw.rejected_at) : "",
    admin_id: sanitizeUserId(raw?.admin_id || ""),
    admin_note: String(raw?.admin_note || "").trim().slice(0, 300),
    updated_at: String(raw?.updated_at || new Date().toISOString())
  };
}

function getVerificationForUser(state, userIdInput){
  const userId = sanitizeUserId(userIdInput);
  if(!userId){
    return normalizeVerificationRecord("", {});
  }
  const raw = state?.verification?.by_user?.[userId] || {};
  const normalized = normalizeVerificationRecord(userId, raw);
  if(state?.verification?.by_user){
    state.verification.by_user[userId] = normalized;
  }
  return normalized;
}

function applyVerificationToIdentity(state, userIdInput){
  const userId = sanitizeUserId(userIdInput);
  if(!userId || !state?.identities) return null;
  const verification = getVerificationForUser(state, userId);
  const identity = state.identities[userId] || {};
  identity.trust_score = verification.trust_score;
  identity.verified = verification.verified;
  identity.verification_status = verification.status;
  identity.search_boost = verification.search_boost;
  identity.updated_at = new Date().toISOString();
  state.identities[userId] = identity;
  return verification;
}

function normalizeVerificationDecision(value){
  const raw = String(value || "").trim().toLowerCase();
  if(raw === "approve" || raw === "approved" || raw === "accept"){
    return "approve";
  }
  if(raw === "reject" || raw === "rejected" || raw === "deny" || raw === "decline"){
    return "reject";
  }
  return "";
}

function isAdminAutomationRequest(req){
  if(!ADMIN_AUTOMATION_TOKEN){
    return true;
  }
  const incoming = String(
    req.headers["x-admin-token"] ||
    req.headers["x-automation-token"] ||
    req.body?.admin_token ||
    ""
  ).trim();
  return incoming && incoming === ADMIN_AUTOMATION_TOKEN;
}

async function fetchFollowersCountForUser(userIdInput){
  const userId = sanitizeUserId(userIdInput);
  const base = getSocialSupabaseBase();
  const key = getSocialSupabaseReadKey();
  if(!userId || !base || !key){
    return 0;
  }
  const endpoint = new URL("/rest/v1/follows", base + "/");
  endpoint.searchParams.set("select", "follower_id");
  endpoint.searchParams.set("following_id", `eq.${userId}`);
  const response = await fetch(endpoint.toString(), {
    method: "GET",
    headers: {
      apikey: key,
      Authorization: `Bearer ${key}`,
      Prefer: "count=exact",
      Range: "0-0"
    }
  });
  if(!response.ok){
    return 0;
  }
  const contentRange = String(response.headers.get("content-range") || "");
  const match = contentRange.match(/\/(\d+)$/);
  if(match && match[1]){
    return Math.max(0, Math.floor(Number(match[1]) || 0));
  }
  const rows = await response.json().catch(() => []);
  return Array.isArray(rows) ? rows.length : 0;
}

async function upsertIdentityToUsersTable(identity){
  const base = getSocialSupabaseBase();
  const key = getSocialSupabaseServiceRoleKey();
  if(!base || !key || !identity?.user_id){
    return null;
  }

  const endpoint = new URL("/rest/v1/users", base + "/");
  endpoint.searchParams.set("on_conflict", "user_id");

  let row = {
    user_id: identity.user_id,
    username: identity.username,
    full_name: identity.display_name,
    display_name: identity.display_name,
    email: identity.email || null,
    email_local: identity.email_local || null,
    search_tokens: identity.search_tokens || [],
    photo: identity.photo || null
  };
  const required = new Set(["user_id"]);

  for(let attempt = 0; attempt < 10; attempt += 1){
    const response = await fetch(endpoint.toString(), {
      method: "POST",
      headers: {
        apikey: key,
        Authorization: `Bearer ${key}`,
        "Content-Type": "application/json",
        Prefer: "resolution=merge-duplicates,return=representation"
      },
      body: JSON.stringify([row])
    });
    if(response.ok){
      const data = await response.json().catch(() => []);
      if(Array.isArray(data) && data[0]){
        return data[0];
      }
      return row;
    }
    const body = await response.text().catch(() => "");
    const missing = detectMissingColumnName(body);
    if(missing && Object.prototype.hasOwnProperty.call(row, missing) && !required.has(missing)){
      delete row[missing];
      continue;
    }
    throw new Error(`users_identity_upsert_failed_${response.status}:${body.slice(0, 240)}`);
  }
  return row;
}

function sanitizeAuthEmailInput(value){
  return String(value || "").trim().toLowerCase().slice(0, 180);
}

function isLikelyValidEmailAddress(value){
  const email = String(value || "").trim().toLowerCase();
  if(!email) return false;
  return /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/i.test(email);
}

function detectEmailAlreadyExistsResponse(rawBody, parsedBody){
  const text = `${rawBody || ""} ${parsedBody?.msg || ""} ${parsedBody?.message || ""} ${parsedBody?.error || ""} ${parsedBody?.error_description || ""}`
    .toLowerCase();
  return (
    text.includes("already registered") ||
    text.includes("already exists") ||
    text.includes("duplicate key") ||
    text.includes("email exists") ||
    text.includes("email already")
  );
}

async function createSupabaseConfirmedAuthUser({ email, password, displayName }){
  const base = getSocialSupabaseBase();
  const serviceKey = getSocialSupabaseServiceRoleKey();
  if(!base || !serviceKey){
    throw new Error("supabase_service_unconfigured");
  }

  const safeEmail = sanitizeAuthEmailInput(email);
  const safePassword = String(password || "");
  const safeName = sanitizeDisplayName(displayName) || titleCaseWords(
    parseEmailLocalToWords(String((safeEmail.split("@")[0] || "")).trim()) ||
    String((safeEmail.split("@")[0] || "")).trim() ||
    "Member"
  );

  const endpoint = new URL("/auth/v1/admin/users", base + "/");
  const response = await fetch(endpoint.toString(), {
    method: "POST",
    headers: {
      apikey: serviceKey,
      Authorization: `Bearer ${serviceKey}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      email: safeEmail,
      password: safePassword,
      email_confirm: true,
      user_metadata: {
        full_name: safeName,
        name: safeName
      }
    })
  });

  const raw = await response.text().catch(() => "");
  let parsed = null;
  try{
    parsed = raw ? JSON.parse(raw) : {};
  }catch(_){
    parsed = {};
  }

  if(!response.ok){
    const err = new Error(`supabase_admin_signup_failed_${response.status}:${String(raw || "").slice(0, 240)}`);
    err.status = response.status;
    err.code = detectEmailAlreadyExistsResponse(raw, parsed) ? "email_exists" : "";
    throw err;
  }

  const userId = sanitizeUserId(parsed?.id || parsed?.user?.id);
  if(!userId){
    throw new Error("supabase_admin_signup_missing_user_id");
  }
  return {
    user_id: userId,
    email: safeEmail,
    display_name: safeName
  };
}

async function fetchUsersForDiscovery(tokens, limit){
  const base = getSocialSupabaseBase();
  const key = getSocialSupabaseReadKey();
  if(!base || !key){
    return [];
  }
  const max = Math.max(1, Math.min(500, Math.floor(Number(limit) || 100)));
  const selectVariants = [
    {
      select: "user_id,username,full_name,display_name,email,email_local,photo,search_tokens,created_at",
      searchCols: ["username", "full_name", "display_name", "email", "email_local"]
    },
    {
      select: "user_id,username,full_name,email,photo,created_at",
      searchCols: ["username", "full_name", "email"]
    },
    {
      select: "user_id,username,full_name,email,photo",
      searchCols: ["username", "full_name", "email"]
    },
    {
      select: "user_id,username,full_name,photo,created_at",
      searchCols: ["username", "full_name"]
    },
    {
      select: "user_id,username,full_name,photo",
      searchCols: ["username", "full_name"]
    }
  ];

  for(const variant of selectVariants){
    const endpoint = new URL("/rest/v1/users", base + "/");
    endpoint.searchParams.set("select", variant.select);
    endpoint.searchParams.set("limit", String(max));
    endpoint.searchParams.set("order", "created_at.desc");

    if(Array.isArray(tokens) && tokens.length){
      const checks = [];
      tokens.slice(0, 4).forEach(token => {
        const clean = String(token || "").replace(/[%*,()]/g, "").trim();
        if(!clean) return;
        (variant.searchCols || []).forEach(col => {
          checks.push(`${col}.ilike.*${clean}*`);
        });
      });
      if(checks.length){
        endpoint.searchParams.set("or", checks.join(","));
      }
    }

    const response = await fetch(endpoint.toString(), {
      method: "GET",
      headers: {
        apikey: key,
        Authorization: `Bearer ${key}`
      }
    });

    if(!response.ok){
      const body = await response.text().catch(() => "");
      const missing = detectMissingColumnName(body);
      if(missing){
        continue;
      }
      return [];
    }

    const rows = await response.json().catch(() => []);
    return Array.isArray(rows) ? rows : [];
  }
  return [];
}

async function fetchUsersByIds(userIdsInput){
  const base = getSocialSupabaseBase();
  const key = getSocialSupabaseReadKey();
  if(!base || !key){
    return [];
  }
  const userIds = Array.from(new Set(
    (Array.isArray(userIdsInput) ? userIdsInput : [])
      .map(sanitizeUserId)
      .filter(Boolean)
  )).slice(0, 200);
  if(!userIds.length){
    return [];
  }

  const selectVariants = [
    "user_id,username,full_name,display_name,email,email_local,photo",
    "user_id,username,full_name,email,photo",
    "user_id,username,full_name,display_name,photo",
    "user_id,username,full_name,photo"
  ];
  const inFilter = `in.(${userIds.map(id => `"${String(id).replace(/"/g, "")}"`).join(",")})`;

  for(const selectFields of selectVariants){
    const endpoint = new URL("/rest/v1/users", base + "/");
    endpoint.searchParams.set("select", selectFields);
    endpoint.searchParams.set("user_id", inFilter);
    endpoint.searchParams.set("limit", String(userIds.length));

    const response = await fetch(endpoint.toString(), {
      method: "GET",
      headers: {
        apikey: key,
        Authorization: `Bearer ${key}`
      }
    });

    if(!response.ok){
      const body = await response.text().catch(() => "");
      const missing = detectMissingColumnName(body);
      if(missing){
        continue;
      }
      return [];
    }

    const rows = await response.json().catch(() => []);
    return Array.isArray(rows) ? rows : [];
  }
  return [];
}

/* ======================
   LIVE FX (BEST-EFFORT)
====================== */
const FX_CURRENCIES = [
  "USD","INR","EUR","GBP","JPY","CNY","AUD","CAD","CHF","RUB","BRL","ZAR",
  "KRW","THB","UAH","VND","NGN","CRC","SGD","HKD","SAR","AED","NOK","SEK"
];
const FX_CACHE = { ts:0, base:"USD", rates:null, source:"" };

async function fetchGoogleRate(base, quote){
  const url = `https://www.google.com/finance/quote/${base}-${quote}`;
  const res = await fetch(url, {
    headers:{
      "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    }
  });
  if(!res.ok) throw new Error("google_fetch_failed");
  const html = await res.text();
  const m1 = html.match(/data-last-price=\"([0-9.,]+)\"/);
  const m2 = html.match(/class=\"YMlKec fxKbKc\">([0-9.,]+)</);
  const raw = (m1 && m1[1]) || (m2 && m2[1]) || "";
  const num = Number(String(raw).replace(/,/g,""));
  if(!num || !isFinite(num)) throw new Error("google_parse_failed");
  return num;
}

async function fetchGoogleRates(base){
  const targets = FX_CURRENCIES.filter(c=>c !== base);
  const tasks = targets.map(cur => fetchGoogleRate(base, cur));
  const results = await Promise.allSettled(tasks);
  const rates = { [base]:1 };
  results.forEach((r, idx)=>{
    if(r.status === "fulfilled") rates[targets[idx]] = r.value;
  });
  if(Object.keys(rates).length < 2) throw new Error("google_no_rates");
  return rates;
}

async function fetchPublicRates(base){
  const urls = [
    `https://open.er-api.com/v6/latest/${base}`,
    `https://api.exchangerate.host/latest?base=${base}`,
    `https://api.frankfurter.app/latest?from=${base}`
  ];
  for(const url of urls){
    try{
      const r = await fetch(url);
      if(!r.ok) continue;
      const data = await r.json();
      const rates = data?.rates || data?.conversion_rates || null;
      if(rates) return rates;
    }catch(_){ }
  }
  return null;
}

app.get("/fx/latest", async (req,res)=>{
  const base = String(req.query.base || "USD").toUpperCase();
  const now = Date.now();
  if(FX_CACHE.rates && FX_CACHE.base === base && (now - FX_CACHE.ts) < 1000 * 60 * 60){
    return res.json({ base, rates: FX_CACHE.rates, source: FX_CACHE.source || "cache" });
  }

  try{
    const rates = await fetchGoogleRates(base);
    FX_CACHE.ts = now;
    FX_CACHE.base = base;
    FX_CACHE.rates = rates;
    FX_CACHE.source = "google_finance";
    return res.json({ base, rates, source:"google_finance" });
  }catch(_){
    const rates = await fetchPublicRates(base);
    if(!rates) return res.status(500).json({ error:"fx_failed" });
    FX_CACHE.ts = now;
    FX_CACHE.base = base;
    FX_CACHE.rates = rates;
    FX_CACHE.source = "public";
    return res.json({ base, rates, source:"public" });
  }
});

/* ======================
   CONTEST + RAZORPAY
====================== */
const CONTEST_DOWNLOAD_TARGET = 5000000;
const CONTEST_DATA_DIR = path.join(UPLOADS_DIR, "contest-data");
const CONTEST_STATE_PATH = path.join(CONTEST_DATA_DIR, "state.json");
const CONTEST_RAZORPAY_KEY_ID = String(
  process.env.RAZORPAY_KEY_ID ||
  process.env.API_KEY ||
  process.env.RAZORPAY_API_KEY ||
  ""
).trim();
const CONTEST_RAZORPAY_KEY_SECRET = String(
  process.env.RAZORPAY_KEY_SECRET ||
  process.env.SECRATE_KEY ||
  process.env.SECRET_KEY ||
  process.env.RAZORPAY_SECRET_KEY ||
  ""
).trim();
const CONTEST_REQUIRE_LIVE_RAZORPAY = String(
  process.env.CONTEST_REQUIRE_LIVE_RAZORPAY || ""
).trim().toLowerCase();
const CONTEST_LIVE_KEY_REQUIRED = CONTEST_REQUIRE_LIVE_RAZORPAY
  ? !["0", "false", "no", "off"].includes(CONTEST_REQUIRE_LIVE_RAZORPAY)
  : false;
const CONTEST_SUPABASE_URL = String(
  process.env.CONTEST_SUPABASE_URL || process.env.SUPABASE_URL || ""
).trim().replace(/\/+$/g, "");
const CONTEST_SUPABASE_SERVICE_ROLE_KEY = String(
  process.env.CONTEST_SUPABASE_SERVICE_ROLE_KEY ||
  process.env.SUPABASE_SERVICE_ROLE_KEY ||
  process.env.CONTEST_SUPABASE_KEY ||
  process.env.SUPABASE_KEY ||
  ""
).trim();
const CONTEST_ACCOUNT_COUNT_CACHE_TTL_MS = 60 * 1000;
const contestAccountCountCache = {
  total: 0,
  ts: 0,
  pending: null
};

const CONTEST_PACKS = [
  { id:"vote_1", votes:1, usd:5, label:"1 Vote = $5" },
  { id:"vote_7", votes:7, usd:25, label:"7 Votes = $25" },
  { id:"vote_20", votes:20, usd:50, label:"20 Votes = $50" }
];

const CONTESTS = [
  {
    id: "contest_1",
    rank: 1,
    title: "Cristiano Ronaldo vs Lionel Messi",
    subtitle: "Football - Biggest Rivalry Ever",
    winners: 100,
    sides: [
      { id:"ronaldo", label:"Cristiano Ronaldo" },
      { id:"messi", label:"Lionel Messi" }
    ],
    prizes: [
      "Original winner-side jersey",
      "Next live football match trip",
      "Round-trip flight + 3 days stay + match ticket",
      "Only for user (family not included)"
    ]
  },
  {
    id: "contest_2",
    rank: 2,
    title: "Virat Kohli vs Babar Azam",
    subtitle: "Cricket - India vs Pakistan Era Rivalry",
    winners: 100,
    sides: [
      { id:"kohli", label:"Virat Kohli" },
      { id:"babar", label:"Babar Azam" }
    ],
    prizes: [
      "Original cricket jersey",
      "India vs Pakistan match ticket",
      "Stadium ticket + travel + 3 days stay",
      "Only for user"
    ]
  },
  {
    id: "contest_3",
    rank: 3,
    title: "Taylor Swift vs BTS",
    subtitle: "Music - Biggest Fanbase War",
    winners: 50,
    sides: [
      { id:"swift", label:"Taylor Swift" },
      { id:"bts", label:"BTS" }
    ],
    prizes: [
      "Live concert ticket",
      "International venue possible",
      "Seat category decided by organiser"
    ]
  },
  {
    id: "contest_4",
    rank: 4,
    title: "Apple vs Samsung",
    subtitle: "Technology - Smartphone Empire War",
    winners: 100,
    sides: [
      { id:"apple", label:"Apple" },
      { id:"samsung", label:"Samsung" }
    ],
    prizes: [
      "Apple / Samsung accessories",
      "AirPods, Buds, AirTag, Watch accessories"
    ]
  },
  {
    id: "contest_5",
    rank: 5,
    title: "Marvel Studios vs DC Studios",
    subtitle: "Entertainment - Superhero Universe War",
    winners: 50,
    sides: [
      { id:"marvel", label:"Marvel Studios" },
      { id:"dc", label:"DC Studios" }
    ],
    prizes: [
      "Movie premiere / fan experience",
      "Merchandise + event access"
    ]
  },
  {
    id: "contest_6",
    rank: 6,
    title: "Real Madrid vs Barcelona (El Clasico)",
    subtitle: "Football - El Clasico Rivalry",
    winners: 100,
    sides: [
      { id:"madrid", label:"Real Madrid" },
      { id:"barca", label:"Barcelona" }
    ],
    prizes: [
      "Official club jersey (winner side)",
      "El Clasico ticket",
      "Round-trip flight + 3 days stay",
      "Only for user"
    ]
  }
];

const CONTEST_MEGA_PRIZES = [
  "Rank 1: Rolls-Royce Cullinan - free",
  "Rank 2 to Rank 10 (9 winners): Disneyland trip for 7 family members",
  "Duration 10 days with flights, hotel and park tickets included"
];

let contestStateQueue = Promise.resolve();

function safeNumber(value){
  const n = Number(value || 0);
  if(!Number.isFinite(n)) return 0;
  return n;
}

function sanitizeUserId(value){
  return String(value || "").trim().slice(0, 80);
}

function sanitizeDisplayName(value){
  return String(value || "")
    .trim()
    .replace(/\s+/g, " ")
    .replace(/[^a-zA-Z0-9 ._'-]/g, "")
    .slice(0, 48);
}

function sanitizeReferralCode(value){
  return String(value || "")
    .trim()
    .replace(/\s+/g, "")
    .replace(/[^a-zA-Z0-9@_-]/g, "")
    .slice(0, 36);
}

function referralCodeKey(value){
  return sanitizeReferralCode(value).toUpperCase();
}

function sanitizeToken(value, maxLen){
  return String(value || "")
    .trim()
    .replace(/[^a-zA-Z0-9_-]/g, "")
    .slice(0, maxLen || 64);
}

function sanitizeAction(value){
  const next = String(value || "share")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]/g, "")
    .slice(0, 30);
  return next || "share";
}

function isLikelySupabaseUserId(value){
  const raw = String(value || "").trim();
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(raw);
}

function isContestPlaceholderUserId(value){
  const raw = String(value || "").trim().toLowerCase();
  if(!raw) return false;
  if(raw === "healthcheck" || raw === "checkuser" || raw === "supa_check") return true;
  return raw.startsWith("guest_") || raw.startsWith("test_");
}

function isContestPlaceholderUser(userId, rawUser){
  if(isContestPlaceholderUserId(userId)) return true;
  const name = String(rawUser?.display_name || "").trim().toLowerCase();
  if(!name) return false;
  if(name === "guest" || name === "guest user" || name === "test") return true;
  return false;
}

function canContestUserPay(userId){
  const safeUserId = sanitizeUserId(userId);
  if(!safeUserId) return false;
  if(isContestPlaceholderUserId(safeUserId)) return false;
  return isLikelySupabaseUserId(safeUserId);
}

function getContestById(contestId){
  return CONTESTS.find(c => c.id === contestId) || null;
}

function getContestPackById(packId){
  return CONTEST_PACKS.find(p => p.id === packId) || null;
}

function createDefaultContestState(){
  const now = new Date().toISOString();
  const votes = {};
  CONTESTS.forEach(contest => {
    votes[contest.id] = {};
    contest.sides.forEach(side => {
      votes[contest.id][side.id] = 0;
    });
  });
  return {
    version: 1,
    downloads: 0,
    registered_accounts: {},
    users: {},
    orders: {},
    votes,
    installs: {},
    share_visit_log: {},
    created_at: now,
    updated_at: now
  };
}

function ensureContestVoteBuckets(state){
  if(!state.votes || typeof state.votes !== "object"){
    state.votes = {};
  }
  CONTESTS.forEach(contest => {
    if(!state.votes[contest.id] || typeof state.votes[contest.id] !== "object"){
      state.votes[contest.id] = {};
    }
    contest.sides.forEach(side => {
      state.votes[contest.id][side.id] = safeNumber(state.votes[contest.id][side.id]);
    });
  });
}

function normalizeContestState(state){
  const next = state && typeof state === "object"
    ? state
    : createDefaultContestState();

  next.version = safeNumber(next.version) || 1;
  next.downloads = safeNumber(next.downloads);
  if(!next.registered_accounts || typeof next.registered_accounts !== "object"){
    next.registered_accounts = {};
  }
  if(!next.users || typeof next.users !== "object") next.users = {};
  if(!next.orders || typeof next.orders !== "object") next.orders = {};
  if(!next.installs || typeof next.installs !== "object") next.installs = {};
  if(!next.share_visit_log || typeof next.share_visit_log !== "object") next.share_visit_log = {};
  ensureContestVoteBuckets(next);
  if(!next.created_at) next.created_at = new Date().toISOString();
  if(!next.updated_at) next.updated_at = new Date().toISOString();
  return next;
}

async function readContestState(){
  try{
    const raw = await fs.readFile(CONTEST_STATE_PATH, "utf8");
    const parsed = raw ? JSON.parse(raw) : null;
    return normalizeContestState(parsed);
  }catch(_){
    return createDefaultContestState();
  }
}

async function writeContestState(state){
  await ensureDir(CONTEST_DATA_DIR);
  await fs.writeFile(CONTEST_STATE_PATH, JSON.stringify(state, null, 2), "utf8");
}

async function mutateContestState(mutator){
  const run = contestStateQueue.then(async () => {
    const state = await readContestState();
    const result = await mutator(state);
    state.updated_at = new Date().toISOString();
    await writeContestState(state);
    return result;
  });
  contestStateQueue = run.catch(() => {});
  return run;
}

function normalizeReferralHandle(userName, userId){
  const cleanName = String(userName || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "")
    .slice(0, 14);
  if(cleanName) return cleanName;

  const cleanUserId = String(userId || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "")
    .slice(0, 14);
  return cleanUserId || "member";
}

function referralCodeForUser(userId, userName, attempt){
  const base = normalizeReferralHandle(userName, userId);
  const safeAttempt = Math.max(0, safeNumber(attempt));
  const suffix = safeAttempt > 0 ? String(safeAttempt + 1) : "";
  const room = Math.max(3, 14 - suffix.length);
  const handle = `${base.slice(0, room)}${suffix}` || "user";
  return `NVG-${handle}@2026`;
}

function isModernReferralCode(value){
  return /^NVG-[A-Z0-9]+@2026$/i.test(String(value || ""));
}

function getUserIdByReferralCode(state, referralCode){
  const code = referralCodeKey(referralCode);
  if(!code) return "";
  const entries = Object.entries(state.users || {});
  for(const [userId, rawUser] of entries){
    const userCode = referralCodeKey(rawUser?.referral_code);
    if(userCode && userCode === code){
      return userId;
    }
  }
  return "";
}

function ensureContestUser(state, userIdInput, userNameInput){
  const userId = sanitizeUserId(userIdInput);
  const incomingName = sanitizeDisplayName(userNameInput);
  if(!userId) return null;
  if(!state.users[userId] || typeof state.users[userId] !== "object"){
    state.users[userId] = {
      user_id: userId,
      display_name: incomingName || "",
      referral_code: "",
      share_actions: 0,
      share_actions_by_type: {},
      unique_share_visits: 0,
      verified_installs: 0,
      paid_votes: 0,
      contest_votes: {},
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    };
  }
  const user = state.users[userId];
  user.user_id = userId;
  user.display_name = sanitizeDisplayName(user.display_name || "");
  if(incomingName){
    user.display_name = incomingName;
  }
  user.share_actions = safeNumber(user.share_actions);
  user.unique_share_visits = safeNumber(user.unique_share_visits);
  user.verified_installs = safeNumber(user.verified_installs);
  user.paid_votes = safeNumber(user.paid_votes);
  if(!user.share_actions_by_type || typeof user.share_actions_by_type !== "object"){
    user.share_actions_by_type = {};
  }
  if(!user.contest_votes || typeof user.contest_votes !== "object"){
    user.contest_votes = {};
  }
  CONTESTS.forEach(contest => {
    user.contest_votes[contest.id] = safeNumber(user.contest_votes[contest.id]);
  });

  const currentCode = sanitizeReferralCode(user.referral_code);
  if(currentCode && isModernReferralCode(currentCode)){
    user.referral_code = currentCode;
  }else{
    const referralName = user.display_name || incomingName || "";
    let chosen = "";
    for(let idx = 0; idx < 500; idx += 1){
      const code = referralCodeForUser(userId, referralName, idx);
      const owner = getUserIdByReferralCode(state, code);
      if(!owner || owner === userId){
        chosen = code;
        break;
      }
    }
    user.referral_code = chosen || referralCodeForUser(userId, referralName, Date.now() % 1000);
  }
  user.updated_at = new Date().toISOString();
  return user;
}

function contestVisitorFingerprint(req){
  const forwarded = String(req.headers["x-forwarded-for"] || "").split(",")[0].trim();
  const ip = forwarded || String(req.ip || "").trim();
  const userAgent = String(req.headers["user-agent"] || "").trim();
  const lang = String(req.headers["accept-language"] || "").trim();
  const raw = `${ip}|${userAgent}|${lang}`;
  return crypto.createHash("sha1").update(raw).digest("hex").slice(0, 24);
}

function computeShareWeight(user){
  const actions = safeNumber(user?.share_actions);
  const visits = safeNumber(user?.unique_share_visits);
  const installs = safeNumber(user?.verified_installs);
  const weighted = (actions * 0.5) + (visits * 2) + (installs * 5);
  return Math.round(weighted * 100) / 100;
}

function computeEntryWeight(user){
  return safeNumber(user?.paid_votes) + computeShareWeight(user);
}

function getContestRegisteredAccountCount(state){
  const bucket = state?.registered_accounts && typeof state.registered_accounts === "object"
    ? state.registered_accounts
    : {};
  return Object.keys(bucket).length;
}

function collectContestKnownAccountIds(state){
  const set = new Set();
  const push = value => {
    const userId = sanitizeUserId(value);
    if(!canContestUserPay(userId)) return;
    set.add(userId);
  };

  Object.keys(state?.registered_accounts || {}).forEach(push);
  Object.keys(state?.users || {}).forEach(push);
  Object.values(state?.orders || {}).forEach(order => push(order?.user_id));
  Object.values(state?.installs || {}).forEach(install => push(install?.user_id));
  return set;
}

function getContestKnownAccountCount(state){
  return collectContestKnownAccountIds(state).size;
}

function registerContestAccount(state, userIdInput, userNameInput, emailInput){
  const userId = sanitizeUserId(userIdInput);
  if(!userId) return null;

  if(!state.registered_accounts || typeof state.registered_accounts !== "object"){
    state.registered_accounts = {};
  }

  const nowIso = new Date().toISOString();
  const safeName = sanitizeDisplayName(userNameInput);
  const safeEmail = String(emailInput || "").trim().toLowerCase().slice(0, 180);
  const existing = state.registered_accounts[userId] && typeof state.registered_accounts[userId] === "object"
    ? state.registered_accounts[userId]
    : null;

  const row = {
    user_id: userId,
    user_name: safeName || String(existing?.user_name || "").trim().slice(0, 48),
    email: safeEmail || String(existing?.email || "").trim().toLowerCase().slice(0, 180),
    registered_at: String(existing?.registered_at || nowIso),
    last_seen_at: nowIso
  };

  state.registered_accounts[userId] = row;
  return row;
}

function buildContestRows(state){
  ensureContestVoteBuckets(state);
  return CONTESTS.map(contest => {
    const bucket = state.votes[contest.id] || {};
    const sides = contest.sides.map(side => ({
      id: side.id,
      label: side.label,
      votes: safeNumber(bucket[side.id])
    }));
    const totalVotes = sides.reduce((sum, side) => sum + safeNumber(side.votes), 0);
    return {
      id: contest.id,
      rank: contest.rank,
      title: contest.title,
      subtitle: contest.subtitle,
      winners: contest.winners,
      prizes: contest.prizes,
      sides,
      total_votes: totalVotes
    };
  });
}

function buildLeaderboard(state, limit){
  const rows = Object.entries(state.users || {})
    .filter(([userId, rawUser]) => {
      if(!canContestUserPay(userId)) return false;
      return !isContestPlaceholderUser(userId, rawUser);
    })
    .map(([userId, rawUser]) => {
      const user = ensureContestUser(state, userId) || rawUser || {};
      const shareWeight = computeShareWeight(user);
      const weightedEntries = computeEntryWeight(user);
      return {
        user_id: userId,
        display_name: sanitizeDisplayName(user.display_name || ""),
        referral_code: user.referral_code || "",
        share_actions: safeNumber(user.share_actions),
        unique_share_visits: safeNumber(user.unique_share_visits),
        verified_installs: safeNumber(user.verified_installs),
        paid_votes: safeNumber(user.paid_votes),
        share_weight: shareWeight,
        weighted_entries: Math.round(weightedEntries * 100) / 100
      };
    });

  rows.sort((a, b) => {
    if(b.weighted_entries !== a.weighted_entries){
      return b.weighted_entries - a.weighted_entries;
    }
    if(b.verified_installs !== a.verified_installs){
      return b.verified_installs - a.verified_installs;
    }
    if(b.unique_share_visits !== a.unique_share_visits){
      return b.unique_share_visits - a.unique_share_visits;
    }
    return b.share_actions - a.share_actions;
  });

  return rows.slice(0, Math.max(1, safeNumber(limit) || 10)).map((row, idx) => ({
    rank: idx + 1,
    ...row
  }));
}

function buildTopSharers(state, limit){
  const rows = Object.entries(state.users || {})
    .filter(([userId, rawUser]) => {
      if(!canContestUserPay(userId)) return false;
      return !isContestPlaceholderUser(userId, rawUser);
    })
    .map(([userId, rawUser]) => {
      const user = ensureContestUser(state, userId) || rawUser || {};
      return {
        user_id: userId,
        display_name: sanitizeDisplayName(user.display_name || ""),
        referral_code: user.referral_code || "",
        share_actions: safeNumber(user.share_actions),
        unique_share_visits: safeNumber(user.unique_share_visits),
        verified_installs: safeNumber(user.verified_installs),
        paid_votes: safeNumber(user.paid_votes),
        weighted_entries: Math.round(computeEntryWeight(user) * 100) / 100
      };
    });

  rows.sort((a, b) => {
    if(b.share_actions !== a.share_actions){
      return b.share_actions - a.share_actions;
    }
    if(b.unique_share_visits !== a.unique_share_visits){
      return b.unique_share_visits - a.unique_share_visits;
    }
    if(b.verified_installs !== a.verified_installs){
      return b.verified_installs - a.verified_installs;
    }
    return b.paid_votes - a.paid_votes;
  });

  return rows.slice(0, Math.max(1, safeNumber(limit) || 10)).map((row, idx) => ({
    rank: idx + 1,
    ...row
  }));
}

function sanitizeContestProfilePhoto(value){
  const raw = String(value || "").trim();
  if(!raw) return "";
  return raw.slice(0, 3000);
}

function collectReferralRankUserIds(state){
  return Object.entries(state.users || {})
    .filter(([userId, rawUser]) => {
      if(!canContestUserPay(userId)) return false;
      return !isContestPlaceholderUser(userId, rawUser);
    })
    .map(([userId]) => sanitizeUserId(userId))
    .filter(Boolean);
}

async function fetchContestUserProfilesByIds(userIds){
  if(!Array.isArray(userIds) || !userIds.length) return {};
  if(!isContestSupabaseConfigured()) return {};

  const ids = [...new Set(userIds.map(sanitizeUserId).filter(Boolean))].slice(0, 500);
  if(!ids.length) return {};

  const quotedIds = ids
    .map(id => `"${String(id).replace(/"/g, "")}"`)
    .join(",");
  if(!quotedIds){
    return {};
  }

  const selectPlans = [
    ["user_id", "username", "full_name", "photo"],
    ["user_id", "username", "full_name", "avatar_url"],
    ["user_id", "username", "photo"],
    ["user_id", "username", "avatar_url"],
    ["user_id", "full_name", "photo"],
    ["user_id", "full_name", "avatar_url"],
    ["user_id", "username", "full_name"],
    ["user_id", "username"],
    ["user_id", "full_name"],
    ["user_id"]
  ];

  for(const fields of selectPlans){
    const endpoint = new URL("/rest/v1/users", CONTEST_SUPABASE_URL + "/");
    endpoint.searchParams.set("select", fields.join(","));
    endpoint.searchParams.set("user_id", `in.(${quotedIds})`);
    endpoint.searchParams.set("limit", String(ids.length));

    const response = await fetch(endpoint.toString(), {
      method: "GET",
      headers: {
        apikey: CONTEST_SUPABASE_SERVICE_ROLE_KEY,
        Authorization: `Bearer ${CONTEST_SUPABASE_SERVICE_ROLE_KEY}`
      }
    });
    if(!response.ok){
      const body = await response.text().catch(() => "");
      const missingColumn = detectMissingColumnName(body);
      if(missingColumn && fields.includes(missingColumn)){
        continue;
      }
      continue;
    }

    const rows = await response.json().catch(() => []);
    if(!Array.isArray(rows)){
      continue;
    }
    const map = {};
    rows.forEach(row => {
      const uid = sanitizeUserId(row?.user_id);
      if(!uid) return;
      map[uid] = {
        user_id: uid,
        username: sanitizeDisplayName(row?.username || ""),
        full_name: sanitizeDisplayName(row?.full_name || ""),
        photo: sanitizeContestProfilePhoto(row?.photo || row?.avatar_url || row?.avatar || "")
      };
    });
    return map;
  }

  return {};
}

function buildReferralRanks(state, profileMap, limit){
  const rows = Object.entries(state.users || {})
    .filter(([userId, rawUser]) => {
      if(!canContestUserPay(userId)) return false;
      return !isContestPlaceholderUser(userId, rawUser);
    })
    .map(([userId, rawUser]) => {
      const user = ensureContestUser(state, userId) || rawUser || {};
      const profile = profileMap?.[userId] || {};
      const fallbackName = sanitizeDisplayName(user.display_name || "") || `User ${String(userId || "").slice(0, 8)}`;
      const displayName =
        sanitizeDisplayName(profile.full_name || "") ||
        sanitizeDisplayName(profile.username || "") ||
        fallbackName;
      const newUsers = safeNumber(user.verified_installs);
      const uniqueVisits = safeNumber(user.unique_share_visits);
      const shareActions = safeNumber(user.share_actions);
      const paidVotes = safeNumber(user.paid_votes);
      const weightedEntries = Math.round(computeEntryWeight(user) * 100) / 100;
      return {
        user_id: userId,
        display_name: displayName || fallbackName,
        username: sanitizeDisplayName(profile.username || ""),
        photo: sanitizeContestProfilePhoto(profile.photo || ""),
        referral_code: user.referral_code || "",
        new_users: newUsers,
        verified_installs: newUsers,
        unique_share_visits: uniqueVisits,
        share_actions: shareActions,
        paid_votes: paidVotes,
        weighted_entries
      };
    });

  rows.sort((a, b) => {
    if(b.new_users !== a.new_users){
      return b.new_users - a.new_users;
    }
    if(b.unique_share_visits !== a.unique_share_visits){
      return b.unique_share_visits - a.unique_share_visits;
    }
    if(b.share_actions !== a.share_actions){
      return b.share_actions - a.share_actions;
    }
    if(b.paid_votes !== a.paid_votes){
      return b.paid_votes - a.paid_votes;
    }
    return b.weighted_entries - a.weighted_entries;
  });

  const cap = Math.max(1, Math.min(5000, Math.floor(Number(limit) || rows.length || 1)));
  return rows.slice(0, cap).map((row, index) => ({
    rank: index + 1,
    ...row
  }));
}

async function buildReferralRanksWithProfiles(state, limit){
  const userIds = collectReferralRankUserIds(state);
  let profiles = {};
  try{
    profiles = await fetchContestUserProfilesByIds(userIds);
  }catch(err){
    console.error("contest_rank_profiles_error:", err?.stack || err);
    profiles = {};
  }
  return buildReferralRanks(state, profiles, limit);
}

function parseContentRangeTotal(raw){
  const text = String(raw || "").trim();
  if(!text) return 0;
  const slash = text.lastIndexOf("/");
  if(slash < 0) return 0;
  const total = Number(text.slice(slash + 1));
  if(!Number.isFinite(total) || total < 0) return 0;
  return Math.floor(total);
}

async function fetchSupabaseUsersTableCount(){
  if(!isContestSupabaseConfigured()) return 0;
  const endpoint = `${CONTEST_SUPABASE_URL}/rest/v1/users?select=user_id&user_id=not.is.null`;
  const commonHeaders = {
    apikey: CONTEST_SUPABASE_SERVICE_ROLE_KEY,
    Authorization: `Bearer ${CONTEST_SUPABASE_SERVICE_ROLE_KEY}`,
    Prefer: "count=exact"
  };

  const headRes = await fetch(endpoint, {
    method: "HEAD",
    headers: commonHeaders
  });
  if(headRes.ok){
    const rangeHeader = String(headRes.headers.get("content-range") || "").trim();
    if(rangeHeader){
      return parseContentRangeTotal(rangeHeader);
    }
  }

  const fallbackRes = await fetch(endpoint, {
    method: "GET",
    headers: {
      ...commonHeaders,
      Range: "0-0"
    }
  });
  if(!fallbackRes.ok){
    const body = await fallbackRes.text().catch(() => "");
    throw new Error(`supabase_users_count_failed_${fallbackRes.status}:${body.slice(0, 160)}`);
  }
  return parseContentRangeTotal(fallbackRes.headers.get("content-range"));
}

async function getContestTotalAccountCount(state){
  const localCount = getContestKnownAccountCount(state);
  if(!isContestSupabaseConfigured()){
    return localCount;
  }

  const now = Date.now();
  if(
    Number.isFinite(contestAccountCountCache.total)
    && contestAccountCountCache.ts
    && (now - contestAccountCountCache.ts) < CONTEST_ACCOUNT_COUNT_CACHE_TTL_MS
  ){
    return Math.max(localCount, Math.floor(contestAccountCountCache.total));
  }

  if(!contestAccountCountCache.pending){
    contestAccountCountCache.pending = (async () => {
      try{
        const supabaseCount = await fetchSupabaseUsersTableCount();
        contestAccountCountCache.total = Math.max(0, Math.floor(supabaseCount));
        contestAccountCountCache.ts = Date.now();
        return contestAccountCountCache.total;
      }catch(err){
        console.warn("contest_account_count_fetch_failed:", err?.message || err);
        contestAccountCountCache.ts = Date.now();
        return Math.max(0, Math.floor(contestAccountCountCache.total || 0));
      }finally{
        contestAccountCountCache.pending = null;
      }
    })();
  }

  const remoteCount = await contestAccountCountCache.pending;
  return Math.max(localCount, Math.floor(remoteCount || 0));
}

function buildContestPayload(state, userIdInput, userNameInput, extra){
  const userId = sanitizeUserId(userIdInput);
  const userName = sanitizeDisplayName(userNameInput);
  const contests = buildContestRows(state);
  const downloads = safeNumber(state.downloads);
  const registeredAccounts = getContestRegisteredAccountCount(state);
  const knownAccounts = getContestKnownAccountCount(state);
  const extraTotalAccounts = safeNumber(extra?.total_accounts);
  const totalAccounts = Math.max(registeredAccounts, knownAccounts, extraTotalAccounts);
  const effectiveDownloads = Math.max(downloads, totalAccounts);
  const target = CONTEST_DOWNLOAD_TARGET;
  const progress = target > 0
    ? Math.min(100, (effectiveDownloads / target) * 100)
    : 0;

  const user = canContestUserPay(userId) ? ensureContestUser(state, userId, userName) : null;
  const userStats = user
    ? {
      user_id: userId,
      display_name: sanitizeDisplayName(user.display_name || ""),
      referral_code: user.referral_code || "",
      share_actions: safeNumber(user.share_actions),
      share_actions_by_type: user.share_actions_by_type || {},
      unique_share_visits: safeNumber(user.unique_share_visits),
      verified_installs: safeNumber(user.verified_installs),
      paid_votes: safeNumber(user.paid_votes),
      contest_votes: user.contest_votes || {},
      share_weight: computeShareWeight(user),
      weighted_entries: Math.round(computeEntryWeight(user) * 100) / 100
    }
    : null;

  return {
    contests,
    packs: CONTEST_PACKS,
    mega_prizes: CONTEST_MEGA_PRIZES,
    payment_gateway: "Razorpay",
    payment_gateway_mode: getContestRazorpayMode(),
    payment_gateway_issue: getContestRazorpayConfigIssue(),
    download_target: target,
    current_downloads: effectiveDownloads,
    total_accounts: Math.max(0, Math.floor(totalAccounts)),
    download_progress: Math.round(progress * 100) / 100,
    results_ready: effectiveDownloads >= target,
    result_rule: "Result tabhi announce hoga jab total users/downloads 5,000,000 complete honge.",
    share_rule: "Chance formula: paid votes + (verified installs x 5) + (unique share visitors x 2) + (share actions x 0.5).",
    top_sharer_rule: "Top sharer ranking: share actions > unique share visitors > verified installs > paid votes.",
    leaderboard: buildLeaderboard(state, 10),
    top_sharers: buildTopSharers(state, 10),
    user_stats: userStats,
    razorpay_ready: isContestRazorpayConfigured(),
    supabase_sync_ready: isContestSupabaseConfigured()
  };
}

function getContestRazorpayMode(){
  const keyId = String(CONTEST_RAZORPAY_KEY_ID || "").trim().toLowerCase();
  if(!keyId) return "unconfigured";
  if(keyId.startsWith("rzp_live_")) return "live";
  if(keyId.startsWith("rzp_test_")) return "test";
  return "unknown";
}

function getContestRazorpayConfigIssue(){
  if(!CONTEST_RAZORPAY_KEY_ID || !CONTEST_RAZORPAY_KEY_SECRET){
    return "missing_keys";
  }
  if(CONTEST_LIVE_KEY_REQUIRED && getContestRazorpayMode() !== "live"){
    return "live_key_required";
  }
  return "";
}

function getContestRazorpaySetupMessage(){
  const issue = getContestRazorpayConfigIssue();
  if(issue === "live_key_required"){
    return "Live Razorpay keys are required for production publish.";
  }
  return "Set RAZORPAY_KEY_ID and RAZORPAY_KEY_SECRET in server environment.";
}

function isContestRazorpayConfigured(){
  return !getContestRazorpayConfigIssue();
}

async function getUsdInrRateSafe(){
  const cachedInr = safeNumber(FX_CACHE?.rates?.INR);
  if(FX_CACHE.base === "USD" && cachedInr > 0){
    return cachedInr;
  }

  const publicRates = await fetchPublicRates("USD");
  const liveInr = safeNumber(publicRates?.INR);
  if(liveInr > 0){
    FX_CACHE.ts = Date.now();
    FX_CACHE.base = "USD";
    FX_CACHE.rates = publicRates;
    FX_CACHE.source = "public";
    return liveInr;
  }

  return 83;
}

function isContestSupabaseConfigured(){
  return Boolean(CONTEST_SUPABASE_URL && CONTEST_SUPABASE_SERVICE_ROLE_KEY);
}

function safeCloneJson(value){
  try{
    return JSON.parse(JSON.stringify(value));
  }catch(_){
    return null;
  }
}

async function contestSupabaseUpsert(tableName, rows, onConflict){
  if(!isContestSupabaseConfigured()) return;
  const list = Array.isArray(rows) ? rows.filter(Boolean) : [];
  if(!list.length) return;

  const table = String(tableName || "").trim();
  const conflict = String(onConflict || "").trim();
  if(!table || !conflict) return;

  const endpoint = `${CONTEST_SUPABASE_URL}/rest/v1/${table}?on_conflict=${encodeURIComponent(conflict)}`;
  const res = await fetch(endpoint, {
    method: "POST",
    headers: {
      apikey: CONTEST_SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${CONTEST_SUPABASE_SERVICE_ROLE_KEY}`,
      "Content-Type": "application/json",
      Prefer: "resolution=merge-duplicates,return=minimal"
    },
    body: JSON.stringify(list)
  });

  if(!res.ok){
    const body = await res.text().catch(() => "");
    throw new Error(`supabase_upsert_failed_${table}_${res.status}:${body.slice(0, 260)}`);
  }
}

function buildContestUserUpsertRow(userLike){
  const userId = sanitizeUserId(userLike?.user_id);
  if(!userId) return null;

  const displayName = sanitizeDisplayName(userLike?.display_name);
  const referralCode = sanitizeReferralCode(userLike?.referral_code);
  const shareActions = safeNumber(userLike?.share_actions);
  const uniqueShareVisits = safeNumber(userLike?.unique_share_visits);
  const verifiedInstalls = safeNumber(userLike?.verified_installs);
  const paidVotes = safeNumber(userLike?.paid_votes);
  const contestVotes = (userLike?.contest_votes && typeof userLike.contest_votes === "object")
    ? userLike.contest_votes
    : {};
  const shareActionsByType = (userLike?.share_actions_by_type && typeof userLike.share_actions_by_type === "object")
    ? userLike.share_actions_by_type
    : {};
  const shareWeight = safeNumber(userLike?.share_weight || 0);
  const weightedEntries = safeNumber(userLike?.weighted_entries || (paidVotes + shareWeight));
  const updatedAt = new Date().toISOString();

  return {
    user_id: userId,
    display_name: displayName,
    referral_code: referralCode,
    share_actions: shareActions,
    share_actions_by_type: shareActionsByType,
    unique_share_visits: uniqueShareVisits,
    verified_installs: verifiedInstalls,
    paid_votes: paidVotes,
    contest_votes: contestVotes,
    share_weight: Math.round(shareWeight * 100) / 100,
    weighted_entries: Math.round(weightedEntries * 100) / 100,
    updated_at: updatedAt
  };
}

function buildContestOrderUpsertRow(orderLike){
  const orderId = String(orderLike?.razorpay_order_id || "").trim();
  if(!orderId) return null;

  return {
    razorpay_order_id: orderId,
    receipt: String(orderLike?.receipt || "").trim(),
    status: String(orderLike?.status || "").trim().toLowerCase() || "created",
    user_id: sanitizeUserId(orderLike?.user_id),
    contest_id: sanitizeToken(orderLike?.contest_id, 40).toLowerCase(),
    side_id: sanitizeToken(orderLike?.side_id, 40).toLowerCase(),
    pack_id: sanitizeToken(orderLike?.pack_id, 40).toLowerCase(),
    votes: safeNumber(orderLike?.votes),
    amount_usd: safeNumber(orderLike?.amount_usd),
    amount_inr_paise: Math.round(safeNumber(orderLike?.amount_inr_paise)),
    usd_inr_rate: safeNumber(orderLike?.usd_inr_rate),
    currency: String(orderLike?.currency || "INR").trim().toUpperCase(),
    payment_id: String(orderLike?.payment_id || "").trim(),
    signature: String(orderLike?.signature || "").trim(),
    created_at: orderLike?.created_at || new Date().toISOString(),
    paid_at: orderLike?.paid_at || null,
    updated_at: new Date().toISOString()
  };
}

function buildContestVoteUpsertRow(voteLike){
  const contestId = sanitizeToken(voteLike?.contest_id, 40).toLowerCase();
  const sideId = sanitizeToken(voteLike?.side_id, 40).toLowerCase();
  if(!contestId || !sideId) return null;
  return {
    contest_id: contestId,
    side_id: sideId,
    votes: safeNumber(voteLike?.votes),
    updated_at: new Date().toISOString()
  };
}

async function syncContestUserSnapshot(userLike){
  const row = buildContestUserUpsertRow(userLike);
  if(!row) return;
  await contestSupabaseUpsert("contest_users", [row], "user_id");
}

async function syncContestOrderSnapshot(orderLike){
  const row = buildContestOrderUpsertRow(orderLike);
  if(!row) return;
  await contestSupabaseUpsert("contest_orders", [row], "razorpay_order_id");
}

async function syncContestVoteSnapshot(voteLike){
  const row = buildContestVoteUpsertRow(voteLike);
  if(!row) return;
  await contestSupabaseUpsert("contest_votes", [row], "contest_id,side_id");
}

async function syncContestSnapshots({ userLike, orderLike, voteLike } = {}){
  if(!isContestSupabaseConfigured()) return;
  const jobs = [];
  if(userLike) jobs.push(syncContestUserSnapshot(userLike));
  if(orderLike) jobs.push(syncContestOrderSnapshot(orderLike));
  if(voteLike) jobs.push(syncContestVoteSnapshot(voteLike));
  if(!jobs.length) return;

  const results = await Promise.allSettled(jobs);
  results.forEach(item => {
    if(item.status === "rejected"){
      console.error("contest_supabase_sync_error:", item.reason?.stack || item.reason);
    }
  });
}

async function createContestRazorpayOrder(payload){
  if(!isContestRazorpayConfigured()){
    throw new Error("Razorpay keys are not configured.");
  }

  const body = new URLSearchParams();
  body.set("amount", String(payload.amount_paise));
  body.set("currency", "INR");
  body.set("receipt", String(payload.receipt || ""));
  body.set("payment_capture", "1");
  Object.entries(payload.notes || {}).forEach(([key, value]) => {
    if(value === undefined || value === null || value === "") return;
    body.append(`notes[${key}]`, String(value));
  });

  const auth = Buffer
    .from(`${CONTEST_RAZORPAY_KEY_ID}:${CONTEST_RAZORPAY_KEY_SECRET}`)
    .toString("base64");

  const res = await fetch("https://api.razorpay.com/v1/orders", {
    method: "POST",
    headers: {
      "Authorization": `Basic ${auth}`,
      "Content-Type": "application/x-www-form-urlencoded"
    },
    body
  });

  const data = await res.json().catch(() => null);
  if(!res.ok){
    const desc =
      data?.error?.description ||
      data?.error?.reason ||
      "Unable to create Razorpay order.";
    throw new Error(desc);
  }
  return data;
}

function verifyContestPaymentSignature(orderId, paymentId, signature){
  if(!isContestRazorpayConfigured()) return false;
  const expectedBuffer = crypto
    .createHmac("sha256", CONTEST_RAZORPAY_KEY_SECRET)
    .update(`${orderId}|${paymentId}`)
    .digest();
  const providedHex = String(signature || "").trim().toLowerCase();
  if(!/^[a-f0-9]{64}$/.test(providedHex)){
    return false;
  }
  const providedBuffer = Buffer.from(providedHex, "hex");
  if(providedBuffer.length !== expectedBuffer.length){
    return false;
  }
  return crypto.timingSafeEqual(expectedBuffer, providedBuffer);
}

function sanitizePaymentNotes(raw){
  const out = {};
  if(!raw || typeof raw !== "object") return out;

  Object.entries(raw).slice(0, 12).forEach(([key, value]) => {
    const cleanKey = sanitizeToken(key, 40).toLowerCase();
    if(!cleanKey) return;
    const cleanValue = String(value === undefined || value === null ? "" : value).trim().slice(0, 180);
    if(!cleanValue) return;
    out[cleanKey] = cleanValue;
  });

  return out;
}

function sanitizeCurrencyCode(value){
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z]/g, "")
    .slice(0, 3);
}

function verifyRazorpayWebhookSignature(rawBody, signature){
  if(!RAZORPAY_WEBHOOK_SECRET) return false;
  const payload = Buffer.isBuffer(rawBody)
    ? rawBody
    : Buffer.from(String(rawBody || ""), "utf8");
  const expected = crypto
    .createHmac("sha256", RAZORPAY_WEBHOOK_SECRET)
    .update(payload)
    .digest();
  const providedHex = String(signature || "").trim().toLowerCase();
  if(!/^[a-f0-9]{64}$/.test(providedHex)){
    return false;
  }
  const provided = Buffer.from(providedHex, "hex");
  if(provided.length !== expected.length){
    return false;
  }
  return crypto.timingSafeEqual(expected, provided);
}

function resolveSubscriptionPlanFromNotes(notes){
  const planFromNotes = normalizePlanCode(notes?.plan || notes?.subscription_plan || notes?.tier || "");
  if(planFromNotes !== "free"){
    return planFromNotes;
  }
  const baseAmountRaw = safeNumber(notes?.base_amount || notes?.amount || 0);
  if(baseAmountRaw >= PLAN_4000_USD){
    return "4000";
  }
  if(baseAmountRaw >= PLAN_40_USD){
    return "40";
  }
  const appAmountPaise = Math.round(safeNumber(notes?.app_amount_paise || notes?.amount_paise || 0));
  if(appAmountPaise >= PLAN_4000_USD * 100){
    return "4000";
  }
  if(appAmountPaise >= PLAN_40_USD * 100){
    return "40";
  }
  return "free";
}

function isSubscriptionPaymentContext(notes){
  const source = String(notes?.source || "").trim().toLowerCase();
  return source === "subscription_page" ||
    source === "m_subscription_page" ||
    source === "subscription" ||
    source === "m-subscription";
}

function parseMonthlyKey(input){
  const raw = String(input || "").trim();
  if(/^\d{4}-\d{2}$/.test(raw)){
    return raw;
  }
  const now = new Date();
  return `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, "0")}`;
}

function toIsoDateKey(input){
  const t = new Date(input || Date.now());
  const y = t.getUTCFullYear();
  const m = String(t.getUTCMonth() + 1).padStart(2, "0");
  const d = String(t.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function buildAssistantReply(mode, payload){
  const objective = String(payload?.objective || "").trim().slice(0, 280) || "increase conversion";
  const leadName = safePublicNameFromIdentity(payload?.lead || {}, payload?.lead?.user_id || "");
  const productName = String(payload?.product_name || "your product").trim().slice(0, 120) || "your product";
  const tone = String(payload?.tone || "friendly").trim().toLowerCase();
  if(mode === "revenue"){
    return {
      opener: `Hi ${leadName}, thanks for checking ${productName}.`,
      angle: `Based on your activity, the best revenue move is a limited-time bundle with direct checkout.`,
      cta: "Reply with your preferred quantity and I will lock the discounted order link.",
      follow_up_in_hours: 12,
      tone
    };
  }
  return {
    opener: `Hi ${leadName}, thanks for your interest in ${productName}.`,
    angle: `I can help you choose quickly so you can complete checkout with confidence.`,
    cta: "Tell me your size/color preference and I will share the best match now.",
    follow_up_in_hours: 24,
    objective,
    tone
  };
}

function sanitizeAutomationMeta(input){
  if(!input || typeof input !== "object"){
    return {};
  }
  const out = {};
  Object.entries(input).slice(0, 20).forEach(([key, value]) => {
    const cleanKey = sanitizeToken(key, 40).toLowerCase();
    if(!cleanKey) return;
    if(value === null || value === undefined){
      return;
    }
    if(typeof value === "number"){
      out[cleanKey] = Number.isFinite(value) ? value : 0;
      return;
    }
    if(typeof value === "boolean"){
      out[cleanKey] = value;
      return;
    }
    out[cleanKey] = String(value).trim().slice(0, 400);
  });
  return out;
}

function runFollowupSweep(state){
  const nowMs = Date.now();
  const due = [];
  (state.automation.followups || []).forEach(item => {
    if(!item || item.status !== "scheduled") return;
    const dueMs = new Date(item.due_at || "").getTime();
    if(Number.isFinite(dueMs) && dueMs <= nowMs){
      item.status = "ready";
      item.ready_at = new Date().toISOString();
      due.push(item);
    }
  });
  return due;
}

function computeMonthlyReport(state, userId, monthKey){
  const uid = sanitizeUserId(userId);
  const month = parseMonthlyKey(monthKey);
  const events = (state.automation.funnel_events || []).filter(item => {
    if(uid && sanitizeUserId(item?.user_id) !== uid) return false;
    return String(item?.month || "") === month;
  });
  const counts = {};
  DEFAULT_TRUST_FLOW.forEach(step => { counts[step] = 0; });
  events.forEach(item => {
    const step = sanitizeTrustStep(item?.step);
    counts[step] = safeNumber(counts[step]) + 1;
  });
  const discovery = safeNumber(counts.discovery);
  const checkout = safeNumber(counts.checkout);
  const conversionRate = discovery > 0 ? Math.round((checkout / discovery) * 10000) / 100 : 0;
  const dropoffs = {
    discovery_to_chat: Math.max(0, safeNumber(counts.discovery) - safeNumber(counts.chat)),
    chat_to_trust: Math.max(0, safeNumber(counts.chat) - safeNumber(counts.trust)),
    trust_to_ai_tryon: Math.max(0, safeNumber(counts.trust) - safeNumber(counts.ai_tryon)),
    ai_tryon_to_checkout: Math.max(0, safeNumber(counts.ai_tryon) - safeNumber(counts.checkout))
  };
  const followups = (state.automation.followups || []).filter(item => {
    if(uid && sanitizeUserId(item?.user_id) !== uid) return false;
    return String(item?.month || "") === month;
  });
  return {
    month,
    total_events: events.length,
    funnel: counts,
    conversion_rate_pct: conversionRate,
    dropoffs,
    followups_scheduled: followups.length,
    followups_ready: followups.filter(item => item.status === "ready").length
  };
}

async function convertBaseAmountToInrPaise(baseAmount, baseCurrency){
  const amount = safeNumber(baseAmount);
  if(!amount || amount <= 0){
    return 0;
  }

  const sourceCurrency = sanitizeCurrencyCode(baseCurrency) || "INR";
  if(sourceCurrency === "INR"){
    return Math.round(amount * 100);
  }

  if(sourceCurrency === "USD"){
    const usdInr = safeNumber(await getUsdInrRateSafe());
    if(usdInr <= 0){
      throw new Error("usd_inr_rate_unavailable");
    }
    return Math.round(amount * usdInr * 100);
  }

  const rates = await fetchPublicRates(sourceCurrency);
  const inrRate = safeNumber(rates?.INR);
  if(inrRate <= 0){
    throw new Error("currency_rate_unavailable");
  }
  return Math.round(amount * inrRate * 100);
}

function isRazorpayPaymentStatusAcceptable(status){
  const clean = String(status || "").trim().toLowerCase();
  return clean === "captured";
}

async function fetchRazorpayPaymentSnapshot(paymentId){
  const cleanPaymentId = sanitizeToken(paymentId, 120);
  if(!cleanPaymentId){
    throw new Error("invalid_payment_id");
  }
  if(!isContestRazorpayConfigured()){
    throw new Error("razorpay_not_configured");
  }

  const auth = Buffer
    .from(`${CONTEST_RAZORPAY_KEY_ID}:${CONTEST_RAZORPAY_KEY_SECRET}`)
    .toString("base64");

  const res = await fetch(
    `https://api.razorpay.com/v1/payments/${encodeURIComponent(cleanPaymentId)}`,
    {
      method: "GET",
      headers: {
        "Authorization": `Basic ${auth}`
      }
    }
  );

  const data = await res.json().catch(() => null);
  if(!res.ok){
    const desc =
      data?.error?.description ||
      data?.error?.reason ||
      "Unable to fetch Razorpay payment.";
    throw new Error(desc);
  }

  return data;
}

app.get("/api/payment/config", (req, res) => {
  const ready = isContestRazorpayConfigured();
  const mode = getContestRazorpayMode();
  const issue = getContestRazorpayConfigIssue();
  return res.json({
    ok: true,
    ready,
    mode,
    issue,
    key_id: ready ? CONTEST_RAZORPAY_KEY_ID : ""
  });
});

app.post("/api/payment/razorpay/order", async (req, res) => {
  if(!isContestRazorpayConfigured()){
    return res.status(503).json({
      ok: false,
      error: "razorpay_not_configured",
      issue: getContestRazorpayConfigIssue(),
      mode: getContestRazorpayMode(),
      message: getContestRazorpaySetupMessage()
    });
  }

  const currency = String(req.body?.currency || "INR").trim().toUpperCase();
  if(currency !== "INR"){
    return res.status(400).json({
      ok: false,
      error: "currency_not_supported",
      message: "Only INR payments are supported through Razorpay."
    });
  }

  const orderId = sanitizeToken(req.body?.order_id, 120);
  const userId = sanitizeUserId(req.body?.user_id);
  const userName = sanitizeDisplayName(req.body?.user_name);
  const rawReceipt = sanitizeToken(req.body?.receipt, 40);
  const receipt = rawReceipt || `shop_${Date.now()}_${crypto.randomBytes(3).toString("hex")}`;
  const rawNotes = sanitizePaymentNotes(req.body?.notes);
  const clientAmountPaise = Math.round(safeNumber(req.body?.amount_paise));
  const baseAmount = safeNumber(rawNotes.base_amount || req.body?.base_amount);
  const baseCurrency = sanitizeCurrencyCode(
    rawNotes.base_currency ||
    req.body?.base_currency ||
    currency
  ) || "INR";

  let amountPaise = clientAmountPaise;
  if(baseAmount > 0){
    try{
      amountPaise = await convertBaseAmountToInrPaise(baseAmount, baseCurrency);
    }catch(err){
      return res.status(400).json({
        ok: false,
        error: "amount_conversion_failed",
        message: "Unable to convert base amount to INR for Razorpay.",
        details: String(err?.message || "")
      });
    }
  }

  if(!amountPaise || amountPaise < 100){
    return res.status(400).json({
      ok: false,
      error: "amount_paise_invalid",
      message: "amount_paise must be at least 100."
    });
  }

  const notes = {
    ...rawNotes,
    base_currency: baseCurrency,
    base_amount: baseAmount > 0
      ? String(Math.round(baseAmount * 1000000) / 1000000)
      : String(rawNotes.base_amount || ""),
    client_amount_paise: String(Math.max(0, clientAmountPaise)),
    app_amount_paise: String(amountPaise),
    app_order_id: orderId || "",
    app_user_id: userId || "",
    app_user_name: userName || ""
  };

  try{
    const razorpayOrder = await createContestRazorpayOrder({
      amount_paise: amountPaise,
      receipt,
      notes
    });

    return res.json({
      ok: true,
      order: {
        key_id: CONTEST_RAZORPAY_KEY_ID,
        razorpay_order_id: razorpayOrder.id,
        amount_paise: Math.round(safeNumber(razorpayOrder.amount) || amountPaise),
        currency: String(razorpayOrder.currency || "INR").toUpperCase(),
        receipt: razorpayOrder.receipt || receipt,
        status: String(razorpayOrder.status || "created").toLowerCase()
      }
    });
  }catch(err){
    console.error("shop_order_create_error:", err?.stack || err);
    return res.status(500).json({
      ok: false,
      error: "shop_order_create_failed",
      message: String(err?.message || "Unable to create Razorpay order.")
    });
  }
});

app.post("/api/payment/razorpay/verify", async (req, res) => {
  if(!isContestRazorpayConfigured()){
    return res.status(503).json({
      ok: false,
      error: "razorpay_not_configured",
      issue: getContestRazorpayConfigIssue(),
      mode: getContestRazorpayMode(),
      message: getContestRazorpaySetupMessage()
    });
  }

  const orderId = String(req.body?.razorpay_order_id || "").trim();
  const paymentId = String(req.body?.razorpay_payment_id || "").trim();
  const signature = String(req.body?.razorpay_signature || "").trim();

  if(!orderId || !paymentId || !signature){
    return res.status(400).json({ ok: false, error: "payment_fields_required" });
  }

  if(!verifyContestPaymentSignature(orderId, paymentId, signature)){
    return res.status(400).json({ ok: false, error: "invalid_payment_signature" });
  }

  try{
    const paymentSnapshot = await fetchRazorpayPaymentSnapshot(paymentId);
    if(String(paymentSnapshot?.order_id || "").trim() !== orderId){
      return res.status(400).json({ ok: false, error: "order_payment_mismatch" });
    }

    const paymentStatus = String(paymentSnapshot?.status || "").trim().toLowerCase();
    const paymentCurrency = String(paymentSnapshot?.currency || "").trim().toUpperCase();
    const paymentAmountPaise = Math.round(safeNumber(paymentSnapshot?.amount));
    const paymentCaptured = Boolean(paymentSnapshot?.captured) || paymentStatus === "captured";
    if(!paymentStatus || !isRazorpayPaymentStatusAcceptable(paymentStatus) || !paymentCaptured){
      return res.status(409).json({
        ok: false,
        error: "invalid_payment_status",
        payment_status: paymentStatus
      });
    }
    if(paymentCurrency && paymentCurrency !== "INR"){
      return res.status(400).json({
        ok:false,
        error:"payment_currency_mismatch",
        payment_currency: paymentCurrency
      });
    }
    if(paymentAmountPaise < 100){
      return res.status(400).json({
        ok:false,
        error:"invalid_payment_amount",
        payment_amount_paise: paymentAmountPaise
      });
    }

    const paymentNotes = sanitizePaymentNotes(paymentSnapshot?.notes || {});
    const noteUserId = sanitizeUserId(
      paymentNotes.app_user_id ||
      paymentNotes.user_id ||
      req.body?.user_id
    );
    const notePlan = resolveSubscriptionPlanFromNotes(paymentNotes);
    const shouldHandleSubscription = isSubscriptionPaymentContext(paymentNotes) && notePlan !== "free" && noteUserId;
    let subscription = null;

    if(shouldHandleSubscription){
      const activation = await mutateSystemState(state => {
        const activated = activateSubscriptionInState(state, {
          user_id: noteUserId,
          plan: notePlan,
          source: "payment_verify",
          payment_id: paymentId,
          order_id: orderId,
          amount_paise: paymentAmountPaise,
          currency: paymentCurrency || "INR",
          metadata: {
            notes: paymentNotes
          }
        });
        ensureSubscriptionExpirySweep(state);
        return activated;
      });
      subscription = buildFeatureGateForSubscription(activation.subscription);
      if(subscription?.user_id){
        automationSupabaseUpsert("subscription_state", [{
          user_id: String(subscription.user_id || ""),
          plan: String(subscription.plan || "free"),
          status: String(subscription.status || "free"),
          expires_at: subscription.expires_at || null,
          source: "payment_verify",
          payment_id: paymentId,
          order_id: orderId,
          amount_paise: paymentAmountPaise,
          currency: paymentCurrency || "INR",
          updated_at: new Date().toISOString()
        }], "user_id").catch(() => {});
      }
    }else if(noteUserId){
      const current = await mutateSystemState(state => {
        ensureSubscriptionExpirySweep(state);
        const sub = getEffectiveSubscription(state, noteUserId);
        return sub;
      });
      subscription = buildFeatureGateForSubscription(current);
    }

    return res.json({
      ok: true,
      verified: true,
      razorpay_order_id: orderId,
      razorpay_payment_id: paymentId,
      payment_status: paymentStatus || "verified",
      payment_currency: paymentCurrency || "INR",
      payment_amount_paise: paymentAmountPaise,
      subscription
    });
  }catch(err){
    console.error("shop_order_verify_error:", err?.stack || err);
    return res.status(500).json({
      ok: false,
      error: "shop_order_verify_failed",
      message: String(err?.message || "Unable to verify payment.")
    });
  }
});

app.post("/api/payment/razorpay/webhook", async (req, res) => {
  if(!RAZORPAY_WEBHOOK_SECRET){
    return res.status(503).json({
      ok:false,
      error:"webhook_secret_missing",
      message:"Set RAZORPAY_WEBHOOK_SECRET for webhook verification."
    });
  }

  const signature = String(req.headers["x-razorpay-signature"] || "").trim();
  const rawBody = req.rawBody || Buffer.from(JSON.stringify(req.body || {}), "utf8");
  if(!signature || !verifyRazorpayWebhookSignature(rawBody, signature)){
    return res.status(401).json({ ok:false, error:"invalid_webhook_signature" });
  }

  const eventName = String(req.body?.event || "").trim().toLowerCase();
  const paymentEntity = req.body?.payload?.payment?.entity || {};
  const eventId = String(req.body?.payload?.payment?.entity?.id || req.body?.event || "").trim().slice(0, 120);
  const paymentId = String(paymentEntity?.id || "").trim();
  const orderId = String(paymentEntity?.order_id || "").trim();
  const notes = sanitizePaymentNotes(paymentEntity?.notes || {});
  const noteUserId = sanitizeUserId(notes.app_user_id || notes.user_id || "");
  const notePlan = resolveSubscriptionPlanFromNotes(notes);
  const amountPaise = Math.max(0, Math.round(safeNumber(paymentEntity?.amount)));
  const currency = sanitizeCurrencyCode(paymentEntity?.currency) || "INR";

  try{
    const remoteEvent = eventId ? await fetchRemoteWebhookEvent(eventId).catch(() => null) : null;
    if(remoteEvent){
      const existingSubscription = noteUserId
        ? await mutateSystemState(state => {
          ensureSubscriptionExpirySweep(state);
          return buildFeatureGateForSubscription(getEffectiveSubscription(state, noteUserId));
        })
        : null;
      return res.json({
        ok:true,
        event:eventName || "unknown",
        idempotent:true,
        payment_id: paymentId || String(remoteEvent.payment_id || ""),
        order_id: orderId || String(remoteEvent.order_id || ""),
        subscription: existingSubscription
      });
    }

    const result = await mutateSystemState(state => {
      ensureSubscriptionExpirySweep(state);
      if(eventId && state.subscriptions.webhooks[eventId]){
        return {
          ok:true,
          idempotent:true,
          subscription: noteUserId
            ? buildFeatureGateForSubscription(getEffectiveSubscription(state, noteUserId))
            : null
        };
      }
      if(eventId){
        state.subscriptions.webhooks[eventId] = {
          event: eventName,
          payment_id: paymentId,
          order_id: orderId,
          processed_at: new Date().toISOString()
        };
      }

      let subscription = null;
      if(eventName === "payment.captured" && isSubscriptionPaymentContext(notes) && notePlan !== "free" && noteUserId){
        const activated = activateSubscriptionInState(state, {
          user_id: noteUserId,
          plan: notePlan,
          source: "payment_webhook",
          payment_id: paymentId,
          order_id: orderId,
          amount_paise: amountPaise,
          currency,
          metadata: { notes, webhook_event: eventName }
        });
        subscription = buildFeatureGateForSubscription(activated.subscription);
      }else if(noteUserId){
        subscription = buildFeatureGateForSubscription(getEffectiveSubscription(state, noteUserId));
      }
      return { ok:true, idempotent:false, subscription };
    });

    if(eventId){
      automationSupabaseUpsert("subscription_webhook_events", [{
        event_id: eventId,
        event_name: eventName || "unknown",
        payment_id: paymentId || null,
        order_id: orderId || null,
        payload: req.body || {},
        created_at: new Date().toISOString()
      }], "event_id").catch(() => {});
    }
    if(result?.subscription?.user_id){
      automationSupabaseUpsert("subscription_state", [{
        user_id: String(result.subscription.user_id || ""),
        plan: String(result.subscription.plan || "free"),
        status: String(result.subscription.status || "free"),
        expires_at: result.subscription.expires_at || null,
        source: "payment_webhook",
        payment_id: paymentId || null,
        order_id: orderId || null,
        amount_paise: amountPaise,
        currency,
        updated_at: new Date().toISOString()
      }], "user_id").catch(() => {});
    }

    return res.json({
      ok:true,
      event:eventName || "unknown",
      idempotent: !!result?.idempotent,
      payment_id: paymentId || "",
      order_id: orderId || "",
      subscription: result?.subscription || null
    });
  }catch(err){
    console.error("razorpay_webhook_error:", err?.stack || err);
    return res.status(500).json({ ok:false, error:"webhook_processing_failed" });
  }
});

app.post("/api/auth/signup-direct", async (req, res) => {
  const email = sanitizeAuthEmailInput(req.body?.email);
  const password = String(req.body?.password || "");
  const displayNameInput = sanitizeDisplayName(
    req.body?.display_name ||
    req.body?.full_name ||
    req.body?.name ||
    ""
  );

  if(!isLikelyValidEmailAddress(email)){
    return res.status(400).json({ ok:false, error:"invalid_email", message:"Valid email required." });
  }
  if(password.length < 6){
    return res.status(400).json({ ok:false, error:"weak_password", message:"Password must be at least 6 characters." });
  }
  if(password.length > 200){
    return res.status(400).json({ ok:false, error:"password_too_long", message:"Password is too long." });
  }

  try{
    const created = await createSupabaseConfirmedAuthUser({
      email,
      password,
      displayName: displayNameInput
    });
    const userId = sanitizeUserId(created?.user_id);
    if(!userId){
      throw new Error("signup_direct_missing_user_id");
    }

    const derived = deriveIdentityDefaults({
      user_id: userId,
      email,
      display_name: created?.display_name || displayNameInput || "",
      full_name: created?.display_name || displayNameInput || "",
      username: req.body?.username || "",
      user_name: created?.display_name || displayNameInput || ""
    });

    const result = await mutateSystemState(state => {
      ensureSubscriptionExpirySweep(state);
      const current = state.identities[userId] && typeof state.identities[userId] === "object"
        ? state.identities[userId]
        : {};
      const merged = {
        ...current,
        ...derived,
        user_id: userId,
        display_name: derived.display_name || safePublicNameFromIdentity(current, userId),
        username: derived.username || current.username || `member_${userId.slice(0, 6)}`,
        email: derived.email || current.email || "",
        email_local: derived.email_local || current.email_local || "",
        photo: derived.photo || current.photo || "",
        search_tokens: Array.from(new Set([
          ...(Array.isArray(current.search_tokens) ? current.search_tokens : []),
          ...(Array.isArray(derived.search_tokens) ? derived.search_tokens : [])
        ])).slice(0, 60),
        created_at: current.created_at || new Date().toISOString(),
        updated_at: new Date().toISOString()
      };
      state.identities[userId] = merged;
      const verification = applyVerificationToIdentity(state, userId);
      const identityWithTrust = state.identities[userId] || merged;
      const subscription = getEffectiveSubscription(state, userId);
      return {
        identity: identityWithTrust,
        subscription,
        verification
      };
    });

    let usersRow = null;
    let syncWarning = "";
    try{
      usersRow = await upsertIdentityToUsersTable(result.identity);
    }catch(err){
      syncWarning = String(err?.message || "users_sync_failed");
    }

    try{
      if(canContestUserPay(userId)){
        await mutateContestState(state => {
          registerContestAccount(
            state,
            userId,
            result.identity.display_name || result.identity.username,
            result.identity.email
          );
          return true;
        });
      }
    }catch(_){ }

    return res.json({
      ok:true,
      user_id: userId,
      email,
      identity: {
        user_id: userId,
        display_name: safePublicNameFromIdentity(result.identity, userId),
        username: String(result.identity.username || "").trim(),
        photo: String(result.identity.photo || "").trim(),
        verified: !!result.identity.verified
      },
      synced_to_users_table: !!usersRow,
      warning: syncWarning || undefined
    });
  }catch(err){
    const message = String(err?.message || "");
    if(String(err?.code || "").trim().toLowerCase() === "email_exists"){
      return res.status(409).json({
        ok:false,
        error:"email_exists",
        message:"Email already registered. Please login."
      });
    }
    if(message.includes("supabase_service_unconfigured")){
      return res.status(500).json({
        ok:false,
        error:"supabase_unconfigured",
        message:"Server signup is not configured."
      });
    }
    console.error("signup_direct_error:", err?.stack || err);
    return res.status(500).json({
      ok:false,
      error:"signup_failed",
      message:"Unable to create account right now."
    });
  }
});

app.post("/api/account/sync", async (req, res) => {
  const userId = sanitizeUserId(req.body?.user_id);
  if(!userId){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  const identityInput = {
    user_id: userId,
    email: req.body?.email,
    display_name: req.body?.display_name,
    full_name: req.body?.full_name,
    username: req.body?.username,
    user_name: req.body?.user_name,
    photo: req.body?.photo || req.body?.avatar_url
  };
  const derived = deriveIdentityDefaults(identityInput);

  try{
    const result = await mutateSystemState(state => {
      ensureSubscriptionExpirySweep(state);
      const current = state.identities[userId] && typeof state.identities[userId] === "object"
        ? state.identities[userId]
        : {};
      const merged = {
        ...current,
        ...derived,
        user_id: userId,
        display_name: derived.display_name || safePublicNameFromIdentity(current, userId),
        username: derived.username || current.username || `member_${userId.slice(0, 6)}`,
        email: derived.email || current.email || "",
        email_local: derived.email_local || current.email_local || "",
        photo: derived.photo || current.photo || "",
        search_tokens: Array.from(new Set([
          ...(Array.isArray(current.search_tokens) ? current.search_tokens : []),
          ...(Array.isArray(derived.search_tokens) ? derived.search_tokens : [])
        ])).slice(0, 60),
        created_at: current.created_at || new Date().toISOString(),
        updated_at: new Date().toISOString()
      };
      state.identities[userId] = merged;
      const verification = applyVerificationToIdentity(state, userId);
      const identityWithTrust = state.identities[userId] || merged;
      const subscription = getEffectiveSubscription(state, userId);
      return {
        identity: identityWithTrust,
        subscription,
        verification
      };
    });

    let usersRow = null;
    let syncWarning = "";
    try{
      usersRow = await upsertIdentityToUsersTable(result.identity);
    }catch(err){
      syncWarning = String(err?.message || "users_sync_failed");
    }

    try{
      if(canContestUserPay(userId)){
        await mutateContestState(state => {
          registerContestAccount(
            state,
            userId,
            result.identity.display_name || result.identity.username,
            result.identity.email
          );
          return true;
        });
      }
    }catch(_){ }

    return res.json({
      ok:true,
      identity:{
        user_id: userId,
        display_name: safePublicNameFromIdentity(result.identity, userId),
        username: String(result.identity.username || "").trim(),
        email: String(result.identity.email || "").trim(),
        photo: String(result.identity.photo || "").trim(),
        search_tokens: Array.isArray(result.identity.search_tokens) ? result.identity.search_tokens : [],
        trust_score: Math.max(0, Math.floor(safeNumber(result.identity.trust_score))),
        verified: !!result.identity.verified
      },
      verification: result.verification || normalizeVerificationRecord(userId, {}),
      subscription: buildFeatureGateForSubscription(result.subscription),
      synced_to_users_table: !!usersRow,
      warning: syncWarning || undefined
    });
  }catch(err){
    console.error("account_sync_error:", err?.stack || err);
    return res.status(500).json({
      ok:false,
      error:"account_sync_failed",
      message:String(err?.message || "Unable to sync profile identity.")
    });
  }
});

app.get("/api/users/discover", async (req, res) => {
  const query = sanitizeUserSearchQuery(req.query?.q);
  const tokens = buildUserSearchTokens(query);
  const limit = Math.max(1, Math.min(200, Math.floor(Number(req.query?.limit) || 60)));

  try{
    const [remoteRows, systemState] = await Promise.all([
      fetchUsersForDiscovery(tokens, Math.max(limit * 3, 120)),
      readSystemState()
    ]);
    ensureSubscriptionExpirySweep(systemState);

    const merged = new Map();
    const push = (row) => {
      const userId = sanitizeUserId(row?.user_id);
      if(!userId) return;
      const stateIdentity = systemState.identities[userId] || {};
      const verification = getVerificationForUser(systemState, userId);
      const displayName = safePublicNameFromIdentity({
        display_name: row?.display_name || row?.full_name || row?.username || stateIdentity.display_name,
        username: row?.username || stateIdentity.username,
        email_local: row?.email_local || stateIdentity.email_local,
        email: row?.email || stateIdentity.email
      }, userId);
      const username = sanitizeDisplayName(row?.username || stateIdentity.username || "");
      const email = String(row?.email || stateIdentity.email || "").trim().toLowerCase().slice(0, 180);
      const emailLocal = String(row?.email_local || stateIdentity.email_local || (email.split("@")[0] || "")).trim().slice(0, 120);
      const photo = String(row?.photo || stateIdentity.photo || "").trim().slice(0, 3000);
      const searchTokens = Array.from(new Set([
        ...(Array.isArray(row?.search_tokens) ? row.search_tokens : []),
        ...(Array.isArray(stateIdentity.search_tokens) ? stateIdentity.search_tokens : []),
        ...tokenizeIdentityText(displayName),
        ...tokenizeIdentityText(username),
        ...tokenizeIdentityText(emailLocal)
      ])).slice(0, 64);
      merged.set(userId, {
        user_id: userId,
        display_name: displayName,
        username,
        full_name: displayName,
        email,
        email_local: emailLocal,
        photo,
        search_tokens: searchTokens,
        created_at: String(row?.created_at || stateIdentity.created_at || "").trim(),
        verified: !!verification.verified,
        trust_score: Math.max(0, Math.floor(safeNumber(verification.trust_score))),
        search_boost: Math.max(0, Math.floor(safeNumber(verification.search_boost))),
        followers_count: Math.max(0, Math.floor(safeNumber(verification.followers_count)))
      });
    };

    (remoteRows || []).forEach(push);
    Object.values(systemState.identities || {}).forEach(push);

    const rows = Array.from(merged.values())
      .map(item => ({
        ...item,
        score: scoreUserSearchMatch(item, tokens) + Math.max(0, Math.floor(safeNumber(item.search_boost)))
      }))
      .filter(item => tokens.length ? item.score > 0 : true)
      .sort((a, b) => {
        if(b.score !== a.score){
          return b.score - a.score;
        }
        return String(b.created_at || "").localeCompare(String(a.created_at || ""));
      })
      .slice(0, limit)
      .map(item => ({
        user_id: item.user_id,
        display_name: item.display_name,
        username: item.username || "",
        photo: item.photo || "",
        verified: !!item.verified,
        verified_badge: item.verified ? "Verified" : "",
        trust_score: Math.max(0, Math.floor(safeNumber(item.trust_score))),
        followers_count: Math.max(0, Math.floor(safeNumber(item.followers_count))),
        search_boost: Math.max(0, Math.floor(safeNumber(item.search_boost))),
        can_message: true,
        profile_url: `m-account.html?uid=${encodeURIComponent(item.user_id)}`
      }));

    return res.json({
      ok:true,
      query,
      tokens,
      total: rows.length,
      users: rows
    });
  }catch(err){
    console.error("users_discover_error:", err?.stack || err);
    return res.status(500).json({
      ok:false,
      error:"users_discover_failed",
      message:String(err?.message || "Unable to load users.")
    });
  }
});

app.get("/api/users/summary", async (req, res) => {
  const ids = String(req.query?.ids || "")
    .split(",")
    .map(sanitizeUserId)
    .filter(Boolean)
    .slice(0, 100);
  if(!ids.length){
    return res.json({ ok:true, users:{} });
  }
  try{
    const [state, remoteRows] = await Promise.all([
      readSystemState(),
      fetchUsersByIds(ids)
    ]);
    const remoteMap = new Map();
    (remoteRows || []).forEach(row => {
      const id = sanitizeUserId(row?.user_id);
      if(!id || remoteMap.has(id)) return;
      remoteMap.set(id, row);
    });

    const map = {};
    ids.forEach(userId => {
      const identity = state.identities[userId] || {};
      const remote = remoteMap.get(userId) || {};
      const verification = getVerificationForUser(state, userId);
      const email = String(remote.email || identity.email || "").trim().toLowerCase().slice(0, 180);
      const emailLocal = String(
        remote.email_local ||
        identity.email_local ||
        (email.split("@")[0] || "")
      ).trim().slice(0, 120);
      const mergedIdentity = {
        display_name: remote.display_name || remote.full_name || identity.display_name || identity.full_name || "",
        username: remote.username || identity.username || "",
        email,
        email_local: emailLocal,
        photo: remote.photo || identity.photo || ""
      };
      map[userId] = {
        user_id: userId,
        display_name: safePublicNameFromIdentity(mergedIdentity, userId),
        username: String(mergedIdentity.username || "").trim(),
        email,
        email_local: emailLocal,
        photo: String(mergedIdentity.photo || "").trim(),
        verified: !!verification.verified,
        trust_score: Math.max(0, Math.floor(safeNumber(verification.trust_score)))
      };
    });
    return res.json({ ok:true, users: map });
  }catch(err){
    return res.status(500).json({ ok:false, error:"users_summary_failed" });
  }
});

app.get("/api/verification/status", async (req, res) => {
  const userId = sanitizeUserId(req.query?.user_id);
  if(!userId){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  try{
    const verification = await mutateSystemState(state => {
      ensureSubscriptionExpirySweep(state);
      const next = applyVerificationToIdentity(state, userId) || getVerificationForUser(state, userId);
      return next;
    });
    return res.json({
      ok:true,
      user_id: userId,
      min_followers: VERIFIED_MIN_FOLLOWERS,
      verification: {
        ...verification,
        verified_badge: verification.verified ? "Verified" : ""
      }
    });
  }catch(err){
    console.error("verification_status_error:", err?.stack || err);
    return res.status(500).json({ ok:false, error:"verification_status_failed" });
  }
});

app.post("/api/verification/request", async (req, res) => {
  const userId = sanitizeUserId(req.body?.user_id);
  if(!userId){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  const kycCompleted = parseBooleanFlag(req.body?.kyc_completed);
  const kycReference = String(req.body?.kyc_reference || req.body?.kyc_id || "").trim().slice(0, 120);
  const fallbackFollowers = Math.max(0, Math.floor(safeNumber(req.body?.followers_count)));
  const requestId = sanitizeToken(req.body?.request_id, 80) || `vr_${Date.now()}_${crypto.randomBytes(3).toString("hex")}`;
  const nowIso = new Date().toISOString();

  try{
    const followersCount = fallbackFollowers > 0
      ? fallbackFollowers
      : await fetchFollowersCountForUser(userId).catch(() => 0);

    const payload = await mutateSystemState(state => {
      ensureSubscriptionExpirySweep(state);
      const previous = getVerificationForUser(state, userId);
      const eligible = followersCount >= VERIFIED_MIN_FOLLOWERS && kycCompleted;
      const status = eligible ? "pending_admin" : "requirements_not_met";
      const next = normalizeVerificationRecord(userId, {
        ...previous,
        user_id: userId,
        request_id: requestId,
        followers_count: followersCount,
        kyc_completed: kycCompleted,
        kyc_reference: kycReference || previous.kyc_reference || "",
        status,
        approved_by_admin: false,
        requested_at: nowIso,
        approved_at: "",
        rejected_at: "",
        admin_id: "",
        admin_note: "",
        updated_at: nowIso
      });
      state.verification.by_user[userId] = next;
      state.verification.requests[requestId] = {
        request_id: requestId,
        user_id: userId,
        followers_count: followersCount,
        kyc_completed: kycCompleted,
        kyc_reference: next.kyc_reference || "",
        status,
        created_at: nowIso,
        updated_at: nowIso
      };
      pushBounded(state.verification.history, {
        type: "request",
        request_id: requestId,
        user_id: userId,
        status,
        at: nowIso
      }, 4000);
      pushBounded(state.automation.notifications, {
        id: `verification_req_${requestId}`,
        type: "verification_request",
        user_id: userId,
        created_at: nowIso,
        payload: {
          request_id: requestId,
          followers_count: followersCount,
          kyc_completed: kycCompleted,
          status
        }
      }, 6000);
      applyVerificationToIdentity(state, userId);
      return {
        eligible,
        verification: getVerificationForUser(state, userId)
      };
    });

    notifyAutomationAdmin("verification_request", {
      request_id: requestId,
      user_id: userId,
      followers_count: followersCount,
      kyc_completed: kycCompleted,
      status: payload.verification.status
    }).catch(() => {});

    return res.json({
      ok:true,
      user_id: userId,
      min_followers: VERIFIED_MIN_FOLLOWERS,
      eligible: payload.eligible,
      next_step: payload.eligible ? "await_admin_approval" : "complete_requirements",
      verification: {
        ...payload.verification,
        verified_badge: payload.verification.verified ? "Verified" : ""
      }
    });
  }catch(err){
    console.error("verification_request_error:", err?.stack || err);
    return res.status(500).json({ ok:false, error:"verification_request_failed" });
  }
});

app.post("/api/admin/verification/decision", async (req, res) => {
  if(!isAdminAutomationRequest(req)){
    return res.status(401).json({ ok:false, error:"admin_auth_required" });
  }
  const requestId = sanitizeToken(req.body?.request_id, 80);
  const decision = normalizeVerificationDecision(req.body?.decision || req.body?.status);
  const adminId = sanitizeUserId(req.body?.admin_id || "admin");
  const adminNote = String(req.body?.admin_note || "").trim().slice(0, 300);
  if(!requestId || !decision){
    return res.status(400).json({ ok:false, error:"request_id_and_decision_required" });
  }
  const nowIso = new Date().toISOString();

  try{
    const payload = await mutateSystemState(state => {
      ensureSubscriptionExpirySweep(state);
      const requestRow = state.verification.requests[requestId];
      if(!requestRow){
        return { ok:false, error:"request_not_found" };
      }
      const userId = sanitizeUserId(requestRow.user_id);
      const previous = getVerificationForUser(state, userId);
      const followersCount = Math.max(0, Math.floor(safeNumber(requestRow.followers_count || previous.followers_count)));
      const kycCompleted = parseBooleanFlag(requestRow.kyc_completed || previous.kyc_completed);
      const eligible = followersCount >= VERIFIED_MIN_FOLLOWERS && kycCompleted;
      let status = decision === "approve" ? "approved" : "rejected";
      let approvedByAdmin = decision === "approve";
      if(decision === "approve" && !eligible){
        status = "requirements_not_met";
        approvedByAdmin = false;
      }
      const next = normalizeVerificationRecord(userId, {
        ...previous,
        request_id: requestId,
        followers_count: followersCount,
        kyc_completed: kycCompleted,
        kyc_reference: requestRow.kyc_reference || previous.kyc_reference || "",
        status,
        approved_by_admin: approvedByAdmin,
        approved_at: approvedByAdmin ? nowIso : "",
        rejected_at: status === "rejected" ? nowIso : "",
        admin_id: adminId,
        admin_note: adminNote,
        updated_at: nowIso
      });
      state.verification.by_user[userId] = next;
      requestRow.status = status;
      requestRow.decision = decision;
      requestRow.eligible = eligible;
      requestRow.admin_id = adminId;
      requestRow.admin_note = adminNote;
      requestRow.decided_at = nowIso;
      requestRow.updated_at = nowIso;
      state.verification.requests[requestId] = requestRow;
      pushBounded(state.verification.history, {
        type: "decision",
        request_id: requestId,
        user_id: userId,
        decision,
        status,
        at: nowIso
      }, 4000);
      pushBounded(state.automation.notifications, {
        id: `verification_decision_${requestId}`,
        type: "verification_decision",
        user_id: userId,
        created_at: nowIso,
        payload: {
          request_id: requestId,
          decision,
          status
        }
      }, 6000);
      const verification = applyVerificationToIdentity(state, userId) || next;
      return {
        ok:true,
        request_id: requestId,
        user_id: userId,
        decision,
        eligible,
        verification
      };
    });

    if(!payload.ok){
      return res.status(404).json(payload);
    }

    notifyAutomationAdmin("verification_decision", {
      request_id: payload.request_id,
      user_id: payload.user_id,
      decision: payload.decision,
      status: payload.verification.status
    }).catch(() => {});

    return res.json({
      ok:true,
      request_id: payload.request_id,
      user_id: payload.user_id,
      decision: payload.decision,
      eligible: payload.eligible,
      verification: {
        ...payload.verification,
        verified_badge: payload.verification.verified ? "Verified" : ""
      }
    });
  }catch(err){
    console.error("verification_decision_error:", err?.stack || err);
    return res.status(500).json({ ok:false, error:"verification_decision_failed" });
  }
});

app.get("/api/subscription/status", async (req, res) => {
  const userId = sanitizeUserId(req.query?.user_id);
  if(!userId){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  try{
    const remote = await fetchRemoteSubscriptionState(userId).catch(() => null);
    const payload = await mutateSystemState(state => {
      if(remote && typeof remote === "object"){
        state.subscriptions.users[userId] = normalizeSubscriptionRecord(userId, {
          user_id: userId,
          plan: remote.plan,
          status: remote.status,
          expires_at: remote.expires_at,
          source: remote.source || "remote_sync",
          payment_id: remote.payment_id,
          order_id: remote.order_id,
          amount_paise: remote.amount_paise,
          currency: remote.currency,
          updated_at: remote.updated_at
        });
      }
      ensureSubscriptionExpirySweep(state);
      const subscription = getEffectiveSubscription(state, userId);
      const gate = buildFeatureGateForSubscription(subscription);
      const ownerId = getSeatOwnerForMember(state, userId) || userId;
      const seat = state.premium.seats[ownerId] || null;
      return {
        subscription: gate,
        seat: seat
          ? {
            owner_user_id: sanitizeUserId(seat.owner_user_id),
            seat_limit: Math.max(1, Math.floor(safeNumber(seat.seat_limit) || 1)),
            members: Array.isArray(seat.members) ? seat.members.map(sanitizeUserId).filter(Boolean) : []
          }
          : null
      };
    });
    return res.json({
      ok:true,
      ...payload
    });
  }catch(err){
    console.error("subscription_status_error:", err?.stack || err);
    return res.status(500).json({ ok:false, error:"subscription_status_failed" });
  }
});

app.post("/api/subscription/activate", async (req, res) => {
  const userId = sanitizeUserId(req.body?.user_id);
  const plan = normalizePlanCode(req.body?.plan);
  if(!userId || plan === "free"){
    return res.status(400).json({
      ok:false,
      error:"invalid_subscription_activation",
      message:"Valid user_id and paid plan are required."
    });
  }

  try{
    const activation = await mutateSystemState(state => {
      ensureSubscriptionExpirySweep(state);
      return activateSubscriptionInState(state, {
        user_id: userId,
        plan,
        source: String(req.body?.source || "automatic_activation").slice(0, 40),
        payment_id: req.body?.payment_id || req.body?.razorpay_payment_id,
        order_id: req.body?.order_id || req.body?.razorpay_order_id,
        amount_paise: req.body?.amount_paise || req.body?.payment_amount_paise,
        currency: req.body?.currency || req.body?.payment_currency,
        metadata: sanitizeAutomationMeta(req.body?.metadata || {})
      });
    });
    const gate = buildFeatureGateForSubscription(activation?.subscription || {});
    automationSupabaseUpsert("subscription_state", [{
      user_id: String(gate.user_id || userId),
      plan: String(gate.plan || "free"),
      status: String(gate.status || "free"),
      expires_at: gate.expires_at || null,
      source: String(req.body?.source || "automatic_activation").slice(0, 40),
      payment_id: String(req.body?.payment_id || req.body?.razorpay_payment_id || ""),
      order_id: String(req.body?.order_id || req.body?.razorpay_order_id || ""),
      amount_paise: Math.max(0, Math.round(safeNumber(req.body?.amount_paise || req.body?.payment_amount_paise))),
      currency: sanitizeCurrencyCode(req.body?.currency || req.body?.payment_currency) || "INR",
      metadata: sanitizeAutomationMeta(req.body?.metadata || {}),
      updated_at: new Date().toISOString()
    }], "user_id").catch(() => {});
    return res.json({
      ok:true,
      activation: {
        idempotent: !!activation?.idempotent
      },
      subscription: gate
    });
  }catch(err){
    console.error("subscription_activate_error:", err?.stack || err);
    return res.status(500).json({ ok:false, error:"subscription_activate_failed" });
  }
});

app.get("/api/feature-gates", async (req, res) => {
  const userId = sanitizeUserId(req.query?.user_id);
  if(!userId){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  try{
    const gate = await mutateSystemState(state => {
      ensureSubscriptionExpirySweep(state);
      return buildFeatureGateForSubscription(getEffectiveSubscription(state, userId));
    });
    return res.json({ ok:true, gates: gate });
  }catch(err){
    return res.status(500).json({ ok:false, error:"feature_gate_failed" });
  }
});

app.post("/api/automation/track", async (req, res) => {
  const userId = sanitizeUserId(req.body?.user_id);
  if(!userId){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  const step = sanitizeTrustStep(req.body?.step);
  const eventId = sanitizeToken(req.body?.event_id, 80) || `${Date.now()}_${crypto.randomBytes(4).toString("hex")}`;
  const meta = sanitizeAutomationMeta(req.body?.meta || {});
  const nowIso = new Date().toISOString();
  const month = parseMonthlyKey(nowIso.slice(0, 7));

  try{
    const result = await mutateSystemState(state => {
      ensureSubscriptionExpirySweep(state);
      runFollowupSweep(state);

      const subscription = getEffectiveSubscription(state, userId);
      const gate = buildFeatureGateForSubscription(subscription);
      pushBounded(state.automation.funnel_events, {
        id: eventId,
        user_id: userId,
        step,
        month,
        at: nowIso,
        meta
      }, 8000);

      if(step === "chat" && gate.features.auto_followup){
        const dueAt = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
        pushBounded(state.automation.followups, {
          id: `fu_${eventId}`,
          user_id: userId,
          month,
          step: "checkout",
          status: "scheduled",
          due_at: dueAt,
          created_at: nowIso,
          meta: {
            reason: "post_chat_nudge",
            objective: "checkout_completion"
          }
        }, 4000);
      }

      if(step === "checkout"){
        const identity = state.identities[userId] || {};
        const points = Math.max(0, Math.floor(safeNumber(identity.growth_points) || 0)) + 10;
        const previous = Math.max(0, Math.floor(safeNumber(identity.growth_points) || 0));
        identity.growth_points = points;
        identity.updated_at = nowIso;
        state.identities[userId] = identity;
        const prevTier = Math.floor(previous / 100);
        const nextTier = Math.floor(points / 100);
        if(nextTier > prevTier){
          pushBounded(state.automation.notifications, {
            id: `growth_${userId}_${points}`,
            type: "growth_reward_unlocked",
            user_id: userId,
            created_at: nowIso,
            payload: {
              points,
              unlocked_tier: nextTier,
              reward: "priority_boost_coupon"
            }
          }, 6000);
        }
      }

      const report = computeMonthlyReport(state, userId, month);
      state.automation.reports[`${userId}:${month}`] = report;
      const identity = state.identities[userId] || {};
      const points = Math.max(0, Math.floor(safeNumber(identity.growth_points) || 0));
      const nextRewardAt = (Math.floor(points / 100) + 1) * 100;
      return {
        gate,
        report,
        growth: {
          points,
          next_reward_at: nextRewardAt,
          to_next_reward: Math.max(0, nextRewardAt - points)
        }
      };
    });
    automationSupabaseUpsert("automation_funnel_events", [{
      id: eventId,
      user_id: userId,
      step,
      month_key: month,
      meta,
      created_at: nowIso
    }], "id").catch(() => {});

    return res.json({
      ok:true,
      tracked:true,
      step,
      subscription: result.gate,
      report: result.report,
      growth: result.growth
    });
  }catch(err){
    console.error("automation_track_error:", err?.stack || err);
    return res.status(500).json({ ok:false, error:"automation_track_failed" });
  }
});

app.get("/api/reports/monthly", async (req, res) => {
  const userId = sanitizeUserId(req.query?.user_id);
  if(!userId){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  const month = parseMonthlyKey(req.query?.month);
  try{
    const payload = await mutateSystemState(state => {
      ensureSubscriptionExpirySweep(state);
      runFollowupSweep(state);
      const subscription = getEffectiveSubscription(state, userId);
      const gate = buildFeatureGateForSubscription(subscription);
      const report = computeMonthlyReport(state, userId, month);
      const key = `${userId}:${month}`;
      state.automation.reports[key] = report;
      return { gate, report };
    });
    return res.json({
      ok:true,
      user_id:userId,
      month,
      subscription: payload.gate,
      report: {
        ...payload.report,
        behavioral_insights: payload.gate.features.behavioral_insights
          ? {
            likely_drop_stage: Object.entries(payload.report.dropoffs || {})
              .sort((a, b) => safeNumber(b[1]) - safeNumber(a[1]))[0]?.[0] || "none",
            recommendation: "Use targeted follow-ups at the largest drop-off stage."
          }
          : null
      }
    });
  }catch(err){
    console.error("monthly_report_error:", err?.stack || err);
    return res.status(500).json({ ok:false, error:"monthly_report_failed" });
  }
});

app.get("/api/growth/incentives", async (req, res) => {
  const userId = sanitizeUserId(req.query?.user_id);
  if(!userId){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  try{
    const payload = await mutateSystemState(state => {
      ensureSubscriptionExpirySweep(state);
      const identity = state.identities[userId] || {};
      const points = Math.max(0, Math.floor(safeNumber(identity.growth_points) || 0));
      const tier = Math.floor(points / 100);
      const nextRewardAt = (tier + 1) * 100;
      const notifications = (state.automation.notifications || [])
        .filter(item => sanitizeUserId(item?.user_id) === userId && item?.type === "growth_reward_unlocked")
        .slice(-10)
        .reverse();
      return {
        points,
        current_tier: tier,
        next_reward_at: nextRewardAt,
        to_next_reward: Math.max(0, nextRewardAt - points),
        recent_rewards: notifications
      };
    });
    return res.json({ ok:true, user_id:userId, ...payload });
  }catch(err){
    return res.status(500).json({ ok:false, error:"growth_incentives_failed" });
  }
});

app.post("/api/assistant/dm", async (req, res) => {
  const userId = sanitizeUserId(req.body?.user_id);
  if(!userId){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  const requestedMode = String(req.body?.mode || "").trim().toLowerCase();
  try{
    const gate = await mutateSystemState(state => {
      ensureSubscriptionExpirySweep(state);
      return buildFeatureGateForSubscription(getEffectiveSubscription(state, userId));
    });
    const canStandard = !!gate.features.smart_dm_assistant;
    const canAdvanced = !!gate.features.advanced_revenue_assistant;
    if(!canStandard && !canAdvanced){
      return res.status(403).json({
        ok:false,
        error:"plan_upgrade_required",
        message:"DM assistant is available on paid plans."
      });
    }
    const mode = (requestedMode === "revenue" && canAdvanced) ? "revenue" : "standard";
    const suggestion = buildAssistantReply(mode, req.body || {});
    return res.json({
      ok:true,
      mode,
      subscription: gate,
      suggestion
    });
  }catch(err){
    console.error("assistant_dm_error:", err?.stack || err);
    return res.status(500).json({ ok:false, error:"assistant_dm_failed" });
  }
});

app.post("/api/premium/invite/create", async (req, res) => {
  const ownerUserId = sanitizeUserId(req.body?.owner_user_id || req.body?.user_id);
  if(!ownerUserId){
    return res.status(400).json({ ok:false, error:"owner_user_id_required" });
  }
  const inviteEmail = String(req.body?.invitee_email || "").trim().toLowerCase().slice(0, 180);
  const nowIso = new Date().toISOString();
  try{
    const result = await mutateSystemState(state => {
      ensureSubscriptionExpirySweep(state);
      const ownerSub = getEffectiveSubscription(state, ownerUserId);
      const ownerGate = buildFeatureGateForSubscription(ownerSub);
      if(ownerGate.plan !== "4000" || !ownerGate.features.invite_only){
        return { ok:false, error:"premium_plan_required" };
      }
      const seat = ensureActiveSeatState(state, ownerUserId);
      const members = Array.isArray(seat.members) ? seat.members : [];
      if(members.length >= seat.seat_limit){
        return { ok:false, error:"seat_limit_reached", seat };
      }
      const token = `inv_${Date.now()}_${crypto.randomBytes(4).toString("hex")}`;
      const expiresAt = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString();
      state.premium.invites[token] = {
        token,
        owner_user_id: ownerUserId,
        invitee_email: inviteEmail || "",
        status: "pending",
        created_at: nowIso,
        expires_at: expiresAt
      };
      return {
        ok:true,
        invite: state.premium.invites[token],
        seat
      };
    });

    if(!result.ok){
      return res.status(409).json(result);
    }
    return res.json({
      ok:true,
      invite: result.invite,
      seat: {
        owner_user_id: result.seat.owner_user_id,
        seat_limit: result.seat.seat_limit,
        members: result.seat.members
      }
    });
  }catch(err){
    console.error("premium_invite_create_error:", err?.stack || err);
    return res.status(500).json({ ok:false, error:"premium_invite_create_failed" });
  }
});

app.post("/api/premium/invite/redeem", async (req, res) => {
  const token = String(req.body?.token || "").trim();
  const userId = sanitizeUserId(req.body?.user_id);
  const email = String(req.body?.email || "").trim().toLowerCase().slice(0, 180);
  if(!token || !userId){
    return res.status(400).json({ ok:false, error:"token_and_user_id_required" });
  }
  try{
    const result = await mutateSystemState(state => {
      ensureSubscriptionExpirySweep(state);
      const invite = state.premium.invites[token];
      if(!invite || invite.status !== "pending"){
        return { ok:false, error:"invite_invalid" };
      }
      if(invite.expires_at && !isIsoFuture(invite.expires_at)){
        invite.status = "expired";
        return { ok:false, error:"invite_expired" };
      }
      if(invite.invitee_email && email && invite.invitee_email !== email){
        return { ok:false, error:"invite_email_mismatch" };
      }
      const ownerId = sanitizeUserId(invite.owner_user_id);
      const ownerSub = getEffectiveSubscription(state, ownerId);
      const ownerGate = buildFeatureGateForSubscription(ownerSub);
      if(ownerGate.plan !== "4000"){
        return { ok:false, error:"owner_plan_inactive" };
      }
      const seat = ensureActiveSeatState(state, ownerId);
      if(!seat.members.includes(userId)){
        if(seat.members.length >= seat.seat_limit){
          return { ok:false, error:"seat_limit_reached" };
        }
        seat.members.push(userId);
        seat.updated_at = new Date().toISOString();
      }
      invite.status = "redeemed";
      invite.redeemed_by = userId;
      invite.redeemed_at = new Date().toISOString();
      return { ok:true, owner_user_id: ownerId, seat };
    });
    if(!result.ok){
      return res.status(409).json(result);
    }
    return res.json({
      ok:true,
      owner_user_id: result.owner_user_id,
      seat: {
        owner_user_id: result.seat.owner_user_id,
        seat_limit: result.seat.seat_limit,
        members: result.seat.members
      }
    });
  }catch(err){
    console.error("premium_invite_redeem_error:", err?.stack || err);
    return res.status(500).json({ ok:false, error:"premium_invite_redeem_failed" });
  }
});

app.get("/api/premium/seats", async (req, res) => {
  const userId = sanitizeUserId(req.query?.user_id);
  if(!userId){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  try{
    const data = await mutateSystemState(state => {
      ensureSubscriptionExpirySweep(state);
      const ownerId = getSeatOwnerForMember(state, userId) || userId;
      const seat = state.premium.seats[ownerId] || null;
      const ownerSub = getEffectiveSubscription(state, ownerId);
      return {
        owner_user_id: ownerId,
        seat: seat
          ? {
            owner_user_id: sanitizeUserId(seat.owner_user_id),
            seat_limit: Math.max(1, Math.floor(safeNumber(seat.seat_limit) || 1)),
            members: Array.isArray(seat.members) ? seat.members.map(sanitizeUserId).filter(Boolean) : []
          }
          : null,
        owner_subscription: buildFeatureGateForSubscription(ownerSub)
      };
    });
    return res.json({ ok:true, ...data });
  }catch(err){
    return res.status(500).json({ ok:false, error:"premium_seats_failed" });
  }
});

app.post("/api/media/events/upload", async (req, res) => {
  const userId = sanitizeUserId(req.body?.user_id);
  if(!userId){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  const event = {
    id: `med_${Date.now()}_${crypto.randomBytes(4).toString("hex")}`,
    user_id: userId,
    type: String(req.body?.type || req.body?.media_type || "upload").trim().slice(0, 40),
    bucket: String(req.body?.bucket || "").trim().slice(0, 80),
    path: String(req.body?.path || "").trim().slice(0, 320),
    url: String(req.body?.url || "").trim().slice(0, 3000),
    file_name: String(req.body?.file_name || "").trim().slice(0, 180),
    mime: String(req.body?.mime || "").trim().slice(0, 120),
    size: Math.max(0, Math.round(safeNumber(req.body?.size))),
    source: String(req.body?.source || "web").trim().slice(0, 80),
    product_id: String(req.body?.product_id || "").trim().slice(0, 120),
    created_at: new Date().toISOString()
  };

  try{
    await mutateSystemState(state => {
      ensureSubscriptionExpirySweep(state);
      pushBounded(state.automation.media_events, event, 6000);
      pushBounded(state.automation.notifications, {
        id: `notif_${event.id}`,
        type: "media_upload",
        user_id: userId,
        created_at: event.created_at,
        payload: event
      }, 6000);
      return true;
    });
    automationSupabaseUpsert("automation_media_events", [{
      id: event.id,
      user_id: event.user_id,
      media_type: event.type || "upload",
      bucket: event.bucket || null,
      path: event.path || null,
      url: event.url || null,
      file_name: event.file_name || null,
      mime: event.mime || null,
      size_bytes: Math.max(0, Math.round(safeNumber(event.size))),
      source: event.source || null,
      product_id: event.product_id || null,
      created_at: event.created_at
    }], "id").catch(() => {});
    notifyAutomationAdmin("media_upload", event).catch(()=>{});
    return res.json({ ok:true, event });
  }catch(err){
    console.error("media_event_upload_error:", err?.stack || err);
    return res.status(500).json({ ok:false, error:"media_event_upload_failed" });
  }
});

app.get("/api/admin/events", async (req, res) => {
  const limit = Math.max(1, Math.min(300, Math.floor(Number(req.query?.limit) || 50)));
  try{
    const state = await readSystemState();
    ensureSubscriptionExpirySweep(state);
    const mediaEvents = (state.automation.media_events || []).slice(-limit).reverse();
    const notifications = (state.automation.notifications || []).slice(-limit).reverse();
    const followups = (state.automation.followups || []).slice(-limit).reverse();
    return res.json({
      ok:true,
      events:{
        media_uploads: mediaEvents,
        notifications,
        followups
      }
    });
  }catch(err){
    return res.status(500).json({ ok:false, error:"admin_events_failed" });
  }
});

app.get("/api/contest/dashboard", async (req, res) => {
  try{
    const userId = sanitizeUserId(req.query.user_id);
    const userName = sanitizeDisplayName(req.query.user_name);
    const state = await readContestState();
    const totalAccounts = await getContestTotalAccountCount(state);
    const data = buildContestPayload(state, userId, userName, {
      total_accounts: totalAccounts
    });
    if(userId && data?.user_stats){
      await syncContestSnapshots({ userLike: data.user_stats });
    }
    return res.json({ ok:true, data });
  }catch(err){
    console.error("contest_dashboard_error:", err?.stack || err);
    return res.status(500).json({ ok:false, error:"contest_dashboard_failed" });
  }
});

app.get("/api/contest/ranks", async (req, res) => {
  try{
    const limit = Math.max(1, Math.min(1000, Math.floor(Number(req.query?.limit) || 200)));
    const userId = sanitizeUserId(req.query?.user_id);
    const state = await readContestState();
    let allRows = [];
    try{
      allRows = await buildReferralRanksWithProfiles(state, 5000);
    }catch(rankErr){
      console.error("contest_ranks_profile_fallback:", rankErr?.stack || rankErr);
      allRows = buildReferralRanks(state, {}, 5000);
    }
    const rows = allRows.slice(0, limit);
    const topReferrer = allRows[0] || null;
    const userRank = userId
      ? (allRows.find(item => String(item?.user_id || "") === userId) || null)
      : null;
    return res.json({
      ok:true,
      data:{
        generated_at: new Date().toISOString(),
        total_rows: allRows.length,
        rows,
        top_referrer: topReferrer,
        user_rank: userRank
      }
    });
  }catch(err){
    console.error("contest_ranks_error:", err?.stack || err);
    return res.status(500).json({ ok:false, error:"contest_ranks_failed" });
  }
});

app.post("/api/contest/account/register", async (req, res) => {
  const userId = sanitizeUserId(req.body?.user_id);
  const userName = sanitizeDisplayName(req.body?.user_name);
  const userEmail = String(req.body?.email || "").trim().toLowerCase();

  if(!userId){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  if(!canContestUserPay(userId)){
    return res.status(401).json({
      ok:false,
      error:"login_required",
      message:"Login required with a valid account."
    });
  }

  try{
    const result = await mutateContestState(state => {
      const existed = Boolean(state?.registered_accounts?.[userId]);
      const row = registerContestAccount(state, userId, userName, userEmail);
      return {
        ok:true,
        already_registered: existed,
        registered_count: getContestRegisteredAccountCount(state),
        account: row
      };
    });
    return res.json(result);
  }catch(err){
    console.error("contest_account_register_error:", err?.stack || err);
    return res.status(500).json({ ok:false, error:"contest_account_register_failed" });
  }
});

app.post("/api/contest/share/action", async (req, res) => {
  const userId = sanitizeUserId(req.body?.user_id);
  const userName = sanitizeDisplayName(req.body?.user_name);
  if(!userId){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  if(!canContestUserPay(userId)){
    return res.status(401).json({
      ok:false,
      error:"login_required",
      message:"Login required with a valid account."
    });
  }
  const action = sanitizeAction(req.body?.action);

  try{
    const data = await mutateContestState(state => {
      const user = ensureContestUser(state, userId, userName);
      user.share_actions += 1;
      user.share_actions_by_type[action] = safeNumber(user.share_actions_by_type[action]) + 1;
      return buildContestPayload(state, userId, userName);
    });
    await syncContestSnapshots({ userLike: data?.user_stats });
    return res.json({ ok:true, data });
  }catch(err){
    console.error("contest_share_action_error:", err?.stack || err);
    return res.status(500).json({ ok:false, error:"contest_share_action_failed" });
  }
});

app.post("/api/contest/share/visit", async (req, res) => {
  const refCode = sanitizeReferralCode(req.body?.ref_code || req.query?.ref);
  if(!refCode){
    return res.status(400).json({ ok:false, error:"ref_code_required" });
  }
  const visitorUserId = sanitizeUserId(req.body?.visitor_user_id);
  const visitorUserName = sanitizeDisplayName(req.body?.visitor_user_name);
  const visitorId = sanitizeToken(req.body?.visitor_id, 80) || contestVisitorFingerprint(req);
  const dayKey = new Date().toISOString().slice(0, 10);

  try{
    const result = await mutateContestState(state => {
      const refUserId = getUserIdByReferralCode(state, refCode);
      if(!refUserId){
        return {
          credited: false,
          reason: "ref_code_not_found",
          ref_user_id: "",
          data: buildContestPayload(state, visitorUserId, visitorUserName)
        };
      }

      if(visitorUserId && visitorUserId === refUserId){
        return {
          credited: false,
          reason: "self_referral",
          ref_user_id: refUserId,
          data: buildContestPayload(state, visitorUserId, visitorUserName)
        };
      }

      const key = `${refUserId}|${dayKey}|${visitorId}`;
      let credited = false;
      let refUserSnapshot = null;
      if(!state.share_visit_log[key]){
        state.share_visit_log[key] = new Date().toISOString();
        const refUser = ensureContestUser(state, refUserId);
        refUser.unique_share_visits += 1;
        refUserSnapshot = safeCloneJson(refUser);
        credited = true;
      }

      return {
        credited,
        reason: credited ? "credited" : "duplicate",
        ref_user_id: refUserId,
        ref_user_snapshot: refUserSnapshot,
        data: buildContestPayload(state, visitorUserId, visitorUserName)
      };
    });

    if(result.reason === "ref_code_not_found"){
      return res.status(404).json({ ok:false, error:"ref_code_not_found", ...result });
    }
    await syncContestSnapshots({
      userLike: result?.data?.user_stats
    });
    if(result?.ref_user_snapshot){
      await syncContestSnapshots({
        userLike: result.ref_user_snapshot
      });
    }
    return res.json({ ok:true, ...result });
  }catch(err){
    console.error("contest_share_visit_error:", err?.stack || err);
    return res.status(500).json({ ok:false, error:"contest_share_visit_failed" });
  }
});

app.post("/api/contest/install", async (req, res) => {
  const requestedUserId = sanitizeUserId(req.body?.user_id);
  const userId = canContestUserPay(requestedUserId) ? requestedUserId : "";
  const userName = sanitizeDisplayName(req.body?.user_name);
  const refCode = sanitizeReferralCode(req.body?.ref_code);
  const source = sanitizeToken(req.body?.source, 40) || "unknown";
  const installId = sanitizeToken(req.body?.device_id, 120) || contestVisitorFingerprint(req);

  try{
    const result = await mutateContestState(state => {
      const existing = state.installs[installId];
      let credited = false;
      let refUserId = "";
      let refUserSnapshot = null;

      if(!existing){
        state.installs[installId] = {
          install_id: installId,
          user_id: userId || "",
          ref_code: refCode || "",
          ref_credited: false,
          source,
          created_at: new Date().toISOString()
        };
        state.downloads += 1;

        if(refCode){
          const found = getUserIdByReferralCode(state, refCode);
          if(found && (!userId || found !== userId)){
            const refUser = ensureContestUser(state, found);
            refUser.verified_installs += 1;
            refUserSnapshot = safeCloneJson(refUser);
            state.installs[installId].ref_credited = true;
            refUserId = found;
            credited = true;
          }
        }
      }else{
        if(userId && !sanitizeUserId(existing.user_id)){
          existing.user_id = userId;
        }
        if(refCode && !sanitizeReferralCode(existing.ref_code)){
          existing.ref_code = refCode;
          const found = getUserIdByReferralCode(state, refCode);
          const alreadyCredited = Boolean(existing.ref_credited);
          if(found && !alreadyCredited && (!userId || found !== userId)){
            const refUser = ensureContestUser(state, found);
            refUser.verified_installs += 1;
            refUserSnapshot = safeCloneJson(refUser);
            existing.ref_credited = true;
            refUserId = found;
            credited = true;
          }
        }
      }

      let visitorSnapshot = null;
      if(userId){
        visitorSnapshot = safeCloneJson(ensureContestUser(state, userId, userName));
      }

      return {
        duplicate_install: Boolean(existing),
        credited,
        ref_user_id: refUserId,
        ref_user_snapshot: refUserSnapshot,
        user_snapshot: visitorSnapshot,
        data: buildContestPayload(state, userId, userName)
      };
    });

    await syncContestSnapshots({ userLike: result?.user_snapshot || result?.data?.user_stats });
    if(result?.ref_user_snapshot){
      await syncContestSnapshots({ userLike: result.ref_user_snapshot });
    }
    return res.json({ ok:true, ...result });
  }catch(err){
    console.error("contest_install_error:", err?.stack || err);
    return res.status(500).json({ ok:false, error:"contest_install_failed" });
  }
});

app.post("/api/contest/order", async (req, res) => {
  const userId = sanitizeUserId(req.body?.user_id);
  const userName = sanitizeDisplayName(req.body?.user_name);
  const contestId = sanitizeToken(req.body?.contest_id, 40).toLowerCase();
  const sideId = sanitizeToken(req.body?.side_id, 40).toLowerCase();
  const packId = sanitizeToken(req.body?.pack_id, 40).toLowerCase();

  if(!userId || !contestId || !sideId || !packId){
    return res.status(400).json({ ok:false, error:"user_id_contest_id_side_id_pack_id_required" });
  }
  if(!canContestUserPay(userId)){
    return res.status(401).json({
      ok:false,
      error:"login_required",
      message:"Please login with a valid account before payment."
    });
  }
  if(!isContestRazorpayConfigured()){
    return res.status(503).json({
      ok:false,
      error:"razorpay_not_configured",
      issue: getContestRazorpayConfigIssue(),
      mode: getContestRazorpayMode(),
      message:getContestRazorpaySetupMessage()
    });
  }

  const contest = getContestById(contestId);
  const pack = getContestPackById(packId);
  const side = contest?.sides?.find(item => item.id === sideId) || null;
  if(!contest || !pack || !side){
    return res.status(400).json({ ok:false, error:"invalid_contest_or_pack_or_side" });
  }

  try{
    const usdInrRate = await getUsdInrRateSafe();
    const amountPaise = Math.max(100, Math.round(pack.usd * usdInrRate * 100));
    const receipt = `contest_${Date.now()}_${crypto.randomBytes(3).toString("hex")}`;

    const razorpayOrder = await createContestRazorpayOrder({
      amount_paise: amountPaise,
      receipt,
      notes: {
        user_id: userId,
        contest_id: contest.id,
        side_id: side.id,
        pack_id: pack.id
      }
    });

    const created = await mutateContestState(state => {
      const user = ensureContestUser(state, userId, userName);
      const nowIso = new Date().toISOString();
      const orderRecord = {
        razorpay_order_id: razorpayOrder.id,
        receipt,
        status: "created",
        user_id: userId,
        contest_id: contest.id,
        side_id: side.id,
        pack_id: pack.id,
        votes: pack.votes,
        amount_usd: pack.usd,
        amount_inr_paise: amountPaise,
        usd_inr_rate: usdInrRate,
        currency: "INR",
        created_at: nowIso,
        updated_at: nowIso
      };
      state.orders[razorpayOrder.id] = orderRecord;
      return {
        order_snapshot: safeCloneJson(orderRecord),
        user_snapshot: safeCloneJson(user)
      };
    });

    await syncContestSnapshots({
      userLike: created?.user_snapshot,
      orderLike: created?.order_snapshot
    });

    return res.json({
      ok: true,
      order: {
        razorpay_order_id: razorpayOrder.id,
        key_id: CONTEST_RAZORPAY_KEY_ID,
        amount_paise: amountPaise,
        amount_inr: Math.round((amountPaise / 100) * 100) / 100,
        currency: "INR",
        votes: pack.votes,
        contest_id: contest.id,
        side_id: side.id,
        pack_id: pack.id
      }
    });
  }catch(err){
    console.error("contest_order_create_error:", err?.stack || err);
    return res.status(500).json({
      ok:false,
      error:"contest_order_create_failed",
      message:String(err?.message || "Unable to create order.")
    });
  }
});

app.post("/api/contest/order/verify", async (req, res) => {
  const orderId = String(req.body?.razorpay_order_id || "").trim();
  const paymentId = String(req.body?.razorpay_payment_id || "").trim();
  const signature = String(req.body?.razorpay_signature || "").trim();
  const userId = sanitizeUserId(req.body?.user_id);
  const userName = sanitizeDisplayName(req.body?.user_name);

  if(!isContestRazorpayConfigured()){
    return res.status(503).json({
      ok:false,
      error:"razorpay_not_configured",
      issue: getContestRazorpayConfigIssue(),
      mode: getContestRazorpayMode(),
      message:getContestRazorpaySetupMessage()
    });
  }

  if(!orderId || !paymentId || !signature){
    return res.status(400).json({ ok:false, error:"payment_fields_required" });
  }
  if(!verifyContestPaymentSignature(orderId, paymentId, signature)){
    return res.status(400).json({ ok:false, error:"invalid_payment_signature" });
  }

  try{
    const paymentSnapshot = await fetchRazorpayPaymentSnapshot(paymentId);
    if(String(paymentSnapshot?.order_id || "").trim() !== orderId){
      return res.status(400).json({ ok:false, error:"order_payment_mismatch" });
    }
    const paymentStatus = String(paymentSnapshot?.status || "").trim().toLowerCase();
    const paymentCurrency = String(paymentSnapshot?.currency || "").trim().toUpperCase();
    const paymentAmountPaise = Math.round(safeNumber(paymentSnapshot?.amount));
    const paymentCaptured = Boolean(paymentSnapshot?.captured) || paymentStatus === "captured";
    const noteContestId = sanitizeToken(paymentSnapshot?.notes?.contest_id, 40).toLowerCase();
    const noteSideId = sanitizeToken(paymentSnapshot?.notes?.side_id, 40).toLowerCase();
    const notePackId = sanitizeToken(paymentSnapshot?.notes?.pack_id, 40).toLowerCase();
    const noteUserId = sanitizeUserId(paymentSnapshot?.notes?.user_id || userId);

    if(!paymentStatus || !isRazorpayPaymentStatusAcceptable(paymentStatus) || !paymentCaptured){
      return res.status(409).json({
        ok:false,
        error:"invalid_payment_status",
        payment_status: paymentStatus
      });
    }
    if(paymentCurrency && paymentCurrency !== "INR"){
      return res.status(400).json({
        ok:false,
        error:"payment_currency_mismatch",
        payment_currency: paymentCurrency
      });
    }
    if(paymentAmountPaise < 100){
      return res.status(400).json({
        ok:false,
        error:"invalid_payment_amount",
        payment_amount_paise: paymentAmountPaise
      });
    }

    const result = await mutateContestState(state => {
      let order = state.orders[orderId];
      if(!order){
        const fallbackContest = getContestById(noteContestId);
        const fallbackPack = getContestPackById(notePackId);
        const fallbackSide = fallbackContest?.sides?.find(item => item.id === noteSideId) || null;
        const fallbackUserId = sanitizeUserId(noteUserId || userId);
        if(!fallbackContest || !fallbackPack || !fallbackSide || !canContestUserPay(fallbackUserId)){
          return { error:"order_not_found" };
        }
        const nowIso = new Date().toISOString();
        order = {
          razorpay_order_id: orderId,
          receipt: String(paymentSnapshot?.notes?.app_order_id || "").trim(),
          status: "created",
          user_id: fallbackUserId,
          contest_id: fallbackContest.id,
          side_id: fallbackSide.id,
          pack_id: fallbackPack.id,
          votes: fallbackPack.votes,
          amount_usd: fallbackPack.usd,
          amount_inr_paise: paymentAmountPaise,
          usd_inr_rate: 0,
          currency: "INR",
          created_at: nowIso,
          updated_at: nowIso
        };
        state.orders[orderId] = order;
      }
      const expectedCurrency = String(order.currency || "INR").trim().toUpperCase() || "INR";
      if(paymentCurrency && expectedCurrency && paymentCurrency !== expectedCurrency){
        return {
          error:"payment_currency_mismatch",
          payment_currency: paymentCurrency,
          expected_currency: expectedCurrency
        };
      }
      const expectedAmountPaise = Math.round(safeNumber(order.amount_inr_paise));
      if(expectedAmountPaise > 0 && paymentAmountPaise !== expectedAmountPaise){
        return {
          error:"payment_amount_mismatch",
          payment_amount_paise: paymentAmountPaise,
          expected_amount_paise: expectedAmountPaise
        };
      }

      const payerId = sanitizeUserId(order.user_id || noteUserId || userId);
      if(!canContestUserPay(payerId)){
        return { error:"order_user_invalid" };
      }
      if(order.user_id && userId && order.user_id !== userId){
        return { error:"order_user_mismatch" };
      }

      let payerSnapshot = null;
      if(order.status !== "paid"){
        order.status = "paid";
        order.payment_id = paymentId;
        order.signature = signature;
        order.paid_at = new Date().toISOString();
        order.updated_at = new Date().toISOString();

        ensureContestVoteBuckets(state);
        if(!state.votes[order.contest_id]){
          state.votes[order.contest_id] = {};
        }
        state.votes[order.contest_id][order.side_id] =
          safeNumber(state.votes[order.contest_id][order.side_id]) + safeNumber(order.votes);

        if(payerId){
          const user = ensureContestUser(state, payerId, userName);
          user.paid_votes += safeNumber(order.votes);
          user.contest_votes[order.contest_id] =
            safeNumber(user.contest_votes[order.contest_id]) + safeNumber(order.votes);
          payerSnapshot = safeCloneJson(user);
        }
      }else if(payerId){
        const user = ensureContestUser(state, payerId, userName);
        payerSnapshot = safeCloneJson(user);
      }

      const voteSnapshot = {
        contest_id: order.contest_id,
        side_id: order.side_id,
        votes: safeNumber(state.votes?.[order.contest_id]?.[order.side_id])
      };

      return {
        ok: true,
        order_snapshot: safeCloneJson(order),
        user_snapshot: payerSnapshot,
        vote_snapshot: voteSnapshot,
        data: buildContestPayload(state, payerId || userId, userName)
      };
    });

    if(result?.error === "order_not_found"){
      return res.status(404).json({ ok:false, error:"order_not_found" });
    }
    if(result?.error === "order_user_mismatch"){
      return res.status(403).json({ ok:false, error:"order_user_mismatch" });
    }
    if(result?.error === "order_user_invalid"){
      return res.status(403).json({
        ok:false,
        error:"login_required",
        message:"Order is not linked to a valid logged-in user."
      });
    }
    if(result?.error === "payment_currency_mismatch"){
      return res.status(400).json({
        ok:false,
        error:"payment_currency_mismatch",
        payment_currency: result.payment_currency || paymentCurrency || "",
        expected_currency: result.expected_currency || "INR"
      });
    }
    if(result?.error === "payment_amount_mismatch"){
      return res.status(400).json({
        ok:false,
        error:"payment_amount_mismatch",
        payment_amount_paise: result.payment_amount_paise || paymentAmountPaise,
        expected_amount_paise: result.expected_amount_paise || 0
      });
    }

    await syncContestSnapshots({
      userLike: result?.user_snapshot || result?.data?.user_stats,
      orderLike: result?.order_snapshot,
      voteLike: result?.vote_snapshot
    });

    return res.json({
      ...result,
      payment_status: String(paymentSnapshot?.status || "").trim().toLowerCase() || "verified",
      payment_currency: paymentCurrency || "INR",
      payment_amount_paise: paymentAmountPaise
    });
  }catch(err){
    console.error("contest_order_verify_error:", err?.stack || err);
    return res.status(500).json({
      ok:false,
      error:"contest_order_verify_failed",
      message:String(err?.message || "Unable to verify payment.")
    });
  }
});

// Kept as no-op to avoid breaking existing publish/edit flows that still call this.
app.options("/products/cache", (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.sendStatus(204);
});

app.post("/products/cache", async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  return res.json({ ok: true, caching: false, message: "Product caching disabled." });
});

app.get("/api/offline/video", async (req, res) => {
  const sourceUrl = parseSafeOfflineVideoUrl(req.query?.url);
  if(!sourceUrl){
    return res.status(400).json({ ok:false, error:"invalid_url" });
  }
  try{
    const upstream = await fetch(sourceUrl, {
      method: "GET",
      headers: {
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "Accept":"video/*,*/*;q=0.8"
      }
    });
    if(!upstream.ok){
      return res.status(upstream.status).json({
        ok:false,
        error:"upstream_fetch_failed",
        status: upstream.status
      });
    }
    const contentType = String(upstream.headers.get("content-type") || "video/mp4").trim() || "video/mp4";
    const contentLengthRaw = String(upstream.headers.get("content-length") || "").trim();
    const contentLength = Number(contentLengthRaw);
    const buffer = Buffer.from(await upstream.arrayBuffer());
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Content-Type", contentType);
    if(Number.isFinite(contentLength) && contentLength > 0){
      res.setHeader("Content-Length", String(contentLength));
    }else{
      res.setHeader("Content-Length", String(buffer.length));
    }
    res.setHeader("Cache-Control", "no-store");
    return res.status(200).send(buffer);
  }catch(err){
    console.error("offline_video_proxy_error:", err?.stack || err);
    return res.status(500).json({ ok:false, error:"offline_video_proxy_failed" });
  }
});

const TRYON_LANE_CONFIG = {
  standard: { concurrency: 1 },
  priority: { concurrency: 2 }
};
const tryonQueueByLane = {
  standard: [],
  priority: []
};
const tryonActiveWorkers = {
  standard: 0,
  priority: 0
};
const tryonJobStore = new Map();
const TRYON_JOB_MAX = 1200;

function trimTryonJobStore(){
  if(tryonJobStore.size <= TRYON_JOB_MAX){
    return;
  }
  const jobs = Array.from(tryonJobStore.values())
    .sort((a, b) => new Date(a?.created_at || 0).getTime() - new Date(b?.created_at || 0).getTime());
  const removeCount = tryonJobStore.size - TRYON_JOB_MAX;
  for(let i = 0; i < removeCount; i += 1){
    const id = jobs[i]?.id;
    if(id) tryonJobStore.delete(id);
  }
}

function getTryonLane(planCode){
  const cfg = getPlanConfig(planCode);
  return cfg.tryon_lane === "priority" ? "priority" : "standard";
}

function getTryonDateKey(){
  const now = new Date();
  return `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, "0")}-${String(now.getUTCDate()).padStart(2, "0")}`;
}

function buildTryonAbuseKey(req, userId){
  const forwarded = String(req.headers["x-forwarded-for"] || "").split(",")[0].trim();
  const ip = forwarded || String(req.ip || "").trim();
  const ua = String(req.headers["user-agent"] || "").trim();
  const seed = `${sanitizeUserId(userId) || "guest"}|${ip}|${ua}`;
  return crypto.createHash("sha1").update(seed).digest("hex").slice(0, 24);
}

function detectTryonAbuse(req, userId){
  const store = app.locals.tryonAbuse || (app.locals.tryonAbuse = new Map());
  const key = buildTryonAbuseKey(req, userId);
  const now = Date.now();
  const bucket = store.get(key) || { stamps: [], blocked_until: 0 };
  bucket.stamps = (bucket.stamps || []).filter(ts => now - ts < 60 * 1000);
  if(bucket.blocked_until && bucket.blocked_until > now){
    store.set(key, bucket);
    return {
      blocked: true,
      retry_after_seconds: Math.max(1, Math.ceil((bucket.blocked_until - now) / 1000)),
      reason: "rate_abuse"
    };
  }
  bucket.stamps.push(now);
  if(bucket.stamps.length > 18){
    bucket.blocked_until = now + 10 * 60 * 1000;
  }
  store.set(key, bucket);
  return {
    blocked: bucket.blocked_until > now,
    retry_after_seconds: bucket.blocked_until > now
      ? Math.max(1, Math.ceil((bucket.blocked_until - now) / 1000))
      : 0,
    reason: bucket.blocked_until > now ? "rate_abuse" : ""
  };
}

async function resolveTryonGate(userIdInput, requestedPlanInput){
  const userId = sanitizeUserId(userIdInput);
  if(!userId || userId === "guest"){
    return buildFeatureGateForSubscription(buildFreeSubscription("guest", "guest"));
  }
  return mutateSystemState(state => {
    ensureSubscriptionExpirySweep(state);
    const sub = getEffectiveSubscription(state, userId);
    return buildFeatureGateForSubscription(sub);
  });
}

async function processTryonJob(job){
  const planCode = normalizePlanCode(job?.plan);
  const payload = job?.payload || {};
  const userImage = payload.userImageBuffer || null;
  const userImageMime = payload.userImageMime || "image/jpeg";
  const productInfo = payload.productInfo || {};

  const manualPayload = {
    userId: payload.userId,
    username: payload.username,
    userEmail: payload.userEmail,
    plan: planCode,
    language: payload.language,
    userImageBuffer: userImage,
    userImageMime,
    productInfo
  };

  if(!userImage || !userImage.length){
    return manualProcessingResponse(planCode);
  }

  const productImageUrl = String(productInfo.image || payload.product_url || "").trim();
  const productFetch = await fetchImageBuffer(productImageUrl);
  if(!productFetch.ok || !productFetch.buffer || !productFetch.buffer.length){
    const saved = await saveManualRequest(manualPayload);
    return manualProcessingResponse(planCode, { request_id: saved?.request_id || "" });
  }

  if(!FormDataCtor){
    const saved = await saveManualRequest(manualPayload);
    return manualProcessingResponse(planCode, { request_id: saved?.request_id || "" });
  }

  const form = new FormDataCtor();
  form.append("model", "gpt-image-1.5");
  const productMime = productFetch.contentType || "image/jpeg";
  if(FormDataCtor === global.FormData && BlobCtor){
    form.append("image[]", new BlobCtor([userImage], { type: userImageMime }), "user.jpg");
    form.append("image[]", new BlobCtor([productFetch.buffer], { type: productMime }), "product.jpg");
  }else{
    form.append("image[]", userImage, "user.jpg");
    form.append("image[]", productFetch.buffer, "product.jpg");
  }
  form.append(
    "prompt",
    "Make the person wear the product naturally. Perfect fit, realistic folds, lighting and shadows. Do not change background. Photorealistic."
  );

  const headers = { Authorization: `Bearer ${OPENAI_API_KEY}` };
  if(typeof form.getHeaders === "function"){
    Object.assign(headers, form.getHeaders());
  }

  const response = await fetch("https://api.openai.com/v1/images/edits", {
    method: "POST",
    headers,
    body: form
  });

  const raw = await response.text();
  let data = null;
  try{
    data = raw ? JSON.parse(raw) : null;
  }catch(_){
    data = { raw };
  }

  if(!response.ok){
    const saved = await saveManualRequest(manualPayload);
    return manualProcessingResponse(planCode, { request_id: saved?.request_id || "" });
  }

  const imageBase64 = data?.data?.[0]?.b64_json || "";
  const image = imageBase64 ? `data:image/png;base64,${imageBase64}` : "";
  if(!image){
    const saved = await saveManualRequest(manualPayload);
    return manualProcessingResponse(planCode, { request_id: saved?.request_id || "" });
  }

  return {
    status: "success",
    image,
    plan: planCode,
    download_allowed: true
  };
}

function scheduleTryonWorkers(lane){
  const laneName = lane === "priority" ? "priority" : "standard";
  const cfg = TRYON_LANE_CONFIG[laneName] || TRYON_LANE_CONFIG.standard;
  while(
    tryonActiveWorkers[laneName] < Math.max(1, Math.floor(cfg.concurrency || 1)) &&
    tryonQueueByLane[laneName].length
  ){
    runTryonWorker(laneName).catch(err => {
      console.error("tryon_worker_error:", err?.stack || err);
    });
  }
}

async function runTryonWorker(lane){
  const laneName = lane === "priority" ? "priority" : "standard";
  if(!tryonQueueByLane[laneName].length){
    return;
  }
  tryonActiveWorkers[laneName] += 1;
  try{
    while(tryonQueueByLane[laneName].length){
      const jobId = tryonQueueByLane[laneName].shift();
      const job = tryonJobStore.get(jobId);
      if(!job) continue;

      job.status = "processing";
      job.started_at = new Date().toISOString();
      try{
        const result = await processTryonJob(job);
        job.result = result;
        job.status = "completed";
        job.completed_at = new Date().toISOString();
        await mutateSystemState(state => {
          state.tryon.lane_metrics.completed += 1;
          return true;
        });
      }catch(err){
        console.error("tryon_job_process_error:", err?.stack || err);
        job.status = "failed";
        job.error = String(err?.message || "tryon_failed");
        job.completed_at = new Date().toISOString();
        await mutateSystemState(state => {
          state.tryon.lane_metrics.failed += 1;
          pushBounded(state.automation.notifications, {
            id: `tryon_fail_${job.id}`,
            type: "tryon_failed",
            user_id: job.user_id,
            lane: laneName,
            created_at: new Date().toISOString(),
            payload: {
              job_id: job.id,
              error: job.error
            }
          }, 6000);
          return true;
        });
      }
      trimTryonJobStore();
    }
  }finally{
    tryonActiveWorkers[laneName] = Math.max(0, tryonActiveWorkers[laneName] - 1);
    if(tryonQueueByLane[laneName].length){
      setImmediate(() => scheduleTryonWorkers(laneName));
    }
  }
}

function enqueueTryonJob(job){
  const lane = job.lane === "priority" ? "priority" : "standard";
  tryonJobStore.set(job.id, job);
  tryonQueueByLane[lane].push(job.id);
  trimTryonJobStore();
  scheduleTryonWorkers(lane);
  return tryonQueueByLane[lane].length;
}

app.options("/tryon", (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.sendStatus(204);
});

app.get("/api/tryon/jobs/:jobId", async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  const jobId = String(req.params?.jobId || "").trim();
  if(!jobId){
    return res.status(400).json({ ok:false, error:"job_id_required" });
  }
  const job = tryonJobStore.get(jobId);
  if(!job){
    return res.status(404).json({ ok:false, error:"job_not_found" });
  }
  if(job.status === "completed"){
    return res.json({
      ok:true,
      job_id: job.id,
      lane: job.lane,
      status: "completed",
      result: job.result || null
    });
  }
  if(job.status === "failed"){
    return res.json({
      ok:true,
      job_id: job.id,
      lane: job.lane,
      status: "failed",
      error: job.error || "tryon_failed"
    });
  }
  const queue = tryonQueueByLane[job.lane === "priority" ? "priority" : "standard"] || [];
  const position = job.status === "processing"
    ? 0
    : Math.max(1, queue.findIndex(id => id === job.id) + 1);
  return res.json({
    ok:true,
    job_id: job.id,
    lane: job.lane,
    status: job.status,
    position,
    poll_after_ms: 1500
  });
});

app.get("/api/tryon/metrics", async (req, res) => {
  try{
    const state = await readSystemState();
    ensureSubscriptionExpirySweep(state);
    return res.json({
      ok:true,
      queues: {
        standard: tryonQueueByLane.standard.length,
        priority: tryonQueueByLane.priority.length
      },
      workers: {
        standard: tryonActiveWorkers.standard,
        priority: tryonActiveWorkers.priority
      },
      metrics: state.tryon?.lane_metrics || {}
    });
  }catch(err){
    return res.status(500).json({ ok:false, error:"tryon_metrics_failed" });
  }
});

app.post("/tryon", upload.single("userImage"), async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");

  const userId = String(req.body?.user_id || "guest").trim() || "guest";
  const username = String(req.body?.username || req.body?.user_name || "guest").trim() || "guest";
  const userEmail = String(req.body?.user_email || "").trim();
  const requestedPlanCode = normalizePlanCode(req.body?.user_plan);
  const language = String(req.body?.user_language || "en").trim() || "en";
  const userImage = req.file?.buffer || null;
  const userImageMime = req.file?.mimetype || "image/jpeg";

  let productInfo = {};
  if(req.body?.product_info){
    try{
      const parsed = JSON.parse(String(req.body.product_info));
      if(parsed && typeof parsed === "object") productInfo = parsed;
    }catch(_){ }
  }
  if(!productInfo.id && req.body?.product_id) productInfo.id = String(req.body.product_id);

  try{
    const gate = await resolveTryonGate(userId, requestedPlanCode);
    const planCode = normalizePlanCode(gate?.plan || requestedPlanCode);
    const lane = getTryonLane(planCode);

    if(userId && userId !== "guest"){
      await mutateSystemState(state => {
        const current = state.identities[userId] || {};
        const derived = deriveIdentityDefaults({
          user_id: userId,
          email: userEmail,
          username,
          display_name: username
        });
        state.identities[userId] = {
          ...current,
          ...derived,
          user_id: userId,
          created_at: current.created_at || new Date().toISOString(),
          updated_at: new Date().toISOString()
        };
        return true;
      });
    }

    const abuse = detectTryonAbuse(req, userId);
    if(abuse.blocked){
      return res.status(429).json({
        status: "blocked",
        error: "abuse_detected",
        retry_after_seconds: abuse.retry_after_seconds,
        plan: planCode
      });
    }

    const usageStore = app.locals.tryonUsage || (app.locals.tryonUsage = new Map());
    const dateKey = getTryonDateKey();
    const usageKey = `${sanitizeUserId(userId) || "guest"}::${dateKey}`;
    const used = usageStore.get(usageKey) || 0;
    const limit = gate?.tryon_daily_limit === Infinity
      ? Infinity
      : Math.max(0, Math.floor(safeNumber(gate?.tryon_daily_limit) || getPlanDailyLimit(planCode)));

    if(limit !== Infinity && used >= limit){
      return res.json({
        status: "limit_reached",
        plan: planCode,
        limit,
        upgrade_prompt: "Daily limit reached. Upgrade your subscription to continue."
      });
    }

    if(!userImage || !userImage.length){
      return res.json(manualProcessingResponse(planCode));
    }

    if(limit !== Infinity){
      usageStore.set(usageKey, used + 1);
    }

    const jobId = typeof crypto.randomUUID === "function"
      ? crypto.randomUUID()
      : `${Date.now()}_${crypto.randomBytes(4).toString("hex")}`;
    const job = {
      id: jobId,
      user_id: sanitizeUserId(userId) || "guest",
      lane,
      plan: planCode,
      status: "queued",
      created_at: new Date().toISOString(),
      payload: {
        userId,
        username,
        userEmail,
        language,
        userImageBuffer: userImage,
        userImageMime,
        productInfo,
        product_url: String(req.body?.product_url || "").trim()
      }
    };
    const position = enqueueTryonJob(job);
    await mutateSystemState(state => {
      if(lane === "priority"){
        state.tryon.lane_metrics.priority_queued += 1;
      }else{
        state.tryon.lane_metrics.standard_queued += 1;
      }
      pushBounded(state.automation.funnel_events, {
        id: `tryon_queue_${jobId}`,
        user_id: job.user_id,
        step: "ai_tryon",
        month: parseMonthlyKey(),
        at: new Date().toISOString(),
        meta: {
          lane,
          plan: planCode
        }
      }, 8000);
      return true;
    });

    return res.json({
      status: "queued",
      job_id: jobId,
      lane,
      position,
      plan: planCode,
      poll_after_ms: 1500,
      poll_url: `/api/tryon/jobs/${encodeURIComponent(jobId)}`
    });

  } catch (err) {
    console.error("Try-on error:", err?.stack || err);
    return res.json(manualProcessingResponse(requestedPlanCode));
  }
});

app.use((err, req, res, next) => {
  if (req.path === "/tryon") {
    console.error("Try-on middleware error:", err?.stack || err);
    res.setHeader("Access-Control-Allow-Origin", "*");
    return res.json(manualProcessingResponse(req.body?.user_plan));
  }
  return next(err);
});

const systemMaintenanceTimer = setInterval(() => {
  mutateSystemState(state => {
    ensureSubscriptionExpirySweep(state);
    const due = runFollowupSweep(state);
    due.forEach(item => {
      pushBounded(state.automation.notifications, {
        id: `fu_ready_${item.id}`,
        type: "followup_ready",
        user_id: sanitizeUserId(item.user_id),
        created_at: new Date().toISOString(),
        payload: item
      }, 6000);
    });
    return true;
  }).catch(err => {
    console.error("system_maintenance_error:", err?.stack || err);
  });
}, 60 * 1000);
if(systemMaintenanceTimer && typeof systemMaintenanceTimer.unref === "function"){
  systemMaintenanceTimer.unref();
}

function startServer(){
  app.listen(PORT, "0.0.0.0", () => {
    console.log(`Server running on port ${PORT}`);
  });
}

process.on("uncaughtException", (err) => {
  console.error("Uncaught exception:", err?.stack || err);
});

process.on("unhandledRejection", (reason) => {
  console.error("Unhandled rejection:", reason);
});

startServer();
