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

function isLocalServicesSupabaseConfigured(){
  return Boolean(getSocialSupabaseBase() && getSocialSupabaseServiceRoleKey());
}

async function localServicesSupabaseRequest(tableName, method, options){
  if(!isLocalServicesSupabaseConfigured()){
    throw new Error("local_services_supabase_unconfigured");
  }
  const table = String(tableName || "").trim();
  if(!table){
    throw new Error("local_services_table_required");
  }
  const base = getSocialSupabaseBase();
  const key = getSocialSupabaseServiceRoleKey();
  const reqMethod = String(method || "GET").trim().toUpperCase();
  const query = options?.query && typeof options.query === "object" ? options.query : {};
  const url = new URL(`/rest/v1/${table}`, base + "/");
  Object.keys(query).forEach((k) => {
    const val = query[k];
    if(val === undefined || val === null || val === "") return;
    url.searchParams.set(k, String(val));
  });
  const headers = {
    apikey: key,
    Authorization: `Bearer ${key}`
  };
  let body = null;
  if(reqMethod === "POST" || reqMethod === "PATCH"){
    headers["Content-Type"] = "application/json";
    headers.Prefer = options?.prefer || (reqMethod === "POST" ? "return=representation" : "return=representation");
    body = JSON.stringify(options?.body ?? {});
  }
  const response = await fetch(url.toString(), {
    method: reqMethod,
    headers,
    body
  });
  if(!response.ok){
    const text = await response.text().catch(() => "");
    throw new Error(`local_services_supabase_failed_${table}_${response.status}:${text.slice(0, 220)}`);
  }
  if(reqMethod === "DELETE"){
    return [];
  }
  return await response.json().catch(() => []);
}

function sanitizeListingType(value){
  const raw = String(value || "").trim().toLowerCase();
  if(raw === "food" || raw === "grocery" || raw === "ride") return raw;
  return "";
}

function localDistanceKm(lat1, lng1, lat2, lng2){
  const toRad = deg => (deg * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
    Math.sin(dLng / 2) * Math.sin(dLng / 2);
  return 6371 * (2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
}

function sanitizeGeoNumber(value){
  const n = Number(value);
  if(!Number.isFinite(n)) return null;
  return n;
}

function parseMonthRange(monthInput){
  const now = new Date();
  const fallback = `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, "0")}`;
  const month = String(monthInput || fallback).trim();
  if(!/^\d{4}-\d{2}$/.test(month)){
    return { month: fallback, fromIso: `${fallback}-01T00:00:00.000Z`, toIso: new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + 1, 1)).toISOString() };
  }
  const year = Number(month.slice(0, 4));
  const mon = Number(month.slice(5, 7));
  const from = new Date(Date.UTC(year, mon - 1, 1));
  const to = new Date(Date.UTC(year, mon, 1));
  return { month, fromIso: from.toISOString(), toIso: to.toISOString() };
}

function sanitizeLocalRole(value){
  const raw = String(value || "").trim().toLowerCase();
  if(raw === "consumer" || raw === "seller" || raw === "rider" || raw === "agent") return raw;
  return "";
}

const LOCAL_AGENT_CATEGORIES = new Set([
  "electrician",
  "plumber",
  "ac_repair",
  "carpenter",
  "mechanic",
  "painter",
  "cleaning"
]);

function sanitizeAgentCategory(value){
  const raw = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^\w\s-]+/g, "")
    .replace(/\s+/g, "_");
  if(LOCAL_AGENT_CATEGORIES.has(raw)) return raw;
  return "";
}

function sanitizeWalletOwnerType(value){
  const raw = String(value || "").trim().toLowerCase();
  if(raw === "platform" || raw === "driver" || raw === "seller" || raw === "agent" || raw === "customer") return raw;
  return "";
}

function sanitizeLocalModule(value){
  const raw = String(value || "").trim().toLowerCase();
  if(raw === "ride" || raw === "food" || raw === "grocery" || raw === "agent" || raw === "payment" || raw === "platform") return raw;
  return "";
}

function sanitizePaymentMethod(value){
  const raw = String(value || "").trim().toLowerCase();
  if(raw === "cash" || raw === "upi" || raw === "card" || raw === "online" || raw === "wallet" || raw === "cod") return raw;
  return "cash";
}

function sanitizePaymentStatus(value){
  const raw = String(value || "").trim().toLowerCase();
  if(raw === "created" || raw === "captured" || raw === "failed" || raw === "pending" || raw === "cod" || raw === "refunded") return raw;
  return "pending";
}

function roundMoney(value){
  const n = Number(value);
  if(!Number.isFinite(n)) return 0;
  return Math.round(n * 100) / 100;
}

function weekRangeUtc(dateInput){
  const base = dateInput ? new Date(dateInput) : new Date();
  const t = Number(base.getTime());
  const ref = Number.isFinite(t) ? base : new Date();
  const dayStart = new Date(Date.UTC(ref.getUTCFullYear(), ref.getUTCMonth(), ref.getUTCDate(), 0, 0, 0, 0));
  const day = dayStart.getUTCDay();
  const shift = (day + 6) % 7; // Monday start
  const from = new Date(dayStart.getTime() - (shift * 24 * 60 * 60 * 1000));
  const to = new Date(from.getTime() + (7 * 24 * 60 * 60 * 1000));
  return {
    startDate: from.toISOString().slice(0, 10),
    endDate: to.toISOString().slice(0, 10),
    fromIso: from.toISOString(),
    toIso: to.toISOString()
  };
}

async function fetchWalletRow(ownerUserIdInput, ownerTypeInput){
  const ownerUserId = String(ownerUserIdInput || "").trim().slice(0, 80);
  const ownerType = sanitizeWalletOwnerType(ownerTypeInput);
  if(!ownerUserId || !ownerType) return null;
  const rows = await localServicesSupabaseRequest("wallets", "GET", {
    query: {
      select: "id,owner_user_id,owner_type,balance_inr,updated_at",
      owner_user_id: `eq.${ownerUserId}`,
      owner_type: `eq.${ownerType}`,
      limit: "1"
    }
  });
  return Array.isArray(rows) ? rows[0] || null : null;
}

async function ensureWalletRow(ownerUserIdInput, ownerTypeInput){
  const ownerUserId = String(ownerUserIdInput || "").trim().slice(0, 80);
  const ownerType = sanitizeWalletOwnerType(ownerTypeInput);
  if(!ownerUserId || !ownerType){
    return null;
  }
  const existing = await fetchWalletRow(ownerUserId, ownerType);
  if(existing) return existing;
  const createdRows = await localServicesSupabaseRequest("wallets", "POST", {
    body: [{
      owner_user_id: ownerUserId,
      owner_type: ownerType,
      balance_inr: 0,
      currency: "INR",
      status: "active",
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    }],
    query: { on_conflict: "owner_user_id,owner_type" },
    prefer: "resolution=merge-duplicates,return=representation"
  });
  return Array.isArray(createdRows) ? createdRows[0] || null : null;
}

async function incrementWalletBalance(ownerUserIdInput, ownerTypeInput, deltaInrInput){
  const ownerUserId = String(ownerUserIdInput || "").trim().slice(0, 80);
  const ownerType = sanitizeWalletOwnerType(ownerTypeInput);
  const deltaInr = roundMoney(deltaInrInput);
  if(!ownerUserId || !ownerType || !Number.isFinite(deltaInr) || deltaInr === 0){
    return null;
  }
  const wallet = await ensureWalletRow(ownerUserId, ownerType);
  if(!wallet?.id){
    return null;
  }
  const current = roundMoney(wallet.balance_inr || 0);
  const next = roundMoney(current + deltaInr);
  const patchedRows = await localServicesSupabaseRequest("wallets", "PATCH", {
    query: { id: `eq.${String(wallet.id || "").trim()}` },
    body: {
      balance_inr: next,
      updated_at: new Date().toISOString()
    },
    prefer: "return=representation"
  });
  return Array.isArray(patchedRows) ? patchedRows[0] || null : null;
}

async function findSettlementTransaction(moduleInput, referenceTypeInput, referenceIdInput){
  const module = sanitizeLocalModule(moduleInput);
  const referenceType = String(referenceTypeInput || "").trim().toLowerCase().slice(0, 40);
  const referenceId = String(referenceIdInput || "").trim().slice(0, 120);
  if(!module || !referenceType || !referenceId) return null;
  const rows = await localServicesSupabaseRequest("transactions", "GET", {
    query: {
      select: "id,module,transaction_type,reference_type,reference_id,status",
      module: `eq.${module}`,
      transaction_type: "eq.settlement",
      reference_type: `eq.${referenceType}`,
      reference_id: `eq.${referenceId}`,
      limit: "1"
    }
  });
  return Array.isArray(rows) ? rows[0] || null : null;
}

async function upsertWeeklyPayoutLedger(input){
  const ownerUserId = String(input?.ownerUserId || "").trim().slice(0, 80);
  const ownerType = sanitizeWalletOwnerType(input?.ownerType);
  const module = sanitizeLocalModule(input?.module);
  if(!ownerUserId || !ownerType || !module) return null;
  const gross = roundMoney(input?.grossInr || 0);
  const commission = roundMoney(input?.commissionInr || 0);
  const payout = roundMoney(input?.payoutInr || 0);
  if(gross <= 0 && commission <= 0 && payout <= 0) return null;

  const week = weekRangeUtc(input?.at || new Date());
  const existingRows = await localServicesSupabaseRequest("weekly_payouts", "GET", {
    query: {
      select: "id,gross_inr,commission_inr,payout_inr",
      owner_user_id: `eq.${ownerUserId}`,
      owner_type: `eq.${ownerType}`,
      module: `eq.${module}`,
      week_start_date: `eq.${week.startDate}`,
      limit: "1"
    }
  });
  const existing = Array.isArray(existingRows) ? existingRows[0] || null : null;
  if(existing?.id){
    const nextGross = roundMoney((existing.gross_inr || 0) + gross);
    const nextCommission = roundMoney((existing.commission_inr || 0) + commission);
    const nextPayout = roundMoney((existing.payout_inr || 0) + payout);
    const patched = await localServicesSupabaseRequest("weekly_payouts", "PATCH", {
      query: { id: `eq.${String(existing.id || "").trim()}` },
      body: {
        gross_inr: nextGross,
        commission_inr: nextCommission,
        payout_inr: nextPayout,
        updated_at: new Date().toISOString()
      },
      prefer: "return=representation"
    });
    return Array.isArray(patched) ? patched[0] || null : null;
  }
  const createdRows = await localServicesSupabaseRequest("weekly_payouts", "POST", {
    body: [{
      owner_user_id: ownerUserId,
      owner_type: ownerType,
      module,
      week_start_date: week.startDate,
      week_end_date: week.endDate,
      gross_inr: gross,
      commission_inr: commission,
      payout_inr: payout,
      status: "pending",
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    }],
    query: { on_conflict: "owner_user_id,owner_type,module,week_start_date" },
    prefer: "resolution=merge-duplicates,return=representation"
  });
  return Array.isArray(createdRows) ? createdRows[0] || null : null;
}

async function recordLocalSettlement(input){
  const module = sanitizeLocalModule(input?.module);
  const referenceType = String(input?.referenceType || "").trim().toLowerCase().slice(0, 40);
  const referenceId = String(input?.referenceId || "").trim().slice(0, 120);
  const payeeUserId = sanitizeUserId(input?.payeeUserId);
  const payerUserId = sanitizeUserId(input?.payerUserId);
  const payeeOwnerType = sanitizeWalletOwnerType(input?.payeeOwnerType);
  if(!module || !referenceType || !referenceId || !payeeUserId || !payeeOwnerType){
    return { ok:false, error:"invalid_settlement_payload" };
  }

  const existing = await findSettlementTransaction(module, referenceType, referenceId);
  if(existing){
    return { ok:true, skipped:true, transaction: existing };
  }

  const grossInr = Math.max(0, roundMoney(input?.grossInr || 0));
  const commissionPercent = Math.max(0, Math.min(100, Number(input?.commissionPercent || 0)));
  const commissionInr = roundMoney((grossInr * commissionPercent) / 100);
  const netInr = roundMoney(grossInr - commissionInr);
  const paymentMethod = sanitizePaymentMethod(input?.paymentMethod);
  const paymentStatus = sanitizePaymentStatus(input?.paymentStatus || (paymentMethod === "cash" ? "cod" : "captured"));
  const paymentGateway = String(input?.paymentGateway || (paymentMethod === "cash" ? "offline" : "razorpay")).trim().slice(0, 40);
  const paymentOrderId = String(input?.paymentOrderId || "").trim().slice(0, 120);
  const paymentId = String(input?.paymentId || "").trim().slice(0, 120);
  const paymentRef = String(input?.paymentRef || "").trim().slice(0, 120);
  const metadata = input?.metadata && typeof input.metadata === "object" ? input.metadata : {};

  const [payeeWallet, platformWallet] = await Promise.all([
    netInr > 0 ? incrementWalletBalance(payeeUserId, payeeOwnerType, netInr) : Promise.resolve(null),
    commissionInr > 0 ? incrementWalletBalance("platform", "platform", commissionInr) : Promise.resolve(null)
  ]);

  const createdRows = await localServicesSupabaseRequest("transactions", "POST", {
    body: [{
      owner_user_id: payeeUserId,
      owner_type: payeeOwnerType,
      module,
      transaction_type: "settlement",
      reference_type: referenceType,
      reference_id: referenceId,
      payer_user_id: payerUserId || null,
      payee_user_id: payeeUserId,
      amount_inr: grossInr,
      gross_inr: grossInr,
      commission_inr: commissionInr,
      net_inr: netInr,
      platform_share_inr: commissionInr,
      payment_method: paymentMethod,
      payment_status: paymentStatus,
      payment_gateway: paymentGateway || null,
      payment_order_id: paymentOrderId || null,
      payment_id: paymentId || null,
      payment_ref: paymentRef || null,
      metadata,
      status: "completed",
      settled_at: new Date().toISOString(),
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    }],
    query: { on_conflict: "module,transaction_type,reference_type,reference_id" },
    prefer: "resolution=merge-duplicates,return=representation"
  });

  await Promise.allSettled([
    upsertWeeklyPayoutLedger({
      ownerUserId: payeeUserId,
      ownerType: payeeOwnerType,
      module,
      grossInr,
      commissionInr,
      payoutInr: netInr,
      at: new Date()
    }),
    upsertWeeklyPayoutLedger({
      ownerUserId: "platform",
      ownerType: "platform",
      module: "platform",
      grossInr: commissionInr,
      commissionInr: 0,
      payoutInr: commissionInr,
      at: new Date()
    })
  ]);

  return {
    ok:true,
    transaction: Array.isArray(createdRows) ? createdRows[0] || null : null,
    payee_wallet: payeeWallet,
    platform_wallet: platformWallet,
    gross_inr: grossInr,
    commission_inr: commissionInr,
    net_inr: netInr
  };
}

const localRateLimitStore = new Map();
function localRateLimitHit(key, max, windowMs){
  const now = Date.now();
  const safeMax = Math.max(1, Math.floor(safeNumber(max) || 1));
  const safeWindow = Math.max(1000, Math.floor(safeNumber(windowMs) || 60000));
  const raw = localRateLimitStore.get(key);
  const stamps = Array.isArray(raw) ? raw.filter(ts => now - ts < safeWindow) : [];
  stamps.push(now);
  localRateLimitStore.set(key, stamps);
  return stamps.length > safeMax;
}

function canTransitionStatus(current, next, flowMap){
  const from = String(current || "").trim().toLowerCase();
  const to = String(next || "").trim().toLowerCase();
  const allowed = flowMap[from];
  if(!Array.isArray(allowed)) return false;
  return allowed.includes(to);
}

async function hasActiveLocalRole(userIdInput, roleInput){
  const userId = sanitizeUserId(userIdInput);
  const role = sanitizeLocalRole(roleInput);
  if(!userId || !role) return false;
  try{
    const rows = await localServicesSupabaseRequest("local_roles", "GET", {
      query: {
        select: "id",
        user_id: `eq.${userId}`,
        role: `eq.${role}`,
        status: "eq.active",
        fee_paid: "eq.true",
        limit: "1"
      }
    });
    return Array.isArray(rows) && !!rows[0];
  }catch(_){
    return false;
  }
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

app.post("/api/local/listings/create", async (req, res) => {
  const userId = sanitizeUserId(req.body?.user_id);
  const listingType = sanitizeListingType(req.body?.listing_type);
  const storeName = String(req.body?.store_name || "").trim().slice(0, 120);
  const phone = String(req.body?.phone || "").trim().slice(0, 30);
  const imageUrl = String(req.body?.image_url || "").trim().slice(0, 3000);
  const deliveryChargeInr = Math.max(0, roundMoney(req.body?.delivery_charge_inr || 0));
  const minimumOrderInr = Math.max(0, roundMoney(req.body?.minimum_order_inr || 0));
  const openTime = String(req.body?.open_time || "").trim().slice(0, 20);
  const closeTime = String(req.body?.close_time || "").trim().slice(0, 20);
  const selfDelivery = req.body?.self_delivery === undefined ? true : Boolean(req.body?.self_delivery);
  const rideVehicleType = sanitizeRideVehicleType(req.body?.ride_vehicle_type || req.body?.vehicle_type);
  const vehicleNumber = String(req.body?.vehicle_number || "").trim().slice(0, 40);
  const baseFareInr = Math.max(0, roundMoney(req.body?.base_fare_inr || 0));
  const perKmRateInr = Math.max(0, roundMoney(req.body?.per_km_rate_inr || 0));
  const perMinRateInr = Math.max(0, roundMoney(req.body?.per_min_rate_inr || 0));
  const serviceRadiusKm = Math.max(1, Math.min(50, Math.round(safeNumber(req.body?.service_radius_km || 5))));
  const documentsUrl = String(req.body?.documents_url || "").trim().slice(0, 3000);
  const openNow = req.body?.open_now === undefined ? true : Boolean(req.body?.open_now);
  const lat = sanitizeGeoNumber(req.body?.lat);
  const lng = sanitizeGeoNumber(req.body?.lng);
  if(!userId || !listingType || !storeName || !phone || lat === null || lng === null){
    return res.status(400).json({ ok:false, error:"invalid_listing_payload" });
  }
  if(localRateLimitHit(`local_listing_create:${userId}`, 12, 10 * 60 * 1000)){
    return res.status(429).json({ ok:false, error:"rate_limited" });
  }
  try{
    if(["food","grocery"].includes(listingType)){
      const allowed = await hasActiveLocalRole(userId, "seller");
      if(!allowed) return res.status(403).json({ ok:false, error:"seller_role_required" });
    }else if(listingType === "ride"){
      const allowed = await hasActiveLocalRole(userId, "rider");
      if(!allowed) return res.status(403).json({ ok:false, error:"rider_role_required" });
    }
    const rows = await localServicesSupabaseRequest("local_listings", "POST", {
      body: [{
        user_id: userId,
        store_name: storeName,
        listing_type: listingType,
        phone,
        image_url: imageUrl || null,
        lat,
        lng,
        listing_fee_inr: 500,
        platform_monthly_share_percent: 5,
        status: "pending_approval",
        open_now: openNow,
        delivery_charge_inr: deliveryChargeInr,
        minimum_order_inr: minimumOrderInr,
        open_time: openTime || null,
        close_time: closeTime || null,
        self_delivery: selfDelivery,
        ride_vehicle_type: listingType === "ride" ? rideVehicleType : null,
        vehicle_number: listingType === "ride" ? (vehicleNumber || null) : null,
        base_fare_inr: listingType === "ride" ? baseFareInr : null,
        per_km_rate_inr: listingType === "ride" ? perKmRateInr : null,
        per_min_rate_inr: listingType === "ride" ? perMinRateInr : null,
        service_radius_km: listingType === "ride" ? serviceRadiusKm : null,
        documents_url: listingType === "ride" ? (documentsUrl || null) : null,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      }],
      prefer: "return=representation"
    });
    return res.json({ ok:true, listing: Array.isArray(rows) ? rows[0] || null : null });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_listing_create_failed", message:String(err?.message || "") });
  }
});

app.post("/api/local/listings/decision", async (req, res) => {
  if(!isAdminAutomationRequest(req)){
    return res.status(403).json({ ok:false, error:"forbidden" });
  }
  const listingId = String(req.body?.listing_id || "").trim();
  const decision = String(req.body?.decision || "").trim().toLowerCase();
  const nextStatus = decision === "approve" ? "approved" : (decision === "reject" ? "rejected" : "");
  const reason = String(req.body?.reason || "").trim().slice(0, 300);
  if(!listingId || !nextStatus){
    return res.status(400).json({ ok:false, error:"listing_id_and_decision_required" });
  }
  try{
    const rows = await localServicesSupabaseRequest("local_listings", "PATCH", {
      query: { id: `eq.${listingId}` },
      body: {
        status: nextStatus,
        rejection_reason: nextStatus === "rejected" ? reason : null,
        approved_at: nextStatus === "approved" ? new Date().toISOString() : null,
        updated_at: new Date().toISOString()
      },
      prefer: "return=representation"
    });
    return res.json({ ok:true, listing: Array.isArray(rows) ? rows[0] || null : null });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_listing_decision_failed", message:String(err?.message || "") });
  }
});

app.post("/api/local/listings/update", async (req, res) => {
  const userId = sanitizeUserId(req.body?.user_id);
  const listingId = String(req.body?.listing_id || "").trim();
  if(!userId || !listingId){
    return res.status(400).json({ ok:false, error:"user_id_and_listing_id_required" });
  }
  try{
    const listingRows = await localServicesSupabaseRequest("local_listings", "GET", {
      query: {
        select: "id,user_id,listing_type",
        id: `eq.${listingId}`,
        limit: "1"
      }
    });
    const listing = Array.isArray(listingRows) ? listingRows[0] || null : null;
    if(!listing || sanitizeUserId(listing.user_id) !== userId){
      return res.status(403).json({ ok:false, error:"listing_owner_required" });
    }
    const payload = {
      store_name: String(req.body?.store_name || "").trim().slice(0, 120) || undefined,
      phone: String(req.body?.phone || "").trim().slice(0, 30) || undefined,
      image_url: String(req.body?.image_url || "").trim().slice(0, 3000) || undefined,
      open_now: req.body?.open_now === undefined ? undefined : Boolean(req.body?.open_now),
      delivery_charge_inr: req.body?.delivery_charge_inr === undefined ? undefined : Math.max(0, roundMoney(req.body?.delivery_charge_inr)),
      minimum_order_inr: req.body?.minimum_order_inr === undefined ? undefined : Math.max(0, roundMoney(req.body?.minimum_order_inr)),
      open_time: req.body?.open_time === undefined ? undefined : (String(req.body?.open_time || "").trim().slice(0, 20) || null),
      close_time: req.body?.close_time === undefined ? undefined : (String(req.body?.close_time || "").trim().slice(0, 20) || null),
      self_delivery: req.body?.self_delivery === undefined ? undefined : Boolean(req.body?.self_delivery),
      ride_vehicle_type: req.body?.ride_vehicle_type === undefined ? undefined : sanitizeRideVehicleType(req.body?.ride_vehicle_type),
      vehicle_number: req.body?.vehicle_number === undefined ? undefined : (String(req.body?.vehicle_number || "").trim().slice(0, 40) || null),
      base_fare_inr: req.body?.base_fare_inr === undefined ? undefined : Math.max(0, roundMoney(req.body?.base_fare_inr)),
      per_km_rate_inr: req.body?.per_km_rate_inr === undefined ? undefined : Math.max(0, roundMoney(req.body?.per_km_rate_inr)),
      per_min_rate_inr: req.body?.per_min_rate_inr === undefined ? undefined : Math.max(0, roundMoney(req.body?.per_min_rate_inr)),
      service_radius_km: req.body?.service_radius_km === undefined ? undefined : Math.max(1, Math.min(50, Math.round(safeNumber(req.body?.service_radius_km || 5)))),
      documents_url: req.body?.documents_url === undefined ? undefined : (String(req.body?.documents_url || "").trim().slice(0, 3000) || null),
      updated_at: new Date().toISOString()
    };
    const patchedRows = await localServicesSupabaseRequest("local_listings", "PATCH", {
      query: { id: `eq.${listingId}`, user_id: `eq.${userId}` },
      body: payload,
      prefer: "return=representation"
    });
    return res.json({ ok:true, listing: Array.isArray(patchedRows) ? patchedRows[0] || null : null });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_listing_update_failed", message:String(err?.message || "") });
  }
});

app.get("/api/local/nearby", async (req, res) => {
  const type = sanitizeListingType(req.query?.type);
  const lat = sanitizeGeoNumber(req.query?.lat);
  const lng = sanitizeGeoNumber(req.query?.lng);
  const radiusKm = Math.min(50, Math.max(1, Math.round(safeNumber(req.query?.radius_km || 30))));
  const limit = Math.min(150, Math.max(1, Math.round(safeNumber(req.query?.limit || 60))));
  if(lat === null || lng === null){
    return res.status(400).json({ ok:false, error:"lat_lng_required" });
  }
  try{
    const query = {
      select: "id,user_id,store_name,listing_type,phone,image_url,lat,lng,status,open_now,delivery_charge_inr,minimum_order_inr,open_time,close_time,self_delivery,ride_vehicle_type,vehicle_number,base_fare_inr,per_km_rate_inr,per_min_rate_inr,service_radius_km,created_at,updated_at",
      status: "eq.approved",
      limit: String(Math.max(limit * 2, 80))
    };
    if(type){
      query.listing_type = `eq.${type}`;
    }
    if(type === "food" || type === "grocery" || type === "ride"){
      query.open_now = "eq.true";
    }
    const rows = await localServicesSupabaseRequest("local_listings", "GET", { query });
    const filtered = (Array.isArray(rows) ? rows : [])
      .map((row) => {
        const la = sanitizeGeoNumber(row?.lat);
        const ln = sanitizeGeoNumber(row?.lng);
        if(la === null || ln === null) return null;
        const distance_km = localDistanceKm(lat, lng, la, ln);
        return { ...row, distance_km: Math.round(distance_km * 100) / 100 };
      })
      .filter(Boolean)
      .filter(row => row.distance_km <= radiusKm)
      .sort((a, b) => a.distance_km - b.distance_km)
      .slice(0, limit);
    return res.json({
      ok:true,
      radius_km: radiusKm,
      count: filtered.length,
      listings: filtered
    });
  }catch(err){
    return res.json({ ok:true, radius_km: radiusKm, count: 0, listings: [], warning: "nearby_temporarily_unavailable" });
  }
});

app.post("/api/local/orders/create", async (req, res) => {
  const userId = sanitizeUserId(req.body?.user_id);
  const listingId = String(req.body?.listing_id || "").trim();
  const serviceTypeInput = sanitizeListingType(req.body?.service_type) || "food";
  const amountInrInput = Math.max(0, safeNumber(req.body?.amount_inr));
  const deliveryAddress = String(req.body?.delivery_address || "").trim().slice(0, 240);
  const note = String(req.body?.note || "").trim().slice(0, 300);
  const paymentMethod = sanitizePaymentMethod(req.body?.payment_method || "cash");
  const paymentStatus = sanitizePaymentStatus(req.body?.payment_status || (paymentMethod === "cash" ? "cod" : "pending"));
  const paymentId = String(req.body?.payment_id || "").trim().slice(0, 120);
  const paymentOrderId = String(req.body?.payment_order_id || "").trim().slice(0, 120);
  const paymentRef = String(req.body?.payment_ref || "").trim().slice(0, 120);
  const itemsRaw = Array.isArray(req.body?.items) ? req.body.items.slice(0, 80) : [];
  const itemSnapshot = itemsRaw
    .map((item) => ({
      item_id: String(item?.item_id || "").trim().slice(0, 120),
      name: String(item?.name || "").trim().slice(0, 120),
      qty: Math.max(1, Math.floor(safeNumber(item?.qty || 1))),
      price_inr: roundMoney(Math.max(0, safeNumber(item?.price_inr || 0))),
      image_url: String(item?.image_url || "").trim().slice(0, 3000)
    }))
    .filter((item) => item.name && item.price_inr >= 0);
  const itemTotal = roundMoney(itemSnapshot.reduce((sum, item) => sum + (item.price_inr * item.qty), 0));
  const amountInr = amountInrInput > 0 ? roundMoney(amountInrInput) : itemTotal;
  if(!userId || !listingId || !deliveryAddress){
    return res.status(400).json({ ok:false, error:"invalid_order_payload" });
  }
  if(amountInr <= 0){
    return res.status(400).json({ ok:false, error:"amount_required" });
  }
  if(localRateLimitHit(`local_order_create:${userId}`, 40, 10 * 60 * 1000)){
    return res.status(429).json({ ok:false, error:"rate_limited" });
  }
  try{
    const consumerAllowed = await hasActiveLocalRole(userId, "consumer");
    if(!consumerAllowed){
      return res.status(403).json({ ok:false, error:"consumer_role_required" });
    }
    const listingRows = await localServicesSupabaseRequest("local_listings", "GET", {
      query: {
        select: "id,user_id,store_name,listing_type,status",
        id: `eq.${listingId}`,
        limit: "1"
      }
    });
    const listing = Array.isArray(listingRows) ? listingRows[0] : null;
    if(!listing || String(listing.status || "") !== "approved"){
      return res.status(404).json({ ok:false, error:"listing_not_available" });
    }
    const listingType = sanitizeListingType(listing?.listing_type);
    if(!["food", "grocery"].includes(listingType)){
      return res.status(400).json({ ok:false, error:"only_food_or_grocery_orders_allowed" });
    }
    const serviceType = ["food", "grocery"].includes(serviceTypeInput) ? serviceTypeInput : listingType;
    if(paymentMethod !== "cash" && paymentStatus !== "captured"){
      return res.status(402).json({
        ok:false,
        error:"payment_not_verified",
        message:"Online orders require verified captured payment before create."
      });
    }
    const createdRows = await localServicesSupabaseRequest("local_orders", "POST", {
      body: [{
        buyer_user_id: userId,
        seller_user_id: sanitizeUserId(listing.user_id),
        listing_id: listingId,
        service_type: serviceType,
        amount_inr: amountInr,
        delivery_address: deliveryAddress,
        note,
        item_snapshot: itemSnapshot.length ? itemSnapshot : null,
        payment_method: paymentMethod,
        payment_status: paymentStatus,
        payment_order_id: paymentOrderId || null,
        payment_id: paymentId || null,
        payment_ref: paymentRef || null,
        commission_inr: 0,
        seller_earning_inr: 0,
        status: "placed",
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      }],
      prefer: "return=representation"
    });
    const created = Array.isArray(createdRows) ? createdRows[0] || null : null;
    if(created?.seller_user_id){
      const state = await readSystemState();
      const tokens = getPushTokensForUser(state, created.seller_user_id);
      sendFcmNotificationToTokens(tokens, {
        title: "New local order",
        body: `${String(listing.store_name || "Store")} has a new ${serviceType} order.`,
        data: { type:"local_order", order_id:String(created.id || "") }
      }).catch(() => {});
    }
    return res.json({ ok:true, order: created });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_order_create_failed", message:String(err?.message || "") });
  }
});

app.post("/api/local/orders/status", async (req, res) => {
  const orderId = String(req.body?.order_id || "").trim();
  const actorUserId = sanitizeUserId(req.body?.user_id);
  const status = String(req.body?.status || "").trim().toLowerCase();
  const allowed = new Set(["accepted", "preparing", "rejected", "out_for_delivery", "delivered", "completed", "cancelled"]);
  if(!orderId || !allowed.has(status)){
    return res.status(400).json({ ok:false, error:"invalid_order_status_payload" });
  }
  try{
    const rows = await localServicesSupabaseRequest("local_orders", "GET", {
      query: {
        select:"id,buyer_user_id,seller_user_id,status,service_type,amount_inr,payment_method,payment_status,payment_order_id,payment_id,payment_ref",
        id:`eq.${orderId}`,
        limit:"1"
      }
    });
    const order = Array.isArray(rows) ? rows[0] : null;
    if(!order){
      return res.status(404).json({ ok:false, error:"order_not_found" });
    }
    const isAdmin = isAdminAutomationRequest(req);
    if(!isAdmin && actorUserId && actorUserId !== sanitizeUserId(order.seller_user_id) && actorUserId !== sanitizeUserId(order.buyer_user_id)){
      return res.status(403).json({ ok:false, error:"order_update_forbidden" });
    }
    const orderFlow = {
      placed: ["accepted", "rejected", "cancelled"],
      accepted: ["preparing", "out_for_delivery", "cancelled"],
      preparing: ["out_for_delivery", "cancelled"],
      out_for_delivery: ["delivered", "completed", "cancelled"],
      delivered: ["completed"],
      completed: [],
      rejected: [],
      cancelled: []
    };
    if(!isAdmin && !canTransitionStatus(order.status, status, orderFlow)){
      return res.status(409).json({ ok:false, error:"invalid_order_status_transition" });
    }
    const patched = await localServicesSupabaseRequest("local_orders", "PATCH", {
      query: { id: `eq.${orderId}` },
      body: {
        status,
        delivered_at: (status === "delivered" || status === "completed") ? new Date().toISOString() : null,
        updated_at: new Date().toISOString()
      },
      prefer: "return=representation"
    });
    const patchedOrder = Array.isArray(patched) ? patched[0] || null : null;
    let settlement = null;
    if((status === "completed" || status === "delivered") && patchedOrder){
      try{
        settlement = await recordLocalSettlement({
          module: sanitizeLocalModule(patchedOrder?.service_type) || "food",
          referenceType: "order",
          referenceId: String(patchedOrder?.id || orderId),
          payerUserId: sanitizeUserId(patchedOrder?.buyer_user_id || order?.buyer_user_id),
          payeeUserId: sanitizeUserId(patchedOrder?.seller_user_id || order?.seller_user_id),
          payeeOwnerType: "seller",
          grossInr: roundMoney(patchedOrder?.amount_inr || order?.amount_inr || 0),
          commissionPercent: 10,
          paymentMethod: sanitizePaymentMethod(patchedOrder?.payment_method || order?.payment_method || "cash"),
          paymentStatus: sanitizePaymentStatus(patchedOrder?.payment_status || order?.payment_status || "cod"),
          paymentGateway: "local_services",
          paymentOrderId: String(patchedOrder?.payment_order_id || order?.payment_order_id || ""),
          paymentId: String(patchedOrder?.payment_id || order?.payment_id || ""),
          paymentRef: String(patchedOrder?.payment_ref || order?.payment_ref || patchedOrder?.id || orderId),
          metadata: {
            service_type: String(patchedOrder?.service_type || order?.service_type || "food")
          }
        });
        if(settlement?.ok && !settlement?.skipped){
          await localServicesSupabaseRequest("local_orders", "PATCH", {
            query: { id: `eq.${String(patchedOrder?.id || orderId)}` },
            body: {
              commission_inr: roundMoney(settlement.commission_inr || 0),
              seller_earning_inr: roundMoney(settlement.net_inr || 0),
              updated_at: new Date().toISOString()
            },
            prefer: "return=minimal"
          }).catch(() => {});
        }
      }catch(err){
        settlement = { ok:false, error:String(err?.message || "order_settlement_failed") };
      }
    }
    const nextOrder = patchedOrder || order;
    const buyerId = sanitizeUserId(nextOrder?.buyer_user_id || order?.buyer_user_id);
    const sellerId = sanitizeUserId(nextOrder?.seller_user_id || order?.seller_user_id);
    if(buyerId || sellerId){
      const state = await readSystemState();
      const data = {
        type: "local_order_status",
        order_id: String(nextOrder?.id || orderId),
        status: String(status || "")
      };
      const title = "Order update";
      const body = `Order is now ${status.replace(/_/g, " ")}.`;
      [buyerId, sellerId].filter(Boolean).forEach((uid) => {
        const tokens = getPushTokensForUser(state, uid);
        sendFcmNotificationToTokens(tokens, { title, body, data }).catch(() => {});
      });
    }
    return res.json({ ok:true, order: patchedOrder, settlement });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_order_status_failed", message:String(err?.message || "") });
  }
});

app.post("/api/local/rides/request", async (req, res) => {
  const riderUserId = sanitizeUserId(req.body?.user_id);
  const pickupLat = sanitizeGeoNumber(req.body?.pickup_lat);
  const pickupLng = sanitizeGeoNumber(req.body?.pickup_lng);
  const dropLat = sanitizeGeoNumber(req.body?.drop_lat);
  const dropLng = sanitizeGeoNumber(req.body?.drop_lng);
  const pickupText = String(req.body?.pickup_text || "").trim().slice(0, 160);
  const dropText = String(req.body?.drop_text || "").trim().slice(0, 160);
  const vehicleType = sanitizeRideVehicleType(req.body?.vehicle_type);
  const paymentMethod = sanitizePaymentMethod(req.body?.payment_method);
  const paymentStatus = sanitizePaymentStatus(req.body?.payment_status || (paymentMethod === "cash" ? "cod" : "pending"));
  const paymentOrderId = String(req.body?.payment_order_id || "").trim().slice(0, 120);
  const paymentId = String(req.body?.payment_id || "").trim().slice(0, 120);
  const paymentRef = String(req.body?.payment_ref || "").trim().slice(0, 120);
  if(!riderUserId || pickupLat === null || pickupLng === null || dropLat === null || dropLng === null){
    return res.status(400).json({ ok:false, error:"invalid_ride_payload" });
  }
  if(localRateLimitHit(`ride_request:${riderUserId}`, 30, 10 * 60 * 1000)){
    return res.status(429).json({ ok:false, error:"rate_limited" });
  }
  try{
    const consumerAllowed = await hasActiveLocalRole(riderUserId, "consumer");
    if(!consumerAllowed){
      return res.status(403).json({ ok:false, error:"consumer_role_required" });
    }
    const estimate = computeRideFareBreakdown({
      vehicle_type: vehicleType,
      pickup_lat: pickupLat,
      pickup_lng: pickupLng,
      drop_lat: dropLat,
      drop_lng: dropLng,
      distance_km: req.body?.distance_km,
      duration_min: req.body?.duration_min
    });
    const fareInput = Math.max(0, safeNumber(req.body?.fare_inr));
    const fareInr = fareInput > 0 ? roundMoney(fareInput) : roundMoney(estimate.fare_inr || 0);
    const distanceKm = Math.max(0, roundMoney(req.body?.distance_km || estimate.distance_km || 0));
    const durationMin = Math.max(0, roundMoney(req.body?.duration_min || estimate.duration_min || 0));
    const commissionInr = roundMoney(fareInr * 0.10);
    const driverNetInr = roundMoney(fareInr - commissionInr);
    if(paymentMethod !== "cash" && paymentStatus !== "captured"){
      return res.status(402).json({
        ok:false,
        error:"payment_not_verified",
        message:"Online ride booking requires captured payment before request."
      });
    }
    const nearbyDrivers = await fetchNearbyRideDrivers(pickupLat, pickupLng, 40);
    const offeredDrivers = nearbyDrivers.slice(0, RIDE_INITIAL_OFFER_COUNT);
    const createdRows = await localServicesSupabaseRequest("local_ride_requests", "POST", {
      body: [{
        rider_user_id: riderUserId,
        pickup_lat: pickupLat,
        pickup_lng: pickupLng,
        drop_lat: dropLat,
        drop_lng: dropLng,
        pickup_text: pickupText,
        drop_text: dropText,
        status: "searching",
        offered_driver_ids: offeredDrivers.map(item => sanitizeUserId(item.user_id)),
        vehicle_type: vehicleType,
        payment_method: paymentMethod,
        payment_status: paymentStatus,
        payment_order_id: paymentOrderId || null,
        payment_id: paymentId || null,
        payment_ref: paymentRef || null,
        fare_inr: fareInr,
        distance_km: distanceKm,
        duration_min: durationMin,
        commission_inr: commissionInr,
        driver_earning_inr: driverNetInr,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      }],
      prefer: "return=representation"
    });
    const created = Array.isArray(createdRows) ? createdRows[0] || null : null;
    const state = await readSystemState();
    const payloadData = { type:"ride_request", request_id:String(created?.id || "") };
    offeredDrivers.forEach((driver) => {
      const driverId = sanitizeUserId(driver?.user_id);
      if(!driverId) return;
      const tokens = getPushTokensForUser(state, driverId);
      sendFcmNotificationToTokens(tokens, {
        title: "New ride request",
        body: `${pickupText || "Pickup point"} to ${dropText || "drop point"}`,
        data: payloadData
      }).catch(() => {});
    });
    return res.json({
      ok:true,
      ride_request: mapRideStatusRowPublic(created),
      offered_driver_count: offeredDrivers.length,
      search_radius_km: RIDE_MATCH_RADIUS_KM
    });
  }catch(err){
    return res.status(500).json({ ok:false, error:"ride_request_failed", message:String(err?.message || "") });
  }
});

const RIDE_MATCH_RADIUS_KM = 3;
const RIDE_INITIAL_OFFER_COUNT = 5;
const RIDE_REMATCH_BATCH_COUNT = 1;
const RIDE_DRIVER_LOCATION_MAX_AGE_MS = 2 * 60 * 1000;
const RIDE_STATUS_ALIAS_TO_INTERNAL = Object.freeze({
  arrived: "arriving",
  started: "on_trip"
});
const RIDE_STATUS_INTERNAL_TO_PUBLIC = Object.freeze({
  arriving: "arrived",
  on_trip: "started"
});

function rideStatusToInternal(statusRaw){
  const clean = String(statusRaw || "").trim().toLowerCase();
  if(!clean) return "";
  return RIDE_STATUS_ALIAS_TO_INTERNAL[clean] || clean;
}

function rideStatusToPublic(statusRaw){
  const internal = rideStatusToInternal(statusRaw);
  if(!internal) return "";
  return RIDE_STATUS_INTERNAL_TO_PUBLIC[internal] || internal;
}

function mapRideStatusRowPublic(row){
  if(!row || typeof row !== "object") return row;
  const mappedStatus = rideStatusToPublic(row.status);
  if(!mappedStatus) return row;
  return { ...row, status: mappedStatus };
}

function mapRideStatusRowsPublic(rows){
  return (Array.isArray(rows) ? rows : []).map(mapRideStatusRowPublic);
}

function isFreshRideDriverLocation(updatedAtRaw){
  const updatedAt = Date.parse(String(updatedAtRaw || ""));
  if(!Number.isFinite(updatedAt)) return false;
  return (Date.now() - updatedAt) <= RIDE_DRIVER_LOCATION_MAX_AGE_MS;
}

async function fetchActiveRiderRoleSet(){
  const rows = await localServicesSupabaseRequest("local_roles", "GET", {
    query: {
      select: "user_id,role,status",
      role: "eq.rider",
      status: "eq.active",
      limit: "5000"
    }
  });
  const set = new Set();
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const id = sanitizeUserId(row?.user_id);
    if(id) set.add(id);
  });
  return set;
}

async function fetchRideDriverProfileMap(){
  const rows = await localServicesSupabaseRequest("local_listings", "GET", {
    query: {
      select: "user_id,store_name,image_url,phone,listing_type,status,open_now,ride_vehicle_type,vehicle_number,base_fare_inr,per_km_rate_inr,per_min_rate_inr,service_radius_km",
      listing_type: "eq.ride",
      status: "eq.approved",
      open_now: "eq.true",
      limit: "5000"
    }
  });
  const map = new Map();
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const id = sanitizeUserId(row?.user_id);
    if(!id) return;
    map.set(id, {
      user_id: id,
      store_name: String(row?.store_name || "").trim(),
      image_url: String(row?.image_url || "").trim(),
      phone: String(row?.phone || "").trim(),
      ride_vehicle_type: sanitizeRideVehicleType(row?.ride_vehicle_type || "auto"),
      vehicle_number: String(row?.vehicle_number || "").trim(),
      base_fare_inr: roundMoney(row?.base_fare_inr || 0),
      per_km_rate_inr: roundMoney(row?.per_km_rate_inr || 0),
      per_min_rate_inr: roundMoney(row?.per_min_rate_inr || 0),
      service_radius_km: Math.max(1, Math.min(50, Math.round(safeNumber(row?.service_radius_km || RIDE_MATCH_RADIUS_KM))))
    });
  });
  return map;
}

async function fetchNearbyRideDrivers(pickupLat, pickupLng, limitCount, options){
  const opts = options || {};
  const radiusKm = Number.isFinite(Number(opts.radiusKm)) ? Number(opts.radiusKm) : RIDE_MATCH_RADIUS_KM;
  const excludeSet = new Set(
    Array.isArray(opts.excludeUserIds)
      ? opts.excludeUserIds.map(sanitizeUserId).filter(Boolean)
      : []
  );
  const [locRows, activeRiderSet, profileMap] = await Promise.all([
    localServicesSupabaseRequest("local_rider_locations", "GET", {
      query: {
        select: "user_id,lat,lng,is_online,updated_at",
        is_online: "eq.true",
        limit: "5000"
      }
    }),
    fetchActiveRiderRoleSet(),
    fetchRideDriverProfileMap()
  ]);
  return (Array.isArray(locRows) ? locRows : [])
    .map((row) => {
      const userId = sanitizeUserId(row?.user_id);
      if(!userId || excludeSet.has(userId)) return null;
      if(!activeRiderSet.has(userId)) return null;
      if(!isFreshRideDriverLocation(row?.updated_at)) return null;
      const lat = sanitizeGeoNumber(row?.lat);
      const lng = sanitizeGeoNumber(row?.lng);
      if(lat === null || lng === null) return null;
      const distanceKm = localDistanceKm(pickupLat, pickupLng, lat, lng);
      const profile = profileMap.get(userId) || null;
      return {
        user_id: userId,
        lat,
        lng,
        distance_km: Number(distanceKm.toFixed(3)),
        updated_at: String(row?.updated_at || ""),
        store_name: String(profile?.store_name || "").trim(),
        image_url: String(profile?.image_url || "").trim(),
        phone: String(profile?.phone || "").trim(),
        ride_vehicle_type: sanitizeRideVehicleType(profile?.ride_vehicle_type || "auto"),
        vehicle_number: String(profile?.vehicle_number || "").trim(),
        base_fare_inr: roundMoney(profile?.base_fare_inr || 0),
        per_km_rate_inr: roundMoney(profile?.per_km_rate_inr || 0),
        per_min_rate_inr: roundMoney(profile?.per_min_rate_inr || 0),
        service_radius_km: Math.max(1, Math.min(50, Math.round(safeNumber(profile?.service_radius_km || radiusKm))))
      };
    })
    .filter(Boolean)
    .filter(row => Number(row.distance_km || 0) <= Math.max(0.1, Math.min(radiusKm, Number(row.service_radius_km || radiusKm))))
    .sort((a, b) => Number(a.distance_km || 0) - Number(b.distance_km || 0))
    .slice(0, Math.max(1, Number(limitCount || 20)));
}

app.post("/api/local/rides/rematch", async (req, res) => {
  const requestId = String(req.body?.request_id || "").trim();
  const riderUserId = sanitizeUserId(req.body?.rider_user_id);
  const pickupLatRaw = sanitizeGeoNumber(req.body?.pickup_lat);
  const pickupLngRaw = sanitizeGeoNumber(req.body?.pickup_lng);
  if(!requestId || !riderUserId){
    return res.status(400).json({ ok:false, error:"request_id_and_rider_user_id_required" });
  }
  try{
    const rows = await localServicesSupabaseRequest("local_ride_requests", "GET", {
      query: {
        select: "id,rider_user_id,status,pickup_lat,pickup_lng,offered_driver_ids,driver_user_id",
        id: `eq.${requestId}`,
        limit: "1"
      }
    });
    const ride = Array.isArray(rows) ? rows[0] || null : null;
    if(!ride){
      return res.status(404).json({ ok:false, error:"ride_request_not_found" });
    }
    if(sanitizeUserId(ride.rider_user_id) !== riderUserId){
      return res.status(403).json({ ok:false, error:"rematch_forbidden" });
    }
    const currentStatus = rideStatusToInternal(ride.status);
    if(currentStatus !== "searching"){
      return res.json({ ok:true, skipped:true, reason:"ride_not_searching", status: rideStatusToPublic(currentStatus) });
    }
    const pickupLat = pickupLatRaw !== null ? pickupLatRaw : sanitizeGeoNumber(ride.pickup_lat);
    const pickupLng = pickupLngRaw !== null ? pickupLngRaw : sanitizeGeoNumber(ride.pickup_lng);
    if(pickupLat === null || pickupLng === null){
      return res.status(400).json({ ok:false, error:"pickup_required_for_rematch" });
    }
    const nearbyDrivers = await fetchNearbyRideDrivers(pickupLat, pickupLng, 60);
    const already = Array.isArray(ride.offered_driver_ids) ? ride.offered_driver_ids.map(sanitizeUserId).filter(Boolean) : [];
    const rejected = getRideRejectedDrivers(requestId);
    const nextBatch = nearbyDrivers
      .filter((item) => {
        const id = sanitizeUserId(item?.user_id);
        return Boolean(id) && !already.includes(id) && !rejected.includes(id);
      })
      .slice(0, RIDE_REMATCH_BATCH_COUNT);
    const merged = Array.from(new Set([
      ...already,
      ...nextBatch.map(item => sanitizeUserId(item.user_id)).filter(Boolean)
    ])).slice(0, 30);
    const patched = await localServicesSupabaseRequest("local_ride_requests", "PATCH", {
      query: { id: `eq.${requestId}` },
      body: {
        offered_driver_ids: merged,
        updated_at: new Date().toISOString()
      },
      prefer: "return=representation"
    });
    const state = await readSystemState();
    const payloadData = { type:"ride_request", request_id: requestId };
    nextBatch.forEach((driver) => {
      const driverId = sanitizeUserId(driver?.user_id);
      if(!driverId) return;
      const tokens = getPushTokensForUser(state, driverId);
      sendFcmNotificationToTokens(tokens, {
        title: "Ride request waiting",
        body: "A nearby rider is waiting for pickup.",
        data: payloadData
      }).catch(() => {});
    });
    return res.json({
      ok:true,
      ride_request: mapRideStatusRowPublic(Array.isArray(patched) ? patched[0] || null : null),
      offered_driver_count: merged.length,
      newly_offered_count: nextBatch.length,
      search_radius_km: RIDE_MATCH_RADIUS_KM
    });
  }catch(err){
    return res.status(500).json({ ok:false, error:"ride_rematch_failed", message:String(err?.message || "") });
  }
});

function getRideRejectStore(){
  if(!app.locals.rideRejectStore){
    app.locals.rideRejectStore = new Map();
  }
  return app.locals.rideRejectStore;
}

function markRideDriverRejected(requestId, driverUserId){
  const reqId = String(requestId || "").trim();
  const driverId = sanitizeUserId(driverUserId);
  if(!reqId || !driverId) return;
  const store = getRideRejectStore();
  const now = Date.now();
  const entry = store.get(reqId) || { driver_ids: [], updated_at: now };
  const set = new Set(Array.isArray(entry.driver_ids) ? entry.driver_ids.map(sanitizeUserId).filter(Boolean) : []);
  set.add(driverId);
  store.set(reqId, {
    driver_ids: Array.from(set),
    updated_at: now
  });
  if(store.size > 2000){
    const rows = Array.from(store.entries())
      .sort((a, b) => Number(a?.[1]?.updated_at || 0) - Number(b?.[1]?.updated_at || 0))
      .slice(0, 600);
    rows.forEach(([key]) => store.delete(key));
  }
}

function getRideRejectedDrivers(requestId){
  const reqId = String(requestId || "").trim();
  if(!reqId) return [];
  const store = getRideRejectStore();
  const entry = store.get(reqId);
  if(!entry) return [];
  return Array.isArray(entry.driver_ids) ? entry.driver_ids.map(sanitizeUserId).filter(Boolean) : [];
}

function sanitizeRideVehicleType(value){
  const raw = String(value || "").trim().toLowerCase();
  if(raw === "bike" || raw === "auto" || raw === "car") return raw;
  return "auto";
}

function ridePricingConfig(vehicleType){
  const key = String(vehicleType || "auto").trim().toLowerCase();
  if(key === "bike") return { vehicle_type:"bike", base:30, per_km:8, per_min:1.5, speed_kmph:28 };
  if(key === "car") return { vehicle_type:"car", base:70, per_km:15, per_min:2.8, speed_kmph:30 };
  return { vehicle_type:"auto", base:45, per_km:11, per_min:2, speed_kmph:24 };
}

function computeRideFareBreakdown(input){
  const cfg = ridePricingConfig(input?.vehicle_type);
  const pickupLat = sanitizeGeoNumber(input?.pickup_lat);
  const pickupLng = sanitizeGeoNumber(input?.pickup_lng);
  const dropLat = sanitizeGeoNumber(input?.drop_lat);
  const dropLng = sanitizeGeoNumber(input?.drop_lng);
  let distanceKm = Number(input?.distance_km || 0);
  let durationMin = Number(input?.duration_min || 0);
  if((!Number.isFinite(distanceKm) || distanceKm <= 0) && pickupLat !== null && pickupLng !== null && dropLat !== null && dropLng !== null){
    distanceKm = localDistanceKm(pickupLat, pickupLng, dropLat, dropLng);
  }
  if(!Number.isFinite(distanceKm) || distanceKm < 0) distanceKm = 0;
  if(!Number.isFinite(durationMin) || durationMin <= 0){
    const speed = Number(cfg.speed_kmph || 24);
    durationMin = speed > 0 ? (distanceKm / speed) * 60 : 0;
  }
  durationMin = Math.max(durationMin, 3);
  const fare = cfg.base + (distanceKm * cfg.per_km) + (durationMin * cfg.per_min);
  const commission = fare * 0.10;
  const driver = fare - commission;
  return {
    vehicle_type: cfg.vehicle_type,
    distance_km: Number(distanceKm.toFixed(3)),
    duration_min: Number(durationMin.toFixed(2)),
    base_fare_inr: Number(cfg.base.toFixed(2)),
    fare_inr: Number(fare.toFixed(2)),
    commission_inr: Number(commission.toFixed(2)),
    driver_earning_inr: Number(driver.toFixed(2))
  };
}

app.post("/api/local/rides/fare-estimate", async (req, res) => {
  const pickupLat = sanitizeGeoNumber(req.body?.pickup_lat);
  const pickupLng = sanitizeGeoNumber(req.body?.pickup_lng);
  const dropLat = sanitizeGeoNumber(req.body?.drop_lat);
  const dropLng = sanitizeGeoNumber(req.body?.drop_lng);
  if(pickupLat === null || pickupLng === null || dropLat === null || dropLng === null){
    return res.status(400).json({ ok:false, error:"invalid_ride_estimate_payload" });
  }
  const summary = computeRideFareBreakdown({
    vehicle_type: sanitizeRideVehicleType(req.body?.vehicle_type),
    pickup_lat: pickupLat,
    pickup_lng: pickupLng,
    drop_lat: dropLat,
    drop_lng: dropLng,
    distance_km: req.body?.distance_km,
    duration_min: req.body?.duration_min
  });
  return res.json({ ok:true, estimate: summary });
});

app.get("/api/local/rides/summary", async (req, res) => {
  const requestId = String(req.query?.request_id || "").trim();
  const tipRaw = Number(req.query?.tip_inr || 0);
  if(!requestId){
    return res.status(400).json({ ok:false, error:"request_id_required" });
  }
  try{
    const rows = await localServicesSupabaseRequest("local_ride_requests", "GET", {
      query: {
        select: "id,pickup_lat,pickup_lng,drop_lat,drop_lng,status,vehicle_type,fare_inr,distance_km,duration_min,commission_inr,driver_earning_inr,payment_method,payment_status",
        id: `eq.${requestId}`,
        limit: "1"
      }
    });
    const ride = Array.isArray(rows) ? rows[0] || null : null;
    if(!ride){
      return res.status(404).json({ ok:false, error:"ride_not_found" });
    }
    const base = computeRideFareBreakdown({
      vehicle_type: sanitizeRideVehicleType(req.query?.vehicle_type || ride.vehicle_type),
      pickup_lat: ride.pickup_lat,
      pickup_lng: ride.pickup_lng,
      drop_lat: ride.drop_lat,
      drop_lng: ride.drop_lng,
      distance_km: ride.distance_km,
      duration_min: ride.duration_min
    });
    if(Number(ride?.fare_inr) > 0){
      base.fare_inr = roundMoney(ride.fare_inr);
      base.commission_inr = roundMoney(ride.commission_inr || (base.fare_inr * 0.10));
      base.driver_earning_inr = roundMoney(ride.driver_earning_inr || (base.fare_inr - base.commission_inr));
    }
    const tip = Number.isFinite(tipRaw) && tipRaw > 0 ? tipRaw : 0;
    const total = base.fare_inr + tip;
    const commission = total * 0.10;
    const driver = total - commission;
    return res.json({
      ok:true,
      ride_status: rideStatusToPublic(ride.status),
      summary: {
        ...base,
        tip_inr: Number(tip.toFixed(2)),
        total_fare_inr: Number(total.toFixed(2)),
        commission_inr: Number(commission.toFixed(2)),
        driver_earning_inr: Number(driver.toFixed(2))
      }
    });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_ride_summary_failed", message:String(err?.message || "") });
  }
});

app.post("/api/local/rides/accept", async (req, res) => {
  const requestId = String(req.body?.request_id || "").trim();
  const driverUserId = sanitizeUserId(req.body?.driver_user_id);
  if(!requestId || !driverUserId){
    return res.status(400).json({ ok:false, error:"request_id_and_driver_user_id_required" });
  }
  try{
    const riderAllowed = await hasActiveLocalRole(driverUserId, "rider");
    if(!riderAllowed){
      return res.status(403).json({ ok:false, error:"rider_role_required" });
    }
    const rows = await localServicesSupabaseRequest("local_ride_requests", "GET", {
      query: {
        select: "id,rider_user_id,status,offered_driver_ids,driver_user_id",
        id: `eq.${requestId}`,
        limit: "1"
      }
    });
    const request = Array.isArray(rows) ? rows[0] : null;
    if(!request){
      return res.status(404).json({ ok:false, error:"ride_request_not_found" });
    }
    if(rideStatusToInternal(request.status) !== "searching"){
      return res.status(409).json({ ok:false, error:"ride_request_not_searching" });
    }
    const offered = Array.isArray(request.offered_driver_ids) ? request.offered_driver_ids.map(sanitizeUserId) : [];
    if(offered.length && !offered.includes(driverUserId)){
      return res.status(403).json({ ok:false, error:"driver_not_offered_for_request" });
    }
    const otp = String(Math.floor(1000 + Math.random() * 9000));
    const patched = await localServicesSupabaseRequest("local_ride_requests", "PATCH", {
      query: {
        id: `eq.${requestId}`,
        status: "eq.searching"
      },
      body: {
        status: "accepted",
        driver_user_id: driverUserId,
        ride_start_otp: otp,
        otp_verified: false,
        otp_verified_at: null,
        accepted_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      },
      prefer: "return=representation"
    });
    const acceptedRide = Array.isArray(patched) ? patched[0] || null : null;
    if(!acceptedRide){
      return res.status(409).json({ ok:false, error:"ride_request_already_taken" });
    }
    const state = await readSystemState();
    const riderTokens = getPushTokensForUser(state, sanitizeUserId(request.rider_user_id));
    sendFcmNotificationToTokens(riderTokens, {
      title: "Driver accepted your ride",
      body: `Your ride is confirmed. Start OTP: ${otp}`,
      data: { type:"ride_accepted", request_id: requestId, start_otp: otp }
    }).catch(() => {});
    return res.json({ ok:true, ride_request: mapRideStatusRowPublic(acceptedRide) });
  }catch(err){
    return res.status(500).json({ ok:false, error:"ride_accept_failed", message:String(err?.message || "") });
  }
});

app.post("/api/local/rides/reject", async (req, res) => {
  const requestId = String(req.body?.request_id || "").trim();
  const driverUserId = sanitizeUserId(req.body?.driver_user_id);
  if(!requestId || !driverUserId){
    return res.status(400).json({ ok:false, error:"request_id_and_driver_user_id_required" });
  }
  try{
    const riderAllowed = await hasActiveLocalRole(driverUserId, "rider");
    if(!riderAllowed){
      return res.status(403).json({ ok:false, error:"rider_role_required" });
    }
    const rows = await localServicesSupabaseRequest("local_ride_requests", "GET", {
      query: {
        select: "id,status,offered_driver_ids",
        id: `eq.${requestId}`,
        limit: "1"
      }
    });
    const ride = Array.isArray(rows) ? rows[0] || null : null;
    if(!ride){
      return res.status(404).json({ ok:false, error:"ride_request_not_found" });
    }
    if(rideStatusToInternal(ride.status) !== "searching"){
      return res.json({ ok:true, skipped:true, reason:"ride_not_searching" });
    }
    const offered = Array.isArray(ride.offered_driver_ids) ? ride.offered_driver_ids.map(sanitizeUserId).filter(Boolean) : [];
    const nextOffered = offered.filter(id => id !== driverUserId);
    markRideDriverRejected(requestId, driverUserId);
    const patched = await localServicesSupabaseRequest("local_ride_requests", "PATCH", {
      query: { id: `eq.${requestId}` },
      body: {
        offered_driver_ids: nextOffered,
        updated_at: new Date().toISOString()
      },
      prefer: "return=representation"
    });
    return res.json({
      ok:true,
      ride_request: mapRideStatusRowPublic(Array.isArray(patched) ? patched[0] || null : null)
    });
  }catch(err){
    return res.status(500).json({ ok:false, error:"ride_reject_failed", message:String(err?.message || "") });
  }
});

function parseDateRangeUtc(dateInput){
  const raw = String(dateInput || "").trim();
  if(!raw){
    const now = new Date();
    const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 0, 0, 0, 0));
    const end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1, 0, 0, 0, 0));
    return { start, end };
  }
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(raw);
  if(!m) return null;
  const year = Number(m[1]);
  const month = Number(m[2]);
  const day = Number(m[3]);
  if(!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null;
  if(month < 1 || month > 12 || day < 1 || day > 31) return null;
  const start = new Date(Date.UTC(year, month - 1, day, 0, 0, 0, 0));
  const end = new Date(Date.UTC(year, month - 1, day + 1, 0, 0, 0, 0));
  return { start, end };
}

app.get("/api/local/rides/driver/earnings", async (req, res) => {
  const userId = sanitizeUserId(req.query?.user_id);
  if(!userId){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  const range = parseDateRangeUtc(req.query?.date);
  if(!range){
    return res.status(400).json({ ok:false, error:"invalid_date_format_use_yyyy_mm_dd" });
  }
  try{
    const rows = await localServicesSupabaseRequest("local_ride_requests", "GET", {
      query: {
        select: "id,pickup_lat,pickup_lng,drop_lat,drop_lng,status,updated_at,vehicle_type,fare_inr,commission_inr,driver_earning_inr,distance_km,duration_min",
        driver_user_id: `eq.${userId}`,
        status: "eq.completed",
        updated_at: `gte.${range.start.toISOString()}`,
        order: "updated_at.desc",
        limit: "1200"
      }
    });
    const list = (Array.isArray(rows) ? rows : []).filter((row) => {
      const updatedAt = Date.parse(String(row?.updated_at || ""));
      return Number.isFinite(updatedAt) && updatedAt < range.end.getTime();
    });
    let totalFare = 0;
    let totalDriver = 0;
    list.forEach((ride) => {
      const storedFare = Number(ride?.fare_inr || 0);
      const storedNet = Number(ride?.driver_earning_inr || 0);
      if(storedFare > 0 && storedNet >= 0){
        totalFare += storedFare;
        totalDriver += storedNet;
        return;
      }
      const summary = computeRideFareBreakdown({
        vehicle_type: sanitizeRideVehicleType(ride?.vehicle_type || "auto"),
        pickup_lat: ride?.pickup_lat,
        pickup_lng: ride?.pickup_lng,
        drop_lat: ride?.drop_lat,
        drop_lng: ride?.drop_lng,
        distance_km: ride?.distance_km,
        duration_min: ride?.duration_min
      });
      totalFare += Number(summary?.fare_inr || 0);
      totalDriver += Number(summary?.driver_earning_inr || 0);
    });
    return res.json({
      ok:true,
      date_utc: range.start.toISOString().slice(0, 10),
      rides_completed: list.length,
      total_fare_inr: Number(totalFare.toFixed(2)),
      driver_earning_inr: Number(totalDriver.toFixed(2))
    });
  }catch(err){
    return res.status(500).json({ ok:false, error:"driver_earnings_failed", message:String(err?.message || "") });
  }
});

app.post("/api/local/rides/otp/verify", async (req, res) => {
  const requestId = String(req.body?.request_id || "").trim();
  const riderUserId = sanitizeUserId(req.body?.rider_user_id);
  const otp = String(req.body?.otp || "").trim();
  if(!requestId || !riderUserId || !otp){
    return res.status(400).json({ ok:false, error:"request_id_rider_user_id_otp_required" });
  }
  try{
    const rows = await localServicesSupabaseRequest("local_ride_requests", "GET", {
      query: {
        select: "id,rider_user_id,driver_user_id,status,ride_start_otp,otp_verified",
        id: `eq.${requestId}`,
        limit: "1"
      }
    });
    const ride = Array.isArray(rows) ? rows[0] || null : null;
    if(!ride){
      return res.status(404).json({ ok:false, error:"ride_not_found" });
    }
    if(sanitizeUserId(ride.rider_user_id) !== riderUserId){
      return res.status(403).json({ ok:false, error:"otp_verify_forbidden" });
    }
    if(rideStatusToInternal(ride.status) !== "accepted"){
      return res.status(409).json({ ok:false, error:"otp_verify_invalid_status" });
    }
    if(String(ride.ride_start_otp || "") !== otp){
      return res.status(400).json({ ok:false, error:"otp_invalid" });
    }
    const patched = await localServicesSupabaseRequest("local_ride_requests", "PATCH", {
      query: { id: `eq.${requestId}` },
      body: {
        otp_verified: true,
        otp_verified_at: new Date().toISOString(),
        status: rideStatusToInternal("started"),
        updated_at: new Date().toISOString()
      },
      prefer: "return=representation"
    });
    return res.json({ ok:true, ride_request: mapRideStatusRowPublic(Array.isArray(patched) ? patched[0] || null : null) });
  }catch(err){
    return res.status(500).json({ ok:false, error:"ride_otp_verify_failed", message:String(err?.message || "") });
  }
});

app.get("/api/local/settlement/monthly", async (req, res) => {
  const userId = sanitizeUserId(req.query?.user_id);
  if(!userId){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  const range = parseMonthRange(req.query?.month);
  try{
    const rows = await localServicesSupabaseRequest("local_orders", "GET", {
      query: {
        select: "id,amount_inr,status,created_at",
        seller_user_id: `eq.${userId}`,
        created_at: `gte.${range.fromIso}`,
        limit: "2500"
      }
    });
    const completed = (Array.isArray(rows) ? rows : [])
      .filter(item => {
        const s = String(item?.status || "").toLowerCase();
        return s === "completed" || s === "delivered";
      })
      .filter(item => String(item?.created_at || "") < range.toIso);
    const gross = completed.reduce((sum, item) => sum + Math.max(0, safeNumber(item?.amount_inr)), 0);
    const due = Math.round(gross * 0.10 * 100) / 100;
    return res.json({
      ok:true,
      month: range.month,
      completed_orders: completed.length,
      gross_inr: Math.round(gross * 100) / 100,
      platform_share_percent: 10,
      platform_due_inr: due
    });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_settlement_failed", message:String(err?.message || "") });
  }
});

app.get("/api/local/admin/settlement/overview", async (req, res) => {
  if(!isAdminAutomationRequest(req)){
    return res.status(403).json({ ok:false, error:"forbidden" });
  }
  const range = parseMonthRange(req.query?.month);
  try{
    const rows = await localServicesSupabaseRequest("local_orders", "GET", {
      query: {
        select: "seller_user_id,amount_inr,status,created_at",
        created_at: `gte.${range.fromIso}`,
        order: "created_at.desc",
        limit: "8000"
      }
    });
    const bySeller = {};
    (Array.isArray(rows) ? rows : []).forEach((row) => {
      const st = String(row?.status || "").toLowerCase();
      if(st !== "completed" && st !== "delivered") return;
      const createdAt = String(row?.created_at || "");
      if(!createdAt || createdAt >= range.toIso) return;
      const seller = sanitizeUserId(row?.seller_user_id);
      if(!seller) return;
      if(!bySeller[seller]){
        bySeller[seller] = { seller_user_id: seller, completed_orders: 0, gross_inr: 0 };
      }
      bySeller[seller].completed_orders += 1;
      bySeller[seller].gross_inr += Math.max(0, safeNumber(row?.amount_inr));
    });
    const sellers = Object.values(bySeller).map((item) => {
      const gross = Math.round(item.gross_inr * 100) / 100;
      const due = Math.round(gross * 0.10 * 100) / 100;
      return {
        seller_user_id: item.seller_user_id,
        completed_orders: item.completed_orders,
        gross_inr: gross,
        platform_due_inr: due
      };
    }).sort((a, b) => b.platform_due_inr - a.platform_due_inr);
    const totalGross = sellers.reduce((sum, s) => sum + s.gross_inr, 0);
    const totalDue = sellers.reduce((sum, s) => sum + s.platform_due_inr, 0);
    return res.json({
      ok:true,
      month: range.month,
      seller_count: sellers.length,
      total_gross_inr: Math.round(totalGross * 100) / 100,
      total_platform_due_inr: Math.round(totalDue * 100) / 100,
      sellers
    });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_admin_settlement_overview_failed", message:String(err?.message || "") });
  }
});

app.post("/api/local/admin/settlement/mark-paid", async (req, res) => {
  if(!isAdminAutomationRequest(req)){
    return res.status(403).json({ ok:false, error:"forbidden" });
  }
  const sellerUserId = sanitizeUserId(req.body?.seller_user_id);
  const month = String(req.body?.month || "").trim();
  const paidRef = String(req.body?.paid_ref || "").trim().slice(0, 120);
  const paidAmountInr = Math.max(0, safeNumber(req.body?.paid_amount_inr));
  if(!sellerUserId || !month || !/^\d{4}-\d{2}$/.test(month)){
    return res.status(400).json({ ok:false, error:"invalid_settlement_mark_paid_payload" });
  }
  try{
    const rows = await localServicesSupabaseRequest("local_settlements", "POST", {
      body: [{
        seller_user_id: sellerUserId,
        month,
        paid_amount_inr: Math.round(paidAmountInr * 100) / 100,
        paid_ref: paidRef || null,
        status: "paid",
        paid_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      }],
      query: { on_conflict: "seller_user_id,month" },
      prefer: "resolution=merge-duplicates,return=representation"
    });
    return res.json({ ok:true, settlement: Array.isArray(rows) ? rows[0] || null : null });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_settlement_mark_paid_failed", message:String(err?.message || "") });
  }
});

app.get("/api/local/admin/health", async (req, res) => {
  if(!isAdminAutomationRequest(req)){
    return res.status(403).json({ ok:false, error:"forbidden" });
  }
  try{
    const [searchingRides, placedOrders, requestedBookings] = await Promise.all([
      localServicesSupabaseRequest("local_ride_requests", "GET", {
        query: {
          select: "id,created_at,status",
          status: "eq.searching",
          order: "created_at.asc",
          limit: "500"
        }
      }),
      localServicesSupabaseRequest("local_orders", "GET", {
        query: {
          select: "id,created_at,status",
          status: "eq.placed",
          order: "created_at.asc",
          limit: "500"
        }
      }),
      localServicesSupabaseRequest("local_agent_bookings", "GET", {
        query: {
          select: "id,created_at,status",
          status: "eq.requested",
          order: "created_at.asc",
          limit: "500"
        }
      })
    ]);

    const now = Date.now();
    const staleMinutes = (rows, min) => (Array.isArray(rows) ? rows : []).filter((r) => {
      const ts = Date.parse(String(r?.created_at || ""));
      if(!Number.isFinite(ts)) return false;
      return (now - ts) > (min * 60 * 1000);
    }).length;

    return res.json({
      ok:true,
      metrics: {
        searching_rides: Array.isArray(searchingRides) ? searchingRides.length : 0,
        placed_orders: Array.isArray(placedOrders) ? placedOrders.length : 0,
        requested_agent_bookings: Array.isArray(requestedBookings) ? requestedBookings.length : 0
      },
      stale: {
        rides_over_10m: staleMinutes(searchingRides, 10),
        orders_over_30m: staleMinutes(placedOrders, 30),
        bookings_over_30m: staleMinutes(requestedBookings, 30)
      }
    });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_admin_health_failed", message:String(err?.message || "") });
  }
});

app.get("/api/local/admin/listings/pending", async (req, res) => {
  if(!isAdminAutomationRequest(req)){
    return res.status(403).json({ ok:false, error:"forbidden" });
  }
  try{
    const rows = await localServicesSupabaseRequest("local_listings", "GET", {
      query: {
        select: "id,user_id,store_name,listing_type,phone,image_url,status,created_at",
        status: "eq.pending_approval",
        order: "created_at.asc",
        limit: "500"
      }
    });
    return res.json({ ok:true, listings: Array.isArray(rows) ? rows : [] });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_admin_pending_listings_failed", message:String(err?.message || "") });
  }
});

app.get("/api/local/orders/for-seller", async (req, res) => {
  const userId = sanitizeUserId(req.query?.user_id);
  if(!userId){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  try{
    const sellerAllowed = await hasActiveLocalRole(userId, "seller");
    if(!sellerAllowed) return res.json({ ok:true, orders: [], requires_role: "seller" });
    const rows = await localServicesSupabaseRequest("local_orders", "GET", {
      query: {
        select: "id,buyer_user_id,seller_user_id,listing_id,service_type,amount_inr,delivery_address,note,item_snapshot,payment_method,payment_status,payment_order_id,payment_id,payment_ref,commission_inr,seller_earning_inr,status,created_at,updated_at,delivered_at",
        seller_user_id: `eq.${userId}`,
        order: "created_at.desc",
        limit: "500"
      }
    });
    return res.json({ ok:true, orders: Array.isArray(rows) ? rows : [] });
  }catch(err){
    return res.json({ ok:true, orders: [], warning: "seller_orders_temporarily_unavailable" });
  }
});

app.get("/api/local/orders/for-buyer", async (req, res) => {
  const userId = sanitizeUserId(req.query?.user_id);
  if(!userId){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  try{
    const consumerAllowed = await hasActiveLocalRole(userId, "consumer");
    if(!consumerAllowed) return res.json({ ok:true, orders: [], requires_role: "consumer" });
    const rows = await localServicesSupabaseRequest("local_orders", "GET", {
      query: {
        select: "id,buyer_user_id,seller_user_id,listing_id,service_type,amount_inr,delivery_address,note,item_snapshot,payment_method,payment_status,payment_order_id,payment_id,payment_ref,commission_inr,seller_earning_inr,status,created_at,updated_at,delivered_at",
        buyer_user_id: `eq.${userId}`,
        order: "created_at.desc",
        limit: "500"
      }
    });
    return res.json({ ok:true, orders: Array.isArray(rows) ? rows : [] });
  }catch(err){
    return res.json({ ok:true, orders: [], warning: "buyer_orders_temporarily_unavailable" });
  }
});

app.get("/api/local/rides/for-rider", async (req, res) => {
  const userId = sanitizeUserId(req.query?.user_id);
  if(!userId){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  try{
    const riderAllowed = await hasActiveLocalRole(userId, "rider");
    if(!riderAllowed) return res.json({ ok:true, rides: [], requires_role: "rider" });
    const rows = await localServicesSupabaseRequest("local_ride_requests", "GET", {
      query: {
        select: "id,rider_user_id,driver_user_id,pickup_lat,pickup_lng,drop_lat,drop_lng,pickup_text,drop_text,status,vehicle_type,fare_inr,distance_km,duration_min,payment_method,payment_status,offered_driver_ids,created_at,updated_at",
        order: "created_at.desc",
        limit: "500"
      }
    });
    const filtered = (Array.isArray(rows) ? rows : []).filter((row) => {
      const status = rideStatusToInternal(row?.status);
      const driver = sanitizeUserId(row?.driver_user_id);
      if(driver && driver === userId) return true;
      if(status !== "searching") return false;
      const offered = Array.isArray(row?.offered_driver_ids) ? row.offered_driver_ids.map(sanitizeUserId) : [];
      return offered.includes(userId);
    });
    return res.json({ ok:true, rides: mapRideStatusRowsPublic(filtered) });
  }catch(err){
    return res.json({ ok:true, rides: [], warning: "rider_feed_temporarily_unavailable" });
  }
});

app.post("/api/local/rides/status", async (req, res) => {
  const requestId = String(req.body?.request_id || "").trim();
  const userId = sanitizeUserId(req.body?.user_id);
  const statusRaw = String(req.body?.status || "").trim().toLowerCase();
  const status = rideStatusToInternal(statusRaw);
  const allowed = new Set(["arriving","on_trip","completed","cancelled"]);
  if(!requestId || !userId || !allowed.has(status)){
    return res.status(400).json({ ok:false, error:"invalid_ride_status_payload" });
  }
  try{
    const rows = await localServicesSupabaseRequest("local_ride_requests", "GET", {
      query: {
        select:"id,rider_user_id,driver_user_id,status,vehicle_type,pickup_lat,pickup_lng,drop_lat,drop_lng,fare_inr,distance_km,duration_min,payment_method,payment_status,payment_id,payment_order_id",
        id:`eq.${requestId}`,
        limit:"1"
      }
    });
    const ride = Array.isArray(rows) ? rows[0] : null;
    if(!ride){
      return res.status(404).json({ ok:false, error:"ride_not_found" });
    }
    if(sanitizeUserId(ride.driver_user_id) !== userId){
      return res.status(403).json({ ok:false, error:"ride_status_forbidden" });
    }
    const current = rideStatusToInternal(ride.status);
    const flow = {
      accepted: ["arriving", "on_trip", "cancelled"],
      arriving: ["on_trip", "cancelled"],
      on_trip: ["completed", "cancelled"],
      completed: [],
      cancelled: []
    };
    if(!canTransitionStatus(current, status, flow)){
      return res.status(409).json({ ok:false, error:"invalid_ride_status_transition" });
    }
    const patched = await localServicesSupabaseRequest("local_ride_requests", "PATCH", {
      query: { id: `eq.${requestId}` },
      body: {
        status,
        completed_at: status === "completed" ? new Date().toISOString() : null,
        updated_at: new Date().toISOString()
      },
      prefer: "return=representation"
    });
    const patchedRide = Array.isArray(patched) ? patched[0] || null : null;

    let settlement = null;
    if(status === "completed" && patchedRide){
      try{
        const storedFare = Number(patchedRide?.fare_inr || ride?.fare_inr || 0);
        const computed = computeRideFareBreakdown({
          vehicle_type: sanitizeRideVehicleType(patchedRide?.vehicle_type || ride?.vehicle_type || "auto"),
          pickup_lat: patchedRide?.pickup_lat ?? ride?.pickup_lat,
          pickup_lng: patchedRide?.pickup_lng ?? ride?.pickup_lng,
          drop_lat: patchedRide?.drop_lat ?? ride?.drop_lat,
          drop_lng: patchedRide?.drop_lng ?? ride?.drop_lng,
          distance_km: patchedRide?.distance_km ?? ride?.distance_km,
          duration_min: patchedRide?.duration_min ?? ride?.duration_min
        });
        const finalFare = storedFare > 0 ? roundMoney(storedFare) : roundMoney(computed?.fare_inr || 0);
        settlement = await recordLocalSettlement({
          module: "ride",
          referenceType: "ride",
          referenceId: String(patchedRide?.id || requestId),
          payerUserId: sanitizeUserId(patchedRide?.rider_user_id || ride?.rider_user_id),
          payeeUserId: sanitizeUserId(patchedRide?.driver_user_id || ride?.driver_user_id),
          payeeOwnerType: "driver",
          grossInr: finalFare,
          commissionPercent: 10,
          paymentMethod: sanitizePaymentMethod(patchedRide?.payment_method || ride?.payment_method || "cash"),
          paymentStatus: sanitizePaymentStatus(patchedRide?.payment_status || ride?.payment_status || "cod"),
          paymentGateway: "local_services",
          paymentOrderId: String(patchedRide?.payment_order_id || ride?.payment_order_id || ""),
          paymentId: String(patchedRide?.payment_id || ride?.payment_id || ""),
          paymentRef: String(patchedRide?.id || requestId),
          metadata: {
            vehicle_type: sanitizeRideVehicleType(patchedRide?.vehicle_type || ride?.vehicle_type || "auto"),
            distance_km: Number(patchedRide?.distance_km || ride?.distance_km || computed?.distance_km || 0),
            duration_min: Number(patchedRide?.duration_min || ride?.duration_min || computed?.duration_min || 0)
          }
        });
        if(settlement?.ok && !settlement?.skipped){
          await localServicesSupabaseRequest("local_ride_requests", "PATCH", {
            query: { id: `eq.${String(patchedRide?.id || requestId)}` },
            body: {
              fare_inr: roundMoney(finalFare),
              commission_inr: roundMoney(settlement.commission_inr || 0),
              driver_earning_inr: roundMoney(settlement.net_inr || 0),
              updated_at: new Date().toISOString()
            },
            prefer: "return=minimal"
          }).catch(() => {});
        }
      }catch(err){
        settlement = { ok:false, error:String(err?.message || "ride_settlement_failed") };
      }
    }

    const nextRide = patchedRide || ride;
    const riderId = sanitizeUserId(nextRide?.rider_user_id || ride?.rider_user_id);
    const driverId = sanitizeUserId(nextRide?.driver_user_id || ride?.driver_user_id);
    if(riderId || driverId){
      const state = await readSystemState();
      const data = {
        type: "ride_status",
        request_id: String(nextRide?.id || requestId),
        status: rideStatusToPublic(status)
      };
      const body = `Ride status: ${rideStatusToPublic(status).replace(/_/g, " ")}`;
      [riderId, driverId].filter(Boolean).forEach((uid) => {
        const tokens = getPushTokensForUser(state, uid);
        sendFcmNotificationToTokens(tokens, {
          title: "Ride update",
          body,
          data
        }).catch(() => {});
      });
    }

    return res.json({
      ok:true,
      ride_request: mapRideStatusRowPublic(patchedRide),
      settlement
    });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_ride_status_failed", message:String(err?.message || "") });
  }
});

app.post("/api/local/rides/cancel", async (req, res) => {
  const requestId = String(req.body?.request_id || "").trim();
  const userId = sanitizeUserId(req.body?.user_id);
  const by = String(req.body?.by || "rider").trim().toLowerCase();
  if(!requestId || !userId || !["rider","driver","system"].includes(by)){
    return res.status(400).json({ ok:false, error:"invalid_ride_cancel_payload" });
  }
  try{
    const rows = await localServicesSupabaseRequest("local_ride_requests", "GET", {
      query: {
        select: "id,rider_user_id,driver_user_id,status",
        id: `eq.${requestId}`,
        limit: "1"
      }
    });
    const ride = Array.isArray(rows) ? rows[0] || null : null;
    if(!ride){
      return res.status(404).json({ ok:false, error:"ride_not_found" });
    }
    const riderId = sanitizeUserId(ride.rider_user_id);
    const driverId = sanitizeUserId(ride.driver_user_id);
    const current = rideStatusToInternal(ride.status);
    const allowedCurrent = new Set(["searching","accepted","arriving","on_trip"]);
    if(!allowedCurrent.has(current)){
      return res.status(409).json({ ok:false, error:"ride_not_cancellable" });
    }
    if(by === "rider"){
      if(riderId !== userId){
        return res.status(403).json({ ok:false, error:"ride_cancel_forbidden" });
      }
      if(!["searching","accepted","arriving"].includes(current)){
        return res.status(409).json({ ok:false, error:"rider_cancel_not_allowed_in_current_status" });
      }
    }else if(by === "driver"){
      if(driverId !== userId){
        return res.status(403).json({ ok:false, error:"ride_cancel_forbidden" });
      }
    }
    const patched = await localServicesSupabaseRequest("local_ride_requests", "PATCH", {
      query: { id: `eq.${requestId}` },
      body: {
        status: "cancelled",
        updated_at: new Date().toISOString()
      },
      prefer: "return=representation"
    });
    const cancelledRide = Array.isArray(patched) ? patched[0] || null : null;
    const notifyIds = [sanitizeUserId(ride.rider_user_id), sanitizeUserId(ride.driver_user_id)].filter(Boolean);
    if(notifyIds.length){
      const state = await readSystemState();
      notifyIds.forEach((uid) => {
        const tokens = getPushTokensForUser(state, uid);
        sendFcmNotificationToTokens(tokens, {
          title: "Ride cancelled",
          body: "Your ride has been cancelled.",
          data: { type:"ride_cancelled", request_id: requestId }
        }).catch(() => {});
      });
    }
    return res.json({ ok:true, ride_request: mapRideStatusRowPublic(cancelledRide) });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_ride_cancel_failed", message:String(err?.message || "") });
  }
});

app.get("/api/local/agent/bookings", async (req, res) => {
  const userId = sanitizeUserId(req.query?.user_id);
  const side = String(req.query?.side || "agent").trim().toLowerCase();
  if(!userId){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  try{
    if(side === "customer"){
      const consumerAllowed = await hasActiveLocalRole(userId, "consumer");
      if(!consumerAllowed) return res.json({ ok:true, bookings: [], requires_role: "consumer" });
    }else{
      const agentAllowed = await hasActiveLocalRole(userId, "agent");
      if(!agentAllowed) return res.json({ ok:true, bookings: [], requires_role: "agent" });
    }
    const query = {
      select: "id,customer_user_id,agent_user_id,agent_id,service_address,note,scheduled_at,hours_booked,estimated_price_inr,payment_method,payment_status,payment_order_id,payment_id,payment_ref,commission_inr,agent_earning_inr,status,created_at,updated_at",
      order: "created_at.desc",
      limit: "500"
    };
    if(side === "customer"){
      query.customer_user_id = `eq.${userId}`;
    }else{
      query.agent_user_id = `eq.${userId}`;
    }
    const rows = await localServicesSupabaseRequest("local_agent_bookings", "GET", { query });
    return res.json({ ok:true, bookings: Array.isArray(rows) ? rows : [] });
  }catch(err){
    return res.json({ ok:true, bookings: [], warning: "agent_bookings_temporarily_unavailable" });
  }
});

app.post("/api/local/agent/bookings/status", async (req, res) => {
  const bookingId = String(req.body?.booking_id || "").trim();
  const userId = sanitizeUserId(req.body?.user_id);
  const status = String(req.body?.status || "").trim().toLowerCase();
  const allowed = new Set(["accepted","on_the_way","started","completed","cancelled"]);
  if(!bookingId || !userId || !allowed.has(status)){
    return res.status(400).json({ ok:false, error:"invalid_booking_status_payload" });
  }
  try{
    const rows = await localServicesSupabaseRequest("local_agent_bookings", "GET", {
      query: {
        select:"id,customer_user_id,agent_user_id,status,estimated_price_inr,payment_method,payment_status,payment_order_id,payment_id,payment_ref",
        id:`eq.${bookingId}`,
        limit:"1"
      }
    });
    const booking = Array.isArray(rows) ? rows[0] : null;
    if(!booking){
      return res.status(404).json({ ok:false, error:"booking_not_found" });
    }
    if(sanitizeUserId(booking.agent_user_id) !== userId){
      return res.status(403).json({ ok:false, error:"booking_status_forbidden" });
    }
    const flow = {
      requested: ["accepted", "cancelled"],
      accepted: ["on_the_way", "cancelled"],
      on_the_way: ["started", "completed", "cancelled"],
      started: ["completed", "cancelled"],
      completed: [],
      cancelled: []
    };
    if(!canTransitionStatus(booking.status, status, flow)){
      return res.status(409).json({ ok:false, error:"invalid_booking_status_transition" });
    }
    const patched = await localServicesSupabaseRequest("local_agent_bookings", "PATCH", {
      query: { id: `eq.${bookingId}` },
      body: {
        status,
        completed_at: status === "completed" ? new Date().toISOString() : null,
        updated_at: new Date().toISOString()
      },
      prefer: "return=representation"
    });
    const patchedBooking = Array.isArray(patched) ? patched[0] || null : null;
    let settlement = null;
    if(status === "completed" && patchedBooking){
      try{
        settlement = await recordLocalSettlement({
          module: "agent",
          referenceType: "service_booking",
          referenceId: String(patchedBooking?.id || bookingId),
          payerUserId: sanitizeUserId(patchedBooking?.customer_user_id || booking?.customer_user_id),
          payeeUserId: sanitizeUserId(patchedBooking?.agent_user_id || booking?.agent_user_id),
          payeeOwnerType: "agent",
          grossInr: roundMoney(patchedBooking?.estimated_price_inr || booking?.estimated_price_inr || 0),
          commissionPercent: 15,
          paymentMethod: sanitizePaymentMethod(patchedBooking?.payment_method || booking?.payment_method || "cash"),
          paymentStatus: sanitizePaymentStatus(patchedBooking?.payment_status || booking?.payment_status || "cod"),
          paymentGateway: "local_services",
          paymentOrderId: String(patchedBooking?.payment_order_id || booking?.payment_order_id || ""),
          paymentId: String(patchedBooking?.payment_id || booking?.payment_id || ""),
          paymentRef: String(patchedBooking?.payment_ref || booking?.payment_ref || patchedBooking?.id || bookingId),
          metadata: {
            booking_status: "completed"
          }
        });
        if(settlement?.ok && !settlement?.skipped){
          await localServicesSupabaseRequest("local_agent_bookings", "PATCH", {
            query: { id: `eq.${String(patchedBooking?.id || bookingId)}` },
            body: {
              commission_inr: roundMoney(settlement.commission_inr || 0),
              agent_earning_inr: roundMoney(settlement.net_inr || 0),
              updated_at: new Date().toISOString()
            },
            prefer: "return=minimal"
          }).catch(() => {});
        }
      }catch(err){
        settlement = { ok:false, error:String(err?.message || "agent_settlement_failed") };
      }
    }
    const nextBooking = patchedBooking || booking;
    const customerId = sanitizeUserId(nextBooking?.customer_user_id || booking?.customer_user_id);
    const agentUserId = sanitizeUserId(nextBooking?.agent_user_id || booking?.agent_user_id);
    if(customerId || agentUserId){
      const state = await readSystemState();
      const data = {
        type: "agent_booking_status",
        booking_id: String(nextBooking?.id || bookingId),
        status: String(status || "")
      };
      const body = `Booking is now ${status.replace(/_/g, " ")}.`;
      [customerId, agentUserId].filter(Boolean).forEach((uid) => {
        const tokens = getPushTokensForUser(state, uid);
        sendFcmNotificationToTokens(tokens, {
          title: "Service booking update",
          body,
          data
        }).catch(() => {});
      });
    }
    return res.json({ ok:true, booking: patchedBooking, settlement });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_booking_status_failed", message:String(err?.message || "") });
  }
});

app.post("/api/local/role/fee/order", async (req, res) => {
  const userId = sanitizeUserId(req.body?.user_id);
  const role = sanitizeLocalRole(req.body?.role);
  if(!userId || !role){
    return res.status(400).json({ ok:false, error:"user_id_and_role_required" });
  }
  if(!["seller","rider"].includes(role)){
    return res.status(400).json({ ok:false, error:"fee_not_required_for_role" });
  }
  if(!isContestRazorpayConfigured()){
    return res.status(503).json({ ok:false, error:"razorpay_not_configured", message:getContestRazorpaySetupMessage() });
  }
  try{
    const receipt = `local_role_${role}_${Date.now()}_${crypto.randomBytes(3).toString("hex")}`;
    const razorpayOrder = await createContestRazorpayOrder({
      amount_paise: 50000,
      receipt,
      notes: {
        module: "local_role",
        user_id: userId,
        role
      }
    });
    return res.json({
      ok:true,
      order: {
        key_id: CONTEST_RAZORPAY_KEY_ID,
        razorpay_order_id: razorpayOrder.id,
        amount_paise: 50000,
        amount_inr: 500,
        currency: "INR",
        role
      }
    });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_role_fee_order_failed", message:String(err?.message || "") });
  }
});

app.post("/api/local/role/fee/verify", async (req, res) => {
  const userId = sanitizeUserId(req.body?.user_id);
  const role = sanitizeLocalRole(req.body?.role);
  const orderId = String(req.body?.razorpay_order_id || "").trim();
  const paymentId = String(req.body?.razorpay_payment_id || "").trim();
  const signature = String(req.body?.razorpay_signature || "").trim();
  if(!userId || !role || !orderId || !paymentId || !signature){
    return res.status(400).json({ ok:false, error:"missing_fee_verify_fields" });
  }
  if(!verifyContestPaymentSignature(orderId, paymentId, signature)){
    return res.status(400).json({ ok:false, error:"invalid_payment_signature" });
  }
  try{
    const paymentSnapshot = await fetchRazorpayPaymentSnapshot(paymentId);
    if(String(paymentSnapshot?.order_id || "").trim() !== orderId){
      return res.status(400).json({ ok:false, error:"order_payment_mismatch" });
    }
    const status = String(paymentSnapshot?.status || "").trim().toLowerCase();
    const captured = Boolean(paymentSnapshot?.captured) || status === "captured";
    const amountPaise = Math.round(safeNumber(paymentSnapshot?.amount));
    if(!captured || !isRazorpayPaymentStatusAcceptable(status) || amountPaise < 50000){
      return res.status(400).json({ ok:false, error:"payment_not_captured_or_invalid_amount" });
    }
    const rows = await localServicesSupabaseRequest("local_role_payments", "POST", {
      body: [{
        user_id: userId,
        role,
        razorpay_order_id: orderId,
        razorpay_payment_id: paymentId,
        amount_inr: 500,
        status: "captured",
        verified_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      }],
      query: { on_conflict: "razorpay_order_id" },
      prefer: "resolution=merge-duplicates,return=representation"
    });
    return res.json({ ok:true, payment: Array.isArray(rows) ? rows[0] || null : null });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_role_fee_verify_failed", message:String(err?.message || "") });
  }
});

app.post("/api/local/role/enroll", async (req, res) => {
  const userId = sanitizeUserId(req.body?.user_id);
  const role = sanitizeLocalRole(req.body?.role);
  const displayName = sanitizeDisplayName(req.body?.display_name || "");
  const phone = String(req.body?.phone || "").trim().slice(0, 30);
  const paymentRef = String(req.body?.payment_ref || "").trim().slice(0, 120);
  const feePaid = Boolean(req.body?.fee_paid);
  if(!userId || !role){
    return res.status(400).json({ ok:false, error:"user_id_and_role_required" });
  }
  const feeRequired = role === "seller" || role === "rider";
  if(feeRequired && !feePaid && !paymentRef){
    return res.status(402).json({ ok:false, error:"listing_fee_required", amount_inr:500 });
  }
  try{
    if(feeRequired){
      const paymentRows = await localServicesSupabaseRequest("local_role_payments", "GET", {
        query: {
          select: "id",
          user_id: `eq.${userId}`,
          role: `eq.${role}`,
          status: "eq.captured",
          order: "verified_at.desc",
          limit: "1"
        }
      });
      const hasVerifiedFee = Array.isArray(paymentRows) && !!paymentRows[0];
      if(!hasVerifiedFee && !paymentRef){
        return res.status(402).json({ ok:false, error:"listing_fee_not_verified" });
      }
    }
    const rows = await localServicesSupabaseRequest("local_roles", "POST", {
      body: [{
        user_id: userId,
        role,
        display_name: displayName || null,
        phone: phone || null,
        fee_required_inr: feeRequired ? 500 : 0,
        fee_paid: true,
        payment_ref: paymentRef || null,
        status: "active",
        updated_at: new Date().toISOString()
      }],
      query: { on_conflict: "user_id,role" },
      prefer: "resolution=merge-duplicates,return=representation"
    });
    return res.json({ ok:true, role: Array.isArray(rows) ? rows[0] || null : null });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_role_enroll_failed", message:String(err?.message || "") });
  }
});

app.get("/api/local/roles", async (req, res) => {
  const userId = sanitizeUserId(req.query?.user_id);
  if(!userId){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  try{
    const rows = await localServicesSupabaseRequest("local_roles", "GET", {
      query: {
        select: "id,user_id,role,status,fee_required_inr,fee_paid,display_name,phone,updated_at",
        user_id: `eq.${userId}`,
        status: "eq.active",
        limit: "10"
      }
    });
    return res.json({ ok:true, roles: Array.isArray(rows) ? rows : [] });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_roles_fetch_failed", message:String(err?.message || "") });
  }
});

app.post("/api/local/payments/order", async (req, res) => {
  const userId = sanitizeUserId(req.body?.user_id);
  const module = sanitizeLocalModule(req.body?.module);
  const amountInr = Math.max(0, roundMoney(req.body?.amount_inr || 0));
  const referenceType = String(req.body?.reference_type || "").trim().toLowerCase().slice(0, 40);
  const referenceId = String(req.body?.reference_id || "").trim().slice(0, 120);
  if(!userId || !["food", "grocery", "agent", "ride"].includes(module) || amountInr <= 0){
    return res.status(400).json({ ok:false, error:"invalid_payment_order_payload" });
  }
  if(!isContestRazorpayConfigured()){
    return res.status(503).json({
      ok:false,
      error:"razorpay_not_configured",
      message:getContestRazorpaySetupMessage()
    });
  }
  try{
    const amountPaise = Math.max(100, Math.round(amountInr * 100));
    const receipt = `local_${module}_${Date.now()}_${crypto.randomBytes(3).toString("hex")}`;
    const razorpayOrder = await createContestRazorpayOrder({
      amount_paise: amountPaise,
      receipt,
      notes: {
        module: "local_services",
        local_module: module,
        user_id: userId,
        reference_type: referenceType || null,
        reference_id: referenceId || null
      }
    });

    await localServicesSupabaseRequest("transactions", "POST", {
      body: [{
        owner_user_id: userId,
        owner_type: "customer",
        module: "payment",
        transaction_type: "payment_order",
        reference_type: referenceType || module,
        reference_id: referenceId || String(razorpayOrder?.id || receipt),
        payer_user_id: userId,
        payee_user_id: null,
        amount_inr: roundMoney(amountPaise / 100),
        gross_inr: roundMoney(amountPaise / 100),
        commission_inr: 0,
        net_inr: roundMoney(amountPaise / 100),
        platform_share_inr: 0,
        payment_method: "online",
        payment_status: "created",
        payment_gateway: "razorpay",
        payment_order_id: String(razorpayOrder?.id || "").trim() || null,
        payment_ref: receipt,
        metadata: {
          module,
          requested_amount_inr: amountInr
        },
        status: "created",
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      }],
      query: { on_conflict: "module,transaction_type,payment_order_id" },
      prefer: "resolution=merge-duplicates,return=minimal"
    }).catch(() => {});

    return res.json({
      ok:true,
      order: {
        key_id: CONTEST_RAZORPAY_KEY_ID,
        razorpay_order_id: String(razorpayOrder?.id || ""),
        amount_paise: amountPaise,
        amount_inr: roundMoney(amountPaise / 100),
        currency: "INR",
        module,
        reference_type: referenceType || null,
        reference_id: referenceId || null
      }
    });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_payment_order_failed", message:String(err?.message || "") });
  }
});

app.post("/api/local/payments/verify", async (req, res) => {
  const userId = sanitizeUserId(req.body?.user_id);
  const orderId = String(req.body?.razorpay_order_id || "").trim();
  const paymentId = String(req.body?.razorpay_payment_id || "").trim();
  const signature = String(req.body?.razorpay_signature || "").trim();
  const referenceType = String(req.body?.reference_type || "").trim().toLowerCase().slice(0, 40);
  const referenceId = String(req.body?.reference_id || "").trim().slice(0, 120);
  const moduleRequested = sanitizeLocalModule(req.body?.module);
  if(!userId || !orderId || !paymentId || !signature){
    return res.status(400).json({ ok:false, error:"missing_verify_fields" });
  }
  if(!verifyContestPaymentSignature(orderId, paymentId, signature)){
    return res.status(400).json({ ok:false, error:"invalid_payment_signature" });
  }
  try{
    const paymentSnapshot = await fetchRazorpayPaymentSnapshot(paymentId);
    if(String(paymentSnapshot?.order_id || "").trim() !== orderId){
      return res.status(400).json({ ok:false, error:"order_payment_mismatch" });
    }
    const status = String(paymentSnapshot?.status || "").trim().toLowerCase();
    const captured = Boolean(paymentSnapshot?.captured) || status === "captured";
    const amountPaise = Math.round(safeNumber(paymentSnapshot?.amount));
    const paymentCurrency = String(paymentSnapshot?.currency || "").trim().toUpperCase() || "INR";
    if(!captured || !isRazorpayPaymentStatusAcceptable(status) || amountPaise < 100){
      return res.status(409).json({ ok:false, error:"payment_not_captured" });
    }
    if(paymentCurrency !== "INR"){
      return res.status(400).json({ ok:false, error:"invalid_payment_currency", payment_currency: paymentCurrency });
    }

    const noteLocalModule = sanitizeLocalModule(paymentSnapshot?.notes?.local_module);
    const noteRefType = String(paymentSnapshot?.notes?.reference_type || "").trim().toLowerCase().slice(0, 40);
    const noteRefId = String(paymentSnapshot?.notes?.reference_id || "").trim().slice(0, 120);
    const module = moduleRequested || noteLocalModule || "payment";
    const refType = referenceType || noteRefType || module;
    const refId = referenceId || noteRefId || paymentId;
    const amountInr = roundMoney(amountPaise / 100);

    const paymentRows = await localServicesSupabaseRequest("transactions", "POST", {
      body: [{
        owner_user_id: userId,
        owner_type: "customer",
        module: "payment",
        transaction_type: "payment_capture",
        reference_type: refType,
        reference_id: refId,
        payer_user_id: userId,
        payee_user_id: null,
        amount_inr: amountInr,
        gross_inr: amountInr,
        commission_inr: 0,
        net_inr: amountInr,
        platform_share_inr: 0,
        payment_method: "online",
        payment_status: "captured",
        payment_gateway: "razorpay",
        payment_order_id: orderId,
        payment_id: paymentId,
        payment_ref: paymentId,
        metadata: {
          module,
          razorpay_status: status
        },
        status: "captured",
        settled_at: new Date().toISOString(),
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      }],
      query: { on_conflict: "module,transaction_type,payment_id" },
      prefer: "resolution=merge-duplicates,return=representation"
    });

    if(refType === "order"){
      await localServicesSupabaseRequest("local_orders", "PATCH", {
        query: { id: `eq.${refId}` },
        body: {
          payment_method: "online",
          payment_status: "captured",
          payment_order_id: orderId,
          payment_id: paymentId,
          payment_ref: paymentId,
          updated_at: new Date().toISOString()
        },
        prefer: "return=minimal"
      }).catch(() => {});
    }else if(refType === "ride"){
      await localServicesSupabaseRequest("local_ride_requests", "PATCH", {
        query: { id: `eq.${refId}` },
        body: {
          payment_method: "online",
          payment_status: "captured",
          payment_order_id: orderId,
          payment_id: paymentId,
          payment_ref: paymentId,
          updated_at: new Date().toISOString()
        },
        prefer: "return=minimal"
      }).catch(() => {});
    }else if(refType === "service_booking"){
      await localServicesSupabaseRequest("local_agent_bookings", "PATCH", {
        query: { id: `eq.${refId}` },
        body: {
          payment_method: "online",
          payment_status: "captured",
          payment_order_id: orderId,
          payment_id: paymentId,
          payment_ref: paymentId,
          updated_at: new Date().toISOString()
        },
        prefer: "return=minimal"
      }).catch(() => {});
    }

    return res.json({
      ok:true,
      payment: Array.isArray(paymentRows) ? paymentRows[0] || null : null,
      amount_inr: amountInr,
      reference_type: refType,
      reference_id: refId,
      module
    });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_payment_verify_failed", message:String(err?.message || "") });
  }
});

app.get("/api/local/wallet", async (req, res) => {
  const userId = sanitizeUserId(req.query?.user_id);
  const ownerTypeInput = sanitizeWalletOwnerType(req.query?.owner_type);
  const isAdmin = isAdminAutomationRequest(req);
  if(!userId && ownerTypeInput !== "platform"){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  try{
    if(ownerTypeInput){
      if(ownerTypeInput === "platform" && !isAdmin){
        return res.status(403).json({ ok:false, error:"forbidden_platform_wallet" });
      }
      const ownerUserId = ownerTypeInput === "platform" ? "platform" : userId;
      const wallet = await ensureWalletRow(ownerUserId, ownerTypeInput);
      return res.json({ ok:true, wallet });
    }
    const rows = await localServicesSupabaseRequest("wallets", "GET", {
      query: {
        select: "id,owner_user_id,owner_type,balance_inr,currency,status,updated_at",
        owner_user_id: `eq.${userId}`,
        order: "updated_at.desc",
        limit: "20"
      }
    });
    return res.json({ ok:true, wallets: Array.isArray(rows) ? rows : [] });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_wallet_fetch_failed", message:String(err?.message || "") });
  }
});

app.get("/api/local/transactions", async (req, res) => {
  const userId = sanitizeUserId(req.query?.user_id);
  const ownerType = sanitizeWalletOwnerType(req.query?.owner_type);
  const module = sanitizeLocalModule(req.query?.module);
  const isAdmin = isAdminAutomationRequest(req);
  const limit = Math.max(1, Math.min(500, Math.floor(safeNumber(req.query?.limit || 80))));
  if(!userId && !isAdmin){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  try{
    const query = {
      select: "id,owner_user_id,owner_type,module,transaction_type,reference_type,reference_id,amount_inr,gross_inr,commission_inr,net_inr,payment_method,payment_status,payment_gateway,payment_order_id,payment_id,payment_ref,status,settled_at,created_at,updated_at,metadata",
      order: "created_at.desc",
      limit: String(limit)
    };
    if(!isAdmin){
      query.owner_user_id = `eq.${userId}`;
    }else if(userId){
      query.owner_user_id = `eq.${userId}`;
    }
    if(ownerType){
      query.owner_type = `eq.${ownerType}`;
    }
    if(module){
      query.module = `eq.${module}`;
    }
    const rows = await localServicesSupabaseRequest("transactions", "GET", { query });
    return res.json({ ok:true, transactions: Array.isArray(rows) ? rows : [] });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_transactions_fetch_failed", message:String(err?.message || "") });
  }
});

app.get("/api/local/payouts/weekly", async (req, res) => {
  const userId = sanitizeUserId(req.query?.user_id);
  const ownerType = sanitizeWalletOwnerType(req.query?.owner_type);
  const module = sanitizeLocalModule(req.query?.module);
  const weekStart = String(req.query?.week_start || "").trim().slice(0, 10);
  const isAdmin = isAdminAutomationRequest(req);
  if(!userId && !isAdmin){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  try{
    const query = {
      select: "id,owner_user_id,owner_type,module,week_start_date,week_end_date,gross_inr,commission_inr,payout_inr,status,paid_at,paid_ref,created_at,updated_at",
      order: "week_start_date.desc",
      limit: "120"
    };
    if(!isAdmin){
      query.owner_user_id = `eq.${userId}`;
    }else if(userId){
      query.owner_user_id = `eq.${userId}`;
    }
    if(ownerType){
      query.owner_type = `eq.${ownerType}`;
    }
    if(module){
      query.module = `eq.${module}`;
    }
    if(/^\d{4}-\d{2}-\d{2}$/.test(weekStart)){
      query.week_start_date = `eq.${weekStart}`;
    }
    const rows = await localServicesSupabaseRequest("weekly_payouts", "GET", { query });
    return res.json({ ok:true, payouts: Array.isArray(rows) ? rows : [] });
  }catch(err){
    return res.status(500).json({ ok:false, error:"weekly_payout_fetch_failed", message:String(err?.message || "") });
  }
});

app.get("/api/local/listings/for-owner", async (req, res) => {
  const userId = sanitizeUserId(req.query?.user_id);
  const type = sanitizeListingType(req.query?.listing_type || req.query?.type);
  if(!userId){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  try{
    const query = {
      select: "id,user_id,store_name,listing_type,phone,image_url,lat,lng,status,open_now,delivery_charge_inr,minimum_order_inr,open_time,close_time,self_delivery,ride_vehicle_type,vehicle_number,base_fare_inr,per_km_rate_inr,per_min_rate_inr,service_radius_km,updated_at,created_at",
      user_id: `eq.${userId}`,
      order: "updated_at.desc",
      limit: "120"
    };
    if(type){
      query.listing_type = `eq.${type}`;
    }
    const rows = await localServicesSupabaseRequest("local_listings", "GET", { query });
    return res.json({ ok:true, listings: Array.isArray(rows) ? rows : [] });
  }catch(err){
    return res.status(500).json({ ok:false, error:"owner_listings_fetch_failed", message:String(err?.message || "") });
  }
});

app.get("/api/local/agents/for-owner", async (req, res) => {
  const userId = sanitizeUserId(req.query?.user_id);
  if(!userId){
    return res.status(400).json({ ok:false, error:"user_id_required" });
  }
  try{
    const rows = await localServicesSupabaseRequest("local_agents", "GET", {
      query: {
        select: "id,user_id,service_category,title,phone,price_per_visit_inr,per_hour_rate_inr,experience_years,service_radius_km,rating,rating_count,lat,lng,image_url,status,available_now,updated_at,created_at",
        user_id: `eq.${userId}`,
        order: "updated_at.desc",
        limit: "120"
      }
    });
    return res.json({ ok:true, agents: Array.isArray(rows) ? rows : [] });
  }catch(err){
    return res.status(500).json({ ok:false, error:"owner_agents_fetch_failed", message:String(err?.message || "") });
  }
});

app.post("/api/local/agents/availability", async (req, res) => {
  const userId = sanitizeUserId(req.body?.user_id);
  const agentId = String(req.body?.agent_id || "").trim();
  const availableNow = Boolean(req.body?.available_now);
  if(!userId || !agentId){
    return res.status(400).json({ ok:false, error:"user_id_and_agent_id_required" });
  }
  try{
    const rows = await localServicesSupabaseRequest("local_agents", "PATCH", {
      query: { id: `eq.${agentId}`, user_id: `eq.${userId}` },
      body: { available_now: availableNow, updated_at: new Date().toISOString() },
      prefer: "return=representation"
    });
    return res.json({ ok:true, agent: Array.isArray(rows) ? rows[0] || null : null });
  }catch(err){
    return res.status(500).json({ ok:false, error:"agent_availability_update_failed", message:String(err?.message || "") });
  }
});

app.post("/api/local/listings/item", async (req, res) => {
  const userId = sanitizeUserId(req.body?.user_id);
  const listingId = String(req.body?.listing_id || "").trim();
  const name = String(req.body?.name || "").trim().slice(0, 120);
  const priceInr = Math.max(0, safeNumber(req.body?.price_inr));
  const stockQty = Math.max(0, Math.floor(safeNumber(req.body?.stock_qty)));
  const imageUrl = String(req.body?.image_url || "").trim().slice(0, 3000);
  const category = String(req.body?.category || "").trim().slice(0, 80);
  if(!userId || !listingId || !name || priceInr <= 0){
    return res.status(400).json({ ok:false, error:"invalid_item_payload" });
  }
  if(localRateLimitHit(`local_listing_item:${userId}`, 80, 10 * 60 * 1000)){
    return res.status(429).json({ ok:false, error:"rate_limited" });
  }
  try{
    const sellerAllowed = await hasActiveLocalRole(userId, "seller");
    if(!sellerAllowed){
      return res.status(403).json({ ok:false, error:"seller_role_required" });
    }
    const listingRows = await localServicesSupabaseRequest("local_listings", "GET", {
      query: { select:"id,user_id,listing_type,status", id:`eq.${listingId}`, limit:"1" }
    });
    const listing = Array.isArray(listingRows) ? listingRows[0] : null;
    if(!listing || sanitizeUserId(listing.user_id) !== userId){
      return res.status(403).json({ ok:false, error:"listing_owner_required" });
    }
    if(String(listing.status || "") !== "approved"){
      return res.status(409).json({ ok:false, error:"listing_not_approved" });
    }
    if(!["food","grocery"].includes(String(listing.listing_type || ""))){
      return res.status(400).json({ ok:false, error:"items_only_for_food_or_grocery" });
    }
    const rows = await localServicesSupabaseRequest("local_listing_items", "POST", {
      body: [{
        listing_id: listingId,
        seller_user_id: userId,
        name,
        category: category || null,
        price_inr: Math.round(priceInr * 100) / 100,
        stock_qty: stockQty,
        image_url: imageUrl || null,
        is_active: true,
        updated_at: new Date().toISOString()
      }],
      query: { on_conflict: "listing_id,name" },
      prefer: "resolution=merge-duplicates,return=representation"
    });
    return res.json({ ok:true, item: Array.isArray(rows) ? rows[0] || null : null });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_item_upsert_failed", message:String(err?.message || "") });
  }
});

app.post("/api/local/listings/item/update", async (req, res) => {
  const userId = sanitizeUserId(req.body?.user_id);
  const itemId = String(req.body?.item_id || "").trim();
  const name = String(req.body?.name || "").trim().slice(0, 120);
  const category = String(req.body?.category || "").trim().slice(0, 80);
  const priceInr = Math.max(0, safeNumber(req.body?.price_inr));
  const stockQty = Math.max(0, Math.floor(safeNumber(req.body?.stock_qty)));
  const imageUrl = String(req.body?.image_url || "").trim().slice(0, 3000);
  const isActive = req.body?.is_active === undefined ? true : Boolean(req.body?.is_active);
  if(!userId || !itemId){
    return res.status(400).json({ ok:false, error:"invalid_item_update_payload" });
  }
  try{
    const sellerAllowed = await hasActiveLocalRole(userId, "seller");
    if(!sellerAllowed){
      return res.status(403).json({ ok:false, error:"seller_role_required" });
    }
    const rows = await localServicesSupabaseRequest("local_listing_items", "PATCH", {
      query: { id: `eq.${itemId}`, seller_user_id: `eq.${userId}` },
      body: {
        name: name || undefined,
        category: category || null,
        price_inr: priceInr > 0 ? roundMoney(priceInr) : undefined,
        stock_qty: Number.isFinite(stockQty) ? stockQty : undefined,
        image_url: imageUrl || null,
        is_active: isActive,
        updated_at: new Date().toISOString()
      },
      prefer: "return=representation"
    });
    const item = Array.isArray(rows) ? rows[0] || null : null;
    if(!item){
      return res.status(404).json({ ok:false, error:"item_not_found_or_forbidden" });
    }
    return res.json({ ok:true, item });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_item_update_failed", message:String(err?.message || "") });
  }
});

app.post("/api/local/listings/item/delete", async (req, res) => {
  const userId = sanitizeUserId(req.body?.user_id);
  const itemId = String(req.body?.item_id || "").trim();
  if(!userId || !itemId){
    return res.status(400).json({ ok:false, error:"invalid_item_delete_payload" });
  }
  try{
    const sellerAllowed = await hasActiveLocalRole(userId, "seller");
    if(!sellerAllowed){
      return res.status(403).json({ ok:false, error:"seller_role_required" });
    }
    const rows = await localServicesSupabaseRequest("local_listing_items", "PATCH", {
      query: { id: `eq.${itemId}`, seller_user_id: `eq.${userId}` },
      body: {
        is_active: false,
        updated_at: new Date().toISOString()
      },
      prefer: "return=representation"
    });
    const item = Array.isArray(rows) ? rows[0] || null : null;
    if(!item){
      return res.status(404).json({ ok:false, error:"item_not_found_or_forbidden" });
    }
    return res.json({ ok:true, item });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_item_delete_failed", message:String(err?.message || "") });
  }
});

app.get("/api/local/listings/items", async (req, res) => {
  const listingId = String(req.query?.listing_id || "").trim();
  const includeInactive = String(req.query?.include_inactive || "").trim() === "1";
  const userId = sanitizeUserId(req.query?.user_id);
  if(!listingId){
    return res.status(400).json({ ok:false, error:"listing_id_required" });
  }
  try{
    if(includeInactive){
      if(!userId){
        return res.status(400).json({ ok:false, error:"user_id_required_for_include_inactive" });
      }
      const listingRows = await localServicesSupabaseRequest("local_listings", "GET", {
        query: { select: "id,user_id", id: `eq.${listingId}`, limit: "1" }
      });
      const listing = Array.isArray(listingRows) ? listingRows[0] || null : null;
      if(!listing || sanitizeUserId(listing.user_id) !== userId){
        return res.status(403).json({ ok:false, error:"listing_owner_required" });
      }
    }
    const query = {
      select: "id,listing_id,seller_user_id,name,category,price_inr,stock_qty,image_url,is_active,created_at,updated_at",
      listing_id: `eq.${listingId}`,
      order: "updated_at.desc",
      limit: "300"
    };
    if(!includeInactive){
      query.is_active = "eq.true";
    }
    const rows = await localServicesSupabaseRequest("local_listing_items", "GET", {
      query
    });
    return res.json({ ok:true, items: Array.isArray(rows) ? rows : [] });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_items_fetch_failed", message:String(err?.message || "") });
  }
});

app.post("/api/local/agents/create", async (req, res) => {
  const userId = sanitizeUserId(req.body?.user_id);
  const serviceCategory = sanitizeAgentCategory(req.body?.service_category);
  const title = String(req.body?.title || req.body?.name || "").trim().slice(0, 120);
  const phone = String(req.body?.phone || "").trim().slice(0, 30);
  const pricePerVisitInr = Math.max(0, safeNumber(req.body?.price_per_visit_inr ?? req.body?.base_visit_charge_inr));
  const perHourRateInr = Math.max(0, safeNumber(req.body?.per_hour_rate_inr));
  const experienceYears = Math.max(0, Math.min(60, Math.floor(safeNumber(req.body?.experience_years))));
  const serviceRadiusKm = Math.max(1, Math.min(50, Math.round(safeNumber(req.body?.service_radius_km || 5))));
  const rating = Math.max(0, Math.min(5, safeNumber(req.body?.rating || 0)));
  const ratingCount = Math.max(0, Math.floor(safeNumber(req.body?.rating_count || 0)));
  const lat = sanitizeGeoNumber(req.body?.lat);
  const lng = sanitizeGeoNumber(req.body?.lng);
  const imageUrl = String(req.body?.image_url || "").trim().slice(0, 3000);
  const availableNow = req.body?.available_now === undefined ? true : Boolean(req.body?.available_now);
  if(!userId || !serviceCategory || !title || !phone || lat === null || lng === null){
    return res.status(400).json({ ok:false, error:"invalid_agent_payload" });
  }
  if(localRateLimitHit(`local_agent_create:${userId}`, 20, 10 * 60 * 1000)){
    return res.status(429).json({ ok:false, error:"rate_limited" });
  }
  try{
    const agentAllowed = await hasActiveLocalRole(userId, "agent");
    if(!agentAllowed){
      return res.status(403).json({ ok:false, error:"agent_role_required" });
    }
    const rows = await localServicesSupabaseRequest("local_agents", "POST", {
      body: [{
        user_id: userId,
        service_category: serviceCategory,
        title,
        phone,
        price_per_visit_inr: Math.round(pricePerVisitInr * 100) / 100,
        per_hour_rate_inr: Math.round(perHourRateInr * 100) / 100,
        experience_years: experienceYears,
        service_radius_km: serviceRadiusKm,
        rating: Math.round(rating * 10) / 10,
        rating_count: ratingCount,
        lat,
        lng,
        image_url: imageUrl || null,
        status: "active",
        available_now: availableNow,
        updated_at: new Date().toISOString()
      }],
      query: { on_conflict: "user_id,service_category" },
      prefer: "resolution=merge-duplicates,return=representation"
    });
    return res.json({ ok:true, agent: Array.isArray(rows) ? rows[0] || null : null });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_agent_upsert_failed", message:String(err?.message || "") });
  }
});

app.get("/api/local/agents/nearby", async (req, res) => {
  const serviceCategory = sanitizeAgentCategory(req.query?.service_category);
  const lat = sanitizeGeoNumber(req.query?.lat);
  const lng = sanitizeGeoNumber(req.query?.lng);
  const radiusKm = Math.min(50, Math.max(1, Math.round(safeNumber(req.query?.radius_km || 30))));
  if(lat === null || lng === null){
    return res.status(400).json({ ok:false, error:"lat_lng_required" });
  }
  try{
    const query = {
      select: "id,user_id,service_category,title,phone,price_per_visit_inr,per_hour_rate_inr,experience_years,service_radius_km,rating,rating_count,lat,lng,image_url,available_now,status",
      status: "eq.active",
      available_now: "eq.true",
      limit: "300"
    };
    if(serviceCategory){
      query.service_category = `eq.${serviceCategory}`;
    }
    const rows = await localServicesSupabaseRequest("local_agents", "GET", { query });
    const agents = (Array.isArray(rows) ? rows : [])
      .map((row) => {
        const la = sanitizeGeoNumber(row?.lat);
        const ln = sanitizeGeoNumber(row?.lng);
        if(la === null || ln === null) return null;
        const distance_km = localDistanceKm(lat, lng, la, ln);
        return {
          ...row,
          distance_km: Math.round(distance_km * 100) / 100,
          service_radius_km: Math.max(1, Math.round(safeNumber(row?.service_radius_km || 5)))
        };
      })
      .filter(Boolean)
      .filter(row => row.distance_km <= Math.min(radiusKm, Number(row.service_radius_km || radiusKm)))
      .sort((a, b) => a.distance_km - b.distance_km);
    return res.json({ ok:true, agents });
  }catch(err){
    return res.json({ ok:true, agents: [], warning: "agents_temporarily_unavailable" });
  }
});

app.post("/api/local/agents/book", async (req, res) => {
  const customerUserId = sanitizeUserId(req.body?.customer_user_id);
  const agentId = String(req.body?.agent_id || "").trim();
  const serviceAddress = String(req.body?.service_address || "").trim().slice(0, 240);
  const note = String(req.body?.note || "").trim().slice(0, 300);
  const date = String(req.body?.service_date || "").trim().slice(0, 20);
  const time = String(req.body?.service_time || "").trim().slice(0, 20);
  const scheduledAtRaw = String(req.body?.scheduled_at || "").trim();
  const hoursBooked = Math.max(1, Math.min(12, Math.round(safeNumber(req.body?.hours_booked || 1))));
  const estimatedPriceInput = Math.max(0, safeNumber(req.body?.estimated_price_inr));
  const paymentMethod = sanitizePaymentMethod(req.body?.payment_method || "cash");
  const paymentStatus = sanitizePaymentStatus(req.body?.payment_status || (paymentMethod === "cash" ? "cod" : "pending"));
  const paymentId = String(req.body?.payment_id || "").trim().slice(0, 120);
  const paymentOrderId = String(req.body?.payment_order_id || "").trim().slice(0, 120);
  const paymentRef = String(req.body?.payment_ref || "").trim().slice(0, 120);
  let scheduledAt = null;
  if(scheduledAtRaw){
    const ts = Date.parse(scheduledAtRaw);
    if(Number.isFinite(ts)) scheduledAt = new Date(ts).toISOString();
  }else if(/^\d{4}-\d{2}-\d{2}$/.test(date) && /^\d{2}:\d{2}/.test(time)){
    const iso = `${date}T${time.length === 5 ? `${time}:00` : time}Z`;
    const ts = Date.parse(iso);
    if(Number.isFinite(ts)) scheduledAt = new Date(ts).toISOString();
  }
  if(!customerUserId || !agentId || !serviceAddress){
    return res.status(400).json({ ok:false, error:"invalid_agent_booking_payload" });
  }
  if(localRateLimitHit(`local_agent_book:${customerUserId}`, 30, 10 * 60 * 1000)){
    return res.status(429).json({ ok:false, error:"rate_limited" });
  }
  try{
    const consumerAllowed = await hasActiveLocalRole(customerUserId, "consumer");
    if(!consumerAllowed){
      return res.status(403).json({ ok:false, error:"consumer_role_required" });
    }
    const rows = await localServicesSupabaseRequest("local_agents", "GET", {
      query: {
        select:"id,user_id,title,phone,status,price_per_visit_inr,per_hour_rate_inr",
        id:`eq.${agentId}`,
        limit:"1"
      }
    });
    const agent = Array.isArray(rows) ? rows[0] : null;
    if(!agent || String(agent.status || "") !== "active"){
      return res.status(404).json({ ok:false, error:"agent_not_available" });
    }
    const baseVisit = Math.max(0, safeNumber(agent?.price_per_visit_inr || 0));
    const perHour = Math.max(0, safeNumber(agent?.per_hour_rate_inr || 0));
    const estimatedPriceInr = estimatedPriceInput > 0
      ? roundMoney(estimatedPriceInput)
      : roundMoney(baseVisit + (perHour * hoursBooked));
    if(estimatedPriceInr <= 0){
      return res.status(400).json({ ok:false, error:"estimated_price_required" });
    }
    if(paymentMethod !== "cash" && paymentStatus !== "captured"){
      return res.status(402).json({
        ok:false,
        error:"payment_not_verified",
        message:"Online booking requires captured payment before create."
      });
    }
    const createdRows = await localServicesSupabaseRequest("local_agent_bookings", "POST", {
      body: [{
        customer_user_id: customerUserId,
        agent_user_id: sanitizeUserId(agent.user_id),
        agent_id: agentId,
        service_address: serviceAddress,
        note,
        scheduled_at: scheduledAt,
        hours_booked: hoursBooked,
        estimated_price_inr: estimatedPriceInr,
        payment_method: paymentMethod,
        payment_status: paymentStatus,
        payment_order_id: paymentOrderId || null,
        payment_id: paymentId || null,
        payment_ref: paymentRef || null,
        commission_inr: 0,
        agent_earning_inr: 0,
        status: "requested",
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      }],
      prefer: "return=representation"
    });
    const created = Array.isArray(createdRows) ? createdRows[0] || null : null;
    const state = await readSystemState();
    const tokens = getPushTokensForUser(state, sanitizeUserId(agent.user_id));
    sendFcmNotificationToTokens(tokens, {
      title: "New service booking",
      body: "You have a new customer request.",
      data: { type:"agent_booking", booking_id:String(created?.id || "") }
    }).catch(() => {});
    return res.json({ ok:true, booking: created });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_agent_booking_failed", message:String(err?.message || "") });
  }
});

app.post("/api/local/riders/location", async (req, res) => {
  const userId = sanitizeUserId(req.body?.user_id);
  const lat = sanitizeGeoNumber(req.body?.lat);
  const lng = sanitizeGeoNumber(req.body?.lng);
  const isOnline = req.body?.is_online === undefined ? true : Boolean(req.body?.is_online);
  const accuracyMeters = Math.max(0, safeNumber(req.body?.accuracy_m || req.body?.accuracy));
  const headingDeg = Number.isFinite(Number(req.body?.heading_deg)) ? Number(req.body?.heading_deg) : null;
  const speedKmph = Number.isFinite(Number(req.body?.speed_kmph)) ? Number(req.body?.speed_kmph) : null;
  if(!userId || lat === null || lng === null){
    return res.status(400).json({ ok:false, error:"invalid_rider_location_payload" });
  }
  if(localRateLimitHit(`rider_location:${userId}`, 240, 10 * 60 * 1000)){
    return res.status(429).json({ ok:false, error:"rate_limited" });
  }
  try{
    const riderAllowed = await hasActiveLocalRole(userId, "rider");
    if(!riderAllowed){
      return res.status(403).json({ ok:false, error:"rider_role_required" });
    }
    const rows = await localServicesSupabaseRequest("local_rider_locations", "POST", {
      body: [{
        user_id: userId,
        lat,
        lng,
        is_online: isOnline,
        accuracy_m: accuracyMeters > 0 ? roundMoney(accuracyMeters) : null,
        heading_deg: headingDeg,
        speed_kmph: speedKmph,
        updated_at: new Date().toISOString()
      }],
      query: { on_conflict: "user_id" },
      prefer: "resolution=merge-duplicates,return=representation"
    });
    return res.json({ ok:true, rider_location: Array.isArray(rows) ? rows[0] || null : null });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_rider_location_failed", message:String(err?.message || "") });
  }
});

app.get("/api/local/riders/nearby", async (req, res) => {
  const lat = sanitizeGeoNumber(req.query?.lat);
  const lng = sanitizeGeoNumber(req.query?.lng);
  const radiusRaw = safeNumber(req.query?.radius_km);
  const limitRaw = Math.floor(safeNumber(req.query?.limit));
  if(lat === null || lng === null){
    return res.status(400).json({ ok:false, error:"lat_lng_required" });
  }
  const radiusKm = Number.isFinite(radiusRaw) && radiusRaw > 0 ? Math.min(20, radiusRaw) : RIDE_MATCH_RADIUS_KM;
  const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(100, limitRaw) : 20;
  try{
    const drivers = await fetchNearbyRideDrivers(lat, lng, limit, { radiusKm });
    return res.json({
      ok:true,
      radius_km: radiusKm,
      count: drivers.length,
      drivers
    });
  }catch(err){
    return res.status(500).json({ ok:false, error:"nearby_riders_failed", message:String(err?.message || "") });
  }
});

app.get("/api/local/rides/track", async (req, res) => {
  const requestId = String(req.query?.request_id || "").trim();
  if(!requestId){
    return res.status(400).json({ ok:false, error:"request_id_required" });
  }
  try{
    const rows = await localServicesSupabaseRequest("local_ride_requests", "GET", {
      query: {
        select: "id,rider_user_id,driver_user_id,status,pickup_text,drop_text,pickup_lat,pickup_lng,drop_lat,drop_lng,vehicle_type,fare_inr,distance_km,duration_min,payment_method,payment_status,payment_order_id,payment_id,updated_at,otp_verified",
        id: `eq.${requestId}`,
        limit: "1"
      }
    });
    const request = Array.isArray(rows) ? rows[0] || null : null;
    if(!request){
      return res.status(404).json({ ok:false, error:"ride_request_not_found" });
    }
    let driverLocation = null;
    let driverPhone = "";
    let driverProfile = null;
    if(request.driver_user_id){
      const locRows = await localServicesSupabaseRequest("local_rider_locations", "GET", {
        query: {
          select: "user_id,lat,lng,is_online,accuracy_m,heading_deg,speed_kmph,updated_at",
          user_id: `eq.${sanitizeUserId(request.driver_user_id)}`,
          limit: "1"
        }
      });
      driverLocation = Array.isArray(locRows) ? locRows[0] || null : null;
      const driverListingRows = await localServicesSupabaseRequest("local_listings", "GET", {
        query: {
          select: "phone,listing_type,user_id,store_name,image_url,vehicle_number,ride_vehicle_type",
          user_id: `eq.${sanitizeUserId(request.driver_user_id)}`,
          listing_type: "eq.ride",
          limit: "1"
        }
      });
      const listing = Array.isArray(driverListingRows) ? driverListingRows[0] || null : null;
      driverPhone = String(listing?.phone || "").trim();
      driverProfile = listing ? {
        name: String(listing.store_name || "").trim(),
        image_url: String(listing.image_url || "").trim(),
        vehicle_number: String(listing.vehicle_number || "").trim(),
        vehicle_type: sanitizeRideVehicleType(listing.ride_vehicle_type || request.vehicle_type)
      } : null;
    }
    return res.json({
      ok:true,
      ride_request: mapRideStatusRowPublic(request),
      driver_location: driverLocation,
      driver_phone: driverPhone,
      driver_profile: driverProfile
    });
  }catch(err){
    return res.status(500).json({ ok:false, error:"local_ride_track_failed", message:String(err?.message || "") });
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

const localOpsSweepTimer = setInterval(() => {
  (async () => {
    if(!isLocalServicesSupabaseConfigured()) return;
    const now = Date.now();
    const isoBefore = (minutes) => new Date(now - (minutes * 60 * 1000)).toISOString();
    try{
      const staleRideRows = await localServicesSupabaseRequest("local_ride_requests", "GET", {
        query: {
          select: "id,status,created_at",
          status: "eq.searching",
          created_at: `lt.${isoBefore(15)}`,
          limit: "200"
        }
      });
      for(const ride of (Array.isArray(staleRideRows) ? staleRideRows : [])){
        try{
          await localServicesSupabaseRequest("local_ride_requests", "PATCH", {
            query: { id: `eq.${String(ride.id || "").trim()}` },
            body: { status: "cancelled", updated_at: new Date().toISOString() },
            prefer: "return=minimal"
          });
        }catch(_){ }
      }

      const staleOrderRows = await localServicesSupabaseRequest("local_orders", "GET", {
        query: {
          select: "id,status,created_at",
          status: "eq.placed",
          created_at: `lt.${isoBefore(60)}`,
          limit: "300"
        }
      });
      for(const order of (Array.isArray(staleOrderRows) ? staleOrderRows : [])){
        try{
          await localServicesSupabaseRequest("local_orders", "PATCH", {
            query: { id: `eq.${String(order.id || "").trim()}` },
            body: { status: "cancelled", updated_at: new Date().toISOString() },
            prefer: "return=minimal"
          });
        }catch(_){ }
      }
    }catch(_){ }
  })();
}, 120 * 1000);
if(localOpsSweepTimer && typeof localOpsSweepTimer.unref === "function"){
  localOpsSweepTimer.unref();
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
