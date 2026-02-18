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
const FREE_PLAN_DAILY_LIMIT = 2;
const PRO_PLAN_DAILY_LIMIT = 20;
const TRYON_PRICE_PER_IMAGE_USD = 1;
const ADMIN_DM_EMAIL = String(process.env.TRYON_ADMIN_EMAIL || "prashikbhalerao0208@gmail.com").trim();
const ADMIN_DM_WEBHOOK_URL = String(process.env.TRYON_ADMIN_DM_WEBHOOK || "").trim();
const RESEND_API_KEY = String(process.env.RESEND_API_KEY || "").trim();
const RESEND_FROM_EMAIL = String(process.env.RESEND_FROM_EMAIL || "onboarding@resend.dev").trim();

const OPENAI_API_KEY = String(process.env.OPENAI_API_KEY || "").trim();

app.use(express.json({ limit: "15mb" }));
app.use(cors());
app.use("/uploads", express.static(UPLOADS_DIR));
app.use(express.static(__dirname, {
  dotfiles: "deny",
  etag: true,
  maxAge: "1h"
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

app.get("/api/public/config", (req, res) => {
  res.setHeader("Cache-Control", "public, max-age=120");
  res.json({
    supabaseUrl: PUBLIC_SUPABASE_URL,
    supabaseAnonKey: PUBLIC_SUPABASE_ANON_KEY,
    razorpayKeyId: PUBLIC_RAZORPAY_KEY_ID
  });
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

    return res.json({
      ok: true,
      verified: true,
      razorpay_order_id: orderId,
      razorpay_payment_id: paymentId,
      payment_status: paymentStatus || "verified",
      payment_currency: paymentCurrency || "INR",
      payment_amount_paise: paymentAmountPaise
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

app.options("/tryon", (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.sendStatus(204);
});

app.post("/tryon", upload.single("userImage"), async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");

  const userId = String(req.body?.user_id || "guest").trim() || "guest";
  const username = String(req.body?.username || req.body?.user_name || "guest").trim() || "guest";
  const userEmail = String(req.body?.user_email || "").trim();
  const planCode = normalizePlanCode(req.body?.user_plan);
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
  const manualPayload = {
    userId,
    username,
    userEmail,
    plan: planCode,
    language,
    userImageBuffer: userImage,
    userImageMime,
    productInfo
  };

  try{
    const usageStore = app.locals.tryonUsage || (app.locals.tryonUsage = new Map());
    const now = new Date();
    const dateKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")}`;
    const usageKey = `${userId}::${dateKey}`;
    const used = usageStore.get(usageKey) || 0;
    const limit = getPlanDailyLimit(planCode);

    if(limit !== Infinity && used >= limit){
      return res.json({
        status: "limit_reached",
        plan: planCode,
        limit,
        upgrade_prompt: "Daily limit reached. Upgrade your subscription to continue."
      });
    }

    if(limit !== Infinity){
      usageStore.set(usageKey, used + 1);
    }

    if(!userImage || !userImage.length){
      return res.json(manualProcessingResponse(planCode));
    }

    const productImageUrl = String(productInfo.image || req.body?.product_url || "").trim();
    const productFetch = await fetchImageBuffer(productImageUrl);
    if(!productFetch.ok || !productFetch.buffer || !productFetch.buffer.length){
      const saved = await saveManualRequest(manualPayload);
      return res.json(manualProcessingResponse(planCode, { request_id: saved?.request_id || "" }));
    }

    if (!FormDataCtor) {
      const saved = await saveManualRequest(manualPayload);
      return res.json(manualProcessingResponse(planCode, { request_id: saved?.request_id || "" }));
    }

    const form = new FormDataCtor();
    form.append("model", "gpt-image-1.5");
    const productMime = productFetch.contentType || "image/jpeg";
    if (FormDataCtor === global.FormData && BlobCtor) {
      form.append("image[]", new BlobCtor([userImage], { type: userImageMime }), "user.jpg");
      form.append("image[]", new BlobCtor([productFetch.buffer], { type: productMime }), "product.jpg");
    } else {
      form.append("image[]", userImage, "user.jpg");
      form.append("image[]", productFetch.buffer, "product.jpg");
    }
    form.append(
      "prompt",
      "Make the person wear the product naturally. Perfect fit, realistic folds, lighting and shadows. Do not change background. Photorealistic."
    );

    const headers = { Authorization: `Bearer ${OPENAI_API_KEY}` };
    if (typeof form.getHeaders === "function") {
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
      return res.json(manualProcessingResponse(planCode, { request_id: saved?.request_id || "" }));
    }

    const imageBase64 = data?.data?.[0]?.b64_json || "";
    const image = imageBase64 ? `data:image/png;base64,${imageBase64}` : "";
    if(!image){
      const saved = await saveManualRequest(manualPayload);
      return res.json(manualProcessingResponse(planCode, { request_id: saved?.request_id || "" }));
    }

    return res.json({
      status: "success",
      image
    });

  } catch (err) {
    console.error("Try-on error:", err?.stack || err);
    let saved = null;
    try{
      if(userImage && userImage.length){
        saved = await saveManualRequest(manualPayload);
      }
    }catch(queueErr){
      console.error("Manual request save failed:", queueErr?.stack || queueErr);
    }
    return res.json(manualProcessingResponse(planCode, { request_id: saved?.request_id || "" }));
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
