/* ================= SUPABASE CONFIG ================= */
let supabase = null;
let supabaseReadyPromise = null;
const DEFAULT_REMOTE_API_BASE = "https://novagapp-mart.onrender.com";
const APP_PUBLIC_ORIGIN = "https://novagapp-mart.onrender.com";

function normalizePublicConfig(raw){
  return {
    supabaseUrl: String(raw?.supabaseUrl || "").trim(),
    supabaseAnonKey: String(raw?.supabaseAnonKey || "").trim()
  };
}

function normalizeBase(value){
  return String(value || "").trim().replace(/\/+$/g, "");
}

function isLocalRuntime(){
  return String(location.protocol || "") === "file:";
}

function isLoopbackBase(base){
  const value = String(base || "").trim().toLowerCase();
  return value.startsWith("http://");
}

function deriveDisplayFromEmail(email, fallback){
  const raw = String(email || "").trim().toLowerCase();
  const local = String((raw.split("@")[0] || "")).replace(/[._-]+/g, " ").replace(/\d+/g, " ").trim();
  const token = String(fallback || local || "Member").trim();
  return token
    .split(/\s+/)
    .map(part => part ? (part[0].toUpperCase() + part.slice(1).toLowerCase()) : "")
    .filter(Boolean)
    .join(" ")
    .slice(0, 60) || "Member";
}

function buildApiCandidates(){
  const out = [];
  const push = (raw) => {
    const value = String(raw || "").trim().replace(/\/+$/g, "");
    if(!value) return;
    if(!/^https?:\/\//i.test(value)) return;
    if(!out.includes(value)) out.push(value);
  };
  try{
    push(window.CONTEST_API_BASE);
    push(window.API_BASE);
    push(localStorage.getItem("contest_api_base"));
    push(localStorage.getItem("api_base"));
  }catch(_){ }
  if(/^https?:\/\//i.test(location.origin || "")) push(location.origin);
  push(DEFAULT_REMOTE_API_BASE);
  return out;
}

async function syncAccountIdentity(user){
  if(!user?.id) return false;
  const displayName = deriveDisplayFromEmail(
    user.email,
    user.user_metadata?.full_name || user.user_metadata?.name || ""
  );
  const payload = {
    user_id: user.id,
    email: user.email || "",
    full_name: displayName,
    display_name: displayName,
    username: String((user.email || "").split("@")[0] || "").toLowerCase()
  };
  const candidates = [""].concat(buildApiCandidates());
  for(const base of candidates){
    const url = `${base}/api/account/sync`;
    try{
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type":"application/json" },
        body: JSON.stringify(payload)
      });
      if(res.ok) return true;
    }catch(_){ }
  }
  return false;
}

function buildPublicConfigEndpoints(){
  const endpoints = [];
  const push = (value) => {
    const next = String(value || "").trim();
    if(!next) return;
    if(endpoints.includes(next)) return;
    endpoints.push(next);
  };

  push("/api/public/config");
  push(DEFAULT_REMOTE_API_BASE + "/api/public/config");

  try{
    const local = isLocalRuntime();
    const onLiveServer = String(location.port || "") === "5500";
    if(local && onLiveServer){
      push("https://novagapp-mart.onrender.com/api/public/config");
    }
  }catch(_){ }

  try{
    [
      window.CONTEST_API_BASE,
      localStorage.getItem("contest_api_base"),
      localStorage.getItem("api_base"),
      sessionStorage.getItem("contest_api_base"),
      sessionStorage.getItem("api_base")
    ]
      .map(normalizeBase)
      .filter(Boolean)
      .forEach(base => {
        if(!isLocalRuntime() && isLoopbackBase(base)) return;
        push(base + "/api/public/config");
      });
  }catch(_){ }

  return endpoints;
}

async function getPublicConfig(){
  const direct = normalizePublicConfig(window.NOVA_PUBLIC_CONFIG || window.__NOVA_PUBLIC_CONFIG__ || {});
  if(direct.supabaseUrl && direct.supabaseAnonKey){
    return direct;
  }
  if(typeof window.getNovaPublicConfig === "function"){
    try{
      const cfg = normalizePublicConfig(await window.getNovaPublicConfig());
      if(cfg.supabaseUrl && cfg.supabaseAnonKey){
        return cfg;
      }
    }catch(_){ }
  }
  const endpoints = buildPublicConfigEndpoints();
  for(const endpoint of endpoints){
    try{
      const res = await fetch(endpoint, {
        method:"GET",
        headers:{ Accept:"application/json" },
        credentials:"omit"
      });
      if(!res.ok) continue;
      const cfg = normalizePublicConfig(await res.json());
      if(cfg.supabaseUrl && cfg.supabaseAnonKey){
        window.NOVA_PUBLIC_CONFIG = {
          ...(window.NOVA_PUBLIC_CONFIG || {}),
          supabaseUrl: cfg.supabaseUrl,
          supabaseAnonKey: cfg.supabaseAnonKey
        };
        window.__NOVA_PUBLIC_CONFIG__ = window.NOVA_PUBLIC_CONFIG;
        return cfg;
      }
    }catch(_){ }
  }
  return direct;
}

async function ensureSupabase(){
  if(supabase) return supabase;
  if(supabaseReadyPromise) return supabaseReadyPromise;
  supabaseReadyPromise = (async () => {
    const cfg = await getPublicConfig();
    if(!cfg.supabaseUrl || !cfg.supabaseAnonKey){
      throw new Error("Supabase public config missing");
    }
    supabase = supabaseJs.createClient(cfg.supabaseUrl, cfg.supabaseAnonKey);
    return supabase;
  })();
  return supabaseReadyPromise;
}

/* ================= EMAIL LOGIN ================= */
async function login(){
  const supabase = await ensureSupabase();
  const email = document.getElementById("email").value.trim();
  const pass = document.getElementById("password").value;

  if(!email || !pass){
    alert("Email aur password required hai");
    return;
  }

  const { data, error } = await supabase.auth.signInWithPassword({
    email: email,
    password: pass
  });

  if(error){
    alert(error.message);
    return;
  }

  const user = data.user;

  // ✅ SINGLE SOURCE OF TRUTH
  localStorage.setItem("isLoggedIn", "true");
  const displayName = deriveDisplayFromEmail(
    user.email,
    user.user_metadata?.full_name || user.user_metadata?.name || ""
  );
  localStorage.setItem("user", JSON.stringify({
    name: displayName,
    email: user.email,
    uid: user.id
  }));
  syncAccountIdentity(user).catch(()=>{});

  location.href = "index.html";
}

/* ================= EMAIL SIGNUP ================= */
async function signup(){
  const supabase = await ensureSupabase();
  const email = document.getElementById("email").value.trim();
  const pass = document.getElementById("password").value;

  if(!email || !pass){
    alert("Email aur password required hai");
    return;
  }

  const { data, error } = await supabase.auth.signUp({
    email: email,
    password: pass
  });

  if(error){
    alert(error.message);
    return;
  }

  const user = data.user;
  const displayName = deriveDisplayFromEmail(
    user.email,
    user.user_metadata?.full_name || user.user_metadata?.name || ""
  );

  localStorage.setItem("isLoggedIn", "true");
  localStorage.setItem("user", JSON.stringify({
    name: displayName,
    email: user.email,
    uid: user.id
  }));
  syncAccountIdentity(user).catch(()=>{});

  location.href = "index.html";
}

/* ================= GOOGLE LOGIN (MOBILE SAFE) ================= */
async function googleLogin(){
  const supabase = await ensureSupabase();
  await supabase.auth.signInWithOAuth({
    provider: "google",
    options: {
      redirectTo: APP_PUBLIC_ORIGIN + "/index.html"
    }
  });
}

/* ================= HANDLE SESSION (REDIRECT RESULT) ================= */
ensureSupabase()
  .then(client => {
    client.auth.onAuthStateChange((event, session) => {
      if(event === "SIGNED_IN" && session?.user){
        const user = session.user;
        const displayName = deriveDisplayFromEmail(
          user.email,
          user.user_metadata?.full_name || user.user_metadata?.name || ""
        );
        localStorage.setItem("isLoggedIn", "true");
        localStorage.setItem("user", JSON.stringify({
          name: displayName,
          email: user.email,
          uid: user.id
        }));
        syncAccountIdentity(user).catch(()=>{});
      }
    });
  })
  .catch(err => {
    console.error("Auth init failed", err);
  });
