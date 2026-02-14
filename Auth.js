/* ================= SUPABASE CONFIG ================= */
let supabase = null;
let supabaseReadyPromise = null;
const DEFAULT_REMOTE_API_BASE = "https://novagapp-mart.onrender.com";

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
  const host = String(location.hostname || "").toLowerCase();
  return host === "127.0.0.1" || host === "localhost" || String(location.protocol || "") === "file:";
}

function isLoopbackBase(base){
  const value = String(base || "").trim().toLowerCase();
  return value.startsWith("http://127.0.0.1:") || value.startsWith("http://localhost:");
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
      push("http://127.0.0.1:3000/api/public/config");
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
  localStorage.setItem("user", JSON.stringify({
    name: user.user_metadata?.name || "User",
    email: user.email,
    uid: user.id
  }));

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

  localStorage.setItem("isLoggedIn", "true");
  localStorage.setItem("user", JSON.stringify({
    name: "User",
    email: user.email,
    uid: user.id
  }));

  location.href = "index.html";
}

/* ================= GOOGLE LOGIN (MOBILE SAFE) ================= */
async function googleLogin(){
  const supabase = await ensureSupabase();
  await supabase.auth.signInWithOAuth({
    provider: "google",
    options: {
      redirectTo: window.location.origin + "/index.html"
    }
  });
}

/* ================= HANDLE SESSION (REDIRECT RESULT) ================= */
ensureSupabase()
  .then(client => {
    client.auth.onAuthStateChange((event, session) => {
      if(event === "SIGNED_IN" && session?.user){
        const user = session.user;
        localStorage.setItem("isLoggedIn", "true");
        localStorage.setItem("user", JSON.stringify({
          name: user.user_metadata?.full_name || user.user_metadata?.name || "User",
          email: user.email,
          uid: user.id
        }));
      }
    });
  })
  .catch(err => {
    console.error("Auth init failed", err);
  });
