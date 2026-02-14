(function(){
  const STORAGE_KEY = "nova_public_config_v1";
  const EMPTY_CONFIG = Object.freeze({
    supabaseUrl: "",
    supabaseAnonKey: "",
    razorpayKeyId: ""
  });
  const FALLBACK_CONFIG = Object.freeze({
    // Keep empty in source. Runtime values should come from /api/public/config.
    supabaseUrl: "",
    supabaseAnonKey: "",
    razorpayKeyId: ""
  });
  const DEFAULT_REMOTE_API_BASE = "https://novagapp-mart.onrender.com";

  function getHostName(){
    try{
      return String(location.hostname || "").toLowerCase();
    }catch(_){
      return "";
    }
  }

  function isLocalRuntime(){
    const host = getHostName();
    return host === "127.0.0.1" || host === "localhost" || String(location.protocol || "") === "file:";
  }

  function isLoopbackBase(base){
    const value = String(base || "").trim().toLowerCase();
    return value.startsWith("http://127.0.0.1:") || value.startsWith("http://localhost:");
  }

  function cleanupLoopbackApiBases(){
    if(isLocalRuntime()) return;
    const keys = ["contest_api_base", "api_base", "tryonApiBase"];
    const cleanupStore = (store, storeKeys) => {
      if(!store) return;
      storeKeys.forEach(key => {
        try{
          const raw = String(store.getItem(key) || "").trim();
          if(isLoopbackBase(raw)){
            store.removeItem(key);
          }
        }catch(_){ }
      });
    };
    cleanupStore(window.localStorage, keys);
    cleanupStore(window.sessionStorage, ["contest_api_base", "api_base"]);
  }

  function sanitizeConfig(raw){
    const next = {
      supabaseUrl: String(raw?.supabaseUrl || "").trim(),
      supabaseAnonKey: String(raw?.supabaseAnonKey || "").trim(),
      razorpayKeyId: String(raw?.razorpayKeyId || "").trim()
    };
    return next;
  }

  function readStored(){
    try{
      const raw = localStorage.getItem(STORAGE_KEY);
      if(!raw) return null;
      return sanitizeConfig(JSON.parse(raw));
    }catch(_){
      return null;
    }
  }

  function writeStored(config){
    try{
      localStorage.setItem(STORAGE_KEY, JSON.stringify(sanitizeConfig(config)));
    }catch(_){ }
  }

  function hasSupabaseConfig(config){
    const safe = sanitizeConfig(config || EMPTY_CONFIG);
    return Boolean(safe.supabaseUrl && safe.supabaseAnonKey);
  }

  function mergePreferPrimary(primary, secondary){
    const p = sanitizeConfig(primary || EMPTY_CONFIG);
    const s = sanitizeConfig(secondary || EMPTY_CONFIG);
    return sanitizeConfig({
      supabaseUrl: p.supabaseUrl || s.supabaseUrl,
      supabaseAnonKey: p.supabaseAnonKey || s.supabaseAnonKey,
      razorpayKeyId: p.razorpayKeyId || s.razorpayKeyId
    });
  }

  function readFallback(){
    const fromWindow = sanitizeConfig(
      window.NOVA_PUBLIC_CONFIG_DEFAULTS ||
      window.__NOVA_PUBLIC_CONFIG_DEFAULTS__ ||
      EMPTY_CONFIG
    );
    if(hasSupabaseConfig(fromWindow) || fromWindow.razorpayKeyId){
      return fromWindow;
    }
    return sanitizeConfig(FALLBACK_CONFIG);
  }

  function normalizeBase(value){
    const raw = String(value || "").trim();
    if(!raw) return "";
    return raw.replace(/\/+$/g, "");
  }

  function buildConfigEndpoints(){
    const endpoints = [];
    const pushEndpoint = (value) => {
      const next = String(value || "").trim();
      if(!next) return;
      if(endpoints.includes(next)) return;
      endpoints.push(next);
    };

    pushEndpoint("/api/public/config");
    pushEndpoint(DEFAULT_REMOTE_API_BASE + "/api/public/config");

    try{
      const isLocal = isLocalRuntime();
      const liveServer = String(location.port || "") === "5500";
      if(isLocal && liveServer){
        pushEndpoint("http://127.0.0.1:3000/api/public/config");
      }
    }catch(_){ }

    const explicitBases = [];
    try{
      explicitBases.push(window.CONTEST_API_BASE);
      explicitBases.push(localStorage.getItem("contest_api_base"));
      explicitBases.push(localStorage.getItem("api_base"));
      explicitBases.push(sessionStorage.getItem("contest_api_base"));
      explicitBases.push(sessionStorage.getItem("api_base"));
    }catch(_){ }

    explicitBases
      .map(normalizeBase)
      .filter(Boolean)
      .forEach(base => {
        if(!isLocalRuntime() && isLoopbackBase(base)) return;
        pushEndpoint(base + "/api/public/config");
      });

    return endpoints;
  }

  function readFromServerSync(){
    const endpoints = buildConfigEndpoints();
    for(const endpoint of endpoints){
      try{
        const xhr = new XMLHttpRequest();
        xhr.open("GET", endpoint, false);
        xhr.setRequestHeader("Accept", "application/json");
        xhr.send(null);
        if(xhr.status < 200 || xhr.status >= 300) continue;
        const parsed = xhr.responseText ? JSON.parse(xhr.responseText) : null;
        if(parsed){
          return sanitizeConfig(parsed);
        }
      }catch(_){ }
    }
    return null;
  }

  async function readFromServerAsync(){
    const endpoints = buildConfigEndpoints();
    for(const endpoint of endpoints){
      try{
        const res = await fetch(endpoint, {
          method: "GET",
          headers: { Accept: "application/json" },
          credentials: "omit"
        });
        if(!res.ok){
          continue;
        }
        const payload = await res.json();
        return sanitizeConfig(payload);
      }catch(_){ }
    }
    throw new Error("public_config_fetch_failed");
  }

  cleanupLoopbackApiBases();

  const fromStorage = readStored();
  const fromFallback = readFallback();
  const localPreferred = mergePreferPrimary(fromStorage, fromFallback);
  const fromServerSync = hasSupabaseConfig(localPreferred) ? null : readFromServerSync();
  const initial = fromServerSync || localPreferred || { ...EMPTY_CONFIG };
  window.NOVA_PUBLIC_CONFIG = sanitizeConfig(initial);
  window.__NOVA_PUBLIC_CONFIG__ = window.NOVA_PUBLIC_CONFIG;
  if(fromServerSync){
    writeStored(fromServerSync);
  }else if(hasSupabaseConfig(localPreferred) || localPreferred.razorpayKeyId){
    writeStored(localPreferred);
  }

  let inflight = null;
  window.getNovaPublicConfig = async function(forceRefresh){
    const force = !!forceRefresh;
    if(!force){
      const current = sanitizeConfig(window.NOVA_PUBLIC_CONFIG || EMPTY_CONFIG);
      if(current.supabaseUrl || current.supabaseAnonKey || current.razorpayKeyId){
        return current;
      }
    }
    if(inflight) return inflight;
    inflight = readFromServerAsync()
      .then(cfg => {
        window.NOVA_PUBLIC_CONFIG = sanitizeConfig(cfg);
        window.__NOVA_PUBLIC_CONFIG__ = window.NOVA_PUBLIC_CONFIG;
        writeStored(window.NOVA_PUBLIC_CONFIG);
        return window.NOVA_PUBLIC_CONFIG;
      })
      .catch(() => {
        return sanitizeConfig(window.NOVA_PUBLIC_CONFIG || EMPTY_CONFIG);
      })
      .finally(() => {
        inflight = null;
      });
    return inflight;
  };

  function getEffectiveConfigSync(){
    const current = sanitizeConfig(window.NOVA_PUBLIC_CONFIG || EMPTY_CONFIG);
    if(current.supabaseUrl && current.supabaseAnonKey){
      return current;
    }
    const fromServer = readFromServerSync();
    if(fromServer){
      const next = sanitizeConfig(fromServer);
      window.NOVA_PUBLIC_CONFIG = next;
      window.__NOVA_PUBLIC_CONFIG__ = next;
      writeStored(next);
      return next;
    }
    return current;
  }

  function createSupabaseClient(options){
    const sdk = window.supabase;
    if(!sdk || typeof sdk.createClient !== "function"){
      return null;
    }
    const cfg = getEffectiveConfigSync();
    if(!cfg.supabaseUrl || !cfg.supabaseAnonKey){
      return null;
    }
    try{
      return sdk.createClient(cfg.supabaseUrl, cfg.supabaseAnonKey, options);
    }catch(_){
      return null;
    }
  }

  window.novaCreateSupabaseClient = function(options){
    if(window.supa) return window.supa;
    const client = createSupabaseClient(options);
    if(client){
      window.supa = client;
    }
    return client;
  };

  window.ensureNovaSupabaseClient = async function(options){
    const existing = window.novaCreateSupabaseClient(options);
    if(existing) return existing;
    try{
      await window.getNovaPublicConfig(true);
    }catch(_){ }
    return window.novaCreateSupabaseClient(options);
  };
})();
