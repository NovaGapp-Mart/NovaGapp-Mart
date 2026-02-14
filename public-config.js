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

    try{
      const host = String(location.hostname || "").toLowerCase();
      const isLocal = host === "127.0.0.1" || host === "localhost";
      const liveServer = String(location.port || "") === "5500";
      if(isLocal && liveServer){
        pushEndpoint("http://127.0.0.1:3000/api/public/config");
        pushEndpoint("https://novagapp-mart.onrender.com
/api/public/config");
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
})();
