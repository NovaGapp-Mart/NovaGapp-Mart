(function(){
  const DB_NAME = "nova_offline_video_db";
  const STORE_NAME = "videos";
  const DB_VERSION = 1;

  function hasIndexedDb(){
    return typeof indexedDB !== "undefined";
  }

  function makeKey(type, id){
    const cleanType = String(type || "video").trim().toLowerCase();
    const cleanId = String(id || "").trim();
    return cleanType + ":" + cleanId;
  }

  function openDb(){
    return new Promise((resolve, reject) => {
      if(!hasIndexedDb()){
        reject(new Error("indexeddb_not_supported"));
        return;
      }
      const request = indexedDB.open(DB_NAME, DB_VERSION);
      request.onupgradeneeded = () => {
        const db = request.result;
        if(!db.objectStoreNames.contains(STORE_NAME)){
          const store = db.createObjectStore(STORE_NAME, { keyPath:"key" });
          store.createIndex("saved_at", "saved_at", { unique:false });
          store.createIndex("type", "type", { unique:false });
        }
      };
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error || new Error("db_open_failed"));
    });
  }

  function runTxn(mode, work){
    return openDb().then(db => new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_NAME, mode);
      const store = tx.objectStore(STORE_NAME);
      let out;
      try{
        out = work(store);
      }catch(err){
        reject(err);
        return;
      }
      tx.oncomplete = () => resolve(out);
      tx.onerror = () => reject(tx.error || new Error("db_tx_failed"));
      tx.onabort = () => reject(tx.error || new Error("db_tx_aborted"));
    }));
  }

  function cleanText(value, fallback){
    const text = String(value || "").trim();
    if(text) return text;
    return fallback || "";
  }

  function toPlainMeta(row){
    if(!row) return null;
    return {
      key: row.key,
      id: row.id,
      type: row.type,
      title: row.title,
      source_url: row.source_url,
      thumb_url: row.thumb_url,
      creator_name: row.creator_name,
      saved_at: row.saved_at,
      size: Number(row.size || 0),
      mime: row.mime || ""
    };
  }

  function fetchVideoBlob(url){
    return fetch(url, {
      method: "GET",
      mode: "cors",
      credentials: "omit",
      cache: "no-store"
    }).then(async response => {
      if(!response.ok){
        throw new Error("video_download_failed_" + response.status);
      }
      return response.blob();
    }).catch(async directErr => {
      const fallback = await fetchVideoBlobViaProxy(url).catch(() => null);
      if(fallback){
        return fallback;
      }
      throw directErr;
    });
  }

  function buildOfflineProxyBases(){
    const out = [""];
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
      push(sessionStorage.getItem("contest_api_base"));
      push(sessionStorage.getItem("api_base"));
    }catch(_){ }
    if(/^https?:\/\//i.test(location.origin || "")){
      push(location.origin);
    }
    push("https://novagapp-mart.onrender.com");
    return out;
  }

  async function fetchVideoBlobViaProxy(url){
    const sourceUrl = String(url || "").trim();
    if(!sourceUrl){
      throw new Error("video_url_required");
    }
    const bases = buildOfflineProxyBases();
    let lastError = null;
    for(const base of bases){
      const endpoint = `${base}/api/offline/video?url=${encodeURIComponent(sourceUrl)}`;
      try{
        const response = await fetch(endpoint, {
          method: "GET",
          credentials: "omit",
          cache: "no-store"
        });
        if(!response.ok){
          lastError = new Error("offline_proxy_failed_" + response.status);
          continue;
        }
        return await response.blob();
      }catch(err){
        lastError = err;
      }
    }
    throw lastError || new Error("offline_proxy_failed");
  }

  async function saveVideo(payload){
    const id = cleanText(payload?.id);
    const type = cleanText(payload?.type, "video").toLowerCase();
    const sourceUrl = cleanText(payload?.sourceUrl);
    if(!id) throw new Error("video_id_required");
    if(!sourceUrl) throw new Error("video_url_required");

    const key = makeKey(type, id);
    const existing = await getVideo(key);
    if(existing && existing.source_url === sourceUrl && existing.blob){
      return { saved:true, existed:true, key, item:toPlainMeta(existing) };
    }

    const blob = await fetchVideoBlob(sourceUrl);
    const next = {
      key,
      id,
      type,
      title: cleanText(payload?.title, type === "reel" ? "Reel" : "Video"),
      source_url: sourceUrl,
      thumb_url: cleanText(payload?.thumbUrl),
      creator_name: cleanText(payload?.creatorName, "User"),
      saved_at: Date.now(),
      size: Number(blob?.size || 0),
      mime: cleanText(blob?.type, "video/mp4"),
      blob
    };

    await runTxn("readwrite", store => {
      store.put(next);
    });
    return { saved:true, existed:false, key, item:toPlainMeta(next) };
  }

  async function listVideos(){
    const rows = await runTxn("readonly", store => store.getAll());
    const list = Array.isArray(rows) ? rows.slice() : [];
    list.sort((a, b) => Number(b?.saved_at || 0) - Number(a?.saved_at || 0));
    return list.map(toPlainMeta);
  }

  function getVideo(key){
    return runTxn("readonly", store => new Promise((resolve, reject) => {
      const req = store.get(String(key || ""));
      req.onsuccess = () => resolve(req.result || null);
      req.onerror = () => reject(req.error || new Error("db_get_failed"));
    }));
  }

  async function hasVideo(type, id){
    const row = await getVideo(makeKey(type, id));
    return !!(row && row.blob);
  }

  function deleteVideo(key){
    return runTxn("readwrite", store => {
      store.delete(String(key || ""));
    });
  }

  window.NOVA_OFFLINE = {
    makeKey,
    save: saveVideo,
    list: listVideos,
    get: getVideo,
    has: hasVideo,
    remove: deleteVideo
  };
})();
