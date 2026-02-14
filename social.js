/* =====================================
   NOVAGAPP SOCIAL HELPERS
===================================== */
(function(){
  window.NOVA = window.NOVA || {};
  if(typeof window.NOVA.requireUser !== "function"){
    window.NOVA.requireUser = async function(){
      location.href = "login.html";
      throw new Error("Supabase client not ready");
    };
  }

  const SUPABASE_CLIENT_OPTIONS = {
    auth:{
      persistSession:true,
      autoRefreshToken:true,
      detectSessionInUrl:true
    }
  };
  let socialInitIssue = "";

  function normalizePublicConfig(raw){
    return {
      supabaseUrl: String(raw?.supabaseUrl || "").trim(),
      supabaseAnonKey: String(raw?.supabaseAnonKey || "").trim()
    };
  }

  function normalizeBase(value){
    return String(value || "").trim().replace(/\/+$/g, "");
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

    try{
      const host = String(location.hostname || "").toLowerCase();
      const local = host === "127.0.0.1" || host === "localhost";
      const onLiveServer = String(location.port || "") === "5500";
      if(local && onLiveServer){
        push("http://127.0.0.1:3000/api/public/config");
        push("http://localhost:3000/api/public/config");
      }
    }catch(_){ }

    try{
      const bases = [
        window.CONTEST_API_BASE,
        localStorage.getItem("contest_api_base"),
        localStorage.getItem("api_base"),
        sessionStorage.getItem("contest_api_base"),
        sessionStorage.getItem("api_base")
      ];
      bases
        .map(normalizeBase)
        .filter(Boolean)
        .forEach(base => push(base + "/api/public/config"));
    }catch(_){ }

    return endpoints;
  }

  async function resolvePublicConfig(){
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
        const payload = normalizePublicConfig(await res.json());
        if(payload.supabaseUrl && payload.supabaseAnonKey){
          window.NOVA_PUBLIC_CONFIG = {
            ...(window.NOVA_PUBLIC_CONFIG || {}),
            supabaseUrl: payload.supabaseUrl,
            supabaseAnonKey: payload.supabaseAnonKey
          };
          window.__NOVA_PUBLIC_CONFIG__ = window.NOVA_PUBLIC_CONFIG;
          return payload;
        }
      }catch(_){ }
    }

    return direct;
  }

  function createClientFromWindow(config){
    if(window.supa) return window.supa;
    const sdk = window.supabase;
    if(!sdk || typeof sdk.createClient !== "function"){
      return null;
    }
    const cfg = normalizePublicConfig(config || {});
    if(!cfg.supabaseUrl || !cfg.supabaseAnonKey){
      return null;
    }
    try{
      return sdk.createClient(
        cfg.supabaseUrl,
        cfg.supabaseAnonKey,
        SUPABASE_CLIENT_OPTIONS
      );
    }catch(_){
      return null;
    }
  }

  function loadSupabaseSdk(src){
    return new Promise((resolve, reject) => {
      const script = document.createElement("script");
      script.src = src;
      script.async = true;
      script.crossOrigin = "anonymous";
      script.onload = () => resolve(true);
      script.onerror = () => reject(new Error("Failed to load Supabase SDK: " + src));
      document.head.appendChild(script);
    });
  }

  async function ensureClient(){
    let client = createClientFromWindow();
    if(client) return client;

    const config = await resolvePublicConfig();
    if(!config.supabaseUrl || !config.supabaseAnonKey){
      socialInitIssue = "config_missing";
      return null;
    }
    client = createClientFromWindow(config);
    if(client) return client;

    const fallbackCdn = [
      "https://unpkg.com/@supabase/supabase-js@2",
      "https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2"
    ];

    for(const src of fallbackCdn){
      try{
        await loadSupabaseSdk(src);
      }catch(_){
        socialInitIssue = "sdk_load_failed";
        continue;
      }
      client = createClientFromWindow(config);
      if(client) return client;
    }
    if(!socialInitIssue){
      socialInitIssue = "client_init_failed";
    }
    return null;
  }

  function showSdkLoadError(){
    const msg = socialInitIssue === "config_missing"
      ? "Supabase config missing. Start backend and ensure /api/public/config works."
      : "Supabase client init failed. Check internet/CDN + backend config.";
    console.error(msg);
    window.__NOVA_SUPABASE_ALERTED__ = true;
  }

  function initSocial(supa){
    window.supa = supa;
    window.NOVA = window.NOVA || {};
    window.NOVA.supa = supa;

  window.NOVA.getUser = async function(){
    try{
      const { data } = await supa.auth.getUser();
      return data?.user || null;
    }catch(err){
      if(err && err.name === "AbortError"){
        return null;
      }
      throw err;
    }
  };

  window.NOVA.requireUser = async function(){
    const user = await window.NOVA.getUser();
    if(!user){
      location.href = "login.html";
      throw new Error("Not authenticated");
    }
    return user;
  };

  let presenceChannel = null;
  let presenceInitPromise = null;
  let presenceListeners = new Set();
  let presenceUserId = null;
  let presenceTimer = null;

  function notifyPresenceListeners(){
    if(!presenceChannel) return;
    const state = presenceChannel.presenceState();
    presenceListeners.forEach(fn => {
      try{ fn(state); }catch(_){}
    });
  }

  function trackPresence(active){
    if(!presenceChannel || !presenceUserId) return;
    try{
      presenceChannel.track({
        user_id: presenceUserId,
        active: !!active,
        last_active: new Date().toISOString()
      });
    }catch(_){}
  }

  window.NOVA.onPresence = function(fn){
    if(typeof fn !== "function") return () => {};
    presenceListeners.add(fn);
    if(presenceChannel){
      try{ fn(presenceChannel.presenceState()); }catch(_){}
    }
    return () => presenceListeners.delete(fn);
  };

  window.NOVA.isUserOnline = function(userId){
    if(!presenceChannel || !userId) return false;
    const state = presenceChannel.presenceState();
    const entries = state?.[userId] || [];
    return entries.some(entry => entry && entry.active !== false);
  };

  window.NOVA.ensurePresence = async function(){
    if(presenceInitPromise) return presenceInitPromise;
    presenceInitPromise = (async () => {
      let data = null;
      try{
        const result = await supa.auth.getUser();
        data = result?.data || null;
      }catch(err){
        if(err && err.name === "AbortError"){
          presenceInitPromise = null;
          return null;
        }
        throw err;
      }
      const user = data?.user;
      if(!user){
        presenceInitPromise = null;
        return null;
      }
      presenceUserId = user.id;
      presenceChannel = supa.channel("online-status", {
        config:{ presence:{ key: user.id } }
      });
      window.NOVA.presenceChannel = presenceChannel;
      presenceChannel.on("presence", { event:"sync" }, notifyPresenceListeners);
      presenceChannel.on("presence", { event:"join" }, notifyPresenceListeners);
      presenceChannel.on("presence", { event:"leave" }, notifyPresenceListeners);
      presenceChannel.subscribe(status => {
        if(status === "SUBSCRIBED"){
          trackPresence(!document.hidden);
        }
      });
      document.addEventListener("visibilitychange", () => trackPresence(!document.hidden));
      window.addEventListener("focus", () => trackPresence(true));
      window.addEventListener("blur", () => trackPresence(false));
      if(!presenceTimer){
        presenceTimer = setInterval(() => trackPresence(!document.hidden), 25000);
      }
      window.addEventListener("beforeunload", () => {
        try{ presenceChannel.untrack(); }catch(_){}
      });
      return presenceChannel;
    })();
    return presenceInitPromise;
  };

  document.addEventListener("DOMContentLoaded", () => {
    if(typeof window.NOVA.ensurePresence === "function"){
      window.NOVA.ensurePresence();
    }
  });

  window.NOVA.pickFile = function(accept){
    return new Promise(resolve => {
      const input = document.createElement("input");
      input.type = "file";
      input.accept = accept || "*/*";
      input.onchange = () => {
        const file = input.files && input.files[0] ? input.files[0] : null;
        resolve(file);
      };
      input.click();
    });
  };

  window.NOVA.getVideoMeta = function(file){
    return new Promise((resolve, reject) => {
      const video = document.createElement("video");
      video.preload = "metadata";
      video.onloadedmetadata = () => {
        const meta = {
          duration: video.duration || 0,
          width: video.videoWidth || 0,
          height: video.videoHeight || 0
        };
        URL.revokeObjectURL(video.src);
        resolve(meta);
      };
      video.onerror = () => reject(new Error("Unable to read video metadata"));
      video.src = URL.createObjectURL(file);
    });
  };

  window.NOVA.validateAspect = function(width, height, target, tolerance){
    if(!width || !height) return true;
    const ratio = width / height;
    return Math.abs(ratio - target) <= (tolerance || 0.15);
  };

  window.NOVA.makePath = function(userId, file){
    const ext = String(file.name || "file").split(".").pop();
    return userId + "/" + Date.now() + "-" + Math.random().toString(36).slice(2, 8) + "." + ext;
  };

  window.NOVA.uploadToBucket = async function(bucket, file, path){
    const { error } = await supa.storage.from(bucket).upload(path, file, {
      contentType: file.type || "application/octet-stream",
      upsert: false
    });
    if(error) throw error;
    const { data } = supa.storage.from(bucket).getPublicUrl(path);
    return data.publicUrl;
  };

  window.NOVA.askMeta = async function(){
    const title = prompt("Title (optional)") || "";
    const description = prompt("Description (optional)") || "";
    const keywords = prompt("Keywords (comma separated, optional)") || "";
    let thumbUrl = "";

    if(confirm("Add a thumbnail image? (optional)")){
      const thumbFile = await window.NOVA.pickFile("image/*");
      if(thumbFile){
        const user = await window.NOVA.requireUser();
        const thumbPath = window.NOVA.makePath(user.id, thumbFile);
        thumbUrl = await window.NOVA.uploadToBucket("thumbnails", thumbFile, thumbPath);
      }
    }

    return { title, description, keywords, thumbUrl };
  };

  window.NOVA.toggleFollow = async function(targetUserId){
    const user = await window.NOVA.requireUser();
    if(!targetUserId || targetUserId === user.id) return { following: false };

    const { data: existing, error: selectError } = await supa
      .from("follows")
      .select("follower_id")
      .eq("follower_id", user.id)
      .eq("following_id", targetUserId)
      .limit(1);

    if(selectError) return { following: null, error: selectError };

    if(existing && existing.length){
      const { error: deleteError } = await supa.from("follows")
        .delete()
        .eq("follower_id", user.id)
        .eq("following_id", targetUserId);
      if(deleteError) return { following: null, error: deleteError };
      return { following: false };
    }

    const { error: insertError } = await supa.from("follows").insert({
      follower_id: user.id,
      following_id: targetUserId
    });
    if(insertError) return { following: null, error: insertError };
    return { following: true };
  };

  const reactionQueues = new Map();
  function queueReactionOperation(key, task){
    const prev = reactionQueues.get(key) || Promise.resolve();
    const next = prev.catch(() => {}).then(task);
    reactionQueues.set(key, next);
    next.finally(() => {
      if(reactionQueues.get(key) === next){
        reactionQueues.delete(key);
      }
    });
    return next;
  }

  window.NOVA.toggleReaction = async function(targetType, targetId, reaction){
    const user = await window.NOVA.requireUser();
    const queueKey = [user.id, targetType, targetId].join("|");

    return queueReactionOperation(queueKey, async () => {
      const wait = (ms) => new Promise(resolve => setTimeout(resolve, ms));
      const keyFilter = (query) => query
        .eq("user_id", user.id)
        .eq("target_type", targetType)
        .eq("target_id", targetId);

      const removeExisting = async () => {
        const { error } = await keyFilter(supa.from("reactions").delete());
        if(error) throw error;
      };

      const readCurrentReaction = async () => {
        const { data, error } = await keyFilter(
          supa.from("reactions").select("reaction").limit(1).maybeSingle()
        );
        if(error) throw error;
        return data?.reaction || null;
      };

      const insertRequestedReaction = async () => {
        const { error } = await supa.from("reactions").upsert(
          {
            user_id: user.id,
            target_type: targetType,
            target_id: targetId,
            reaction
          },
          {
            onConflict: "user_id,target_type,target_id"
          }
        );
        return error || null;
      };

      const previousReaction = await readCurrentReaction();

      if(previousReaction === reaction){
        await removeExisting();
        const finalAfterDelete = await readCurrentReaction();
        return {
          active: !!finalAfterDelete,
          reaction: finalAfterDelete,
          previousReaction
        };
      }

      if(previousReaction){
        await removeExisting();
      }

      let writeError = await insertRequestedReaction();
      if(writeError){
        const isConflict = String(writeError.code || "") === "23505";
        if(!isConflict) throw writeError;

        const currentFromConflict = await readCurrentReaction();
        if(currentFromConflict !== reaction){
          await removeExisting();
          writeError = await insertRequestedReaction();
          if(writeError){
            const retryConflict = String(writeError.code || "") === "23505";
            if(!retryConflict) throw writeError;
          }
        }
      }

      let finalReaction = null;
      for(let attempt = 0; attempt < 3; attempt += 1){
        finalReaction = await readCurrentReaction();
        if(finalReaction) break;
        await wait(120);
      }

      return {
        active: !!finalReaction,
        reaction: finalReaction,
        previousReaction
      };
    });
  };

  window.NOVA.getReactionCounts = async function(targetType, targetId){
    const likeQuery = supa
      .from("reactions")
      .select("id", { count: "exact", head: true })
      .eq("target_type", targetType)
      .eq("target_id", targetId)
      .eq("reaction", "like");

    const dislikeQuery = supa
      .from("reactions")
      .select("id", { count: "exact", head: true })
      .eq("target_type", targetType)
      .eq("target_id", targetId)
      .eq("reaction", "dislike");

    const [{ count: likes, error: likeError }, { count: dislikes, error: dislikeError }] = await Promise.all([
      likeQuery,
      dislikeQuery
    ]);
    if(!likeError && !dislikeError && likes !== null && dislikes !== null){
      return { likes: likes || 0, dislikes: dislikes || 0 };
    }

    const { data, error } = await supa
      .from("reactions")
      .select("reaction")
      .eq("target_type", targetType)
      .eq("target_id", targetId)
      .in("reaction", ["like", "dislike"])
      .limit(5000);
    if(error){
      return { likes: 0, dislikes: 0 };
    }

    let likeCount = 0;
    let dislikeCount = 0;
    (data || []).forEach(row => {
      if(row?.reaction === "like") likeCount += 1;
      if(row?.reaction === "dislike") dislikeCount += 1;
    });
    return { likes: likeCount, dislikes: dislikeCount };
  };

  const COMMENT_TEXT_KEYS = ["body", "content"];
  const COMMENT_QUERY_PLANS = [
    { typeKey:"target_type", idKey:"target_id" },
    { typeKey:"target_type", idKey:"post_id" },
    { typeKey:"type", idKey:"target_id" },
    { typeKey:"type", idKey:"post_id" },
    { typeKey:null, idKey:"target_id" },
    { typeKey:null, idKey:"post_id" }
  ];
  const COMMENT_EVENT_PREFIX = "__NOVA_EVT__:";
  let commentReadPlanCache = null;
  let commentReadTextKeyCache = COMMENT_TEXT_KEYS[0];
  let commentWritePlanCache = null;
  let commentWriteTextKeyCache = COMMENT_TEXT_KEYS[0];
  const invalidCommentReadCombos = new Set();
  const invalidCommentWriteCombos = new Set();

  function getCommentPlanKey(plan){
    return `${String(plan?.typeKey || "-")}|${String(plan?.idKey || "-")}`;
  }

  function getCommentPlanSequence(cachedPlan){
    const list = [];
    const seen = new Set();
    if(cachedPlan){
      const key = getCommentPlanKey(cachedPlan);
      seen.add(key);
      list.push(cachedPlan);
    }
    COMMENT_QUERY_PLANS.forEach(plan => {
      const key = getCommentPlanKey(plan);
      if(seen.has(key)) return;
      seen.add(key);
      list.push(plan);
    });
    return list;
  }

  function getCommentTextKeySequence(cachedTextKey){
    const sequence = [];
    const seen = new Set();
    const add = key => {
      const normalized = String(key || "").trim();
      if(!normalized || seen.has(normalized)) return;
      seen.add(normalized);
      sequence.push(normalized);
    };
    add(cachedTextKey);
    COMMENT_TEXT_KEYS.forEach(add);
    return sequence;
  }

  function getCommentComboKey(plan, textKey){
    return `${getCommentPlanKey(plan)}|${String(textKey || "")}`;
  }

  function toBase64Utf8(value){
    const utf8 = encodeURIComponent(String(value || "")).replace(/%([0-9A-F]{2})/g, (_, hex) => {
      return String.fromCharCode(parseInt(hex, 16));
    });
    return btoa(utf8);
  }

  function fromBase64Utf8(value){
    try{
      const bytes = atob(String(value || ""));
      const encoded = Array.from(bytes, ch => "%" + ch.charCodeAt(0).toString(16).padStart(2, "0")).join("");
      return decodeURIComponent(encoded);
    }catch(_){
      return "";
    }
  }

  function getSupabaseErrorText(error){
    return [error?.message, error?.details, error?.hint]
      .filter(Boolean)
      .join(" ");
  }

  function getMissingColumnName(error){
    const text = getSupabaseErrorText(error);
    if(!text) return "";

    const pgrstMatch = text.match(/'([a-zA-Z0-9_]+)' column/i);
    if(pgrstMatch && pgrstMatch[1]) return pgrstMatch[1].toLowerCase();

    const pgMatch = text.match(/column\s+\"?([a-zA-Z0-9_.]+)\"?\s+does not exist/i);
    if(pgMatch && pgMatch[1]){
      const normalized = pgMatch[1].replace(/\"/g, "").toLowerCase();
      return normalized.split(".").pop() || "";
    }
    return "";
  }

  let shareStorageMode = "";
  let shareStorageModePromise = null;

  function isMissingRelation(error, relationName){
    const text = getSupabaseErrorText(error).toLowerCase();
    const relation = String(relationName || "").toLowerCase();
    if(!text || !relation) return false;
    return text.includes("does not exist") && text.includes(relation);
  }

  async function detectShareStorageMode(){
    if(shareStorageMode) return shareStorageMode;
    if(shareStorageModePromise) return shareStorageModePromise;

    shareStorageModePromise = (async () => {
      try{
        const { error } = await supa
          .from("post_shares")
          .select("id", { count:"exact", head:true })
          .limit(1);
        if(!error){
          shareStorageMode = "post_shares";
          return shareStorageMode;
        }
        shareStorageMode = isMissingRelation(error, "post_shares")
          ? "comments"
          : "post_shares";
      }catch(_){
        shareStorageMode = "comments";
      }
      return shareStorageMode;
    })();

    const mode = await shareStorageModePromise;
    shareStorageModePromise = null;
    return mode;
  }

  async function insertPostShareRow(userId, targetType, targetId, channel){
    const payload = {
      user_id: String(userId || ""),
      target_type: String(targetType || ""),
      target_id: String(targetId || ""),
      channel: String(channel || "unknown")
    };
    const { error } = await supa.from("post_shares").insert(payload);
    if(!error){
      return { ok:true, error:null, missingTable:false };
    }
    return {
      ok:false,
      error,
      missingTable: isMissingRelation(error, "post_shares")
    };
  }

  async function readPostShareCount(targetType, targetId){
    const { count, error } = await supa
      .from("post_shares")
      .select("id", { count:"exact", head:true })
      .eq("target_type", targetType)
      .eq("target_id", targetId);
    if(!error && count !== null){
      return { count: Number(count || 0), error:null, missingTable:false };
    }
    return {
      count: 0,
      error,
      missingTable: isMissingRelation(error, "post_shares")
    };
  }

  function extractCommentBody(row){
    if(!row) return "";
    if(row.body !== undefined && row.body !== null) return String(row.body);
    if(row.content !== undefined && row.content !== null) return String(row.content);
    return "";
  }

  function encodeCommentEvent(payload){
    const json = JSON.stringify(payload || {});
    return COMMENT_EVENT_PREFIX + toBase64Utf8(json);
  }

  function parseCommentEvent(text){
    const raw = String(text || "");
    if(!raw.startsWith(COMMENT_EVENT_PREFIX)) return null;
    const encoded = raw.slice(COMMENT_EVENT_PREFIX.length);
    if(!encoded) return null;
    const decoded = fromBase64Utf8(encoded);
    if(!decoded) return null;
    try{
      const evt = JSON.parse(decoded);
      if(!evt || typeof evt !== "object") return null;
      return evt;
    }catch(_){
      return null;
    }
  }

  async function fetchCommentRowsCompat(targetType, targetId, limit){
    const maxRows = Math.min(Math.max(Number(limit) || 300, 1), 5000);
    let lastError = null;

    for(const plan of getCommentPlanSequence(commentReadPlanCache)){
      const planKey = getCommentPlanKey(plan);
      for(const textKey of getCommentTextKeySequence(commentReadTextKeyCache)){
        const comboKey = getCommentComboKey(plan, textKey);
        if(invalidCommentReadCombos.has(comboKey)) continue;

        let query = supa
          .from("comments")
          .select(`id,user_id,created_at,${textKey}`)
          .eq(plan.idKey, targetId)
          .order("created_at", { ascending:true })
          .limit(maxRows);
        if(plan.typeKey){
          query = query.eq(plan.typeKey, targetType);
        }

        const { data, error } = await query;
        if(!error){
          commentReadPlanCache = plan;
          commentReadTextKeyCache = textKey;
          const normalized = (data || []).map(row => {
            if(!row || typeof row !== "object") return row;
            if(row.content === undefined){
              return { ...row, content: row[textKey] ?? "" };
            }
            return row;
          });
          return { data: normalized, error:null };
        }

        lastError = error;
        const missing = getMissingColumnName(error);
        if(!missing) continue;

        if(missing === plan.idKey || (plan.typeKey && missing === plan.typeKey)){
          COMMENT_TEXT_KEYS.forEach(key => {
            invalidCommentReadCombos.add(getCommentComboKey(plan, key));
          });
          break;
        }
        if(missing === textKey){
          invalidCommentReadCombos.add(comboKey);
        }
      }
    }
    return { data: [], error:lastError || new Error("comments_fetch_failed") };
  }

  async function insertCommentCompat(userId, targetType, targetId, text){
    const commentText = String(text || "").trim();
    if(!commentText) return { ok:false, error:new Error("empty_comment"), data:null };

    let lastError = null;
    for(const plan of getCommentPlanSequence(commentWritePlanCache)){
      for(const textKey of getCommentTextKeySequence(commentWriteTextKeyCache)){
        const comboKey = getCommentComboKey(plan, textKey);
        if(invalidCommentWriteCombos.has(comboKey)) continue;

        const row = {
          user_id: userId,
          [plan.idKey]: targetId,
          [textKey]: commentText
        };
        if(plan.typeKey){
          row[plan.typeKey] = targetType;
        }
        const { data, error } = await supa.from("comments").insert(row).select("id").maybeSingle();
        if(!error){
          commentWritePlanCache = plan;
          commentWriteTextKeyCache = textKey;
          return { ok:true, error:null, data:data || null };
        }
        lastError = error;
        const missing = getMissingColumnName(error);
        if(!missing) continue;

        if(missing === plan.idKey || (plan.typeKey && missing === plan.typeKey)){
          COMMENT_TEXT_KEYS.forEach(key => {
            invalidCommentWriteCombos.add(getCommentComboKey(plan, key));
          });
          break;
        }
        if(missing === textKey){
          invalidCommentWriteCombos.add(comboKey);
          break;
        }
      }
    }
    return { ok:false, error:lastError || new Error("comments_insert_failed"), data:null };
  }

  function buildCommentThread(rows, options){
    const opts = options || {};
    const ownerUserId = String(opts.ownerUserId || "");
    const list = Array.isArray(rows) ? rows.slice() : [];
    list.sort((a, b) => {
      return Date.parse(String(a?.created_at || "")) - Date.parse(String(b?.created_at || ""));
    });

    const byRowId = new Map();
    const visible = [];
    let pinnedRowId = "";

    for(const row of list){
      const rowId = String(row?.id || "");
      if(!rowId) continue;
      const userId = String(row?.user_id || "");
      const createdAt = String(row?.created_at || new Date().toISOString());
      const rawBody = extractCommentBody(row).trim();
      if(!rawBody) continue;

      const evt = parseCommentEvent(rawBody);
      if(!evt){
        const comment = {
          id: "comment-" + rowId,
          rowId,
          sourceRowId: rowId,
          user_id: userId,
          body: rawBody,
          created_at: createdAt,
          parentRowId: "",
          edited: false,
          pinned: false,
          replies: []
        };
        byRowId.set(rowId, comment);
        visible.push(comment);
        continue;
      }

      if(evt.kind === "reply"){
        const text = String(evt.body || "").trim();
        if(!text) continue;
        const parentRowId = String(evt.parentRowId || evt.parent_id || "");
        const hasParent = !!(parentRowId && byRowId.has(parentRowId));
        const reply = {
          id: "comment-" + rowId,
          rowId,
          sourceRowId: rowId,
          user_id: userId,
          body: text,
          created_at: createdAt,
          parentRowId: hasParent ? parentRowId : "",
          edited: false,
          pinned: false,
          replies: []
        };
        byRowId.set(rowId, reply);
        visible.push(reply);
        continue;
      }

      if(evt.kind === "edit"){
        const targetRowId = String(evt.targetRowId || evt.target_id || "");
        const nextBody = String(evt.body || "").trim();
        const target = targetRowId ? byRowId.get(targetRowId) : null;
        if(target && nextBody && target.user_id && target.user_id === userId){
          target.body = nextBody;
          target.edited = true;
          target.edited_at = createdAt;
        }
        continue;
      }

      if(evt.kind === "pin"){
        if(ownerUserId && userId === ownerUserId){
          const targetRowId = String(evt.targetRowId || evt.target_id || "");
          if(!targetRowId){
            pinnedRowId = "";
          }else if(byRowId.has(targetRowId)){
            pinnedRowId = targetRowId;
          }
        }
      }
    }

    const roots = [];
    const sortByDateAsc = (a, b) => {
      return Date.parse(String(a?.created_at || "")) - Date.parse(String(b?.created_at || ""));
    };

    visible.forEach(item => {
      if(item.parentRowId && byRowId.has(item.parentRowId)){
        const parent = byRowId.get(item.parentRowId);
        parent.replies.push(item);
      }else{
        roots.push(item);
      }
    });

    roots.sort(sortByDateAsc);
    visible.forEach(item => {
      item.replies.sort(sortByDateAsc);
    });

    if(pinnedRowId){
      const pinnedIndex = roots.findIndex(item => item.rowId === pinnedRowId);
      if(pinnedIndex >= 0){
        const [pinned] = roots.splice(pinnedIndex, 1);
        pinned.pinned = true;
        roots.unshift(pinned);
      }
    }

    return {
      roots,
      flat: visible,
      visibleCount: visible.length,
      pinnedRowId
    };
  }

  async function addCommentEvent(targetType, targetId, payload){
    const user = await window.NOVA.requireUser();
    const encoded = encodeCommentEvent(payload);
    return insertCommentCompat(user.id, targetType, targetId, encoded);
  }

  async function readShareCountFromCommentEvents(targetType, targetId){
    const { data, error } = await fetchCommentRowsCompat(targetType, targetId, 5000);
    if(error){
      return { count:0, error };
    }
    let count = 0;
    (data || []).forEach(row => {
      const evt = parseCommentEvent(extractCommentBody(row));
      if(evt && evt.kind === "share"){
        count += 1;
      }
    });
    return { count, error:null };
  }

  window.NOVA.trackShare = async function(targetType, targetId, channel){
    const safeType = String(targetType || "").trim();
    const safeId = String(targetId || "").trim();
    if(!safeType || !safeId){
      return { ok:false, error:new Error("invalid_share_target"), mode:"none" };
    }

    if(safeType !== "post"){
      const fallbackOnly = await addCommentEvent(safeType, safeId, {
        v: 1,
        kind: "share",
        channel: String(channel || "unknown")
      });
      if(!fallbackOnly.ok){
        return { ok:false, error:fallbackOnly.error || new Error("share_insert_failed"), mode:"comments" };
      }
      return { ok:true, error:null, mode:"comments" };
    }

    const user = await window.NOVA.requireUser();
    const mode = await detectShareStorageMode();
    if(mode === "post_shares"){
      const rowInsert = await insertPostShareRow(user.id, safeType, safeId, channel);
      if(rowInsert.ok){
        return { ok:true, error:null, mode:"post_shares" };
      }
      if(rowInsert.missingTable){
        shareStorageMode = "comments";
      }else{
        console.error("Share insert failed", rowInsert.error);
      }
    }

    const fallback = await addCommentEvent(safeType, safeId, {
      v: 1,
      kind: "share",
      channel: String(channel || "unknown")
    });
    if(!fallback.ok){
      return { ok:false, error:fallback.error || new Error("share_insert_failed"), mode:"comments" };
    }
    return { ok:true, error:null, mode:"comments" };
  };

  window.NOVA.getShareCount = async function(targetType, targetId){
    const safeType = String(targetType || "").trim();
    const safeId = String(targetId || "").trim();
    if(!safeType || !safeId){
      return 0;
    }

    if(safeType !== "post"){
      const fallbackCountOnly = await readShareCountFromCommentEvents(safeType, safeId);
      if(fallbackCountOnly.error){
        return 0;
      }
      return Math.max(0, Number(fallbackCountOnly.count || 0));
    }

    const mode = await detectShareStorageMode();
    if(mode === "post_shares"){
      const rowCount = await readPostShareCount(safeType, safeId);
      if(!rowCount.error){
        return Math.max(0, Number(rowCount.count || 0));
      }
      if(rowCount.missingTable){
        shareStorageMode = "comments";
      }else{
        console.error("Share count read failed", rowCount.error);
      }
    }

    const fallbackCount = await readShareCountFromCommentEvents(safeType, safeId);
    if(fallbackCount.error){
      return 0;
    }
    return Math.max(0, Number(fallbackCount.count || 0));
  };

  window.NOVA.addComment = async function(targetType, targetId, directText){
    const user = await window.NOVA.requireUser();
    const pickedText = typeof directText === "string" ? directText : prompt("Write a comment");
    if(!pickedText || !String(pickedText).trim()) return false;

    const result = await insertCommentCompat(user.id, targetType, targetId, pickedText);
    if(!result.ok){
      console.error("Comment insert failed", result.error);
      return false;
    }
    return true;
  };

  window.NOVA.replyComment = async function(targetType, targetId, parentRowId, text){
    const body = String(text || "").trim();
    if(!body || !parentRowId) return false;
    const res = await addCommentEvent(targetType, targetId, {
      v: 1,
      kind: "reply",
      parentRowId: String(parentRowId),
      body
    });
    if(!res.ok){
      console.error("Reply insert failed", res.error);
      return false;
    }
    return true;
  };

  window.NOVA.editComment = async function(targetType, targetId, targetRowId, text){
    const body = String(text || "").trim();
    if(!body || !targetRowId) return false;
    const res = await addCommentEvent(targetType, targetId, {
      v: 1,
      kind: "edit",
      targetRowId: String(targetRowId),
      body
    });
    if(!res.ok){
      console.error("Comment edit failed", res.error);
      return false;
    }
    return true;
  };

  window.NOVA.pinComment = async function(targetType, targetId, targetRowId){
    const cleanTarget = String(targetRowId || "");
    const res = await addCommentEvent(targetType, targetId, {
      v: 1,
      kind: "pin",
      targetRowId: cleanTarget
    });
    if(!res.ok){
      console.error("Pin comment failed", res.error);
      return false;
    }
    return true;
  };

  window.NOVA.deleteCommentById = async function(commentRowId){
    const id = String(commentRowId || "");
    if(!id) return false;
    const { error } = await supa.from("comments").delete().eq("id", id);
    if(error){
      console.error("Comment delete failed", error);
      return false;
    }
    return true;
  };

  window.NOVA.getCommentThread = async function(targetType, targetId, options){
    const opts = options || {};
    const { data, error } = await fetchCommentRowsCompat(targetType, targetId, opts.limit || 300);
    if(error){
      return {
        error,
        roots: [],
        flat: [],
        visibleCount: 0,
        pinnedRowId: ""
      };
    }
    const built = buildCommentThread(data, opts);
    return {
      error: null,
      roots: built.roots,
      flat: built.flat,
      visibleCount: built.visibleCount,
      pinnedRowId: built.pinnedRowId
    };
  };

  window.NOVA.getCommentCount = async function(targetType, targetId, options){
    const { visibleCount } = await window.NOVA.getCommentThread(targetType, targetId, options || {});
    return Number(visibleCount || 0);
  };

  window.NOVA.getRecentChatContacts = async function(limit){
    const user = await window.NOVA.requireUser();
    const maxContacts = Math.min(Math.max(Number(limit) || 20, 1), 100);

    const { data, error } = await supa
      .from("chats")
      .select("sender_id,receiver_id,created_at")
      .or(`sender_id.eq.${user.id},receiver_id.eq.${user.id}`)
      .order("created_at", { ascending:false })
      .limit(600);

    if(error){
      return { contacts: [], error };
    }

    const recentByUser = new Map();
    (data || []).forEach(row => {
      const sender = String(row?.sender_id || "");
      const receiver = String(row?.receiver_id || "");
      const otherId = sender === user.id ? receiver : sender;
      if(!otherId || otherId === user.id || recentByUser.has(otherId)) return;
      recentByUser.set(otherId, {
        id: otherId,
        created_at: row?.created_at || ""
      });
    });

    const ids = Array.from(recentByUser.keys()).slice(0, maxContacts);
    if(!ids.length){
      return { contacts: [], error:null };
    }

    const { data: usersData, error: usersError } = await supa
      .from("users")
      .select("user_id,username,full_name,photo")
      .in("user_id", ids)
      .limit(200);

    if(usersError){
      return { contacts: [], error:usersError };
    }

    const userMap = new Map();
    (usersData || []).forEach(row => {
      const id = String(row?.user_id || "");
      if(!id) return;
      userMap.set(id, {
        id,
        username: row?.username || "",
        name: row?.full_name || row?.username || "User",
        avatarUrl: row?.photo || "",
        created_at: recentByUser.get(id)?.created_at || ""
      });
    });

    const contacts = ids
      .map(id => userMap.get(id))
      .filter(Boolean);

    return { contacts, error:null };
  };

  window.NOVA.sendChatMessage = async function(receiverId, text){
    const user = await window.NOVA.requireUser();
    const receiver = String(receiverId || "");
    const messageText = String(text || "").trim();
    if(!receiver || receiver === user.id || !messageText){
      return { ok:false, error:new Error("invalid_chat_message"), data:null };
    }

    const payload = {
      sender_id: user.id,
      receiver_id: receiver,
      message: messageText,
      media_type: "text"
    };

    const { data, error } = await supa
      .from("chats")
      .insert(payload)
      .select("id,sender_id,receiver_id,message,created_at")
      .maybeSingle();

    if(error){
      return { ok:false, error, data:null };
    }
    return { ok:true, error:null, data:data || null };
  };

  let storyViewStorageMode = "comments";
  let storyViewStorageModePromise = null;
  const storyOwnerCache = new Map();

  async function resolveViewerName(user){
    const fallbackName = String((user?.email || "user").split("@")[0] || "user");
    const viewerId = String(user?.id || "");
    if(!viewerId){
      return fallbackName;
    }
    try{
      const { data: profile } = await supa
        .from("users")
        .select("username,full_name")
        .eq("user_id", viewerId)
        .maybeSingle();
      if(profile?.username) return String(profile.username);
      if(profile?.full_name) return String(profile.full_name);
    }catch(_){}
    return fallbackName;
  }

  function getStoryEventTarget(storyId){
    return {
      targetType: "post",
      targetId: String(storyId || "").trim()
    };
  }

  async function getStoryOwnerId(storyId){
    const cleanId = String(storyId || "").trim();
    if(!cleanId) return "";
    if(storyOwnerCache.has(cleanId)){
      return String(storyOwnerCache.get(cleanId) || "");
    }
    try{
      const { data, error } = await supa
        .from("stories")
        .select("user_id")
        .eq("id", cleanId)
        .maybeSingle();
      if(!error && data?.user_id){
        const ownerId = String(data.user_id || "");
        storyOwnerCache.set(cleanId, ownerId);
        return ownerId;
      }
    }catch(_){}
    return "";
  }

  async function canCurrentUserSeeStoryInsights(storyId){
    const cleanId = String(storyId || "").trim();
    if(!cleanId) return false;
    let viewerId = "";
    try{
      const viewer = await window.NOVA.getUser();
      viewerId = String(viewer?.id || "");
    }catch(_){
      viewerId = "";
    }
    if(!viewerId) return false;
    const ownerId = await getStoryOwnerId(cleanId);
    return !!ownerId && ownerId === viewerId;
  }

  async function detectStoryViewStorageMode(){
    if(storyViewStorageModePromise) return storyViewStorageModePromise;
    return storyViewStorageMode || "comments";
  }

  function parseStoryInteractionEvent(row){
    const evt = parseCommentEvent(extractCommentBody(row));
    if(!evt || !evt.kind) return null;
    const kind = String(evt.kind || "").toLowerCase();
    if(kind !== "story_view" && kind !== "story_like") return null;
    return {
      ...evt,
      kind
    };
  }

  async function readStoryEventsFromComments(storyId){
    const target = getStoryEventTarget(storyId);
    if(!target.targetId){
      return {
        error: new Error("story_id_required"),
        viewByUser: new Map(),
        likeByUser: new Map()
      };
    }

    const { data, error } = await fetchCommentRowsCompat(target.targetType, target.targetId, 5000);
    if(error){
      return {
        error,
        viewByUser: new Map(),
        likeByUser: new Map()
      };
    }

    const rows = Array.isArray(data) ? data.slice() : [];
    rows.sort((a, b) => {
      const aTs = Date.parse(String(a?.created_at || "")) || 0;
      const bTs = Date.parse(String(b?.created_at || "")) || 0;
      return aTs - bTs;
    });

    const viewByUser = new Map();
    const likeByUser = new Map();

    rows.forEach(row => {
      const evt = parseStoryInteractionEvent(row);
      if(!evt) return;

      if(evt.kind === "story_view"){
        const viewerId = String(evt.viewerId || evt.userId || row?.user_id || "").trim();
        if(!viewerId) return;

        const nextCreatedAt = String(row?.created_at || "");
        const nextName = String(evt.viewerName || evt.userName || "").trim() || "user";
        const prev = viewByUser.get(viewerId);
        if(!prev){
          viewByUser.set(viewerId, {
            viewer_id: viewerId,
            viewer_name: nextName,
            created_at: nextCreatedAt
          });
          return;
        }
        const prevTs = Date.parse(String(prev.created_at || "")) || 0;
        const nextTs = Date.parse(nextCreatedAt) || 0;
        if(nextTs >= prevTs){
          viewByUser.set(viewerId, {
            viewer_id: viewerId,
            viewer_name: nextName || prev.viewer_name || "user",
            created_at: nextCreatedAt || prev.created_at || ""
          });
        }
        return;
      }

      if(evt.kind === "story_like"){
        const likerId = String(evt.userId || evt.viewerId || row?.user_id || "").trim();
        if(!likerId) return;
        likeByUser.set(likerId, {
          user_id: likerId,
          active: evt.active !== false,
          user_name: String(evt.userName || evt.viewerName || "").trim(),
          created_at: String(row?.created_at || "")
        });
      }
    });

    return {
      error: null,
      viewByUser,
      likeByUser
    };
  }

  function countActiveStoryLikes(likeByUser){
    let count = 0;
    likeByUser.forEach(row => {
      if(row?.active) count += 1;
    });
    return count;
  }

  async function markStoryViewWithTable(storyId, viewerId, viewerName){
    const upsertOptions = {
      onConflict: "story_id,viewer_id",
      ignoreDuplicates: false
    };
    let { error } = await supa
      .from("story_views")
      .upsert({
        story_id: storyId,
        viewer_id: viewerId,
        viewer_name: viewerName
      }, upsertOptions);

    if(error && getMissingColumnName(error) === "viewer_name"){
      const retry = await supa
        .from("story_views")
        .upsert({
          story_id: storyId,
          viewer_id: viewerId
        }, upsertOptions);
      error = retry.error || null;
    }

    if(error){
      return {
        ok: false,
        error,
        missingTable: isMissingRelation(error, "story_views")
      };
    }
    return { ok:true, error:null, missingTable:false };
  }

  async function markStoryViewWithComments(storyId, viewerId, viewerName){
    const state = await readStoryEventsFromComments(storyId);
    if(state.error){
      return { ok:false, error:state.error, count:0 };
    }
    if(state.viewByUser.has(viewerId)){
      return { ok:true, error:null, count:state.viewByUser.size };
    }

    const target = getStoryEventTarget(storyId);
    const inserted = await addCommentEvent(target.targetType, target.targetId, {
      v: 1,
      kind: "story_view",
      viewerId,
      viewerName
    });
    if(!inserted.ok){
      return { ok:false, error:inserted.error || new Error("story_view_insert_failed"), count:state.viewByUser.size };
    }
    return { ok:true, error:null, count:state.viewByUser.size + 1 };
  }

  async function getStoryViewCountFromComments(storyId){
    const state = await readStoryEventsFromComments(storyId);
    if(state.error) return 0;
    return state.viewByUser.size;
  }

  async function getStoryViewersFromComments(storyId, limit){
    const max = Math.max(1, Math.min(200, Number(limit) || 100));
    const state = await readStoryEventsFromComments(storyId);
    if(state.error) return [];
    return Array.from(state.viewByUser.values())
      .sort((a, b) => {
        const aTs = Date.parse(String(a?.created_at || "")) || 0;
        const bTs = Date.parse(String(b?.created_at || "")) || 0;
        return bTs - aTs;
      })
      .slice(0, max);
  }

  async function readReelViewEvents(reelId){
    const cleanId = String(reelId || "").trim();
    if(!cleanId){
      return { error:new Error("reel_id_required"), viewers:new Set() };
    }
    const { data, error } = await fetchCommentRowsCompat("reel", cleanId, 5000);
    if(error){
      return { error, viewers:new Set() };
    }
    const viewers = new Set();
    (data || []).forEach(row => {
      const evt = parseCommentEvent(extractCommentBody(row));
      if(!evt || evt.kind !== "reel_view") return;
      const viewerId = String(evt.viewerId || evt.userId || row?.user_id || "").trim();
      if(!viewerId) return;
      viewers.add(viewerId);
    });
    return { error:null, viewers };
  }

  window.NOVA.markStoryView = async function(storyId){
    const cleanId = String(storyId || "").trim();
    if(!cleanId){
      return { ok:false, error:new Error("story_id_required"), count:0 };
    }

    const user = await window.NOVA.requireUser();
    const viewerId = String(user?.id || "");
    if(!viewerId){
      return { ok:false, error:new Error("user_id_required"), count:0 };
    }
    const viewerName = await resolveViewerName(user);

    const mode = await detectStoryViewStorageMode();
    if(mode === "story_views"){
      const tableResult = await markStoryViewWithTable(cleanId, viewerId, viewerName);
      if(tableResult.ok){
        const count = await window.NOVA.getStoryViewCount(cleanId);
        return { ok:true, error:null, count:Number(count || 0) };
      }
      if(tableResult.missingTable){
        storyViewStorageMode = "comments";
      }else{
        console.error("Story view insert failed on story_views, trying fallback", tableResult.error);
      }
    }

    return markStoryViewWithComments(cleanId, viewerId, viewerName);
  };

  window.NOVA.getStoryViewCount = async function(storyId){
    const cleanId = String(storyId || "").trim();
    if(!cleanId) return 0;
    const allowed = await canCurrentUserSeeStoryInsights(cleanId);
    if(!allowed){
      return 0;
    }

    const mode = await detectStoryViewStorageMode();
    if(mode === "story_views"){
      try{
        const { count, error } = await supa
          .from("story_views")
          .select("id", { count: "exact", head: true })
          .eq("story_id", cleanId);
        if(!error && count !== null){
          return Number(count || 0);
        }
        if(error && isMissingRelation(error, "story_views")){
          storyViewStorageMode = "comments";
        }else if(error){
          console.error("Story view count failed on story_views, trying fallback", error);
        }
      }catch(_){}
    }

    return getStoryViewCountFromComments(cleanId);
  };

  window.NOVA.getStoryViewers = async function(storyId, limit){
    const cleanId = String(storyId || "").trim();
    if(!cleanId) return [];
    const allowed = await canCurrentUserSeeStoryInsights(cleanId);
    if(!allowed){
      return [];
    }
    const max = Math.max(1, Math.min(200, Number(limit) || 100));

    const mode = await detectStoryViewStorageMode();
    if(mode === "story_views"){
      try{
        const { data, error } = await supa
          .from("story_views")
          .select("viewer_id,viewer_name,created_at")
          .eq("story_id", cleanId)
          .order("created_at", { ascending:false })
          .limit(max);
        if(!error){
          return Array.isArray(data) ? data : [];
        }
        if(isMissingRelation(error, "story_views")){
          storyViewStorageMode = "comments";
        }else{
          console.error("Story viewers fetch failed on story_views, trying fallback", error);
        }
      }catch(_){}
    }

    return getStoryViewersFromComments(cleanId, max);
  };

  window.NOVA.getStoryLikeState = async function(storyId, viewerId){
    const cleanId = String(storyId || "").trim();
    if(!cleanId){
      return { count:0, active:false, likers:[] };
    }

    const state = await readStoryEventsFromComments(cleanId);
    if(state.error){
      return { count:0, active:false, likers:[] };
    }

    let me = String(viewerId || "").trim();
    if(!me){
      try{
        const currentUser = await window.NOVA.getUser();
        me = String(currentUser?.id || "");
      }catch(_){}
    }

    const likers = Array.from(state.likeByUser.values()).filter(row => row?.active);
    const canSeeTotals = await canCurrentUserSeeStoryInsights(cleanId);
    const count = canSeeTotals ? likers.length : 0;
    const active = !!(me && state.likeByUser.get(me)?.active);
    return { count, active, likers: canSeeTotals ? likers : [] };
  };

  window.NOVA.getStoryLikeCount = async function(storyId){
    const state = await window.NOVA.getStoryLikeState(storyId);
    return Number(state?.count || 0);
  };

  window.NOVA.toggleStoryLike = async function(storyId){
    const cleanId = String(storyId || "").trim();
    if(!cleanId){
      return { ok:false, error:new Error("story_id_required"), count:0, active:false };
    }

    const user = await window.NOVA.requireUser();
    const userId = String(user?.id || "");
    if(!userId){
      return { ok:false, error:new Error("user_id_required"), count:0, active:false };
    }

    const state = await readStoryEventsFromComments(cleanId);
    if(state.error){
      return { ok:false, error:state.error, count:0, active:false };
    }

    const prevActive = !!state.likeByUser.get(userId)?.active;
    const nextActive = !prevActive;
    const userName = await resolveViewerName(user);
    const target = getStoryEventTarget(cleanId);
    const inserted = await addCommentEvent(target.targetType, target.targetId, {
      v: 1,
      kind: "story_like",
      userId,
      userName,
      active: nextActive
    });

    if(!inserted.ok){
      return { ok:false, error:inserted.error || new Error("story_like_insert_failed"), count:countActiveStoryLikes(state.likeByUser), active:prevActive };
    }

    state.likeByUser.set(userId, {
      user_id: userId,
      active: nextActive,
      user_name: userName,
      created_at: new Date().toISOString()
    });
    const canSeeTotals = await canCurrentUserSeeStoryInsights(cleanId);
    return {
      ok: true,
      error: null,
      count: canSeeTotals ? countActiveStoryLikes(state.likeByUser) : 0,
      active: nextActive
    };
  };

  window.NOVA.markReelView = async function(reelId){
    const cleanId = String(reelId || "").trim();
    if(!cleanId){
      return { ok:false, error:new Error("reel_id_required"), count:0 };
    }

    const user = await window.NOVA.requireUser();
    const viewerId = String(user?.id || "");
    if(!viewerId){
      return { ok:false, error:new Error("user_id_required"), count:0 };
    }

    const state = await readReelViewEvents(cleanId);
    if(state.error){
      return { ok:false, error:state.error, count:0 };
    }
    if(state.viewers.has(viewerId)){
      return { ok:true, error:null, count:state.viewers.size };
    }

    const inserted = await addCommentEvent("reel", cleanId, {
      v: 1,
      kind: "reel_view",
      viewerId
    });
    if(!inserted.ok){
      return { ok:false, error:inserted.error || new Error("reel_view_insert_failed"), count:state.viewers.size };
    }

    return { ok:true, error:null, count:state.viewers.size + 1 };
  };

  window.NOVA.getReelViewCount = async function(reelId){
    const cleanId = String(reelId || "").trim();
    if(!cleanId) return 0;
    const state = await readReelViewEvents(cleanId);
    if(state.error){
      return 0;
    }
    return state.viewers.size;
  };

  }

  const immediateClient = createClientFromWindow();
  if(immediateClient){
    initSocial(immediateClient);
    return;
  }

  (async () => {
    const supa = await ensureClient();
    if(!supa){
      showSdkLoadError();
      return;
    }
    initSocial(supa);
  })();
})();
