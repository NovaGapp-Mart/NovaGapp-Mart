(function(){
  "use strict";

  const PAGE_SIZE = 24;
  const SEARCH_HISTORY_KEY = "mv_search_history_v1";
  const WATCH_LATER_KEY = "mv_watch_later_v1";
  const QUEUE_KEY = "mv_queue_v1";
  const MAX_VIDEO_SIZE = 600 * 1024 * 1024;
  const MAX_THUMB_SIZE = 8 * 1024 * 1024;
  const VIDEO_SELECT = "id,user_id,title,description,video_url,thumbnail_url,views,likes_count,dislikes_count,monetized,created_at,category,tags,duration_seconds";
  const VIDEO_MIME_TYPES = new Set(["video/mp4","video/webm","video/quicktime","video/x-matroska","video/ogg","video/mpeg"]);
  const THUMB_MIME_TYPES = new Set(["image/jpeg","image/png","image/webp"]);

  const state = {
    supa:null,
    me:null,
    panel:"feed",
    videos:new Map(),
    channels:new Map(),
    cards:new Map(),
    feedOffset:0,
    feedDone:false,
    feedLoading:false,
    searchTerm:"",
    searchPool:[],
    searchDebounce:0,
    suggestDebounce:0,
    activeVideoId:"",
    activeReaction:null,
    subscribed:false,
    channelSubscribers:0,
    reactionBusy:false,
    subscribeBusy:false,
    commentBusy:false,
    replyParentId:"",
    watchToken:0,
    realtimeChannel:null,
    observer:null,
    timers:{},
    uploadVideoFile:null,
    uploadThumbFile:null
  };

  const dom = {
    panels:{
      feed:document.getElementById("mvFeedView"),
      watch:document.getElementById("mvWatchView"),
      upload:document.getElementById("mvUploadView"),
      dashboard:document.getElementById("mvDashboardView")
    },
    sideButtons:Array.from(document.querySelectorAll("[data-panel]")),
    openUpload:document.getElementById("mvOpenUpload"),
    openDashboard:document.getElementById("mvOpenDashboard"),
    searchForm:document.getElementById("mvSearchForm"),
    searchInput:document.getElementById("mvSearchInput"),
    searchClear:document.getElementById("mvSearchClear"),
    suggestBox:document.getElementById("mvSuggestBox"),
    grid:document.getElementById("mvFeedGrid"),
    feedEmpty:document.getElementById("mvFeedEmpty"),
    sentinel:document.getElementById("mvFeedSentinel"),
    watchBack:document.getElementById("mvWatchBack"),
    player:document.getElementById("mvPlayer"),
    watchTitle:document.getElementById("mvWatchTitle"),
    watchMeta:document.getElementById("mvWatchMeta"),
    watchAvatar:document.getElementById("mvWatchAvatar"),
    watchChannelName:document.getElementById("mvWatchChannelName"),
    watchChannelSub:document.getElementById("mvWatchChannelSub"),
    subscribeBtn:document.getElementById("mvSubscribeBtn"),
    likeBtn:document.getElementById("mvLikeBtn"),
    dislikeBtn:document.getElementById("mvDislikeBtn"),
    likeCount:document.getElementById("mvLikeCount"),
    dislikeCount:document.getElementById("mvDislikeCount"),
    shareBtn:document.getElementById("mvShareBtn"),
    saveBtn:document.getElementById("mvSaveBtn"),
    descText:document.getElementById("mvDescText"),
    descToggle:document.getElementById("mvDescToggle"),
    commentCount:document.getElementById("mvCommentCount"),
    commentForm:document.getElementById("mvCommentForm"),
    commentInput:document.getElementById("mvCommentInput"),
    commentsEmpty:document.getElementById("mvCommentsEmpty"),
    commentsList:document.getElementById("mvCommentsList"),
    replying:document.getElementById("mvReplying"),
    replyingText:document.getElementById("mvReplyingText"),
    replyCancel:document.getElementById("mvReplyCancel"),
    recommendList:document.getElementById("mvRecommendList"),
    uploadForm:document.getElementById("mvUploadForm"),
    uploadTitle:document.getElementById("mvUploadTitle"),
    uploadDescription:document.getElementById("mvUploadDescription"),
    uploadTags:document.getElementById("mvUploadTags"),
    uploadCategory:document.getElementById("mvUploadCategory"),
    uploadMonetized:document.getElementById("mvUploadMonetized"),
    videoDrop:document.getElementById("mvVideoDrop"),
    thumbDrop:document.getElementById("mvThumbDrop"),
    videoFileInput:document.getElementById("mvVideoFileInput"),
    thumbFileInput:document.getElementById("mvThumbFileInput"),
    videoFileName:document.getElementById("mvVideoFileName"),
    thumbFileName:document.getElementById("mvThumbFileName"),
    thumbPreviewWrap:document.getElementById("mvThumbPreviewWrap"),
    thumbPreview:document.getElementById("mvThumbPreview"),
    uploadSubmit:document.getElementById("mvUploadSubmit"),
    uploadProgress:document.getElementById("mvUploadProgress"),
    uploadStatus:document.getElementById("mvUploadStatus"),
    dashViews:document.getElementById("mvDashViews"),
    dashRevenue:document.getElementById("mvDashRevenue"),
    dashRpm:document.getElementById("mvDashRpm"),
    dashMonthly:document.getElementById("mvDashMonthly"),
    dashRefresh:document.getElementById("mvDashRefresh"),
    footerButtons:Array.from(document.querySelectorAll("[data-go]")),
    toast:document.getElementById("mvToast")
  };

  const util = {
    safe(v){ return String(v || "").trim(); },
    num(v){ const n = Number(v); return Number.isFinite(n) ? n : 0; },
    sleep(ms){ return new Promise(r => setTimeout(r, ms)); },
    esc(v){ return util.safe(v).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/\"/g,"&quot;").replace(/'/g,"&#39;"); },
    compact(v){ const n = Math.max(0, util.num(v)); if(n >= 1e9) return (n / 1e9).toFixed(1).replace(/\.0$/,"") + "B"; if(n >= 1e6) return (n / 1e6).toFixed(1).replace(/\.0$/,"") + "M"; if(n >= 1e3) return (n / 1e3).toFixed(1).replace(/\.0$/,"") + "K"; return String(Math.round(n)); },
    money(v){ return new Intl.NumberFormat("en-US", { style:"currency", currency:"USD", minimumFractionDigits:2, maximumFractionDigits:2 }).format(Math.max(0, util.num(v))); },
    ago(v){ const ts = Date.parse(util.safe(v)); if(!Number.isFinite(ts)) return ""; const sec = Math.max(1, Math.floor((Date.now() - ts) / 1000)); if(sec < 60) return sec + "s ago"; if(sec < 3600) return Math.floor(sec / 60) + "m ago"; if(sec < 86400) return Math.floor(sec / 3600) + "h ago"; if(sec < 604800) return Math.floor(sec / 86400) + "d ago"; return new Date(ts).toLocaleDateString(undefined, { month:"short", day:"numeric" }); },
    duration(v){ const sec = Math.max(0, Math.floor(util.num(v))); const h = Math.floor(sec / 3600); const m = Math.floor((sec % 3600) / 60); const s = sec % 60; if(h > 0) return h + ":" + String(m).padStart(2, "0") + ":" + String(s).padStart(2, "0"); return m + ":" + String(s).padStart(2, "0"); },
    clean(v){ return util.safe(v).toLowerCase().replace(/[,%]/g, " ").replace(/\s+/g, " ").slice(0, 80); },
    tags(v){ return util.safe(v).split(",").map(s => s.trim()).filter(Boolean).slice(0, 20); },
    uniq(arr){ return Array.from(new Set((Array.isArray(arr) ? arr : []).map(v => util.safe(v)).filter(Boolean))); }
  };

  const store = {
    read(key, fallback){ try{ const raw = localStorage.getItem(key); if(!raw) return fallback; const parsed = JSON.parse(raw); return parsed === null || parsed === undefined ? fallback : parsed; }catch(_){ return fallback; } },
    write(key, value){ try{ localStorage.setItem(key, JSON.stringify(value)); }catch(_){ } },
    pushUnique(key, value, limit){ const val = util.safe(value); if(!val) return; const list = store.read(key, []); const next = [val].concat(list.filter(item => util.safe(item) !== val)).slice(0, limit || 50); store.write(key, next); }
  };

  function normalizeVideo(row){
    const tagsRaw = Array.isArray(row?.tags) ? row.tags.join(", ") : util.safe(row?.tags);
    return {
      id:util.safe(row?.id),
      user_id:util.safe(row?.user_id),
      title:util.safe(row?.title) || "Untitled",
      description:util.safe(row?.description),
      video_url:util.safe(row?.video_url),
      thumbnail_url:util.safe(row?.thumbnail_url),
      views:Math.max(0, util.num(row?.views)),
      likes_count:Math.max(0, util.num(row?.likes_count)),
      dislikes_count:Math.max(0, util.num(row?.dislikes_count)),
      monetized:!!row?.monetized,
      created_at:util.safe(row?.created_at),
      category:util.safe(row?.category) || "General",
      tags:tagsRaw,
      duration_seconds:Math.max(0, util.num(row?.duration_seconds))
    };
  }

  function showToast(message, isError){
    dom.toast.textContent = util.safe(message);
    dom.toast.style.background = isError ? "#9f1d1d" : "#111";
    dom.toast.classList.add("show");
    clearTimeout(state.timers.toast);
    state.timers.toast = setTimeout(() => dom.toast.classList.remove("show"), 2000);
  }

  function currentVideo(){ return state.activeVideoId ? state.videos.get(state.activeVideoId) || null : null; }

  function getChannel(userId){
    const id = util.safe(userId);
    if(!id) return { id:"", name:"User", avatar:"", subscribers:0 };
    if(state.channels.has(id)) return state.channels.get(id);
    return { id, name:"User", avatar:"", subscribers:0 };
  }

  function avatarHtml(channel){
    const name = util.safe(channel?.name) || "U";
    const avatarUrl = util.safe(channel?.avatar);
    if(avatarUrl) return '<img src="' + util.esc(avatarUrl) + '" alt="' + util.esc(name) + '">';
    return util.esc(name.slice(0, 1).toUpperCase());
  }

  function closeMenus(){ document.querySelectorAll(".mv-menu.show").forEach(node => node.classList.remove("show")); }

  function setFeedEmpty(message){
    const hasRows = dom.grid.children.length > 0;
    dom.feedEmpty.classList.toggle("mv-hidden", hasRows);
    if(!hasRows) dom.feedEmpty.textContent = util.safe(message) || "No videos found.";
  }

  function setSaveButton(){
    const video = currentVideo();
    if(!video){
      dom.saveBtn.textContent = "Save";
      dom.saveBtn.classList.remove("active");
      return;
    }
    const list = store.read(WATCH_LATER_KEY, []);
    const saved = list.includes(video.id);
    dom.saveBtn.textContent = saved ? "Saved" : "Save";
    dom.saveBtn.classList.toggle("active", saved);
  }

  function setReactionButtons(){
    dom.likeBtn.classList.toggle("active", state.activeReaction === "like");
    dom.dislikeBtn.classList.toggle("active", state.activeReaction === "dislike");
  }

  function setUploadProgress(percent, text){
    const pct = Math.max(0, Math.min(100, util.num(percent)));
    dom.uploadProgress.style.width = pct + "%";
    dom.uploadStatus.textContent = util.safe(text);
  }

  function resetReplyTarget(){
    state.replyParentId = "";
    dom.replying.hidden = true;
    dom.replyingText.textContent = "";
  }

  function schedule(key, fn, delay){
    clearTimeout(state.timers[key]);
    state.timers[key] = setTimeout(fn, delay || 180);
  }

  const api = {
    async waitForSupabase(timeoutMs){
      const start = Date.now();
      const timeout = util.num(timeoutMs) || 9000;
      while(Date.now() - start < timeout){
        if(window.NOVA && window.NOVA.supa) return window.NOVA.supa;
        await util.sleep(80);
      }
      return null;
    },

    async ensureAuth(){
      if(state.me && state.me.id) return state.me;
      if(window.NOVA && typeof window.NOVA.getUser === "function"){
        try{
          const user = await window.NOVA.getUser({ silent:true });
          if(user && user.id) state.me = user;
        }catch(_){ }
      }
      return state.me;
    },

    async requireAuth(){
      const existing = await api.ensureAuth();
      if(existing && existing.id) return existing;
      if(window.NOVA && typeof window.NOVA.requireUser === "function"){
        try{
          const user = await window.NOVA.requireUser();
          if(user && user.id){
            state.me = user;
            return user;
          }
        }catch(_){ }
      }
      location.href = "login.html";
      return null;
    },

    async fetchProfiles(userIds){
      const ids = util.uniq(userIds);
      if(!ids.length) return;
      const attempts = [
        { idField:"user_id", select:"user_id,username,full_name,photo,channel_subscribers_count" },
        { idField:"id", select:"id,username,full_name,photo,channel_subscribers_count" },
        { idField:"user_id", select:"user_id,username,full_name,photo" },
        { idField:"id", select:"id,username,full_name,photo" }
      ];
      for(const plan of attempts){
        const { data, error } = await state.supa.from("users").select(plan.select).in(plan.idField, ids).limit(500);
        if(error) continue;
        (data || []).forEach(row => {
          const id = util.safe(row[plan.idField]);
          if(!id) return;
          state.channels.set(id, {
            id,
            name:util.safe(row.full_name) || util.safe(row.username) || "User",
            avatar:util.safe(row.photo),
            subscribers:Math.max(0, util.num(row.channel_subscribers_count))
          });
        });
        return;
      }
    },

    async searchChannelIds(term){
      const clean = util.clean(term);
      if(!clean) return [];
      const attempts = [
        { idField:"user_id", select:"user_id,username,full_name" },
        { idField:"id", select:"id,username,full_name" },
        { idField:"user_id", select:"user_id,username" },
        { idField:"id", select:"id,username" }
      ];
      for(const plan of attempts){
        const { data, error } = await state.supa.from("users").select(plan.select).or("username.ilike.%" + clean + "%,full_name.ilike.%" + clean + "%").limit(40);
        if(error) continue;
        return util.uniq((data || []).map(row => row[plan.idField]));
      }
      return [];
    },

    async fetchVideos(options){
      const opts = options || {};
      let query = state.supa.from("videos").select(VIDEO_SELECT).order("created_at", { ascending:false });
      if(opts.excludeId) query = query.neq("id", opts.excludeId);
      if(opts.category) query = query.eq("category", opts.category);
      if(Array.isArray(opts.userIds) && opts.userIds.length) query = query.in("user_id", opts.userIds.slice(0, 100));
      if(opts.search){
        const filter = util.clean(opts.search);
        if(filter) query = query.or("title.ilike.%" + filter + "%,tags.ilike.%" + filter + "%,category.ilike.%" + filter + "%");
      }
      if(typeof opts.offset === "number" && typeof opts.limit === "number"){
        const from = Math.max(0, opts.offset);
        query = query.range(from, from + Math.max(1, opts.limit) - 1);
      }else if(typeof opts.limit === "number"){
        query = query.limit(Math.max(1, opts.limit));
      }
      const { data, error } = await query;
      if(error) throw error;
      return (data || []).map(normalizeVideo);
    },

    async fetchVideoById(videoId){
      const id = util.safe(videoId);
      if(!id) return null;
      const { data, error } = await state.supa.from("videos").select(VIDEO_SELECT).eq("id", id).maybeSingle();
      if(error || !data) return null;
      return normalizeVideo(data);
    }
  };

  api.buildSearchPool = async function(term){
    const text = util.clean(term);
    if(!text) return [];
    const [contentRows, channelIds] = await Promise.all([
      api.fetchVideos({ search:text, limit:180 }),
      api.searchChannelIds(text)
    ]);
    let channelRows = [];
    if(channelIds.length){
      channelRows = await api.fetchVideos({ userIds:channelIds, limit:180 });
    }
    const seen = new Set();
    const merged = [];
    contentRows.concat(channelRows).forEach(row => {
      if(!row || !row.id || seen.has(row.id)) return;
      seen.add(row.id);
      merged.push(row);
    });
    merged.sort((a, b) => {
      const left = Date.parse(a.created_at || "") || 0;
      const right = Date.parse(b.created_at || "") || 0;
      return right - left;
    });
    return merged;
  };

  api.fetchSuggestions = async function(term){
    const text = util.clean(term);
    if(!text){
      return store.read(SEARCH_HISTORY_KEY, []).slice(0, 6).map(item => ({ kind:"history", term:item }));
    }
    const rows = await api.fetchVideos({ search:text, limit:8 });
    await api.fetchProfiles(rows.map(row => row.user_id));
    return rows.slice(0, 6).map(row => ({ kind:"video", video:row }));
  };

  function upsertVideos(rows){
    (Array.isArray(rows) ? rows : []).forEach(row => {
      if(!row || !row.id) return;
      state.videos.set(row.id, normalizeVideo(row));
    });
  }

  function updateFeedCard(videoId){
    const id = util.safe(videoId);
    const card = state.cards.get(id);
    const video = state.videos.get(id);
    if(!card || !video) return;
    const channel = getChannel(video.user_id);
    const avatar = card.querySelector("[data-avatar]");
    const title = card.querySelector("[data-title]");
    const channelNode = card.querySelector("[data-channel]");
    const stats = card.querySelector("[data-stats]");
    const image = card.querySelector(".mv-thumb-wrap img");
    const duration = card.querySelector(".mv-duration");
    if(avatar) avatar.innerHTML = avatarHtml(channel);
    if(title) title.textContent = video.title;
    if(channelNode) channelNode.textContent = channel.name;
    if(stats) stats.textContent = util.compact(video.views) + " views . " + util.ago(video.created_at);
    if(image && video.thumbnail_url) image.src = video.thumbnail_url;
    if(duration) duration.textContent = util.duration(video.duration_seconds);
  }

  function renderFeedCard(video){
    const item = normalizeVideo(video);
    const existing = state.cards.get(item.id);
    if(existing){
      updateFeedCard(item.id);
      return;
    }
    const channel = getChannel(item.user_id);
    const card = document.createElement("article");
    card.className = "mv-card";
    card.dataset.videoId = item.id;
    card.innerHTML =
      '<button type="button" class="mv-thumb-btn" data-open="1">' +
        '<div class="mv-thumb-wrap">' +
          '<img src="' + util.esc(item.thumbnail_url || "Images/no-image.jpg") + '" alt="' + util.esc(item.title) + '">' +
          '<span class="mv-duration">' + util.esc(util.duration(item.duration_seconds)) + '</span>' +
          '<div class="mv-thumb-tools">' +
            '<button type="button" class="mv-thumb-tool" data-thumb-action="watch_later" aria-label="Watch later"><svg viewBox="0 0 24 24"><path d="M12 2a10 10 0 1 0 10 10A10 10 0 0 0 12 2zm1 11h5v2h-7V7h2z"></path></svg></button>' +
            '<button type="button" class="mv-thumb-tool" data-thumb-action="queue" aria-label="Add to queue"><svg viewBox="0 0 24 24"><path d="M4 10h16v2H4zm0-4h16v2H4zm0 8h10v2H4zm12.5 0v-3h2v3h3v2h-3v3h-2v-3h-3v-2z"></path></svg></button>' +
          '</div>' +
        '</div>' +
      '</button>' +
      '<div class="mv-card-meta">' +
        '<div class="mv-avatar" data-avatar>' + avatarHtml(channel) + '</div>' +
        '<div class="mv-card-copy">' +
          '<h3 class="mv-card-title" data-title>' + util.esc(item.title) + '</h3>' +
          '<p class="mv-card-channel" data-channel>' + util.esc(channel.name) + '</p>' +
          '<p class="mv-card-stats" data-stats>' + util.esc(util.compact(item.views) + " views . " + util.ago(item.created_at)) + '</p>' +
        '</div>' +
        '<div class="mv-menu-wrap">' +
          '<button type="button" class="mv-menu-btn" data-menu-btn="1" aria-label="More options"></button>' +
          '<div class="mv-menu" data-menu="1">' +
            '<button type="button" data-menu-action="watch">Watch</button>' +
            '<button type="button" data-menu-action="watch_later">Save to Watch Later</button>' +
            '<button type="button" data-menu-action="queue">Add to Queue</button>' +
            '<button type="button" data-menu-action="share">Share</button>' +
          '</div>' +
        '</div>' +
      '</div>';

    const menu = card.querySelector("[data-menu='1']");
    card.querySelector("[data-open='1']").addEventListener("click", () => openVideo(item.id, true, true));
    card.querySelector("[data-menu-btn='1']").addEventListener("click", (event) => {
      event.stopPropagation();
      const visible = !menu.classList.contains("show");
      closeMenus();
      menu.classList.toggle("show", visible);
    });
    card.querySelectorAll("[data-thumb-action]").forEach(btn => {
      btn.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        const action = btn.getAttribute("data-thumb-action");
        if(action === "watch_later"){
          store.pushUnique(WATCH_LATER_KEY, item.id, 100);
          showToast("Saved to Watch Later");
        }else if(action === "queue"){
          store.pushUnique(QUEUE_KEY, item.id, 100);
          showToast("Added to Queue");
        }
      });
    });
    card.querySelectorAll("[data-menu-action]").forEach(btn => {
      btn.addEventListener("click", () => {
        const action = btn.getAttribute("data-menu-action");
        menu.classList.remove("show");
        if(action === "watch") return void openVideo(item.id, true, true);
        if(action === "watch_later"){
          store.pushUnique(WATCH_LATER_KEY, item.id, 100);
          return void showToast("Saved to Watch Later");
        }
        if(action === "queue"){
          store.pushUnique(QUEUE_KEY, item.id, 100);
          return void showToast("Added to Queue");
        }
        if(action === "share") shareVideo(item.id);
      });
    });

    state.cards.set(item.id, card);
    dom.grid.appendChild(card);
  }

  async function loadFeed(reset){
    if(state.feedLoading) return;
    if(reset){
      state.feedOffset = 0;
      state.feedDone = false;
      dom.grid.innerHTML = "";
      state.cards.clear();
      if(state.searchTerm){
        state.searchPool = await api.buildSearchPool(state.searchTerm);
        upsertVideos(state.searchPool);
      }else{
        state.searchPool = [];
      }
    }
    if(state.feedDone) return;
    state.feedLoading = true;
    try{
      let rows = [];
      if(state.searchTerm){
        rows = state.searchPool.slice(state.feedOffset, state.feedOffset + PAGE_SIZE);
      }else{
        rows = await api.fetchVideos({ offset:state.feedOffset, limit:PAGE_SIZE });
        upsertVideos(rows);
      }
      if(!rows.length){
        state.feedDone = true;
        setFeedEmpty(state.searchTerm ? "No matching videos found." : "No videos available yet.");
        return;
      }
      await api.fetchProfiles(rows.map(row => row.user_id));
      rows.forEach(row => renderFeedCard(row));
      state.feedOffset += rows.length;
      if(rows.length < PAGE_SIZE) state.feedDone = true;
      setFeedEmpty("");
    }catch(err){
      console.error("feed_load_failed", err);
      setFeedEmpty("Unable to load feed. Run SQL migration for videos and retry.");
      state.feedDone = true;
    }finally{
      state.feedLoading = false;
    }
  }

  async function shareVideo(videoId){
    const id = util.safe(videoId);
    if(!id) return;
    const video = state.videos.get(id);
    const url = new URL("m-videos.html", location.href);
    url.searchParams.set("v", id);
    try{
      if(navigator.share){
        await navigator.share({ title:video ? video.title : "NOVAGAPP Video", url:url.href });
        return;
      }
    }catch(_){ }
    try{
      if(navigator.clipboard && navigator.clipboard.writeText){
        await navigator.clipboard.writeText(url.href);
        showToast("Video link copied");
        return;
      }
    }catch(_){ }
    window.prompt("Copy this link", url.href);
  }

  async function openVideo(videoId, pushState, autoplay){
    const id = util.safe(videoId);
    if(!id) return;

    let video = state.videos.get(id) || null;
    if(!video){
      video = await api.fetchVideoById(id);
      if(video) state.videos.set(video.id, video);
    }
    if(!video){
      showToast("Video not found", true);
      return;
    }

    state.activeVideoId = id;
    state.watchToken += 1;
    const token = state.watchToken;

    await api.fetchProfiles([video.user_id]);
    const channel = getChannel(video.user_id);

    dom.player.src = video.video_url;
    dom.watchTitle.textContent = video.title;
    dom.watchMeta.textContent = util.compact(video.views) + " views . " + util.ago(video.created_at);
    dom.watchAvatar.innerHTML = avatarHtml(channel);
    dom.watchChannelName.textContent = channel.name;
    dom.watchChannelSub.textContent = util.compact(channel.subscribers) + " subscribers";
    dom.likeCount.textContent = util.compact(video.likes_count);
    dom.dislikeCount.textContent = util.compact(video.dislikes_count);
    dom.descText.textContent = video.description || "No description available.";
    dom.descText.classList.add("clamp");
    dom.descToggle.textContent = "Show more";
    dom.descToggle.hidden = (video.description || "").length < 160;
    setSaveButton();
    setPanel("watch", false);

    if(pushState){
      const url = new URL(location.href);
      url.searchParams.set("v", id);
      url.searchParams.delete("view");
      history.pushState({ mv:"watch", videoId:id }, "", url.pathname + url.search);
    }

    await Promise.all([
      loadReactionState(token),
      loadSubscribeState(token),
      loadComments(token),
      loadRecommendations(token)
    ]);

    await recordView();

    if(autoplay){
      try{ await dom.player.play(); }catch(_){ }
    }
  }

  async function loadReactionState(token){
    if(token !== state.watchToken) return;
    const video = currentVideo();
    if(!video) return;

    const user = await api.ensureAuth();
    if(!user){
      state.activeReaction = null;
      setReactionButtons();
      return;
    }

    const { data } = await state.supa
      .from("video_likes")
      .select("type")
      .eq("video_id", video.id)
      .eq("user_id", user.id)
      .limit(1)
      .maybeSingle();

    state.activeReaction = util.safe(data?.type) || null;
    setReactionButtons();
  }

  async function loadSubscribeState(token){
    if(token !== state.watchToken) return;
    const video = currentVideo();
    if(!video) return;

    const user = await api.ensureAuth();
    const channelId = util.safe(video.user_id);

    const { count } = await state.supa
      .from("channel_subscribers")
      .select("subscriber_user_id", { count:"exact", head:true })
      .eq("channel_id", channelId);

    state.channelSubscribers = Math.max(0, util.num(count));
    const channel = getChannel(channelId);
    state.channels.set(channelId, {
      id:channelId,
      name:channel.name,
      avatar:channel.avatar,
      subscribers:state.channelSubscribers
    });

    dom.watchChannelSub.textContent = util.compact(state.channelSubscribers) + " subscribers";

    if(!user){
      state.subscribed = false;
      dom.subscribeBtn.disabled = false;
      dom.subscribeBtn.classList.remove("active");
      dom.subscribeBtn.textContent = "Subscribe";
      return;
    }

    if(util.safe(user.id) === channelId){
      dom.subscribeBtn.disabled = true;
      dom.subscribeBtn.classList.add("active");
      dom.subscribeBtn.textContent = "Your Channel";
      state.subscribed = false;
      return;
    }

    const { data } = await state.supa
      .from("channel_subscribers")
      .select("subscriber_user_id")
      .eq("channel_id", channelId)
      .eq("subscriber_user_id", user.id)
      .limit(1);

    state.subscribed = Array.isArray(data) && data.length > 0;
    dom.subscribeBtn.disabled = false;
    dom.subscribeBtn.classList.toggle("active", state.subscribed);
    dom.subscribeBtn.textContent = state.subscribed ? "Subscribed" : "Subscribe";
  }

  async function recordView(){
    const video = currentVideo();
    if(!video) return;
    const user = await api.ensureAuth();
    if(!user) return;

    const { data, error } = await state.supa.rpc("video_record_view_rpc", {
      p_video_id: video.id
    });
    if(error){
      console.error("record_view_failed", error);
      return;
    }

    video.views = Math.max(0, util.num(data?.views));
    state.videos.set(video.id, video);
    dom.watchMeta.textContent = util.compact(video.views) + " views . " + util.ago(video.created_at);
    updateFeedCard(video.id);
  }

  async function toggleReaction(type){
    const video = currentVideo();
    if(!video || state.reactionBusy) return;

    const user = await api.requireAuth();
    if(!user) return;

    state.reactionBusy = true;
    try{
      const { data, error } = await state.supa.rpc("video_toggle_reaction_rpc", {
        p_video_id: video.id,
        p_type: type
      });
      if(error) throw error;

      state.activeReaction = util.safe(data?.reaction) || null;
      video.likes_count = Math.max(0, util.num(data?.likes_count));
      video.dislikes_count = Math.max(0, util.num(data?.dislikes_count));
      state.videos.set(video.id, video);

      dom.likeCount.textContent = util.compact(video.likes_count);
      dom.dislikeCount.textContent = util.compact(video.dislikes_count);
      setReactionButtons();
      updateFeedCard(video.id);
    }catch(err){
      console.error("toggle_reaction_failed", err);
      showToast("Unable to update reaction", true);
    }finally{
      state.reactionBusy = false;
    }
  }

  async function toggleSubscribe(){
    const video = currentVideo();
    if(!video || state.subscribeBusy) return;

    const user = await api.requireAuth();
    if(!user) return;
    if(util.safe(user.id) === util.safe(video.user_id)) return;

    state.subscribeBusy = true;
    try{
      const { data, error } = await state.supa.rpc("video_toggle_subscribe_rpc", {
        p_channel_id: video.user_id
      });
      if(error) throw error;

      state.subscribed = !!data?.subscribed;
      state.channelSubscribers = Math.max(0, util.num(data?.subscribers_count));

      const channel = getChannel(video.user_id);
      state.channels.set(video.user_id, {
        id:video.user_id,
        name:channel.name,
        avatar:channel.avatar,
        subscribers:state.channelSubscribers
      });

      dom.subscribeBtn.classList.toggle("active", state.subscribed);
      dom.subscribeBtn.textContent = state.subscribed ? "Subscribed" : "Subscribe";
      dom.watchChannelSub.textContent = util.compact(state.channelSubscribers) + " subscribers";
    }catch(err){
      console.error("toggle_subscribe_failed", err);
      showToast("Unable to update subscription", true);
    }finally{
      state.subscribeBusy = false;
    }
  }

  async function loadComments(token){
    if(token !== state.watchToken) return;
    const video = currentVideo();
    if(!video) return;

    const { data, error } = await state.supa
      .from("video_comments")
      .select("id,video_id,user_id,parent_comment_id,comment_text,likes,created_at")
      .eq("video_id", video.id)
      .order("created_at", { ascending:true })
      .limit(500);

    if(error){
      console.error("comments_load_failed", error);
      dom.commentsList.innerHTML = "";
      dom.commentsEmpty.classList.remove("mv-hidden");
      dom.commentsEmpty.textContent = "Unable to load comments.";
      dom.commentCount.textContent = "0";
      return;
    }

    const rows = Array.isArray(data) ? data : [];
    dom.commentCount.textContent = String(rows.length);
    if(!rows.length){
      dom.commentsList.innerHTML = "";
      dom.commentsEmpty.classList.remove("mv-hidden");
      dom.commentsEmpty.textContent = "No comments yet.";
      return;
    }

    dom.commentsEmpty.classList.add("mv-hidden");
    await api.fetchProfiles(rows.map(row => row.user_id));

    const commentIds = rows.map(row => row.id).filter(Boolean);
    const liked = new Set();
    if(state.me && commentIds.length){
      const { data:likedRows } = await state.supa
        .from("video_comment_likes")
        .select("comment_id")
        .eq("user_id", state.me.id)
        .in("comment_id", commentIds);
      (likedRows || []).forEach(row => liked.add(util.safe(row.comment_id)));
    }

    const byParent = new Map();
    rows.forEach(row => {
      const key = util.safe(row.parent_comment_id) || "__root__";
      if(!byParent.has(key)) byParent.set(key, []);
      byParent.get(key).push(row);
    });

    const renderBranch = (parentId) => {
      const frag = document.createDocumentFragment();
      const list = byParent.get(parentId) || [];
      list.forEach(row => {
        const rowId = util.safe(row.id);
        const channel = getChannel(row.user_id);
        const node = document.createElement("div");
        node.className = "mv-comment";
        node.innerHTML =
          '<div class="mv-comment-row">' +
            '<div class="mv-avatar">' + avatarHtml(channel) + '</div>' +
            '<div class="mv-comment-body">' +
              '<div class="mv-comment-meta"><strong>' + util.esc(channel.name) + '</strong><span>' + util.esc(util.ago(row.created_at)) + '</span></div>' +
              '<p class="mv-comment-text">' + util.esc(row.comment_text) + '</p>' +
              '<div class="mv-comment-actions">' +
                '<button type="button" data-action="like" class="' + (liked.has(rowId) ? "active" : "") + '">Like (' + util.compact(row.likes) + ')</button>' +
                '<button type="button" data-action="reply">Reply</button>' +
                (state.me && util.safe(state.me.id) === util.safe(row.user_id) ? '<button type="button" data-action="delete">Delete</button>' : '') +
              '</div>' +
            '</div>' +
          '</div>';

        node.querySelectorAll("[data-action]").forEach(btn => {
          btn.addEventListener("click", async () => {
            const action = btn.getAttribute("data-action");
            if(action === "reply"){
              state.replyParentId = rowId;
              dom.replying.hidden = false;
              dom.replyingText.textContent = "Replying to " + channel.name;
              dom.commentInput.focus();
              return;
            }
            if(action === "delete") return void deleteComment(rowId);
            if(action === "like") return void toggleCommentLike(rowId);
          });
        });

        const child = renderBranch(rowId);
        if(child.childNodes.length){
          const childWrap = document.createElement("div");
          childWrap.className = "mv-comment-children";
          childWrap.appendChild(child);
          node.appendChild(childWrap);
        }
        frag.appendChild(node);
      });
      return frag;
    };

    dom.commentsList.innerHTML = "";
    dom.commentsList.appendChild(renderBranch("__root__"));
  }

  async function submitComment(event){
    event.preventDefault();
    const video = currentVideo();
    if(!video || state.commentBusy) return;

    const text = util.safe(dom.commentInput.value);
    if(!text) return;

    const user = await api.requireAuth();
    if(!user) return;

    state.commentBusy = true;
    try{
      const payload = {
        video_id:video.id,
        user_id:user.id,
        comment_text:text
      };
      if(state.replyParentId) payload.parent_comment_id = state.replyParentId;

      const { error } = await state.supa.from("video_comments").insert(payload);
      if(error) throw error;

      dom.commentInput.value = "";
      resetReplyTarget();
      await loadComments(state.watchToken);
    }catch(err){
      console.error("submit_comment_failed", err);
      showToast("Unable to add comment", true);
    }finally{
      state.commentBusy = false;
    }
  }

  async function deleteComment(commentId){
    const id = util.safe(commentId);
    if(!id) return;

    const user = await api.requireAuth();
    if(!user) return;

    const { error } = await state.supa
      .from("video_comments")
      .delete()
      .eq("id", id)
      .eq("user_id", user.id);

    if(error){
      console.error("delete_comment_failed", error);
      showToast("Unable to delete comment", true);
      return;
    }
    await loadComments(state.watchToken);
  }

  async function toggleCommentLike(commentId){
    const id = util.safe(commentId);
    if(!id) return;

    const user = await api.requireAuth();
    if(!user) return;

    const { error } = await state.supa.rpc("video_toggle_comment_like_rpc", {
      p_comment_id: id
    });
    if(error){
      console.error("comment_like_failed", error);
      showToast("Unable to update comment like", true);
      return;
    }

    await loadComments(state.watchToken);
  }

  async function loadRecommendations(token){
    if(token !== state.watchToken) return;
    const video = currentVideo();
    if(!video) return;

    const seen = new Set([video.id]);
    const rows = [];
    const addRows = (list) => {
      (list || []).forEach(row => {
        if(!row || !row.id || seen.has(row.id)) return;
        seen.add(row.id);
        rows.push(row);
      });
    };

    try{
      if(video.category) addRows(await api.fetchVideos({ category:video.category, excludeId:video.id, limit:16 }));
      const tags = util.tags(video.tags);
      if(tags.length) addRows(await api.fetchVideos({ search:tags[0], excludeId:video.id, limit:16 }));
      addRows(await api.fetchVideos({ excludeId:video.id, limit:18 }));

      rows.sort((a, b) => Math.max(0, util.num(b.views)) - Math.max(0, util.num(a.views)));
      const top = rows.slice(0, 10);
      await api.fetchProfiles(top.map(item => item.user_id));

      dom.recommendList.innerHTML = "";
      if(!top.length){
        dom.recommendList.innerHTML = '<div class="mv-empty">No recommendations available.</div>';
        return;
      }

      top.forEach(item => {
        const channel = getChannel(item.user_id);
        const button = document.createElement("button");
        button.type = "button";
        button.className = "mv-rec-item";
        button.innerHTML =
          '<div class="mv-rec-thumb"><img src="' + util.esc(item.thumbnail_url || "Images/no-image.jpg") + '" alt="' + util.esc(item.title) + '"></div>' +
          '<div class="mv-rec-body"><p class="mv-rec-title">' + util.esc(item.title) + '</p><p class="mv-rec-sub">' + util.esc(channel.name + " . " + util.compact(item.views) + " views") + '</p></div>';
        button.addEventListener("click", () => openVideo(item.id, true, true));
        dom.recommendList.appendChild(button);
      });
    }catch(err){
      console.error("recommend_failed", err);
      dom.recommendList.innerHTML = '<div class="mv-empty">Unable to load recommendations.</div>';
    }
  }

  async function loadDashboard(){
    const user = await api.requireAuth();
    if(!user) return;

    const { data, error } = await state.supa.rpc("video_creator_dashboard_rpc", {
      p_owner_id: user.id
    });
    if(error){
      console.error("dashboard_failed", error);
      showToast("Unable to load dashboard", true);
      return;
    }

    dom.dashViews.textContent = util.compact(data?.total_views);
    dom.dashRevenue.textContent = util.money(data?.total_revenue);
    dom.dashRpm.textContent = util.money(data?.rpm);
    dom.dashMonthly.textContent = util.money(data?.estimated_monthly_earnings);
  }

  function resetUpload(){
    state.uploadVideoFile = null;
    state.uploadThumbFile = null;
    dom.uploadForm.reset();
    dom.videoFileName.textContent = "Supported: MP4, WEBM, MOV, MKV (max 600MB)";
    dom.thumbFileName.textContent = "Supported: JPG, PNG, WEBP (max 8MB)";
    dom.thumbPreviewWrap.classList.add("mv-hidden");
    dom.thumbPreview.removeAttribute("src");
    setUploadProgress(0, "Ready to upload.");
  }

  function validateVideo(file){
    if(!file) return "Video file is required.";
    if(file.size > MAX_VIDEO_SIZE) return "Video file exceeds 600MB.";
    if(file.type && !VIDEO_MIME_TYPES.has(file.type)) return "Unsupported video format.";
    return "";
  }

  function validateThumb(file){
    if(!file) return "";
    if(file.size > MAX_THUMB_SIZE) return "Thumbnail exceeds 8MB.";
    if(file.type && !THUMB_MIME_TYPES.has(file.type)) return "Unsupported thumbnail format.";
    return "";
  }

  function pickVideo(file){
    const error = validateVideo(file);
    if(error) return void showToast(error, true);
    state.uploadVideoFile = file;
    dom.videoFileName.textContent = file.name + " (" + util.compact(Math.round(file.size / 1024 / 1024)) + " MB)";
  }

  function pickThumb(file){
    const error = validateThumb(file);
    if(error) return void showToast(error, true);

    state.uploadThumbFile = file || null;
    if(!file){
      dom.thumbFileName.textContent = "Supported: JPG, PNG, WEBP (max 8MB)";
      dom.thumbPreviewWrap.classList.add("mv-hidden");
      dom.thumbPreview.removeAttribute("src");
      return;
    }

    dom.thumbFileName.textContent = file.name;
    const reader = new FileReader();
    reader.onload = () => {
      dom.thumbPreview.src = String(reader.result || "");
      dom.thumbPreviewWrap.classList.remove("mv-hidden");
    };
    reader.readAsDataURL(file);
  }

  function wireDrop(zone, onPick){
    ["dragenter", "dragover"].forEach(name => zone.addEventListener(name, (event) => {
      event.preventDefault();
      zone.classList.add("is-drag");
    }));
    ["dragleave", "drop"].forEach(name => zone.addEventListener(name, (event) => {
      event.preventDefault();
      zone.classList.remove("is-drag");
    }));
    zone.addEventListener("drop", (event) => {
      const file = event.dataTransfer && event.dataTransfer.files ? event.dataTransfer.files[0] : null;
      if(file) onPick(file);
    });
  }

  async function uploadVideo(event){
    event.preventDefault();

    const user = await api.requireAuth();
    if(!user) return;

    const title = util.safe(dom.uploadTitle.value);
    const description = util.safe(dom.uploadDescription.value);
    const tags = util.tags(dom.uploadTags.value).join(", ");
    const category = util.safe(dom.uploadCategory.value) || "General";
    const monetized = !!dom.uploadMonetized.checked;

    if(!title) return void showToast("Title is required", true);

    const videoError = validateVideo(state.uploadVideoFile);
    if(videoError) return void showToast(videoError, true);

    const thumbError = validateThumb(state.uploadThumbFile);
    if(thumbError) return void showToast(thumbError, true);

    dom.uploadSubmit.disabled = true;
    try{
      setUploadProgress(8, "Reading video metadata...");
      const meta = await window.NOVA.getVideoMeta(state.uploadVideoFile);

      setUploadProgress(30, "Uploading video...");
      const videoPath = window.NOVA.makePath(user.id, state.uploadVideoFile);
      const videoUrl = await window.NOVA.uploadToBucket("long_videos", state.uploadVideoFile, videoPath);

      let thumbUrl = "";
      if(state.uploadThumbFile){
        setUploadProgress(58, "Uploading thumbnail...");
        const thumbPath = window.NOVA.makePath(user.id, state.uploadThumbFile);
        thumbUrl = await window.NOVA.uploadToBucket("thumbnails", state.uploadThumbFile, thumbPath);
      }

      setUploadProgress(82, "Saving video record...");
      const payload = {
        user_id:user.id,
        title,
        description,
        video_url:videoUrl,
        thumbnail_url:thumbUrl || null,
        category,
        tags,
        monetized,
        duration_seconds:Math.max(0, Math.round(util.num(meta?.duration)))
      };

      const { data, error } = await state.supa.from("videos").insert(payload).select(VIDEO_SELECT).maybeSingle();
      if(error) throw error;

      setUploadProgress(100, "Upload complete.");
      showToast("Video uploaded successfully");
      if(data) upsertVideos([data]);
      resetUpload();
      state.searchTerm = "";
      dom.searchInput.value = "";
      await loadFeed(true);
      setPanel("feed", true);
    }catch(err){
      console.error("upload_failed", err);
      setUploadProgress(0, "Upload failed.");
      showToast("Upload failed. Check SQL + storage policies.", true);
    }finally{
      dom.uploadSubmit.disabled = false;
    }
  }

  async function refreshVideo(videoId){
    const id = util.safe(videoId);
    if(!id) return;
    const fresh = await api.fetchVideoById(id);
    if(!fresh) return;
    state.videos.set(id, fresh);
    updateFeedCard(id);
    if(state.activeVideoId === id){
      dom.watchMeta.textContent = util.compact(fresh.views) + " views . " + util.ago(fresh.created_at);
      dom.likeCount.textContent = util.compact(fresh.likes_count);
      dom.dislikeCount.textContent = util.compact(fresh.dislikes_count);
    }
  }

  function setPanel(panel, pushState){
    const next = panel in dom.panels ? panel : "feed";
    state.panel = next;
    Object.keys(dom.panels).forEach(key => dom.panels[key].classList.toggle("active", key === next));
    dom.sideButtons.forEach(btn => btn.classList.toggle("active", btn.getAttribute("data-panel") === next));

    if(pushState){
      const url = new URL(location.href);
      url.searchParams.delete("v");
      if(next === "feed") url.searchParams.delete("view");
      else url.searchParams.set("view", next);
      history.pushState({ mv:next }, "", url.pathname + url.search);
    }

    if(next === "dashboard") loadDashboard();
  }

  function setupRealtime(){
    if(state.realtimeChannel){
      try{ state.supa.removeChannel(state.realtimeChannel); }catch(_){ }
    }

    const channel = state.supa.channel("mvideos-" + Date.now());

    channel.on("postgres_changes", { event:"*", schema:"public", table:"videos" }, (payload) => {
      const row = payload.new || payload.old;
      const id = util.safe(row?.id);
      if(!id) return;

      if(payload.eventType === "DELETE"){
        state.videos.delete(id);
        const card = state.cards.get(id);
        if(card && card.parentNode) card.parentNode.removeChild(card);
        state.cards.delete(id);
      }else if(payload.new){
        state.videos.set(id, normalizeVideo(payload.new));
        updateFeedCard(id);
      }

      if(state.activeVideoId === id){
        schedule("active_video", () => refreshVideo(id), 120);
      }
    });

    channel.on("postgres_changes", { event:"*", schema:"public", table:"video_likes" }, (payload) => {
      const id = util.safe(payload.new?.video_id || payload.old?.video_id);
      if(!id) return;
      schedule("likes_" + id, () => refreshVideo(id), 120);
      if(id === state.activeVideoId) schedule("reaction_state", () => loadReactionState(state.watchToken), 180);
    });

    channel.on("postgres_changes", { event:"*", schema:"public", table:"video_comments" }, (payload) => {
      const id = util.safe(payload.new?.video_id || payload.old?.video_id);
      if(!id || id !== state.activeVideoId) return;
      schedule("comments", () => loadComments(state.watchToken), 140);
    });

    channel.on("postgres_changes", { event:"*", schema:"public", table:"channel_subscribers" }, (payload) => {
      const channelId = util.safe(payload.new?.channel_id || payload.old?.channel_id);
      if(!channelId) return;
      schedule("subs_" + channelId, async () => {
        const { count } = await state.supa
          .from("channel_subscribers")
          .select("subscriber_user_id", { count:"exact", head:true })
          .eq("channel_id", channelId);

        const channelRow = getChannel(channelId);
        state.channels.set(channelId, {
          id:channelId,
          name:channelRow.name,
          avatar:channelRow.avatar,
          subscribers:Math.max(0, util.num(count))
        });

        if(currentVideo() && util.safe(currentVideo().user_id) === channelId){
          dom.watchChannelSub.textContent = util.compact(Math.max(0, util.num(count))) + " subscribers";
        }
      }, 180);
    });

    channel.on("postgres_changes", { event:"*", schema:"public", table:"video_earnings" }, (payload) => {
      const userId = util.safe(payload.new?.user_id || payload.old?.user_id);
      if(!state.me || userId !== util.safe(state.me.id)) return;
      schedule("dashboard", () => {
        if(state.panel === "dashboard") loadDashboard();
      }, 220);
    });

    channel.subscribe();
    state.realtimeChannel = channel;
  }

  async function runSearch(term){
    const value = util.safe(term);
    state.searchTerm = value;
    if(value) store.pushUnique(SEARCH_HISTORY_KEY, value, 12);
    await loadFeed(true);
  }

  async function renderSuggestions(term){
    try{
      const items = await api.fetchSuggestions(term);
      if(!items.length){
        dom.suggestBox.classList.add("mv-hidden");
        dom.suggestBox.innerHTML = "";
        return;
      }

      dom.suggestBox.innerHTML = "";
      items.forEach(item => {
        const button = document.createElement("button");
        button.type = "button";
        button.className = "mv-suggest-item";

        if(item.kind === "history"){
          button.innerHTML =
            '<div class="mv-suggest-thumb"><img src="Images/no-image.jpg" alt="Recent search"></div>' +
            '<div class="mv-suggest-meta"><p class="mv-suggest-title">' + util.esc(item.term) + '</p><p class="mv-suggest-sub">Recent search</p></div>';
          button.addEventListener("click", async () => {
            dom.searchInput.value = item.term;
            dom.suggestBox.classList.add("mv-hidden");
            await runSearch(item.term);
          });
        }else{
          const video = item.video;
          const channel = getChannel(video.user_id);
          button.innerHTML =
            '<div class="mv-suggest-thumb"><img src="' + util.esc(video.thumbnail_url || "Images/no-image.jpg") + '" alt="' + util.esc(video.title) + '"></div>' +
            '<div class="mv-suggest-meta"><p class="mv-suggest-title">' + util.esc(video.title) + '</p><p class="mv-suggest-sub">' + util.esc(channel.name) + '</p></div>';
          button.addEventListener("click", () => {
            dom.suggestBox.classList.add("mv-hidden");
            openVideo(video.id, true, true);
          });
        }

        dom.suggestBox.appendChild(button);
      });
      dom.suggestBox.classList.remove("mv-hidden");
    }catch(err){
      console.error("suggestions_failed", err);
      dom.suggestBox.classList.add("mv-hidden");
    }
  }

  async function applyRoute(){
    const params = new URLSearchParams(location.search);
    const videoId = util.safe(params.get("v"));
    const view = util.safe(params.get("view"));

    if(videoId){
      await openVideo(videoId, false, false);
      return;
    }
    if(view === "upload"){
      setPanel("upload", false);
      return;
    }
    if(view === "dashboard"){
      setPanel("dashboard", false);
      await loadDashboard();
      return;
    }
    setPanel("feed", false);
  }

  function bindEvents(){
    dom.openUpload.addEventListener("click", () => setPanel("upload", true));
    dom.openDashboard.addEventListener("click", () => setPanel("dashboard", true));
    dom.sideButtons.forEach(btn => btn.addEventListener("click", () => setPanel(btn.getAttribute("data-panel"), true)));

    dom.watchBack.addEventListener("click", () => {
      setPanel("feed", true);
      try{ dom.player.pause(); }catch(_){ }
    });

    dom.likeBtn.addEventListener("click", () => toggleReaction("like"));
    dom.dislikeBtn.addEventListener("click", () => toggleReaction("dislike"));
    dom.subscribeBtn.addEventListener("click", toggleSubscribe);
    dom.shareBtn.addEventListener("click", () => {
      const video = currentVideo();
      if(video) shareVideo(video.id);
    });
    dom.saveBtn.addEventListener("click", () => {
      const video = currentVideo();
      if(!video) return;
      store.pushUnique(WATCH_LATER_KEY, video.id, 100);
      setSaveButton();
      showToast("Saved to Watch Later");
    });

    dom.descToggle.addEventListener("click", () => {
      const clamped = dom.descText.classList.contains("clamp");
      if(clamped){
        dom.descText.classList.remove("clamp");
        dom.descToggle.textContent = "Show less";
      }else{
        dom.descText.classList.add("clamp");
        dom.descToggle.textContent = "Show more";
      }
    });

    dom.commentForm.addEventListener("submit", submitComment);
    dom.replyCancel.addEventListener("click", resetReplyTarget);

    dom.searchForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      dom.suggestBox.classList.add("mv-hidden");
      await runSearch(dom.searchInput.value);
    });

    dom.searchClear.addEventListener("click", async () => {
      dom.searchInput.value = "";
      dom.suggestBox.classList.add("mv-hidden");
      state.searchTerm = "";
      await runSearch("");
    });

    dom.searchInput.addEventListener("focus", () => {
      clearTimeout(state.suggestDebounce);
      state.suggestDebounce = setTimeout(() => renderSuggestions(dom.searchInput.value), 80);
    });

    dom.searchInput.addEventListener("input", () => {
      clearTimeout(state.suggestDebounce);
      clearTimeout(state.searchDebounce);
      state.suggestDebounce = setTimeout(() => renderSuggestions(dom.searchInput.value), 180);
      state.searchDebounce = setTimeout(() => runSearch(dom.searchInput.value), 300);
    });

    document.addEventListener("click", (event) => {
      if(!event.target.closest(".mv-search-wrap")) dom.suggestBox.classList.add("mv-hidden");
      if(!event.target.closest(".mv-menu-wrap")) closeMenus();
    });

    wireDrop(dom.videoDrop, pickVideo);
    wireDrop(dom.thumbDrop, pickThumb);

    dom.videoDrop.addEventListener("click", () => dom.videoFileInput.click());
    dom.thumbDrop.addEventListener("click", () => dom.thumbFileInput.click());

    dom.videoFileInput.addEventListener("change", (event) => {
      const file = event.target.files && event.target.files[0];
      if(file) pickVideo(file);
      event.target.value = "";
    });

    dom.thumbFileInput.addEventListener("change", (event) => {
      const file = event.target.files && event.target.files[0];
      if(file) pickThumb(file);
      event.target.value = "";
    });

    dom.uploadForm.addEventListener("submit", uploadVideo);
    dom.dashRefresh.addEventListener("click", loadDashboard);

    dom.footerButtons.forEach(btn => {
      btn.addEventListener("click", () => {
        const target = util.safe(btn.getAttribute("data-go"));
        if(target) location.href = target;
      });
    });

    window.addEventListener("popstate", async (event) => {
      const info = event.state || {};
      if(info.mv === "watch" && info.videoId){
        await openVideo(info.videoId, false, false);
        return;
      }
      if(info.mv === "upload"){
        setPanel("upload", false);
        return;
      }
      if(info.mv === "dashboard"){
        setPanel("dashboard", false);
        await loadDashboard();
        return;
      }
      setPanel("feed", false);
    });
  }

  async function init(){
    bindEvents();
    resetUpload();

    state.supa = await api.waitForSupabase(10000);
    if(!state.supa){
      setFeedEmpty("Supabase init failed. Start backend config and reload.");
      return;
    }

    await api.ensureAuth();
    setupRealtime();

    state.observer = new IntersectionObserver((entries) => {
      if(entries[0] && entries[0].isIntersecting) loadFeed(false);
    }, { rootMargin:"320px" });
    state.observer.observe(dom.sentinel);

    await loadFeed(true);
    await applyRoute();
  }

  init();
})();

