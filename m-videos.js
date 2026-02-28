(function(){
  "use strict";

  const PAGE_SIZE = 24;
  const SEARCH_HISTORY_KEY = "mv_search_history_v1";
  const WATCH_LATER_KEY = "mv_watch_later_v1";
  const QUEUE_KEY = "mv_queue_v1";
  const MAX_VIDEO_SIZE = 4 * 1024 * 1024 * 1024;
  const MAX_THUMB_SIZE = 8 * 1024 * 1024;
  const UPLOAD_TIMEOUT_MS = 30 * 60 * 1000;
  const DIRECT_TO_SERVER_UPLOAD_THRESHOLD = 8 * 1024 * 1024;
  const SERVER_VIDEO_UPLOAD_TIMEOUT_MS = 45 * 60 * 1000;
  const VIDEO_SELECT = "id,user_id,title,description,video_url,thumbnail_url,views,likes_count,dislikes_count,monetized,created_at,category,tags,duration_seconds";
  const VIDEO_MIME_TYPES = new Set(["video/mp4","video/webm","video/quicktime","video/x-matroska","video/ogg","video/mpeg"]);
  const THUMB_MIME_TYPES = new Set(["image/jpeg","image/png","image/webp"]);
  const DEFAULT_REMOTE_API_BASE = "https://novagapp-mart.onrender.com";
  const MONETIZATION_MIN_FOLLOWERS = 5000;
  const MONETIZATION_UNLOCK_USD = 10;
  const SEARCH_SCAN_LIMIT = 700;

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
    channelMembers:0,
    channelMembershipFeeInr:0,
    memberJoined:false,
    shareCount:0,
    commentsOpen:false,
    descriptionOpen:false,
    reactionBusy:false,
    subscribeBusy:false,
    memberBusy:false,
    commentBusy:false,
    replyParentId:"",
    watchToken:0,
    realtimeChannel:null,
    observer:null,
    timers:{},
    uploadVideoFile:null,
    uploadThumbFile:null,
    monetization:{
      followers:0,
      followersReady:false,
      unlocked:false,
      paymentReady:false,
      keyId:"",
      statusMessage:"",
      loading:false,
      unlockBusy:false
    },
    feature:{
      profileLookupEnabled:true,
      channelSubscribers:false,
      videoLikes:false,
      videoComments:false,
      videoCommentLikes:false,
      channelMembers:false,
      channelMembershipPlans:true,
      rpcRecordView:false,
      rpcToggleReaction:false,
      rpcToggleSubscribe:false,
      rpcToggleCommentLike:false
    }
  };

  const dom = {
    panels:{
      feed:document.getElementById("mvFeedView"),
      watch:document.getElementById("mvWatchView"),
      upload:document.getElementById("mvUploadView"),
      dashboard:document.getElementById("mvDashboardView")
    },
    sideButtons:Array.from(document.querySelectorAll("[data-panel]")),
    openChannelSide:document.getElementById("mvOpenChannelSide"),
    openChannelTop:document.getElementById("mvOpenChannel"),
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
    playerPoster:document.getElementById("mvPlayerPoster"),
    playerLoading:document.getElementById("mvPlayerLoading"),
    playerOverlay:document.getElementById("mvPlayerOverlay"),
    playerPrev:document.getElementById("mvPlayerPrev"),
    playerCenterToggle:document.getElementById("mvPlayerCenterToggle"),
    playerCenterIcon:document.getElementById("mvPlayerCenterIcon"),
    playerNext:document.getElementById("mvPlayerNext"),
    seekBack:document.getElementById("mvSeekBack"),
    seekForward:document.getElementById("mvSeekForward"),
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
    shareCount:document.getElementById("mvShareCount"),
    commentBtn:document.getElementById("mvCommentBtn"),
    commentActionCount:document.getElementById("mvCommentActionCount"),
    memberBtn:document.getElementById("mvMemberBtn"),
    memberCount:document.getElementById("mvMemberCount"),
    saveBtn:document.getElementById("mvSaveBtn"),
    descTab:document.getElementById("mvDescTab"),
    descPanel:document.getElementById("mvDescPanel"),
    descText:document.getElementById("mvDescText"),
    commentsTab:document.getElementById("mvCommentsTab"),
    commentsPanel:document.getElementById("mvCommentsPanel"),
    commentsClose:document.getElementById("mvCommentsClose"),
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
    monetizeHint:document.getElementById("mvMonetizeHint"),
    monetizeUnlockBtn:document.getElementById("mvMonetizeUnlockBtn"),
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

  function matchesVideoSearch(video, term){
    const query = util.clean(term);
    if(!query) return true;
    const hay = [
      util.safe(video?.title),
      util.safe(video?.description),
      util.safe(video?.category),
      util.safe(video?.tags)
    ].join(" ").toLowerCase();
    return hay.includes(query);
  }
  let razorpaySdkPromise = null;

  function shuffleRows(list){
    const arr = Array.isArray(list) ? list.slice() : [];
    for(let i = arr.length - 1; i > 0; i -= 1){
      const j = Math.floor(Math.random() * (i + 1));
      const tmp = arr[i];
      arr[i] = arr[j];
      arr[j] = tmp;
    }
    return arr;
  }

  function setDescriptionOpen(open){
    const next = !!open;
    state.descriptionOpen = next;
    if(dom.descPanel) dom.descPanel.classList.toggle("mv-hidden", !next);
    if(dom.descTab){
      dom.descTab.classList.toggle("active", next);
      dom.descTab.setAttribute("aria-expanded", next ? "true" : "false");
    }
  }

  function setCommentsOpen(open, focusInput){
    const next = !!open;
    state.commentsOpen = next;
    if(dom.commentsPanel) dom.commentsPanel.classList.toggle("mv-hidden", !next);
    if(dom.commentsTab){
      dom.commentsTab.classList.toggle("active", next);
      dom.commentsTab.setAttribute("aria-expanded", next ? "true" : "false");
    }
    if(next && focusInput && dom.commentInput && typeof dom.commentInput.focus === "function"){
      dom.commentInput.focus();
    }
  }

  function setPlayerLoading(showLoading, keepPoster){
    const loading = !!showLoading;
    const posterVisible = !!keepPoster;
    if(dom.playerLoading) dom.playerLoading.classList.toggle("mv-hidden", !loading);
    if(dom.playerPoster) dom.playerPoster.classList.toggle("mv-hidden", !posterVisible);
  }

  function getWatchSequenceIds(){
    const ids = [];
    const seen = new Set();
    const push = (value) => {
      const id = util.safe(value);
      if(!id || seen.has(id)) return;
      seen.add(id);
      ids.push(id);
    };

    if(dom.grid){
      dom.grid.querySelectorAll(".mv-card[data-video-id]").forEach(card => {
        push(card.getAttribute("data-video-id"));
      });
    }
    store.read(QUEUE_KEY, []).forEach(push);
    Array.from(state.videos.values())
      .sort((a, b) => (Date.parse(util.safe(b?.created_at)) || 0) - (Date.parse(util.safe(a?.created_at)) || 0))
      .forEach(item => push(item?.id));
    push(state.activeVideoId);
    return ids;
  }

  function getAdjacentVideoId(direction){
    const step = direction < 0 ? -1 : 1;
    const order = getWatchSequenceIds();
    if(order.length < 2) return "";
    const activeId = util.safe(state.activeVideoId);
    const currentIndex = Math.max(0, order.indexOf(activeId));
    const nextIndex = (currentIndex + step + order.length) % order.length;
    if(nextIndex === currentIndex) return "";
    return util.safe(order[nextIndex]);
  }

  function setPlayerOverlayVisible(show){
    if(!dom.playerOverlay) return;
    const visible = !!show;
    dom.playerOverlay.classList.toggle("show", visible);
    dom.playerOverlay.setAttribute("aria-hidden", visible ? "false" : "true");
  }

  function refreshPlayerOverlayState(){
    const paused = !!dom.player && (!!dom.player.paused || !!dom.player.ended);
    if(dom.playerCenterIcon){
      dom.playerCenterIcon.textContent = paused ? "\u25B6" : "\u275A\u275A";
    }
    if(dom.playerCenterToggle){
      dom.playerCenterToggle.setAttribute("aria-label", paused ? "Play video" : "Pause video");
    }
    if(dom.playerPrev){
      dom.playerPrev.disabled = !getAdjacentVideoId(-1);
    }
    if(dom.playerNext){
      dom.playerNext.disabled = !getAdjacentVideoId(1);
    }
    setPlayerOverlayVisible(paused && state.panel === "watch");
  }

  async function openAdjacentVideo(direction){
    const targetId = getAdjacentVideoId(direction);
    if(!targetId){
      showToast(direction < 0 ? "No previous video found." : "No next video found.", true);
      return;
    }
    await openVideo(targetId, true, true);
  }

  function errorCode(err){
    return util.safe(err?.code).toUpperCase();
  }

  function errorStatus(err){
    const direct = Number(err?.status);
    if(Number.isFinite(direct) && direct > 0) return direct;
    const nested = Number(err?.response?.status);
    if(Number.isFinite(nested) && nested > 0) return nested;
    return 0;
  }

  function isMissingRelationError(err, relationName){
    const code = errorCode(err);
    const status = errorStatus(err);
    const message = util.safe(err?.message).toLowerCase();
    const details = util.safe(err?.details).toLowerCase();
    if(status === 404) return true;
    if(code === "PGRST205" || code === "42P01") return true;
    if(message.includes("could not find the table")) return true;
    if(message.includes("relation") && message.includes("does not exist")) return true;
    if(message === "not found" || details.includes("not found")) return true;
    if(relationName){
      const relation = String(relationName).toLowerCase();
      if(message.includes(`public.${relation}`) || details.includes(`public.${relation}`)){
        return true;
      }
      if(message.includes(relation) && (message.includes("not found") || message.includes("does not exist"))){
        return true;
      }
    }
    return false;
  }

  function isNoRowsError(err){
    const code = errorCode(err);
    const status = errorStatus(err);
    const message = util.safe(err?.message).toLowerCase();
    if(code === "PGRST116") return true;
    if(status === 406 && (message.includes("results contain 0 rows") || message.includes("json object requested"))){
      return true;
    }
    return false;
  }

  function isMissingFunctionError(err, fnName){
    const code = errorCode(err);
    const status = errorStatus(err);
    const message = util.safe(err?.message).toLowerCase();
    const details = util.safe(err?.details).toLowerCase();
    if(status === 404) return true;
    if(code === "PGRST202" || code === "42883") return true;
    if(message.includes("could not find the function")) return true;
    if(message === "not found" || details.includes("not found")) return true;
    if(fnName && message.includes(String(fnName).toLowerCase())) return true;
    return false;
  }

  function isBrokenRpcError(err){
    const code = errorCode(err);
    const message = util.safe(err?.message).toLowerCase();
    const details = util.safe(err?.details).toLowerCase();
    if(code === "42702" || code === "42804" || code === "42P13"){
      return true;
    }
    if(message.includes("ambiguous") || details.includes("ambiguous")){
      return true;
    }
    if(message.includes("structure of query does not match function result type")){
      return true;
    }
    if(details.includes("does not match expected type")){
      return true;
    }
    return false;
  }

  function isMissingColumnError(err, columnName){
    const code = errorCode(err);
    const message = util.safe(err?.message).toLowerCase();
    const details = util.safe(err?.details).toLowerCase();
    if(code === "42703" || message.includes("column") && message.includes("does not exist")){
      if(!columnName) return true;
      const key = String(columnName).toLowerCase();
      return message.includes(key) || details.includes(key);
    }
    return false;
  }

  function normalizeBase(value){
    return util.safe(value).replace(/\/+$/g, "");
  }

  function buildAutomationApiBases(){
    const bases = [];
    const push = (value) => {
      const clean = normalizeBase(value);
      if(!clean) return;
      if(!/^https?:\/\//i.test(clean)) return;
      if(!bases.includes(clean)) bases.push(clean);
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
    push(DEFAULT_REMOTE_API_BASE);
    return bases;
  }

  async function fetchApiJson(path, options){
    const endpoint = util.safe(path);
    if(!endpoint){
      throw new Error("api_path_required");
    }
    const localFirst = [""].concat(buildAutomationApiBases());
    let lastError = null;

    for(const base of localFirst){
      const url = base
        ? `${base}${endpoint.startsWith("/") ? endpoint : `/${endpoint}`}`
        : endpoint;
      let response = null;
      try{
        response = await fetch(url, options || {});
      }catch(err){
        lastError = err;
        continue;
      }

      let payload = null;
      try{
        payload = await response.json();
      }catch(_){
        payload = null;
      }

      if(response.ok){
        return payload || { ok:true };
      }

      if(response.status === 404 || response.status === 405){
        lastError = new Error(`api_not_found_${response.status}`);
        continue;
      }

      const message = util.safe(payload?.message || payload?.error) || `api_failed_${response.status}`;
      const err = new Error(message);
      err.status = response.status;
      err.payload = payload || null;
      throw err;
    }

    throw lastError || new Error("api_unreachable");
  }

  function ensureRazorpaySdk(){
    if(window.Razorpay) return Promise.resolve(true);
    if(razorpaySdkPromise) return razorpaySdkPromise;

    razorpaySdkPromise = new Promise((resolve, reject) => {
      const script = document.createElement("script");
      script.src = "https://checkout.razorpay.com/v1/checkout.js";
      script.async = true;
      script.onload = () => resolve(true);
      script.onerror = () => reject(new Error("razorpay_sdk_load_failed"));
      document.head.appendChild(script);
    });

    return razorpaySdkPromise;
  }

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

  function openChannelPage(channelId){
    const id = util.safe(channelId);
    const url = new URL("m-channel.html", location.href);
    if(id){
      url.searchParams.set("uid", id);
    }
    location.href = url.pathname + url.search;
  }

  async function openMyChannel(){
    const user = await api.requireAuth();
    if(!user || !user.id) return;
    openChannelPage(user.id);
  }

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
    if(!dom.saveBtn) return;
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

  function renderActionMetrics(){
    const video = currentVideo();
    const channelId = util.safe(video?.user_id);
    const selfChannel = !!(state.me && channelId && util.safe(state.me.id) === channelId);
    const followerCount = Math.max(0, util.num(state.channelSubscribers));
    const memberCount = Math.max(0, util.num(state.channelMembers));
    const feeInr = Math.max(0, Math.floor(util.num(state.channelMembershipFeeInr)));
    const feeText = "INR " + String(feeInr);
    const shareCount = Math.max(0, util.num(state.shareCount));
    const memberText = selfChannel
      ? ("Set Fee " + (feeInr > 0 ? feeText : "Free"))
      : (state.memberJoined ? "Member" : (feeInr > 0 ? ("Join " + feeText) : "Join Free"));

    if(dom.memberCount){
      dom.memberCount.textContent = util.compact(memberCount);
    }
    if(dom.memberBtn){
      dom.memberBtn.classList.toggle("active", !!state.memberJoined || selfChannel || feeInr > 0);
      dom.memberBtn.disabled = !!state.memberBusy;
      dom.memberBtn.title = selfChannel ? "Set membership join fee" : (feeInr > 0 ? ("Pay " + feeText + " to join") : "Join this creator");
      dom.memberBtn.innerHTML = memberText + ' <span id="mvMemberCount">' + util.esc(util.compact(memberCount)) + "</span>";
      dom.memberCount = dom.memberBtn.querySelector("#mvMemberCount");
    }

    if(dom.shareCount){
      dom.shareCount.textContent = util.compact(shareCount);
    }
    if(dom.subscribeBtn){
      dom.subscribeBtn.classList.toggle("active", !!state.subscribed || selfChannel);
      dom.subscribeBtn.disabled = !!state.subscribeBusy || selfChannel;
      dom.subscribeBtn.textContent = selfChannel ? "Your Channel" : (state.subscribed ? "Following" : "Follow");
    }
    if(dom.watchChannelSub){
      const followerText = util.compact(followerCount) + " followers";
      const memberMeta = util.compact(memberCount) + " members";
      dom.watchChannelSub.textContent = followerText + " . " + memberMeta;
    }
  }

  async function getReactionCountsCompat(videoId){
    const id = util.safe(videoId);
    if(!id) return { likes:0, dislikes:0 };

    if(state.feature.videoLikes !== false){
      const likeQuery = state.supa
        .from("video_likes")
        .select("type", { count:"exact", head:true })
        .eq("video_id", id)
        .eq("type", "like");
      const dislikeQuery = state.supa
        .from("video_likes")
        .select("type", { count:"exact", head:true })
        .eq("video_id", id)
        .eq("type", "dislike");
      const [{ count:likes, error:likeError }, { count:dislikes, error:dislikeError }] = await Promise.all([
        likeQuery,
        dislikeQuery
      ]);
      if(!likeError && !dislikeError){
        state.feature.videoLikes = true;
        return { likes:Math.max(0, util.num(likes)), dislikes:Math.max(0, util.num(dislikes)) };
      }
      if(isMissingRelationError(likeError, "video_likes") || isMissingRelationError(dislikeError, "video_likes")){
        state.feature.videoLikes = false;
      }
    }

    if(window.NOVA && typeof window.NOVA.getReactionCounts === "function"){
      try{
        const counts = await window.NOVA.getReactionCounts("video", id);
        return {
          likes:Math.max(0, util.num(counts?.likes)),
          dislikes:Math.max(0, util.num(counts?.dislikes))
        };
      }catch(_){ }
    }

    const video = state.videos.get(id);
    return {
      likes:Math.max(0, util.num(video?.likes_count)),
      dislikes:Math.max(0, util.num(video?.dislikes_count))
    };
  }

  async function getUserReactionCompat(videoId, userId){
    const vId = util.safe(videoId);
    const uId = util.safe(userId);
    if(!vId || !uId) return null;

    if(state.feature.videoLikes !== false){
      const { data, error } = await state.supa
        .from("video_likes")
        .select("type")
        .eq("video_id", vId)
        .eq("user_id", uId)
        .limit(1)
        .maybeSingle();
      if(!error){
        state.feature.videoLikes = true;
        return util.safe(data?.type) || null;
      }
      if(isMissingRelationError(error, "video_likes")){
        state.feature.videoLikes = false;
      }
    }

    try{
      const { data, error } = await state.supa
        .from("reactions")
        .select("reaction")
        .eq("target_type", "video")
        .eq("target_id", vId)
        .eq("user_id", uId)
        .limit(1)
        .maybeSingle();
      if(error) return null;
      return util.safe(data?.reaction) || null;
    }catch(_){
      return null;
    }
  }

  function setUploadProgress(percent, text){
    const pct = Math.max(0, Math.min(100, util.num(percent)));
    dom.uploadProgress.style.width = pct + "%";
    dom.uploadStatus.textContent = util.safe(text);
  }

  function isMonetizationAllowed(){
    const m = state.monetization || {};
    return !!(m.followersReady && m.unlocked);
  }

  function monetizationBlockedMessage(){
    const m = state.monetization || {};
    const followers = Math.max(0, util.num(m.followers));
    if(followers < MONETIZATION_MIN_FOLLOWERS){
      return `Monetization needs ${MONETIZATION_MIN_FOLLOWERS} followers. Current: ${followers}.`;
    }
    if(!m.unlocked){
      return `Pay one-time $${MONETIZATION_UNLOCK_USD} unlock fee first.`;
    }
    return "Monetization is not enabled for this account yet.";
  }

  function renderMonetizationUi(){
    const m = state.monetization || {};
    const followers = Math.max(0, util.num(m.followers));
    m.followersReady = followers >= MONETIZATION_MIN_FOLLOWERS;

    const canMonetize = isMonetizationAllowed();
    dom.uploadMonetized.disabled = !canMonetize;
    if(!canMonetize){
      dom.uploadMonetized.checked = false;
    }

    if(dom.monetizeHint){
      let text = util.safe(m.statusMessage);
      if(!text){
        if(!m.followersReady){
          text = `Followers: ${followers}/${MONETIZATION_MIN_FOLLOWERS}. Reach ${MONETIZATION_MIN_FOLLOWERS} to start monetization.`;
        }else if(!m.unlocked){
          text = `Followers done. Pay $${MONETIZATION_UNLOCK_USD} one-time to unlock monetization.`;
        }else{
          text = "Monetization unlocked. You can enable it for this upload.";
        }
      }
      dom.monetizeHint.textContent = text;
    }

    if(dom.monetizeUnlockBtn){
      dom.monetizeUnlockBtn.hidden = false;
      dom.monetizeUnlockBtn.disabled = !!m.unlockBusy || !!m.loading || !m.followersReady || !!m.unlocked || !m.paymentReady;
      if(m.unlockBusy){
        dom.monetizeUnlockBtn.textContent = "Processing...";
      }else if(m.unlocked){
        dom.monetizeUnlockBtn.textContent = "Unlocked";
      }else if(!m.followersReady){
        dom.monetizeUnlockBtn.textContent = `Need ${MONETIZATION_MIN_FOLLOWERS} Followers`;
      }else if(!m.paymentReady){
        dom.monetizeUnlockBtn.textContent = "Payment unavailable";
      }else{
        dom.monetizeUnlockBtn.textContent = `Pay $${MONETIZATION_UNLOCK_USD} to Unlock`;
      }
    }
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
      if(!state.feature.profileLookupEnabled) return;
      const attempts = [
        { idField:"user_id", select:"user_id,username,full_name,photo" },
        { idField:"id", select:"id,username,full_name,photo" },
        { idField:"user_id", select:"user_id,username,full_name,photo,channel_subscribers_count" },
        { idField:"id", select:"id,username,full_name,photo,channel_subscribers_count" }
      ];
      let success = false;
      for(const plan of attempts){
        const { data, error } = await state.supa.from("users").select(plan.select).in(plan.idField, ids).limit(500);
        if(error){
          if(isMissingRelationError(error, "users")){
            state.feature.profileLookupEnabled = false;
            return;
          }
          if(isMissingColumnError(error, "channel_subscribers_count")){
            continue;
          }
          continue;
        }
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
        success = true;
        break;
      }
      if(!success){
        state.feature.profileLookupEnabled = false;
      }
    },

    async searchChannelIds(term){
      const clean = util.clean(term);
      if(!clean) return [];
      if(!state.feature.profileLookupEnabled) return [];
      const attempts = [
        { idField:"user_id", select:"user_id,username,full_name", filter:`username.ilike.%${clean}%,full_name.ilike.%${clean}%` },
        { idField:"id", select:"id,username,full_name", filter:`username.ilike.%${clean}%,full_name.ilike.%${clean}%` },
        { idField:"user_id", select:"user_id,username", filter:`username.ilike.%${clean}%` },
        { idField:"id", select:"id,username", filter:`username.ilike.%${clean}%` }
      ];
      for(const plan of attempts){
        const { data, error } = await state.supa.from("users").select(plan.select).or(plan.filter).limit(40);
        if(error){
          if(isMissingRelationError(error, "users")){
            state.feature.profileLookupEnabled = false;
            return [];
          }
          continue;
        }
        return util.uniq((data || []).map(row => row[plan.idField]));
      }
      state.feature.profileLookupEnabled = false;
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
        if(filter) query = query.or("title.ilike.%" + filter + "%,description.ilike.%" + filter + "%,category.ilike.%" + filter + "%");
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
    },

    async fetchFollowerCount(userId){
      const id = util.safe(userId);
      if(!id) return 0;

      let best = 0;
      try{
        const follows = await state.supa
          .from("follows")
          .select("follower_id", { count:"exact", head:true })
          .eq("following_id", id);
        if(!follows.error){
          best = Math.max(best, Math.max(0, util.num(follows.count)));
        }
      }catch(_){ }

      if(state.feature.channelSubscribers !== false){
        try{
          const subs = await state.supa
            .from("channel_subscribers")
            .select("subscriber_user_id", { count:"exact", head:true })
            .eq("channel_id", id);
          if(!subs.error){
            state.feature.channelSubscribers = true;
            best = Math.max(best, Math.max(0, util.num(subs.count)));
          }else if(isMissingRelationError(subs.error, "channel_subscribers")){
            state.feature.channelSubscribers = false;
          }
        }catch(_){ }
      }

      return best;
    },

    async getSubscriberCount(channelId){
      const id = util.safe(channelId);
      if(!id) return 0;

      if(state.feature.channelSubscribers !== false){
        const subs = await state.supa
          .from("channel_subscribers")
          .select("subscriber_user_id", { count:"exact", head:true })
          .eq("channel_id", id);
        if(!subs.error){
          state.feature.channelSubscribers = true;
          return Math.max(0, util.num(subs.count));
        }
        if(isMissingRelationError(subs.error, "channel_subscribers")){
          state.feature.channelSubscribers = false;
        }
      }

      const follows = await state.supa
        .from("follows")
        .select("follower_id", { count:"exact", head:true })
        .eq("following_id", id);
      if(!follows.error){
        return Math.max(0, util.num(follows.count));
      }
      return 0;
    },

    async isSubscribed(channelId, userId){
      const cId = util.safe(channelId);
      const uId = util.safe(userId);
      if(!cId || !uId) return false;

      if(state.feature.channelSubscribers !== false){
        const { data, error } = await state.supa
          .from("channel_subscribers")
          .select("subscriber_user_id")
          .eq("channel_id", cId)
          .eq("subscriber_user_id", uId)
          .limit(1);
        if(!error){
          state.feature.channelSubscribers = true;
          return Array.isArray(data) && data.length > 0;
        }
        if(isMissingRelationError(error, "channel_subscribers")){
          state.feature.channelSubscribers = false;
        }
      }

      const { data, error } = await state.supa
        .from("follows")
        .select("follower_id")
        .eq("follower_id", uId)
        .eq("following_id", cId)
        .limit(1);
      if(error) return false;
      return Array.isArray(data) && data.length > 0;
    },

    async getMemberCount(channelId){
      const id = util.safe(channelId);
      if(!id) return 0;

      if(state.feature.channelMembers !== false){
        const members = await state.supa
          .from("channel_members")
          .select("member_user_id", { count:"exact", head:true })
          .eq("channel_id", id);
        if(!members.error){
          state.feature.channelMembers = true;
          return Math.max(0, util.num(members.count));
        }
        if(isMissingRelationError(members.error, "channel_members")){
          state.feature.channelMembers = false;
        }
      }
      return api.getSubscriberCount(id);
    },

    async isMember(channelId, userId){
      const cId = util.safe(channelId);
      const uId = util.safe(userId);
      if(!cId || !uId) return false;

      if(state.feature.channelMembers !== false){
        const { data, error } = await state.supa
          .from("channel_members")
          .select("member_user_id")
          .eq("channel_id", cId)
          .eq("member_user_id", uId)
          .limit(1);
        if(!error){
          state.feature.channelMembers = true;
          return Array.isArray(data) && data.length > 0;
        }
        if(isMissingRelationError(error, "channel_members")){
          state.feature.channelMembers = false;
        }
      }

      return api.isSubscribed(cId, uId);
    },

    async getMembershipFee(channelId){
      const id = util.safe(channelId);
      if(!id) return 0;
      if(state.feature.channelMembershipPlans === false){
        return 0;
      }

      const { data, error } = await state.supa
        .from("channel_membership_plans")
        .select("join_fee_inr")
        .eq("channel_id", id)
        .maybeSingle();
      if(error){
        if(isMissingRelationError(error, "channel_membership_plans")){
          state.feature.channelMembershipPlans = false;
          return 0;
        }
        throw error;
      }
      state.feature.channelMembershipPlans = true;
      return Math.max(0, Math.floor(util.num(data?.join_fee_inr)));
    },

    async setMembershipFee(channelId, feeInr){
      const id = util.safe(channelId);
      const fee = Math.max(0, Math.floor(util.num(feeInr)));
      if(!id) throw new Error("channel_id_required");
      const payload = { channel_id:id, join_fee_inr:fee, currency:"INR", updated_at:new Date().toISOString() };
      const { error } = await state.supa
        .from("channel_membership_plans")
        .upsert(payload, { onConflict:"channel_id" });
      if(error){
        if(isMissingRelationError(error, "channel_membership_plans")){
          state.feature.channelMembershipPlans = false;
        }
        throw error;
      }
      state.feature.channelMembershipPlans = true;
      return fee;
    },

    async fetchMonetizationStatus(userId){
      const id = util.safe(userId);
      if(!id){
        return { ok:false, unlocked:false, payment_ready:false, status_message:"Login required." };
      }
      return fetchApiJson(`/api/videos/monetization/status?user_id=${encodeURIComponent(id)}`, {
        method:"GET",
        headers:{ Accept:"application/json" },
        credentials:"omit"
      });
    },

    async createMonetizationOrder(payload){
      return fetchApiJson("/api/videos/monetization/order", {
        method:"POST",
        headers:{ "Content-Type":"application/json", Accept:"application/json" },
        body:JSON.stringify(payload || {}),
        credentials:"omit"
      });
    },

    async verifyMonetizationPayment(payload){
      return fetchApiJson("/api/videos/monetization/verify", {
        method:"POST",
        headers:{ "Content-Type":"application/json", Accept:"application/json" },
        body:JSON.stringify(payload || {}),
        credentials:"omit"
      });
    },

    async createMembershipOrder(payload){
      return fetchApiJson("/api/payment/razorpay/order", {
        method:"POST",
        headers:{ "Content-Type":"application/json", Accept:"application/json" },
        body:JSON.stringify(payload || {}),
        credentials:"omit"
      });
    },

    async verifyMembershipPayment(payload){
      return fetchApiJson("/api/payment/razorpay/verify", {
        method:"POST",
        headers:{ "Content-Type":"application/json", Accept:"application/json" },
        body:JSON.stringify(payload || {}),
        credentials:"omit"
      });
    }
  };

  api.buildSearchPool = async function(term){
    const text = util.clean(term);
    if(!text) return [];
    const [contentRows, broadRows, channelIds] = await Promise.all([
      api.fetchVideos({ search:text, limit:SEARCH_SCAN_LIMIT }),
      api.fetchVideos({ limit:SEARCH_SCAN_LIMIT }),
      api.searchChannelIds(text)
    ]);
    let channelRows = [];
    if(channelIds.length){
      channelRows = await api.fetchVideos({ userIds:channelIds, limit:SEARCH_SCAN_LIMIT });
    }
    const seen = new Set();
    const merged = [];
    contentRows.concat(channelRows).concat(broadRows.filter(row => matchesVideoSearch(row, text))).forEach(row => {
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
        '</div>' +
      '</button>' +
      '<div class="mv-card-meta">' +
        '<div class="mv-avatar mv-channel-link" data-avatar data-channel-open="1">' + avatarHtml(channel) + '</div>' +
        '<div class="mv-card-copy">' +
          '<h3 class="mv-card-title" data-title>' + util.esc(item.title) + '</h3>' +
          '<p class="mv-card-channel mv-channel-link" data-channel data-channel-open="1">' + util.esc(channel.name) + '</p>' +
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
    card.querySelectorAll("[data-channel-open='1']").forEach(node => {
      node.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        openChannelPage(item.user_id);
      });
    });
    card.querySelector("[data-menu-btn='1']").addEventListener("click", (event) => {
      event.stopPropagation();
      const visible = !menu.classList.contains("show");
      closeMenus();
      menu.classList.toggle("show", visible);
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

  async function refreshShareCount(videoId){
    const id = util.safe(videoId);
    if(!id){
      state.shareCount = 0;
      renderActionMetrics();
      return 0;
    }

    let count = 0;
    if(window.NOVA && typeof window.NOVA.getShareCount === "function"){
      try{
        count = Math.max(0, util.num(await window.NOVA.getShareCount("video", id)));
      }catch(_){ }
    }
    state.shareCount = count;
    if(state.activeVideoId === id){
      renderActionMetrics();
    }
    return count;
  }

  async function shareVideo(videoId){
    const id = util.safe(videoId);
    if(!id) return;
    const video = state.videos.get(id);
    const url = new URL("m-videos.html", location.href);
    url.searchParams.set("v", id);

    let tracked = false;
    let channel = "copy";
    try{
      if(navigator.share){
        await navigator.share({ title:video ? video.title : "NOVAGAPP Video", url:url.href });
        tracked = true;
        channel = "native";
      }
    }catch(_){ }

    if(!tracked){
      try{
        if(navigator.clipboard && navigator.clipboard.writeText){
          await navigator.clipboard.writeText(url.href);
          showToast("Video link copied");
          tracked = true;
          channel = "copy";
        }
      }catch(_){ }
    }

    if(!tracked){
      window.prompt("Copy this link", url.href);
      return;
    }

    const user = await api.ensureAuth();
    if(user && window.NOVA && typeof window.NOVA.trackShare === "function"){
      try{
        await window.NOVA.trackShare("video", id, channel);
      }catch(_){ }
    }
    await refreshShareCount(id);
  }

  async function loadShareState(token){
    if(token !== state.watchToken) return;
    const video = currentVideo();
    if(!video) return;
    await refreshShareCount(video.id);
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

    const posterUrl = util.safe(video.thumbnail_url) || "Images/no-image.jpg";
    if(dom.playerPoster){
      dom.playerPoster.src = posterUrl;
      dom.playerPoster.alt = util.safe(video.title) || "Video thumbnail";
    }
    setPlayerLoading(true, true);
    dom.player.poster = posterUrl;
    dom.player.src = video.video_url;
    try{ dom.player.load(); }catch(_){ }
    dom.watchTitle.textContent = video.title;
    dom.watchMeta.textContent = util.compact(video.views) + " views . " + util.ago(video.created_at);
    dom.watchAvatar.innerHTML = avatarHtml(channel);
    dom.watchChannelName.textContent = channel.name;
    dom.watchChannelSub.textContent = util.compact(channel.subscribers) + " followers . " + util.compact(channel.subscribers) + " members";
    dom.watchAvatar.dataset.channelId = util.safe(video.user_id);
    dom.watchChannelName.dataset.channelId = util.safe(video.user_id);
    dom.watchChannelSub.dataset.channelId = util.safe(video.user_id);
    dom.likeCount.textContent = util.compact(video.likes_count);
    dom.dislikeCount.textContent = util.compact(video.dislikes_count);
    dom.commentCount.textContent = "0";
    if(dom.commentActionCount) dom.commentActionCount.textContent = "0";
    state.shareCount = 0;
    state.channelSubscribers = Math.max(0, util.num(channel.subscribers));
    state.channelMembers = Math.max(0, util.num(channel.subscribers));
    state.channelMembershipFeeInr = 0;
    state.memberJoined = false;
    renderActionMetrics();
    dom.descText.textContent = video.description || "No description available.";
    setDescriptionOpen(false);
    setCommentsOpen(false, false);
    setSaveButton();
    setPanel("watch", false);
    refreshPlayerOverlayState();

    if(pushState){
      const url = new URL(location.href);
      url.searchParams.set("v", id);
      url.searchParams.delete("view");
      history.pushState({ mv:"watch", videoId:id }, "", url.pathname + url.search);
    }

    await Promise.allSettled([
      loadReactionState(token),
      loadSubscribeState(token),
      loadMemberState(token),
      loadShareState(token),
      loadComments(token),
      loadRecommendations(token)
    ]);

    await recordView();

    if(autoplay){
      try{ await dom.player.play(); }catch(_){ }
    }
    refreshPlayerOverlayState();
  }

  async function loadReactionState(token){
    if(token !== state.watchToken) return;
    const video = currentVideo();
    if(!video) return;

    const user = await api.ensureAuth();
    state.activeReaction = user ? await getUserReactionCompat(video.id, user.id) : null;

    const counts = await getReactionCountsCompat(video.id);
    video.likes_count = Math.max(0, util.num(counts.likes));
    video.dislikes_count = Math.max(0, util.num(counts.dislikes));
    state.videos.set(video.id, video);
    dom.likeCount.textContent = util.compact(video.likes_count);
    dom.dislikeCount.textContent = util.compact(video.dislikes_count);
    updateFeedCard(video.id);
    setReactionButtons();
  }

  async function loadSubscribeState(token){
    if(token !== state.watchToken) return;
    const video = currentVideo();
    if(!video) return;

    const user = await api.ensureAuth();
    const channelId = util.safe(video.user_id);
    state.channelSubscribers = await api.getSubscriberCount(channelId);
    const channel = getChannel(channelId);
    state.channels.set(channelId, {
      id:channelId,
      name:channel.name,
      avatar:channel.avatar,
      subscribers:state.channelSubscribers
    });

    if(!user){
      state.subscribed = false;
      renderActionMetrics();
      return;
    }

    if(util.safe(user.id) === channelId){
      state.subscribed = false;
      renderActionMetrics();
      return;
    }

    state.subscribed = await api.isSubscribed(channelId, user.id);
    renderActionMetrics();
  }

  async function loadMemberState(token){
    if(token !== state.watchToken) return;
    const video = currentVideo();
    if(!video) return;

    const channelId = util.safe(video.user_id);
    const user = await api.ensureAuth();
    try{
      state.channelMembershipFeeInr = await api.getMembershipFee(channelId);
    }catch(err){
      console.error("membership_fee_load_failed", err);
      state.channelMembershipFeeInr = 0;
    }
    state.channelMembers = await api.getMemberCount(channelId);
    if(!user || util.safe(user.id) === channelId){
      state.memberJoined = false;
      renderActionMetrics();
      return;
    }
    state.memberJoined = await api.isMember(channelId, user.id);
    renderActionMetrics();
  }

  async function recordView(){
    const video = currentVideo();
    if(!video) return;
    const user = await api.ensureAuth();
    if(!user) return;

    if(state.feature.rpcRecordView !== false){
      const { data, error } = await state.supa.rpc("video_record_view_rpc", {
        p_video_id: video.id
      });
      if(!error){
        state.feature.rpcRecordView = true;
        video.views = Math.max(0, util.num(data?.views));
        state.videos.set(video.id, video);
        dom.watchMeta.textContent = util.compact(video.views) + " views . " + util.ago(video.created_at);
        updateFeedCard(video.id);
        return;
      }
      if(isMissingFunctionError(error, "video_record_view_rpc")){
        state.feature.rpcRecordView = false;
      }else if(isBrokenRpcError(error)){
        state.feature.rpcRecordView = false;
      }else{
        console.error("record_view_failed", error);
        return;
      }
    }

    if(window.NOVA && typeof window.NOVA.trackVideoView === "function"){
      try{
        const tracked = await window.NOVA.trackVideoView(video.id);
        const trackedCount = Math.max(0, util.num(tracked?.views || tracked?.count));
        if(trackedCount > 0){
          video.views = Math.max(Math.max(0, util.num(video.views)), trackedCount);
        }else{
          video.views = Math.max(0, util.num(video.views) + 1);
        }
        state.videos.set(video.id, video);
        dom.watchMeta.textContent = util.compact(video.views) + " views . " + util.ago(video.created_at);
        updateFeedCard(video.id);
        return;
      }catch(err){
        console.error("record_view_compat_failed", err);
      }
    }

    if(util.safe(user.id) !== util.safe(video.user_id)){
      return;
    }

    const nextViews = Math.max(0, util.num(video.views) + 1);
    const { data, error } = await state.supa
      .from("videos")
      .update({ views:nextViews })
      .eq("id", video.id)
      .select("views")
      .maybeSingle();
    if(error){
      if(isNoRowsError(error) || errorStatus(error) === 406){
        return;
      }
      console.error("record_view_patch_failed", error);
      return;
    }
    video.views = Math.max(0, util.num(data?.views || nextViews));
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
      let handled = false;
      if(state.feature.rpcToggleReaction !== false){
        const { data, error } = await state.supa.rpc("video_toggle_reaction_rpc", {
          p_video_id: video.id,
          p_type: type
        });
        if(!error){
          state.feature.rpcToggleReaction = true;
          state.activeReaction = util.safe(data?.reaction) || null;
          video.likes_count = Math.max(0, util.num(data?.likes_count));
          video.dislikes_count = Math.max(0, util.num(data?.dislikes_count));
          handled = true;
        }else if(isMissingFunctionError(error, "video_toggle_reaction_rpc")){
          state.feature.rpcToggleReaction = false;
        }else if(isBrokenRpcError(error)){
          state.feature.rpcToggleReaction = false;
        }else{
          throw error;
        }
      }

      if(!handled){
        if(!window.NOVA || typeof window.NOVA.toggleReaction !== "function"){
          throw new Error("reaction_api_unavailable");
        }
        const res = await window.NOVA.toggleReaction("video", video.id, type);
        state.activeReaction = util.safe(res?.reaction) || null;
        const counts = await getReactionCountsCompat(video.id);
        video.likes_count = Math.max(0, util.num(counts.likes));
        video.dislikes_count = Math.max(0, util.num(counts.dislikes));
      }

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
    renderActionMetrics();
    try{
      let handled = false;
      if(state.feature.rpcToggleSubscribe !== false){
        const { data, error } = await state.supa.rpc("video_toggle_subscribe_rpc", {
          p_channel_id: video.user_id
        });
        if(!error){
          state.feature.rpcToggleSubscribe = true;
          state.subscribed = !!data?.subscribed;
          state.channelSubscribers = Math.max(0, util.num(data?.subscribers_count));
          handled = true;
        }else if(isMissingFunctionError(error, "video_toggle_subscribe_rpc")){
          state.feature.rpcToggleSubscribe = false;
        }else if(isBrokenRpcError(error)){
          state.feature.rpcToggleSubscribe = false;
        }else{
          throw error;
        }
      }

      if(!handled){
        if(window.NOVA && typeof window.NOVA.toggleFollow === "function"){
          const result = await window.NOVA.toggleFollow(video.user_id);
          if(result?.error) throw result.error;
          state.subscribed = !!result?.following;
          state.channelSubscribers = await api.getSubscriberCount(video.user_id);
        }else{
          throw new Error("subscribe_api_unavailable");
        }
      }

      if(state.feature.channelMembers === false){
        state.channelMembers = Math.max(0, util.num(state.channelSubscribers));
        state.memberJoined = state.subscribed;
      }

      const channel = getChannel(video.user_id);
      state.channels.set(video.user_id, {
        id:video.user_id,
        name:channel.name,
        avatar:channel.avatar,
        subscribers:state.channelSubscribers
      });
      renderActionMetrics();
    }catch(err){
      console.error("toggle_subscribe_failed", err);
      showToast("Unable to update subscription", true);
    }finally{
      state.subscribeBusy = false;
      renderActionMetrics();
    }
  }

  async function payMembershipFee(options){
    const opts = options || {};
    const amountInr = Math.max(0, Math.floor(util.num(opts.amountInr)));
    if(amountInr <= 0) return true;

    const user = opts.user || null;
    const channelId = util.safe(opts.channelId);
    if(!user || !user.id || !channelId){
      showToast("Login required before payment.", true);
      return false;
    }

    let orderPayload = null;
    try{
      orderPayload = await api.createMembershipOrder({
        amount_paise:amountInr * 100,
        currency:"INR",
        user_id:user.id,
        user_name:util.safe(user.full_name || user.name || user.username || user.email || "member"),
        notes:{
          feature:"channel_membership",
          channel_id:channelId,
          channel_name:util.safe(opts.channelName || "").slice(0, 40)
        }
      });
    }catch(err){
      console.error("membership_order_failed", err);
      showToast("Membership payment service unavailable.", true);
      return false;
    }

    const order = orderPayload?.order || null;
    const keyId = util.safe(order?.key_id);
    const orderId = util.safe(order?.razorpay_order_id);
    const amountPaise = Math.round(util.num(order?.amount_paise));
    if(!keyId || !orderId || amountPaise < 100){
      showToast("Unable to create membership payment order.", true);
      return false;
    }

    try{
      await ensureRazorpaySdk();
    }catch(err){
      console.error("membership_sdk_failed", err);
      showToast("Payment SDK failed to load.", true);
      return false;
    }

    try{
      await new Promise((resolve, reject) => {
        let completed = false;
        const finish = (fn, value) => {
          if(completed) return;
          completed = true;
          fn(value);
        };

        const rzp = new window.Razorpay({
          key:keyId,
          order_id:orderId,
          amount:amountPaise,
          currency:"INR",
          name:"NOVAGAPP Membership",
          description:`Channel membership (INR ${amountInr})`,
          handler:async function(res){
            try{
              const verifyPayload = await api.verifyMembershipPayment({
                razorpay_order_id:util.safe(res?.razorpay_order_id || orderId),
                razorpay_payment_id:util.safe(res?.razorpay_payment_id),
                razorpay_signature:util.safe(res?.razorpay_signature)
              });
              if(!verifyPayload?.verified){
                throw new Error(util.safe(verifyPayload?.message || "payment_verify_failed"));
              }
              finish(resolve, true);
            }catch(err){
              finish(reject, err);
            }
          },
          modal:{
            ondismiss:function(){
              finish(reject, new Error("payment_cancelled"));
            }
          },
          prefill:{
            name:util.safe(user.full_name || user.name || user.username || ""),
            email:util.safe(user.email || "")
          },
          theme:{ color:"#ff6a00" }
        });
        rzp.on("payment.failed", function(payload){
          const reason = util.safe(payload?.error?.description) || "payment_failed";
          finish(reject, new Error(reason));
        });
        rzp.open();
      });
      showToast("Payment verified. Joining...");
      return true;
    }catch(err){
      if(util.safe(err?.message) !== "payment_cancelled"){
        console.error("membership_payment_failed", err);
        showToast(util.safe(err?.message) || "Payment failed.", true);
      }
      return false;
    }
  }

  async function toggleMember(){
    const video = currentVideo();
    if(!video || state.memberBusy) return;

    const user = await api.requireAuth();
    if(!user) return;
    const channelId = util.safe(video.user_id);
    if(!channelId){
      return;
    }
    const selfChannel = util.safe(user.id) === channelId;

    if(selfChannel){
      if(state.feature.channelMembershipPlans === false){
        showToast("Membership fee table is not configured.", true);
        return;
      }
      const raw = window.prompt("Set join fee in INR (0 for free)", String(Math.max(0, Math.floor(util.num(state.channelMembershipFeeInr)))));
      if(raw === null) return;
      const parsed = Number(raw);
      if(!Number.isFinite(parsed) || parsed < 0){
        showToast("Enter a valid INR amount.", true);
        return;
      }
      state.memberBusy = true;
      renderActionMetrics();
      try{
        const savedFee = await api.setMembershipFee(channelId, Math.floor(parsed));
        state.channelMembershipFeeInr = savedFee;
        showToast("Membership fee updated.");
      }catch(err){
        console.error("membership_fee_update_failed", err);
        showToast("Unable to update membership fee.", true);
      }finally{
        state.memberBusy = false;
        renderActionMetrics();
      }
      return;
    }

    state.memberBusy = true;
    renderActionMetrics();
    try{
      let nextJoined = state.memberJoined;
      let handled = false;
      const feeInr = Math.max(0, Math.floor(util.num(state.channelMembershipFeeInr)));

      if(!nextJoined && feeInr > 0){
        const paid = await payMembershipFee({
          amountInr:feeInr,
          channelId,
          channelName:getChannel(channelId).name,
          user
        });
        if(!paid){
          return;
        }
      }

      if(state.feature.channelMembers !== false){
        if(nextJoined){
          const { error } = await state.supa
            .from("channel_members")
            .delete()
            .eq("channel_id", channelId)
            .eq("member_user_id", user.id);
          if(!error){
            state.feature.channelMembers = true;
            nextJoined = false;
            handled = true;
          }else if(isMissingRelationError(error, "channel_members")){
            state.feature.channelMembers = false;
          }else{
            throw error;
          }
        }else{
          const { error } = await state.supa
            .from("channel_members")
            .insert({ channel_id:channelId, member_user_id:user.id });
          if(!error || errorCode(error) === "23505"){
            state.feature.channelMembers = true;
            nextJoined = true;
            handled = true;
          }else if(isMissingRelationError(error, "channel_members")){
            state.feature.channelMembers = false;
          }else{
            throw error;
          }
        }
      }

      if(!handled){
        await toggleSubscribe();
        nextJoined = state.subscribed;
      }

      state.memberJoined = !!nextJoined;
      state.channelMembers = await api.getMemberCount(channelId);
      renderActionMetrics();
    }catch(err){
      console.error("toggle_member_failed", err);
      showToast("Unable to update membership", true);
    }finally{
      state.memberBusy = false;
      renderActionMetrics();
    }
  }

  async function loadComments(token){
    if(token !== state.watchToken) return;
    const video = currentVideo();
    if(!video) return;

    let rows = [];
    let compatMode = false;

    if(state.feature.videoComments !== false){
      const { data, error } = await state.supa
        .from("video_comments")
        .select("id,video_id,user_id,parent_comment_id,comment_text,likes,created_at")
        .eq("video_id", video.id)
        .order("created_at", { ascending:true })
        .limit(500);
      if(!error){
        state.feature.videoComments = true;
        rows = Array.isArray(data) ? data : [];
      }else if(isMissingRelationError(error, "video_comments")){
        state.feature.videoComments = false;
      }else{
        console.error("comments_load_failed", error);
        dom.commentsList.innerHTML = "";
        dom.commentsEmpty.classList.remove("mv-hidden");
        dom.commentsEmpty.textContent = "Unable to load comments.";
        dom.commentCount.textContent = "0";
        if(dom.commentActionCount) dom.commentActionCount.textContent = "0";
        return;
      }
    }

    if(state.feature.videoComments === false){
      if(!window.NOVA || typeof window.NOVA.getCommentThread !== "function"){
        dom.commentsList.innerHTML = "";
        dom.commentsEmpty.classList.remove("mv-hidden");
        dom.commentsEmpty.textContent = "Comments are unavailable right now.";
        dom.commentCount.textContent = "0";
        if(dom.commentActionCount) dom.commentActionCount.textContent = "0";
        return;
      }
      const thread = await window.NOVA.getCommentThread("video", video.id, {
        limit:500,
        ownerUserId:video.user_id
      });
      if(thread?.error){
        console.error("comments_load_failed", thread.error);
        dom.commentsList.innerHTML = "";
        dom.commentsEmpty.classList.remove("mv-hidden");
        dom.commentsEmpty.textContent = "Unable to load comments.";
        dom.commentCount.textContent = "0";
        if(dom.commentActionCount) dom.commentActionCount.textContent = "0";
        return;
      }
      compatMode = true;
      rows = (thread?.flat || []).map(item => ({
        id:item?.rowId || item?.id || "",
        video_id:video.id,
        user_id:item?.user_id || "",
        parent_comment_id:item?.parentRowId || null,
        comment_text:item?.body || "",
        likes:0,
        created_at:item?.created_at || new Date().toISOString()
      }));
    }

    dom.commentCount.textContent = String(rows.length);
    if(dom.commentActionCount){
      dom.commentActionCount.textContent = util.compact(rows.length);
    }
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
    if(!compatMode && state.me && commentIds.length && state.feature.videoCommentLikes !== false){
      const { data:likedRows, error:likedError } = await state.supa
        .from("video_comment_likes")
        .select("comment_id")
        .eq("user_id", state.me.id)
        .in("comment_id", commentIds);
      if(!likedError && Array.isArray(likedRows)){
        state.feature.videoCommentLikes = true;
        likedRows.forEach(row => liked.add(util.safe(row.comment_id)));
      }else if(isMissingRelationError(likedError, "video_comment_likes")){
        state.feature.videoCommentLikes = false;
      }
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
              '<div class="mv-comment-meta"><strong class="mv-channel-link" data-comment-channel="1">' + util.esc(channel.name) + '</strong><span>' + util.esc(util.ago(row.created_at)) + '</span></div>' +
              '<p class="mv-comment-text">' + util.esc(row.comment_text) + '</p>' +
              '<div class="mv-comment-actions">' +
                (compatMode ? '' : '<button type="button" data-action="like" class="' + (liked.has(rowId) ? "active" : "") + '">Like (' + util.compact(row.likes) + ')</button>') +
                '<button type="button" data-action="reply">Reply</button>' +
                (state.me && util.safe(state.me.id) === util.safe(row.user_id) ? '<button type="button" data-action="delete">Delete</button>' : '') +
              '</div>' +
            '</div>' +
          '</div>';

        const channelOpenNode = node.querySelector("[data-comment-channel='1']");
        if(channelOpenNode){
          channelOpenNode.addEventListener("click", () => openChannelPage(row.user_id));
        }

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
            if(action === "delete") return void deleteComment(rowId, compatMode);
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
      if(state.feature.videoComments === false && window.NOVA){
        let ok = false;
        if(state.replyParentId && typeof window.NOVA.replyComment === "function"){
          ok = await window.NOVA.replyComment("video", video.id, state.replyParentId, text);
        }else if(typeof window.NOVA.addComment === "function"){
          ok = await window.NOVA.addComment("video", video.id, text);
        }
        if(!ok){
          throw new Error("comment_submit_failed");
        }
      }else{
        const payload = {
          video_id:video.id,
          user_id:user.id,
          comment_text:text
        };
        if(state.replyParentId) payload.parent_comment_id = state.replyParentId;

        const { error } = await state.supa.from("video_comments").insert(payload);
        if(error){
          if(isMissingRelationError(error, "video_comments")){
            state.feature.videoComments = false;
            if(window.NOVA && typeof window.NOVA.addComment === "function"){
              let ok = false;
              if(state.replyParentId && typeof window.NOVA.replyComment === "function"){
                ok = await window.NOVA.replyComment("video", video.id, state.replyParentId, text);
              }else{
                ok = await window.NOVA.addComment("video", video.id, text);
              }
              if(!ok) throw new Error("comment_submit_failed");
            }else{
              throw error;
            }
          }else{
            throw error;
          }
        }
      }

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

  async function deleteComment(commentId, compatMode){
    const id = util.safe(commentId);
    if(!id) return;

    const user = await api.requireAuth();
    if(!user) return;
    const useCompat = compatMode || state.feature.videoComments === false;

    if(useCompat){
      if(!window.NOVA || typeof window.NOVA.deleteCommentById !== "function"){
        showToast("Delete is unavailable", true);
        return;
      }
      const ok = await window.NOVA.deleteCommentById(id);
      if(!ok){
        showToast("Unable to delete comment", true);
        return;
      }
      await loadComments(state.watchToken);
      return;
    }

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
    if(state.feature.videoComments === false){
      showToast("Comment likes are unavailable in this comment mode.", true);
      return;
    }
    const id = util.safe(commentId);
    if(!id) return;

    const user = await api.requireAuth();
    if(!user) return;

    if(state.feature.rpcToggleCommentLike !== false){
      const { error } = await state.supa.rpc("video_toggle_comment_like_rpc", {
        p_comment_id: id
      });
      if(!error){
        state.feature.rpcToggleCommentLike = true;
        await loadComments(state.watchToken);
        return;
      }
      if(isMissingFunctionError(error, "video_toggle_comment_like_rpc")){
        state.feature.rpcToggleCommentLike = false;
      }else{
        console.error("comment_like_failed", error);
        showToast("Unable to update comment like", true);
        return;
      }
    }

    showToast("Comment likes are unavailable in this setup.", true);
  }

  async function loadRecommendations(token){
    if(token !== state.watchToken) return;
    const video = currentVideo();
    if(!video) return;

    const seen = new Set([video.id]);
    const pool = [];
    const addRows = (list) => {
      (list || []).forEach(row => {
        if(!row || !row.id || seen.has(row.id)) return;
        seen.add(row.id);
        pool.push(row);
      });
    };
    const renderRows = async (list) => {
      dom.recommendList.innerHTML = "";
      if(!list.length){
        dom.recommendList.innerHTML = '<div class="mv-empty">No recommendations available.</div>';
        return;
      }
      await api.fetchProfiles(list.map(item => item.user_id));
      list.forEach(item => {
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
    };

    try{
      addRows(await api.fetchVideos({ excludeId:video.id, limit:140 }));
      addRows(Array.from(state.videos.values()));
      await renderRows(shuffleRows(pool).slice(0, 10));
    }catch(err){
      console.error("recommend_failed", err);
      const fallback = shuffleRows(
        Array.from(state.videos.values()).filter(item => item && item.id && item.id !== video.id)
      ).slice(0, 10);
      if(!fallback.length){
        dom.recommendList.innerHTML = '<div class="mv-empty">Unable to load recommendations.</div>';
        return;
      }
      await renderRows(fallback);
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

  async function refreshMonetizationStatus(){
    if(!state.supa){
      return;
    }
    const user = await api.ensureAuth();
    const m = state.monetization || {};
    if(!user || !user.id){
      m.followers = 0;
      m.followersReady = false;
      m.unlocked = false;
      m.paymentReady = false;
      m.keyId = "";
      m.statusMessage = "Login required for monetization.";
      renderMonetizationUi();
      return;
    }

    m.loading = true;
    m.statusMessage = "Checking monetization requirements...";
    renderMonetizationUi();

    try{
      const followers = await api.fetchFollowerCount(user.id);
      m.followers = Math.max(0, util.num(followers));

      try{
        const statusPayload = await api.fetchMonetizationStatus(user.id);
        m.unlocked = !!statusPayload?.unlocked;
        m.paymentReady = !!statusPayload?.payment_ready;
        m.keyId = util.safe(statusPayload?.key_id || "");
        m.statusMessage = util.safe(statusPayload?.status_message || "");
      }catch(err){
        console.error("monetization_status_failed", err);
        m.unlocked = false;
        m.paymentReady = false;
        m.statusMessage = "Payment service unavailable right now.";
      }
    }finally{
      m.loading = false;
      renderMonetizationUi();
    }
  }

  async function unlockMonetization(){
    const user = await api.requireAuth();
    if(!user || !user.id) return;

    const m = state.monetization || {};
    if(m.unlockBusy) return;

    await refreshMonetizationStatus();
    if(!m.followersReady){
      showToast(monetizationBlockedMessage(), true);
      return;
    }
    if(m.unlocked){
      showToast("Monetization already unlocked.");
      renderMonetizationUi();
      return;
    }
    if(!m.paymentReady){
      showToast("Payment is not configured right now.", true);
      return;
    }

    m.unlockBusy = true;
    m.statusMessage = "Creating payment order...";
    renderMonetizationUi();

    try{
      const orderPayload = await api.createMonetizationOrder({
        user_id: user.id,
        followers_count: Math.max(0, util.num(m.followers))
      });
      const order = orderPayload?.order || null;
      if(!order?.razorpay_order_id){
        throw new Error(util.safe(orderPayload?.message) || "monetization_order_create_failed");
      }

      const paymentKey = util.safe(order.key_id || m.keyId);
      if(!paymentKey){
        throw new Error("payment_key_missing");
      }

      await ensureRazorpaySdk();

      await new Promise((resolve, reject) => {
        let done = false;
        const finish = (fn, value) => {
          if(done) return;
          done = true;
          fn(value);
        };

        const rzp = new window.Razorpay({
          key: paymentKey,
          order_id: order.razorpay_order_id,
          amount: Number(order.amount_paise || 0),
          currency: util.safe(order.currency || "INR") || "INR",
          name: "NOVAGAPP",
          description: `Video monetization unlock ($${MONETIZATION_UNLOCK_USD})`,
          theme: { color:"#ff6a00" },
          handler: async function(res){
            try{
              const verifyPayload = await api.verifyMonetizationPayment({
                user_id: user.id,
                razorpay_order_id: util.safe(res?.razorpay_order_id || order.razorpay_order_id),
                razorpay_payment_id: util.safe(res?.razorpay_payment_id),
                razorpay_signature: util.safe(res?.razorpay_signature)
              });
              if(!verifyPayload?.verified || !verifyPayload?.unlocked){
                throw new Error(util.safe(verifyPayload?.message) || "monetization_verify_failed");
              }
              finish(resolve, true);
            }catch(err){
              finish(reject, err);
            }
          },
          modal:{
            ondismiss: function(){
              finish(reject, new Error("payment_cancelled"));
            }
          }
        });

        rzp.on("payment.failed", function(payload){
          const reason = util.safe(payload?.error?.description) || "payment_failed";
          finish(reject, new Error(reason));
        });
        rzp.open();
      });

      m.unlocked = true;
      m.statusMessage = "Monetization unlocked successfully.";
      showToast("Monetization unlocked.");
      await refreshMonetizationStatus();
    }catch(err){
      console.error("unlock_monetization_failed", err);
      if(util.safe(err?.message) !== "payment_cancelled"){
        showToast(util.safe(err?.message) || "Unable to unlock monetization", true);
      }
      m.statusMessage = "Monetization unlock not completed.";
      renderMonetizationUi();
    }finally{
      m.unlockBusy = false;
      renderMonetizationUi();
    }
  }

  function resetUpload(){
    state.uploadVideoFile = null;
    state.uploadThumbFile = null;
    dom.uploadForm.reset();
    dom.videoFileName.textContent = "Supported: MP4, WEBM, MOV, MKV (max 4GB)";
    dom.thumbFileName.textContent = "Supported: JPG, PNG, WEBP (max 8MB)";
    dom.thumbPreviewWrap.classList.add("mv-hidden");
    dom.thumbPreview.removeAttribute("src");
    setUploadProgress(0, "Ready to upload.");
    renderMonetizationUi();
  }

  function validateVideo(file){
    if(!file) return "Video file is required.";
    if(file.size > MAX_VIDEO_SIZE) return "Video file exceeds 4GB.";
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

  function shouldRetryTagsAsText(error){
    const code = util.safe(error?.code).toUpperCase();
    const message = util.safe(error?.message).toLowerCase();
    const details = util.safe(error?.details).toLowerCase();
    if(code === "42804") return true;
    if(message.includes("column") && message.includes("tags") && message.includes("type text")) return true;
    if(details.includes("type text") && details.includes("tags")) return true;
    return false;
  }

  function withTimeout(promise, timeoutMs, label){
    const ms = Math.max(30000, util.num(timeoutMs) || UPLOAD_TIMEOUT_MS);
    let timer = 0;
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error((label || "upload") + "_timeout")), ms);
    });
    return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
  }

  function toAbsoluteAssetUrl(value){
    const raw = util.safe(value);
    if(!raw) return "";
    if(/^https?:\/\//i.test(raw) || raw.startsWith("data:")) return raw;
    try{
      return new URL(raw, location.origin).href;
    }catch(_){
      return raw;
    }
  }

  function shouldUseServerUploadByError(err){
    const text = util.safe(err?.message || err?.error_description || err?.details).toLowerCase();
    if(!text) return false;
    return (
      text.includes("maximum allowed size") ||
      text.includes("object exceeded") ||
      text.includes("payload too large") ||
      text.includes("upload_timeout") ||
      text.includes("timeout") ||
      text.includes("row level security") ||
      text.includes("permission") ||
      text.includes("not allowed") ||
      text.includes("bucket")
    );
  }

  function uploadAssetsThroughServerEndpoint(url, userId, videoFile, thumbFile){
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open("POST", url, true);
      xhr.responseType = "json";
      xhr.timeout = Math.max(SERVER_VIDEO_UPLOAD_TIMEOUT_MS, UPLOAD_TIMEOUT_MS);
      xhr.upload.onprogress = (event) => {
        if(!event.lengthComputable) return;
        const ratio = Math.max(0, Math.min(1, event.loaded / Math.max(1, event.total)));
        const pct = 30 + Math.round(ratio * 52);
        setUploadProgress(pct, thumbFile ? "Uploading video + thumbnail..." : "Uploading video...");
      };
      xhr.onerror = () => reject(new Error("server_upload_network_error"));
      xhr.ontimeout = () => reject(new Error("server_upload_timeout"));
      xhr.onload = () => {
        let payload = xhr.response;
        if(!payload || typeof payload !== "object"){
          try{
            payload = JSON.parse(xhr.responseText || "{}");
          }catch(_){
            payload = {};
          }
        }
        if(xhr.status >= 200 && xhr.status < 300 && payload && payload.ok){
          resolve(payload);
          return;
        }
        const message = util.safe(payload?.message || payload?.error) || `server_upload_failed_${xhr.status}`;
        reject(new Error(message));
      };
      const form = new FormData();
      form.append("user_id", util.safe(userId));
      form.append("video", videoFile, videoFile?.name || "video.mp4");
      if(thumbFile){
        form.append("thumbnail", thumbFile, thumbFile?.name || "thumbnail.jpg");
      }
      xhr.send(form);
    });
  }

  async function uploadAssetsThroughServer(userId, videoFile, thumbFile){
    const bases = [""].concat(buildAutomationApiBases());
    let lastError = null;
    for(const base of bases){
      const cleanBase = util.safe(base).replace(/\/+$/g, "");
      const endpoint = cleanBase ? `${cleanBase}/api/videos/upload-assets` : "/api/videos/upload-assets";
      try{
        const payload = await uploadAssetsThroughServerEndpoint(endpoint, userId, videoFile, thumbFile);
        if(payload && payload.ok){
          return payload;
        }
      }catch(err){
        lastError = err;
      }
    }
    throw lastError || new Error("server_upload_unreachable");
  }

  function explainUploadError(err){
    const text = util.safe(err?.message || err?.error_description || err?.details).toLowerCase();
    if(text.includes("maximum allowed size") || text.includes("object exceeded") || text.includes("payload too large")){
      return "Upload blocked by Supabase file size limit. Check Storage Settings global file size limit (Free plan max is 50MB).";
    }
    if(text.includes("upload_timeout") || text.includes("timeout")){
      return "Upload timed out. Check internet speed and retry with a smaller video.";
    }
    if(text.includes("network") || text.includes("failed to fetch") || text.includes("connection")){
      return "Network issue during upload. Retry after stable internet.";
    }
    if(text.includes("permission") || text.includes("row level security") || text.includes("not allowed")){
      return "Upload blocked by storage policy. Update Supabase storage policies for long_videos bucket.";
    }
    if(text.includes("server_upload") || text.includes("video_asset_upload")){
      return "Large video upload server unavailable. Start backend server and retry.";
    }
    return "Upload failed. Check storage bucket limit and policies.";
  }

  async function insertVideoWithAdaptiveTags(basePayload, tagList){
    const tags = Array.isArray(tagList) ? tagList : [];
    const firstPayload = { ...basePayload, tags };
    let result = await state.supa.from("videos").insert(firstPayload).select(VIDEO_SELECT).maybeSingle();
    if(!result.error){
      return result;
    }
    if(!shouldRetryTagsAsText(result.error)){
      return result;
    }
    const fallbackPayload = { ...basePayload, tags: tags.join(", ") };
    result = await state.supa.from("videos").insert(fallbackPayload).select(VIDEO_SELECT).maybeSingle();
    return result;
  }

  async function uploadVideo(event){
    event.preventDefault();

    const user = await api.requireAuth();
    if(!user) return;

    const title = util.safe(dom.uploadTitle.value);
    const description = util.safe(dom.uploadDescription.value);
    const tags = util.tags(dom.uploadTags.value);
    const category = util.safe(dom.uploadCategory.value) || "General";
    const monetizedRequested = !!dom.uploadMonetized.checked;
    let monetized = false;

    if(!title) return void showToast("Title is required", true);
    if(monetizedRequested){
      if(!isMonetizationAllowed()){
        await refreshMonetizationStatus();
      }
      if(!isMonetizationAllowed()){
        return void showToast(monetizationBlockedMessage(), true);
      }
      monetized = true;
    }

    const videoError = validateVideo(state.uploadVideoFile);
    if(videoError) return void showToast(videoError, true);

    const thumbError = validateThumb(state.uploadThumbFile);
    if(thumbError) return void showToast(thumbError, true);

    dom.uploadSubmit.disabled = true;
    try{
      setUploadProgress(8, "Reading video metadata...");
      const meta = await window.NOVA.getVideoMeta(state.uploadVideoFile);
      const shouldForceServerUpload = Number(state.uploadVideoFile?.size || 0) >= DIRECT_TO_SERVER_UPLOAD_THRESHOLD;
      let videoUrl = "";
      let thumbUrl = "";

      if(shouldForceServerUpload){
        setUploadProgress(26, "Uploading large video on optimized channel...");
        const serverUpload = await withTimeout(
          uploadAssetsThroughServer(user.id, state.uploadVideoFile, state.uploadThumbFile),
          SERVER_VIDEO_UPLOAD_TIMEOUT_MS,
          "server_video_upload"
        );
        videoUrl = toAbsoluteAssetUrl(serverUpload?.video_url);
        thumbUrl = toAbsoluteAssetUrl(serverUpload?.thumbnail_url);
      }else{
        try{
          setUploadProgress(30, "Uploading video...");
          const videoPath = window.NOVA.makePath(user.id, state.uploadVideoFile);
          videoUrl = await withTimeout(
            window.NOVA.uploadToBucket("long_videos", state.uploadVideoFile, videoPath),
            UPLOAD_TIMEOUT_MS,
            "video_upload"
          );

          if(state.uploadThumbFile){
            setUploadProgress(58, "Uploading thumbnail...");
            const thumbPath = window.NOVA.makePath(user.id, state.uploadThumbFile);
            thumbUrl = await withTimeout(
              window.NOVA.uploadToBucket("thumbnails", state.uploadThumbFile, thumbPath),
              Math.max(120000, Math.floor(UPLOAD_TIMEOUT_MS / 2)),
              "thumbnail_upload"
            );
          }
        }catch(primaryUploadErr){
          if(!shouldUseServerUploadByError(primaryUploadErr)){
            throw primaryUploadErr;
          }
          setUploadProgress(36, "Switching to large-video upload mode...");
          const serverUpload = await withTimeout(
            uploadAssetsThroughServer(user.id, state.uploadVideoFile, state.uploadThumbFile),
            SERVER_VIDEO_UPLOAD_TIMEOUT_MS,
            "server_video_upload"
          );
          videoUrl = toAbsoluteAssetUrl(serverUpload?.video_url);
          thumbUrl = toAbsoluteAssetUrl(serverUpload?.thumbnail_url);
        }
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

      const { data, error } = await insertVideoWithAdaptiveTags(payload, tags);
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
      showToast(explainUploadError(err), true);
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

  async function refreshCompatViewCount(videoId){
    const id = util.safe(videoId);
    if(!id || !window.NOVA || typeof window.NOVA.getVideoViewCount !== "function") return;
    try{
      const compatViews = Math.max(0, util.num(await window.NOVA.getVideoViewCount(id)));
      const video = state.videos.get(id);
      if(!video) return;
      const nextViews = Math.max(Math.max(0, util.num(video.views)), compatViews);
      if(nextViews !== util.num(video.views)){
        video.views = nextViews;
        state.videos.set(id, video);
        updateFeedCard(id);
      }
      if(state.activeVideoId === id){
        dom.watchMeta.textContent = util.compact(nextViews) + " views . " + util.ago(video.created_at);
      }
    }catch(_){ }
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
    if(next === "upload") refreshMonetizationStatus().catch(() => {});
    refreshPlayerOverlayState();
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

    if(state.feature.videoLikes !== false){
      channel.on("postgres_changes", { event:"*", schema:"public", table:"video_likes" }, (payload) => {
        const id = util.safe(payload.new?.video_id || payload.old?.video_id);
        if(!id) return;
        schedule("likes_" + id, () => refreshVideo(id), 120);
        if(id === state.activeVideoId) schedule("reaction_state", () => loadReactionState(state.watchToken), 180);
      });
    }

    channel.on("postgres_changes", { event:"*", schema:"public", table:"reactions" }, (payload) => {
      const targetType = util.safe(payload.new?.target_type || payload.old?.target_type).toLowerCase();
      const id = util.safe(payload.new?.target_id || payload.old?.target_id);
      if(targetType !== "video" || !id) return;
      schedule("compat_reaction_" + id, async () => {
        const video = state.videos.get(id);
        if(video){
          const counts = await getReactionCountsCompat(id);
          video.likes_count = Math.max(0, util.num(counts.likes));
          video.dislikes_count = Math.max(0, util.num(counts.dislikes));
          state.videos.set(id, video);
          updateFeedCard(id);
        }
        if(id === state.activeVideoId){
          await loadReactionState(state.watchToken);
        }
      }, 140);
    });

    if(state.feature.videoComments !== false){
      channel.on("postgres_changes", { event:"*", schema:"public", table:"video_comments" }, (payload) => {
        const id = util.safe(payload.new?.video_id || payload.old?.video_id);
        if(!id || id !== state.activeVideoId) return;
        schedule("comments", () => loadComments(state.watchToken), 140);
      });
    }

    channel.on("postgres_changes", { event:"*", schema:"public", table:"comments" }, (payload) => {
      const type = util.safe(payload.new?.target_type || payload.old?.target_type || payload.new?.type || payload.old?.type).toLowerCase();
      const id = util.safe(payload.new?.target_id || payload.old?.target_id || payload.new?.post_id || payload.old?.post_id);
      if(type !== "video" || !id) return;
      schedule("compat_comments_" + id, async () => {
        await refreshCompatViewCount(id);
        if(state.activeVideoId !== id) return;
        await Promise.allSettled([
          loadComments(state.watchToken),
          refreshShareCount(id)
        ]);
      }, 180);
    });

    if(state.feature.channelSubscribers !== false){
      channel.on("postgres_changes", { event:"*", schema:"public", table:"channel_subscribers" }, (payload) => {
        const channelId = util.safe(payload.new?.channel_id || payload.old?.channel_id);
        if(!channelId) return;
        schedule("subs_" + channelId, async () => {
          const { count } = await state.supa
            .from("channel_subscribers")
            .select("subscriber_user_id", { count:"exact", head:true })
            .eq("channel_id", channelId);

          const channelRow = getChannel(channelId);
          const nextSubscribers = Math.max(0, util.num(count));
          state.channels.set(channelId, {
            id:channelId,
            name:channelRow.name,
            avatar:channelRow.avatar,
            subscribers:nextSubscribers
          });

          if(currentVideo() && util.safe(currentVideo().user_id) === channelId){
            state.channelSubscribers = nextSubscribers;
            if(state.feature.channelMembers === false){
              state.channelMembers = nextSubscribers;
            }
            renderActionMetrics();
          }
        }, 180);
        if(state.me && util.safe(state.me.id) === channelId){
          schedule("monetization_subs", () => refreshMonetizationStatus(), 260);
        }
      });
    }

    channel.on("postgres_changes", { event:"*", schema:"public", table:"follows" }, (payload) => {
      const followingId = util.safe(payload.new?.following_id || payload.old?.following_id);
      if(!followingId) return;
      if(state.me && util.safe(state.me.id) === followingId){
        schedule("monetization_follows", () => refreshMonetizationStatus(), 260);
      }
    });

    if(state.feature.channelMembers !== false){
      channel.on("postgres_changes", { event:"*", schema:"public", table:"channel_members" }, (payload) => {
        const channelId = util.safe(payload.new?.channel_id || payload.old?.channel_id);
        if(!channelId) return;
        schedule("members_" + channelId, async () => {
          const nextMembers = await api.getMemberCount(channelId);
          if(currentVideo() && util.safe(currentVideo().user_id) === channelId){
            state.channelMembers = Math.max(0, util.num(nextMembers));
            renderActionMetrics();
          }
        }, 200);
      });
    }

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
    if(state.panel !== "feed"){
      setPanel("feed", true);
    }
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
    if(dom.openChannelTop){
      dom.openChannelTop.addEventListener("click", openMyChannel);
    }
    if(dom.openChannelSide){
      dom.openChannelSide.addEventListener("click", openMyChannel);
    }

    dom.watchBack.addEventListener("click", () => {
      setPanel("feed", true);
      try{ dom.player.pause(); }catch(_){ }
      refreshPlayerOverlayState();
    });
    [dom.watchAvatar, dom.watchChannelName, dom.watchChannelSub].forEach(node => {
      if(!node) return;
      node.addEventListener("click", () => {
        const channelId = util.safe(node.dataset.channelId || currentVideo()?.user_id);
        if(channelId) openChannelPage(channelId);
      });
    });
    dom.player.addEventListener("loadstart", () => setPlayerLoading(true, true));
    dom.player.addEventListener("waiting", () => setPlayerLoading(true, util.num(dom.player.currentTime) < 0.2));
    dom.player.addEventListener("stalled", () => setPlayerLoading(true, util.num(dom.player.currentTime) < 0.2));
    dom.player.addEventListener("canplay", () => {
      if(dom.playerLoading) dom.playerLoading.classList.add("mv-hidden");
    });
    dom.player.addEventListener("playing", () => {
      setPlayerLoading(false, false);
      refreshPlayerOverlayState();
    });
    dom.player.addEventListener("play", refreshPlayerOverlayState);
    dom.player.addEventListener("pause", refreshPlayerOverlayState);
    dom.player.addEventListener("ended", refreshPlayerOverlayState);
    dom.player.addEventListener("loadedmetadata", refreshPlayerOverlayState);
    dom.player.addEventListener("error", () => {
      setPlayerLoading(false, true);
      refreshPlayerOverlayState();
    });
    if(dom.playerCenterToggle){
      dom.playerCenterToggle.addEventListener("click", async () => {
        if(dom.player.paused || dom.player.ended){
          try{ await dom.player.play(); }catch(_){ }
        }else{
          dom.player.pause();
        }
        refreshPlayerOverlayState();
      });
    }
    if(dom.playerPrev){
      dom.playerPrev.addEventListener("click", () => {
        openAdjacentVideo(-1).catch(() => {});
      });
    }
    if(dom.playerNext){
      dom.playerNext.addEventListener("click", () => {
        openAdjacentVideo(1).catch(() => {});
      });
    }
    if(dom.seekBack){
      dom.seekBack.addEventListener("click", () => {
        const current = Math.max(0, util.num(dom.player.currentTime));
        dom.player.currentTime = Math.max(0, current - 10);
      });
    }
    if(dom.seekForward){
      dom.seekForward.addEventListener("click", () => {
        const current = Math.max(0, util.num(dom.player.currentTime));
        const total = Math.max(0, util.num(dom.player.duration));
        const next = current + 10;
        dom.player.currentTime = total > 0 ? Math.min(total, next) : next;
      });
    }

    dom.likeBtn.addEventListener("click", () => toggleReaction("like"));
    dom.dislikeBtn.addEventListener("click", () => toggleReaction("dislike"));
    dom.subscribeBtn.addEventListener("click", toggleSubscribe);
    if(dom.memberBtn){
      dom.memberBtn.addEventListener("click", toggleMember);
    }
    dom.shareBtn.addEventListener("click", () => {
      const video = currentVideo();
      if(video) shareVideo(video.id);
    });
    if(dom.commentBtn){
      dom.commentBtn.addEventListener("click", () => {
        setCommentsOpen(true, true);
        if(dom.commentForm && typeof dom.commentForm.scrollIntoView === "function"){
          dom.commentForm.scrollIntoView({ behavior:"smooth", block:"center" });
        }
      });
    }
    if(dom.saveBtn){
      dom.saveBtn.addEventListener("click", () => {
        const video = currentVideo();
        if(!video) return;
        store.pushUnique(WATCH_LATER_KEY, video.id, 100);
        setSaveButton();
        showToast("Saved to Watch Later");
      });
    }

    if(dom.descTab){
      dom.descTab.addEventListener("click", () => setDescriptionOpen(!state.descriptionOpen));
    }
    if(dom.commentsTab){
      dom.commentsTab.addEventListener("click", () => setCommentsOpen(!state.commentsOpen, false));
    }
    if(dom.commentsClose){
      dom.commentsClose.addEventListener("click", () => setCommentsOpen(false, false));
    }

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
      if(state.panel === "feed"){
        await runSearch("");
      }
    });

    dom.searchInput.addEventListener("focus", () => {
      clearTimeout(state.suggestDebounce);
      state.suggestDebounce = setTimeout(() => renderSuggestions(dom.searchInput.value), 80);
    });

    dom.searchInput.addEventListener("input", () => {
      clearTimeout(state.suggestDebounce);
      clearTimeout(state.searchDebounce);
      state.suggestDebounce = setTimeout(() => renderSuggestions(dom.searchInput.value), 180);
      state.searchDebounce = setTimeout(() => {
        if(state.panel === "feed"){
          runSearch(dom.searchInput.value);
        }
      }, 300);
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
    if(dom.monetizeUnlockBtn){
      dom.monetizeUnlockBtn.addEventListener("click", unlockMonetization);
    }
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
    renderMonetizationUi();

    state.supa = await api.waitForSupabase(10000);
    if(!state.supa){
      setFeedEmpty("Supabase init failed. Start backend config and reload.");
      return;
    }

    await api.ensureAuth();
    await refreshMonetizationStatus();
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

