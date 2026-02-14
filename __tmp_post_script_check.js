
const storyInput = document.getElementById("storyInput");
const postInput = document.getElementById("postInput");
const postCameraInput = document.getElementById("postCameraInput");
const feed = document.getElementById("feed");
const feedNotice = document.getElementById("feedNotice");
const storiesWrap = document.getElementById("stories");
const sentinel = document.getElementById("sentinel");
const storyViewer = document.getElementById("storyViewer");
const storyProgress = document.getElementById("storyProgress");
const storyViewerAvatar = document.getElementById("storyViewerAvatar");
const storyViewerName = document.getElementById("storyViewerName");
const storyViewerTime = document.getElementById("storyViewerTime");
const storyCloseBtn = document.getElementById("storyCloseBtn");
const storyPrevBtn = document.getElementById("storyPrevBtn");
const storyNextBtn = document.getElementById("storyNextBtn");
const storyContent = document.getElementById("storyContent");
const storyCaption = document.getElementById("storyCaption");
const storyInfo = document.getElementById("storyInfo");
const storyMusic = document.getElementById("storyMusic");
const storyMeta = document.getElementById("storyMeta");
const storyOwnerTools = document.getElementById("storyOwnerTools");
const storySeenByBtn = document.getElementById("storySeenByBtn");
const storyViewersPanel = document.getElementById("storyViewersPanel");
const myStoryAvatar = document.getElementById("myStoryAvatar");
const myStoryLabel = document.getElementById("myStoryLabel");
const storyComposer = document.getElementById("storyComposer");
const storyComposerMedia = document.getElementById("storyComposerMedia");
const storyComposerCloseBtn = document.getElementById("storyComposerCloseBtn");
const storyFilterList = document.getElementById("storyFilterList");
const storyCaptionInput = document.getElementById("storyCaptionInput");
const storyInfoInput = document.getElementById("storyInfoInput");
const storyPickMusicBtn = document.getElementById("storyPickMusicBtn");
const storyClearMusicBtn = document.getElementById("storyClearMusicBtn");
const storyMusicSelected = document.getElementById("storyMusicSelected");
const storyThumbList = document.getElementById("storyThumbList");
const storyAddMoreBtn = document.getElementById("storyAddMoreBtn");
const storyPublishBtn = document.getElementById("storyPublishBtn");
const storyComposerStatus = document.getElementById("storyComposerStatus");
const postComposer = document.getElementById("postComposer");
const postComposerMedia = document.getElementById("postComposerMedia");
const postComposerCloseBtn = document.getElementById("postComposerCloseBtn");
const postTextModeInput = document.getElementById("postTextModeInput");
const postFilterList = document.getElementById("postFilterList");
const postTitleInput = document.getElementById("postTitleInput");
const postDescriptionInput = document.getElementById("postDescriptionInput");
const postKeywordsInput = document.getElementById("postKeywordsInput");
const postMusicSearchInput = document.getElementById("postMusicSearchInput");
const postMusicSearchBtn = document.getElementById("postMusicSearchBtn");
const postMusicClearBtn = document.getElementById("postMusicClearBtn");
const postMusicResults = document.getElementById("postMusicResults");
const postMusicSelected = document.getElementById("postMusicSelected");
const postMediaQueue = document.getElementById("postMediaQueue");
const postAddMediaBtn = document.getElementById("postAddMediaBtn");
const postShootVideoBtn = document.getElementById("postShootVideoBtn");
const postPublishBtn = document.getElementById("postPublishBtn");
const postResetBtn = document.getElementById("postResetBtn");
const postComposerStatus = document.getElementById("postComposerStatus");
const appToast = document.getElementById("appToast");
const commentsBackdrop = document.getElementById("commentsBackdrop");
const commentsPanel = document.getElementById("commentsPanel");
const commentsClose = document.getElementById("commentsClose");
const commentsList = document.getElementById("commentsList");
const commentInput = document.getElementById("commentInput");
const commentSend = document.getElementById("commentSend");
const commentReplying = document.getElementById("commentReplying");
const commentReplyLabel = document.getElementById("commentReplyLabel");
const commentReplyCancel = document.getElementById("commentReplyCancel");
const shareBackdrop = document.getElementById("shareBackdrop");
const shareSheet = document.getElementById("shareSheet");
const shareClose = document.getElementById("shareClose");
const shareButtons = Array.from(document.querySelectorAll("[data-share]"));
const shareChatList = document.getElementById("shareChatList");

let user = null;
let page = 0;
let loading = false;
let done = false;
const PAGE_SIZE = 6;
const params = new URLSearchParams(location.search);
const feedUserId = params.get("uid") || "";
const focusPostId = params.get("focus") || "";
const renderedPostIds = new Set();
const storyProfileCache = {};
const commentUserCache = {};
const DEFAULT_AVATAR_DATA_URI = "data:image/gif;base64,R0lGODlhAQABAAAAACwAAAAAAQABAAA=";
let focusApplied = false;
let activeCommentPostId = "";
let activeCommentTargetType = "post";
let activeCommentCountEl = null;
let activeCommentOwnerId = "";
let activeReplyRowId = "";
let activeEditRowId = "";
let activeCommentThread = null;
let activeShareItem = null;
let activeShareCountEl = null;
let currentPlayingAudio = null;
const MUSIC_MARKER = "__NOVA_MUSIC__";
const POST_META_MARKER = "__NOVA_POST_META__";
const SHARE_COUNT_PREFIX = "m_post_share_count_";
const SAVED_POST_IDS_KEY = "m_saved_post_ids";
const POST_PUBLIC_COMPAT_PAYLOAD = {
  is_public: true,
  visibility: "public",
  post_visibility: "public",
  status: "published",
  publish_status: "published",
  privacy: "public",
  audience: "public",
  scope: "public",
  is_private: false,
  private: false,
  is_draft: false,
  draft: false
};
const COUNT_POLL_INTERVAL_MS = 12000;
const postCountRefs = new Map();
const pendingPostCountRefresh = new Map();
let feedRealtimeChannel = null;
let shareRealtimeChannel = null;
let feedReloadTimer = null;
let countPollTimer = null;
let feedPermissionAlerted = false;
let feedVersion = 0;
let feedReady = false;
const STORY_META_MARKER = "__NOVA_STORY_META__";
const STORY_IMAGE_DURATION_MS = 5000;
const STORY_FILTERS = [
  { id:"none", label:"Normal", css:"none" },
  { id:"warm", label:"Warm", css:"saturate(1.15) contrast(1.04) hue-rotate(-8deg)" },
  { id:"cool", label:"Cool", css:"saturate(1.05) contrast(1.02) hue-rotate(12deg)" },
  { id:"mono", label:"Mono", css:"grayscale(1) contrast(1.05)" },
  { id:"vivid", label:"Vivid", css:"saturate(1.35) contrast(1.08)" },
  { id:"noir", label:"Noir", css:"grayscale(0.82) contrast(1.2) brightness(0.92)" },
  { id:"vintage", label:"Vintage", css:"sepia(0.45) contrast(1.06) brightness(1.02)" }
];
const STORY_FILTER_BY_ID = STORY_FILTERS.reduce((acc, item) => {
  acc[item.id] = item.css;
  return acc;
}, {});
const POST_FILTERS = [
  { id:"none", label:"Original", css:"none" },
  { id:"cinematic", label:"Cinematic", css:"contrast(1.12) saturate(1.18) brightness(1.03)" },
  { id:"sunset", label:"Sunset", css:"hue-rotate(-10deg) saturate(1.22) brightness(1.04)" },
  { id:"neon", label:"Neon", css:"saturate(1.45) contrast(1.16) hue-rotate(14deg)" },
  { id:"noir", label:"Noir", css:"grayscale(0.9) contrast(1.22) brightness(0.93)" },
  { id:"pastel", label:"Pastel", css:"saturate(0.88) brightness(1.14) contrast(0.95)" },
  { id:"retro", label:"Retro", css:"sepia(0.35) saturate(1.08) contrast(1.08)" },
  { id:"cold", label:"Cold", css:"hue-rotate(24deg) saturate(0.92) contrast(1.08)" },
  { id:"warm", label:"Warm", css:"hue-rotate(-8deg) saturate(1.18) contrast(1.06)" },
  { id:"dream", label:"Dream", css:"brightness(1.1) saturate(1.06) blur(0.2px)" },
  { id:"crisp", label:"Crisp", css:"contrast(1.2) saturate(1.2)" },
  { id:"dramatic", label:"Dramatic", css:"contrast(1.34) brightness(0.9) saturate(1.15)" }
];
const POST_FILTER_BY_ID = POST_FILTERS.reduce((acc, item) => {
  acc[item.id] = item.css;
  return acc;
}, {});
const storyComposerState = {
  open: false,
  busy: false,
  items: [],
  activeIndex: 0
};
const postComposerState = {
  open: false,
  busy: false,
  items: [],
  activeIndex: 0,
  defaultFilter: "none",
  title: "",
  description: "",
  keywords: "",
  textMode: false,
  textContent: "",
  music: null
};
const storyPlaybackState = {
  list: [],
  index: 0,
  ownerId: "",
  ownerName: "",
  ownerAvatar: "",
  viewersOpen: false
};
let storyAdvanceTimer = null;
let storyProgressTimer = null;
let storyGroupsByUser = new Map();
let storyAutoRefreshTimer = null;
let toastTimer = null;

function getSupabaseErrorText(error){
  return [error?.message, error?.details, error?.hint]
    .filter(Boolean)
    .join(" ");
}

function setFeedNotice(text){
  if(feedNotice){
    feedNotice.hidden = true;
    feedNotice.textContent = "";
  }
  if(text){
    console.warn(String(text));
  }
}

function showToast(text){
  const msg = String(text || "").trim();
  if(!msg) return;
  if(!appToast){
    console.log(msg);
    return;
  }
  appToast.textContent = msg;
  appToast.classList.add("show");
  if(toastTimer){
    clearTimeout(toastTimer);
  }
  toastTimer = setTimeout(() => {
    appToast.classList.remove("show");
  }, 2400);
}

function buildAvatarDataUri(label){
  const rawInitial = String(label || "U").trim();
  const initial = (rawInitial || "U").slice(0, 1).toUpperCase();
  const safeInitial = escapeHtml(initial);
  const svg = `
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 96 96">
      <defs>
        <linearGradient id="g" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%" stop-color="#ff6a00"/>
          <stop offset="100%" stop-color="#ff2d55"/>
        </linearGradient>
      </defs>
      <rect width="96" height="96" fill="url(#g)"/>
      <text x="50%" y="54%" text-anchor="middle" dominant-baseline="middle" fill="#fff" font-family="Arial, sans-serif" font-size="42" font-weight="700">${safeInitial}</text>
    </svg>
  `;
  return "data:image/svg+xml;charset=UTF-8," + encodeURIComponent(svg);
}

function resolveAvatarUrl(url, label){
  const clean = String(url || "").trim();
  if(clean) return clean;
  return buildAvatarDataUri(label || "U") || DEFAULT_AVATAR_DATA_URI;
}

function isPermissionDeniedError(error){
  const code = String(error?.code || "").toLowerCase();
  if(code === "42501" || code === "403" || code === "401") return true;
  const text = getSupabaseErrorText(error).toLowerCase();
  return text.includes("permission denied")
    || text.includes("row-level security")
    || text.includes("not authorized")
    || text.includes("new row violates row-level security");
}

function showFeedPermissionAlert(error){
  if(feedPermissionAlerted || !isPermissionDeniedError(error)) return;
  feedPermissionAlerted = true;
  setFeedNotice("Public feed access blocked by Supabase policy. Run sql/fix_public_feed_visibility.sql in Supabase SQL Editor, then reload.");
  showToast("Feed access blocked by policy. Run SQL visibility fix.");
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

function getLocalProfileOverride(id){
  try{
    const raw = localStorage.getItem("m_profile_" + id);
    return raw ? JSON.parse(raw) : null;
  }catch(_){
    return null;
  }
}

function escapeHtml(value){
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function fromBase64Utf8(value){
  try{
    const bytes = atob(String(value || ""));
    const encoded = Array.from(bytes, ch => {
      return "%" + ch.charCodeAt(0).toString(16).padStart(2, "0");
    }).join("");
    return decodeURIComponent(encoded);
  }catch(_){
    return "";
  }
}

function parseMusicFromKeywords(rawKeywords){
  const parsed = parsePostKeywordPayload(rawKeywords);
  return {
    plainKeywords: parsed.plainKeywords,
    music: parsed.music,
    postMeta: parsed.postMeta
  };
}

function normalizePostFilter(filterId){
  const key = String(filterId || "none").toLowerCase();
  return POST_FILTER_BY_ID[key] ? key : "none";
}

function getPostFilterCss(filterId){
  return POST_FILTER_BY_ID[normalizePostFilter(filterId)] || "none";
}

function parsePostKeywordPayload(rawKeywords){
  const lines = String(rawKeywords || "")
    .split(/\r?\n/)
    .map(line => String(line || "").trim())
    .filter(Boolean);

  let music = null;
  let postMeta = { filter:"none", textMode:false };
  const plainParts = [];

  lines.forEach(line => {
    if(line.startsWith(MUSIC_MARKER + ":")){
      const encoded = line.slice((MUSIC_MARKER + ":").length).trim();
      const decoded = fromBase64Utf8(encoded);
      if(decoded){
        try{
          const parsed = JSON.parse(decoded);
          if(parsed && parsed.url){
            music = {
              url: String(parsed.url || ""),
              title: String(parsed.title || "Original Audio"),
              artist: String(parsed.artist || "")
            };
          }
        }catch(_){ }
      }
      return;
    }

    if(line.startsWith(POST_META_MARKER + ":")){
      const encoded = line.slice((POST_META_MARKER + ":").length).trim();
      const decoded = fromBase64Utf8(encoded);
      if(decoded){
        try{
          const parsed = JSON.parse(decoded);
          postMeta = {
            filter: normalizePostFilter(parsed?.filter),
            textMode: !!parsed?.textMode
          };
        }catch(_){ }
      }
      return;
    }

    plainParts.push(line);
  });

  return {
    plainKeywords: plainParts.join("\n").trim(),
    music,
    postMeta
  };
}

function buildPostKeywords(rawKeywords, musicMeta, postMeta){
  const payload = [];
  const base = String(rawKeywords || "").trim();
  if(base){
    payload.push(base);
  }

  if(musicMeta && musicMeta.url){
    payload.push(MUSIC_MARKER + ":" + toBase64Utf8(JSON.stringify({
      url: String(musicMeta.url || ""),
      title: String(musicMeta.title || ""),
      artist: String(musicMeta.artist || "")
    })));
  }

  payload.push(POST_META_MARKER + ":" + toBase64Utf8(JSON.stringify({
    filter: normalizePostFilter(postMeta?.filter),
    textMode: !!postMeta?.textMode
  })));

  return payload.join("\n");
}

function normalizeStoryFilter(filterId){
  const key = String(filterId || "none").toLowerCase();
  return STORY_FILTER_BY_ID[key] ? key : "none";
}

function getStoryFilterCss(filterId){
  return STORY_FILTER_BY_ID[normalizeStoryFilter(filterId)] || "none";
}

function parseStoryMetaFromKeywords(rawKeywords){
  const text = String(rawKeywords || "");
  const marker = STORY_META_MARKER + ":";
  const markerIndex = text.lastIndexOf(marker);
  if(markerIndex < 0){
    return {
      plainKeywords: text.trim(),
      meta: { filter:"none", info:"", music:null }
    };
  }

  const plainKeywords = text.slice(0, markerIndex).trim();
  const encoded = text.slice(markerIndex + marker.length).trim();
  if(!encoded){
    return {
      plainKeywords,
      meta: { filter:"none", info:"", music:null }
    };
  }

  const decoded = fromBase64Utf8(encoded);
  if(!decoded){
    return {
      plainKeywords,
      meta: { filter:"none", info:"", music:null }
    };
  }

  try{
    const parsed = JSON.parse(decoded);
    const filter = normalizeStoryFilter(parsed?.filter);
    const info = String(parsed?.info || "");
    const music = parsed?.music && parsed.music.url
      ? {
        url: String(parsed.music.url || ""),
        title: String(parsed.music.title || "Original Audio"),
        artist: String(parsed.music.artist || "")
      }
      : null;
    return { plainKeywords, meta: { filter, info, music } };
  }catch(_){
    return {
      plainKeywords,
      meta: { filter:"none", info:"", music:null }
    };
  }
}

function buildStoryKeywords(meta){
  const payload = {
    filter: normalizeStoryFilter(meta?.filter),
    info: String(meta?.info || ""),
    music: meta?.music && meta.music.url
      ? {
        url: String(meta.music.url || ""),
        title: String(meta.music.title || "Original Audio"),
        artist: String(meta.music.artist || "")
      }
      : null
  };
  const encoded = toBase64Utf8(JSON.stringify(payload));
  return STORY_META_MARKER + ":" + encoded;
}

function hydrateStoryRow(row){
  const parsed = parseStoryMetaFromKeywords(row?.keywords || "");
  return {
    ...(row || {}),
    caption: String(row?.title || ""),
    info: parsed.meta.info || String(row?.description || ""),
    filter: normalizeStoryFilter(parsed.meta.filter),
    music: parsed.meta.music || null,
    plainKeywords: String(parsed.plainKeywords || "")
  };
}

function getShareUrlForItem(item){
  const shareUrl = new URL("post.html", location.href);
  if(item?.user_id) shareUrl.searchParams.set("uid", item.user_id);
  shareUrl.searchParams.set("focus", item?.id || "");
  return shareUrl.href;
}

function formatRelativeTime(value){
  const ts = Date.parse(String(value || ""));
  if(!Number.isFinite(ts)) return "";
  const diff = Math.max(1, Math.floor((Date.now() - ts) / 1000));
  if(diff < 60) return diff + "s";
  if(diff < 3600) return Math.floor(diff / 60) + "m";
  if(diff < 86400) return Math.floor(diff / 3600) + "h";
  if(diff < 604800) return Math.floor(diff / 86400) + "d";
  const d = new Date(ts);
  return d.toLocaleDateString(undefined, { month:"short", day:"numeric" });
}

function formatCount(value){
  const n = Math.max(0, Number(value) || 0);
  return new Intl.NumberFormat("en-US").format(n);
}

function getLocalShareCount(targetType, targetId){
  let type = String(targetType || "post");
  let id = String(targetId || "");
  if(!id){
    id = type;
    type = "post";
  }
  if(!id) return 0;
  const key = SHARE_COUNT_PREFIX + type + ":" + id;
  const raw = localStorage.getItem(key);
  return Math.max(0, Number(raw) || 0);
}

function incrementLocalShareCount(targetType, targetId){
  let type = String(targetType || "post");
  let id = String(targetId || "");
  if(!id){
    id = type;
    type = "post";
  }
  if(!id) return 0;
  const next = getLocalShareCount(type, id) + 1;
  localStorage.setItem(SHARE_COUNT_PREFIX + type + ":" + id, String(next));
  return next;
}

async function getShareCount(targetType, targetId){
  let type = String(targetType || "post");
  let id = String(targetId || "");
  if(!id){
    id = type;
    type = "post";
  }
  if(!id) return 0;
  if(window.NOVA && typeof window.NOVA.getShareCount === "function"){
    try{
      const value = await window.NOVA.getShareCount(type, id);
      return Math.max(0, Number(value) || 0);
    }catch(err){
      console.error("Share count read failed", err);
    }
  }
  return getLocalShareCount(type, id);
}

async function trackShare(targetType, targetId, channel){
  let type = String(targetType || "post");
  let id = String(targetId || "");
  if(!id){
    id = type;
    type = "post";
  }
  if(!id) return { ok:false, count:0 };

  if(window.NOVA && typeof window.NOVA.trackShare === "function"){
    try{
      const res = await window.NOVA.trackShare(type, id, channel || "share");
      if(res?.ok){
        const latest = await getShareCount(type, id);
        return { ok:true, count:latest };
      }
      return { ok:false, count:await getShareCount(type, id), error:res?.error || null };
    }catch(err){
      console.error("Share track failed", err);
      return { ok:false, count:await getShareCount(type, id), error:err };
    }
  }

  const next = incrementLocalShareCount(type, id);
  return { ok:true, count:next };
}

function registerPostCountRefs(postId, ownerUserId, targetType, refs){
  const id = String(postId || "");
  if(!id) return;
  postCountRefs.set(id, {
    ownerUserId: String(ownerUserId || ""),
    targetType: String(targetType || "post"),
    likeEl: refs?.likeEl || null,
    dislikeEl: refs?.dislikeEl || null,
    commentEl: refs?.commentEl || null,
    shareCountEl: refs?.shareCountEl || null,
    likeBtn: refs?.likeBtn || null,
    dislikeBtn: refs?.dislikeBtn || null
  });
}

async function refreshPostCounts(postId){
  const id = String(postId || "");
  if(!id || !postCountRefs.has(id)) return;
  const refs = postCountRefs.get(id);
  if(!refs) return;

  if(refs.likeEl && !document.body.contains(refs.likeEl)){
    postCountRefs.delete(id);
    pendingPostCountRefresh.delete(id);
    return;
  }

  const [counts, comments, shares] = await Promise.all([
    window.NOVA.getReactionCounts(refs.targetType || "post", id).catch(() => ({ likes:0, dislikes:0 })),
    window.NOVA.getCommentCount(refs.targetType || "post", id, {
      ownerUserId: refs.ownerUserId || ""
    }).catch(() => 0),
    getShareCount(refs.targetType || "post", id).catch(() => 0)
  ]);

  if(refs.likeEl){
    refs.likeEl.textContent = formatCount(counts.likes);
  }
  if(refs.dislikeEl){
    refs.dislikeEl.textContent = formatCount(counts.dislikes);
  }
  if(refs.commentEl){
    refs.commentEl.textContent = formatCount(comments);
  }
  if(refs.shareCountEl){
    refs.shareCountEl.textContent = formatCount(shares);
  }
}

function queuePostCountRefresh(postId){
  const id = String(postId || "");
  if(!id || !postCountRefs.has(id)) return;
  if(pendingPostCountRefresh.has(id)) return pendingPostCountRefresh.get(id);

  const task = (async() => {
    await refreshPostCounts(id);
  })();

  pendingPostCountRefresh.set(id, task);
  task.finally(() => {
    if(pendingPostCountRefresh.get(id) === task){
      pendingPostCountRefresh.delete(id);
    }
  });
  return task;
}

function startCountPolling(){
  if(countPollTimer) return;
  countPollTimer = setInterval(() => {
    if(document.hidden) return;
    postCountRefs.forEach((_refs, postId) => {
      queuePostCountRefresh(postId);
    });
  }, COUNT_POLL_INTERVAL_MS);
}

function getChangeField(payload, keys){
  const next = payload?.new || {};
  const prev = payload?.old || {};
  for(const key of keys){
    if(next && next[key] !== undefined && next[key] !== null){
      return next[key];
    }
    if(prev && prev[key] !== undefined && prev[key] !== null){
      return prev[key];
    }
  }
  return "";
}

function getChangeTargetType(payload){
  return String(getChangeField(payload, ["target_type", "type"]) || "").toLowerCase();
}

function getChangeTargetId(payload){
  return String(getChangeField(payload, ["target_id", "post_id"]) || "");
}

function matchesFeedUser(userId){
  if(!feedUserId) return true;
  return String(userId || "") === String(feedUserId || "");
}

function resetFeedAndReload(){
  feedVersion += 1;
  renderedPostIds.clear();
  postCountRefs.clear();
  pendingPostCountRefresh.clear();
  page = 0;
  loading = false;
  done = false;
  focusApplied = false;
  feed.innerHTML = "";
  if(!feedReady) return;
  ensureFocusPostLoaded().then(() => loadMore());
}

function scheduleFeedReload(){
  if(feedReloadTimer){
    clearTimeout(feedReloadTimer);
  }
  feedReloadTimer = setTimeout(() => {
    resetFeedAndReload();
  }, 450);
}

async function initFeedRealtime(){
  if(feedRealtimeChannel || !window.NOVA?.supa) return;

  feedRealtimeChannel = window.NOVA.supa.channel("post-feed-live");

  feedRealtimeChannel.on("postgres_changes", {
    event: "*",
    schema: "public",
    table: "reactions"
  }, payload => {
    const id = getChangeTargetId(payload);
    if(!id || !postCountRefs.has(id)) return;
    queuePostCountRefresh(id);
  });

  feedRealtimeChannel.on("postgres_changes", {
    event: "*",
    schema: "public",
    table: "comments"
  }, payload => {
    const type = getChangeTargetType(payload);
    const id = getChangeTargetId(payload);
    if(id && postCountRefs.has(id)){
      queuePostCountRefresh(id);
    }
    if(
      id &&
      id === activeCommentPostId &&
      commentsPanel.classList.contains("show") &&
      (!type || type === activeCommentTargetType)
    ){
      loadComments(id);
    }
  });

  feedRealtimeChannel.on("postgres_changes", {
    event: "INSERT",
    schema: "public",
    table: "posts"
  }, payload => {
    const row = payload?.new || {};
    if(!row?.id || !matchesFeedUser(row.user_id)) return;
    scheduleFeedReload();
  });

  feedRealtimeChannel.subscribe(status => {
    if(status === "CHANNEL_ERROR"){
      console.error("Realtime subscription error for post feed");
    }
  });

  if(window.NOVA && typeof window.NOVA.getShareCount === "function"){
    shareRealtimeChannel = window.NOVA.supa.channel("post-share-live");
    shareRealtimeChannel.on("postgres_changes", {
      event: "*",
      schema: "public",
      table: "post_shares"
    }, payload => {
      if(getChangeTargetType(payload) !== "post") return;
      const id = getChangeTargetId(payload);
      queuePostCountRefresh(id);
    });
    shareRealtimeChannel.subscribe();
  }

  window.addEventListener("beforeunload", () => {
    if(window.NOVA?.supa && feedRealtimeChannel){
      try{ window.NOVA.supa.removeChannel(feedRealtimeChannel); }catch(_){}
    }
    if(window.NOVA?.supa && shareRealtimeChannel){
      try{ window.NOVA.supa.removeChannel(shareRealtimeChannel); }catch(_){}
    }
    if(storyAutoRefreshTimer){
      clearInterval(storyAutoRefreshTimer);
      storyAutoRefreshTimer = null;
    }
    if(toastTimer){
      clearTimeout(toastTimer);
      toastTimer = null;
    }
  }, { once:true });
}

function getSavedPostIds(){
  try{
    const raw = localStorage.getItem(SAVED_POST_IDS_KEY);
    const list = raw ? JSON.parse(raw) : [];
    if(!Array.isArray(list)) return [];
    return list.map(id => String(id || "")).filter(Boolean);
  }catch(_){
    return [];
  }
}

function isPostSaved(postId){
  const id = String(postId || "");
  if(!id) return false;
  const list = getSavedPostIds();
  return list.includes(id);
}

function toggleSavePost(postId){
  const id = String(postId || "");
  if(!id) return false;
  const list = getSavedPostIds();
  const exists = list.includes(id);
  const next = exists ? list.filter(item => item !== id) : [id, ...list];
  localStorage.setItem(SAVED_POST_IDS_KEY, JSON.stringify(next));
  return !exists;
}

function applySaveUi(saved, saveBtn){
  if(!saveBtn) return;
  saveBtn.classList.toggle("active-save", !!saved);
}

async function getPostAuthorInfo(userId){
  const id = String(userId || "");
  if(!id){
    return { name:"User", avatarUrl:resolveAvatarUrl("", "User") };
  }

  if(!storyProfileCache[id]){
    await loadStoryProfiles([id]);
  }
  const info = storyProfileCache[id] || {};
  const resolvedName = info.name && info.name !== "Story" ? info.name : "User";
  return {
    name: resolvedName,
    avatarUrl: resolveAvatarUrl(info.avatarUrl, resolvedName)
  };
}

function toBase64Utf8(value){
  const utf8 = encodeURIComponent(String(value || "")).replace(/%([0-9A-F]{2})/g, (_, hex) => {
    return String.fromCharCode(parseInt(hex, 16));
  });
  return btoa(utf8);
}

async function searchMusicMeta(query){
  const q = String(query || "").trim();
  if(!q) return [];
  const url = "https://itunes.apple.com/search?media=music&entity=song&limit=12&term=" + encodeURIComponent(q);
  const res = await fetch(url);
  if(!res.ok) throw new Error("music_search_failed");
  const payload = await res.json();
  return (Array.isArray(payload?.results) ? payload.results : [])
    .filter(row => row && row.previewUrl)
    .map(row => ({
      title: String(row.trackName || "Unknown"),
      artist: String(row.artistName || "Unknown Artist"),
      url: String(row.previewUrl || "")
    }));
}

async function pickStoryMusicMeta(){
  const query = prompt("Search story music (song or artist)");
  if(!query || !query.trim()) return null;
  let results = [];
  try{
    results = await searchMusicMeta(query);
  }catch(_){
    showToast("Music search failed.");
    return null;
  }
  if(!results.length){
    showToast("No music found.");
    return null;
  }
  const list = results.map((row, idx) => `${idx + 1}. ${row.title} - ${row.artist}`).join("\n");
  const selectedRaw = prompt("Select music number:\n" + list, "1");
  const selectedIndex = Math.max(1, Math.min(results.length, Number(selectedRaw) || 1)) - 1;
  return results[selectedIndex];
}

async function insertRowWithFallback(table, payload, requiredColumns){
  const row = { ...(payload || {}) };
  const required = new Set(requiredColumns || []);

  while(true){
    const { error } = await window.NOVA.supa.from(table).insert(row);
    if(!error){
      return { error: null };
    }

    const missingColumn = getMissingColumnName(error);
    if(
      !missingColumn ||
      !Object.prototype.hasOwnProperty.call(row, missingColumn) ||
      required.has(missingColumn)
    ){
      return { error };
    }

    delete row[missingColumn];
  }
}

function withPostPublicCompat(payload){
  return {
    ...(payload || {}),
    ...POST_PUBLIC_COMPAT_PAYLOAD
  };
}

async function normalizeMyPostsPublicVisibility(){
  if(!user?.id) return;

  const entries = Object.entries(POST_PUBLIC_COMPAT_PAYLOAD);
  for(const [column, value] of entries){
    const patch = { [column]: value };
    try{
      const { error } = await window.NOVA.supa
        .from("posts")
        .update(patch)
        .eq("user_id", user.id);
      if(!error) continue;

      const missingColumn = getMissingColumnName(error);
      if(missingColumn && missingColumn === String(column).toLowerCase()){
        continue;
      }
    }catch(_){
      // Ignore compat failures; visibility columns vary by deployment.
    }
  }
}

function createPostBatchId(){
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
}

function getFileExt(file){
  const rawName = String(file?.name || "file");
  const ext = rawName.includes(".") ? rawName.split(".").pop() : "";
  return (ext || "bin").toLowerCase();
}

function makePostBatchPath(userId, file, batchId, index){
  const safeUser = String(userId || "user");
  const ext = getFileExt(file);
  const rand = Math.random().toString(36).slice(2, 7);
  return `${safeUser}/post-${batchId}-${index}-${rand}.${ext}`;
}

function extractPostBatchId(url){
  const text = String(url || "");
  const match = text.match(/\/post-([a-z0-9]+)-\d+-/i);
  return match && match[1] ? match[1] : "";
}

function extractPostBatchIndex(url){
  const text = String(url || "");
  const match = text.match(/\/post-[a-z0-9]+-(\d+)-/i);
  if(!match || !match[1]) return null;
  const idx = Number(match[1]);
  return Number.isFinite(idx) ? idx : null;
}

function mediaTypeFromUrl(url){
  return /\.(mp4|webm|ogg|mov|m4v|avi)(\?|#|$)/i.test(String(url || ""))
    ? "video"
    : "image";
}

async function runPostsQuery(options){
  let query = window.NOVA.supa
    .from("posts")
    .select(options.fields.join(","))
    .range(options.from, options.to);

  if(feedUserId){
    query = query.eq("user_id", feedUserId);
  }
  if(options.useOrder){
    query = query.order("created_at", { ascending:false });
  }
  return query;
}

function groupPostRows(rows){
  const ordered = [];
  const groups = new Map();
  (rows || []).forEach(row => {
    if(!row || !row.id || !row.media_url) return;

    const batchId = extractPostBatchId(row.media_url);
    const batchIndex = extractPostBatchIndex(row.media_url);
    const key = batchId
      ? `batch:${row.user_id || "u"}:${batchId}`
      : `single:${row.id}`;

    if(!groups.has(key)){
      const firstMediaType = row.media_type || mediaTypeFromUrl(row.media_url);
      const keywordParse = parseMusicFromKeywords(row.keywords);
      const grouped = {
        id: row.id,
        post_ids: [row.id],
        group_key: key,
        batch_id: batchId || "",
        user_id: row.user_id,
        title: row.title || null,
        description: row.description || null,
        keywords: keywordParse.plainKeywords || "",
        music: keywordParse.music || null,
        filter: normalizePostFilter(keywordParse?.postMeta?.filter || "none"),
        text_mode: !!keywordParse?.postMeta?.textMode,
        created_at: row.created_at || null,
        media_url: row.media_url,
        media_type: firstMediaType,
        media_urls: [row.media_url],
        media_types: [firstMediaType],
        media_rows: [{
          post_id: row.id,
          media_url: row.media_url,
          media_type: firstMediaType,
          batch_index: batchIndex
        }]
      };
      groups.set(key, grouped);
      ordered.push(grouped);
      return;
    }

    const grouped = groups.get(key);
    grouped.post_ids.push(row.id);
    grouped.media_urls.push(row.media_url);
    grouped.media_types.push(row.media_type || mediaTypeFromUrl(row.media_url));
    grouped.media_rows.push({
      post_id: row.id,
      media_url: row.media_url,
      media_type: row.media_type || mediaTypeFromUrl(row.media_url),
      batch_index: batchIndex
    });
    if(!grouped.title && row.title) grouped.title = row.title;
    if(!grouped.description && row.description) grouped.description = row.description;
    if(!grouped.keywords && row.keywords){
      grouped.keywords = parseMusicFromKeywords(row.keywords).plainKeywords || "";
    }
    if(!grouped.music && row.keywords){
      grouped.music = parseMusicFromKeywords(row.keywords).music || null;
    }
    if((!grouped.filter || grouped.filter === "none") && row.keywords){
      grouped.filter = normalizePostFilter(parseMusicFromKeywords(row.keywords)?.postMeta?.filter || "none");
    }
    if(!grouped.text_mode && row.keywords){
      grouped.text_mode = !!parseMusicFromKeywords(row.keywords)?.postMeta?.textMode;
    }
  });

  ordered.forEach(grouped => {
    if(!Array.isArray(grouped.media_rows) || !grouped.media_rows.length){
      return;
    }

    if(grouped.batch_id){
      grouped.media_rows.sort((a, b) => {
        const aIndex = Number.isFinite(a.batch_index) ? a.batch_index : 0;
        const bIndex = Number.isFinite(b.batch_index) ? b.batch_index : 0;
        return aIndex - bIndex;
      });
    }

    grouped.post_ids = grouped.media_rows.map(entry => entry.post_id);
    grouped.media_urls = grouped.media_rows.map(entry => entry.media_url);
    grouped.media_types = grouped.media_rows.map(entry => entry.media_type);
    grouped.media_url = grouped.media_urls[0] || grouped.media_url;
    grouped.media_type = grouped.media_types[0] || grouped.media_type;
    delete grouped.media_rows;
  });

  return ordered;
}

async function fetchPostsPage(from, to){
  const queryPlans = [
    { fields:["id","user_id","media_url","media_type","title","description","keywords","created_at"], useOrder:true, from, to },
    { fields:["id","user_id","media_url","media_type","title","description","created_at"], useOrder:true, from, to },
    { fields:["id","user_id","media_url","media_type","created_at"], useOrder:true, from, to },
    { fields:["id","user_id","media_url","created_at"], useOrder:true, from, to },
    { fields:["id","user_id","media_url"], useOrder:false, from, to },
    { fields:["id","media_url"], useOrder:false, from, to }
  ];

  let lastError = null;
  for(const plan of queryPlans){
    const { data, error } = await runPostsQuery(plan);
    if(!error){
      return { data: groupPostRows(data), error:null };
    }
    lastError = error;
  }

  return { data: [], error: lastError };
}

async function fetchPostByIdWithFallback(postId){
  const queryPlans = [
    ["id","user_id","media_url","media_type","title","description","keywords","created_at"],
    ["id","user_id","media_url","media_type","title","description","created_at"],
    ["id","user_id","media_url","media_type","created_at"],
    ["id","user_id","media_url","created_at"],
    ["id","user_id","media_url"],
    ["id","media_url"]
  ];
  let lastError = null;

  for(const fields of queryPlans){
    let query = window.NOVA.supa
      .from("posts")
      .select(fields.join(","))
      .eq("id", postId)
      .maybeSingle();
    if(feedUserId){
      query = query.eq("user_id", feedUserId);
    }
    const { data, error } = await query;
    if(!error && data){
      const grouped = groupPostRows([data]);
      return { data: grouped[0] || null, error:null };
    }
    lastError = error;
  }

  return { data:null, error:lastError };
}

async function loadStoryProfiles(userIds){
  const ids = [...new Set((userIds || []).map(id => String(id || "")).filter(Boolean))];
  const profileMap = {};

  ids.forEach(id => {
    profileMap[id] = { name: "Story", avatarUrl: "" };
  });

  if(ids.length){
    try{
      const { data } = await window.NOVA.supa
        .from("users")
        .select("user_id,username,full_name,photo")
        .in("user_id", ids);
      (data || []).forEach(row => {
        const id = String(row?.user_id || "");
        if(!id) return;
        profileMap[id] = {
          name: row?.full_name || row?.username || "Story",
          avatarUrl: row?.photo || ""
        };
      });
    }catch(_){}
  }

  ids.forEach(id => {
    const local = getLocalProfileOverride(id);
    if(local){
      const current = profileMap[id] || { name: "Story", avatarUrl: "" };
      if(local.displayName) current.name = local.displayName;
      else if(local.username && (!current.name || current.name === "Story")){
        current.name = local.username;
      }
      if(local.avatarData) current.avatarUrl = local.avatarData;
      profileMap[id] = current;
    }
    storyProfileCache[id] = profileMap[id];
  });

  return profileMap;
}

async function hydrateMyStoryTile(){
  if(!user) return;
  const id = String(user.id || "");
  if(!id) return;

  let profile = storyProfileCache[id];
  if(!profile){
    const profileMap = await loadStoryProfiles([id]);
    profile = profileMap[id];
  }

  if(myStoryAvatar){
    myStoryAvatar.src = resolveAvatarUrl(profile?.avatarUrl, profile?.name || "You");
  }
  if(myStoryLabel){
    myStoryLabel.textContent = "You";
  }
}

async function waitForNovaReady(timeoutMs){
  const timeout = Math.max(1000, Number(timeoutMs) || 9000);
  const startedAt = Date.now();
  while((Date.now() - startedAt) < timeout){
    if(window.NOVA && typeof window.NOVA.requireUser === "function"){
      return true;
    }
    await new Promise(resolve => setTimeout(resolve, 90));
  }
  return false;
}

(async()=>{
  const ready = await waitForNovaReady(10000);
  if(!ready){
    showToast("Backend/Supabase config not ready. Start server and refresh.");
    return;
  }
  user = await window.NOVA.requireUser();
  await normalizeMyPostsPublicVisibility();
  await hydrateMyStoryTile();
  await loadStories();
  if(!storyAutoRefreshTimer){
    storyAutoRefreshTimer = setInterval(() => {
      loadStories();
    }, 60000);
  }
  await initFeedRealtime();
  startCountPolling();
  feedReady = true;
  await ensureFocusPostLoaded();
  await loadMore();

  if(params.get("create") === "1"){
    if(params.get("type") === "story") addStory();
    if(params.get("type") === "post") addPost();
  }
})();

function addStory(){
  storyInput.value = "";
  storyInput.click();
}

function addPost(){
  openPostComposer();
}

function isStoryFile(file){
  const type = String(file?.type || "").toLowerCase();
  return type.startsWith("image/") || type.startsWith("video/");
}

function createStoryDraft(file){
  const mediaType = String(file?.type || "").toLowerCase().startsWith("video/")
    ? "video"
    : "image";
  return {
    id: "story_" + Date.now().toString(36) + "_" + Math.random().toString(36).slice(2, 7),
    file,
    mediaType,
    previewUrl: URL.createObjectURL(file),
    caption: "",
    info: "",
    filter: "none",
    music: null
  };
}

function releaseStoryDraft(item){
  if(!item || !item.previewUrl) return;
  if(String(item.previewUrl).startsWith("blob:")){
    try{ URL.revokeObjectURL(item.previewUrl); }catch(_){}
  }
}

function clearStoryComposerState(){
  (storyComposerState.items || []).forEach(item => releaseStoryDraft(item));
  storyComposerState.items = [];
  storyComposerState.activeIndex = 0;
  storyComposerState.busy = false;
  storyComposerState.open = false;
  storyPlaybackState.viewersOpen = false;
}

function setStoryComposerStatus(text){
  if(!storyComposerStatus) return;
  storyComposerStatus.textContent = text ? String(text) : "";
}

function getActiveStoryDraft(){
  if(!storyComposerState.items.length) return null;
  const safeIndex = Math.max(0, Math.min(storyComposerState.items.length - 1, storyComposerState.activeIndex));
  storyComposerState.activeIndex = safeIndex;
  return storyComposerState.items[safeIndex] || null;
}

function renderStoryComposerFilters(activeFilter){
  if(!storyFilterList) return;
  storyFilterList.innerHTML = STORY_FILTERS.map(item => {
    const active = normalizeStoryFilter(activeFilter) === item.id ? "active" : "";
    return `<button type="button" class="story-filter-chip ${active}" data-story-filter="${item.id}">${escapeHtml(item.label)}</button>`;
  }).join("");
}

function renderStoryComposerThumbs(){
  if(!storyThumbList) return;
  storyThumbList.innerHTML = "";
  storyComposerState.items.forEach((item, index) => {
    const el = document.createElement("button");
    el.type = "button";
    el.className = "story-thumb" + (index === storyComposerState.activeIndex ? " active" : "");
    el.dataset.storyThumb = String(index);
    el.innerHTML = item.mediaType === "video"
      ? `<video src="${item.previewUrl}" muted playsinline preload="metadata"></video>`
      : `<img src="${item.previewUrl}" alt="">`;
    storyThumbList.appendChild(el);
  });
}

function renderStoryComposerPreview(item){
  if(!storyComposerMedia) return;
  if(!item){
    storyComposerMedia.innerHTML = "";
    return;
  }
  const style = escapeHtml(`filter:${getStoryFilterCss(item.filter)};`);
  storyComposerMedia.innerHTML = item.mediaType === "video"
    ? `<video src="${item.previewUrl}" autoplay loop muted playsinline style="${style}"></video>`
    : `<img src="${item.previewUrl}" alt="" style="${style}">`;
}

function renderStoryComposerForm(item){
  if(!item) return;
  if(storyCaptionInput) storyCaptionInput.value = item.caption || "";
  if(storyInfoInput) storyInfoInput.value = item.info || "";
  if(storyMusicSelected){
    if(item.music && item.music.url){
      const label = item.music.artist
        ? `${item.music.title} - ${item.music.artist}`
        : item.music.title;
      storyMusicSelected.textContent = label;
    }else{
      storyMusicSelected.textContent = "No music selected";
    }
  }
}

function renderStoryComposer(){
  if(!storyComposerState.open){
    if(storyComposer){
      storyComposer.classList.remove("active");
    }
    return;
  }
  if(storyComposer){
    storyComposer.classList.add("active");
  }
  const active = getActiveStoryDraft();
  renderStoryComposerPreview(active);
  renderStoryComposerFilters(active?.filter || "none");
  renderStoryComposerForm(active);
  renderStoryComposerThumbs();
}

function openStoryComposer(){
  storyComposerState.open = true;
  renderStoryComposer();
  syncOverlayScrollLock();
}

function closeStoryComposer(){
  clearStoryComposerState();
  if(storyComposer){
    storyComposer.classList.remove("active");
  }
  if(storyComposerMedia){
    storyComposerMedia.innerHTML = "";
  }
  if(storyThumbList){
    storyThumbList.innerHTML = "";
  }
  setStoryComposerStatus("");
  syncOverlayScrollLock();
}

function addStoryDraftFiles(files){
  const accepted = (files || []).filter(file => isStoryFile(file));
  if(!accepted.length){
    showToast("Only image/video files are allowed for story.");
    return;
  }

  accepted.forEach(file => {
    storyComposerState.items.push(createStoryDraft(file));
  });
  if(!storyComposerState.open){
    openStoryComposer();
  }
  storyComposerState.activeIndex = Math.max(0, storyComposerState.items.length - accepted.length);
  setStoryComposerStatus(`${storyComposerState.items.length} media selected`);
  renderStoryComposer();
}

storyInput.onchange = e => {
  const files = Array.from(e.target.files || []);
  if(!files.length) return;
  addStoryDraftFiles(files);
  storyInput.value = "";
};

async function publishStorySequence(){
  if(storyComposerState.busy) return;
  const items = storyComposerState.items || [];
  if(!items.length){
    setStoryComposerStatus("Select media first");
    return;
  }
  storyComposerState.busy = true;
  if(storyPublishBtn) storyPublishBtn.disabled = true;
  if(storyAddMoreBtn) storyAddMoreBtn.disabled = true;

  let uploaded = 0;
  let permissionBlocked = false;

  for(let index = 0; index < items.length; index += 1){
    const item = items[index];
    if(!item?.file) continue;

    setStoryComposerStatus(`Uploading ${index + 1}/${items.length}...`);
    let mediaUrl = "";
    try{
      const path = window.NOVA.makePath(user.id, item.file);
      mediaUrl = await window.NOVA.uploadToBucket("stories", item.file, path);
    }catch(uploadErr){
      console.error("Story media upload failed", uploadErr);
      continue;
    }

    const expiresAt = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
    const keywordsPayload = buildStoryKeywords({
      filter: item.filter,
      info: item.info || "",
      music: item.music || null
    });

    const { error } = await insertRowWithFallback("stories", {
      user_id: user.id,
      media_url: mediaUrl,
      media_type: item.mediaType === "video" ? "video" : "image",
      title: item.caption || null,
      description: item.info || null,
      keywords: keywordsPayload || null,
      expires_at: expiresAt
    }, ["user_id", "media_url", "media_type", "expires_at"]);

    if(error){
      console.error("Story insert failed", error);
      if(isPermissionDeniedError(error)){
        permissionBlocked = true;
        break;
      }
      continue;
    }
    uploaded += 1;
  }

  storyComposerState.busy = false;
  if(storyPublishBtn) storyPublishBtn.disabled = false;
  if(storyAddMoreBtn) storyAddMoreBtn.disabled = false;

  if(permissionBlocked){
    setStoryComposerStatus("Blocked by Supabase policy");
    showToast("Story upload blocked by policy.");
    return;
  }
  if(uploaded === 0){
    setStoryComposerStatus("Upload failed");
    showToast("Story upload failed.");
    return;
  }

  closeStoryComposer();
  await loadStories();
  showToast(uploaded > 1 ? `${uploaded} stories uploaded` : "Story uploaded");
}

function isPostFile(file){
  const type = String(file?.type || "").toLowerCase();
  return type.startsWith("image/") || type.startsWith("video/");
}

function createPostDraft(file){
  const mediaType = String(file?.type || "").toLowerCase().startsWith("video/")
    ? "video"
    : "image";
  return {
    id: "post_" + Date.now().toString(36) + "_" + Math.random().toString(36).slice(2, 7),
    file,
    mediaType,
    previewUrl: URL.createObjectURL(file),
    filter: normalizePostFilter(postComposerState.defaultFilter)
  };
}

function releasePostDraft(item){
  if(!item || !item.previewUrl) return;
  if(String(item.previewUrl).startsWith("blob:")){
    try{ URL.revokeObjectURL(item.previewUrl); }catch(_){}
  }
}

function setPostComposerStatus(text){
  if(!postComposerStatus) return;
  postComposerStatus.textContent = text ? String(text) : "";
}

function clearPostComposerState(){
  (postComposerState.items || []).forEach(item => releasePostDraft(item));
  postComposerState.items = [];
  postComposerState.activeIndex = 0;
  postComposerState.busy = false;
  postComposerState.title = "";
  postComposerState.description = "";
  postComposerState.keywords = "";
  postComposerState.textMode = false;
  postComposerState.textContent = "";
  postComposerState.music = null;
  postComposerState.defaultFilter = "none";
  setPostComposerStatus("");
}

function getActivePostDraft(){
  if(!postComposerState.items.length) return null;
  const safeIndex = Math.max(0, Math.min(postComposerState.items.length - 1, postComposerState.activeIndex));
  postComposerState.activeIndex = safeIndex;
  return postComposerState.items[safeIndex] || null;
}

function renderPostComposerFilters(){
  if(!postFilterList) return;
  const activeDraft = getActivePostDraft();
  const activeFilter = activeDraft
    ? normalizePostFilter(activeDraft.filter)
    : normalizePostFilter(postComposerState.defaultFilter);
  postFilterList.innerHTML = POST_FILTERS.map(item => {
    const active = item.id === activeFilter ? " active" : "";
    return `<button type="button" class="post-filter-chip${active}" data-post-filter="${item.id}">${escapeHtml(item.label)}</button>`;
  }).join("");
}

function renderPostComposerQueue(){
  if(!postMediaQueue) return;
  postMediaQueue.innerHTML = "";
  postComposerState.items.forEach((item, index) => {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "post-media-thumb" + (index === postComposerState.activeIndex ? " active" : "");
    btn.dataset.postThumb = String(index);
    btn.innerHTML = item.mediaType === "video"
      ? `<video src="${item.previewUrl}" muted playsinline preload="metadata"></video>`
      : `<img src="${item.previewUrl}" alt="">`;
    postMediaQueue.appendChild(btn);
  });
}

function renderPostComposerMusicResults(rows){
  if(!postMusicResults) return;
  const list = Array.isArray(rows) ? rows : [];
  if(!list.length){
    postMusicResults.innerHTML = "";
    return;
  }
  postMusicResults.innerHTML = list.map((row, idx) => {
    const label = `${row.title} - ${row.artist}`;
    return `<button type="button" class="post-music-item" data-post-music="${idx}">${escapeHtml(label)}</button>`;
  }).join("");
  postMusicResults.dataset.rows = JSON.stringify(list);
}

function renderPostComposerPreview(){
  if(!postComposerMedia) return;
  if(postComposerState.textMode){
    const title = String(postComposerState.title || "").trim() || "Text Post";
    const body = String(postComposerState.description || "").trim() || "Write your text and publish.";
    const filterCss = getPostFilterCss(postComposerState.defaultFilter);
    postComposerMedia.innerHTML = `
      <div class="post-composer-text-preview" style="filter:${escapeHtml(filterCss)}">
        <div class="post-composer-text-title">${escapeHtml(title)}</div>
        <div class="post-composer-text-body">${escapeHtml(body)}</div>
      </div>
    `;
    return;
  }

  const active = getActivePostDraft();
  if(!active){
    postComposerMedia.innerHTML = `<div class="post-composer-text-preview"><div class="post-composer-text-title">Add media or use text mode</div><div class="post-composer-text-body">You can upload images, videos, or shoot video from camera.</div></div>`;
    return;
  }
  const filterCss = getPostFilterCss(active.filter);
  const style = escapeHtml(`filter:${filterCss};`);
  postComposerMedia.innerHTML = active.mediaType === "video"
    ? `<video src="${active.previewUrl}" autoplay muted loop playsinline style="${style}"></video>`
    : `<img src="${active.previewUrl}" alt="" style="${style}">`;
}

function renderPostComposerForm(){
  if(postTitleInput) postTitleInput.value = postComposerState.title || "";
  if(postDescriptionInput) postDescriptionInput.value = postComposerState.description || "";
  if(postKeywordsInput) postKeywordsInput.value = postComposerState.keywords || "";
  if(postTextModeInput) postTextModeInput.checked = !!postComposerState.textMode;
  if(postMusicSelected){
    if(postComposerState.music && postComposerState.music.url){
      const label = postComposerState.music.artist
        ? `${postComposerState.music.title} - ${postComposerState.music.artist}`
        : postComposerState.music.title;
      postMusicSelected.textContent = label;
    }else{
      postMusicSelected.textContent = "No music selected";
    }
  }
}

function renderPostComposer(){
  if(!postComposerState.open){
    if(postComposer){
      postComposer.classList.remove("active");
    }
    return;
  }
  if(postComposer){
    postComposer.classList.add("active");
  }
  renderPostComposerFilters();
  renderPostComposerQueue();
  renderPostComposerPreview();
  renderPostComposerForm();
}

function openPostComposer(){
  postComposerState.open = true;
  if(!postComposerState.items.length && !postComposerState.textMode){
    setPostComposerStatus("Add media or switch to text mode.");
  }
  renderPostComposer();
  syncOverlayScrollLock();
}

function closePostComposer(){
  clearPostComposerState();
  postComposerState.open = false;
  if(postComposer){
    postComposer.classList.remove("active");
  }
  if(postComposerMedia){
    postComposerMedia.innerHTML = "";
  }
  if(postMediaQueue){
    postMediaQueue.innerHTML = "";
  }
  if(postMusicResults){
    postMusicResults.innerHTML = "";
    postMusicResults.dataset.rows = "[]";
  }
  syncOverlayScrollLock();
}

function addPostDraftFiles(files){
  const accepted = (files || []).filter(file => isPostFile(file));
  if(!accepted.length){
    showToast("Only image/video files are supported.");
    return;
  }
  accepted.forEach(file => {
    postComposerState.items.push(createPostDraft(file));
  });
  postComposerState.textMode = false;
  if(!postComposerState.open){
    openPostComposer();
  }
  postComposerState.activeIndex = Math.max(0, postComposerState.items.length - accepted.length);
  setPostComposerStatus(`${postComposerState.items.length} media selected`);
  renderPostComposer();
}

function canvasToBlob(canvas, type, quality){
  return new Promise(resolve => {
    canvas.toBlob(blob => resolve(blob), type || "image/png", quality);
  });
}

async function createTextPostFile(title, body){
  const canvas = document.createElement("canvas");
  canvas.width = 1080;
  canvas.height = 1350;
  const ctx = canvas.getContext("2d");
  if(!ctx){
    throw new Error("canvas_not_supported");
  }

  const grad = ctx.createLinearGradient(0, 0, canvas.width, canvas.height);
  grad.addColorStop(0, "#ff7a14");
  grad.addColorStop(0.5, "#ff3d77");
  grad.addColorStop(1, "#6b5dff");
  ctx.fillStyle = grad;
  ctx.fillRect(0, 0, canvas.width, canvas.height);

  const safeTitle = String(title || "").trim() || "Text Post";
  const safeBody = String(body || "").trim() || " ";
  ctx.fillStyle = "#ffffff";
  ctx.textBaseline = "top";
  ctx.font = "700 58px Arial";
  const titleLines = wrapCanvasText(ctx, safeTitle, canvas.width - 150, 2);
  let y = 120;
  titleLines.forEach(line => {
    ctx.fillText(line, 75, y);
    y += 74;
  });
  y += 26;
  ctx.font = "500 44px Arial";
  const bodyLines = wrapCanvasText(ctx, safeBody, canvas.width - 150, 12);
  bodyLines.forEach(line => {
    ctx.fillText(line, 75, y);
    y += 56;
  });

  const blob = await canvasToBlob(canvas, "image/png", 0.94);
  if(!blob){
    throw new Error("text_post_blob_failed");
  }
  return new File([blob], `text-post-${Date.now()}.png`, { type:"image/png" });
}

function wrapCanvasText(ctx, text, maxWidth, maxLines){
  const words = String(text || "").split(/\s+/).filter(Boolean);
  if(!words.length) return [""];
  const lines = [];
  let line = "";
  for(const word of words){
    const test = line ? `${line} ${word}` : word;
    if(ctx.measureText(test).width <= maxWidth){
      line = test;
      continue;
    }
    if(line){
      lines.push(line);
      if(lines.length >= maxLines) return lines;
    }
    line = word;
  }
  if(line && lines.length < maxLines){
    lines.push(line);
  }
  return lines;
}

async function publishPostFromComposer(){
  if(postComposerState.busy) return;

  const isTextMode = !!postComposerState.textMode;
  const title = String(postComposerState.title || "").trim();
  const description = String(postComposerState.description || "").trim();
  const keywords = String(postComposerState.keywords || "").trim();
  let items = postComposerState.items.slice();

  if(isTextMode){
    if(!title && !description){
      setPostComposerStatus("Add title or description for text post.");
      return;
    }
    try{
      const textFile = await createTextPostFile(title, description);
      items = [{
        id: "text_post_" + Date.now().toString(36),
        file: textFile,
        mediaType: "image",
        filter: normalizePostFilter(postComposerState.defaultFilter)
      }];
    }catch(err){
      console.error("Text post create failed", err);
      setPostComposerStatus("Unable to create text post.");
      return;
    }
  }else if(!items.length){
    setPostComposerStatus("Add at least one image or video.");
    return;
  }

  postComposerState.busy = true;
  if(postPublishBtn) postPublishBtn.disabled = true;
  if(postAddMediaBtn) postAddMediaBtn.disabled = true;
  if(postShootVideoBtn) postShootVideoBtn.disabled = true;
  if(postResetBtn) postResetBtn.disabled = true;

  const batchId = createPostBatchId();
  let uploadedCount = 0;
  let permissionBlocked = false;

  for(let index = 0; index < items.length; index += 1){
    const item = items[index];
    if(!item?.file) continue;
    setPostComposerStatus(`Uploading ${index + 1}/${items.length}...`);
    let mediaUrl = "";
    try{
      const path = makePostBatchPath(user.id, item.file, batchId, index);
      mediaUrl = await window.NOVA.uploadToBucket("posts", item.file, path);
    }catch(uploadErr){
      console.error("Post media upload failed", uploadErr);
      continue;
    }

    const keywordPayload = buildPostKeywords(keywords, postComposerState.music, {
      filter: item.filter || postComposerState.defaultFilter,
      textMode: isTextMode
    });

    const { error } = await insertRowWithFallback("posts", withPostPublicCompat({
      user_id: user.id,
      media_url: mediaUrl,
      media_type: item.mediaType === "video" ? "video" : "image",
      title: title || (isTextMode ? "Text Post" : null),
      description: description || null,
      keywords: keywordPayload || null
    }), ["user_id", "media_url"]);

    if(error){
      console.error("Post upload failed", error);
      if(isPermissionDeniedError(error)){
        permissionBlocked = true;
        break;
      }
      continue;
    }
    uploadedCount += 1;
  }

  postComposerState.busy = false;
  if(postPublishBtn) postPublishBtn.disabled = false;
  if(postAddMediaBtn) postAddMediaBtn.disabled = false;
  if(postShootVideoBtn) postShootVideoBtn.disabled = false;
  if(postResetBtn) postResetBtn.disabled = false;

  if(permissionBlocked){
    setPostComposerStatus("Blocked by Supabase policy.");
    return;
  }
  if(uploadedCount === 0){
    setPostComposerStatus("Post upload failed.");
    return;
  }

  closePostComposer();
  await normalizeMyPostsPublicVisibility();
  showToast(uploadedCount > 1 ? `${uploadedCount} media uploaded` : "Post uploaded");
  scheduleFeedReload();
}

postInput.onchange = e => {
  const files = Array.from(e.target.files || []);
  if(!files.length) return;
  addPostDraftFiles(files);
  postInput.value = "";
};

postCameraInput.onchange = e => {
  const files = Array.from(e.target.files || []);
  if(!files.length) return;
  addPostDraftFiles(files);
  postCameraInput.value = "";
};

function getStoryExpiryMs(row){
  const expiresTs = Date.parse(String(row?.expires_at || ""));
  if(Number.isFinite(expiresTs)) return expiresTs;
  const createdTs = Date.parse(String(row?.created_at || ""));
  if(Number.isFinite(createdTs)){
    return createdTs + (24 * 60 * 60 * 1000);
  }
  return 0;
}

function isStoryActiveNow(row, nowMs){
  const expiry = getStoryExpiryMs(row);
  if(!expiry) return false;
  return expiry > (Number(nowMs) || Date.now());
}

async function fetchStoriesWithFallback(limit){
  const now = new Date().toISOString();
  const nowMs = Date.now();
  const max = Math.max(1, Math.min(200, Number(limit) || 120));
  const plans = [
    ["id","user_id","media_url","media_type","created_at","expires_at","title","description","keywords"],
    ["id","user_id","media_url","media_type","created_at","expires_at","title","description"],
    ["id","user_id","media_url","media_type","created_at","expires_at"]
  ];
  let lastError = null;
  for(const fields of plans){
    const { data, error } = await window.NOVA.supa
      .from("stories")
      .select(fields.join(","))
      .gt("expires_at", now)
      .order("created_at", { ascending:false })
      .limit(max);
    if(!error){
      const list = (Array.isArray(data) ? data : [])
        .filter(row => row && row.id && row.user_id && row.media_url)
        .filter(row => isStoryActiveNow(row, nowMs))
        .map(row => hydrateStoryRow(row));
      return { data:list, error:null };
    }
    lastError = error;
  }
  return { data:[], error:lastError };
}

function groupStoriesByUser(stories){
  const groups = new Map();
  (stories || []).forEach(row => {
    const uid = String(row?.user_id || "");
    if(!uid) return;
    if(!groups.has(uid)){
      groups.set(uid, []);
    }
    groups.get(uid).push(row);
  });

  groups.forEach(list => {
    list.sort((a, b) => {
      const aTs = Date.parse(String(a?.created_at || "")) || 0;
      const bTs = Date.parse(String(b?.created_at || "")) || 0;
      return aTs - bTs;
    });
  });

  const orderedUserIds = Array.from(groups.keys()).sort((a, b) => {
    const aRows = groups.get(a) || [];
    const bRows = groups.get(b) || [];
    const aTs = Date.parse(String((aRows[aRows.length - 1] || {}).created_at || "")) || 0;
    const bTs = Date.parse(String((bRows[bRows.length - 1] || {}).created_at || "")) || 0;
    return bTs - aTs;
  });

  return { groups, orderedUserIds };
}

function stopStoryPlaybackTimers(){
  if(storyAdvanceTimer){
    clearTimeout(storyAdvanceTimer);
    storyAdvanceTimer = null;
  }
  if(storyProgressTimer){
    clearTimeout(storyProgressTimer);
    storyProgressTimer = null;
  }
}

function updateStoryProgress(durationMs){
  if(!storyProgress) return;
  const list = storyPlaybackState.list || [];
  const activeIndex = Math.max(0, storyPlaybackState.index || 0);
  storyProgress.innerHTML = list.map((_, idx) => {
    const done = idx < activeIndex ? " done" : "";
    const active = idx === activeIndex ? " active" : "";
    const width = idx < activeIndex ? "100%" : "0%";
    return `<div class="story-progress-item${done}${active}"><span style="width:${width}"></span></div>`;
  }).join("");

  const activeBar = storyProgress.querySelector(".story-progress-item.active > span");
  if(!activeBar) return;
  activeBar.style.transition = "none";
  activeBar.style.width = "0%";
  requestAnimationFrame(() => {
    activeBar.style.transition = `width ${Math.max(500, durationMs || STORY_IMAGE_DURATION_MS)}ms linear`;
    activeBar.style.width = "100%";
  });
}

function getStoryMediaDurationMs(item, mediaEl){
  if(item?.media_type === "video"){
    const sec = Number(mediaEl?.duration) || 0;
    if(sec > 0){
      return Math.min(Math.max(sec * 1000, 2000), 30000);
    }
  }
  return STORY_IMAGE_DURATION_MS;
}

function scheduleStoryAdvance(ms){
  stopStoryPlaybackTimers();
  const wait = Math.max(1000, Number(ms) || STORY_IMAGE_DURATION_MS);
  storyAdvanceTimer = setTimeout(() => {
    goToNextStory(1);
  }, wait);
}

async function renderStoryViewers(storyId){
  if(!storyViewersPanel || !storyId) return;
  storyViewersPanel.innerHTML = "<div class='story-viewers-row'>Loading viewers...</div>";
  let viewers = [];
  if(window.NOVA && typeof window.NOVA.getStoryViewers === "function"){
    viewers = await window.NOVA.getStoryViewers(storyId, 80);
  }else{
    const { data } = await window.NOVA.supa
      .from("story_views")
      .select("viewer_id,viewer_name,created_at")
      .eq("story_id", storyId)
      .order("created_at", { ascending:false })
      .limit(80);
    viewers = Array.isArray(data) ? data : [];
  }
  if(!viewers.length){
    storyViewersPanel.innerHTML = "<div class='story-viewers-row'>No viewers yet.</div>";
    return;
  }
  storyViewersPanel.innerHTML = viewers.map(row => {
    const name = row?.viewer_name || "user";
    const time = formatRelativeTime(row?.created_at || "") || "";
    const suffix = time ? ` - ${time}` : "";
    return `<div class="story-viewers-row">${escapeHtml(name + suffix)}</div>`;
  }).join("");
}

async function showStoryIndex(index){
  const list = storyPlaybackState.list || [];
  if(!list.length){
    closeStory();
    return;
  }
  const safeIndex = Math.max(0, Math.min(list.length - 1, Number(index) || 0));
  storyPlaybackState.index = safeIndex;
  storyPlaybackState.viewersOpen = false;
  if(storyViewersPanel){
    storyViewersPanel.hidden = true;
    storyViewersPanel.innerHTML = "";
  }
  const story = list[safeIndex];
  if(!story){
    closeStory();
    return;
  }
  if(!isStoryActiveNow(story, Date.now())){
    goToNextStory(1);
    return;
  }

  if(storyViewerName){
    storyViewerName.textContent = storyPlaybackState.ownerName || "Story";
  }
  if(storyViewerAvatar){
    storyViewerAvatar.src = storyPlaybackState.ownerAvatar || resolveAvatarUrl("", "Story");
  }
  if(storyViewerTime){
    storyViewerTime.textContent = formatRelativeTime(story.created_at) || "now";
  }

  const filterCss = getStoryFilterCss(story.filter);
  const safeUrl = escapeHtml(story.media_url || "");
  if(story.media_type === "video"){
    storyContent.innerHTML = `<video src="${safeUrl}" autoplay playsinline controls style="filter:${filterCss}"></video>`;
  }else{
    storyContent.innerHTML = `<img src="${safeUrl}" alt="" style="filter:${filterCss}">`;
  }

  if(storyCaption){
    storyCaption.textContent = story.caption || "";
    storyCaption.hidden = !story.caption;
  }
  if(storyInfo){
    storyInfo.textContent = story.info || "";
    storyInfo.hidden = !story.info;
  }
  if(storyMusic){
    if(story.music && story.music.url){
      const label = story.music.artist
        ? `${story.music.title} - ${story.music.artist}`
        : story.music.title;
      storyMusic.hidden = false;
      storyMusic.textContent = "Music: " + label;
    }else{
      storyMusic.hidden = true;
      storyMusic.textContent = "";
    }
  }

  if(user && String(story.user_id || "") !== String(user.id || "")){
    try{
      await window.NOVA.markStoryView(story.id);
    }catch(_){}
  }

  const views = await window.NOVA.getStoryViewCount(story.id);
  if(storyMeta){
    storyMeta.textContent = `Views: ${formatCount(views)}${story.plainKeywords ? " | " + story.plainKeywords : ""}`;
  }

  const mine = user && String(story.user_id || "") === String(user.id || "");
  if(storyOwnerTools){
    storyOwnerTools.hidden = !mine;
  }
  if(storySeenByBtn){
    storySeenByBtn.textContent = `Seen by ${formatCount(views)}`;
  }

  let durationMs = STORY_IMAGE_DURATION_MS;
  const mediaEl = storyContent.querySelector("video, img");
  if(mediaEl && story.media_type === "video"){
    if(mediaEl.tagName === "VIDEO"){
      mediaEl.addEventListener("ended", () => goToNextStory(1), { once:true });
      mediaEl.addEventListener("loadedmetadata", () => {
        const nextDuration = getStoryMediaDurationMs(story, mediaEl);
        updateStoryProgress(nextDuration);
        scheduleStoryAdvance(nextDuration + 120);
      }, { once:true });
      durationMs = 7000;
    }
  }

  updateStoryProgress(durationMs);
  scheduleStoryAdvance(durationMs + 120);
}

function goToNextStory(step){
  const direction = Number(step) || 1;
  const nextIndex = storyPlaybackState.index + direction;
  if(nextIndex < 0){
    closeStory();
    return;
  }
  if(nextIndex >= storyPlaybackState.list.length){
    closeStory();
    return;
  }
  showStoryIndex(nextIndex);
}

function openStorySequence(list, options){
  if(!Array.isArray(list) || !list.length){
    return false;
  }
  storyPlaybackState.list = list.slice();
  storyPlaybackState.ownerId = String(options?.ownerId || "");
  storyPlaybackState.ownerName = String(options?.ownerName || "Story");
  storyPlaybackState.ownerAvatar = String(options?.ownerAvatar || "");
  storyPlaybackState.index = Math.max(0, Math.min(list.length - 1, Number(options?.startIndex) || 0));
  storyPlaybackState.viewersOpen = false;
  storyViewer.classList.add("active");
  syncOverlayScrollLock();
  showStoryIndex(storyPlaybackState.index);
  return true;
}

async function openUserStories(userId, options){
  const id = String(userId || "");
  if(!id) return false;
  let rows = storyGroupsByUser.get(id) || [];
  if(!rows.length){
    await loadStories();
    rows = storyGroupsByUser.get(id) || [];
  }
  if(!rows.length){
    if(options?.allowComposerFallback && user && id === String(user.id || "")){
      addStory();
      return false;
    }
    if(options?.fallbackToProfile){
      location.href = `m-account.html?uid=${encodeURIComponent(id)}`;
      return false;
    }
    showToast("No active story available.");
    return false;
  }

  let profile = storyProfileCache[id];
  if(!profile){
    const profileMap = await loadStoryProfiles([id]);
    profile = profileMap[id] || {};
  }
  const ownerName = user && id === String(user.id || "")
    ? "You"
    : (profile?.name || "Story");
  const ownerAvatar = resolveAvatarUrl(profile?.avatarUrl || "", ownerName);
  return openStorySequence(rows, {
    ownerId: id,
    ownerName,
    ownerAvatar,
    startIndex: options?.startIndex || 0
  });
}

async function loadStories(){
  const { data, error } = await fetchStoriesWithFallback(160);
  if(error){
    console.error("Failed to load stories", error);
    return;
  }
  const hydrated = Array.isArray(data) ? data : [];
  const { groups, orderedUserIds } = groupStoriesByUser(hydrated);
  storyGroupsByUser = groups;

  const profileMap = await loadStoryProfiles([
    ...orderedUserIds,
    user?.id
  ]);
  await hydrateMyStoryTile();

  const myId = String(user?.id || "");
  const hasMyStories = myId ? (storyGroupsByUser.get(myId) || []).length > 0 : false;
  if(myStoryLabel){
    myStoryLabel.textContent = hasMyStories ? "Your Story" : "You";
  }

  const myTile = storiesWrap ? storiesWrap.querySelector(".story") : null;
  if(myTile){
    myTile.onclick = () => {
      const uid = String(user?.id || "");
      if(!uid){
        addStory();
        return;
      }
      if((storyGroupsByUser.get(uid) || []).length){
        openUserStories(uid, { allowComposerFallback:true });
      }else{
        addStory();
      }
    };
  }
  const plusEl = myTile ? myTile.querySelector(".plus") : null;
  if(plusEl){
    plusEl.onclick = e => {
      e.stopPropagation();
      addStory();
    };
  }

  const base = storiesWrap.querySelectorAll(".story");
  base.forEach((node, idx) => { if(idx > 0) node.remove(); });

  orderedUserIds.forEach(uid => {
    if(myId && uid === myId) return;
    const rows = storyGroupsByUser.get(uid) || [];
    if(!rows.length) return;
    const latest = rows[rows.length - 1];
    const profile = profileMap[String(uid || "")] || {};
    const label = profile.name || "Story";
    const avatarUrl = resolveAvatarUrl(profile.avatarUrl, label);
    const el = document.createElement("div");
    el.className = "story";
    el.innerHTML = `
      <div class="avatar">
        <img src="${avatarUrl}" alt="">
      </div>
      <div>${escapeHtml(label)}</div>
    `;
    el.onclick = () => openUserStories(uid, { fallbackToProfile:true, startIndex:0 });
    if(latest?.created_at){
      el.title = "Latest: " + (formatRelativeTime(latest.created_at) || "now");
    }
    storiesWrap.appendChild(el);
  });
}

function closeStory(){
  stopStoryPlaybackTimers();
  storyPlaybackState.list = [];
  storyPlaybackState.index = 0;
  storyPlaybackState.ownerId = "";
  storyPlaybackState.ownerName = "";
  storyPlaybackState.ownerAvatar = "";
  storyPlaybackState.viewersOpen = false;
  if(storyViewer){
    storyViewer.classList.remove("active");
  }
  if(storyContent){
    storyContent.innerHTML = "";
  }
  if(storyProgress){
    storyProgress.innerHTML = "";
  }
  if(storyViewersPanel){
    storyViewersPanel.hidden = true;
    storyViewersPanel.innerHTML = "";
  }
  syncOverlayScrollLock();
}

async function loadMore(){
  if(!feedReady) return;
  if(loading || done) return;
  loading = true;
  const requestVersion = feedVersion;

  const from = page * PAGE_SIZE;
  const to = from + PAGE_SIZE - 1;
  const { data, error } = await fetchPostsPage(from, to);

  if(requestVersion !== feedVersion){
    loading = false;
    return;
  }

  if(error){
    console.error(error);
    showFeedPermissionAlert(error);
    loading = false;
    return;
  }

  if(!data || !data.length){
    if(page === 0){
      setFeedNotice("No posts visible yet. Agar aapko pata hai users ne post kiya hai aur yahan blank aa raha hai, to sql/fix_public_feed_visibility.sql run karo.");
    }
    done = true;
    loading = false;
    return;
  }

  if(page === 0){
    setFeedNotice("");
    const currentUserId = String(user?.id || "");
    const hasOtherUserPost = (data || []).some(item => {
      return String(item?.user_id || "") !== currentUserId;
    });
    if(!hasOtherUserPost && !feedUserId){
      setFeedNotice("Abhi feed me sirf aapke posts visible hain. Agar aur users ke posts expected hain, to sql/fix_public_feed_visibility.sql run karke policies update karo.");
    }
  }

  await Promise.all((data || []).map(item => renderPost(item)));
  if(requestVersion !== feedVersion){
    loading = false;
    return;
  }
  page += 1;
  loading = false;
}

function itemIncludesPostId(item, postId){
  const target = String(postId || "");
  if(!target || !item) return false;
  if(String(item.id || "") === target) return true;
  const ids = Array.isArray(item.post_ids) ? item.post_ids : [];
  return ids.some(id => String(id || "") === target);
}

function renderPostMediaHtml(item){
  const urls = Array.isArray(item?.media_urls) && item.media_urls.length
    ? item.media_urls
    : [item?.media_url].filter(Boolean);
  const types = Array.isArray(item?.media_types) ? item.media_types : [];
  const filterCss = getPostFilterCss(item?.filter || "none");

  if(!urls.length){
    return "<div class='post-media'><div class='media-slide'></div></div>";
  }

  const slides = urls.map((url, index) => {
    const type = String(types[index] || mediaTypeFromUrl(url)).toLowerCase();
    const safeUrl = escapeHtml(url);
    const style = escapeHtml(`filter:${filterCss};`);
    if(type === "video"){
      return `<div class="media-slide" data-slide="${index}"><video src="${safeUrl}" controls preload="metadata" style="${style}"></video></div>`;
    }
    return `<div class="media-slide" data-slide="${index}"><img src="${safeUrl}" alt="" style="${style}"></div>`;
  }).join("");

  const multi = urls.length > 1;
  const nav = multi
    ? `
      <button class="media-nav prev" data-nav="prev" aria-label="Previous slide">&#8249;</button>
      <button class="media-nav next" data-nav="next" aria-label="Next slide">&#8250;</button>
      <div class="media-dots">
        ${urls.map((_, index) => `<span data-dot="${index}" class="${index === 0 ? "active" : ""}"></span>`).join("")}
      </div>
    `
    : "";

  return `<div class="post-media" data-carousel><div class="media-track">${slides}</div>${nav}</div>`;
}

function initPostCarousel(postEl){
  const carousel = postEl.querySelector("[data-carousel]");
  if(!carousel) return;
  const track = carousel.querySelector(".media-track");
  if(!track) return;

  const dots = Array.from(carousel.querySelectorAll("[data-dot]"));
  const slideCount = track.children.length;
  if(slideCount <= 1){
    return;
  }

  const getActiveIndex = () => {
    const width = track.clientWidth || 1;
    return Math.max(0, Math.min(slideCount - 1, Math.round(track.scrollLeft / width)));
  };

  const updateDots = () => {
    const index = getActiveIndex();
    dots.forEach((dot, i) => {
      dot.classList.toggle("active", i === index);
    });
  };

  let scrollTicking = false;
  track.addEventListener("scroll", () => {
    if(scrollTicking) return;
    scrollTicking = true;
    requestAnimationFrame(() => {
      updateDots();
      scrollTicking = false;
    });
  }, { passive:true });

  const moveTo = (index) => {
    const width = track.clientWidth || 1;
    const safeIndex = Math.max(0, Math.min(slideCount - 1, index));
    track.scrollTo({ left: width * safeIndex, behavior:"smooth" });
  };

  const prevBtn = carousel.querySelector("[data-nav='prev']");
  const nextBtn = carousel.querySelector("[data-nav='next']");
  if(prevBtn){
    prevBtn.addEventListener("click", e => {
      e.stopPropagation();
      moveTo(getActiveIndex() - 1);
    });
  }
  if(nextBtn){
    nextBtn.addEventListener("click", e => {
      e.stopPropagation();
      moveTo(getActiveIndex() + 1);
    });
  }

  dots.forEach(dot => {
    dot.addEventListener("click", e => {
      e.stopPropagation();
      const idx = Number(dot.getAttribute("data-dot") || 0);
      moveTo(idx);
    });
  });
}

function applyReactionUi(reaction, likeBtn, dislikeBtn){
  if(likeBtn){
    likeBtn.classList.toggle("active-like", reaction === "like");
  }
  if(dislikeBtn){
    dislikeBtn.classList.toggle("active-dislike", reaction === "dislike");
  }
}

async function getMyReaction(targetType, targetId){
  if(!user || !user.id) return null;
  const { data, error } = await window.NOVA.supa
    .from("reactions")
    .select("reaction")
    .eq("user_id", user.id)
    .eq("target_type", targetType)
    .eq("target_id", targetId)
    .maybeSingle();
  if(error) return null;
  return data?.reaction || null;
}

function renderPostMusicHtml(item){
  const music = item?.music;
  if(!music || !music.url) return "";
  const title = escapeHtml(music.title || "Original Audio");
  const artist = escapeHtml(music.artist || "");
  const label = artist ? `${title} - ${artist}` : title;
  return `
    <div class="post-author-music" data-music>
      <button class="post-author-music-btn" data-music-play type="button">Play</button>
      <span class="post-author-music-text">Music: ${label}</span>
      <audio src="${escapeHtml(music.url)}" preload="none"></audio>
    </div>
  `;
}

function initMusicStrip(postEl){
  const strip = postEl.querySelector("[data-music]");
  if(!strip) return;
  const btn = strip.querySelector("[data-music-play]");
  const audio = strip.querySelector("audio");
  if(!btn || !audio) return;

  const sync = () => {
    btn.textContent = audio.paused ? "Play" : "Pause";
  };
  sync();

  btn.addEventListener("click", e => {
    e.stopPropagation();
    if(audio.paused){
      if(currentPlayingAudio && currentPlayingAudio !== audio){
        currentPlayingAudio.pause();
      }
      currentPlayingAudio = audio;
      audio.play().catch(()=>{});
    }else{
      audio.pause();
    }
    sync();
  });
  audio.addEventListener("play", sync);
  audio.addEventListener("pause", sync);
  audio.addEventListener("ended", () => {
    if(currentPlayingAudio === audio){
      currentPlayingAudio = null;
    }
    sync();
  });
}

function syncOverlayScrollLock(){
  const commentsOpen = commentsPanel.classList.contains("show");
  const shareOpen = shareSheet.classList.contains("show");
  const storyOpen = storyViewer.classList.contains("active");
  const storyComposerOpen = storyComposer.classList.contains("active");
  const postComposerOpen = postComposer.classList.contains("active");
  document.body.style.overflow = (commentsOpen || shareOpen || storyOpen || storyComposerOpen || postComposerOpen) ? "hidden" : "";
}

function openComments(postId, countEl, ownerId, targetType){
  activeCommentPostId = String(postId || "");
  activeCommentTargetType = String(targetType || "post");
  activeCommentCountEl = countEl || null;
  activeCommentOwnerId = String(ownerId || "");
  commentsList.innerHTML = "<div class='comment-empty'>Loading comments...</div>";
  commentsBackdrop.classList.add("show");
  commentsPanel.classList.add("show");
  clearCommentDraftState();
  syncOverlayScrollLock();
  loadComments(activeCommentPostId);
}

function closeComments(){
  commentsPanel.classList.remove("show");
  commentsBackdrop.classList.remove("show");
  commentInput.value = "";
  activeCommentPostId = "";
  activeCommentTargetType = "post";
  activeCommentOwnerId = "";
  activeCommentCountEl = null;
  activeCommentThread = null;
  clearCommentDraftState();
  closeCommentMenus();
  syncOverlayScrollLock();
}

function closeCommentMenus(){
  commentsList.querySelectorAll(".comment-menu.show").forEach(menu => menu.classList.remove("show"));
}

function clearCommentDraftState(){
  activeReplyRowId = "";
  activeEditRowId = "";
  commentReplying.hidden = true;
  commentReplyLabel.textContent = "";
  commentInput.placeholder = "Write a comment...";
  commentSend.textContent = "Send";
}

function setReplyState(rowId, name){
  activeReplyRowId = String(rowId || "");
  activeEditRowId = "";
  commentReplying.hidden = false;
  commentReplyLabel.textContent = "Replying to " + (name || "User");
  commentInput.placeholder = "Write a reply...";
  commentSend.textContent = "Reply";
  commentInput.focus();
}

function setEditState(rowId, body){
  activeEditRowId = String(rowId || "");
  activeReplyRowId = "";
  commentReplying.hidden = false;
  commentReplyLabel.textContent = "Editing your comment";
  commentInput.placeholder = "Edit comment...";
  commentSend.textContent = "Update";
  commentInput.value = body || "";
  commentInput.focus();
}

function formatCommentTime(value){
  const ts = Date.parse(String(value || ""));
  if(!Number.isFinite(ts)) return "";
  const diff = Math.max(1, Math.floor((Date.now() - ts) / 1000));
  if(diff < 60) return diff + "s";
  if(diff < 3600) return Math.floor(diff / 60) + "m";
  if(diff < 86400) return Math.floor(diff / 3600) + "h";
  if(diff < 604800) return Math.floor(diff / 86400) + "d";
  return new Date(ts).toLocaleDateString(undefined, { month:"short", day:"numeric" });
}

async function getCommentUser(userId){
  const key = String(userId || "");
  if(!key) return { name:"User", avatarUrl:"" };
  if(commentUserCache[key]) return commentUserCache[key];

  let name = "User";
  let avatarUrl = "";
  try{
    const { data } = await window.NOVA.supa
      .from("users")
      .select("username,full_name,photo")
      .eq("user_id", key)
      .maybeSingle();
    if(data){
      name = data.full_name || data.username || name;
      avatarUrl = data.photo || "";
    }
  }catch(_){}

  const local = getLocalProfileOverride(key);
  if(local){
    if(local.displayName) name = local.displayName;
    else if(local.username && (!name || name === "User")) name = local.username;
    if(local.avatarData) avatarUrl = local.avatarData;
  }

  const info = { name, avatarUrl };
  commentUserCache[key] = info;
  return info;
}

async function renderCommentNode(comment, depth){
  const info = await getCommentUser(comment.user_id);
  const displayName = info.name || "User";
  const avatarUrl = info.avatarUrl || "";
  const initial = (displayName || "U").slice(0, 1).toUpperCase();
  const me = String(user?.id || "");
  const isMine = !!(me && comment.user_id === me);
  const isOwner = !!(me && activeCommentOwnerId && me === activeCommentOwnerId);
  const canPin = isOwner && !comment.parentRowId;
  const isPinned = !!comment.pinned;

  const wrap = document.createElement("div");
  wrap.className = "comment-wrap";
  const row = document.createElement("div");
  row.className = "comment-row";

  const menuActions = [];
  if(isMine){
    menuActions.push({ action:"edit", label:"Edit" });
    menuActions.push({ action:"delete", label:"Delete" });
  }
  if(canPin){
    menuActions.push({ action:isPinned ? "unpin" : "pin", label:isPinned ? "Unpin" : "Pin to top" });
  }

  row.innerHTML = `
    <div class="comment-avatar">
      ${avatarUrl ? `<img src="${escapeHtml(avatarUrl)}" alt="">` : `<span>${escapeHtml(initial)}</span>`}
    </div>
    <div class="comment-content">
      <div class="comment-top">
        <span class="comment-name">${escapeHtml(displayName)}</span>
        <span class="comment-time">${escapeHtml(formatCommentTime(comment.created_at))}</span>
        ${isPinned ? `<span class="comment-pin">Pinned</span>` : ""}
        ${menuActions.length ? `
          <div class="comment-menu-wrap">
            <button type="button" class="comment-menu-btn" data-comment-menu-btn>...</button>
            <div class="comment-menu" data-comment-menu>
              ${menuActions.map(item => `<button type="button" data-comment-action="${escapeHtml(item.action)}">${escapeHtml(item.label)}</button>`).join("")}
            </div>
          </div>
        ` : ""}
      </div>
      <div class="comment-body">${escapeHtml(comment.body || "")}${comment.edited ? `<span class="comment-edited">(edited)</span>` : ""}</div>
      <div class="comment-actions">
        <button type="button" class="comment-action" data-comment-reply>Reply</button>
      </div>
    </div>
  `;

  const replyBtn = row.querySelector("[data-comment-reply]");
  if(replyBtn){
    replyBtn.addEventListener("click", () => {
      setReplyState(comment.rowId, displayName);
    });
  }

  const menuBtn = row.querySelector("[data-comment-menu-btn]");
  const menu = row.querySelector("[data-comment-menu]");
  if(menuBtn && menu){
    menuBtn.addEventListener("click", e => {
      e.stopPropagation();
      const wasOpen = menu.classList.contains("show");
      closeCommentMenus();
      menu.classList.toggle("show", !wasOpen);
    });

    menu.addEventListener("click", async e => {
      const action = e.target?.getAttribute("data-comment-action") || "";
      if(!action) return;
      closeCommentMenus();

      if(action === "edit"){
        setEditState(comment.rowId, comment.body || "");
        return;
      }
      if(action === "delete"){
        const yes = confirm("Delete this comment?");
        if(!yes) return;
        const ok = await window.NOVA.deleteCommentById(comment.rowId);
        if(!ok){
          alert("Unable to delete comment.");
          return;
        }
        await loadComments(activeCommentPostId);
        return;
      }
      if(action === "pin"){
        const ok = await window.NOVA.pinComment(activeCommentTargetType || "post", activeCommentPostId, comment.rowId);
        if(!ok){
          alert("Unable to pin comment.");
          return;
        }
        await loadComments(activeCommentPostId);
        return;
      }
      if(action === "unpin"){
        const ok = await window.NOVA.pinComment(activeCommentTargetType || "post", activeCommentPostId, "");
        if(!ok){
          alert("Unable to unpin comment.");
          return;
        }
        await loadComments(activeCommentPostId);
      }
    });
  }

  wrap.appendChild(row);
  if(Array.isArray(comment.replies) && comment.replies.length){
    const children = document.createElement("div");
    children.className = "comment-children";
    for(const child of comment.replies){
      children.appendChild(await renderCommentNode(child, depth + 1));
    }
    wrap.appendChild(children);
  }
  return wrap;
}

async function loadComments(postId){
  const thread = await window.NOVA.getCommentThread(activeCommentTargetType || "post", postId, {
    ownerUserId: activeCommentOwnerId,
    limit: 500
  });
  if(thread.error){
    commentsList.innerHTML = "<div class='comment-empty'>Unable to load comments.</div>";
    return 0;
  }

  activeCommentThread = thread;
  if(!thread.roots || !thread.roots.length){
    commentsList.innerHTML = "<div class='comment-empty'>No comments yet.</div>";
    if(activeCommentCountEl){
      activeCommentCountEl.textContent = "0";
    }
    return 0;
  }

  commentsList.innerHTML = "";
  const frag = document.createDocumentFragment();
  for(const root of thread.roots){
    frag.appendChild(await renderCommentNode(root, 0));
  }
  commentsList.appendChild(frag);
  commentsList.scrollTop = commentsList.scrollHeight;

  if(activeCommentCountEl){
    activeCommentCountEl.textContent = formatCount(thread.visibleCount);
  }
  return thread.visibleCount;
}

async function sendComment(){
  const body = String(commentInput.value || "").trim();
  if(!body || !activeCommentPostId) return;
  const targetType = activeCommentTargetType || "post";

  let ok = false;
  if(activeEditRowId){
    ok = await window.NOVA.editComment(targetType, activeCommentPostId, activeEditRowId, body);
  }else if(activeReplyRowId){
    ok = await window.NOVA.replyComment(targetType, activeCommentPostId, activeReplyRowId, body);
  }else{
    ok = await window.NOVA.addComment(targetType, activeCommentPostId, body);
  }

  if(!ok){
    alert("Unable to submit comment.");
    return;
  }

  commentInput.value = "";
  clearCommentDraftState();
  const latestCount = await loadComments(activeCommentPostId);
  if(activeCommentCountEl){
    activeCommentCountEl.textContent = formatCount(latestCount);
  }
}

function openShareSheet(item, shareCountEl){
  activeShareItem = item || null;
  activeShareCountEl = shareCountEl || null;
  shareChatList.innerHTML = "<div class='share-chat-empty'>Loading chats...</div>";
  shareBackdrop.classList.add("show");
  shareSheet.classList.add("show");
  syncOverlayScrollLock();
  loadShareChatTargets();
}

function closeShareSheet(){
  shareSheet.classList.remove("show");
  shareBackdrop.classList.remove("show");
  activeShareItem = null;
  activeShareCountEl = null;
  shareChatList.innerHTML = "";
  syncOverlayScrollLock();
}

function getSharePayload(){
  const text = activeShareItem?.title || "Post";
  const url = activeShareItem ? getShareUrlForItem(activeShareItem) : location.href;
  return {
    text,
    url,
    message: text + "\n" + url
  };
}

function renderShareChatTargets(list){
  shareChatList.innerHTML = "";
  if(!Array.isArray(list) || !list.length){
    shareChatList.innerHTML = "<div class='share-chat-empty'>No recent chats found.</div>";
    return;
  }

  list.forEach(contact => {
    const row = document.createElement("div");
    row.className = "share-chat-item";
    const name = contact?.name || "User";
    const username = contact?.username ? "@" + contact.username : "";
    const initial = (name || "U").slice(0, 1).toUpperCase();
    row.innerHTML = `
      ${contact?.avatarUrl ? `<img src="${escapeHtml(contact.avatarUrl)}" alt="">` : `<div class="share-chat-avatar">${escapeHtml(initial)}</div>`}
      <div class="share-chat-meta">
        <div class="share-chat-name">${escapeHtml(name)}</div>
        <div class="share-chat-user">${escapeHtml(username)}</div>
      </div>
      <button type="button" class="share-chat-send" data-send-chat="${escapeHtml(contact.id)}">Send</button>
    `;
    shareChatList.appendChild(row);
  });
}

async function loadShareChatTargets(){
  if(!window.NOVA || typeof window.NOVA.getRecentChatContacts !== "function"){
    shareChatList.innerHTML = "<div class='share-chat-empty'>Chat share not available.</div>";
    return;
  }
  const { contacts, error } = await window.NOVA.getRecentChatContacts(24);
  if(error){
    shareChatList.innerHTML = "<div class='share-chat-empty'>Unable to load chats.</div>";
    return;
  }
  renderShareChatTargets(contacts || []);
}

async function sendShareToChat(userId, sendBtn){
  if(!activeShareItem || !userId) return;
  if(!window.NOVA || typeof window.NOVA.sendChatMessage !== "function"){
    alert("Chat share not available.");
    return;
  }
  if(sendBtn) sendBtn.disabled = true;
  const payload = getSharePayload();
  const { ok } = await window.NOVA.sendChatMessage(userId, payload.message);
  if(!ok){
    if(sendBtn) sendBtn.disabled = false;
    alert("Unable to send in chat.");
    return;
  }
  if(activeShareItem?.id){
    const tracked = await trackShare("post", activeShareItem.id, "chat");
    const next = tracked.count;
    if(activeShareCountEl){
      activeShareCountEl.textContent = formatCount(next);
    }
    queuePostCountRefresh(activeShareItem.id);
  }
  if(sendBtn){
    sendBtn.textContent = "Sent";
  }
}

async function handleShareAction(action){
  if(!activeShareItem) return;
  const payload = getSharePayload();
  const text = payload.text;
  const shareUrl = payload.url;
  let shared = false;

  if(action === "native"){
    if(navigator.share){
      try{
        await navigator.share({ title:"NOVAGAPP", text, url: shareUrl });
        shared = true;
      }catch(_){
        return;
      }
    }else{
      action = "copy";
    }
  }

  if(action === "copy"){
    try{
      if(navigator.clipboard && navigator.clipboard.writeText){
        await navigator.clipboard.writeText(shareUrl);
        alert("Link copied");
      }else{
        window.prompt("Copy this link", shareUrl);
      }
    }catch(_){
      window.prompt("Copy this link", shareUrl);
    }
    shared = true;
  }else if(action === "whatsapp"){
    const msg = encodeURIComponent(text + "\n" + shareUrl);
    window.open(`https://wa.me/?text=${msg}`, "_blank");
    shared = true;
  }else if(action === "telegram"){
    const msg = encodeURIComponent(text);
    const url = encodeURIComponent(shareUrl);
    window.open(`https://t.me/share/url?url=${url}&text=${msg}`, "_blank");
    shared = true;
  }

  if(shared && activeShareItem?.id){
    const tracked = await trackShare("post", activeShareItem.id, action || "share");
    const next = tracked.count;
    if(activeShareCountEl){
      activeShareCountEl.textContent = formatCount(next);
    }
    queuePostCountRefresh(activeShareItem.id);
  }

  closeShareSheet();
}

async function renderPost(item, options){
  if(!item || !item.id) return;
  const renderKey = String(item.group_key || item.id);
  if(renderedPostIds.has(renderKey)) return;
  renderedPostIds.add(renderKey);

  const opts = options || {};
  const prepend = !!opts.prepend;

  const post = document.createElement("div");
  post.className = "post";
  post.dataset.id = String(item.id || "");
  post.dataset.targetType = "post";
  const targetType = "post";
  const author = await getPostAuthorInfo(item.user_id);
  const timeLabel = formatRelativeTime(item.created_at) || "now";
  const media = renderPostMediaHtml(item);
  const music = renderPostMusicHtml(item);
  const heartIcon = "<svg viewBox='0 0 24 24' aria-hidden='true'><path d='M12 21s-6.7-4.35-9.33-7.25A5.5 5.5 0 0 1 12 6.43a5.5 5.5 0 0 1 9.33 7.32C18.7 16.65 12 21 12 21z'/></svg>";
  const dislikeIcon = "<svg viewBox='0 0 24 24' aria-hidden='true'><path d='M10 20v-7m0 0h8l-1-9H8L6 13h4zM6 13H4'/></svg>";
  const commentIcon = "<svg viewBox='0 0 24 24' aria-hidden='true'><path d='M21 14a4 4 0 0 1-4 4H8l-5 3V6a4 4 0 0 1 4-4h10a4 4 0 0 1 4 4z'/></svg>";
  const shareIcon = "<svg viewBox='0 0 24 24' aria-hidden='true'><path d='M14 3h7v7M10 14 21 3M21 14v7h-7M3 10V3h7'/></svg>";
  const saveIcon = "<svg viewBox='0 0 24 24' aria-hidden='true'><path d='M6 3h12v18l-6-3-6 3z'/></svg>";

  post.innerHTML = `
    <div class="post-head">
      <div class="post-author">
        <button type="button" class="post-author-avatar-btn" data-open-story-user="${escapeHtml(String(item.user_id || ""))}" aria-label="Open story">
          <img src="${escapeHtml(author.avatarUrl)}" alt="">
        </button>
        <div class="post-author-meta" data-open-profile="${escapeHtml(String(item.user_id || ""))}">
          <div class="post-author-name">${escapeHtml(author.name)}</div>
          <div class="post-author-time">${escapeHtml(timeLabel)}</div>
          ${music}
        </div>
      </div>
      <button class="post-more" type="button" aria-label="More">...</button>
    </div>
    ${media}
    <div class="actions">
      <div class="actions-left">
        <button class="action-btn" data-action="like" aria-label="Like"><span class="icon">${heartIcon}</span><span class="count" data-like>0</span></button>
        <button class="action-btn" data-action="dislike" aria-label="Dislike"><span class="icon">${dislikeIcon}</span><span class="count" data-dislike>0</span></button>
        <button class="action-btn" data-action="comment" aria-label="Comment"><span class="icon">${commentIcon}</span><span class="count" data-comment>0</span></button>
        <button class="action-btn" data-action="share" aria-label="Share"><span class="icon">${shareIcon}</span><span class="count" data-share-count>0</span></button>
      </div>
      <button class="action-btn" data-action="save" aria-label="Save"><span class="icon">${saveIcon}</span></button>
    </div>
    <div class="caption">
      <strong>${escapeHtml(item.title || "Post")}</strong><br>
      ${escapeHtml(item.description || "")}
    </div>
  `;

  if(prepend && feed.firstChild){
    feed.insertBefore(post, feed.firstChild);
  }else{
    feed.appendChild(post);
  }
  initPostCarousel(post);
  initMusicStrip(post);

  if(itemIncludesPostId(item, focusPostId)){
    focusPostElement(post);
  }

  const likeEl = post.querySelector("[data-like]");
  const dislikeEl = post.querySelector("[data-dislike]");
  const commentEl = post.querySelector("[data-comment]");
  const shareCountEl = post.querySelector("[data-share-count]");
  const likeBtn = post.querySelector("[data-action='like']");
  const dislikeBtn = post.querySelector("[data-action='dislike']");
  const saveBtn = post.querySelector("[data-action='save']");
  const authorAvatarBtn = post.querySelector("[data-open-story-user]");
  const authorMeta = post.querySelector("[data-open-profile]");

  if(authorAvatarBtn && item.user_id){
    authorAvatarBtn.addEventListener("click", async e => {
      e.stopPropagation();
      const opened = await openUserStories(String(item.user_id || ""), { fallbackToProfile:false });
      if(!opened){
        location.href = `m-account.html?uid=${encodeURIComponent(String(item.user_id || ""))}`;
      }
    });
  }
  if(authorMeta && item.user_id){
    authorMeta.addEventListener("click", e => {
      e.stopPropagation();
      location.href = `m-account.html?uid=${encodeURIComponent(String(item.user_id || ""))}`;
    });
  }

  const counts = await window.NOVA.getReactionCounts(targetType, item.id);
  const comments = await window.NOVA.getCommentCount(targetType, item.id, {
    ownerUserId: String(item.user_id || "")
  });
  const shares = await getShareCount(targetType, item.id);
  const myReaction = await getMyReaction(targetType, item.id);
  likeEl.textContent = formatCount(counts.likes);
  dislikeEl.textContent = formatCount(counts.dislikes);
  commentEl.textContent = formatCount(comments);
  if(shareCountEl){
    shareCountEl.textContent = formatCount(shares);
  }
  registerPostCountRefs(item.id, item.user_id, targetType, {
    likeEl,
    dislikeEl,
    commentEl,
    shareCountEl,
    likeBtn,
    dislikeBtn
  });
  applySaveUi(isPostSaved(item.id), saveBtn);
  applyReactionUi(myReaction, likeBtn, dislikeBtn);

  post.querySelector("[data-action='like']").onclick = async e => {
    e.stopPropagation();
    const res = await window.NOVA.toggleReaction(targetType, item.id, "like");
    await queuePostCountRefresh(item.id);
    applyReactionUi(res?.reaction || null, likeBtn, dislikeBtn);
  };
  post.querySelector("[data-action='dislike']").onclick = async e => {
    e.stopPropagation();
    const res = await window.NOVA.toggleReaction(targetType, item.id, "dislike");
    await queuePostCountRefresh(item.id);
    applyReactionUi(res?.reaction || null, likeBtn, dislikeBtn);
  };
  post.querySelector("[data-action='comment']").onclick = e => {
    e.stopPropagation();
    openComments(item.id, commentEl, item.user_id, targetType);
  };
  post.querySelector("[data-action='share']").onclick = e => {
    e.stopPropagation();
    openShareSheet(item, shareCountEl);
  };
  post.querySelector("[data-action='save']").onclick = e => {
    e.stopPropagation();
    const saved = toggleSavePost(item.id);
    applySaveUi(saved, saveBtn);
  };
}

function isVideoPost(item){
  const mediaType = String(item?.media_type || "").toLowerCase();
  if(mediaType === "video") return true;
  const primaryUrl = Array.isArray(item?.media_urls) && item.media_urls.length
    ? item.media_urls[0]
    : item?.media_url;
  return mediaTypeFromUrl(primaryUrl) === "video";
}

function focusPostElement(post){
  if(!post || focusApplied) return;
  focusApplied = true;
  requestAnimationFrame(() => {
    post.scrollIntoView({ behavior:"smooth", block:"center" });
    const video = post.querySelector("video");
    if(video){
      video.play().catch(()=>{});
    }
  });
}

async function ensureFocusPostLoaded(){
  if(!focusPostId) return;

  const { data, error } = await fetchPostByIdWithFallback(focusPostId);
  if(error || !data || !data.id) return;
  await renderPost(data, { prepend:true });
}

commentsBackdrop.addEventListener("click", closeComments);
commentsClose.addEventListener("click", closeComments);
commentSend.addEventListener("click", sendComment);
commentReplyCancel.addEventListener("click", clearCommentDraftState);
commentInput.addEventListener("keydown", e => {
  if(e.key === "Enter") sendComment();
});
document.addEventListener("click", e => {
  if(!e.target.closest(".comment-menu-wrap")){
    closeCommentMenus();
  }
});

shareBackdrop.addEventListener("click", closeShareSheet);
shareClose.addEventListener("click", closeShareSheet);
shareButtons.forEach(btn => {
  btn.addEventListener("click", () => handleShareAction(btn.getAttribute("data-share") || ""));
});
shareChatList.addEventListener("click", e => {
  const btn = e.target.closest("[data-send-chat]");
  if(!btn) return;
  const targetId = btn.getAttribute("data-send-chat") || "";
  sendShareToChat(targetId, btn);
});

storyComposerCloseBtn.addEventListener("click", () => {
  if(storyComposerState.busy) return;
  closeStoryComposer();
});
storyAddMoreBtn.addEventListener("click", () => {
  if(storyComposerState.busy) return;
  storyInput.value = "";
  storyInput.click();
});
storyPublishBtn.addEventListener("click", publishStorySequence);
storyCaptionInput.addEventListener("input", () => {
  const active = getActiveStoryDraft();
  if(!active) return;
  active.caption = storyCaptionInput.value || "";
});
storyInfoInput.addEventListener("input", () => {
  const active = getActiveStoryDraft();
  if(!active) return;
  active.info = storyInfoInput.value || "";
});
storyFilterList.addEventListener("click", e => {
  const chip = e.target.closest("[data-story-filter]");
  if(!chip) return;
  const active = getActiveStoryDraft();
  if(!active) return;
  active.filter = normalizeStoryFilter(chip.getAttribute("data-story-filter") || "none");
  renderStoryComposer();
});
storyThumbList.addEventListener("click", e => {
  const thumb = e.target.closest("[data-story-thumb]");
  if(!thumb) return;
  const idx = Number(thumb.getAttribute("data-story-thumb") || 0);
  if(!Number.isFinite(idx)) return;
  storyComposerState.activeIndex = Math.max(0, Math.min(storyComposerState.items.length - 1, idx));
  renderStoryComposer();
});
storyPickMusicBtn.addEventListener("click", async () => {
  if(storyComposerState.busy) return;
  const active = getActiveStoryDraft();
  if(!active) return;
  const music = await pickStoryMusicMeta();
  if(!music) return;
  active.music = music;
  renderStoryComposer();
});
storyClearMusicBtn.addEventListener("click", () => {
  if(storyComposerState.busy) return;
  const active = getActiveStoryDraft();
  if(!active) return;
  active.music = null;
  renderStoryComposer();
});

storyCloseBtn.addEventListener("click", closeStory);
storyPrevBtn.addEventListener("click", e => {
  e.stopPropagation();
  goToNextStory(-1);
});
storyNextBtn.addEventListener("click", e => {
  e.stopPropagation();
  goToNextStory(1);
});
storySeenByBtn.addEventListener("click", async () => {
  const activeStory = (storyPlaybackState.list || [])[storyPlaybackState.index];
  if(!activeStory) return;
  storyPlaybackState.viewersOpen = !storyPlaybackState.viewersOpen;
  if(storyPlaybackState.viewersOpen){
    storyViewersPanel.hidden = false;
    await renderStoryViewers(activeStory.id);
  }else{
    storyViewersPanel.hidden = true;
    storyViewersPanel.innerHTML = "";
  }
});
storyViewer.addEventListener("click", e => {
  if(e.target === storyViewer){
    closeStory();
  }
});
storyComposer.addEventListener("click", e => {
  if(e.target !== storyComposer) return;
  if(storyComposerState.busy) return;
  closeStoryComposer();
});

postComposerCloseBtn.addEventListener("click", () => {
  if(postComposerState.busy) return;
  closePostComposer();
});
postComposer.addEventListener("click", e => {
  if(e.target !== postComposer) return;
  if(postComposerState.busy) return;
  closePostComposer();
});
postAddMediaBtn.addEventListener("click", () => {
  if(postComposerState.busy) return;
  postInput.value = "";
  postInput.click();
});
postShootVideoBtn.addEventListener("click", () => {
  if(postComposerState.busy) return;
  postCameraInput.value = "";
  postCameraInput.click();
});
postPublishBtn.addEventListener("click", publishPostFromComposer);
postResetBtn.addEventListener("click", () => {
  if(postComposerState.busy) return;
  clearPostComposerState();
  postComposerState.open = true;
  renderPostComposer();
});
postTextModeInput.addEventListener("change", () => {
  postComposerState.textMode = !!postTextModeInput.checked;
  if(postComposerState.textMode && postComposerState.items.length){
    postComposerState.items.forEach(item => releasePostDraft(item));
    postComposerState.items = [];
    postComposerState.activeIndex = 0;
  }
  setPostComposerStatus(postComposerState.textMode ? "Text mode enabled." : "");
  renderPostComposer();
});
postTitleInput.addEventListener("input", () => {
  postComposerState.title = postTitleInput.value || "";
  if(postComposerState.textMode) renderPostComposerPreview();
});
postDescriptionInput.addEventListener("input", () => {
  postComposerState.description = postDescriptionInput.value || "";
  if(postComposerState.textMode) renderPostComposerPreview();
});
postKeywordsInput.addEventListener("input", () => {
  postComposerState.keywords = postKeywordsInput.value || "";
});
postFilterList.addEventListener("click", e => {
  const chip = e.target.closest("[data-post-filter]");
  if(!chip) return;
  const next = normalizePostFilter(chip.getAttribute("data-post-filter") || "none");
  const active = getActivePostDraft();
  postComposerState.defaultFilter = next;
  if(active){
    active.filter = next;
  }
  renderPostComposer();
});
postMediaQueue.addEventListener("click", e => {
  const thumb = e.target.closest("[data-post-thumb]");
  if(!thumb) return;
  const idx = Number(thumb.getAttribute("data-post-thumb") || 0);
  if(!Number.isFinite(idx)) return;
  postComposerState.activeIndex = Math.max(0, Math.min(postComposerState.items.length - 1, idx));
  renderPostComposer();
});
postMusicSearchBtn.addEventListener("click", async () => {
  const query = String(postMusicSearchInput.value || "").trim();
  if(!query){
    setPostComposerStatus("Type music name first.");
    return;
  }
  setPostComposerStatus("Searching music...");
  try{
    const rows = await searchMusicMeta(query);
    renderPostComposerMusicResults(rows.slice(0, 10));
    setPostComposerStatus(rows.length ? "Select one track." : "No track found.");
  }catch(err){
    console.error("music search failed", err);
    setPostComposerStatus("Music search failed.");
  }
});
postMusicSearchInput.addEventListener("keydown", e => {
  if(e.key !== "Enter") return;
  e.preventDefault();
  postMusicSearchBtn.click();
});
postMusicClearBtn.addEventListener("click", () => {
  postComposerState.music = null;
  if(postMusicResults){
    postMusicResults.innerHTML = "";
    postMusicResults.dataset.rows = "[]";
  }
  renderPostComposerForm();
});
postMusicResults.addEventListener("click", e => {
  const btn = e.target.closest("[data-post-music]");
  if(!btn) return;
  let rows = [];
  try{
    rows = JSON.parse(postMusicResults.dataset.rows || "[]");
  }catch(_){
    rows = [];
  }
  const idx = Number(btn.getAttribute("data-post-music") || -1);
  if(!Number.isFinite(idx) || idx < 0 || idx >= rows.length) return;
  postComposerState.music = rows[idx];
  renderPostComposerForm();
  setPostComposerStatus("Music selected.");
});
document.addEventListener("keydown", e => {
  if(e.key === "Escape"){
    if(postComposer.classList.contains("active")){
      if(postComposerState.busy) return;
      closePostComposer();
      return;
    }
    if(storyComposer.classList.contains("active")){
      if(storyComposerState.busy) return;
      closeStoryComposer();
      return;
    }
    if(storyViewer.classList.contains("active")){
      closeStory();
      return;
    }
  }
  if(!storyViewer.classList.contains("active")) return;
  if(e.key === "ArrowLeft"){
    e.preventDefault();
    goToNextStory(-1);
  }else if(e.key === "ArrowRight"){
    e.preventDefault();
    goToNextStory(1);
  }
});

const scrollObserver = new IntersectionObserver(entries => {
  if(entries[0].isIntersecting){
    loadMore();
  }
});
scrollObserver.observe(sentinel);

function go(p){
  location.href = p;
}
