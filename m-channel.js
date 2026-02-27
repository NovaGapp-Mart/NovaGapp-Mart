(function(){
  "use strict";

  const VIDEO_SELECT = "id,user_id,title,description,video_url,thumbnail_url,views,likes_count,dislikes_count,created_at,category,tags,duration_seconds";

  const state = {
    supa:null,
    me:null,
    channelId:"",
    owner:false,
    channel:{ id:"", name:"User", avatar:"" },
    followers:0,
    members:0,
    joinFeeInr:0,
    videos:[],
    timers:{}
  };

  const dom = {
    backBtn:document.getElementById("mcBackToVideos"),
    uploadBtn:document.getElementById("mcOpenUpload"),
    uploadInline:document.getElementById("mcOpenUploadInline"),
    refreshBtn:document.getElementById("mcRefreshBtn"),
    avatar:document.getElementById("mcAvatar"),
    name:document.getElementById("mcName"),
    meta:document.getElementById("mcMeta"),
    ownerNote:document.getElementById("mcOwnerNote"),
    ownerActions:document.getElementById("mcOwnerActions"),
    list:document.getElementById("mcVideoList"),
    empty:document.getElementById("mcEmpty"),
    toast:document.getElementById("mcToast")
  };

  const util = {
    safe(v){ return String(v || "").trim(); },
    num(v){ const n = Number(v); return Number.isFinite(n) ? n : 0; },
    esc(v){ return util.safe(v).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/\"/g,"&quot;").replace(/'/g,"&#39;"); },
    compact(v){ const n = Math.max(0, util.num(v)); if(n >= 1e9) return (n / 1e9).toFixed(1).replace(/\.0$/,"") + "B"; if(n >= 1e6) return (n / 1e6).toFixed(1).replace(/\.0$/,"") + "M"; if(n >= 1e3) return (n / 1e3).toFixed(1).replace(/\.0$/,"") + "K"; return String(Math.round(n)); },
    ago(v){ const ts = Date.parse(util.safe(v)); if(!Number.isFinite(ts)) return ""; const sec = Math.max(1, Math.floor((Date.now() - ts) / 1000)); if(sec < 60) return sec + "s ago"; if(sec < 3600) return Math.floor(sec / 60) + "m ago"; if(sec < 86400) return Math.floor(sec / 3600) + "h ago"; if(sec < 604800) return Math.floor(sec / 86400) + "d ago"; return new Date(ts).toLocaleDateString(undefined, { month:"short", day:"numeric" }); },
    duration(v){ const sec = Math.max(0, Math.floor(util.num(v))); const h = Math.floor(sec / 3600); const m = Math.floor((sec % 3600) / 60); const s = sec % 60; if(h > 0) return h + ":" + String(m).padStart(2, "0") + ":" + String(s).padStart(2, "0"); return m + ":" + String(s).padStart(2, "0"); }
  };

  function showToast(message, isError){
    if(!dom.toast) return;
    dom.toast.textContent = util.safe(message);
    dom.toast.style.background = isError ? "#9f1d1d" : "#111";
    dom.toast.classList.add("show");
    clearTimeout(state.timers.toast);
    state.timers.toast = setTimeout(() => dom.toast.classList.remove("show"), 2200);
  }

  function avatarHtml(channel){
    const name = util.safe(channel?.name) || "U";
    const avatarUrl = util.safe(channel?.avatar);
    if(avatarUrl) return '<img src="' + util.esc(avatarUrl) + '" alt="' + util.esc(name) + '">';
    return util.esc(name.slice(0, 1).toUpperCase());
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
      created_at:util.safe(row?.created_at),
      category:util.safe(row?.category) || "General",
      tags:tagsRaw,
      duration_seconds:Math.max(0, util.num(row?.duration_seconds))
    };
  }

  function parseChannelIdFromUrl(){
    const params = new URLSearchParams(location.search);
    return util.safe(params.get("uid"));
  }

  async function waitForSupabase(timeoutMs){
    const start = Date.now();
    const timeout = Math.max(2000, util.num(timeoutMs) || 10000);
    while(Date.now() - start < timeout){
      if(window.NOVA && window.NOVA.supa) return window.NOVA.supa;
      await new Promise(resolve => setTimeout(resolve, 80));
    }
    return null;
  }

  async function ensureAuth(){
    if(state.me && state.me.id) return state.me;
    if(window.NOVA && typeof window.NOVA.getUser === "function"){
      try{
        const user = await window.NOVA.getUser({ silent:true });
        if(user && user.id){
          state.me = user;
          return user;
        }
      }catch(_){ }
    }
    return null;
  }

  async function requireAuth(){
    const existing = await ensureAuth();
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
  }

  async function fetchChannelProfile(userId){
    const id = util.safe(userId);
    if(!id) return { id:"", name:"User", avatar:"" };
    const attempts = [
      { idField:"user_id", select:"user_id,username,full_name,photo" },
      { idField:"id", select:"id,username,full_name,photo" }
    ];
    for(const plan of attempts){
      try{
        const { data, error } = await state.supa
          .from("users")
          .select(plan.select)
          .eq(plan.idField, id)
          .maybeSingle();
        if(error) continue;
        if(data){
          return {
            id,
            name:util.safe(data.full_name) || util.safe(data.username) || "User",
            avatar:util.safe(data.photo)
          };
        }
      }catch(_){ }
    }
    return { id, name:"User", avatar:"" };
  }

  async function fetchFollowerCount(channelId){
    const id = util.safe(channelId);
    if(!id) return 0;
    try{
      const { count, error } = await state.supa
        .from("channel_subscribers")
        .select("subscriber_user_id", { count:"exact", head:true })
        .eq("channel_id", id);
      if(!error) return Math.max(0, util.num(count));
    }catch(_){ }
    try{
      const { count, error } = await state.supa
        .from("follows")
        .select("follower_id", { count:"exact", head:true })
        .eq("following_id", id);
      if(!error) return Math.max(0, util.num(count));
    }catch(_){ }
    return 0;
  }

  async function fetchMemberCount(channelId){
    const id = util.safe(channelId);
    if(!id) return 0;
    try{
      const { count, error } = await state.supa
        .from("channel_members")
        .select("member_user_id", { count:"exact", head:true })
        .eq("channel_id", id);
      if(!error) return Math.max(0, util.num(count));
    }catch(_){ }
    return fetchFollowerCount(id);
  }

  async function fetchJoinFee(channelId){
    const id = util.safe(channelId);
    if(!id) return 0;
    try{
      const { data, error } = await state.supa
        .from("channel_membership_plans")
        .select("join_fee_inr")
        .eq("channel_id", id)
        .maybeSingle();
      if(error || !data) return 0;
      return Math.max(0, Math.floor(util.num(data.join_fee_inr)));
    }catch(_){
      return 0;
    }
  }

  async function fetchChannelVideos(channelId){
    const id = util.safe(channelId);
    if(!id) return [];
    const { data, error } = await state.supa
      .from("videos")
      .select(VIDEO_SELECT)
      .eq("user_id", id)
      .order("created_at", { ascending:false })
      .limit(500);
    if(error) throw error;
    return (data || []).map(normalizeVideo);
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

  async function updateVideoWithAdaptiveTags(videoId, payload, tagList){
    const base = { ...payload };
    const tags = Array.isArray(tagList) ? tagList : [];
    let result = await state.supa
      .from("videos")
      .update({ ...base, tags })
      .eq("id", videoId)
      .eq("user_id", state.me.id)
      .select(VIDEO_SELECT)
      .maybeSingle();
    if(!result.error){
      return result;
    }
    if(!shouldRetryTagsAsText(result.error)){
      return result;
    }
    result = await state.supa
      .from("videos")
      .update({ ...base, tags:tags.join(", ") })
      .eq("id", videoId)
      .eq("user_id", state.me.id)
      .select(VIDEO_SELECT)
      .maybeSingle();
    return result;
  }

  function parseStoragePath(publicUrl, bucket){
    const url = util.safe(publicUrl);
    const bucketName = util.safe(bucket);
    if(!url || !bucketName) return "";
    const marker = "/storage/v1/object/public/" + bucketName + "/";
    const index = url.indexOf(marker);
    if(index >= 0){
      return decodeURIComponent(url.slice(index + marker.length));
    }
    try{
      const parsed = new URL(url);
      const path = String(parsed.pathname || "");
      const i = path.indexOf(marker);
      if(i >= 0){
        return decodeURIComponent(path.slice(i + marker.length));
      }
    }catch(_){ }
    return "";
  }

  async function cleanupStorageForVideo(video){
    const item = video || {};
    const videoPath = parseStoragePath(item.video_url, "long_videos");
    const thumbPath = parseStoragePath(item.thumbnail_url, "thumbnails");
    if(videoPath){
      try{ await state.supa.storage.from("long_videos").remove([videoPath]); }catch(_){ }
    }
    if(thumbPath){
      try{ await state.supa.storage.from("thumbnails").remove([thumbPath]); }catch(_){ }
    }
  }

  function renderChannelHeader(){
    dom.avatar.innerHTML = avatarHtml(state.channel);
    dom.name.textContent = state.channel.name;
    const feeText = state.joinFeeInr > 0 ? ("Join INR " + state.joinFeeInr) : "Join Free";
    dom.meta.textContent = util.compact(state.followers) + " followers . " + util.compact(state.members) + " members . " + feeText;
    dom.ownerNote.classList.toggle("mc-hidden", !state.owner);
    dom.ownerActions.classList.toggle("mc-hidden", !state.owner);
  }

  function renderVideos(){
    dom.list.innerHTML = "";
    dom.empty.classList.toggle("mc-hidden", state.videos.length > 0);
    if(!state.videos.length){
      return;
    }

    state.videos.forEach(video => {
      const card = document.createElement("article");
      card.className = "mc-card";
      card.innerHTML =
        '<button type="button" class="mc-thumb" data-action="watch">' +
          '<div class="mc-thumb-wrap">' +
            '<img src="' + util.esc(video.thumbnail_url || "Images/no-image.jpg") + '" alt="' + util.esc(video.title) + '">' +
            '<span class="mc-duration">' + util.esc(util.duration(video.duration_seconds)) + '</span>' +
          '</div>' +
        '</button>' +
        '<div class="mc-card-body">' +
          '<h3 class="mc-title">' + util.esc(video.title) + '</h3>' +
          '<p class="mc-stats">' + util.esc(util.compact(video.views) + " views . " + util.ago(video.created_at)) + '</p>' +
          '<div class="mc-actions">' +
            '<button type="button" data-action="watch">Watch</button>' +
            (state.owner ? '<button type="button" data-action="edit">Edit</button><button type="button" class="danger" data-action="delete">Delete</button>' : '') +
          '</div>' +
        '</div>';

      card.querySelectorAll("[data-action='watch']").forEach(btn => {
        btn.addEventListener("click", () => {
          const url = new URL("m-videos.html", location.href);
          url.searchParams.set("v", video.id);
          location.href = url.pathname + url.search;
        });
      });

      if(state.owner){
        const editBtn = card.querySelector("[data-action='edit']");
        const deleteBtn = card.querySelector("[data-action='delete']");
        if(editBtn) editBtn.addEventListener("click", () => editVideo(video.id));
        if(deleteBtn) deleteBtn.addEventListener("click", () => deleteVideo(video.id));
      }

      dom.list.appendChild(card);
    });
  }

  async function editVideo(videoId){
    if(!state.owner || !state.me) return;
    const id = util.safe(videoId);
    const current = state.videos.find(item => item.id === id);
    if(!current) return;

    const titleRaw = window.prompt("Edit title", current.title);
    if(titleRaw === null) return;
    const descriptionRaw = window.prompt("Edit description", current.description || "");
    if(descriptionRaw === null) return;
    const categoryRaw = window.prompt("Edit category", current.category || "General");
    if(categoryRaw === null) return;
    const tagsRaw = window.prompt("Edit tags (comma separated)", current.tags || "");
    if(tagsRaw === null) return;

    const title = util.safe(titleRaw);
    if(!title){
      showToast("Title cannot be empty.", true);
      return;
    }

    const tags = util.safe(tagsRaw).split(",").map(item => item.trim()).filter(Boolean).slice(0, 20);
    const payload = {
      title,
      description:util.safe(descriptionRaw),
      category:util.safe(categoryRaw) || "General"
    };

    const { data, error } = await updateVideoWithAdaptiveTags(id, payload, tags);
    if(error){
      console.error("channel_edit_video_failed", error);
      showToast("Unable to edit video.", true);
      return;
    }

    const next = normalizeVideo(data || { ...current, ...payload, tags:tags.join(", ") });
    state.videos = state.videos.map(item => item.id === id ? next : item);
    renderVideos();
    showToast("Video updated.");
  }

  async function deleteVideo(videoId){
    if(!state.owner || !state.me) return;
    const id = util.safe(videoId);
    if(!id) return;
    const item = state.videos.find(video => video.id === id);
    if(!item) return;
    if(!window.confirm("Delete this video permanently?")) return;

    const { error } = await state.supa
      .from("videos")
      .delete()
      .eq("id", id)
      .eq("user_id", state.me.id);
    if(error){
      console.error("channel_delete_video_failed", error);
      showToast("Unable to delete video.", true);
      return;
    }

    await cleanupStorageForVideo(item);
    state.videos = state.videos.filter(video => video.id !== id);
    renderVideos();
    showToast("Video deleted.");
  }

  async function loadChannel(){
    state.me = await ensureAuth();
    let channelId = parseChannelIdFromUrl();
    if(!channelId){
      const me = await requireAuth();
      if(!me || !me.id) return;
      channelId = util.safe(me.id);
      const url = new URL(location.href);
      url.searchParams.set("uid", channelId);
      history.replaceState({}, "", url.pathname + url.search);
    }

    state.channelId = channelId;
    state.owner = !!(state.me && util.safe(state.me.id) === channelId);

    state.channel = await fetchChannelProfile(channelId);
    const [followers, members, fee, videos] = await Promise.all([
      fetchFollowerCount(channelId),
      fetchMemberCount(channelId),
      fetchJoinFee(channelId),
      fetchChannelVideos(channelId)
    ]);
    state.followers = Math.max(0, util.num(followers));
    state.members = Math.max(0, util.num(members));
    state.joinFeeInr = Math.max(0, Math.floor(util.num(fee)));
    state.videos = videos;

    renderChannelHeader();
    renderVideos();
  }

  function bindEvents(){
    dom.backBtn.addEventListener("click", () => { location.href = "m-videos.html"; });
    dom.uploadBtn.addEventListener("click", () => { location.href = "m-videos.html?view=upload"; });
    dom.uploadInline.addEventListener("click", () => { location.href = "m-videos.html?view=upload"; });
    dom.refreshBtn.addEventListener("click", () => loadChannel().catch(err => {
      console.error("channel_refresh_failed", err);
      showToast("Unable to refresh channel.", true);
    }));
  }

  async function init(){
    bindEvents();
    state.supa = await waitForSupabase(10000);
    if(!state.supa){
      showToast("Supabase init failed. Reload page.", true);
      return;
    }
    try{
      await loadChannel();
    }catch(err){
      console.error("channel_load_failed", err);
      showToast("Unable to load channel.", true);
    }
  }

  init();
})();
