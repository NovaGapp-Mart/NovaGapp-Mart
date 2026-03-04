(function(){
  const supa = window.supa || (typeof window.novaCreateSupabaseClient === "function" ? window.novaCreateSupabaseClient() : null);
  const params = new URLSearchParams(location.search);
  const receiverId = String(params.get("id") || "").trim();
  const isGroupChat = String(params.get("isGroup") || "").trim().toLowerCase() === "true";
  const receiverNameParam = decodeURIComponent(params.get("name") || "User");
  const receiverPicParam = String(params.get("img") || "").trim();
  const autoAnswer = {
    enabled: String(params.get("call_action") || "").trim().toLowerCase() === "answer",
    callId: String(params.get("call_id") || "").trim()
  };

  const msgHolder = document.getElementById("msgHolder");
  const chatWindow = document.getElementById("chatWindow");
  const mainInput = document.getElementById("mainInput");
  const sendBtn = document.getElementById("sendBtn");
  const audioCallBtn = document.getElementById("audioCallBtn");
  const videoCallBtn = document.getElementById("videoCallBtn");
  const chatHeaderMenu = document.getElementById("chatHeaderMenu");
  const filterBanner = document.getElementById("filterBanner");
  const blockedBanner = document.getElementById("blockedBanner");
  const replyPreview = document.getElementById("replyPreview");
  const replyPreviewText = document.getElementById("replyPreviewText");
  const messageContextMenu = document.getElementById("messageContextMenu");
  const deleteChoiceModal = document.getElementById("deleteChoiceModal");
  const mediaPreviewModal = document.getElementById("mediaPreviewModal");
  const mediaPreviewImage = document.getElementById("mediaPreviewImage");
  const mediaPreviewVideo = document.getElementById("mediaPreviewVideo");
  const mediaPreviewDownloadBtn = document.getElementById("mediaPreviewDownloadBtn");
  const incomingCallBar = document.getElementById("incomingCallBar");
  const incomingCallText = document.getElementById("incomingCallText");
  const callOverlay = document.getElementById("callOverlay");
  const callTitle = document.getElementById("callTitle");
  const callSub = document.getElementById("callSub");
  const remoteVideo = document.getElementById("remoteVideo");
  const localVideo = document.getElementById("localVideo");
  const callAudioMask = document.getElementById("callAudioMask");
  const muteCallBtn = document.getElementById("muteCallBtn");
  const cameraCallBtn = document.getElementById("cameraCallBtn");

  const RTC_CONFIG = {
    iceServers: [
      { urls: "stun:stun.l.google.com:19302" },
      { urls: "stun:stun1.l.google.com:19302" }
    ]
  };

  let myUser = null;
  let myId = "";
  let peerProfile = null;
  let renderedMessageIds = new Set();
  let realtimeChannel = null;
  let callPollTimer = null;
  let incomingOfferTimer = null;
  let seenCallSignalIds = new Set();
  let activeCall = null;
  let pendingIncomingOffer = null;
  const bufferedIceByCall = new Map();
  let messageRows = [];
  let localDeletedMessageIds = new Set();
  let chatClearedAt = 0;
  let showingStarredOnly = false;
  let selectedContextMessage = null;
  let pendingReply = null;
  let isPeerBlocked = false;
  let storageDeleteKey = "";
  let storageClearKey = "";
  let storageBlockKey = "";
  let longPressTimer = null;
  let previewMediaUrl = "";
  let previewMediaType = "";

  function isUuid(value){
    return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(String(value || "").trim());
  }

  function maybeMissingColumn(error){
    const text = `${error?.message || ""} ${error?.details || ""}`.toLowerCase();
    return text.includes("column") && text.includes("does not exist");
  }

  function getDisplayName(profile, fallback){
    const row = profile || {};
    return String(row.display_name || row.full_name || row.username || fallback || "User").trim() || "User";
  }

  function getProfilePhoto(profile, fallback){
    const row = profile || {};
    return String(row.photo || row.avatar_url || fallback || "").trim();
  }

  function getInitial(text){
    const clean = String(text || "").trim();
    return clean ? clean[0].toUpperCase() : "U";
  }

  function setAvatar(el, imageUrl, fallbackText){
    if(!el) return;
    const src = String(imageUrl || "").trim();
    if(src){
      el.style.backgroundImage = `url('${src.replace(/'/g, "%27")}')`;
      el.textContent = "";
      return;
    }
    el.style.backgroundImage = "";
    el.textContent = getInitial(fallbackText || "U");
  }

  function formatClock(raw){
    const d = new Date(raw || Date.now());
    if(Number.isNaN(d.getTime())) return "";
    return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  }

  function rememberMessageId(id){
    const key = String(id || "").trim();
    if(!key) return false;
    if(renderedMessageIds.has(key)) return false;
    renderedMessageIds.add(key);
    if(renderedMessageIds.size > 2500){
      renderedMessageIds = new Set(Array.from(renderedMessageIds).slice(-1200));
    }
    return true;
  }

  function rememberCallSignalId(id){
    const key = String(id || "").trim();
    if(!key) return true;
    if(seenCallSignalIds.has(key)) return false;
    seenCallSignalIds.add(key);
    if(seenCallSignalIds.size > 1200){
      seenCallSignalIds = new Set(Array.from(seenCallSignalIds).slice(-600));
    }
    return true;
  }

  function toEpoch(raw){
    if(!raw) return 0;
    const t = new Date(raw).getTime();
    return Number.isFinite(t) ? t : 0;
  }

  function chatStorageId(prefix){
    return `${prefix}:${myId}:${receiverId}`;
  }

  function getStoredArray(key){
    try{
      const parsed = JSON.parse(localStorage.getItem(key) || "[]");
      return Array.isArray(parsed) ? parsed : [];
    }catch(_){
      return [];
    }
  }

  function loadThreadLocalState(){
    storageDeleteKey = chatStorageId("deleted_for_me");
    storageClearKey = chatStorageId("chat_cleared_at");
    storageBlockKey = chatStorageId("blocked_user");
    localDeletedMessageIds = new Set(getStoredArray(storageDeleteKey).map(v => String(v || "").trim()).filter(Boolean));
    chatClearedAt = Number(localStorage.getItem(storageClearKey) || "0") || 0;
    isPeerBlocked = localStorage.getItem(storageBlockKey) === "1";
  }

  function saveDeletedForMeState(){
    localStorage.setItem(storageDeleteKey, JSON.stringify(Array.from(localDeletedMessageIds)));
  }

  function setChatClearedAt(ts){
    chatClearedAt = Number(ts) || Date.now();
    localStorage.setItem(storageClearKey, String(chatClearedAt));
  }

  function setBlockedLocalState(flag){
    isPeerBlocked = !!flag;
    localStorage.setItem(storageBlockKey, isPeerBlocked ? "1" : "0");
  }

  function getMessageId(row){
    return String(row?.id || "").trim();
  }

  function textOrEmpty(v){
    return String(v ?? "").trim();
  }

  function messageSenderName(row){
    if(String(row?.sender_id || "") === myId){
      return getDisplayName(myUser, "You");
    }
    return getDisplayName(peerProfile, receiverNameParam);
  }

  function isMessageDeletedEverywhere(row){
    if(row?.deleted_for_everyone === true) return true;
    const content = textOrEmpty(row?.content);
    return /^this message was deleted by /i.test(content);
  }

  function isMessageStarred(row){
    return row?.is_starred === true || row?.is_starred === 1;
  }

  function getReplySnippet(row){
    return textOrEmpty(row?.reply_preview || row?.reply_text || row?.reply_content);
  }

  function getAttachmentUrl(row){
    return textOrEmpty(row?.attachment_url || row?.media_url || row?.file_url);
  }

  function isImageUrl(url){
    return /\.(png|jpe?g|gif|webp|bmp|svg)$/i.test(String(url || "").split("?")[0]);
  }

  function isVideoUrl(url){
    return /\.(mp4|webm|ogg|mov|m4v)$/i.test(String(url || "").split("?")[0]);
  }

  function getMessageTextForPreview(row){
    const attachment = getAttachmentUrl(row);
    if(attachment){
      return isVideoUrl(attachment) ? "Video attachment" : "Attachment";
    }
    return textOrEmpty(row?.content || row?.message || "Message");
  }

  function getDeletedMessageText(row){
    const deleter = textOrEmpty(row?.deleted_by_name) || messageSenderName(row);
    return `This message was deleted by ${deleter}`;
  }

  function shouldHideRow(row){
    const messageId = getMessageId(row);
    if(messageId && localDeletedMessageIds.has(messageId)) return true;
    const createdAt = toEpoch(row?.created_at);
    if(chatClearedAt && createdAt && createdAt <= chatClearedAt) return true;
    if(showingStarredOnly && !isMessageStarred(row)) return true;
    return false;
  }

  function mediaExtFromMime(type){
    const map = {
      "image/jpeg": "jpg",
      "image/jpg": "jpg",
      "image/png": "png",
      "image/webp": "webp",
      "image/gif": "gif",
      "video/mp4": "mp4",
      "video/webm": "webm",
      "video/ogg": "ogg",
      "video/quicktime": "mov"
    };
    return map[String(type || "").toLowerCase()] || "";
  }

  function inferDownloadName(url, mediaType, mimeType){
    const cleanUrl = String(url || "").split("?")[0];
    const fromPath = cleanUrl.split("/").pop() || "";
    if(fromPath.includes(".")) return fromPath;
    const ext = mediaExtFromMime(mimeType) || (mediaType === "video" ? "mp4" : "jpg");
    return `chat-${Date.now()}.${ext}`;
  }

  function openMediaPreview(url, mediaType){
    const src = String(url || "").trim();
    if(!src) return;
    previewMediaUrl = src;
    previewMediaType = mediaType === "video" ? "video" : "image";
    mediaPreviewModal.style.display = "flex";
    if(previewMediaType === "video"){
      mediaPreviewImage.classList.add("hidden");
      mediaPreviewVideo.classList.remove("hidden");
      mediaPreviewVideo.src = src;
      mediaPreviewVideo.currentTime = 0;
      mediaPreviewVideo.play().catch(() => {});
      return;
    }
    mediaPreviewVideo.pause();
    mediaPreviewVideo.src = "";
    mediaPreviewVideo.classList.add("hidden");
    mediaPreviewImage.classList.remove("hidden");
    mediaPreviewImage.src = src;
  }

  function closeMediaPreview(){
    mediaPreviewModal.style.display = "none";
    previewMediaUrl = "";
    previewMediaType = "";
    mediaPreviewImage.src = "";
    mediaPreviewVideo.pause();
    mediaPreviewVideo.src = "";
  }

  async function downloadPreviewMedia(){
    const url = String(previewMediaUrl || "").trim();
    if(!url) return;
    mediaPreviewDownloadBtn.disabled = true;
    mediaPreviewDownloadBtn.textContent = "Downloading...";
    try{
      const response = await fetch(url, { cache: "no-store" });
      if(!response.ok) throw new Error(`download_http_${response.status}`);
      const blob = await response.blob();
      const blobUrl = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = blobUrl;
      anchor.download = inferDownloadName(url, previewMediaType, blob.type);
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      URL.revokeObjectURL(blobUrl);
    }catch(err){
      console.error("media_download_failed", err);
      const fallbackAnchor = document.createElement("a");
      fallbackAnchor.href = url;
      fallbackAnchor.download = inferDownloadName(url, previewMediaType, "");
      fallbackAnchor.target = "_blank";
      fallbackAnchor.rel = "noopener";
      document.body.appendChild(fallbackAnchor);
      fallbackAnchor.click();
      fallbackAnchor.remove();
    }finally{
      mediaPreviewDownloadBtn.disabled = false;
      mediaPreviewDownloadBtn.textContent = "Download";
    }
  }

  function scrollBottom(){
    chatWindow.scrollTop = chatWindow.scrollHeight + 900;
  }

  function renderPeerHeader(){
    const name = getDisplayName(peerProfile, receiverNameParam || (isGroupChat ? "Group" : "User"));
    const photo = getProfilePhoto(peerProfile, receiverPicParam);
    document.getElementById("userName").textContent = name;
    document.getElementById("infoName").textContent = name;
    setAvatar(document.getElementById("userAvatar"), photo, name);
    setAvatar(document.getElementById("infoPic"), photo, name);
  }

  function applyBlockedUi(){
    blockedBanner.classList.toggle("show", isPeerBlocked);
    mainInput.disabled = isPeerBlocked;
    sendBtn.disabled = isPeerBlocked;
    audioCallBtn.disabled = isPeerBlocked;
    videoCallBtn.disabled = isPeerBlocked;
    mainInput.placeholder = isPeerBlocked ? "User is blocked" : "Type a message";
    sendBtn.classList.toggle("opacity-50", isPeerBlocked);
    audioCallBtn.classList.toggle("opacity-50", isPeerBlocked);
    videoCallBtn.classList.toggle("opacity-50", isPeerBlocked);
  }

  function updateFilterUi(){
    filterBanner.classList.toggle("show", showingStarredOnly);
  }

  function closeContextMenu(){
    messageContextMenu.style.display = "none";
  }

  function openContextMenu(x, y, row){
    selectedContextMessage = row || null;
    const menuWidth = 180;
    const menuHeight = 150;
    const maxX = Math.max(8, window.innerWidth - menuWidth - 8);
    const maxY = Math.max(8, window.innerHeight - menuHeight - 8);
    messageContextMenu.style.left = `${Math.min(Math.max(8, x || 8), maxX)}px`;
    messageContextMenu.style.top = `${Math.min(Math.max(8, y || 8), maxY)}px`;
    messageContextMenu.style.display = "block";
  }

  function bindBubbleContextMenu(bubble, row){
    bubble.addEventListener("contextmenu", (event) => {
      event.preventDefault();
      openContextMenu(event.clientX, event.clientY, row);
    });
    bubble.addEventListener("pointerdown", (event) => {
      if(event.pointerType === "mouse") return;
      if(longPressTimer) clearTimeout(longPressTimer);
      longPressTimer = setTimeout(() => {
        openContextMenu(event.clientX || 20, event.clientY || 20, row);
      }, 520);
    });
    const clearLongPress = () => {
      if(longPressTimer){
        clearTimeout(longPressTimer);
        longPressTimer = null;
      }
    };
    bubble.addEventListener("pointerup", clearLongPress);
    bubble.addEventListener("pointercancel", clearLongPress);
    bubble.addEventListener("pointerleave", clearLongPress);
  }

  function upsertMessageRow(row){
    if(!row || typeof row !== "object") return;
    const id = getMessageId(row);
    if(id){
      const i = messageRows.findIndex(r => getMessageId(r) === id);
      if(i >= 0){
        messageRows[i] = { ...messageRows[i], ...row };
      }else{
        messageRows.push(row);
      }
      return;
    }
    const content = textOrEmpty(row.content || row.message);
    if(!content && !getAttachmentUrl(row)) return;
    const syntheticId = `${row.sender_id || ""}:${row.receiver_id || ""}:${row.created_at || ""}:${content.slice(0, 30)}`;
    if(!rememberMessageId(syntheticId)) return;
    messageRows.push({ ...row, __synthetic_id: syntheticId });
  }

  function resolveReplyRow(row){
    const replyId = textOrEmpty(row?.reply_to_id || row?.reply_to || row?.reply_message_id);
    if(!replyId) return null;
    return messageRows.find(m => getMessageId(m) === replyId) || null;
  }

  function createReplyBlock(row){
    const explicit = getReplySnippet(row);
    const byRef = resolveReplyRow(row);
    const text = explicit || (byRef ? getMessageTextForPreview(byRef) : "");
    if(!text) return null;
    const replyBox = document.createElement("div");
    replyBox.className = "text-[11px] bg-black/5 rounded px-2 py-1 mb-1 italic";
    replyBox.textContent = text;
    return replyBox;
  }

  function createAttachmentBlock(row){
    const attachment = getAttachmentUrl(row);
    if(!attachment) return null;
    const box = document.createElement("div");
    box.className = "attachment";
    box.style.cursor = "pointer";
    if(isImageUrl(attachment)){
      const img = document.createElement("img");
      img.src = attachment;
      img.alt = "attachment";
      img.loading = "lazy";
      img.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        openMediaPreview(attachment, "image");
      });
      box.appendChild(img);
      return box;
    }
    if(isVideoUrl(attachment)){
      const video = document.createElement("video");
      video.src = attachment;
      video.muted = true;
      video.playsInline = true;
      video.preload = "metadata";
      video.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        openMediaPreview(attachment, "video");
      });
      box.appendChild(video);
      return box;
    }
    const link = document.createElement("a");
    link.href = attachment;
    link.target = "_blank";
    link.rel = "noopener";
    link.className = "text-xs text-blue-700 underline break-all";
    link.textContent = attachment;
    link.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      const type = isVideoUrl(attachment) ? "video" : "image";
      openMediaPreview(attachment, type);
    });
    box.appendChild(link);
    return box;
  }

  function createMessageMainContent(row, outgoing){
    const contentWrap = document.createElement("div");
    if(!outgoing){
      const sender = document.createElement("div");
      sender.className = "text-[10px] text-orange-700 font-semibold";
      sender.textContent = getDisplayName(peerProfile, receiverNameParam);
      contentWrap.appendChild(sender);
    }

    const replyBox = createReplyBlock(row);
    if(replyBox) contentWrap.appendChild(replyBox);

    const messageText = isMessageDeletedEverywhere(row)
      ? getDeletedMessageText(row)
      : textOrEmpty(row.content || row.message);
    if(messageText){
      const textNode = document.createElement("div");
      textNode.textContent = messageText;
      if(isMessageDeletedEverywhere(row)){
        textNode.className = "italic text-gray-500";
      }
      contentWrap.appendChild(textNode);
    }

    if(!isMessageDeletedEverywhere(row)){
      const attachment = createAttachmentBlock(row);
      if(attachment) contentWrap.appendChild(attachment);
    }
    return contentWrap;
  }

  function buildMessageBubble(row){
    const outgoing = String(row.sender_id || "") === myId;
    const bubble = document.createElement("div");
    bubble.className = `bubble ${outgoing ? "bubble-out" : "bubble-in"} ${isMessageStarred(row) ? "starred-bubble" : ""}`;
    const id = getMessageId(row);
    if(id) bubble.dataset.messageId = id;
    bindBubbleContextMenu(bubble, row);

    if(outgoing){
      bubble.appendChild(createMessageMainContent(row, true));
    }else{
      const wrap = document.createElement("div");
      wrap.className = "flex items-start gap-2";
      const avatar = document.createElement("div");
      avatar.className = "msg-mini-avatar";
      setAvatar(avatar, getProfilePhoto(peerProfile, receiverPicParam), getDisplayName(peerProfile, receiverNameParam));
      const textWrap = document.createElement("div");
      textWrap.className = "flex-1";
      textWrap.appendChild(createMessageMainContent(row, false));
      wrap.appendChild(avatar);
      wrap.appendChild(textWrap);
      bubble.appendChild(wrap);
    }

    const metaRow = document.createElement("div");
    metaRow.className = "text-[9px] text-gray-400 mt-1 flex items-center justify-end gap-2";
    if(isMessageStarred(row)){
      const starTag = document.createElement("span");
      starTag.className = "star-tag";
      starTag.textContent = "STARRED";
      metaRow.appendChild(starTag);
    }
    const clock = document.createElement("span");
    clock.textContent = formatClock(row.created_at);
    metaRow.appendChild(clock);
    bubble.appendChild(metaRow);
    return bubble;
  }

  function renderMessageList(){
    msgHolder.innerHTML = "";
    const rows = [...messageRows]
      .filter(row => !shouldHideRow(row))
      .sort((a, b) => toEpoch(a?.created_at) - toEpoch(b?.created_at));
    if(!rows.length){
      msgHolder.innerHTML = "<p class='text-center text-xs text-gray-400'>No messages.</p>";
      return;
    }
    rows.forEach(row => msgHolder.appendChild(buildMessageBubble(row)));
  }

  function addMessageRow(row){
    upsertMessageRow(row);
    renderMessageList();
  }

  async function fetchUserProfile(uid){
    const attempts = [
      "user_id,username,full_name,display_name,photo,avatar_url",
      "user_id,username,full_name,display_name,photo",
      "user_id,username,full_name,photo",
      "user_id,username,full_name"
    ];
    for(const fields of attempts){
      const { data, error } = await supa.from("users").select(fields).eq("user_id", uid).maybeSingle();
      if(!error) return data || null;
      if(!maybeMissingColumn(error)){
        console.error("profile_fetch_failed", error);
        break;
      }
    }
    return null;
  }

  async function resolveCurrentUser(){
    if(window.NOVA && typeof window.NOVA.requireUser === "function"){
      return await window.NOVA.requireUser();
    }
    const { data, error } = await supa.auth.getUser();
    if(error) throw error;
    return data?.user || null;
  }

  async function queryThreadMessages(condition, fields){
    return await supa
      .from("messages")
      .select(fields)
      .or(condition)
      .order("created_at", { ascending: true });
  }

  async function fetchThreadRows(condition){
    try{
      const selections = [
        "id,sender_id,receiver_id,content,created_at,attachment_url,message_type,reply_to_id,reply_preview,deleted_for_everyone,is_starred,deleted_by_name",
        "id,sender_id,receiver_id,content,created_at,attachment_url,message_type,reply_to_id,deleted_for_everyone",
        "id,sender_id,receiver_id,content,created_at"
      ];
      for(const fields of selections){
        const { data, error } = await queryThreadMessages(condition, fields);
        if(!error) return { data: data || [], error: null };
        if(!maybeMissingColumn(error)) return { data: [], error };
      }
      const fallback = await queryThreadMessages(condition, "id,sender_id,receiver_id,content,created_at");
      if(fallback.error) return { data: [], error: fallback.error };
      return { data: fallback.data || [], error: null };
    }catch(err){
      console.error("thread_query_failed", err);
      return { data: [], error: err || new Error("thread_query_failed") };
    }
  }

  async function loadMessages(){
    msgHolder.innerHTML = "<p class='text-center text-xs text-gray-400'>Syncing messages...</p>";
    const condition = `and(sender_id.eq.${myId},receiver_id.eq.${receiverId}),and(sender_id.eq.${receiverId},receiver_id.eq.${myId})`;
    const { data, error } = await fetchThreadRows(condition);
    if(error){
      console.error("message_load_failed", error);
      msgHolder.innerHTML = "<p class='text-center text-xs text-red-500'>Unable to load messages.</p>";
      return;
    }
    messageRows = [];
    renderedMessageIds = new Set();
    (data || []).forEach(upsertMessageRow);
    renderMessageList();
    scrollBottom();
  }

  async function insertMessageWithFallback(payload){
    const variants = [];
    variants.push({ ...payload });
    if("reply_preview" in payload){ const v = { ...payload }; delete v.reply_preview; variants.push(v); }
    if("reply_to_id" in payload){ const v = { ...payload }; delete v.reply_to_id; variants.push(v); }
    if("attachment_url" in payload){ const v = { ...payload }; delete v.attachment_url; variants.push(v); }
    if("message_type" in payload){ const v = { ...payload }; delete v.message_type; variants.push(v); }
    variants.push({ sender_id: payload.sender_id, receiver_id: payload.receiver_id, content: payload.content });

    for(const candidate of variants){
      const { data, error } = await supa
        .from("messages")
        .insert(candidate)
        .select("id,sender_id,receiver_id,content,created_at")
        .maybeSingle();
      if(!error) return { data: data ? { ...candidate, ...data } : null, error: null };
      if(!maybeMissingColumn(error)) return { data: null, error };
    }
    return { data: null, error: new Error("message_insert_failed") };
  }

  function getReplyPayload(){
    if(!pendingReply) return {};
    const messageId = getMessageId(pendingReply);
    const preview = getMessageTextForPreview(pendingReply).slice(0, 140);
    const payload = {};
    if(messageId) payload.reply_to_id = messageId;
    if(preview) payload.reply_preview = preview;
    return payload;
  }

  function clearReplyState(){
    pendingReply = null;
    replyPreviewText.textContent = "";
    replyPreview.classList.remove("show");
  }

  function setReplyState(row){
    pendingReply = row || null;
    if(!pendingReply){
      clearReplyState();
      return;
    }
    replyPreviewText.textContent = getMessageTextForPreview(pendingReply);
    replyPreview.classList.add("show");
  }

  function cancelReply(){
    clearReplyState();
  }

  async function sendMessageWithPayload(rawPayload, restoreText){
    if(!isUuid(myId) || !isUuid(receiverId)){
      alert("Invalid sender/receiver id.");
      return false;
    }
    if(isPeerBlocked){
      alert("You blocked this user.");
      return false;
    }
    const { data, error } = await insertMessageWithFallback(rawPayload);
    if(error){
      console.error("message_send_failed", error);
      alert("Message failed to send.");
      if(typeof restoreText === "string") mainInput.value = restoreText;
      return false;
    }
    if(data){
      addMessageRow(data);
      scrollBottom();
    }
    return true;
  }

  async function sendMessage(){
    const text = String(mainInput.value || "").trim();
    if(!text) return;
    mainInput.value = "";
    const payload = {
      sender_id: myId,
      receiver_id: receiverId,
      content: text,
      ...getReplyPayload()
    };
    const ok = await sendMessageWithPayload(payload, text);
    if(ok) clearReplyState();
  }

  function subscribeToMessages(){
    if(realtimeChannel){
      try{ supa.removeChannel(realtimeChannel); }catch(_){ }
      realtimeChannel = null;
    }
    const processPayload = (row, type) => {
      const s = String(row?.sender_id || "");
      const r = String(row?.receiver_id || "");
      const inThread = (s === myId && r === receiverId) || (s === receiverId && r === myId);
      if(!inThread) return;
      if(type === "DELETE"){
        const id = getMessageId(row);
        if(id){
          messageRows = messageRows.filter(item => getMessageId(item) !== id);
          renderMessageList();
          scrollBottom();
        }
        return;
      }
      addMessageRow(row);
      scrollBottom();
    };

    realtimeChannel = supa.channel(`dm_${myId}_${receiverId}_${Date.now()}`)
      .on("postgres_changes", { event: "INSERT", schema: "public", table: "messages" }, payload => {
        processPayload(payload?.new || {}, "INSERT");
      })
      .on("postgres_changes", { event: "UPDATE", schema: "public", table: "messages" }, payload => {
        processPayload(payload?.new || {}, "UPDATE");
      })
      .on("postgres_changes", { event: "DELETE", schema: "public", table: "messages" }, payload => {
        processPayload(payload?.old || {}, "DELETE");
      })
      .subscribe();
  }

  function normalizeSdp(raw){
    if(!raw || typeof raw !== "object") return null;
    const type = String(raw.type || "").trim().toLowerCase();
    const sdp = String(raw.sdp || "");
    if(!type || !sdp) return null;
    return { type, sdp };
  }

  function normalizeCandidate(raw){
    if(!raw || typeof raw !== "object") return null;
    const candidate = String(raw.candidate || "");
    if(!candidate) return null;
    return {
      candidate,
      sdpMid: String(raw.sdpMid || ""),
      sdpMLineIndex: Number.isFinite(Number(raw.sdpMLineIndex)) ? Number(raw.sdpMLineIndex) : null,
      usernameFragment: String(raw.usernameFragment || "")
    };
  }

  function normalizeSignal(row){
    const src = row && typeof row === "object" ? row : {};
    const payload = src.payload && typeof src.payload === "object" ? src.payload : src;
    const type = String(payload.type || src.type || "").trim().toLowerCase();
    const callId = String(payload.call_id || payload.callId || src.call_id || src.callId || "").trim();
    const toUserId = String(payload.to_user_id || payload.to || src.to_user_id || src.to || "").trim();
    const fromUserId = String(payload.from_user_id || payload.from || src.from_user_id || src.from || "").trim();
    if(!type || !callId || !toUserId || !fromUserId) return null;
    return {
      id: String(src.id || "").trim(),
      type,
      call_id: callId,
      to_user_id: toUserId,
      from_user_id: fromUserId,
      media_type: String(payload.media_type || payload.mediaType || src.media_type || src.mediaType || "").trim().toLowerCase() === "video" ? "video" : "audio",
      reason: String(payload.reason || src.reason || "").trim(),
      sdp: normalizeSdp(payload.sdp || src.sdp),
      candidate: normalizeCandidate(payload.candidate || src.candidate)
    };
  }

  function buildApiBases(){
    const out = [""];
    const push = (raw) => {
      const clean = String(raw || "").trim().replace(/\/+$/g, "");
      if(!clean) return;
      if(!/^https?:\/\//i.test(clean)) return;
      if(!out.includes(clean)) out.push(clean);
    };
    try{
      push(window.CONTEST_API_BASE || window.API_BASE || "");
      push(localStorage.getItem("contest_api_base"));
      push(localStorage.getItem("api_base"));
      push(sessionStorage.getItem("contest_api_base"));
      push(sessionStorage.getItem("api_base"));
    }catch(_){ }
    if(/^https?:\/\//i.test(location.origin || "")) push(location.origin);
    push("https://novagapp-mart.onrender.com");
    return out;
  }

  function apiUrl(base, path){
    return base ? `${base}${path}` : path;
  }

  async function sendCallSignal(payload){
    const type = String(payload?.type || "").trim().toLowerCase();
    const reqBody = {
      type,
      call_id: String(payload?.call_id || "").trim(),
      from_user_id: String(payload?.from_user_id || "").trim(),
      to_user_id: String(payload?.to_user_id || "").trim(),
      media_type: String(payload?.media_type || "").trim().toLowerCase() === "video" ? "video" : "audio"
    };
    if(!type || !isUuid(reqBody.from_user_id) || !isUuid(reqBody.to_user_id) || !reqBody.call_id) return false;
    if(type === "call-offer" || type === "call-answer"){
      reqBody.sdp = normalizeSdp(payload?.sdp);
      if(!reqBody.sdp) return false;
    }
    if(type === "ice"){
      reqBody.candidate = normalizeCandidate(payload?.candidate);
      if(!reqBody.candidate) return false;
    }
    if(payload?.reason) reqBody.reason = String(payload.reason).slice(0, 120);

    const bases = buildApiBases();
    for(const base of bases){
      try{
        const res = await fetch(apiUrl(base, "/api/call/signal"), {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(reqBody)
        });
        if(res.ok) return true;
      }catch(_){ }
    }
    return false;
  }

  function showIncomingOffer(offer){
    pendingIncomingOffer = offer;
    incomingCallText.textContent = `${offer.media_type === "video" ? "Video" : "Audio"} call from ${getDisplayName(peerProfile, receiverNameParam)}`;
    incomingCallBar.style.display = "flex";
    if(incomingOfferTimer) clearTimeout(incomingOfferTimer);
    incomingOfferTimer = setTimeout(() => { declineIncomingCall("timeout").catch(() => {}); }, 60000);
  }

  function hideIncomingOffer(){
    if(incomingOfferTimer){ clearTimeout(incomingOfferTimer); incomingOfferTimer = null; }
    pendingIncomingOffer = null;
    incomingCallBar.style.display = "none";
  }

  function setCallStatus(text){
    callSub.textContent = String(text || "").trim() || "Connecting...";
  }

  function showCallOverlay(call){
    callOverlay.style.display = "flex";
    const who = getDisplayName(peerProfile, receiverNameParam);
    callTitle.textContent = call.direction === "outgoing" ? `Calling ${who}` : `In call with ${who}`;
    callAudioMask.style.display = call.mediaType === "video" ? "none" : "flex";
    remoteVideo.classList.toggle("hidden", call.mediaType !== "video");
    localVideo.classList.toggle("hidden", call.mediaType !== "video");
    cameraCallBtn.classList.toggle("hidden", call.mediaType !== "video");
  }

  function closeCallOverlay(){
    callOverlay.style.display = "none";
    remoteVideo.srcObject = null;
    localVideo.srcObject = null;
  }

  function stopTracks(stream){
    if(!stream) return;
    stream.getTracks().forEach(track => { try{ track.stop(); }catch(_){ } });
  }

  function closeCallResources(call){
    if(!call) return;
    try{
      if(call.pc){
        call.pc.onicecandidate = null;
        call.pc.ontrack = null;
        call.pc.onconnectionstatechange = null;
        call.pc.close();
      }
    }catch(_){ }
    stopTracks(call.localStream);
    stopTracks(call.remoteStream);
  }

  async function endActiveCall(sendSignal, reason){
    if(!activeCall) return;
    const call = activeCall;
    activeCall = null;
    if(sendSignal){
      await sendCallSignal({
        type: "call-end",
        call_id: call.callId,
        from_user_id: myId,
        to_user_id: receiverId,
        media_type: call.mediaType,
        reason: reason || "ended"
      });
    }
    closeCallResources(call);
    setCallStatus(reason || "Call ended");
    closeCallOverlay();
    muteCallBtn.textContent = "Mute";
    cameraCallBtn.textContent = "Camera Off";
  }

  async function getLocalStream(mediaType){
    try{
      return await navigator.mediaDevices.getUserMedia({
        audio: true,
        video: mediaType === "video" ? { facingMode: "user" } : false
      });
    }catch(err){
      console.error("media_permission_failed", err);
      alert("Microphone/Camera permission denied.");
      return null;
    }
  }

  function normalizeCandidateFromRtc(candidate){
    if(!candidate) return null;
    if(typeof candidate.toJSON === "function") return normalizeCandidate(candidate.toJSON());
    return normalizeCandidate({
      candidate: candidate.candidate,
      sdpMid: candidate.sdpMid,
      sdpMLineIndex: candidate.sdpMLineIndex,
      usernameFragment: candidate.usernameFragment
    });
  }

  async function createCallSession(callId, mediaType, direction, localStream){
    const call = {
      callId, mediaType, direction, localStream,
      remoteStream: new MediaStream(), pc: null, pendingIce: [], muted: false, cameraOff: false
    };
    const pc = new RTCPeerConnection(RTC_CONFIG);
    call.pc = pc;
    localVideo.srcObject = localStream;
    remoteVideo.srcObject = call.remoteStream;
    localStream.getTracks().forEach(track => pc.addTrack(track, localStream));

    pc.onicecandidate = (event) => {
      if(!event.candidate || !activeCall || activeCall.callId !== call.callId) return;
      const candidate = normalizeCandidateFromRtc(event.candidate);
      if(!candidate) return;
      sendCallSignal({
        type: "ice",
        call_id: call.callId,
        from_user_id: myId,
        to_user_id: receiverId,
        media_type: call.mediaType,
        candidate
      }).catch(() => {});
    };
    pc.ontrack = (event) => {
      const stream = Array.isArray(event.streams) && event.streams.length ? event.streams[0] : null;
      if(stream){
        stream.getTracks().forEach(track => {
          if(!call.remoteStream.getTracks().find(t => t.id === track.id)) call.remoteStream.addTrack(track);
        });
      }else if(event.track){
        if(!call.remoteStream.getTracks().find(t => t.id === event.track.id)) call.remoteStream.addTrack(event.track);
      }
    };
    pc.onconnectionstatechange = () => {
      if(!activeCall || activeCall.callId !== call.callId) return;
      const state = String(pc.connectionState || "").toLowerCase();
      if(state === "connected"){ setCallStatus("Connected"); return; }
      if(state === "connecting"){ setCallStatus("Connecting..."); return; }
      if(state === "failed" || state === "disconnected" || state === "closed"){
        endActiveCall(false, "Call ended").catch(() => {});
      }
    };
    return call;
  }

  function pushBufferedIce(callId, candidate){
    if(!callId || !candidate) return;
    const list = bufferedIceByCall.get(callId) || [];
    list.push(candidate);
    bufferedIceByCall.set(callId, list);
  }

  async function flushPendingIce(call){
    if(!call?.pc) return;
    const queue = [];
    if(call.pendingIce.length){ queue.push(...call.pendingIce); call.pendingIce = []; }
    const buffered = bufferedIceByCall.get(call.callId);
    if(buffered && buffered.length){ queue.push(...buffered); bufferedIceByCall.delete(call.callId); }
    for(const candidate of queue){
      try{ await call.pc.addIceCandidate(new RTCIceCandidate(candidate)); }catch(_){ }
    }
  }

  async function startOutgoingCall(mediaType){
    if(!isUuid(myId) || !isUuid(receiverId)) return;
    if(isPeerBlocked){
      alert("You blocked this user.");
      return;
    }
    if(activeCall){ alert("A call is already active."); return; }
    hideIncomingOffer();
    const localStream = await getLocalStream(mediaType);
    if(!localStream) return;
    const callId = typeof crypto.randomUUID === "function" ? crypto.randomUUID() : `${Date.now()}_${Math.random().toString(16).slice(2)}`;
    const call = await createCallSession(callId, mediaType, "outgoing", localStream);
    activeCall = call;
    showCallOverlay(call);
    setCallStatus("Creating offer...");
    try{
      const offer = await call.pc.createOffer({ offerToReceiveAudio: true, offerToReceiveVideo: mediaType === "video" });
      await call.pc.setLocalDescription(offer);
      const ok = await sendCallSignal({
        type: "call-offer",
        call_id: call.callId,
        from_user_id: myId,
        to_user_id: receiverId,
        media_type: mediaType,
        sdp: call.pc.localDescription
      });
      if(!ok){
        await endActiveCall(false, "Signaling failed");
        alert("Unable to reach call server.");
        return;
      }
      setCallStatus("Ringing...");
    }catch(err){
      console.error("call_offer_failed", err);
      await endActiveCall(false, "Call failed");
    }
  }

  async function answerIncomingCall(){
    const offer = pendingIncomingOffer;
    if(!offer) return;
    hideIncomingOffer();
    if(activeCall){
      await sendCallSignal({
        type: "call-busy",
        call_id: offer.call_id,
        from_user_id: myId,
        to_user_id: offer.from_user_id,
        media_type: offer.media_type
      });
      return;
    }
    const localStream = await getLocalStream(offer.media_type);
    if(!localStream){
      await sendCallSignal({
        type: "call-decline",
        call_id: offer.call_id,
        from_user_id: myId,
        to_user_id: offer.from_user_id,
        media_type: offer.media_type,
        reason: "media_permission_denied"
      });
      return;
    }
    const call = await createCallSession(offer.call_id, offer.media_type, "incoming", localStream);
    activeCall = call;
    showCallOverlay(call);
    setCallStatus("Connecting...");
    try{
      if(!offer.sdp) throw new Error("missing_offer_sdp");
      await call.pc.setRemoteDescription(new RTCSessionDescription(offer.sdp));
      await flushPendingIce(call);
      const answer = await call.pc.createAnswer();
      await call.pc.setLocalDescription(answer);
      const ok = await sendCallSignal({
        type: "call-answer",
        call_id: call.callId,
        from_user_id: myId,
        to_user_id: receiverId,
        media_type: call.mediaType,
        sdp: call.pc.localDescription
      });
      if(!ok) await endActiveCall(false, "Signaling failed");
    }catch(err){
      console.error("call_answer_failed", err);
      await endActiveCall(false, "Call failed");
    }
  }

  async function declineIncomingCall(reason){
    if(!pendingIncomingOffer){ hideIncomingOffer(); return; }
    const offer = pendingIncomingOffer;
    hideIncomingOffer();
    await sendCallSignal({
      type: "call-decline",
      call_id: offer.call_id,
      from_user_id: myId,
      to_user_id: offer.from_user_id,
      media_type: offer.media_type,
      reason: reason || "decline"
    });
  }

  async function hangupActiveCall(){ await endActiveCall(true, "Call ended"); }

  function toggleMute(){
    if(!activeCall?.localStream) return;
    activeCall.muted = !activeCall.muted;
    activeCall.localStream.getAudioTracks().forEach(track => { track.enabled = !activeCall.muted; });
    muteCallBtn.textContent = activeCall.muted ? "Unmute" : "Mute";
  }

  function toggleCamera(){
    if(!activeCall || activeCall.mediaType !== "video" || !activeCall.localStream) return;
    activeCall.cameraOff = !activeCall.cameraOff;
    activeCall.localStream.getVideoTracks().forEach(track => { track.enabled = !activeCall.cameraOff; });
    cameraCallBtn.textContent = activeCall.cameraOff ? "Camera On" : "Camera Off";
  }

  async function handleCallSignal(signal){
    if(!signal || signal.to_user_id !== myId || signal.from_user_id !== receiverId) return;
    if(signal.type === "call-offer"){
      if(isPeerBlocked){
        await sendCallSignal({
          type: "call-decline",
          call_id: signal.call_id,
          from_user_id: myId,
          to_user_id: receiverId,
          media_type: signal.media_type,
          reason: "blocked"
        });
        return;
      }
      if(activeCall){
        await sendCallSignal({
          type: "call-busy",
          call_id: signal.call_id,
          from_user_id: myId,
          to_user_id: receiverId,
          media_type: signal.media_type
        });
        return;
      }
      showIncomingOffer(signal);
      if(autoAnswer.enabled && (!autoAnswer.callId || autoAnswer.callId === signal.call_id)){
        autoAnswer.enabled = false;
        answerIncomingCall().catch(() => {});
      }
      return;
    }
    if(signal.type === "ice"){
      if(activeCall && activeCall.callId === signal.call_id){
        if(signal.candidate){
          if(activeCall.pc?.remoteDescription?.type){
            try{ await activeCall.pc.addIceCandidate(new RTCIceCandidate(signal.candidate)); }catch(_){ }
          }else{
            activeCall.pendingIce.push(signal.candidate);
          }
        }
        return;
      }
      if(signal.candidate) pushBufferedIce(signal.call_id, signal.candidate);
      return;
    }
    if(signal.type === "call-answer"){
      if(!activeCall || activeCall.callId !== signal.call_id || activeCall.direction !== "outgoing" || !signal.sdp) return;
      try{
        await activeCall.pc.setRemoteDescription(new RTCSessionDescription(signal.sdp));
        await flushPendingIce(activeCall);
        setCallStatus("Connected");
      }catch(err){
        console.error("set_remote_answer_failed", err);
        await endActiveCall(false, "Call failed");
      }
      return;
    }
    if(signal.type === "call-end" || signal.type === "call-decline" || signal.type === "call-busy"){
      if(pendingIncomingOffer && pendingIncomingOffer.call_id === signal.call_id) hideIncomingOffer();
      if(activeCall && activeCall.callId === signal.call_id){
        const reason = signal.type === "call-busy" ? "User busy" : (signal.type === "call-decline" ? "Call declined" : "Call ended");
        await endActiveCall(false, reason);
      }
    }
  }

  async function pollCallSignals(force){
    if(!isUuid(myId)) return;
    if(document.hidden && !force && !activeCall && !pendingIncomingOffer) return;
    const query = new URLSearchParams();
    query.set("user_id", myId);
    query.set("with_user_id", receiverId);
    query.set("limit", "50");
    const bases = buildApiBases();
    for(const base of bases){
      try{
        const res = await fetch(apiUrl(base, `/api/call/signals?${query.toString()}`), { cache: "no-store" });
        if(!res.ok) continue;
        const payload = await res.json().catch(() => null);
        if(!payload || payload.ok !== true) continue;
        const rows = Array.isArray(payload.signals) ? payload.signals : [];
        for(const row of rows){
          const id = String(row?.id || "").trim();
          if(id && !rememberCallSignalId(id)) continue;
          const signal = normalizeSignal(row);
          if(signal) await handleCallSignal(signal);
        }
        return;
      }catch(_){ }
    }
  }

  function startCallPolling(){
    if(callPollTimer) return;
    pollCallSignals(true).catch(() => {});
    callPollTimer = setInterval(() => { pollCallSignals(false).catch(() => {}); }, 1500);
  }

  function stopCallPolling(){
    if(callPollTimer){
      clearInterval(callPollTimer);
      callPollTimer = null;
    }
  }

  function toggleHeaderMenu(){
    const show = chatHeaderMenu.style.display !== "block";
    chatHeaderMenu.style.display = show ? "block" : "none";
    if(show) closeContextMenu();
  }

  function closeHeaderMenu(){
    chatHeaderMenu.style.display = "none";
  }

  function showStarredMessages(){
    showingStarredOnly = true;
    updateFilterUi();
    closeHeaderMenu();
    renderMessageList();
  }

  function showAllMessages(){
    showingStarredOnly = false;
    updateFilterUi();
    renderMessageList();
  }

  async function clearAllChat(){
    closeHeaderMenu();
    if(!confirm("Clear all chat only on your side?")) return;
    setChatClearedAt(Date.now());
    renderMessageList();
  }

  async function persistBlockInDatabase(){
    const payload = { user_id: myId, blocked_user_id: receiverId, created_at: new Date().toISOString() };
    try{
      const { error: upsertError } = await supa
        .from("blocked_users")
        .upsert(payload, { onConflict: "user_id,blocked_user_id" });
      if(!upsertError) return true;
      if(maybeMissingColumn(upsertError)){
        console.warn("blocked_users columns missing", upsertError?.message || upsertError);
        return false;
      }
      const { error: insertError } = await supa.from("blocked_users").insert(payload);
      if(!insertError) return true;
      if(maybeMissingColumn(insertError)){
        console.warn("blocked_users columns missing", insertError?.message || insertError);
        return false;
      }
      if(String(insertError?.code || "") === "23505"){
        return true;
      }
      console.error("block_insert_failed", insertError);
    }catch(err){
      console.error("block_persist_error", err);
    }
    return false;
  }

  async function checkBlockedInDatabase(){
    try{
      const { data, error } = await supa
        .from("blocked_users")
        .select("id")
        .eq("user_id", myId)
        .eq("blocked_user_id", receiverId)
        .limit(1);
      if(error){
        if(maybeMissingColumn(error)){
          console.warn("blocked_users columns missing", error?.message || error);
          return false;
        }
        console.error("block_check_failed", error);
        return false;
      }
      return Array.isArray(data) && data.length > 0;
    }catch(err){
      console.error("block_check_exception", err);
    }
    return false;
  }

  async function blockCurrentUser(){
    closeHeaderMenu();
    if(isPeerBlocked){
      alert("User already blocked.");
      return;
    }
    if(!confirm("Block this user?")) return;
    await persistBlockInDatabase();
    setBlockedLocalState(true);
    applyBlockedUi();
    alert("User blocked.");
  }

  function getContextMessage(){
    return selectedContextMessage || null;
  }

  function updateMessageInMemory(updated){
    const id = getMessageId(updated);
    if(!id) return;
    const idx = messageRows.findIndex(row => getMessageId(row) === id);
    if(idx >= 0){
      messageRows[idx] = { ...messageRows[idx], ...updated };
      renderMessageList();
    }
  }

  async function updateMessageRowWithFallback(messageId, payloadCandidates){
    const id = String(messageId || "").trim();
    if(!id || !Array.isArray(payloadCandidates)) return null;
    for(const payload of payloadCandidates){
      if(!payload || typeof payload !== "object" || !Object.keys(payload).length) continue;
      const { data, error } = await supa
        .from("messages")
        .update(payload)
        .eq("id", id)
        .select("id,sender_id,receiver_id,content,created_at")
        .maybeSingle();
      if(!error){
        return data ? { ...payload, ...data } : { id, ...payload };
      }
      if(!maybeMissingColumn(error)){
        console.error("message_update_failed", error);
        return null;
      }
    }
    return null;
  }

  async function contextMenuStar(){
    const row = getContextMessage();
    closeContextMenu();
    if(!row) return;
    const messageId = getMessageId(row);
    if(!messageId){
      row.is_starred = true;
      renderMessageList();
      return;
    }
    updateMessageInMemory({ id: messageId, is_starred: true });
    await updateMessageRowWithFallback(messageId, [
      { is_starred: true },
      { starred: true }
    ]);
  }

  function contextMenuReply(){
    const row = getContextMessage();
    closeContextMenu();
    if(!row) return;
    setReplyState(row);
    mainInput.focus();
  }

  function contextMenuDelete(){
    if(!getContextMessage()) return;
    closeContextMenu();
    deleteChoiceModal.style.display = "flex";
  }

  function closeDeleteChoice(){
    deleteChoiceModal.style.display = "none";
  }

  function deleteForMe(){
    const row = getContextMessage();
    closeDeleteChoice();
    if(!row) return;
    const id = getMessageId(row);
    if(id){
      localDeletedMessageIds.add(id);
      saveDeletedForMeState();
    }
    renderMessageList();
  }

  async function deleteForEveryone(){
    const row = getContextMessage();
    closeDeleteChoice();
    if(!row) return;
    const id = getMessageId(row);
    if(!id) return;
    const deleteText = `This message was deleted by ${getDisplayName(myUser, "User")}`;
    updateMessageInMemory({
      id,
      content: deleteText,
      deleted_for_everyone: true,
      deleted_by_name: getDisplayName(myUser, "User"),
      attachment_url: "",
      message_type: "text"
    });
    await updateMessageRowWithFallback(id, [
      { content: deleteText, deleted_for_everyone: true, deleted_by_name: getDisplayName(myUser, "User"), attachment_url: null, message_type: "text" },
      { content: deleteText, deleted_for_everyone: true, deleted_by_name: getDisplayName(myUser, "User") },
      { content: deleteText }
    ]);
  }

  function openProfile(){
    closeHeaderMenu();
    const name = getDisplayName(peerProfile, receiverNameParam || "User");
    setAvatar(document.getElementById("infoPic"), getProfilePhoto(peerProfile, receiverPicParam), name);
    document.getElementById("infoName").textContent = name;
    document.getElementById("profileOverlay").style.display = "flex";
  }

  function closeProfile(){ document.getElementById("profileOverlay").style.display = "none"; }

  function confirmAction(){ clearAllChat().catch(() => {}); }

  function sanitizeFileName(name){
    return String(name || "file").replace(/[^a-zA-Z0-9._-]/g, "_").slice(-80) || "file";
  }

  async function handleMediaUpload(el){
    const file = el?.files?.[0];
    if(!file) return;
    try{
      if(isPeerBlocked){
        alert("You blocked this user.");
        return;
      }
      const fileName = `${myId}/${Date.now()}_${sanitizeFileName(file.name)}`;
      const { error: uploadError } = await supa
        .storage
        .from("chat-attachments")
        .upload(fileName, file, { cacheControl: "3600", upsert: false, contentType: file.type || undefined });
      if(uploadError){
        console.error("attachment_upload_failed", uploadError);
        alert("File upload failed.");
        return;
      }
      const { data } = supa.storage.from("chat-attachments").getPublicUrl(fileName);
      const fileUrl = String(data?.publicUrl || "").trim();
      if(!fileUrl){
        alert("File URL not available.");
        return;
      }
      const type = String(file.type || "").toLowerCase();
      const isVideo = type.startsWith("video/");
      const isImage = type.startsWith("image/");
      const content = isVideo ? "Video attachment" : (isImage ? "Image attachment" : (file.name || "Attachment"));
      const payload = {
        sender_id: myId,
        receiver_id: receiverId,
        content,
        attachment_url: fileUrl,
        message_type: isVideo ? "video" : (isImage ? "image" : "file"),
        ...getReplyPayload()
      };
      const ok = await sendMessageWithPayload(payload, "");
      if(ok) clearReplyState();
    }finally{
      el.value = "";
    }
  }

  async function initPage(){
    if(!supa){
      console.error("supabase_client_missing");
      msgHolder.innerHTML = "<p class='text-center text-xs text-red-500'>Chat unavailable. Please refresh.</p>";
      return;
    }
    if(!receiverId){
      alert("Invalid chat user.");
      location.href = "chat.html";
      return;
    }
    if(!isGroupChat && !isUuid(receiverId)){
      alert("Invalid chat user.");
      location.href = "chat.html";
      return;
    }
    try{
      myUser = await resolveCurrentUser();
    }catch(err){
      console.error("user_resolve_failed", err);
    }
    myId = String(myUser?.id || "").trim();
    if(!isUuid(myId)){
      alert("Please login with a valid account.");
      location.href = "login.html";
      return;
    }
    if(!isGroupChat && myId === receiverId){
      alert("Cannot open self-chat.");
      location.href = "chat.html";
      return;
    }
    if(isGroupChat){
      peerProfile = { display_name: receiverNameParam || "Group", photo: receiverPicParam };
      renderPeerHeader();
      msgHolder.innerHTML = "<p class='text-center text-xs text-gray-400'>Group room opened.</p>";
      mainInput.disabled = true;
      sendBtn.disabled = true;
      mainInput.placeholder = "Group messages are disabled on this screen";
      audioCallBtn.disabled = true;
      videoCallBtn.disabled = true;
      audioCallBtn.classList.add("opacity-50");
      videoCallBtn.classList.add("opacity-50");
      sendBtn.classList.add("opacity-50");
      return;
    }
    loadThreadLocalState();
    const blockedInDb = await checkBlockedInDatabase().catch(() => false);
    if(blockedInDb) setBlockedLocalState(true);
    peerProfile = await fetchUserProfile(receiverId);
    renderPeerHeader();
    applyBlockedUi();
    updateFilterUi();
    await loadMessages();
    subscribeToMessages();
    startCallPolling();
    const canCall = isUuid(receiverId) && receiverId !== myId && !isPeerBlocked;
    audioCallBtn.disabled = !canCall;
    videoCallBtn.disabled = !canCall;
    if(!canCall){
      audioCallBtn.classList.add("opacity-50");
      videoCallBtn.classList.add("opacity-50");
    }
  }

  document.addEventListener("visibilitychange", () => {
    if(document.visibilityState === "visible") pollCallSignals(true).catch(() => {});
  });

  document.addEventListener("click", (event) => {
    const target = event.target;
    if(chatHeaderMenu.style.display === "block" && !chatHeaderMenu.contains(target)){
      closeHeaderMenu();
    }
    if(messageContextMenu.style.display === "block" && !messageContextMenu.contains(target)){
      closeContextMenu();
    }
    if(deleteChoiceModal.style.display === "flex" && target === deleteChoiceModal){
      closeDeleteChoice();
    }
    if(mediaPreviewModal.style.display === "flex" && target === mediaPreviewModal){
      closeMediaPreview();
    }
  });

  document.addEventListener("keydown", (event) => {
    if(event.key === "Escape" && mediaPreviewModal.style.display === "flex"){
      closeMediaPreview();
    }
  });

  window.addEventListener("resize", () => {
    closeHeaderMenu();
    closeContextMenu();
  });

  chatWindow.addEventListener("scroll", () => {
    closeContextMenu();
  });

  window.addEventListener("beforeunload", () => {
    stopCallPolling();
    if(realtimeChannel){ try{ supa.removeChannel(realtimeChannel); }catch(_){ } }
    if(activeCall){ closeCallResources(activeCall); activeCall = null; }
  });

  mainInput.addEventListener("keydown", (event) => {
    if(event.key === "Enter" && !event.shiftKey){
      event.preventDefault();
      sendMessage().catch(() => {});
    }
  });

  window.sendMessage = sendMessage;
  window.handleMediaUpload = handleMediaUpload;
  window.openProfile = openProfile;
  window.closeProfile = closeProfile;
  window.confirmAction = confirmAction;
  window.toggleHeaderMenu = toggleHeaderMenu;
  window.showStarredMessages = showStarredMessages;
  window.showAllMessages = showAllMessages;
  window.clearAllChat = clearAllChat;
  window.blockCurrentUser = blockCurrentUser;
  window.cancelReply = cancelReply;
  window.contextMenuStar = contextMenuStar;
  window.contextMenuReply = contextMenuReply;
  window.contextMenuDelete = contextMenuDelete;
  window.deleteForMe = deleteForMe;
  window.deleteForEveryone = deleteForEveryone;
  window.closeDeleteChoice = closeDeleteChoice;
  window.closeMediaPreview = closeMediaPreview;
  window.downloadPreviewMedia = downloadPreviewMedia;
  window.startOutgoingCall = startOutgoingCall;
  window.answerIncomingCall = answerIncomingCall;
  window.declineIncomingCall = declineIncomingCall;
  window.hangupActiveCall = hangupActiveCall;
  window.toggleMute = toggleMute;
  window.toggleCamera = toggleCamera;
  window.scrollBottom = scrollBottom;
  window.onload = initPage;
})();
