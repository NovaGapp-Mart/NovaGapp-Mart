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
  const fileIn = document.getElementById("fileIn");
  const sendBtn = document.getElementById("sendBtn");
  const audioCallBtn = document.getElementById("audioCallBtn");
  const videoCallBtn = document.getElementById("videoCallBtn");
  const chatHeaderMenu = document.getElementById("chatHeaderMenu");
  const filterBanner = document.getElementById("filterBanner");
  const blockedBanner = document.getElementById("blockedBanner");
  const userMeta = document.getElementById("userMeta");
  const replyPreview = document.getElementById("replyPreview");
  const replyPreviewText = document.getElementById("replyPreviewText");
  const messageContextMenu = document.getElementById("messageContextMenu");
  const deleteChoiceModal = document.getElementById("deleteChoiceModal");
  const infoMeta = document.getElementById("infoMeta");
  const groupMembersSection = document.getElementById("groupMembersSection");
  const groupMembersList = document.getElementById("groupMembersList");
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
  let groupInfo = null;
  let groupMembers = [];
  let noticeTimer = null;
  const URL_PATTERN = /(https?:\/\/[^\s]+)/ig;

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

  function normalizeMemberCount(value){
    const count = Number(value);
    return Number.isFinite(count) && count >= 0 ? Math.floor(count) : 0;
  }

  function groupMemberLabel(count){
    const n = normalizeMemberCount(count);
    return n > 0 ? `${n} Members` : "Members";
  }

  async function fetchGroupMembers(groupId){
    try{
      const { data, error } = await supa
        .from("group_members")
        .select("group_id,user_id,user_name,user_avatar")
        .eq("group_id", groupId)
        .limit(500);
      if(error){
        return [];
      }
      return (data || []).map((row, index) => ({
        user_id: String(row?.user_id || `member_${index}`).trim(),
        display_name: String(row?.user_name || "Member").trim() || "Member",
        photo: String(row?.user_avatar || "").trim()
      }));
    }catch(_){
      return [];
    }
  }

  async function fetchGroupInfo(groupId){
    let groupRow = null;
    try{
      const { data, error } = await supa
        .from("groups")
        .select("id,name,icon_url")
        .eq("id", groupId)
        .maybeSingle();
      if(!error){
        groupRow = data || null;
      }
    }catch(_){ }
    const members = await fetchGroupMembers(groupId);
    return {
      id: groupId,
      name: String(groupRow?.name || receiverNameParam || "Group").trim() || "Group",
      icon_url: String(groupRow?.icon_url || receiverPicParam || "").trim(),
      member_count: normalizeMemberCount(members.length),
      members
    };
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

  function getGroupMemberProfile(senderId){
    const sid = String(senderId || "").trim();
    if(!sid) return null;
    return groupMembers.find(member => String(member?.user_id || "").trim() === sid) || null;
  }

  function getSenderDisplayName(row){
    const senderId = String(row?.sender_id || "").trim();
    if(senderId === myId){
      return getDisplayName(myUser, "You");
    }
    if(isGroupChat){
      const member = getGroupMemberProfile(senderId);
      return getDisplayName(member, String(row?.sender_name || row?.user_name || "Member"));
    }
    return getDisplayName(peerProfile, receiverNameParam);
  }

  function getSenderPhoto(row){
    const senderId = String(row?.sender_id || "").trim();
    if(senderId === myId){
      return getProfilePhoto(myUser, "");
    }
    if(isGroupChat){
      const member = getGroupMemberProfile(senderId);
      return getProfilePhoto(member, String(row?.sender_avatar || row?.user_avatar || ""));
    }
    return getProfilePhoto(peerProfile, receiverPicParam);
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

  function parseAttachmentUrls(raw){
    if(Array.isArray(raw)){
      return raw.map(v => String(v || "").trim()).filter(Boolean);
    }
    if(typeof raw === "string"){
      const txt = raw.trim();
      if(!txt) return [];
      if((txt.startsWith("[") && txt.endsWith("]")) || (txt.startsWith("{") && txt.endsWith("}"))){
        try{
          const parsed = JSON.parse(txt);
          if(Array.isArray(parsed)){
            return parsed.map(v => String(v || "").trim()).filter(Boolean);
          }
        }catch(_){ }
      }
      return [txt];
    }
    return [];
  }

  function getAttachmentUrl(row){
    return textOrEmpty(row?.attachment_url || row?.media_url || row?.file_url);
  }

  function getAttachmentUrls(row){
    const urls = parseAttachmentUrls(row?.attachment_urls);
    const single = getAttachmentUrl(row);
    if(single && !urls.includes(single)) urls.unshift(single);
    return urls.filter(Boolean);
  }

  function isImageUrl(url){
    return /\.(png|jpe?g|gif|webp|bmp|svg)$/i.test(String(url || "").split("?")[0]);
  }

  function isVideoUrl(url){
    return /\.(mp4|webm|ogg|mov|m4v)$/i.test(String(url || "").split("?")[0]);
  }

  function getMessageTextForPreview(row){
    const attachments = getAttachmentUrls(row);
    if(attachments.length > 1){
      return `${attachments.length} attachments`;
    }
    if(attachments.length === 1){
      return isVideoUrl(attachments[0]) ? "Video attachment" : "Attachment";
    }
    return textOrEmpty(row?.content || row?.message || "Message");
  }

  function sanitizeTextForCard(text){
    return String(text || "").replace(/\s+/g, " ").trim();
  }

  function isReelLink(url){
    const value = String(url || "").trim();
    return /\/reel\.html(\?|$)/i.test(value) || /[?&]reel(_id)?=/i.test(value);
  }

  function extractUrls(text){
    const value = String(text || "");
    const found = value.match(URL_PATTERN);
    if(!Array.isArray(found)) return [];
    return found.map(url => String(url || "").trim()).filter(Boolean);
  }

  function getFirstReelUrl(text){
    const links = extractUrls(text);
    return links.find(isReelLink) || "";
  }

  function buildReelCardTitle(text, reelUrl){
    const src = String(text || "");
    const withoutLink = src.replace(reelUrl, "").trim();
    const cleaned = sanitizeTextForCard(withoutLink);
    if(!cleaned) return "Open shared reel";
    const withoutPrefix = cleaned.replace(/^reel:\s*/i, "").trim();
    return withoutPrefix || "Open shared reel";
  }

  function appendTextWithLinks(parent, text){
    const value = String(text || "");
    const wrap = document.createElement("div");
    wrap.className = "msg-text";
    let cursor = 0;
    let hasUrl = false;
    URL_PATTERN.lastIndex = 0;
    let match = URL_PATTERN.exec(value);
    while(match){
      hasUrl = true;
      const url = String(match[0] || "").trim();
      const start = Number(match.index || 0);
      if(start > cursor){
        const txt = document.createTextNode(value.slice(cursor, start));
        wrap.appendChild(txt);
      }
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.target = "_blank";
      anchor.rel = "noopener";
      anchor.className = "msg-link";
      anchor.textContent = url;
      wrap.appendChild(anchor);
      cursor = start + url.length;
      match = URL_PATTERN.exec(value);
    }
    if(!hasUrl){
      wrap.textContent = value;
    }else if(cursor < value.length){
      wrap.appendChild(document.createTextNode(value.slice(cursor)));
    }
    parent.appendChild(wrap);
  }

  function appendMessageTextContent(parent, text){
    const value = String(text || "").trim();
    if(!value) return;
    const reelUrl = getFirstReelUrl(value);
    if(reelUrl){
      const card = document.createElement("a");
      card.href = reelUrl;
      card.target = "_blank";
      card.rel = "noopener";
      card.className = "reel-link-card";

      const label = document.createElement("span");
      label.className = "reel-link-card-label";
      label.textContent = "Shared Reel";

      const title = document.createElement("span");
      title.className = "reel-link-card-title";
      title.textContent = buildReelCardTitle(value, reelUrl);

      const linkText = document.createElement("span");
      linkText.className = "reel-link-card-url";
      linkText.textContent = reelUrl;

      card.appendChild(label);
      card.appendChild(title);
      card.appendChild(linkText);
      parent.appendChild(card);
      return;
    }
    appendTextWithLinks(parent, value);
  }

  function getThreadTargetKey(){
    return isGroupChat ? "group_id" : "receiver_id";
  }

  function buildThreadPayload(){
    return isGroupChat
      ? { group_id: receiverId }
      : { receiver_id: receiverId };
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

  function showNotice(message){
    const text = String(message || "").trim();
    if(!text || !mainInput) return;
    mainInput.placeholder = text;
    if(noticeTimer) clearTimeout(noticeTimer);
    noticeTimer = setTimeout(() => {
      if(mainInput && !mainInput.value){
        mainInput.placeholder = isPeerBlocked ? "User is blocked" : "Type a message";
      }
    }, 2800);
  }

  function renderGroupMembers(){
    if(!groupMembersSection || !groupMembersList) return;
    if(!isGroupChat){
      groupMembersSection.classList.add("hidden");
      groupMembersList.innerHTML = "";
      return;
    }
    groupMembersSection.classList.remove("hidden");
    groupMembersList.innerHTML = "";
    if(!groupMembers.length){
      groupMembersList.innerHTML = "<p class='text-sm text-gray-500'>No members found.</p>";
      return;
    }
    groupMembers.forEach(member => {
      const row = member && typeof member === "object" ? member : {};
      const name = getDisplayName(row, "Member");
      const photo = getProfilePhoto(row, "");
      const line = document.createElement("div");
      line.className = "flex items-center gap-3 p-2 rounded-lg bg-gray-50";
      const avatar = document.createElement("div");
      avatar.className = "w-9 h-9 rounded-full bg-orange-200 bg-cover bg-center text-xs font-bold flex items-center justify-center";
      setAvatar(avatar, photo, name);
      const nameEl = document.createElement("div");
      nameEl.className = "text-sm font-medium text-gray-800 truncate";
      nameEl.textContent = name;
      line.appendChild(avatar);
      line.appendChild(nameEl);
      groupMembersList.appendChild(line);
    });
  }

  function renderPeerHeader(){
    const name = isGroupChat
      ? String(groupInfo?.name || receiverNameParam || "Group").trim() || "Group"
      : getDisplayName(peerProfile, receiverNameParam || "User");
    const photo = isGroupChat
      ? String(groupInfo?.icon_url || receiverPicParam || "").trim()
      : getProfilePhoto(peerProfile, receiverPicParam);
    document.getElementById("userName").textContent = name;
    document.getElementById("infoName").textContent = name;
    setAvatar(document.getElementById("userAvatar"), photo, name);
    setAvatar(document.getElementById("infoPic"), photo, name);
    if(isGroupChat){
      const memberCount = normalizeMemberCount(groupInfo?.member_count || groupMembers.length);
      const label = groupMemberLabel(memberCount);
      if(userMeta) userMeta.textContent = label;
      if(infoMeta) infoMeta.textContent = label;
      renderGroupMembers();
      return;
    }
    if(userMeta) userMeta.textContent = "";
    if(infoMeta) infoMeta.textContent = "Real-time verified account";
    renderGroupMembers();
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
    const syntheticId = `${row.sender_id || ""}:${row.receiver_id || ""}:${row.group_id || ""}:${row.created_at || ""}:${content.slice(0, 30)}`;
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
    const attachments = getAttachmentUrls(row);
    if(!attachments.length) return null;

    const makeTile = (url, index, total) => {
      const cleanUrl = String(url || "").trim();
      const tile = document.createElement("button");
      tile.type = "button";
      tile.className = "attachment-tile";
      tile.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        openMediaPreview(cleanUrl, isVideoUrl(cleanUrl) ? "video" : "image");
      });

      if(isVideoUrl(cleanUrl)){
        const video = document.createElement("video");
        video.src = cleanUrl;
        video.muted = true;
        video.playsInline = true;
        video.preload = "metadata";
        tile.appendChild(video);
      }else{
        const img = document.createElement("img");
        img.src = cleanUrl;
        img.alt = "attachment";
        img.loading = "lazy";
        tile.appendChild(img);
      }

      if(index === 3 && total > 4){
        const overlay = document.createElement("div");
        overlay.className = "attachment-more";
        overlay.textContent = `+${total - 4}`;
        tile.appendChild(overlay);
      }
      return tile;
    };

    if(attachments.length === 1){
      const box = document.createElement("div");
      box.className = "attachment";
      box.appendChild(makeTile(attachments[0], 0, 1));
      return box;
    }

    const grid = document.createElement("div");
    grid.className = "attachment attachment-grid";
    attachments.slice(0, 4).forEach((url, index) => {
      grid.appendChild(makeTile(url, index, attachments.length));
    });
    return grid;
  }

  function createMessageMainContent(row, outgoing){
    const contentWrap = document.createElement("div");
    if(!outgoing){
      const sender = document.createElement("div");
      sender.className = "text-[10px] text-orange-700 font-semibold";
      sender.textContent = getSenderDisplayName(row);
      contentWrap.appendChild(sender);
    }

    const replyBox = createReplyBlock(row);
    if(replyBox) contentWrap.appendChild(replyBox);

    const messageText = isMessageDeletedEverywhere(row)
      ? getDeletedMessageText(row)
      : textOrEmpty(row.content || row.message);
    if(messageText){
      if(isMessageDeletedEverywhere(row)){
        const textNode = document.createElement("div");
        textNode.textContent = messageText;
        textNode.className = "italic text-gray-500";
        contentWrap.appendChild(textNode);
      }else{
        appendMessageTextContent(contentWrap, messageText);
      }
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
      setAvatar(avatar, getSenderPhoto(row), getSenderDisplayName(row));
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

  function buildDirectThreadCondition(){
    return `and(sender_id.eq.${myId},receiver_id.eq.${receiverId}),and(sender_id.eq.${receiverId},receiver_id.eq.${myId})`;
  }

  async function queryThreadMessages(fields){
    let query = supa
      .from("messages")
      .select(fields)
      .order("created_at", { ascending: true });
    if(isGroupChat){
      query = query.eq("group_id", receiverId);
    }else{
      query = query.or(buildDirectThreadCondition());
    }
    return await query;
  }

  async function fetchThreadRows(){
    try{
      const selections = [
        "id,sender_id,receiver_id,group_id,content,created_at,attachment_urls,attachment_url,message_type,reply_to_id,reply_preview,deleted_for_everyone,is_starred,deleted_by_name",
        "id,sender_id,receiver_id,group_id,content,created_at,attachment_urls,attachment_url,message_type,reply_to_id,deleted_for_everyone,is_starred",
        "id,sender_id,receiver_id,group_id,content,created_at,attachment_urls,attachment_url,message_type",
        "id,sender_id,receiver_id,group_id,content,created_at"
      ];
      for(const fields of selections){
        const { data, error } = await queryThreadMessages(fields);
        if(!error) return { data: data || [], error: null };
        if(!maybeMissingColumn(error)) return { data: [], error };
      }
      const fallback = await queryThreadMessages("id,sender_id,receiver_id,group_id,content,created_at");
      if(fallback.error) return { data: [], error: fallback.error };
      return { data: fallback.data || [], error: null };
    }catch(err){
      console.error("thread_query_failed", err);
      return { data: [], error: err || new Error("thread_query_failed") };
    }
  }

  async function loadMessages(){
    msgHolder.innerHTML = "<p class='text-center text-xs text-gray-400'>Syncing messages...</p>";
    const { data, error } = await fetchThreadRows();
    if(error){
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
    if("attachment_urls" in payload){ const v = { ...payload }; delete v.attachment_urls; variants.push(v); }
    if("attachment_url" in payload){ const v = { ...payload }; delete v.attachment_url; variants.push(v); }
    if("message_type" in payload){ const v = { ...payload }; delete v.message_type; variants.push(v); }
    variants.push({
      sender_id: payload.sender_id,
      ...buildThreadPayload(),
      content: payload.content
    });

    for(const candidate of variants){
      const { data, error } = await supa
        .from("messages")
        .insert(candidate)
        .select("id,sender_id,receiver_id,group_id,content,created_at,attachment_urls,attachment_url,message_type,reply_to_id,reply_preview")
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
    const hasValidThread = isGroupChat ? !!receiverId : isUuid(receiverId);
    if(!isUuid(myId) || !hasValidThread){
      showNotice("Invalid chat id.");
      return false;
    }
    if(!isGroupChat && isPeerBlocked){
      showNotice("User is blocked.");
      return false;
    }
    const { data, error } = await insertMessageWithFallback(rawPayload);
    if(error){
      console.error("message_send_failed", error);
      showNotice("Message failed to send.");
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
      ...buildThreadPayload(),
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
      const g = String(row?.group_id || "");
      const inThread = isGroupChat
        ? g === receiverId
        : ((s === myId && r === receiverId) || (s === receiverId && r === myId));
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

    realtimeChannel = supa.channel(`${isGroupChat ? "group" : "dm"}_${myId}_${receiverId}_${Date.now()}`)
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
      showNotice("Microphone/Camera permission denied.");
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
    if(!isGroupChat && isPeerBlocked){
      showNotice("User is blocked.");
      return;
    }
    if(activeCall){ showNotice("A call is already active."); return; }
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
        setCallStatus("Unable to reach call server");
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

  function handleBackNavigation(event){
    if(event) event.stopPropagation();
    const fallback = () => { location.href = "chat.html"; };
    if(window.history.length <= 1){
      fallback();
      return;
    }
    const startPath = String(location.pathname || "");
    window.history.back();
    setTimeout(() => {
      if(String(location.pathname || "") === startPath){
        fallback();
      }
    }, 240);
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
    if(isGroupChat){
      closeHeaderMenu();
      return;
    }
    closeHeaderMenu();
    if(isPeerBlocked){
      showNotice("User already blocked.");
      return;
    }
    if(!confirm("Block this user?")) return;
    await persistBlockInDatabase();
    setBlockedLocalState(true);
    applyBlockedUi();
    showNotice("User blocked.");
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
      attachment_urls: [],
      attachment_url: "",
      message_type: "text"
    });
    await updateMessageRowWithFallback(id, [
      { content: deleteText, deleted_for_everyone: true, deleted_by_name: getDisplayName(myUser, "User"), attachment_urls: [], attachment_url: null, message_type: "text" },
      { content: deleteText, deleted_for_everyone: true, deleted_by_name: getDisplayName(myUser, "User") },
      { content: deleteText }
    ]);
  }

  function openProfile(){
    closeHeaderMenu();
    const name = getDisplayName(peerProfile, receiverNameParam || "User");
    setAvatar(document.getElementById("infoPic"), getProfilePhoto(peerProfile, receiverPicParam), name);
    document.getElementById("infoName").textContent = name;
    if(isGroupChat){
      const count = normalizeMemberCount(groupInfo?.member_count || groupMembers.length);
      if(infoMeta) infoMeta.textContent = groupMemberLabel(count);
      renderGroupMembers();
    }else{
      if(infoMeta) infoMeta.textContent = "Real-time verified account";
      renderGroupMembers();
    }
    document.getElementById("profileOverlay").style.display = "flex";
  }

  function closeProfile(){ document.getElementById("profileOverlay").style.display = "none"; }

  function confirmAction(){ clearAllChat().catch(() => {}); }

  function sanitizeFileName(name){
    return String(name || "file").replace(/[^a-zA-Z0-9._-]/g, "_").slice(-80) || "file";
  }

  function isAllowedMediaFile(file){
    const type = String(file?.type || "").toLowerCase();
    return type.startsWith("image/") || type.startsWith("video/");
  }

  function isPermissionUploadError(error){
    const text = `${error?.message || ""} ${error?.code || ""} ${error?.statusCode || ""}`.toLowerCase();
    return text.includes("403") || text.includes("permission") || text.includes("forbidden") || text.includes("not authorized");
  }

  async function handleMediaUpload(el){
    const files = Array.from(el?.files || []);
    if(!files.length) return;
    try{
      if(!isGroupChat && isPeerBlocked){
        console.warn("attachment_blocked_user");
        return;
      }
      const uploaded = [];
      const mediaTypes = [];
      let invalidCount = 0;
      let permissionDenied = false;
      for(let i = 0; i < files.length; i += 1){
        const file = files[i];
        if(!isAllowedMediaFile(file)){
          invalidCount += 1;
          continue;
        }
        const fileName = `${myId}/${Date.now()}_${i}_${sanitizeFileName(file.name)}`;
        const { error: uploadError } = await supa
          .storage
          .from("chat-attachments")
          .upload(fileName, file, { cacheControl: "3600", upsert: false, contentType: file.type || undefined });
        if(uploadError){
          if(isPermissionUploadError(uploadError)){
            permissionDenied = true;
          }
          console.error("attachment_upload_failed", uploadError);
          continue;
        }
        const { data } = supa.storage.from("chat-attachments").getPublicUrl(fileName);
        const fileUrl = String(data?.publicUrl || "").trim();
        if(!fileUrl) continue;
        uploaded.push(fileUrl);
        mediaTypes.push(String(file.type || "").toLowerCase());
      }
      if(!uploaded.length){
        if(permissionDenied){
          showNotice("Upload permission denied.");
          return;
        }
        if(invalidCount > 0){
          showNotice("Only image/video files are allowed.");
          return;
        }
        showNotice("Upload failed.");
        return;
      }
      const allVideos = mediaTypes.length > 0 && mediaTypes.every(t => t.startsWith("video/"));
      const allImages = mediaTypes.length > 0 && mediaTypes.every(t => t.startsWith("image/"));
      const content = uploaded.length > 1
        ? `${uploaded.length} attachments`
        : (allVideos ? "Video attachment" : (allImages ? "Image attachment" : "Attachment"));
      const payload = {
        sender_id: myId,
        ...buildThreadPayload(),
        content,
        attachment_urls: uploaded,
        attachment_url: uploaded[0],
        message_type: allVideos ? "video" : (allImages ? "image" : "file"),
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
    if(fileIn) fileIn.multiple = true;
    if(!receiverId){
      location.href = "chat.html";
      return;
    }
    if(!isGroupChat && !isUuid(receiverId)){
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
      location.href = "login.html";
      return;
    }
    if(!isGroupChat && myId === receiverId){
      location.href = "chat.html";
      return;
    }
    if(isGroupChat){
      try{
        groupInfo = await fetchGroupInfo(receiverId);
      }catch(err){
        console.error("group_info_fetch_exception", err);
        groupInfo = {
          id: receiverId,
          name: receiverNameParam || "Group",
          icon_url: receiverPicParam,
          member_count: 0,
          members: []
        };
      }
      groupMembers = Array.isArray(groupInfo?.members) ? groupInfo.members : [];
      peerProfile = {
        display_name: groupInfo?.name || receiverNameParam || "Group",
        photo: groupInfo?.icon_url || receiverPicParam
      };
      renderPeerHeader();
      mainInput.disabled = false;
      sendBtn.disabled = false;
      mainInput.placeholder = "Type a message";
      blockedBanner.classList.remove("show");
      audioCallBtn.disabled = true;
      videoCallBtn.disabled = true;
      audioCallBtn.classList.add("opacity-50");
      videoCallBtn.classList.add("opacity-50");
      await loadMessages();
      subscribeToMessages();
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
  window.handleBackNavigation = handleBackNavigation;
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
