(function(){
  const supa = window.supa || (typeof window.novaCreateSupabaseClient === "function" ? window.novaCreateSupabaseClient() : null);
  const params = new URLSearchParams(location.search);
  let receiverId = String(params.get("id") || "").trim();
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
  const menuBlockUserBtn = document.getElementById("menuBlockUserBtn");
  const menuLeaveGroupBtn = document.getElementById("menuLeaveGroupBtn");
  const menuDeleteGroupBtn = document.getElementById("menuDeleteGroupBtn");
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
  const infoTabMedia = document.getElementById("infoTabMedia");
  const infoTabChatting = document.getElementById("infoTabChatting");
  const infoMediaPanel = document.getElementById("infoMediaPanel");
  const infoChattingPanel = document.getElementById("infoChattingPanel");
  const infoMediaStrip = document.getElementById("infoMediaStrip");
  const infoChattingStrip = document.getElementById("infoChattingStrip");
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
  let callRealtimeChannel = null;
  let messageSyncTimer = null;
  let lastThreadSnapshot = "";
  let callPollTimer = null;
  let incomingOfferTimer = null;
  let outgoingOfferTimer = null;
  let seenCallSignalIds = new Set();
  let seenCallSignalKeys = new Set();
  let activeCall = null;
  let pendingIncomingOffer = null;
  let ringToneTimer = null;
  let ringFallbackTimer = null;
  let ringAudioCtx = null;
  let ringAudioElement = null;
  let ringMode = "";
    function isUsersOnCallColumnEnabled(){
    const raw = String(window.CALL_USE_USERS_ON_CALL ?? "").trim().toLowerCase();
    return raw === "1" || raw === "true" || raw === "on" || raw === "yes";
  }

  let supportsUsersOnCallColumn = isUsersOnCallColumnEnabled();
  const bufferedIceByCall = new Map();
  const callOutcomeMessageKeys = new Set();
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
  let groupOwnerId = "";
  let groupAdminOnly = false;
  let activeInfoTab = "media";
  let noticeTimer = null;
  const CALL_RING_TIMEOUT_MS = 30000;
  const CALL_SIGNAL_BROADCAST_EVENT = "call-signal";
  const CALL_RINGTONE_URL = String(window.CALL_RINGTONE_URL || "").trim();
  const CALL_ANSWER_HANDOFF_KEY = "nova_pending_answer_offer";
  const LAST_CALL_INTERACTION_KEY = "nova_last_call_interaction_at";
  const URL_PATTERN = /(https?:\/\/[^\s]+)/ig;
  const DEFAULT_GROUP_ICON = "Images/no-image.png";
  let lastCallInteractionAt = 0;

  function getConfiguredSupabaseUrl(){
    return String(window.NOVA_PUBLIC_CONFIG?.supabaseUrl || window.__NOVA_PUBLIC_CONFIG__?.supabaseUrl || "").trim().replace(/\/+$/g, "");
  }

  function normalizeAssetUrl(value){
    const raw = String(value || "").trim().replace(/\\/g, "/");
    if(!raw) return "";
    if(/^https?:\/\//i.test(raw) || /^data:/i.test(raw) || /^blob:/i.test(raw)) return raw;
    const supabaseBase = getConfiguredSupabaseUrl();
    if(/^\/?storage\/v1\//i.test(raw) && supabaseBase){
      return `${supabaseBase}/${raw.replace(/^\/+/, "")}`;
    }
    try{
      return new URL(raw.startsWith("/") ? raw : `/${raw.replace(/^\/+/, "")}`, location.origin).href;
    }catch(_){
      return raw;
    }
  }

  function resolveGroupIconUrl(value){
    const resolved = normalizeAssetUrl(value);
    return resolved || DEFAULT_GROUP_ICON;
  }

  function markCallInteraction(){
    lastCallInteractionAt = Date.now();
    try{ sessionStorage.setItem(LAST_CALL_INTERACTION_KEY, String(lastCallInteractionAt)); }catch(_){ }
  }

  function readLastCallInteractionAt(){
    let stored = 0;
    try{ stored = Number(sessionStorage.getItem(LAST_CALL_INTERACTION_KEY) || 0); }catch(_){ }
    return Math.max(Number(lastCallInteractionAt || 0), stored || 0);
  }

  function safeCallVibrate(pattern){
    const lastInteraction = readLastCallInteractionAt();
    if(!lastInteraction || (Date.now() - lastInteraction) > 15000) return false;
    try{
      if(typeof navigator.vibrate === "function") return navigator.vibrate(pattern) !== false;
    }catch(_){ }
    return false;
  }

  function consumeAnswerHandoffOffer(expectedPeerId, expectedCallId){
    try{
      const raw = sessionStorage.getItem(CALL_ANSWER_HANDOFF_KEY);
      if(!raw) return null;
      const parsed = JSON.parse(raw);
      const storedAt = Number(parsed?.stored_at || 0);
      if(storedAt && (Date.now() - storedAt) > (CALL_RING_TIMEOUT_MS + 15000)){
        sessionStorage.removeItem(CALL_ANSWER_HANDOFF_KEY);
        return null;
      }
      const signal = normalizeSignal(parsed);
      if(!signal) return null;
      if(expectedCallId && signal.call_id !== expectedCallId) return null;
      if(expectedPeerId && signal.from_user_id !== expectedPeerId) return null;
      sessionStorage.removeItem(CALL_ANSWER_HANDOFF_KEY);
      return signal;
    }catch(_){
      return null;
    }
  }

  document.addEventListener("pointerdown", markCallInteraction, { passive:true });
  document.addEventListener("touchstart", markCallInteraction, { passive:true });
  document.addEventListener("keydown", markCallInteraction, { passive:true });

  function handledCallKey(callId){
    const clean = String(callId || "").trim();
    return clean ? `nova_handled_call:${clean}` : "";
  }

  function hasHandledCall(callId){
    const key = handledCallKey(callId);
    if(!key) return false;
    try{
      const expiresAt = Number(sessionStorage.getItem(key) || 0);
      if(expiresAt > Date.now()) return true;
      if(expiresAt) sessionStorage.removeItem(key);
    }catch(_){ }
    return false;
  }

  function markHandledCall(callId, ttlMs){
    const key = handledCallKey(callId);
    if(!key) return;
    const expiresAt = Date.now() + Math.max(60000, Number(ttlMs) || 120000);
    try{ sessionStorage.setItem(key, String(expiresAt)); }catch(_){ }
  }

  function isUuid(value){
    return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(String(value || "").trim());
  }

  function maybeMissingColumn(error){
    const text = `${error?.message || ""} ${error?.details || ""} ${error?.hint || ""} ${error?.code || ""}`.toLowerCase();
    if(!text) return false;
    if(text.includes("pgrst204")) return true;
    if(text.includes("schema cache") && text.includes("column")) return true;
    if(text.includes("could not find") && text.includes("column")) return true;
    if(text.includes("column") && text.includes("does not exist")) return true;
    if(text.includes("unknown column")) return true;
    return false;
  }

  function maybeMissingResource(error, resource){
    const text = `${error?.message || ""} ${error?.details || ""} ${error?.hint || ""}`.toLowerCase();
    if(!text) return false;
    const name = String(resource || "").trim().toLowerCase();
    return !!name && text.includes(name) && (text.includes("does not exist") || text.includes("not found") || text.includes("could not find the table") || text.includes("relation"));
  }

  function extractMissingColumnName(error){
    const text = `${error?.message || ""} ${error?.details || ""} ${error?.hint || ""}`;
    if(!text) return "";
    const match = text.match(/column\s+["'`]?([a-zA-Z0-9_]+)["'`]?/i);
    return match && match[1] ? String(match[1]).trim() : "";
  }

  function isUsersOnCallColumnError(error){
    const text = `${error?.message || ""} ${error?.details || ""} ${error?.hint || ""}`.toLowerCase();
    if(!text.includes("is_on_call")) return false;
    return maybeMissingColumn(error) || text.includes("does not exist");
  }

  function callSignalKey(signal){
    const type = String(signal?.type || "").trim().toLowerCase();
    const callId = String(signal?.call_id || "").trim();
    const fromUserId = String(signal?.from_user_id || "").trim();
    const toUserId = String(signal?.to_user_id || "").trim();
    if(!type || !callId || !fromUserId || !toUserId) return "";
    if(type === "ice") return "";
    const reason = String(signal?.reason || "").trim().toLowerCase();
    return `${type}:${callId}:${fromUserId}:${toUserId}:${reason}`;
  }

  function rememberCallSignalKey(signal){
    const key = callSignalKey(signal);
    if(!key) return true;
    if(seenCallSignalKeys.has(key)) return false;
    seenCallSignalKeys.add(key);
    if(seenCallSignalKeys.size > 900){
      const trimmed = Array.from(seenCallSignalKeys).slice(-450);
      seenCallSignalKeys.clear();
      trimmed.forEach(item => seenCallSignalKeys.add(item));
    }
    return true;
  }

    function isPlaceholderProfileName(value){
    const text = String(value || "").trim().toLowerCase();
    if(!text) return true;
    if(text === "user" || text === "owner" || text === "member" || text === "guest" || text === "unknown") return true;
    if(/^user[\s._-]*[a-z0-9]{4,}$/i.test(text)) return true;
    if(/^member[\s._-]*[a-z0-9]{4,}$/i.test(text)) return true;
    if(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(text)) return true;
    return false;
  }

  function fallbackNameFromEmail(value){
    const email = String(value || "").trim().toLowerCase();
    const local = String(email.split("@")[0] || "").replace(/[^a-z0-9._-]+/g, " ").replace(/[._-]+/g, " ").trim();
    if(!local) return "";
    return local.split(/\s+/).map(part => part ? (part[0].toUpperCase() + part.slice(1)) : "").filter(Boolean).join(" ").trim();
  }

  function getDisplayName(profile, fallback){
    const row = profile || {};
    const meta = row?.user_metadata && typeof row.user_metadata === "object" ? row.user_metadata : {};
    const candidates = [
      row.display_name,
      row.full_name,
      row.name,
      row.user_name,
      row.member_name,
      row.username,
      meta.full_name,
      meta.name,
      meta.username,
      fallback
    ];
    for(const candidate of candidates){
      const clean = String(candidate || "").trim();
      if(clean && !isPlaceholderProfileName(clean)) return clean;
    }
    return fallbackNameFromEmail(row.email || meta.email || "") || "User";
  }

  function getProfilePhoto(profile, fallback){
    const row = profile || {};
    return normalizeAssetUrl(row.photo || row.avatar_url || row.profile_icon || row.user_icon || row.user_avatar || row.member_avatar || row?.user_metadata?.avatar_url || row?.user_metadata?.picture || fallback || "");
  }

  function normalizeUserProfileRow(raw){
    const row = raw && typeof raw === "object" ? raw : {};
    const userId = String(row.user_id || row.id || "").trim();
    return {
      ...row,
      user_id: userId,
      display_name: getDisplayName(row, "User"),
      photo: getProfilePhoto(row, "")
    };
  }

  async function fetchUsersByIds(userIds){
    const ids = Array.from(new Set(Array.from(userIds || []).map(id => String(id || "").trim()).filter(id => isUuid(id))));
    if(!ids.length) return [];
    try{
      const { data, error } = await supa
        .from("users")
        .select("user_id,username,full_name,photo")
        .in("user_id", ids)
        .limit(Math.max(50, ids.length + 6));
      if(error){
        console.error("users_fetch_failed", error);
        return [];
      }
      return (data || []).map(normalizeUserProfileRow).filter(row => row.user_id);
    }catch(_){
      return [];
    }
  }

  function normalizeMemberCount(value){
    const count = Number(value);
    return Number.isFinite(count) && count >= 0 ? Math.floor(count) : 0;
  }

  function groupMemberLabel(count){
    const n = normalizeMemberCount(count);
    return n > 0 ? `${n} Members` : "Members";
  }

  function isAdminMemberRow(row){
    const memberId = String(row?.user_id || "").trim();
    return !!(groupOwnerId && memberId && memberId === groupOwnerId);
  }

  function canManageGroupMembers(){
    if(!isGroupChat || !isUuid(myId)) return false;
    return String(groupOwnerId || "").trim() === myId;
  }

  function canCurrentUserSendGroupMessages(){
    return true;
  }

  function closeGroupMemberActionMenus(){
    document.querySelectorAll(".group-member-menu").forEach(node => {
      node.classList.add("hidden");
    });
  }

  function findGroupCreatorMember(){
    const owner = String(groupOwnerId || "").trim();
    if(!owner) return null;
    return groupMembers.find(member => String(member?.user_id || "").trim() === owner) || null;
  }

  function findGroupAdminMember(){
    return findGroupCreatorMember();
  }

  function buildGroupMetaText(){
    const memberCount = normalizeMemberCount(groupInfo?.member_count || groupMembers.length);
    const memberLabel = groupMemberLabel(memberCount);
    const admin = findGroupAdminMember();
    const parts = [memberLabel];
    if(admin){
      parts.push(`Admin: ${getDisplayName(admin, "Admin")}`);
    }
    return parts.join(" | ");
  }

  function isPlaceholderGroupName(value){
    const text = String(value || "").trim().toLowerCase();
    return !text || text === "group" || text === "novagroup" || text === "new group" || text === "chat group" || text === "untitled group";
  }

  const GROUP_NAME_FIELDS = ["name", "group_name", "title"];
  const GROUP_ICON_FIELDS = ["group_icon", "icon_url", "image_url", "image", "photo", "avatar_url", "img"];

  function readGroupField(row, keys){
    const source = row && typeof row === "object" ? row : {};
    const list = Array.isArray(keys) ? keys : [];
    for(const key of list){
      const value = String(source?.[key] || "").trim();
      if(value) return value;
    }
    return "";
  }

  function resolveGroupName(groupRow, fallbackName){
    const row = groupRow && typeof groupRow === "object" ? groupRow : {};
    const candidates = [readGroupField(row, GROUP_NAME_FIELDS), fallbackName];
    for(const candidate of candidates){
      const clean = String(candidate || "").trim();
      if(clean && !isPlaceholderGroupName(clean)) return clean;
    }
    for(const candidate of candidates){
      const clean = String(candidate || "").trim();
      if(clean) return clean;
    }
    return "Group";
  }

  function resolveGroupAvatar(groupRow, members, fallbackImage){
    const row = groupRow && typeof groupRow === "object" ? groupRow : {};
    return resolveGroupIconUrl(readGroupField(row, GROUP_ICON_FIELDS) || fallbackImage || "");
  }

  function readFirstValue(row, keys){
    const source = row && typeof row === "object" ? row : {};
    const list = Array.isArray(keys) ? keys : [];
    for(const key of list){
      const value = String(source?.[key] || "").trim();
      if(value) return value;
    }
    return "";
  }

  function resolveGroupOwnerId(groupRow){
    const row = groupRow && typeof groupRow === "object" ? groupRow : {};
    return String(row?.owner_id || "").trim();
  }

  function parseRoleText(row){
    const value = String(row?.role || row?.member_role || row?.user_role || "").trim().toLowerCase();
    return value;
  }

  function resolveMemberUserId(row, index){
    return String(row?.user_id || "").trim() || `member_${index}`;
  }

  function resolveMemberName(row){
    return String(row?.user_name || row?.display_name || row?.full_name || row?.username || row?.name || row?.member_name || "Member").trim() || "Member";
  }

  function resolveMemberAvatar(row){
    return normalizeAssetUrl(row?.user_avatar || row?.avatar_url || row?.photo || row?.member_avatar || "");
  }

  function resolveMemberGroupId(row){
    return String(row?.group_id || "").trim();
  }

  function isMemberAdmin(row, ownerHint, memberUserId){
    const ownerId = String(ownerHint || groupOwnerId || "").trim();
    const userId = String(memberUserId || resolveMemberUserId(row, 0) || "").trim();
    return !!(ownerId && userId && userId === ownerId);
  }

  async function enrichGroupMembers(members, ownerHint){
    const ownerId = String(ownerHint || groupOwnerId || "").trim();
    const rows = Array.isArray(members) ? members.slice() : [];
    const ids = Array.from(new Set(rows.map(member => String(member?.user_id || "").trim()).filter(id => isUuid(id))));
    const profiles = await fetchUsersByIds(ids);
    const profileById = new Map();
    profiles.forEach(profile => {
      const id = String(profile?.user_id || "").trim();
      if(id) profileById.set(id, profile);
    });
    return rows.map(member => {
      const userId = String(member?.user_id || "").trim();
      const owner = !!(ownerId && userId === ownerId);
      const profile = profileById.get(userId) || null;
      const next = { ...(member || {}) };
      next.user_id = userId || String(resolveMemberUserId(member, 0) || "").trim();
      next.display_name = profile ? getDisplayName(profile, resolveMemberName(member)) : getDisplayName(member, resolveMemberName(member));
      next.photo = profile ? getProfilePhoto(profile, resolveMemberAvatar(member)) : getProfilePhoto(member, resolveMemberAvatar(member));
      next.role = owner ? "owner" : "member";
      next.is_admin = owner;
      if(!next._source && member && typeof member === "object" && member !== next){
        next._source = member._source || member;
      }
      return next;
    }).filter(member => String(member?.user_id || "").trim());
  }

  const GROUP_MEMBER_GROUP_KEYS = ["group_id"];
  const GROUP_MEMBER_USER_KEYS = ["user_id"];

  async function removeGroupMemberFromDatabase(member){
    const memberId = String(member?.user_id || "").trim();
    const cleanGroupId = String(groupInfo?.id || receiverId || "").trim();
    if(!memberId || !cleanGroupId) return false;
    try{
      const { error } = await supa
        .from("group_members")
        .delete()
        .eq("group_id", cleanGroupId)
        .eq("user_id", memberId);
      return !error;
    }catch(_){
      return false;
    }
  }

  async function makeGroupMemberAdminInDatabase(member){
    return false;
  }

  const GROUP_LOOKUP_ID_KEYS = ["id"];

  function normalizeGroupAdminOnly(row){
    return false;
  }

  async function fetchGroupRowById(groupId){
    const cleanGroupId = String(groupId || receiverId || "").trim();
    if(!cleanGroupId) return null;
    try{
      const { data, error } = await supa
        .from("groups")
        .select("*")
        .eq("id", cleanGroupId)
        .limit(1);
      if(error){
        if(!maybeMissingResource(error, "groups")) console.error("group_fetch_failed", error);
        return null;
      }
      return Array.isArray(data) && data.length ? data[0] : null;
    }catch(_){
      return null;
    }
  }

  async function canCurrentUserAccessGroup(groupId, groupRow){
    const cleanGroupId = String(groupId || receiverId || "").trim();
    if(!cleanGroupId || !isUuid(myId)) return false;
    const ownerId = String(resolveGroupOwnerId(groupRow) || "").trim();
    if(ownerId && ownerId === myId) return true;
    try{
      const { data, error } = await supa
        .from("group_members")
        .select("group_id,user_id")
        .eq("group_id", cleanGroupId)
        .eq("user_id", myId)
        .limit(1);
      if(error){
        if(!maybeMissingResource(error, "group_members")) console.error("group_access_fetch_failed", error);
        return false;
      }
      return Array.isArray(data) && data.length > 0;
    }catch(_){
      return false;
    }
  }

  async function fetchGroupMembers(groupId, ownerHint){
    const cleanGroupId = String(groupId || receiverId || "").trim();
    const ownerId = String(ownerHint || groupOwnerId || "").trim();
    if(!cleanGroupId) return [];
    let membershipRows = [];
    try{
      const { data, error } = await supa
        .from("group_members")
        .select("group_id,user_id")
        .eq("group_id", cleanGroupId)
        .limit(1200);
      if(error){
        if(!maybeMissingResource(error, "group_members")) console.error("group_members_fetch_failed", error);
        return [];
      }
      membershipRows = Array.isArray(data) ? data : [];
    }catch(_){
      return [];
    }
    const userIds = [];
    const seen = new Set();
    const addUserId = (value) => {
      const uid = String(value || "").trim();
      if(!isUuid(uid) || seen.has(uid)) return;
      seen.add(uid);
      userIds.push(uid);
    };
    membershipRows.forEach(row => addUserId(row?.user_id));
    addUserId(ownerId);
    const profiles = await fetchUsersByIds(userIds);
    const profileById = new Map();
    profiles.forEach(profile => {
      const uid = String(profile?.user_id || "").trim();
      if(uid) profileById.set(uid, profile);
    });
    return userIds.map(uid => {
      const owner = !!(ownerId && uid === ownerId);
      const profile = profileById.get(uid) || {};
      return {
        user_id: uid,
        display_name: getDisplayName(profile, owner ? "Owner" : "Member"),
        photo: getProfilePhoto(profile, ""),
        role: owner ? "owner" : "member",
        is_admin: owner,
        _source: { group_id: cleanGroupId, user_id: uid }
      };
    }).filter(member => String(member?.user_id || "").trim());
  }

  async function fetchGroupInfo(groupId){
    const cleanGroupId = String(groupId || receiverId || "").trim();
    const groupRow = await fetchGroupRowById(cleanGroupId);
    groupOwnerId = resolveGroupOwnerId(groupRow);
    const resolvedGroupId = String(groupRow?.id || cleanGroupId).trim();
    let members = await fetchGroupMembers(resolvedGroupId, groupOwnerId);
    const hasKnownMembership = !!(groupOwnerId && groupOwnerId === myId)
      || members.some(member => String(member?.user_id || "").trim() === myId);
    let allowed = hasKnownMembership;
    if(!allowed){
      allowed = await canCurrentUserAccessGroup(resolvedGroupId, groupRow);
    }
    if(!allowed && (groupRow || members.length)){
      allowed = true;
    }
    if(!allowed){
      groupAdminOnly = false;
      const resolvedName = resolveGroupName(groupRow, receiverNameParam || "Group");
      const resolvedAvatar = resolveGroupAvatar(groupRow, [], DEFAULT_GROUP_ICON);
      return {
        id: resolvedGroupId,
        name: resolvedName,
        icon_url: resolvedAvatar,
        group_icon: resolvedAvatar,
        member_count: 0,
        members: [],
        allowed: false,
        is_admin_only: false
      };
    }

    if(groupOwnerId && !members.some(member => String(member?.user_id || "").trim() === groupOwnerId)){
      const ownerProfiles = await fetchUsersByIds([groupOwnerId]);
      const ownerProfile = ownerProfiles[0] || null;
      if(ownerProfile){
        members.unshift({
          user_id: groupOwnerId,
          display_name: getDisplayName(ownerProfile, "Owner"),
          photo: getProfilePhoto(ownerProfile, ""),
          role: "owner",
          is_admin: true,
          _source: null
        });
      }
    }
    members = await enrichGroupMembers(members, groupOwnerId);
    if(!groupOwnerId){
      const adminMember = members.find(member => member?.is_admin);
      if(adminMember?.user_id){
        groupOwnerId = String(adminMember.user_id || "").trim();
      }
    }
    groupAdminOnly = normalizeGroupAdminOnly(groupRow);
    const resolvedName = resolveGroupName(groupRow, receiverNameParam || "Group");
    const resolvedAvatar = resolveGroupAvatar(groupRow, members, DEFAULT_GROUP_ICON);
    return {
      id: resolvedGroupId,
      name: resolvedName,
      icon_url: resolvedAvatar,
      group_icon: resolvedAvatar,
      member_count: normalizeMemberCount(members.length),
      members,
      allowed: true,
      is_admin_only: groupAdminOnly
    };
  }

  function getInitial(text){
    const clean = String(text || "").trim();
    return clean ? clean[0].toUpperCase() : "U";
  }

  function setAvatar(el, imageUrl, fallbackText){
    if(!el) return;
    const src = String(imageUrl || "").trim();
    el.style.display = "flex";
    el.style.visibility = "visible";
    el.style.opacity = "1";
    el.style.alignItems = "center";
    el.style.justifyContent = "center";
    el.style.overflow = "hidden";
    if(src){
      el.style.backgroundImage = `url('${src.replace(/'/g, "%27")}')`;
      el.style.backgroundColor = "transparent";
      el.style.color = "transparent";
      el.textContent = "";
      return;
    }
    el.style.backgroundImage = "";
    el.style.backgroundColor = "rgba(255,255,255,0.22)";
    el.style.color = "#fff";
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
    closeGroupMemberActionMenus();
    if(!groupMembers.length){
      groupMembersList.innerHTML = "<p class='text-sm text-gray-500'>No members found.</p>";
      return;
    }
    const canManage = canManageGroupMembers();
    groupMembers.forEach(member => {
      const row = member && typeof member === "object" ? member : {};
      const memberId = String(row?.user_id || "").trim();
      const name = getDisplayName(row, "Member");
      const photo = getProfilePhoto(row, "");
      const owner = !!(groupOwnerId && memberId && memberId === groupOwnerId);
      const line = document.createElement("div");
      line.className = "flex items-center gap-3 p-2 rounded-lg bg-gray-50";
      const avatar = document.createElement("div");
      avatar.className = "w-9 h-9 rounded-full bg-orange-200 bg-cover bg-center text-xs font-bold flex items-center justify-center";
      setAvatar(avatar, photo, name);
      const meta = document.createElement("div");
      meta.className = "flex-1 min-w-0 flex items-center gap-2";
      const nameEl = document.createElement("div");
      nameEl.className = "text-sm font-medium text-gray-800 truncate";
      nameEl.textContent = name;
      meta.appendChild(nameEl);
      const badge = document.createElement("span");
      badge.className = owner ? "text-[10px] font-semibold px-2 py-[2px] rounded-full bg-orange-100 text-orange-700" : "text-[10px] font-semibold px-2 py-[2px] rounded-full bg-gray-100 text-gray-600";
      badge.textContent = owner ? "Admin" : "Member";
      meta.appendChild(badge);
      line.appendChild(avatar);
      line.appendChild(meta);
      if(canManage && memberId && memberId !== myId && !owner){
        const actionWrap = document.createElement("div");
        actionWrap.className = "relative group-member-actions";
        const actionBtn = document.createElement("button");
        actionBtn.type = "button";
        actionBtn.className = "w-8 h-8 rounded-full text-lg text-gray-500 hover:bg-gray-200";
        actionBtn.textContent = "\u22EE";
        const menu = document.createElement("div");
        menu.className = "group-member-menu hidden absolute right-0 top-9 min-w-[160px] rounded-xl border border-gray-200 bg-white shadow-lg overflow-hidden z-20";
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "block w-full text-left px-4 py-3 text-sm text-red-600 hover:bg-red-50";
        btn.textContent = "Remove from group";
        btn.addEventListener("click", async event => {
          event.stopPropagation();
          closeGroupMemberActionMenus();
          if(actionBtn.disabled) return;
          actionBtn.disabled = true;
          try{
            const ok = await removeGroupMemberFromDatabase(row);
            if(!ok){
              alert("Unable to remove member from group.");
              return;
            }
            groupMembers = groupMembers.filter(memberRow => String(memberRow?.user_id || "").trim() !== memberId);
            if(groupInfo){
              groupInfo.members = groupMembers.slice();
              groupInfo.member_count = normalizeMemberCount(groupMembers.length);
            }
            renderPeerHeader();
          }finally{
            actionBtn.disabled = false;
          }
        });
        menu.appendChild(btn);
        actionBtn.addEventListener("click", event => {
          event.stopPropagation();
          const shouldOpen = menu.classList.contains("hidden");
          closeGroupMemberActionMenus();
          if(shouldOpen) menu.classList.remove("hidden");
        });
        actionWrap.appendChild(actionBtn);
        actionWrap.appendChild(menu);
        line.appendChild(actionWrap);
      }
      groupMembersList.appendChild(line);
    });
  }

  function collectInfoMediaEntries(){
    const rows = [...messageRows]
      .filter(row => !shouldHideRow(row))
      .sort((a, b) => toEpoch(b?.created_at) - toEpoch(a?.created_at));
    const out = [];
    const seen = new Set();
    rows.forEach((row) => {
      const messageId = getMessageId(row) || `${row?.created_at || ""}_${row?.sender_id || ""}`;
      const attachments = getAttachmentUrls(row);
      attachments.forEach((url, index) => {
        const clean = String(url || "").trim();
        if(!clean) return;
        if(seen.has(clean)) return;
        seen.add(clean);
        out.push({
          id: `${messageId}_${index}`,
          url: clean,
          type: isVideoUrl(clean) ? "video" : "image"
        });
      });
    });
    return out.slice(0, 60);
  }

  function collectInfoChattingEntries(){
    const rows = [...messageRows]
      .filter(row => !shouldHideRow(row))
      .sort((a, b) => toEpoch(b?.created_at) - toEpoch(a?.created_at));
    const out = [];
    rows.forEach(row => {
      const text = getMessageTextForPreview(row);
      if(!text) return;
      out.push(text);
    });
    return out.slice(0, 40);
  }

  function renderInfoMediaStrip(){
    if(!infoMediaStrip) return;
    infoMediaStrip.innerHTML = "";
    const media = collectInfoMediaEntries();
    if(!media.length){
      infoMediaStrip.innerHTML = "<div class='info-empty'>No media shared yet.</div>";
      return;
    }
    media.forEach(entry => {
      const tile = document.createElement("button");
      tile.type = "button";
      tile.className = "info-media-tile";
      tile.addEventListener("click", () => {
        openMediaPreview(entry.url, entry.type);
      });
      if(entry.type === "video"){
        const video = document.createElement("video");
        video.src = entry.url;
        video.muted = true;
        video.playsInline = true;
        video.preload = "metadata";
        tile.appendChild(video);
        const badge = document.createElement("span");
        badge.className = "info-media-video-badge";
        badge.textContent = "Video";
        tile.appendChild(badge);
      }else{
        const img = document.createElement("img");
        img.src = entry.url;
        img.alt = "media";
        img.loading = "lazy";
        tile.appendChild(img);
      }
      infoMediaStrip.appendChild(tile);
    });
  }

  function renderInfoChattingStrip(){
    if(!infoChattingStrip) return;
    infoChattingStrip.innerHTML = "";
    const snippets = collectInfoChattingEntries();
    if(!snippets.length){
      infoChattingStrip.innerHTML = "<div class='info-empty'>No chats yet.</div>";
      return;
    }
    snippets.forEach(text => {
      const chip = document.createElement("div");
      chip.className = "info-chat-chip";
      chip.textContent = text.length > 180 ? `${text.slice(0, 177)}...` : text;
      infoChattingStrip.appendChild(chip);
    });
  }

  function applyInfoTabUi(){
    if(!infoTabMedia || !infoTabChatting || !infoMediaPanel || !infoChattingPanel) return;
    const showMedia = activeInfoTab !== "chatting";
    infoTabMedia.classList.toggle("active", showMedia);
    infoTabChatting.classList.toggle("active", !showMedia);
    infoMediaPanel.classList.toggle("show", showMedia);
    infoChattingPanel.classList.toggle("show", !showMedia);
  }

  function switchInfoTab(tabName){
    activeInfoTab = String(tabName || "media").toLowerCase() === "chatting" ? "chatting" : "media";
    applyInfoTabUi();
  }

  function renderInfoPanels(){
    renderInfoMediaStrip();
    renderInfoChattingStrip();
    applyInfoTabUi();
  }

  function renderPeerHeader(){
    updateHeaderMenuActions();
    const name = isGroupChat
      ? resolveGroupName(groupInfo, receiverNameParam || "Group")
      : getDisplayName(peerProfile, receiverNameParam || "User");
    const photo = isGroupChat
      ? resolveGroupAvatar(groupInfo, groupMembers, DEFAULT_GROUP_ICON)
      : getProfilePhoto(peerProfile, receiverPicParam);
    const userNameEl = document.getElementById("userName");
    const userMetaEl = document.getElementById("userMeta");
    const userAvatarEl = document.getElementById("userAvatar");
    const infoNameEl = document.getElementById("infoName");
    const infoPicEl = document.getElementById("infoPic");
    userNameEl.textContent = name;
    infoNameEl.textContent = name;
    userNameEl.style.display = "block";
    userNameEl.style.visibility = "visible";
    userNameEl.style.opacity = "1";
    if(userMetaEl){
      userMetaEl.style.display = "block";
      userMetaEl.style.visibility = "visible";
      userMetaEl.style.opacity = "1";
    }
    setAvatar(userAvatarEl, photo, name);
    setAvatar(infoPicEl, photo, name);
    if(isGroupChat){
      const label = [buildGroupMetaText(), groupAdminOnly ? "Only admins can send messages" : ""]
        .filter(Boolean)
        .join(" | ");
      if(userMeta) userMeta.textContent = label;
      if(infoMeta) infoMeta.textContent = label;
      renderGroupMembers();
      applyGroupMessagingPermissions();
      return;
    }
    if(userMeta) userMeta.textContent = "";
    if(infoMeta) infoMeta.textContent = "Real-time verified account";
    renderGroupMembers();
  }

  function applyGroupMessagingPermissions(){
    if(!isGroupChat) return;
    const canSend = canCurrentUserSendGroupMessages();
    mainInput.disabled = !canSend;
    sendBtn.disabled = !canSend;
    mainInput.placeholder = canSend ? "Type a message" : "Only admins can send messages";
    sendBtn.classList.toggle("opacity-50", !canSend);
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
    if(outgoing && !isGroupChat){
      const tick = document.createElement("span");
      const isRead = toReadFlag(row?.is_read) === 1;
      tick.className = `msg-tick ${isRead ? "msg-tick-read" : ""}`.trim();
      tick.textContent = "\u2713\u2713";
      tick.title = isRead ? "Seen" : "Sent";
      metaRow.appendChild(tick);
    }
    bubble.appendChild(metaRow);
    return bubble;
  }

  function toReadFlag(value){
    if(value === true || value === 1) return 1;
    const text = String(value || "").trim().toLowerCase();
    return (text === "true" || text === "1") ? 1 : 0;
  }

  function messageRowFingerprint(row){
    const id = String(getMessageId(row) || row?.__synthetic_id || "").trim();
    const senderId = String(row?.sender_id || "").trim();
    const receiverIdValue = String(row?.receiver_id || "").trim();
    const groupId = String(row?.group_id || "").trim();
    const createdAt = String(row?.created_at || "").trim();
    const content = textOrEmpty(row?.content || row?.message).slice(0, 120);
    const msgType = String(row?.message_type || "").trim();
    const readFlag = toReadFlag(row?.is_read);
    const deletedFlag = isMessageDeletedEverywhere(row) ? 1 : 0;
    return `${id}|${senderId}|${receiverIdValue}|${groupId}|${createdAt}|${msgType}|${readFlag}|${deletedFlag}|${content}`;
  }

  function computeThreadSnapshot(rows){
    return (rows || [])
      .map(row => messageRowFingerprint(row))
      .sort()
      .join("||");
  }

  function refreshThreadSnapshot(){
    lastThreadSnapshot = computeThreadSnapshot(messageRows);
  }

  function isNearBottom(){
    return Math.abs((chatWindow.scrollHeight - chatWindow.clientHeight) - chatWindow.scrollTop) < 100;
  }

  function renderMessageList(){
    msgHolder.innerHTML = "";
    const rows = [...messageRows]
      .filter(row => !shouldHideRow(row))
      .sort((a, b) => toEpoch(a?.created_at) - toEpoch(b?.created_at));
    if(!rows.length){
      msgHolder.innerHTML = "<p class='text-center text-xs text-gray-400'>No messages.</p>";
      renderInfoPanels();
      return;
    }
    rows.forEach(row => msgHolder.appendChild(buildMessageBubble(row)));
    renderInfoPanels();
  }

  function addMessageRow(row){
    upsertMessageRow(row);
    renderMessageList();
    refreshThreadSnapshot();
  }

  async function fetchUserProfile(uid){
    const cleanUid = String(uid || "").trim();
    if(!isUuid(cleanUid)) return null;
    try{
      const { data, error } = await supa
        .from("users")
        .select("user_id,username,full_name,photo")
        .eq("user_id", cleanUid)
        .maybeSingle();
      if(error){
        console.error("profile_fetch_failed", error);
        return null;
      }
      return data ? normalizeUserProfileRow(data) : null;
    }catch(_){
      return null;
    }
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
        "id,sender_id,receiver_id,group_id,content,created_at,attachment_urls,attachment_url,message_type,reply_to_id,reply_preview,deleted_for_everyone,is_starred,deleted_by_name,is_read",
        "id,sender_id,receiver_id,group_id,content,created_at,attachment_urls,attachment_url,message_type,reply_to_id,deleted_for_everyone,is_starred,is_read",
        "id,sender_id,receiver_id,group_id,content,created_at,attachment_urls,attachment_url,message_type,is_read",
        "id,sender_id,receiver_id,group_id,content,created_at,is_read",
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
          refreshThreadSnapshot();
          scrollBottom();
  }

  async function syncMessagesSilently(){
    const { data, error } = await fetchThreadRows();
    if(error) return;
    const rows = Array.isArray(data) ? data : [];
    const nextSnapshot = computeThreadSnapshot(rows);
    if(nextSnapshot === lastThreadSnapshot) return;
    const stickToBottom = isNearBottom();
    messageRows = [];
    renderedMessageIds = new Set();
    rows.forEach(upsertMessageRow);
    renderMessageList();
    refreshThreadSnapshot();
    if(stickToBottom) scrollBottom();
    if(!isGroupChat && document.visibilityState === "visible"){
      markDirectMessagesRead().catch(() => {});
    }
  }

  function startMessageSync(){
    if(messageSyncTimer){
      clearInterval(messageSyncTimer);
      messageSyncTimer = null;
    }
    messageSyncTimer = setInterval(() => {
      if(document.hidden) return;
      syncMessagesSilently().catch(() => {});
    }, 2200);
  }

  function stopMessageSync(){
    if(!messageSyncTimer) return;
    clearInterval(messageSyncTimer);
    messageSyncTimer = null;
  }

  function canMarkDirectMessagesReadNow(){
    if(document.visibilityState && document.visibilityState !== "visible") return false;
    if(typeof document.hasFocus === "function" && !document.hasFocus()) return false;
    return true;
  }

  async function markDirectMessagesRead(){
    if(isGroupChat || !isUuid(myId) || !isUuid(receiverId)) return;
    if(!canMarkDirectMessagesReadNow()) return;
    const attempts = [
      () => supa
        .from("messages")
        .update({ is_read: true })
        .eq("receiver_id", myId)
        .eq("sender_id", receiverId)
        .eq("is_read", false),
      () => supa
        .from("messages")
        .update({ is_read: true })
        .eq("receiver_id", myId)
        .eq("sender_id", receiverId)
    ];
    for(const run of attempts){
      try{
        const { error } = await run();
        if(!error) return;
        if(!maybeMissingColumn(error)) return;
      }catch(_){ return; }
    }
  }

  async function insertMessageWithFallback(payload){
    const variants = [];
    const seen = new Set();
    const pushVariant = (candidate) => {
      const row = candidate && typeof candidate === "object" ? candidate : {};
      const compact = Object.fromEntries(Object.entries(row).filter(([, value]) => value !== undefined));
      if(!Object.keys(compact).length) return;
      const key = JSON.stringify(Object.keys(compact).sort().map(name => [name, compact[name]]));
      if(seen.has(key)) return;
      seen.add(key);
      variants.push(compact);
    };

    pushVariant({ ...payload });
    if("reply_preview" in payload){ const v = { ...payload }; delete v.reply_preview; pushVariant(v); }
    if("reply_to_id" in payload){ const v = { ...payload }; delete v.reply_to_id; pushVariant(v); }
    if("attachment_urls" in payload){ const v = { ...payload }; delete v.attachment_urls; pushVariant(v); }
    if("attachment_url" in payload){ const v = { ...payload }; delete v.attachment_url; pushVariant(v); }
    if("message_type" in payload){ const v = { ...payload }; delete v.message_type; pushVariant(v); }
    pushVariant({
      sender_id: payload.sender_id,
      ...buildThreadPayload(),
      content: payload.content,
      is_read: false
    });

    for(let idx = 0; idx < variants.length; idx += 1){
      const candidate = variants[idx];
      const { data, error } = await supa
        .from("messages")
        .insert(candidate)
        .select("id,sender_id,receiver_id,group_id,content,created_at,attachment_urls,attachment_url,message_type,reply_to_id,reply_preview,is_read")
        .maybeSingle();
      if(!error) return { data: data ? { ...candidate, ...data } : null, error: null };
      if(!maybeMissingColumn(error)) return { data: null, error };
      const missingColumn = extractMissingColumnName(error);
      if(missingColumn && Object.prototype.hasOwnProperty.call(candidate, missingColumn)){
        const stripped = { ...candidate };
        delete stripped[missingColumn];
        pushVariant(stripped);
      }
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
      if(!isGroupChat && String(data?.sender_id || "") === myId && String(data?.receiver_id || "") === receiverId){
        if(toReadFlag(data?.is_read) === 1){
          data.is_read = false;
          const messageId = String(data?.id || "").trim();
          if(messageId){
            updateMessageRowWithFallback(messageId, [{ is_read: false }]).catch(() => {});
          }
        }
      }
      addMessageRow(data);
      scrollBottom();
    }
    if(!isGroupChat){
      notifyChatPush(data || rawPayload || {}).catch(() => {});
    }
    return true;
  }

  async function sendMessage(){
    if(isGroupChat && !canCurrentUserSendGroupMessages()){
      showNotice("Only admins can send messages.");
      return;
    }
    const text = String(mainInput.value || "").trim();
    if(!text) return;
    mainInput.value = "";
    const payload = {
      sender_id: myId,
      ...buildThreadPayload(),
      content: text,
      is_read: false,
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
          refreshThreadSnapshot();
          scrollBottom();
        }
        return;
      }
      addMessageRow(row);
      scrollBottom();
      if(!isGroupChat && String(row?.sender_id || "") === receiverId && String(row?.receiver_id || "") === myId){
        markDirectMessagesRead().catch(() => {});
      }
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
      id: String(src.id || payload.signal_id || payload.signalId || src.signal_id || src.signalId || "").trim(),
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

  function getCallRoomKey(){
    if(!isUuid(myId) || !isUuid(receiverId)) return "";
    return [myId, receiverId].sort().join("_");
  }

  function isPeerOnCallByPresence(){
    if(!callRealtimeChannel || !isUuid(receiverId)) return false;
    try{
      const state = typeof callRealtimeChannel.presenceState === "function" ? callRealtimeChannel.presenceState() : {};
      const rows = Array.isArray(state?.[receiverId]) ? state[receiverId] : [];
      return rows.some(row => !!row?.on_call);
    }catch(_){
      return false;
    }
  }

  async function trackCallPresence(){
    if(!callRealtimeChannel || !isUuid(myId)) return;
    try{
      await callRealtimeChannel.track({ user_id: myId, on_call: !!activeCall, ts: Date.now() });
    }catch(_){ }
  }

  function teardownCallRealtimeChannel(){
    if(!callRealtimeChannel) return;
    try{ supa.removeChannel(callRealtimeChannel); }catch(_){ }
    callRealtimeChannel = null;
  }

  function setupCallRealtimeChannel(){
    teardownCallRealtimeChannel();
    if(!supa || isGroupChat) return;
    const roomKey = getCallRoomKey();
    if(!roomKey) return;
    callRealtimeChannel = supa
      .channel(`call_dm_${roomKey}`, {
        config: {
          broadcast: { ack: true },
          presence: { key: myId }
        }
      })
      .on("broadcast", { event: CALL_SIGNAL_BROADCAST_EVENT }, payload => {
        const signal = normalizeSignal(payload?.payload || payload);
        if(signal) handleCallSignal(signal).catch(() => {});
      })
      .on("presence", { event: "sync" }, () => {
        if(!activeCall && isPeerOnCallByPresence()){
          setCallStatus("User busy in call");
        }
      })
      .subscribe((status) => {
        if(status === "SUBSCRIBED"){
          trackCallPresence().catch(() => {});
        }
      });
  }

  async function sendCallSignalBroadcast(reqBody){
    if(!callRealtimeChannel) return false;
    try{
      const status = await callRealtimeChannel.send({
        type: "broadcast",
        event: CALL_SIGNAL_BROADCAST_EVENT,
        payload: reqBody
      });
      return status === "ok";
    }catch(_){
      return false;
    }
  }

  async function setMyOnCallStatus(isOnCall){
    if(!supa || !isUuid(myId) || !supportsUsersOnCallColumn) return;
    const nextValue = !!isOnCall;
    const idColumns = ["user_id", "id"];
    for(const idColumn of idColumns){
      try{
        const { error } = await supa
          .from("users")
          .update({ is_on_call: nextValue })
          .eq(idColumn, myId);
        if(!error) return;
        if(isUsersOnCallColumnError(error)){
          supportsUsersOnCallColumn = false;
          return;
        }
        const text = `${error?.message || ""} ${error?.details || ""}`.toLowerCase();
        const missingIdColumn = maybeMissingColumn(error) && text.includes(idColumn);
        if(missingIdColumn) continue;
        return;
      }catch(_){
        return;
      }
    }
  }

  async function readUserOnCallStatus(userId){
    const uid = String(userId || "").trim();
    if(!supa || !isUuid(uid) || !supportsUsersOnCallColumn) return false;
    const idColumns = ["user_id", "id"];
    for(const idColumn of idColumns){
      try{
        const { data, error } = await supa
          .from("users")
          .select("is_on_call")
          .eq(idColumn, uid)
          .maybeSingle();
        if(!error){
          return !!(data && data.is_on_call);
        }
        if(isUsersOnCallColumnError(error)){
          supportsUsersOnCallColumn = false;
          return false;
        }
        const text = `${error?.message || ""} ${error?.details || ""}`.toLowerCase();
        const missingIdColumn = maybeMissingColumn(error) && text.includes(idColumn);
        if(missingIdColumn) continue;
        return false;
      }catch(_){
        return false;
      }
    }
    return false;
  }

  async function isPeerBusyForOutgoingCall(){
    if(isPeerOnCallByPresence()) return true;
    return readUserOnCallStatus(receiverId);
  }

  function getMyDisplayNameForPush(){
    return String(
      myUser?.user_metadata?.full_name ||
      myUser?.user_metadata?.name ||
      myUser?.user_metadata?.username ||
      myUser?.email ||
      "New Message"
    ).trim() || "New Message";
  }

  async function notifyChatPush(rawPayload){
    if(isGroupChat) return;
    const toUserId = String(receiverId || "").trim();
    if(!isUuid(toUserId) || toUserId === myId) return;
    const payload = rawPayload && typeof rawPayload === "object" ? rawPayload : {};
    const text = textOrEmpty(payload.content || payload.message);
    const attachments = getAttachmentUrls(payload);
    const bodyText = text
      ? text.slice(0, 200)
      : (attachments.length ? "Sent an attachment" : "New message");
    const requestBody = {
      to_user_id: toUserId,
      title: getMyDisplayNameForPush(),
      body: bodyText,
      data: {
        type: "chat_message",
        sender_id: String(myId || "").trim(),
        receiver_id: toUserId,
        is_group: "0"
      }
    };

    const bases = buildApiBases();
    for(const base of bases){
      try{
        const res = await fetch(apiUrl(base, "/api/push/notify"), {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(requestBody)
        });
        if(res.ok) return;
      }catch(_){ }
    }
  }

  async function sendCallSignal(payload){
    const type = String(payload?.type || "").trim().toLowerCase();
    const reqBody = {
      type,
      call_id: String(payload?.call_id || "").trim(),
      from_user_id: String(payload?.from_user_id || "").trim(),
      to_user_id: String(payload?.to_user_id || "").trim(),
      media_type: String(payload?.media_type || "").trim().toLowerCase() === "video" ? "video" : "audio",
      signal_id: typeof crypto.randomUUID === "function"
        ? crypto.randomUUID()
        : `${Date.now()}_${Math.random().toString(16).slice(2)}`
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

    const broadcastOk = await sendCallSignalBroadcast(reqBody);
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
    return broadcastOk;
  }

  function clearIncomingOfferTimer(){
    if(incomingOfferTimer){
      clearTimeout(incomingOfferTimer);
      incomingOfferTimer = null;
    }
  }

  function clearOutgoingOfferTimer(){
    if(outgoingOfferTimer){
      clearTimeout(outgoingOfferTimer);
      outgoingOfferTimer = null;
    }
  }

  function playRingBurst(mode){
    const AudioCtx = window.AudioContext || window.webkitAudioContext;
    if(!AudioCtx) return;
    try{
      if(!ringAudioCtx) ringAudioCtx = new AudioCtx();
      if(ringAudioCtx.state === "suspended") ringAudioCtx.resume().catch(() => {});
      const now = ringAudioCtx.currentTime;
      const freqA = mode === "incoming" ? 840 : 720;
      const freqB = mode === "incoming" ? 620 : 560;
      [freqA, freqB].forEach((freq, idx) => {
        const startAt = now + (idx * 0.22);
        const osc = ringAudioCtx.createOscillator();
        const gain = ringAudioCtx.createGain();
        osc.type = "sine";
        osc.frequency.setValueAtTime(freq, startAt);
        gain.gain.setValueAtTime(0.0001, startAt);
        gain.gain.exponentialRampToValueAtTime(0.07, startAt + 0.02);
        gain.gain.exponentialRampToValueAtTime(0.0001, startAt + 0.17);
        osc.connect(gain);
        gain.connect(ringAudioCtx.destination);
        osc.start(startAt);
        osc.stop(startAt + 0.19);
      });
    }catch(_){ }
  }

  function playRingFile(){
    if(!CALL_RINGTONE_URL) return false;
    try{
      if(!ringAudioElement || ringAudioElement.src !== CALL_RINGTONE_URL){
        ringAudioElement = new Audio(CALL_RINGTONE_URL);
        ringAudioElement.loop = true;
        ringAudioElement.preload = "auto";
      }
      ringAudioElement.currentTime = 0;
      const playPromise = ringAudioElement.play();
      if(playPromise && typeof playPromise.catch === "function"){
        playPromise.catch(() => {});
      }
      return true;
    }catch(_){
      return false;
    }
  }

  function stopRingFile(){
    if(!ringAudioElement) return;
    try{
      ringAudioElement.pause();
      ringAudioElement.currentTime = 0;
    }catch(_){ }
  }

  function startRingTone(mode){
    const cleanMode = String(mode || "").trim().toLowerCase() === "incoming" ? "incoming" : "outgoing";
    const fileIsPlaying = !!(ringAudioElement && !ringAudioElement.paused);
    if(ringMode === cleanMode && (ringToneTimer || fileIsPlaying)) return;
    stopRingTone();
    ringMode = cleanMode;
    const fileStarted = playRingFile();
    ringFallbackTimer = setTimeout(() => {
      if(ringMode !== cleanMode) return;
      const filePlayingNow = !!(fileStarted && ringAudioElement && !ringAudioElement.paused);
      if(filePlayingNow) return;
      playRingBurst(cleanMode);
      ringToneTimer = setInterval(() => { playRingBurst(cleanMode); }, 1400);
    }, 180);
  }

  function stopRingTone(){
    if(ringFallbackTimer){
      clearTimeout(ringFallbackTimer);
      ringFallbackTimer = null;
    }
    if(ringToneTimer){
      clearInterval(ringToneTimer);
      ringToneTimer = null;
    }
    stopRingFile();
    ringMode = "";
  }

  function scheduleOutgoingNotAnswered(call){
    clearOutgoingOfferTimer();
    const row = call && typeof call === "object" ? call : null;
    if(!row?.callId || row.direction !== "outgoing") return;
    outgoingOfferTimer = setTimeout(() => {
      const current = activeCall;
      if(!current || current.callId !== row.callId || current.direction !== "outgoing") return;
      const mediaType = current.mediaType || row.mediaType || "audio";
      markHandledCall(row.callId);
      sendCallSignal({
        type: "call-end",
        call_id: row.callId,
        from_user_id: myId,
        to_user_id: String(current.peerId || row.peerId || receiverId || "").trim(),
        media_type: mediaType,
        reason: "not_answered"
      }).catch(() => {});
      endActiveCall(false, "They not answered").catch(() => {});
      sendMissedCallThreadMessage(row.callId, mediaType).catch(() => {});
    }, CALL_RING_TIMEOUT_MS);
  }

  function mapRemoteCallEndReason(signal){
    const type = String(signal?.type || "").trim().toLowerCase();
    const reason = String(signal?.reason || "").trim().toLowerCase();
    if(type === "call-busy" || reason === "busy") return "User busy in call";
    if(reason === "not_answered" || reason === "timeout") return "They not answered";
    if(reason === "not_available" || reason === "unreachable") return "User not reachable";
    if(reason === "media_permission_denied") return "Call unavailable";
    if(reason === "blocked") return "User blocked";
    if(type === "call-decline") return "Call declined";
    return "Call ended";
  }

  function callOutcomeMessageKey(callId, tag){
    const id = String(callId || "").trim();
    const kind = String(tag || "").trim().toLowerCase();
    if(!id || !kind) return "";
    return `${id}:${kind}`;
  }

  function rememberCallOutcomeMessage(callId, tag){
    const key = callOutcomeMessageKey(callId, tag);
    if(!key) return false;
    if(callOutcomeMessageKeys.has(key)) return false;
    callOutcomeMessageKeys.add(key);
    if(callOutcomeMessageKeys.size > 600){
      const trimmed = Array.from(callOutcomeMessageKeys).slice(-300);
      callOutcomeMessageKeys.clear();
      trimmed.forEach(item => callOutcomeMessageKeys.add(item));
    }
    return true;
  }

  function buildMissedCallText(mediaType){
    const mode = String(mediaType || "").trim().toLowerCase() === "video" ? "video" : "audio";
    const caller = getDisplayName(myUser, "User");
    return `Missed ${mode} call`;
  }

  async function sendMissedCallThreadMessage(callId, mediaType){
    if(isGroupChat || !isUuid(myId) || !isUuid(receiverId)) return false;
    if(!rememberCallOutcomeMessage(callId, "missed")) return false;
    const payload = {
      sender_id: myId,
      ...buildThreadPayload(),
      content: buildMissedCallText(mediaType),
      message_type: "call_missed",
      is_read: false
    };
    const { data, error } = await insertMessageWithFallback(payload);
    if(error){
      console.error("missed_call_message_failed", error);
      return false;
    }
    if(data){
      if(!isGroupChat && String(data?.sender_id || "") === myId && String(data?.receiver_id || "") === receiverId){
        if(toReadFlag(data?.is_read) === 1){
          data.is_read = false;
          const messageId = String(data?.id || "").trim();
          if(messageId){
            updateMessageRowWithFallback(messageId, [{ is_read: false }]).catch(() => {});
          }
        }
      }
      addMessageRow(data);
      scrollBottom();
    }
    notifyChatPush(data || payload).catch(() => {});
    return true;
  }

  function showIncomingOffer(offer){
    if(hasHandledCall(offer?.call_id)) return;
    pendingIncomingOffer = offer;
    incomingCallText.textContent = (offer.media_type === "video" ? "Video" : "Audio") + " call from " + getDisplayName(peerProfile, receiverNameParam);
    incomingCallBar.style.display = "flex";
    clearIncomingOfferTimer();
    startRingTone("incoming");
    safeCallVibrate([200, 120, 240, 120, 200]);
    incomingOfferTimer = setTimeout(() => { declineIncomingCall("not_answered").catch(() => {}); }, CALL_RING_TIMEOUT_MS);
  }

  function hideIncomingOffer(){
    clearIncomingOfferTimer();
    pendingIncomingOffer = null;
    incomingCallBar.style.display = "none";
    if(ringMode === "incoming") stopRingTone();
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
    clearOutgoingOfferTimer();
    clearIncomingOfferTimer();
    stopRingTone();
    if(sendSignal){
      await sendCallSignal({
        type: "call-end",
        call_id: call.callId,
        from_user_id: myId,
        to_user_id: String(call.peerId || receiverId || "").trim(),
        media_type: call.mediaType,
        reason: reason || "ended"
      });
    }
    closeCallResources(call);
    setCallStatus(reason || "Call ended");
    closeCallOverlay();
    muteCallBtn.textContent = "Mute";
    cameraCallBtn.textContent = "Camera Off";
    setMyOnCallStatus(false).catch(() => {});
    trackCallPresence().catch(() => {});
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

  async function createCallSession(callId, mediaType, direction, localStream, peerId){
    const call = {
      callId,
      mediaType,
      direction,
      localStream,
      peerId: String(peerId || receiverId || "").trim(),
      remoteStream: new MediaStream(),
      pc: null,
      pendingIce: [],
      muted: false,
      cameraOff: false
    };
    const pc = new RTCPeerConnection(RTC_CONFIG);
    call.pc = pc;
    localVideo.srcObject = localStream;
    remoteVideo.srcObject = call.remoteStream;
    if(mediaType === "video"){
      localVideo.play().catch(() => {});
      remoteVideo.play().catch(() => {});
    }
    localStream.getTracks().forEach(track => pc.addTrack(track, localStream));

    pc.onicecandidate = (event) => {
      if(!event.candidate || !activeCall || activeCall.callId !== call.callId) return;
      const candidate = normalizeCandidateFromRtc(event.candidate);
      if(!candidate) return;
      sendCallSignal({
        type: "ice",
        call_id: call.callId,
        from_user_id: myId,
        to_user_id: String(call.peerId || receiverId || "").trim(),
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
      if(activeCall && activeCall.callId === call.callId){
        clearOutgoingOfferTimer();
        stopRingTone();
        remoteVideo.play().catch(() => {});
        setCallStatus("Connected");
      }
    };
    pc.oniceconnectionstatechange = () => {
      if(!activeCall || activeCall.callId !== call.callId) return;
      const state = String(pc.iceConnectionState || "").toLowerCase();
      if(state === "connected" || state === "completed"){
        clearOutgoingOfferTimer();
        stopRingTone();
        setCallStatus("Connected");
        return;
      }
      if(state === "checking"){
        setCallStatus("Connecting...");
        return;
      }
      if(state === "failed" || state === "closed"){
        endActiveCall(false, "Call ended").catch(() => {});
      }
    };
    pc.onconnectionstatechange = () => {
      if(!activeCall || activeCall.callId !== call.callId) return;
      const state = String(pc.connectionState || "").toLowerCase();
      if(state === "connected"){ clearOutgoingOfferTimer(); stopRingTone(); setCallStatus("Connected"); return; }
      if(state === "connecting"){ setCallStatus("Connecting..."); return; }
      if(state === "failed" || state === "closed"){
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
    const peerBusy = await isPeerBusyForOutgoingCall();
    if(peerBusy){
      showNotice("User Busy");
      setCallStatus("User busy in call");
      return;
    }
    hideIncomingOffer();
    stopRingTone();
    clearOutgoingOfferTimer();
    const localStream = await getLocalStream(mediaType);
    if(!localStream) return;
    const callId = typeof crypto.randomUUID === "function" ? crypto.randomUUID() : `${Date.now()}_${Math.random().toString(16).slice(2)}`;
    const call = await createCallSession(callId, mediaType, "outgoing", localStream, receiverId);
    activeCall = call;
    setMyOnCallStatus(true).catch(() => {});
    trackCallPresence().catch(() => {});
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
      startRingTone("outgoing");
      scheduleOutgoingNotAnswered(call);
    }catch(err){
      console.error("call_offer_failed", err);
      await endActiveCall(false, "Call failed");
    }
  }

  async function answerIncomingCall(){
    const offer = pendingIncomingOffer;
    if(!offer) return;
    markCallInteraction();
    markHandledCall(offer.call_id);
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
    setCallStatus(offer.media_type === "video" ? "Starting video..." : "Starting audio...");
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
    const call = await createCallSession(offer.call_id, offer.media_type, "incoming", localStream, offer.from_user_id);
    activeCall = call;
    setMyOnCallStatus(true).catch(() => {});
    trackCallPresence().catch(() => {});
    showCallOverlay(call);
    setCallStatus(call.mediaType === "video" ? "Starting video..." : "Starting audio...");
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
        to_user_id: offer.from_user_id,
        media_type: call.mediaType,
        sdp: call.pc.localDescription
      });
      if(ok){
        setCallStatus(call.mediaType === "video" ? "Joining video call..." : "Joining call...");
      }else{
        await endActiveCall(false, "Signaling failed");
      }
    }catch(err){
      console.error("call_answer_failed", err);
      await endActiveCall(false, "Call failed");
    }
  }

  async function declineIncomingCall(reason){
    if(!pendingIncomingOffer){ hideIncomingOffer(); return; }
    const offer = pendingIncomingOffer;
    markHandledCall(offer.call_id);
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
    if(!signal || signal.to_user_id !== myId) return;
    if(hasHandledCall(signal.call_id) && signal.type === "call-offer") return;
    if(!rememberCallSignalKey(signal)) return;
    const isActivePeerSignal = signal.from_user_id === receiverId;
    if(!isActivePeerSignal){
      if(signal.type === "call-offer"){
        const accountBusy = await readUserOnCallStatus(myId);
        const isReallyBusy = !!(activeCall || pendingIncomingOffer || accountBusy || hasHandledCall(signal.call_id));
        await sendCallSignal({
          type: isReallyBusy ? "call-busy" : "call-decline",
          call_id: signal.call_id,
          from_user_id: myId,
          to_user_id: signal.from_user_id,
          media_type: signal.media_type,
          reason: isReallyBusy ? "busy" : "not_available"
        });
        markHandledCall(signal.call_id);
      }
      return;
    }
    if(signal.type === "call-offer"){
      if(hasHandledCall(signal.call_id)) return;
      if(isPeerBlocked){
        await sendCallSignal({
          type: "call-decline",
          call_id: signal.call_id,
          from_user_id: myId,
          to_user_id: signal.from_user_id,
          media_type: signal.media_type,
          reason: "blocked"
        });
        markHandledCall(signal.call_id);
        return;
      }
      if(pendingIncomingOffer && pendingIncomingOffer.call_id === signal.call_id){
        return;
      }
      const accountBusy = await readUserOnCallStatus(myId);
      if(activeCall || pendingIncomingOffer || accountBusy){
        await sendCallSignal({
          type: "call-busy",
          call_id: signal.call_id,
          from_user_id: myId,
          to_user_id: signal.from_user_id,
          media_type: signal.media_type,
          reason: "busy"
        });
        markHandledCall(signal.call_id);
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
        clearOutgoingOfferTimer();
        stopRingTone();
        setCallStatus("Connected");
      }catch(err){
        console.error("set_remote_answer_failed", err);
        await endActiveCall(false, "Call failed");
      }
      return;
    }
    if(signal.type === "call-end" || signal.type === "call-decline" || signal.type === "call-busy"){
      markHandledCall(signal.call_id);
      if(pendingIncomingOffer && pendingIncomingOffer.call_id === signal.call_id) hideIncomingOffer();
      if(activeCall && activeCall.callId === signal.call_id){
        const localCall = activeCall;
        const reasonCode = String(signal?.reason || "").trim().toLowerCase();
        const shouldSendMissed = localCall.direction === "outgoing" && (
          signal.type === "call-busy" ||
          reasonCode === "busy" ||
          reasonCode === "not_answered" ||
          reasonCode === "timeout" ||
          reasonCode === "not_available" ||
          reasonCode === "unreachable" ||
          reasonCode === "media_permission_denied"
        );
        const reason = mapRemoteCallEndReason(signal);
        await endActiveCall(false, reason);
        if(shouldSendMissed){
          sendMissedCallThreadMessage(signal.call_id, localCall.mediaType || signal.media_type).catch(() => {});
        }
      }
    }
  }

  async function pollCallSignals(force){
    if(!isUuid(myId)) return;
    // Keep polling even in background so incoming calls are picked without refresh.
    const query = new URLSearchParams();
    query.set("user_id", myId);
    query.set("limit", "80");
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

  function updateHeaderMenuActions(){
    const owner = isGroupChat && String(groupOwnerId || "").trim() === myId;
    if(menuBlockUserBtn) menuBlockUserBtn.classList.toggle("hidden", isGroupChat);
    if(menuLeaveGroupBtn) menuLeaveGroupBtn.classList.toggle("hidden", !isGroupChat || owner);
    if(menuDeleteGroupBtn) menuDeleteGroupBtn.classList.toggle("hidden", !isGroupChat || !owner);
  }

  function removeManualGroupCache(groupId){
    const cleanGroupId = String(groupId || receiverId || "").trim();
    if(!cleanGroupId || !isUuid(myId)) return;
    const storageKey = `manual_groups:${myId}`;
    try{
      const parsed = JSON.parse(localStorage.getItem(storageKey) || "[]");
      const next = Array.isArray(parsed)
        ? parsed.filter(row => String(row?.id || "").trim() !== cleanGroupId)
        : [];
      localStorage.setItem(storageKey, JSON.stringify(next));
    }catch(_){ }
  }

  async function deleteCurrentGroupFromDatabase(){
    const cleanGroupId = String(groupInfo?.id || receiverId || "").trim();
    if(!cleanGroupId) return false;
    const tasks = [
      ["messages", () => supa.from("messages").delete().eq("group_id", cleanGroupId)],
      ["group_members", () => supa.from("group_members").delete().eq("group_id", cleanGroupId)],
      ["groups", () => supa.from("groups").delete().eq("id", cleanGroupId)]
    ];
    for(const [resource, run] of tasks){
      try{
        const { error } = await run();
        if(error && !maybeMissingResource(error, resource)){
          console.error("delete_group_failed", error);
          return false;
        }
      }catch(err){
        console.error("delete_group_exception", err);
        return false;
      }
    }
    return true;
  }

  async function leaveCurrentGroup(){
    closeHeaderMenu();
    if(!isGroupChat) return;
    if(!confirm("Leave this group?")) return;
    const ok = await removeGroupMemberFromDatabase({ user_id: myId });
    if(!ok){
      alert("Unable to leave group.");
      return;
    }
    removeManualGroupCache(receiverId);
    location.href = "chat.html";
  }

  async function deleteCurrentGroup(){
    closeHeaderMenu();
    if(!isGroupChat) return;
    if(!canManageGroupMembers()){
      showNotice("Only the group owner can delete this group.");
      return;
    }
    if(!confirm("Delete this group for everyone?")) return;
    const ok = await deleteCurrentGroupFromDatabase();
    if(!ok){
      alert("Unable to delete group.");
      return;
    }
    removeManualGroupCache(receiverId);
    location.href = "chat.html";
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
    const name = isGroupChat
      ? resolveGroupName(groupInfo, receiverNameParam || "Group")
      : getDisplayName(peerProfile, receiverNameParam || "User");
    const photo = isGroupChat
      ? resolveGroupAvatar(groupInfo, groupMembers, DEFAULT_GROUP_ICON)
      : getProfilePhoto(peerProfile, receiverPicParam);
    setAvatar(document.getElementById("infoPic"), photo, name);
    document.getElementById("infoName").textContent = name;
    if(isGroupChat){
      const count = normalizeMemberCount(groupInfo?.member_count || groupMembers.length);
      if(infoMeta) infoMeta.textContent = groupMemberLabel(count);
      renderGroupMembers();
    }else{
      if(infoMeta) infoMeta.textContent = "Real-time verified account";
      renderGroupMembers();
    }
    renderInfoPanels();
    document.getElementById("profileOverlay").style.display = "flex";
  }

  function closeProfile(){ document.getElementById("profileOverlay").style.display = "none"; }

  function confirmAction(){ clearAllChat().catch(() => {}); }

  function sanitizeFileName(name){
    return String(name || "file").replace(/[^a-zA-Z0-9._-]/g, "_").slice(-80) || "file";
  }

  function inferMediaKind(file){
    const type = String(file?.type || "").toLowerCase();
    const fileName = String(file?.name || "").toLowerCase();
    if(type.startsWith("video/")) return "video";
    if(type.startsWith("image/")) return "image";
    if(/\.(mp4|mov|m4v|webm|ogg|avi|mkv|3gp)$/i.test(fileName)) return "video";
    if(/\.(png|jpe?g|gif|webp|bmp|svg)$/i.test(fileName)) return "image";
    return "";
  }

  function isAllowedMediaFile(file){
    return inferMediaKind(file) !== "";
  }

  function isPermissionUploadError(error){
    const text = `${error?.message || ""} ${error?.code || ""} ${error?.statusCode || ""}`.toLowerCase();
    return text.includes("403") || text.includes("permission") || text.includes("forbidden") || text.includes("not authorized");
  }

  async function uploadAttachmentFile(bucket, path, file){
    if(window.NOVA && typeof window.NOVA.uploadToBucket === "function"){
      try{
        const url = await window.NOVA.uploadToBucket(bucket, file, path);
        if(url) return String(url).trim();
      }catch(_){ }
    }
    const { error: uploadError } = await supa
      .storage
      .from(bucket)
      .upload(path, file, {
        cacheControl: "3600",
        upsert: true,
        contentType: file.type || "application/octet-stream"
      });
    if(uploadError) throw uploadError;
    const { data } = supa.storage.from(bucket).getPublicUrl(path);
    return String(data?.publicUrl || "").trim();
  }

  async function handleMediaUpload(el){
    const files = Array.from(el?.files || []);
    if(!files.length) return;
    try{
      if(isGroupChat && !canCurrentUserSendGroupMessages()){
        showNotice("Only admins can send messages.");
        return;
      }
      if(!isGroupChat && isPeerBlocked){
        console.warn("attachment_blocked_user");
        return;
      }
      const uploaded = [];
      let invalidCount = 0;
      let permissionDenied = false;
      for(let i = 0; i < files.length; i += 1){
        const file = files[i];
        const mediaKind = inferMediaKind(file);
        if(!mediaKind){
          invalidCount += 1;
          continue;
        }
        const fileName = `${myId}/${Date.now()}_${i}_${sanitizeFileName(file.name)}`;
        let fileUrl = "";
        try{
          fileUrl = await uploadAttachmentFile("chat-attachments", fileName, file);
        }catch(uploadError){
          if(isPermissionUploadError(uploadError)){
            permissionDenied = true;
          }
          console.error("attachment_upload_failed", uploadError);
          continue;
        }
        if(!fileUrl) continue;
        uploaded.push({ url: fileUrl, mediaKind });
      }
      if(!uploaded.length){
        if(permissionDenied){
          showNotice("chat-attachments upload permission denied.");
          return;
        }
        if(invalidCount > 0){
          showNotice("Only image/video formats are allowed.");
          return;
        }
        showNotice("Upload failed.");
        return;
      }
      const replyPayload = getReplyPayload();
      let sentCount = 0;
      for(let i = 0; i < uploaded.length; i += 1){
        const entry = uploaded[i];
        const payload = {
          sender_id: myId,
          ...buildThreadPayload(),
          content: entry.mediaKind === "video" ? "Video attachment" : "Image attachment",
          attachment_urls: [entry.url],
          attachment_url: entry.url,
          message_type: entry.mediaKind === "video" ? "video" : "image",
          is_read: false,
          ...(i === 0 ? replyPayload : {})
        };
        const ok = await sendMessageWithPayload(payload, "");
        if(ok) sentCount += 1;
      }
      if(sentCount > 0) clearReplyState();
      if(sentCount !== uploaded.length){
        showNotice("Some files could not be sent.");
      }
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
          icon_url: DEFAULT_GROUP_ICON,
          group_icon: DEFAULT_GROUP_ICON,
          member_count: 0,
          members: [],
          allowed: false,
          is_admin_only: false
        };
      }
      if(groupInfo?.allowed === false){
        console.warn("group_access_unverified", receiverId);
      }
      if(groupInfo?.id){
        receiverId = String(groupInfo.id || "").trim() || receiverId;
      }
      groupAdminOnly = !!groupInfo?.is_admin_only;
      groupMembers = Array.isArray(groupInfo?.members) ? groupInfo.members : [];
      if(groupInfo){
        groupInfo.member_count = normalizeMemberCount(groupMembers.length);
      }
      peerProfile = {
        display_name: resolveGroupName(groupInfo, receiverNameParam || "Group"),
        photo: resolveGroupAvatar(groupInfo, groupMembers, DEFAULT_GROUP_ICON)
      };
      renderPeerHeader();
      if(!groupMembers.length && receiverId){
        fetchGroupMembers(receiverId, groupOwnerId).then(members => {
          if(Array.isArray(members) && members.length){
            groupMembers = members;
            if(groupInfo){
              groupInfo.member_count = normalizeMemberCount(groupMembers.length);
              groupInfo.members = groupMembers.slice();
            }
            renderPeerHeader();
            applyGroupMessagingPermissions();
          }
        }).catch(() => {});
      }
      blockedBanner.classList.remove("show");
      applyGroupMessagingPermissions();
      audioCallBtn.disabled = true;
      videoCallBtn.disabled = true;
      audioCallBtn.classList.add("opacity-50");
      videoCallBtn.classList.add("opacity-50");
      await loadMessages();
      subscribeToMessages();
      startMessageSync();
      return;
    }
    loadThreadLocalState();
    const blockedInDb = await checkBlockedInDatabase().catch(() => false);
    if(blockedInDb) setBlockedLocalState(true);
    peerProfile = await fetchUserProfile(receiverId);
    renderPeerHeader();
    applyBlockedUi();
    updateFilterUi();
    const answerHandoffOffer = autoAnswer.enabled ? consumeAnswerHandoffOffer(receiverId, autoAnswer.callId) : null;
    if(answerHandoffOffer){
      pendingIncomingOffer = answerHandoffOffer;
      autoAnswer.enabled = false;
      answerIncomingCall().catch(() => {});
    }
    await loadMessages();
    subscribeToMessages();
    startMessageSync();
    markDirectMessagesRead().catch(() => {});
    setupCallRealtimeChannel();
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
    if(document.visibilityState === "visible"){
      pollCallSignals(true).catch(() => {});
      syncMessagesSilently().catch(() => {});
      if(!isGroupChat) markDirectMessagesRead().catch(() => {});
    }
  });

  window.addEventListener("focus", () => {
    if(!isGroupChat){
      pollCallSignals(true).catch(() => {});
      markDirectMessagesRead().catch(() => {});
    }
  });

  document.addEventListener("click", (event) => {
    const target = event.target;
    if(chatHeaderMenu.style.display === "block" && !chatHeaderMenu.contains(target)){
      closeHeaderMenu();
    }
    if(messageContextMenu.style.display === "block" && !messageContextMenu.contains(target)){
      closeContextMenu();
    }
    if(target instanceof Element && !target.closest(".group-member-actions")){
      closeGroupMemberActionMenus();
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
    stopMessageSync();
    clearIncomingOfferTimer();
    clearOutgoingOfferTimer();
    stopRingTone();
    if(ringAudioCtx){ try{ ringAudioCtx.close(); }catch(_){ } ringAudioCtx = null; }
    if(realtimeChannel){ try{ supa.removeChannel(realtimeChannel); }catch(_){ } }
    teardownCallRealtimeChannel();
    if(activeCall){
      closeCallResources(activeCall);
      activeCall = null;
      setMyOnCallStatus(false).catch(() => {});
    }
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
  window.switchInfoTab = switchInfoTab;
  window.confirmAction = confirmAction;
  window.handleBackNavigation = handleBackNavigation;
  window.toggleHeaderMenu = toggleHeaderMenu;
  window.showStarredMessages = showStarredMessages;
  window.showAllMessages = showAllMessages;
  window.clearAllChat = clearAllChat;
  window.blockCurrentUser = blockCurrentUser;
  window.leaveCurrentGroup = leaveCurrentGroup;
  window.deleteCurrentGroup = deleteCurrentGroup;
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


