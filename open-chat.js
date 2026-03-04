(function(){
  const supa = window.supa || (typeof window.novaCreateSupabaseClient === "function" ? window.novaCreateSupabaseClient() : null);
  const params = new URLSearchParams(location.search);
  const receiverId = String(params.get("id") || "").trim();
  const receiverNameParam = decodeURIComponent(params.get("name") || "User");
  const receiverPicParam = String(params.get("img") || "").trim();
  const autoAnswer = {
    enabled: String(params.get("call_action") || "").trim().toLowerCase() === "answer",
    callId: String(params.get("call_id") || "").trim()
  };

  const msgHolder = document.getElementById("msgHolder");
  const chatWindow = document.getElementById("chatWindow");
  const mainInput = document.getElementById("mainInput");
  const audioCallBtn = document.getElementById("audioCallBtn");
  const videoCallBtn = document.getElementById("videoCallBtn");
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

  function scrollBottom(){
    chatWindow.scrollTop = chatWindow.scrollHeight + 900;
  }

  function renderPeerHeader(){
    const name = getDisplayName(peerProfile, receiverNameParam || "User");
    const photo = getProfilePhoto(peerProfile, receiverPicParam);
    document.getElementById("userName").textContent = name;
    document.getElementById("infoName").textContent = name;
    setAvatar(document.getElementById("userAvatar"), photo, name);
    setAvatar(document.getElementById("infoPic"), photo, name);
  }

  function addMessageRow(row){
    if(!row || typeof row !== "object") return;
    const content = String(row.content ?? row.message ?? "").trim();
    if(!content) return;
    const idKey = String(row.id || `${row.sender_id}_${row.receiver_id}_${row.created_at}_${content.slice(0, 30)}`).trim();
    if(!rememberMessageId(idKey)) return;
    const outgoing = String(row.sender_id || "") === myId;
    const bubble = document.createElement("div");
    bubble.className = `bubble ${outgoing ? "bubble-out" : "bubble-in"}`;

    if(outgoing){
      const textNode = document.createElement("div");
      textNode.textContent = content;
      bubble.appendChild(textNode);
    }else{
      const wrap = document.createElement("div");
      wrap.className = "flex items-start gap-2";
      const avatar = document.createElement("div");
      avatar.className = "msg-mini-avatar";
      setAvatar(avatar, getProfilePhoto(peerProfile, receiverPicParam), getDisplayName(peerProfile, receiverNameParam));
      const textWrap = document.createElement("div");
      textWrap.className = "flex-1";
      const sender = document.createElement("div");
      sender.className = "text-[10px] text-orange-700 font-semibold";
      sender.textContent = getDisplayName(peerProfile, receiverNameParam);
      const textNode = document.createElement("div");
      textNode.textContent = content;
      textWrap.appendChild(sender);
      textWrap.appendChild(textNode);
      wrap.appendChild(avatar);
      wrap.appendChild(textWrap);
      bubble.appendChild(wrap);
    }

    const meta = document.createElement("div");
    meta.className = "text-[9px] text-gray-400 text-right mt-1";
    meta.textContent = formatClock(row.created_at);
    bubble.appendChild(meta);
    msgHolder.appendChild(bubble);
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

  async function loadMessages(){
    msgHolder.innerHTML = "<p class='text-center text-xs text-gray-400'>Syncing messages...</p>";
    const condition = `and(sender_id.eq.${myId},receiver_id.eq.${receiverId}),and(sender_id.eq.${receiverId},receiver_id.eq.${myId})`;
    const { data, error } = await supa
      .from("messages")
      .select("id,sender_id,receiver_id,content,created_at")
      .or(condition)
      .order("created_at", { ascending: true });
    if(error){
      console.error("message_load_failed", error);
      msgHolder.innerHTML = "<p class='text-center text-xs text-red-500'>Unable to load messages.</p>";
      return;
    }
    msgHolder.innerHTML = "";
    (data || []).forEach(addMessageRow);
    scrollBottom();
  }

  async function sendMessage(){
    const text = String(mainInput.value || "").trim();
    if(!text) return;
    if(!isUuid(myId) || !isUuid(receiverId)){
      alert("Invalid sender/receiver id.");
      return;
    }
    mainInput.value = "";
    const payload = { sender_id: myId, receiver_id: receiverId, content: text };
    const { data, error } = await supa
      .from("messages")
      .insert(payload)
      .select("id,sender_id,receiver_id,content,created_at")
      .maybeSingle();
    if(error){
      console.error("message_send_failed", error);
      alert("Message failed to send.");
      mainInput.value = text;
      return;
    }
    if(data) addMessageRow(data);
    scrollBottom();
  }

  function subscribeToMessages(){
    if(realtimeChannel){
      try{ supa.removeChannel(realtimeChannel); }catch(_){ }
      realtimeChannel = null;
    }
    realtimeChannel = supa.channel(`dm_${myId}_${receiverId}_${Date.now()}`)
      .on("postgres_changes", { event: "INSERT", schema: "public", table: "messages" }, payload => {
        const row = payload?.new || {};
        const s = String(row.sender_id || "");
        const r = String(row.receiver_id || "");
        const inThread = (s === myId && r === receiverId) || (s === receiverId && r === myId);
        if(!inThread) return;
        addMessageRow(row);
        scrollBottom();
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

  function openProfile(){
    const name = getDisplayName(peerProfile, receiverNameParam || "User");
    setAvatar(document.getElementById("infoPic"), getProfilePhoto(peerProfile, receiverPicParam), name);
    document.getElementById("infoName").textContent = name;
    document.getElementById("profileOverlay").style.display = "flex";
  }

  function closeProfile(){ document.getElementById("profileOverlay").style.display = "none"; }

  function confirmAction(){
    if(confirm("Clear current chat view?")){
      msgHolder.innerHTML = "";
      renderedMessageIds = new Set();
    }
  }

  async function handleMediaUpload(el){
    if(!el?.files?.[0]) return;
    alert("Storage bucket upload flow is not configured on this page yet.");
  }

  async function initPage(){
    if(!supa){
      alert("Supabase not configured. Please refresh.");
      return;
    }
    if(!isUuid(receiverId)){
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
    if(myId === receiverId){
      alert("Cannot open self-chat.");
      location.href = "chat.html";
      return;
    }
    peerProfile = await fetchUserProfile(receiverId);
    renderPeerHeader();
    await loadMessages();
    subscribeToMessages();
    startCallPolling();
    const canCall = isUuid(receiverId) && receiverId !== myId;
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
  window.startOutgoingCall = startOutgoingCall;
  window.answerIncomingCall = answerIncomingCall;
  window.declineIncomingCall = declineIncomingCall;
  window.hangupActiveCall = hangupActiveCall;
  window.toggleMute = toggleMute;
  window.toggleCamera = toggleCamera;
  window.scrollBottom = scrollBottom;
  window.onload = initPage;
})();
