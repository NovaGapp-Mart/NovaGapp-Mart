importScripts("https://www.gstatic.com/firebasejs/10.14.1/firebase-app-compat.js");
importScripts("https://www.gstatic.com/firebasejs/10.14.1/firebase-messaging-compat.js");

fetch("/api/public/config", { cache: "no-store" })
  .then((res) => res.json())
  .then((cfg) => {
    const firebaseConfig = {
      apiKey: String(cfg?.firebaseApiKey || "").trim(),
      authDomain: String(cfg?.firebaseAuthDomain || "").trim(),
      projectId: String(cfg?.firebaseProjectId || "").trim(),
      storageBucket: String(cfg?.firebaseStorageBucket || "").trim(),
      messagingSenderId: String(cfg?.firebaseMessagingSenderId || "").trim(),
      appId: String(cfg?.firebaseAppId || "").trim(),
      measurementId: String(cfg?.firebaseMeasurementId || "").trim()
    };
    if(!firebaseConfig.apiKey || !firebaseConfig.projectId || !firebaseConfig.messagingSenderId || !firebaseConfig.appId){
      return;
    }
    if(!firebase.apps || firebase.apps.length === 0){
      firebase.initializeApp(firebaseConfig);
    }
    const messaging = firebase.messaging();
    messaging.onBackgroundMessage((payload) => {
      const data = payload?.data || {};
      const notif = payload?.notification || {};
      const type = String(data?.type || "").trim().toLowerCase();
      const isCallInvite = type === "call_invite";
      const title = String(notif?.title || data?.title || "NOVAGAPP");
      const body = String(notif?.body || data?.body || "");
      const link = String(data?.link || payload?.fcmOptions?.link || "/").trim() || "/";
      const callId = String(data?.call_id || "").trim();
      const fromUserId = String(data?.from_user_id || "").trim();
      const options = {
        body,
        icon: "/Images/logo.png",
        badge: "/Images/logo.png",
        tag: isCallInvite ? `incoming_call_${callId || fromUserId || "nova"}` : undefined,
        renotify: !!isCallInvite,
        requireInteraction: !!isCallInvite,
        actions: isCallInvite
          ? [
              { action: "answer", title: "Answer" },
              { action: "dismiss", title: "Dismiss" }
            ]
          : [],
        data: {
          link,
          type,
          call_id: callId,
          from_user_id: fromUserId
        }
      };
      self.registration.showNotification(title, options);
    });
  })
  .catch(() => {});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  if(event.action === "dismiss"){
    return;
  }
  const data = event?.notification?.data || {};
  const baseTarget = String(data?.link || "/").trim() || "/";
  let target = baseTarget;
  if(event.action === "answer"){
    try{
      const url = new URL(baseTarget, self.location.origin);
      url.searchParams.set("call_action", "answer");
      const callId = String(data?.call_id || "").trim();
      if(callId){
        url.searchParams.set("call_id", callId);
      }
      target = url.toString();
    }catch(_){
      target = baseTarget;
    }
  }
  event.waitUntil(
    clients.matchAll({ type: "window", includeUncontrolled: true }).then((list) => {
      for(const client of list){
        if("focus" in client){
          try{
            client.navigate(target);
          }catch(_){ }
          return client.focus();
        }
      }
      return clients.openWindow(target);
    })
  );
});
