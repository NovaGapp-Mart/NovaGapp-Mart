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
      const title = String(payload?.notification?.title || payload?.data?.title || "NOVAGAPP");
      const body = String(payload?.notification?.body || payload?.data?.body || "");
      const link = String(payload?.data?.link || payload?.fcmOptions?.link || "/").trim() || "/";
      self.registration.showNotification(title, {
        body,
        icon: "/Images/logo.png",
        data: { link }
      });
    });
  })
  .catch(() => {});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const target = String(event?.notification?.data?.link || "/").trim() || "/";
  event.waitUntil(
    clients.matchAll({ type: "window", includeUncontrolled: true }).then((list) => {
      for(const client of list){
        if("focus" in client){
          client.navigate(target);
          return client.focus();
        }
      }
      return clients.openWindow(target);
    })
  );
});
