
/* 🔐 SUPABASE CONFIG */
const supabase = supabase.createClient(
  window.NOVA_PUBLIC_CONFIG?.supabaseUrl || "",
  window.NOVA_PUBLIC_CONFIG?.supabaseAnonKey || ""
);

/* AUTH CHECK */
(async()=>{
  const { data } = await supabase.auth.getUser();
  if(!data.user){
    location.href="login.html";
    return;
  }

  // Session tracking (local, real)
  const start = Date.now();
  const todayKey = "time_" + new Date().toDateString();

  let spent = Number(localStorage.getItem(todayKey)) || 0;
  document.getElementById("todayTime").innerText =
    Math.floor(spent/60000) + "m";

  window.addEventListener("beforeunload",()=>{
    const now = Date.now();
    spent += (now - start);
    localStorage.setItem(todayKey, spent);
  });
})();

/* ACTIONS */
function setReminder(){
  alert("Daily reminder will be available soon.");
}

function takeBreak(){
  alert("Break mode activated.");
}
