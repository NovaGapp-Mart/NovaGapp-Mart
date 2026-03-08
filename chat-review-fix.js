(function(){
  if(window.__novaChatReviewFix) return;
  window.__novaChatReviewFix = true;

  function ensureUi(){
    if(document.getElementById("chatProfilePreviewModal")) return;
    const style = document.createElement("style");
    style.textContent = [
      ".chat-bottom-nav{position:fixed;left:0;right:0;bottom:0;z-index:120;background:rgba(255,255,255,.98);border-top:1px solid #d9dee7;display:grid;grid-template-columns:repeat(5,minmax(0,1fr));padding-bottom:env(safe-area-inset-bottom,0px);backdrop-filter:blur(8px)}",
      ".chat-bottom-nav-item{border:0;background:transparent;padding:9px 4px 8px;display:flex;flex-direction:column;align-items:center;gap:4px;font:600 11px sans-serif;color:#5b6472;cursor:pointer}",
      ".chat-bottom-nav-item img{width:22px;height:22px;opacity:.72}",
      ".chat-bottom-nav-item.active{color:#ff6a00}",
      ".chat-bottom-nav-item.active img{opacity:1}",
      "#chatList{padding-bottom:calc(168px + env(safe-area-inset-bottom,0px)) !important}",
      ".fab{bottom:calc(92px + env(safe-area-inset-bottom,0px)) !important}",
      ".modal-fab{bottom:calc(90px + env(safe-area-inset-bottom,0px)) !important}",
      ".chat-profile-preview{position:fixed;inset:0;z-index:250;background:rgba(15,23,42,.82);display:flex;align-items:center;justify-content:center;padding:16px}",
      ".chat-profile-preview-card{width:min(460px,100%);background:#fff;border-radius:22px;padding:16px;box-shadow:0 24px 52px rgba(15,23,42,.34)}",
      ".chat-profile-preview-head{display:flex;justify-content:space-between;align-items:center;gap:10px}",
      ".chat-profile-preview-close{border:0;background:#111827;color:#fff;width:34px;height:34px;border-radius:999px;font-size:18px;cursor:pointer}",
      ".chat-profile-preview-image{width:100%;aspect-ratio:1/1;object-fit:cover;border-radius:18px;background:#f4f4f5;margin-top:14px}",
      ".chat-profile-preview-name{margin-top:14px;font:800 24px/1.15 sans-serif;color:#111827}",
      ".chat-profile-preview-sub{margin-top:8px;font:600 13px/1.5 sans-serif;color:#5b6472}",
      ".chat-profile-preview-actions{display:flex;justify-content:flex-end;gap:10px;margin-top:16px}",
      ".chat-profile-open-btn{border:0;background:#ff6a00;color:#fff;border-radius:12px;padding:11px 16px;font:700 13px sans-serif;cursor:pointer}"
    ].join("");
    document.head.appendChild(style);

    const nav = document.createElement("nav");
    nav.className = "chat-bottom-nav";
    nav.innerHTML = [
      '<button class="chat-bottom-nav-item" type="button" data-chat-nav="home"><img src="https://img.icons8.com/ios-filled/24/000000/home.png" alt=""><span>Home</span></button>',
      '<button class="chat-bottom-nav-item active" type="button" data-chat-nav="chats"><img src="https://img.icons8.com/ios-filled/24/ff6a00/topic.png" alt=""><span>Chats</span></button>',
      '<button class="chat-bottom-nav-item" type="button" data-chat-nav="ride-food"><img src="https://img.icons8.com/ios-filled/24/000000/marker.png" alt=""><span>Ride/Food</span></button>',
      '<button class="chat-bottom-nav-item" type="button" data-chat-nav="reels"><img src="https://img.icons8.com/ios-filled/24/000000/circled-play.png" alt=""><span>Reels</span></button>',
      '<button class="chat-bottom-nav-item" type="button" data-chat-nav="account"><img src="https://img.icons8.com/ios-filled/24/000000/user.png" alt=""><span>Account</span></button>'
    ].join("");
    nav.addEventListener("click", (event) => {
      const button = event.target.closest("[data-chat-nav]");
      if(!button) return;
      const key = String(button.getAttribute("data-chat-nav") || "").trim();
      const routes = {
        home: "index.html",
        chats: "chat.html",
        "ride-food": "food-auto.html?view=ride",
        reels: "reel.html",
        account: "account.html"
      };
      const next = routes[key];
      if(next) location.href = next;
    });
    document.body.appendChild(nav);

    const modal = document.createElement("div");
    modal.id = "chatProfilePreviewModal";
    modal.className = "chat-profile-preview hide";
    modal.innerHTML = [
      '<div class="chat-profile-preview-card">',
      '  <div class="chat-profile-preview-head">',
      '    <strong>Profile Photo</strong>',
      '    <button type="button" class="chat-profile-preview-close">&times;</button>',
      '  </div>',
      '  <img id="chatProfilePreviewImage" class="chat-profile-preview-image" src="Images/no-image.png" alt="Profile Photo">',
      '  <div id="chatProfilePreviewName" class="chat-profile-preview-name">User</div>',
      '  <div id="chatProfilePreviewSub" class="chat-profile-preview-sub">Tap Open Chat to continue the conversation.</div>',
      '  <div class="chat-profile-preview-actions">',
      '    <button id="chatProfilePreviewChatBtn" type="button" class="chat-profile-open-btn">Open Chat</button>',
      '  </div>',
      '</div>'
    ].join("");
    modal.addEventListener("click", () => closeChatProfilePreview());
    modal.querySelector(".chat-profile-preview-card").addEventListener("click", (event) => event.stopPropagation());
    modal.querySelector(".chat-profile-preview-close").addEventListener("click", () => closeChatProfilePreview());
    document.body.appendChild(modal);
  }

  function closeChatProfilePreview(){
    const modal = document.getElementById("chatProfilePreviewModal");
    if(modal) modal.classList.add("hide");
  }

  function openChatProfilePreview(item, row){
    ensureUi();
    const modal = document.getElementById("chatProfilePreviewModal");
    const image = document.getElementById("chatProfilePreviewImage");
    const name = document.getElementById("chatProfilePreviewName");
    const sub = document.getElementById("chatProfilePreviewSub");
    const chatBtn = document.getElementById("chatProfilePreviewChatBtn");
    const itemName = String(item?.name || row?.querySelector(".font-bold")?.textContent || "User").trim() || "User";
    const rowImg = String(row?.querySelector("img.avatar")?.getAttribute("src") || "").trim();
    const rawImg = String(item?.img || rowImg || "").trim();
    const resolvedImg = item?.isGroup && typeof resolveGroupIconUrl === "function"
      ? resolveGroupIconUrl(rawImg)
      : (typeof normalizeAssetUrl === "function" ? normalizeAssetUrl(rawImg) : rawImg);
    image.src = resolvedImg || "Images/no-image.png";
    image.alt = `${itemName} photo`;
    name.textContent = itemName;
    sub.textContent = item?.isGroup ? "Previewing the group icon. Use Open Chat to continue in chatting mode." : "Previewing the profile photo. Use Open Chat to continue in chatting mode.";
    chatBtn.onclick = () => {
      closeChatProfilePreview();
      if(item?.isGroup){
        location.href = buildOpenChatUrl(item.id, { name:itemName, img:resolvedImg || rowImg || "", isGroup:true });
        return;
      }
      if(item?.id) openChat(itemName, resolvedImg || rowImg || "", item.id);
    };
    modal.classList.remove("hide");
  }

  function findItemForRow(row){
    if(!row || !Array.isArray(chatCache)) return null;
    return chatCache.find((item) => row.id && row.id === rowIdForKey(itemKey(item))) || null;
  }

  ensureUi();
  const list = document.getElementById("chatList");
  if(list){
    list.addEventListener("click", (event) => {
      const avatar = event.target.closest(".avatar-wrap");
      if(!avatar) return;
      const row = avatar.closest(".chat-item");
      if(!row) return;
      event.preventDefault();
      event.stopPropagation();
      if(typeof isSelectionMode === "function" && isSelectionMode()) return;
      const item = findItemForRow(row);
      openChatProfilePreview(item, row);
    }, true);
  }

  window.closeChatProfilePreview = closeChatProfilePreview;
})();



