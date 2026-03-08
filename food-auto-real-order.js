(function(){
  if(window.__novaFoodAutoRealOrderFix) return;
  window.__novaFoodAutoRealOrderFix = true;

  let serviceActionPrimaryHandler = null;

  function ensureUi(){
    if(document.getElementById("serviceActionOverlay")) return;
    const style = document.createElement("style");
    style.textContent = [
      ".service-action-overlay{position:fixed;inset:0;z-index:180;background:rgba(15,23,42,.66);display:flex;align-items:center;justify-content:center;padding:18px}",
      ".service-action-card{width:min(460px,100%);background:#fff;border:1px solid #ead8ca;border-radius:22px;box-shadow:0 26px 56px rgba(15,23,42,.32);padding:20px}",
      ".service-action-top{display:flex;align-items:flex-start;gap:14px}",
      ".service-action-spinner{width:48px;height:48px;border-radius:999px;border:4px solid #ffe0c7;border-top-color:#ff6a00;animation:service-spin .8s linear infinite;flex-shrink:0}",
      ".service-action-icon{width:48px;height:48px;border-radius:999px;display:flex;align-items:center;justify-content:center;font-size:22px;font-weight:800;color:#fff;flex-shrink:0}",
      ".service-action-icon.success{background:linear-gradient(135deg,#0f9f50 0%,#18c268 100%)}",
      ".service-action-icon.error{background:linear-gradient(135deg,#d14343 0%,#b91c1c 100%)}",
      ".service-action-copy{min-width:0}",
      ".service-action-label{font-size:12px;font-weight:800;color:#9a4a0e;letter-spacing:.06em;text-transform:uppercase}",
      ".service-action-title{margin-top:4px;font-size:22px;line-height:1.15;font-weight:800;color:#111827}",
      ".service-action-message{margin-top:8px;font-size:14px;line-height:1.5;color:#4b5563;font-weight:600}",
      ".service-action-meta{margin-top:16px;display:grid;gap:10px}",
      ".service-action-meta-row{display:flex;justify-content:space-between;gap:10px;font-size:13px;line-height:1.45;color:#334155}",
      ".service-action-meta-row span:first-child{color:#8b5a3c;font-weight:700}",
      ".service-action-meta-row span:last-child{text-align:right;font-weight:800;color:#0f172a}",
      ".service-action-actions{display:flex;justify-content:flex-end;gap:10px;margin-top:18px}",
      ".status-line.is-error{color:#b91c1c}",
      ".status-line.is-success{color:#0f9f50}",
      "@keyframes service-spin{from{transform:rotate(0deg)}to{transform:rotate(360deg)}}"
    ].join("");
    document.head.appendChild(style);

    const overlay = document.createElement("div");
    overlay.id = "serviceActionOverlay";
    overlay.className = "service-action-overlay hide";
    overlay.setAttribute("aria-hidden", "true");
    overlay.innerHTML = [
      '<div class="service-action-card">',
      '  <div class="service-action-top">',
      '    <div id="serviceActionSpinner" class="service-action-spinner"></div>',
      '    <div id="serviceActionIcon" class="service-action-icon success hide">&#10003;</div>',
      '    <div class="service-action-copy">',
      '      <div id="serviceActionLabel" class="service-action-label">Processing</div>',
      '      <div id="serviceActionTitle" class="service-action-title">Working on your request</div>',
      '      <div id="serviceActionMessage" class="service-action-message">Please wait while we sync your request.</div>',
      '    </div>',
      '  </div>',
      '  <div id="serviceActionMeta" class="service-action-meta hide"></div>',
      '  <div class="service-action-actions">',
      '    <button id="serviceActionCloseBtn" type="button" class="btn gray hide">Close</button>',
      '    <button id="serviceActionPrimaryBtn" type="button" class="btn brand hide">Continue</button>',
      '  </div>',
      '</div>'
    ].join("");
    overlay.addEventListener("click", () => closeServiceActionOverlay());
    overlay.querySelector(".service-action-card").addEventListener("click", (event) => event.stopPropagation());
    document.body.appendChild(overlay);
    overlay.querySelector("#serviceActionCloseBtn").addEventListener("click", () => closeServiceActionOverlay(true));
    overlay.querySelector("#serviceActionPrimaryBtn").addEventListener("click", () => runServiceActionPrimary());

    const matchingText = document.getElementById("matchingText");
    if(matchingText && !document.getElementById("matchingRequestMeta")){
      const meta = document.createElement("div");
      meta.id = "matchingRequestMeta";
      meta.className = "muted status-line hide";
      matchingText.insertAdjacentElement("afterend", meta);
    }
  }

  function escapeHtml(value){
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function formatEntityCode(id){
    const clean = String(id || "").replace(/[^a-z0-9]/gi, "").toUpperCase();
    return clean ? clean.slice(0, 10) : "-";
  }

  function formatFoodStatus(statusRaw){
    const status = String(statusRaw || "placed").trim().toLowerCase();
    if(status === "placed") return "Pending confirmation";
    return typeof prettyLabel === "function" ? prettyLabel(status) : status;
  }

  function formatRideStatus(statusRaw){
    const status = typeof rideStatusPublic === "function" ? rideStatusPublic(statusRaw) : String(statusRaw || "").trim().toLowerCase();
    if(status === "searching") return "Pending driver match";
    const label = typeof rideStatusLabel === "function" ? rideStatusLabel(status) : status;
    return typeof prettyLabel === "function" ? prettyLabel(label) : label;
  }

  function humanizeServiceError(payload, fallback){
    const row = payload && typeof payload === "object" ? payload : {};
    const code = String(row.error || "").trim().toLowerCase();
    const message = String(row.message || "").trim();
    if(message && !/^request failed/i.test(message)) return message;
    const map = {
      network_error: "Network Error",
      rate_limited: "Too many requests. Please wait a moment and try again.",
      invalid_order_payload: "Order details are incomplete. Please review the form and retry.",
      invalid_ride_payload: "Ride request details are incomplete. Please review pickup and drop locations.",
      listing_not_available: "Selected store is currently unavailable.",
      consumer_role_required: "Consumer role is not active yet. Please retry in a few seconds."
    };
    if(map[code]) return map[code];
    if(message) return message;
    if(code && typeof prettyLabel === "function") return prettyLabel(code);
    return fallback || "Unable to complete the request right now.";
  }

  function renderOverlay(config){
    ensureUi();
    const overlay = document.getElementById("serviceActionOverlay");
    const spinner = document.getElementById("serviceActionSpinner");
    const icon = document.getElementById("serviceActionIcon");
    const label = document.getElementById("serviceActionLabel");
    const title = document.getElementById("serviceActionTitle");
    const message = document.getElementById("serviceActionMessage");
    const meta = document.getElementById("serviceActionMeta");
    const closeBtn = document.getElementById("serviceActionCloseBtn");
    const primaryBtn = document.getElementById("serviceActionPrimaryBtn");
    const state = config && typeof config === "object" ? config : {};
    const mode = String(state.mode || "loading").trim().toLowerCase();
    const metaRows = Array.isArray(state.metaRows) ? state.metaRows.filter(Boolean) : [];
    overlay.classList.remove("hide");
    overlay.dataset.mode = mode;
    overlay.setAttribute("aria-hidden", "false");
    spinner.classList.toggle("hide", mode !== "loading");
    icon.classList.toggle("hide", mode === "loading");
    icon.classList.toggle("success", mode === "success");
    icon.classList.toggle("error", mode === "error");
    icon.innerHTML = mode === "error" ? "!" : "&#10003;";
    label.textContent = String(state.label || (mode === "error" ? "Failed" : mode === "success" ? "Success" : "Processing"));
    title.textContent = String(state.title || "");
    message.textContent = String(state.message || "");
    meta.classList.toggle("hide", !metaRows.length);
    meta.innerHTML = metaRows.map((row) => `<div class="service-action-meta-row"><span>${escapeHtml(row.label || "")}</span><span>${escapeHtml(row.value || "")}</span></div>`).join("");
    closeBtn.classList.toggle("hide", mode === "loading");
    primaryBtn.classList.toggle("hide", mode === "loading");
    primaryBtn.textContent = String(state.primaryLabel || (mode === "error" ? "Close" : "Continue"));
    serviceActionPrimaryHandler = typeof state.onPrimary === "function" ? state.onPrimary : null;
  }
  function closeServiceActionOverlay(force){
    ensureUi();
    const overlay = document.getElementById("serviceActionOverlay");
    if(!overlay) return;
    if(!force && overlay.dataset.mode === "loading") return;
    overlay.classList.add("hide");
    overlay.setAttribute("aria-hidden", "true");
    overlay.dataset.mode = "";
    serviceActionPrimaryHandler = null;
  }

  function runServiceActionPrimary(){
    const handler = serviceActionPrimaryHandler;
    closeServiceActionOverlay(true);
    if(typeof handler === "function") handler();
  }

  window.closeServiceActionOverlay = closeServiceActionOverlay;
  window.runServiceActionPrimary = runServiceActionPrimary;

  function setButtonBusy(buttonId, busy, busyLabel, idleLabel){
    const button = document.getElementById(buttonId);
    if(!button) return;
    if(busy){
      if(!button.dataset.idleLabel) button.dataset.idleLabel = button.textContent;
      button.disabled = true;
      button.textContent = String(busyLabel || "Please wait...");
      return;
    }
    button.disabled = false;
    button.textContent = String(idleLabel || button.dataset.idleLabel || button.textContent || "");
    delete button.dataset.idleLabel;
  }

  function updateMatchingMeta(requestId, statusRaw){
    ensureUi();
    const el = document.getElementById("matchingRequestMeta");
    const cleanId = String(requestId || "").trim();
    if(!el) return;
    if(!cleanId){
      el.textContent = "";
      el.classList.add("hide");
      return;
    }
    el.textContent = `Ride ID ${formatEntityCode(cleanId)} | ${formatRideStatus(statusRaw)}`;
    el.classList.remove("hide");
  }

  async function callApi(method, path, body){
    const headers = {};
    try{
      const sessionRes = await supa.auth.getSession();
      const token = String(sessionRes?.data?.session?.access_token || "").trim();
      if(token) headers.Authorization = `Bearer ${token}`;
    }catch(_){ }
    if(method !== "GET") headers["Content-Type"] = "application/json";
    let lastError = null;
    const bases = typeof apiBases === "function" ? apiBases() : [location.origin, "https://novagapp-mart.onrender.com"];
    for(const base of bases){
      try{
        const res = await fetch(base + path, {
          method,
          cache: "no-store",
          headers,
          body: method === "GET" ? undefined : JSON.stringify(body || {})
        });
        const json = await res.json().catch(() => null);
        if(res.ok && json?.ok) return json;
        lastError = {
          ok: false,
          status: res.status,
          error: String(json?.error || "request_failed").trim(),
          message: String(json?.message || json?.error || `Request failed (${res.status})`).trim() || "Request failed",
          details: json
        };
      }catch(err){
        lastError = { ok:false, error:"network_error", message:String(err?.message || "Network Error").trim() || "Network Error" };
      }
    }
    return lastError || { ok:false, error:"network_error", message:"Network Error" };
  }

  apiGet = function(path){
    return callApi("GET", path);
  };

  apiPost = function(path, body){
    return callApi("POST", path, body);
  };

  async function fetchFoodOrder(orderId, buyerUserId){
    try{
      const { data } = await supa
        .from("local_orders")
        .select("id,status,service_type,amount_inr,delivery_address,note,item_snapshot,payment_method,payment_status,payment_order_id,payment_id,payment_ref,created_at,updated_at")
        .eq("id", String(orderId || "").trim())
        .eq("buyer_user_id", String(buyerUserId || "").trim())
        .maybeSingle();
      return data || null;
    }catch(_){
      return null;
    }
  }

  async function persistFoodOrder(orderId, buyerUserId, patch){
    try{
      const { data, error } = await supa
        .from("local_orders")
        .update({ ...(patch || {}), updated_at:new Date().toISOString() })
        .eq("id", String(orderId || "").trim())
        .eq("buyer_user_id", String(buyerUserId || "").trim())
        .select("id,status,service_type,amount_inr,delivery_address,note,item_snapshot,payment_method,payment_status,payment_order_id,payment_id,payment_ref,created_at,updated_at")
        .maybeSingle();
      if(error) return fetchFoodOrder(orderId, buyerUserId);
      return data || await fetchFoodOrder(orderId, buyerUserId);
    }catch(_){
      return fetchFoodOrder(orderId, buyerUserId);
    }
  }

  async function fetchRideRequestRow(requestId, riderUserId){
    try{
      const { data } = await supa
        .from("local_ride_requests")
        .select("id,status,vehicle_type,fare_inr,distance_km,duration_min,payment_method,payment_status,payment_order_id,payment_id,payment_ref,pickup_text,drop_text,created_at,updated_at")
        .eq("id", String(requestId || "").trim())
        .eq("rider_user_id", String(riderUserId || "").trim())
        .maybeSingle();
      return data || null;
    }catch(_){
      return null;
    }
  }

  async function persistRideRequest(requestId, riderUserId, patch){
    try{
      const { data, error } = await supa
        .from("local_ride_requests")
        .update({ ...(patch || {}), updated_at:new Date().toISOString() })
        .eq("id", String(requestId || "").trim())
        .eq("rider_user_id", String(riderUserId || "").trim())
        .select("id,status,vehicle_type,fare_inr,distance_km,duration_min,payment_method,payment_status,payment_order_id,payment_id,payment_ref,pickup_text,drop_text,created_at,updated_at")
        .maybeSingle();
      if(error) return fetchRideRequestRow(requestId, riderUserId);
      return data || await fetchRideRequestRow(requestId, riderUserId);
    }catch(_){
      return fetchRideRequestRow(requestId, riderUserId);
    }
  }

  const originalRenderFoodOrders = renderFoodOrders;
  renderFoodOrders = function(){
    const out = originalRenderFoodOrders.apply(this, arguments);
    const rows = Array.isArray(foodState?.orders) ? foodState.orders : [];
    const box = document.getElementById("foodOrdersList");
    rows.forEach((order, index) => {
      const card = box?.children?.[index];
      const chip = card?.querySelector?.(".status-chip");
      if(chip) chip.textContent = formatFoodStatus(order?.status || "placed");
    });
    return out;
  };

  const originalSetRideHint = setRideHint;
  setRideHint = function(text){
    originalSetRideHint.call(this, text);
    const el = document.getElementById("matchingText");
    const msg = String(text || "").trim();
    if(!el) return;
    el.classList.toggle("is-error", /error|failed|required|invalid|unavailable/i.test(msg));
    el.classList.toggle("is-success", /confirmed|accepted|ready/i.test(msg));
  };
  checkoutFoodOrder = async function(){
    const me = await ensureMe();
    if(!me) return;
    if(!foodState.selectedListing){ alert("Select a restaurant/store first."); return; }
    if(!foodState.cart.length){ alert("Cart is empty."); return; }
    const roleOk = await ensureConsumerRole();
    if(!roleOk){ alert("Consumer role activation failed."); return; }
    const address = String(document.getElementById("foodAddressInput")?.value || "").trim();
    if(!address){ alert("Delivery address required."); return; }
    const note = String(document.getElementById("foodNoteInput")?.value || "").trim().slice(0, 300);
    const paymentMethod = String(document.getElementById("foodPaymentMethod")?.value || "cash").trim();
    const subtotal = calcFoodCartTotal();
    const delivery = Number(foodState.selectedListing?.delivery_charge_inr || 0);
    const total = typeof roundMoney === "function" ? roundMoney(subtotal + delivery) : subtotal + delivery;
    const minOrder = Number(foodState.selectedListing?.minimum_order_inr || 0);
    if(total < minOrder){ alert(`Minimum order is ${money(minOrder)}.`); return; }
    let paymentPayload = { payment_method:"cash", payment_status:"cod", payment_order_id:"", payment_id:"", payment_ref:"" };
    if(paymentMethod === "online"){
      try{
        paymentPayload = await handleOnlinePayment(foodState.type, total, "order", "", `Pay ${money(total)} for ${prettyLabel(foodState.type)} order`);
      }catch(err){
        renderOverlay({ mode:"error", label:"Order Failed", title:"Order could not be placed", message:String(err?.message || "Payment failed"), primaryLabel:"Close" });
        return;
      }
    }
    const items = foodState.cart.map((row) => ({ item_id:row.id, name:row.name, qty:row.qty, price_inr:row.price_inr, image_url:row.image_url, line_total_inr:(typeof roundMoney === "function" ? roundMoney(Number(row.qty || 0) * Number(row.price_inr || 0)) : Number(row.qty || 0) * Number(row.price_inr || 0)) }));
    setButtonBusy("foodCheckoutBtn", true, "Placing Order...", "Place Order");
    renderOverlay({ mode:"loading", label:"Placing Order", title:"Placing your order...", message:"We are saving your food order to NovaGapp in real time." });
    const out = await apiPost("/api/local/orders/create", { user_id:me.id, listing_id:foodState.selectedListing.id, service_type:foodState.type, amount_inr:total, items, delivery_address:address, note, ...paymentPayload });
    if(!out?.ok){
      setButtonBusy("foodCheckoutBtn", false, "", "Place Order");
      renderOverlay({ mode:"error", label:"Order Failed", title:"Order could not be placed", message:humanizeServiceError(out, "Unable to place the order right now."), primaryLabel:"Close" });
      return;
    }
    const orderId = String(out?.order?.id || "").trim();
    const liveOrder = await persistFoodOrder(orderId, me.id, { item_snapshot:items, payment_method:String(paymentPayload.payment_method || paymentMethod || "cash").trim(), payment_status:String(paymentPayload.payment_status || "pending").trim(), payment_order_id:String(paymentPayload.payment_order_id || "").trim(), payment_id:String(paymentPayload.payment_id || "").trim(), payment_ref:String(paymentPayload.payment_ref || "").trim(), amount_inr:total, delivery_address:address, note }) || out.order || {};
    foodState.cart = [];
    const noteInput = document.getElementById("foodNoteInput");
    if(noteInput) noteInput.value = "";
    renderFoodMenu();
    renderFoodCartSummary();
    await loadFoodOrders();
    setButtonBusy("foodCheckoutBtn", false, "", "Place Order");
    renderOverlay({
      mode:"success",
      label:"Order Confirmed",
      title:"Food order saved successfully",
      message:"Your order is now live in the database and available in My Orders.",
      metaRows:[
        { label:"Order ID", value:formatEntityCode(liveOrder.id || orderId) },
        { label:"Status", value:formatFoodStatus(liveOrder.status || "placed") },
        { label:"Total", value:money(liveOrder.amount_inr || total) },
        { label:"Created", value:liveOrder.created_at ? new Date(liveOrder.created_at).toLocaleString() : new Date().toLocaleString() }
      ],
      primaryLabel:"View Order",
      onPrimary:() => document.getElementById("foodOrdersList")?.scrollIntoView({ behavior:"smooth", block:"start" })
    });
  };

  requestRide = async function(){
    const me = await ensureMe();
    if(!me) return;
    state.pickupLandmark = String(document.getElementById("pickupLandmarkInput")?.value || state.pickupLandmark || "").trim().slice(0, 120);
    state.dropLandmark = String(document.getElementById("dropLandmarkInput")?.value || state.dropLandmark || "").trim().slice(0, 120);
    if(state.pickupLat == null || state.pickupLng == null){ setRideHint("Pickup not detected yet. Tap Current Location."); return; }
    if(state.dropLat == null || state.dropLng == null){ setRideHint("Please set destination first."); return; }
    const roleOk = await ensureConsumerRole();
    if(!roleOk){ setRideHint("Consumer role setup failed. Retry in few seconds."); return; }
    const fareRes = await apiPost("/api/local/rides/fare-estimate", { pickup_lat:state.pickupLat, pickup_lng:state.pickupLng, drop_lat:state.dropLat, drop_lng:state.dropLng, vehicle_type:state.vehicle, distance_km:state.distanceKm, duration_min:state.durationMin });
    if(fareRes?.ok && fareRes?.estimate?.fare_inr) state.fare = Number(fareRes.estimate.fare_inr);
    const selectedPayment = String(document.getElementById("paymentMethod")?.value || "cash").trim().toLowerCase();
    let paymentPayload = { payment_method:"cash", payment_status:"cod", payment_order_id:"", payment_id:"", payment_ref:"" };
    setButtonBusy("confirmRideBtn", true, "Placing Ride...", "Confirm Ride");
    renderOverlay({ mode:"loading", label:"Placing Ride", title:"Placing your ride request...", message:"We are saving pickup, drop and fare details to NovaGapp in real time." });
    if(selectedPayment === "online"){
      try{
        paymentPayload = await handleOnlinePayment("ride", state.fare, "ride", "", `Pay ${money(state.fare)} for ${prettyLabel(state.vehicle)} ride`);
      }catch(err){
        const paymentError = String(err?.message || "Payment failed");
        setButtonBusy("confirmRideBtn", false, "", "Confirm Ride");
        setRideHint(paymentError);
        renderOverlay({ mode:"error", label:"Ride Failed", title:"Ride request could not continue", message:paymentError, primaryLabel:"Close" });
        return;
      }
    }
    const req = await apiPost("/api/local/rides/request", { user_id:me.id, pickup_lat:state.pickupLat, pickup_lng:state.pickupLng, drop_lat:state.dropLat, drop_lng:state.dropLng, pickup_text:composePlaceText(state.pickupText || "Pickup", state.pickupLandmark).slice(0, 160), drop_text:composePlaceText(state.dropText || "Drop", state.dropLandmark).slice(0, 160), vehicle_type:state.vehicle, ...paymentPayload, fare_inr:state.fare, distance_km:state.distanceKm, duration_min:state.durationMin });
    if(!req?.ok || !req?.ride_request?.id){
      const msg = humanizeServiceError(req, "Ride request failed");
      setButtonBusy("confirmRideBtn", false, "", "Confirm Ride");
      setRideHint(msg);
      renderOverlay({ mode:"error", label:"Ride Failed", title:"Ride request could not be placed", message:msg, primaryLabel:"Close" });
      return;
    }
    const rideId = String(req.ride_request.id || "").trim();
    const liveRide = await persistRideRequest(rideId, me.id, { vehicle_type:state.vehicle, payment_method:String(paymentPayload.payment_method || selectedPayment || "cash").trim(), payment_status:String(paymentPayload.payment_status || "pending").trim(), payment_order_id:String(paymentPayload.payment_order_id || "").trim(), payment_id:String(paymentPayload.payment_id || "").trim(), payment_ref:String(paymentPayload.payment_ref || "").trim(), fare_inr:(typeof roundMoney === "function" ? roundMoney(state.fare) : state.fare), distance_km:Number(state.distanceKm || 0), duration_min:Number(state.durationMin || 0), pickup_text:composePlaceText(state.pickupText || "Pickup", state.pickupLandmark).slice(0, 160), drop_text:composePlaceText(state.dropText || "Drop", state.dropLandmark).slice(0, 160) }) || req.ride_request || {};
    state.requestId = rideId;
    setPhase("matching");
    setMatchingText("Searching in 3km...");
    updateMatchingMeta(rideId, liveRide.status || req.ride_request.status || "searching");
    startRideRealtime(state.requestId).catch(() => {});
    startMatchingAndTrackLoops();
    saveState();
    setButtonBusy("confirmRideBtn", false, "", "Confirm Ride");
    renderOverlay({ mode:"success", label:"Ride Confirmed", title:"Ride request saved successfully", message:"Drivers are now receiving your live ride request.", metaRows:[{ label:"Ride ID", value:formatEntityCode(liveRide.id || rideId) }, { label:"Status", value:formatRideStatus(liveRide.status || "searching") }, { label:"Fare", value:money(liveRide.fare_inr || state.fare) }, { label:"Created", value:liveRide.created_at ? new Date(liveRide.created_at).toLocaleString() : new Date().toLocaleString() }], primaryLabel:"Track Ride", onPrimary:() => document.getElementById("matchingPanel")?.scrollIntoView({ behavior:"smooth", block:"nearest" }) });
  };

  const originalResetRideFlow = resetRideFlow;
  resetRideFlow = async function(){
    const out = await originalResetRideFlow.apply(this, arguments);
    updateMatchingMeta("", "");
    return out;
  };

  ensureUi();
  const requestedView = (() => {
    try{
      const raw = String(new URLSearchParams(location.search).get("view") || "ride").trim().toLowerCase();
      return ["ride", "food", "agent"].includes(raw) ? raw : "ride";
    }catch(_){
      return "ride";
    }
  })();
  if(requestedView !== "ride"){
    setTimeout(() => { openViewWithConsent(requestedView).catch(() => {}); }, 0);
  }
})();

