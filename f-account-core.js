(function(){
  "use strict";

  const roleLabelMap = Object.freeze({
    rider: "Driver",
    seller: "Food Seller",
    agent: "Service Agent",
    consumer: "Customer"
  });

  const walletOwnerMap = Object.freeze({
    rider: "driver",
    seller: "seller",
    agent: "agent",
    consumer: "customer"
  });

  const supa = window.supa || (typeof window.novaCreateSupabaseClient === "function" ? window.novaCreateSupabaseClient() : null);
  const state = {
    me: null
  };

  function cleanBase(value){
    const out = String(value || "").trim().replace(/\/+$/g, "");
    if(!out || !/^https?:\/\//i.test(out)) return "";
    return out;
  }

  function apiBases(){
    const out = [];
    const push = (value) => {
      const clean = cleanBase(value);
      if(!clean) return;
      if(!out.includes(clean)) out.push(clean);
    };
    push(window.CONTEST_API_BASE || window.API_BASE || "");
    try{
      push(localStorage.getItem("contest_api_base"));
      push(localStorage.getItem("api_base"));
    }catch(_){ }
    if(/^https?:\/\//i.test(String(location.origin || ""))){
      push(location.origin);
    }
    push("https://novagapp-mart.onrender.com");
    return out;
  }

  async function parseJson(res){
    try{
      return await res.json();
    }catch(_){
      return null;
    }
  }

  async function apiGet(path){
    let lastError = "api_unreachable";
    for(const base of apiBases()){
      try{
        const res = await fetch(base + path, { cache: "no-store" });
        const body = await parseJson(res);
        if(res.ok && body?.ok){
          return body;
        }
        const err = String(body?.error || ("http_" + String(res.status || 500)));
        const msg = String(body?.message || "");
        if(Number(res.status || 500) >= 500){
          lastError = err;
          continue;
        }
        return { ok:false, error: err, message: msg, status: Number(res.status || 0), raw: body };
      }catch(err){
        lastError = String(err?.message || "network_error");
      }
    }
    return { ok:false, error: lastError };
  }

  async function apiPost(path, payload){
    let lastError = "api_unreachable";
    for(const base of apiBases()){
      try{
        const res = await fetch(base + path, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload || {})
        });
        const body = await parseJson(res);
        if(res.ok && body?.ok){
          return body;
        }
        const err = String(body?.error || ("http_" + String(res.status || 500)));
        const msg = String(body?.message || "");
        if(Number(res.status || 500) >= 500){
          lastError = err;
          continue;
        }
        return { ok:false, error: err, message: msg, status: Number(res.status || 0), raw: body };
      }catch(err){
        lastError = String(err?.message || "network_error");
      }
    }
    return { ok:false, error: lastError };
  }

  function normalizePhone(value){
    const raw = String(value || "").trim();
    if(!raw) return "";
    if(raw.startsWith("+")){
      return raw.replace(/[^\d+]/g, "");
    }
    const digits = raw.replace(/\D/g, "");
    if(digits.length === 10) return "+91" + digits;
    if(digits.length >= 11 && digits.length <= 15) return "+" + digits;
    return "";
  }

  function safeNumber(value, fallback){
    const n = Number(value);
    return Number.isFinite(n) ? n : Number(fallback || 0);
  }

  function roleLabel(role){
    const clean = String(role || "").trim().toLowerCase();
    return roleLabelMap[clean] || "Role";
  }

  function walletOwnerType(role){
    const clean = String(role || "").trim().toLowerCase();
    return walletOwnerMap[clean] || "";
  }

  function isValidLatLng(latValue, lngValue){
    const lat = Number(latValue);
    const lng = Number(lngValue);
    if(!Number.isFinite(lat) || !Number.isFinite(lng)) return false;
    if(lat < -90 || lat > 90 || lng < -180 || lng > 180) return false;
    if(Math.abs(lat) < 0.000001 && Math.abs(lng) < 0.000001) return false;
    return true;
  }

  async function getSessionUser(forceRefresh){
    if(!forceRefresh && state.me){
      return state.me;
    }
    if(!supa || !supa.auth || typeof supa.auth.getSession !== "function"){
      state.me = null;
      return null;
    }
    try{
      const { data } = await supa.auth.getSession();
      const user = data?.session?.user || null;
      state.me = user;
      return user;
    }catch(_){
      state.me = null;
      return null;
    }
  }

  async function requireSessionUser(){
    const user = await getSessionUser(true);
    if(user) return user;
    location.href = "login.html";
    throw new Error("login_required");
  }

  async function loadRoles(userId){
    const id = String(userId || "").trim();
    if(!id) return { ok:false, error: "user_id_required" };
    return apiGet(`/api/local/roles?user_id=${encodeURIComponent(id)}`);
  }

  async function sendProviderOtp(userId, phone){
    const id = String(userId || "").trim();
    const ph = normalizePhone(phone);
    if(!id || !ph){
      return { ok:false, error:"user_id_and_phone_required" };
    }
    return apiPost("/api/local/provider/otp/send", { user_id: id, phone: ph });
  }

  async function verifyProviderOtp(userId, otpToken, otp){
    const id = String(userId || "").trim();
    const tok = String(otpToken || "").trim();
    const pin = String(otp || "").trim();
    if(!id || !tok || !/^\d{6}$/.test(pin)){
      return { ok:false, error:"invalid_otp_payload" };
    }
    return apiPost("/api/local/provider/otp/verify", {
      user_id: id,
      otp_token: tok,
      otp: pin
    });
  }

  async function payProviderListingFee(options){
    const opts = options || {};
    const userId = String(opts.userId || "").trim();
    const role = String(opts.role || "").trim().toLowerCase();
    if(!userId || !role){
      throw new Error("user_and_role_required");
    }
    const orderRes = await apiPost("/api/local/role/fee/order", { user_id: userId, role });
    if(!orderRes?.ok){
      throw new Error(String(orderRes?.error || "fee_order_failed"));
    }
    if(typeof window.Razorpay !== "function"){
      throw new Error("razorpay_sdk_missing");
    }
    const order = orderRes.order || {};
    return await new Promise((resolve, reject) => {
      const instance = new window.Razorpay({
        key: String(order.key_id || ""),
        amount: Number(order.amount_paise || 50000),
        currency: "INR",
        order_id: String(order.razorpay_order_id || ""),
        name: "NovaGapp",
        description: `${roleLabel(role)} listing fee`,
        prefill: {
          name: String(opts.name || "").trim(),
          email: String(opts.email || "").trim(),
          contact: normalizePhone(opts.phone || "")
        },
        handler: async (payment) => {
          const verifyRes = await apiPost("/api/local/role/fee/verify", {
            user_id: userId,
            role,
            razorpay_order_id: String(payment?.razorpay_order_id || ""),
            razorpay_payment_id: String(payment?.razorpay_payment_id || ""),
            razorpay_signature: String(payment?.razorpay_signature || "")
          });
          if(!verifyRes?.ok){
            reject(new Error(String(verifyRes?.error || "fee_verify_failed")));
            return;
          }
          resolve({
            payment_ref: String(payment?.razorpay_payment_id || ""),
            payment_order_id: String(payment?.razorpay_order_id || "")
          });
        },
        modal: {
          ondismiss: () => reject(new Error("payment_cancelled"))
        }
      });
      instance.open();
    });
  }

  async function enrollRole(payload){
    const body = Object.assign({}, payload || {});
    body.user_id = String(body.user_id || "").trim();
    body.role = String(body.role || "").trim().toLowerCase();
    body.display_name = String(body.display_name || "").trim().slice(0, 80);
    body.phone = normalizePhone(body.phone || "");
    body.payment_ref = String(body.payment_ref || "").trim().slice(0, 120);
    body.otp_token = String(body.otp_token || "").trim().slice(0, 120);
    body.fee_paid = Boolean(body.fee_paid);
    if(!body.user_id || !body.role){
      return { ok:false, error: "user_id_and_role_required" };
    }
    return apiPost("/api/local/role/enroll", body);
  }

  async function getOwnerListing(userId, listingType){
    const id = String(userId || "").trim();
    const type = String(listingType || "").trim();
    if(!id || !type) return null;
    const out = await apiGet(`/api/local/listings/for-owner?user_id=${encodeURIComponent(id)}&listing_type=${encodeURIComponent(type)}`);
    if(!out?.ok) return null;
    const rows = Array.isArray(out.listings) ? out.listings : [];
    return rows[0] || null;
  }

  async function upsertDriverListing(payload){
    const body = Object.assign({}, payload || {});
    const userId = String(body.user_id || "").trim();
    if(!userId) return { ok:false, error:"user_id_required" };
    if(!isValidLatLng(body.lat, body.lng)) return { ok:false, error:"lat_lng_required" };
    const vehicleNumber = String(body.vehicle_number || "").trim();
    if(!vehicleNumber) return { ok:false, error:"vehicle_number_required" };

    const existing = await getOwnerListing(userId, "ride");
    const requestBody = {
      user_id: userId,
      listing_type: "ride",
      store_name: String(body.store_name || "").trim().slice(0, 120) || "Driver",
      phone: normalizePhone(body.phone || ""),
      lat: Number(body.lat),
      lng: Number(body.lng),
      open_now: body.open_now === undefined ? true : Boolean(body.open_now),
      ride_vehicle_type: String(body.ride_vehicle_type || "auto").trim().toLowerCase(),
      vehicle_number: vehicleNumber,
      base_fare_inr: Math.max(0, safeNumber(body.base_fare_inr, 0)),
      per_km_rate_inr: Math.max(0, safeNumber(body.per_km_rate_inr, 0)),
      per_min_rate_inr: Math.max(0, safeNumber(body.per_min_rate_inr, 0)),
      service_radius_km: Math.max(1, Math.min(50, Math.round(safeNumber(body.service_radius_km, 5)))),
      documents_url: String(body.documents_url || "").trim().slice(0, 3000)
    };
    if(!requestBody.phone){
      return { ok:false, error:"phone_required" };
    }
    if(existing?.id){
      return apiPost("/api/local/listings/update", Object.assign({ listing_id: String(existing.id) }, requestBody));
    }
    return apiPost("/api/local/listings/create", requestBody);
  }

  async function upsertSellerListing(payload){
    const body = Object.assign({}, payload || {});
    const userId = String(body.user_id || "").trim();
    const listingType = String(body.listing_type || "food").trim().toLowerCase();
    const storeName = String(body.store_name || "").trim().slice(0, 120);
    if(!userId) return { ok:false, error:"user_id_required" };
    if(!["food", "grocery"].includes(listingType)) return { ok:false, error:"invalid_listing_type" };
    if(!storeName) return { ok:false, error:"store_name_required" };
    if(!isValidLatLng(body.lat, body.lng)) return { ok:false, error:"lat_lng_required" };

    const existing = await getOwnerListing(userId, listingType);
    const requestBody = {
      user_id: userId,
      listing_type: listingType,
      store_name: storeName,
      phone: normalizePhone(body.phone || ""),
      image_url: String(body.image_url || "").trim().slice(0, 3000),
      lat: Number(body.lat),
      lng: Number(body.lng),
      open_now: body.open_now === undefined ? true : Boolean(body.open_now),
      delivery_charge_inr: Math.max(0, safeNumber(body.delivery_charge_inr, 0)),
      minimum_order_inr: Math.max(0, safeNumber(body.minimum_order_inr, 0)),
      open_time: String(body.open_time || "").trim().slice(0, 20),
      close_time: String(body.close_time || "").trim().slice(0, 20),
      self_delivery: body.self_delivery === undefined ? true : Boolean(body.self_delivery)
    };
    if(!requestBody.phone){
      return { ok:false, error:"phone_required" };
    }
    if(existing?.id){
      return apiPost("/api/local/listings/update", Object.assign({ listing_id: String(existing.id) }, requestBody));
    }
    return apiPost("/api/local/listings/create", requestBody);
  }

  async function upsertAgentProfile(payload){
    const body = Object.assign({}, payload || {});
    const userId = String(body.user_id || "").trim();
    const title = String(body.title || "").trim().slice(0, 120);
    if(!userId) return { ok:false, error:"user_id_required" };
    if(!title) return { ok:false, error:"agent_title_required" };
    if(!isValidLatLng(body.lat, body.lng)) return { ok:false, error:"lat_lng_required" };
    const requestBody = {
      user_id: userId,
      service_category: String(body.service_category || "electrician").trim().toLowerCase(),
      title,
      phone: normalizePhone(body.phone || ""),
      price_per_visit_inr: Math.max(0, safeNumber(body.price_per_visit_inr, 0)),
      per_hour_rate_inr: Math.max(0, safeNumber(body.per_hour_rate_inr, 0)),
      experience_years: Math.max(0, Math.min(60, Math.floor(safeNumber(body.experience_years, 0)))),
      service_radius_km: Math.max(1, Math.min(50, Math.round(safeNumber(body.service_radius_km, 5)))),
      available_now: body.available_now === undefined ? true : Boolean(body.available_now),
      image_url: String(body.image_url || "").trim().slice(0, 3000),
      lat: Number(body.lat),
      lng: Number(body.lng)
    };
    if(!requestBody.phone){
      return { ok:false, error:"phone_required" };
    }
    return apiPost("/api/local/agents/create", requestBody);
  }

  async function updateRiderLocation(userId, lat, lng){
    const id = String(userId || "").trim();
    if(!id || !isValidLatLng(lat, lng)){
      return { ok:false, error:"invalid_rider_location_payload" };
    }
    return apiPost("/api/local/riders/location", {
      user_id: id,
      lat: Number(lat),
      lng: Number(lng),
      is_online: true
    });
  }

  async function reverseGeocode(lat, lng){
    if(!isValidLatLng(lat, lng)) return "";
    try{
      const url = new URL("https://nominatim.openstreetmap.org/reverse");
      url.searchParams.set("format", "jsonv2");
      url.searchParams.set("addressdetails", "1");
      url.searchParams.set("zoom", "18");
      url.searchParams.set("lat", String(Number(lat)));
      url.searchParams.set("lon", String(Number(lng)));
      const res = await fetch(url.toString(), {
        cache: "no-store",
        headers: {
          "Accept": "application/json",
          "Accept-Language": "en,en-US;q=0.9"
        }
      });
      if(!res.ok) return "";
      const out = await parseJson(res);
      const addr = out?.address || {};
      return [
        addr.road || addr.neighbourhood || addr.suburb || "",
        addr.city || addr.town || addr.village || addr.county || "",
        addr.state || "",
        addr.postcode || "",
        addr.country || ""
      ].filter(Boolean).join(", ").slice(0, 180);
    }catch(_){
      return "";
    }
  }

  window.FAccountCore = Object.freeze({
    state,
    supa,
    roleLabel,
    walletOwnerType,
    normalizePhone,
    isValidLatLng,
    getSessionUser,
    requireSessionUser,
    apiGet,
    apiPost,
    loadRoles,
    sendProviderOtp,
    verifyProviderOtp,
    payProviderListingFee,
    enrollRole,
    upsertDriverListing,
    upsertSellerListing,
    upsertAgentProfile,
    updateRiderLocation,
    reverseGeocode
  });
})();
