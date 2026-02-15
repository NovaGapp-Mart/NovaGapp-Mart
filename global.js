/* =====================================
   GLOBAL USER SETTINGS
===================================== */
window.USER_LANG = localStorage.getItem("lang") || "en";
window.USER_CURRENCY = (localStorage.getItem("currency") || "USD").toUpperCase();
window.USER_COUNTRY = localStorage.getItem("country") || "";
window.USER_COUNTRY_CODE = localStorage.getItem("country_code") || "";

/* =====================================
   API BASE SAFETY (NON-LOCAL)
===================================== */
(function(){
  const DEFAULT_REMOTE_API_BASE = "https://novagapp-mart.onrender.com";
  const isLocal = String(location.protocol || "") === "file:";
  const isLoopback = (value) => /^http:\/\//i.test(String(value || "").trim());

  if(!isLocal){
    const clearLoopback = (store, keys) => {
      if(!store) return;
      keys.forEach((key) => {
        try{
          const raw = String(store.getItem(key) || "").trim();
          if(isLoopback(raw)){
            store.removeItem(key);
          }
        }catch(_){ }
      });
    };
    clearLoopback(window.localStorage, ["api_base", "contest_api_base", "tryonApiBase"]);
    clearLoopback(window.sessionStorage, ["api_base", "contest_api_base", "tryonApiBase"]);
  }

  const globalBase = String(window.CONTEST_API_BASE || window.API_BASE || "").trim();
  if(!globalBase || (!isLocal && isLoopback(globalBase))){
    window.CONTEST_API_BASE = DEFAULT_REMOTE_API_BASE;
    window.API_BASE = DEFAULT_REMOTE_API_BASE;
  }
})();

/* =====================================
   TRANSLATIONS
===================================== */
window.TEXT = {
  en:{search:"Search for products",banner:"Big Launching Days Sale",bannerSub:"Premium Sale",rec:"Recommended For You",add:"Add to Cart",added:"Added to cart"},
  hi:{search:"प्रोडक्ट खोजें",banner:"बिग लॉन्चिंग डेज़ सेल",bannerSub:"प्रीमियम सेल",rec:"आपके लिए सुझाव",add:"कार्ट में जोड़ें",added:"कार्ट में जोड़ दिया"},
  de:{search:"Produkte suchen",banner:"Big Launching Days Sale",bannerSub:"Premium-Sale",rec:"Für dich empfohlen",add:"In den Warenkorb",added:"Zum Warenkorb hinzugefügt"},
  fr:{search:"Rechercher des produits",banner:"Big Launching Days Sale",bannerSub:"Vente premium",rec:"Recommandé pour vous",add:"Ajouter au panier",added:"Ajouté au panier"},
  es:{search:"Buscar productos",banner:"Big Launching Days Sale",bannerSub:"Venta premium",rec:"Recomendado para ti",add:"Agregar al carrito",added:"Agregado al carrito"}
};

window.t = k => window.TEXT[window.USER_LANG]?.[k] || window.TEXT.en[k] || k;

/* =====================================
   UI TRANSLATIONS (BASIC)
===================================== */
window.UI_TRANSLATIONS = {
  hi:{
    "Search for products":"प्रोडक्ट खोजें",
    "Recommended For You":"आपके लिए सुझाव",
    "Launching Big Deal":"बड़ा ऑफर लॉन्च",
    "Big Launching Days Sale":"बिग लॉन्चिंग डेज़ सेल",
    "Premium Sale":"प्रीमियम सेल",
    "Add to cart":"कार्ट में जोड़ें",
    "Add to Cart":"कार्ट में जोड़ें",
    "Added to cart":"कार्ट में जोड़ दिया",
    "BUY NOW":"अभी खरीदें",
    "Buy Now":"अभी खरीदें",
    "Place Order":"ऑर्डर करें",
    "Your Cart":"आपका कार्ट",
    "Price":"कीमत",
    "Discount":"छूट",
    "Total":"कुल",
    "Checkout":"चेकआउट",
    "Order Summary":"ऑर्डर सारांश",
    "Payment":"भुगतान",
    "Continue to Payment":"भुगतान जारी रखें",
    "Secure Payment":"सुरक्षित भुगतान",
    "Total Amount":"कुल राशि",
    "Available offers":"उपलब्ध ऑफ़र",
    "Cash on Delivery":"कैश ऑन डिलीवरी",
    "Net Banking":"नेट बैंकिंग",
    "Credit / Debit Card":"क्रेडिट / डेबिट कार्ड",
    "UPI / Wallets":"यूपीआई / वॉलेट्स",
    "Order Details":"ऑर्डर विवरण",
    "Order Journey":"ऑर्डर स्थिति",
    "Ordered Items":"ऑर्डर आइटम्स",
    "My Orders":"मेरे ऑर्डर",
    "Order ID":"ऑर्डर आईडी",
    "Qty":"मात्रा",
    "Remove":"हटाएं",
    "Back":"वापस",
    "Your cart is empty":"आपका कार्ट खाली है",
    "No orders yet":"अभी तक कोई ऑर्डर नहीं",
    "Pay Securely":"सुरक्षित भुगतान करें",
    "Plans":"प्लान",
    "Default plan":"डिफ़ॉल्ट प्लान",
    "Upgrade":"अपग्रेड करें",
    "Contact Sales":"सेल्स से संपर्क करें",
    "Products":"प्रोडक्ट्स",
    "Total Sales":"कुल बिक्री",
    "Wallet":"वॉलेट",
    "Add Product":"प्रोडक्ट जोड़ें",
    "My Products":"मेरे प्रोडक्ट्स",
    "Orders":"ऑर्डर्स",
    "Seller Plan":"सेलर प्लान",
    "View listings":"लिस्टिंग देखें",
    "Seller plan required":"सेलर प्लान आवश्यक",
    "Request Withdrawal":"निकासी का अनुरोध",
    "Wallet History":"वॉलेट इतिहास",
    "Home":"होम",
    "Account":"अकाउंट",
    "Seller":"सेलर",
    "Pay once":"एक बार भुगतान",
    "Pay":"भुगतान करें",
    "Start Selling":"बेचना शुरू करें"
  },
  de:{
    "Search for products":"Produkte suchen",
    "Recommended For You":"Für dich empfohlen",
    "Launching Big Deal":"Großes Angebot startet",
    "Big Launching Days Sale":"Big Launching Days Sale",
    "Premium Sale":"Premium-Sale",
    "Add to cart":"In den Warenkorb",
    "Add to Cart":"In den Warenkorb",
    "Added to cart":"Zum Warenkorb hinzugefügt",
    "BUY NOW":"Jetzt kaufen",
    "Buy Now":"Jetzt kaufen",
    "Place Order":"Bestellung aufgeben",
    "Your Cart":"Warenkorb",
    "Price":"Preis",
    "Discount":"Rabatt",
    "Total":"Gesamt",
    "Checkout":"Kasse",
    "Order Summary":"Bestellübersicht",
    "Payment":"Zahlung",
    "Continue to Payment":"Zur Zahlung",
    "Secure Payment":"Sichere Zahlung",
    "Total Amount":"Gesamtbetrag",
    "Available offers":"Verfügbare Angebote",
    "Cash on Delivery":"Nachnahme",
    "Net Banking":"Online-Banking",
    "Credit / Debit Card":"Kredit-/Debitkarte",
    "UPI / Wallets":"UPI / Wallets",
    "Order Details":"Bestelldetails",
    "Order Journey":"Bestellverlauf",
    "Ordered Items":"Bestellte Artikel",
    "My Orders":"Meine Bestellungen",
    "Order ID":"Bestell-ID",
    "Qty":"Menge",
    "Remove":"Entfernen",
    "Back":"Zurück",
    "Your cart is empty":"Dein Warenkorb ist leer",
    "No orders yet":"Noch keine Bestellungen",
    "Pay Securely":"Sicher bezahlen",
    "Plans":"Pläne",
    "Default plan":"Standardplan",
    "Upgrade":"Upgrade",
    "Contact Sales":"Vertrieb kontaktieren",
    "Products":"Produkte",
    "Total Sales":"Gesamtumsatz",
    "Wallet":"Wallet",
    "Add Product":"Produkt hinzufügen",
    "My Products":"Meine Produkte",
    "Orders":"Bestellungen",
    "Seller Plan":"Verkäuferplan",
    "View listings":"Angebote ansehen",
    "Seller plan required":"Verkäuferplan erforderlich",
    "Request Withdrawal":"Auszahlung anfordern",
    "Wallet History":"Wallet-Verlauf",
    "Home":"Start",
    "Account":"Konto",
    "Seller":"Verkäufer",
    "Pay once":"Einmal bezahlen",
    "Pay":"Bezahlen",
    "Start Selling":"Verkauf starten"
  },
  fr:{
    "Search for products":"Rechercher des produits",
    "Recommended For You":"Recommandé pour vous",
    "Launching Big Deal":"Grande offre en lancement",
    "Big Launching Days Sale":"Big Launching Days Sale",
    "Premium Sale":"Vente premium",
    "Add to cart":"Ajouter au panier",
    "Add to Cart":"Ajouter au panier",
    "Added to cart":"Ajouté au panier",
    "BUY NOW":"Acheter maintenant",
    "Buy Now":"Acheter maintenant",
    "Place Order":"Passer la commande",
    "Your Cart":"Votre panier",
    "Price":"Prix",
    "Discount":"Remise",
    "Total":"Total",
    "Checkout":"Paiement",
    "Order Summary":"Récapitulatif de commande",
    "Payment":"Paiement",
    "Continue to Payment":"Continuer vers le paiement",
    "Secure Payment":"Paiement sécurisé",
    "Total Amount":"Montant total",
    "Available offers":"Offres disponibles",
    "Cash on Delivery":"Paiement à la livraison",
    "Net Banking":"Banque en ligne",
    "Credit / Debit Card":"Carte de crédit / débit",
    "UPI / Wallets":"UPI / Portefeuilles",
    "Order Details":"Détails de commande",
    "Order Journey":"Suivi de commande",
    "Ordered Items":"Articles commandés",
    "My Orders":"Mes commandes",
    "Order ID":"ID de commande",
    "Qty":"Qté",
    "Remove":"Supprimer",
    "Back":"Retour",
    "Your cart is empty":"Votre panier est vide",
    "No orders yet":"Aucune commande pour l'instant",
    "Pay Securely":"Payer en toute sécurité",
    "Plans":"Forfaits",
    "Default plan":"Forfait par défaut",
    "Upgrade":"Mettre à niveau",
    "Contact Sales":"Contacter les ventes",
    "Products":"Produits",
    "Total Sales":"Ventes totales",
    "Wallet":"Portefeuille",
    "Add Product":"Ajouter un produit",
    "My Products":"Mes produits",
    "Orders":"Commandes",
    "Seller Plan":"Plan vendeur",
    "View listings":"Voir les annonces",
    "Seller plan required":"Plan vendeur requis",
    "Request Withdrawal":"Demander un retrait",
    "Wallet History":"Historique du portefeuille",
    "Home":"Accueil",
    "Account":"Compte",
    "Seller":"Vendeur",
    "Pay once":"Paiement unique",
    "Pay":"Payer",
    "Start Selling":"Commencer à vendre"
  },
  es:{
    "Search for products":"Buscar productos",
    "Recommended For You":"Recomendado para ti",
    "Launching Big Deal":"Gran oferta en lanzamiento",
    "Big Launching Days Sale":"Big Launching Days Sale",
    "Premium Sale":"Venta premium",
    "Add to cart":"Agregar al carrito",
    "Add to Cart":"Agregar al carrito",
    "Added to cart":"Agregado al carrito",
    "BUY NOW":"Comprar ahora",
    "Buy Now":"Comprar ahora",
    "Place Order":"Hacer pedido",
    "Your Cart":"Tu carrito",
    "Price":"Precio",
    "Discount":"Descuento",
    "Total":"Total",
    "Checkout":"Pagar",
    "Order Summary":"Resumen del pedido",
    "Payment":"Pago",
    "Continue to Payment":"Continuar al pago",
    "Secure Payment":"Pago seguro",
    "Total Amount":"Importe total",
    "Available offers":"Ofertas disponibles",
    "Cash on Delivery":"Pago contra entrega",
    "Net Banking":"Banca en línea",
    "Credit / Debit Card":"Tarjeta de crédito / débito",
    "UPI / Wallets":"UPI / Carteras",
    "Order Details":"Detalles del pedido",
    "Order Journey":"Estado del pedido",
    "Ordered Items":"Artículos pedidos",
    "My Orders":"Mis pedidos",
    "Order ID":"ID de pedido",
    "Qty":"Cantidad",
    "Remove":"Eliminar",
    "Back":"Atrás",
    "Your cart is empty":"Tu carrito está vacío",
    "No orders yet":"Aún no hay pedidos",
    "Pay Securely":"Pagar seguro",
    "Plans":"Planes",
    "Default plan":"Plan predeterminado",
    "Upgrade":"Actualizar",
    "Contact Sales":"Contactar ventas",
    "Products":"Productos",
    "Total Sales":"Ventas totales",
    "Wallet":"Billetera",
    "Add Product":"Agregar producto",
    "My Products":"Mis productos",
    "Orders":"Pedidos",
    "Seller Plan":"Plan de vendedor",
    "View listings":"Ver listados",
    "Seller plan required":"Se requiere plan de vendedor",
    "Request Withdrawal":"Solicitar retiro",
    "Wallet History":"Historial de billetera",
    "Home":"Inicio",
    "Account":"Cuenta",
    "Seller":"Vendedor",
    "Pay once":"Pago único",
    "Pay":"Pagar",
    "Start Selling":"Empezar a vender"
  }
};

window.applyTranslations = function(root){
  const map = window.UI_TRANSLATIONS[window.USER_LANG];
  if(!map) return;

  let base = document.body;
  if(root && root.nodeType){
    base = root;
  }else if(root && root.target && root.target.nodeType){
    base = (root.target === document && document.body) ? document.body : root.target;
  }
  if(!base || !base.nodeType) return;

  const walker = document.createTreeWalker(base, NodeFilter.SHOW_TEXT);
  const nodes = [];
  while(walker.nextNode()){
    nodes.push(walker.currentNode);
  }
  nodes.forEach(node=>{
    const raw = node.nodeValue;
    if(!raw) return;
    const parentTag = node.parentNode && node.parentNode.tagName;
    if(parentTag === "SCRIPT" || parentTag === "STYLE") return;
    const key = raw.trim();
    if(!key) return;
    let translated = map[key];
    if(!translated && key.endsWith(":")){
      const baseKey = key.slice(0, -1);
      translated = map[baseKey];
      if(translated) translated = translated + ":";
    }
    if(!translated) return;
    node.nodeValue = raw.replace(key, translated);
  });

  (base.querySelectorAll ? base : document).querySelectorAll("input[placeholder],textarea[placeholder]").forEach(el=>{
    const key = String(el.placeholder || "").trim();
    if(map[key]) el.placeholder = map[key];
  });
};

/* =====================================
   SYMBOLS
===================================== */
window.SYMBOL = {
  USD:"$",
  INR:"\u20B9",
  EUR:"\u20AC",
  GBP:"\u00A3",
  JPY:"\u00A5",
  CNY:"\u00A5",
  AUD:"A$",
  CAD:"C$",
  CHF:"CHF",
  RUB:"\u20BD",
  BRL:"R$",
  ZAR:"R",
  KRW:"\u20A9",
  THB:"\u0E3F",
  UAH:"\u20B4",
  VND:"\u20AB",
  NGN:"\u20A6",
  CRC:"\u20A1",
  SGD:"S$",
  HKD:"HK$",
  SAR:"SAR",
  AED:"AED",
  NOK:"kr",
  SEK:"kr"
};

/* =====================================
   🔥 FIXED USD BASE RATES (NO API)
   1 UNIT = HOW MUCH USD
===================================== */
window.USD_RATE = {
  USD:1,
  INR:0.011,
  EUR:1.08,
  GBP:1.26,
  JPY:0.0067,
  CNY:0.14,
  AUD:0.66,
  CAD:0.74,
  CHF:1.10,
  RUB:0.011,
  BRL:0.20,
  ZAR:0.054,
  KRW:0.00075,
  THB:0.028,
  UAH:0.026,
  VND:0.000041,
  NGN:0.0021,
  CRC:0.0019,
  SGD:0.74,
  HKD:0.13,
  SAR:0.27,
  AED:0.27,
  NOK:0.095,
  SEK:0.095
};

/* =====================================
   LIVE RATES (OPTIONAL)
===================================== */
window.loadLiveRates = async function(){
  const cacheKey = "usd_rate_cache";
  const cacheTsKey = "usd_rate_cache_ts";
  const maxAge = 1000 * 60 * 60;
  const now = Date.now();

  try{
    const cached = localStorage.getItem(cacheKey);
    const cachedTs = Number(localStorage.getItem(cacheTsKey) || 0);
    if(cached && (now - cachedTs) < maxAge){
      const cachedRates = JSON.parse(cached);
      applyLiveRates(cachedRates);
      return;
    }
  }catch(_){ }

  const publicEndpoints = [
    "https://open.er-api.com/v6/latest/USD",
    "https://api.exchangerate.host/latest?base=USD",
    "https://api.frankfurter.app/latest?from=USD"
  ];
  const isBackendOrigin = location.port === "3000";
  const endpoints = isBackendOrigin
    ? ["/fx/latest?base=USD", ...publicEndpoints]
    : publicEndpoints;

  for(const url of endpoints){
    try{
      const res = await fetch(url);
      if(!res.ok) continue;
      const data = await res.json();
      const rates = data?.rates || data?.conversion_rates || null;
      if(!rates) continue;
      applyLiveRates(rates);
      localStorage.setItem(cacheKey, JSON.stringify(rates));
      localStorage.setItem(cacheTsKey, String(Date.now()));
      return;
    }catch(_){ }
  }
};

function applyLiveRates(rates){
  Object.keys(rates || {}).forEach(cur=>{
    const perUsd = Number(rates[cur]);
    if(!perUsd || !isFinite(perUsd)) return;
    // USD_RATE stores USD per 1 unit of currency
    window.USD_RATE[cur] = 1 / perUsd;
  });
  updateAllPrices();
  if(typeof window.refreshMoneyElements === "function"){
    window.refreshMoneyElements();
  }
  try{ document.dispatchEvent(new Event("ratesUpdated")); }catch(_){}
}

/* =====================================
   MONEY CONVERTER (FINAL)
   price can be ANY currency
===================================== */
window.convertCurrency = function(amount, from, to){
  const fromCur = String(from || "USD").toUpperCase();
  const toCur = String(to || "USD").toUpperCase();
  const val = Number(amount) || 0;
  if(fromCur === toCur) return val;
  const fromRate = window.USD_RATE[fromCur];
  const toRate = window.USD_RATE[toCur];
  if(!fromRate || !toRate) return val;
  const usd = val * fromRate;
  return usd / toRate;
};

window.formatCurrency = function(value, currency){
  const cur = String(currency || "USD").toUpperCase();
  const num = Number(value) || 0;
  if(typeof Intl !== "undefined" && Intl.NumberFormat){
    try{
      return new Intl.NumberFormat(window.USER_LANG || "en", {
        style:"currency",
        currency: cur,
        currencyDisplay:"narrowSymbol",
        maximumFractionDigits: 2
      }).format(num);
    }catch(_){}
  }
  const sym = window.SYMBOL[cur] || (cur + " ");
  return sym + num.toFixed(2);
};

window.moneyTag = function(amount, currency){
  const cur = String(currency || "USD").toUpperCase();
  const val = Number(amount) || 0;
  const display = (typeof window.money === "function")
    ? money(val, cur)
    : (cur + " " + val.toFixed(2));
  return `<span class="money" data-money="${val}" data-currency="${cur}">${display}</span>`;
};

window.money = function(amount, fromCurrency){
  const fromCur = String(fromCurrency || "USD").toUpperCase();
  const cur = (window.USER_CURRENCY || "USD").toUpperCase();
  const converted = window.convertCurrency(amount, fromCur, cur);
  return window.formatCurrency(converted, cur);
};

window.formatPrice = function(amount, fromCurrency){
  return window.money(amount, fromCurrency);
};

/* =====================================
   AUTO UPDATE PRICES
===================================== */
window.updateAllPrices = function(){
  document.querySelectorAll("[data-usd]").forEach(el=>{
    el.textContent = money(el.dataset.usd, "USD");
  });
};

window.refreshMoneyElements = function(root){
  const scope = root || document;
  document.querySelectorAll("[data-money]").forEach(el=>{
    const amount = Number(el.dataset.money || 0);
    const cur = el.dataset.currency || "USD";
    el.textContent = money(amount, cur);
  });
};

/* =====================================
   CHANGE HANDLERS
===================================== */
window.setCurrency = function(cur){
  window.USER_CURRENCY = String(cur || "USD").toUpperCase();
  localStorage.setItem("currency", window.USER_CURRENCY);
  updateAllPrices();
  if(typeof window.refreshMoneyElements === "function"){
    window.refreshMoneyElements();
  }
};

window.setLanguage = function(lang){
  window.USER_LANG = lang;
  localStorage.setItem("lang", lang);
  document.dispatchEvent(new Event("languageUpdated"));
};

/* =====================================
   ON LOAD
===================================== */
document.addEventListener("DOMContentLoaded", updateAllPrices);
document.addEventListener("DOMContentLoaded", loadLiveRates);
document.addEventListener("DOMContentLoaded", refreshMoneyElements);
document.addEventListener("ratesUpdated", refreshMoneyElements);
document.addEventListener("DOMContentLoaded", applyTranslations);
document.addEventListener("languageUpdated", applyTranslations);

document.addEventListener("DOMContentLoaded", ()=>{
  let refreshPending = false;
  const schedule = () => {
    if(refreshPending) return;
    refreshPending = true;
    requestAnimationFrame(() => {
      refreshPending = false;
      refreshMoneyElements();
      applyTranslations();
    });
  };
  if(document.body && typeof MutationObserver !== "undefined"){
    const obs = new MutationObserver(() => schedule());
    obs.observe(document.body, { childList:true, subtree:true });
  }
});
document.addEventListener("DOMContentLoaded", ()=> {
  try{ document.documentElement.lang = window.USER_LANG || "en"; }catch(_){}
});
document.addEventListener("languageUpdated", ()=> {
  try{ document.documentElement.lang = window.USER_LANG || "en"; }catch(_){}
});

/* =====================================
   SIMPLE PRODUCT NAME TRANSLATION
===================================== */
window.PRODUCT_TRANSLATIONS = {
  de:{
    "cricket bat":"der Kricketschläger"
  }
};

window.translateProductName = function(name){
  const lang = window.USER_LANG || "en";
  const key = String(name || "").trim().toLowerCase();
  const map = window.PRODUCT_TRANSLATIONS[lang];
  return (map && map[key]) ? map[key] : name;
};

/* =====================================
   BOTTOM NAV DOWNLOAD ICON
===================================== */
(function(){
  function ensureDownloadNavIcon(){
    const navs = Array.from(document.querySelectorAll(".nav"));
    navs.forEach(nav => {
      if(!nav || nav.nodeType !== 1) return;
      const itemCount = nav.children ? nav.children.length : 0;
      if(itemCount < 3 || itemCount > 10) return;
      if(nav.querySelector("[data-nav-download='1']")) return;
      if(nav.querySelector("[onclick*='dowlond.html'],[href*='dowlond.html']")) return;

      const hasSvg = !!nav.querySelector("svg");
      if(!hasSvg) return;

      const item = document.createElement("div");
      item.setAttribute("data-nav-download", "1");
      item.setAttribute("role", "button");
      item.tabIndex = 0;
      item.onclick = () => { location.href = "dowlond.html"; };
      item.onkeydown = (event) => {
        if(event.key === "Enter" || event.key === " "){
          event.preventDefault();
          location.href = "dowlond.html";
        }
      };

      const icon = "<svg viewBox='0 0 24 24' aria-hidden='true'><path d='M5 20h14v-2H5v2zm7-18v10.17l3.59-3.58L17 10l-5 5-5-5 1.41-1.41L11 12.17V2h1z'/></svg>";
      const hasLabel = !!nav.querySelector(".nav-label");
      item.innerHTML = hasLabel
        ? icon + "<span class='nav-label'>Download</span>"
        : icon;

      if(/dowlond\.html$/i.test(String(location.pathname || ""))){
        item.classList.add("active");
      }
      nav.appendChild(item);
    });
  }

  document.addEventListener("DOMContentLoaded", ensureDownloadNavIcon);
})();
