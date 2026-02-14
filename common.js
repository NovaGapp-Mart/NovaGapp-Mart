// ===== GLOBAL PLAN FLAGS =====
const PLAN_FREE = "free";
const PLAN_BASIC = "4";
const PLAN_PRO = "40";
const PLAN_ENTERPRISE = "4000";

const userPlan = localStorage.getItem("userPlan") || PLAN_FREE;

const isFree = userPlan === PLAN_FREE;
const isBasic = userPlan === PLAN_BASIC;
const isPro = userPlan === PLAN_PRO;
const isEnterprise = userPlan === PLAN_ENTERPRISE;