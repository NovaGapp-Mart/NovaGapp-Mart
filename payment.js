const Razorpay = require("razorpay");
const router = require("express").Router();

const RAZORPAY_KEY_ID = String(
  process.env.RAZORPAY_KEY_ID ||
  process.env.RAZORPAY_API_KEY ||
  process.env.API_KEY ||
  ""
).trim();
const RAZORPAY_KEY_SECRET = String(
  process.env.RAZORPAY_KEY_SECRET ||
  process.env.RAZORPAY_SECRET_KEY ||
  process.env.SECRET_KEY ||
  process.env.SECRATE_KEY ||
  ""
).trim();

const razorpay = new Razorpay({
  key_id: RAZORPAY_KEY_ID,
  key_secret: RAZORPAY_KEY_SECRET
});

router.post("/pay", async (req, res) => {
  if(!RAZORPAY_KEY_ID || !RAZORPAY_KEY_SECRET){
    return res.status(503).json({ error:"razorpay_not_configured" });
  }

  const amount = Math.max(1, Number(req.body?.amount) || 0) * 100;
  if(!amount){
    return res.status(400).json({ error:"invalid_amount" });
  }

  const order = await razorpay.orders.create({
    amount,
    currency: "INR"
  });
  res.json(order);
});

module.exports = router;