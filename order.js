const router = require("express").Router();
const Order = require("../models/Order");

router.post("/place", async (req,res)=>{
  const order = new Order(req.body);
  await order.save();
  res.json("Order placed");
});

router.get("/", async (req,res)=>{
  res.json(await Order.find());
});

module.exports = router;