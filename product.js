const router = require("express").Router();
const Product = require("../models/Product");

router.post("/add", async (req,res)=>{
  const p = new Product(req.body);
  await p.save();
  res.json("Product added");
});

router.get("/", async (req,res)=>{
  const data = await Product.find();
  res.json(data);
});

module.exports = router;