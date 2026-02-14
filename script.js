// CART ARRAY
let cart = JSON.parse(localStorage.getItem("cart")) || [];

// ADD TO CART
function addToCart(name, price, image){
  cart.push({ name, price, image });
  localStorage.setItem("cart", JSON.stringify(cart));
  alert(name + " added to cart");
}