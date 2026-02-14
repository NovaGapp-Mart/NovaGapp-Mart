fetch("products.json")
.then(res => res.json())
.then(data => {
  const box = document.getElementById("products");
  data.forEach(p => {
    box.innerHTML += `
      <div class="card">
        <img src="${p.image}">
        <h4>${p.name}</h4>
        <p>₹${p.price}</p>
        <button onclick="addCart(${p.id})">Add to Cart</button>
      </div>`;
  });
});

function addCart(id){
  let cart = JSON.parse(localStorage.getItem("cart") || "[]");
  cart.push(id);
  localStorage.setItem("cart", JSON.stringify(cart));
  alert("Added to Cart");
}