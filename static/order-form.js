// ========================
// ORDER FORM JS (ĐÃ FIX)
// ========================

// Lấy params từ URL
const urlParams = new URLSearchParams(window.location.search);
const productName = urlParams.get("product_name") || "";
const productPrice = parseInt(urlParams.get("price") || 0);
const productImage = urlParams.get("image") || "";
const pageName = urlParams.get("page_name") || "";

// ===== 1. Update tên shop theo Fanpage =====
document.getElementById("shopName").innerText = pageName;

// ===== 2. ẨN TRƯỜNG HUYỆN + THÊM GHI CHÚ =====
document.getElementById("districtWrapper").style.display = "none";
document.getElementById("addressNote").innerText = "Vui lòng nhập địa chỉ mới sau sáp nhập (không cần chọn quận/huyện).";

// ===== Load JSON tỉnh – xã =====
async function loadLocation() {
    const res = await fetch("/static/vietnam2025.json");
    const data = await res.json();

    const provinceSelect = document.getElementById("province");
    const wardSelect = document.getElementById("ward");

    // Load tỉnh
    data.forEach(p => {
        const option = document.createElement("option");
        option.value = p.name;
        option.textContent = p.name;
        provinceSelect.appendChild(option);
    });

    // Khi chọn tỉnh -> load xã
    provinceSelect.addEventListener("change", () => {
        const selected = data.find(p => p.name === provinceSelect.value);

        wardSelect.innerHTML = `<option value="">Chọn Xã / Phường</option>`;
        if (selected) {
            selected.wards.forEach(w => {
                const option = document.createElement("option");
                option.value = w;
                option.textContent = w;
                wardSelect.appendChild(option);
            });
        }
    });
}
loadLocation();

// ===== 3. HIỂN THỊ TÊN SẢN PHẨM, ĐƠN GIÁ, THÀNH TIỀN =====
document.getElementById("productName").value = productName;
document.getElementById("price").value = productPrice.toLocaleString("vi-VN") + " đ";

function calcTotal() {
    const qty = Number(document.getElementById("quantity").value || 1);
    const total = qty * productPrice;
    document.getElementById("total").value = total.toLocaleString("vi-VN") + " đ";
}

// Tự tính lại thành tiền khi chọn số lượng
document.getElementById("quantity").addEventListener("change", calcTotal);
calcTotal();

// ===== 4. GỬI ĐƠN HÀNG =====
document.getElementById("orderForm").addEventListener("submit", async (e) => {
    e.preventDefault();

    const payload = {
        fullname: document.getElementById("fullname").value,
        phone: document.getElementById("phone").value,
        province: document.getElementById("province").value,
        ward: document.getElementById("ward").value,
        address_more: document.getElementById("address_more").value,
        product: productName,
        quantity: document.getElementById("quantity").value,
        price: productPrice,
        total: productPrice * Number(document.getElementById("quantity").value),
        note: document.getElementById("note").value,
        image: productImage,
        page_name: pageName
    };

    const res = await fetch("/submit-order", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
    });

    if (res.status === 200) {
        alert("Đặt hàng thành công! Shop sẽ liên hệ xác nhận.");
    } else {
        alert("Có lỗi xảy ra, vui lòng thử lại!");
    }
});
