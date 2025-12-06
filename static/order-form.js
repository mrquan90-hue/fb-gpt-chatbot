// ========================
// ORDER FORM JS (BẢN FIX FULL KHỚP HTML)
// ========================

// Lấy params từ URL
const urlParams = new URLSearchParams(window.location.search);
const ms = urlParams.get("ms") || "";
const uid = urlParams.get("uid") || "";

// Load thông tin sản phẩm từ API
async function loadProduct() {
    const res = await fetch(`/api/get-product?ms=${ms}`);
    const data = await res.json();

    if (data.error) {
        alert("Không tìm thấy sản phẩm.");
        return;
    }

    // Tên Fanpage
    document.getElementById("fanpageName").innerText = data.page_name;

    // Tên SP
    document.getElementById("productName").value = data.name;

    // Ảnh SP
    document.getElementById("productImage").src = data.image;

    // Giá
    document.getElementById("price").value =
        data.price.toLocaleString("vi-VN") + " đ";

    // Size
    const sizeSelect = document.getElementById("size");
    data.sizes.forEach(s => {
        const opt = document.createElement("option");
        opt.value = s;
        opt.textContent = s;
        sizeSelect.appendChild(opt);
    });

    // Màu
    const colorSelect = document.getElementById("color");
    data.colors.forEach(c => {
        const opt = document.createElement("option");
        opt.value = c;
        opt.textContent = c;
        colorSelect.appendChild(opt);
    });

    // Lưu giá để tính tổng
    window.PRODUCT_PRICE = data.price;
}
loadProduct();


// ========================
// ẨN HUYỆN + GHI CHÚ
// ========================
document.getElementById("district").style.display = "none";

const note = document.createElement("div");
note.style.color = "red";
note.style.marginTop = "6px";
note.innerText = "Vui lòng nhập địa chỉ mới sau sáp nhập (không cần chọn quận/huyện)";
document.getElementById("district").after(note);


// ========================
// TẢI DANH SÁCH TỈNH – XÃ (34 tỉnh)
// ========================
async function loadLocation() {
    const res = await fetch("/static/vietnam2025.json");
    const data = await res.json();

    const provinceSelect = document.getElementById("province");
    const wardSelect = document.getElementById("ward");

    data.forEach(p => {
        const opt = document.createElement("option");
        opt.value = p.name;
        opt.textContent = p.name;
        provinceSelect.appendChild(opt);
    });

    provinceSelect.addEventListener("change", () => {
        wardSelect.innerHTML = "";
        const province = data.find(p => p.name === provinceSelect.value);
        if (!province) return;

        province.wards.forEach(w => {
            const opt = document.createElement("option");
            opt.value = w;
            opt.textContent = w;
            wardSelect.appendChild(opt);
        });
    });
}
loadLocation();


// ========================
// TÍNH THÀNH TIỀN
// ========================
function calcTotal() {
    const qty = Number(document.getElementById("quantity").value || 1);
    const total = qty * (window.PRODUCT_PRICE || 0);
    document.getElementById("total").value =
        total.toLocaleString("vi-VN") + " đ";
}
document.getElementById("quantity").addEventListener("change", calcTotal);


// ========================
// GỬI ĐƠN HÀNG
// ========================
document.getElementById("orderBtn").addEventListener("click", async () => {
    const payload = {
        uid,
        ms,
        customerName: document.getElementById("customerName").value,
        phone: document.getElementById("phone").value,
        home: document.getElementById("home").value,
        province: document.getElementById("province").value,
        ward: document.getElementById("ward").value,
        size: document.getElementById("size").value,
        color: document.getElementById("color").value,
        quantity: document.getElementById("quantity").value,
        productName: document.getElementById("productName").value,
        total: document.getElementById("total").value,
        note: document.getElementById("note").value
    };

    const res = await fetch("/api/order", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(payload)
    });

    if (res.status === 200) {
        alert("Đặt hàng thành công! Shop sẽ liên hệ xác nhận.");
    } else {
        alert("Có lỗi khi gửi đơn hàng.");
    }
});
