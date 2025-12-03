let uid = "";
let ms = "";
let productData = null;

document.addEventListener("DOMContentLoaded", () => {
    init();
});

async function init() {
    const params = new URLSearchParams(window.location.search);
    uid = params.get("uid");
    ms = params.get("ms");

    try {
        const res = await fetch(`/api/get-product?ms=${ms}`);
        productData = await res.json();
    } catch (e) {
        console.error("Lỗi tải sản phẩm", e);
        alert("Không tải được thông tin sản phẩm, vui lòng thử lại sau.");
        return;
    }

    document.getElementById("productName").value = productData.name || "";
    const price = Number(productData.price || 0);
    document.getElementById("price").value = formatVND(price);
    document.getElementById("fanpageName").innerText = productData.fanpageName || "Shop";

    if (productData.image) {
        document.getElementById("productImage").src = productData.image;
    }

    loadDropdown("size", productData.sizes || []);
    loadDropdown("color", productData.colors || []);

    await loadAddressData();

    document.getElementById("quantity").addEventListener("change", calcTotal);
    document.getElementById("orderBtn").addEventListener("click", submitOrder);

    calcTotal();
}

function loadDropdown(id, arr) {
    const el = document.getElementById(id);
    el.innerHTML = "";
    if (!arr || arr.length === 0) {
        const op = document.createElement("option");
        op.value = "";
        op.innerText = "Không có dữ liệu";
        el.appendChild(op);
        return;
    }
    arr.forEach(v => {
        const op = document.createElement("option");
        op.value = v;
        op.innerText = v;
        el.appendChild(op);
    });
}

async function loadAddressData() {
    try {
        const res = await fetch("/static/vietnam2025.json");
        const data = await res.json();

        const province = document.getElementById("province");
        const district = document.getElementById("district");
        const ward = document.getElementById("ward");

        data.forEach(p => {
            let op = document.createElement("option");
            op.value = p.name;
            op.innerText = p.name;
            province.appendChild(op);
        });

        province.onchange = () => {
            const p = data.find(x => x.name === province.value);
            district.innerHTML = "";
            ward.innerHTML = "";

            if (!p) return;

            p.districts.forEach(d => {
                let op = document.createElement("option");
                op.value = d.name;
                op.innerText = d.name;
                district.appendChild(op);
            });

            district.onchange = () => {
                const d = p.districts.find(x => x.name === district.value);
                ward.innerHTML = "";
                if (!d) return;
                d.wards.forEach(w => {
                    let op = document.createElement("option");
                    op.value = w;
                    op.innerText = w;
                    ward.appendChild(op);
                });
            };

            district.dispatchEvent(new Event("change"));
        };

        province.dispatchEvent(new Event("change"));
    } catch (e) {
        console.error("Lỗi tải dữ liệu địa chỉ", e);
    }
}

function calcTotal() {
    const qty = parseInt(document.getElementById("quantity").value || "1", 10);
    const priceText = document.getElementById("price").value.replace(/[^\d]/g, "");
    const price = Number(priceText || "0");
    const total = qty * price;
    document.getElementById("total").value = formatVND(total);
}

function formatVND(num) {
    if (!num || isNaN(num)) return "0đ";
    return num.toLocaleString("vi-VN") + "đ";
}

async function submitOrder() {
    const payload = {
        uid,
        ms,
        productName: productData.name,
        price: productData.price,
        size: document.getElementById("size").value,
        color: document.getElementById("color").value,
        quantity: parseInt(document.getElementById("quantity").value || "1", 10),
        total: document.getElementById("total").value,
        customerName: document.getElementById("customerName").value,
        phone: document.getElementById("phone").value,
        home: document.getElementById("home").value,
        province: document.getElementById("province").value,
        district: document.getElementById("district").value,
        ward: document.getElementById("ward").value,
        note: document.getElementById("note").value
    };

    try:
        const res = await fetch("/api/order", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        });
        if (!res.ok) {
            throw new Error("Status " + res.status);
        }
        alert("Đặt hàng thành công! Nhân viên sẽ liên hệ xác nhận đơn ạ ❤️");
    } catch (e) {
        console.error("Lỗi gửi đơn", e);
        alert("Gửi đơn không thành công, anh/chị thử lại sau ạ.");
    }
}
