let uid = "";
let ms = "";
let productData = null;

document.addEventListener("DOMContentLoaded", () => {
    init().catch((err) => {
        console.error("INIT ERROR:", err);
        alert("Không tải được thông tin đơn hàng, anh/chị vui lòng tải lại trang giúp em ạ.");
    });
});

async function init() {
    const params = new URLSearchParams(window.location.search);
    uid = params.get("uid") || "";
    ms = params.get("ms") || "";

    // 1. Tải thông tin sản phẩm
    const res = await fetch(`/api/get-product?ms=${encodeURIComponent(ms)}`);
    if (!res.ok) {
        throw new Error("Không tải được sản phẩm");
    }
    productData = await res.json();
    if (productData.error) {
        throw new Error("Không tìm thấy sản phẩm");
    }

    // 2. Gán dữ liệu sản phẩm vào form
    document.getElementById("productName").value = productData.name || "";
    const price = Number(productData.price || 0);
    document.getElementById("price").value = formatVND(price);
    document.getElementById("fanpageName").innerText =
        productData.fanpageName || "Shop";

    if (productData.image) {
        const imgEl = document.getElementById("productImage");
        imgEl.src = productData.image;
        imgEl.style.display = "block";
    }

    loadDropdown("size", productData.sizes || []);
    loadDropdown("color", productData.colors || []);

    // 3. Load dữ liệu Tỉnh/Quận/Xã
    await loadAddressData();

    // 4. Gán event
    document.getElementById("quantity").addEventListener("change", calcTotal);
    document.getElementById("orderBtn").addEventListener("click", submitOrder);

    // 5. Tính thành tiền lần đầu
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
    arr.forEach((v) => {
        const op = document.createElement("option");
        op.value = v;
        op.innerText = v;
        el.appendChild(op);
    });
}

async function loadAddressData() {
    try {
        const res = await fetch("/static/vietnam2025.json");
        if (!res.ok) throw new Error("fail load vietnam2025.json");
        const data = await res.json();

        const province = document.getElementById("province");
        const district = document.getElementById("district");
        const ward = document.getElementById("ward");

        province.innerHTML = "";
        district.innerHTML = "";
        ward.innerHTML = "";

        data.forEach((p) => {
            const op = document.createElement("option");
            op.value = p.name;
            op.innerText = p.name;
            province.appendChild(op);
        });

        province.onchange = () => {
            const p = data.find((x) => x.name === province.value);
            district.innerHTML = "";
            ward.innerHTML = "";
            if (!p) return;

            p.districts.forEach((d) => {
                const op = document.createElement("option");
                op.value = d.name;
                op.innerText = d.name;
                district.appendChild(op);
            });

            district.onchange = () => {
                const d = p.districts.find((x) => x.name === district.value);
                ward.innerHTML = "";
                if (!d) return;
                d.wards.forEach((w) => {
                    const op = document.createElement("option");
                    op.value = w;
                    op.innerText = w;
                    ward.appendChild(op);
                });
            };

            // Gọi 1 lần để load ward theo district đầu tiên
            if (p.districts.length > 0) {
                district.value = p.districts[0].name;
                district.onchange();
            }
        };

        // Gọi 1 lần để load district/ward theo tỉnh đầu tiên
        if (data.length > 0) {
            province.value = data[0].name;
            province.onchange();
        }
    } catch (e) {
        console.error("Lỗi tải dữ liệu địa chỉ:", e);
    }
}

function calcTotal() {
    const qty = parseInt(document.getElementById("quantity").value || "1", 10);
    const priceText = document
        .getElementById("price")
        .value.replace(/[^\d]/g, "");
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
        quantity: parseInt(
            document.getElementById("quantity").value || "1",
            10
        ),
        total: document.getElementById("total").value,
        customerName: document.getElementById("customerName").value,
        phone: document.getElementById("phone").value,
        home: document.getElementById("home").value,
        province: document.getElementById("province").value,
        district: document.getElementById("district").value,
        ward: document.getElementById("ward").value,
        note: document.getElementById("note").value,
    };

    try {
        const res = await fetch("/api/order", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
        });
        if (!res.ok) {
            throw new Error("Status " + res.status);
        }
        alert(
            "Đặt hàng thành công! Nhân viên sẽ liên hệ xác nhận đơn trong ít phút nữa ạ ❤️"
        );
    } catch (e) {
        console.error("Lỗi gửi đơn:", e);
        alert(
            "Gửi đơn không thành công, anh/chị vui lòng thử lại sau hoặc nhắn trực tiếp cho shop giúp em ạ."
        );
    }
}
