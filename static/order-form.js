// ========================
// ORDER FORM JS - BẢN MỚI (KHÔNG CÓ HUYỆN)
// ========================

// Lấy params từ URL
const urlParams = new URLSearchParams(window.location.search);
const ms = urlParams.get("ms") || "";
const uid = urlParams.get("uid") || "";

// Kiểm tra nếu không có mã sản phẩm
if (!ms) {
    document.body.innerHTML = `
        <div style="text-align: center; padding: 50px; font-family: Arial, sans-serif;">
            <h2 style="color: #FF3B30;">⚠️ Không tìm thấy sản phẩm</h2>
            <p>Vui lòng quay lại Messenger và chọn sản phẩm để đặt hàng.</p>
            <a href="/" style="color: #1DB954; text-decoration: none; font-weight: bold;">Quay về trang chủ</a>
        </div>
    `;
    throw new Error("Không có mã sản phẩm");
}

// Hiển thị thông báo
function showMessage(text, isError = false) {
    const messageDiv = document.getElementById("message");
    messageDiv.textContent = text;
    messageDiv.style.display = "block";
    messageDiv.style.backgroundColor = isError ? "#FFE6E6" : "#E6F4EA";
    messageDiv.style.color = isError ? "#FF3B30" : "#1DB954";
    messageDiv.style.border = isError ? "1px solid #FF3B30" : "1px solid #1DB954";
    
    if (!isError) {
        setTimeout(() => {
            messageDiv.style.display = "none";
        }, 5000);
    }
}

// Load thông tin sản phẩm từ API
async function loadProduct() {
    try {
        showMessage("Đang tải thông tin sản phẩm...", false);
        
        const res = await fetch(`/api/get-product?ms=${ms}`);
        if (!res.ok) {
            throw new Error("Không tìm thấy sản phẩm");
        }
        
        const data = await res.json();

        if (data.error) {
            showMessage("Không tìm thấy sản phẩm.", true);
            setTimeout(() => {
                window.location.href = "/";
            }, 2000);
            return;
        }

        // Tên Fanpage
        document.getElementById("fanpageName").innerText = data.page_name || "Shop";

        // Tên SP
        document.getElementById("productName").value = data.name || "Sản phẩm";

        // Ảnh SP
        if (data.image) {
            document.getElementById("productImage").src = data.image;
            document.getElementById("productImage").style.display = "block";
        }

        // Giá
        const priceValue = data.price || 0;
        document.getElementById("price").value = priceValue.toLocaleString("vi-VN") + " đ";

        // Size - tạo dropdown
        const sizeSelect = document.getElementById("size");
        sizeSelect.innerHTML = '<option value="">-- Chọn size --</option>';
        
        if (data.sizes && data.sizes.length > 0) {
            data.sizes.forEach(s => {
                const opt = document.createElement("option");
                opt.value = s;
                opt.textContent = s;
                sizeSelect.appendChild(opt);
            });
        } else {
            const opt = document.createElement("option");
            opt.value = "Mặc định";
            opt.textContent = "Mặc định";
            sizeSelect.appendChild(opt);
        }

        // Màu - tạo dropdown
        const colorSelect = document.getElementById("color");
        colorSelect.innerHTML = '<option value="">-- Chọn màu --</option>';
        
        if (data.colors && data.colors.length > 0) {
            data.colors.forEach(c => {
                const opt = document.createElement("option");
                opt.value = c;
                opt.textContent = c;
                colorSelect.appendChild(opt);
            });
        } else {
            const opt = document.createElement("option");
            opt.value = "Mặc định";
            opt.textContent = "Mặc định";
            colorSelect.appendChild(opt);
        }

        // Lưu giá để tính tổng
        window.PRODUCT_PRICE = priceValue;
        
        // Tính tổng ban đầu
        calcTotal();
        
        showMessage("Tải thông tin sản phẩm thành công!", false);
        
    } catch (error) {
        console.error("Lỗi khi tải sản phẩm:", error);
        showMessage("Có lỗi khi tải thông tin sản phẩm.", true);
    }
}

// Tải dữ liệu địa chỉ
async function loadLocation() {
    try {
        const res = await fetch("/static/vietnam2025.json");
        const data = await res.json();

        const provinceSelect = document.getElementById("province");
        const wardSelect = document.getElementById("ward");

        // Đổ dữ liệu tỉnh
        data.forEach(p => {
            const opt = document.createElement("option");
            opt.value = p.name;
            opt.textContent = p.name;
            provinceSelect.appendChild(opt);
        });

        // Khi chọn tỉnh thay đổi
        provinceSelect.addEventListener("change", () => {
            wardSelect.innerHTML = '<option value="">-- Chọn phường/xã --</option>';
            
            const selectedProvince = provinceSelect.value;
            if (!selectedProvince) return;

            const province = data.find(p => p.name === selectedProvince);
            if (!province || !province.wards) return;

            // Sắp xếp xã theo thứ tự alphabet
            province.wards.sort().forEach(w => {
                const opt = document.createElement("option");
                opt.value = w;
                opt.textContent = w;
                wardSelect.appendChild(opt);
            });
        });

    } catch (error) {
        console.error("Lỗi khi tải địa chỉ:", error);
        showMessage("Không thể tải danh sách địa chỉ.", true);
    }
}

// Tính thành tiền
function calcTotal() {
    const qty = Number(document.getElementById("quantity").value || 1);
    const total = qty * (window.PRODUCT_PRICE || 0);
    document.getElementById("total").value = total.toLocaleString("vi-VN") + " đ";
}

// Gửi đơn hàng
async function submitOrder() {
    // Lấy dữ liệu
    const customerName = document.getElementById("customerName").value.trim();
    const phone = document.getElementById("phone").value.trim();
    const home = document.getElementById("home").value.trim();
    const province = document.getElementById("province").value;
    const ward = document.getElementById("ward").value;
    const size = document.getElementById("size").value;
    const color = document.getElementById("color").value;
    const quantity = document.getElementById("quantity").value;
    const note = document.getElementById("note").value.trim();

    // Validate
    if (!customerName) {
        showMessage("Vui lòng nhập họ tên", true);
        document.getElementById("customerName").focus();
        return false;
    }
    
    if (!phone) {
        showMessage("Vui lòng nhập số điện thoại", true);
        document.getElementById("phone").focus();
        return false;
    }
    
    // Kiểm tra số điện thoại
    const phoneRegex = /^(0[3|5|7|8|9])+([0-9]{8})\b/;
    if (!phoneRegex.test(phone)) {
        showMessage("Số điện thoại không hợp lệ. Vui lòng nhập số Việt Nam (10 số, bắt đầu 03, 05, 07, 08, 09)", true);
        document.getElementById("phone").focus();
        return false;
    }
    
    if (!home) {
        showMessage("Vui lòng nhập địa chỉ chi tiết", true);
        document.getElementById("home").focus();
        return false;
    }
    
    if (!province) {
        showMessage("Vui lòng chọn tỉnh/thành phố", true);
        document.getElementById("province").focus();
        return false;
    }
    
    if (!ward) {
        showMessage("Vui lòng chọn phường/xã", true);
        document.getElementById("ward").focus();
        return false;
    }
    
    if (!size) {
        showMessage("Vui lòng chọn size", true);
        document.getElementById("size").focus();
        return false;
    }
    
    if (!color) {
        showMessage("Vui lòng chọn màu", true);
        document.getElementById("color").focus();
        return false;
    }

    const payload = {
        uid,
        ms,
        customerName,
        phone,
        home,
        province,
        ward,
        size,
        color,
        quantity,
        productName: document.getElementById("productName").value,
        total: document.getElementById("total").value,
        note
    };

    try {
        showMessage("Đang gửi đơn hàng...", false);
        
        const res = await fetch("/api/order", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify(payload)
        });

        if (res.ok) {
            const result = await res.json();
            showMessage("✅ Đặt hàng thành công! Shop sẽ liên hệ xác nhận trong 5-10 phút.", false);
            
            // Clear form sau 3 giây
            setTimeout(() => {
                document.getElementById("customerName").value = "";
                document.getElementById("phone").value = "";
                document.getElementById("home").value = "";
                document.getElementById("province").selectedIndex = 0;
                document.getElementById("ward").innerHTML = '<option value="">-- Chọn phường/xã --</option>';
                document.getElementById("note").value = "";
            }, 3000);
            
            return true;
        } else {
            showMessage("❌ Có lỗi khi gửi đơn hàng. Vui lòng thử lại.", true);
            return false;
        }
    } catch (error) {
        console.error("Lỗi khi gửi đơn:", error);
        showMessage("❌ Có lỗi kết nối. Vui lòng thử lại.", true);
        return false;
    }
}

// Khởi tạo
document.addEventListener("DOMContentLoaded", function() {
    // Load dữ liệu
    loadProduct();
    loadLocation();
    
    // Gắn sự kiện tính tổng
    document.getElementById("quantity").addEventListener("change", calcTotal);
    document.getElementById("size").addEventListener("change", calcTotal);
    document.getElementById("color").addEventListener("change", calcTotal);
    
    // Gắn sự kiện nút đặt hàng
    document.getElementById("orderBtn").addEventListener("click", submitOrder);
    
    // Cho phép submit bằng Enter
    document.addEventListener("keypress", function(e) {
        if (e.key === "Enter" && e.target.tagName !== "TEXTAREA") {
            e.preventDefault();
            submitOrder();
        }
    });
});
