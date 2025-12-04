document.addEventListener("DOMContentLoaded", () => {
  // Helper lấy element với nhiều id fallback cho an toàn
  const $ = (...ids) => {
    for (const id of ids) {
      const el = document.getElementById(id);
      if (el) return el;
    }
    return null;
  };

  const provinceSelect = $("province", "province-select");
  const districtSelect = $("district", "district-select");
  const wardSelect = $("ward", "ward-select");
  const form = $("order-form", "orderForm");
  const qtyInput = $("quantity", "qty");
  const unitPriceInput = $("unit-price", "unitPrice");
  const totalPriceInput = $("total-price", "totalPrice");
  const productNameEl = $("product-name", "productName");
  const sizeSelect = $("size", "size-select");
  const colorSelect = $("color", "color-select");
  const imgEl = $("product-image", "productImage");
  const shopNameEl = $("shop-name", "shopName");

  const urlParams = new URLSearchParams(window.location.search);
  const ms = urlParams.get("ms") || "";
  const uid = urlParams.get("uid") || "";

  let locationData = []; // [{name, wards: []}]
  let unitPrice = 0;

  // ---------------------------
  // LOAD FILE ĐỊA GIỚI
  // ---------------------------
  if (provinceSelect && wardSelect) {
    fetch("/static/vietnam2025.json")
      .then((res) => res.json())
      .then((data) => {
        locationData = data || [];
        // Fill tỉnh
        provinceSelect.innerHTML = '<option value="">-- Chọn Tỉnh / Thành phố --</option>';
        locationData.forEach((p) => {
          const opt = document.createElement("option");
          opt.value = p.name;
          opt.textContent = p.name;
          provinceSelect.appendChild(opt);
        });

        // District không có dữ liệu => disable
        if (districtSelect) {
          districtSelect.innerHTML =
            '<option value="">(Không áp dụng)</option>';
          districtSelect.disabled = true;
        }

        wardSelect.innerHTML = '<option value="">-- Chọn Xã / Phường --</option>';
        wardSelect.disabled = true;
      })
      .catch((err) => {
        console.error("Lỗi load vietnam2025.json", err);
      });

    provinceSelect.addEventListener("change", () => {
      const provName = provinceSelect.value;
      const province = locationData.find((p) => p.name === provName);

      wardSelect.innerHTML = '<option value="">-- Chọn Xã / Phường --</option>';
      if (province && Array.isArray(province.wards)) {
        province.wards.forEach((w) => {
          const opt = document.createElement("option");
          opt.value = w;
          opt.textContent = w;
          wardSelect.appendChild(opt);
        });
        wardSelect.disabled = false;
      } else {
        wardSelect.disabled = true;
      }

      if (districtSelect) {
        districtSelect.value = "";
      }
    });
  }

  // ---------------------------
  // LOAD THÔNG TIN SẢN PHẨM
  // ---------------------------
  if (ms) {
    fetch(`/api/get-product?ms=${encodeURIComponent(ms)}`)
      .then((res) => res.json())
      .then((data) => {
        if (data.error) return;

        if (productNameEl) productNameEl.textContent = data.name || "";
        if (imgEl && data.image) imgEl.src = data.image;

        // Giá
        unitPrice = Number(data.price || 0);
        if (unitPriceInput) unitPriceInput.value = unitPrice.toString();
        updateTotal();

        // Fanpage name (shop name)
        if (shopNameEl && data.fanpageName) {
          shopNameEl.textContent = data.fanpageName;
        }

        // Size
        if (sizeSelect && Array.isArray(data.sizes)) {
          sizeSelect.innerHTML =
            '<option value="">-- Chọn size --</option>';
          data.sizes.forEach((s) => {
            const opt = document.createElement("option");
            opt.value = s;
            opt.textContent = s;
            sizeSelect.appendChild(opt);
          });
        }

        // Màu
        if (colorSelect && Array.isArray(data.colors)) {
          colorSelect.innerHTML =
            '<option value="">-- Chọn màu --</option>';
          data.colors.forEach((c) => {
            const opt = document.createElement("option");
            opt.value = c;
            opt.textContent = c;
            colorSelect.appendChild(opt);
          });
        }
      })
      .catch((err) => console.error("Lỗi load product", err));
  }

  // ---------------------------
  // TÍNH THÀNH TIỀN
  // ---------------------------
  function updateTotal() {
    if (!qtyInput || !totalPriceInput) return;
    const q = Number(qtyInput.value || 0);
    const total = q * unitPrice;
    totalPriceInput.value = total > 0 ? total.toString() : "";
  }

  if (qtyInput) {
    qtyInput.addEventListener("input", updateTotal);
  }

  // ---------------------------
  // SUBMIT FORM
  // ---------------------------
  if (form) {
    form.addEventListener("submit", (e) => {
      e.preventDefault();

      const payload = {
        uid: uid,
        ms: ms,
        productName: productNameEl ? productNameEl.textContent.trim() : "",
        size: sizeSelect ? sizeSelect.value : "",
        color: colorSelect ? colorSelect.value : "",
        quantity: qtyInput ? qtyInput.value : "",
        unitPrice: unitPrice,
        total: totalPriceInput ? totalPriceInput.value : "",
        province: provinceSelect ? provinceSelect.value : "",
        district: districtSelect ? districtSelect.value : "",
        ward: wardSelect ? wardSelect.value : "",
        customerName: $("customer-name", "customerName")?.value || "",
        phone: $("phone", "phoneNumber")?.value || "",
        home: $("home", "homeAddress")?.value || "",
        note: $("note", "noteField")?.value || ""
      };

      fetch("/api/order", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      })
        .then((res) => res.json())
        .then((data) => {
          alert("Đặt hàng thành công! Shop sẽ liên hệ xác nhận sớm.");
        })
        .catch((err) => {
          console.error("Lỗi gửi đơn", err);
          alert("Có lỗi khi gửi đơn, anh/chị thử lại giúp em ạ.");
        });
    });
  }
});
