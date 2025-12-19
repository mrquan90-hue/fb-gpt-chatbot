// order-form.js

// Global variables
let PRODUCT_MS = '';
let PRODUCT_UID = '';
let BASE_PRICE = 0;
let DOMAIN = '';
let API_BASE_URL = '';

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    // Get data from window object
    if (window.PRODUCT_DATA) {
        PRODUCT_MS = window.PRODUCT_DATA.ms;
        PRODUCT_UID = window.PRODUCT_DATA.uid;
        BASE_PRICE = window.PRODUCT_DATA.basePrice;
        DOMAIN = window.PRODUCT_DATA.domain;
        API_BASE_URL = window.PRODUCT_DATA.apiBaseUrl;
        
        console.log('Product Data Loaded:', {
            ms: PRODUCT_MS,
            uid: PRODUCT_UID,
            basePrice: BASE_PRICE
        });
    } else {
        // Fallback: get from URL
        const params = new URLSearchParams(window.location.search);
        PRODUCT_MS = params.get('ms') || '';
        PRODUCT_UID = params.get('uid') || '';
        
        if (!PRODUCT_MS) {
            alert('❌ Không tìm thấy sản phẩm. Vui lòng quay lại Messenger.');
            return;
        }
    }
    
    // Initialize components
    loadProvinces();
    setupEventListeners();
    updateVariantInfo();
});

// ============================================
// PRODUCT VARIANT HANDLING
// ============================================

function formatPrice(n) {
    return n.toLocaleString('vi-VN') + ' đ';
}

async function updateImageByVariant() {
    const color = document.getElementById('color').value;
    const size = document.getElementById('size').value;
    const imageContainer = document.getElementById('image-container');
    
    // Show loading state
    const currentImg = imageContainer.querySelector('img');
    if (currentImg) {
        currentImg.style.opacity = '0.5';
    }
    
    try {
        const res = await fetch(`${API_BASE_URL}/get-variant-image?ms=${PRODUCT_MS}&color=${encodeURIComponent(color)}&size=${encodeURIComponent(size)}`);
        if (res.ok) {
            const data = await res.json();
            if (data.image && data.image.trim() !== '') {
                let imgElement = imageContainer.querySelector('img');
                if (!imgElement) {
                    imgElement = document.createElement('img');
                    imgElement.className = 'product-image';
                    imgElement.onerror = function() {
                        this.onerror = null;
                        this.src = 'https://via.placeholder.com/300x300?text=Không+có+ảnh';
                    };
                    imageContainer.innerHTML = '';
                    imageContainer.appendChild(imgElement);
                }
                imgElement.src = data.image;
                imgElement.style.opacity = '1';
                console.log('Updated image:', data.image.substring(0, 100));
            } else {
                // Show placeholder
                imageContainer.innerHTML = `
                    <div class="placeholder-image">
                        <i class="fas fa-image"></i>
                        <p>Chưa có ảnh cho thuộc tính này</p>
                    </div>`;
            }
        }
    } catch (e) {
        console.error('Error updating image:', e);
        // Restore original opacity
        if (currentImg) {
            currentImg.style.opacity = '1';
        }
    }
}

async function updatePriceByVariant() {
    const color = document.getElementById('color').value;
    const size = document.getElementById('size').value;
    const quantity = parseInt(document.getElementById('quantity').value || '1');

    try {
        const res = await fetch(`${API_BASE_URL}/get-variant-price?ms=${PRODUCT_MS}&color=${encodeURIComponent(color)}&size=${encodeURIComponent(size)}`);
        if (res.ok) {
            const data = await res.json();
            const price = data.price || BASE_PRICE;

            document.getElementById('price-display').innerText = formatPrice(price);
            document.getElementById('total-display').innerText = formatPrice(price * quantity);
            console.log('Updated price:', price);
        }
    } catch (e) {
        console.error('Error updating price:', e);
        // Fallback to base price
        document.getElementById('price-display').innerText = formatPrice(BASE_PRICE);
        document.getElementById('total-display').innerText = formatPrice(BASE_PRICE * quantity);
    }
}

async function updateVariantInfo() {
    await Promise.all([
        updateImageByVariant(),
        updatePriceByVariant()
    ]);
}

function changeQuantity(delta) {
    const quantityInput = document.getElementById('quantity');
    let current = parseInt(quantityInput.value) || 1;
    current += delta;
    if (current < 1) current = 1;
    if (current > 99) current = 99;
    quantityInput.value = current;
    updatePriceByVariant();
}

// ============================================
// VIETNAM ADDRESS API (Open API)
// ============================================

async function loadProvinces() {
    const provinceSelect = document.getElementById('province');
    
    try {
        // Show loading
        provinceSelect.innerHTML = '<option value="">Đang tải tỉnh/thành...</option>';
        provinceSelect.disabled = true;
        
        const response = await fetch('https://provinces.open-api.vn/api/p/');
        const data = await response.json();
        
        // Sort provinces by name
        const provinces = data.sort((a, b) => 
            a.name.localeCompare(b.name, 'vi')
        );
        
        provinceSelect.innerHTML = '<option value="">-- Chọn Tỉnh/Thành --</option>';
        provinces.forEach(province => {
            const option = document.createElement('option');
            option.value = province.code;
            option.textContent = province.name;
            provinceSelect.appendChild(option);
        });
        
        console.log(`✅ Đã tải ${provinces.length} tỉnh/thành phố từ Open API`);
        
    } catch (error) {
        console.error('❌ Lỗi khi load tỉnh/thành:', error);
        // Fallback to static list
        loadStaticProvinces();
    } finally {
        provinceSelect.disabled = false;
    }
}

async function loadDistricts(provinceId) {
    const districtSelect = document.getElementById('district');
    const wardSelect = document.getElementById('ward');
    
    if (!provinceId) {
        districtSelect.innerHTML = '<option value="">-- Chọn Quận/Huyện --</option>';
        wardSelect.innerHTML = '<option value="">-- Chọn Phường/Xã --</option>';
        districtSelect.disabled = true;
        wardSelect.disabled = true;
        updateFullAddress();
        return;
    }
    
    try {
        districtSelect.innerHTML = '<option value="">Đang tải quận/huyện...</option>';
        districtSelect.disabled = true;
        wardSelect.disabled = true;
        
        const response = await fetch(`https://provinces.open-api.vn/api/p/${provinceId}?depth=2`);
        const provinceData = await response.json();
        
        const districts = provinceData.districts || [];
        districts.sort((a, b) => a.name.localeCompare(b.name, 'vi'));
        
        districtSelect.innerHTML = '<option value="">-- Chọn Quận/Huyện --</option>';
        districts.forEach(district => {
            const option = document.createElement('option');
            option.value = district.code;
            option.textContent = district.name;
            districtSelect.appendChild(option);
        });
        
        console.log(`✅ Đã tải ${districts.length} quận/huyện`);
        districtSelect.disabled = false;
        
        // Clear wards
        wardSelect.innerHTML = '<option value="">-- Chọn Phường/Xã --</option>';
        wardSelect.disabled = true;
    } catch (error) {
        console.error('❌ Lỗi khi load quận/huyện:', error);
        districtSelect.innerHTML = '<option value="">Lỗi tải dữ liệu</option>';
    } finally {
        updateFullAddress();
    }
}

async function loadWards(districtId) {
    const wardSelect = document.getElementById('ward');
    
    if (!districtId) {
        wardSelect.innerHTML = '<option value="">-- Chọn Phường/Xã --</option>';
        wardSelect.disabled = true;
        updateFullAddress();
        return;
    }
    
    try {
        wardSelect.innerHTML = '<option value="">Đang tải phường/xã...</option>';
        wardSelect.disabled = true;
        
        const response = await fetch(`https://provinces.open-api.vn/api/d/${districtId}?depth=2`);
        const districtData = await response.json();
        
        const wards = districtData.wards || [];
        wards.sort((a, b) => a.name.localeCompare(b.name, 'vi'));
        
        wardSelect.innerHTML = '<option value="">-- Chọn Phường/Xã --</option>';
        wards.forEach(ward => {
            const option = document.createElement('option');
            option.value = ward.code;
            option.textContent = ward.name;
            wardSelect.appendChild(option);
        });
        
        console.log(`✅ Đã tải ${wards.length} phường/xã`);
        wardSelect.disabled = false;
    } catch (error) {
        console.error('❌ Lỗi khi load phường/xã:', error);
        wardSelect.innerHTML = '<option value="">Lỗi tải dữ liệu</option>';
    } finally {
        updateFullAddress();
    }
}

// Fallback: Static province list
function loadStaticProvinces() {
    const staticProvinces = [
        "An Giang", "Bà Rịa - Vũng Tàu", "Bắc Giang", "Bắc Kạn", "Bạc Liêu", 
        "Bắc Ninh", "Bến Tre", "Bình Định", "Bình Dương", "Bình Phước", 
        "Bình Thuận", "Cà Mau", "Cao Bằng", "Cần Thơ", "Đà Nẵng", 
        "Đắk Lắk", "Đắk Nông", "Điện Biên", "Đồng Nai", "Đồng Tháp", 
        "Gia Lai", "Hà Giang", "Hà Nam", "Hà Nội", "Hà Tĩnh", 
        "Hải Dương", "Hải Phòng", "Hậu Giang", "Hòa Bình", "Hưng Yên", 
        "Khánh Hòa", "Kiên Giang", "Kon Tum", "Lai Châu", "Lâm Đồng", 
        "Lạng Sơn", "Lào Cai", "Long An", "Nam Định", "Nghệ An", 
        "Ninh Bình", "Ninh Thuận", "Phú Thọ", "Phú Yên", "Quảng Bình", 
        "Quảng Nam", "Quảng Ngãi", "Quảng Ninh", "Quảng Trị", "Sóc Trăng", 
        "Sơn La", "Tây Ninh", "Thái Bình", "Thái Nguyên", "Thanh Hóa", 
        "Thừa Thiên Huế", "Tiền Giang", "TP Hồ Chí Minh", "Trà Vinh", 
        "Tuyên Quang", "Vĩnh Long", "Vĩnh Phúc", "Yên Bái"
    ];
    
    const provinceSelect = document.getElementById('province');
    provinceSelect.innerHTML = '<option value="">-- Chọn Tỉnh/Thành --</option>';
    
    staticProvinces.forEach((province, index) => {
        const option = document.createElement('option');
        option.value = index + 1;
        option.textContent = province;
        provinceSelect.appendChild(option);
    });
    
    provinceSelect.disabled = false;
    console.log('⚠️ Đã tải danh sách tỉnh thành tĩnh (fallback)');
}

// Update full address from all components
function updateFullAddress() {
    const provinceSelect = document.getElementById('province');
    const districtSelect = document.getElementById('district');
    const wardSelect = document.getElementById('ward');
    
    const provinceText = provinceSelect.options[provinceSelect.selectedIndex]?.text || '';
    const districtText = districtSelect.options[districtSelect.selectedIndex]?.text || '';
    const wardText = wardSelect.options[wardSelect.selectedIndex]?.text || '';
    const detailText = document.getElementById('addressDetail').value || '';
    
    // Save to hidden fields
    document.getElementById('provinceName').value = provinceText;
    document.getElementById('districtName').value = districtText;
    document.getElementById('wardName').value = wardText;
    
    // Build full address
    const fullAddress = [detailText, wardText, districtText, provinceText]
        .filter(part => part.trim() !== '' && part !== '-- Chọn Tỉnh/Thành --' 
                && part !== '-- Chọn Quận/Huyện --' && part !== '-- Chọn Phường/Xã --')
        .join(', ');
    
    document.getElementById('fullAddress').value = fullAddress;
    
    // Update preview
    const previewElement = document.getElementById('addressPreview');
    const previewText = document.getElementById('addressPreviewText');
    
    if (fullAddress.trim()) {
        previewText.textContent = fullAddress;
        previewElement.style.display = 'block';
    } else {
        previewElement.style.display = 'none';
    }
    
    return fullAddress;
}

// ============================================
// FORM VALIDATION AND SUBMISSION
// ============================================

async function submitOrder() {
    // Collect form data
    const formData = {
        ms: PRODUCT_MS,
        uid: PRODUCT_UID,
        color: document.getElementById('color').value,
        size: document.getElementById('size').value,
        quantity: parseInt(document.getElementById('quantity').value || '1'),
        customerName: document.getElementById('customerName').value.trim(),
        phone: document.getElementById('phone').value.trim(),
        address: updateFullAddress(),
        provinceId: document.getElementById('province').value,
        districtId: document.getElementById('district').value,
        wardId: document.getElementById('ward').value,
        provinceName: document.getElementById('provinceName').value,
        districtName: document.getElementById('districtName').value,
        wardName: document.getElementById('wardName').value,
        addressDetail: document.getElementById('addressDetail').value.trim()
    };
    
    // Validate required fields
    if (!formData.customerName) {
        alert('⚠️ Vui lòng nhập họ và tên');
        document.getElementById('customerName').focus();
        return;
    }
    
    if (!formData.phone) {
        alert('⚠️ Vui lòng nhập số điện thoại');
        document.getElementById('phone').focus();
        return;
    }
    
    // Validate phone number
    const phoneRegex = /^(0|\+84)(\d{9,10})$/;
    if (!phoneRegex.test(formData.phone)) {
        alert('⚠️ Số điện thoại không hợp lệ. Vui lòng nhập số điện thoại 10-11 chữ số (ví dụ: 0912345678 hoặc +84912345678)');
        document.getElementById('phone').focus();
        return;
    }
    
    // Validate address
    if (!formData.provinceId || document.getElementById('province').selectedIndex === 0) {
        alert('⚠️ Vui lòng chọn Tỉnh/Thành phố');
        document.getElementById('province').focus();
        return;
    }
    
    if (!formData.districtId || document.getElementById('district').selectedIndex === 0) {
        alert('⚠️ Vui lòng chọn Quận/Huyện');
        document.getElementById('district').focus();
        return;
    }
    
    if (!formData.wardId || document.getElementById('ward').selectedIndex === 0) {
        alert('⚠️ Vui lòng chọn Phường/Xã');
        document.getElementById('ward').focus();
        return;
    }
    
    if (!formData.addressDetail) {
        alert('⚠️ Vui lòng nhập địa chỉ chi tiết (số nhà, tên đường)');
        document.getElementById('addressDetail').focus();
        return;
    }
    
    // Show loading
    const submitBtn = document.getElementById('submitBtn');
    const originalText = submitBtn.innerHTML;
    submitBtn.innerHTML = '<span class="loading-spinner"></span> ĐANG XỬ LÝ...';
    submitBtn.disabled = true;
    
    try {
        const response = await fetch(`${API_BASE_URL}/submit-order`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(formData)
        });
        
        const data = await response.json();
        
        if (response.ok) {
            // Success
            alert('🎉 ĐÃ GỬI ĐƠN HÀNG THÀNH CÔNG!\n\nShop sẽ liên hệ xác nhận trong 5-10 phút.\nCảm ơn anh/chị đã đặt hàng! ❤️');
            
            // Reset form
            document.getElementById('customerName').value = '';
            document.getElementById('phone').value = '';
            document.getElementById('addressDetail').value = '';
            document.getElementById('province').selectedIndex = 0;
            document.getElementById('district').innerHTML = '<option value="">-- Chọn Quận/Huyện --</option>';
            document.getElementById('ward').innerHTML = '<option value="">-- Chọn Phường/Xã --</option>';
            document.getElementById('district').disabled = true;
            document.getElementById('ward').disabled = true;
            updateFullAddress();
            
        } else {
            // Error
            alert(`❌ ${data.message || 'Có lỗi xảy ra. Vui lòng thử lại sau'}`);
        }
    } catch (error) {
        console.error('Lỗi khi gửi đơn hàng:', error);
        alert('❌ Lỗi kết nối. Vui lòng thử lại sau!');
    } finally {
        // Restore button
        submitBtn.innerHTML = originalText;
        submitBtn.disabled = false;
    }
}

// ============================================
// EVENT LISTENERS SETUP
// ============================================

function setupEventListeners() {
    // Product variant change events
    document.getElementById('color').addEventListener('change', updateVariantInfo);
    document.getElementById('size').addEventListener('change', updateVariantInfo);
    document.getElementById('quantity').addEventListener('input', updatePriceByVariant);
    
    // Address change events
    document.getElementById('province').addEventListener('change', function() {
        loadDistricts(this.value);
        updateFullAddress();
    });
    
    document.getElementById('district').addEventListener('change', function() {
        loadWards(this.value);
        updateFullAddress();
    });
    
    document.getElementById('ward').addEventListener('change', updateFullAddress);
    document.getElementById('addressDetail').addEventListener('input', updateFullAddress);
    
    // Enter key to submit form
    document.getElementById('orderForm').addEventListener('keypress', function(e) {
        if (e.which === 13) {
            e.preventDefault();
            submitOrder();
        }
    });
    
    // Focus on first field
    setTimeout(() => {
        document.getElementById('customerName').focus();
    }, 500);
}

// ============================================
// INITIALIZATION
// ============================================

// Make functions available globally
window.changeQuantity = changeQuantity;
window.submitOrder = submitOrder;
