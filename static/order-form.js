// ============================================
// PRODUCT VARIANT HANDLING
// ============================================

let currentProductData = {
    ms: PRODUCT_MS,
    basePrice: BASE_PRICE,
    color: document.getElementById('color').value,
    size: document.getElementById('size').value,
    quantity: 1
};

function formatPrice(n) {
    return n.toLocaleString('vi-VN') + ' đ';
}

async function updateImageByVariant() {
    const color = document.getElementById('color').value;
    const size = document.getElementById('size').value;
    const imageContainer = document.getElementById('image-container');
    
    // Show loading
    const currentImg = imageContainer.querySelector('img');
    if (currentImg) {
        currentImg.classList.add('loading');
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
                        this.src = 'https://via.placeholder.com/120x120?text=No+Image';
                    };
                    imageContainer.innerHTML = '';
                    imageContainer.appendChild(imgElement);
                }
                imgElement.src = data.image;
            } else {
                imageContainer.innerHTML = '<div class="placeholder-image">Chưa có ảnh cho thuộc tính này</div>';
            }
        }
    } catch (e) {
        console.error('Error updating image:', e);
    } finally {
        if (currentImg) {
            setTimeout(() => currentImg.classList.remove('loading'), 300);
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
            
            // Update current product data
            currentProductData.price = price;
            currentProductData.total = price * quantity;
        }
    } catch (e) {
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

// ============================================
// VIETTELPOST ADDRESS API
// ============================================

let provincesCache = [];
let districtsCache = [];
let wardsCache = [];

// Initialize Select2 for address dropdowns
function initAddressSelect2() {
    $('.select2-address').select2({
        language: 'vi',
        width: '100%',
        placeholder: function() {
            return $(this).data('placeholder');
        },
        allowClear: true
    });
}

// Load provinces from ViettelPost API
async function loadProvinces() {
    const provinceSelect = $('#province');
    
    try {
        // Show loading
        provinceSelect.html('<option value="">Đang tải tỉnh/thành...</option>');
        provinceSelect.prop('disabled', true);
        
        const response = await fetch('https://partner.viettelpost.vn/v2/categories/listProvince', {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Token': VIETTELPOST_TOKEN
            }
        });
        
        const data = await response.json();
        
        if (data.status === 200 && data.data) {
            provincesCache = data.data.sort((a, b) => 
                a.PROVINCE_NAME.localeCompare(b.PROVINCE_NAME, 'vi')
            );
            
            provinceSelect.html('<option value=""></option>');
            provincesCache.forEach(province => {
                provinceSelect.append(new Option(province.PROVINCE_NAME, province.PROVINCE_ID));
            });
            
            console.log(`Đã tải ${provincesCache.length} tỉnh/thành phố`);
            
            // Reinitialize Select2
            provinceSelect.select2({
                language: 'vi',
                placeholder: "Chọn Tỉnh/Thành phố",
                width: '100%',
                allowClear: true
            });
            
            // Load preset address from URL if any
            loadPresetAddress();
        } else {
            throw new Error('Không lấy được dữ liệu từ ViettelPost');
        }
    } catch (error) {
        console.error('Lỗi khi load tỉnh/thành:', error);
        // Fallback to static list
        loadStaticProvinces();
    } finally {
        provinceSelect.prop('disabled', false);
    }
}

// Load districts based on selected province
async function loadDistricts(provinceId) {
    const districtSelect = $('#district');
    const wardSelect = $('#ward');
    
    if (!provinceId) {
        districtSelect.html('<option value=""></option>').prop('disabled', true);
        wardSelect.html('<option value=""></option>').prop('disabled', true);
        
        districtSelect.select2({
            placeholder: "Chọn Quận/Huyện",
            disabled: true
        });
        wardSelect.select2({
            placeholder: "Chọn Phường/Xã",
            disabled: true
        });
        
        updateFullAddress();
        return;
    }
    
    try {
        districtSelect.html('<option value="">Đang tải quận/huyện...</option>');
        districtSelect.prop('disabled', true);
        wardSelect.prop('disabled', true);
        
        const response = await fetch(`https://partner.viettelpost.vn/v2/categories/listDistrict?provinceId=${provinceId}`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Token': VIETTELPOST_TOKEN
            }
        });
        
        const data = await response.json();
        
        if (data.status === 200 && data.data) {
            districtsCache = data.data.sort((a, b) => 
                a.DISTRICT_NAME.localeCompare(b.DISTRICT_NAME, 'vi')
            );
            
            districtSelect.html('<option value=""></option>');
            districtsCache.forEach(district => {
                districtSelect.append(new Option(district.DISTRICT_NAME, district.DISTRICT_ID));
            });
            
            console.log(`Đã tải ${districtsCache.length} quận/huyện`);
            
            // Reinitialize Select2
            districtSelect.select2({
                language: 'vi',
                placeholder: "Chọn Quận/Huyện",
                width: '100%',
                allowClear: true
            }).prop('disabled', false);
            
            // Clear wards
            wardSelect.html('<option value=""></option>').prop('disabled', true);
            wardSelect.select2({
                placeholder: "Chọn Phường/Xã",
                disabled: true
            });
        }
    } catch (error) {
        console.error('Lỗi khi load quận/huyện:', error);
        districtSelect.html('<option value="">Lỗi tải dữ liệu</option>');
    } finally {
        updateFullAddress();
    }
}

// Load wards based on selected district
async function loadWards(districtId) {
    const wardSelect = $('#ward');
    
    if (!districtId) {
        wardSelect.html('<option value=""></option>').prop('disabled', true);
        wardSelect.select2({
            placeholder: "Chọn Phường/Xã",
            disabled: true
        });
        
        updateFullAddress();
        return;
    }
    
    try {
        wardSelect.html('<option value="">Đang tải phường/xã...</option>');
        wardSelect.prop('disabled', true);
        
        const response = await fetch(`https://partner.viettelpost.vn/v2/categories/listWards?districtId=${districtId}`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Token': VIETTELPOST_TOKEN
            }
        });
        
        const data = await response.json();
        
        if (data.status === 200 && data.data) {
            wardsCache = data.data.sort((a, b) => 
                a.WARDS_NAME.localeCompare(b.WARDS_NAME, 'vi')
            );
            
            wardSelect.html('<option value=""></option>');
            wardsCache.forEach(ward => {
                wardSelect.append(new Option(ward.WARDS_NAME, ward.WARDS_ID));
            });
            
            console.log(`Đã tải ${wardsCache.length} phường/xã`);
            
            // Reinitialize Select2
            wardSelect.select2({
                language: 'vi',
                placeholder: "Chọn Phường/Xã",
                width: '100%',
                allowClear: true
            }).prop('disabled', false);
        }
    } catch (error) {
        console.error('Lỗi khi load phường/xã:', error);
        wardSelect.html('<option value="">Lỗi tải dữ liệu</option>');
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
    
    const provinceSelect = $('#province');
    provinceSelect.html('<option value=""></option>');
    
    staticProvinces.forEach((province, index) => {
        provinceSelect.append(new Option(province, index + 1));
    });
    
    provinceSelect.select2({
        language: 'vi',
        placeholder: "Chọn Tỉnh/Thành phố",
        width: '100%',
        allowClear: true
    }).prop('disabled', false);
    
    console.log('Đã tải danh sách tỉnh thành tĩnh (fallback)');
}

// Update full address from all components
function updateFullAddress() {
    const provinceText = $('#province option:selected').text() || '';
    const districtText = $('#district option:selected').text() || '';
    const wardText = $('#ward option:selected').text() || '';
    const detailText = $('#addressDetail').val() || '';
    
    // Save to hidden fields
    $('#provinceName').val(provinceText);
    $('#districtName').val(districtText);
    $('#wardName').val(wardText);
    
    // Build full address
    const fullAddress = [detailText, wardText, districtText, provinceText]
        .filter(part => part.trim() !== '')
        .join(', ');
    
    $('#fullAddress').val(fullAddress);
    
    // Update preview
    const previewElement = $('#addressPreview');
    if (fullAddress.trim()) {
        previewElement.html(`
            <div class="address-preview-content">
                <strong>Địa chỉ nhận hàng:</strong>
                <p>${fullAddress}</p>
            </div>
        `).show();
    } else {
        previewElement.hide();
    }
    
    return fullAddress;
}

// Load preset address from URL parameters
function loadPresetAddress() {
    const urlParams = new URLSearchParams(window.location.search);
    const presetAddress = urlParams.get('address');
    
    if (presetAddress) {
        $('#addressDetail').val(presetAddress);
        updateFullAddress();
    }
}

// ============================================
// FORM VALIDATION AND SUBMISSION
// ============================================

async function submitOrder() {
    // Collect form data
    const formData = {
        ms: PRODUCT_MS,
        uid: PRODUCT_UID,
        color: $('#color').val(),
        size: $('#size').val(),
        quantity: parseInt($('#quantity').val() || '1'),
        customerName: $('#customerName').val().trim(),
        phone: $('#phone').val().trim(),
        address: updateFullAddress(),
        provinceId: $('#province').val(),
        districtId: $('#district').val(),
        wardId: $('#ward').val(),
        provinceName: $('#provinceName').val(),
        districtName: $('#districtName').val(),
        wardName: $('#wardName').val(),
        addressDetail: $('#addressDetail').val().trim()
    };
    
    // Validate required fields
    if (!formData.customerName) {
        alert('Vui lòng nhập họ và tên');
        $('#customerName').focus();
        return;
    }
    
    if (!formData.phone) {
        alert('Vui lòng nhập số điện thoại');
        $('#phone').focus();
        return;
    }
    
    // Validate phone number
    const phoneRegex = /^(0|\+84)(\d{9,10})$/;
    if (!phoneRegex.test(formData.phone)) {
        alert('Số điện thoại không hợp lệ. Vui lòng nhập số điện thoại 10-11 chữ số');
        $('#phone').focus();
        return;
    }
    
    // Validate address
    if (!formData.provinceId) {
        alert('Vui lòng chọn Tỉnh/Thành phố');
        $('#province').select2('open');
        return;
    }
    
    if (!formData.districtId) {
        alert('Vui lòng chọn Quận/Huyện');
        $('#district').select2('open');
        return;
    }
    
    if (!formData.wardId) {
        alert('Vui lòng chọn Phường/Xã');
        $('#ward').select2('open');
        return;
    }
    
    if (!formData.addressDetail) {
        alert('Vui lòng nhập địa chỉ chi tiết (số nhà, tên đường)');
        $('#addressDetail').focus();
        return;
    }
    
    // Show loading
    const submitBtn = $('#submitBtn');
    const originalText = submitBtn.text();
    submitBtn.html('<span class="loading-spinner"></span> ĐANG XỬ LÝ...');
    submitBtn.prop('disabled', true);
    
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
            alert('🎉 Đã gửi đơn hàng thành công!\n\nShop sẽ liên hệ xác nhận trong 5-10 phút.\nCảm ơn anh/chị đã đặt hàng! ❤️');
            
            // Reset form (optional)
            $('#customerName').val('');
            $('#phone').val('');
            $('#addressDetail').val('');
            $('#province').val(null).trigger('change');
            $('#district').val(null).trigger('change');
            $('#ward').val(null).trigger('change');
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
        submitBtn.text(originalText);
        submitBtn.prop('disabled', false);
    }
}

// ============================================
// INITIALIZATION
// ============================================

$(document).ready(function() {
    // Initialize Select2 for address dropdowns
    initAddressSelect2();
    
    // Load provinces
    loadProvinces();
    
    // Event listeners for product variant changes
    $('#color').change(updateVariantInfo);
    $('#size').change(updateVariantInfo);
    $('#quantity').on('input', updatePriceByVariant);
    
    // Event listeners for address changes
    $('#province').on('change', function() {
        loadDistricts($(this).val());
        updateFullAddress();
    });
    
    $('#district').on('change', function() {
        loadWards($(this).val());
        updateFullAddress();
    });
    
    $('#ward').on('change', updateFullAddress);
    $('#addressDetail').on('input', updateFullAddress);
    
    // Initialize product variant info
    updateVariantInfo();
    
    // Enter key to submit form
    $('#orderForm').on('keypress', function(e) {
        if (e.which === 13) {
            e.preventDefault();
            submitOrder();
        }
    });
    
    // Focus on first field
    setTimeout(() => {
        $('#customerName').focus();
    }, 500);
});
