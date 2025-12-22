from_ad and ms_from_ad in PRODUCTS:
                        print(f"[ADS PRODUCT] Xác định sản phẩm từ ad_title: {ms_from_ad}")
                        
                        # KHÔNG reset context, mà update context với sản phẩm mới
                        ctx["last_ms"] = ms_from_ad
                        ctx["pending_carousel_ms"] = ms_from_ad  # Đánh dấu cần gửi carousel
                        ctx["first_message_after_referral"] = True
                        update_product_context(sender_id, ms_from_ad)
                        
                        # Gửi thông báo ngắn, KHÔNG gửi thông tin chi tiết
                        welcome_msg = f"""Chào anh/chị! 👋 
Em là trợ lý AI của {get_fanpage_name_from_api()}.

Em thấy anh/chị quan tâm đến sản phẩm **[{ms_from_ad}]** từ quảng cáo.
Để xem thông tin chi tiết, anh/chị vui lòng gửi tin nhắn bất kỳ ạ!"""
                        
                        send_message(sender_id, welcome_msg)
                        handled = True
                    
                    # ƯU TIÊN 2: Kiểm tra referral payload
                    if not handled and referral_payload:
                        detected_ms = detect_ms_from_text(referral_payload)
                        if detected_ms and detected_ms in PRODUCTS:
                            print(f"[ADS REFERRAL] Nhận diện mã từ payload: {detected_ms}")
                            ctx["last_ms"] = detected_ms
                            ctx["pending_carousel_ms"] = detected_ms  # Đánh dấu cần gửi carousel
                            ctx["first_message_after_referral"] = True
                            update_product_context(sender_id, detected_ms)
                            
                            welcome_msg = f"""Chào anh/chị! 👋 
Em là trợ lý AI của {get_fanpage_name_from_api()}.

Em thấy anh/chị quan tâm đến sản phẩm **[{detected_ms}]**.
Để xem thông tin chi tiết, anh/chị vui lòng gửi tin nhắn bất kỳ ạ!"""
                            
                            send_message(sender_id, welcome_msg)
                            handled = True
                
                # Nếu đã xử lý xong (ADS có sản phẩm) thì bỏ qua phần sau
                if handled:
                    continue
                
                # CHỈ reset context nếu KHÔNG phải từ ADS hoặc không xác định được sản phẩm
                if ctx.get("referral_source") != "ADS" or not ctx.get("last_ms"):
                    print(f"[REFERRAL RESET] Reset context cho user {sender_id}")
                    ctx["last_ms"] = None
                    ctx["product_history"] = []
                
                # Fallback: Xử lý referral bình thường
                if referral_payload:
                    detected_ms = detect_ms_from_text(referral_payload)
                    
                    if detected_ms and detected_ms in PRODUCTS:
                        print(f"[REFERRAL AUTO] Nhận diện mã sản phẩm từ referral: {detected_ms}")
                        
                        ctx["last_ms"] = detected_ms
                        ctx["pending_carousel_ms"] = detected_ms  # Đánh dấu cần gửi carousel
                        ctx["first_message_after_referral"] = True
                        update_product_context(sender_id, detected_ms)
                        
                        welcome_msg = f"""Chào anh/chị! 👋 
Em là trợ lý AI của {FANPAGE_NAME}.

Em thấy anh/chị quan tâm đến sản phẩm mã [{detected_ms}].
Để xem thông tin chi tiết, anh/chị vui lòng gửi tin nhắn bất kỳ ạ!"""
                        send_message(sender_id, welcome_msg)
                        continue
                    else:
                        welcome_msg = f"""Chào anh/chị! 👋 
Em là trợ lý AI của {FANPAGE_NAME}.

Để em tư vấn chính xác, anh/chị vui lòng:
1. Gửi mã sản phẩm (ví dụ: [MS123456])
2. Hoặc gõ "xem sản phẩm" để xem danh sách
3. Hoặc mô tả sản phẩm bạn đang tìm

Anh/chị quan tâm sản phẩm nào ạ?"""
                        send_message(sender_id, welcome_msg)
                        continue
            
            # ============================================
            # XỬ LÝ POSTBACK (GET_STARTED, ADVICE_, ORDER_)
            # ============================================
            if "postback" in m:
                payload = m["postback"].get("payload")
                if payload:
                    postback_id = m["postback"].get("mid")
                    
                    # KIỂM TRA NHANH TRƯỚC KHI XỬ LÝ
                    ctx = USER_CONTEXT.get(sender_id, {})
                    last_payload = ctx.get("last_postback_payload")
                    last_payload_time = ctx.get("last_postback_time", 0)
                    
                    now = time.time()
                    if payload == last_payload and (now - last_payload_time) < 1:
                        print(f"[WEBHOOK QUICK SKIP] Bỏ qua postback trùng trong 1s: {payload}")
                        continue  # Bỏ qua ngay lập tức
                    
                    # Sử dụng hàm xử lý mới
                    handle_postback_with_recovery(sender_id, payload, postback_id)
                    continue
            
            # ============================================
            # XỬ LÝ TIN NHẮN THƯỜNG (TEXT & ẢNH) - ĐÃ SỬA DUPLICATE CHECK 30s
            # ============================================
            if "message" in m:
                msg = m["message"]
                text = msg.get("text")
                attachments = msg.get("attachments") or []
                
                msg_mid = msg.get("mid")
                timestamp = m.get("timestamp", 0)
                
                if msg_mid:
                    ctx = USER_CONTEXT[sender_id]
                    if "processed_message_mids" not in ctx:
                        ctx["processed_message_mids"] = {}
                    
                    if msg_mid in ctx["processed_message_mids"]:
                        processed_time = ctx["processed_message_mids"][msg_mid]
                        now = time.time()
                        if now - processed_time < 30:  # TĂNG TỪ 3s LÊN 30s ĐỂ TRÁNH DUPLICATE
                            print(f"[MSG DUPLICATE] Bỏ qua message đã xử lý: {msg_mid}")
                            continue
                    
                    last_msg_time = ctx.get("last_msg_time", 0)
                    now = time.time()
                    
                    if now - last_msg_time < 0.5:
                        print(f"[MSG DEBOUNCE] Message đến quá nhanh, bỏ qua: {msg_mid}")
                        continue
                    
                    ctx["last_msg_time"] = now
                    ctx["processed_message_mids"][msg_mid] = now
                    
                    if len(ctx["processed_message_mids"]) > 50:
                        sorted_items = sorted(ctx["processed_message_mids"].items(), key=lambda x: x[1], reverse=True)[:30]
                        ctx["processed_message_mids"] = dict(sorted_items)
                
                if text:
                    ctx = USER_CONTEXT[sender_id]
                    if ctx.get("processing_lock"):
                        print(f"[TEXT LOCKED] User {sender_id} đang được xử lý, bỏ qua text: {text[:50]}...")
                        continue
                    
                    handle_text(sender_id, text)
                elif attachments:
                    for att in attachments:
                        if att.get("type") == "image":
                            image_url = att.get("payload", {}).get("url")
                            if image_url:
                                ctx = USER_CONTEXT[sender_id]
                                if ctx.get("processing_lock"):
                                    print(f"[IMAGE LOCKED] User {sender_id} đang được xử lý, bỏ qua image")
                                    continue
                                
                                handle_image(sender_id, image_url)

    return "OK", 200

# ============================================
# ORDER FORM PAGE
# ============================================

@app.route("/order-form", methods=["GET"])
def order_form():
    ms = (request.args.get("ms") or "").upper()
    uid = request.args.get("uid") or ""
    if not ms:
        return (
            """
        <html>
        <body style="text-align: center; padding: 50px; font-family: Arial, sans-serif;">
            <h2 style="color: #FF3B30;">⚠️ Không tìm thấy sản phẩm</h2>
            <p>Vui lòng quay lại Messenger và chọn sản phẩm để đặt hàng.</p>
            <a href="/" style="color: #1DB954; text-decoration: none; font-weight: bold;">Quay về trang chủ</a>
        </body>
        </html>
        """,
            400,
        )

    load_products()
    if ms not in PRODUCTS:
        return (
            """
        <html>
        <body style="text-align: center; padding: 50px; font-family: Arial, sans-serif;">
            <h2 style="color: #FF3B30;">⚠️ Sản phẩm không tồn tại</h2>
            <p>Vui lòng quay lại Messenger và chọn sản phẩm khác giúp shop ạ.</p>
            <a href="/" style="color: #1DB954; text-decoration: none; font-weight: bold;">Quay về trang chủ</a>
        </body>
        </html>
        """,
            404,
        )

    # Lấy tên fanpage từ API
    current_fanpage_name = get_fanpage_name_from_api()
    
    row = PRODUCTS[ms]
    
    # Lấy ảnh mặc định (ảnh đầu tiên từ sản phẩm)
    images_field = row.get("Images", "")
    urls = parse_image_urls(images_field)
    default_image = urls[0] if urls else ""

    size_field = row.get("size (Thuộc tính)", "")
    color_field = row.get("màu (Thuộc tính)", "")

    sizes = []
    if size_field:
        sizes = [s.strip() for s in size_field.split(",") if s.strip()]

    colors = []
    if color_field:
        colors = [c.strip() for c in color_field.split(",") if c.strip()]

    if not sizes:
        sizes = ["Mặc định"]
    if not colors:
        colors = ["Mặc định"]

    price_str = row.get("Gia", "0")
    price_int = extract_price_int(price_str) or 0

    # Tạo HTML với form địa chỉ sử dụng API miễn phí
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8" />
        <title>Đặt hàng - {row.get('Ten','')}</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <style>
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}
            
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
                min-height: 100vh;
                display: flex;
                justify-content: center;
                align-items: center;
                padding: 20px;
                color: #333;
            }}
            
            .container {{
                max-width: 480px;
                width: 100%;
                background: #fff;
                border-radius: 20px;
                box-shadow: 0 10px 30px rgba(0, 0, 0, 0.1);
                overflow: hidden;
            }}
            
            .header {{
                background: linear-gradient(135deg, #1DB954 0%, #17a74d 100%);
                padding: 20px;
                text-align: center;
                color: white;
            }}
            
            .header h2 {{
                font-size: 20px;
                font-weight: 600;
                margin: 0;
            }}
            
            .content {{
                padding: 20px;
            }}
            
            .product-section {{
                display: flex;
                gap: 15px;
                margin-bottom: 25px;
                padding-bottom: 20px;
                border-bottom: 1px solid #eee;
            }}
            
            .product-image-container {{
                width: 120px;
                height: 120px;
                border-radius: 12px;
                overflow: hidden;
                background: #f8f9fa;
                display: flex;
                align-items: center;
                justify-content: center;
                flex-shrink: 0;
            }}
            
            .product-image {{
                width: 100%;
                height: 100%;
                object-fit: cover;
                transition: transform 0.3s ease;
            }}
            
            .product-image:hover {{
                transform: scale(1.05);
            }}
            
            .product-image.loading {{
                opacity: 0.7;
            }}
            
            .placeholder-image {{
                width: 100%;
                height: 100%;
                display: flex;
                align-items: center;
                justify-content: center;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                font-size: 13px;
                text-align: center;
                padding: 10px;
                border-radius: 12px;
            }}
            
            .product-info {{
                flex: 1;
            }}
            
            .product-code {{
                font-size: 12px;
                color: #666;
                background: #f5f5f5;
                padding: 6px 10px;
                border-radius: 6px;
                display: inline-block;
                margin-bottom: 8px;
                font-family: 'Courier New', monospace;
                font-weight: 500;
            }}
            
            .product-title {{
                font-size: 16px;
                font-weight: 600;
                margin: 0 0 8px 0;
                line-height: 1.4;
                color: #222;
            }}
            
            .product-price {{
                color: #FF3B30;
                font-size: 18px;
                font-weight: 700;
            }}
            
            .form-group {{
                margin-bottom: 18px;
            }}
            
            .form-group label {{
                display: block;
                margin-bottom: 6px;
                font-size: 14px;
                font-weight: 500;
                color: #444;
            }}
            
            .form-control {{
                width: 100%;
                padding: 12px 15px;
                border: 2px solid #e1e5e9;
                border-radius: 10px;
                font-size: 14px;
                transition: all 0.3s ease;
                background: #fff;
            }}
            
            .form-control:focus {{
                outline: none;
                border-color: #1DB954;
                box-shadow: 0 0 0 3px rgba(29, 185, 84, 0.1);
            }}
            
            .form-control:disabled {{
                background-color: #f8f9fa;
                cursor: not-allowed;
            }}
            
            .address-row {{
                display: flex;
                gap: 10px;
                margin-bottom: 10px;
            }}
            
            .address-col {{
                flex: 1;
            }}
            
            .address-preview {{
                margin-top: 15px;
                padding: 15px;
                background: #f8f9fa;
                border-radius: 10px;
                border-left: 4px solid #1DB954;
                display: none;
            }}
            
            .address-preview-content {{
                font-size: 13px;
                line-height: 1.5;
            }}
            
            .address-preview-content strong {{
                color: #444;
                display: block;
                margin-bottom: 5px;
            }}
            
            .address-preview-content p {{
                margin: 0;
                color: #666;
            }}
            
            .total-section {{
                background: #f8f9fa;
                padding: 18px;
                border-radius: 12px;
                margin: 25px 0;
                text-align: center;
            }}
            
            .total-label {{
                font-size: 14px;
                color: #666;
                margin-bottom: 5px;
            }}
            
            .total-amount {{
                font-size: 24px;
                font-weight: 700;
                color: #FF3B30;
            }}
            
            .submit-btn {{
                width: 100%;
                padding: 16px;
                border: none;
                border-radius: 50px;
                background: linear-gradient(135deg, #1DB954 0%, #17a74d 100%);
                color: white;
                font-size: 16px;
                font-weight: 600;
                cursor: pointer;
                transition: all 0.3s ease;
                margin-top: 10px;
                display: flex;
                align-items: center;
                justify-content: center;
                gap: 10px;
            }}
            
            .submit-btn:hover {{
                transform: translateY(-2px);
                box-shadow: 0 5px 15px rgba(29, 185, 84, 0.3);
            }}
            
            .submit-btn:active {{
                transform: translateY(0);
            }}
            
            .submit-btn:disabled {{
                opacity: 0.7;
                cursor: not-allowed;
                transform: none;
            }}
            
            .loading-spinner {{
                display: inline-block;
                width: 18px;
                height: 18px;
                border: 2px solid rgba(255, 255, 255, 0.3);
                border-top: 2px solid white;
                border-radius: 50%;
                animation: spin 1s linear infinite;
            }}
            
            @keyframes spin {{
                0% {{ transform: rotate(0deg); }}
                100% {{ transform: rotate(360deg); }}
            }}
            
            .note {{
                margin-top: 15px;
                font-size: 12px;
                color: #888;
                text-align: center;
                line-height: 1.5;
            }}
            
            @media (max-width: 480px) {{
                .container {{
                    border-radius: 15px;
                }}
                
                .content {{
                    padding: 15px;
                }}
                
                .product-section {{
                    flex-direction: column;
                    text-align: center;
                }}
                
                .product-image-container {{
                    width: 100%;
                    height: 200px;
                    margin: 0 auto 15px;
                }}
                
                .address-row {{
                    flex-direction: column;
                    gap: 10px;
                }}
                
                .header h2 {{
                    font-size: 18px;
                }}
                
                .total-amount {{
                    font-size: 22px;
                }}
            }}
            
            .error-message {{
                color: #FF3B30;
                font-size: 12px;
                margin-top: 5px;
                display: none;
            }}
            
            .form-control.error + .error-message {{
                display: block;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h2>ĐẶT HÀNG - {current_fanpage_name}</h2>
            </div>
            
            <div class="content">
                <!-- Product Info Section -->
                <div class="product-section">
                    <div class="product-image-container" id="image-container">
                        {"<img id='product-image' src='" + default_image + "' class='product-image' onerror=\"this.onerror=null; this.src='https://via.placeholder.com/120x120?text=No+Image'\" />" if default_image else "<div class='placeholder-image'>Chưa có ảnh sản phẩm</div>"}
                    </div>
                    <div class="product-info">
                        <div class="product-code">Mã: {ms}</div>
                        <h3 class="product-title">{row.get('Ten','')}</h3>
                        <div class="product-price" id="price-display">{price_int:,.0f} đ</div>
                    </div>
                </div>

                <!-- Order Form -->
                <form id="orderForm">
                    <!-- Color Selection -->
                    <div class="form-group">
                        <label for="color">Màu sắc:</label>
                        <select id="color" class="form-control">
                            {''.join(f"<option value='{c}'>{c}</option>" for c in colors)}
                        </select>
                    </div>

                    <!-- Size Selection -->
                    <div class="form-group">
                        <label for="size">Size:</label>
                        <select id="size" class="form-control">
                            {''.join(f"<option value='{s}'>{s}</option>" for s in sizes)}
                        </select>
                    </div>

                    <!-- Quantity -->
                    <div class="form-group">
                        <label for="quantity">Số lượng:</label>
                        <input type="number" id="quantity" class="form-control" value="1" min="1">
                    </div>

                    <!-- Total Price -->
                    <div class="total-section">
                        <div class="total-label">Tạm tính:</div>
                        <div class="total-amount" id="total-display">{price_int:,.0f} đ</div>
                    </div>

                    <!-- Customer Information -->
                    <div class="form-group">
                        <label for="customerName">Họ và tên:</label>
                        <input type="text" id="customerName" class="form-control" required>
                    </div>

                    <div class="form-group">
                        <label for="phone">Số điện thoại:</label>
                        <input type="tel" id="phone" class="form-control" required>
                    </div>

                    <!-- Address Section với Open API -->
                    <div class="form-group">
                        <label>Địa chỉ nhận hàng:</label>
                        
                        <div class="address-row">
                            <div class="address-col">
                                <select id="province" class="form-control" 
                                        onchange="loadDistricts(this.value)">
                                    <option value="">Chọn Tỉnh/Thành phố</option>
                                </select>
                            </div>
                            <div class="address-col">
                                <select id="district" class="form-control" disabled
                                        onchange="loadWards(this.value)">
                                    <option value="">Chọn Quận/Huyện</option>
                                </select>
                            </div>
                            <div class="address-col">
                                <select id="ward" class="form-control" disabled>
                                    <option value="">Chọn Phường/Xã</option>
                                </select>
                            </div>
                        </div>
                        
                        <div class="form-group" style="margin-top: 10px;">
                            <input type="text" id="addressDetail" class="form-control" 
                                   placeholder="Số nhà, tên đường, tòa nhà..." required>
                        </div>
                        
                        <!-- Address Preview -->
                        <div id="addressPreview" class="address-preview"></div>
                        
                        <input type="hidden" id="fullAddress" name="fullAddress">
                        <input type="hidden" id="provinceName">
                        <input type="hidden" id="districtName">
                        <input type="hidden" id="wardName">
                    </div>

                    <!-- Submit Button -->
                    <button type="button" id="submitBtn" class="submit-btn" onclick="submitOrder()">
                        ĐẶT HÀNG NGAY
                    </button>

                    <p class="note">
                        Shop sẽ gọi xác nhận trong 5-10 phút. Thanh toán khi nhận hàng (COD).
                    </p>
                </form>
            </div>
        </div>

        <script>
            // Global variables
            const PRODUCT_MS = "{ms}";
            const PRODUCT_UID = "{uid}";
            const BASE_PRICE = {price_int};
            const DOMAIN = "{'https://' + DOMAIN if not DOMAIN.startswith('http') else DOMAIN}";
            const API_BASE_URL = "{('/api' if DOMAIN.startswith('http') else 'https://' + DOMAIN + '/api')}";
            
            // ============================================
            // PRODUCT VARIANT HANDLING
            // ============================================
            
            function formatPrice(n) {{
                return n.toLocaleString('vi-VN') + ' đ';
            }}
            
            async function updateImageByVariant() {{
                const color = document.getElementById('color').value;
                const size = document.getElementById('size').value;
                const imageContainer = document.getElementById('image-container');
                
                // Show loading
                const currentImg = imageContainer.querySelector('img');
                if (currentImg) {{
                    currentImg.classList.add('loading');
                }}
                
                try {{
                    const res = await fetch(`${{API_BASE_URL}}/get-variant-image?ms=${{PRODUCT_MS}}&color=${{encodeURIComponent(color)}}&size=${{encodeURIComponent(size)}}`);
                    if (res.ok) {{
                        const data = await res.json();
                        if (data.image && data.image.trim() !== '') {{
                            let imgElement = imageContainer.querySelector('img');
                            if (!imgElement) {{
                                imgElement = document.createElement('img');
                                imgElement.className = 'product-image';
                                imgElement.onerror = function() {{
                                    this.onerror = null;
                                    this.src = 'https://via.placeholder.com/120x120?text=No+Image';
                                }};
                                imageContainer.innerHTML = '';
                                imageContainer.appendChild(imgElement);
                            }}
                            imgElement.src = data.image;
                        }} else {{
                            imageContainer.innerHTML = '<div class="placeholder-image">Chưa có ảnh cho thuộc tính này</div>';
                        }}
                    }}
                }} catch (e) {{
                    console.error('Error updating image:', e);
                }} finally {{
                    if (currentImg) {{
                        setTimeout(() => currentImg.classList.remove('loading'), 300);
                    }}
                }}
            }}
            
            async function updatePriceByVariant() {{
                const color = document.getElementById('color').value;
                const size = document.getElementById('size').value;
                const quantity = parseInt(document.getElementById('quantity').value || '1');

                try {{
                    const res = await fetch(`${{API_BASE_URL}}/get-variant-price?ms=${{PRODUCT_MS}}&color=${{encodeURIComponent(color)}}&size=${{encodeURIComponent(size)}}`);
                    if (res.ok) {{
                        const data = await res.json();
                        const price = data.price || BASE_PRICE;

                        document.getElementById('price-display').innerText = formatPrice(price);
                        document.getElementById('total-display').innerText = formatPrice(price * quantity);
                    }}
                }} catch (e) {{
                    document.getElementById('price-display').innerText = formatPrice(BASE_PRICE);
                    document.getElementById('total-display').innerText = formatPrice(BASE_PRICE * quantity);
                }}
            }}
            
            async function updateVariantInfo() {{
                await Promise.all([
                    updateImageByVariant(),
                    updatePriceByVariant()
                ]);
            }}
            
            // ============================================
            // VIETNAM ADDRESS API (Open API - provinces.open-api.vn)
            // ============================================
            
            // Load provinces từ Open API
            async function loadProvinces() {{
                const provinceSelect = document.getElementById('province');
                
                try {{
                    // Show loading
                    provinceSelect.innerHTML = '<option value="">Đang tải tỉnh/thành...</option>';
                    provinceSelect.disabled = true;
                    
                    const response = await fetch('https://provinces.open-api.vn/api/p/');
                    const data = await response.json();
                    
                    // Sắp xếp provinces theo tên
                    const provinces = data.sort((a, b) => 
                        a.name.localeCompare(b.name, 'vi')
                    );
                    
                    provinceSelect.innerHTML = '<option value="">Chọn Tỉnh/Thành phố</option>';
                    provinces.forEach(province => {{
                        const option = document.createElement('option');
                        option.value = province.code;
                        option.textContent = province.name;
                        provinceSelect.appendChild(option);
                    }});
                    
                    console.log(`✅ Đã tải ${{provinces.length}} tỉnh/thành phố từ Open API`);
                    
                    // Load preset address từ URL nếu có
                    loadPresetAddress();
                }} catch (error) {{
                    console.error('❌ Lỗi khi load tỉnh/thành:', error);
                    // Fallback to static list
                    loadStaticProvinces();
                }} finally {{
                    provinceSelect.disabled = false;
                }}
            }}
            
            // Load districts dựa trên selected province
            async function loadDistricts(provinceId) {{
                const districtSelect = document.getElementById('district');
                const wardSelect = document.getElementById('ward');
                
                if (!provinceId) {{
                    districtSelect.innerHTML = '<option value="">Chọn Quận/Huyện</option>';
                    wardSelect.innerHTML = '<option value="">Chọn Phường/Xã</option>';
                    districtSelect.disabled = true;
                    wardSelect.disabled = true;
                    updateFullAddress();
                    return;
                }}
                
                try {{
                    districtSelect.innerHTML = '<option value="">Đang tải quận/huyện...</option>';
                    districtSelect.disabled = true;
                    wardSelect.disabled = true;
                    
                    const response = await fetch(`https://provinces.open-api.vn/api/p/${{provinceId}}?depth=2`);
                    const provinceData = await response.json();
                    
                    const districts = provinceData.districts || [];
                    districts.sort((a, b) => a.name.localeCompare(b.name, 'vi'));
                    
                    districtSelect.innerHTML = '<option value="">Chọn Quận/Huyện</option>';
                    districts.forEach(district => {{
                        const option = document.createElement('option');
                        option.value = district.code;
                        option.textContent = district.name;
                        districtSelect.appendChild(option);
                    }});
                    
                    console.log(`✅ Đã tải ${{districts.length}} quận/huyện`);
                    districtSelect.disabled = false;
                    
                    // Clear wards
                    wardSelect.innerHTML = '<option value="">Chọn Phường/Xã</option>';
                    wardSelect.disabled = true;
                }} catch (error) {{
                    console.error('❌ Lỗi khi load quận/huyện:', error);
                    districtSelect.innerHTML = '<option value="">Lỗi tải dữ liệu</option>';
                }} finally {{
                    updateFullAddress();
                }}
            }}
            
            // Load wards dựa trên selected district
            async function loadWards(districtId) {{
                const wardSelect = document.getElementById('ward');
                
                if (!districtId) {{
                    wardSelect.innerHTML = '<option value="">Chọn Phường/Xã</option>';
                    wardSelect.disabled = true;
                    updateFullAddress();
                    return;
                }}
                
                try {{
                    wardSelect.innerHTML = '<option value="">Đang tải phường/xã...</option>';
                    wardSelect.disabled = true;
                    
                    const response = await fetch(`https://provinces.open-api.vn/api/d/${{districtId}}?depth=2`);
                    const districtData = await response.json();
                    
                    const wards = districtData.wards || [];
                    wards.sort((a, b) => a.name.localeCompare(b.name, 'vi'));
                    
                    wardSelect.innerHTML = '<option value="">Chọn Phường/Xã</option>';
                    wards.forEach(ward => {{
                        const option = document.createElement('option');
                        option.value = ward.code;
                        option.textContent = ward.name;
                        wardSelect.appendChild(option);
                    }});
                    
                    console.log(`✅ Đã tải ${{wards.length}} phường/xã`);
                    wardSelect.disabled = false;
                }} catch (error) {{
                    console.error('❌ Lỗi khi load phường/xã:', error);
                    wardSelect.innerHTML = '<option value="">Lỗi tải dữ liệu</option>';
                }} finally {{
                    updateFullAddress();
                }}
            }}
            
            // Fallback: Static province list
            function loadStaticProvinces() {{
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
                provinceSelect.innerHTML = '<option value="">Chọn Tỉnh/Thành phố</option>';
                
                staticProvinces.forEach((province, index) => {{
                    const option = document.createElement('option');
                    option.value = index + 1;
                    option.textContent = province;
                    provinceSelect.appendChild(option);
                }});
                
                provinceSelect.disabled = false;
                console.log('⚠️ Đã tải danh sách tỉnh thành tĩnh (fallback)');
            }}
            
            // Update full address từ tất cả các components
            function updateFullAddress() {{
                const provinceText = document.getElementById('province').options[document.getElementById('province').selectedIndex]?.text || '';
                const districtText = document.getElementById('district').options[document.getElementById('district').selectedIndex]?.text || '';
                const wardText = document.getElementById('ward').options[document.getElementById('ward').selectedIndex]?.text || '';
                const detailText = document.getElementById('addressDetail').value || '';
                
                // Save to hidden fields
                document.getElementById('provinceName').value = provinceText;
                document.getElementById('districtName').value = districtText;
                document.getElementById('wardName').value = wardText;
                
                // Build full address
                const fullAddress = [detailText, wardText, districtText, provinceText]
                    .filter(part => part.trim() !== '')
                    .join(', ');
                
                document.getElementById('fullAddress').value = fullAddress;
                
                // Update preview
                const previewElement = document.getElementById('addressPreview');
                if (fullAddress.trim()) {{
                    previewElement.innerHTML = `
                        <div class="address-preview-content">
                            <strong>Địa chỉ nhận hàng:</strong>
                            <p>${{fullAddress}}</p>
                        </div>
                    `;
                    previewElement.style.display = 'block';
                }} else {{
                    previewElement.style.display = 'none';
                }}
                
                return fullAddress;
            }}
            
            // Load preset address từ URL parameters
            function loadPresetAddress() {{
                const urlParams = new URLSearchParams(window.location.search);
                const presetAddress = urlParams.get('address');
                
                if (presetAddress) {{
                    document.getElementById('addressDetail').value = presetAddress;
                    updateFullAddress();
                }}
            }}
            
            // ============================================
            // FORM VALIDATION AND SUBMISSION
            // ============================================
            
            async function submitOrder() {{
                // Collect form data
                const formData = {{
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
                }};
                
                // Validate required fields
                if (!formData.customerName) {{
                    alert('Vui lòng nhập họ và tên');
                    document.getElementById('customerName').focus();
                    return;
                }}
                
                if (!formData.phone) {{
                    alert('Vui lòng nhập số điện thoại');
                    document.getElementById('phone').focus();
                    return;
                }}
                
                // Validate phone number
                const phoneRegex = /^(0|\+84)(\d{9,10})$/;
                if (!phoneRegex.test(formData.phone)) {{
                    alert('Số điện thoại không hợp lệ. Vui lòng nhập số điện thoại 10-11 chữ số');
                    document.getElementById('phone').focus();
                    return;
                }}
                
                // Validate address
                if (!formData.provinceId) {{
                    alert('Vui lòng chọn Tỉnh/Thành phố');
                    document.getElementById('province').focus();
                    return;
                }}
                
                if (!formData.districtId) {{
                    alert('Vui lòng chọn Quận/Huyện');
                    document.getElementById('district').focus();
                    return;
                }}
                
                if (!formData.wardId) {{
                    alert('Vui lòng chọn Phường/Xã');
                    document.getElementById('ward').focus();
                    return;
                }}
                
                if (!formData.addressDetail) {{
                    alert('Vui lòng nhập địa chỉ chi tiết (số nhà, tên đường)');
                    document.getElementById('addressDetail').focus();
                    return;
                }}
                
                // Show loading
                const submitBtn = document.getElementById('submitBtn');
                const originalText = submitBtn.innerHTML;
                submitBtn.innerHTML = '<span class="loading-spinner"></span> ĐANG XỬ LÝ...';
                submitBtn.disabled = true;
                
                try {{
                    const response = await fetch(`${{API_BASE_URL}}/submit-order`, {{
                        method: 'POST',
                        headers: {{
                            'Content-Type': 'application/json'
                        }},
                        body: JSON.stringify(formData)
                    }});
                    
                    const data = await response.json();
                    
                    if (response.ok) {{
                        // Success
                        alert('🎉 Đã gửi đơn hàng thành công!\\n\\nShop sẽ liên hệ xác nhận trong 5-10 phút.\\nCảm ơn anh/chị đã đặt hàng! ❤️');
                        
                        // Reset form (optional)
                        document.getElementById('customerName').value = '';
                        document.getElementById('phone').value = '';
                        document.getElementById('addressDetail').value = '';
                        document.getElementById('province').selectedIndex = 0;
                        document.getElementById('district').innerHTML = '<option value="">Chọn Quận/Huyện</option>';
                        document.getElementById('ward').innerHTML = '<option value="">Chọn Phường/Xã</option>';
                        document.getElementById('district').disabled = true;
                        document.getElementById('ward').disabled = true;
                        updateFullAddress();
                        
                    }} else {{
                        // Error
                        alert(`❌ ${{data.message || 'Có lỗi xảy ra. Vui lòng thử lại sau'}}`);
                    }}
                }} catch (error) {{
                    console.error('Lỗi khi gửi đơn hàng:', error);
                    alert('❌ Lỗi kết nối. Vui lòng thử lại sau!');
                }} finally {{
                    // Restore button
                    submitBtn.innerHTML = originalText;
                    submitBtn.disabled = false;
                }}
            }}
            
            // ============================================
            // INITIALIZATION
            // ============================================
            
            document.addEventListener('DOMContentLoaded', function() {{
                // Load provinces
                loadProvinces();
                
                // Event listeners for product variant changes
                document.getElementById('color').addEventListener('change', updateVariantInfo);
                document.getElementById('size').addEventListener('change', updateVariantInfo);
                document.getElementById('quantity').addEventListener('input', updatePriceByVariant);
                
                // Event listeners for address changes
                document.getElementById('province').addEventListener('change', function() {{
                    loadDistricts(this.value);
                    updateFullAddress();
                }});
                
                document.getElementById('district').addEventListener('change', function() {{
                    loadWards(this.value);
                    updateFullAddress();
                }});
                
                document.getElementById('ward').addEventListener('change', updateFullAddress);
                document.getElementById('addressDetail').addEventListener('input', updateFullAddress);
                
                // Initialize product variant info
                updateVariantInfo();
                
                // Enter key to submit form
                document.getElementById('orderForm').addEventListener('keypress', function(e) {{
                    if (e.which === 13) {{
                        e.preventDefault();
                        submitOrder();
                    }}
                }});
                
                // Focus on first field
                setTimeout(() => {{
                    document.getElementById('customerName').focus();
                }}, 500);
            }});
        </script>
    </body>
    </html>
    """
    return html

# ============================================
# API ENDPOINTS
# ============================================

@app.route("/api/get-product")
def api_get_product():
    load_products()
    ms = (request.args.get("ms") or "").upper()
    if ms not in PRODUCTS:
        return {"error": "not_found"}, 404

    row = PRODUCTS[ms]
    images_field = row.get("Images", "")
    urls = parse_image_urls(images_field)
    image = urls[0] if urls else ""

    size_field = row.get("size (Thuộc tính)", "")
    color_field = row.get("màu (Thuộc tính)", "")

    sizes = []
    if size_field:
        sizes = [s.strip() for s in size_field.split(",") if s.strip()]

    colors = []
    if color_field:
        colors = [c.strip() for c in color_field.split(",") if c.strip()]

    if not sizes:
        sizes = ["Mặc định"]
    if not colors:
        colors = ["Mặc định"]

    price_str = row.get("Gia", "0")
    price_int = extract_price_int(price_str) or 0

    return {
        "ms": ms,
        "name": row.get("Ten", ""),
        "image": image,
        "sizes": sizes,
        "colors": colors,
        "price": price_int,
        "price_display": f"{price_int:,.0f} đ",
    }

@app.route("/api/get-variant-price")
def api_get_variant_price():
    ms = (request.args.get("ms") or "").upper()
    color = (request.args.get("color") or "").strip()
    size = (request.args.get("size") or "").strip()

    load_products()
    if ms not in PRODUCTS:
        return {"error": "not_found"}, 404

    product = PRODUCTS[ms]
    variants = product.get("variants") or []

    chosen = None
    for v in variants:
        vm = (v.get("mau") or "").strip().lower()
        vs = (v.get("size") or "").strip().lower()
        want_color = color.strip().lower()
        want_size = size.strip().lower()

        if want_color and vm != want_color:
            continue
        if want_size and vs != want_size:
            continue
        chosen = v
        break

    if not chosen and variants:
        chosen = variants[0]

    price = 0
    price_display = product.get("Gia", "0")

    if chosen:
        if chosen.get("gia") is not None:
            price = chosen["gia"]
            price_display = chosen.get("gia_raw") or price_display
        else:
            p_int = extract_price_int(chosen.get("gia_raw"))
            if p_int is not None:
                price = p_int
                price_display = chosen.get("gia_raw") or price_display
            else:
                p_int = extract_price_int(product.get("Gia", "0"))
                price = p_int or 0
    else:
        p_int = extract_price_int(product.get("Gia", "0"))
        price = p_int or 0

    return {
        "ms": ms,
        "color": color,
        "size": size,
        "price": int(price),
        "price_display": price_display,
    }

@app.route("/api/get-variant-image")
def api_get_variant_image():
    """API trả về ảnh tương ứng với màu và size"""
    ms = (request.args.get("ms") or "").upper()
    color = request.args.get("color", "").strip()
    size = request.args.get("size", "").strip()
    
    load_products()
    if ms not in PRODUCTS:
        return {"error": "not_found"}, 404
    
    variant_image = get_variant_image(ms, color, size)
    
    return {
        "ms": ms,
        "color": color,
        "size": size,
        "image": variant_image
    }

@app.route("/api/submit-order", methods=["POST"])
def api_submit_order():
    data = request.get_json() or {}
    ms = (data.get("ms") or "").upper()
    uid = data.get("uid") or ""
    color = data.get("color") or ""
    size = data.get("size") or ""
    quantity = int(data.get("quantity") or 1)
    customer_name = data.get("customerName") or ""
    phone = data.get("phone") or ""
    address = data.get("address") or ""
    
    # Thêm các trường mới từ form địa chỉ
    province_name = data.get("provinceName", "")
    district_name = data.get("districtName", "")
    ward_name = data.get("wardName", "")
    address_detail = data.get("addressDetail", "")
    
    load_products()
    row = PRODUCTS.get(ms)
    if not row:
        return {"error": "not_found", "message": "Sản phẩm không tồn tại"}, 404

    price_str = row.get("Gia", "0")
    price_int = extract_price_int(price_str) or 0
    total = price_int * quantity
    
    product_name = row.get('Ten', '')

    if uid:
        # Lấy referral source từ context
        ctx = USER_CONTEXT.get(uid, {})
        referral_source = ctx.get("referral_source", "direct")
        
        # Tin nhắn chi tiết hơn với thông tin địa chỉ đầy đủ
        msg = (
            "🎉 Shop đã nhận được đơn hàng mới:\n"
            f"🛍 Sản phẩm: [{ms}] {product_name}\n"
            f"🎨 Phân loại: {color} / {size}\n"
            f"📦 Số lượng: {quantity}\n"
            f"💰 Thành tiền: {total:,.0f} đ\n"
            f"👤 Người nhận: {customer_name}\n"
            f"📱 SĐT: {phone}\n"
            f"🏠 Địa chỉ: {address}\n"
            f"📍 Chi tiết: {address_detail}\n"
            f"🗺️ Khu vực: {ward_name}, {district_name}, {province_name}\n"
            "────────────────────\n"
            "⏰ Shop sẽ gọi điện xác nhận trong 5-10 phút.\n"
            "🚚 Đơn hàng sẽ được giao bởi ViettelPost\n"
            "💳 Thanh toán khi nhận hàng (COD)\n"
            "────────────────────\n"
            "Cảm ơn anh/chị đã đặt hàng! ❤️"
        )
        send_message(uid, msg)
    
    # ============================================
    # GHI ĐƠN HÀNG VÀO GOOGLE SHEET QUA API
    # ============================================
    order_data = {
        "ms": ms,
        "uid": uid,
        "color": color,
        "size": size,
        "quantity": quantity,
        "customer_name": customer_name,
        "phone": phone,
        "address": address,
        "province": province_name,
        "district": district_name,
        "ward": ward_name,
        "address_detail": address_detail,
        "product_name": product_name,
        "unit_price": price_int,
        "total_price": total,
        "referral_source": ctx.get("referral_source", "direct")
    }
    
    # Ưu tiên 1: Ghi vào Google Sheet qua API
    write_success = write_order_to_google_sheet_api(order_data)
    
    # Fallback: Nếu không thành công, lưu vào file local backup
    if not write_success:
        print("⚠️ Ghi Google Sheet thất bại, thực hiện lưu vào file local backup...")
        save_order_to_local_csv(order_data)
    
    # Gửi notification đến Fchat webhook (nếu có)
    if FCHAT_WEBHOOK_URL and FCHAT_TOKEN:
        try:
            fchat_payload = {
                "token": FCHAT_TOKEN,
                "message": f"🛒 ĐƠN HÀNG MỚI\nMã: {ms}\nKH: {customer_name}\nSĐT: {phone}\nTổng: {total:,.0f}đ",
                "metadata": {
                    "order_data": order_data,
                    "timestamp": datetime.now().isoformat()
                }
            }
            requests.post(FCHAT_WEBHOOK_URL, json=fchat_payload, timeout=5)
        except Exception as e:
            print(f"⚠️ Không thể gửi notification đến Fchat: {str(e)}")

    return {
        "status": "ok", 
        "message": "Đơn hàng đã được tiếp nhận",
        "order_written": write_success,
        "order_details": {
            "order_id": f"ORD{int(time.time())}_{uid[-4:] if uid else '0000'}",
            "product_code": ms,
            "product_name": product_name,
            "customer_name": customer_name,
            "phone": phone,
            "address": address,
            "province": province_name,
            "district": district_name,
            "ward": ward_name,
            "total": total,
            "timestamp": datetime.now().isoformat()
        }
    }

@app.route("/static/<path:path>")
def static_files(path):
    return send_from_directory("static", path)

# ============================================
# HEALTH CHECK
# ============================================

@app.route("/health", methods=["GET"])
def health_check():
    """Kiểm tra tình trạng server và bot"""
    current_fanpage_name = get_fanpage_name_from_api()
    
    # Tính tổng số variants và variants có ảnh
    total_variants = 0
    variants_with_images = 0
    
    for ms, product in PRODUCTS.items():
        variants = product.get("variants", [])
        total_variants += len(variants)
        for variant in variants:
            if variant.get("variant_image"):
                variants_with_images += 1
    
    # Kiểm tra Google Sheets Service
    sheets_service_status = "Not Configured"
    if GOOGLE_SHEET_ID and GOOGLE_SHEETS_CREDENTIALS_JSON:
        try:
            service = get_google_sheets_service()
            if service:
                # Thử một thao tác đọc nhẹ để kiểm tra quyền
                result = service.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()
                sheet_title = result.get('properties', {}).get('title', 'Unknown')
                sheets_service_status = f"Connected to Sheet: '{sheet_title}' (ID: {GOOGLE_SHEET_ID[:10]}...)"
            else:
                sheets_service_status = "Service Initialization Failed"
        except Exception as e:
            sheets_service_status = f"Connection Error: {type(e).__name__}"
    
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "products_loaded": len(PRODUCTS),
        "variants_loaded": total_variants,
        "variants_with_images": variants_with_images,
        "variant_images_percentage": f"{(variants_with_images/total_variants*100):.1f}%" if total_variants > 0 else "0%",
        "last_load_time": LAST_LOAD,
        "openai_configured": bool(client),
        "openai_vision_available": bool(client and OPENAI_API_KEY),
        "facebook_configured": bool(PAGE_ACCESS_TOKEN),
        "fanpage_name": current_fanpage_name,
        "google_sheets_integration": {
            "method": "Official Google Sheets API v4",
            "sheet_id_configured": bool(GOOGLE_SHEET_ID),
            "credentials_configured": bool(GOOGLE_SHEETS_CREDENTIALS_JSON),
            "service_status": sheets_service_status,
            "order_write_logic": "Primary API -> Local CSV Backup"
        },
        "fchat_webhook": "Configured" if FCHAT_WEBHOOK_URL and FCHAT_TOKEN else "Not configured",
        "fanpage_name_source": "Facebook Graph API" if FANPAGE_NAME_CACHE and FANPAGE_NAME_CACHE != FANPAGE_NAME else "Environment Variable",
        "fanpage_cache_age": int(time.time() - FANPAGE_NAME_CACHE_TIME) if FANPAGE_NAME_CACHE_TIME else 0,
        "fanpage_cache_valid": (FANPAGE_NAME_CACHE_TIME and (time.time() - FANPAGE_NAME_CACHE_TIME) < FANPAGE_NAME_CACHE_TTL),
        "variant_image_support": "ENABLED (ảnh theo thuộc tính)",
        "variant_image_api": "/api/get-variant-image",
        "image_processing": "base64+fallback",
        "image_debounce_enabled": True,
        "image_carousel": "5_products",
        "search_algorithm": "TF-IDF_cosine_similarity",
        "accuracy_improved": True,
        "fchat_echo_processing": True,
        "bot_echo_filter": True,
        "catalog_support": "Enabled (retailer_id extraction)",
        "catalog_retailer_id_extraction": "MSxxxxxx_xx -> MSxxxxxx",
        "ads_referral_processing": "ENABLED (trích xuất mã từ ad_title)",
        "ads_context_handling": "ENABLED (không reset context khi có sản phẩm từ ADS)",
        "referral_auto_processing": True,
        "message_debounce_enabled": True,
        "duplicate_protection": True,
        "image_send_debounce": "5s",
        "image_request_processing": "Enabled with confidence > 0.85",
        "address_form": "Open API - provinces.open-api.vn (dropdown 3 cấp)",
        "address_validation": "enabled",
        "phone_validation": "regex validation",
        "order_response_mode": "SHORT - Chỉ báo còn hàng khi hỏi tồn kho",
        "price_detailed_response": "ENABLED (hiển thị chi tiết các biến thể giá)",
        "max_gpt_tokens": 150,
        "stock_assumption": "Chỉ báo khi hỏi tồn kho",
        "order_keywords_priority": "HIGH",
        "context_tracking": "ENABLED (tracks last_ms and product_history)",
        "facebook_shop_guidance": "ENABLED (hướng dẫn vào gian hàng khi yêu cầu sản phẩm khác)",
        "openai_function_calling": "ENABLED (tích hợp từ ai_studio_code.py)",
        "tools_available": [
            "get_product_info",
            "send_product_images", 
            "provide_order_link",
            "show_featured_carousel"
        ],
        "function_calling_model": "gpt-4o-mini",
        "system_prompt_optimized": "True",
        "conversation_history_tracking": "ENABLED (10 messages)",
        "first_message_carousel_feature": "ENABLED (gửi carousel 1 sản phẩm cho tin nhắn đầu tiên sau referral)",
        "carousel_trigger_sources": ["ADS (ad_title)", "Catalog (retailer_id)", "Fchat echo"],
        "carousel_buttons": "3 nút: 🛒 Đặt ngay, 🔍 Xem chi tiết, 🖼️ Xem ảnh",
        "first_message_processing": "Carousel 1 sản phẩm → Từ tin nhắn thứ 2: Function Calling",
        "postback_double_processing_fix": "ENABLED (idempotency key + 30s memory + strict duplicate detection)",
        "product_info_debounce": "15s cho cùng sản phẩm, 5s cho bất kỳ sản phẩm",
        "lock_recovery_mechanism": "ENABLED (auto release sau 15s)",
        "idempotency_mechanism": "ENABLED (30s idempotency for postbacks)",
        "worker_mode": "SINGLE WORKER (optimized for Koyeb 1-worker deployment)"
    }, 200

# ============================================
# DEBUG LOCKS ENDPOINT
# ============================================

@app.route("/debug/locks", methods=["GET"])
def debug_locks():
    """Debug locks để kiểm tra deadlock"""
    now = time.time()
    locked_users = []
    
    for uid, ctx in USER_CONTEXT.items():
        if ctx.get("processing_lock"):
            lock_time = ctx.get("processing_lock_time", 0)
            lock_age = now - lock_time
            if lock_age > 5:  # Lock quá 5 giây
                locked_users.append({
                    "uid": uid,
                    "lock_age": lock_age,
                    "last_ms": ctx.get("last_ms"),
                    "last_activity": ctx.get("last_msg_time", 0),
                    "idempotent_postbacks_count": len(ctx.get("idempotent_postbacks", {}))
                })
    
    return jsonify({
        "total_users": len(USER_CONTEXT),
        "locked_users": len(locked_users),
        "locked_details": locked_users,
        "in_memory_locks": len(POSTBACK_LOCKS),
        "timestamp": now
    }), 200

# ============================================
# MAIN - ĐÃ CẬP NHẬT CHO 1 WORKER KOYEB
# ============================================

if __name__ == "__main__":
    import os
    import multiprocessing
    
    print("=" * 80)
    print("🟢 KHỞI ĐỘNG FACEBOOK CHATBOT - SINGLE WORKER MODE")
    print("=" * 80)
    print(f"🟢 Process ID: {os.getpid()}")
    print(f"🟢 Parent Process ID: {os.getppid()}")
    print(f"🟢 CPU Count: {multiprocessing.cpu_count()}")
    print(f"🟢 Worker Mode: SINGLE (optimized for Koyeb)")
    print(f"🟢 Duplicate Protection: IDEMPOTENCY KEY + 30s MEMORY")
    print(f"🟢 Postback Processing: STRICT (each postback processed once)")
    print("=" * 80)
    
    print(f"🟢 GPT-4o Vision API: {'SẴN SÀNG' if client and OPENAI_API_KEY else 'CHƯA CẤU HÌNH'}")
    print(f"🟢 Fanpage: {get_fanpage_name_from_api()}")
    print(f"🟢 Domain: {DOMAIN}")
    print(f"🟢 Google Sheets API: {'SẴN SÀNG' if GOOGLE_SHEET_ID and GOOGLE_SHEETS_CREDENTIALS_JSON else 'CHƯA CẤU HÌNH'}")
    print(f"🟢 Sheet ID: {GOOGLE_SHEET_ID[:20]}..." if GOOGLE_SHEET_ID else "🟡 Chưa cấu hình")
    print(f"🟢 OpenAI Function Calling: {'TÍCH HỢP THÀNH CÔNG' if client else 'CHƯA CẤU HÌNH'}")
    print(f"🟢 Tools Available: get_product_info, send_product_images, provide_order_link, show_featured_carousel")
    print(f"🟢 Image Processing: Base64 + Fallback URL")
    print(f"🟢 Search Algorithm: TF-IDF + Cosine Similarity")
    print(f"🟢 Image Carousel: 5 sản phẩm phù hợp nhất")
    print(f"🟢 Address Form: Open API - provinces.open-api.vn (dropdown 3 cấp)")
    print(f"🟢 Address Validation: BẬT")
    print(f"🟢 Phone Validation: BẬT (regex)")
    print(f"🟢 Image Debounce: 3 giây")
    print(f"🟢 Text Message Debounce: 2 giây (tăng từ 1s)")
    print(f"🟢 Echo Message Debounce: 2 giây")
    print(f"🟢 Bot Echo Filter: BẬT (phân biệt echo từ bot vs Fchat)")
    print(f"🟢 Fchat Echo Processing: BẬT (giữ nguyên logic trích xuất mã từ Fchat)")
    print(f"🟢 Catalog Support: BẬT (trích xuất retailer_id từ catalog)")
    print(f"🟢 Retailer ID Extraction: MSxxxxxx_xx → MSxxxxxx")
    print(f"🟢 ADS Referral Processing: BẬT (trích xuất mã từ ad_title)")
    print(f"🟢 ADS Context: KHÔNG reset khi đã xác định được sản phẩm")
    print(f"🟢 Referral Auto Processing: BẬT")
    print(f"🟢 Duplicate Message Protection: BẬT (30s)")
    print(f"🟢 Image Send Debounce: 5 giây")
    print(f"🟢 Max Images per Product: 20 ảnh")
    print(f"🟢 Catalog Context: Lưu retailer_id và tự động nhận diện sản phẩm")
    print(f"🟢 Fanpage Name Source: Facebook Graph API (cache 1h)")
    print(f"🟢 Variant Image Support: BẬT (ảnh theo từng thuộc tính)")
    print(f"🟢 Variant Image API: /api/get-variant-image")
    print(f"🟢 Form Dynamic Images: BẬT (ảnh thay đổi theo màu/size)")
    print(f"🟢 Catalog Follow-up Processing: BẬT (30 giây sau khi xem catalog)")
    print(f"🟢 ADS Follow-up Processing: BẬT (xử lý tin nhắn sau click quảng cáo)")
    print(f"🟢 Order Backup System: Local CSV khi Google Sheet không kết nối được")
    print(f"🟢 Context Tracking: BẬT (ghi nhớ last_ms và product_history)")
    print(f"🟢 Facebook Shop Guidance: BẬT (hướng dẫn vào gian hàng)")
    print(f"🟢 Price Detailed Response: BẬT (hiển thị chi tiết các biến thể giá)")
    print("=" * 80)
    print("🔴 QUAN TRỌNG: FIX CHO LỖI DUPLICATE POSTBACK")
    print("=" * 80)
    print(f"🔴 BOT ƯU TIÊN CONTEXT HIỆN TẠI")
    print(f"🔴 BOT CHỈ BÁO CÒN HÀNG KHI KHÁCH HỎI VỀ TỒN KHO")
    print(f"🔴 GPT Reply Mode: FUNCTION CALLING (gpt-4o-mini) với CONTEXT PRIORITY")
    print(f"🔴 FIRST MESSAGE: CAROUSEL 1 SẢN PHẨM (không dùng function calling)")
    print(f"🔴 FROM SECOND MESSAGE: FUNCTION CALLING với CONTEXT PRIORITY")
    print(f"🔴 Order Priority: ƯU TIÊN GỬI LINK KHI CÓ TỪ KHÓA ĐẶT HÀNG")
    print(f"🔴 Price Priority: HIỂN THỊ CHI TIẾT KHI KHÁCH HỎI VỀ GIÁ")
    print(f"🔴 Function Calling Integration: HOÀN THÀNH")
    print(f"🔴 POSTBACK FIX: IDEMPOTENCY KEY + 30s MEMORY (sửa vấn đề duplicate)")
    print(f"🔴 Product Info Debounce: 15s cho cùng sản phẩm, 5s cho bất kỳ sản phẩm")
    print(f"🔴 Lock Recovery Mechanism: TỰ ĐỘNG release sau 15s")
    print(f"🔴 Postback Idempotency: MỖI POSTBACK CHỈ XỬ LÝ 1 LẦN DUY NHẤT")
    print(f"🔴 Debug Endpoint: /debug/locks (kiểm tra deadlock)")
    print(f"🔴 Health Check: /health (kiểm tra tình trạng server)")
    print(f"🔴 MÔ TẢ SẢN PHẨM MỚI: 5 gạch đầu dòng")
    print(f"🔴 PHÂN TÍCH GIÁ THÔNG MINH: Theo màu/Size/Nhóm giá")
    print(f"🔴 ẢNH SẢN PHẨM: 5 ảnh không trùng, gửi tuần tự")
    print("=" * 80)
    print("🚀 Starting app on http://0.0.0.0:5000")
    print("=" * 80)
    
    # Load products ngay khi khởi động
    load_products()
    
    app.run(host="0.0.0.0", port=5000, debug=False)
