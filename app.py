import os
import json
import re
import time
import csv
from collections import defaultdict
from datetime import datetime

import requests
from flask import Flask, request, send_from_directory
from openai import OpenAI

# ============================================
# FLASK APP & CONFIG
# ============================================
app = Flask(__name__)

PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN", "").strip()
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
GOOGLE_SHEET_CSV_URL = os.getenv("SHEET_CSV_URL", "").strip() or "https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv"
DOMAIN = os.getenv("DOMAIN", "").strip() or "fb-gpt-chatbot.onrender.com"
FANPAGE_NAME = os.getenv("FANPAGE_NAME", "Shop thời trang")

# ============================================
# OPENAI CLIENT
# ============================================
client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ============================================
# GLOBAL STATE
# ============================================
USER_CONTEXT = defaultdict(lambda: {
    "last_msg_time": 0,
    "current_ms": None,
    "processing_lock": False,
    "product_history": [],
    "conversation_history": [],
    "referral_source": None,
    "referral_payload": None,
})
PRODUCTS = {}
LAST_LOAD = 0
LOAD_TTL = 300

# ============================================
# HELPER: SEND MESSAGE
# ============================================

def call_facebook_send_api(payload: dict, retry_count=2):
    """Gửi tin nhắn qua Facebook API"""
    if not PAGE_ACCESS_TOKEN:
        print("[WARN] PAGE_ACCESS_TOKEN chưa được cấu hình")
        return {}
    
    url = f"https://graph.facebook.com/v12.0/me/messages?access_token={PAGE_ACCESS_TOKEN}"
    
    for attempt in range(retry_count):
        try:
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code == 200:
                return resp.json()
            else:
                print(f"Facebook API error (attempt {attempt+1}):", resp.text)
                if attempt < retry_count - 1:
                    time.sleep(0.5)
        except Exception as e:
            print(f"Facebook API exception (attempt {attempt+1}):", e)
            if attempt < retry_count - 1:
                time.sleep(0.5)
    return {}


def send_message(recipient_id: str, text: str):
    if not text:
        return
    if len(text) > 2000:
        text = text[:1997] + "..."
    
    payload = {
        "recipient": {"id": recipient_id},
        "message": {"text": text},
    }
    return call_facebook_send_api(payload)


def send_carousel_template(recipient_id: str, elements: list):
    if not elements:
        return ""
    payload = {
        "recipient": {"id": recipient_id},
        "message": {
            "attachment": {
                "type": "template",
                "payload": {
                    "template_type": "generic",
                    "elements": elements[:10],
                },
            }
        },
    }
    return call_facebook_send_api(payload)


# ============================================
# HELPER: PRODUCTS LOADING
# ============================================

def extract_price_int(price_str: str):
    if not price_str:
        return None
    m = re.search(r"(\d[\d.,]*)", str(price_str))
    if not m:
        return None
    cleaned = m.group(1).replace(".", "").replace(",", "")
    try:
        return int(cleaned)
    except Exception:
        return None


def load_products(force=False):
    """
    Đọc dữ liệu từ Google Sheet CSV
    """
    global PRODUCTS, LAST_LOAD
    now = time.time()
    if not force and PRODUCTS and (now - LAST_LOAD) < LOAD_TTL:
        return

    if not GOOGLE_SHEET_CSV_URL:
        print("❌ GOOGLE_SHEET_CSV_URL chưa được cấu hình!")
        return

    try:
        print(f"🟦 Loading sheet: {GOOGLE_SHEET_CSV_URL}")
        r = requests.get(GOOGLE_SHEET_CSV_URL, timeout=20)
        r.raise_for_status()
        r.encoding = "utf-8"
        content = r.text

        reader = csv.DictReader(content.splitlines())
        products = {}

        for raw_row in reader:
            row = dict(raw_row)

            ms = (row.get("Mã sản phẩm") or "").strip()
            if not ms:
                continue

            ten = (row.get("Tên sản phẩm") or "").strip()
            gia_raw = (row.get("Giá bán") or "").strip()
            tonkho_raw = (row.get("Tồn kho") or row.get("Có thể bán") or "").strip()
            mota = (row.get("Mô tả") or "").strip()
            
            # Thuộc tính: màu, size
            mau = (row.get("màu (Thuộc tính)") or row.get("Thuộc tính") or "").strip()
            size = (row.get("size (Thuộc tính)") or "").strip()
            
            # Nếu cột Thuộc tính chứa cả màu và size
            if not mau and not size and row.get("Thuộc tính"):
                thuoc_tinh = row.get("Thuộc tính", "").lower()
                if 'màu' in thuoc_tinh or 'color' in thuoc_tinh:
                    mau = row.get("Thuộc tính", "")
                elif 'size' in thuoc_tinh:
                    size = row.get("Thuộc tính", "")

            gia_int = extract_price_int(gia_raw)
            tonkho = tonkho_raw if tonkho_raw else "Còn hàng"

            products[ms] = {
                "MS": ms,
                "Ten": ten,
                "Gia": gia_raw,
                "GiaInt": gia_int,
                "TonKho": tonkho,
                "MoTa": mota,
                "Mau": mau,
                "Size": size,
                "FullRow": row,  # Lưu toàn bộ row để GPT có thể truy cập mọi field
            }

        PRODUCTS = products
        LAST_LOAD = now
        print(f"📦 Loaded {len(PRODUCTS)} products")
        
    except Exception as e:
        print("❌ load_products ERROR:", e)


# ============================================
# GPT PROMPT ENGINEERING
# ============================================

def build_comprehensive_product_context(ms: str) -> str:
    """
    Xây dựng context đầy đủ về sản phẩm để cung cấp cho GPT
    """
    if not ms or ms not in PRODUCTS:
        return "KHÔNG CÓ THÔNG TIN SẢN PHẨM"
    
    product = PRODUCTS[ms]
    
    # Tách các phần từ mô tả
    mota = product.get("MoTa", "")
    
    # Tìm thông tin chính sách trong mô tả
    shipping_info = ""
    warranty_info = ""
    return_info = ""
    payment_info = ""
    
    # Phân tích mô tả để tìm thông tin
    lines = mota.split('\n')
    current_section = ""
    
    for line in lines:
        line_lower = line.lower()
        
        if any(keyword in line_lower for keyword in ['ship', 'vận chuyển', 'giao hàng', 'phí ship']):
            shipping_info += line + " "
        elif any(keyword in line_lower for keyword in ['bảo hành', 'warranty', 'đảm bảo']):
            warranty_info += line + " "
        elif any(keyword in line_lower for keyword in ['đổi trả', 'hoàn tiền', 'trả hàng']):
            return_info += line + " "
        elif any(keyword in line_lower for keyword in ['thanh toán', 'payment', 'cod', 'chuyển khoản']):
            payment_info += line + " "
    
    context = f"""
=== THÔNG TIN SẢN PHẨM [{ms}] ===

1. TÊN SẢN PHẨM: {product.get('Ten', '')}

2. GIÁ BÁN: {product.get('Gia', '')}

3. TỒN KHO: {product.get('TonKho', '')}

4. THUỘC TÍNH:
   - Màu sắc: {product.get('Mau', 'Chưa có thông tin')}
   - Size/Kích thước: {product.get('Size', 'Chưa có thông tin')}

5. MÔ TẢ CHI TIẾT:
{product.get('MoTa', 'Chưa có mô tả chi tiết')}

6. THÔNG TIN CHÍNH SÁCH:
   - Vận chuyển: {shipping_info if shipping_info else 'Chưa có thông tin cụ thể. Chính sách chung: Giao hàng toàn quốc, phí ship 20-50k. Miễn phí ship cho đơn từ 500k.'}
   - Bảo hành: {warranty_info if warranty_info else 'Chưa có thông tin cụ thể. Chính sách chung: Bảo hành theo chính sách của nhà sản xuất.'}
   - Đổi trả: {return_info if return_info else 'Chưa có thông tin cụ thể. Chính sách chung: Đổi/trả trong 3-7 ngày nếu sản phẩm lỗi, còn nguyên tem mác.'}
   - Thanh toán: {payment_info if payment_info else 'Chưa có thông tin cụ thể. Chính sách chung: Thanh toán khi nhận hàng (COD) hoặc chuyển khoản ngân hàng.'}

7. ĐÁNH GIÁ PHÙ HỢP:
   - Sản phẩm phù hợp với: {product.get('Ten', '').lower()} 
   - Tính năng nổi bật: {extract_key_features(product.get('MoTa', ''))}
"""
    
    return context


def extract_key_features(description: str) -> str:
    """Trích xuất tính năng nổi bật từ mô tả"""
    if not description:
        return "Chưa có thông tin"
    
    # Tìm các câu quan trọng
    sentences = re.split(r'[.!?]', description)
    key_features = []
    
    keywords = ['chất liệu', 'material', 'vải', 'cotton', 'poly', 'len', 'da', 
                'thiết kế', 'design', 'kiểu dáng', 'form', 'mẫu mã',
                'công nghệ', 'technology', 'tính năng', 'feature',
                'phù hợp', 'suitable', 'dành cho', 'cho']
    
    for sentence in sentences:
        if len(sentence.strip()) > 10:
            sentence_lower = sentence.lower()
            if any(keyword in sentence_lower for keyword in keywords):
                key_features.append(sentence.strip())
    
    if key_features:
        return ". ".join(key_features[:5]) + "."
    else:
        # Lấy 2 câu đầu nếu không tìm thấy keyword
        valid_sentences = [s.strip() for s in sentences if len(s.strip()) > 10]
        if valid_sentences:
            return ". ".join(valid_sentences[:2]) + "."
        return "Chưa có thông tin chi tiết"


def detect_ms_from_text(text: str):
    """Tìm mã sản phẩm trong tin nhắn"""
    ms_list = re.findall(r"\[MS(\d{6})\]", text.upper())
    if ms_list:
        return "MS" + ms_list[0]
    
    # Tìm không có dấu []
    ms_list = re.findall(r"MS(\d{6})", text.upper())
    if ms_list:
        return "MS" + ms_list[0]
    
    return None


def get_product_suggestions(limit=5):
    """Lấy danh sách sản phẩm gợi ý"""
    load_products()
    suggestions = []
    
    for ms, product in list(PRODUCTS.items())[:limit]:
        suggestions.append({
            "ms": ms,
            "name": product.get("Ten", ""),
            "price": product.get("Gia", ""),
            "description": product.get("MoTa", "")[:100] + "..." if len(product.get("MoTa", "")) > 100 else product.get("MoTa", "")
        })
    
    return suggestions


def build_gpt_system_prompt(uid: str, ms: str = None):
    """
    Xây dựng system prompt cho GPT dựa trên ngữ cảnh
    """
    load_products()
    
    if ms and ms in PRODUCTS:
        product_context = build_comprehensive_product_context(ms)
        
        prompt = f"""Bạn là CHUYÊN GIA TƯ VẤN BÁN HÀNG của {FANPAGE_NAME}. 
Bạn đang tư vấn cho sản phẩm có mã: {ms}

THÔNG TIN SẢN PHẨM (BẮT BUỘC CHỈ SỬ DỤNG THÔNG TIN NÀY):
{product_context}

QUY TẮC TRẢ LỜI (TUYỆT ĐỐI TUÂN THỦ):
1. CHỈ sử dụng thông tin có trong "THÔNG TIN SẢN PHẨM" ở trên
2. KHÔNG ĐƯỢC bịa thêm bất kỳ thông tin nào không có trong dữ liệu
3. Nếu không có thông tin, hãy trả lời: "Dạ, phần này trong hệ thống chưa có thông tin ạ, em sợ nói sai nên không dám khẳng định."
4. Nếu khách hỏi về sản phẩm khác, hãy đề nghị khách cung cấp mã sản phẩm mới
5. Giọng điệu: Thân thiện, chuyên nghiệp, xưng "em", gọi khách là "anh/chị"
6. Luôn hướng đến chốt đơn: Cuối mỗi câu trả lời, nhẹ nhàng đề nghị đặt hàng

LINK ĐẶT HÀNG: {DOMAIN}/order-form?ms={ms}&uid={uid}

Hãy trả lời bằng tiếng Việt, tự nhiên như đang chat Messenger."""
        
        return prompt
    
    else:
        # Không có mã sản phẩm - prompt chung
        suggestions = get_product_suggestions(3)
        suggestion_text = "\n".join([f"- [{p['ms']}] {p['name']} - {p['price']}" for p in suggestions])
        
        prompt = f"""Bạn là CHUYÊN GIA TƯ VẤN BÁN HÀNG của {FANPAGE_NAME}.

HIỆN TẠI BẠN CHƯA BIẾT KHÁCH QUAN TÂM SẢN PHẨM NÀO.

NHIỆM VỤ CỦA BẠN:
1. Hỏi khách về sản phẩm họ quan tâm
2. Đề nghị khách cung cấp mã sản phẩm (ví dụ: [MS123456])
3. Hoặc giới thiệu một số sản phẩm nổi bật

MỘT SỐ SẢN PHẨM GỢI Ý:
{suggestion_text}

QUY TẮC:
1. KHÔNG tự ý giới thiệu chi tiết sản phẩm khi chưa biết mã
2. Luôn hướng khách đến việc cung cấp mã sản phẩm
3. Có thể đề nghị khách gõ "xem sản phẩm" để xem danh sách đầy đủ
4. Giọng điệu: Thân thiện, chuyên nghiệp, xưng "em", gọi khách là "anh/chị"

Hãy bắt đầu bằng câu chào và hỏi khách về sản phẩm họ quan tâm."""
        
        return prompt


def generate_gpt_response(uid: str, user_message: str, ms: str = None):
    """
    Gọi GPT để trả lời câu hỏi của khách
    """
    if not client or not OPENAI_API_KEY:
        return "Hiện tại hệ thống trợ lý AI đang bảo trì, vui lòng thử lại sau ạ."
    
    try:
        # Xây dựng system prompt
        system_prompt = build_gpt_system_prompt(uid, ms)
        
        # Lấy conversation history
        ctx = USER_CONTEXT[uid]
        conversation = ctx.get("conversation_history", [])
        
        # Giới hạn history để tránh token quá nhiều
        if len(conversation) > 10:
            conversation = conversation[-10:]
        
        # Tạo messages
        messages = [{"role": "system", "content": system_prompt}]
        
        # Thêm conversation history
        for msg in conversation:
            messages.append(msg)
        
        # Thêm message hiện tại
        messages.append({"role": "user", "content": user_message})
        
        # Gọi OpenAI API
        response = client.chat.completions.create(
            model="gpt-4o-mini",  # Có thể dùng gpt-3.5-turbo để tiết kiệm
            messages=messages,
            temperature=0.7,
            max_tokens=500,
            timeout=15.0,
        )
        
        reply = response.choices[0].message.content.strip()
        
        # Lưu vào conversation history
        conversation.append({"role": "user", "content": user_message})
        conversation.append({"role": "assistant", "content": reply})
        ctx["conversation_history"] = conversation
        
        return reply
        
    except Exception as e:
        print(f"GPT Error: {e}")
        return "Dạ em đang gặp chút trục trặc kỹ thuật. Anh/chị vui lòng thử lại sau ít phút ạ."


# ============================================
# HANDLE MESSAGES
# ============================================

def handle_postback(uid: str, payload: str):
    """Xử lý postback từ button, menu"""
    ctx = USER_CONTEXT[uid]
    
    if payload == "GET_STARTED":
        # Khách bấm "Bắt đầu"
        ctx["referral_source"] = "get_started"
        welcome_msg = f"""Chào anh/chị! 👋 
Em là trợ lý AI của {FANPAGE_NAME}.

Để em tư vấn chính xác, anh/chị vui lòng:
1. Gửi mã sản phẩm (ví dụ: [MS123456])
2. Hoặc gõ "xem sản phẩm" để xem danh sách
3. Hoặc mô tả sản phẩm bạn đang tìm

Anh/chị quan tâm sản phẩm nào ạ?"""
        send_message(uid, welcome_msg)
    
    elif payload.startswith("ADVICE_"):
        # Khách bấm "Xem chi tiết" từ carousel
        ms = payload.replace("ADVICE_", "")
        if ms in PRODUCTS:
            ctx["current_ms"] = ms
            ctx["referral_source"] = "carousel_click"
            # Gọi GPT để giới thiệu sản phẩm
            response = generate_gpt_response(uid, f"Giới thiệu chi tiết sản phẩm {ms}", ms)
            send_message(uid, response)
        else:
            send_message(uid, "❌ Em không tìm thấy sản phẩm này. Anh/chị vui lòng kiểm tra lại mã sản phẩm ạ.")
    
    elif payload.startswith("ORDER_"):
        # Khách bấm "Đặt ngay" từ carousel
        ms = payload.replace("ORDER_", "")
        if ms in PRODUCTS:
            ctx["current_ms"] = ms
            domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
            order_link = f"{domain}/order-form?ms={ms}&uid={uid}"
            product_name = PRODUCTS[ms].get('Ten', '')
            send_message(uid, f"🎯 Anh/chị chọn sản phẩm [{ms}] {product_name}!\n\n📋 Đặt hàng ngay tại đây:\n{order_link}")
    

def handle_text(uid: str, text: str):
    """Xử lý tin nhắn văn bản - TẤT CẢ do GPT xử lý"""
    if not text or len(text.strip()) == 0:
        return
    
    ctx = USER_CONTEXT[uid]
    
    if ctx.get("processing_lock"):
        return
    
    ctx["processing_lock"] = True
    
    try:
        load_products()
        
        # Tìm mã sản phẩm trong tin nhắn
        detected_ms = detect_ms_from_text(text)
        
        # Xác định mã sản phẩm sẽ dùng
        current_ms = None
        if detected_ms and detected_ms in PRODUCTS:
            # Có mã sản phẩm trong tin nhắn
            current_ms = detected_ms
            ctx["current_ms"] = detected_ms
            print(f"[DEBUG] Detected MS from text: {detected_ms}")
        else:
            # Dùng mã sản phẩm từ context
            current_ms = ctx.get("current_ms")
        
        # Kiểm tra từ khóa đặc biệt "xem sản phẩm"
        lower = text.lower()
        if "xem sản phẩm" in lower or "show sản phẩm" in lower or "danh sách sản phẩm" in lower:
            # Gửi carousel
            if PRODUCTS:
                carousel_elements = []
                for i, (ms, product) in enumerate(list(PRODUCTS.items())[:5]):
                    images_field = product.get("FullRow", {}).get("Images", "")
                    image_url = ""
                    if images_field:
                        urls = images_field.split(',')
                        if urls:
                            image_url = urls[0].strip()
                    
                    element = {
                        "title": f"[{ms}] {product.get('Ten', '')}",
                        "image_url": image_url,
                        "subtitle": f"💰 {product.get('Gia', '')} | 📦 {product.get('TonKho', '')}",
                        "default_action": {
                            "type": "web_url",
                            "url": f"{DOMAIN}/order-form?ms={ms}&uid={uid}",
                            "webview_height_ratio": "tall"
                        },
                        "buttons": [
                            {
                                "type": "web_url",
                                "url": f"{DOMAIN}/order-form?ms={ms}&uid={uid}",
                                "title": "🛒 Đặt ngay"
                            },
                            {
                                "type": "postback",
                                "title": "🔍 Xem chi tiết",
                                "payload": f"ADVICE_{ms}"
                            }
                        ]
                    }
                    carousel_elements.append(element)
                
                if carousel_elements:
                    send_carousel_template(uid, carousel_elements)
                    send_message(uid, "📱 Anh/chị vuốt sang trái/phải để xem thêm sản phẩm nhé!")
                else:
                    send_message(uid, "Hiện tại shop chưa có sản phẩm nào để hiển thị ạ.")
            else:
                send_message(uid, "Hiện tại shop chưa có sản phẩm nào ạ.")
            
            ctx["processing_lock"] = False
            return
        
        # TẤT CẢ CÂU HỎI CÒN LẠI do GPT xử lý
        print(f"[GPT CALL] User: {uid}, MS: {current_ms}, Text: {text}")
        gpt_response = generate_gpt_response(uid, text, current_ms)
        send_message(uid, gpt_response)
        
    except Exception as e:
        print(f"Error in handle_text: {e}")
        send_message(uid, "Dạ em đang gặp chút trục trặc. Anh/chị vui lòng thử lại sau ít phút ạ.")
    finally:
        ctx["processing_lock"] = False


def handle_image(uid: str, image_url: str):
    """Xử lý ảnh - Yêu cầu khách cung cấp mã sản phẩm"""
    ctx = USER_CONTEXT[uid]
    ctx["referral_source"] = "image_upload"
    
    response = """📷 Em đã nhận được ảnh từ anh/chị!

Hiện tại hệ thống chưa hỗ trợ nhận diện ảnh tự động.

Để em tư vấn chính xác, anh/chị vui lòng:
1. Gửi mã sản phẩm (ví dụ: [MS123456])
2. Hoặc mô tả sản phẩm trong ảnh
3. Hoặc gõ "xem sản phẩm" để xem danh sách

Anh/chị có mã sản phẩm không ạ?"""
    
    send_message(uid, response)


# ============================================
# WEBHOOK HANDLER
# ============================================

@app.route("/", methods=["GET"])
def home():
    return "OK", 200


@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        mode = request.args.get("hub.mode")
        token = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")
        
        if mode == "subscribe" and token == VERIFY_TOKEN:
            print("[WEBHOOK VERIFY] Success!")
            return challenge, 200
        else:
            print("[WEBHOOK VERIFY] Failed!")
            return "Verification token mismatch", 403

    data = request.get_json() or {}
    print("Webhook received:", json.dumps(data, ensure_ascii=False)[:500])

    entry = data.get("entry", [])
    for e in entry:
        messaging = e.get("messaging", [])
        for m in messaging:
            sender_id = m.get("sender", {}).get("id")
            if not sender_id:
                continue

            if m.get("message", {}).get("is_echo"):
                print(f"[ECHO] Bỏ qua tin nhắn từ bot")
                continue
            
            if m.get("delivery") or m.get("read"):
                continue
            
            # Xử lý referral (từ CTA, ads)
            if m.get("referral"):
                ref = m["referral"]
                ctx = USER_CONTEXT[sender_id]
                ctx["referral_source"] = ref.get("source", "unknown")
                ctx["referral_payload"] = ref.get("ref", "")
                print(f"[REFERRAL] User {sender_id} từ {ctx['referral_source']} với payload: {ctx['referral_payload']}")
            
            if "postback" in m:
                payload = m["postback"].get("payload")
                if payload:
                    handle_postback(sender_id, payload)
                    continue
            
            if "message" in m:
                msg = m["message"]
                text = msg.get("text")
                attachments = msg.get("attachments") or []
                if text:
                    handle_text(sender_id, text)
                elif attachments:
                    for att in attachments:
                        if att.get("type") == "image":
                            image_url = att.get("payload", {}).get("url")
                            if image_url:
                                handle_image(sender_id, image_url)

    return "OK", 200


# ============================================
# ORDER FORM & API (giữ nguyên)
# ============================================

@app.route("/order-form", methods=["GET"])
def order_form():
    ms = (request.args.get("ms") or "").upper()
    uid = request.args.get("uid") or ""
    
    if not ms:
        return """
        <html><body style="text-align: center; padding: 50px;">
            <h2 style="color: #FF3B30;">⚠️ Không tìm thấy sản phẩm</h2>
            <p>Vui lòng quay lại Messenger và chọn sản phẩm để đặt hàng.</p>
        </body></html>
        """, 400

    load_products()
    if ms not in PRODUCTS:
        return """
        <html><body style="text-align: center; padding: 50px;">
            <h2 style="color: #FF3B30;">⚠️ Sản phẩm không tồn tại</h2>
            <p>Vui lòng quay lại Messenger và chọn sản phẩm khác.</p>
        </body></html>
        """, 404

    # ... (giữ nguyên phần HTML form)

    return "Order form HTML here"  # Giữ nguyên code form cũ


@app.route("/api/submit-order", methods=["POST"])
def api_submit_order():
    data = request.get_json() or {}
    ms = (data.get("ms") or "").upper()
    uid = data.get("uid") or ""
    
    load_products()
    row = PRODUCTS.get(ms)
    if not row:
        return {"error": "not_found", "message": "Sản phẩm không tồn tại"}, 404

    # Gửi thông báo cho user
    if uid:
        msg = f"""🎉 Đơn hàng của anh/chị đã được tiếp nhận!

🛍 Sản phẩm: [{ms}] {row.get('Ten','')}
💰 Giá: {row.get('Gia', '')}

⏰ Shop sẽ liên hệ xác nhận trong 5-10 phút.
💳 Thanh toán khi nhận hàng (COD)

Cảm ơn anh/chị đã đặt hàng! ❤️"""
        send_message(uid, msg)

    return {"status": "ok", "message": "Đơn hàng đã được tiếp nhận"}


@app.route("/health", methods=["GET"])
def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "products_loaded": len(PRODUCTS),
        "openai_configured": bool(client),
        "facebook_configured": bool(PAGE_ACCESS_TOKEN)
    }, 200


# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    print("🚀 Starting GPT-Powered Chatbot...")
    print(f"📊 Products URL: {GOOGLE_SHEET_CSV_URL}")
    print(f"🤖 OpenAI: {'Enabled' if client else 'Disabled'}")
    app.run(host="0.0.0.0", port=5000, debug=True)
