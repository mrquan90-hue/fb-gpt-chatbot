import os
import json
import re
import time
import csv
from collections import defaultdict
from urllib.parse import quote
from datetime import datetime

import requests
from flask import Flask, request, send_from_directory
from openai import OpenAI

# ============================================
# FLASK APP
# ============================================

app = Flask(__name__, static_folder="static", static_url_path="/static")

# ============================================
# ENVIRONMENT (Render)
# ============================================

OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY")
PAGE_ACCESS_TOKEN  = os.getenv("PAGE_ACCESS_TOKEN")
VERIFY_TOKEN       = os.getenv("VERIFY_TOKEN")
FREEIMAGE_API_KEY  = os.getenv("FREEIMAGE_API_KEY")
SHEET_URL          = os.getenv("SHEET_CSV_URL")
DOMAIN             = os.getenv("DOMAIN", "fb-gpt-chatbot.onrender.com")
PAGE_ID            = os.getenv("PAGE_ID", "516937221685203")

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ============================================
# GLOBAL STATE
# ============================================

USER_CONTEXT = defaultdict(lambda: {
    "last_ms": None,               # mã sản phẩm gần nhất bot hiểu
    "inbox_entry_ms": None,        # mã từ Fchat/referral
    "vision_ms": None,             # mã từ GPT Vision
    "caption_ms": None,            # dự phòng (caption bài viết)
    "history": [],                 # lịch sử hội thoại cho GPT
    "greeted": False,              # đã chào chưa
    "recommended_sent": False,     # đã gửi 5 sản phẩm gợi ý chưa
    "product_info_sent_ms": None,  # đã gửi thông tin sản phẩm nào
    "carousel_sent": False,        # đã gửi carousel chưa
    "last_postback_time": 0,       # thời gian postback cuối cùng (chống lặp)
    "sent_message_ids": set(),     # ID các tin nhắn đã gửi (chống lặp echo)
    "order_state": None,           # Trạng thái đặt hàng
    "order_data": {},              # Dữ liệu đơn hàng
    "page_info": None,             # Thông tin fanpage
})

PRODUCTS = {}
LAST_LOAD = 0
LOAD_TTL = 300  # 5 phút

# ============================================
# TỪ KHOÁ THỂ HIỆN Ý ĐỊNH "ĐẶT HÀNG / MUA"
# ============================================

ORDER_KEYWORDS = [
    "đặt hàng nha",
    "ok đặt",
    "ok mua",
    "ok em",
    "ok e",
    "mua 1 cái",
    "mua cái này",
    "mua luôn",
    "chốt",
    "lấy mã",
    "lấy mẫu",
    "lấy luôn",
    "lấy em này",
    "lấy e này",
    "gửi cho",
    "ship cho",
    "ship 1 cái",
    "chốt 1 cái",
    "cho tôi mua",
    "tôi lấy nhé",
    "cho mình đặt",
    "tôi cần mua",
    "xác nhận đơn hàng giúp tôi",
    "tôi đồng ý mua",
    "làm đơn cho tôi đi",
    "tôi chốt đơn nhé",
    "cho xin 1 cái",
    "cho đặt 1 chiếc",
    "bên shop tạo đơn giúp em",
    "okela",
    "ok bạn",
    "đồng ý",
    "được đó",
    "vậy cũng được",
    "được vậy đi",
    "chốt như bạn nói",
    "ok giá đó đi",
    "lấy mẫu đó đi",
    "tư vấn giúp mình đặt hàng",
    "hướng dẫn mình mua với",
    "bạn giúp mình đặt nhé",
    "muốn có nó quá",
    "muốn mua quá",
    "ưng quá, làm sao để mua",
    "chốt đơn",
    "bán cho em",
    "bán cho em vé",
    "xuống đơn giúp em",
    "đơm hàng",
    "lấy nha",
    "lấy nhé",
    "mua nha",
    "mình lấy đây",
    "shop ơi, của em",
    "vậy lấy cái",
    "thôi lấy cái",
    "order nhé",
]

# ============================================
# TIỆN ÍCH FACEBOOK
# ============================================

def send_message(uid: str, text: str) -> str:
    """
    Gửi tin nhắn text và trả về message_id
    """
    if not text:
        return ""
    url = "https://graph.facebook.com/v18.0/me/messages"
    params = {"access_token": PAGE_ACCESS_TOKEN}
    payload = {
        "recipient": {"id": uid},
        "message": {"text": text},
        "messaging_type": "RESPONSE",
    }
    try:
        r = requests.post(url, params=params, json=payload, timeout=15)
        print("SEND MSG:", r.status_code, r.text)
        if r.status_code == 200:
            response = r.json()
            message_id = response.get("message_id", "")
            if message_id:
                USER_CONTEXT[uid]["sent_message_ids"].add(message_id)
            return message_id
        return ""
    except Exception as e:
        print("SEND MSG ERROR:", e)
        return ""


def send_image(uid: str, image_url: str) -> str:
    """
    Gửi ảnh qua Facebook Messenger và trả về message_id.
    """
    url_source = image_url
    try:
        resp = requests.get(url_source, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print("DOWNLOAD IMG ERROR:", e, "URL:", url_source)
        return ""

    files = {
        "filedata": ("image.jpg", resp.content, "image/jpeg")
    }
    params = {
        "access_token": PAGE_ACCESS_TOKEN
    }
    data = {
        "recipient": json.dumps({"id": uid}, ensure_ascii=False),
        "message": json.dumps({
            "attachment": {
                "type": "image",
                "payload": {}
            }
        }, ensure_ascii=False),
        "messaging_type": "RESPONSE",
    }

    try:
        r = requests.post(
            "https://graph.facebook.com/v18.0/me/messages",
            params=params,
            data=data,
            files=files,
            timeout=30
        )
        print("SEND IMG:", r.status_code, r.text)
        if r.status_code == 200:
            response = r.json()
            message_id = response.get("message_id", "")
            if message_id:
                USER_CONTEXT[uid]["sent_message_ids"].add(message_id)
            return message_id
        return ""
    except Exception as e:
        print("SEND IMG ERROR:", e)
        return ""


# ============================================
# CAROUSEL TEMPLATE - ĐÃ SỬA: NÚT "CHỌN SẢN PHẨM" CHUYỂN THÀNH LINK WEB_URL
# ============================================

def send_carousel_template(recipient_id: str, products_data: list) -> str:
    """
    Gửi carousel template với danh sách sản phẩm
    Trả về message_id
    """
    try:
        # Tạo các element cho carousel
        elements = []
        for product in products_data[:10]:  # Facebook giới hạn 10 element
            # Lấy ảnh đầu tiên từ field Images
            image_field = product.get("Images", "")
            image_urls = parse_image_urls(image_field)
            image_url = image_urls[0] if image_urls else ""
            
            # Nếu không có ảnh, bỏ qua sản phẩm này
            if not image_url:
                continue
            
            ms = product.get('MS', '')
            
            # Tạo URL đặt hàng với user_id và mã sản phẩm
            order_url = f"https://{DOMAIN}/order-form?ms={ms}&uid={recipient_id}"
                
            element = {
                "title": f"[{ms}] {product.get('Ten', '')}",
                "subtitle": f"💰 Giá: {product.get('Gia', '')}\n{product.get('MoTa', '')[:60]}..." if product.get('MoTa') else f"💰 Giá: {product.get('Gia', '')}",
                "image_url": image_url,
                "buttons": [
                    {
                        "type": "postback",
                        "title": "📋 Xem chi tiết",
                        "payload": f"VIEW_{ms}"
                    },
                    {
                        "type": "web_url",  # ĐÃ SỬA: Thay postback bằng web_url
                        "title": "🛒 Đặt ngay",
                        "url": order_url,
                        "webview_height_ratio": "tall",
                        "messenger_extensions": True
                    }
                ]
            }
            elements.append(element)
        
        if not elements:
            print("Không có sản phẩm nào có ảnh để hiển thị trong carousel")
            return ""
        
        # Tạo payload carousel
        url = "https://graph.facebook.com/v18.0/me/messages"
        params = {"access_token": PAGE_ACCESS_TOKEN}
        payload = {
            "recipient": {"id": recipient_id},
            "message": {
                "attachment": {
                    "type": "template",
                    "payload": {
                        "template_type": "generic",
                        "elements": elements
                    }
                }
            },
            "messaging_type": "RESPONSE"
        }
        
        r = requests.post(url, params=params, json=payload, timeout=15)
        print("SEND CAROUSEL:", r.status_code, r.text)
        if r.status_code == 200:
            response = r.json()
            message_id = response.get("message_id", "")
            if message_id:
                USER_CONTEXT[recipient_id]["sent_message_ids"].add(message_id)
            return message_id
        return ""
        
    except Exception as e:
        print("SEND CAROUSEL ERROR:", e)
        return ""


def send_product_carousel(recipient_id: str) -> None:
    """
    Gửi 5 sản phẩm đầu tiên dưới dạng Carousel Template
    """
    load_products()
    if not PRODUCTS:
        return
    
    # Lấy 5 sản phẩm đầu tiên
    products = list(PRODUCTS.values())[:5]
    
    # Gửi carousel
    send_carousel_template(recipient_id, products)


# ============================================
# ORDER FORM FUNCTIONS
# ============================================

def send_order_form_quick_replies(uid: str, product_info: dict) -> None:
    """
    Gửi form đặt hàng dạng quick replies
    """
    # Gửi tổng hợp thông tin sản phẩm
    summary = f"""
📋 THÔNG TIN ĐƠN HÀNG
────────────────────
🛍️ Sản phẩm: {product_info['name']}
💰 Giá: {product_info['price']}
🎨 Màu: {product_info['color']}
📏 Size: {product_info['size']}
────────────────────
"""
    send_message(uid, summary)
    
    # Gửi form với quick replies
    form_message = {
        "recipient": {"id": uid},
        "message": {
            "text": "Để hoàn tất đơn hàng, vui lòng cung cấp thông tin sau:",
            "quick_replies": [
                {
                    "content_type": "text",
                    "title": "👤 Họ tên",
                    "payload": "ORDER_PROVIDE_NAME"
                },
                {
                    "content_type": "text",
                    "title": "📱 Số điện thoại",
                    "payload": "ORDER_PROVIDE_PHONE"
                },
                {
                    "content_type": "text",
                    "title": "🏠 Địa chỉ",
                    "payload": "ORDER_PROVIDE_ADDRESS"
                }
            ]
        },
        "messaging_type": "RESPONSE"
    }
    
    try:
        r = requests.post(
            "https://graph.facebook.com/v18.0/me/messages",
            params={"access_token": PAGE_ACCESS_TOKEN},
            json=form_message,
            timeout=15
        )
        print("SEND ORDER FORM:", r.status_code, r.text)
    except Exception as e:
        print("SEND ORDER FORM ERROR:", e)


def send_order_confirmation(uid: str) -> None:
    """
    Gửi xác nhận đơn hàng cuối cùng
    """
    ctx = USER_CONTEXT[uid]
    order_data = ctx.get("order_data", {})
    product_info = order_data.get("product_info", {})
    
    if not product_info:
        send_message(uid, "Có lỗi xảy ra khi xử lý đơn hàng. Vui lòng thử lại.")
        return
    
    confirmation_text = f"""
✅ ĐÃ XÁC NHẬN ĐƠN HÀNG THÀNH CÔNG!
────────────────────
🛍️ Sản phẩm: {product_info.get('name', '')}
💰 Giá: {product_info.get('price', '')}
🎨 Màu: {product_info.get('color', '')}
📏 Size: {product_info.get('size', '')}
────────────────────
👤 Người nhận: {order_data.get('name', '')}
📱 SĐT: {order_data.get('phone', '')}
🏠 Địa chỉ: {order_data.get('address', '')}
────────────────────
⏰ Thời gian đặt hàng: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}
📦 Đơn hàng sẽ được giao trong 2-4 ngày làm việc
💳 Thanh toán khi nhận hàng (COD)
────────────────────
Cảm ơn bạn đã đặt hàng! ❤️
Shop sẽ liên hệ xác nhận trong thời gian sớm nhất.
"""
    
    send_message(uid, confirmation_text)
    
    # Reset trạng thái đặt hàng
    ctx["order_state"] = None
    ctx["order_data"] = {}


def handle_order_form_step(uid: str, text: str) -> bool:
    """
    Xử lý từng bước điền form đặt hàng
    Trả về True nếu đã xử lý, False nếu không phải ở trạng thái đặt hàng
    """
    ctx = USER_CONTEXT[uid]
    order_state = ctx.get("order_state")
    
    if not order_state:
        return False
    
    if order_state == "waiting_name":
        ctx["order_data"]["name"] = text
        ctx["order_state"] = "waiting_phone"
        send_message(uid, "✅ Đã lưu họ tên: " + text)
        send_message(uid, "📱 Vui lòng nhập số điện thoại của bạn:")
        return True
        
    elif order_state == "waiting_phone":
        # Kiểm tra số điện thoại hợp lệ
        phone_pattern = r'^(0|\+84)[1-9]\d{8}$'
        phone = text.strip().replace(" ", "")
        
        if not re.match(phone_pattern, phone):
            send_message(uid, "❌ Số điện thoại không hợp lệ. Vui lòng nhập lại số điện thoại (ví dụ: 0912345678 hoặc +84912345678):")
            return True
            
        ctx["order_data"]["phone"] = phone
        ctx["order_state"] = "waiting_address"
        send_message(uid, "✅ Đã lưu số điện thoại: " + phone)
        send_message(uid, "🏠 Vui lòng nhập địa chỉ giao hàng chi tiết (số nhà, đường, phường/xã, quận/huyện, tỉnh/thành phố):")
        return True
        
    elif order_state == "waiting_address":
        if len(text.strip()) < 10:
            send_message(uid, "❌ Địa chỉ quá ngắn. Vui lòng nhập địa chỉ chi tiết hơn:")
            return True
            
        ctx["order_data"]["address"] = text.strip()
        ctx["order_state"] = "confirming"
        
        # Hiển thị tổng hợp và yêu cầu xác nhận
        order_data = ctx["order_data"]
        product_info = order_data.get("product_info", {})
        
        summary = f"""
📋 THÔNG TIN ĐƠN HÀNG ĐẦY ĐỦ
────────────────────
🛍️ Sản phẩm: {product_info.get('name', '')}
💰 Giá: {product_info.get('price', '')}
🎨 Màu: {product_info.get('color', '')}
📏 Size: {product_info.get('size', '')}
────────────────────
👤 Người nhận: {order_data.get('name', '')}
📱 SĐT: {order_data.get('phone', '')}
🏠 Địa chỉ: {order_data.get('address', '')}
────────────────────
"""
        send_message(uid, summary)
        
        # Gửi quick replies để xác nhận
        confirm_message = {
            "recipient": {"id": uid},
            "message": {
                "text": "Vui lòng xác nhận thông tin trên là chính xác:",
                "quick_replies": [
                    {
                        "content_type": "text",
                        "title": "✅ Xác nhận đặt hàng",
                        "payload": "ORDER_CONFIRM"
                    },
                    {
                        "content_type": "text",
                        "title": "✏️ Sửa thông tin",
                        "payload": "ORDER_EDIT"
                    }
                ]
            },
            "messaging_type": "RESPONSE"
        }
        
        try:
            r = requests.post(
                "https://graph.facebook.com/v18.0/me/messages",
                params={"access_token": PAGE_ACCESS_TOKEN},
                json=confirm_message,
                timeout=15
            )
            print("SEND ORDER CONFIRM:", r.status_code, r.text)
        except Exception as e:
            print("SEND ORDER CONFIRM ERROR:", e)
            
        return True
        
    return False


def start_order_process(uid: str, ms: str) -> None:
    """
    Bắt đầu quá trình đặt hàng
    """
    load_products()
    
    if ms not in PRODUCTS:
        send_message(uid, "❌ Không tìm thấy thông tin sản phẩm. Vui lòng thử lại.")
        return
    
    product_row = PRODUCTS[ms]
    ctx = USER_CONTEXT[uid]
    
    # Lưu thông tin sản phẩm vào order_data
    ctx["order_data"] = {
        "product_info": {
            "ms": ms,
            "name": f"[{ms}] {product_row.get('Ten', '')}",
            "price": product_row.get('Gia', ''),
            "color": product_row.get('màu (Thuộc tính)', ''),
            "size": product_row.get('size (Thuộc tính)', '')
        }
    }
    
    # Bắt đầu form đặt hàng
    send_order_form_quick_replies(uid, ctx["order_data"]["product_info"])
    ctx["order_state"] = "waiting_name"


# ============================================
# REHOST IMAGE (freeimage.host - tuỳ chọn)
# ============================================

def rehost_image(url: str) -> str:
    if not FREEIMAGE_API_KEY:
        return url
    try:
        api = "https://freeimage.host/api/1/upload"
        payload = {
            "key": FREEIMAGE_API_KEY,
            "source": url,
            "action": "upload",
        }
        r = requests.post(api, data=payload, timeout=30)
        data = r.json()
        if "image" in data and "url" in data["image"]:
            return data["image"]["url"]
        return url
    except Exception as e:
        print("REHOST ERROR:", e)
        return url


# ============================================
# LOAD SẢN PHẨM TỪ SHEET
# ============================================

def load_products(force: bool = False) -> None:
    """
    Đọc CSV từ SHEET_CSV_URL với các cột:
      - Mã sản phẩm
      - Tên sản phẩm
      - Images
      - Videos
      - Tồn kho
      - Giá bán
      - Mô tả
      - màu (Thuộc tính)
      - size (Thuộc tính)
    """
    global PRODUCTS, LAST_LOAD

    now = time.time()
    if not force and PRODUCTS and now - LAST_LOAD < LOAD_TTL:
        return

    if not SHEET_URL:
        print("❌ SHEET_CSV_URL chưa cấu hình")
        PRODUCTS = {}
        return

    print("🟦 Loading sheet:", SHEET_URL)

    try:
        resp = requests.get(SHEET_URL, timeout=30)
        resp.raise_for_status()

        csv_text = resp.content.decode("utf-8", errors="replace")
        lines = csv_text.splitlines()
        reader = csv.DictReader(lines)

        products = {}
        for raw_row in reader:
            row = dict(raw_row)

            ms = (row.get("Mã sản phẩm") or "").strip()
            if not ms:
                continue

            ten = (row.get("Tên sản phẩm") or "").strip()
            if not ten:
                continue

            gia = (row.get("Giá bán") or "").strip()
            images = (row.get("Images") or "").strip()
            videos = (row.get("Videos") or "").strip()
            tonkho = (row.get("Tồn kho") or "").strip()
            mota = (row.get("Mô tả") or "").strip()
            mau = (row.get("màu (Thuộc tính)") or "").strip()
            size = (row.get("size (Thuộc tính)") or "").strip()

            row["MS"] = ms
            row["Ten"] = ten
            row["Gia"] = gia
            row["MoTa"] = mota
            row["Images"] = images
            row["Videos"] = videos
            row["Tồn kho"] = tonkho
            row["màu (Thuộc tính)"] = mau
            row["size (Thuộc tính)"] = size

            products[ms] = row

        PRODUCTS = products
        LAST_LOAD = now
        print(f"📦 Loaded {len(PRODUCTS)} products.")
    except Exception as e:
        print("❌ load_products error:", e)
        PRODUCTS = {}


# ============================================
# IMAGE HELPER & GPT VISION
# ============================================

def parse_image_urls(images_field: str) -> list:
    if not images_field:
        return []
    parts = [u.strip() for u in images_field.split(",") if u.strip()]
    # loại trùng nhưng vẫn giữ thứ tự
    seen = set()
    result = []
    for u in parts:
        if u not in seen:
            seen.add(u)
            result.append(u)
    return result


def gpt_analyze_image(url: str):
    if not client:
        return None, None
    try:
        prompt = f"""
        Bạn là trợ lý bán hàng. Hãy mô tả sản phẩm trong ảnh
        và cố gắng tìm mã sản phẩm gần nhất trong danh sách:
        {', '.join(PRODUCTS.keys())}

        Trả về JSON dạng:
        {{
          "description": "...",
          "matched_ms": "MS000123" hoặc null
        }}
        """
        r = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Bạn là trợ lý bán hàng chuyên nghiệp."},
                {"role": "user", "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": url}},
                ]},
            ],
            temperature=0.3,
        )
        text = r.choices[0].message.content
        m = re.search(r"(MS\d+)", text)
        return (m.group(1) if m else None), text
    except Exception as e:
        print("Vision error:", e)
        return None, None


# ============================================
# MS DETECT & CONTEXT
# ============================================

def extract_ms(text: str):
    if not text:
        return None
    m = re.search(r"(MS\d+)", text, flags=re.I)
    return m.group(1).upper() if m else None


def extract_short_code(text: str):
    """
    Tìm pattern dạng 'mã 09', 'ma so 9', 'mã số 18'...
    Trả về phần số (ví dụ '09', '18').
    """
    if not text:
        return None
    lower = text.lower()
    m = re.search(r"mã\s*(?:số\s*)?(\d{1,3})", lower)
    if not m:
        m = re.search(r"ma\s*(?:so\s*)?(\d{1,3})", lower)
    if not m:
        return None
    return m.group(1)


def find_ms_by_short_code(code: str):
    """
    Map '09' -> mã trong PRODUCTS kết thúc bằng 09 / 009...
    Ví dụ: MS000009, MS009,...
    """
    if not code:
        return None
    code = code.lstrip("0") or code
    candidates = []
    for ms in PRODUCTS.keys():
        if not ms.upper().startswith("MS"):
            continue
        digits = re.sub(r"\D", "", ms)
        if digits.endswith(code):
            candidates.append(ms)

    if not candidates:
        return None

    candidates.sort(key=len, reverse=True)
    return candidates[0]


def resolve_best_ms(ctx: dict):
    """
    Ưu tiên mã sản phẩm theo thứ tự:
    1. Mã từ tin nhắn khách gửi gần nhất (last_ms) nếu nó tồn tại trong PRODUCTS
    2. Mã từ vision (nếu khách gửi ảnh)
    3. Mã từ inbox_entry_ms (từ comment/referral)
    4. Mã từ caption
    """
    # Ưu tiên last_ms nếu nó tồn tại trong danh sách sản phẩm
    if ctx.get("last_ms") and ctx["last_ms"] in PRODUCTS:
        return ctx["last_ms"]
    
    # Các nguồn khác
    for key in ["vision_ms", "inbox_entry_ms", "caption_ms"]:
        if ctx.get(key) and ctx[key] in PRODUCTS:
            return ctx[key]
    return None


# ============================================
# GPT CONTEXT ENGINE - ĐÃ SỬA: NÂNG CẤP CÂU TƯ VẤN CHUYÊN NGHIỆP
# ============================================

def gpt_reply(history: list, product_row: dict | None):
    if not client:
        return "Dạ hệ thống AI đang bận, anh/chị chờ em 1 lát với ạ."

    sys = """
    Bạn là MIU - trợ lý bán hàng chuyên nghiệp của Fashion Shop Premium.
    
    **QUY TẮC GIAO TIẾP:**
    - Xưng "em", gọi khách là "anh/chị"
    - Luôn lịch sự, nhiệt tình, thân thiện
    - Sử dụng icon cảm xúc phù hợp (❤️, 😊, 💕)
    - Định dạng tin nhắn rõ ràng, có cấu trúc
    
    **CHIẾN LƯỢC BÁN HÀNG:**
    1. TƯ VẤN CHUYÊN SÂU:
       - Hỏi về dáng người (cao/gầy, mũm mĩm, vai rộng)
       - Hỏi phong cách yêu thích (công sở, dạo phố, đi tiệc)
       - Hỏi ngân sách dự kiến
       - Tư vấn theo đặc điểm cá nhân
    
    2. TỐI ƯU CHỐT ĐƠN:
       - Nhấn mạnh ưu điểm nổi bật của sản phẩm
       - Gợi ý size/màu phù hợp với dáng người
       - Thông báo ưu đãi: Freeship 30K, giảm 5% khi đặt chat
       - Kêu gọi hành động rõ ràng: "Đặt ngay", "Chốt đơn"
    
    3. XỬ LÝ TỪ CHỐI:
       - Thấu hiểu: "Em hiểu ạ, mỗi người có gu riêng mà"
       - Chuyển hướng: "Để em gợi ý mẫu khác phù hợp hơn nhé"
       - Giữ liên lạc: "Khi nào cần tư vấn, anh/chị cứ nhắn em ạ"
    
    **KHÔNG BAO GIỜ:**
    - Bịa đặt thông tin sản phẩm
    - Hứa hẹn không thực tế
    - Thiếu nhiệt tình trong trả lời
    - Để khách chờ quá lâu (luôn phản hồi nhanh)
    """

    if product_row:
        tonkho = product_row.get("Tồn kho", "")
        mau = product_row.get("màu (Thuộc tính)", "")
        size = product_row.get("size (Thuộc tính)", "")
        gia = product_row.get("Gia", "")
        
        sys += (
            f"\n\n📦 **THÔNG TIN SẢN PHẨM HIỆN TẠI:**\n"
            f"- Tên: {product_row.get('Ten', '')}\n"
            f"- Mô tả: {product_row.get('MoTa', '')}\n"
            f"- Giá bán: {gia}\n"
            f"- Tồn kho: {tonkho}\n"
            f"- Màu sắc có sẵn: {mau if mau else 'Nhiều màu'}\n"
            f"- Size có sẵn: {size if size else 'Đa dạng size'}\n\n"
            f"💎 **ƯU ĐÃI ĐẶC BIỆT:**\n"
            f"- Freeship 30K cho đơn đầu tiên\n"
            f"- Giảm thêm 5% khi đặt qua chat\n"
            f"- Tặng voucher 50K cho lần mua sau\n"
            f"- Đổi trả dễ dàng trong 7 ngày\n\n"
            f"🎯 **CHIẾN LƯỢC CHỐT ĐƠN:**\n"
            f"1. Tư vấn size phù hợp dựa trên dáng người\n"
            f"2. Gợi ý màu sắc hợp phong cách\n"
            f"3. Thông báo ưu đãi hấp dẫn\n"
            f"4. Kêu gọi đặt hàng ngay: 'Bấm ĐẶT NGAY để nhận ưu đãi'"
        )

    # giới hạn lịch sử ~10 turns
    if len(history) > 10:
        history = history[-10:]

    try:
        r = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": sys}] + history,
            temperature=0.6,
        )
        return r.choices[0].message.content
    except Exception as e:
        print("GPT ERROR:", e)
        return "Dạ em đang tư vấn sản phẩm cho anh/chị ạ. Anh/chị có thể cho em biết thêm về dáng người và phong cách để em tư vấn chính xác hơn không ạ? ❤️"


# ============================================
# GỬI THÔNG TIN SẢN PHẨM - ĐÃ SỬA: NÂNG CẤP NỘI DUNG
# ============================================

def build_product_info_text(ms: str, row: dict) -> str:
    ten = row.get("Ten", "")
    gia = row.get("Gia", "")
    mota = (row.get("MoTa", "") or "").strip()
    tonkho = row.get("Tồn kho", "")
    mau = row.get("màu (Thuộc tính)", "")
    size = row.get("size (Thuộc tính)", "")

    # Ưu điểm nổi bật: format đẹp hơn
    highlight = mota
    if len(highlight) > 300:
        highlight = highlight[:280].rsplit(" ", 1)[0] + "..."

    text = f"""🌟 **[{ms}] {ten}** 🌟

✨ **ƯU ĐIỂM NỔI BẬT:**
{highlight}

🎨 **THÔNG TIN CHI TIẾT:**
"""
    if mau:
        text += f"• Màu sắc: {mau}\n"
    if size:
        text += f"• Size có sẵn: {size}\n"
    if gia:
        text += f"• Giá chỉ: **{gia}**\n"
    if tonkho:
        text += f"• Tồn kho: {tonkho}\n"
    
    text += f"""
💎 **ƯU ĐÃI KHI MUA NGAY:**
✓ Freeship 30K cho đơn đầu tiên
✓ Giảm thêm 5% khi đặt qua chat
✓ Tặng voucher 50K cho lần sau
✓ Đổi trả dễ dàng trong 7 ngày

👉 **EM TƯ VẤN THÊM CHO ANH/CHỊ NHÉ:**
1. Dáng người của anh/chị thế nào?
2. Thích phong cách gì (công sở, dạo phố, đi tiệc)?
3. Màu sắc yêu thích?

Hoặc anh/chị có thể bấm **"ĐẶT NGAY"** để chốt đơn trong 2 phút! 🛍️❤️"""
    
    return text


def send_product_info(uid: str, ms: str):
    load_products()
    ms = ms.upper()
    if ms not in PRODUCTS:
        send_message(uid, "Dạ em chưa tìm thấy mã này trong kho ạ, anh/chị gửi lại giúp em mã sản phẩm hoặc ảnh mẫu nhé.")
        return

    row = PRODUCTS[ms]
    info_text = build_product_info_text(ms, row)
    send_message(uid, info_text)

    # Gửi tất cả ảnh (loại trùng) – tối đa 5 ảnh
    images_field = row.get("Images", "")
    urls = parse_image_urls(images_field)
    urls = urls[:5]  # tránh spam
    for u in urls:
        final_url = rehost_image(u)
        send_image(uid, final_url)
        time.sleep(0.5)  # Thêm delay nhỏ giữa các ảnh để tránh spam


def send_recommendations(uid: str):
    """
    Gửi 5 sản phẩm gợi ý khi khách chủ động inbox mà chưa có MS nào.
    """
    load_products()
    if not PRODUCTS:
        return

    prods = list(PRODUCTS.values())[:5]
    send_message(uid, "✨ **EM GỬI ANH/CHỊ 5 MẪU HOT NHẤT TUẦN NÀY:**\n(Các chị em đang săn đón nhiều lắm ạ 💕)")

    for row in prods:
        ms = row.get("MS", "")
        ten = row.get("Ten", "")
        gia = row.get("Gia", "")
        txt = f"🔥 **[{ms}] {ten}**"
        if gia:
            txt += f"\n💰 Giá chỉ: {gia}"
        send_message(uid, txt)

        images_field = row.get("Images", "")
        urls = parse_image_urls(images_field)
        if urls:
            final_url = rehost_image(urls[0])
            send_image(uid, final_url)
            time.sleep(0.5)


# ============================================
# GREETING - ĐÃ SỬA: NÂNG CẤP CÂU CHÀO
# ============================================

def maybe_greet(uid: str, ctx: dict, has_ms: bool):
    """
    Chào khách chuyên nghiệp, cuốn hút
    """
    if ctx["greeted"]:
        return

    # Nếu có inbox_entry_ms -> luồng comment/referral, đã có tin nhắn Fchat chào trước -> bot không chào nữa
    if ctx.get("inbox_entry_ms"):
        return

    msg = """🌸 **CHÀO MỪNG BẠN ĐẾN VỚI FASHION SHOP PREMIUM!** 🌸

Xin chào anh/chị! Em là **MIU** - trợ lý ảo của shop, rất vui được hỗ trợ bạn ❤️

🎯 **EM CÓ THỂ GIÚP BẠN:**
✓ Tư vấn set đồ phù hợp với dáng người
✓ Chọn size chuẩn, đẹp dáng
✓ Hỗ trợ đặt hàng nhanh trong 2 phút
✓ Tư vấn mix & match phong cách

💎 **ƯU ĐÃI ĐẶC BIỆT HÔM NAY:**
• Freeship 30K cho đơn đầu tiên
• Giảm thêm 5% khi đặt qua chat
• Tặng voucher 50K cho lần sau
• Đổi trả dễ dàng trong 7 ngày

👇 Dưới đây là 5 mẫu **HOT NHẤT TUẦN** được các chị em săn đón ạ!"""
    
    send_message(uid, msg)
    ctx["greeted"] = True

    # Gửi carousel sản phẩm
    if not has_ms and not ctx["carousel_sent"]:
        send_product_carousel(uid)  # Gửi carousel thay vì từng sản phẩm
        ctx["carousel_sent"] = True
        ctx["recommended_sent"] = True


# ============================================
# HANDLE IMAGE MESSAGE
# ============================================

def handle_image(uid: str, image_url: str):
    load_products()
    ctx = USER_CONTEXT[uid]

    # Luồng gửi ảnh thường là khách chủ động -> cho phép chào
    if not ctx["greeted"] and not ctx.get("inbox_entry_ms"):
        maybe_greet(uid, ctx, has_ms=False)

    hosted = rehost_image(image_url)
    ms, desc = gpt_analyze_image(hosted)
    print("VISION RESULT:", ms, desc)

    if ms and ms in PRODUCTS:
        ctx["vision_ms"] = ms
        ctx["last_ms"] = ms
        ctx["product_info_sent_ms"] = ms

        send_message(uid, f"✨ **ẢNH NÀY GIỐNG MẪU [{ms}] CỦA SHOP ĐÓ Ạ!**\nEm gửi thông tin chi tiết cho anh/chị tham khảo nhé 💕")
        send_product_info(uid, ms)
    else:
        send_message(
            uid,
            "Dạ hình này hơi khó nhận mẫu chính xác ạ, anh/chị gửi giúp em caption hoặc mã sản phẩm để em kiểm tra cho chuẩn nhé.\n\nHoặc anh/chị có thể mô tả:\n• Dáng người của mình\n• Phong cách yêu thích\n• Ngân sách dự kiến\n\nEm sẽ tư vấn mẫu phù hợp nhất ạ! ❤️",
        )


# ============================================
# HANDLE TEXT MESSAGE - ĐÃ SỬA: THÊM ICON LINK ĐẶT HÀNG
# ============================================

def handle_text(uid: str, text: str):
    """
    Xử lý tin nhắn text từ khách
    """
    load_products()
    ctx = USER_CONTEXT[uid]

    # Kiểm tra nếu đang ở trạng thái điền form đặt hàng
    if handle_order_form_step(uid, text):
        return

    # 1. Cập nhật mã từ chính tin nhắn
    ms_from_text = extract_ms(text)
    if not ms_from_text:
        short = extract_short_code(text)
        if short:
            ms_from_text = find_ms_by_short_code(short)

    if ms_from_text:
        ctx["last_ms"] = ms_from_text

    # 2. MS tổng hợp từ nhiều nguồn (ƯU TIÊN last_ms nếu nó hợp lệ)
    ms = resolve_best_ms(ctx)

    # 3. Nếu là direct inbox (không có inbox_entry_ms) -> chào theo chuẩn
    maybe_greet(uid, ctx, has_ms=bool(ms))

    # 4. Nếu đã có MS nhưng chưa từng gửi thông tin sản phẩm -> gửi card sản phẩm trước
    if ms and ms in PRODUCTS and ctx.get("product_info_sent_ms") != ms:
        ctx["product_info_sent_ms"] = ms
        send_product_info(uid, ms)

    # 5. GPT tư vấn theo ngữ cảnh & sản phẩm (nếu có)
    ctx["history"].append({"role": "user", "content": text})

    product = PRODUCTS.get(ms) if ms and ms in PRODUCTS else None
    reply = gpt_reply(ctx["history"], product)
    ctx["history"].append({"role": "assistant", "content": reply})
    send_message(uid, reply)

    # 6. Nếu tin nhắn khách có ý định đặt hàng -> gửi link form đặt hàng với icon hấp dẫn
    lower = text.lower()
    if ms and ms in PRODUCTS and any(kw in lower for kw in ORDER_KEYWORDS):
        # Gửi link form đặt hàng với icon hấp dẫn
        send_order_link_with_icon(uid, ms)


# ============================================
# SEND ORDER LINK WITH ICON - HÀM MỚI: GỬI LINK VỚI ICON HẤP DẪN
# ============================================

def send_order_link_with_icon(uid: str, ms: str):
    """
    Gửi link form đặt hàng với icon hấp dẫn
    """
    base = DOMAIN or ""
    if base and not base.startswith("http"):
        base = "https://" + base
    
    # Tạo URL đặt hàng
    url = f"{base}/order-form?ms={quote(ms)}&uid={quote(uid)}"
    
    # Tin nhắn với icon hấp dẫn
    msg = f"""🎁 **ĐẶT HÀNG NHANH - NHẬN ƯU ĐÃI NGAY** 🎁

✨ Bấm vào link dưới đây để đặt hàng nhanh và nhận ưu đãi đặc biệt:
🔗 {url}

💎 **ƯU ĐÃI KHI ĐẶT NGAY:**
✓ Freeship 30K cho đơn đầu tiên
✓ Giảm thêm 5% khi đặt qua chat
✓ Tặng voucher 50K cho lần mua sau
✓ Đổi trả dễ dàng trong 7 ngày

⏰ **ĐẶT NGAY để nhận hàng sớm nhất!**
(Form đặt hàng chỉ mất 2 phút thôi ạ)"""
    
    send_message(uid, msg)


# Giữ nguyên hàm send_order_link cũ để tương thích
def send_order_link(uid: str, ms: str):
    """
    Gửi link form đặt hàng cho khách (phiên bản cũ, giữ để tương thích)
    """
    send_order_link_with_icon(uid, ms)


# ============================================
# ECHO & REF / FCHAT
# ============================================

def extract_ms_from_ref(ref: str | None):
    if not ref:
        return None
    return extract_ms(ref)


def handle_echo_outgoing(page_id: str, user_id: str, text: str, mid: str = ""):
    """
    Tin nhắn do PAGE / FCHAT gửi (is_echo = true).
    Bot không trả lời, chỉ dùng để lưu MS:
      - COMMENT flow: Fchat auto msg chứa [MS000046]...
    """
    if not user_id:
        return
    ms = extract_ms(text)
    if ms:
        ctx = USER_CONTEXT[user_id]
        ctx["inbox_entry_ms"] = ms
        ctx["last_ms"] = ms
        print(f"[ECHO] Ghi nhận mã từ page/Fchat cho user {user_id}: {ms}")


# ============================================
# WEBHOOK
# ============================================

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        if request.args.get("hub.verify_token") == VERIFY_TOKEN:
            return request.args.get("hub.challenge")
        return "Token không hợp lệ", 403

    data = request.get_json() or {}
    print("WEBHOOK:", json.dumps(data, ensure_ascii=False))

    for entry in data.get("entry", []):
        for ev in entry.get("messaging", []):
            sender_id = ev.get("sender", {}).get("id")
            recipient_id = ev.get("recipient", {}).get("id")
            message = ev.get("message", {}) or {}

            if not sender_id:
                continue

            # 1) ECHO: tin nhắn do page/Fchat gửi - XỬ LÝ ĐẦU TIÊN
            if message.get("is_echo"):
                text = message.get("text") or ""
                mid = message.get("mid") or ""
                attachments = message.get("attachments", [])
                
                # Kiểm tra nếu message_id này đã được bot gửi (tránh xử lý lặp)
                ctx = USER_CONTEXT[sender_id]
                if mid in ctx.get("sent_message_ids", set()):
                    print(f"[ECHO SKIP] Bỏ qua echo của tin nhắn bot đã gửi: {mid}")
                    continue
                    
                # Nếu là echo của text (từ Fchat/PAGE)
                if text:
                    handle_echo_outgoing(page_id=sender_id, user_id=recipient_id, text=text, mid=mid)
                # Nếu là echo của attachments (ảnh bot vừa gửi) - KHÔNG XỬ LÝ
                elif attachments:
                    print(f"[ECHO SKIP] Bỏ qua echo attachments từ bot: {mid}")
                continue

            # từ đây trở xuống: sender_id = user
            ctx = USER_CONTEXT[sender_id]

            # 2) POSTBACK HANDLER - THÊM CHỐNG LẶP MẠNH
            if "postback" in ev:
                current_time = time.time()
                # Chống lặp: nếu postback mới cách postback cũ < 10 giây thì bỏ qua
                if current_time - ctx.get("last_postback_time", 0) < 10:
                    print(f"[POSTBACK SKIP] Bỏ qua postback lặp (cách {current_time - ctx.get('last_postback_time', 0):.1f}s)")
                    return "ok"
                
                ctx["last_postback_time"] = current_time
                
                payload = ev["postback"].get("payload")
                print(f"[POSTBACK] User {sender_id}: {payload}")
                
                # Xử lý order quick replies
                if payload == "ORDER_PROVIDE_NAME":
                    ctx["order_state"] = "waiting_name"
                    send_message(sender_id, "👤 Vui lòng nhập họ tên người nhận hàng:")
                    return "ok"
                elif payload == "ORDER_PROVIDE_PHONE":
                    ctx["order_state"] = "waiting_phone"
                    send_message(sender_id, "📱 Vui lòng nhập số điện thoại (ví dụ: 0912345678 hoặc +84912345678):")
                    return "ok"
                elif payload == "ORDER_PROVIDE_ADDRESS":
                    ctx["order_state"] = "waiting_address"
                    send_message(sender_id, "🏠 Vui lòng nhập địa chỉ giao hàng chi tiết:")
                    return "ok"
                elif payload == "ORDER_CONFIRM":
                    send_order_confirmation(sender_id)
                    return "ok"
                elif payload == "ORDER_EDIT":
                    ctx["order_state"] = "waiting_name"
                    send_message(sender_id, "✏️ Vui lòng nhập lại họ tên người nhận:")
                    return "ok"
                
                # Xử lý postback từ carousel
                if payload and payload.startswith("VIEW_"):
                    product_code = payload.replace("VIEW_", "")
                    # Kiểm tra nếu đã gửi sản phẩm này gần đây (trong 30 giây)
                    if ctx.get("product_info_sent_ms") == product_code and current_time - ctx.get("last_postback_time", 0) < 30:
                        send_message(sender_id, "Bạn đang xem sản phẩm này rồi ạ. Cần em hỗ trợ gì thêm không?")
                        return "ok"
                    
                    # Gửi thông tin sản phẩm chi tiết
                    if product_code in PRODUCTS:
                        ctx["last_ms"] = product_code
                        ctx["product_info_sent_ms"] = product_code
                        send_product_info(sender_id, product_code)
                    else:
                        send_message(sender_id, f"Dạ em không tìm thấy sản phẩm mã {product_code} ạ.")
                    return "ok"
                    
                elif payload and payload.startswith("SELECT_"):
                    product_code = payload.replace("SELECT_", "")
                    # Xử lý khi khách chọn sản phẩm - GIỮ NGUYÊN POSTBACK ĐỂ TƯƠNG THÍCH
                    if product_code in PRODUCTS:
                        ctx["last_ms"] = product_code
                        ctx["product_info_sent_ms"] = product_code
                        
                        product_info = PRODUCTS[product_code]
                        response = f"""✅ **BẠN ĐÃ CHỌN SẢN PHẨM {product_code}!** 

🛍️ **{product_info.get('Ten', '')}**

💎 **ĐỂ EM HỖ TRỢ ĐẶT HÀNG NHANH:**
1. Size bạn muốn đặt là gì?
2. Màu sắc bạn thích?
3. Số lượng cần mua?

🎁 **ƯU ĐÃI KHI ĐẶT NGAY:**
• Freeship 30K • Giảm 5% • Voucher 50K

📝 Bạn có thể nhắn **"Đặt hàng"** hoặc bấm **"ĐẶT NGAY"** trên carousel để hoàn tất đơn nhé! ❤️"""
                        send_message(sender_id, response)
                    else:
                        send_message(sender_id, f"Dạ em không tìm thấy sản phẩm mã {product_code} ạ.")
                    return "ok"

                # Xử lý referral trong postback (nếu có)
                ref = ev["postback"].get("referral", {}).get("ref")
                if ref:
                    ms_ref = extract_ms_from_ref(ref)
                    if ms_ref:
                        ctx["inbox_entry_ms"] = ms_ref
                        ctx["last_ms"] = ms_ref
                        print(f"[REF] Nhận mã từ referral: {ms_ref}")
                        
                        # Nếu là luồng referral, không chào
                        ctx["greeted"] = True
                        
                        # Gửi thông tin sản phẩm
                        send_product_info(sender_id, ms_ref)
                        return "ok"
                
                # Nếu postback không có ref hoặc payload không phải từ carousel
                if not ctx["greeted"]:
                    maybe_greet(sender_id, ctx, has_ms=False)
                send_message(sender_id, "Anh/chị cho em biết đang quan tâm mẫu nào hoặc gửi ảnh mẫu để em xem giúp ạ.")
                return "ok"

            # 3) REFERRAL (nhấn nút Inbox, hoặc quảng cáo Click-to-Message)
            ref = ev.get("referral", {}).get("ref") \
                or ev.get("postback", {}).get("referral", {}).get("ref")
            if ref:
                ms_ref = extract_ms_from_ref(ref)
                if ms_ref:
                    ctx["inbox_entry_ms"] = ms_ref
                    ctx["last_ms"] = ms_ref
                    print(f"[REF] Nhận mã từ referral: {ms_ref}")

            # 4) ATTACHMENTS → ảnh (CHỈ xử lý khi KHÔNG phải echo)
            if "message" in ev and "attachments" in message:
                # Đảm bảo không phải echo message
                if not message.get("is_echo"):
                    for att in message["attachments"]:
                        if att.get("type") == "image":
                            image_url = att["payload"].get("url")
                            if image_url:
                                handle_image(sender_id, image_url)
                                return "ok"
                continue

            # 5) TEXT (CHỈ xử lý khi KHÔNG phải echo)
            if "message" in ev and "text" in message:
                # Đảm bảo không phải echo message
                if not message.get("is_echo"):
                    text = message.get("text", "")
                    handle_text(sender_id, text)
                    return "ok"

    return "ok"


# ============================================
# API LẤY THÔNG TIN PAGE - MỚI THÊM
# ============================================

@app.route('/api/page-info')
def get_page_info():
    """Lấy thông tin fanpage từ Facebook API"""
    try:
        # Nếu đã cache thông tin page, trả về luôn
        if USER_CONTEXT["global"].get("page_info"):
            return jsonify(USER_CONTEXT["global"]["page_info"])
        
        response = requests.get(
            f'https://graph.facebook.com/v20.0/{PAGE_ID}',
            params={
                'access_token': PAGE_ACCESS_TOKEN, 
                'fields': 'name,about,cover'
            }
        )
        
        if response.status_code == 200:
            page_data = response.json()
            page_info = {
                'success': True,
                'page_name': page_data.get('name', 'Fashion Shop Premium'),
                'page_about': page_data.get('about', 'Chuyên thời trang cao cấp cho phái đẹp'),
                'cover_photo': page_data.get('cover', {}).get('source', '')
            }
            # Cache thông tin page
            USER_CONTEXT["global"]["page_info"] = page_info
            return jsonify(page_info)
        else:
            return jsonify({
                'success': False,
                'page_name': 'Fashion Shop Premium',
                'page_about': 'Chuyên thời trang cao cấp cho phái đẹp',
                'cover_photo': ''
            })
    except Exception as e:
        print("GET PAGE INFO ERROR:", e)
        return jsonify({
            'success': False,
            'page_name': 'Fashion Shop Premium',
            'page_about': 'Chuyên thời trang cao cấp cho phái đẹp',
            'cover_photo': ''
        })


# ============================================
# ORDER FORM & API - ĐÃ SỬA: THÊM SIZE/MÀU VÀ PAGE INFO
# ============================================

@app.route("/order-form")
def order_form():
    # Lấy thông tin page
    page_info_response = get_page_info()
    page_info = page_info_response.get_json()
    
    # Lấy thông tin sản phẩm từ query params
    ms = request.args.get("ms", "").upper()
    uid = request.args.get("uid", "")
    
    if not ms:
        return "Thiếu mã sản phẩm", 400
    
    # Trả về template với thông tin page
    return send_from_directory("static", "order-form.html")


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
    
    # Lấy thông tin size và màu từ sản phẩm
    size_field = row.get("size (Thuộc tính)", "")
    color_field = row.get("màu (Thuộc tính)", "")
    
    # Parse size và màu thành list
    sizes = []
    if size_field:
        # Có thể là "S, M, L, XL" hoặc "S-M-L-XL"
        sizes = [s.strip() for s in re.split(r'[,/|-]', size_field) if s.strip()]
    
    colors = []
    if color_field:
        # Có thể là "Đen, Trắng, Đỏ" hoặc "Đen-Trắng-Đỏ"
        colors = [c.strip() for c in re.split(r'[,/|-]', color_field) if c.strip()]
    
    # Nếu không có size/color, cung cấp options mặc định
    if not sizes:
        sizes = ["S", "M", "L", "XL", "XXL"]
    if not colors:
        colors = ["Đen", "Trắng", "Kaki", "Xám", "Hồng", "Xanh Navy"]

    return {
        "ms": ms,
        "name": row.get("Ten", ""),
        "price": row.get("Gia", ""),
        "desc": row.get("MoTa", ""),
        "image": image,
        "sizes": sizes,
        "colors": colors,
        "stock": row.get("Tồn kho", ""),
    }


@app.route("/api/order", methods=["POST"])
def api_order():
    data = request.json or {}
    print("ORDER RECEIVED:", data)

    uid = data.get("uid") or data.get("user_id")
    ms = (data.get("ms") or data.get("product_code") or "").upper()

    if uid:
        msg = (
            f"✅ **ĐƠN HÀNG ĐÃ ĐƯỢC XÁC NHẬN!**\n"
            f"────────────────────\n"
            f"🛍️ Sản phẩm: {data.get('productName', '')} ({ms})\n"
            f"💰 Giá: {data.get('price', data.get('total', ''))}\n"
            f"🎨 Màu: {data.get('color', '')}\n"
            f"📏 Size: {data.get('size', '')}\n"
            f"📦 Số lượng: {data.get('quantity', '')}\n"
            f"💵 Thành tiền: {data.get('total', '')}\n"
            f"────────────────────\n"
            f"👤 Người nhận: {data.get('customerName', '')}\n"
            f"📱 SĐT: {data.get('phone', '')}\n"
            f"🏠 Địa chỉ: {data.get('home', '')}, {data.get('ward', '')}, {data.get('district', '')}, {data.get('province', '')}\n"
            f"────────────────────\n"
            f"📝 Ghi chú: {data.get('note', 'Không có ghi chú')}\n"
            f"────────────────────\n"
            f"⏰ Thời gian đặt: {datetime.now().strftime('%d/%m/%Y %H:%M')}\n"
            f"🚚 Dự kiến giao: 2-4 ngày làm việc\n"
            f"💳 Thanh toán: COD (nhận hàng trả tiền)\n"
            f"────────────────────\n"
            f"Trong ít phút nữa bên em sẽ gọi xác nhận, anh/chị để ý điện thoại giúp em nha! ❤️"
        )
        send_message(uid, msg)

    return {"status": "ok", "message": "Đơn hàng đã được ghi nhận"}


# ============================================
# HEALTHCHECK & START
# ============================================

@app.route("/")
def home():
    load_products()
    return f"Chatbot OK – {len(PRODUCTS)} products loaded."


if __name__ == "__main__":
    load_products(force=True)
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
