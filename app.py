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
FANPAGE_NAME       = os.getenv("FANPAGE_NAME", "Shop")

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ============================================
# GLOBAL STATE
# ============================================

USER_CONTEXT = defaultdict(lambda: {
    "last_ms": None,
    "inbox_entry_ms": None,
    "vision_ms": None,
    "caption_ms": None,
    "history": [],
    "greeted": False,
    "recommended_sent": False,
    "product_info_sent_ms": None,
    "carousel_sent": False,
    "last_postback_time": 0,
    "sent_message_ids": set(),
    "order_state": None,
    "order_data": {},
    "last_message_time": 0,
    "last_product_info_time": 0,
    "get_started_processed": False,  # Thêm biến này
})

PRODUCTS = {}
LAST_LOAD = 0
LOAD_TTL = 300

# Cache cho ảnh đã rehost
IMAGE_REHOST_CACHE = {}

# User processing lock to prevent duplicate processing
USER_PROCESSING_LOCK = {}

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
        r = requests.post(url, params=params, json=payload, timeout=10)
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
    try:
        files = {
            "filedata": ("image.jpg", requests.get(image_url, timeout=10).content, "image/jpeg")
        }
    except Exception as e:
        print(f"DOWNLOAD IMG ERROR: {e}, URL: {image_url}")
        return ""

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
            timeout=15
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
# CAROUSEL TEMPLATE - FIXED
# ============================================

def send_carousel_template(recipient_id: str, products_data: list) -> str:
    try:
        elements = []
        for product in products_data[:10]:
            image_field = product.get("Images", "")
            image_urls = parse_image_urls(image_field)
            original_image_url = image_urls[0] if image_urls else ""
            
            if not original_image_url:
                continue
            
            # Sử dụng URL gốc trực tiếp thay vì rehost (vì Facebook chặn domain whitelist)
            final_image_url = original_image_url
            
            # Sửa lỗi domain - đảm bảo có https://
            domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
            order_link = f"{domain}/order-form?ms={product.get('MS', '')}&uid={recipient_id}"
                
            element = {
                "title": f"[{product.get('MS', '')}] {product.get('Ten', '')}",
                "subtitle": f"💰 Giá: {product.get('Gia', '')}\n{product.get('MoTa', '')[:60]}..." if product.get('MoTa') else f"💰 Giá: {product.get('Gia', '')}",
                "image_url": final_image_url,
                "buttons": [
                    {
                        "type": "postback",
                        "title": "📋 Xem chi tiết",
                        "payload": f"VIEW_{product.get('MS', '')}"
                    },
                    {
                        "type": "web_url",
                        "title": "🛒 Chọn sản phẩm",
                        "url": order_link,
                        "webview_height_ratio": "tall",
                        "messenger_extensions": False,  # Đặt thành False vì domain chưa whitelist
                        "webview_share_button": "hide"
                    }
                ]
            }
            elements.append(element)
        
        if not elements:
            print("Không có sản phẩm nào có ảnh để hiển thị trong carousel")
            return ""
        
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
        
        r = requests.post(url, params=params, json=payload, timeout=10)
        print("SEND CAROUSEL:", r.status_code, r.text)
        
        if r.status_code == 200:
            response = r.json()
            message_id = response.get("message_id", "")
            if message_id:
                USER_CONTEXT[recipient_id]["sent_message_ids"].add(message_id)
            return message_id
        elif r.status_code == 400 and "2018062" in r.text:
            print("⚠️ LỖI CAROUSEL: Domain chưa được whitelist!")
            # Fallback: gửi dạng text thay vì carousel
            return ""
        return ""
        
    except Exception as e:
        print("SEND CAROUSEL ERROR:", e)
        return ""


def send_product_carousel(recipient_id: str) -> None:
    load_products()
    if not PRODUCTS:
        return
    
    products = list(PRODUCTS.values())[:5]
    message_id = send_carousel_template(recipient_id, products)
    
    # Nếu carousel không gửi được, gửi danh sách text thay thế
    if not message_id:
        send_message(recipient_id, "Em gửi anh/chị 5 mẫu đang được nhiều khách quan tâm:")
        for i, product in enumerate(products[:5], 1):
            ms = product.get('MS', '')
            ten = product.get('Ten', '')
            gia = product.get('Gia', '')
            send_message(recipient_id, f"{i}. [{ms}] {ten}\n💰 Giá: {gia}")
            time.sleep(0.1)


# ============================================
# CDN IMAGE UPLOAD FUNCTION (giữ lại cho các tính năng khác)
# ============================================

def rehost_image_to_cdn(image_url: str) -> str:
    """
    Hàm này giữ lại nhưng chỉ trả về URL gốc do vấn đề whitelist domain
    """
    # Vì Facebook không cho whitelist domain, chúng ta sử dụng URL gốc
    return image_url


# ============================================
# ORDER FORM FUNCTIONS
# ============================================

def send_order_form_quick_replies(uid: str, product_info: dict) -> None:
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
            timeout=10
        )
        print("SEND ORDER FORM:", r.status_code, r.text)
    except Exception as e:
        print("SEND ORDER FORM ERROR:", e)


def send_order_confirmation(uid: str) -> None:
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
    
    ctx["order_state"] = None
    ctx["order_data"] = {}


def handle_order_form_step(uid: str, text: str) -> bool:
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
        phone_pattern = r'^(0|\+84)[1-9]\d{8}$'
        phone = text.strip().replace(" ", "")
        
        if not re.match(phone_pattern, phone):
            send_message(uid, "❌ Số điện thoại không hợp lệ. Vui lòng nhập lại số điện thoại (ví dụ: 0912345678 hoặc +84912345678):")
            return True
            
        ctx["order_data"]["phone"] = phone
        ctx["order_state"] = "waiting_address"
        send_message(uid, "✅ Đã lưu số điện thoại: " + phone)
        send_message(uid, "🏠 Vui lòng nhập địa chỉ giao hàng chi tiết (số nhà, đường, phường/xã, tỉnh/thành phố):")
        return True
        
    elif order_state == "waiting_address":
        if len(text.strip()) < 10:
            send_message(uid, "❌ Địa chỉ quá ngắn. Vui lòng nhập địa chỉ chi tiết hơn:")
            return True
            
        ctx["order_data"]["address"] = text.strip()
        ctx["order_state"] = "confirming"
        
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
                timeout=10
            )
            print("SEND ORDER CONFIRM:", r.status_code, r.text)
        except Exception as e:
            print("SEND ORDER CONFIRM ERROR:", e)
            
        return True
        
    return False


def start_order_process(uid: str, ms: str) -> None:
    load_products()
    
    if ms not in PRODUCTS:
        send_message(uid, "❌ Không tìm thấy thông tin sản phẩm. Vui lòng thử lại.")
        return
    
    product_row = PRODUCTS[ms]
    ctx = USER_CONTEXT[uid]
    
    ctx["order_data"] = {
        "product_info": {
            "ms": ms,
            "name": f"[{ms}] {product_row.get('Ten', '')}",
            "price": product_row.get('Gia', ''),
            "color": product_row.get('màu (Thuộc tính)', ''),
            "size": product_row.get('size (Thuộc tính)', '')
        }
    }
    
    send_order_form_quick_replies(uid, ctx["order_data"]["product_info"])
    ctx["order_state"] = "waiting_name"


# ============================================
# REHOST IMAGE (giữ lại cho tương thích)
# ============================================

def rehost_image(url: str) -> str:
    """Giữ lại hàm cũ cho tương thích với các phần code khác"""
    return rehost_image_to_cdn(url)


# ============================================
# LOAD SẢN PHẨM TỪ SHEET
# ============================================

def load_products(force: bool = False) -> None:
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
    if ctx.get("last_ms") and ctx["last_ms"] in PRODUCTS:
        return ctx["last_ms"]
    
    for key in ["vision_ms", "inbox_entry_ms", "caption_ms"]:
        if ctx.get(key) and ctx[key] in PRODUCTS:
            return ctx[key]
    return None


# ============================================
# GPT CONTEXT ENGINE
# ============================================

def gpt_reply(history: list, product_row: dict | None):
    if not client:
        return "Dạ hệ thống AI đang bận, anh/chị chờ em 1 lát với ạ."

    sys = """
    Bạn là trợ lý bán hàng của shop quần áo.
    - Xưng "em", gọi khách là "anh/chị".
    - Trả lời ngắn gọn, lịch sự, dễ hiểu.
    - Không bịa đặt chất liệu/giá/ưu đãi nếu không có trong dữ liệu.
    - Nếu đã biết sản phẩm khách đang xem, hãy:
      + Tóm tắt mẫu, giá, ưu điểm.
      + Gợi ý size/màu phù hợp.
      + Hỏi thêm 1 câu để chốt (size, màu hoặc đặt hàng).
    - Nếu CHƯA biết sản phẩm:
      + Hỏi rõ nhu cầu (mục đích, dáng người, ngân sách).
      + Gợi ý hướng lựa chọn chung, không tự đặt mã.
    """

    if product_row:
        tonkho = product_row.get("Tồn kho", "")
        mau = product_row.get("màu (Thuộc tính)", "")
        size = product_row.get("size (Thuộc tính)", "")
        sys += (
            f"\nDữ liệu sản phẩm hiện tại:\n"
            f"- Tên: {product_row.get('Ten', '')}\n"
            f"- Mô tả: {product_row.get('MoTa', '')}\n"
            f"- Giá bán: {product_row.get('Gia', '')}\n"
            f"- Tồn kho: {tonkho}\n"
            f"- Màu: {mau}\n"
            f"- Size: {size}\n"
        )

    if len(history) > 10:
        history = history[-10:]

    r = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "system", "content": sys}] + history,
        temperature=0.5,
    )
    return r.choices[0].message.content


# ============================================
# GỬI THÔNG TIN SẢN PHẨM
# ============================================

def build_product_info_text(ms: str, row: dict) -> str:
    ten = row.get("Ten", "")
    gia = row.get("Gia", "")
    mota = (row.get("MoTa", "") or "").strip()
    tonkho = row.get("Tồn kho", "")
    mau = row.get("màu (Thuộc tính)", "")
    size = row.get("size (Thuộc tính)", "")

    highlight = mota
    if len(highlight) > 350:
        highlight = highlight[:330].rsplit(" ", 1)[0] + "..."

    text = f"[{ms}] {ten}\n"
    text += f"\n✨ Ưu điểm nổi bật:\n- {highlight}\n" if highlight else ""
    if mau or size:
        text += "\n🎨 Màu/Size:\n"
        if mau:
            text += f"- Màu: {mau}\n"
        if size:
            text += f"- Size: {size}\n"
    if gia:
        text += f"\n💰 Giá bán: {gia}\n"
    if tonkho:
        text += f"📦 Tồn kho: {tonkho}\n"
    text += "\n👉 Anh/chị xem giúp em mẫu này có hợp gu không, nếu ưng em tư vấn thêm màu/size và chốt đơn cho mình ạ. ❤️"
    return text


def send_product_info(uid: str, ms: str, force_send_images: bool = True):
    load_products()
    ms = ms.upper()
    if ms not in PRODUCTS:
        send_message(uid, "Dạ em chưa tìm thấy mã này trong kho ạ, anh/chị gửi lại giúp em mã sản phẩm hoặc ảnh mẫu nhé.")
        return

    ctx = USER_CONTEXT[uid]
    current_time = time.time()
    
    # Kiểm tra thời gian gửi product info lần cuối
    if ctx.get("last_product_info_time") and current_time - ctx["last_product_info_time"] < 5:
        print(f"[SKIP] Đã gửi product info cho {uid} quá gần đây")
        return
    
    row = PRODUCTS[ms]
    info_text = build_product_info_text(ms, row)
    send_message(uid, info_text)
    
    # Gửi link form đặt hàng
    domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
    order_link = f"{domain}/order-form?ms={ms}&uid={uid}"
    send_message(uid, f"📋 Anh/chị có thể đặt hàng ngay tại đây:\n{order_link}")

    # Gửi 5 ảnh (sử dụng URL gốc)
    if force_send_images:
        images_field = row.get("Images", "")
        urls = parse_image_urls(images_field)
        urls = urls[:5]  # Gửi 5 ảnh đầu tiên
        
        for u in urls:
            send_image(uid, u)
            time.sleep(0.2)  # Giảm thời gian chờ
    
    # Cập nhật thời gian
    ctx["product_info_sent_ms"] = ms
    ctx["last_product_info_time"] = current_time
    ctx["last_message_time"] = current_time


def send_recommendations(uid: str):
    load_products()
    if not PRODUCTS:
        return

    prods = list(PRODUCTS.values())[:5]
    send_message(uid, "Em gửi anh/chị 5 mẫu đang được nhiều khách quan tâm, mình tham khảo thử ạ:")
    send_product_carousel(uid)


# ============================================
# GREETING
# ============================================

def maybe_greet(uid: str, ctx: dict, has_ms: bool):
    if ctx["greeted"]:
        return

    if ctx.get("inbox_entry_ms"):
        return

    msg = (
        "Em chào anh/chị 😊\n"
        "Em là trợ lý chăm sóc khách hàng của shop, hỗ trợ anh/chị xem mẫu, tư vấn size và chốt đơn nhanh ạ."
    )
    send_message(uid, msg)
    ctx["greeted"] = True

    if not has_ms and not ctx["carousel_sent"]:
        send_message(uid, "Em gửi anh/chị 5 mẫu đang được nhiều khách quan tâm, mình tham khảo thử ạ:")
        send_product_carousel(uid)
        ctx["carousel_sent"] = True
        ctx["recommended_sent"] = True


# ============================================
# HANDLE IMAGE MESSAGE
# ============================================

def handle_image(uid: str, image_url: str):
    load_products()
    ctx = USER_CONTEXT[uid]

    if not ctx["greeted"] and not ctx.get("inbox_entry_ms"):
        maybe_greet(uid, ctx, has_ms=False)

    hosted = rehost_image_to_cdn(image_url)
    ms, desc = gpt_analyze_image(hosted)
    print("VISION RESULT:", ms, desc)

    if ms and ms in PRODUCTS:
        ctx["vision_ms"] = ms
        ctx["last_ms"] = ms
        ctx["product_info_sent_ms"] = ms

        send_message(uid, f"Dạ ảnh này giống mẫu [{ms}] của shop đó anh/chị, em gửi thông tin sản phẩm cho mình nhé. 💕")
        send_product_info(uid, ms)
    else:
        send_message(
            uid,
            "Dạ hình này hơi khó nhận mẫu chính xác ạ, anh/chị gửi giúp em caption hoặc mã sản phẩm để em kiểm tra cho chuẩn nhé.",
        )


# ============================================
# HANDLE TEXT MESSAGE
# ============================================

def handle_text(uid: str, text: str):
    # Kiểm tra lock để tránh xử lý trùng
    if uid in USER_PROCESSING_LOCK and USER_PROCESSING_LOCK[uid]:
        print(f"[SKIP] User {uid} đang được xử lý")
        return
    
    USER_PROCESSING_LOCK[uid] = True
    
    try:
        load_products()
        ctx = USER_CONTEXT[uid]

        if handle_order_form_step(uid, text):
            return

        ms_from_text = extract_ms(text)
        if not ms_from_text:
            short = extract_short_code(text)
            if short:
                ms_from_text = find_ms_by_short_code(short)

        if ms_from_text:
            ctx["last_ms"] = ms_from_text

        ms = resolve_best_ms(ctx)
        maybe_greet(uid, ctx, has_ms=bool(ms))

        # Chỉ gửi product info khi có mã mới hoặc chưa gửi gần đây
        current_time = time.time()
        if ms and ms in PRODUCTS and ctx.get("product_info_sent_ms") != ms:
            # Kiểm tra thời gian gửi gần nhất
            if current_time - ctx.get("last_message_time", 0) > 2:
                ctx["product_info_sent_ms"] = ms
                send_product_info(uid, ms)
                ctx["last_message_time"] = current_time

        ctx["history"].append({"role": "user", "content": text})

        product = PRODUCTS.get(ms) if ms and ms in PRODUCTS else None
        reply = gpt_reply(ctx["history"], product)
        ctx["history"].append({"role": "assistant", "content": reply})
        
        # Chỉ gửi reply nếu không phải đang trong order process
        if not ctx.get("order_state"):
            send_message(uid, reply)

        lower = text.lower()
        if ms and ms in PRODUCTS and any(kw in lower for kw in ORDER_KEYWORDS):
            domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
            order_link = f"{domain}/order-form?ms={ms}&uid={uid}"
            send_message(uid, f"📋 Anh/chị có thể đặt hàng ngay tại đây:\n{order_link}")
    
    finally:
        USER_PROCESSING_LOCK[uid] = False


# ============================================
# ECHO & REF / FCHAT
# ============================================

def extract_ms_from_ref(ref: str | None):
    if not ref:
        return None
    return extract_ms(ref)


def handle_echo_outgoing(page_id: str, user_id: str, text: str, mid: str = ""):
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

            # XỬ LÝ ECHO - QUAN TRỌNG: tránh xử lý tin nhắn lặp
            if message.get("is_echo"):
                text = message.get("text") or ""
                mid = message.get("mid") or ""
                attachments = message.get("attachments", [])
                
                # Kiểm tra trong sent_message_ids của recipient (user)
                ctx = USER_CONTEXT.get(recipient_id, {})
                if mid in ctx.get("sent_message_ids", set()):
                    print(f"[ECHO SKIP] Bỏ qua echo của tin nhắn bot đã gửi: {mid}")
                    continue
                    
                if text:
                    handle_echo_outgoing(page_id=sender_id, user_id=recipient_id, text=text, mid=mid)
                elif attachments:
                    print(f"[ECHO SKIP] Bỏ qua echo attachments từ bot: {mid}")
                continue

            ctx = USER_CONTEXT[sender_id]

            if "postback" in ev:
                current_time = time.time()
                if current_time - ctx.get("last_postback_time", 0) < 2:  # Giảm thời gian chống spam
                    print(f"[POSTBACK SKIP] Bỏ qua postback lặp (cách {current_time - ctx.get('last_postback_time', 0):.1f}s)")
                    return "ok"
                
                ctx["last_postback_time"] = current_time
                
                payload = ev["postback"].get("payload")
                print(f"[POSTBACK] User {sender_id}: {payload}")
                
                # XỬ LÝ GET_STARTED_PAYLOAD - CHỈ CHẠY 1 LẦN
                if payload == "GET_STARTED_PAYLOAD":
                    # Kiểm tra đã xử lý GET_STARTED chưa
                    if ctx.get("get_started_processed"):
                        print(f"[POSTBACK SKIP] Đã xử lý GET_STARTED cho user {sender_id}")
                        return "ok"
                    
                    # Đánh dấu đã xử lý GET_STARTED
                    ctx["get_started_processed"] = True
                    
                    # Chỉ gửi chào hỏi nếu chưa chào
                    if not ctx["greeted"]:
                        maybe_greet(sender_id, ctx, has_ms=False)
                    
                    # Chỉ gửi tin nhắn nhắc nếu chưa gửi carousel
                    if not ctx["carousel_sent"]:
                        send_message(sender_id, "Anh/chị cho em biết đang quan tâm mẫu nào hoặc gửi ảnh mẫu để em xem giúp ạ.")
                    return "ok"
                
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
                
                if payload and payload.startswith("VIEW_"):
                    product_code = payload.replace("VIEW_", "")
                    
                    # Kiểm tra nếu đã gửi sản phẩm này gần đây
                    if ctx.get("product_info_sent_ms") == product_code and current_time - ctx.get("last_product_info_time", 0) < 10:
                        send_message(sender_id, "Bạn đang xem sản phẩm này rồi ạ. Cần em hỗ trợ gì thêm không?")
                        return "ok"
                    
                    if product_code in PRODUCTS:
                        ctx["last_ms"] = product_code
                        send_product_info(sender_id, product_code)
                    else:
                        send_message(sender_id, f"Dạ em không tìm thấy sản phẩm mã {product_code} ạ.")
                    return "ok"
                    
                elif payload and payload.startswith("SELECT_"):
                    product_code = payload.replace("SELECT_", "")
                    domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
                    order_link = f"{domain}/order-form?ms={product_code}&uid={sender_id}"
                    response_msg = f"📋 Anh/chị có thể đặt hàng sản phẩm [{product_code}] ngay tại đây:\n{order_link}"
                    send_message(sender_id, response_msg)
                    return "ok"

                ref = ev["postback"].get("referral", {}).get("ref")
                if ref:
                    ms_ref = extract_ms_from_ref(ref)
                    if ms_ref:
                        ctx["inbox_entry_ms"] = ms_ref
                        ctx["last_ms"] = ms_ref
                        print(f"[REF] Nhận mã từ referral: {ms_ref}")
                        ctx["greeted"] = True
                        send_product_info(sender_id, ms_ref)
                        return "ok"
                
                if not ctx["greeted"]:
                    maybe_greet(sender_id, ctx, has_ms=False)
                send_message(sender_id, "Anh/chị cho em biết đang quan tâm mẫu nào hoặc gửi ảnh mẫu để em xem giúp ạ.")
                return "ok"

            ref = ev.get("referral", {}).get("ref") \
                or ev.get("postback", {}).get("referral", {}).get("ref")
            if ref:
                ms_ref = extract_ms_from_ref(ref)
                if ms_ref:
                    ctx["inbox_entry_ms"] = ms_ref
                    ctx["last_ms"] = ms_ref
                    print(f"[REF] Nhận mã từ referral: {ms_ref}")

            if "message" in ev and "attachments" in message:
                if not message.get("is_echo"):
                    for att in message["attachments"]:
                        if att.get("type") == "image":
                            image_url = att["payload"].get("url")
                            if image_url:
                                handle_image(sender_id, image_url)
                                return "ok"
                continue

            if "message" in ev and "text" in message:
                if not message.get("is_echo"):
                    text = message.get("text", "")
                    handle_text(sender_id, text)
                    return "ok"

    return "ok"


# ============================================
# ORDER FORM & API
# ============================================

def send_order_link(uid: str, ms: str):
    domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
    url = f"{domain}/order-form?ms={quote(ms)}&uid={quote(uid)}"
    msg = f"Anh/chị có thể đặt hàng nhanh tại đây ạ: {url}"
    send_message(uid, msg)


@app.route("/o/<ms>")
def order_link(ms: str):
    load_products()
    ms = ms.upper()
    if ms not in PRODUCTS:
        return f"Không tìm thấy sản phẩm {ms}", 404
    pd_row = PRODUCTS[ms]
    ten = pd_row["Ten"]
    gia = pd_row["Gia"]
    mota = pd_row["MoTa"]
    return f"""
    <html><body>
    <h2>Đặt hàng {ms}</h2>
    <p><b>Tên:</b> {ten}</p>
    <p><b>Giá:</b> {gia}</p>
    <p><b>Mô tả:</b> {mota}</p>
    </body></html>
    """


@app.route("/order-form")
def order_form():
    ms = request.args.get("ms", "")
    uid = request.args.get("uid", "")
    
    if not ms:
        return """
        <html>
        <body style="text-align: center; padding: 50px; font-family: Arial, sans-serif;">
            <h2 style="color: #FF3B30;">⚠️ Không tìm thấy sản phẩm</h2>
            <p>Vui lòng quay lại Messenger và chọn sản phẩm để đặt hàng.</p>
            <a href="/" style="color: #1DB954; text-decoration: none; font-weight: bold;">Quay về trang chủ</a>
        </body>
        </html>
        """, 400
    
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
    price_match = re.search(r'(\d[\d.,]*)', price_str)
    price = 0
    if price_match:
        price_str_clean = price_match.group(1).replace(',', '').replace('.', '')
        try:
            price = int(price_str_clean)
        except:
            price = 0

    return {
        "ms": ms,
        "name": row.get("Ten", ""),
        "price": price,
        "price_display": row.get("Gia", "0"),
        "desc": row.get("MoTa", ""),
        "image": image,
        "page_name": FANPAGE_NAME,
        "sizes": sizes,
        "colors": colors
    }


@app.route("/api/order", methods=["POST"])
def api_order():
    data = request.json or {}
    print("ORDER RECEIVED:", json.dumps(data, indent=2))

    uid = data.get("uid") or data.get("user_id")
    ms = (data.get("ms") or data.get("product_code") or "").upper()

    if uid:
        load_products()
        product_name = ""
        if ms in PRODUCTS:
            product_name = PRODUCTS[ms].get("Ten", "")
        
        address_components = [
            data.get('home', ''),
            data.get('ward', ''),
            data.get('province', '')
        ]
        address = ", ".join([comp for comp in address_components if comp])
        
        msg = (
            "✅ SHOP ĐÃ NHẬN ĐƠN CỦA ANH/CHỊ!\n"
            "────────────────────\n"
            f"🛍️ Sản phẩm: {product_name} ({ms})\n"
            f"🎨 Màu: {data.get('color', '')}\n"
            f"📏 Size: {data.get('size', '')}\n"
            f"📦 Số lượng: {data.get('quantity', '')}\n"
            f"💰 Thành tiền: {data.get('total', '')}\n"
            f"👤 Người nhận: {data.get('customerName', '')}\n"
            f"📱 SĐT: {data.get('phone', '')}\n"
            f"🏠 Địa chỉ: {address}\n"
            "────────────────────\n"
            "⏰ Shop sẽ gọi điện xác nhận trong 5-10 phút.\n"
            "💳 Thanh toán khi nhận hàng (COD)\n"
            "────────────────────\n"
            "Cảm ơn anh/chị đã đặt hàng! ❤️"
        )
        send_message(uid, msg)

    return {"status": "ok", "message": "Đơn hàng đã được tiếp nhận"}


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
