import os
import re
import csv
import json
import time
import hmac
import hashlib
import threading
from collections import defaultdict

import requests
from flask import Flask, request, jsonify, render_template_string

# ============================================
# CẤU HÌNH CƠ BẢN
# ============================================

PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN", "")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "verify_token_mau")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
FANPAGE_NAME = os.getenv("FANPAGE_NAME", "Shop Thời Trang")
DOMAIN = os.getenv("DOMAIN", "https://example.com")

# URL Google Sheet CSV
GOOGLE_SHEET_CSV_URL = os.getenv("GOOGLE_SHEET_CSV_URL", "")

# Cache sản phẩm
PRODUCTS = {}
LAST_LOAD = 0
LOAD_TTL = 300  # 300 giây

# Đảm bảo thread-safe cho USER_CONTEXT
USER_CONTEXT = defaultdict(
    lambda: {
        "history": [],
        "last_ms": None,
        "current_product_ms": None,
        "greeted": False,
        "carousel_sent": False,
        "processing_lock": False,
        "last_postback_payload": None,
        "last_postback_time": 0,
        "postback_count": 0,
        "order_state": None,
        "order_info": {},
        "last_message_time": 0,
        "product_info_sent_ms": None,
        "last_product_info_time": 0,
        "sent_message_ids": set(),
    }
)

# Khóa dùng cho debounce gửi sản phẩm theo mã
DEBOUNCE_LOCK = threading.Lock()

# Các từ khóa liên quan đến đặt hàng
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
# HÀM GỌI FACEBOOK SEND API
# ============================================


def call_facebook_send_api(payload: dict):
    if not PAGE_ACCESS_TOKEN:
        print("PAGE_ACCESS_TOKEN chưa được cấu hình.")
        return ""
    url = f"https://graph.facebook.com/v16.0/me/messages?access_token={PAGE_ACCESS_TOKEN}"
    try:
        print("SEND PAYLOAD:", json.dumps(payload, ensure_ascii=False))
        r = requests.post(url, json=payload, timeout=15)
        r.raise_for_status()
        data = r.json()
        print("SEND RESPONSE:", json.dumps(data, ensure_ascii=False))

        # Lưu lại message_id để tránh xử lý echo
        if "message_id" in data.get("message", {}):
            message_id = data["message"]["message_id"]
            recipient_id = payload.get("recipient", {}).get("id")
            if recipient_id:
                ctx = USER_CONTEXT[recipient_id]
                ctx["sent_message_ids"].add(message_id)
            return message_id
        return ""
    except Exception as e:
        print("SEND MSG ERROR:", e)
        return ""


def send_message(recipient_id: str, text: str):
    if not text:
        return ""
    payload = {
        "recipient": {"id": recipient_id},
        "message": {"text": text},
    }
    return call_facebook_send_api(payload)


def send_image(recipient_id: str, image_url: str):
    if not image_url:
        return ""
    payload = {
        "recipient": {"id": recipient_id},
        "message": {
            "attachment": {
                "type": "image",
                "payload": {"url": image_url, "is_reusable": True},
            }
        },
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
                "payload": {"template_type": "generic", "elements": elements},
            }
        },
    }
    return call_facebook_send_api(payload)


def send_quick_replies(recipient_id: str, text: str, quick_replies: list):
    if not quick_replies:
        return send_message(recipient_id, text)
    payload = {
        "recipient": {"id": recipient_id},
        "message": {"text": text, "quick_replies": quick_replies},
    }
    return call_facebook_send_api(payload)


def parse_image_urls(raw: str):
    if not raw:
        return []
    parts = re.split(r"[,\n;|]+", raw)
    urls = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        if "alicdn.com" in p or "taobao" in p or "1688.com" in p or p.startswith("http"):
            urls.append(p)
    # Loại trùng
    seen = set()
    result = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            result.append(u)
    return result


def should_use_as_first_image(url: str):
    # Không loại watermark Trung Quốc theo yêu cầu: chỉ bỏ trùng
    if not url:
        return False
    return True


def short_description(text: str, limit: int = 220) -> str:
    """Rút gọn mô tả sản phẩm cho dễ đọc trong chat."""
    if not text:
        return ""
    clean = re.sub(r"\s+", " ", str(text)).strip()
    if len(clean) <= limit:
        return clean
    return clean[:limit].rstrip() + "..."


def extract_price_int(price_str: str):
    """Trả về giá dạng int từ chuỗi '849.000đ', '849,000'... Nếu không đọc được trả về None."""
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
    Đọc dữ liệu từ Google Sheet CSV, cache trong 300s.

    PHƯƠNG ÁN A:
    - Mỗi dòng trong sheet = 1 biến thể (màu/size/giá/tồn kho).
    - Gom các biến thể cùng Mã sản phẩm.
    - Vẫn giữ cấu trúc row cũ để không phá vỡ logic khác.
    """
    global PRODUCTS, LAST_LOAD
    now = time.time()
    if not force and PRODUCTS and (now - LAST_LOAD) < LOAD_TTL:
        return

    try:
        print(f"🟦 Loading sheet: {GOOGLE_SHEET_CSV_URL}")
        r = requests.get(GOOGLE_SHEET_CSV_URL, timeout=20)
        r.raise_for_status()
        r.encoding = "utf-8"
        content = r.text

        reader = csv.DictReader(content.splitlines())
        products: dict[str, dict] = {}
        for raw_row in reader:
            row = dict(raw_row)

            ms = (row.get("Mã sản phẩm") or "").strip()
            if not ms:
                continue

            ten = (row.get("Tên sản phẩm") or "").strip()
            if not ten:
                continue

            gia_raw = (row.get("Giá bán") or "").strip()
            images = (row.get("Images") or "").strip()
            videos = (row.get("Videos") or "").strip()
            tonkho_raw = (row.get("Tồn kho") or "").strip()
            mota = (row.get("Mô tả") or "").strip()
            mau = (row.get("màu (Thuộc tính)") or "").strip()
            size = (row.get("size (Thuộc tính)") or "").strip()

            # Chuẩn hóa giá & tồn kho cho biến thể
            gia_int = extract_price_int(gia_raw)
            try:
                tonkho_int = int(str(tonkho_raw)) if str(tonkho_raw).strip() else None
            except Exception:
                tonkho_int = None

            if ms not in products:
                # Khởi tạo sản phẩm gốc, giữ cấu trúc như cũ
                base = {
                    "MS": ms,
                    "Ten": ten,
                    "Gia": gia_raw,
                    "MoTa": mota,
                    "Images": images,
                    "Videos": videos,
                    "Tồn kho": tonkho_raw,
                    "màu (Thuộc tính)": mau,
                    "size (Thuộc tính)": size,
                }
                base["variants"] = []
                base["all_colors"] = set()
                base["all_sizes"] = set()
                products[ms] = base

            p = products[ms]

            # Cập nhật ảnh/video nếu chưa có
            if not p.get("Images") and images:
                p["Images"] = images
            if not p.get("Videos") and videos:
                p["Videos"] = videos

            # Thêm biến thể
            variant = {
                "mau": mau,
                "size": size,
                "gia": gia_int,
                "gia_raw": gia_raw,
                "tonkho": tonkho_int if tonkho_int is not None else tonkho_raw,
            }
            p["variants"].append(variant)

            # Gom màu & size tổng hợp
            if mau:
                p["all_colors"].add(mau)
            if size:
                p["all_sizes"].add(size)

            # Nếu chưa có mô tả thì dùng mô tả của dòng hiện tại
            if not p.get("MoTa") and mota:
                p["MoTa"] = mota

            # Nếu chưa có giá hiển thị thì dùng giá dòng đầu tiên
            if not p.get("Gia") and gia_raw:
                p["Gia"] = gia_raw

            # Tồn kho tổng (tạm để dòng đầu tiên)
            if not p.get("Tồn kho") and tonkho_raw:
                p["Tồn kho"] = tonkho_raw

        # Hậu xử lý: chuyển set → chuỗi và tạo mô tả ngắn
        for ms, p in products.items():
            colors = sorted(list(p.get("all_colors") or []))
            sizes = sorted(list(p.get("all_sizes") or []))
            p["màu (Thuộc tính)"] = (
                ", ".join(colors) if colors else p.get("màu (Thuộc tính)", "")
            )
            p["size (Thuộc tính)"] = (
                ", ".join(sizes) if sizes else p.get("size (Thuộc tính)", "")
            )
            p["ShortDesc"] = short_description(p.get("MoTa", ""))

        PRODUCTS = products
        LAST_LOAD = now
        print(f"📦 Loaded {len(PRODUCTS)} products (PHƯƠNG ÁN A).")
    except Exception as e:
        print("❌ load_products ERROR:", e)


# ============================================
# GPT PROMPT
# ============================================


def build_product_system_prompt(product: dict | None, ms: str | None):
    base = (
        "Bạn là trợ lý bán hàng thời trang cho shop Facebook. "
        "Nhiệm vụ của bạn là tư vấn size, màu, chất liệu, giá, tồn kho và hỗ trợ chốt đơn. "
        "Luôn trả lời bằng tiếng Việt thân thiện, xưng 'em' và gọi khách là 'anh/chị'. "
        "Nếu không chắc thông tin (ví dụ thiếu trong dữ liệu sản phẩm) thì nói rõ là không chắc, "
        "và gợi ý khách inbox để được hỗ trợ thêm.\n\n"
    )

    if not product or not ms:
        base += (
            "Hiện tại bạn KHÔNG có dữ liệu chi tiết sản phẩm. "
            "Hãy trả lời chung chung, khéo léo xin khách gửi mã sản phẩm hoặc hình ảnh "
            "để bạn kiểm tra lại trên hệ thống.\n"
        )
        return base

    ten = product.get("Ten", "")
    gia = product.get("Gia", "")
    mau = product.get("màu (Thuộc tính)", "")
    size = product.get("size (Thuộc tính)", "")
    tonkho = product.get("Tồn kho", "")
    mota = product.get("MoTa", "")

    base += f"Thông tin sản phẩm hiện tại (Mã: {ms}):\n"
    base += f"- Tên: {ten}\n"
    base += f"- Giá: {gia}\n"
    if mau:
        base += f"- Màu: {mau}\n"
    if size:
        base += f"- Size: {size}\n"
    if tonkho:
        base += f"- Tồn kho: {tonkho}\n"
    if mota:
        base += f"- Mô tả: {mota}\n"

    base += (
        "\nKhi khách hỏi về sản phẩm này, hãy ưu tiên dùng các thông tin trên. "
        "Câu trả lời cần ngắn gọn, dễ hiểu, không lặp lại toàn bộ mô tả dài nếu không cần thiết. "
        "Luôn kết thúc bằng việc gợi ý khách để lại SĐT và địa chỉ để chốt đơn nhanh nếu khách đã ưng.\n"
    )

    return base


# ============================================
# GPT CHAT FUNCTION
# ============================================


def gpt_reply(history: list, product: dict | None, ms: str | None) -> str:
    """
    Gọi OpenAI GPT để trả lời.
    """
    if not OPENAI_API_KEY:
        return (
            "Dạ hiện tại em chưa được cấu hình API để tư vấn thông minh hơn. "
            "Anh/chị có thể hỏi trực tiếp về giá, size, màu hoặc để lại SĐT để shop gọi tư vấn ạ."
        )

    system_msg = build_product_system_prompt(product, ms)

    messages = [{"role": "system", "content": system_msg}]
    for item in history[-10:]:
        messages.append(item)

    try:
        url = "https://api.openai.com/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": "gpt-4o-mini",
            "messages": messages,
            "temperature": 0.7,
            "max_tokens": 500,
        }

        print("GPT REQUEST:", json.dumps(payload, ensure_ascii=False))
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        print("GPT RESPONSE:", json.dumps(data, ensure_ascii=False))

        reply = data["choices"][0]["message"]["content"]
        return reply.strip()
    except Exception as e:
        print("GPT ERROR:", e)
        return (
            "Dạ hiện tại em đang gặp chút trục trặc khi tư vấn tự động. "
            "Anh/chị cho em xin mã sản phẩm hoặc hình ảnh, em sẽ hỗ trợ theo thông tin có sẵn ạ."
        )


# ============================================
# HELPER XỬ LÝ MÃ SẢN PHẨM / SHORT CODE
# ============================================


def extract_ms(text: str) -> str | None:
    if not text:
        return None
    # Mã sản phẩm dạng [MS0001] hoặc MS0001
    m = re.search(r"\b(MS\d{6})\b", text.upper())
    if m:
        return m.group(1)
    return None


def extract_short_code(text: str) -> str | None:
    """
    Tìm short code dạng S1, S01, A1, B2,... trong câu.
    """
    if not text:
        return None
    m = re.search(r"\b([A-Z]\d{1,2})\b", text.upper())
    if m:
        return m.group(1)
    return None


def find_ms_by_short_code(short_code: str) -> str | None:
    """
    Dò trong PRODUCTS xem có cột nào chứa short_code (ví dụ cột 'Mã mẫu mã' hoặc 'Keyword mẫu mã').
    Ở đây giả sử Google Sheet đã có những cột đó.
    """
    if not short_code:
        return None
    load_products()
    sc = short_code.upper()
    for ms, row in PRODUCTS.items():
        for col in ["Mã mẫu mã", "Keyword mẫu mã"]:
            val = (row.get(col) or "").upper()
            if sc in val.split():
                return ms
    return None


# ============================================
# ORDER FORM STATE MACHINE
# ============================================


def reset_order_state(ctx: dict):
    ctx["order_state"] = None
    ctx["order_info"] = {}


def handle_order_form_step(uid: str, text: str) -> bool:
    """
    Xử lý từng bước trong quy trình nhập form đặt hàng qua chat.
    Trả về True nếu tin nhắn đã được xử lý cho luồng order.
    """
    ctx = USER_CONTEXT[uid]
    state = ctx.get("order_state")

    if not state:
        return False

    info = ctx.setdefault("order_info", {})

    if state == "waiting_name":
        info["name"] = text.strip()
        ctx["order_state"] = "waiting_phone"
        send_message(uid, "📱 Anh/chị vui lòng nhập số điện thoại người nhận:")
        return True

    if state == "waiting_phone":
        phone = re.sub(r"\D", "", text)
        if len(phone) < 9:
            send_message(
                uid,
                "Số điện thoại chưa đúng định dạng ạ. Anh/chị nhập lại giúp em (ít nhất 9 số) ạ.",
            )
            return True
        info["phone"] = phone
        ctx["order_state"] = "waiting_address"
        send_message(uid, "🏠 Anh/chị cho em xin địa chỉ nhận hàng chi tiết:")
        return True

    if state == "waiting_address":
        info["address"] = text.strip()
        ctx["order_state"] = "confirm"
        summary = (
            "✅ Thông tin anh/chị cung cấp:\n"
            f"- Họ tên: {info.get('name','')}\n"
            f"- SĐT: {info.get('phone','')}\n"
            f"- Địa chỉ: {info.get('address','')}\n"
        )
        send_message(uid, summary)
        send_quick_replies(
            uid,
            "Anh/chị kiểm tra lại giúp em. Nếu đúng rồi bấm 'Xác nhận', nếu muốn sửa bấm 'Sửa thông tin' ạ.",
            [
                {
                    "content_type": "text",
                    "title": "Xác nhận",
                    "payload": "ORDER_CONFIRM",
                },
                {
                    "content_type": "text",
                    "title": "Sửa thông tin",
                    "payload": "ORDER_EDIT",
                },
            ],
        )
        return True

    return False


# ============================================
# GỬI CAROUSEL TOP SẢN PHẨM
# ============================================


def send_top_products_carousel(uid: str, limit=5):
    load_products()
    elements = []
    count = 0
    for ms, row in PRODUCTS.items():
        if count >= limit:
            break
        title = row.get("Ten", f"Sản phẩm {ms}")
        subtitle = row.get("Gia", "")
        images = parse_image_urls(row.get("Images", ""))
        image_url = images[0] if images else ""

        domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
        order_link = f"{domain}/order-form?ms={ms}&uid={uid}"

        buttons = [
            {
                "type": "postback",
                "title": "Xem chi tiết",
                "payload": f"VIEW_{ms}",
            },
            {
                "type": "web_url",
                "url": order_link,
                "title": "Đặt hàng nhanh",
            },
        ]

        elements.append(
            {
                "title": f"[{ms}] {title}",
                "subtitle": subtitle,
                "image_url": image_url,
                "buttons": buttons,
            }
        )
        count += 1

    if elements:
        send_carousel_template(uid, elements)
        ctx = USER_CONTEXT[uid]
        ctx["carousel_sent"] = True


# ============================================
# OPENAI SIGNATURE (CHO CÁC TÍNH NĂNG NÂNG CAO, NẾU CÓ)
# ============================================


def verify_openai_signature(req) -> bool:
    """
    Hàm mẫu nếu sau này bạn cần verify signature của OpenAI Webhook (không bắt buộc).
    """
    return True


# ============================================
# GREETING
# ============================================


def maybe_greet(uid: str, ctx: dict, has_ms: bool):
    now = time.time()
    if ctx["greeted"]:
        return
    if now - ctx.get("last_message_time", 0) < 5:
        return

    ctx["greeted"] = True
    ctx["last_message_time"] = now

    send_message(
        uid,
        "Em chào anh/chị 😊\nEm là trợ lý chăm sóc khách hàng của shop, hỗ trợ anh/chị xem mẫu, tư vấn size và chốt đơn nhanh ạ.",
    )


def handle_image(uid: str, image_url: str):
    """
    Khi khách gửi ảnh, bot sẽ gửi sang Fchat nếu có cấu hình,
    đồng thời nhắc khách cung cấp thêm thông tin.
    """
    ctx = USER_CONTEXT[uid]
    ctx["last_message_time"] = time.time()

    send_message(
        uid,
        "Dạ em đã nhận được hình anh/chị gửi ạ. Em sẽ kiểm tra mẫu tương tự cho anh/chị.\n"
        "Trong lúc chờ, anh/chị cho em xin thêm thông tin về size/màu anh/chị thích nhé.",
    )


# ============================================
# XỬ LÝ TEXT
# ============================================


def handle_text(uid: str, text: str):
    ctx = USER_CONTEXT[uid]
    ctx["last_message_time"] = time.time()

    load_products()

    # Reset postback counter khi có text mới
    ctx["postback_count"] = 0

    # Xử lý order form trước
    if handle_order_form_step(uid, text):
        return

    # Tìm mã sản phẩm trong tin nhắn
    ms_from_text = extract_ms(text)
    if not ms_from_text:
        short = extract_short_code(text)
        if short:
            ms_from_text = find_ms_by_short_code(short)

    # Cập nhật last_ms nếu tìm thấy mã
    if ms_from_text:
        ctx["last_ms"] = ms_from_text
        ctx["current_product_ms"] = ms_from_text
        print(f"[TEXT] User {uid} đang hỏi về sản phẩm {ms_from_text}")

    ms = ctx.get("current_product_ms")
    product = PRODUCTS.get(ms) if ms else None

    # Lưu history
    ctx["history"].append({"role": "user", "content": text})

    lower_text = text.lower()

    # Trả lời nhanh dựa trên sản phẩm hiện tại nếu có
    if product and ms:
        if any(keyword in lower_text for keyword in ["giá", "bao nhiêu tiền", "nhiêu tiền"]):
            reply = f"Dạ sản phẩm [{ms}] {product.get('Ten','')} đang có giá {product.get('Gia','')} ạ.\nAnh/chị muốn em tư vấn thêm về size hoặc màu không ạ?"
            send_message(uid, reply)
            ctx["history"].append({"role": "assistant", "content": reply})
            return
        elif any(
            keyword in lower_text
            for keyword in ["size nào", "có size", "size gì", "size nào", "size bao nhiêu"]
        ):
            size_info = product.get("size (Thuộc tính)", "Không có thông tin")
            reply = f"Dạ sản phẩm này có các size: {size_info}\n\nAnh/chị quan tâm size nào ạ?"
            send_message(uid, reply)
            ctx["history"].append({"role": "assistant", "content": reply})
            return
        elif any(
            keyword in lower_text
            for keyword in ["màu nào", "có màu", "màu gì", "màu nào", "màu sắc"]
        ):
            color_info = product.get("màu (Thuộc tính)", "Không có thông tin")
            reply = f"Dạ sản phẩm này có các màu: {color_info}\n\nAnh/chị quan tâm màu nào ạ?"
            send_message(uid, reply)
            ctx["history"].append({"role": "assistant", "content": reply})
            return
        elif any(
            keyword in lower_text
            for keyword in ["tồn kho", "còn hàng", "hết hàng", "bao nhiêu cái"]
        ):
            stock_info = product.get("Tồn kho", "Không có thông tin")
            reply = f"Dạ sản phẩm này hiện còn {stock_info} cái trong kho ạ.\n\nAnh/chị muốn đặt bao nhiêu ạ?"
            send_message(uid, reply)
            ctx["history"].append({"role": "assistant", "content": reply})
            return
        elif any(
            keyword in lower_text
            for keyword in ["xem hàng", "xem sản phẩm", "xem mẫu", "có được xem"]
        ):
            desc = product.get("MoTa", "Sản phẩm có sẵn để xem và đặt hàng ạ.")
            reply = (
                f"Dạ anh/chị có thể xem hàng qua hình ảnh em đã gửi. {desc[:100]}...\n\n"
                "Anh/chị muốn xem thêm hình ảnh nào không ạ?"
            )
            send_message(uid, reply)
            ctx["history"].append({"role": "assistant", "content": reply})
            return

    # Gọi GPT để trả lời với thông tin sản phẩm hiện tại
    reply = gpt_reply(ctx["history"], product, ms)
    ctx["history"].append({"role": "assistant", "content": reply})

    # Chỉ gửi reply nếu không phải đang trong order process
    if not ctx.get("order_state"):
        send_message(uid, reply)

    # Kiểm tra từ khóa đặt hàng
    lower = text.lower()
    if ms and ms in PRODUCTS and any(kw in lower for kw in ORDER_KEYWORDS):
        domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
        order_link = f"{domain}/order-form?ms={ms}&uid={uid}"
        send_message(uid, f"📋 Anh/chị có thể đặt hàng ngay tại đây:\n{order_link}")


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
        ctx["last_ms"] = ms
        ctx["current_product_ms"] = ms
        ctx["history"].append({"role": "assistant", "content": text})
        print(f"[ECHO OUTGOING] page={page_id}, user={user_id}, ms={ms}, mid={mid}")


# ============================================
# WEBHOOK
# ============================================

app = Flask(__name__)


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
            message = ev.get("message", {})
            postback = ev.get("postback")
            is_echo = message.get("is_echo", False)

            if not sender_id:
                continue

            # BỎ QUA ECHO CỦA BOT (đã có message_id)
            if is_echo:
                mid = message.get("mid")
                text = message.get("text", "")
                attachments = message.get("attachments", [])

                ctx = USER_CONTEXT[recipient_id or sender_id]
                if mid and mid in ctx.get("sent_message_ids", set()):
                    print(f"[ECHO SKIP] Bỏ qua echo của tin nhắn bot đã gửi: {mid}")
                    continue

                if text:
                    handle_echo_outgoing(
                        page_id=sender_id, user_id=recipient_id, text=text, mid=mid
                    )
                elif attachments:
                    print(f"[ECHO SKIP] Bỏ qua echo attachments từ bot: {mid}")
                continue

            ctx = USER_CONTEXT[sender_id]

            # KIỂM TRA LOCK ĐỂ TRÁNH XỬ LÝ TRÙNG
            if ctx.get("processing_lock"):
                # Nếu lock đã giữ quá 5s thì coi như bị kẹt và mở lại
                if time.time() - ctx.get("last_message_time", 0) > 5:
                    ctx["processing_lock"] = False
                else:
                    print(f"[SKIP] User {sender_id} đang được xử lý, bỏ qua sự kiện mới")
                    return "ok"

            # SET LOCK
            ctx["processing_lock"] = True
            ctx["last_message_time"] = time.time()
            try:
                if "postback" in ev:
                    current_time = time.time()
                    payload = ev["postback"].get("payload")

                    # KIỂM TRA DEBOUNCE: NẾU CÙNG PAYLOAD TRONG VÒNG 3 GIÂY THÌ BỎ QUA
                    if (
                        payload == ctx.get("last_postback_payload")
                        and current_time - ctx.get("last_postback_time", 0) < 3
                    ):
                        print(
                            f"[DEBOUNCE] Bỏ qua postback trùng lặp payload={payload} user={sender_id}"
                        )
                        return "ok"

                    ctx["last_postback_payload"] = payload
                    ctx["last_postback_time"] = current_time
                    ctx["postback_count"] = ctx.get("postback_count", 0) + 1

                    # Nếu postback quá nhiều trong thời gian ngắn -> gửi cảnh báo nhẹ
                    if ctx["postback_count"] > 5:
                        send_message(
                            sender_id,
                            "Dạ em thấy anh/chị thao tác khá nhiều, nếu cần hỗ trợ gì cứ nhắn cho em nhé.",
                        )

                    # XỬ LÝ NÚT BẤM ORDER FORM
                    if payload and payload.startswith("ORDER_"):
                        if payload == "ORDER_PROVIDE_NAME":
                            ctx["order_state"] = "waiting_name"
                            send_message(
                                sender_id, "👤 Vui lòng nhập họ tên người nhận hàng:"
                            )
                            return "ok"
                        elif payload == "ORDER_PROVIDE_PHONE":
                            ctx["order_state"] = "waiting_phone"
                            send_message(
                                sender_id,
                                "📱 Vui lòng nhập số điện thoại (ví dụ: 0912345678 hoặc +84912345678):",
                            )
                            return "ok"
                        elif payload == "ORDER_PROVIDE_ADDRESS":
                            ctx["order_state"] = "waiting_address"
                            send_message(
                                sender_id, "🏠 Vui lòng nhập địa chỉ giao hàng chi tiết:"
                            )
                            return "ok"
                        elif payload == "ORDER_CONFIRM":
                            send_order_confirmation(sender_id)
                            return "ok"
                        elif payload == "ORDER_EDIT":
                            ctx["order_state"] = "waiting_name"
                            send_message(
                                sender_id,
                                "✏️ Vui lòng nhập lại họ tên người nhận:",
                            )
                            return "ok"

                    # XỬ LÝ VIEW PRODUCT
                    if payload and payload.startswith("VIEW_"):
                        product_code = payload.replace("VIEW_", "")

                        # KIỂM TRA NẾU ĐÃ GỬI SẢN PHẨM NÀY GẦN ĐÂY (10 GIÂY)
                        if (
                            ctx.get("product_info_sent_ms") == product_code
                            and current_time - ctx.get("last_product_info_time", 0) < 10
                        ):
                            print(
                                f"[PRODUCT INFO SKIP] Đã gửi {product_code} gần đây"
                            )
                            send_message(
                                sender_id,
                                f"Bạn đang xem sản phẩm {product_code}. Cần em hỗ trợ gì thêm không ạ?",
                            )
                            return "ok"

                        if product_code in PRODUCTS:
                            ctx["last_ms"] = product_code
                            ctx["current_product_ms"] = product_code
                            send_product_info_debounced(sender_id, product_code)
                        else:
                            send_message(
                                sender_id,
                                f"Dạ em không tìm thấy sản phẩm mã {product_code} ạ.",
                            )
                        return "ok"

                    elif payload and payload.startswith("SELECT_"):
                        product_code = payload.replace("SELECT_", "")
                        if product_code in PRODUCTS:
                            ctx["last_ms"] = product_code
                            ctx["current_product_ms"] = product_code
                            send_product_info_debounced(sender_id, product_code)
                        else:
                            send_message(
                                sender_id,
                                f"Dạ em không tìm thấy sản phẩm mã {product_code} ạ.",
                            )
                        return "ok"

                    elif payload == "SHOW_MORE_PRODUCTS":
                        send_top_products_carousel(sender_id, limit=10)
                        return "ok"

                    elif payload == "CHAT_WITH_STAFF":
                        send_message(
                            sender_id,
                            "Dạ anh/chị chờ một chút, em sẽ chuyển thông tin cho nhân viên hỗ trợ ạ.",
                        )
                        return "ok"

                    elif payload == "VIEW_ORDER_FORM":
                        ms = ctx.get("current_product_ms") or ctx.get("last_ms")
                        if not ms or ms not in PRODUCTS:
                            send_message(
                                sender_id,
                                "Dạ em chưa biết anh/chị đang quan tâm mẫu nào. Anh/chị gửi giúp em mã sản phẩm hoặc hình ảnh ạ.",
                            )
                            return "ok"

                        domain = (
                            DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
                        )
                        order_link = f"{domain}/order-form?ms={ms}&uid={sender_id}"
                        send_message(
                            sender_id,
                            f"📋 Anh/chị có thể đặt hàng sản phẩm [{ms}] ngay tại đây:\n{order_link}",
                        )
                        return "ok"

                    # XỬ LÝ REFERRAL
                    ref = ev["postback"].get("referral", {}).get("ref")
                    if ref:
                        ms_ref = extract_ms_from_ref(ref)
                        if ms_ref:
                            ctx["inbox_entry_ms"] = ms_ref
                            ctx["last_ms"] = ms_ref
                            ctx["current_product_ms"] = ms_ref
                            print(f"[REF] Nhận mã từ referral: {ms_ref}")
                            ctx["greeted"] = True
                            send_product_info_debounced(sender_id, ms_ref)
                            return "ok"

                    # DEFAULT RESPONSE
                    if not ctx["greeted"]:
                        maybe_greet(sender_id, ctx, has_ms=False)
                    send_message(
                        sender_id,
                        "Anh/chị cho em biết đang quan tâm mẫu nào hoặc gửi ảnh mẫu để em xem giúp ạ.",
                    )
                    return "ok"

                # XỬ LÝ REFERRAL TỪ MESSAGING
                ref = ev.get("referral", {}).get("ref") or ev.get(
                    "postback", {}
                ).get("referral", {}).get("ref")
                if ref:
                    ms_ref = extract_ms_from_ref(ref)
                    if ms_ref:
                        ctx["inbox_entry_ms"] = ms_ref
                        ctx["last_ms"] = ms_ref
                        ctx["current_product_ms"] = ms_ref
                        print(f"[REF] Nhận mã từ referral messaging: {ms_ref}")
                        send_product_info_debounced(sender_id, ms_ref)
                        return "ok"

                # XỬ LÝ ATTACHMENTS (ẢNH)
                if "message" in ev and "attachments" in message:
                    for att in message.get("attachments", []):
                        if att.get("type") == "image":
                            image_url = att.get("payload", {}).get("url")
                            handle_image(sender_id, image_url)
                            return "ok"

                # XỬ LÝ TEXT MESSAGE
                if "message" in ev and "text" in message:
                    if not message.get("is_echo"):
                        text = message.get("text", "")
                        handle_text(sender_id, text)
                        return "ok"

            finally:
                # RELEASE LOCK
                ctx["processing_lock"] = False
                # Reset postback counter sau 10 giây
                if time.time() - ctx.get("last_postback_time", 0) > 10:
                    ctx["postback_count"] = 0

    return "ok"


# ============================================
# SEND PRODUCT INFO (DEBOUNCED)
# ============================================


def send_product_info_debounced(uid: str, ms: str):
    ctx = USER_CONTEXT[uid]
    now = time.time()

    last_ms = ctx.get("product_info_sent_ms")
    last_time = ctx.get("last_product_info_time", 0)

    if last_ms == ms and (now - last_time) < 5:
        print(f"[DEBOUNCE] Bỏ qua gửi lại thông tin sản phẩm {ms} cho user {uid}")
        return

    ctx["product_info_sent_ms"] = ms
    ctx["last_product_info_time"] = now

    load_products()
    product = PRODUCTS.get(ms)
    if not product:
        send_message(uid, f"Em không tìm thấy thông tin sản phẩm [{ms}] ạ.")
        return

    images = parse_image_urls(product.get("Images", ""))
    sent_first = False
    for url in images[:5]:
        if not should_use_as_first_image(url):
            continue
        if not sent_first:
            send_image(uid, url)
            sent_first = True
        else:
            send_image(uid, url)
        time.sleep(0.3)

    short_desc = product.get("ShortDesc") or short_description(product.get("MoTa", ""))
    detail = (
        f"📌 Thông tin sản phẩm [{ms}] {product.get('Ten','')}:\n"
        f"- Giá: {product.get('Gia','')}\n"
        f"- Màu: {product.get('màu (Thuộc tính)','')}\n"
        f"- Size: {product.get('size (Thuộc tính)','')}\n"
        f"- Tồn kho: {product.get('Tồn kho','')}\n\n"
        f"{short_desc}"
    )

    send_message(uid, detail)

    domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
    order_link = f"{domain}/order-form?ms={ms}&uid={uid}"
    send_message(uid, f"📋 Anh/chị có thể đặt hàng ngay tại đây:\n{order_link}")


# ============================================
# ORDER FORM & API - CẢI THIỆN
# ============================================


@app.route("/order-form")
def order_form():
    ms = (request.args.get("ms") or "").upper()
    uid = request.args.get("uid") or ""

    load_products()
    if ms not in PRODUCTS:
        return "Không tìm thấy sản phẩm.", 404

    row = PRODUCTS[ms]
    images_field = row.get("Images", "")
    urls = parse_image_urls(images_field)
    image = urls[0] if urls else ""

    size_field = row.get("size (Thuộc tính)", "")
    color_field = row.get("màu (Thuộc tính)", "")

    # Xử lý size - tách bằng dấu phẩy
    sizes = []
    if size_field:
        sizes = [s.strip() for s in size_field.split(",") if s.strip()]

    # Xử lý màu - tách bằng dấu phẩy
    colors = []
    if color_field:
        colors = [c.strip() for c in color_field.split(",") if c.strip()]

    # Nếu không có size/color thì dùng mặc định
    if not sizes:
        sizes = ["Mặc định"]
    if not colors:
        colors = ["Mặc định"]

    page_name = FANPAGE_NAME or "Trang Facebook"

    # Template HTML đơn giản
    html = f"""
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8" />
    <title>Đặt hàng - {page_name}</title>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 0; padding: 0;
            background: #f5f5f5;
        }}
        .container {{
            max-width: 500px;
            margin: 0 auto;
            background: #fff;
            padding: 16px;
        }}
        .product {{
            display: flex;
            gap: 10px;
        }}
        .product img {{
            max-width: 150px;
            border-radius: 4px;
        }}
        .field {{
            margin-bottom: 12px;
        }}
        label {{
            display: block;
            font-weight: bold;
            margin-bottom: 4px;
        }}
        input, select, textarea {{
            width: 100%;
            padding: 8px;
            box-sizing: border-box;
        }}
        button {{
            background: #2b7cff;
            color: #fff;
            border: none;
            border-radius: 4px;
            padding: 10px 16px;
            cursor: pointer;
            font-size: 16px;
        }}
        button:disabled {{
            background: #ccc;
            cursor: not-allowed;
        }}
        .price-display {{
            font-size: 18px;
            font-weight: bold;
            color: #e53935;
        }}
        .note {{
            font-size: 12px;
            color: #666;
        }}
    </style>
</head>
<body>
<div class="container">
    <h2>Đặt hàng sản phẩm</h2>
    <div class="product">
        <div>
            <img src="{image}" alt="Sản phẩm" />
        </div>
        <div>
            <div><strong>{row.get("Ten","")}</strong></div>
            <div>Mã: <strong>{ms}</strong></div>
            <div class="price-display" id="priceDisplay">Đang tải giá...</div>
        </div>
    </div>

    <hr />

    <form id="orderForm">
        <input type="hidden" name="ms" value="{ms}" />
        <input type="hidden" name="uid" value="{uid}" />

        <div class="field">
            <label for="color">Màu sắc</label>
            <select name="color" id="color">
                {''.join(f'<option value="{{c}}">{{c}}</option>' for c in colors)}
            </select>
        </div>

        <div class="field">
            <label for="size">Size</label>
            <select name="size" id="size">
                {''.join(f'<option value="{{s}}">{{s}}</option>' for s in sizes)}
            </select>
        </div>

        <div class="field">
            <label for="name">Họ tên người nhận</label>
            <input type="text" id="name" name="name" required />
        </div>

        <div class="field">
            <label for="phone">Số điện thoại</label>
            <input type="tel" id="phone" name="phone" required />
        </div>

        <div class="field">
            <label for="address">Địa chỉ nhận hàng</label>
            <textarea id="address" name="address" rows="3" required></textarea>
        </div>

        <div class="field">
            <label for="note">Ghi chú thêm</label>
            <textarea id="note" name="note" rows="2"></textarea>
        </div>

        <button type="submit" id="submitBtn">Gửi đơn hàng</button>
        <p class="note">
            Sau khi gửi, shop sẽ liên hệ xác nhận đơn trước khi giao hàng.
        </p>
    </form>
</div>

<script>
const ms = "{ms}";

async function updatePrice() {{
    const colorEl = document.getElementById("color");
    const sizeEl = document.getElementById("size");
    const priceDisplay = document.getElementById("priceDisplay");

    const color = colorEl ? colorEl.value : "";
    const size = sizeEl ? sizeEl.value : "";

    priceDisplay.textContent = "Đang cập nhật giá...";

    try {{
        const url = `/api/get-variant-price?ms=${{encodeURIComponent(ms)}}&color=${{encodeURIComponent(color)}}&size=${{encodeURIComponent(size)}}`;
        const res = await fetch(url);
        if (!res.ok) {{
            throw new Error("Không lấy được giá");
        }}
        const data = await res.json();
        if (data.price_display) {{
            priceDisplay.textContent = "Giá: " + data.price_display;
        }} else {{
            priceDisplay.textContent = "Giá: Liên hệ";
        }}
    }} catch (err) {{
        console.error(err);
        priceDisplay.textContent = "Giá: Liên hệ";
    }}
}}

document.getElementById("color").addEventListener("change", updatePrice);
document.getElementById("size").addEventListener("change", updatePrice);

document.addEventListener("DOMContentLoaded", updatePrice);

const form = document.getElementById("orderForm");
form.addEventListener("submit", async function(e) {{
    e.preventDefault();
    const submitBtn = document.getElementById("submitBtn");
    submitBtn.disabled = true;
    submitBtn.textContent = "Đang gửi...";

    const formData = new FormData(form);
    const payload = {{}};
    for (const [key, value] of formData.entries()) {{
        payload[key] = value;
    }}

    try {{
        const res = await fetch("/api/order", {{
            method: "POST",
            headers: {{
                "Content-Type": "application/json"
            }},
            body: JSON.stringify(payload)
        }});
        const data = await res.json();
        alert(data.message || "Đã gửi đơn hàng thành công!");
    }} catch (err) {{
        console.error(err);
        alert("Có lỗi xảy ra khi gửi đơn hàng. Anh/chị thử lại giúp em nhé.");
    }} finally {{
        submitBtn.disabled = false;
        submitBtn.textContent = "Gửi đơn hàng";
    }}
}});
</script>
</body>
</html>
"""
    return html


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

    # Xử lý size - tách bằng dấu phẩy
    sizes = []
    if size_field:
        sizes = [s.strip() for s in size_field.split(",") if s.strip()]

    # Xử lý màu - tách bằng dấu phẩy
    colors = []
    if color_field:
        colors = [c.strip() for c in color_field.split(",") if c.strip()]

    # Nếu không có size/color thì dùng mặc định
    if not sizes:
        sizes = ["Mặc định"]
    if not colors:
        colors = ["Mặc định"]

    price_str = row.get("Gia", "0")
    price_int = extract_price_int(price_str) or 0

    return {
        "ms": ms,
        "name": row.get("Ten", ""),
        "price": price_int,
        "price_display": row.get("Gia", "0"),
        "desc": short_description(row.get("MoTa", "")),
        "image": image,
        "page_name": FANPAGE_NAME,
        "sizes": sizes,
        "colors": colors,
        "all_sizes": sizes,
        "all_colors": colors,
    }


@app.route("/api/order", methods=["POST"])
def api_order():
    data = request.get_json() or {}
    ms = (data.get("ms") or "").upper()
    uid = data.get("uid") or ""
    name = (data.get("name") or "").strip()
    phone = re.sub(r"\D", "", data.get("phone") or "")
    address = (data.get("address") or "").strip()
    note = (data.get("note") or "").strip()
    color = (data.get("color") or "").strip()
    size = (data.get("size") or "").strip()

    if not ms or ms not in PRODUCTS:
        return {"error": "Sản phẩm không tồn tại."}, 400

    if not name or not phone or not address:
        return {"error": "Thiếu thông tin bắt buộc."}, 400

    row = PRODUCTS[ms]

    # Ở đây bạn có thể tích hợp gửi đơn qua Fchat, Google Sheet, v.v.
    print("====== NEW ORDER ======")
    print("UID:", uid)
    print("Mã SP:", ms)
    print("Tên SP:", row.get("Ten", ""))
    print("Màu:", color)
    print("Size:", size)
    print("Tên khách:", name)
    print("SĐT:", phone)
    print("Địa chỉ:", address)
    print("Ghi chú:", note)
    print("=======================")

    # Gửi lại xác nhận cho khách (nếu uid là PSID)
    if uid:
        msg = (
            f"Dạ em đã nhận được đơn hàng của anh/chị.\n"
            f"Sản phẩm: [{ms}] {row.get('Ten','')}\n"
            f"Màu: {color}\n"
            f"Size: {size}\n"
            f"Người nhận: {name}\n"
            f"SĐT: {phone}\n"
            f"Địa chỉ: {address}\n\n"
            "⏰ Shop sẽ gọi điện xác nhận trong 5-10 phút.\n"
            "💳 Thanh toán khi nhận hàng (COD)\n"
            "────────────────────\n"
            "Cảm ơn anh/chị đã đặt hàng! ❤️"
        )
        send_message(uid, msg)

    return {"status": "ok", "message": "Đơn hàng đã được tiếp nhận"}


def get_variant_price(product: dict, color: str, size: str):
    """Trả về giá theo đúng biến thể màu/size. Nếu không tìm được thì trả về giá nhỏ nhất."""
    color = (color or "").strip()
    size = (size or "").strip()
    variants = product.get("variants") or []

    # Ưu tiên khớp cả màu & size (bỏ qua 'Mặc định')
    for v in variants:
        vm = (v.get("mau") or "").strip()
        vs = (v.get("size") or "").strip()
        if color and color.lower() != "mặc định" and vm and vm != color:
            continue
        if size and size.lower() != "mặc định" and vs and vs != size:
            continue
        gia_int = v.get("gia")
        if isinstance(gia_int, int):
            return gia_int

    # Nếu không khớp chính xác, lấy giá nhỏ nhất
    min_price = None
    for v in variants:
        gia_int = v.get("gia")
        if isinstance(gia_int, int):
            if min_price is None or gia_int < min_price:
                min_price = gia_int
    return min_price


# ============================================
# API LẤY GIÁ THEO BIẾN THỂ
# ============================================


@app.route("/api/get-variant-price")
def api_get_variant_price():
    ms = (request.args.get("ms") or "").upper()
    color = (request.args.get("color") or "").strip()
    size = (request.args.get("size") or "").strip()

    load_products()
    if ms not in PRODUCTS:
        return {"error": "not_found"}, 404

    product = PRODUCTS[ms]
    price_int = get_variant_price(product, color, size)

    if price_int is None:
        price_int = extract_price_int(product.get("Gia", "0")) or 0

    price_display = f"{price_int:,}đ" if isinstance(price_int, int) else str(price_int)

    return {
        "ms": ms,
        "color": color,
        "size": size,
        "price": price_int,
        "price_display": price_display,
    }


# ============================================
# MAIN
# ============================================


@app.route("/")
def index():
    return "OK"


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)
