import os
import json
import re
import time
import csv
import hashlib
from collections import defaultdict
from urllib.parse import quote
from datetime import datetime

import requests
from flask import Flask, request, send_from_directory
from openai import OpenAI

# ============================================
# FLASK APP
# ============================================
app = Flask(__name__, static_folder="static")

# ============================================
# ENV & CONFIG
# ============================================
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN", "").strip()
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
GOOGLE_SHEET_CSV_URL = os.getenv("GOOGLE_SHEET_CSV_URL", "").strip()
DOMAIN = os.getenv("DOMAIN", "").strip() or "fb-gpt-chatbot.onrender.com"
FANPAGE_NAME = os.getenv("FANPAGE_NAME", "Shop thời trang")
FCHAT_WEBHOOK_URL = os.getenv("FCHAT_WEBHOOK_URL", "").strip()
FCHAT_TOKEN = os.getenv("FCHAT_TOKEN", "").strip()

if not GOOGLE_SHEET_CSV_URL:
    GOOGLE_SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv"

# ============================================
# OPENAI CLIENT
# ============================================
client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ============================================
# GLOBAL STATE
# ============================================
USER_CONTEXT = defaultdict(lambda: {
    "history": [],
    "last_ms": None,
    "inbox_entry_ms": None,
    "greeted": False,
    "carousel_sent": False,
    "last_message_time": 0,
    "last_product_info_time": 0,
    "product_info_sent_ms": None,
    "get_started_processed": False,
    "processing_lock": False,
    "last_postback_payload": None,
    "postback_count": 0,
    "current_product_ms": None,
    "last_order_time": 0,
    "last_order_hash": None,
})

PRODUCTS = {}
LAST_LOAD = 0
LOAD_TTL = 300

# Các từ khóa liên quan đến đặt hàng
ORDER_KEYWORDS = [
    "đặt hàng", "chốt đơn", "mua", "lấy", "ship", "gửi", "mua hàng",
    "ok em", "ok chị", "ok anh", "em chốt", "chị chốt", "anh chốt",
    "đặt luôn", "lấy luôn", "giao hàng"
]

# ============================================
# HELPER: SEND MESSAGE
# ============================================

def call_facebook_send_api(payload: dict):
    if not PAGE_ACCESS_TOKEN:
        print("[WARN] PAGE_ACCESS_TOKEN is not set. Skip sending.")
        return ""

    url = "https://graph.facebook.com/v18.0/me/messages"
    params = {"access_token": PAGE_ACCESS_TOKEN}

    try:
        r = requests.post(url, params=params, json=payload, timeout=10)
        print("SEND MSG:", r.status_code, r.text)
        if r.status_code == 200:
            response = r.json()
            message_id = response.get("message_id", "")
            if message_id:
                # Lưu lại message_id để tránh xử lý echo
                recipient_id = payload.get("recipient", {}).get("id")
                if recipient_id:
                    ctx = USER_CONTEXT[recipient_id]
                    ctx["history"]  # kích hoạt tạo ctx
                    ctx.setdefault("sent_message_ids", set()).add(message_id)
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
                "payload": {
                    "template_type": "generic",
                    "elements": elements,
                },
            }
        },
    }
    mid = call_facebook_send_api(payload)
    print("SEND CAROUSEL:", mid)
    return mid


# ============================================
# PRODUCT LOADING
# ============================================

def parse_image_urls(raw: str):
    if not raw:
        return []
    parts = re.split(r'[,\n;|]+', raw)
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


def load_products(force=False):
    """
    Đọc dữ liệu từ Google Sheet CSV, cache trong 300s.
    Gom các biến thể cùng Mã sản phẩm, đồng thời gộp đủ màu & size.
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

            # Gộp các biến thể theo cùng mã sản phẩm: gom đủ màu & size
            if ms in products:
                existing = products[ms]

                def _merge_attr(old_val, new_val):
                    values = []
                    for v in (old_val, new_val):
                        if not v:
                            continue
                        for part in v.split(","):
                            part = part.strip()
                            if part and part not in values:
                                values.append(part)
                    return ", ".join(values)

                existing["màu (Thuộc tính)"] = _merge_attr(
                    existing.get("màu (Thuộc tính)", ""), mau
                )
                existing["size (Thuộc tính)"] = _merge_attr(
                    existing.get("size (Thuộc tính)", ""), size
                )
            else:
                products[ms] = row

        PRODUCTS = products
        LAST_LOAD = now
        print(f"📦 Loaded {len(PRODUCTS)} products.")
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
        "không được bịa.\n\n"
    )

    if not product:
        return base + (
            "Hiện tại em chưa xác định được sản phẩm cụ thể, "
            "hãy hỏi khách đang quan tâm mẫu nào, mã nào hoặc bảo khách gửi hình mẫu."
        )

    mo_ta = product.get("MoTa", "")
    gia = product.get("Gia", "")
    tonkho = product.get("Tồn kho", "")
    mau = product.get("màu (Thuộc tính)", "")
    size = product.get("size (Thuộc tính)", "")

    detail = f"""Dưới đây là thông tin sản phẩm mã {ms}:

- Tên: {product.get('Ten', '')}
- Giá niêm yết: {gia}
- Tồn kho: {tonkho}
- Màu: {mau}
- Size: {size}
- Mô tả: {mo_ta}

Khi khách hỏi:
- Về size: hãy dựa trên size hiện có, gợi ý size phù hợp chung (không bịa số đo chi tiết nếu không có).
- Về màu: liệt kê các màu trong dữ liệu.
- Về giá: trả lời đúng giá, nếu có mô tả 'giá từ ... tới ...' thì giải thích ngắn gọn.
- Về tồn kho: trả lời dựa trên cột Tồn kho.
- Nếu khách đồng ý mua: hỏi rõ màu, size, số lượng và hướng khách bấm vào link đặt hàng nếu có.

Không tự ý thay đổi giá, không tư vấn sang sản phẩm khác nếu khách đang hỏi 1 mã cụ thể.
"""
    return base + detail


def gpt_reply(history: list, product: dict | None, ms: str | None):
    if not OPENAI_API_KEY or not client:
        # Fallback: trả lời rule-based đơn giản
        if product and ms:
            return (
                f"Dạ em đang tư vấn cho anh/chị về sản phẩm mã {ms} - {product.get('Ten','')} ạ. "
                "Anh/chị cho em biết đang quan tâm size, màu hay giá để em hỗ trợ chi tiết hơn nhé."
            )
        else:
            return (
                "Dạ anh/chị cho em xin mã sản phẩm hoặc gửi hình mẫu để em kiểm tra giúp ạ."
            )

    messages = [{"role": "system", "content": build_product_system_prompt(product, ms)}]
    messages.extend(history)

    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=messages,
            temperature=0.6,
            max_tokens=500,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print("GPT ERROR:", e)
        if product and ms:
            return (
                f"Dạ em đang tư vấn cho anh/chị về sản phẩm mã {ms} - {product.get('Ten','')} ạ. "
                "Anh/chị cho em biết đang quan tâm size, màu hay giá để em hỗ trợ chi tiết hơn nhé."
            )
        return "Dạ anh/chị cho em xin mã sản phẩm hoặc gửi hình mẫu để em kiểm tra giúp ạ."


# ============================================
# HELPER: EXTRACT MS
# ============================================

MS_PATTERN = re.compile(r"\bMS0*\d{3,}\b", re.IGNORECASE)


def extract_ms(text: str | None):
    if not text:
        return None
    m = MS_PATTERN.search(text)
    if not m:
        return None
    return m.group(0).upper()


def extract_short_code(text: str | None):
    """
    Trích mã rút gọn dạng [ABC123] nếu có.
    """
    if not text:
        return None
    m = re.search(r"\[([A-Za-z0-9_-]{3,})\]", text)
    if not m:
        return None
    return m.group(1).upper()


def find_ms_by_short_code(short_code: str | None):
    if not short_code:
        return None
    load_products()
    for ms, row in PRODUCTS.items():
        code_in_sheet = (row.get("Mã sản phẩm") or "").strip().upper()
        if code_in_sheet == short_code:
            return ms
    return None


def resolve_best_ms(ctx: dict):
    """
    Ưu tiên: last_ms > inbox_entry_ms
    """
    if ctx.get("last_ms"):
        return ctx["last_ms"]
    if ctx.get("inbox_entry_ms"):
        return ctx["inbox_entry_ms"]
    if ctx.get("current_product_ms"):
        return ctx["current_product_ms"]
    return None


# ============================================
# ORDER FORM STATE MACHINE
# ============================================

def handle_order_form_step(uid: str, text: str):
    ctx = USER_CONTEXT[uid]
    state = ctx.get("order_state")
    if not state:
        return False

    if state == "waiting_name":
        ctx["order_name"] = text.strip()
        ctx["order_state"] = "waiting_phone"
        send_message(uid, "📱 Vui lòng nhập số điện thoại (ví dụ: 0912345678 hoặc +84912345678):")
        return True

    if state == "waiting_phone":
        phone = re.sub(r"[^\d+]", "", text)
        if len(phone) < 8:
            send_message(uid, "❌ Số điện thoại chưa đúng, anh/chị nhập lại giúp em với ạ.")
            return True
        ctx["order_phone"] = phone
        ctx["order_state"] = "waiting_address"
        send_message(uid, "🏠 Vui lòng nhập địa chỉ giao hàng chi tiết:")
        return True

    if state == "waiting_address":
        ctx["order_address"] = text.strip()
        ctx["order_state"] = None
        summary = (
            "✅ Thông tin nhận hàng em đã ghi lại:\n"
            f"- Họ tên: {ctx.get('order_name','')}\n"
            f"- Số điện thoại: {ctx.get('order_phone','')}\n"
            f"- Địa chỉ: {ctx.get('order_address','')}\n"
            "Anh/chị xem giúp em đã đúng chưa ạ?"
        )
        send_message(uid, summary)
        send_message(uid, "Nếu muốn chỉnh sửa, anh/chị bấm 'Sửa thông tin'. Nếu đúng rồi, anh/chị bấm 'Xác nhận' giúp em ạ.")
        return True

    return False


def send_order_confirmation(uid: str):
    ctx = USER_CONTEXT[uid]
    ms = resolve_best_ms(ctx)
    load_products()
    product_name = ""
    if ms and ms in PRODUCTS:
        product_name = PRODUCTS[ms].get("Ten", "")

    msg = (
        "✅ SHOP ĐÃ NHẬN THÔNG TIN ĐẶT HÀNG CỦA ANH/CHỊ!\n"
        "────────────────────\n"
        f"🛍️ Sản phẩm: {product_name} ({ms})\n"
        f"👤 Người nhận: {ctx.get('order_name','')}\n"
        f"📱 SĐT: {ctx.get('order_phone','')}\n"
        f"🏠 Địa chỉ: {ctx.get('order_address','')}\n"
        "────────────────────\n"
        "⏰ Shop sẽ gọi điện xác nhận trong 5-10 phút.\n"
        "💳 Thanh toán khi nhận hàng (COD)\n"
        "────────────────────\n"
        "Cảm ơn anh/chị đã đặt hàng! ❤️"
    )
    send_message(uid, msg)


# ============================================
# GREETING & CAROUSEL
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

    if not ctx["carousel_sent"]:
        send_top_products_carousel(uid)
        ctx["carousel_sent"] = True


def send_top_products_carousel(uid: str, limit=5):
    load_products()
    elements = []
    cnt = 0
    for ms, product in list(PRODUCTS.items())[:limit]:
        images_field = product.get("Images", "")
        urls = parse_image_urls(images_field)
        if not urls:
            continue

        original_image_url = None
        for u in urls:
            if should_use_as_first_image(u):
                original_image_url = u
                break

        if not original_image_url:
            continue

        final_image_url = original_image_url

        domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
        order_link = f"{domain}/order-form?ms={product.get('MS', '')}&uid={uid}"

        element = {
            "title": f"[{product.get('MS', '')}] {product.get('Ten', '')}",
            "subtitle": f"💰 Giá: {product.get('Gia', '')}\n{product.get('MoTa', '')[:60]}..." if product.get('MoTa') else f"💰 Giá: {product.get('Gia', '')}",
            "image_url": final_image_url,
            "buttons": [
                {
                    "type": "postback",
                    "title": "📋 Xem chi tiết",
                    "payload": f"VIEW_{product.get('MS', '')}",
                },
                {
                    "type": "web_url",
                    "title": "🛒 Chọn sản phẩm",
                    "url": order_link,
                },
            ],
        }
        elements.append(element)
        cnt += 1
        if cnt >= limit:
            break

    if elements:
        send_carousel_template(uid, elements)


# ============================================
# IMAGE HANDLING
# ============================================

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

    if FCHAT_WEBHOOK_URL and FCHAT_TOKEN:
        try:
            payload = {
                "token": FCHAT_TOKEN,
                "type": "image",
                "sender_id": uid,
                "image_url": image_url,
            }
            r = requests.post(FCHAT_WEBHOOK_URL, json=payload, timeout=10)
            print("FCHAT IMAGE WEBHOOK:", r.status_code, r.text)
        except Exception as e:
            print("❌ FCHAT IMAGE WEBHOOK ERROR:", e)


# ============================================
# TEXT HANDLING
# ============================================

def handle_text(uid: str, text: str):
    ctx = USER_CONTEXT[uid]

    if ctx.get("processing_lock"):
        print(f"[TEXT SKIP] User {uid} đang được xử lý")
        return

    ctx["processing_lock"] = True

    try:
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

        # Xác định sản phẩm đang được thảo luận
        ms = resolve_best_ms(ctx)
        if not ms and ctx.get("current_product_ms"):
            ms = ctx["current_product_ms"]

        # Chào hỏi nếu cần
        maybe_greet(uid, ctx, has_ms=bool(ms))

        # Thêm tin nhắn user vào history
        ctx["history"].append({"role": "user", "content": text})

        # Lấy thông tin sản phẩm nếu có
        product = None
        if ms and ms in PRODUCTS:
            product = PRODUCTS[ms]

            # Nếu user hỏi về size/màu/tồn kho, gửi thông tin chi tiết
            lower_text = text.lower()
            if any(
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

    finally:
        ctx["processing_lock"] = False


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
                    handle_echo_outgoing(
                        page_id=sender_id, user_id=recipient_id, text=text, mid=mid
                    )
                elif attachments:
                    print(f"[ECHO SKIP] Bỏ qua echo attachments từ bot: {mid}")
                continue

            ctx = USER_CONTEXT[sender_id]

            # KIỂM TRA LOCK ĐỂ TRÁNH XỬ LÝ TRÙNG
            if ctx.get("processing_lock"):
                print(f"[SKIP] User {sender_id} đang được xử lý, bỏ qua sự kiện mới")
                return "ok"

            # SET LOCK
            ctx["processing_lock"] = True

            try:
                if "postback" in ev:
                    current_time = time.time()
                    payload = ev["postback"].get("payload")

                    # KIỂM TRA DEBOUNCE: NẾU CÙNG PAYLOAD TRONG VÒNG 3 GIÂY THÌ BỎ QUA
                    if (
                        payload == ctx.get("last_postback_payload")
                        and current_time - ctx.get("last_postback_time", 0) < 3
                    ):
                        print(f"[POSTBACK DEBOUNCE] Bỏ qua postback trùng: {payload}")
                        return "ok"

                    # KIỂM TRA SPAM: NẾU NHIỀU POSTBACK QUÁ NHANH
                    ctx["postback_count"] = ctx.get("postback_count", 0) + 1
                    if (
                        ctx["postback_count"] > 3
                        and current_time - ctx.get("last_postback_time", 0) < 5
                    ):
                        print(f"[POSTBACK SPAM] Phát hiện spam từ user {sender_id}")
                        time.sleep(1)

                    ctx["last_postback_time"] = current_time
                    ctx["last_postback_payload"] = payload

                    print(f"[POSTBACK] User {sender_id}: {payload}")

                    # XỬ LÝ GET_STARTED_PAYLOAD - CHỈ CHẠY 1 LẦN
                    if payload == "GET_STARTED_PAYLOAD":
                        if ctx.get("get_started_processed"):
                            print(f"[POSTBACK SKIP] Đã xử lý GET_STARTED cho user {sender_id}")
                            return "ok"

                        ctx["get_started_processed"] = True

                        if not ctx["greeted"]:
                            maybe_greet(sender_id, ctx, has_ms=False)

                        if not ctx["carousel_sent"]:
                            send_message(
                                sender_id,
                                "Anh/chị cho em biết đang quan tâm mẫu nào hoặc gửi ảnh mẫu để em xem giúp ạ.",
                            )
                        return "ok"

                    # XỬ LÝ ORDER FORM QUICK REPLIES
                    if payload == "ORDER_PROVIDE_NAME":
                        ctx["order_state"] = "waiting_name"
                        send_message(sender_id, "👤 Vui lòng nhập họ tên người nhận hàng:")
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
                            sender_id, "✏️ Vui lòng nhập lại họ tên người nhận:"
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
                        domain = (
                            DOMAIN
                            if DOMAIN.startswith("http")
                            else f"https://{DOMAIN}"
                        )
                        order_link = (
                            f"{domain}/order-form?ms={product_code}&uid={sender_id}"
                        )
                        response_msg = (
                            f"📋 Anh/chị có thể đặt hàng sản phẩm [{product_code}] ngay tại đây:\n"
                            f"{order_link}"
                        )
                        send_message(sender_id, response_msg)
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
                        print(f"[REF] Nhận mã từ referral: {ms_ref}")

                # XỬ LÝ IMAGE MESSAGE
                if "message" in ev and "attachments" in message:
                    if not message.get("is_echo"):
                        for att in message["attachments"]:
                            if att.get("type") == "image":
                                image_url = att["payload"].get("url")
                                if image_url:
                                    handle_image(sender_id, image_url)
                                    return "ok"
                    continue

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
        send_message(uid, f"Dạ em không tìm thấy sản phẩm mã {ms} ạ.")
        return

    images_field = product.get("Images", "")
    urls = parse_image_urls(images_field)
    sent_first = False

    for url in urls[:5]:
        if not should_use_as_first_image(url):
            continue
        if not sent_first:
            send_image(uid, url)
            sent_first = True
        else:
            send_image(uid, url)
        time.sleep(0.3)

    detail = (
        f"📌 Thông tin sản phẩm [{ms}] {product.get('Ten','')}:\n"
        f"- Giá: {product.get('Gia','')}\n"
        f"- Màu: {product.get('màu (Thuộc tính)','')}\n"
        f"- Size: {product.get('size (Thuộc tính)','')}\n"
        f"- Tồn kho: {product.get('Tồn kho','')}\n\n"
        f"{product.get('MoTa','')}"
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

    # Xử lý giá
    price_str = row.get("Gia", "0")
    price_match = re.search(r"(\d[\d.,]*)", price_str)
    price = 0
    if price_match:
        price_str_clean = price_match.group(1).replace(",", "").replace(".", "")
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
        "colors": colors,
        "all_sizes": sizes,
        "all_colors": colors,
    }


@app.route("/api/order", methods=["POST"])
def api_order():
    data = request.json or {}
    print("ORDER RECEIVED:", json.dumps(data, indent=2))

    uid = data.get("uid") or data.get("user_id")
    ms = (data.get("ms") or data.get("product_code") or "").upper()

    if uid:
        # Chống gửi 2 lần khi form bị submit trùng
        ctx = USER_CONTEXT[uid]
        try:
            key_fields = {
                "ms": ms,
                "color": data.get("color", ""),
                "size": data.get("size", ""),
                "quantity": data.get("quantity", ""),
                "total": data.get("total", ""),
                "customerName": data.get("customerName", ""),
                "phone": data.get("phone", ""),
                "home": data.get("home", ""),
                "ward": data.get("ward", ""),
                "province": data.get("province", ""),
            }
            payload_str = json.dumps(key_fields, sort_keys=True, ensure_ascii=False)
        except Exception:
            payload_str = json.dumps(data, sort_keys=True, default=str, ensure_ascii=False)

        order_hash = hashlib.md5(payload_str.encode("utf-8")).hexdigest()
        now = time.time()
        last_hash = ctx.get("last_order_hash")
        last_time = ctx.get("last_order_time", 0)

        if last_hash == order_hash and (now - last_time) < 5:
            print(f"[ORDER DUP] Bỏ qua đơn hàng trùng lặp cho user {uid}")
            return {"status": "ok", "message": "Đơn hàng đã được tiếp nhận"}

        ctx["last_order_hash"] = order_hash
        ctx["last_order_time"] = now

        load_products()
        product_name = ""
        if ms in PRODUCTS:
            product_name = PRODUCTS[ms].get("Ten", "")

        address_components = [
            data.get("home", ""),
            data.get("ward", ""),
            data.get("province", ""),
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
# API LẤY GIÁ THEO BIẾN THỂ (OPTIONAL)
# ============================================

@app.route("/api/get-variant-price")
def api_get_variant_price():
    ms = (request.args.get("ms") or "").upper()
    color = (request.args.get("color") or "").strip()
    size = (request.args.get("size") or "").strip()

    load_products()
    if ms not in PRODUCTS:
        return {"error": "not_found"}, 404

    row = PRODUCTS[ms]
    price_str = row.get("Gia", "0")
    price_match = re.search(r"(\d[\d.,]*)", price_str)
    price = 0
    if price_match:
        price_str_clean = price_match.group(1).replace(",", "").replace(".", "")
        try:
            price = int(price_str_clean)
        except:
            price = 0

    return {
        "ms": ms,
        "color": color,
        "size": size,
        "price": price,
        "price_display": row.get("Gia", "0"),
    }


# ============================================
# STATIC & ROOT
# ============================================

@app.route("/")
def index():
    return "Chatbot is running."


@app.route("/static/<path:path>")
def static_files(path):
    return send_from_directory("static", path)


# ============================================
# MAIN (LOCAL RUN)
# ============================================

if __name__ == "__main__":
    print("Starting app on http://0.0.0.0:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)
