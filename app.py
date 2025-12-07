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
app = Flask(__name__)

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

# Nếu không truyền biến môi trường thì dùng URL mặc định
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
    "last_msg_time": 0,
    "last_ms": None,
    "order_state": None,
    "order_data": {},
    "processing_lock": False,
    "postback_count": 0,
    "product_info_sent_ms": None,
    "last_product_info_time": 0,
})
PRODUCTS = {}
LAST_LOAD = 0
LOAD_TTL = 300

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
# HELPER: SEND MESSAGE
# ============================================

def call_facebook_send_api(payload: dict):
    if not PAGE_ACCESS_TOKEN:
        print("[WARN] PAGE_ACCESS_TOKEN chưa được cấu hình, bỏ qua gửi tin nhắn.")
        return {}
    url = f"https://graph.facebook.com/v12.0/me/messages?access_token={PAGE_ACCESS_TOKEN}"
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if not resp.ok:
            print("Facebook Send API error:", resp.text)
        return resp.json()
    except Exception as e:
        print("Facebook Send API exception:", e)
        return {}


def send_message(recipient_id: str, text: str):
    if not text:
        return
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
                    "elements": elements[:10],
                },
            }
        },
    }
    return call_facebook_send_api(payload)


def send_quick_replies(recipient_id: str, text: str, quick_replies: list):
    payload = {
        "recipient": {"id": recipient_id},
        "message": {
            "text": text,
            "quick_replies": quick_replies,
        },
    }
    return call_facebook_send_api(payload)


# ============================================
# HELPER: PRODUCTS
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
    PHƯƠNG ÁN A: Mỗi dòng = 1 biến thể, gom theo Mã sản phẩm và lưu danh sách variants.
    """
    global PRODUCTS, LAST_LOAD
    now = time.time()
    if not force and PRODUCTS and (now - LAST_LOAD) < LOAD_TTL:
        return

    if not GOOGLE_SHEET_CSV_URL:
        print("❌ GOOGLE_SHEET_CSV_URL chưa được cấu hình! Không thể load sản phẩm.")
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

            gia_raw = (row.get("Giá bán") or "").strip()
            images = (row.get("Images") or "").strip()
            videos = (row.get("Videos") or "").strip()
            tonkho_raw = (row.get("Tồn kho") or "").strip()
            mota = (row.get("Mô tả") or "").strip()
            mau = (row.get("màu (Thuộc tính)") or "").strip()
            size = (row.get("size (Thuộc tính)") or "").strip()

            gia_int = extract_price_int(gia_raw)
            try:
                tonkho_int = int(str(tonkho_raw)) if str(tonkho_raw).strip() else None
            except Exception:
                tonkho_int = None

            if ms not in products:
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

            # Cập nhật thông tin chung nếu còn thiếu
            if not p.get("Images") and images:
                p["Images"] = images
            if not p.get("Videos") and videos:
                p["Videos"] = videos
            if not p.get("MoTa") and mota:
                p["MoTa"] = mota
            if not p.get("Gia") and gia_raw:
                p["Gia"] = gia_raw
            if not p.get("Tồn kho") and tonkho_raw:
                p["Tồn kho"] = tonkho_raw

            # Thêm biến thể
            variant = {
                "mau": mau,
                "size": size,
                "gia": gia_int,
                "gia_raw": gia_raw,
                "tonkho": tonkho_int if tonkho_int is not None else tonkho_raw,
            }
            p["variants"].append(variant)

            if mau:
                p["all_colors"].add(mau)
            if size:
                p["all_sizes"].add(size)

        # Hậu xử lý: gộp màu/size & tạo mô tả ngắn
        for ms, p in products.items():
            colors = sorted(list(p.get("all_colors") or []))
            sizes = sorted(list(p.get("all_sizes") or []))
            p["màu (Thuộc tính)"] = ", ".join(colors) if colors else p.get("màu (Thuộc tính)", "")
            p["size (Thuộc tính)"] = ", ".join(sizes) if sizes else p.get("size (Thuộc tính)", "")
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
        "Nếu không chắc thông tin (ví dụ thiếu trong dữ liệu sản phẩm) "
        "thì nói rõ là không chắc, và gợi ý khách inbox để được hỗ trợ thêm.\n"
    )

    if not product or not ms:
        base += (
            "\nHiện tại bạn KHÔNG có dữ liệu chi tiết sản phẩm. "
            "Hãy trả lời chung chung, khéo léo xin khách gửi mã sản phẩm hoặc hình ảnh "
            "để bạn kiểm tra lại trên hệ thống."
        )
        return base

    ten = product.get("Ten", "")
    gia = product.get("Gia", "")
    mau = product.get("màu (Thuộc tính)", "")
    size = product.get("size (Thuộc tính)", "")
    tonkho = product.get("Tồn kho", "")
    mota = product.get("MoTa", "")

    base += (
        f"\nDưới đây là thông tin sản phẩm hiện tại trong hệ thống:\n"
        f"- Mã sản phẩm: {ms}\n"
        f"- Tên: {ten}\n"
        f"- Giá: {gia}\n"
        f"- Màu: {mau}\n"
        f"- Size: {size}\n"
        f"- Tồn kho: {tonkho}\n"
        f"- Mô tả: {mota}\n\n"
        "Khi khách hỏi về sản phẩm này, ưu tiên dựa vào các thông tin trên để tư vấn. "
        "Nếu khách hỏi những câu chung chung (ví dụ: còn hàng không, có màu/size nào, "
        "bao lâu nhận được hàng, phí ship, cách đổi trả, v.v.) thì trả lời rõ ràng, "
        "kèm theo gợi ý đặt hàng."
    )

    return base


def build_chatgpt_reply(uid: str, text: str, ms: str | None):
    """
    Gọi OpenAI để trả lời câu hỏi của khách hàng.
    """
    if not client or not OPENAI_API_KEY:
        return "Hiện tại hệ thống AI đang tạm thời bảo trì, anh/chị inbox trực tiếp để shop hỗ trợ ạ."

    load_products()
    product = PRODUCTS.get(ms) if ms else None

    system_prompt = build_product_system_prompt(product, ms)
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": text},
    ]

    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.4,
            max_tokens=500,
        )
        reply = resp.choices[0].message.content.strip()
        return reply
    except Exception as e:
        print("OpenAI error:", e)
        return "Hiện tại em đang gặp chút trục trặc kỹ thuật, anh/chị vui lòng nhắn lại sau ít phút giúp em ạ."


# ============================================
# HANDLE ORDER FORM STATE
# ============================================

def reset_order_state(uid: str):
    ctx = USER_CONTEXT[uid]
    ctx["order_state"] = None
    ctx["order_data"] = {}


def handle_order_form_step(uid: str, text: str):
    """
    Xử lý luồng hỏi thông tin đặt hàng nếu user đang trong trạng thái order_state.
    """
    ctx = USER_CONTEXT[uid]
    state = ctx.get("order_state")
    if not state:
        return False

    data = ctx.get("order_data", {})

    if state == "ask_name":
        data["customerName"] = text.strip()
        ctx["order_state"] = "ask_phone"
        send_message(uid, "Dạ em cảm ơn anh/chị. Anh/chị cho em xin số điện thoại ạ?")
        return True

    if state == "ask_phone":
        phone = re.sub(r"[^\d+]", "", text)
        if len(phone) < 9:
            send_message(uid, "Số điện thoại chưa đúng lắm, anh/chị nhập lại giúp em (tối thiểu 9 số) ạ?")
            return True
        data["phone"] = phone
        ctx["order_state"] = "ask_address"
        send_message(uid, "Dạ vâng. Anh/chị cho em xin địa chỉ nhận hàng (đầy đủ: số nhà, đường, phường/xã, quận/huyện, tỉnh/thành) ạ?")
        return True

    if state == "ask_address":
        data["address"] = text.strip()
        ctx["order_state"] = None
        ctx["order_data"] = data

        # Xác nhận lại đơn
        summary = (
            "Dạ em tóm tắt lại đơn hàng của anh/chị:\n"
            f"- Sản phẩm: {data.get('productName', '')}\n"
            f"- Mã: {data.get('ms', '')}\n"
            f"- Phân loại: {data.get('color', '')} / {data.get('size', '')}\n"
            f"- Số lượng: {data.get('quantity', '1')}\n"
            f"- Thành tiền dự kiến: {data.get('total', '')}\n"
            f"- Người nhận: {data.get('customerName', '')}\n"
            f"- SĐT: {data.get('phone', '')}\n"
            f"- Địa chỉ: {data.get('address', '')}\n\n"
            "Anh/chị kiểm tra giúp em xem đã đúng chưa ạ?"
        )
        send_message(uid, summary)
        return True

    return False


# ============================================
# HANDLE IMAGE
# ============================================

def handle_image(uid: str, image_url: str):
    """
    Khi khách gửi ảnh, ta không có OCR nên chỉ trả lời chung chung.
    """
    send_message(
        uid,
        "Dạ em cảm ơn anh/chị đã gửi ảnh.\n"
        "Hiện tại em chưa xem được chi tiết trong hình. "
        "Anh/chị giúp em gửi kèm mã sản phẩm hoặc mô tả sản phẩm cần tư vấn nhé.",
    )


# ============================================
# HANDLE TEXT
# ============================================

def detect_ms_from_text(text: str):
    """
    Tìm mã sản phẩm dạng [MS000123] trong tin nhắn.
    """
    ms_list = re.findall(r"\[MS(\d{6})\]", text.upper())
    if ms_list:
        return "MS" + ms_list[0]
    return None


def find_latest_ms_in_context(uid: str):
    """
    Lấy mã sản phẩm gần nhất trong context của user (nếu có).
    """
    ctx = USER_CONTEXT[uid]
    ms = ctx.get("last_ms")
    if ms and ms in PRODUCTS:
        return ms
    return None


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
        send_message(uid, "Em không tìm thấy sản phẩm này trong hệ thống, anh/chị kiểm tra lại mã giúp em ạ.")
        return

    # Gửi ảnh sản phẩm (1 ảnh đại diện)
    images_field = product.get("Images", "")
    urls = parse_image_urls(images_field)
    main_image = ""
    for u in urls:
        if should_use_as_first_image(u):
            main_image = u
            break
    if main_image:
        send_image(uid, main_image)

    # Mô tả ngắn gọn, đủ ý
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

        # Thử lấy mã sản phẩm từ text
        ms = detect_ms_from_text(text)
        if not ms:
            ms = find_latest_ms_in_context(uid)

        if ms and ms in PRODUCTS:
            USER_CONTEXT[uid]["last_ms"] = ms

        # Gọi GPT trả lời
        reply = build_chatgpt_reply(uid, text, ms)

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
            return challenge, 200
        return "Verification token mismatch", 403

    data = request.get_json() or {}
    print("Webhook received:", json.dumps(data, ensure_ascii=False))

    entry = data.get("entry", [])
    for e in entry:
        messaging = e.get("messaging", [])
        for m in messaging:
            sender_id = m.get("sender", {}).get("id")
            if not sender_id:
                continue

            # Echo handler
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

            if "postback" in m:
                payload = m["postback"].get("payload")
                if payload:
                    handle_postback(sender_id, payload)

    return "OK", 200


# ============================================
# POSTBACK HANDLER
# ============================================

def handle_postback(uid: str, payload: str):
    ctx = USER_CONTEXT[uid]
    ctx["postback_count"] = ctx.get("postback_count", 0) + 1

    if payload == "GET_STARTED":
        send_message(
            uid,
            f"Em chào anh/chị, em là trợ lý bán hàng của {FANPAGE_NAME}. "
            "Anh/chị cần em tư vấn sản phẩm hoặc hỗ trợ đặt hàng gì không ạ?",
        )
        return

    # Các postback khác do bạn tự định nghĩa nếu cần
    send_message(uid, "Dạ em đã nhận được thao tác của anh/chị ạ.")


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

    row = PRODUCTS[ms]
    images_field = row.get("Images", "")
    urls = parse_image_urls(images_field)
    image = ""
    for u in urls:
        if should_use_as_first_image(u):
            image = u
            break
    if not image and urls:
        image = urls[0]

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

    html = f"""
    <html>
    <head>
        <meta charset="utf-8" />
        <title>Đặt hàng - {row.get('Ten','')}</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
    </head>
    <body style="font-family: Arial, sans-serif; margin: 0; padding: 0; background: #f5f5f5;">
        <div style="max-width: 480px; margin: 0 auto; background: #fff; min-height: 100vh;">
            <div style="padding: 16px; border-bottom: 1px solid #eee; text-align: center;">
                <h2 style="margin: 0; font-size: 18px;">ĐẶT HÀNG - {FANPAGE_NAME}</h2>
            </div>
            <div style="padding: 16px;">
                <div style="display: flex; gap: 12px;">
                    <div style="width: 120px; height: 120px; overflow: hidden; border-radius: 8px; background: #f0f0f0;">
                        {"<img src='" + image + "' style='width: 100%; height: 100%; object-fit: cover;' />" if image else ""}
                    </div>
                    <div style="flex: 1;">
                        <h3 style="margin-top: 0; font-size: 16px;">[{ms}] {row.get('Ten','')}</h3>
                        <div style="color: #FF3B30; font-weight: bold; font-size: 16px;" id="price-display">
                            {price_int:,.0f} đ
                        </div>
                    </div>
                </div>

                <div style="margin-top: 16px;">
                    <label for="color" style="display: block; margin-bottom: 4px; font-size: 14px;">Màu sắc:</label>
                    <select id="color" style="width: 100%; padding: 8px; border-radius: 4px; border: 1px solid #ccc;">
                        {''.join(f"<option value='{c}'>{c}</option>" for c in colors)}
                    </select>
                </div>

                <div style="margin-top: 12px;">
                    <label for="size" style="display: block; margin-bottom: 4px; font-size: 14px;">Size:</label>
                    <select id="size" style="width: 100%; padding: 8px; border-radius: 4px; border: 1px solid #ccc;">
                        {''.join(f"<option value='{s}'>{s}</option>" for s in sizes)}
                    </select>
                </div>

                <div style="margin-top: 12px;">
                    <label for="quantity" style="display: block; margin-bottom: 4px; font-size: 14px;">Số lượng:</label>
                    <input type="number" id="quantity" value="1" min="1" style="width: 100%; padding: 8px; border-radius: 4px; border: 1px solid #ccc;" />
                </div>

                <div style="margin-top: 16px; padding: 12px; background: #f9f9f9; border-radius: 8px;">
                    <div style="font-size: 14px; margin-bottom: 4px;">Tạm tính:</div>
                    <div id="total-display" style="font-size: 18px; color: #FF3B30; font-weight: bold;">
                        {price_int:,.0f} đ
                    </div>
                </div>

                <div style="margin-top: 16px;">
                    <label for="customerName" style="display: block; margin-bottom: 4px; font-size: 14px;">Họ và tên:</label>
                    <input type="text" id="customerName" style="width: 100%; padding: 8px; border-radius: 4px; border: 1px solid #ccc;" />
                </div>

                <div style="margin-top: 12px;">
                    <label for="phone" style="display: block; margin-bottom: 4px; font-size: 14px;">Số điện thoại:</label>
                    <input type="tel" id="phone" style="width: 100%; padding: 8px; border-radius: 4px; border: 1px solid #ccc;" />
                </div>

                <div style="margin-top: 12px;">
                    <label for="address" style="display: block; margin-bottom: 4px; font-size: 14px;">Địa chỉ nhận hàng:</label>
                    <textarea id="address" rows="3" style="width: 100%; padding: 8px; border-radius: 4px; border: 1px solid #ccc;"></textarea>
                </div>

                <button onclick="submitOrder()" style="margin-top: 20px; width: 100%; padding: 12px; border-radius: 999px; border: none; background: #1DB954; color: #fff; font-size: 16px; font-weight: bold;">
                    ĐẶT HÀNG NGAY
                </button>

                <p style="margin-top: 12px; font-size: 12px; color: #666; text-align: center;">
                    Shop sẽ gọi xác nhận trong 5-10 phút. Thanh toán khi nhận hàng (COD).
                </p>
            </div>
        </div>

        <script>
            const basePrice = {price_int};

            function formatPrice(n) {{
                return n.toLocaleString('vi-VN') + ' đ';
            }}

            async function updatePriceByVariant() {{
                const color = document.getElementById('color').value;
                const size = document.getElementById('size').value;
                const quantity = parseInt(document.getElementById('quantity').value || '1');

                try {{
                    const res = await fetch(`/api/get-variant-price?ms={ms}&color=${{encodeURIComponent(color)}}&size=${{encodeURIComponent(size)}}`);
                    if (!res.ok) throw new Error('request failed');
                    const data = await res.json();
                    const price = data.price || basePrice;

                    document.getElementById('price-display').innerText = formatPrice(price);
                    document.getElementById('total-display').innerText = formatPrice(price * quantity);
                }} catch (e) {{
                    document.getElementById('price-display').innerText = formatPrice(basePrice);
                    document.getElementById('total-display').innerText = formatPrice(basePrice * quantity);
                }}
            }}

            document.getElementById('color').addEventListener('change', updatePriceByVariant);
            document.getElementById('size').addEventListener('change', updatePriceByVariant);
            document.getElementById('quantity').addEventListener('input', updatePriceByVariant);

            async function submitOrder() {{
                const color = document.getElementById('color').value;
                const size = document.getElementById('size').value;
                const quantity = parseInt(document.getElementById('quantity').value || '1');
                const customerName = document.getElementById('customerName').value;
                const phone = document.getElementById('phone').value;
                const address = document.getElementById('address').value;

                const res = await fetch('/api/submit-order', {{
                    method: 'POST',
                    headers: {{
                        'Content-Type': 'application/json'
                    }},
                    body: JSON.stringify({{
                        ms: "{ms}",
                        uid: "{uid}",
                        color,
                        size,
                        quantity,
                        customerName,
                        phone,
                        address
                    }})
                }});

                const data = await res.json();
                alert(data.message || 'Đã gửi đơn hàng thành công, shop sẽ liên hệ lại anh/chị sớm nhất!');
            }}
        </script>
    </body>
    </html>
    """
    return html


# ============================================
# API: GET PRODUCT (CHO FORM)
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


# ============================================
# API: GET VARIANT PRICE
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
    variants = product.get("variants") or []

    chosen = None
    # Ưu tiên khớp cả màu & size (nếu có truyền)
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

    # Nếu không match chính xác, lấy biến thể đầu tiên (nếu có)
    if not chosen and variants:
        chosen = variants[0]

    price = 0
    price_display = product.get("Gia", "0")

    if chosen:
        if chosen.get("gia") is not None:
            price = chosen["gia"]
            price_display = chosen.get("gia_raw") or price_display
        else:
            # Thử parse từ chuỗi giá biến thể
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


# ============================================
# API: SUBMIT ORDER
# ============================================

@app.route("/api/submit-order", methods=["POST"])
def api_submit_order():
    data = request.get_json() or {}
    ms = (data.get("ms") or "").upper()
    uid = data.get("uid") or ""
    color = (data.get("color") or "").strip()
    size = (data.get("size") or "").strip()
    quantity = int(data.get("quantity") or 1)
    customerName = data.get("customerName") or ""
    phone = data.get("phone") or ""
    address = data.get("address") or ""

    load_products()
    row = PRODUCTS.get(ms)
    if not row:
        return {"error": "not_found", "message": "Sản phẩm không tồn tại"}, 404

    price_str = row.get("Gia", "0")
    price_int = extract_price_int(price_str) or 0
    total = price_int * quantity

    # Gửi tin nhắn xác nhận về Messenger
    if uid:
        msg = (
            "🎉 Shop đã nhận được đơn hàng mới:\n"
            f"🛍 Sản phẩm: [{ms}] {row.get('Ten','')}\n"
            f"🎨 Phân loại: {color} / {size}\n"
            f"📦 Số lượng: {quantity}\n"
            f"💰 Thành tiền: {total:,.0f} đ\n"
            f"👤 Người nhận: {customerName}\n"
            f"📱 SĐT: {phone}\n"
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
# STATIC
# ============================================

@app.route("/static/<path:path>")
def static_files(path):
    return send_from_directory("static", path)


# ============================================
# MAIN (LOCAL RUN)
# ============================================

if __name__ == "__main__":
    print("Starting app on http://0.0.0.0:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)
