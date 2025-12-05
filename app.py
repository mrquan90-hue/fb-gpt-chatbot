import os
import json
import re
import io
import time
import csv
from collections import defaultdict
from urllib.parse import quote

import requests
import pandas as pd
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
SHEET_URL          = os.getenv("SHEET_CSV_URL")  # đúng với Render của bạn
DOMAIN             = os.getenv("DOMAIN", "fb-gpt-chatbot.onrender.com")

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ============================================
# GLOBAL STATE
# ============================================

USER_CONTEXT = defaultdict(lambda: {
    "last_ms": None,          # mã sản phẩm gần nhất
    "inbox_entry_ms": None,   # mã từ ref Fchat / CTA
    "caption_ms": None,       # dự phòng mã từ caption (nếu sau bổ sung)
    "vision_ms": None,        # mã từ GPT Vision
    "history": [],            # lịch sử hội thoại
    "greeted": False,         # đã chào chưa
    "last_image_ms": None,    # mã sản phẩm đã gửi ảnh gần nhất (tránh spam)
})

PRODUCTS = {}
LAST_LOAD = 0
LOAD_TTL = 300  # 5 phút cache sheet

# ============================================
# TỪ KHOÁ THỂ HIỆN Ý ĐỊNH "ĐẶT HÀNG / MUA"
# (đã loại bỏ các từ quá chung như "ok", "ừ", "được")
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

def send_message(uid: str, text: str) -> None:
    if not text:
        return
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
    except Exception as e:
        print("SEND MSG ERROR:", e)


def send_image(uid: str, image_url: str) -> None:
    url = "https://graph.facebook.com/v18.0/me/messages"
    params = {"access_token": PAGE_ACCESS_TOKEN}
    payload = {
        "recipient": {"id": uid},
        "message": {
            "attachment": {
                "type": "image",
                "payload": {"url": image_url, "is_reusable": True},
            }
        },
        "messaging_type": "RESPONSE",
    }
    try:
        r = requests.post(url, params=params, json=payload, timeout=15)
        print("SEND IMG:", r.status_code, r.text)
    except Exception as e:
        print("SEND IMG ERROR:", e)


# ============================================
# REHOST IMAGE (freeimage.host)
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
# LOAD SHEET THEO ĐÚNG CỘT BẠN YÊU CẦU
# ============================================

def load_products(force: bool = False) -> None:
    """
    Load CSV từ Google Sheet theo đúng cấu trúc sheet bạn đưa:
    BẮT BUỘC đọc các cột (tên chính xác trên sheet):
      - Mã sản phẩm
      - Tên sản phẩm
      - Images
      - Videos
      - Tồn kho
      - Giá bán
      - Mô tả
      - màu (Thuộc tính)
      - size (Thuộc tính)
    Các cột khác nếu có sẽ giữ nguyên trong row, nhưng GPT chỉ ưu tiên dùng các cột này.
    """
    global PRODUCTS, LAST_LOAD

    now = time.time()
    if not force and PRODUCTS and now - LAST_LOAD < LOAD_TTL:
        return

    if not SHEET_URL:
        print("❌ SHEET_CSV_URL chưa cấu hình")
        PRODUCTS = {}
        return

    print("🟦 Loading sheet (DictReader, fixed columns):", SHEET_URL)

    try:
        resp = requests.get(SHEET_URL, timeout=30)
        resp.raise_for_status()

        # Ép decode UTF-8, nếu lỗi thì thay ký tự lạ bằng �
        csv_text = resp.content.decode("utf-8", errors="replace")
        lines = csv_text.splitlines()
        reader = csv.DictReader(lines)

        products = {}
        for raw_row in reader:
            row = dict(raw_row)

            # ---- CỘT BẮT BUỘC: MÃ SẢN PHẨM ----
            ms = (row.get("Mã sản phẩm") or "").strip()
            if not ms:
                continue  # không có mã → bỏ

            # ---- CỘT BẮT BUỘC: TÊN SẢN PHẨM ----
            ten = (row.get("Tên sản phẩm") or "").strip()
            if not ten:
                continue

            # ---- CỘT BẮT BUỘC: GIÁ BÁN ----
            gia = (row.get("Giá bán") or "").strip()
            if not gia:
                # để an toàn vẫn cho qua, không continue
                pass

            # ---- CÁC CỘT QUAN TRỌNG KHÁC ----
            images = (row.get("Images") or "").strip()
            videos = (row.get("Videos") or "").strip()
            tonkho = (row.get("Tồn kho") or "").strip()
            mota = (row.get("Mô tả") or "").strip()
            mau = (row.get("màu (Thuộc tính)") or "").strip()
            size = (row.get("size (Thuộc tính)") or "").strip()

            # Chuẩn hoá key GPT sẽ dùng
            row["MS"] = ms
            row["Ten"] = ten
            row["Gia"] = gia
            row["MoTa"] = mota

            # Đảm bảo các cột cần thiết luôn tồn tại trong row
            row["Images"] = images
            row["Videos"] = videos
            row["Tồn kho"] = tonkho
            row["màu (Thuộc tính)"] = mau
            row["size (Thuộc tính)"] = size

            products[ms] = row

        PRODUCTS = products
        LAST_LOAD = now
        print(f"📦 Loaded {len(PRODUCTS)} products (fixed columns).")

    except Exception as e:
        print("❌ load_products error:", e)
        PRODUCTS = {}


# ============================================
# IMAGE HELPER & GPT VISION
# ============================================

def extract_images(row: dict) -> list:
    imgs = []
    for k, v in row.items():
        lk = k.lower()
        if any(x in lk for x in ["ảnh", "image", "img"]):
            if isinstance(v, str) and v.startswith("http"):
                imgs.append(v.strip())
    return imgs


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
            model="gpt-4.1-mini",
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
    # bỏ 0 thừa bên trái để tránh trường hợp '' sau khi lstrip
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

    # Ưu tiên mã dài hơn (đủ 6 số) để hạn chế nhầm
    candidates.sort(key=len, reverse=True)
    return candidates[0]


def resolve_best_ms(ctx: dict):
    for key in ["vision_ms", "inbox_entry_ms", "caption_ms", "last_ms"]:
        if ctx.get(key):
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

    r = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[{"role": "system", "content": sys}] + history,
        temperature=0.5,
    )
    return r.choices[0].message.content


# ============================================
# HANDLE IMAGE MESSAGE
# ============================================

def handle_image(uid: str, image_url: str):
    load_products()
    ctx = USER_CONTEXT[uid]
    maybe_greet(uid)

    hosted = rehost_image(image_url)
    ms, desc = gpt_analyze_image(hosted)
    print("VISION RESULT:", ms, desc)

    if ms and ms in PRODUCTS:
        ctx["vision_ms"] = ms
        ctx["last_ms"] = ms
        send_message(uid, f"Dạ ảnh này giống mẫu {ms} của shop đó ạ!")
        imgs = extract_images(PRODUCTS[ms])
        if imgs:
            send_image(uid, rehost_image(imgs[0]))
    else:
        send_message(
            uid,
            "Dạ hình này hơi khó nhận mẫu chính xác ạ, anh/chị gửi giúp em caption hoặc mã sản phẩm nhé.",
        )


# ============================================
# GREETING
# ============================================

def maybe_greet(uid: str):
    ctx = USER_CONTEXT[uid]
    if not ctx["greeted"]:
        ctx["greeted"] = True
        send_message(
            uid,
            "Dạ em chào anh/chị ạ 😊 Em là trợ lý bán hàng của shop, hỗ trợ mình xem mẫu và chốt đơn nhanh ạ!",
        )


# ============================================
# HANDLE TEXT MESSAGE (NEW)
# ============================================

def handle_text(uid: str, text: str):
    """
    - GPT tư vấn theo ngữ cảnh (no-rule)
    - Hiểu mã đầy đủ (MS000046) + mã ngắn ('Mã 09')
    - Tự động gửi ảnh 1 lần / mã / hội thoại
    - Gửi link form đặt hàng khi khách thể hiện ý định mua
    """
    load_products()
    ctx = USER_CONTEXT[uid]
    maybe_greet(uid)

    # 1) Cập nhật mã từ chính tin nhắn khách
    ms_text = extract_ms(text)
    if not ms_text:
        short = extract_short_code(text)
        if short:
            ms_text = find_ms_by_short_code(short)

    if ms_text:
        ctx["last_ms"] = ms_text

    ms = resolve_best_ms(ctx)

    # 2) Đẩy câu hỏi vào lịch sử rồi gọi GPT
    ctx["history"].append({"role": "user", "content": text})

    if ms and ms in PRODUCTS:
        product = PRODUCTS[ms]
        reply = gpt_reply(ctx["history"], product)
    else:
        product = None
        reply = gpt_reply(ctx["history"], None)

    ctx["history"].append({"role": "assistant", "content": reply})
    send_message(uid, reply)

    # 3) Nếu đã xác định được mã sản phẩm → gửi ảnh + link đặt hàng khi có ý định mua
    if ms and ms in PRODUCTS:
        product = PRODUCTS[ms]

        # Gửi ảnh: mỗi mã chỉ gửi 1 lần / hội thoại
        last_img_ms = ctx.get("last_image_ms")
        imgs = extract_images(product)
        if imgs and ms != last_img_ms:
            try:
                hosted = rehost_image(imgs[0])
                send_image(uid, hosted)
                ctx["last_image_ms"] = ms
            except Exception as e:
                print("[IMAGE_SEND_ERROR]", e)

        # Nếu câu của khách có ý 'mua / chốt' thì gửi link form
        lower = text.lower()
        if any(kw in lower for kw in ORDER_KEYWORDS):
            send_order_link(uid, ms)


# ============================================
# MS TỪ REF / ECHO
# ============================================

def extract_ms_from_ref(ref: str | None):
    if not ref:
        return None
    return extract_ms(ref)


def handle_echo_outgoing(page_id: str, user_id: str, text: str):
    """
    Tin nhắn do PAGE/FCHAT gửi (echo).
    Dùng để cập nhật mã sản phẩm cho user, KHÔNG được trả lời lại.
    Ví dụ: "[MS000046] ..." hoặc "#MS000046 ..."
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

            if not sender_id:
                continue

            # ECHO: sender_id = page, recipient_id = user
            msg = ev.get("message", {}) or {}

            if msg.get("is_echo"):
                text = msg.get("text") or ""
                handle_echo_outgoing(page_id=sender_id, user_id=recipient_id, text=text)
                continue

            # Từ đây trở xuống: sender_id = user
            ctx = USER_CONTEXT[sender_id]

            # 2) REF (khách đến từ post/comment/CTA)
            ref = ev.get("referral", {}).get("ref") \
                or ev.get("postback", {}).get("referral", {}).get("ref")
            if ref:
                ms_ref = extract_ms_from_ref(ref)
                if ms_ref:
                    ctx["inbox_entry_ms"] = ms_ref
                    ctx["last_ms"] = ms_ref
                    print(f"[REF] Nhận mã từ ref: {ms_ref}")

            # 3) ATTACHMENTS → ảnh
            if "message" in ev and "attachments" in msg:
                for att in msg["attachments"]:
                    if att.get("type") == "image":
                        image_url = att["payload"].get("url")
                        if image_url:
                            handle_image(sender_id, image_url)
                            return "ok"
                continue

            # 4) TEXT
            if "message" in ev and "text" in msg:
                text = msg.get("text", "")
                handle_text(sender_id, text)
                return "ok"

            # 5) POSTBACK
            if "postback" in ev:
                maybe_greet(sender_id)
                send_message(sender_id, "Dạ anh/chị muốn xem mẫu nào ạ?")
                return "ok"

    return "ok"


# ============================================
# ORDER FORM & API
# ============================================

def send_order_link(uid: str, ms: str):
    """
    Gửi link form đặt hàng cho 1 sản phẩm cụ thể, dùng DOMAIN + route /o/<ms>.
    """
    base = DOMAIN or ""
    if base and not base.startswith("http"):
        base = "https://" + base
    url = f"{base}/o/{quote(ms)}"
    msg = f"Để chốt đơn nhanh, anh/chị điền giúp em thông tin nhận hàng tại đây ạ: {url}"
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
    # giả định bạn đã có file static/order-form.html
    return send_from_directory("static", "order-form.html")


@app.route("/api/get-product")
def api_get_product():
    load_products()
    ms = (request.args.get("ms") or "").upper()
    if ms not in PRODUCTS:
        return {"error": "not_found"}, 404

    row = PRODUCTS[ms]
    image = ""
    imgs = extract_images(row)
    if imgs:
        image = imgs[0]

    return {
        "ms": ms,
        "name": row.get("Ten", ""),
        "price": row.get("Gia", ""),
        "desc": row.get("MoTa", ""),
        "image": image,
    }


@app.route("/api/order", methods=["POST"])
def api_order():
    data = request.json or {}
    print("ORDER RECEIVED:", data)

    uid = data.get("uid") or data.get("user_id")
    ms = (data.get("ms") or data.get("product_code") or "").upper()

    if uid:
        msg = (
            "✅ Shop đã nhận đơn của anh/chị ạ:\n"
            f"- Sản phẩm: {data.get('productName', '')} ({ms})\n"
            f"- Màu: {data.get('color', '')}\n"
            f"- Size: {data.get('size', '')}\n"
            f"- Số lượng: {data.get('quantity', '')}\n"
            f"- Thành tiền: {data.get('total', '')}\n"
            f"- Khách: {data.get('customerName', '')}\n"
            f"- SĐT: {data.get('phone', '')}\n"
            f"- Địa chỉ: {data.get('home', '')}, {data.get('ward', '')}, {data.get('district', '')}, {data.get('province', '')}\n\n"
            "Trong ít phút nữa bên em sẽ gọi xác nhận, anh/chị để ý điện thoại giúp em nha ❤️"
        )
        send_message(uid, msg)

    return {"status": "ok"}


# ============================================
# HEALTHCHECK & START
# ============================================

@app.route("/")
def home():
    load_products()
    return f"Chatbot OK – {len(PRODUCTS)} products loaded."


# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    load_products(force=True)
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
