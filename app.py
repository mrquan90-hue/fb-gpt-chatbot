import os
import json
import re
import time
import csv
from collections import defaultdict
from urllib.parse import quote

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
    "last_postback": None,   # <<< thêm biến này để chống lặp carousel
})

PRODUCTS = {}
LAST_LOAD = 0
LOAD_TTL = 300  # 5 phút

# ============================================
# TỪ KHOÁ ĐẶT HÀNG
# ============================================

ORDER_KEYWORDS = [
    "đặt hàng nha","ok đặt","ok mua","ok em","ok e","mua 1 cái","mua cái này",
    "mua luôn","chốt","lấy mã","lấy mẫu","lấy luôn","lấy em này","lấy e này",
    "gửi cho","ship cho","ship 1 cái","chốt 1 cái","cho tôi mua","tôi lấy nhé",
    "cho mình đặt","tôi cần mua","xác nhận đơn hàng","tôi đồng ý mua",
    "làm đơn cho tôi","tôi chốt đơn nhé","cho xin 1 cái","cho đặt 1 chiếc",
    "tạo đơn","xuống đơn","lấy nha","lấy nhé","mua nha","mình lấy đây",
    "order nhé",
]

# ============================================
# FACEBOOK UTIL
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
    try:
        resp = requests.get(image_url, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print("DOWNLOAD IMG ERROR:", e, "URL:", image_url)
        return

    files = {"filedata": ("image.jpg", resp.content, "image/jpeg")}
    params = {"access_token": PAGE_ACCESS_TOKEN}
    data = {
        "recipient": json.dumps({"id": uid}, ensure_ascii=False),
        "message": json.dumps({
            "attachment": {"type": "image", "payload": {}}
        }, ensure_ascii=False),
        "messaging_type": "RESPONSE",
    }

    try:
        r = requests.post(
            "https://graph.facebook.com/v18.0/me/messages",
            params=params, data=data, files=files, timeout=30
        )
        print("SEND IMG:", r.status_code, r.text)
    except Exception as e:
        print("SEND IMG ERROR:", e)


# ============================================
# CAROUSEL
# ============================================

def send_carousel_template(recipient_id: str, products_data: list) -> None:
    try:
        elements = []
        for product in products_data[:10]:
            imgs = parse_image_urls(product.get("Images", ""))
            if not imgs:
                continue
            img = imgs[0]

            elements.append({
                "title": f"[{product.get('MS','')}] {product.get('Ten','')}",
                "subtitle": f"💰 Giá: {product.get('Gia','')}",
                "image_url": img,
                "buttons": [
                    {"type": "postback", "title": "📋 Xem chi tiết",
                     "payload": f"VIEW_{product.get('MS','')}"},
                    {"type": "postback", "title": "🛒 Chọn sản phẩm",
                     "payload": f"SELECT_{product.get('MS','')}"}
                ]
            })

        if not elements:
            return

        url = "https://graph.facebook.com/v18.0/me/messages"
        params = {"access_token": PAGE_ACCESS_TOKEN}
        payload = {
            "recipient": {"id": recipient_id},
            "message": {
                "attachment": {"type": "template",
                               "payload": {"template_type": "generic",
                                           "elements": elements}}
            },
            "messaging_type": "RESPONSE"
        }
        r = requests.post(url, params=params, json=payload, timeout=15)
        print("SEND CAROUSEL:", r.status_code, r.text)

    except Exception as e:
        print("SEND CAROUSEL ERROR:", e)
# ============================================
# REHOST IMAGE (optional)
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
        js = r.json()
        return js.get("image", {}).get("url", url)
    except:
        return url


# ============================================
# LOAD PRODUCTS
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

        data = {}
        for raw in reader:
            row = dict(raw)
            ms = (row.get("Mã sản phẩm") or "").strip()
            if not ms:
                continue

            row["MS"] = ms
            row["Ten"] = (row.get("Tên sản phẩm") or "").strip()
            row["Gia"] = (row.get("Giá bán") or "").strip()
            row["MoTa"] = (row.get("Mô tả") or "").strip()
            row["Images"] = (row.get("Images") or "").strip()
            row["Videos"] = (row.get("Videos") or "").strip()
            row["Tồn kho"] = (row.get("Tồn kho") or "").strip()
            row["màu (Thuộc tính)"] = (row.get("màu (Thuộc tính)") or "").strip()
            row["size (Thuộc tính)"] = (row.get("size (Thuộc tính)") or "").strip()

            data[ms] = row

        PRODUCTS = data
        LAST_LOAD = now
        print(f"📦 Loaded {len(PRODUCTS)} products.")

    except Exception as e:
        print("❌ load_products error:", e)
        PRODUCTS = {}


# ============================================
# IMAGE PARSER
# ============================================

def parse_image_urls(images_field: str) -> list:
    if not images_field:
        return []
    urls = [u.strip() for u in images_field.split(",") if u.strip()]

    seen = set()
    result = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            result.append(u)
    return result


# ============================================
# GPT VISION
# ============================================

def gpt_analyze_image(url: str):
    if not client:
        return None, None

    try:
        prompt = f"""
        Bạn là trợ lý bán hàng. Hãy mô tả sản phẩm trong ảnh
        và cố gắng tìm mã sản phẩm trong danh sách:
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
# PRODUCT CODE EXTRACTION
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

    cand = []
    for ms in PRODUCTS.keys():
        if not ms.upper().startswith("MS"):
            continue
        digits = re.sub(r"\D", "", ms)
        if digits.endswith(code):
            cand.append(ms)
    if not cand:
        return None

    cand.sort(key=len, reverse=True)
    return cand[0]


def resolve_best_ms(ctx: dict):
    if ctx.get("last_ms") and ctx["last_ms"] in PRODUCTS:
        return ctx["last_ms"]

    for k in ["vision_ms", "inbox_entry_ms", "caption_ms"]:
        if ctx.get(k) in PRODUCTS:
            return ctx[k]

    return None


# ============================================
# GPT REPLY ENGINE
# ============================================

def gpt_reply(history: list, product_row: dict | None):
    if not client:
        return "Dạ hệ thống AI đang bận, anh/chị chờ em 1 lát nhé."

    sys = """
    Bạn là trợ lý bán hàng của shop quần áo.
    - Xưng em, gọi khách là anh/chị.
    - Trả lời ngắn gọn, thân thiện.
    - Không bịa đặt thông tin sản phẩm.
    """

    if product_row:
        sys += f"""
        Dữ liệu sản phẩm:
        - Tên: {product_row.get('Ten','')}
        - Mô tả: {product_row.get('MoTa','')}
        - Giá bán: {product_row.get('Gia','')}
        - Tồn kho: {product_row.get('Tồn kho','')}
        - Màu: {product_row.get('màu (Thuộc tính)','')}
        - Size: {product_row.get('size (Thuộc tính)','')}
        """

    if len(history) > 10:
        history = history[-10:]

    r = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "system", "content": sys}] + history,
        temperature=0.5,
    )
    return r.choices[0].message.content


# ============================================
# PRODUCT INFO BUILDER
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

    txt = f"[{ms}] {ten}\n"
    txt += f"\n✨ Ưu điểm nổi bật:\n- {highlight}\n" if highlight else ""
    if mau or size:
        txt += "\n🎨 Màu/Size:\n"
        if mau:
            txt += f"- Màu: {mau}\n"
        if size:
            txt += f"- Size: {size}\n"
    if gia:
        txt += f"\n💰 Giá bán: {gia}\n"
    if tonkho:
        txt += f"📦 Tồn kho: {tonkho}\n"
    txt += "\n👉 Anh/chị xem giúp em mẫu này có hợp gu không ạ?"
    return txt


def send_product_info(uid: str, ms: str):
    load_products()
    if ms not in PRODUCTS:
        send_message(uid, "Dạ em chưa tìm thấy mã này trong kho ạ.")
        return

    row = PRODUCTS[ms]
    send_message(uid, build_product_info_text(ms, row))

    imgs = parse_image_urls(row.get("Images", ""))
    imgs = imgs[:5]
    for u in imgs:
        send_image(uid, rehost_image(u))
# ============================================
# SEND RECOMMENDATIONS
# ============================================

def send_recommendations(uid: str):
    load_products()
    if not PRODUCTS:
        return

    prods = list(PRODUCTS.values())[:5]
    send_message(uid, "Em gửi anh/chị 5 mẫu đang được nhiều khách quan tâm ạ:")

    for row in prods:
        ms = row.get("MS", "")
        ten = row.get("Ten", "")
        gia = row.get("Gia", "")
        send_message(uid, f"- [{ms}] {ten} – Giá: {gia}")

        imgs = parse_image_urls(row.get("Images", ""))
        if imgs:
            send_image(uid, rehost_image(imgs[0]))


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
        "Em là trợ lý của shop, hỗ trợ mình xem mẫu – tư vấn size – chốt đơn nhanh ạ."
    )
    send_message(uid, msg)
    ctx["greeted"] = True

    if not has_ms and not ctx["carousel_sent"]:
        send_message(uid, "Em gửi anh/chị 5 mẫu đang hot nhất hiện tại ạ:")
        send_product_carousel(uid)
        ctx["carousel_sent"] = True
        ctx["recommended_sent"] = True


# ============================================
# HANDLE IMAGE
# ============================================

def handle_image(uid: str, image_url: str):
    load_products()
    ctx = USER_CONTEXT[uid]

    if not ctx["greeted"] and not ctx.get("inbox_entry_ms"):
        maybe_greet(uid, ctx, has_ms=False)

    hosted = rehost_image(image_url)
    ms, desc = gpt_analyze_image(hosted)

    if ms and ms in PRODUCTS:
        ctx["vision_ms"] = ms
        ctx["last_ms"] = ms
        ctx["product_info_sent_ms"] = ms
        send_message(uid, f"Dạ ảnh này giống mẫu [{ms}] của shop ạ. Em gửi thông tin cho mình nhé 💕")
        send_product_info(uid, ms)
    else:
        send_message(uid, "Dạ ảnh này hơi khó nhận, anh/chị gửi mã hoặc caption giúp em nhé.")


# ============================================
# HANDLE TEXT
# ============================================

def handle_text(uid: str, text: str):
    load_products()
    ctx = USER_CONTEXT[uid]

    ms_from_text = extract_ms(text)
    if not ms_from_text:
        short = extract_short_code(text)
        if short:
            ms_from_text = find_ms_by_short_code(short)

    if ms_from_text:
        ctx["last_ms"] = ms_from_text

    ms = resolve_best_ms(ctx)
    maybe_greet(uid, ctx, has_ms=bool(ms))

    if ms and ms in PRODUCTS and ctx.get("product_info_sent_ms") != ms:
        ctx["product_info_sent_ms"] = ms
        send_product_info(uid, ms)

    ctx["history"].append({"role": "user", "content": text})

    product = PRODUCTS.get(ms) if ms in PRODUCTS else None
    reply = gpt_reply(ctx["history"], product)
    ctx["history"].append({"role": "assistant", "content": reply})
    send_message(uid, reply)

    lower = text.lower()
    if ms and any(kw in lower for kw in ORDER_KEYWORDS):
        send_message(uid,
            "Dạ anh/chị cho em xin họ tên + SĐT + địa chỉ + màu + size ạ, em lên đơn ngay ❤️"
        )


# ============================================
# ECHO HANDLER
# ============================================

def handle_echo_outgoing(page_id: str, user_id: str, text: str):
    if not user_id:
        return
    ms = extract_ms(text)
    if ms:
        ctx = USER_CONTEXT[user_id]
        ctx["inbox_entry_ms"] = ms
        ctx["last_ms"] = ms
        print(f"[ECHO] Fchat/page ghi nhận mã {ms} cho user {user_id}")


# ============================================
# WEBHOOK (POSTBACK FIX)
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

            sender = ev.get("sender", {}).get("id")
            recipient = ev.get("recipient", {}).get("id")
            if not sender:
                continue

            msg = ev.get("message", {}) or {}
            ctx = USER_CONTEXT[sender]

            # =======================================================
            # 1) ECHO
            # =======================================================
            if msg.get("is_echo"):
                text = msg.get("text") or ""
                handle_echo_outgoing(sender, recipient, text)
                continue

            # =======================================================
            # 2) POSTBACK (BẢN ĐÃ FIX LOOP)
            # =======================================================
            if "postback" in ev:
                payload = ev["postback"].get("payload")
                print(f"[POSTBACK] User {sender}: {payload}")

                # 🔥 FIX LOOP: CHẶN LẶP POSTBACK
                if payload == ctx.get("last_postback"):
                    print("⚠ Bỏ qua postback lặp:", payload)
                    return "ok"

                ctx["last_postback"] = payload

                # ====== VIEW ======
                if payload and payload.startswith("VIEW_"):
                    code = payload.replace("VIEW_", "")
                    if code in PRODUCTS:
                        ctx["last_ms"] = code
                        ctx["product_info_sent_ms"] = code
                        send_product_info(sender, code)
                    else:
                        send_message(sender, f"Dạ em không tìm thấy mã {code} ạ.")
                    return "ok"

                # ====== SELECT ======
                if payload and payload.startswith("SELECT_"):
                    code = payload.replace("SELECT_", "")
                    if code in PRODUCTS:
                        ctx["last_ms"] = code
                        ctx["product_info_sent_ms"] = code
                        pd = PRODUCTS[code]
                        send_message(
                            sender,
                            f"Bạn đã chọn 🎉 [{code}] {pd.get('Ten','')}\n"
                            "Cho em xin màu – size – số lượng để lên đơn ạ 🛍️"
                        )
                    else:
                        send_message(sender, f"Không tìm thấy mã {code} ạ.")
                    return "ok"

                # ====== REFERRAL TRONG POSTBACK ======
                ref = ev["postback"].get("referral", {}).get("ref")
                if ref:
                    ms_ref = extract_ms(ref)
                    if ms_ref and ms_ref in PRODUCTS:
                        ctx["inbox_entry_ms"] = ms_ref
                        ctx["last_ms"] = ms_ref
                        ctx["greeted"] = True
                        send_product_info(sender, ms_ref)
                    return "ok"

                # ====== POSTBACK KHÁC ======
                if not ctx.get("greeted"):
                    maybe_greet(sender, ctx, has_ms=False)
                send_message(sender, "Anh/chị đang quan tâm mẫu nào ạ?")
                return "ok"

            # =======================================================
            # 3) REFERRAL (Click-to-Message)
            # =======================================================
            ref = (
                ev.get("referral", {}).get("ref") or
                ev.get("postback", {}).get("referral", {}).get("ref")
            )
            if ref:
                ms_ref = extract_ms(ref)
                if ms_ref:
                    ctx["inbox_entry_ms"] = ms_ref
                    ctx["last_ms"] = ms_ref
                    ctx["greeted"] = True
                    send_product_info(sender, ms_ref)
                return "ok"

            # =======================================================
            # 4) ATTACHMENTS (IMAGE)
            # =======================================================
            if "attachments" in msg:
                for att in msg["attachments"]:
                    if att.get("type") == "image":
                        img = att["payload"].get("url")
                        if img:
                            handle_image(sender, img)
                            return "ok"
                continue

            # =======================================================
            # 5) TEXT
            # =======================================================
            if "text" in msg:
                handle_text(sender, msg.get("text", ""))
                return "ok"

    return "ok"
# ============================================
# ORDER LINK & ORDER API
# ============================================

def send_order_link(uid: str, ms: str):
    base = DOMAIN or ""
    if base and not base.startswith("http"):
        base = "https://" + base

    url = f"{base}/o/{quote(ms)}"
    txt = f"Anh/chị có thể đặt hàng nhanh tại đây ạ: {url}"
    send_message(uid, txt)


@app.route("/o/<ms>")
def order_link(ms: str):
    load_products()
    ms = ms.upper()

    if ms not in PRODUCTS:
        return f"Không tìm thấy sản phẩm {ms}", 404

    row = PRODUCTS[ms]
    ten = row.get("Ten", "")
    gia = row.get("Gia", "")
    mota = row.get("MoTa", "")

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
    return send_from_directory("static", "order-form.html")


@app.route("/api/get-product")
def api_get_product():
    load_products()
    ms = (request.args.get("ms") or "").upper()

    if ms not in PRODUCTS:
        return {"error": "not_found"}, 404

    row = PRODUCTS[ms]
    imgs = parse_image_urls(row.get("Images", ""))
    img = imgs[0] if imgs else ""

    return {
        "ms": ms,
        "name": row.get("Ten", ""),
        "price": row.get("Gia", ""),
        "desc": row.get("MoTa", ""),
        "image": img,
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
            f"- Sản phẩm: {data.get('productName','')} ({ms})\n"
            f"- Màu: {data.get('color','')}\n"
            f"- Size: {data.get('size','')}\n"
            f"- Số lượng: {data.get('quantity','')}\n"
            f"- Thành tiền: {data.get('total','')}\n"
            f"- Khách: {data.get('customerName','')}\n"
            f"- SĐT: {data.get('phone','')}\n"
            f"- Địa chỉ: {data.get('home','')}, {data.get('ward','')}, "
            f"{data.get('district','')}, {data.get('province','')}\n\n"
            "Trong ít phút nữa nhân viên sẽ gọi xác nhận, anh/chị để ý điện thoại giúp em nhé ❤️"
        )
        send_message(uid, msg)

    return {"status": "ok"}


# ============================================
# HEALTH CHECK
# ============================================

@app.route("/")
def home():
    load_products()
    return f"Chatbot OK – {len(PRODUCTS)} products loaded."


# ============================================
# START SERVER
# ============================================

if __name__ == "__main__":
    load_products(force=True)
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
