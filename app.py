# ============================================
# fb-gpt-chatbot | FULL VERSION | PART 1/4
# ============================================

import os
import json
import re
import requests
import csv
import time
from flask import Flask, request
from datetime import datetime
from urllib.parse import quote
from collections import defaultdict

from openai import OpenAI

app = Flask(__name__)

# ============================================
# LOAD ENVIRONMENT VARIABLES (Render.com)
# ============================================

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")
FREEIMAGE_API_KEY = os.getenv("FREEIMAGE_API_KEY")

# ⚠️ Đây là dòng bạn cần – đúng theo Render của bạn
SHEET_URL = os.getenv("SHEET_CSV_URL")  

# DOMAIN để tạo link đặt hàng
DOMAIN = os.getenv("DOMAIN", "fb-gpt-chatbot.onrender.com")

client = OpenAI(api_key=OPENAI_API_KEY)

# ============================================
# GLOBAL CONTEXT LƯU TRẠNG THÁI TỪNG KHÁCH
# ============================================

USER_CONTEXT = defaultdict(lambda: {
    "last_ms": None,            # mã sản phẩm cuối cùng tư vấn
    "inbox_entry_ms": None,     # mã từ ref / Fchat
    "caption_ms": None,         # mã từ caption bài viết
    "vision_ms": None,          # mã từ ảnh GPT phân tích
    "history": [],              # lịch sử hội thoại
    "greeted": False,           # chào hỏi hay chưa
})

# ============================================
# TIỆN ÍCH: GỬI TIN NHẮN FB
# ============================================

def send_message(recipient_id, message_text):
    """
    Gửi tin nhắn dạng text tới Messenger.
    (Bot không gửi lại nội dung của chính nó nhờ rule anti-loop)
    """
    if not message_text:
        return

    url = "https://graph.facebook.com/v16.0/me/messages"
    params = {"access_token": PAGE_ACCESS_TOKEN}

    payload = {
        "recipient": {"id": recipient_id},
        "message": {"text": message_text},
    }

    response = requests.post(url, params=params, json=payload)
    print("FB SEND RESPONSE:", response.text)


def send_image(recipient_id, image_url):
    """
    Gửi hình ảnh cho khách.
    Dùng ảnh rehost → tránh lỗi domain.
    """
    url = "https://graph.facebook.com/v16.0/me/messages"
    params = {"access_token": PAGE_ACCESS_TOKEN}

    payload = {
        "recipient": {"id": recipient_id},
        "message": {
            "attachment": {
                "type": "image",
                "payload": {"url": image_url, "is_reusable": True}
            }
        },
    }

    response = requests.post(url, params=params, json=payload)
    print("FB SEND IMAGE RESPONSE:", response.text)

# ============================================
# LOAD GOOGLE SHEET → DANH SÁCH SẢN PHẨM
# ============================================

PRODUCTS = {}

def load_products():
    """
    Load file CSV từ Google Sheet.
    Cấu trúc yêu cầu:
    MS, Ten, MoTa, Gia, Anh1, Anh2,...
    """
    global PRODUCTS

    print("🟦 Loading product sheet:", SHEET_URL)

    try:
        r = requests.get(SHEET_URL)
        r.encoding = "utf-8"
        lines = r.text.splitlines()
        reader = csv.DictReader(lines)

        products = {}
        for row in reader:
            ms = row.get("MS") or row.get("Mã sản phẩm") or ""
            ms = ms.strip()

            if not ms:
                continue

            products[ms] = row

        PRODUCTS = products
        print(f"📦 Loaded {len(PRODUCTS)} products.")

    except Exception as e:
        print("❌ ERROR load_products:", e)


# ============================================
# HÀM TÌM ẢNH TỪ ROW SẢN PHẨM
# ============================================

def extract_images(row):
    """
    Tìm tất cả cột chứa link ảnh.
    VD: cột tên chứa 'Ảnh', 'Image', 'Img'
    """
    imgs = []
    for key, val in row.items():
        if any(k in key.lower() for k in ["ảnh", "image", "img"]):
            if val and str(val).startswith("http"):
                imgs.append(val.strip())
    return imgs


# ============================================
# FREEIMAGE.HOST – REHOST ẢNH
# ============================================

def rehost_image(url):
    """
    Rehost ảnh sang freeimage.host API
    """
    try:
        api = "https://freeimage.host/api/1/upload"
        payload = {
            "key": FREEIMAGE_API_KEY,
            "source": url,
            "action": "upload"
        }

        r = requests.post(api, data=payload, timeout=20)
        data = r.json()

        if "image" in data and "url" in data["image"]:
            return data["image"]["url"]

        print("⚠️ Rehost fail:", data)
        return url

    except Exception as e:
        print("❌ Rehost error:", e)
        return url


# ============================================
# GPT VISION – ĐỌC ẢNH KHÁCH GỬI
# ============================================

def gpt_analyze_image(image_url):
    """
    GPT Vision phân tích ảnh → mô tả → tìm MS phù hợp.
    """
    try:
        prompt = """
        Bạn là trợ lý bán hàng. Hãy mô tả chi tiết sản phẩm trong ảnh,
        sau đó tìm mã sản phẩm (MSxxxxxx) phù hợp nhất 
        từ danh sách sau: %s

        Trả về JSON:
        {
            "description": "...",
            "matched_ms": "MSxxxx"
        }
        """ % ", ".join(PRODUCTS.keys())

        result = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": "Bạn là trợ lý tư vấn bán hàng."},
                {"role": "user", "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": image_url}},
                ]}
            ],
            temperature=0.3
        )

        text = result.choices[0].message.content
        print("VISION RAW:", text)

        match = re.search(r"(MS\d+)", text)
        ms = match.group(1) if match else None

        return ms, text

    except Exception as e:
        print("❌ GPT Vision error:", e)
        return None, None

# ============================================
# EXTRACT MÃ SẢN PHẨM TỪ TEXT / COMMENT
# ============================================

def extract_ms(text):
    if not text:
        return None
    match = re.search(r"(MS\d+)", text, flags=re.IGNORECASE)
    if match:
        return match.group(1).upper()
    return None
# ============================================
# TÌM MÃ TỪ NHIỀU NGUỒN (caption, inbox, vision…)
# ============================================

def resolve_best_ms(ctx):
    """
    Chọn mã sản phẩm hợp lệ theo độ ưu tiên:
    1. Ảnh khách gửi (vision_ms)
    2. Khách tự nhắn có MS (text)
    3. Mã từ tin Fchat gửi (#MSxxxx → inbox_entry_ms)
    4. Mã trích từ caption bài viết (caption_ms)
    5. Mã cuối cùng bot đã tư vấn (last_ms)
    """
    for key in ["vision_ms", "inbox_entry_ms", "caption_ms", "last_ms"]:
        if ctx.get(key):
            return ctx[key]
    return None


# ============================================
# GPT TƯ VẤN NGỮ CẢNH (NO-RULE ENGINE)
# ============================================

def gpt_reply(context_messages, product_row=None):
    """
    GPT tư vấn theo ngữ cảnh cuộc hội thoại.
    Nếu đã biết mã sản phẩm → tư vấn sâu.
    Nếu chưa biết → hỏi nhu cầu, gợi ý.
    """

    system_prompt = """
    Bạn là trợ lý bán hàng chuyên nghiệp.
    - Xưng hô: em – anh/chị
    - Giọng văn tự nhiên, lễ phép.
    - Không bịa đặt thông số.
    - Chỉ dùng đúng dữ liệu từ sản phẩm.
    - Nếu khách không nói về mua hàng → vẫn tư vấn lịch sự.
    - Luôn giữ mạch hội thoại.

    Nếu đã biết sản phẩm:
      - Tóm tắt sản phẩm.
      - Gợi ý màu, size, giá.
      - Hỏi khách muốn chốt đơn không.

    Nếu CHƯA biết sản phẩm:
      - Hỏi rõ nhu cầu khách.
      - Đề xuất 2–3 sản phẩm phù hợp từ dữ liệu shop.
    """

    if product_row:
        pd = (
            f"Tên: {product_row.get('Ten', '')}\n"
            f"Mô tả: {product_row.get('MoTa', '')}\n"
            f"Giá: {product_row.get('Gia', '')}\n"
        )
        system_prompt += "\nDữ liệu sản phẩm:\n" + pd

    try:
        result = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": system_prompt},
            ] + context_messages,
            temperature=0.4
        )
        return result.choices[0].message.content

    except Exception as e:
        print("GPT error:", e)
        return "Dạ em xin lỗi anh/chị, hệ thống đang bận. Em kiểm tra lại ngay ạ."


# ============================================
# XỬ LÝ HÌNH ẢNH KHÁCH GỬI
# ============================================

def handle_image_message(sender_id, image_url):
    ctx = USER_CONTEXT[sender_id]

    print("📸 Image from user:", image_url)

    # 1) rehost ảnh →
    hosted_url = rehost_image(image_url)
    print("🟦 Hosted:", hosted_url)

    # 2) phân tích →
    ms, desc = gpt_analyze_image(hosted_url)
    print("VISION RESULT:", ms, desc)

    if ms and ms in PRODUCTS:
        ctx["vision_ms"] = ms
        ctx["last_ms"] = ms
        product = PRODUCTS[ms]

        send_message(sender_id, f"Dạ em thấy ảnh anh/chị gửi giống mẫu **{ms}** đó ạ!")
        images = extract_images(product)
        if images:
            send_image(sender_id, rehost_image(images[0]))

        USER_CONTEXT[sender_id]["history"].append(
            {"role": "assistant", "content": f"(Vision detect {ms})"}
        )

    else:
        send_message(sender_id, "Dạ để em xem kỹ hơn ạ… hình này chưa rõ sản phẩm ạ.")

    return


# ============================================
# CHÀO HỎI KHÁCH 1 LẦN DUY NHẤT
# ============================================

def maybe_greet(sender_id):
    ctx = USER_CONTEXT[sender_id]
    if not ctx["greeted"]:
        ctx["greeted"] = True
        send_message(sender_id, 
            "Dạ em chào anh/chị ạ 😊 Em là trợ lý bán hàng của shop, em hỗ trợ mình xem sản phẩm và chốt đơn nhanh ạ!")


# ============================================
# XỬ LÝ TIN NHẮN KHÁCH GỬI (TEXT)
# ============================================

def handle_text_message(sender_id, text):
    ctx = USER_CONTEXT[sender_id]
    maybe_greet(sender_id)

    # 1) detect mã khách tự nhắn
    ms_from_text = extract_ms(text)
    if ms_from_text:
        ctx["last_ms"] = ms_from_text

    # 2) Chọn mã hợp lệ nhất
    ms = resolve_best_ms(ctx)

    USER_CONTEXT[sender_id]["history"].append({"role": "user", "content": text})

    if ms and ms in PRODUCTS:
        product = PRODUCTS[ms]
        reply = gpt_reply(USER_CONTEXT[sender_id]["history"], product_row=product)
    else:
        reply = gpt_reply(USER_CONTEXT[sender_id]["history"])

    USER_CONTEXT[sender_id]["history"].append({"role": "assistant", "content": reply})
    send_message(sender_id, reply)
# ============================================
# XỬ LÝ REF → LẤY MÃ TỪ FCHAT / BÀI VIẾT
# ============================================

def extract_ms_from_ref(ref):
    """
    ref dạng: MS000123 | POST:1758895174936344 | any custom ref
    Ưu tiên tìm MSxxxx trong ref.
    """
    if not ref:
        return None
    match = re.search(r"(MS\d+)", ref, flags=re.IGNORECASE)
    return match.group(1).upper() if match else None


# ============================================
# XỬ LÝ TIN NHẮN ĐẾN TỪ MỘT POST COMMENT
# ============================================

def extract_post_ms(event):
    """
    Facebook không gửi caption trực tiếp trong webhook.
    Nhưng Fchat gửi ref dạng: "MS000123" → ta lấy được.

    Trường hợp khách nhấn "Gửi tin nhắn" dưới bài viết:
    FB gửi ref_id = <post_id>
    Nếu Fchat đã gắn ref: bot sẽ nhận ref dạng: "MS000123"
    """
    try:
        ref = event.get("ref")
        if ref:
            ms = extract_ms_from_ref(ref)
            if ms:
                return ms
        return None
    except:
        return None


# ============================================
# XỬ LÝ WEBHOOK FACEBOOK
# ============================================

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        verify = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")
        if verify == VERIFY_TOKEN:
            return challenge
        return "Verification token mismatch", 403

    # POST → handle events
    data = request.get_json()
    print("📩 WEBHOOK DATA:", json.dumps(data, ensure_ascii=False))

    if "entry" not in data:
        return "ok"

    for entry in data["entry"]:
        if "messaging" not in entry:
            continue

        for event in entry["messaging"]:
            sender_id = event["sender"]["id"]

            # ============================
            # 1) Anti-loop: Tin nhắn echo
            # ============================
            if event.get("message", {}).get("is_echo"):
                print("⛔ Echo → IGNORE")
                continue

            ctx = USER_CONTEXT[sender_id]

            # ============================
            # 2) Lấy mã từ ref (Fchat gắn)
            # ============================
            ms_from_ref = extract_post_ms(event)
            if ms_from_ref:
                ctx["inbox_entry_ms"] = ms_from_ref
                ctx["last_ms"] = ms_from_ref
                print("📌 REF DETECT MS:", ms_from_ref)

            # ============================
            # 3) Message type: IMAGE
            # ============================
            if "message" in event and "attachments" in event["message"]:
                attachments = event["message"]["attachments"]
                for att in attachments:
                    if att["type"] == "image":
                        image_url = att["payload"]["url"]
                        handle_image_message(sender_id, image_url)
                        return "ok"
                continue

            # ============================
            # 4) Message type: TEXT
            # ============================
            if "message" in event and "text" in event["message"]:
                text = event["message"]["text"]
                handle_text_message(sender_id, text)
                return "ok"

            # ============================
            # 5) Postback (button, get started)
            # ============================
            if "postback" in event:
                pb = event["postback"]
                ref = pb.get("referral", {}).get("ref")
                if ref:
                    ms = extract_ms_from_ref(ref)
                    if ms:
                        ctx["inbox_entry_ms"] = ms
                        ctx["last_ms"] = ms

                maybe_greet(sender_id)
                send_message(sender_id, "Dạ anh/chị muốn xem mẫu nào ạ?")
                return "ok"

    return "ok"


# ============================================
# LINK ĐẶT HÀNG NGẮN GỌN
# ============================================

@app.route("/o/<ms>")
def order_link(ms):
    """
    URL đặt hàng chuẩn:
    https://<DOMAIN>/o/MS000123
    """
    ms = ms.upper()
    if ms not in PRODUCTS:
        return f"Không tìm thấy sản phẩm {ms}"

    pd = PRODUCTS[ms]
    ten = quote(pd.get("Ten", ""))
    gia = quote(pd.get("Gia", ""))
    mota = quote(pd.get("MoTa", ""))

    html = f"""
    <html><body>
    <h2>Đặt hàng: {ms}</h2>
    <p><b>Tên:</b> {ten}</p>
    <p><b>Giá:</b> {gia}</p>
    <p><b>Mô tả:</b> {mota}</p>
    </body></html>
    """
    return html
# ============================================
# RE-DEFINE load_products (chuẩn hóa cột)
# ============================================

def load_products():
    """
    Load CSV từ Google Sheet (SHEET_CSV_URL) và chuẩn hóa tên cột:
    - MS      ← MS / Mã sản phẩm / ma_san_pham
    - Ten     ← Ten / Tên sản phẩm / ten_san_pham / Title
    - MoTa    ← MoTa / Mô tả / Mo ta / Description
    - Gia     ← Gia / Giá bán / Price
    Giữ nguyên các cột còn lại (ảnh, thuộc tính...).
    """
    global PRODUCTS

    if not SHEET_URL:
        print("❌ SHEET_CSV_URL chưa được cấu hình.")
        PRODUCTS = {}
        return

    print("🟦 Reloading product sheet from:", SHEET_URL)
    try:
        r = requests.get(SHEET_URL, timeout=30)
        r.encoding = "utf-8"
        lines = r.text.splitlines()
        reader = csv.DictReader(lines)

        products = {}
        for raw_row in reader:
            row = dict(raw_row)

            ms = (
                row.get("MS")
                or row.get("Mã sản phẩm")
                or row.get("ma_san_pham")
                or row.get("Ma san pham")
                or row.get("MaSP")
                or ""
            )
            ms = str(ms).strip()
            if not ms:
                continue

            name = (
                row.get("Ten")
                or row.get("Tên sản phẩm")
                or row.get("ten_san_pham")
                or row.get("Title")
                or ""
            )
            desc = (
                row.get("MoTa")
                or row.get("Mô tả")
                or row.get("Mo ta")
                or row.get("Description")
                or ""
            )
            price = (
                row.get("Gia")
                or row.get("Giá bán")
                or row.get("Gia ban")
                or row.get("Price")
                or ""
            )

            row["MS"] = ms
            row["Ten"] = str(name).strip()
            row["MoTa"] = str(desc).strip()
            row["Gia"] = str(price).strip()

            products[ms] = row

        PRODUCTS = products
        print(f"📦 Loaded {len(PRODUCTS)} products (normalized).")

    except Exception as e:
        print("❌ ERROR load_products (override):", e)
        PRODUCTS = {}


# ============================================
# HEALTHCHECK & STARTUP
# ============================================

@app.route("/")
def index():
    return f"Chatbot OK – {len(PRODUCTS)} products loaded.", 200


@app.before_first_request
def startup():
    print("🚀 Flask starting, loading products...")
    load_products()


# ============================================
# MAIN ENTRY
# ============================================

if __name__ == "__main__":
    load_products()
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
