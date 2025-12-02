import os
import requests
from flask import Flask, request
import pandas as pd
import re
import time

app = Flask(__name__)

PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")

# =========================================
# 1. GLOBAL FLAGS
# =========================================
BOT_ENABLED = True                     # bật/tắt bot
processed_messages = set()             # chống xử lý trùng
last_sent_media = {}                   # chống gửi ảnh 2 lần trong 1 phiên

# =========================================
# 2. LOAD GOOGLE SHEET
# =========================================
SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv"

df = None

def load_sheet():
    global df
    try:
        df = pd.read_csv(SHEET_CSV_URL)
        print(f"[Sheet] Loaded {len(df)} rows")
    except Exception as e:
        print("[Sheet] Load ERROR:", e)

load_sheet()

# =========================================
# 3. FACEBOOK SEND MESSAGE
# =========================================
def send_text(recipient_id, text):
    if not BOT_ENABLED:
        return

    url = f"https://graph.facebook.com/v17.0/me/messages?access_token={PAGE_ACCESS_TOKEN}"
    payload = {
        "recipient": {"id": recipient_id},
        "message": {"text": text}
    }
    requests.post(url, json=payload)

def send_image(recipient_id, image_url, product_id=None):
    """
    - Chỉ gửi ảnh 1 lần duy nhất cho mỗi sản phẩm mỗi khách.
    - Không gửi lại trong vòng 24 giờ.
    """
    if not BOT_ENABLED:
        return

    if product_id:
        if recipient_id not in last_sent_media:
            last_sent_media[recipient_id] = set()

        key = f"{product_id}-{image_url}"
        if key in last_sent_media[recipient_id]:
            print("[IMG] SKIPPED duplicate:", key)
            return

        last_sent_media[recipient_id].add(key)

    url = f"https://graph.facebook.com/v17.0/me/messages?access_token={PAGE_ACCESS_TOKEN}"
    payload = {
        "recipient": {"id": recipient_id},
        "message": {
            "attachment": {
                "type": "image",
                "payload": {"url": image_url, "is_reusable": True}
            }
        }
    }
    requests.post(url, json=payload)

# =========================================
# 4. ANTI LOOP
# =========================================

def is_echo(event):
    return "message" in event and event["message"].get("is_echo") == True

def get_mid(event):
    return event.get("message", {}).get("mid")

def processed(event):
    """
    Check mid trùng để tránh lặp lại xử lý FB retry.
    """
    mid = get_mid(event)
    if not mid:
        return False

    if mid in processed_messages:
        return True

    processed_messages.add(mid)
    return False

# =========================================
# 5. PRODUCT LOOKUP
# =========================================

def find_product_by_code(ms_code):
    """Tim sản phẩm theo mã MSxxxxx."""
    if df is None:
        return None
    matched = df[df["Mã sản phẩm"].astype(str).str.contains(ms_code, na=False)]
    return matched if len(matched) > 0 else None

def extract_ms_from_text(text):
    """Tìm Mã sản phẩm trong dạng [MSxxxx] hoặc MSxxxx."""
    match = re.search(r"MS(\d+)", text.upper())
    return f"MS{match.group(1)}" if match else None

def get_clean_images(rows):
    """Lấy ảnh từ tất cả Image rows, loại trùng và watermark Trung Quốc."""
    all_imgs = []
    for imgcell in rows["Images"].fillna(""):
        parts = re.split(r"[\n,]", str(imgcell))
        for p in parts:
            url = p.strip()
            if len(url) > 5:
                all_imgs.append(url)

    # loại trùng
    all_imgs = list(dict.fromkeys(all_imgs))

    # loại watermark chữ Trung Quốc
    clean = []
    for url in all_imgs:
        if any(bad in url.lower() for bad in ["taobao", "tmall", "1688"]):
            continue
        clean.append(url)

    return clean[:10]  # gửi tối đa 10 ảnh

# =========================================
# 6. PRODUCT CONSULT
# =========================================
def consult_product(user_id, rows):
    product_name = rows["Tên sản phẩm"].iloc[0]
    description = rows["Mô tả"].iloc[0] if "Mô tả" in rows else ""
    price_list = rows["Giá bán"].unique()

    # Tên
    send_text(user_id, f"🔎 *{product_name}*")

    # Ảnh chung
    imgs = get_clean_images(rows)
    for img in imgs:
        send_image(user_id, img, product_id=product_name)
        time.sleep(0.4)

    # Ưu điểm
    short = description[:220] + "..."
    send_text(user_id, f"✨ Ưu điểm nổi bật:\n{short}")

    # Giá
    if len(price_list) == 1:
        send_text(user_id, f"💵 Giá đặc biệt: {price_list[0]:,}đ miễn ship")
    else:
        send_text(user_id, "💵 *Bảng giá theo biến thể:*")
        for price in sorted(price_list):
            subrows = rows[rows["Giá bán"] == price]
            colors = subrows["màu (Thuộc tính)"].fillna("").unique()
            size = subrows["size (Thuộc tính)"].fillna("").unique()
            send_text(user_id, f"- Màu: {','.join(colors)} Size: {','.join(size)} → {price:,}đ")

    # CTA
    send_text(user_id, "👉 Anh/chị quan tâm màu nào ạ?")

# =========================================
# 7. WEBHOOK
# =========================================

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    global BOT_ENABLED

    if request.method == "GET":
        if request.args.get("hub.verify_token") == VERIFY_TOKEN:
            return request.args.get("hub.challenge")
        return "Invalid token"

    data = request.json
    print("[Webhook]", data)

    entry = data.get("entry", [])
    for e in entry:
        for event in e.get("messaging", []):

            # ==========================
            # ANTI-LOOP LAYER 1 (echo)
            # ==========================
            if is_echo(event):
                print("[SKIP] Echo message.")
                continue

            # ==========================
            # ANTI-LOOP LAYER 2 (mid)
            # ==========================
            if processed(event):
                print("[SKIP] Duplicate mid.")
                continue

            # ==========================
            # ADMIN COMMANDS
            # ==========================
            sender = event["sender"]["id"]
            msg = event.get("message", {}).get("text", "")

            if msg.lower() == "tắt bot":
                BOT_ENABLED = False
                send_text(sender, "⚠️ Bot đã tắt. Không tự động trả lời nữa.")
                continue

            if msg.lower() == "bật bot":
                BOT_ENABLED = True
                send_text(sender, "✅ Bot đã bật lại.")
                continue

            # ==========================
            # ANTI-LOOP LAYER 3 (bot off)
            # ==========================
            if not BOT_ENABLED:
                print("[SKIP] Bot đang tắt.")
                continue

            # ==========================
            # PRODUCT CONSULT
            # ==========================
            ms = extract_ms_from_text(msg)
            if ms:
                prod = find_product_by_code(ms)
                if prod is not None:
                    consult_product(sender, prod)
                else:
                    send_text(sender, "❌ Shop không tìm thấy mã sản phẩm này.")
            else:
                send_text(sender, "Bạn muốn xem mã sản phẩm nào ạ?")

    return "ok"


@app.route("/")
def home():
    return "Chatbot đang chạy."


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
