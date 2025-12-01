import os
import json
import requests
from flask import Flask, request
import pandas as pd
import re

app = Flask(__name__)

VERIFY_TOKEN = "my_verify_token"
PAGE_ACCESS_TOKEN = os.environ.get("PAGE_ACCESS_TOKEN", "")

# =============================
# BOT STATE (TẮT/BẬT BOT)
# =============================
BOT_ACTIVE = True
STATUS_FILE = "bot_status.json"

ADMIN_PSID = "516937221685203"
PAGE_ID = "516937221685203"

def save_status():
    with open(STATUS_FILE, "w") as f:
        json.dump({"BOT_ACTIVE": BOT_ACTIVE}, f)

def load_status():
    global BOT_ACTIVE
    if os.path.exists(STATUS_FILE):
        try:
            BOT_ACTIVE = json.load(open(STATUS_FILE))["BOT_ACTIVE"]
        except:
            BOT_ACTIVE = True

load_status()

# =============================
# LOAD DATA
# =============================
SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv"

def load_products():
    df = pd.read_csv(SHEET_CSV_URL, dtype=str).fillna("")
    return df

PRODUCTS = load_products()

# =============================
# HELPER: FILTER WATERMARK CN
# =============================
def has_chinese(text):
    return bool(re.search(r'[\u4e00-\u9fff]', text))

def clean_images(raw_images):
    imgs = []
    for block in raw_images.split("\n"):
        for url in block.split(","):
            u = url.strip()
            if u and not has_chinese(u):
                imgs.append(u)
    return list(dict.fromkeys(imgs))

# =============================
# FACEBOOK SEND API
# =============================
def send_text(user_id, text):
    url = f"https://graph.facebook.com/v19.0/me/messages?access_token={PAGE_ACCESS_TOKEN}"
    payload = {
        "recipient": {"id": user_id},
        "message": {"text": text}
    }
    requests.post(url, json=payload)

def send_image(user_id, image_url):
    url = f"https://graph.facebook.com/v19.0/me/messages?access_token={PAGE_ACCESS_TOKEN}"
    payload = {
        "recipient": {"id": user_id},
        "message": {"attachment": {
            "type": "image",
            "payload": {"url": image_url, "is_reusable": True}
        }}
    }
    requests.post(url, json=payload)

def send_video(user_id, video_url):
    url = f"https://graph.facebook.com/v19.0/me/messages?access_token={PAGE_ACCESS_TOKEN}"
    payload = {
        "recipient": {"id": user_id},
        "message": {"attachment": {
            "type": "video",
            "payload": {"url": video_url, "is_reusable": True}
        }}
    }
    requests.post(url, json=payload)

# =============================
# SEARCH PRODUCT
# =============================
def find_products_by_text(text):
    text = text.lower()
    results = PRODUCTS[
        PRODUCTS["Tên sản phẩm"].str.lower().str.contains(text) |
        PRODUCTS["Keyword sản phẩm"].str.lower().str.contains(text)
    ]
    return results

def get_product_group(ma_san_pham):
    return PRODUCTS[PRODUCTS["Mã sản phẩm"] == ma_san_pham]

# =============================
# WEBHOOK
# =============================
@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    global BOT_ACTIVE

    # VERIFY
    if request.method == "GET":
        if request.args.get("hub.verify_token") == VERIFY_TOKEN:
            return request.args.get("hub.challenge")
        return "Invalid verification token"

    # HANDLE MESSAGE
    data = request.json
    if data.get("object") != "page":
        return "OK", 200

    entry = data["entry"][0]
    messaging = entry.get("messaging", [])
    if not messaging:
        return "OK", 200

    message = messaging[0]

    sender_id = message["sender"]["id"]
    recipient_id = message["recipient"]["id"]

    # =============================
    # CHẶN echo (TIN CỦA PAGE TỰ GỬI)
    # =============================
    if "message" in message and message["message"].get("is_echo"):
        print("[ECHO] Bỏ qua tin của chính page.")
        return "OK", 200

    # =============================
    # EMERGENCY STOP – BOT TẮT NGAY
    # =============================
    msg = message.get("message", {})
    text = msg.get("text", "").lower().strip()

    # Lệnh tắt bot
    if text in ["tắt bot", "tat bot", "stop bot", "dừng bot"]:
        if sender_id == ADMIN_PSID:
            BOT_ACTIVE = False
            save_status()
            send_text(sender_id, "⛔ Bot đã được TẮT NGAY LẬP TỨC.")
        else:
            send_text(sender_id, "Bạn không có quyền tắt bot.")
        return "OK", 200

    # Lệnh bật bot
    if text in ["bật bot", "bat bot", "start bot"]:
        if sender_id == ADMIN_PSID:
            BOT_ACTIVE = True
            save_status()
            send_text(sender_id, "▶ Bot đã BẬT lại.")
        else:
            send_text(sender_id, "Bạn không có quyền bật bot.")
        return "OK", 200

    # Nếu bot đang tắt → không phản hồi
    if BOT_ACTIVE is False:
        print("[STOP] Bot đang tắt. Không phản hồi.")
        return "OK", 200

    # =============================
    # XỬ LÝ TIN NHẮN KHÁCH
    # =============================
    if text:
        results = find_products_by_text(text)
        if results.empty:
            send_text(sender_id, "Shop chưa có sản phẩm bạn tìm. Bạn mô tả rõ hơn giúp shop nhé ❤️")
            return "OK", 200

        # Lấy sản phẩm đầu tiên
        product = results.iloc[0]
        ma_sp = product["Mã sản phẩm"]

        group = get_product_group(ma_sp)

        ten_sp = product["Tên sản phẩm"]
        mo_ta = product["Mô tả"]
        gia_list = group["Giá bán"].unique()

        # Format giá
        gia_text = ", ".join([f"{g}đ" for g in gia_list])

        send_text(sender_id, f"✨ *{ten_sp}*\nGiá: {gia_text}\n\n{mo_ta}")

        # Gửi ảnh
        all_imgs = []
        for _, row in group.iterrows():
            imgs1 = clean_images(row["Images"])
            imgs2 = clean_images(row["Hình sản phẩm"])
            all_imgs.extend(imgs1 + imgs2)

        all_imgs = list(dict.fromkeys(all_imgs))  # unique

        # gửi 5 ảnh đầu
        for img in all_imgs[:5]:
            send_image(sender_id, img)

        send_text(sender_id, "Bạn muốn xem thêm ảnh hoặc xem video không ạ? ❤️")

    return "OK", 200

@app.route("/")
def home():
    return "Chatbot OK"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
