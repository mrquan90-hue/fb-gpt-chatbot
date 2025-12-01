import os
import requests
import csv
import io
from flask import Flask, request, jsonify
from openai import OpenAI

# KHỞI TẠO APP
app = Flask(__name__)

# ===== ENV =====
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "YOUR_VERIFY_TOKEN")
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN", "YOUR_PAGE_ACCESS_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "YOUR_OPENAI_KEY")
SHEET_CSV_URL = os.getenv("SHEET_CSV_URL", "")

client = OpenAI(api_key=OPENAI_API_KEY)

# ===== ANTI-LOOP STORAGE =====
RECENT_MIDS = set()   # lưu 100 MID gần nhất

# ===== LOAD DATA =====
PRODUCTS = {}

def load_sheet():
    global PRODUCTS
    PRODUCTS = {}

    if not SHEET_CSV_URL:
        print("[ERROR] SHEET_CSV_URL chưa được cài!")
        return

    print("[Sheet] Loading:", SHEET_CSV_URL)

    try:
        resp = requests.get(SHEET_CSV_URL)
        resp.encoding = "utf-8"

        f = io.StringIO(resp.text)
        reader = csv.DictReader(f)

        for row in reader:
            code = row.get("Mã sản phẩm", "").strip()
            if not code:
                continue

            if code not in PRODUCTS:
                PRODUCTS[code] = {
                    "rows": []
                }

            PRODUCTS[code]["rows"].append(row)

        print(f"[Sheet] Loaded {len(PRODUCTS)} products")

    except Exception as e:
        print("[Sheet ERROR]", e)


load_sheet()


# ====== SEND TEXT ======
def send_text(recipient_id, text):
    url = "https://graph.facebook.com/v19.0/me/messages"
    headers = {"Content-Type": "application/json"}

    payload = {
        "recipient": {"id": recipient_id},
        "message": {"text": text},
        "messaging_type": "RESPONSE"
    }

    params = {"access_token": PAGE_ACCESS_TOKEN}

    r = requests.post(url, json=payload, params=params)
    print("[send_text]", r.text)
    return r


# ====== SEND IMAGE ======
def send_image(recipient_id, img_url):
    url = "https://graph.facebook.com/v19.0/me/messages"
    headers = {"Content-Type": "application/json"}

    payload = {
        "recipient": {"id": recipient_id},
        "message": {
            "attachment": {
                "type": "image",
                "payload": {"url": img_url, "is_reusable": False}
            }
        },
        "messaging_type": "RESPONSE"
    }

    params = {"access_token": PAGE_ACCESS_TOKEN}

    r = requests.post(url, json=payload, params=params)
    print("[send_image]", r.text)
    return r


# ====== CHATGPT ======
def chatgpt_reply(prompt):
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message["content"]
    except Exception as e:
        print("[OpenAI ERROR]", e)
        return "Xin lỗi, hệ thống đang quá tải. Bạn thử lại giúp shop nhé."


# =========== WEBHOOK VERIFY ===========
@app.route("/webhook", methods=["GET"])
def verify_token():
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    if token == VERIFY_TOKEN:
        print("[Webhook] Verified")
        return challenge
    return "Invalid verification token", 403


# =========== WEBHOOK HANDLER ===========
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json()
    print("[Webhook] EVENT:", data)

    # Không phải page event → bỏ
    if data.get("object") != "page":
        return "OK", 200

    for entry in data.get("entry", []):
        for messaging_event in entry.get("messaging", []):

            # ===== 1. BỎ QUA DELIVERY / READ =====
            if "delivery" in messaging_event or "read" in messaging_event:
                print("[Skip] delivery/read")
                continue

            # ===== 2. CHECK MESSAGE =====
            message = messaging_event.get("message")
            if not message:
                continue

            # ===== 3. BỎ QUA ECHO =====
            if message.get("is_echo"):
                print("[Skip] echo")
                continue

            sender_id = messaging_event["sender"]["id"]
            text = message.get("text")
            mid = message.get("mid")

            # ===== 4. ANTI-LOOP MID =====
            if mid in RECENT_MIDS:
                print("[Anti-loop] MID duplicated → skip")
                continue

            RECENT_MIDS.add(mid)
            if len(RECENT_MIDS) > 100:
                RECENT_MIDS.clear()

            # ===== 5. ẢNH KHÁCH GỬI =====
            if "attachments" in message:
                send_text(sender_id, "Shop nhận được ảnh rồi nha! Bạn mô tả rõ hơn giúp shop để tôi tư vấn đúng mẫu nhé.")
                continue

            # ===== 6. XỬ LÝ NỘI DUNG KHÁCH =====
            if text:
                reply = chatgpt_reply(f"Khách: {text}\n---\nBạn là chatbot hỗ trợ bán hàng.")
                send_text(sender_id, reply)

    return "OK", 200


@app.route("/")
def home():
    return "FB Chatbot running OK"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
