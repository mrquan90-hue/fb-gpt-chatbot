from flask import Flask, request
import requests
import os

app = Flask(__name__)

VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

OPENAI_URL = "https://api.openai.com/v1/chat/completions"

# ===== VERIFY WEBHOOK =====
@app.route("/webhook", methods=["GET"])
def verify_webhook():
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    if mode == "subscribe" and token == VERIFY_TOKEN:
        return challenge, 200
    return "Verification failed", 403

# ===== HANDLE INCOMING MESSAGE =====
@app.route("/webhook", methods=["POST"])
def handle_message():
    data = request.json
    print("Webhook received:", data)

    # Messenger mới → payload nằm trong "entry"
    for entry in data.get("entry", []):
        for event in entry.get("messaging", []):
            if "message" in event:
                sender_id = event["sender"]["id"]
                user_message = event["message"].get("text", "")

                bot_reply = generate_gpt_reply(user_message)
                send_message(sender_id, bot_reply)

    return "OK", 200


# ===== CALL OPENAI GPT =====
def generate_gpt_reply(user_message):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_API_KEY}"
    }

    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": "Bạn là chatbot bán hàng thân thiện, trả lời ngắn gọn, chốt đơn khéo léo."},
            {"role": "user", "content": user_message}
        ]
    }

    try:
        response = requests.post(OPENAI_URL, json=payload, headers=headers, timeout=12)
        result = response.json()
        return result["choices"][0]["message"]["content"]

    except Exception as e:
        print("OpenAI error:", e)
        return "Xin lỗi, hệ thống đang bận. Bạn thử lại giúp shop nhé!"


# ===== SEND MESSAGE TO CUSTOMER =====
def send_message(user_id, text):
    url = f"https://graph.facebook.com/v18.0/me/messages?access_token={PAGE_ACCESS_TOKEN}"
    payload = {
        "recipient": {"id": user_id},
        "message": {"text": text}
    }
    requests.post(url, json=payload)


# ===== RUN =====
@app.route("/")
def home():
    return "ChatGPT Messenger Bot is running!", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
