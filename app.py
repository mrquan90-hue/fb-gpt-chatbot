import os
import io
import csv
import requests
from flask import Flask, request
from openai import OpenAI

# ENV variables
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
PAGE_ACCESS_TOKEN = os.environ.get("PAGE_ACCESS_TOKEN")
VERIFY_TOKEN = os.environ.get("VERIFY_TOKEN", "my_verify_token")
SHEET_CSV_URL = os.environ.get("SHEET_CSV_URL")

client = OpenAI(api_key=OPENAI_API_KEY)
app = Flask(__name__)

# Load products
def load_products():
    if not SHEET_CSV_URL:
        return []
    try:
        r = requests.get(SHEET_CSV_URL)
        f = io.StringIO(r.text)
        return list(csv.DictReader(f))
    except:
        return []

# Find matching products
def search_products(query, products):
    query = query.lower()
    results = []
    for p in products:
        name = (p.get("OUT_TITLE") or p.get("Tên sản phẩm") or "").lower()
        desc = (p.get("OUT_DESC") or p.get("Mô tả ngắn") or "").lower()
        if query in name or query in desc:
            results.append(p)
        if len(results) >= 5:
            break
    return results

def format_products(products):
    if not products:
        return "Không tìm thấy sản phẩm phù hợp."
    txt = ""
    for p in products:
        name = p.get("OUT_TITLE") or p.get("Tên sản phẩm") or ""
        price = p.get("Giá khuyến mãi") or p.get("Giá cả") or ""
        sku = p.get("SKU") or ""
        txt += f"- {name} | Giá: {price} | Mã: {sku}\n"
    return txt

# GPT response
def ask_gpt(message, products):
    product_text = format_products(products)

    system = """
Bạn là nhân viên tư vấn bán hàng thân thiện.
Luôn chào hỏi, hỏi nhu cầu khách, đề xuất sản phẩm phù hợp, và dẫn dắt chốt đơn.
Trả lời bằng tiếng Việt, giọng lịch sự, dễ hiểu.
"""

    user = f"""
Tin nhắn khách: {message}

Sản phẩm gợi ý:
{product_text}
"""

    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        max_tokens=500,
        temperature=0.7
    )
    return resp.choices[0].message.content.strip()

# Send message to user
def send_message(psid, text):
    url = "https://graph.facebook.com/v18.0/me/messages"
    params = {"access_token": PAGE_ACCESS_TOKEN}
    data = {
        "recipient": {"id": psid},
        "message": {"text": text}
    }
    requests.post(url, params=params, json=data)

# Home
@app.route("/", methods=["GET"])
def home():
    return "Bot is running!", 200

# Verify webhook
@app.route("/webhook", methods=["GET"])
def verify():
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")
    if mode == "subscribe" and token == VERIFY_TOKEN:
        return challenge, 200
    return "Verification failed", 403

# Receive messages
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json()
    if data.get("object") == "page":
        for entry in data.get("entry", []):
            for e in entry.get("messaging", []):
                if "message" in e and "text" in e["message"]:
                    psid = e["sender"]["id"]
                    text = e["message"]["text"]

                    products = load_products()
                    matched = search_products(text, products)
                    reply = ask_gpt(text, matched)
                    send_message(psid, reply)

        return "EVENT_RECEIVED", 200

    return "Not a page event", 404

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
