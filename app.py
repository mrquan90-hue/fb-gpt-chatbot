from flask import Flask, request
import requests
import os
import csv
import io

app = Flask(__name__)

PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SHEET_CSV_URL = os.getenv("SHEET_CSV_URL")


# ==========================
# 1. Load sản phẩm từ Google Sheet
# ==========================
def load_products():
    try:
        response = requests.get(SHEET_CSV_URL)
        response.encoding = "utf-8"
        csv_file = io.StringIO(response.text)
        reader = csv.DictReader(csv_file)
        return list(reader)
    except Exception as e:
        print("Lỗi load sheet:", e)
        return []


# ==========================
# 2. Gửi tin nhắn trở lại Messenger
# ==========================
def send_message(recipient_id, text):
    url = f"https://graph.facebook.com/v17.0/me/messages?access_token={PAGE_ACCESS_TOKEN}"
    payload = {
        "recipient": {"id": recipient_id},
        "message": {"text": text}
    }
    headers = {"Content-Type": "application/json"}
    requests.post(url, json=payload, headers=headers)


# ==========================
# 3. Gọi GPT
# ==========================
def ask_gpt(question, product_list):
    content = "Dưới đây là danh sách sản phẩm:\n"

    for p in product_list[:20]:
        content += f"- {p.get('Tên sản phẩm', '')}: {p.get('Giá bán', '')} | {p.get('Mã sản phẩm', '')}\n"

    system_prompt = """
    Bạn là chatbot bán hàng. Nhiệm vụ:
    - Hiểu câu hỏi của khách
    - Gợi ý sản phẩm phù hợp
    - Trích xuất tên sản phẩm, giá, mã SP từ dữ liệu
    - Tư vấn thân thiện, dễ chốt đơn
    """

    full_prompt = f"{system_prompt}\n\nDanh sách sản phẩm:\n{content}\n\nKhách hỏi: {question}"

    response = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {OPENAI_API_KEY}"
        },
        json={
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": full_prompt}
            ],
            "temperature": 0.6
        }
    )

    data = response.json()
    return data["choices"][0]["message"]["content"]


# ==========================
# 4. Webhook verify (Facebook kiểm tra)
# ==========================
@app.route("/webhook", methods=["GET"])
def verify_webhook():
    if request.args.get("hub.verify_token") == VERIFY_TOKEN:
        return request.args.get("hub.challenge")
    return "Sai verify token"


# ==========================
# 5. Nhận tin nhắn từ khách
# ==========================
@app.route("/webhook", methods=["POST"])
def handle_message():
    data = request.get_json()

    # Kiểm tra Messenger event
    if data.get("object") == "page":
        for entry in data.get("entry", []):
            for messaging in entry.get("messaging", []):
                sender_id = messaging["sender"]["id"]

                # Kiểm tra nếu là message text
                if "message" in messaging and "text" in messaging["message"]:
                    user_text = messaging["message"]["text"]

                    # Load sản phẩm
                    products = load_products()

                    # Gọi GPT trả lời
                    reply = ask_gpt(user_text, products)

                    # Gửi trả Messenger
                    send_message(sender_id, reply)

        return "ok", 200

    return "not messenger event", 200


# ==========================
# 6. Health check
# ==========================
@app.route("/healthz")
def health_check():
    return "OK", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
