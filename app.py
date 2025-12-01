# ==============================================
#    FB GPT CHATBOT – BẢN CẤM BỊA SẢN PHẨM
#    • Hỗ trợ ảnh + video
#    • Đọc ảnh khách gửi (GPT Vision)
# ==============================================

import os
import re
import csv
import io
import requests
from flask import Flask, request

app = Flask(__name__)

VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SHEET_CSV_URL = os.getenv("SHEET_CSV_URL")

OPENAI_URL = "https://api.openai.com/v1/chat/completions"

# ==============================================
# 1. LOAD GOOGLE SHEET
# ==============================================
def load_products():
    try:
        resp = requests.get(SHEET_CSV_URL, timeout=20)
        resp.encoding = "utf-8"
        csv_file = io.StringIO(resp.text)
        reader = csv.DictReader(csv_file)
        products = list(reader)
        print(f"[Sheet] Loaded {len(products)} products")
        return products
    except Exception as e:
        print("CSV ERROR:", e)
        return []

# ==============================================
# 2. TÁCH ẢNH + VIDEO
# ==============================================
def extract_urls(cell):
    if not cell:
        return []
    text = cell.replace("\n", ",")
    parts = [p.strip() for p in text.split(",") if p.strip()]
    urls = []
    seen = set()
    for u in parts:
        if not u.startswith("http"):
            continue
        if not (".jpg" in u or ".png" in u or ".webp" in u or ".mp4" in u):
            continue
        if re.search(r"[\u4e00-\u9fff]", u):
            continue
        if u not in seen:
            seen.add(u)
            urls.append(u)
    return urls

def get_images(row, limit=5):
    img1 = extract_urls(row.get("Images"))
    img2 = extract_urls(row.get("Hình sản phẩm"))
    all_imgs = img1 + img2
    # loại trùng
    uniq = []
    seen = set()
    for x in all_imgs:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq[:limit]

def get_videos(row, limit=1):
    vids = extract_urls(row.get("Videos"))
    return vids[:limit]

# ==============================================
# 3. TÌM SẢN PHẨM – KHÔNG BAO GIỜ BỊA!
# ==============================================
FIELDS = [
    "Mã sản phẩm",
    "Mã mẫu mã",
    "Mã mẫu mã mới",
    "Tên sản phẩm",
    "Keyword sản phẩm",
    "Keyword mẫu mã",
    "Thuộc tính",
    "Thuộc tính sản phẩm",
]

def search_products(query, products):
    q = query.lower()
    matches = []

    # Match trực tiếp
    for r in products:
        combined = " ".join(r.get(f, "") for f in FIELDS).lower()
        if q in combined:
            matches.append(r)

    # nếu không có → phân tách từ khóa
    if not matches:
        tokens = q.split()
        for r in products:
            combined = " ".join(r.get(f, "") for f in FIELDS).lower()
            if all(t in combined for t in tokens[:2]):
                matches.append(r)

    return matches
# ==============================================
# 4. FORMAT BẢNG GIÁ – KHÔNG SỬ DỤNG DẤU |
# ==============================================
def format_price_list(rows):
    lines = []
    for r in rows:
        code = r.get("Mã mẫu mã mới") or r.get("Mã mẫu mã")
        price = r.get("Giá bán", "")
        attr = r.get("Thuộc tính sản phẩm") or r.get("Thuộc tính")
        lines.append(f"- Mã mẫu: {code}, Giá: {price}đ, Thuộc tính: {attr}")
    return "\n".join(lines)


# ==============================================
# 5. GPT – CẤM BỊA SẢN PHẨM
# ==============================================
def call_gpt_text(user_query, matched_rows, all_products):
    """
    matched_rows: danh sách biến thể sản phẩm
    """

    # Nếu tìm được sản phẩm
    if matched_rows:
        product_name = matched_rows[0].get("Tên sản phẩm", "Sản phẩm")
        price_table = format_price_list(matched_rows)

        system = (
            "Bạn là chatbot bán hàng thời trang của shop. "
            "TUYỆT ĐỐI KHÔNG ĐƯỢC BỊA SẢN PHẨM. "
            "Chỉ tư vấn dựa trên dữ liệu shop cung cấp. "
            "Nếu sản phẩm khách hỏi không có → phải nói không có sản phẩm đó. "
            "Nếu nhiều biến thể → phải gửi bảng giá gọn đẹp (dùng dấu phẩy và xuống dòng). "
            "Khi khách muốn xem ảnh → shop sẽ gửi ảnh ngay sau text.\n"
        )

        user_msg = (
            f"KHÁCH HỎI: {user_query}\n"
            f"SẢN PHẨM ĐÚNG TRONG SHOP: {product_name}\n"
            f"CÁC BIẾN THỂ:\n{price_table}"
        )

    else:
        # Không tìm thấy sản phẩm → gợi ý sản phẩm liên quan SHOP CÓ
        names = list({p.get('Tên sản phẩm', '') for p in all_products})
        suggestion = "\n".join(f"- {name}" for name in names[:10])

        system = (
            "Bạn là chatbot bán hàng của shop. Không tìm thấy sản phẩm khách hỏi. "
            "KHÔNG BAO GIỜ ĐƯỢC BỊA SẢN PHẨM. "
            "Chỉ được phép gợi ý những sản phẩm shop thực sự có."
        )

        user_msg = (
            f"KHÁCH HỎI: {user_query}\n\n"
            f"Shop KHÔNG CÓ sản phẩm này.\n"
            f"Các sản phẩm khác shop có:\n{suggestion}"
        )

    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user_msg}
        ],
        "temperature": 0.4
    }

    try:
        r = requests.post(OPENAI_URL, json=payload, headers=headers, timeout=30).json()
        return r["choices"][0]["message"]["content"]
    except Exception as e:
        print("GPT ERROR:", e)
        return "Shop đang xử lý yêu cầu, bạn chờ 1 chút nhé ❤️"


# ==============================================
# 6. GPT VISION – ĐỌC ẢNH KHÁCH GỬI
# ==============================================
def call_gpt_vision(image_url, all_products):
    """
    GPT phân tích hình → gợi ý sản phẩm shop có
    """
    product_list = "\n".join(
        [f"- {p.get('Tên sản phẩm')} ({p.get('Mã sản phẩm')})" for p in all_products[:50]]
    )

    system = (
        "Bạn là chatbot bán hàng thời trang. "
        "Bạn sẽ xem ảnh khách gửi, mô tả sản phẩm trong ảnh, "
        "và tìm sản phẩm tương tự trong danh sách shop có. "
        "TUYỆT ĐỐI KHÔNG BỊA SẢN PHẨM."
    )

    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system},
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": f"Đây là danh sách sản phẩm shop có:\n{product_list}"},
                    {
                        "type": "input_image",
                        "image_url": image_url
                    }
                ]
            }
        ],
    }

    try:
        r = requests.post(OPENAI_URL, json=payload, headers=headers).json()
        return r["choices"][0]["message"]["content"]
    except:
        return "Shop nhận được ảnh rồi, nhưng chưa xem rõ. Bạn gửi lại giúp shop ảnh rõ nét hơn nhé."


# ==============================================
# 7. FACEBOOK SEND API
# ==============================================
def send_text(psid, text):
    url = "https://graph.facebook.com/v18.0/me/messages"
    body = {
        "recipient": {"id": psid},
        "message": {"text": text}
    }
    requests.post(url, params={"access_token": PAGE_ACCESS_TOKEN}, json=body)


def send_image(psid, image_url):
    url = "https://graph.facebook.com/v18.0/me/messages"
    body = {
        "recipient": {"id": psid},
        "message": {
            "attachment": {
                "type": "image",
                "payload": {
                    "url": image_url,
                    "is_reusable": True
                }
            }
        }
    }
    requests.post(url, params={"access_token": PAGE_ACCESS_TOKEN}, json=body)


def send_video(psid, video_url):
    url = "https://graph.facebook.com/v18.0/me/messages"
    body = {
        "recipient": {"id": psid},
        "message": {
            "attachment": {
                "type": "video",
                "payload": {
                    "url": video_url,
                    "is_reusable": True
                }
            }
        }
    }
    requests.post(url, params={"access_token": PAGE_ACCESS_TOKEN}, json=body)


# ==============================================
# 8. XỬ LÝ WEBHOOK FACEBOOK
# ==============================================
@app.route("/webhook", methods=["GET"])
def verify():
    if request.args.get("hub.verify_token") == VERIFY_TOKEN:
        return request.args["hub.challenge"]
    return "Verification failed", 403


@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    print("[Webhook]", data)

    products = load_products()

    for entry in data.get("entry", []):
        for msg in entry.get("messaging", []):

            psid = msg["sender"]["id"]

            # ========== TRƯỜNG HỢP ẢNH KHÁCH GỬI ==========
            if "attachments" in msg.get("message", {}):
                att = msg["message"]["attachments"][0]
                if att["type"] == "image":
                    img_url = att["payload"]["url"]
                    reply = call_gpt_vision(img_url, products)
                    send_text(psid, reply)
                    return "OK", 200

            # ========== TRƯỜNG HỢP VĂN BẢN ==========
            if "text" in msg.get("message", {}):
                user_text = msg["message"]["text"]

                # Tìm sản phẩm
                matched = search_products(user_text, products)

                # Nếu có 1 sản phẩm → gửi ảnh trước
                if len(matched) == 1:
                    imgs = get_images(matched[0])
                    vids = get_videos(matched[0])

                    for i in imgs:
                        send_image(psid, i)

                    for v in vids:
                        send_video(psid, v)

                # Gọi GPT
                reply = call_gpt_text(user_text, matched, products)
                send_text(psid, reply)

    return "OK", 200


# ==============================================
# 9. HEALTHCHECK
# ==============================================
@app.route("/")
def home():
    return "Messenger GPT Bot is running OK", 200


# ==============================================
# 10. RUN
# ==============================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
