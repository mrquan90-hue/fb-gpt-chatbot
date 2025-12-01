# ======= FB GPT CHATBOT - BẢN TỐI ƯU CHO SHOP THỜI TRANG =======
# Hỗ trợ đa biến thể – bảng giá đẹp – gửi ảnh hoàn chỉnh
# ======================================================

import os
import re
import csv
import io
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SHEET_CSV_URL = os.getenv("SHEET_CSV_URL")

OPENAI_URL = "https://api.openai.com/v1/chat/completions"


# ======================================================
# 1. ĐỌC GOOGLE SHEET (CSV)
# ======================================================
def load_products():
    try:
        print(f"[Sheet] Fetching CSV: {SHEET_CSV_URL}")
        resp = requests.get(SHEET_CSV_URL, timeout=20)
        resp.encoding = "utf-8"

        csv_file = io.StringIO(resp.text)
        reader = csv.DictReader(csv_file)
        products = list(reader)

        print(f"[Sheet] OK — Tải {len(products)} dòng")
        return products

    except Exception as e:
        print("[Sheet] ERROR:", e)
        return []


# ======================================================
# 2. XỬ LÝ ẢNH — bản nâng cấp mạnh
# ======================================================
def extract_images(cell_value: str):
    """
    Chuẩn hóa ảnh:
    - Tách bởi dấu phẩy và xuống dòng
    - Loại bỏ URL trùng
    - Loại bỏ ảnh chứa chữ Trung Quốc
    - Chỉ giữ .jpg/.png
    """
    if not cell_value:
        return []

    text = cell_value.replace("\n", ",")
    parts = [p.strip() for p in text.split(",") if p.strip()]

    urls = []
    seen = set()

    for url in parts:
        if not url.startswith("http"):
            continue
        if not (".jpg" in url or ".png" in url or ".webp" in url):
            continue
        if re.search(r"[\u4e00-\u9fff]", url):
            continue
        if url not in seen:
            seen.add(url)
            urls.append(url)

    return urls


def get_product_images(row: dict, max_images: int = 5):
    """
    Ưu tiên ảnh biến thể → ảnh sản phẩm
    """
    img1 = extract_images(row.get("Images", ""))
    img2 = extract_images(row.get("Hình sản phẩm", ""))

    urls = img1 + img2

    # loại trùng lần cuối
    unique = []
    seen = set()
    for u in urls:
        if u not in seen:
            seen.add(u)
            unique.append(u)

    return unique[:max_images]


# ======================================================
# 3. TÌM SẢN PHẨM
# ======================================================
SEARCH_FIELDS = [
    "Mã sản phẩm",
    "Mã mẫu mã",
    "Mã mẫu mã mới",
    "Tên sản phẩm",
    "Keyword sản phẩm",
    "Keyword mẫu mã",
    "Thuộc tính",
    "Thuộc tính sản phẩm",
]


def search_products(query: str, products: list, limit: int = 10):
    q = query.lower()
    matches = []

    for row in products:
        combined = " ".join((row.get(f, "") for f in SEARCH_FIELDS))
        if q in combined.lower():
            matches.append(row)
            if len(matches) >= limit:
                break

    if not matches:
        tokens = q.split()
        for row in products:
            combined = " ".join((row.get(f, "") for f in SEARCH_FIELDS)).lower()
            if all(t in combined for t in tokens[:2]):
                matches.append(row)
                if len(matches) >= limit:
                    break

    return matches


# ======================================================
# 4. TẠO BẢNG GIÁ ĐẸP — không dùng dấu |
# ======================================================
def build_price_list(rows):
    """
    Xuất bảng giá gọn gàng:
    - Mã mẫu
    - Giá
    - Thuộc tính
    """
    lines = []
    for r in rows:
        code = r.get("Mã mẫu mã mới") or r.get("Mã mẫu mã")
        price = r.get("Giá bán", "")
        attr = r.get("Thuộc tính sản phẩm") or r.get("Thuộc tính")

        line = f"- Mã mẫu: {code}, Giá: {price}đ, Thuộc tính: {attr}"
        lines.append(line)

    return "\n".join(lines)


# ======================================================
# 5. GPT TƯ VẤN
# ======================================================
def call_gpt(user_message: str, matched_products: list):
    price_table = build_price_list(matched_products)

    product_summary = (
        f"Có {len(matched_products)} biến thể phù hợp:\n{price_table}\n"
        if matched_products else "Không tìm thấy sản phẩm phù hợp."
    )

    system_prompt = (
        "Bạn là chatbot bán hàng thời trang. Nói chuyện thân thiện, rõ ràng. "
        "Luôn đưa bảng giá ngắn gọn, dễ hiểu. "
        "Không dùng dấu |. "
        "Chỉ dùng dấu phẩy và xuống dòng.\n\n"
        "Nếu khách cung cấp đầy đủ thông tin → xuất dòng ##ORDER## theo format:\n"
        "##ORDER##|<mã sản phẩm>|<mã mẫu>|<số lượng>|<tên>|<sđt>|<địa chỉ>|<ghi chú>\n"
    )

    user_msg = f"SẢN PHẨM TÌM THẤY:\n{product_summary}\n\nKHÁCH HỎI:\n{user_message}"

    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_msg},
        ],
        "temperature": 0.5,
    }

    try:
        r = requests.post(OPENAI_URL, json=payload, headers=headers, timeout=20).json()
        return r["choices"][0]["message"]["content"]
    except Exception as e:
        print("[GPT ERROR]:", e)
        return "Shop đang xử lý, bạn chờ 1 chút giúp shop nhé ❤️"


def split_answer_and_order(answer: str):
    order_line = None
    visible = []

    for line in answer.splitlines():
        if line.startswith("##ORDER##"):
            order_line = line.strip()
        else:
            visible.append(line)

    return "\n".join(visible), order_line


# ======================================================
# 6. GỬI TEXT + ẢNH FACEBOOK
# ======================================================
def send_text(recipient, text):
    url = "https://graph.facebook.com/v18.0/me/messages"
    payload = {
        "recipient": {"id": recipient},
        "message": {"text": text},
    }
    requests.post(url, params={"access_token": PAGE_ACCESS_TOKEN}, json=payload)


def send_image(recipient, url_image):
    url = "https://graph.facebook.com/v18.0/me/messages"
    payload = {
        "recipient": {"id": recipient},
        "message": {
            "attachment": {
                "type": "image",
                "payload": {"url": url_image, "is_reusable": True},
            }
        },
    }
    requests.post(url, params={"access_token": PAGE_ACCESS_TOKEN}, json=payload)


# ======================================================
# 7. WEBHOOK FACEBOOK
# ======================================================
@app.route("/webhook", methods=["GET"])
def webhook_verify():
    if request.args.get("hub.verify_token") == VERIFY_TOKEN:
        return request.args["hub.challenge"]
    return "Verification failed", 403


@app.route("/webhook", methods=["POST"])
def webhook_handle():
    data = request.json
    print("[Webhook]", data)

    products = load_products()

    for entry in data.get("entry", []):
        for event in entry.get("messaging", []):
            if "message" in event and "text" in event["message"]:

                user_message = event["message"]["text"]
                psid = event["sender"]["id"]

                # Tìm sản phẩm
                matched = search_products(user_message, products)

                # Gửi ảnh nếu có đúng 1 sản phẩm
                if len(matched) == 1:
                    imgs = get_product_images(matched[0])
                    for img in imgs:
                        send_image(psid, img)

                # Gửi tư vấn AI
                gpt_answer = call_gpt(user_message, matched)
                visible, order_line = split_answer_and_order(gpt_answer)
                send_text(psid, visible)

                # Nếu phát hiện đơn hàng
                if order_line:
                    print("[ORDER]", order_line)

    return "OK", 200


# ======================================================
# 8. HEALTHCHECK
# ======================================================
@app.route("/")
def home():
    return "GPT Messenger bot is running", 200


# ======================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
