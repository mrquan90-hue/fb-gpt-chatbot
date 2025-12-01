# ======= FB GPT CHATBOT - BẢN NÂNG CẤP ĐẶC BIỆT =======
# Hỗ trợ đa biến thể – bảng giá chi tiết – gửi ảnh tư vấn
# Tối ưu cho shop thời trang Việt Nam
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
    """
    Tải dữ liệu sản phẩm từ Google Sheet CSV.
    Trả về: list[dict]
    """
    try:
        if not SHEET_CSV_URL:
            print("❌ SHEET_CSV_URL chưa được cấu hình")
            return []

        print(f"[Sheet] Fetching CSV: {SHEET_CSV_URL}")

        resp = requests.get(SHEET_CSV_URL, timeout=20)
        resp.encoding = "utf-8"

        csv_file = io.StringIO(resp.text)
        reader = csv.DictReader(csv_file)

        products = list(reader)
        print(f"[Sheet] OK — tải {len(products)} dòng sản phẩm")
        return products

    except Exception as e:
        print("[Sheet] ERROR:", e)
        return []


# ======================================================
# 2. XỬ LÝ ẢNH
# ======================================================
def parse_image_urls(cell_value: str):
    """
    - Nhiều URL, ngăn bởi xuống dòng và dấu phẩy.
    - Loại bỏ URL rỗng, trùng, chứa ký tự TQ.
    """
    if not cell_value:
        return []

    text = cell_value.replace("\n", ",")
    parts = [p.strip() for p in text.split(",") if p.strip()]

    urls = []
    seen = set()

    for url in parts:
        # Bỏ các link chứa chữ Trung Quốc
        if re.search(r"[\u4e00-\u9fff]", url):
            continue
        if url not in seen:
            seen.add(url)
            urls.append(url)

    return urls


def get_product_images(row: dict, max_images: int = 5):
    images_raw = row.get("Images") or row.get("Hình sản phẩm") or ""
    urls = parse_image_urls(images_raw)
    return urls[:max_images]


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

    # fallback: tách từ khoá
    if not matches:
        tokens = [t for t in q.split() if t]
        for row in products:
            combined = " ".join((row.get(f, "") for f in SEARCH_FIELDS)).lower()
            if all(t in combined for t in tokens[:2]):
                matches.append(row)
                if len(matches) >= limit:
                    break

    return matches


# ======================================================
# 4. CHUẨN HOÁ BẢNG GIÁ (CHO NHIỀU BIẾN THỂ)
# ======================================================
def build_price_list(rows):
    """
    Gom các biến thể thành bảng giá đẹp để gửi GPT
    """
    lines = []
    for r in rows:
        line = (
            f"- Mã mẫu: {r.get('Mã mẫu mã mới') or r.get('Mã mẫu mã')}"
            f" | Giá: {r.get('Giá bán','')}đ"
            f" | Thuộc tính: {r.get('Thuộc tính sản phẩm') or r.get('Thuộc tính')}"
        )
        lines.append(line)
    return "\n".join(lines)


# ======================================================
# 5. GPT TƯ VẤN
# ======================================================
def call_gpt(user_message: str, matched_products: list):
    price_table = build_price_list(matched_products)

    product_summary = (
        f"Có {len(matched_products)} biến thể phù hợp. Bảng giá chi tiết:\n{price_table}\n"
        if matched_products else "Không tìm thấy sản phẩm phù hợp."
    )

    system_prompt = (
        "Bạn là chatbot bán hàng thời trang. Nói chuyện dễ hiểu, thân thiện. "
        "Phải tư vấn đúng giá và mã mẫu mã.\n\n"
        "Luôn xuất 1 bảng giá rõ ràng nếu sản phẩm có nhiều biến thể.\n"
        "Nếu khách đã chốt đơn đầy đủ thông tin → xuất dòng ##ORDER## theo format:\n"
        "##ORDER##|<mã sản phẩm>|<mã mẫu>|<số lượng>|<tên>|<sđt>|<địa chỉ>|<ghi chú>\n"
    )

    user_msg = (
        f"SẢN PHẨM PHÙ HỢP:\n{product_summary}\n\n"
        f"KHÁCH HỎI:\n{user_message}"
    )

    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_msg},
        ],
        "temperature": 0.6,
    }

    try:
        r = requests.post(OPENAI_URL, json=payload, headers=headers, timeout=20).json()
        return r["choices"][0]["message"]["content"]
    except Exception as e:
        print("[GPT ERROR]:", e)
        return "Shop đang xử lý, bạn chờ 1 chút giúp shop nhé ❤️"


def split_answer_and_order(answer: str):
    order_line = None
    lines = answer.splitlines()

    visible = []
    for line in lines:
        if line.startswith("##ORDER##"):
            order_line = line.strip()
        else:
            visible.append(line)

    return "\n".join(visible), order_line


# ======================================================
# 6. GỬI TIN NHẮN MESSENGER
# ======================================================
def send_text(recipient, text):
    url = "https://graph.facebook.com/v18.0/me/messages"
    payload = {
        "recipient": {"id": recipient},
        "message": {"text": text},
    }
    requests.post(url, params={"access_token": PAGE_ACCESS_TOKEN}, json=payload)


def send_image(recipient, image_url):
    url = "https://graph.facebook.com/v18.0/me/messages"
    payload = {
        "recipient": {"id": recipient},
        "message": {
            "attachment": {
                "type": "image",
                "payload": {"url": image_url, "is_reusable": True},
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

                matched = search_products(user_message, products)

                # Gửi ảnh trước nếu chỉ có 1 sản phẩm
                if len(matched) == 1:
                    for img in get_product_images(matched[0]):
                        send_image(psid, img)

                gpt_answer = call_gpt(user_message, matched)
                visible, order_line = split_answer_and_order(gpt_answer)

                send_text(psid, visible)

                if order_line:
                    print("[ORDER DETECTED]", order_line)

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
