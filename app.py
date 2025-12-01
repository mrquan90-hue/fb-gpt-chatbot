# ==============================================
# FB GPT CHATBOT — BẢN ỔN ĐỊNH
# - 1 sản phẩm = Mã sản phẩm
# - 1 biến thể = Mã mẫu mã
# - Không bịa sản phẩm
# - Gửi 5 ảnh đầu tiên, khách xin thì gửi thêm
# - Gửi ảnh + video
# - Đọc ảnh khách gửi (GPT Vision)
# - Chống loop (echo + delivery/read + chống trùng mid)
# - Công tắc bật / tắt bot điều khiển bởi ADMIN_PSID
# ==============================================

import os
import re
import csv
import io
import time
import requests
from flask import Flask, request

app = Flask(__name__)

VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN")
SHEET_CSV_URL = os.getenv("SHEET_CSV_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Page ID để tham khảo, không dùng để so sánh sender
PAGE_ID = "516937221685203"

# Admin PSID – bạn nên set trong Environment của Render:
# ADMIN_PSID=26225402767048945 (ví dụ)
ADMIN_PSID = os.getenv("ADMIN_PSID")

OPENAI_URL = "https://api.openai.com/v1/chat/completions"

# Trạng thái bot
BOT_ENABLED = True  # mặc định bật

# Lưu trạng thái ảnh đã gửi
LAST_IMAGES = {}      # { psid: {"images": [...], "sent": 5} }
LAST_MESSAGE_IDS = {} # { psid: last_mid } để chống xử lý trùng


# ==============================================
# 1. LOAD GOOGLE SHEET (CSV)
# ==============================================
def load_products():
    try:
        print(f"[Sheet] Fetching CSV from: {SHEET_CSV_URL}")
        resp = requests.get(SHEET_CSV_URL, timeout=20)
        resp.encoding = "utf-8"

        csv_file = io.StringIO(resp.text)
        reader = csv.DictReader(csv_file)
        products = list(reader)
        print(f"[Sheet] Loaded {len(products)} products")
        return products
    except Exception as e:
        print("[Sheet] ERROR:", e)
        return []


# ==============================================
# 2. XỬ LÝ ẢNH + VIDEO
# ==============================================
def extract_urls(cell):
    """
    - Tách URL bởi dấu phẩy và xuống dòng
    - Bỏ URL rỗng
    - Bỏ URL không phải hình/video
    - Bỏ URL có ký tự tiếng Trung trong đường link (watermark chữ TQ)
      (Không loại domain alicdn.com, chỉ loại nếu trong URL có ký tự 汉字)
    """
    if not cell:
        return []

    text = str(cell).replace("\n", ",")
    parts = [p.strip() for p in text.split(",") if p.strip()]

    urls = []
    seen = set()

    for u in parts:
        if not u.startswith("http"):
            continue

        # chỉ nhận hình/video
        if not (".jpg" in u or ".jpeg" in u or ".png" in u or ".webp" in u or ".mp4" in u):
            continue

        # loại URL có ký tự tiếng Trung trong path
        if re.search(r"[\u4e00-\u9fff]", u):
            continue

        if u not in seen:
            seen.add(u)
            urls.append(u)

    return urls


def get_images_for_rows(rows, max_images=None):
    """
    Gom ảnh từ nhiều dòng (cùng Mã sản phẩm).
    Trả về list URL ảnh (đã loại trùng).
    """
    all_imgs = []
    seen = set()

    for row in rows:
        imgs1 = extract_urls(row.get("Images", ""))
        imgs2 = extract_urls(row.get("Hình sản phẩm", ""))

        for u in imgs1 + imgs2:
            if u not in seen:
                seen.add(u)
                all_imgs.append(u)

    if max_images is not None:
        return all_imgs[:max_images]
    return all_imgs


def get_videos_for_rows(rows, max_videos=1):
    all_videos = []
    seen = set()
    for row in rows:
        vids = extract_urls(row.get("Videos", ""))
        for v in vids:
            if v not in seen:
                seen.add(v)
                all_videos.append(v)
    return all_videos[:max_videos]


# ==============================================
# 3. TÌM & NHÓM SẢN PHẨM
# ==============================================
SEARCH_FIELDS = [
    "Mã sản phẩm",
    "Mã sản phẩm mới",
    "Mã mẫu mã",
    "Tên sản phẩm",
    "Keyword sản phẩm",
    "Keyword mẫu mã",
    "Thuộc tính",
    "Thuộc tính sản phẩm",
    "Mô tả",
]


def search_products(query, products, limit=50):
    q = query.lower().strip()
    if not q:
        return []

    matches = []

    # Match nguyên câu
    for row in products:
        combined = " ".join(row.get(f, "") for f in SEARCH_FIELDS).lower()
        if q in combined:
            matches.append(row)
            if len(matches) >= limit:
                break

    # Nếu chưa có → match theo từ khóa
    if not matches:
        tokens = [t for t in re.split(r"\s+", q) if t]
        for row in products:
            combined = " ".join(row.get(f, "") for f in SEARCH_FIELDS).lower()
            if tokens and all(t in combined for t in tokens[:2]):
                matches.append(row)
                if len(matches) >= limit:
                    break

    return matches


def group_variants_by_product(matched_rows, all_products):
    """
    Quy ước:
    - 1 SẢN PHẨM = cột "Mã sản phẩm"
    - 1 BIẾN THỂ = cột "Mã mẫu mã"
    main_rows: tất cả dòng có cùng Mã sản phẩm
    """
    if not matched_rows:
        return []

    first = matched_rows[0]
    main_product_code = (first.get("Mã sản phẩm") or "").strip()

    if not main_product_code:
        return matched_rows

    main_rows = [
        r for r in all_products
        if (r.get("Mã sản phẩm") or "").strip() == main_product_code
    ]

    return main_rows or matched_rows


# ==============================================
# 4. BẢNG GIÁ GỌN, DỄ ĐỌC
# ==============================================
def build_price_list(rows):
    """
    Mỗi dòng là 1 biến thể (Mã mẫu mã).
    """
    lines = []
    for r in rows:
        product_code = (r.get("Mã sản phẩm") or "").strip()
        variant_code = (r.get("Mã mẫu mã") or "").strip()
        price = (r.get("Giá bán") or "").strip()
        attr = (r.get("Thuộc tính sản phẩm") or r.get("Thuộc tính") or "").strip()

        if price and not price.endswith("đ"):
            price = f"{price}đ"

        line = f"- Mã SP: {product_code}, Mã mẫu: {variant_code}, Giá: {price}, Thuộc tính: {attr}"
        lines.append(line)

    return "\n".join(lines)


# ==============================================
# 5. GPT TƯ VẤN (TEXT) — CẤM BỊA SẢN PHẨM
# ==============================================
def call_gpt_text(user_text, main_rows, all_products):
    # Chuẩn bị danh sách sản phẩm để gợi ý khi không có kết quả
    other_names = []
    for p in all_products:
        name = (p.get("Tên sản phẩm") or "").strip()
        code = (p.get("Mã sản phẩm") or "").strip()
        if name and code:
            other_names.append(f"- {name} (Mã: {code})")
        if len(other_names) >= 30:
            break
    other_products_text = "\n".join(other_names)

    if main_rows:
        product_name = main_rows[0].get("Tên sản phẩm", "").strip() or "Sản phẩm"
        product_code = (main_rows[0].get("Mã sản phẩm") or "").strip()
        price_table = build_price_list(main_rows)

        user_content = (
            f"KHÁCH HỎI: {user_text}\n\n"
            f"SẢN PHẨM SHOP CÓ (1 sản phẩm, nhiều biến thể):\n"
            f"- Tên sản phẩm: {product_name}\n"
            f"- Mã sản phẩm: {product_code}\n\n"
            f"CÁC BIẾN THỂ & GIÁ BÁN:\n{price_table}\n\n"
            f"LƯU Ý: Chỉ được phép tư vấn dựa trên các biến thể ở trên."
        )
    else:
        user_content = (
            f"KHÁCH HỎI: {user_text}\n\n"
            "KẾT QUẢ: Không tìm thấy sản phẩm nào khớp rõ ràng trong dữ liệu.\n\n"
            "DANH SÁCH MỘT SỐ SẢN PHẨM SHOP ĐANG CÓ:\n"
            f"{other_products_text}\n\n"
            "Chỉ được phép gợi ý trong danh sách trên, tuyệt đối không được nghĩ thêm sản phẩm mới."
        )

    system_prompt = (
        "Bạn là chatbot bán hàng của shop.\n"
        "- Chỉ được sử dụng CÁC SẢN PHẨM CÓ TRONG DỮ LIỆU cung cấp.\n"
        "- 1 sản phẩm được xác định bởi 'Mã sản phẩm'.\n"
        "- 1 biến thể được xác định bởi 'Mã mẫu mã'.\n"
        "- TUYỆT ĐỐI KHÔNG ĐƯỢC BỊA TÊN SẢN PHẨM, MÃ SẢN PHẨM, GIÁ, THUỘC TÍNH hay BIẾN THỂ MỚI.\n"
        "- Nếu không tìm thấy sản phẩm phù hợp → nói rõ là shop chưa có sản phẩm đó, "
        "sau đó gợi ý một vài sản phẩm khác shop ĐANG CÓ.\n"
        "- Văn phong linh hoạt: đọc cách khách nói để điều chỉnh giọng nhẹ nhàng, thân thiện, không dài dòng.\n"
        "- Luôn nhắc rõ mã sản phẩm, mã mẫu và giá khi tư vấn.\n"
    )

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        "temperature": 0.4,
    }

    try:
        resp = requests.post(OPENAI_URL, json=payload, headers=headers, timeout=30)
        data = resp.json()
        return data["choices"][0]["message"]["content"]
    except Exception as e:
        print("[GPT TEXT ERROR]:", e)
        return "Shop đang xử lý yêu cầu, bạn chờ một chút giúp shop nhé ❤️"


# ==============================================
# 6. GPT VISION – ĐỌC ẢNH KHÁCH GỬI
# ==============================================
def call_gpt_vision(image_url, all_products):
    product_list = []
    for p in all_products[:80]:
        name = (p.get("Tên sản phẩm") or "").strip()
        code = (p.get("Mã sản phẩm") or "").strip()
        if name or code:
            product_list.append(f"- {name} (Mã: {code})")
    products_text = "\n".join(product_list)

    system_prompt = (
        "Bạn là chatbot bán hàng của shop.\n"
        "- Khách gửi 1 bức ảnh. Hãy mô tả ngắn gọn sản phẩm trong ảnh.\n"
        "- Sau đó gợi ý 1–3 sản phẩm trong DANH SÁCH SHOP CÓ, "
        "gần giống nhất về kiểu dáng / công năng.\n"
        "- Chỉ được gợi ý sản phẩm có trong danh sách, không được bịa sản phẩm mới.\n"
    )

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "Danh sách sản phẩm shop đang có:\n"
                            f"{products_text}\n\n"
                            "Hãy xem ảnh bên dưới, mô tả ngắn gọn sản phẩm trong ảnh và gợi ý "
                            "những sản phẩm trong danh sách trên phù hợp nhất."
                        ),
                    },
                    {
                        "type": "image_url",
                        "image_url": {"url": image_url},
                    },
                ],
            },
        ],
    }

    try:
        resp = requests.post(OPENAI_URL, json=payload, headers=headers, timeout=40)
        data = resp.json()
        return data["choices"][0]["message"]["content"]
    except Exception as e:
        print("[GPT VISION ERROR]:", e)
        return "Shop nhận được ảnh rồi, nhưng chưa xem rõ. Bạn gửi lại giúp shop ảnh rõ nét hơn nhé."


# ==============================================
# 7. SEND API FACEBOOK
# ==============================================
def send_text(psid, text):
    url = "https://graph.facebook.com/v18.0/me/messages"
    payload = {
        "recipient": {"id": psid},
        "message": {"text": text},
    }
    params = {"access_token": PAGE_ACCESS_TOKEN}
    try:
        r = requests.post(url, params=params, json=payload, timeout=15)
        if r.status_code != 200:
            print("[SEND_TEXT ERROR]", r.status_code, r.text)
    except Exception as e:
        print("[SEND_TEXT EXCEPTION]", e)


def send_image(psid, image_url):
    url = "https://graph.facebook.com/v18.0/me/messages"
    payload = {
        "recipient": {"id": psid},
        "message": {
            "attachment": {
                "type": "image",
                "payload": {
                    "url": image_url,
                    "is_reusable": True,
                },
            }
        },
        "messaging_type": "RESPONSE",
    }
    params = {"access_token": PAGE_ACCESS_TOKEN}
    try:
        r = requests.post(url, params=params, json=payload, timeout=15)
        if r.status_code != 200:
            print("[SEND_IMAGE ERROR]", r.status_code, r.text)
    except Exception as e:
        print("[SEND_IMAGE EXCEPTION]", e)


def send_video(psid, video_url):
    url = "https://graph.facebook.com/v18.0/me/messages"
    payload = {
        "recipient": {"id": psid},
        "message": {
            "attachment": {
                "type": "video",
                "payload": {
                    "url": video_url,
                    "is_reusable": True,
                },
            }
        },
        "messaging_type": "RESPONSE",
    }
    params = {"access_token": PAGE_ACCESS_TOKEN}
    try:
        r = requests.post(url, params=params, json=payload, timeout=15)
        if r.status_code != 200:
            print("[SEND_VIDEO ERROR]", r.status_code, r.text)
    except Exception as e:
        print("[SEND_VIDEO EXCEPTION]", e)


# ==============================================
# 8. QUẢN LÝ ẢNH "GỬI THÊM"
# ==============================================
MORE_IMG_KEYWORDS = [
    "thêm ảnh",
    "thêm hình",
    "xem thêm ảnh",
    "xem thêm hình",
    "cho xem thêm ảnh",
    "cho xem thêm hình",
    "có ảnh khác không",
    "xem nhiều ảnh hơn",
    "full ảnh",
    "ảnh còn lại",
]


def is_more_images_request(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in MORE_IMG_KEYWORDS)


def handle_more_images(psid):
    info = LAST_IMAGES.get(psid)
    if not info:
        send_text(psid, "Hiện tại shop chưa có thêm ảnh nào khác cho mẫu này ạ.")
        return

    images = info.get("images", [])
    sent = info.get("sent", 0)

    if sent >= len(images):
        send_text(psid, "Shop đã gửi cho bạn toàn bộ ảnh hiện có của mẫu này rồi ạ ❤️")
        return

    next_batch = images[sent: sent + 5]
    for img in next_batch:
        send_image(psid, img)

    LAST_IMAGES[psid]["sent"] = sent + len(next_batch)
    send_text(psid, "Shop gửi thêm ảnh cho bạn xem rõ hơn nhé ❤️")


# ==============================================
# 9. QUẢN LÝ BẬT / TẮT BOT (ADMIN)
# ==============================================
def is_admin(psid: str) -> bool:
    if ADMIN_PSID:
        return psid == ADMIN_PSID
    # nếu chưa set ADMIN_PSID, tạm cho mọi người là admin (nên set cho an toàn)
    return True


def handle_admin_command(psid: str, text: str):
    global BOT_ENABLED

    t = text.lower().strip()

    # Bật bot
    if t in ["bật bot", "bat bot", "bat lại bot", "bật lại bot", "tiếp tục bot", "tiep tuc bot"]:
        BOT_ENABLED = True
        send_text(psid, "✅ Bot tự động đã được BẬT. Từ giờ bot sẽ hỗ trợ tư vấn cho khách.")
        return True

    # Tắt / tạm dừng bot
    if t in ["tắt bot", "tat bot", "tạm dừng bot", "tam dung bot", "dung bot"]:
        BOT_ENABLED = False
        send_text(psid, "⏸ Bot tự động đã được TẠM DỪNG. Tin nhắn mới sẽ do nhân viên xử lý.")
        return True

    return False


# ==============================================
# 10. WEBHOOK FACEBOOK
# ==============================================
@app.route("/webhook", methods=["GET"])
def verify():
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")
    if mode == "subscribe" and token == VERIFY_TOKEN:
        return challenge, 200
    return "Verification failed", 403


@app.route("/webhook", methods=["POST"])
def webhook():
    global BOT_ENABLED

    data = request.json
    print("[Webhook] Event:", data)

    if data.get("object") != "page":
        return "Ignored", 200

    products = load_products()

    for entry in data.get("entry", []):
        for event in entry.get("messaging", []):
            # Bỏ qua các event không phải message (delivery, read, reaction...)
            if "message" not in event:
                continue

            message = event.get("message", {})
            sender_id = event["sender"]["id"]

            # Bỏ qua echo (tin nhắn do page/bot gửi ra)
            if message.get("is_echo"):
                continue

            # Chống xử lý trùng cùng 1 mid
            mid = message.get("mid")
            if mid:
                last_mid = LAST_MESSAGE_IDS.get(sender_id)
                if last_mid == mid:
                    # đã xử lý rồi
                    continue
                LAST_MESSAGE_IDS[sender_id] = mid

            # Nếu là admin → kiểm tra lệnh bật/tắt bot
            text = message.get("text", "")
            if text and is_admin(sender_id):
                if handle_admin_command(sender_id, text):
                    # đã xử lý lệnh admin, không tư vấn tiếp
                    continue

            # Nếu bot đang tắt → trả lời 1 câu mặc định rồi dừng
            if not BOT_ENABLED:
                send_text(
                    sender_id,
                    "Hiện tại bot tự động đang tạm dừng để nhân viên hỗ trợ trực tiếp. "
                    "Bạn cứ để lại tin nhắn, nhân viên sẽ phản hồi sớm nhất ạ ❤️"
                )
                continue

            # ====== ẢNH KHÁCH GỬI ======
            if "attachments" in message:
                att = message["attachments"][0]
                if att.get("type") == "image":
                    img_url = att["payload"]["url"]
                    reply = call_gpt_vision(img_url, products)
                    send_text(sender_id, reply)
                    continue

            # ====== TEXT ======
            if text:
                # 1) Nếu khách yêu cầu "gửi thêm ảnh"
                if is_more_images_request(text):
                    handle_more_images(sender_id)
                    continue

                # 2) Tìm sản phẩm theo câu khách hỏi
                matched_rows = search_products(text, products)
                main_rows = group_variants_by_product(matched_rows, products)

                # 3) Gửi 5 ảnh đầu tiên
                if main_rows:
                    all_imgs = get_images_for_rows(main_rows, max_images=None)
                    if all_imgs:
                        first_batch = all_imgs[:5]
                        for img in first_batch:
                            send_image(sender_id, img)

                        LAST_IMAGES[sender_id] = {
                            "images": all_imgs,
                            "sent": len(first_batch),
                        }

                    # Gửi 1 video nếu có
                    videos = get_videos_for_rows(main_rows, max_videos=1)
                    for v in videos:
                        send_video(sender_id, v)

                # 4) Gọi GPT tư vấn (KHÔNG BỊA)
                reply = call_gpt_text(text, main_rows, products)
                send_text(sender_id, reply)

    return "OK", 200


# ==============================================
# 11. HEALTHCHECK
# ==============================================
@app.route("/")
def home():
    return "Messenger GPT bot is running.", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
