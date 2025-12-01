# ==============================================
# FB GPT CHATBOT — BẢN SỬA TOÀN DIỆN
# - Không bịa sản phẩm
# - Tự nhận diện sản phẩm & danh mục từ Sheet
# - Gửi 5 ảnh đẹp nhất, khách hỏi mới gửi thêm
# - Gửi ảnh + video
# - Đọc ảnh khách gửi (GPT Vision)
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
SHEET_CSV_URL = os.getenv("SHEET_CSV_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

OPENAI_URL = "https://api.openai.com/v1/chat/completions"

# Lưu trạng thái ảnh đã gửi cho từng khách
LAST_IMAGES = {}  # { psid: {"images": [...], "sent": 5} }


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
    - Bỏ URL có ký tự tiếng Trung trong đường link (watermark Trung Quốc)
    (Lưu ý: không loại domain alicdn.com, chỉ loại URL có ký tự 汉字 trong path)
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

        # chỉ chấp nhận jpg/png/webp/mp4
        if not (".jpg" in u or ".jpeg" in u or ".png" in u or ".webp" in u or ".mp4" in u):
            continue

        # bỏ URL chứa ký tự tiếng Trung trong đường link (thường là ảnh có chữ TQ)
        if re.search(r"[\u4e00-\u9fff]", u):
            continue

        if u not in seen:
            seen.add(u)
            urls.append(u)

    return urls


def get_images_for_rows(rows, max_images=None):
    """
    Gom ảnh từ nhiều dòng sản phẩm (cùng Mã sản phẩm)
    Trả về list URL ảnh (đã loại trùng).
    max_images=None → lấy hết, nếu truyền số thì cắt bớt.
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
    "Mã mẫu mã mới",
    "Tên sản phẩm",
    "Keyword sản phẩm",
    "Keyword mẫu mã",
    "Thuộc tính",
    "Thuộc tính sản phẩm",
    "Mô tả",
]


def search_products(query, products, limit=30):
    q = query.lower().strip()
    if not q:
        return []

    matches = []

    # Match chặt: nguyên câu
    for row in products:
        combined = " ".join(row.get(f, "") for f in SEARCH_FIELDS).lower()
        if q in combined:
            matches.append(row)
            if len(matches) >= limit:
                break

    # Nếu chưa có, match theo từ khoá
    if not matches:
        tokens = [t for t in re.split(r"\s+", q) if t]
        for row in products:
            combined = " ".join(row.get(f, "") for f in SEARCH_FIELDS).lower()
            # chỉ cần khớp vài từ đầu
            if tokens and all(t in combined for t in tokens[:2]):
                matches.append(row)
                if len(matches) >= limit:
                    break

    return matches


def group_variants_by_product(matched_rows, all_products):
    """
    matched_rows: những dòng tìm được theo câu khách hỏi
    Trả về:
      - main_rows: list các dòng cùng Mã sản phẩm chính (dùng để hiển thị bảng giá & ảnh)
    """
    if not matched_rows:
        return []

    # Lấy dòng đầu tiên làm "sản phẩm chính"
    first = matched_rows[0]
    main_code = first.get("Mã sản phẩm") or first.get("Mã sản phẩm mới")

    if not main_code:
        # không có mã sản phẩm → dùng luôn matched_rows
        return matched_rows

    main_rows = [
        r for r in all_products
        if (r.get("Mã sản phẩm") or r.get("Mã sản phẩm mới")) == main_code
    ]

    return main_rows or matched_rows


# ==============================================
# 4. TẠO BẢNG GIÁ GỌN, DỄ ĐỌC
# ==============================================
def build_price_list(rows):
    """
    Mỗi dòng là 1 biến thể:
    - Mã mẫu
    - Giá bán
    - Thuộc tính
    """
    lines = []
    for r in rows:
        code = r.get("Mã mẫu mã mới") or r.get("Mã mẫu mã") or ""
        price = r.get("Giá bán", "").strip()
        attr = r.get("Thuộc tính sản phẩm") or r.get("Thuộc tính") or ""

        if price and not price.endswith("đ"):
            price = f"{price}đ"

        line = f"- Mã mẫu: {code}, Giá: {price}, Thuộc tính: {attr}"
        lines.append(line)

    return "\n".join(lines)


# ==============================================
# 5. GPT TƯ VẤN (TEXT) — CẤM BỊA SẢN PHẨM
# ==============================================
def call_gpt_text(user_text, main_rows, all_products):
    """
    main_rows: danh sách biến thể thuộc 1 sản phẩm chính
    all_products: toàn bộ sản phẩm trong sheet (để gợi ý liên quan)
    """

    # Lấy vài sản phẩm khác để gợi ý khi không có kết quả khớp
    other_names = []
    for p in all_products:
        name = (p.get("Tên sản phẩm") or "").strip()
        code = (p.get("Mã sản phẩm") or p.get("Mã sản phẩm mới") or "").strip()
        if name and code:
            other_names.append(f"- {name} (Mã: {code})")
        if len(other_names) >= 30:
            break
    other_products_text = "\n".join(other_names)

    if main_rows:
        product_name = main_rows[0].get("Tên sản phẩm", "").strip() or "Sản phẩm"
        product_code = main_rows[0].get("Mã sản phẩm") or main_rows[0].get("Mã sản phẩm mới") or ""
        price_table = build_price_list(main_rows)

        user_content = (
            f"KHÁCH HỎI: {user_text}\n\n"
            f"ĐÂY LÀ SẢN PHẨM SHOP THỰC SỰ CÓ:\n"
            f"- Tên sản phẩm: {product_name}\n"
            f"- Mã sản phẩm: {product_code}\n"
            f"- CÁC BIẾN THỂ & GIÁ BÁN:\n{price_table}\n\n"
            f"LƯU Ý: Chỉ được phép tư vấn dựa trên các biến thể ở trên."
        )
    else:
        user_content = (
            f"KHÁCH HỎI: {user_text}\n\n"
            f"KẾT QUẢ: Không tìm thấy sản phẩm khớp rõ ràng trong danh sách.\n\n"
            f"DANH SÁCH MỘT SỐ SẢN PHẨM SHOP ĐANG CÓ:\n{other_products_text}\n\n"
            f"Chỉ được phép gợi ý các sản phẩm nằm trong danh sách trên."
        )

    system_prompt = (
        "Bạn là chatbot bán hàng của shop.\n"
        "- Chỉ được sử dụng CÁC SẢN PHẨM CÓ TRONG DỮ LIỆU cung cấp.\n"
        "- TUYỆT ĐỐI KHÔNG ĐƯỢC BỊA TÊN SẢN PHẨM, MÃ SẢN PHẨM, GIÁ HAY BIẾN THỂ MỚI.\n"
        "- Nếu khách hỏi sản phẩm SHOP KHÔNG CÓ → trả lời rõ ràng là shop không có, "
        "sau đó gợi ý MỘT VÀI SẢN PHẨM KHÁC shop ĐANG CÓ (trong danh sách all_products).\n"
        "- Văn phong linh hoạt, đọc cách khách nói để điều chỉnh: nếu khách thân mật thì trả lời thân mật, "
        "nếu khách lịch sự thì trả lời lịch sự. Không nói quá dài, không lan man.\n"
        "- Luôn nhắc rõ mã mẫu, giá và thuộc tính khi tư vấn.\n"
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
    """
    GPT xem ảnh khách gửi, mô tả sản phẩm trong ảnh,
    và cố gắng map với sản phẩm shop CÓ.
    """
    # Chuẩn bị danh sách rút gọn sản phẩm để AI bám vào
    product_list = []
    for p in all_products[:80]:
        name = (p.get("Tên sản phẩm") or "").strip()
        code = (p.get("Mã sản phẩm") or p.get("Mã sản phẩm mới") or "").strip()
        if name or code:
            product_list.append(f"- {name} (Mã: {code})")
    products_text = "\n".join(product_list)

    system_prompt = (
        "Bạn là chatbot bán hàng thời trang và đồ dùng.\n"
        "- Khách gửi một bức ảnh, bạn cần mô tả ngắn gọn sản phẩm trong ảnh, "
        "sau đó đối chiếu với danh sách sản phẩm shop có.\n"
        "- Chỉ được gợi ý những sản phẩm CÓ trong danh sách, không được bịa sản phẩm mới.\n"
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
                            "Đây là danh sách sản phẩm shop đang có:\n"
                            f"{products_text}\n\n"
                            "Hãy xem ảnh khách gửi, mô tả ngắn gọn sản phẩm trong ảnh và gợi ý "
                            "1–3 sản phẩm trong danh sách trên mà shop có, phù hợp nhất với ảnh."
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
# 8. NHẬN TEXT "GỬI THÊM ẢNH"
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


def is_more_images_request(text):
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

    # Gửi tiếp tối đa 5 ảnh nữa
    next_batch = images[sent: sent + 5]
    for img in next_batch:
        send_image(psid, img)

    LAST_IMAGES[psid]["sent"] = sent + len(next_batch)
    send_text(psid, "Shop gửi thêm ảnh cho bạn xem rõ hơn nhé ❤️")


# ==============================================
# 9. WEBHOOK FACEBOOK
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
    data = request.json
    print("[Webhook] Event:", data)

    products = load_products()

    if data.get("object") != "page":
        return "Ignored", 200

    for entry in data.get("entry", []):
        for event in entry.get("messaging", []):
            sender_id = event["sender"]["id"]

            message = event.get("message", {})

            # Bỏ qua echo (tin nhắn do bot gửi, FB trả ngược lại)
            if message.get("is_echo"):
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
            if "text" in message:
                user_text = message["text"]

                # 1) Nếu khách yêu cầu "gửi thêm ảnh"
                if is_more_images_request(user_text):
                    handle_more_images(sender_id)
                    continue

                # 2) Tìm sản phẩm theo câu khách hỏi
                matched_rows = search_products(user_text, products)
                main_rows = group_variants_by_product(matched_rows, products)

                # 3) Gửi 5 ảnh đẹp nhất ngay lần đầu
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

                    # Gửi video nếu có (1 video)
                    videos = get_videos_for_rows(main_rows, max_videos=1)
                    for v in videos:
                        send_video(sender_id, v)

                # 4) Gọi GPT tư vấn (không bịa)
                reply = call_gpt_text(user_text, main_rows, products)
                send_text(sender_id, reply)

    return "OK", 200


# ==============================================
# 10. HEALTHCHECK
# ==============================================
@app.route("/")
def home():
    return "Messenger GPT bot is running.", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
