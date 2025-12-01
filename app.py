import os
import time
import csv
import re
import logging
from collections import defaultdict, deque
from io import StringIO

import requests
from flask import Flask, request, jsonify
from openai import OpenAI

# -----------------------
# CẤU HÌNH CƠ BẢN
# -----------------------

PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")
SHEET_CSV_URL = os.getenv("SHEET_CSV_URL")  # link export?format=csv
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not PAGE_ACCESS_TOKEN:
    raise RuntimeError("Missing PAGE_ACCESS_TOKEN")
if not VERIFY_TOKEN:
    raise RuntimeError("Missing VERIFY_TOKEN")
if not SHEET_CSV_URL:
    raise RuntimeError("Missing SHEET_CSV_URL")
if not OPENAI_API_KEY:
    raise RuntimeError("Missing OPENAI_API_KEY")

client = OpenAI(api_key=OPENAI_API_KEY)

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# -----------------------
# BIẾN TOÀN CỤC
# -----------------------

PRODUCTS_BY_ID = {}          # {product_id: [row_dict, ...]}
LAST_SHEET_LOAD = 0
SHEET_TTL = 300              # 5 phút reload 1 lần

BOT_ENABLED = True           # lệnh BẬT BOT / TẮT BOT
PROCESSED_MIDS = deque(maxlen=500)  # tránh xử lý 2 lần cùng 1 MID
USER_CONTEXT = {}            # {sender_id: {"last_product_id": str, "last_time": ts}}

# -----------------------
# TIỆN ÍCH CHUNG
# -----------------------


def has_chinese(s: str) -> bool:
    if not s:
        return False
    for ch in s:
        if "\u4e00" <= ch <= "\u9fff":
            return True
    return False


def split_images_cell(cell: str):
    """Tách 1 ô Images thành list URL, bỏ rỗng / khoảng trắng."""
    if not cell:
        return []
    # Tách theo xuống dòng, dấu phẩy, dấu chấm phẩy, khoảng trắng
    parts = re.split(r"[\n,\s;]+", cell.strip())
    urls = [p for p in parts if p.startswith("http")]
    return urls


def filter_images(urls):
    """Loại trùng + bỏ ảnh có watermark chữ Trung Quốc (dựa trên URL có ký tự TQ)."""
    seen = set()
    clean = []
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        # CHỈ loại watermark chữ Trung Quốc (URL có ký tự TQ)
        if has_chinese(u):
            continue
        clean.append(u)
    return clean


def send_fb_request(payload):
    url = "https://graph.facebook.com/v18.0/me/messages"
    params = {"access_token": PAGE_ACCESS_TOKEN}
    try:
        r = requests.post(url, params=params, json=payload, timeout=15)
        logging.info("[FB SEND] %s %s", r.status_code, r.text)
        return r
    except Exception as e:
        logging.exception("[FB ERROR] %s", e)
        return None


def send_text(recipient_id, text):
    payload = {
        "recipient": {"id": recipient_id},
        "message": {"text": text},
    }
    logging.info("[FB TEXT] -> %s: %s", recipient_id, text)
    return send_fb_request(payload)


def send_image(recipient_id, image_url):
    payload = {
        "recipient": {"id": recipient_id},
        "message": {
            "attachment": {
                "type": "image",
                "payload": {"url": image_url, "is_reusable": False},
            }
        },
    }
    logging.info("[FB IMAGE] -> %s: %s", recipient_id, image_url)
    return send_fb_request(payload)


# -----------------------
# LOAD SHEET SẢN PHẨM
# -----------------------


def load_products(force=False):
    global PRODUCTS_BY_ID, LAST_SHEET_LOAD
    now = time.time()
    if not force and (now - LAST_SHEET_LOAD) < SHEET_TTL and PRODUCTS_BY_ID:
        return

    logging.info("[Sheet] Loading products from %s", SHEET_CSV_URL)
    try:
        resp = requests.get(SHEET_CSV_URL, timeout=30)
        resp.raise_for_status()
        content = resp.content.decode("utf-8-sig")
        f = StringIO(content)
        reader = csv.DictReader(f)

        products = defaultdict(list)
        for row in reader:
            product_id = (row.get("Mã sản phẩm") or "").strip()
            if not product_id:
                continue
            products[product_id].append(row)

        PRODUCTS_BY_ID = dict(products)
        LAST_SHEET_LOAD = now
        total_rows = sum(len(v) for v in PRODUCTS_BY_ID.values())
        logging.info("[Sheet] Loaded %s rows, %s products", total_rows, len(PRODUCTS_BY_ID))
    except Exception as e:
        logging.exception("[Sheet] Error loading sheet: %s", e)


def get_all_products_list():
    """Trả về list (product_id, rows) để tiện iterate."""
    load_products()
    return list(PRODUCTS_BY_ID.items())


# -----------------------
# TÌM SẢN PHẨM PHÙ HỢP
# -----------------------


def normalize(text: str) -> str:
    return (text or "").lower().strip()


def find_product_by_code(message_text):
    """
    Nếu khách gửi mã sản phẩm hoặc mã mẫu mã: tìm chính xác.
    - So sánh nguyên chuỗi với 'Mã sản phẩm' hoặc 'Mã mẫu mã'
    """
    msg = normalize(message_text)
    tokens = re.split(r"[\s,.;:\-_/]+", msg)
    token_set = set(t for t in tokens if t)

    load_products()
    # Tìm theo mã sản phẩm
    for pid, rows in PRODUCTS_BY_ID.items():
        pid_norm = normalize(pid)
        if pid_norm in token_set:
            return pid, rows

    # Tìm theo mã mẫu mã
    for pid, rows in PRODUCTS_BY_ID.items():
        for r in rows:
            variant_code = normalize(r.get("Mã mẫu mã") or "")
            if variant_code and variant_code in token_set:
                return pid, rows

    return None, None


def score_product_for_query(rows, query):
    """
    Chấm điểm đơn giản dựa trên từ khóa xuất hiện trong:
    - Tên sản phẩm
    - Keyword sản phẩm
    - Danh mục
    - Thương hiệu
    """
    q = normalize(query)
    if not q:
        return 0
    words = [w for w in re.split(r"\s+", q) if len(w) > 2]
    if not words:
        return 0

    base = rows[0]
    fields = [
        base.get("Tên sản phẩm") or "",
        base.get("Keyword sản phẩm") or "",
        base.get("Danh mục") or "",
        base.get("Thương hiệu") or "",
    ]
    text = normalize(" ".join(fields))

    score = 0
    for w in words:
        if w in text:
            score += 1
    return score


def find_best_product_for_query(message_text):
    """
    1. Ưu tiên tìm theo mã sản phẩm / mã mẫu mã.
    2. Nếu không có, dùng điểm từ khóa để tìm sản phẩm phù hợp nhất.
    """
    pid, rows = find_product_by_code(message_text)
    if pid:
        return pid, rows

    products = get_all_products_list()
    best_pid = None
    best_rows = None
    best_score = 0

    for pid, rows in products:
        s = score_product_for_query(rows, message_text)
        if s > best_score:
            best_score = s
            best_pid = pid
            best_rows = rows

    # Nếu score = 0 => coi như không match
    if best_score == 0:
        return None, None
    return best_pid, best_rows


# -----------------------
# XỬ LÝ GIÁ & ẢNH
# -----------------------


def collect_all_images_for_product(rows):
    """
    Lấy toàn bộ ảnh từ cột Images của mọi dòng của 1 sản phẩm.
    """
    urls = []
    for r in rows:
        cell = r.get("Images") or ""
        urls.extend(split_images_cell(cell))
    return filter_images(urls)


def group_variants_by_price(rows):
    """
    Gom biến thể theo Giá bán.
    - Mỗi dòng sheet là 1 biến thể.
    - Dùng:
       - màu (Thuộc tính)
       - size (Thuộc tính)
       - Giá bán
    Trả về: {price_str: {"colors": set(), "sizes": set()}}
    """
    groups = defaultdict(lambda: {"colors": set(), "sizes": set()})

    for r in rows:
        price_raw = (r.get("Giá bán") or "").strip()
        if not price_raw:
            continue

        color = (r.get("màu (Thuộc tính)") or "").strip()
        size = (r.get("size (Thuộc tính)") or "").strip()

        g = groups[price_raw]
        if color:
            g["colors"].add(color)
        if size:
            g["sizes"].add(size)

    return groups


def format_price_groups(groups):
    """
    Định dạng phần giá theo cấu trúc:
    - Nếu 1 giá: "Giá đặc biệt ưu đãi cho anh/chị hôm nay là: 365k"
    - Nếu nhiều giá:
         "Màu trắng, đen (size S/M/L) giá: 365k
          Màu đỏ (size M/L) giá: 420k"
    """
    if not groups:
        return "Hiện tại sản phẩm chưa có thông tin giá rõ ràng, anh/chị cho shop xin thêm một chút thời gian để kiểm tra lại ạ."

    if len(groups) == 1:
        price = next(iter(groups.keys()))
        return f"Giá đặc biệt ưu đãi cho anh/chị hôm nay là: {price}."

    lines = []
    for price, info in groups.items():
        colors = ", ".join(sorted(info["colors"])) if info["colors"] else "Nhiều màu"
        if info["sizes"]:
            sizes = ", ".join(sorted(info["sizes"]))
            lines.append(f"{colors} (size {sizes}) giá: {price}.")
        else:
            lines.append(f"{colors} giá: {price}.")
    return "\n".join(lines)


# -----------------------
# GỌI GPT ĐỂ TÓM TẮT MÔ TẢ & TẠO CTA
# -----------------------


def ask_gpt_for_summary_and_cta(product_name, description, user_message):
    """
    GPT chỉ được phép:
    - Tóm tắt ưu điểm dựa trên mô tả có sẵn.
    - Tạo CTA mềm, không bịa sản phẩm mới.
    """
    prompt = f"""
Bạn là nhân viên bán hàng online của shop quần áo.

Dựa trên thông tin sản phẩm dưới đây, hãy:
1) Viết 2-3 câu ngắn gọn nêu bật ưu điểm của sản phẩm, dễ hiểu, thân thiện.
2) Viết 1 câu CTA khuyến khích khách chọn mẫu và chốt đơn.

YÊU CẦU QUAN TRỌNG:
- Chỉ dùng thông tin có trong mô tả, KHÔNG bịa ra chất liệu, công dụng hay tính năng không có.
- Không nhắc đến sản phẩm khác ngoài sản phẩm này.
- Trả lời hoàn toàn bằng tiếng Việt.

Tên sản phẩm: {product_name}
Mô tả sản phẩm: {description}

Tin nhắn gần nhất của khách: {user_message}

Hãy trả lời đúng theo cấu trúc:
[ƯU ĐIỂM]
(các câu ưu điểm)

[CTA]
(câu kêu gọi hành động)
"""
    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": "Bạn là trợ lý bán hàng tư vấn sản phẩm dựa trên dữ liệu có sẵn, tuyệt đối không bịa đặt sản phẩm mới."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.5,
            max_tokens=300,
        )
        answer = resp.choices[0].message.content.strip()
        # Tách phần ưu điểm & CTA
        advantages = ""
        cta = ""
        parts = re.split(r"\[CTA\]", answer, flags=re.IGNORECASE)
        if len(parts) == 2:
            advantages_raw = parts[0]
            cta_raw = parts[1]
            advantages = re.sub(r"\[ƯU ĐIỂM\]", "", advantages_raw, flags=re.IGNORECASE).strip()
            cta = cta_raw.strip()
        else:
            advantages = answer
            cta = "Anh/chị ưng mẫu nào để shop chốt đơn giúp mình luôn nhé! ❤️"
        return advantages, cta
    except Exception as e:
        logging.exception("[GPT ERROR] %s", e)
        # Fallback an toàn
        return (
            "Sản phẩm có thiết kế đẹp, chất liệu dễ chịu và phù hợp nhiều hoàn cảnh sử dụng.",
            "Anh/chị ưng mẫu nào thì báo lại để shop chốt đơn và giữ hàng cho mình nhé! ❤️",
        )


# -----------------------
# TƯ VẤN 1 SẢN PHẨM THEO CẤU TRÚC MỚI
# -----------------------


def consult_single_product(sender_id, rows, user_message):
    """
    Cấu trúc tư vấn:
    1) Tên sản phẩm
    2) Ảnh chung (gửi tất cả ảnh 1 lần, đã lọc trùng & watermark TQ)
    3) Ưu điểm nổi bật (2-3 câu) -> từ GPT
    4) Giá bán:
       - 1 giá: "Giá đặc biệt..."
       - nhiều giá: nhóm theo giá như yêu cầu
    5) CTA
    """
    base = rows[0]
    product_name = (base.get("Tên sản phẩm") or "").strip()
    description = (base.get("Mô tả") or "").strip()

    # 1) Tên sản phẩm
    if product_name:
        send_text(sender_id, product_name)
    else:
        send_text(sender_id, "Shop gửi anh/chị thông tin sản phẩm phù hợp nhé:")

    # 2) Ảnh chung
    all_images = collect_all_images_for_product(rows)
    logging.info("[PRODUCT IMAGES] %s images after filter", len(all_images))
    for img in all_images:
        send_image(sender_id, img)

    # 3) Ưu điểm + 5) CTA (từ GPT)
    advantages, cta = ask_gpt_for_summary_and_cta(product_name, description, user_message)

    # 4) Giá bán
    price_groups = group_variants_by_price(rows)
    price_text = format_price_groups(price_groups)

    # Ghép message text cho bước 3 + 4
    msg_parts = []
    if advantages:
        msg_parts.append(advantages)
    if price_text:
        msg_parts.append("\n" + price_text)
    if cta:
        msg_parts.append("\n" + cta)

    final_text = "\n".join(msg_parts).strip()
    send_text(sender_id, final_text)


# -----------------------
# GỬI ẢNH BIẾN THỂ KHI KHÁCH YÊU CẦU
# -----------------------


def extract_color_from_message(message_text):
    """
    Bắt đơn giản các cụm sau từ câu: 'màu ...'
    Ví dụ: 'gửi ảnh màu đỏ' -> 'đỏ'
    """
    msg = normalize(message_text)
    # Tìm 'màu ' + từ tiếp theo
    m = re.search(r"màu\s+([^\s,.;!?]+)", msg)
    if m:
        return m.group(1).strip()
    return None


def send_variant_image_for_color(sender_id, color_keyword, product_rows):
    """
    Ảnh biến thể:
      - chỉ gửi khi khách yêu cầu rõ màu
      - tìm dòng đầu tiên có 'màu (Thuộc tính)' chứa từ khóa color_keyword
      - lấy ảnh đầu tiên trong ô Images
      - chỉ gửi 1 ảnh
    """
    color_keyword_norm = normalize(color_keyword)
    for r in product_rows:
        color_val = normalize(r.get("màu (Thuộc tính)") or "")
        if color_keyword_norm and color_keyword_norm in color_val:
            images = split_images_cell(r.get("Images") or "")
            images = filter_images(images)
            if images:
                send_text(sender_id, f"Shop gửi anh/chị ảnh mẫu màu {r.get('màu (Thuộc tính)')} nhé:")
                send_image(sender_id, images[0])
                return True
    # Không tìm thấy
    send_text(sender_id, "Hiện tại shop chưa tìm thấy ảnh đúng với màu anh/chị yêu cầu, anh/chị mô tả lại giúp shop với ạ.")
    return False


# -----------------------
# XỬ LÝ INTENT CHUNG CHUNG
# -----------------------


def handle_general_interest(sender_id, message_text):
    """
    Khi khách hỏi chung chung:
    - Kết hợp:
        1) Gợi ý chọn danh mục (từ cột Danh mục)
        3) Hỏi thêm nhu cầu cụ thể
    """
    load_products()
    categories = set()
    for rows in PRODUCTS_BY_ID.values():
        for r in rows:
            cat = (r.get("Danh mục") or "").strip()
            if cat:
                categories.add(cat)
    cat_list = sorted(categories)

    if cat_list:
        cat_text = ", ".join(cat_list[:10])
        msg = (
            "Hiện tại shop đang có khá nhiều mẫu, để tư vấn chuẩn hơn anh/chị giúp shop chọn 1 trong các nhóm sau nhé:\n"
            f"- {cat_text}\n\n"
            "Hoặc anh/chị mô tả giúp shop: muốn tìm váy/áo/quần/set, phong cách thế nào, tầm giá khoảng bao nhiêu ạ?"
        )
    else:
        msg = (
            "Anh/chị mô tả giúp shop đang muốn tìm loại sản phẩm nào (váy/áo/quần/set...), màu sắc, size và tầm giá khoảng bao nhiêu để shop lọc mẫu phù hợp nhất cho mình nhé."
        )
    send_text(sender_id, msg)


# -----------------------
# WEBHOOK FACEBOOK
# -----------------------


@app.route("/webhook", methods=["GET"])
def verify():
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")
    if mode == "subscribe" and token == VERIFY_TOKEN:
        logging.info("[Webhook] Verified")
        return challenge, 200
    return "Verification token mismatch", 403


@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True, silent=True) or {}
    logging.info("[Webhook] %s", data)

    if data.get("object") != "page":
        return "ignored", 200

    load_products()

    for entry in data.get("entry", []):
        for event in entry.get("messaging", []):
            sender_id = event.get("sender", {}).get("id")
            recipient_id = event.get("recipient", {}).get("id")
            ts = event.get("timestamp")

            # Bỏ qua echo / delivery / read
            if event.get("message", {}).get("is_echo"):
                logging.info("[ECHO] Skip echo message")
                continue
            if "delivery" in event or "read" in event:
                logging.info("[STATUS] Skip delivery/read")
                continue

            # Tránh xử lý 2 lần cùng MID
            mid = event.get("message", {}).get("mid") if "message" in event else None
            if mid:
                if mid in PROCESSED_MIDS:
                    logging.info("[DUP MID] Skip %s", mid)
                    continue
                PROCESSED_MIDS.append(mid)

            if "message" in event:
                handle_message_event(sender_id, event["message"])
            elif "postback" in event:
                handle_postback_event(sender_id, event["postback"])

    return "ok", 200


def handle_postback_event(sender_id, postback):
    payload = (postback.get("payload") or "").upper().strip()
    if payload == "GET_STARTED":
        send_text(
            sender_id,
            "Chào anh/chị, em là trợ lý bán hàng của shop. Anh/chị muốn tìm mẫu nào để em tư vấn ạ?",
        )
    else:
        send_text(sender_id, "Shop đã nhận được yêu cầu của anh/chị, anh/chị mô tả chi tiết hơn giúp em nhé.")


def handle_message_event(sender_id, message):
    global BOT_ENABLED

    text = message.get("text")
    attachments = message.get("attachments", [])

    # Lệnh bật/tắt bot
    if text:
        t_norm = normalize(text)
        if "tắt bot" in t_norm:
            BOT_ENABLED = False
            send_text(sender_id, "Bot đã tạm dừng. Khi nào cần tư vấn tự động anh/chị gõ 'BẬT BOT' giúp shop nhé.")
            return
        if "bật bot" in t_norm:
            BOT_ENABLED = True
            send_text(sender_id, "Bot đã được bật lại, anh/chị cứ nhắn nhu cầu để em tư vấn ạ.")
            return

    if not BOT_ENABLED:
        # Cho phép admin vẫn trò chuyện thủ công, nhưng bot không trả lời
        return

    # Nếu có hình ảnh khách gửi -> phản hồi đơn giản (chưa phân tích ảnh)
    if attachments:
        has_image = any(att.get("type") == "image" for att in attachments)
        if has_image:
            send_text(
                sender_id,
                "Shop nhận được ảnh của anh/chị rồi ạ. Anh/chị mô tả giúp em đang muốn tìm mẫu giống ảnh hay chỉ tham khảo cho vui thôi ạ?",
            )
            return

    if not text:
        send_text(sender_id, "Anh/chị mô tả giúp shop đang tìm mẫu gì để em tư vấn ạ.")
        return

    # Lưu context thời gian
    USER_CONTEXT.setdefault(sender_id, {})
    USER_CONTEXT[sender_id]["last_time"] = time.time()

    # Kiểm tra xem khách đang hỏi ảnh theo màu?
    color_kw = None
    if "ảnh" in normalize(text) and "màu" in normalize(text):
        color_kw = extract_color_from_message(text)

    if color_kw and USER_CONTEXT[sender_id].get("last_product_id"):
        pid = USER_CONTEXT[sender_id]["last_product_id"]
        rows = PRODUCTS_BY_ID.get(pid, [])
        if rows:
            send_variant_image_for_color(sender_id, color_kw, rows)
            return

    # Nếu khách hỏi chung chung (không có từ khóa rõ ràng)
    if len(text.strip()) < 4:
        handle_general_interest(sender_id, text)
        return

    # Tìm sản phẩm phù hợp
    pid, rows = find_best_product_for_query(text)
    if not pid or not rows:
        send_text(
            sender_id,
            "Hiện tại shop chưa tìm thấy sản phẩm phù hợp đúng với mô tả của anh/chị trong kho. Anh/chị mô tả chi tiết hơn (loại sản phẩm, màu, size, tầm giá) để em tìm lại giúp nhé.",
        )
        return

    # Lưu lại sản phẩm vừa tư vấn để sau nếu khách bảo "gửi ảnh màu đỏ" thì biết đang nói về sản phẩm nào
    USER_CONTEXT[sender_id]["last_product_id"] = pid

    consult_single_product(sender_id, rows, text)


# -----------------------
# HEALTHCHECK
# -----------------------


@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({"status": "ok"}), 200


# -----------------------
# CHẠY LOCAL (DEBUG)
# -----------------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)
