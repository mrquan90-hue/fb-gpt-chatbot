import os
import json
import re
import time
import csv
from collections import defaultdict
from urllib.parse import quote

import requests
from flask import Flask, request, send_from_directory
from openai import OpenAI

# ============================================
# FLASK APP
# ============================================

app = Flask(__name__, static_folder="static", static_url_path="/static")

# ============================================
# ENVIRONMENT (Render)
# ============================================

OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY")
PAGE_ACCESS_TOKEN  = os.getenv("PAGE_ACCESS_TOKEN")
VERIFY_TOKEN       = os.getenv("VERIFY_TOKEN")
FREEIMAGE_API_KEY  = os.getenv("FREEIMAGE_API_KEY")
SHEET_URL          = os.getenv("SHEET_CSV_URL")
DOMAIN             = os.getenv("DOMAIN", "fb-gpt-chatbot.onrender.com")

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ============================================
# GLOBAL STATE
# ============================================

USER_CONTEXT = defaultdict(lambda: {
    "last_ms": None,               # mã sản phẩm gần nhất bot hiểu
    "inbox_entry_ms": None,        # mã từ Fchat/referral
    "vision_ms": None,             # mã từ GPT Vision
    "caption_ms": None,            # dự phòng (caption bài viết)
    "history": [],                 # lịch sử hội thoại cho GPT
    "greeted": False,              # đã chào chưa
    "recommended_sent": False,     # đã gửi 5 sản phẩm gợi ý chưa
    "product_info_sent_ms": None,  # đã gửi thông tin sản phẩm nào
    "carousel_sent": False,        # đã gửi carousel chưa
})

PRODUCTS = {}
LAST_LOAD = 0
LOAD_TTL = 300  # 5 phút

# ============================================
# TỪ KHOÁ THỂ HIỆN Ý ĐỊNH "ĐẶT HÀNG / MUA"
# (ĐÃ LOẠI BỎ "ok", "ừ", "được" để tránh nhầm)
# ============================================

ORDER_KEYWORDS = [
    "đặt hàng nha",
    "ok đặt",
    "ok mua",
    "ok em",
    "ok e",
    "mua 1 cái",
    "mua cái này",
    "mua luôn",
    "chốt",
    "lấy mã",
    "lấy mẫu",
    "lấy luôn",
    "lấy em này",
    "lấy e này",
    "gửi cho",
    "ship cho",
    "ship 1 cái",
    "chốt 1 cái",
    "cho tôi mua",
    "tôi lấy nhé",
    "cho mình đặt",
    "tôi cần mua",
    "xác nhận đơn hàng giúp tôi",
    "tôi đồng ý mua",
    "làm đơn cho tôi đi",
    "tôi chốt đơn nhé",
    "cho xin 1 cái",
    "cho đặt 1 chiếc",
    "bên shop tạo đơn giúp em",
    "okela",
    "ok bạn",
    "đồng ý",
    "được đó",
    "vậy cũng được",
    "được vậy đi",
    "chốt như bạn nói",
    "ok giá đó đi",
    "lấy mẫu đó đi",
    "tư vấn giúp mình đặt hàng",
    "hướng dẫn mình mua với",
    "bạn giúp mình đặt nhé",
    "muốn có nó quá",
    "muốn mua quá",
    "ưng quá, làm sao để mua",
    "chốt đơn",
    "bán cho em",
    "bán cho em vé",
    "xuống đơn giúp em",
    "đơm hàng",
    "lấy nha",
    "lấy nhé",
    "mua nha",
    "mình lấy đây",
    "shop ơi, của em",
    "vậy lấy cái",
    "thôi lấy cái",
    "order nhé",
]

# ============================================
# TIỆN ÍCH FACEBOOK
# ============================================

def send_message(uid: str, text: str) -> None:
    if not text:
        return
    url = "https://graph.facebook.com/v18.0/me/messages"
    params = {"access_token": PAGE_ACCESS_TOKEN}
    payload = {
        "recipient": {"id": uid},
        "message": {"text": text},
        "messaging_type": "RESPONSE",
    }
    try:
        r = requests.post(url, params=params, json=payload, timeout=15)
        print("SEND MSG:", r.status_code, r.text)
    except Exception as e:
        print("SEND MSG ERROR:", e)


def send_image(uid: str, image_url: str) -> None:
    """
    Gửi ảnh qua Facebook Messenger bằng cách UPLOAD file trực tiếp lên Graph API.
    Không phụ thuộc việc Facebook có lấy được URL gốc hay không.
    """
    url_source = image_url
    try:
        resp = requests.get(url_source, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print("DOWNLOAD IMG ERROR:", e, "URL:", url_source)
        return

    files = {
        "filedata": ("image.jpg", resp.content, "image/jpeg")
    }
    params = {
        "access_token": PAGE_ACCESS_TOKEN
    }
    data = {
        "recipient": json.dumps({"id": uid}, ensure_ascii=False),
        "message": json.dumps({
            "attachment": {
                "type": "image",
                "payload": {}
            }
        }, ensure_ascii=False),
        "messaging_type": "RESPONSE",
    }

    try:
        r = requests.post(
            "https://graph.facebook.com/v18.0/me/messages",
            params=params,
            data=data,
            files=files,
            timeout=30
        )
        print("SEND IMG:", r.status_code, r.text)
    except Exception as e:
        print("SEND IMG ERROR:", e)


# ============================================
# CAROUSEL TEMPLATE (MỚI THÊM)
# ============================================

def send_carousel_template(recipient_id: str, products_data: list) -> None:
    """
    Gửi carousel template với danh sách sản phẩm
    products_data: list of dict với keys: code, name, price, desc, image_url
    """
    try:
        # Tạo các element cho carousel
        elements = []
        for product in products_data[:10]:  # Facebook giới hạn 10 element
            # Lấy ảnh đầu tiên từ field Images
            image_field = product.get("Images", "")
            image_urls = parse_image_urls(image_field)
            image_url = image_urls[0] if image_urls else ""
            
            # Nếu không có ảnh, bỏ qua sản phẩm này
            if not image_url:
                continue
                
            element = {
                "title": f"[{product.get('MS', '')}] {product.get('Ten', '')}",
                "subtitle": f"💰 Giá: {product.get('Gia', '')}\n{product.get('MoTa', '')[:60]}..." if product.get('MoTa') else f"💰 Giá: {product.get('Gia', '')}",
                "image_url": image_url,
                "buttons": [
                    {
                        "type": "postback",
                        "title": "📋 Xem chi tiết",
                        "payload": f"VIEW_{product.get('MS', '')}"
                    },
                    {
                        "type": "postback",
                        "title": "🛒 Chọn sản phẩm",
                        "payload": f"SELECT_{product.get('MS', '')}"
                    }
                ]
            }
            elements.append(element)
        
        if not elements:
            print("Không có sản phẩm nào có ảnh để hiển thị trong carousel")
            return
        
        # Tạo payload carousel
        url = "https://graph.facebook.com/v18.0/me/messages"
        params = {"access_token": PAGE_ACCESS_TOKEN}
        payload = {
            "recipient": {"id": recipient_id},
            "message": {
                "attachment": {
                    "type": "template",
                    "payload": {
                        "template_type": "generic",
                        "elements": elements
                    }
                }
            },
            "messaging_type": "RESPONSE"
        }
        
        r = requests.post(url, params=params, json=payload, timeout=15)
        print("SEND CAROUSEL:", r.status_code, r.text)
        
    except Exception as e:
        print("SEND CAROUSEL ERROR:", e)


def send_product_carousel(recipient_id: str) -> None:
    """
    Gửi 5 sản phẩm đầu tiên dưới dạng Carousel Template
    """
    load_products()
    if not PRODUCTS:
        return
    
    # Lấy 5 sản phẩm đầu tiên
    products = list(PRODUCTS.values())[:5]
    
    # Gửi carousel
    send_carousel_template(recipient_id, products)


# ============================================
# REHOST IMAGE (freeimage.host - tuỳ chọn)
# ============================================

def rehost_image(url: str) -> str:
    if not FREEIMAGE_API_KEY:
        return url
    try:
        api = "https://freeimage.host/api/1/upload"
        payload = {
            "key": FREEIMAGE_API_KEY,
            "source": url,
            "action": "upload",
        }
        r = requests.post(api, data=payload, timeout=30)
        data = r.json()
        if "image" in data and "url" in data["image"]:
            return data["image"]["url"]
        return url
    except Exception as e:
        print("REHOST ERROR:", e)
        return url


# ============================================
# LOAD SẢN PHẨM TỪ SHEET
# ============================================

def load_products(force: bool = False) -> None:
    """
    Đọc CSV từ SHEET_CSV_URL với các cột:
      - Mã sản phẩm
      - Tên sản phẩm
      - Images
      - Videos
      - Tồn kho
      - Giá bán
      - Mô tả
      - màu (Thuộc tính)
      - size (Thuộc tính)
    """
    global PRODUCTS, LAST_LOAD

    now = time.time()
    if not force and PRODUCTS and now - LAST_LOAD < LOAD_TTL:
        return

    if not SHEET_URL:
        print("❌ SHEET_CSV_URL chưa cấu hình")
        PRODUCTS = {}
        return

    print("🟦 Loading sheet:", SHEET_URL)

    try:
        resp = requests.get(SHEET_URL, timeout=30)
        resp.raise_for_status()

        csv_text = resp.content.decode("utf-8", errors="replace")
        lines = csv_text.splitlines()
        reader = csv.DictReader(lines)

        products = {}
        for raw_row in reader:
            row = dict(raw_row)

            ms = (row.get("Mã sản phẩm") or "").strip()
            if not ms:
                continue

            ten = (row.get("Tên sản phẩm") or "").strip()
            if not ten:
                continue

            gia = (row.get("Giá bán") or "").strip()
            images = (row.get("Images") or "").strip()
            videos = (row.get("Videos") or "").strip()
            tonkho = (row.get("Tồn kho") or "").strip()
            mota = (row.get("Mô tả") or "").strip()
            mau = (row.get("màu (Thuộc tính)") or "").strip()
            size = (row.get("size (Thuộc tính)") or "").strip()

            row["MS"] = ms
            row["Ten"] = ten
            row["Gia"] = gia
            row["MoTa"] = mota
            row["Images"] = images
            row["Videos"] = videos
            row["Tồn kho"] = tonkho
            row["màu (Thuộc tính)"] = mau
            row["size (Thuộc tính)"] = size

            products[ms] = row

        PRODUCTS = products
        LAST_LOAD = now
        print(f"📦 Loaded {len(PRODUCTS)} products.")
    except Exception as e:
        print("❌ load_products error:", e)
        PRODUCTS = {}


# ============================================
# IMAGE HELPER & GPT VISION
# ============================================

def parse_image_urls(images_field: str) -> list:
    if not images_field:
        return []
    parts = [u.strip() for u in images_field.split(",") if u.strip()]
    # loại trùng nhưng vẫn giữ thứ tự
    seen = set()
    result = []
    for u in parts:
        if u not in seen:
            seen.add(u)
            result.append(u)
    return result


def gpt_analyze_image(url: str):
    if not client:
        return None, None
    try:
        prompt = f"""
        Bạn là trợ lý bán hàng. Hãy mô tả sản phẩm trong ảnh
        và cố gắng tìm mã sản phẩm gần nhất trong danh sách:
        {', '.join(PRODUCTS.keys())}

        Trả về JSON dạng:
        {{
          "description": "...",
          "matched_ms": "MS000123" hoặc null
        }}
        """
        r = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Bạn là trợ lý bán hàng chuyên nghiệp."},
                {"role": "user", "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": url}},
                ]},
            ],
            temperature=0.3,
        )
        text = r.choices[0].message.content
        m = re.search(r"(MS\d+)", text)
        return (m.group(1) if m else None), text
    except Exception as e:
        print("Vision error:", e)
        return None, None


# ============================================
# MS DETECT & CONTEXT
# ============================================

def extract_ms(text: str):
    if not text:
        return None
    m = re.search(r"(MS\d+)", text, flags=re.I)
    return m.group(1).upper() if m else None


def extract_short_code(text: str):
    """
    Tìm pattern dạng 'mã 09', 'ma so 9', 'mã số 18'...
    Trả về phần số (ví dụ '09', '18').
    """
    if not text:
        return None
    lower = text.lower()
    m = re.search(r"mã\s*(?:số\s*)?(\d{1,3})", lower)
    if not m:
        m = re.search(r"ma\s*(?:so\s*)?(\d{1,3})", lower)
    if not m:
        return None
    return m.group(1)


def find_ms_by_short_code(code: str):
    """
    Map '09' -> mã trong PRODUCTS kết thúc bằng 09 / 009...
    Ví dụ: MS000009, MS009,...
    """
    if not code:
        return None
    code = code.lstrip("0") or code
    candidates = []
    for ms in PRODUCTS.keys():
        if not ms.upper().startswith("MS"):
            continue
        digits = re.sub(r"\D", "", ms)
        if digits.endswith(code):
            candidates.append(ms)

    if not candidates:
        return None

    candidates.sort(key=len, reverse=True)
    return candidates[0]


def resolve_best_ms(ctx: dict):
    """
    Ưu tiên mã sản phẩm theo thứ tự:
    1. Mã từ tin nhắn khách gửi gần nhất (last_ms) nếu nó tồn tại trong PRODUCTS
    2. Mã từ vision (nếu khách gửi ảnh)
    3. Mã từ inbox_entry_ms (từ comment/referral)
    4. Mã từ caption
    """
    # Ưu tiên last_ms nếu nó tồn tại trong danh sách sản phẩm
    if ctx.get("last_ms") and ctx["last_ms"] in PRODUCTS:
        return ctx["last_ms"]
    
    # Các nguồn khác
    for key in ["vision_ms", "inbox_entry_ms", "caption_ms"]:
        if ctx.get(key) and ctx[key] in PRODUCTS:
            return ctx[key]
    return None


# ============================================
# GPT CONTEXT ENGINE
# ============================================

def gpt_reply(history: list, product_row: dict | None):
    if not client:
        return "Dạ hệ thống AI đang bận, anh/chị chờ em 1 lát với ạ."

    sys = """
    Bạn là trợ lý bán hàng của shop quần áo.
    - Xưng "em", gọi khách là "anh/chị".
    - Trả lời ngắn gọn, lịch sự, dễ hiểu.
    - Không bịa đặt chất liệu/giá/ưu đãi nếu không có trong dữ liệu.
    - Nếu đã biết sản phẩm khách đang xem, hãy:
      + Tóm tắt mẫu, giá, ưu điểm.
      + Gợi ý size/màu phù hợp.
      + Hỏi thêm 1 câu để chốt (size, màu hoặc đặt hàng).
    - Nếu CHƯA biết sản phẩm:
      + Hỏi rõ nhu cầu (mục đích, dáng người, ngân sách).
      + Gợi ý hướng lựa chọn chung, không tự đặt mã.
    """

    if product_row:
        tonkho = product_row.get("Tồn kho", "")
        mau = product_row.get("màu (Thuộc tính)", "")
        size = product_row.get("size (Thuộc tính)", "")
        sys += (
            f"\nDữ liệu sản phẩm hiện tại:\n"
            f"- Tên: {product_row.get('Ten', '')}\n"
            f"- Mô tả: {product_row.get('MoTa', '')}\n"
            f"- Giá bán: {product_row.get('Gia', '')}\n"
            f"- Tồn kho: {tonkho}\n"
            f"- Màu: {mau}\n"
            f"- Size: {size}\n"
        )

    # giới hạn lịch sử ~10 turns
    if len(history) > 10:
        history = history[-10:]

    r = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "system", "content": sys}] + history,
        temperature=0.5,
    )
    return r.choices[0].message.content


# ============================================
# GỬI THÔNG TIN SẢN PHẨM
# ============================================

def build_product_info_text(ms: str, row: dict) -> str:
    ten = row.get("Ten", "")
    gia = row.get("Gia", "")
    mota = (row.get("MoTa", "") or "").strip()
    tonkho = row.get("Tồn kho", "")
    mau = row.get("màu (Thuộc tính)", "")
    size = row.get("size (Thuộc tính)", "")

    # Ưu điểm nổi bật: rút gọn mô tả
    highlight = mota
    if len(highlight) > 350:
        highlight = highlight[:330].rsplit(" ", 1)[0] + "..."

    text = f"[{ms}] {ten}\n"
    text += f"\n✨ Ưu điểm nổi bật:\n- {highlight}\n" if highlight else ""
    if mau or size:
        text += "\n🎨 Màu/Size:\n"
        if mau:
            text += f"- Màu: {mau}\n"
        if size:
            text += f"- Size: {size}\n"
    if gia:
        text += f"\n💰 Giá bán: {gia}\n"
    if tonkho:
        text += f"📦 Tồn kho: {tonkho}\n"
    text += "\n👉 Anh/chị xem giúp em mẫu này có hợp gu không, nếu ưng em tư vấn thêm màu/size và chốt đơn cho mình ạ. ❤️"
    return text


def send_product_info(uid: str, ms: str):
    load_products()
    ms = ms.upper()
    if ms not in PRODUCTS:
        send_message(uid, "Dạ em chưa tìm thấy mã này trong kho ạ, anh/chị gửi lại giúp em mã sản phẩm hoặc ảnh mẫu nhé.")
        return

    row = PRODUCTS[ms]
    info_text = build_product_info_text(ms, row)
    send_message(uid, info_text)

    # Gửi tất cả ảnh (loại trùng) – tối đa 5 ảnh
    images_field = row.get("Images", "")
    urls = parse_image_urls(images_field)
    urls = urls[:5]  # tránh spam
    for u in urls:
        final_url = rehost_image(u)
        send_image(uid, final_url)


def send_recommendations(uid: str):
    """
    Gửi 5 sản phẩm gợi ý khi khách chủ động inbox mà chưa có MS nào.
    """
    load_products()
    if not PRODUCTS:
        return

    prods = list(PRODUCTS.values())[:5]
    send_message(uid, "Em gửi anh/chị 5 mẫu đang được nhiều khách quan tâm, mình tham khảo thử ạ:")

    for row in prods:
        ms = row.get("MS", "")
        ten = row.get("Ten", "")
        gia = row.get("Gia", "")
        txt = f"- [{ms}] {ten}"
        if gia:
            txt += f" – Giá: {gia}"
        send_message(uid, txt)

        images_field = row.get("Images", "")
        urls = parse_image_urls(images_field)
        if urls:
            final_url = rehost_image(urls[0])
            send_image(uid, final_url)


# ============================================
# GREETING (SỬA ĐỔI: LUỒNG KHÁCH CHỦ ĐỘNG INBOX)
# ============================================

def maybe_greet(uid: str, ctx: dict, has_ms: bool):
    """
    Chào khách:
    - Nếu là luồng direct inbox (không có inbox_entry_ms từ Fchat/referral)
    - Chỉ chào 1 lần
    - Nếu ngay tin đầu đã có mã (vd: 'Mã 09') thì vẫn chào nhưng KHÔNG gửi 5 gợi ý
    """
    if ctx["greeted"]:
        return

    # Nếu có inbox_entry_ms -> luồng comment/referral, đã có tin nhắn Fchat chào trước -> bot không chào nữa
    if ctx.get("inbox_entry_ms"):
        return

    msg = (
        "Em chào anh/chị 😊\n"
        "Em là trợ lý chăm sóc khách hàng của shop, hỗ trợ anh/chị xem mẫu, tư vấn size và chốt đơn nhanh ạ."
    )
    send_message(uid, msg)
    ctx["greeted"] = True

    # SỬA ĐỔI CHÍNH Ở ĐÂY: Gửi carousel thay vì từng sản phẩm riêng lẻ
    if not has_ms and not ctx["carousel_sent"]:
        send_message(uid, "Em gửi anh/chị 5 mẫu đang được nhiều khách quan tâm, mình tham khảo thử ạ:")
        send_product_carousel(uid)  # THAY ĐỔI: Gửi carousel
        ctx["carousel_sent"] = True
        ctx["recommended_sent"] = True


# ============================================
# HANDLE IMAGE MESSAGE (LUỒNG GỬI ẢNH)
# ============================================

def handle_image(uid: str, image_url: str):
    load_products()
    ctx = USER_CONTEXT[uid]

    # Luồng gửi ảnh thường là khách chủ động -> cho phép chào
    if not ctx["greeted"] and not ctx.get("inbox_entry_ms"):
        maybe_greet(uid, ctx, has_ms=False)

    hosted = rehost_image(image_url)
    ms, desc = gpt_analyze_image(hosted)
    print("VISION RESULT:", ms, desc)

    if ms and ms in PRODUCTS:
        ctx["vision_ms"] = ms
        ctx["last_ms"] = ms
        ctx["product_info_sent_ms"] = ms

        send_message(uid, f"Dạ ảnh này giống mẫu [{ms}] của shop đó anh/chị, em gửi thông tin sản phẩm cho mình nhé. 💕")
        send_product_info(uid, ms)
    else:
        send_message(
            uid,
            "Dạ hình này hơi khó nhận mẫu chính xác ạ, anh/chị gửi giúp em caption hoặc mã sản phẩm để em kiểm tra cho chuẩn nhé.",
        )


# ============================================
# HANDLE TEXT MESSAGE (LUỒNG CHÍNH)
# ============================================

def handle_text(uid: str, text: str):
    """
    Flow:
    - COMMENT: Fchat auto msg → echo → bot lưu MS vào inbox_entry_ms
      → khi khách trả lời inbox: dùng MS đó → gửi thông tin sản phẩm → GPT tư vấn & chốt
    - REFERRAL (nhấn nút Inbox trên bài viết): có ref:MS → inbox_entry_ms → giống COMMENT
    - CHỦ ĐỘNG INBOX:
        + Tin đầu: greet + 5 sản phẩm gợi ý (nếu chưa có mã)
        + Khi khách gõ mã (đủ / 'Mã 09') → gửi thông tin sản phẩm → GPT tư vấn & chốt
    """
    load_products()
    ctx = USER_CONTEXT[uid]

    # 1. Cập nhật mã từ chính tin nhắn
    ms_from_text = extract_ms(text)
    if not ms_from_text:
        short = extract_short_code(text)
        if short:
            ms_from_text = find_ms_by_short_code(short)

    if ms_from_text:
        ctx["last_ms"] = ms_from_text

    # 2. MS tổng hợp từ nhiều nguồn (ƯU TIÊN last_ms nếu nó hợp lệ)
    ms = resolve_best_ms(ctx)

    # 3. Nếu là direct inbox (không có inbox_entry_ms) -> chào theo chuẩn
    maybe_greet(uid, ctx, has_ms=bool(ms))

    # 4. Nếu đã có MS nhưng chưa từng gửi thông tin sản phẩm -> gửi card sản phẩm trước
    if ms and ms in PRODUCTS and ctx.get("product_info_sent_ms") != ms:
        ctx["product_info_sent_ms"] = ms
        send_product_info(uid, ms)

    # 5. GPT tư vấn theo ngữ cảnh & sản phẩm (nếu có)
    ctx["history"].append({"role": "user", "content": text})

    product = PRODUCTS.get(ms) if ms and ms in PRODUCTS else None
    reply = gpt_reply(ctx["history"], product)
    ctx["history"].append({"role": "assistant", "content": reply})
    send_message(uid, reply)

    # 6. Nếu tin nhắn khách có ý định đặt hàng -> gửi CTA chốt đơn
    lower = text.lower()
    if ms and ms in PRODUCTS and any(kw in lower for kw in ORDER_KEYWORDS):
        send_message(
            uid,
            "Dạ anh/chị cho em xin họ tên, số điện thoại, địa chỉ cụ thể, màu và size muốn lấy, em lên đơn ngay cho mình ạ. ❤️",
        )


# ============================================
# ECHO & REF / FCHAT
# ============================================

def extract_ms_from_ref(ref: str | None):
    if not ref:
        return None
    return extract_ms(ref)


def handle_echo_outgoing(page_id: str, user_id: str, text: str):
    """
    Tin nhắn do PAGE / FCHAT gửi (is_echo = true).
    Bot không trả lời, chỉ dùng để lưu MS:
      - COMMENT flow: Fchat auto msg chứa [MS000046]...
    """
    if not user_id:
        return
    ms = extract_ms(text)
    if ms:
        ctx = USER_CONTEXT[user_id]
        ctx["inbox_entry_ms"] = ms
        ctx["last_ms"] = ms
        print(f"[ECHO] Ghi nhận mã từ page/Fchat cho user {user_id}: {ms}")


# ============================================
# WEBHOOK (SỬA ĐỔI: THÊM XỬ LÝ POSTBACK CAROUSEL)
# ============================================

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        if request.args.get("hub.verify_token") == VERIFY_TOKEN:
            return request.args.get("hub.challenge")
        return "Token không hợp lệ", 403

    data = request.get_json() or {}
    print("WEBHOOK:", json.dumps(data, ensure_ascii=False))

    for entry in data.get("entry", []):
        for ev in entry.get("messaging", []):
            sender_id = ev.get("sender", {}).get("id")
            recipient_id = ev.get("recipient", {}).get("id")

            if not sender_id:
                continue

            msg = ev.get("message", {}) or {}

            # 1) ECHO: tin nhắn do page/Fchat gửi
            if msg.get("is_echo"):
                text = msg.get("text") or ""
                handle_echo_outgoing(page_id=sender_id, user_id=recipient_id, text=text)
                continue

            # từ đây trở xuống: sender_id = user
            ctx = USER_CONTEXT[sender_id]

            # 2) POSTBACK HANDLER (MỚI THÊM: Xử lý khi khách bấm nút trong carousel)
            if "postback" in ev:
                payload = ev["postback"].get("payload")
                print(f"[POSTBACK] User {sender_id}: {payload}")
                
                # Xử lý postback từ carousel
                if payload and payload.startswith("VIEW_"):
                    product_code = payload.replace("VIEW_", "")
                    # Gửi thông tin sản phẩm chi tiết
                    if product_code in PRODUCTS:
                        ctx["last_ms"] = product_code
                        ctx["product_info_sent_ms"] = product_code
                        send_product_info(sender_id, product_code)
                    else:
                        send_message(sender_id, f"Dạ em không tìm thấy sản phẩm mã {product_code} ạ.")
                    return "ok"
                    
                elif payload and payload.startswith("SELECT_"):
                    product_code = payload.replace("SELECT_", "")
                    # Xử lý khi khách chọn sản phẩm
                    if product_code in PRODUCTS:
                        ctx["last_ms"] = product_code
                        ctx["product_info_sent_ms"] = product_code
                        
                        product_info = PRODUCTS[product_code]
                        response = f"""✅ Bạn đã chọn sản phẩm **{product_code}** - {product_info.get('Ten', '')}!

Vui lòng cho em biết:
1. Size bạn muốn đặt là gì?
2. Màu sắc bạn thích?
3. Số lượng cần mua?

Hoặc bạn có thể nhắn "Đặt hàng" để em hỗ trợ bạn hoàn tất đơn nhé! 🛍️"""
                        send_message(sender_id, response)
                    else:
                        send_message(sender_id, f"Dạ em không tìm thấy sản phẩm mã {product_code} ạ.")
                    return "ok"

                # Xử lý referral trong postback (nếu có) - GIỮ NGUYÊN
                ref = ev["postback"].get("referral", {}).get("ref")
                if ref:
                    ms_ref = extract_ms_from_ref(ref)
                    if ms_ref:
                        ctx["inbox_entry_ms"] = ms_ref
                        ctx["last_ms"] = ms_ref
                        print(f"[REF] Nhận mã từ referral: {ms_ref}")
                        
                        # Nếu là luồng referral, không chào
                        ctx["greeted"] = True
                        
                        # Gửi thông tin sản phẩm
                        send_product_info(sender_id, ms_ref)
                        return "ok"
                
                # Nếu postback không có ref hoặc payload không phải từ carousel
                if not ctx["greeted"]:
                    maybe_greet(sender_id, ctx, has_ms=False)
                send_message(sender_id, "Anh/chị cho em biết đang quan tâm mẫu nào hoặc gửi ảnh mẫu để em xem giúp ạ.")
                return "ok"

            # 3) REFERRAL (nhấn nút Inbox, hoặc quảng cáo Click-to-Message)
            ref = ev.get("referral", {}).get("ref") \
                or ev.get("postback", {}).get("referral", {}).get("ref")
            if ref:
                ms_ref = extract_ms_from_ref(ref)
                if ms_ref:
                    ctx["inbox_entry_ms"] = ms_ref
                    ctx["last_ms"] = ms_ref
                    print(f"[REF] Nhận mã từ referral: {ms_ref}")

            # 4) ATTACHMENTS → ảnh
            if "message" in ev and "attachments" in msg:
                for att in msg["attachments"]:
                    if att.get("type") == "image":
                        image_url = att["payload"].get("url")
                        if image_url:
                            handle_image(sender_id, image_url)
                            return "ok"
                continue

            # 5) TEXT
            if "message" in ev and "text" in msg:
                text = msg.get("text", "")
                handle_text(sender_id, text)
                return "ok"

    return "ok"


# ============================================
# ORDER FORM & API (GIỮ NGUYÊN CHO SAU NÀY DÙNG)
# ============================================

def send_order_link(uid: str, ms: str):
    """
    Nếu sau này anh muốn dùng form, có thể gọi hàm này từ ORDER_KEYWORDS.
    Hiện tại mình đang dùng CTA hỏi thông tin trực tiếp.
    """
    base = DOMAIN or ""
    if base and not base.startswith("http"):
        base = "https://" + base
    url = f"{base}/o/{quote(ms)}"
    msg = f"Anh/chị có thể đặt hàng nhanh tại đây ạ: {url}"
    send_message(uid, msg)


@app.route("/o/<ms>")
def order_link(ms: str):
    load_products()
    ms = ms.upper()
    if ms not in PRODUCTS:
        return f"Không tìm thấy sản phẩm {ms}", 404
    pd_row = PRODUCTS[ms]
    ten = pd_row["Ten"]
    gia = pd_row["Gia"]
    mota = pd_row["MoTa"]
    return f"""
    <html><body>
    <h2>Đặt hàng {ms}</h2>
    <p><b>Tên:</b> {ten}</p>
    <p><b>Giá:</b> {gia}</p>
    <p><b>Mô tả:</b> {mota}</p>
    </body></html>
    """


@app.route("/order-form")
def order_form():
    return send_from_directory("static", "order-form.html")


@app.route("/api/get-product")
def api_get_product():
    load_products()
    ms = (request.args.get("ms") or "").upper()
    if ms not in PRODUCTS:
        return {"error": "not_found"}, 404

    row = PRODUCTS[ms]
    images_field = row.get("Images", "")
    urls = parse_image_urls(images_field)
    image = urls[0] if urls else ""

    return {
        "ms": ms,
        "name": row.get("Ten", ""),
        "price": row.get("Gia", ""),
        "desc": row.get("MoTa", ""),
        "image": image,
    }


@app.route("/api/order", methods=["POST"])
def api_order():
    data = request.json or {}
    print("ORDER RECEIVED:", data)

    uid = data.get("uid") or data.get("user_id")
    ms = (data.get("ms") or data.get("product_code") or "").upper()

    if uid:
        msg = (
            "✅ Shop đã nhận đơn của anh/chị ạ:\n"
            f"- Sản phẩm: {data.get('productName', '')} ({ms})\n"
            f"- Màu: {data.get('color', '')}\n"
            f"- Size: {data.get('size', '')}\n"
            f"- Số lượng: {data.get('quantity', '')}\n"
            f"- Thành tiền: {data.get('total', '')}\n"
            f"- Khách: {data.get('customerName', '')}\n"
            f"- SĐT: {data.get('phone', '')}\n"
            f"- Địa chỉ: {data.get('home', '')}, {data.get('ward', '')}, {data.get('district', '')}, {data.get('province', '')}\n\n"
            "Trong ít phút nữa bên em sẽ gọi xác nhận, anh/chị để ý điện thoại giúp em nha ❤️"
        )
        send_message(uid, msg)

    return {"status": "ok"}


# ============================================
# HEALTHCHECK & START
# ============================================

@app.route("/")
def home():
    load_products()
    return f"Chatbot OK – {len(PRODUCTS)} products loaded."


if __name__ == "__main__":
    load_products(force=True)
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
