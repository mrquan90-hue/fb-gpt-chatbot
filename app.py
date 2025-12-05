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

PAGE_ID = None  # sẽ được ghi nhận tự động từ webhook

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
    "recommended_sent": False,     # đã gửi 5 sp gợi ý chưa
    "product_info_sent_ms": None,  # đã gửi info chi tiết mã nào
})

PRODUCTS = {}         # {MS000001: {row}}
PRODUCT_LIST = []      # list row gốc (nếu cần)
LAST_LOAD_TIME = 0.0   # timestamp lần load gần nhất
CSV_CACHE_PATH = "products_cache.csv"


# ============================================
# HELPER: LOAD SHEET
# ============================================

def download_sheet_to_cache():
    """
    Tải file CSV từ SHEET_URL (Google Sheets published CSV) về local,
    để tránh mỗi request đều phải kéo về.
    """
    global CSV_CACHE_PATH
    if not SHEET_URL:
        print("⚠️ SHEET_URL không được cấu hình.")
        return False

    try:
        print("⬇️ Đang tải CSV từ SHEET_URL...")
        resp = requests.get(SHEET_URL, timeout=30)
        resp.raise_for_status()
        with open(CSV_CACHE_PATH, "wb") as f:
            f.write(resp.content)
        print("✅ Đã tải CSV về products_cache.csv")
        return True
    except Exception as e:
        print("❌ Lỗi tải CSV:", e)
        return False


def load_products(force=False):
    """
    Đọc sản phẩm từ CSV cache vào PRODUCT.
    Chỉ reload mỗi 300s hoặc nếu force=True.
    """
    global LAST_LOAD_TIME, PRODUCTS, PRODUCT_LIST

    now = time.time()
    if not force and PRODUCTS and now - LAST_LOAD_TIME < 300:
        return

    if not os.path.exists(CSV_CACHE_PATH):
        ok = download_sheet_to_cache()
        if not ok:
            return

    PRODUCTS.clear()
    PRODUCT_LIST.clear()

    try:
        with open(CSV_CACHE_PATH, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = (row.get("Mã sản phẩm") or "").strip()
                if not code:
                    continue
                ms = code
                PRODUCTS[ms] = row
                PRODUCT_LIST.append(row)

        LAST_LOAD_TIME = now
        print(f"✅ Đã load {len(PRODUCTS)} sản phẩm từ CSV.")
    except Exception as e:
        print("❌ Lỗi đọc CSV:", e)


# ============================================
# HELPER: FACEBOOK SEND API
# ============================================

def call_send_api(payload: dict):
    url = f"https://graph.facebook.com/v18.0/me/messages?access_token={PAGE_ACCESS_TOKEN}"
    try:
        resp = requests.post(url, json=payload, timeout=15)
        data = resp.json()
        print("SEND_API_RESP:", data)
        if resp.status_code != 200:
            print("❌ Send API error:", resp.status_code, data)
    except Exception as e:
        print("❌ Send API exception:", e)


def send_message(uid: str, text: str):
    if not uid or not text:
        return
    payload = {
        "recipient": {"id": uid},
        "message": {"text": text},
        "messaging_type": "RESPONSE",
    }
    call_send_api(payload)


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
            files=files,
            data=data,
            timeout=30,
        )
        print("send_image RESP:", r.status_code, r.text)
    except Exception as e:
        print("❌ send_image exception:", e)


# ============================================
# HELPER: REHOST IMAGE (FREEIMAGE)
# ============================================

def rehost_image(url: str) -> str:
    """
    Rehost ảnh sang freeimage.host để giảm khả năng bị chặn.
    Nếu lỗi thì trả về url gốc.
    """
    if not FREEIMAGE_API_KEY or not url:
        return url
    try:
        api = "https://freeimage.host/api/1/upload"
        payload = {
            "key": FREEIMAGE_API_KEY,
            "source": url,
            "format": "json",
        }
        r = requests.post(api, data=payload, timeout=20)
        data = r.json()
        if data.get("status_code") == 200:
            new_url = data["image"]["url"]
            print("Rehost OK:", url, "->", new_url)
            return new_url
        else:
            print("Rehost FAIL:", data)
            return url
    except Exception as e:
        print("Rehost exception:", e)
        return url


def parse_image_urls(field: str):
    """
    Tách cột Images thành list URL, loại trùng, loại rỗng.
    """
    if not field:
        return []
    parts = re.split(r"[,\n]", field)
    seen = set()
    out = []
    for p in parts:
        u = p.strip()
        if not u:
            continue
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


# ============================================
# GPT TEXT CHAT (CONTEXT ENGINE)
# ============================================

def gpt_reply(context_messages):
    if not client:
        return "Hiện tại em chưa kết nối được GPT, anh/chị chờ em kiểm tra lại một chút ạ."
    try:
        r = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=context_messages,
            temperature=0.4,
        )
        return r.choices[0].message.content
    except Exception as e:
        print("GPT error:", e)
        return "Hiện tại em đang lỗi kết nối một chút, anh/chị cho em xin lại câu hỏi hoặc đợi em ít phút nhé ạ."


# ============================================
# GPT VISION: PHÂN TÍCH ẢNH VÀ BẮT MÃ SẢN PHẨM
# ============================================

def gpt_analyze_image(url: str):
    """
    Phân tích ảnh bằng GPT-4.1 Vision:
    - Mô tả sản phẩm trong ảnh
    - Chọn mã sản phẩm (MSxxxx) gần nhất trong catalog hiện có
    """
    if not client or not PRODUCTS:
        return None, None
    try:
        # Chuẩn bị catalog dạng: "MS000001: Tên sản phẩm | mô tả ngắn"
        items = []
        # Giới hạn số lượng để tránh prompt quá dài (có thể chỉnh nếu cần)
        for ms, row in list(PRODUCTS.items())[:60]:
            name = (row.get("Ten") or row.get("Tên sản phẩm") or "").strip()
            desc = (row.get("MoTa") or row.get("Mô tả") or "").strip()
            if len(desc) > 120:
                desc = desc[:120] + "..."
            line = f"{ms}: {name}"
            if desc:
                line += f" | {desc}"
            items.append(line)
        catalog_text = "\n".join(items)

        prompt = f"""
        Bạn là trợ lý bán hàng của shop thời trang.

        Dưới đây là CATALOG sản phẩm (mỗi dòng gồm mã và tên sản phẩm):

        {catalog_text}

        Nhiệm vụ:
        1. Nhìn vào bức ảnh khách gửi (đính kèm bên dưới).
        2. So sánh với catalog và chọn ra sản phẩm giống nhất.
        3. Nếu không sản phẩm nào đủ giống, hãy trả về matched_ms = null.

        TRẢ VỀ DUY NHẤT MỘT ĐOẠN JSON HỢP LỆ, dạng:

        {{
          "description": "mô tả ngắn gọn về sản phẩm trong ảnh",
          "matched_ms": "MS000123" hoặc null
        }}

        Lưu ý:
        - matched_ms PHẢI là một trong các mã có trong catalog phía trên.
        - Nếu không chắc chắn, hãy để matched_ms = null.
        """

        r = client.chat.completions.create(
            model="gpt-4.1",
            messages=[
                {"role": "system", "content": "Bạn là trợ lý bán hàng chuyên nghiệp, chuyên nhận diện sản phẩm từ hình ảnh và đối chiếu với catalog."},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": url}},
                    ],
                },
            ],
            temperature=0.1,
        )

        raw = r.choices[0].message.content.strip()
        # Cố gắng parse JSON trước
        try:
            data = json.loads(raw)
            desc = data.get("description") or raw
            ms = data.get("matched_ms")
            if isinstance(ms, str):
                ms = ms.upper()
            else:
                ms = None
            return ms, desc
        except Exception:
            # Nếu không parse được JSON thì fallback về regex
            m = re.search(r"(MS\\d+)", raw, flags=re.I)
            ms = m.group(1).upper() if m else None
            return ms, raw
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
    if m:
        return m.group(1)
    return None


INTENT_ORDER_KEYWORDS = [
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


def detect_order_intent(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    for kw in INTENT_ORDER_KEYWORDS:
        if kw in t:
            return True
    return False


def maybe_greet(uid: str, ctx: dict, has_ms: bool):
    """
    Chào khách 1 lần duy nhất per user.
    """
    if ctx["greeted"]:
        return

    if has_ms:
        msg = (
            "Em chào anh/chị ạ, em là trợ lý bán hàng online của shop. "
            "Em thấy anh/chị đang quan tâm mẫu của shop, em hỗ trợ tư vấn size, màu và chốt đơn cho mình nha. 💕"
        )
    else:
        msg = (
            "Em chào anh/chị ạ, em là trợ lý bán hàng online của shop. "
            "Anh/chị gửi giúp em mã sản phẩm (ví dụ: MS000012) hoặc hình mẫu/miêu tả sản phẩm, "
            "em tư vấn nhanh và báo giá chi tiết cho mình nhé. 💕"
        )
    send_message(uid, msg)
    ctx["greeted"] = True


# ============================================
# BUILD PRODUCT TEXT
# ============================================

def build_product_info_text(ms: str, row: dict) -> str:
    name = row.get("Tên sản phẩm") or row.get("Ten") or ""
    price = row.get("Giá bán") or row.get("Gia ban") or ""
    stock = row.get("Tồn kho") or row.get("Ton kho") or ""
    desc = row.get("Mô tả") or row.get("Mo ta") or ""
    color = row.get("màu (Thuộc tính)") or row.get("mau (Thuoc tinh)") or ""
    size = row.get("size (Thuộc tính)") or row.get("size (Thuoc tinh)") or ""

    text = f"📌 *{name}* ({ms})\n"
    if price:
        text += f"💰 Giá bán: {price} \n"
    if stock:
        text += f"📦 Tồn kho: {stock}\n"
    if color:
        text += f"🎨 Màu: {color}\n"
    if size:
        text += f"📏 Size: {size}\n"
    if desc:
        text += "\n✨ Mô tả:\n" + desc.strip() + "\n"

    text += (
        "\n👉 Nếu anh/chị ưng mẫu này, nhắn cho em: *'Đặt hàng nha'*, "
        "hoặc để lại giúp em: *Họ tên + SĐT + Địa chỉ + Màu + Size + Số lượng* để em lên đơn ạ."
    )
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

    # Gửi tất cả ảnh (loại trùng) – tối đa 10 ảnh
    images_field = row.get("Images", "")
    urls = parse_image_urls(images_field)
    urls = urls[:10]  # tránh spam
    for u in urls:
        final_url = rehost_image(u)
        send_image(uid, final_url)


def send_recommendations(uid: str):
    """
    Gửi 5 sản phẩm gợi ý khi khách chủ động inbox/gõ mã nhưng chưa rõ mẫu.
    """
    load_products()
    ctx = USER_CONTEXT[uid]
    if ctx["recommended_sent"]:
        return

    rows = list(PRODUCTS.items())[:5]
    if not rows:
        return

    msg = "Một vài mẫu hot bên em, anh/chị tham khảo thêm ạ:\n"
    for ms, row in rows:
        name = row.get("Tên sản phẩm") or row.get("Ten") or ""
        price = row.get("Giá bán") or ""
        msg += f"- [{ms}] {name}"
        if price:
            msg += f" — {price}"
        msg += "\n"

    msg += "\nAnh/chị có thể nhắn: *Mã 01, Mã 02...* hoặc gửi ảnh mẫu ưng ý, em tìm đúng sản phẩm cho mình ạ."
    send_message(uid, msg)
    ctx["recommended_sent"] = True


# ============================================
# HANDLE ECHO (PAGE/FCHAT OUTGOING)
# ============================================

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
# HANDLE IMAGE MESSAGE
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
            "Dạ hình này hơi khó nhận mẫu chính xác ạ, anh/chị gửi giúp em *mã sản phẩm* hoặc một ảnh rõ hơn/caption sản phẩm để em kiểm tra cho chuẩn nhé.",
        )


# ============================================
# HANDLE TEXT MESSAGE (LUỒNG CHÍNH)
# ============================================

def handle_text(uid: str, text: str):
    """
    Flow:
    - COMMENT: Fchat auto msg → echo → bot lưu MS vào inbox_entry_ms
      → khi khách trả lời inbox: dùng MS đó → gửi thông tin sản phẩm → GPT tư vấn & chốt đơn
    - REFERRAL: từ nút Inbox/quảng cáo → ref chứa MS → inbox_entry_ms
    - DIRECT INBOX: không có mã → gửi gợi ý 5 sản phẩm → hỏi nhu cầu → GPT gợi ý & tìm MS
    """
    load_products()
    ctx = USER_CONTEXT[uid]
    raw_text = text or ""
    lower = raw_text.lower().strip()

    # 1) Nếu khách gõ "mã 09" dạng short code
    short_code = extract_short_code(raw_text)
    if short_code:
        # Chuẩn hóa thành MS0000xx nếu có thể
        # Tìm trong PRODUCTS mã có phần đuôi trùng short_code
        candidate = None
        for ms in PRODUCTS.keys():
            if ms[-len(short_code):] == short_code:
                candidate = ms
                break
        if candidate:
            ctx["last_ms"] = candidate
            ctx["product_info_sent_ms"] = candidate
            maybe_greet(uid, ctx, has_ms=True)
            send_product_info(uid, candidate)
            return
        else:
            send_message(uid, "Em chưa tìm thấy mã này trong kho ạ, anh/chị gửi giúp em *mã đầy đủ* hoặc *ảnh mẫu* nhé.")
            return

    # 2) Nếu text có chứa MS đầy đủ
    ms = extract_ms(raw_text)
    if ms and ms in PRODUCTS:
        ctx["last_ms"] = ms
        ctx["product_info_sent_ms"] = ms
        maybe_greet(uid, ctx, has_ms=True)
        send_product_info(uid, ms)
        return

    # 3) Nếu không có mã nhưng đã có inbox_entry_ms (từ comment/Fchat)
    if not ms and ctx.get("inbox_entry_ms"):
        ms = ctx["inbox_entry_ms"]
        ctx["last_ms"] = ms
        # Nếu chưa gửi info sp thì gửi
        if ctx.get("product_info_sent_ms") != ms:
            maybe_greet(uid, ctx, has_ms=True)
            send_product_info(uid, ms)
        # Sau đó dùng GPT tư vấn tiếp dựa trên nội dung mới
        # → tiếp tục xuống phần GPT
    else:
        # 4) Nếu chưa có mã từ bất kỳ nguồn nào
        if not ctx["greeted"]:
            maybe_greet(uid, ctx, has_ms=False)
        if not ctx["recommended_sent"]:
            send_recommendations(uid)

    # 5) Xử lý intent đặt hàng
    is_order = detect_order_intent(raw_text)

    # 6) Chuẩn bị context cho GPT
    system_prompt = (
        "Bạn là trợ lý bán hàng online xưng 'em' với khách là 'anh/chị'. "
        "Nhiệm vụ:\n"
        "- Luôn giữ ngữ điệu thân thiện, ngắn gọn, dễ hiểu.\n"
        "- Nếu đã biết sản phẩm (có mã trong bối cảnh), hãy tư vấn đúng sản phẩm đó, không tự bịa thêm sản phẩm mới.\n"
        "- Nếu chưa rõ sản phẩm, hãy hỏi lại để làm rõ mẫu/màu/size trước khi chốt đơn.\n"
        "- Khi khách có ý định đặt hàng, hãy hướng dẫn khách cung cấp: Họ tên, SĐT, Địa chỉ, Màu, Size, Số lượng.\n"
        "- Không đưa ra thông tin giá, tồn kho nếu trong dữ liệu không có."
    )

    history = ctx["history"]
    messages = [{"role": "system", "content": system_prompt}]

    if history:
        messages.extend(history[-8:])

    # Thêm thông tin sản phẩm hiện tại (nếu có)
    active_ms = ctx.get("last_ms")
    if active_ms and active_ms in PRODUCTS:
        row = PRODUCTS[active_ms]
        prod_text = build_product_info_text(active_ms, row)
        messages.append({
            "role": "system",
            "content": f"Thông tin sản phẩm hiện tại (mã {active_ms}):\n{prod_text}"
        })

    messages.append({"role": "user", "content": raw_text})

    reply = gpt_reply(messages)

    # Lưu vào history
    history.append({"role": "user", "content": raw_text})
    history.append({"role": "assistant", "content": reply})
    ctx["history"] = history

    send_message(uid, reply)

    if is_order and active_ms:
        send_message(uid, "Nếu anh/chị muốn đặt luôn, em gửi link form đặt hàng để mình điền thông tin cho tiện ạ:")
        send_order_form_link(uid, active_ms)


# ============================================
# ORDER FORM LINK
# ============================================

def send_order_form_link(uid: str, ms: str):
    """
    Gửi link form đặt hàng, kèm theo mã sản phẩm.
    """
    if not DOMAIN:
        return
    url = f"https://{DOMAIN}/order_form?ms={quote(ms)}&uid={quote(uid)}"
    text = (
        "Anh/chị có thể bấm vào link sau để điền thông tin đặt hàng ạ:\n"
        f"{url}\n\n"
        "Sau khi anh/chị điền xong, bên em sẽ gọi xác nhận đơn trong thời gian sớm nhất ạ."
    )
    send_message(uid, text)


# ============================================
# WEBHOOK
# ============================================

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    global PAGE_ID
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

            # Ghi nhận PAGE_ID (id của page) cho các message đến từ khách
            if PAGE_ID is None and recipient_id:
                PAGE_ID = recipient_id

            # từ đây trở xuống: sender_id = user
            ctx = USER_CONTEXT[sender_id]

            # 2) REFERRAL (nhấn nút Inbox, hoặc quảng cáo Click-to-Message)
            ref = ev.get("referral", {}).get("ref") \
                or ev.get("postback", {}).get("referral", {}).get("ref")
            if ref:
                ms_ref = extract_ms(ref) or extract_ms_from_ref(ref)
                if ms_ref:
                    ctx["inbox_entry_ms"] = ms_ref
                    ctx["last_ms"] = ms_ref
                    print(f"[REF] Nhận mã từ referral: {ms_ref}")

            # 3) ATTACHMENTS → ảnh
            if "message" in ev and "attachments" in msg:
                # Chặn loop: nếu là ảnh do page/bot gửi ra thì bỏ qua
                if PAGE_ID and sender_id == PAGE_ID:
                    continue
                for att in msg["attachments"]:
                    if att.get("type") == "image":
                        image_url = att["payload"].get("url")
                        if image_url:
                            handle_image(sender_id, image_url)
                            return "ok"
                continue

            # 4) TEXT
            if "message" in ev and "text" in msg:
                text = msg.get("text", "")
                handle_text(sender_id, text)
                return "ok"

            # 5) POSTBACK (nút bấm mà không có ref)
            if "postback" in ev and not ref:
                maybe_greet(sender_id, ctx, has_ms=False)
                send_message(sender_id, "Anh/chị cho em biết đang quan tâm mẫu nào hoặc gửi ảnh mẫu để em xem giúp ạ.")
                return "ok"

    return "ok"


def extract_ms_from_ref(ref: str):
    """
    Parse ref dạng: POST_ID:xxx|MS:MS000123
    hoặc đơn giản là 'MS000123'
    """
    if not ref:
        return None
    ms = extract_ms(ref)
    if ms:
        return ms
    m = re.search(r"MS:(MS\d+)", ref, flags=re.I)
    return m.group(1).upper() if m else None


# ============================================
# ORDER FORM (FRONTEND)
# ============================================

@app.route("/order_form")
def order_form():
    ms = request.args.get("ms", "")
    uid = request.args.get("uid", "")
    return f"""
<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8" />
  <title>Form đặt hàng</title>
</head>
<body>
  <h1>Form đặt hàng</h1>
  <p>Mã sản phẩm: {ms}</p>
  <form method="POST" action="/submit_order">
    <input type="hidden" name="ms" value="{ms}" />
    <input type="hidden" name="uid" value="{uid}" />
    <div>
      <label>Họ và tên:</label>
      <input type="text" name="customerName" required />
    </div>
    <div>
      <label>Số điện thoại:</label>
      <input type="text" name="phone" required />
    </div>
    <div>
      <label>Địa chỉ:</label>
      <input type="text" name="home" required />
    </div>
    <div>
      <label>Tỉnh / Thành phố:</label>
      <input type="text" name="province" />
    </div>
    <div>
      <label>Quận / Huyện:</label>
      <input type="text" name="district" />
    </div>
    <div>
      <label>Phường / Xã:</label>
      <input type="text" name="ward" />
    </div>
    <div>
      <label>Màu:</label>
      <input type="text" name="color" />
    </div>
    <div>
      <label>Size:</label>
      <input type="text" name="size" />
    </div>
    <div>
      <label>Số lượng:</label>
      <input type="number" name="quantity" value="1" min="1" />
    </div>
    <div>
      <label>Ghi chú thêm:</label>
      <textarea name="note"></textarea>
    </div>
    <button type="submit">Gửi đơn</button>
  </form>
</body>
</html>
"""


@app.route("/submit_order", methods=["POST"])
def submit_order():
    data = request.form.to_dict()
    ms = data.get("ms", "")
    uid = data.get("uid", "")

    print("ORDER_SUBMIT:", json.dumps(data, ensure_ascii=False))

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

    return "Đã nhận đơn, cảm ơn anh/chị."


# ============================================
# HEALTH CHECK
# ============================================

@app.route("/")
def index():
    return "Chatbot FB + GPT đang chạy."


if __name__ == "__main__":
    load_products(force=True)
    app.run(host="0.0.0.0", port=10000, debug=True)
