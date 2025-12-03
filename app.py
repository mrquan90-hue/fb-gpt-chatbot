import os
import time
import re
import io
import requests
import pandas as pd
import openai
from openai import OpenAI
from flask import Flask, request

app = Flask(__name__)

# =========================
# 0. CẤU HÌNH
# =========================
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN", "")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "verify_token_123")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

BOT_ENABLED = True  # Có thể bật/tắt bot bằng lệnh trong Messenger

if OPENAI_API_KEY:
    client = OpenAI(api_key=OPENAI_API_KEY)
else:
    client = None


# =========================
# 1. HÀM GỬI TIN NHẮN FACEBOOK
# =========================
FB_API_URL = "https://graph.facebook.com/v18.0/me/messages"


def fb_send(payload):
    """
    Gửi POST lên Facebook Messenger.
    """
    if not PAGE_ACCESS_TOKEN:
        print("[fb_send] Thiếu PAGE_ACCESS_TOKEN, chỉ in payload:")
        print(payload)
        return

    params = {"access_token": PAGE_ACCESS_TOKEN}
    try:
        r = requests.post(FB_API_URL, params=params, json=payload, timeout=8)
        if r.status_code != 200:
            print("[fb_send] ERROR:", r.status_code, r.text)
    except Exception as e:
        print("[fb_send] EXCEPTION:", e)


def send_text(user_id, text):
    if not BOT_ENABLED:
        print("[TEXT] Bot OFF, skip:", text)
        return
    fb_send({
        "recipient": {"id": user_id},
        "message": {"text": text}
    })


def send_image(user_id, image_url, product_key=None):
    """
    Gửi ảnh. Đã có cơ chế chống trùng lặp theo (user, product_key, url).
    """
    if not BOT_ENABLED:
        print("[IMAGE] Bot OFF, skip image:", image_url)
        return

    if product_key:
        if not _mark_media_sent(user_id, "image", product_key, image_url):
            # đã gửi ảnh này cho user với sản phẩm này rồi
            return

    fb_send({
        "recipient": {"id": user_id},
        "message": {
            "attachment": {
                "type": "image",
                "payload": {"url": image_url, "is_reusable": True}
            }
        }
    })


def send_video(user_id, video_url, product_key=None):
    """
    Gửi video – cũng tránh trùng lặp theo (user, product_key, url).
    """
    if not BOT_ENABLED:
        print("[VIDEO] Bot OFF, skip video.")
        return

    if product_key:
        if not _mark_media_sent(user_id, "video", product_key, video_url):
            return

    fb_send({
        "recipient": {"id": user_id},
        "message": {
            "attachment": {
                "type": "video",
                "payload": {"url": video_url, "is_reusable": True}
            }
        }
    })


# =========================
# 2. LOAD GOOGLE SHEET
# =========================
SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv"

df = None
LAST_LOAD = 0
LOAD_TTL = 300  # 5 phút reload 1 lần


def load_sheet(force=False):
    """Tải data sản phẩm từ Google Sheet."""
    global df, LAST_LOAD
    now = time.time()
    if not force and df is not None and now - LAST_LOAD < LOAD_TTL:
        return
    try:
        print(f"[Sheet] Loading from: {SHEET_CSV_URL}")
        resp = requests.get(SHEET_CSV_URL, timeout=15)
        resp.raise_for_status()
        content = resp.content.decode("utf-8")
        df_local = pd.read_csv(io.StringIO(content))
        df_local.fillna("", inplace=True)
        df = df_local
        LAST_LOAD = now
        print("[Sheet] Loaded, rows =", len(df))
    except Exception as e:
        print("[Sheet] ERROR loading:", e)
        # không override df cũ nếu lỗi


# =========================
# 3. QUẢN LÝ CONTEXT & MEDIA
# =========================

# context: user_id -> {"current_ms": "MS000001", ...}
USER_CONTEXT = {}
# media_sent: (user_id, media_type, product_key, url) -> True
MEDIA_SENT_CACHE = {}


def get_user_context(user_id):
    return USER_CONTEXT.get(user_id, {})


def set_user_context(user_id, **kwargs):
    ctx = USER_CONTEXT.get(user_id, {})
    ctx.update(kwargs)
    USER_CONTEXT[user_id] = ctx
    return ctx


def _media_key(user_id, media_type, product_key, url):
    return f"{user_id}|{media_type}|{product_key}|{url}"


def _mark_media_sent(user_id, media_type, product_key, url):
    """
    Trả về True nếu chưa gửi, đánh dấu là đã gửi.
    Trả về False nếu đã gửi rồi (tránh trùng).
    """
    global MEDIA_SENT_CACHE
    key = _media_key(user_id, media_type, product_key, url)
    if key in MEDIA_SENT_CACHE:
        return False
    MEDIA_SENT_CACHE[key] = True
    return True


# =========================
# 4. ANTI-LOOP
# =========================
def is_echo_event(event):
    msg = event.get("message")
    return bool(msg and msg.get("is_echo"))


# =========================
# 5. XỬ LÝ SẢN PHẨM & GROUNDING
# =========================
def normalize_text(text: str) -> str:
    return (text or "").strip().lower()


def extract_ms_from_text(text):
    """
    Tìm mã sản phẩm dạng MSxxxx trong câu chat (hỗ trợ cả viết liền, viết cách, ms123, MS 123...).
    Trả về mã chuẩn dạng MS000123 nếu tìm được trong dữ liệu, ngược lại None.
    """
    if not text:
        return None
    raw = str(text).upper()
    # Bắt các pattern có MS trước dãy số: MS123, MS 123, M S123...
    m = re.search(r"MS\s*(\d{3,})", raw)
    if m:
        digits = m.group(1)
        code = "MS" + digits.zfill(6)
        return code
    return None


def guess_ms_from_text(text):
    """
    Đoán mã sản phẩm khi khách chỉ gõ số hoặc gõ 'mã 123', 'mã sp 123'...
    Chỉ trả về nếu tìm được đúng 1 mã trong dữ liệu, tránh đoán bừa.
    """
    if not text:
        return None
    load_sheet()
    global df
    if df is None or "Mã sản phẩm" not in df.columns:
        return None

    raw = str(text).upper()
    # Ưu tiên bắt theo cụm 'MÃ', 'MA ', 'MÃ SP', 'MA SP' đi kèm số
    candidates = []

    # 1. Pattern có chữ "MÃ"
    for m in re.finditer(r"M[ÃA]?\s*(SP)?\s*(\d{3,})", raw):
        digits = m.group(2)
        if not digits:
            continue
        padded = digits.zfill(6)
        code = "MS" + padded
        candidates.append(code)

    # 2. Nếu không có 'mã', bắt mọi cụm số 3–6 chữ số trong câu
    if not candidates:
        nums = re.findall(r"\d{3,6}", raw)
        for digits in nums:
            padded = digits.zfill(6)
            code = "MS" + padded
            candidates.append(code)

    # Lọc theo dữ liệu thực tế
    valid_codes = []
    for code in candidates:
        mask = df["Mã sản phẩm"].astype(str).str.contains(code, case=False, na=False)
        if mask.any():
            valid_codes.append(code)

    # Loại bỏ trùng
    seen = set()
    unique_valid = []
    for c in valid_codes:
        if c not in seen:
            seen.add(c)
            unique_valid.append(c)

    if len(unique_valid) == 1:
        return unique_valid[0]
    return None


def find_product_by_code(ms_code):
    if df is None or "Mã sản phẩm" not in df.columns:
        return None
    subset = df[df["Mã sản phẩm"].astype(str).str.contains(ms_code, case=False, na=False)]
    if subset.empty:
        return None
    return subset


def search_products_by_text(query, limit=5):
    """
    Tìm sản phẩm theo nội dung khách gõ, dùng cột 'Tên sản phẩm' và 'Mô tả'.
    """
    if df is None:
        return None
    q = normalize_text(query)
    if not q:
        return None
    # đơn giản: chứa chuỗi q trong tên / mô tả
    name_col = "Tên sản phẩm"
    desc_col = "Mô tả"
    if name_col not in df.columns:
        return None
    name_mask = df[name_col].astype(str).str.lower().str.contains(q, na=False)
    if desc_col in df.columns:
        desc_mask = df[desc_col].astype(str).str.lower().str.contains(q, na=False)
    else:
        desc_mask = False
    mask = name_mask | desc_mask
    subset = df[mask]
    if subset.empty:
        return None
    return subset.head(limit)


def format_price(v):
    try:
        p = float(v)
        return f"{p:,.0f}đ".replace(",", ".")
    except Exception:
        return str(v)


def answer_stock(rows, ms_code):
    """
    Trả lời còn/hết hàng dựa vào cột 'Có thể bán'.
    """
    if "Có thể bán" not in rows.columns:
        return f"Hiện em chưa có dữ liệu tồn kho chi tiết cho mã {ms_code}, anh/chị cho em xin số lượng cần, em nhờ nhân viên check lại ạ."

    can_sell = rows["Có thể bán"].astype(str).str.strip().str.lower()
    if all(x in ["0", "false", "hết hàng", "het hang", "no"] for x in can_sell):
        return f"Mã {ms_code} hiện đang tạm hết hàng ạ. Anh/chị có thể tham khảo thêm mẫu khác bên em nhé."
    return f"Mã {ms_code} hiện vẫn còn hàng anh/chị nha. Anh/chị cần số lượng khoảng bao nhiêu ạ?"


def answer_price(rows, ms_code):
    """
    Trả lời về giá dựa vào cột 'Giá bán' và nhóm theo giá.
    """
    if "Giá bán" not in rows.columns:
        return f"Hiện em chưa có dữ liệu giá chi tiết cho mã {ms_code}, anh/chị cho em xin số lượng cụ thể, em nhờ nhân viên báo giá chuẩn cho mình ạ."

    prices = rows["Giá bán"].astype(str).str.strip()
    unique_prices = sorted(set(prices))
    if not unique_prices:
        return f"Hiện em chưa có dữ liệu giá chi tiết cho mã {ms_code}, anh/chị cho em xin nhu cầu cụ thể, em nhờ nhân viên hỗ trợ thêm ạ."

    if len(unique_prices) == 1:
        p_str = format_price(unique_prices[0])
        return f"Mã {ms_code} hiện đang có giá khoảng {p_str} anh/chị nha."
    else:
        # nhóm theo giá + màu/size nếu có
        lines = [f"Mã {ms_code} hiện có một số mức giá tuỳ màu/size:"]
        prices_numeric = []
        for price in unique_prices:
            p_str = format_price(price)
            try:
                prices_numeric.append(float(price))
            except Exception:
                pass
            colors = []
            sizes = []
            sub = rows[rows["Giá bán"].astype(str).str.strip() == price]
            if "màu (Thuộc tính)" in sub.columns:
                colors = [c for c in sub["màu (Thuộc tính)"].fillna("").unique() if c]
            if "size (Thuộc tính)" in sub.columns:
                sizes = [s for s in sub["size (Thuộc tính)"].fillna("").unique() if s]
            seg = f"- {p_str}"
            extra = []
            if colors:
                extra.append("màu: " + ", ".join(colors))
            if sizes:
                extra.append("size: " + ", ".join(sizes))
            if extra:
                seg += " (" + "; ".join(extra) + ")"
            lines.append(seg)
        lines.append("Anh/chị cho em xin màu/size cụ thể để em chốt đúng giá giúp mình ạ.")
        return "\n".join(lines)


def get_clean_images(rows):
    """
    Lấy danh sách ảnh từ cột 'Images', loại trùng, loại watermark chữ Trung Quốc nhưng KHÔNG loại domain Trung Quốc.
    """
    if "Images" not in rows.columns:
        return []
    all_urls = []
    for cell in rows["Images"].fillna(""):
        parts = re.split(r"[\n,; ]+", str(cell))
        for p in parts:
            url = p.strip()
            if url.startswith("http"):
                all_urls.append(url)
    seen = set()
    clean = []
    for u in all_urls:
        if u not in seen:
            seen.add(u)
            # Loại watermark chữ Trung Quốc (ví dụ có 'watermark' hoặc text chèn chữ)
            # Ở đây ta chỉ minh họa bằng rule đơn giản: nếu url chứa 'watermark' thì bỏ
            if "watermark" in u.lower():
                continue
            clean.append(u)
    return clean


def get_videos(rows):
    """
    Lấy video từ cột 'Videos'.
    """
    if "Videos" not in rows.columns:
        return []
    all_urls = []
    for cell in rows["Videos"].fillna(""):
        parts = re.split(r"[\n,; ]+", str(cell))
        for p in parts:
            url = p.strip()
            if url.startswith("http"):
                all_urls.append(url)
    seen = set()
    clean = []
    for u in all_urls:
        if u not in seen:
            seen.add(u)
            clean.append(u)
    return clean


def answer_color_size(rows, ms_code):
    """
    Tư vấn màu & size từ cột 'màu (Thuộc tính)' và 'size (Thuộc tính)'.
    """
    colors = []
    sizes = []
    if "màu (Thuộc tính)" in rows.columns:
        colors = [c for c in rows["màu (Thuộc tính)"].fillna("").unique() if c]
    if "size (Thuộc tính)" in rows.columns:
        sizes = [s for s in rows["size (Thuộc tính)"].fillna("").unique() if s]

    lines = []
    if colors:
        lines.append("Màu hiện có: " + ", ".join(colors))
    if sizes:
        lines.append("Size hiện có: " + ", ".join(sizes))

    if not lines:
        return f"Mã {ms_code} hiện em chưa có dữ liệu màu/size chi tiết, anh/chị cho em thêm thông tin về chiều cao/cân nặng, em nhờ nhân viên hỗ trợ chọn size cho mình ạ."
    lines.append("Anh/chị cho em xin chiều cao, cân nặng (hoặc số đo quen mặc) để em tư vấn size chuẩn hơn cho mình nhé.")
    return "\n".join(lines)


def build_product_summary(rows, ms_code):
    """
    Gom thông tin mô tả sản phẩm để gửi vào GPT.
    """
    name = ""
    desc = ""
    if "Tên sản phẩm" in rows.columns:
        name = str(rows.iloc[0]["Tên sản phẩm"])
    if "Mô tả" in rows.columns:
        desc = str(rows.iloc[0]["Mô tả"])
    # Có thể mở rộng thêm: chất liệu, công suất, tính năng... nếu cột có
    summary = f"Mã: {ms_code}\nTên sản phẩm: {name}\nMô tả chi tiết:\n{desc}"
    return summary


# =========================
# 5.4. GPT CHO TƯ VẤN SẢN PHẨM
# =========================
SHOP_POLICIES_TEXT = """
- Hàng mới 100%, kiểm tra kỹ trước khi gửi.
- Hỗ trợ đổi size nếu còn hàng, sản phẩm còn nguyên tag, chưa qua sử dụng.
- Thời gian giao hàng tùy khu vực, thường sẽ được nhân viên tư vấn báo trước khi chốt đơn.
- Có hỗ trợ đồng kiểm (xem hàng bên ngoài trước khi thanh toán) tùy khu vực và đơn vị vận chuyển.
"""


SYSTEM_INSTRUCTION = """
Bạn là trợ lý bán hàng online nói tiếng Việt, tư vấn qua Facebook Messenger.

NGUYÊN TẮC BẮT BUỘC:
- Chỉ sử dụng thông tin trong 'product_data' và 'shop_policies'.
- KHÔNG được bịa đặt giá, tính năng, chất liệu, kích thước, thời gian giao hàng, chính sách đổi trả, bảo hành, quà tặng, giá sỉ...
- Nếu khách hỏi thông tin mà dữ liệu không có, hãy nói rõ là hệ thống chưa có thông tin chính xác và gợi ý khách để lại SĐT hoặc chờ nhân viên tư vấn thêm.
- Luôn trả lời NGẮN GỌN, TỰ NHIÊN, GIỐNG NGƯỜI THẬT BÁN HÀNG.
- Giữ giọng lịch sự, thân thiện, xưng hô "em" – "anh/chị" hoặc "cô/chú" tùy ngữ cảnh trong câu hỏi của khách.

VỀ SẢN PHẨM:
- Chỉ tư vấn dựa trên sản phẩm hiện tại (product_data['code']), không tự nhảy sang sản phẩm khác.
- Không đưa ra con số cụ thể về giá, kích thước, thời gian giao hàng nếu trong dữ liệu không có.
- Nếu product_data có nhiều mức giá, hãy giải thích ngắn gọn theo nhóm màu/size nếu có.
- Nếu khách hỏi về độ phù hợp (tuổi, dáng người, mục đích sử dụng...), hãy phân tích dựa trên mô tả, chất liệu, kiểu dáng trong product_data một cách hợp lý, nhưng KHÔNG được khẳng định những gì trái với dữ liệu.

VỀ VẬN CHUYỂN:
- Nếu dữ liệu không ghi rõ thời gian giao hàng hoặc phí ship, KHÔNG đoán số ngày cụ thể.
- Có thể trả lời kiểu: "Thời gian giao hàng và phí ship sẽ tuỳ khu vực, nhân viên sẽ báo chính xác cho anh/chị sau", KHÔNG đưa con số cứng.

VỀ NGỮ ĐIỆU:
- Trả lời ngắn gọn 2–5 câu, tránh dông dài.
- Có thể gợi ý khách hỏi tiếp về màu, size, giá, chất liệu, độ phù hợp...
"""


def call_gpt_for_product(user_message, product_summary, conversation_hint=""):
    """
    Gọi GPT để tư vấn thêm dựa trên product_summary.
    """
    if not OPENAI_API_KEY or client is None:
        # Không có key thì trả lời fallback
        return (
            "Hiện hệ thống AI đang tạm thời quá tải, anh/chị cho em xin mã sản phẩm và câu hỏi chi tiết, "
            "em sẽ nhờ nhân viên hỗ trợ thêm ạ."
        )

    messages = [
        {"role": "system", "content": SYSTEM_INSTRUCTION},
        {
            "role": "system",
            "content": "Dưới đây là chính sách tổng quát của shop (chỉ mang tính định hướng, không chứa con số cụ thể):\n"
                       + SHOP_POLICIES_TEXT
        },
        {
            "role": "system",
            "content": "Dưới đây là dữ liệu sản phẩm hiện tại (product_data):\n" + product_summary,
        },
        {
            "role": "system",
            "content": "Ngữ cảnh hội thoại hiện tại:\n" + conversation_hint,
        },
        {
            "role": "user",
            "content": user_message,
        },
    ]

    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=messages,
            temperature=0.4,
            max_tokens=350,
        )
        reply = resp.choices[0].message.content.strip()
        return reply
    except Exception as e:
        print("[GPT] ERROR:", e)
        return (
            "Hiện hệ thống AI đang có chút trục trặc, anh/chị cho em xin câu hỏi cụ thể, "
            "em nhờ nhân viên hỗ trợ thêm ạ."
        )


def consult_product_first_time(user_id, rows, ms_code, user_message=""):
    """
    Khi khách gửi mã sản phẩm lần đầu -> giới thiệu sản phẩm + gợi ý hỏi tiếp.
    """
    set_user_context(user_id, current_ms=ms_code)

    product_summary = build_product_summary(rows, ms_code)
    reply = call_gpt_for_product(
        user_message=user_message or f"Khách vừa hỏi về mã sản phẩm {ms_code}. Hãy giới thiệu ngắn gọn.",
        product_summary=product_summary,
        conversation_hint=(
            "Khách vừa gửi mã sản phẩm này, hãy giới thiệu ngắn gọn về sản phẩm và gợi ý khách "
            "hỏi thêm về màu, size, giá hoặc tính phù hợp."
        )
    )
    send_text(user_id, reply)

    # 4. Gửi tối đa 5 ảnh chung (1 lần duy nhất mỗi sản phẩm / user)
    imgs = get_clean_images(rows)
    for img in imgs[:5]:
        send_image(user_id, img, product_key=ms_code)
        time.sleep(0.3)


# =========================
# 5.5. RULE SHIP & INTENT HỖ TRỢ
# =========================

# Các cụm từ liên quan ship nhưng KHÔNG phải đặt hàng (chủ yếu hỏi freeship / phí ship)
SHIPPING_NEGATIVE = [
    "miễn ship", "mien ship", "free ship", "freeship",
    "phí ship", "phi ship", "tiền ship", "tien ship",
    "mien sip", "free sip"
]

# Các pattern thể hiện rõ ý "ship X cái/bộ/đôi" => chốt đơn
ORDER_SHIP_PATTERNS = [
    r"\bship\s*\d+",
    r"\bsip\s*\d+",
    r"\bsíp\s*\d+",
    r"\bship\b.*\b(cái|cai|bộ|bo|đôi|doi)\b",
    r"\bsip\b.*\b(cái|cai|bộ|bo|đôi|doi)\b",
    r"\bsíp\b.*\b(cái|cai|bộ|bo|đôi|doi)\b",
]


def is_order_ship(text: str) -> bool:
    """Trả về True nếu câu dạng 'ship 1 cái', 'sip 2 bo'... (chốt đơn theo ship).
    Không kích hoạt nếu câu đang hỏi freeship / phí ship (miễn ship, free ship...)."""
    if not text:
        return False
    lower = text.lower()
    for bad in SHIPPING_NEGATIVE:
        if bad in lower:
            return False
    for pattern in ORDER_SHIP_PATTERNS:
        if re.search(pattern, lower):
            return True
    return False


# =========================
# 6. WEBHOOK FACEBOOK
# =========================
@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    global BOT_ENABLED

    if request.method == "GET":
        # Xác minh webhook
        mode = request.args.get("hub.mode")
        token = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")

        if mode == "subscribe" and token == VERIFY_TOKEN:
            return challenge, 200
        return "Verification failed", 403

    # POST: xử lý message
    data = request.get_json()
    print("[Webhook] Received:", data)

    if "entry" not in data:
        return "ok", 200

    for entry in data["entry"]:
        messaging_events = entry.get("messaging", [])
        for event in messaging_events:
            sender_id = event["sender"]["id"]

            # 1. Bỏ qua echo của chính page
            if is_echo_event(event):
                print("[Echo] skip")
                continue

            # 2. Lệnh bật/tắt bot (admin gõ)
            if "message" in event and "text" in event["message"]:
                text = event["message"]["text"].strip()
            else:
                text = ""

            t_norm = normalize_text(text)

            # Admin command
            if t_norm == "tắt bot" or t_norm == "tat bot":
                BOT_ENABLED = False
                fb_send({
                    "recipient": {"id": sender_id},
                    "message": {"text": "❌ Bot đã tạm dừng trả lời tự động."}
                })
                print("[BOT] turned OFF by", sender_id)
                continue
            if t_norm == "bật bot" or t_norm == "bat bot":
                BOT_ENABLED = True
                fb_send({
                    "recipient": {"id": sender_id},
                    "message": {"text": "✅ Bot đã bật lại, sẵn sàng hỗ trợ khách."}
                })
                print("[BOT] turned ON by", sender_id)
                continue

            # 4. Nếu bot đang off -> im lặng
            if not BOT_ENABLED:
                print("[SKIP] bot is OFF, ignore message from", sender_id)
                continue

            # 5. Logic tư vấn
            load_sheet()

            if not text:
                send_text(sender_id, "Dạ em chưa nhận được nội dung, anh/chị nhắn lại giúp em ạ.")
                continue

            # Lấy context hiện tại (nếu có)
            ctx = get_user_context(sender_id)
            current_ms = ctx.get("current_ms") if ctx else None
            current_rows = find_product_by_code(current_ms) if current_ms else None

            # Kiểm tra xem khách có gửi mã mới không
            ms_code_in_text = extract_ms_from_text(text)
            # Nếu khách gõ sai / thiếu (vd: "ma 123", "mã sp 45", "123") thì thử đoán theo dữ liệu
            if not ms_code_in_text:
                ms_code_in_text = guess_ms_from_text(text)

            if ms_code_in_text:
                rows = find_product_by_code(ms_code_in_text)
                if rows is None or rows is False:
                    send_text(
                        sender_id,
                        f"Shop không tìm thấy sản phẩm với mã {ms_code_in_text}. Anh/chị kiểm tra lại giúp em nhé."
                    )
                else:
                    consult_product_first_time(sender_id, rows, ms_code_in_text, user_message=text)
                continue

            # Nếu có context sản phẩm hiện tại -> ưu tiên trả lời theo cột dữ liệu trước, rồi mới gọi GPT
            if current_ms and current_rows is not None:
                lower = t_norm
                handled = False

                # Ưu tiên: nếu khách dùng câu dạng "ship 1 cái", "sip 2 bo"… thì hiểu là đang chốt đơn
                if is_order_ship(text):
                    send_text(
                        sender_id,
                        "Dạ anh/chị muốn chốt đơn sản phẩm này ạ? "
                        "Anh/chị cho em xin SĐT, họ tên và địa chỉ nhận hàng để em tạo đơn nhé ❤️"
                    )
                    handled = True

                # 1. Còn hàng / hết hàng?
                stock_keywords = [
                    "còn hàng", "con hang", "hết hàng", "het hang",
                    "còn không", "con khong", "còn ko", "con ko", "còn k", "con k",
                    "có sẵn", "co san", "còn size", "con size"
                ]
                if not handled and any(k in lower for k in stock_keywords):
                    msg = answer_stock(current_rows, current_ms)
                    send_text(sender_id, msg)
                    handled = True

                # 2. Giá bao nhiêu?
                price_keywords = [
                    "giá", "gia", "bao nhiêu", "bao nhieu",
                    "nhiêu tiền", "nhieu tien", "bn", "bao nhieu 1", "bao nhieu 1 bo"
                ]
                if not handled and any(k in lower for k in price_keywords):
                    msg = answer_price(current_rows, current_ms)
                    send_text(sender_id, msg)
                    handled = True

                # 3. Ảnh sản phẩm
                image_keywords = [
                    "ảnh", "anh", "hình", "hinh", "hình ảnh", "hinh anh",
                    "xem mẫu", "xem mau", "xem hình", "xem hinh", "gửi ảnh", "gui anh"
                ]
                if not handled and any(k in lower for k in image_keywords):
                    imgs = get_clean_images(current_rows)
                    if not imgs:
                        send_text(
                            sender_id,
                            f"Dữ liệu sản phẩm mã {current_ms} hiện chưa có link ảnh để gửi trực tiếp. "
                            "Anh/chị giúp em chụp lại màn hình hoặc mô tả thêm để em hỗ trợ kỹ hơn ạ."
                        )
                    else:
                        send_text(
                            sender_id,
                            f"Em gửi anh/chị một số ảnh của sản phẩm mã {current_ms} để mình xem thêm ạ:"
                        )
                        for img in imgs[:5]:
                            send_image(sender_id, img, product_key=current_ms)
                            time.sleep(0.3)
                    handled = True

                # 4. Video sản phẩm
                video_keywords = ["video", "clip", "tiktok", "reels"]
                if not handled and any(k in lower for k in video_keywords):
                    vids = get_videos(current_rows)
                    if not vids:
                        send_text(
                            sender_id,
                            f"Mã {current_ms} hiện chưa có video sẵn. "
                            "Anh/chị có thể xem thêm ảnh hoặc em gửi mô tả chi tiết cho mình ạ."
                        )
                    else:
                        send_text(
                            sender_id,
                            f"Em gửi anh/chị video tham khảo thêm về sản phẩm mã {current_ms} ạ:"
                        )
                        for vurl in vids[:2]:
                            send_video(sender_id, vurl, product_key=current_ms)
                            time.sleep(0.3)
                    handled = True

                # 5. Hỏi về phí ship / freeship / thời gian giao hàng
                shipping_keywords = [
                    "miễn ship", "mien ship", "free ship", "freeship",
                    "phí ship", "phi ship", "tiền ship", "tien ship",
                    "ship bao lâu", "ship bao nhieu ngay", "ship mấy ngày", "ship may ngay",
                    "bao lâu nhận", "bao lau nhan", "thời gian vận chuyển", "thoi gian van chuyen",
                    "giao hàng mấy ngày", "giao hang may ngay", "ship nhanh không", "ship nhanh khong"
                ]
                if not handled and any(k in lower for k in shipping_keywords):
                    send_text(
                        sender_id,
                        "Dạ phí ship và thời gian giao hàng sẽ tuỳ khu vực và đơn vị vận chuyển ạ. "
                        "Anh/chị cho em xin khu vực nhận hàng (tỉnh/huyện) và nếu được thì cho em luôn số lượng để em nhờ nhân viên báo phí và thời gian dự kiến thật chính xác cho mình nhé ❤️"
                    )
                    handled = True

                # 6. Màu & size
                color_size_keywords = [
                    "màu", "mau", "màu sắc", "mau sac",
                    "size", "sai", "kích cỡ", "kich co", "kích thước", "kich thuoc"
                ]
                if not handled and any(k in lower for k in color_size_keywords):
                    msg = answer_color_size(current_rows, current_ms)
                    send_text(sender_id, msg)
                    handled = True

                # Nếu các case trên không trúng -> gọi GPT tư vấn theo mô tả / chất liệu / độ phù hợp...
                if not handled:
                    product_summary = build_product_summary(current_rows, current_ms)
                    reply = call_gpt_for_product(
                        user_message=text,
                        product_summary=product_summary,
                        conversation_hint=f"Hiện đang tư vấn sản phẩm mã {current_ms} cho khách."
                    )
                    send_text(sender_id, reply)

                continue

            # Nếu chưa có context sản phẩm nào -> thử search theo nội dung khách hỏi
            results = search_products_by_text(text, limit=5)
            if (
                results is not None
                and len(results) > 0
                and "Mã sản phẩm" in results.columns
                and "Tên sản phẩm" in results.columns
            ):
                lines = ["Em gợi ý một số sản phẩm phù hợp với anh/chị:"]
                for _, row in results.iterrows():
                    ms = str(row["Mã sản phẩm"])
                    name = str(row["Tên sản phẩm"])
                    lines.append(f"- [{ms}] {name}")
                lines.append(
                    "\nAnh/chị quan tâm mã nào, gửi giúp em mã (dạng MSxxxxx), em tư vấn chi tiết ạ."
                )
                send_text(sender_id, "\n".join(lines))
            else:
                send_text(
                    sender_id,
                    "Hiện tại em chưa xác định được sản phẩm anh/chị cần. "
                    "Anh/chị có thể gửi *mã sản phẩm* (MSxxxxx) hoặc mô tả rõ hơn tên sản phẩm/bài viết "
                    "anh/chị đang xem giúp em nhé."
                )

    return "ok", 200


@app.route("/")
def home():
    return "Chatbot running.", 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
