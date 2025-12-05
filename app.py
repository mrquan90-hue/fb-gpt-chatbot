import os
import re
import io
import time
import json
import base64
import random
from typing import Dict, Any, List, Tuple, Optional

import requests
import pandas as pd
from flask import Flask, request, send_from_directory, redirect
from openai import OpenAI

# ---------------------------------
# FLASK APP
# ---------------------------------
app = Flask(__name__, static_folder="static", static_url_path="/static")

# ---------------------------------
# CONFIG
# ---------------------------------
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN", "")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "verify_token_123")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
DOMAIN = os.getenv("DOMAIN", "fb-gpt-chatbot.onrender.com")

# Sheet URL
SHEET_URL = os.getenv(
    "SHEET_URL",
    "https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv",
)

# Freeimage.host API key (rehost ảnh cho Vision)
FREEIMAGE_API_KEY = os.getenv("FREEIMAGE_API_KEY", "").strip()

BOT_ENABLED = True

# OpenAI client
client: Optional[OpenAI] = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ---------------------------------
# GLOBAL STATE
# ---------------------------------
df: Optional[pd.DataFrame] = None
LAST_LOAD: float = 0.0
LOAD_TTL: int = 300  # 5 phút

USER_CONTEXT: Dict[str, Dict[str, Any]] = {}
LAST_MESSAGE_MID: Dict[str, str] = {}

# ---------------------------------
# UTILS & CONTEXT
# ---------------------------------
def normalize(t: Any) -> str:
    return str(t).strip().lower()


def ignore_event(ev: Dict[str, Any]) -> bool:
    if "delivery" in ev:
        print("[IGNORE] delivery")
        return True
    if "read" in ev:
        print("[IGNORE] read")
        return True
    # KHÔNG bỏ qua echo ở đây – echo xử lý riêng trong webhook
    return False


def get_ctx(uid: str) -> Dict[str, Any]:
    ctx = USER_CONTEXT.get(uid)
    if not ctx:
        ctx = {
            "current_ms": None,   # mã đang tư vấn
            "post_ms": None,      # mã từ bài viết / Fchat
            "introduced": False,  # đã chào chưa
            "history": [],        # [(role, text)]
        }
        USER_CONTEXT[uid] = ctx
    return ctx


def set_ctx(uid: str, **kwargs) -> Dict[str, Any]:
    ctx = get_ctx(uid)
    ctx.update(kwargs)
    USER_CONTEXT[uid] = ctx
    return ctx


# ---------------------------------
# SHEET LOADER
# ---------------------------------
def load_sheet(force: bool = False) -> None:
    global df, LAST_LOAD
    now = time.time()
    if not force and df is not None and now - LAST_LOAD < LOAD_TTL:
        return

    if not SHEET_URL:
        print("[load_sheet] MISSING SHEET_URL")
        return

    try:
        print("[load_sheet] Reloading sheet...")
        resp = requests.get(SHEET_URL, timeout=30)
        resp.raise_for_status()
        _df = pd.read_csv(io.StringIO(resp.text))
        _df.fillna("", inplace=True)
        if "Mã sản phẩm" not in _df.columns:
            print("[load_sheet] ERROR: missing column 'Mã sản phẩm'")
            return
        df = _df
        LAST_LOAD = now
        print(f"[load_sheet] Loaded {len(df)} rows.")
    except Exception as e:
        print("[load_sheet] ERROR:", e)


def find_product(ms: str) -> Optional[pd.DataFrame]:
    global df
    if df is None:
        load_sheet()
    if df is None:
        return None
    ms = str(ms).strip()
    if not ms:
        return None
    rows = df[df["Mã sản phẩm"].astype(str) == ms]
    if rows.empty:
        return None
    return rows


# ---------------------------------
# PRODUCT MATCHING
# ---------------------------------
STOPWORDS = {
    "cần", "can", "tư", "van", "tưvấn", "tuvan", "vấn",
    "shop", "mẫu", "mau", "quan", "tâm", "quan tâm",
    "giúp", "giup", "em", "anh", "chị", "ac", "ạ", "ạ!", "vs",
    "cho", "xem", "giùm", "gium", "mình", "minh", "giá", "gia",
    "mua", "đặt", "dat", "chốt", "chot", "bộ", "set", "áo", "quần",
}

MS_PATTERN = re.compile(r"\bMS\d{5,6}\b", re.IGNORECASE)


def guess_ms_by_content(text: str) -> Optional[str]:
    """
    Đoán mã sản phẩm theo nội dung mô tả (dùng cho Vision / direct inbox).
    """
    global df
    if df is None or not text:
        return None

    raw = normalize(text)
    tokens = re.findall(r"\w+", raw)
    tokens = [t for t in tokens if len(t) >= 3 and t not in STOPWORDS]
    if not tokens:
        return None

    best_ms = None
    best_score = 0

    for _, row in df.iterrows():
        ms_code = str(row.get("Mã sản phẩm", "")).strip()
        if not ms_code:
            continue
        st = normalize(f"{row.get('Tên sản phẩm', '')} {row.get('Mô tả', '')}")
        score = 0
        for t in tokens:
            if t in st:
                score += 1
        if score > best_score:
            best_score = score
            best_ms = ms_code

    # ngưỡng tối thiểu để coi là match
    if best_score < 2:
        return None
    return best_ms


def guess_ms(text: str) -> Optional[str]:
    """
    Đoán mã từ các dạng 'Mã 123', 'M SP 123', 'MÃ SP 123'...
    (coi là 'nhập mã' tương đối rõ ràng)
    """
    global df
    if df is None or not text:
        return None
    raw = text.upper()

    m = re.search(r"M[ÃA]?\s*(SP)?\s*(\d{3,})", raw)
    if m:
        num = m.group(2)
        if not num:
            return None
        code = "MS" + num.zfill(6)
        if code in df["Mã sản phẩm"].astype(str).values:
            return code

    nums = re.findall(r"\d{3,6}", raw)
    if len(nums) == 1:
        code = "MS" + nums[0].zfill(6)
        if code in df["Mã sản phẩm"].astype(str).values:
            return code

    return None


def extract_ms(text: str) -> Optional[str]:
    if not text:
        return None
    m = MS_PATTERN.search(text)
    if m:
        # chuẩn hóa về MS000000
        digits = re.sub(r"\D", "", m.group(0)[2:])
        return "MS" + digits.zfill(6)
    return None


def extract_ms_from_hashtag(text: str) -> Optional[str]:
    """
    Tìm mã sản phẩm dạng hashtag: #MS000123 hoặc [MS000123]
    """
    if not text:
        return None
    raw = text.upper()
    m = re.search(r"#MS(\d{1,6})", raw)
    if m:
        return "MS" + m.group(1).zfill(6)
    m2 = re.search(r"\[(MS\d{1,6})\]", raw)
    if m2:
        code = m2.group(1)
        digits = re.sub(r"\D", "", code[2:])
        return "MS" + digits.zfill(6)
    return None


def format_price(v: Any) -> str:
    try:
        return f"{float(v):,.0f}đ".replace(",", ".")
    except Exception:
        return str(v)
# ---------------------------------
# FB SEND
# ---------------------------------
def fb_send(payload: Dict[str, Any]) -> bool:
    if not PAGE_ACCESS_TOKEN:
        print("[fb_send] MISSING PAGE_ACCESS_TOKEN")
        print(payload)
        return False

    params = {"access_token": PAGE_ACCESS_TOKEN}
    try:
        r = requests.post(
            "https://graph.facebook.com/v19.0/me/messages",
            params=params,
            json=payload,
            timeout=15,
        )
        if r.status_code != 200:
            print("[fb_send] ERROR:", r.status_code, r.text)
            return False
        data = r.json()
        if "error" in data:
            print("[fb_send] FB ERROR:", data)
            return False
        return True
    except Exception as e:
        print("[fb_send] EXCEPTION:", e)
        return False


def send_text(uid: str, text: str) -> bool:
    print(f"[SEND_TEXT] -> {uid}: {text[:120]!r}")
    return fb_send(
        {
            "recipient": {"id": uid},
            "message": {"text": text},
            "messaging_type": "RESPONSE",
        }
    )


def send_image(uid: str, url: str) -> bool:
    return fb_send(
        {
            "recipient": {"id": uid},
            "message": {
                "attachment": {
                    "type": "image",
                    "payload": {"url": url, "is_reusable": True},
                }
            },
            "messaging_type": "RESPONSE",
        }
    )


def send_video(uid: str, url: str) -> bool:
    return fb_send(
        {
            "recipient": {"id": uid},
            "message": {
                "attachment": {
                    "type": "video",
                    "payload": {"url": url, "is_reusable": True},
                }
            },
            "messaging_type": "RESPONSE",
        }
    )


# ---------------------------------
# LINK ĐẶT HÀNG
# ---------------------------------
def send_order_link(uid: str, ms: str) -> None:
    short_url = f"https://{DOMAIN}/o/{ms}?uid={uid}"
    text = (
        "🛒💥 ĐẶT HÀNG NHANH (1 chạm):\n"
        f"👉 {short_url}\n\n"
        "Anh/chị bấm vào link, điền thông tin nhận hàng, "
        "shop sẽ gọi xác nhận đơn trong ít phút ạ ❤️"
    )
    send_text(uid, text)


# ---------------------------------
# GOOGLE SHEET HELPERS
# ---------------------------------
def build_summary(rows: pd.DataFrame, ms: str) -> str:
    row0 = rows.iloc[0]
    name = row0.get("Tên sản phẩm", "")
    desc = row0.get("Mô tả", "")
    price = format_price(row0.get("Giá bán", 0))
    return f"Mã: {ms}\nTên: {name}\nGiá bán: {price}\nMô tả:\n{desc}"


def clean_images(rows: pd.DataFrame) -> List[str]:
    urls: List[str] = []
    if "Images" not in rows.columns:
        return urls
    for cell in rows["Images"]:
        parts = re.split(r"[\n,; ]+", str(cell))
        for u in parts:
            u = u.strip()
            if u.startswith("http"):
                if "watermark" in u.lower():
                    continue
                if u not in urls:
                    urls.append(u)
    return urls


def intro_product(uid: str, rows: pd.DataFrame, ms: str, msg: str = "") -> None:
    """
    Giới thiệu sản phẩm tổng quát khi mới nhận mã.
    """
    set_ctx(uid, current_ms=ms)
    summary = build_summary(rows, ms)
    reply = call_gpt_simple(
        user_msg=msg or f"Giới thiệu mã {ms}",
        product_summary=summary,
        hint=f"Khách vừa chọn mã sản phẩm {ms}.",
    )
    send_text(uid, reply)
    imgs = clean_images(rows)
    for img in imgs[:5]:
        send_image(uid, img)
        time.sleep(0.2)


# ---------------------------------
# SIMPLE GPT (FALLBACK)
# ---------------------------------
SYSTEM_INSTRUCT = """
Bạn là trợ lý bán hàng của shop, trả lời chính xác theo dữ liệu sản phẩm.
Không bịa, không thêm thông tin không có trong sheet.
Luôn xưng "em" và gọi khách là "anh/chị".
"""


def call_gpt_simple(user_msg: str, product_summary: str, hint: str = "") -> str:
    if not client:
        return "Dạ hiện hệ thống AI đang bận, anh/chị mô tả rõ hơn giúp em ạ."

    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            temperature=0.4,
            messages=[
                {"role": "system", "content": SYSTEM_INSTRUCT},
                {
                    "role": "system",
                    "content": "Dữ liệu sản phẩm:\n" + product_summary,
                },
                {"role": "system", "content": "Ngữ cảnh:\n" + hint},
                {"role": "user", "content": user_msg},
            ],
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print("[GPT SIMPLE ERROR]", e)
        return "Dạ em đang xử lý hơi chậm, anh/chị nhắn lại giúp em nội dung cần tư vấn với ạ."


# ---------------------------------
# GPT CONTEXT ENGINE (MAIN)
# ---------------------------------
def build_gpt_messages(
    uid: str, user_msg: str, rows: Optional[pd.DataFrame], ms: Optional[str]
) -> List[Dict[str, str]]:
    """
    Tạo messages gửi lên GPT, dùng toàn bộ ngữ cảnh hội thoại.
    """
    ctx = get_ctx(uid)
    history = ctx.get("history", [])
    post_ms = ctx.get("post_ms")

    hist_lines = [f"{role}: {msg}" for role, msg in history]
    hist_text = "\n".join(hist_lines)

    if rows is not None and ms:
        summary = build_summary(rows, ms)
    else:
        summary = "Chưa xác định được sản phẩm nào."

    current_ms = ms or "None"
    post_ms_str = post_ms or "None"

    system_msg = f"""
Bạn là trợ lý bán hàng của shop thời trang, xưng "em", gọi khách là "anh/chị".

YÊU CẦU BẮT BUỘC:
- Luôn xưng "em" và gọi khách là "anh/chị".
- Giọng lịch sự, chuyên nghiệp, ngắn gọn, dễ hiểu.
- Không bao giờ nói sai mã sản phẩm.
- Nếu đã có mã sản phẩm hiện tại (current_ms) thì coi như đang tư vấn đúng mẫu đó.
- Nếu current_ms rỗng nhưng post_ms có thì ưu tiên dùng post_ms.
- Không tự bịa thông tin ngoài dữ liệu sản phẩm.
- Không hỏi quá 1 câu ngược lại khách trong một lần trả lời.

Nếu current_ms = "None" và post_ms = "None":
- Xem như khách đang đến từ nút nhắn tin chung trên page, chưa chọn mã.
- Nhiệm vụ của em là: hỏi rõ nhu cầu (mục đích, kiểu dáng, size, ngân sách...),
  gợi ý 1–2 hướng lựa chọn chung (không nói mã cụ thể), và kết thúc bằng 1 câu hỏi
  để khai thác thêm nhu cầu.
- Tuyệt đối không tự đặt tên/mã sản phẩm khi chưa có current_ms.

DỮ LIỆU SẢN PHẨM HIỆN TẠI:
- current_ms: {current_ms}
- post_ms: {post_ms_str}
- Tóm tắt:
{summary}

LỊCH SỬ HỘI THOẠI (gần nhất):
{hist_text}
""".strip()

    user_prompt = f"""
Tin nhắn mới nhất của khách:
"{user_msg}"

Nhiệm vụ của bạn:
1) Xác định đang tư vấn sản phẩm mã nào (final_ms). Nếu không có thì để null.
2) Đọc dữ liệu sản phẩm + lịch sử hội thoại để hiểu khách đang hỏi gì.
3) Soạn câu trả lời phù hợp, giọng "em – anh/chị", đúng ngữ cảnh.
4) Nếu đã có current_ms != null thì có thể đi sâu vào tư vấn chi tiết, chốt đơn.
5) Nếu chưa có current_ms (None) thì chỉ nên hỏi nhu cầu, gợi ý chung,
   KHÔNG nói mã cụ thể.

Trả về JSON thuần (không giải thích thêm), theo format:

{{
  "final_ms": "MS000123" hoặc null,
  "reply": "nội dung tin nhắn em sẽ gửi cho anh/chị"
}}
""".strip()

    return [
        {"role": "system", "content": system_msg},
        {"role": "user", "content": user_prompt},
    ]


def gpt_reply_for_user(uid: str, user_msg: str) -> Tuple[Optional[str], str]:
    """
    Gọi GPT để phân tích ngữ cảnh và sinh câu trả lời + mã sản phẩm cuối cùng.
    """
    if not client:
        return None, "Dạ hệ thống AI đang bận, anh/chị nhắn lại giúp em sau ít phút ạ."

    load_sheet()
    ctx = get_ctx(uid)
    ms = ctx.get("current_ms") or ctx.get("post_ms")

    rows = find_product(ms) if ms else None
    messages = build_gpt_messages(uid, user_msg, rows, ms)

    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            temperature=0.5,
            messages=messages,
        )
        content = resp.choices[0].message.content
        data = json.loads(content)
    except Exception as e:
        print("[GPT CONTEXT ERROR]", e)
        if rows is not None and ms:
            summary = build_summary(rows, ms)
            fallback = call_gpt_simple(user_msg, summary, hint=f"Đang tư vấn mã {ms}")
        else:
            fallback = (
                "Dạ em đang xử lý hơi chậm, anh/chị nhắn lại giúp em nội dung cần tư vấn với ạ."
            )
        return ms, fallback

    final_ms = data.get("final_ms")
    reply = (data.get("reply") or "").strip()
    if not reply:
        reply = "Dạ em đang xử lý hơi chậm, anh/chị nhắn lại giúp em nội dung cần tư vấn với ạ."

    return final_ms, reply


# ---------------------------------
# GPT VISION + FREEIMAGE.HOST
# ---------------------------------
def download_image(url: str) -> Optional[bytes]:
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        return r.content
    except Exception as e:
        print("[download_image] ERROR:", e)
        return None


def rehost_image_freeimage(image_bytes: bytes) -> Optional[str]:
    """
    Upload ảnh lên freeimage.host (cần FREEIMAGE_API_KEY).
    Trả về link công khai hoặc None nếu lỗi.
    """
    api_key = FREEIMAGE_API_KEY
    if not api_key:
        print("[rehost_image_freeimage] Missing FREEIMAGE_API_KEY")
        return None

    files = {"source": ("image.jpg", image_bytes)}
    data = {"key": api_key, "action": "upload"}

    try:
        r = requests.post("https://freeimage.host/api/1/upload", data=data, files=files, timeout=30)
        r.raise_for_status()
        js = r.json()
        link = js.get("image", {}).get("display_url")
        print("[rehost_image_freeimage] link:", link)
        return link
    except Exception as e:
        print("[rehost_image_freeimage] EXCEPTION:", e)
        return None


def call_gpt_vision_describe_image(public_url: str) -> Optional[str]:
    """
    Dùng GPT (vision) để mô tả ảnh sản phẩm.
    """
    if not client:
        return None
    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            temperature=0.3,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                "Mô tả ngắn gọn (bằng tiếng Việt) kiểu dáng, màu sắc, "
                                "phong cách của sản phẩm trong ảnh để em dùng tìm sản phẩm tương tự "
                                "trong kho hàng."
                            ),
                        },
                        {
                            "type": "image_url",
                            "image_url": {"url": public_url},
                        },
                    ],
                }
            ],
        )
        desc = resp.choices[0].message.content.strip()
        return desc
    except Exception as e:
        print("[GPT VISION ERROR]", e)
        return None


def analyze_image_and_find_ms(uid: str, image_url: str) -> Optional[str]:
    """
    Khi khách gửi ảnh, dùng proxy (freeimage.host) + Vision để tìm mã sản phẩm gần nhất.
    """
    load_sheet()
    img_bytes = download_image(image_url)
    if not img_bytes:
        return None

    public_url = rehost_image_freeimage(img_bytes)
    if not public_url:
        return None

    desc = call_gpt_vision_describe_image(public_url)
    if not desc:
        return None

    print("[VISION DESC]", desc)
    ms = guess_ms_by_content(desc)
    if ms:
        ctx = get_ctx(uid)
        ctx["current_ms"] = ms
        if not ctx.get("post_ms"):
            ctx["post_ms"] = ms
        USER_CONTEXT[uid] = ctx
    return ms
# ---------------------------------
# COMMENT AUTO REPLY
# ---------------------------------
def detect_comment_intent(message: str) -> str:
    t = normalize(message)
    if not t:
        return "other"

    if any(k in t for k in ["giá", "bao nhiêu", "bn", "nhiu"]):
        return "price"
    if any(k in t for k in ["size", "siz", "kg", "cân nặng", "ký"]):
        return "size"
    if any(k in t for k in ["màu", "mau", "mầu", "color"]):
        return "color"
    if any(k in t for k in ["ship", "giao", "vận chuyển", "gửi về"]):
        return "ship"
    if any(k in t for k in ["tư vấn", "tuvan", "hỗ trợ", "giúp em"]):
        return "consult"
    if any(k in t for k in ["mua", "chốt", "đặt hàng"]):
        return "order"
    return "other"


COMMENT_TEMPLATES = {
    "price": [
        "{name} ơi, em đã inbox báo giá chi tiết cho anh/chị rồi ạ, mình check Messenger giúp em nha ❤️",
    ],
    "size": [
        "{name} ơi, em đã inbox tư vấn size chuẩn theo chiều cao/cân nặng cho anh/chị rồi ạ. Anh/chị mở tin nhắn giúp em nha 😊",
    ],
    "color": [
        "{name} ơi, em gửi đủ các màu còn sẵn trong inbox cho anh/chị rồi ạ, mình xem giúp em nha 🎨",
    ],
    "ship": [
        "{name} ơi, em đã nhắn phí ship & thời gian nhận hàng cụ thể trong inbox cho anh/chị rồi ạ 🚚",
    ],
    "consult": [
        "{name} ơi, em đã inbox tư vấn chi tiết mẫu – giá – size – màu cho anh/chị rồi ạ 💬",
    ],
    "order": [
        "{name} ơi, em đã nhắn hướng dẫn đặt hàng nhanh trong inbox cho anh/chị rồi ạ ❤️",
    ],
    "other": [
        "{name} ơi, em đã gửi thông tin chi tiết trong inbox cho anh/chị rồi ạ, mình check Messenger giúp em nha 🥰",
    ],
}
LAST_COMMENT_TEMPLATE_IDX: Dict[str, Optional[int]] = {k: None for k in COMMENT_TEMPLATES.keys()}


def pick_comment_template(intent: str, name: str) -> str:
    if intent not in COMMENT_TEMPLATES:
        intent = "other"
    templates = COMMENT_TEMPLATES[intent]
    if not templates:
        return f"{name} ơi, em đã inbox cho anh/chị rồi ạ ❤️"

    last_idx = LAST_COMMENT_TEMPLATE_IDX.get(intent)
    idx = random.randint(0, len(templates) - 1)
    if last_idx is not None and len(templates) > 1 and idx == last_idx:
        idx = (idx + 1) % len(templates)
    LAST_COMMENT_TEMPLATE_IDX[intent] = idx

    name_display = name or "anh/chị"
    if not name_display.startswith("@"):
        name_display = f"@{name_display}"
    return templates[idx].format(name=name_display)


def fb_reply_comment(comment_id: str, text: str) -> bool:
    if not PAGE_ACCESS_TOKEN:
        print("[fb_reply_comment] missing PAGE_ACCESS_TOKEN")
        return False
    try:
        url = f"https://graph.facebook.com/v19.0/{comment_id}/comments"
        params = {"access_token": PAGE_ACCESS_TOKEN}
        data = {"message": text}
        r = requests.post(url, params=params, data=data, timeout=10)
        if r.status_code != 200:
            print("[fb_reply_comment] ERROR:", r.status_code, r.text)
            return False
        res = r.json()
        if "error" in res:
            print("[fb_reply_comment] FB ERROR:", res)
            return False
        return True
    except Exception as e:
        print("[fb_reply_comment] EXC:", e)
        return False


def handle_change(change: Dict[str, Any]) -> None:
    """
    Xử lý webhook dạng entry['changes'] cho sự kiện comment trên bài viết.
    Auto-reply comment theo intent (giá/size/màu/ship/tư vấn/khác).
    """
    try:
        field = change.get("field")
        if field != "feed":
            return

        value = change.get("value", {})
        if value.get("item") != "comment":
            return

        verb = value.get("verb")
        if verb not in ("add", "edited"):
            return

        comment_id = value.get("comment_id") or value.get("commentId")
        if not comment_id:
            return

        from_info = value.get("from", {}) or {}
        name = from_info.get("name") or "anh/chị"
        message = value.get("message", "") or value.get("message_text", "") or ""

        intent = detect_comment_intent(message)
        reply_text = pick_comment_template(intent, name)
        ok = fb_reply_comment(comment_id, reply_text)
        if ok:
            print(f"[COMMENT REPLY] {comment_id} intent={intent} msg='{message}' -> {reply_text}")
        else:
            print(f"[COMMENT REPLY] FAILED {comment_id}")
    except Exception as e:
        print("[handle_change] ERROR:", e)


# ---------------------------------
# HANDLE PAGE / FCHAT OUTGOING (ECHO)
# ---------------------------------
def handle_page_outgoing_message(uid: str, text: str) -> None:
    """
    Tin nhắn do PAGE/FCHAT gửi tới khách (echo).
    Bot chỉ dùng để cập nhật ngữ cảnh sản phẩm nếu có hashtag #MSxxxxx,
    KHÔNG trả lời lại tin này.
    """
    if not text:
        print("[PAGE MSG] empty")
        return

    ms = extract_ms_from_hashtag(text)
    if ms:
        print(f"[PAGE MSG] Detected product from echo: {ms}")
        ctx = get_ctx(uid)
        ctx["current_ms"] = ms
        if not ctx.get("post_ms"):
            ctx["post_ms"] = ms
        USER_CONTEXT[uid] = ctx
    else:
        print("[PAGE MSG] no product code in echo")


# ---------------------------------
# WEBHOOK CORE
# ---------------------------------
@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    global BOT_ENABLED

    if request.method == "GET":
        if request.args.get("hub.verify_token") == VERIFY_TOKEN:
            return request.args.get("hub.challenge")
        return "Verification failed", 403

    data = request.get_json() or {}

    for entry in data.get("entry", []):
        # 0. Comment
        for change in entry.get("changes", []):
            handle_change(change)

        # 1. Messaging
        for event in entry.get("messaging", []):
            message = event.get("message")

            is_echo = bool(message and message.get("is_echo"))
            if is_echo:
                user_id = event.get("recipient", {}).get("id")
            else:
                user_id = event.get("sender", {}).get("id")

            if not user_id:
                print("[WARN] Missing user_id in event:", event)
                continue

            # 1.1 Echo: update context, không trả lời
            if is_echo:
                text = message.get("text", "") or ""
                print(f"[ECHO] -> user {user_id}: {text}")
                handle_page_outgoing_message(user_id, text)
                continue

            # 1.2 Bỏ qua delivery/read
            if ignore_event(event):
                continue

            if not message:
                continue

            text = (message.get("text") or "").strip()
            attachments = message.get("attachments") or []
            lower = normalize(text) if text else ""

            mid = message.get("mid")
            if mid and LAST_MESSAGE_MID.get(user_id) == mid:
                print("[DUPLICATE] mid đã xử lý, bỏ qua.")
                continue
            if mid:
                LAST_MESSAGE_MID[user_id] = mid

            print(f"[MSG] from {user_id}: {text!r}")

            load_sheet()

            # Lệnh bật/tắt bot
            if text:
                if lower in ["tắt bot", "tat bot"]:
                    BOT_ENABLED = False
                    send_text(user_id, "Dạ em tạm dừng tư vấn tự động ạ.")
                    continue
                if lower in ["bật bot", "bat bot"]:
                    BOT_ENABLED = True
                    send_text(
                        user_id,
                        "Dạ em đã bật tư vấn tự động, anh/chị gửi giúp em mã hoặc mẫu cần xem ạ.",
                    )
                    continue

            if not BOT_ENABLED:
                continue

            # 1.3 Khách gửi ảnh KHÔNG có text
            if attachments and not text:
                image_url = None
                for att in attachments:
                    if att.get("type") == "image":
                        image_url = att.get("payload", {}).get("url")
                        if image_url:
                            break
                if not image_url:
                    print("[ATTACHMENT] Không tìm thấy ảnh hợp lệ.")
                    continue

                print("[IMAGE ONLY] from", user_id, image_url)
                ms_img = analyze_image_and_find_ms(user_id, image_url)
                if ms_img:
                    rows = find_product(ms_img)
                    if rows is not None:
                        intro_product(
                            user_id,
                            rows,
                            ms_img,
                            msg="Khách gửi ảnh sản phẩm, giới thiệu giúp em.",
                        )
                    else:
                        send_text(
                            user_id,
                            "Dạ em chưa tìm thấy sản phẩm giống hình anh/chị gửi ạ.",
                        )
                else:
                    # C1: không đoán bừa, xin khách gửi mã hoặc info rõ hơn
                    send_text(
                        user_id,
                        (
                            "Dạ em xem ảnh rồi nhưng chưa tìm thấy mã sản phẩm trong danh mục của shop ạ. "
                            "Anh/chị gửi giúp em mã sản phẩm hoặc caption/bài viết để em hỗ trợ nhanh nhất ạ."
                        ),
                    )
                continue

            # 1.4 Có text (có thể kèm ảnh)
            ctx = get_ctx(user_id)
            history = ctx.get("history", [])
            history.append(("user", text))
            ctx["history"] = history[-10:]
            USER_CONTEXT[user_id] = ctx

            current_ms = ctx.get("current_ms")
            print(f"[CTX] current_ms={current_ms}")

            # Ưu tiên cao nhất: khách gửi mã rõ ràng
            explicit_ms = (
                extract_ms_from_hashtag(text)
                or extract_ms(text)
                or guess_ms(text)
            )

            # Nếu chưa có mã và khách mô tả nhu cầu khá rõ -> thử đoán từ nội dung
            ms_by_pref = None
            if not explicit_ms and not current_ms and len(text) >= 10:
                ms_by_pref = guess_ms_by_content(text)

            chosen_ms = explicit_ms or ms_by_pref

            if chosen_ms:
                rows = find_product(chosen_ms)
                if rows is None:
                    send_text(user_id, f"Dạ em không tìm thấy sản phẩm {chosen_ms} ạ.")
                else:
                    set_ctx(user_id, current_ms=chosen_ms)
                    intro_product(user_id, rows, chosen_ms, msg=text)
                continue

            # GPT phân tích ngữ cảnh (không còn rule giá/ảnh/ship)
            final_ms, reply = gpt_reply_for_user(user_id, text)

            # Cập nhật context
            ctx = get_ctx(user_id)
            if final_ms:
                ctx["current_ms"] = final_ms
            USER_CONTEXT[user_id] = ctx

            # Greeting lần đầu
            if not ctx.get("introduced", False):
                intro = "Dạ em chào anh/chị ạ 😊 Em là trợ lý bán hàng của shop."
                full_reply = intro + "\n" + reply
                ctx["introduced"] = True
                USER_CONTEXT[user_id] = ctx
            else:
                full_reply = reply

            send_text(user_id, full_reply)

            # Lưu vào history
            ctx = get_ctx(user_id)
            h2 = ctx.get("history", [])
            h2.append(("assistant", full_reply))
            ctx["history"] = h2[-10:]
            USER_CONTEXT[user_id] = ctx

    return "ok", 200
# ---------------------------------
# SHORT LINK / ORDER FORM
# ---------------------------------
@app.route("/o/<ms>")
def short_order(ms: str):
    uid = request.args.get("uid", "")
    return redirect(f"/order-form?uid={uid}&ms={ms}")


@app.route("/order-form")
def order_form():
    return send_from_directory("static", "order-form.html")


# ---------------------------------
# API GET PRODUCT
# ---------------------------------
@app.route("/api/get-product")
def api_get_product():
    load_sheet()
    ms = request.args.get("ms", "")
    rows = find_product(ms)
    if rows is None:
        return {"error": "not_found"}

    row0 = rows.iloc[0]

    image = ""
    parts = re.split(r"[\s,;]+", str(row0.get("Images", "")))
    for u in parts:
        if u.startswith("http"):
            image = u
            break

    sizes = (
        rows["size (Thuộc tính)"].dropna().unique().tolist()
        if "size (Thuộc tính)" in rows.columns
        else []
    )
    colors = (
        rows["màu (Thuộc tính)"].dropna().unique().tolist()
        if "màu (Thuộc tính)" in rows.columns
        else []
    )

    return {
        "ms": ms,
        "name": row0.get("Tên sản phẩm", ""),
        "price": float(row0.get("Giá bán", 0) or 0),
        "desc": row0.get("Mô tả", ""),
        "image": image,
        "sizes": sizes,
        "colors": colors,
    }


# ---------------------------------
# API ORDER
# ---------------------------------
@app.route("/api/order", methods=["POST"])
def api_order():
    data = request.json or {}
    print("ORDER RECEIVED:", data)

    uid = data.get("uid") or data.get("user_id")
    ms_code = data.get("ms") or data.get("product_code")

    if uid:
        summary = (
            "✅ Shop đã nhận được đơn của anh/chị ạ:\n"
            f"- Sản phẩm: {data.get('productName', '')} ({ms_code})\n"
            f"- Màu: {data.get('color', '')}\n"
            f"- Size: {data.get('size', '')}\n"
            f"- Số lượng: {data.get('quantity', '')}\n"
            f"- Thành tiền: {data.get('total', '')}\n"
            f"- Khách: {data.get('customerName', '')}\n"
            f"- SĐT: {data.get('phone', '')}\n"
            f"- Địa chỉ: {data.get('home', '')}, "
            f"{data.get('ward', '')}, {data.get('district', '')}, {data.get('province', '')}\n\n"
            "Trong ít phút nữa bên em sẽ gọi xác nhận, anh/chị để ý điện thoại giúp em nha ❤️"
        )
        send_text(uid, summary)

    return {"status": "ok"}


# ---------------------------------
# ROOT
# ---------------------------------
@app.route("/")
def home():
    return "Chatbot running OK", 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
