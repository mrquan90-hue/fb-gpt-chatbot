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
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

if OPENAI_API_KEY:
    openai.api_key = OPENAI_API_KEY
    client = OpenAI(api_key=OPENAI_API_KEY)
else:
    client = OpenAI()

# URL Google Sheet CSV (bạn thay link thật của bạn vào đây)
SHEET_CSV_URL = os.getenv("SHEET_CSV_URL", "")

# =========================
# 1. BIẾN TOÀN CỤC
# =========================

BOT_ENABLED = True
PROCESSED_MIDS = set()
LAST_SENT_MEDIA = {}  # {user_id: set( "image:MSxxx|url", "video:MSxxx|url" )}
USER_CONTEXT = {}  # {user_id: {"current_ms": "MS000018", "last_ts": 123456}}

# =========================
# 2. LOAD GOOGLE SHEET
# =========================

df = None
LAST_LOAD = 0
LOAD_TTL = 300  # giây, 5 phút


def load_sheet(force=False):
    """Load CSV từ Google Sheet, cache trong 5 phút."""
    global df, LAST_LOAD
    now = time.time()
    if not force and df is not None and (now - LAST_LOAD) < LOAD_TTL:
        return df

    if not SHEET_CSV_URL:
        print("[WARN] SHEET_CSV_URL not set")
        return df

    try:
        resp = requests.get(SHEET_CSV_URL, timeout=20)
        resp.raise_for_status()
        content = resp.content.decode("utf-8", errors="ignore")
        df_local = pd.read_csv(io.StringIO(content))
        df = df_local
        LAST_LOAD = now
        print(f"[Sheet] Loaded {len(df)} rows")
    except Exception as e:
        print("[Sheet] ERROR loading:", e)
    return df


# =========================
# 3. HÀM GỬI MESSAGE FACEBOOK
# =========================

def fb_send(payload):
    if not PAGE_ACCESS_TOKEN:
        print("[FB_SEND] PAGE_ACCESS_TOKEN missing, payload:", payload)
        return

    try:
        r = requests.post(
            "https://graph.facebook.com/v19.0/me/messages",
            params={"access_token": PAGE_ACCESS_TOKEN},
            json=payload,
            timeout=20,
        )
        if r.status_code != 200:
            print("[FB_SEND] Error:", r.status_code, r.text)
    except Exception as e:
        print("[FB_SEND] Exception:", e)


def send_text(user_id, text):
    if not BOT_ENABLED:
        print("[BOT OFF] Not sending text.")
        return

    fb_send(
        {
            "recipient": {"id": user_id},
            "message": {"text": text},
        }
    )


def _mark_media_sent(user_id, media_type, product_key, url):
    """
    Lưu dấu media đã gửi để không gửi trùng (anti spam).
    media_type: "image" / "video"
    """
    if not product_key:
        return
    if user_id not in LAST_SENT_MEDIA:
        LAST_SENT_MEDIA[user_id] = set()
    key = f"{media_type}:{product_key}|{url}"
    if key in LAST_SENT_MEDIA[user_id]:
        print(f"[MEDIA] Skip duplicate {media_type}:", key)
        return False
    LAST_SENT_MEDIA[user_id].add(key)
    return True


def send_image(user_id, image_url, product_key=None):
    """
    Chỉ gửi 1 ảnh 1 lần cho mỗi (user, product_key, url).
    """
    if not BOT_ENABLED:
        print("[IMG] Bot OFF, skip image.")
        return

    if product_key:
        if not _mark_media_sent(user_id, "image", product_key, image_url):
            return

    fb_send(
        {
            "recipient": {"id": user_id},
            "message": {
                "attachment": {
                    "type": "image",
                    "payload": {"url": image_url, "is_reusable": True},
                }
            },
        }
    )


def send_video(user_id, video_url, product_key=None):
    """
    Gửi video, có chống trùng nếu có product_key.
    """
    if not BOT_ENABLED:
        print("[VIDEO] Bot OFF, skip video.")
        return

    if product_key:
        if not _mark_media_sent(user_id, "video", product_key, video_url):
            return

    fb_send(
        {
            "recipient": {"id": user_id},
            "message": {
                "attachment": {
                    "type": "video",
                    "payload": {"url": video_url, "is_reusable": True},
                }
            },
        }
    )


# =========================
# 4. XỬ LÝ DỮ LIỆU SẢN PHẨM
# =========================

def normalize_text(s):
    if not isinstance(s, str):
        s = str(s)
    s = s.lower()
    s = s.strip()
    return s


def extract_ms_from_text(text):
    """
    Tìm mã sản phẩm trong câu dạng MS000018, ms12345...
    """
    if not text:
        return None
    matches = re.findall(r"\bms\s*\d{3,}\b", text, flags=re.IGNORECASE)
    if not matches:
        return None
    raw = matches[0]
    digits = re.findall(r"\d+", raw)
    if not digits:
        return None
    return "MS" + digits[0].zfill(6)


def find_product_by_code(ms_code):
    """
    Tìm các dòng có mã sản phẩm trùng (1 sản phẩm nhiều biến thể).
    """
    load_sheet()
    global df
    if df is None or "Mã sản phẩm" not in df.columns:
        return None
    ms_code = str(ms_code).strip()
    rows = df[df["Mã sản phẩm"].astype(str).str.contains(ms_code, case=False, na=False)]
    if rows.empty:
        return None
    return rows


def search_products_by_text(query, limit=5):
    """
    Tìm sản phẩm theo tên/mô tả (search sơ bộ).
    """
    load_sheet()
    global df
    if df is None:
        return None
    if not query:
        return None
    q = normalize_text(query)
    if "Tên sản phẩm" not in df.columns:
        return None
    mask = df["Tên sản phẩm"].astype(str).str.lower().str.contains(q, na=False)
    results = df[mask].head(limit)
    if results.empty:
        return None
    results = results.drop_duplicates(subset=["Mã sản phẩm"])
    return results


def clean_image_url(url: str) -> str:
    """
    Loại bỏ ảnh watermark Trung Quốc theo tên file, KHÔNG loại domain TQ.
    """
    if not url:
        return ""
    url = url.strip()
    lower = url.lower()
    bad_keywords = ["watermark", "logo", "sample", "yangshi", "baidu", "zhanqi"]
    if any(bk in lower for bk in bad_keywords):
        return ""
    return url


def get_clean_images(rows):
    """
    Lấy danh sách ảnh (unique, không watermark chữ Trung Quốc).
    Cột: Images
    """
    if rows is None or "Images" not in rows.columns:
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
        cu = clean_image_url(u)
        if not cu:
            continue
        if cu not in seen:
            seen.add(cu)
            clean.append(cu)
    return clean


def get_clean_videos(rows):
    """
    Lấy danh sách video (unique) từ cột Videos.
    """
    if rows is None:
        return []
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


def format_price(p):
    """Chuẩn hóa giá dạng 1.234.567đ."""
    try:
        v = float(p)
        v_int = int(round(v))
        return f"{v_int:,}".replace(",", ".") + "đ"
    except Exception:
        return str(p)


def is_available_flag(v):
    """
    Cột 'Có thể bán' (hoặc tên tương đương), coi:
    - True / 1 / "có" / "yes" => còn bán
    - False / 0 / "không" / "het" => không bán
    """
    if isinstance(v, str):
        v = v.strip().lower()
        if v in ["1", "true", "có", "co", "yes"]:
            return True
        if v in ["0", "false", "không", "khong", "no", "het"]:
            return False
    try:
        return bool(int(v))
    except Exception:
        return False


def answer_stock(rows, ms_code):
    """
    Trả lời còn hàng hay không dựa cột 'Có thể bán'.
    """
    col_candidates = ["Có thể bán", "Co the ban", "Co_the_ban", "Available", "available"]
    col_name = None
    for c in col_candidates:
        if c in rows.columns:
            col_name = c
            break

    if not col_name:
        return f"Hiện hệ thống chưa có cột 'Có thể bán' cho mã {ms_code}, em chưa kiểm tra tồn kho tự động được ạ."

    total_variants = len(rows)
    available_count = 0
    for _, r in rows.iterrows():
        if is_available_flag(r.get(col_name)):
            available_count += 1

    if available_count <= 0:
        return f"Sản phẩm mã {ms_code} hiện trong dữ liệu đang hết hàng hoặc tạm ngừng bán ạ."

    if available_count == total_variants:
        return f"Sản phẩm mã {ms_code} hiện trong dữ liệu đều còn hàng cho các biến thể ạ."

    return (
        f"Sản phẩm mã {ms_code} hiện vẫn còn hàng với một số biến thể (màu/size). "
        "Anh/chị cho em xin màu và size cụ thể để em kiểm tra chính xác giúp mình nhé."
    )


def answer_price(rows, ms_code):
    """
    Gom theo giá bán, báo giá dạng:
    - Nếu chỉ 1 giá: báo 1 giá
    - Nếu nhiều giá: báo khoảng
    """
    if "Giá bán" not in rows.columns:
        return f"Hệ thống chưa có cột 'Giá bán' cho mã {ms_code}, em chưa báo giá tự động được ạ."

    prices = []
    for _, r in rows.iterrows():
        val = r.get("Giá bán")
        try:
            f = float(val)
            prices.append(f)
        except Exception:
            continue

    if not prices:
        return f"Hiện dữ liệu chưa có giá bán cụ thể cho mã {ms_code}, anh/chị vui lòng chờ nhân viên báo giá ạ."

    unique_prices = sorted(set(prices))
    if len(unique_prices) == 1:
        p = format_price(unique_prices[0])
        return f"Sản phẩm mã {ms_code} hiện giá bán là {p} ạ."
    else:
        p_min = format_price(min(unique_prices))
        p_max = format_price(max(unique_prices))
        return f"Sản phẩm mã {ms_code} hiện có nhiều mức giá, dao động từ {p_min} đến {p_max} tuỳ màu/size ạ."


def answer_color_size(rows, ms_code):
    """
    Trả lời danh sách màu/size còn trong dữ liệu.
    """
    color_cols = [c for c in rows.columns if "màu" in c.lower() or "mau" in c.lower()]
    size_cols = [c for c in rows.columns if "size" in c.lower() or "kich thuoc" in c.lower()]

    colors = set()
    sizes = set()

    for _, r in rows.iterrows():
        for c in color_cols:
            val = str(r.get(c, "")).strip()
            if val:
                colors.add(val)
        for c in size_cols:
            val = str(r.get(c, "")).strip()
            if val:
                sizes.add(val)

    msg_parts = [f"Sản phẩm mã {ms_code} trong dữ liệu có:"]

    if colors:
        msg_parts.append("• Màu: " + ", ".join(sorted(colors)))
    if sizes:
        msg_parts.append("• Size: " + ", ".join(sorted(sizes)))
    if not colors and not sizes:
        msg_parts.append("Hiện dữ liệu chưa ghi rõ màu/size cụ thể ạ.")

    msg_parts.append("Anh/chị cho em biết màu/size mình cần, em kiểm tra tồn kho giúp ạ.")
    return "\n".join(msg_parts)


def build_product_summary(rows, ms_code):
    """
    Tóm tắt dữ liệu sản phẩm để gửi vào GPT.
    """
    if rows is None or rows.empty:
        return {}

    # Lấy 1 dòng đại diện
    r0 = rows.iloc[0]
    name = str(r0.get("Tên sản phẩm", ""))
    desc = str(r0.get("Mô tả", ""))
    images = get_clean_images(rows)
    videos = get_clean_videos(rows)

    price_info = answer_price(rows, ms_code)
    stock_info = answer_stock(rows, ms_code)
    variant_info = answer_color_size(rows, ms_code)

    return {
        "code": ms_code,
        "name": name,
        "desc": desc,
        "price_info": price_info,
        "stock_info": stock_info,
        "variant_info": variant_info,
        "images": images,
        "videos": videos,
    }


# =========================
# 5. GPT TƯ VẤN
# =========================

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
- Nếu product_data có nhiều mức giá, hãy giải thích ngắn gọn theo nhóm màu/size nếu có.
- Nếu khách hỏi về độ phù hợp (tuổi, dáng người, mục đích sử dụng...), hãy dựa trên mô tả, chất liệu, danh mục để phân tích hợp lý, nhưng KHÔNG được khẳng định những gì trái với dữ liệu.

VỀ NGỮ ĐIỆU:
- Ưu tiên trả lời đúng trọng tâm câu hỏi của khách.
- Có thể gợi ý nhẹ nhàng thêm 1–2 ý nếu cần, nhưng không lan man.
- Cuối câu có thể kèm 1 câu CTA nhẹ: mời khách chọn màu/size, để lại SĐT, hoặc cho biết thêm nhu cầu.
"""


def call_gpt_for_product(user_message, product_summary, conversation_hint=None):
    """
    Gọi GPT để tư vấn như người thật, dựa trên dữ liệu sản phẩm.
    Không bịa thông tin.
    """
    if not OPENAI_API_KEY:
        # Không có key thì trả lời fallback
        return (
            "Hiện hệ thống AI chưa được cấu hình đầy đủ. "
            "Anh/chị cho em xin thêm thông tin hoặc chờ nhân viên hỗ trợ trực tiếp ạ."
        )

    product_data_str = f"""
Mã sản phẩm: {product_summary.get('code')}
Tên sản phẩm: {product_summary.get('name')}
Mô tả: {product_summary.get('desc')}
Thông tin giá: {product_summary.get('price_info')}
Thông tin tồn kho: {product_summary.get('stock_info')}
Biến thể (màu/size): {product_summary.get('variant_info')}
"""

    policies = """
CHÍNH SÁCH CHUNG (MẪU – có thể khác thực tế, KHÔNG ĐƯỢC BỊA):
- Thời gian giao hàng: thường từ 3–7 ngày tùy khu vực.
- Có thể cho khách kiểm hàng (đồng kiểm) trước khi thanh toán nếu đơn vị vận chuyển hỗ trợ.
- Chính sách đổi/trả/bảo hành: phụ thuộc từng sản phẩm. Nếu dữ liệu không ghi rõ thì hãy nói là cần nhân viên kiểm tra thêm.
"""

    full_user_content = f"""
[HƯỚNG DẪN HỆ THỐNG]
{conversation_hint or ""}

[THÔNG TIN SẢN PHẨM]
{product_data_str}

[CHÍNH SÁCH SHOP]
{policies}

[HỎI CỦA KHÁCH]
{user_message}
"""

    try:
        completion = client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.3,
            messages=[
                {"role": "system", "content": SYSTEM_INSTRUCTION},
                {"role": "user", "content": full_user_content},
            ],
        )
        answer = completion.choices[0].message.content.strip()
        return answer
    except Exception as e:
        print("[GPT ERROR]", e)
        return (
            "Hiện tại hệ thống tư vấn tự động đang bận. "
            "Anh/chị vui lòng chờ trong giây lát hoặc để lại SĐT để nhân viên gọi lại hỗ trợ ạ."
        )


def consult_product_first_time(user_id, rows, ms_code):
    """
    Khi khách vừa gửi mã sản phẩm lần đầu, bot:
    - Cập nhật context
    - Gửi mô tả ngắn
    - Gửi 1–2 thông tin (giá/tồn kho)
    - Gửi ảnh (tối đa 5 ảnh)
    """
    USER_CONTEXT[user_id] = {"current_ms": ms_code, "last_ts": time.time()}
    summary = build_product_summary(rows, ms_code)

    intro_lines = []
    intro_lines.append(f"Anh/chị đang xem sản phẩm mã {ms_code}: {summary.get('name')}")
    if summary.get("price_info"):
        intro_lines.append(summary["price_info"])
    if summary.get("stock_info"):
        intro_lines.append(summary["stock_info"])

    intro_lines.append(
        "Anh/chị có thể hỏi em về: giá, còn hàng, màu/size, chất liệu, bảo hành, thời gian giao hàng..."
    )

    send_text(user_id, "\n".join(intro_lines))

    imgs = get_clean_images(rows)
    for img in imgs[:5]:
        send_image(user_id, img, product_key=ms_code)
        time.sleep(0.3)


# =========================
# 6. INTENT ENGINE (Rule-based với SHIP thông minh)
# =========================

# Các cụm từ liên quan ship nhưng KHÔNG phải đặt hàng (hỏi phí / freeship)
SHIPPING_NEGATIVE = [
    "miễn ship",
    "mien ship",
    "free ship",
    "freeship",
    "phí ship",
    "phi ship",
    "tiền ship",
    "tien ship",
    "mien sip",
    "free sip",
]

# Các pattern: ship + số lượng => coi là đặt hàng
ORDER_SHIP_PATTERNS = [
    r"\bship\s*\d+",
    r"\bsip\s*\d+",
    r"\bsíp\s*\d+",
    r"\bship\b.*\b(cái|cai|bộ|bo|đôi|doi)\b",
    r"\bsip\b.*\b(cái|cai|bộ|bo|đôi|doi)\b",
    r"\bsíp\b.*\b(cái|cai|bộ|bo|đôi|doi)\b",
]


def is_order_ship(text: str) -> bool:
    """
    Trả về True nếu câu chứa 'ship/sip/síp' theo kiểu chốt đơn (ship 1 cái, ship 2 bộ...).
    Không kích hoạt nếu câu đang hỏi về phí ship (miễn ship, free ship...).
    """
    text = text.lower()
    for bad in SHIPPING_NEGATIVE:
        if bad in text:
            return False
    for pattern in ORDER_SHIP_PATTERNS:
        if re.search(pattern, text):
            return True
    return False


INTENT_KEYWORDS = {
    # Đặt hàng — không dùng từ 'ship' đơn lẻ, chỉ các cụm từ thể hiện ý chốt đơn rõ ràng
    "ORDER": [
        "lấy luôn",
        "lấy cho chị",
        "lấy cho anh",
        "mua luôn",
        "mua nhe",
        "mua nhé",
        "ok chốt",
        "ok em chốt",
        "ok mua",
        "ok ship",
        "lên đơn",
        "len don",
        "tạo đơn",
        "tao don",
        "làm đơn",
        "lam don",
        "chốt đơn",
        "chot don",
        "em chốt",
        "chot nhe",
    ],
    # Xem / gợi ý sản phẩm khác
    "BROWSE": [
        "mẫu khác",
        "mau khac",
        "xem mẫu khác",
        "xem mau khac",
        "sản phẩm khác",
        "san pham khac",
        "mã khác",
        "ma khac",
        "gửi mẫu khác",
        "gui mau khac",
        "còn mẫu nào",
        "co mau nao",
        "xem thêm",
        "xem them",
    ],
    # Hỏi giá
    "PRICE": ["giá", "gia", "bao nhiêu", "bao nhieu", "nhiêu tiền", "nhieu tien", "bn", "bao nhieu 1", "bao nhieu 1 bo"],
    # Hỏi còn hàng / hết hàng
    "STOCK": [
        "còn hàng",
        "con hang",
        "hết hàng",
        "het hang",
        "còn không",
        "con khong",
        "còn ko",
        "con ko",
        "còn k",
        "con k",
        "có sẵn",
        "co san",
        "còn size",
        "con size",
    ],
    # Hỏi màu / size / biến thể
    "VARIANT": [
        "size",
        "sai",
        "màu",
        "mau",
        "màu sắc",
        "mau sac",
        "kích cỡ",
        "kich co",
        "kích thước",
        "kich thuoc",
    ],
    # Hỏi xem ảnh
    "IMAGE": [
        "ảnh",
        "anh",
        "hình",
        "hinh",
        "hình ảnh",
        "hinh anh",
        "xem mẫu",
        "xem mau",
        "xem hình",
        "xem hinh",
        "gửi ảnh",
        "gui anh",
    ],
    # Hỏi video
    "VIDEO": ["video", "clip", "tiktok", "reels", "xem video"],
    # Hỏi về phí ship / freeship / chính sách vận chuyển
    "SHIPPING": [
        "miễn ship",
        "mien ship",
        "free ship",
        "freeship",
        "phí ship",
        "phi ship",
        "tiền ship",
        "tien ship",
        "ship bao nhiêu",
        "ship bao nhieu",
        "tính ship",
        "tinh ship",
        "ship được không",
        "ship duoc khong",
        "có ship không",
        "co ship khong",
        "ship tới đâu",
        "ship tinh",
        "ship nhanh",
        "ship chậm",
        "ship cham",
    ],
    # Hỏi thông tin chi tiết về tính năng / bảo hành / công suất / đồng kiểm / quà tặng...
    "PRODUCT_INFO": [
        "bảo hành",
        "bao hanh",
        "bh",
        "bảo trì",
        "bao tri",
        "bảo dưỡng",
        "bao duong",
        "tính năng",
        "tinh nang",
        "chức năng",
        "chuc nang",
        "công dụng",
        "cong dung",
        "công suất",
        "cong suat",
        "hướng dẫn",
        "huong dan",
        "cách dùng",
        "cach dung",
        "cách sử dụng",
        "huong dan su dung",
        "hdsd",
        "đồng kiểm",
        "dong kiem",
        "quà tặng",
        "qua tang",
        "tặng gì",
        "tang gi",
        "bền không",
        "ben khong",
        "tốt không",
        "tot khong",
        "chất lượng",
        "chat luong",
    ],
    # Chào hỏi / câu chung chung
    "SMALLTALK": [
        "alo",
        "hello",
        "hi",
        "shop ơi",
        "shop oi",
        "tư vấn",
        "tu van",
        "đang xem",
        "dang xem",
        "ờ",
        "uhm",
        "uk",
        "ừ",
    ],
}


def detect_intent(user_text: str) -> str:
    """
    Nhận diện intent của câu hỏi.
    Trả về một trong các giá trị:
    ORDER / BROWSE / PRICE / STOCK / VARIANT / IMAGE / VIDEO / SHIPPING / PRODUCT_INFO / SMALLTALK / NONE
    """
    if not user_text:
        return "NONE"

    text = user_text.lower()

    # 1. Ưu tiên tuyệt đối: ship chốt đơn
    if is_order_ship(text):
        return "ORDER"

    # 2. Order theo cụm từ mạnh
    for kw in INTENT_KEYWORDS["ORDER"]:
        if kw in text:
            return "ORDER"

    # 3. Các intent còn lại (theo thứ tự ưu tiên logic bán hàng)
    for intent in [
        "BROWSE",
        "PRICE",
        "STOCK",
        "VARIANT",
        "IMAGE",
        "VIDEO",
        "SHIPPING",
        "PRODUCT_INFO",
        "SMALLTALK",
    ]:
        for kw in INTENT_KEYWORDS[intent]:
            if kw in text:
                return intent

    return "NONE"


def handle_intent(intent: str, sender_id: str, user_message: str, current_ms=None, current_rows=None):
    """
    Điều hướng xử lý theo intent.
    current_ms / current_rows: sản phẩm đang tư vấn (nếu có).
    Nếu chưa có sản phẩm trong context, một số intent sẽ yêu cầu khách cung cấp mã sản phẩm trước.
    """

    # 1 — ĐẶT HÀNG
    if intent == "ORDER":
        if current_ms and current_rows is not None:
            send_text(
                sender_id,
                "Dạ anh/chị muốn chốt đơn sản phẩm này ạ? "
                "Anh/chị cho em xin *SĐT + Họ tên + Địa chỉ* để em tạo đơn nha ❤️",
            )
        else:
            send_text(
                sender_id,
                "Dạ anh/chị cho em xin mã sản phẩm (dạng MSxxxxx) hoặc gửi lại link/bài đăng để em biết mình đặt mẫu nào ạ.",
            )
        return

    # 2 — XEM SẢN PHẨM KHÁC
    if intent == "BROWSE":
        results = search_products_by_text(user_message, limit=5)
        if results is None or len(results) == 0:
            send_text(
                sender_id,
                "Dạ em chưa tìm được mẫu khác phù hợp. Anh/chị mô tả rõ hơn kiểu dáng, màu sắc hoặc gửi hình tham khảo giúp em ạ?",
            )
        else:
            lines = ["Dạ đây là một số mẫu khác phù hợp với anh/chị:"]
            if "Mã sản phẩm" in results.columns and "Tên sản phẩm" in results.columns:
                for _, row in results.iterrows():
                    lines.append(f"- [{row['Mã sản phẩm']}] {row['Tên sản phẩm']}")
            lines.append(
                "\nAnh/chị quan tâm mẫu nào, gửi giúp em mã (MSxxxxx) để em tư vấn chi tiết ạ.",
            )
            send_text(sender_id, "\n".join(lines))
        return

    # 3 — HỎI GIÁ
    if intent == "PRICE":
        if current_ms and current_rows is not None:
            msg = answer_price(current_rows, current_ms)
            send_text(sender_id, msg)
        else:
            send_text(
                sender_id,
                "Dạ anh/chị cho em xin mã sản phẩm hoặc tên sản phẩm cụ thể để em báo giá chính xác ạ.",
            )
        return

    # 4 — HỎI CÒN HÀNG
    if intent == "STOCK":
        if current_ms and current_rows is not None:
            msg = answer_stock(current_rows, current_ms)
            send_text(sender_id, msg)
        else:
            send_text(
                sender_id,
                "Dạ anh/chị đang hỏi tồn kho của sản phẩm nào ạ? Cho em xin mã sản phẩm hoặc tên sản phẩm giúp em nhé.",
            )
        return

    # 5 — HỎI MÀU / SIZE
    if intent == "VARIANT":
        if current_ms and current_rows is not None:
            msg = answer_color_size(current_rows, current_ms)
            send_text(sender_id, msg)
        else:
            send_text(
                sender_id,
                "Dạ anh/chị cho em xin mã sản phẩm hoặc gửi lại hình sản phẩm, em sẽ kiểm tra giúp mình màu/size còn hàng ạ.",
            )
        return

    # 6 — HỎI ẢNH
    if intent == "IMAGE":
        if current_ms and current_rows is not None:
            imgs = get_clean_images(current_rows)
            if not imgs:
                send_text(
                    sender_id,
                    f"Dữ liệu sản phẩm mã {current_ms} hiện chưa có link ảnh chi tiết trong hệ thống ạ.",
                )
            else:
                send_text(
                    sender_id,
                    f"Em gửi anh/chị một số ảnh tham khảo của sản phẩm mã {current_ms} ạ:",
                )
                for img in imgs[:5]:
                    send_image(sender_id, img, product_key=current_ms)
                    time.sleep(0.3)
        else:
            send_text(
                sender_id,
                "Dạ anh/chị đang cần xem ảnh của sản phẩm nào ạ? Cho em xin mã sản phẩm hoặc mô tả sản phẩm giúp em nhé.",
            )
        return

    # 7 — HỎI VIDEO
    if intent == "VIDEO":
        if current_ms and current_rows is not None:
            vids = get_clean_videos(current_rows)
            if not vids:
                send_text(
                    sender_id,
                    "Hiện trong dữ liệu sản phẩm này chưa có video demo sẵn. "
                    "Anh/chị có thể xem thêm hình ảnh hoặc em nhờ nhân viên gửi video sau cho mình ạ.",
                )
            else:
                send_text(sender_id, "Em gửi anh/chị một vài video tham khảo của sản phẩm ạ:")
                for vurl in vids[:3]:
                    send_video(sender_id, vurl, product_key=current_ms)
                    time.sleep(0.3)
        else:
            send_text(
                sender_id,
                "Dạ sản phẩm nào anh/chị cần xem video ạ? Cho em xin mã hoặc tên sản phẩm để em kiểm tra giúp ạ.",
            )
        return

    # 8 — HỎI CHÍNH SÁCH SHIP / FREESHIP
    if intent == "SHIPPING":
        send_text(
            sender_id,
            "Dạ phí ship và thời gian giao hàng sẽ phụ thuộc vào địa chỉ nhận và từng đợt ưu đãi ạ. "
            "Anh/chị cho em xin khu vực (tỉnh/huyện) và mã sản phẩm quan tâm, em nhờ nhân viên báo phí chính xác cho mình nhé ❤️",
        )
        return

    # 9 — HỎI THÔNG TIN CHI TIẾT (tính năng, công suất, bảo hành...) – dùng GPT diễn đạt
    if intent == "PRODUCT_INFO" and current_ms and current_rows is not None:
        product_summary = build_product_summary(current_rows, current_ms)
        reply = call_gpt_for_product(
            user_message=user_message,
            product_summary=product_summary,
            conversation_hint="Khách đang hỏi thông tin chi tiết (tính năng, công suất, bảo hành, độ phù hợp...) của sản phẩm.",
        )
        send_text(sender_id, reply)
        return

    if intent == "PRODUCT_INFO" and (not current_ms or current_rows is None):
        send_text(
            sender_id,
            "Dạ anh/chị cho em xin mã sản phẩm hoặc tên sản phẩm để em kiểm tra thông tin chi tiết và tư vấn kỹ hơn ạ.",
        )
        return

    # 10 — SMALLTALK
    if intent == "SMALLTALK":
        send_text(
            sender_id,
            "Dạ em đây ạ. Anh/chị cho em xin mã sản phẩm hoặc mô tả giúp em biết mình đang quan tâm mẫu nào để em tư vấn chi tiết hơn ạ.",
        )
        return

    # 11 — NONE / không nhận diện được
    send_text(
        sender_id,
        "Dạ anh/chị cho em xin mã sản phẩm (MSxxxxx) hoặc mô tả rõ hơn sản phẩm đang xem để em hỗ trợ mình tốt hơn ạ.",
    )
    return


# =========================
# 7. WEBHOOK FACEBOOK
# =========================

def is_echo_event(event):
    """Kiểm tra có phải echo message do page gửi chính nó không."""
    message = event.get("message")
    if not message:
        return False
    return bool(message.get("is_echo"))


def get_mid(event):
    """Lấy MID để chống xử lý trùng."""
    message = event.get("message") or {}
    mid = message.get("mid")
    return mid or ""


def is_processed_mid(mid):
    if not mid:
        return False
    if mid in PROCESSED_MIDS:
        return True
    PROCESSED_MIDS.add(mid)
    return False


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

    data = request.get_json()
    print("[Webhook]", data)

    if not data:
        return "no data", 200

    if "entry" not in data:
        return "no entry", 200

    for entry in data["entry"]:
        messaging = entry.get("messaging", [])
        for event in messaging:
            if "message" not in event:
                continue

            # 1. Bỏ qua echo
            if is_echo_event(event):
                print("[SKIP] echo")
                continue

            # 2. Chống xử lý trùng mid
            mid = get_mid(event)
            if is_processed_mid(mid):
                print("[SKIP] duplicate mid:", mid)
                continue

            sender_id = event.get("sender", {}).get("id")
            if not sender_id:
                continue

            message = event["message"]
            text = message.get("text", "")
            t_norm = normalize_text(text)

            # 3. Lệnh bật/tắt bot (cho admin / test)
            if t_norm in ["tắt bot", "tat bot", "off bot", "stop bot"]:
                BOT_ENABLED = False
                fb_send(
                    {
                        "recipient": {"id": sender_id},
                        "message": {"text": "⚠️ Bot đã tắt. Em sẽ không tự động trả lời nữa."},
                    }
                )
                print("[BOT] turned OFF by", sender_id)
                continue

            if t_norm in ["bật bot", "bat bot", "start bot", "on bot", "bat lai"]:
                BOT_ENABLED = True
                fb_send(
                    {
                        "recipient": {"id": sender_id},
                        "message": {"text": "✅ Bot đã bật lại, sẵn sàng hỗ trợ khách."},
                    }
                )
                print("[BOT] turned ON by", sender_id)
                continue

            # 4. Nếu bot đang off -> im lặng
            if not BOT_ENABLED:
                print("[SKIP] bot is OFF, ignore message from", sender_id)
                continue

            # 5. Logic tư vấn
            load_sheet()

            if not text:
                send_text(sender_id, "Anh/chị mô tả giúp shop đang tìm mã sản phẩm nào ạ?")
                continue

            # Lấy context nếu có
            ctx = USER_CONTEXT.get(sender_id)
            current_ms = None
            current_rows = None
            if ctx:
                ms = ctx.get("current_ms")
                if ms:
                    rows = find_product_by_code(ms)
                    if rows is not None and not rows.empty:
                        current_ms = ms
                        current_rows = rows

            # Thử xem khách có gửi mã sản phẩm mới không
            ms_code_in_text = extract_ms_from_text(text)
            if ms_code_in_text:
                rows = find_product_by_code(ms_code_in_text)
                if rows is None or rows.empty:
                    send_text(
                        sender_id,
                        f"Em không tìm thấy sản phẩm với mã {ms_code_in_text} trong dữ liệu. "
                        "Anh/chị kiểm tra lại mã hoặc gửi hình/minh mô tả sản phẩm giúp em nhé.",
                    )
                    continue

                # Cập nhật context, gửi intro + ảnh
                consult_product_first_time(sender_id, rows, ms_code_in_text)
                continue

            # Nếu có context sản phẩm hiện tại -> dùng Intent Engine để trả lời theo đúng ý khách
            if current_ms and current_rows is not None:
                intent = detect_intent(text)
                handle_intent(
                    intent=intent,
                    sender_id=sender_id,
                    user_message=text,
                    current_ms=current_ms,
                    current_rows=current_rows,
                )
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
                lines.append("\nAnh/chị quan tâm mã nào, gửi giúp em mã (dạng MSxxxxx), em tư vấn chi tiết ạ.")
                send_text(sender_id, "\n".join(lines))
            else:
                send_text(
                    sender_id,
                    "Dạ em chưa tìm thấy sản phẩm phù hợp trong dữ liệu theo mô tả vừa rồi. "
                    "Anh/chị mô tả rõ hơn (mã, tên, kiểu dáng, màu sắc, hình ảnh...) "
                    "anh/chị đang xem giúp em nhé.",
                )

    return "ok", 200


@app.route("/")
def home():
    return "Chatbot running.", 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
