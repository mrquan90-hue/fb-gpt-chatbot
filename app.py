# =======================
#   APP.PY – PHIÊN BẢN PRO
#   + WEBVIEW FORM + CHỐNG LẶP + STATE ĐẶT HÀNG + HYBRID INTENT
#   + AUTO-REPLY COMMENT (XOAY VÒNG NỘI DUNG)
# =======================

import os
import re
import time
import io
import random  # thêm để xoay vòng nội dung trả lời comment
import requests
import pandas as pd
from flask import Flask, request, send_from_directory, redirect
from openai import OpenAI

app = Flask(__name__, static_folder="static", static_url_path="/static")

# --------------------------
# CONFIG
# --------------------------
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN", "")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "verify_token_123")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
DOMAIN = os.getenv("DOMAIN", "fb-gpt-chatbot.onrender.com")  # domain mặc định khi deploy Render

BOT_ENABLED = True
client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

FB_API_URL = "https://graph.facebook.com/v18.0/me/messages"
FB_GRAPH_URL = "https://graph.facebook.com/v18.0"

# --------------------------
# AUTO-REPLY COMMENT – TEMPLATE DÀI, XOAY VÒNG
# --------------------------
COMMENT_REPLY_TEMPLATES = [
    "{name} ơi, shop đã gửi đầy đủ giá + ảnh thật mẫu anh/chị quan tâm vào inbox rồi ạ. Anh/chị mở tin nhắn giúp em để em tư vấn chi tiết hơn nha ❤️",
    "{name} ơi, em vừa inbox thông tin chi tiết về mẫu anh/chị đang hỏi, kèm giá và ưu đãi hôm nay. Anh/chị check tin nhắn giúp em với ạ ❤️",
    "{name} ơi, shop đã nhắn riêng báo giá, hình thật và tư vấn size cho anh/chị rồi đó ạ. Anh/chị xem tin nhắn để em hỗ trợ đặt đơn luôn nha ❤️",
    "{name} ơi, em gửi qua inbox toàn bộ thông tin mẫu, giá và các màu còn sẵn cho anh/chị rồi ạ. Anh/chị mở Messenger giúp em để mình trao đổi nhanh hơn nha ❤️",
    "{name} ơi, shop đã gửi giá + ảnh thật sản phẩm vào inbox rồi ạ. Anh/chị xem giúp em, nếu cần em tư vấn thêm về size/màu luôn cho mình nha ❤️",
    "{name} ơi, em vừa nhắn riêng cho anh/chị bảng giá và hình thật sản phẩm. Anh/chị check tin nhắn để em hỗ trợ chốt đơn nhanh trong hôm nay nha ❤️",
    "{name} ơi, thông tin chi tiết về mẫu anh/chị hỏi (giá, màu, size) em đã gửi vào inbox rồi ạ. Anh/chị xem giúp em, có gì em hỗ trợ ngay nha ❤️",
    "{name} ơi, shop đã inbox đầy đủ thông tin và ưu đãi hiện tại cho anh/chị. Anh/chị mở Messenger xem giúp em để em tư vấn kỹ hơn nha ❤️",
    "{name} ơi, em gửi tin nhắn riêng kèm hình thật và mô tả chi tiết sản phẩm rồi ạ. Anh/chị xem tin nhắn giúp em nhé, em luôn sẵn sàng hỗ trợ ạ ❤️",
    "{name} ơi, giá và hình ảnh chi tiết em đã gửi vào inbox cho anh/chị rồi ạ. Anh/chị kiểm tra tin nhắn để mình chốt đơn với ưu đãi tốt nhất hôm nay nha ❤️"
]

LAST_COMMENT_TEMPLATE_IDX = None


def get_comment_reply_text(name: str = None) -> str:
    """
    Chọn ngẫu nhiên 1 câu trả lời comment, hạn chế trùng lặp liên tiếp.
    name: dùng để @Tên khách hoặc xưng hô cá nhân hóa.
    """
    global LAST_COMMENT_TEMPLATE_IDX
    if not COMMENT_REPLY_TEMPLATES:
        return "Shop đã inbox anh/chị đầy đủ thông tin rồi ạ, anh/chị check giúp em nhé ❤️"

    n = len(COMMENT_REPLY_TEMPLATES)
    idx = random.randint(0, n - 1)
    if LAST_COMMENT_TEMPLATE_IDX is not None and n > 1 and idx == LAST_COMMENT_TEMPLATE_IDX:
        idx = (idx + 1) % n
    LAST_COMMENT_TEMPLATE_IDX = idx

    template = COMMENT_REPLY_TEMPLATES[idx]
    if not name:
        name = "anh/chị"
    return template.format(name=name)


# --------------------------
# Facebook Send
# --------------------------
def fb_send(payload):
    if not PAGE_ACCESS_TOKEN:
        print("[fb_send] MISSING PAGE_ACCESS_TOKEN")
        print(payload)
        return False

    params = {"access_token": PAGE_ACCESS_TOKEN}
    try:
        r = requests.post(FB_API_URL, params=params, json=payload, timeout=10)
        if r.status_code != 200:
            print("[fb_send] ERROR:", r.status_code, r.text)
            return False
        return True
    except Exception as e:
        print("[fb_send] EXCEPTION:", e)
        return False


def send_text(uid, text):
    fb_send({"recipient": {"id": uid}, "message": {"text": text}})


def send_image(uid, url):
    fb_send({
        "recipient": {"id": uid},
        "message": {
            "attachment": {
                "type": "image",
                "payload": {"url": url, "is_reusable": True}
            }
        }
    })


def send_video(uid, url):
    fb_send({
        "recipient": {"id": uid},
        "message": {
            "attachment": {
                "type": "video",
                "payload": {"url": url, "is_reusable": True}
            }
        }
    })


# --------------------------
# REPLY COMMENT TRÊN BÀI VIẾT
# --------------------------
def fb_reply_comment(comment_id, text):
    """
    Trả lời comment ngay trên bài viết.
    """
    if not PAGE_ACCESS_TOKEN:
        print("[fb_reply_comment] MISSING PAGE_ACCESS_TOKEN")
        print(comment_id, text)
        return False

    url = f"{FB_GRAPH_URL}/{comment_id}/comments"
    params = {"access_token": PAGE_ACCESS_TOKEN}
    payload = {"message": text}
    try:
        r = requests.post(url, params=params, json=payload, timeout=10)
        if r.status_code != 200:
            print("[fb_reply_comment] ERROR:", r.status_code, r.text)
            return False
        return True
    except Exception as e:
        print("[fb_reply_comment] EXCEPTION:", e)
        return False


# --------------------------
# LINK ĐẶT HÀNG (KHÔNG DÙNG WEBVIEW)
# --------------------------
def send_order_link(uid, ms):
    """Gửi link đặt hàng dạng rút gọn, mở bằng trình duyệt thường."""
    short_url = f"https://{DOMAIN}/o/{ms}?uid={uid}"
    text = (
        "🛒💥 ĐẶT HÀNG NHANH (1 chạm):\n"
        f"👉 {short_url}\n\n"
        "Anh/chị bấm vào link, điền thông tin nhận hàng, "
        "shop sẽ gọi xác nhận đơn trong ít phút ạ ❤️"
    )
    send_text(uid, text)


# --------------------------
# GOOGLE SHEET LOADER
# --------------------------
SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv"

df = None
LAST_LOAD = 0
LOAD_TTL = 300


def load_sheet(force=False):
    global df, LAST_LOAD
    now = time.time()
    if not force and df is not None and now - LAST_LOAD < LOAD_TTL:
        return
    try:
        print("[Sheet] Reloading...")
        resp = requests.get(SHEET_CSV_URL, timeout=15)
        resp.raise_for_status()
        content = resp.content.decode("utf-8")
        df_local = pd.read_csv(io.StringIO(content))
        df_local.fillna("", inplace=True)
        df = df_local
        LAST_LOAD = now
        print("[Sheet] Loaded:", len(df))
    except Exception as e:
        print("[Sheet] ERROR:", e)


# --------------------------
# CONTEXT
# --------------------------
USER_CONTEXT = {}
LAST_MESSAGE_MID = {}


def get_ctx(uid):
    return USER_CONTEXT.get(uid, {})


def set_ctx(uid, **kwargs):
    ctx = USER_CONTEXT.get(uid, {})
    ctx.update(kwargs)
    USER_CONTEXT[uid] = ctx
    return ctx


def normalize(t):
    return str(t).strip().lower()


# --------------------------
# IGNORE FB SYSTEM EVENTS
# --------------------------
def ignore_event(ev):
    if "delivery" in ev:
        print("[IGNORE] delivery")
        return True
    if "read" in ev:
        print("[IGNORE] read")
        return True
    if ev.get("message", {}).get("is_echo"):
        print("[IGNORE] echo")
        return True
    return False


# --------------------------
# GET PAGE NAME (CACHE)
# --------------------------
PAGE_NAME = None


def get_page_name():
    """Lấy tên Fanpage bằng Graph API và cache."""
    global PAGE_NAME
    if PAGE_NAME:
        return PAGE_NAME

    try:
        resp = requests.get(
            f"{FB_GRAPH_URL}/me",
            params={"access_token": PAGE_ACCESS_TOKEN, "fields": "name"},
            timeout=10
        )
        data = resp.json()
        PAGE_NAME = data.get("name", "Shop")
        print("[get_page_name] Fanpage:", PAGE_NAME)
    except Exception as e:
        print("[get_page_name] ERROR", e)
        PAGE_NAME = "Shop"

    return PAGE_NAME


# --------------------------
# PRODUCT EXTRACTION
# --------------------------
def extract_ms(text: str):
    if not text:
        return None
    raw = text.upper()
    m = re.search(r"MS\s*(\d+)", raw)
    if m:
        return "MS" + m.group(1).zfill(6)
    return None


def guess_ms(text: str):
    global df
    if df is None:
        return None
    raw = text.upper()

    m = re.search(r"M[ÃA]?\s*(SP)?\s*(\d{3,})", raw)
    if m:
        code = "MS" + m.group(2).zfill(6)
        if code in df["Mã sản phẩm"].astype(str).values:
            return code

    nums = re.findall(r"\d{3,6}", raw)
    if len(nums) == 1:
        code = "MS" + nums[0].zfill(6)
        if code in df["Mã sản phẩm"].astype(str).values:
            return code
    return None


def guess_ms_by_content(text: str):
    """
    Đoán mã sản phẩm theo nội dung mô tả (fallback khi không có số mã).
    Đơn giản: đếm số từ khóa trùng giữa text và (Tên sản phẩm + Mô tả).
    """
    global df
    if df is None or not text:
        return None

    raw = normalize(text)
    tokens = re.findall(r"\w+", raw)
    tokens = [t for t in tokens if len(t) >= 3]
    if not tokens:
        return None

    best_ms = None
    best_score = 0

    for _, row in df.iterrows():
        ms_code = str(row.get("Mã sản phẩm", "")).strip()
        if not ms_code:
            continue
        st = f"{row.get('Tên sản phẩm', '')} {row.get('Mô tả', '')}"
        st_norm = normalize(st)
        score = 0
        for t in tokens:
            if t in st_norm:
                score += 1
        if score > best_score:
            best_score = score
            best_ms = ms_code

    if best_score == 0:
        return None
    return best_ms


def find_product(ms):
    rows = df[df["Mã sản phẩm"] == ms]
    return rows if not rows.empty else None


def format_price(v):
    try:
        return f"{float(v):,.0f}đ".replace(",", ".")
    except Exception:
        return str(v)


# --------------------------
# SHIP = ĐẶT HÀNG INTENT
# --------------------------
NEG_SHIP = ["miễn ship", "mien ship", "free ship", "freeship", "phí ship"]
SHIP_PATTERNS = [
    r"\bship\s*\d+",
    r"\bsip\s*\d+",
    r"\bship\b.*\b(cái|cai|bộ|bo)",
    r"\bsip\b.*\b(cái|cai|bộ|bo)"
]


def is_order_ship(text):
    t = text.lower()
    for neg in NEG_SHIP:
        if neg in t:
            return False
    for pat in SHIP_PATTERNS:
        if re.search(pat, t):
            return True
    return False


# --------------------------
# GPT SUMMARIZER
# --------------------------
SYSTEM_INSTRUCT = """
Bạn là trợ lý bán hàng, trả lời chính xác theo dữ liệu sản phẩm.
Không bịa, không thêm thông tin không có trong sheet.
"""


def call_gpt(user_msg, product_summary, hint=""):
    if not client:
        return "Hiện hệ thống AI bận, anh/chị mô tả rõ hơn giúp em ạ."

    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            temperature=0.4,
            messages=[
                {"role": "system", "content": SYSTEM_INSTRUCT},
                {"role": "system", "content": "Dữ liệu sản phẩm:\n" + product_summary},
                {"role": "system", "content": "Ngữ cảnh:\n" + hint},
                {"role": "user", "content": user_msg}
            ]
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print("[GPT ERROR]", e)
        return "Hệ thống hơi chậm, anh/chị mô tả chi tiết hơn giúp em ạ."


# --------------------------
# BUILD PRODUCT SUMMARY
# --------------------------
def build_summary(rows, ms):
    name = rows.iloc[0]["Tên sản phẩm"]
    desc = rows.iloc[0]["Mô tả"]
    return f"Mã: {ms}\nTên: {name}\nMô tả:\n{desc}"


# --------------------------
# CLEAN IMAGES
# --------------------------
def clean_images(rows):
    if "Images" not in rows.columns:
        return []
    urls = []
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


# --------------------------
# INTRODUCE PRODUCT
# --------------------------
def intro_product(uid, rows, ms, msg=""):
    set_ctx(uid, current_ms=ms, order_state=None)
    summary = build_summary(rows, ms)
    reply = call_gpt(msg or f"Giới thiệu mã {ms}",
                     summary,
                     hint="Khách vừa gửi mã sản phẩm.")
    send_text(uid, reply)

    imgs = clean_images(rows)
    for img in imgs[:5]:
        send_image(uid, img)
        time.sleep(0.3)


# --------------------------
# HANDLE FEED CHANGES (COMMENT) – AUTO-REPLY COMMENT
# --------------------------
def handle_change(change):
    """
    Xử lý webhook dạng entry['changes'] cho sự kiện comment trên bài viết.
    Auto-reply comment bằng nội dung xoay vòng, tránh trùng lặp.
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

        from_info = value.get("from", {})
        name = from_info.get("name") or "anh/chị"

        # Tạo câu trả lời dài, xoay vòng, có @tên khách
        reply_text = get_comment_reply_text(name=f"@{name}")
        ok = fb_reply_comment(comment_id, reply_text)
        if ok:
            print(f"[COMMENT REPLY] {comment_id} -> {reply_text}")
        else:
            print(f"[COMMENT REPLY] FAILED {comment_id}")

    except Exception as e:
        print("[handle_change] ERROR:", e)


# --------------------------
# WEBHOOK CORE
# --------------------------
@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    global BOT_ENABLED

    if request.method == "GET":
        if request.args.get("hub.verify_token") == VERIFY_TOKEN:
            return request.args.get("hub.challenge")
        return "Verification failed", 403

    data = request.get_json()

    for entry in data.get("entry", []):
        # 0. Xử lý thay đổi feed (comment) – auto-reply comment
        for change in entry.get("changes", []):
            handle_change(change)

        # 1. Xử lý tin nhắn Messenger
        for event in entry.get("messaging", []):

            if ignore_event(event):
                continue

            sender = event["sender"]["id"]
            message = event.get("message")

            if not (message and "text" in message):
                continue

            text = message["text"].strip()
            lower = normalize(text)
            mid = message.get("mid")

            # CHỐNG TRÙNG MID
            if LAST_MESSAGE_MID.get(sender) == mid:
                print("[IGNORE] duplicate mid")
                continue
            LAST_MESSAGE_MID[sender] = mid

            load_sheet()

            # BOT ON/OFF
            if lower in ["tắt bot", "tat bot"]:
                BOT_ENABLED = False
                send_text(sender, "❌ Bot đã tắt.")
                continue
            if lower in ["bật bot", "bat bot"]:
                BOT_ENABLED = True
                send_text(sender, "✅ Bot đã bật lại.")
                continue

            if not BOT_ENABLED:
                continue

            ctx = get_ctx(sender)
            current_ms = ctx.get("current_ms")

            # 1. Khách gửi MÃ SẢN PHẨM hoặc mô tả có thể map sang sản phẩm
            ms = extract_ms(text) or guess_ms(text) or guess_ms_by_content(text)
            if ms:
                rows = find_product(ms)
                if rows is None:
                    send_text(sender, f"Không tìm thấy sản phẩm {ms} ạ.")
                else:
                    intro_product(sender, rows, ms, msg=text)
                continue

            # 2. ĐẶT HÀNG → MỞ FORM
            if current_ms and is_order_ship(text):
                send_order_link(sender, current_ms)
                continue

            # 3. PHẢN HỒI THEO SẢN PHẨM (đã có current_ms)
            if current_ms:
                rows = find_product(current_ms)
                if rows is None:
                    set_ctx(sender, current_ms=None)
                    send_text(sender, "Anh/chị gửi lại mã sản phẩm giúp em ạ.")
                    continue

                summary = build_summary(rows, current_ms)

                # Hỏi giá
                if any(x in lower for x in ["giá", "bao nhiêu", "nhiêu tiền", "bn"]):
                    price = rows.iloc[0]["Giá bán"]
                    send_text(sender, f"Mã {current_ms} giá {format_price(price)} ạ.")
                    continue

                # Hỏi ảnh
                if any(x in lower for x in ["ảnh", "hình", "xem mẫu"]):
                    imgs = clean_images(rows)
                    if imgs:
                        for img in imgs[:5]:
                            send_image(sender, img)
                    else:
                        send_text(sender, "Mã này chưa có ảnh ạ.")
                    continue

                # Hỏi video
                if any(x in lower for x in ["video", "clip", "reels"]):
                    vids = rows["Videos"].astype(str).tolist()
                    ok = False
                    for v in vids:
                        parts = re.split(r"[\s,;]+", v)
                        for u in parts:
                            if u.startswith("http"):
                                send_video(sender, u)
                                ok = True
                                break
                        if ok:
                            break
                    if not ok:
                        send_text(sender, "Mã này chưa có video ạ.")
                    continue

                # Còn lại → GPT
                reply = call_gpt(text, summary, hint=f"Đang tư vấn mã {current_ms}")
                send_text(sender, reply)
                continue

            # 4. KHÔNG CÓ NGỮ CẢNH (TIN NHẮN ĐẦU TIÊN)
            send_text(
                sender,
                "Shop chào anh/chị 👋\n"
                "Anh/chị đang quan tâm mẫu nào để em hỗ trợ nhanh ạ?\n"
                "- Nếu đã có mã sản phẩm → gửi mã “MSxxxxx”.\n"
                "- Nếu có ảnh mẫu → gửi ảnh để em tìm đúng mã giúp anh/chị ❤️"
            )

    return "ok", 200


# --------------------------
# SHORT LINK /o/<MSxxxxxx> -> REDIRECT SANG /order-form
# --------------------------
@app.route("/o/<ms>")
def short_order(ms):
    uid = request.args.get("uid", "")
    # Redirect sang form đặt hàng chính, giữ lại uid & ms
    return redirect(f"/order-form?uid={uid}&ms={ms}")


@app.route("/order-form")
def order_form():
    return send_from_directory("static", "order-form.html")


# --------------------------
# API GET PRODUCT (Form)
# --------------------------
@app.route("/api/get-product")
def api_get_product():
    load_sheet()
    ms = request.args.get("ms", "")
    rows = find_product(ms)
    if rows is None:
        return {"error": "not_found"}

    row0 = rows.iloc[0]

    # ẢNH đầu tiên của biến thể đầu tiên
    image = ""
    parts = re.split(r"[\s,;]+", str(row0.get("Images", "")))
    for u in parts:
        if u.startswith("http"):
            image = u
            break

    sizes = rows["size (Thuộc tính)"].dropna().unique().tolist()
    colors = rows["màu (Thuộc tính)"].dropna().unique().tolist()

    fanpage_name = get_page_name()

    return {
        "name": row0["Tên sản phẩm"],
        "price": float(row0["Giá bán"]),
        "sizes": sizes,
        "colors": colors,
        "image": image,
        "fanpageName": fanpage_name,
        "page_name": fanpage_name  # thêm key này để JS mới đọc được
    }


# --------------------------
# API ORDER (Form)
# --------------------------
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


# --------------------------
# ROOT
# --------------------------
@app.route("/")
def home():
    return "Chatbot running OK", 200


# --------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
