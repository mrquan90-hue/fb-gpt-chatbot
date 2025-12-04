import os
import re
import time
import io
import random
import json
import requests
import pandas as pd
from flask import Flask, request, send_from_directory, redirect
from openai import OpenAI

app = Flask(__name__, static_folder="static", static_url_path="/static")

# --------------------------
# CONFIG
# --------------------------
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN", "")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "x5bot_verify_2025")
SHEET_URL = os.getenv("SHEET_URL", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
SHORTLINK_API = os.getenv("SHORTLINK_API", "")  # tuỳ chọn

BOT_ENABLED = True

# --------------------------
# OPENAI CLIENT
# --------------------------
client = None
if OPENAI_API_KEY:
    client = OpenAI(api_key=OPENAI_API_KEY)


# --------------------------
# STATE
# --------------------------
PRODUCT_DF = None
PRODUCT_DF_LAST_LOAD = 0
PRODUCT_DF_TTL = 300  # 5 phút

USER_CONTEXT = {}
LAST_MESSAGE_MID = {}


def get_ctx(uid):
    ctx = USER_CONTEXT.get(uid)
    if not ctx:
        ctx = {
            'current_ms': None,
            'post_ms': None,
            'introduced': False,
            'history': [],
        }
        USER_CONTEXT[uid] = ctx
    return ctx


def set_ctx(uid, **kwargs):
    ctx = USER_CONTEXT.get(uid, {})
    ctx.update(kwargs)
    USER_CONTEXT[uid] = ctx
    return ctx


def normalize(t):
    return str(t).strip().lower()


# --------------------------
# LOAD SHEET
# --------------------------
def load_sheet(force=False):
    global PRODUCT_DF, PRODUCT_DF_LAST_LOAD

    now = time.time()
    if not force and PRODUCT_DF is not None and now - PRODUCT_DF_LAST_LOAD < PRODUCT_DF_TTL:
        return

    if not SHEET_URL:
        print("[load_sheet] MISSING SHEET_URL")
        return

    try:
        print("[load_sheet] Reloading sheet...")
        resp = requests.get(SHEET_URL, timeout=30)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
        if "Mã sản phẩm" not in df.columns:
            print("[load_sheet] ERROR: Missing column 'Mã sản phẩm'")
            return

        PRODUCT_DF = df
        PRODUCT_DF_LAST_LOAD = now
        print(f"[load_sheet] Loaded {len(df)} rows.")
    except Exception as e:
        print("[load_sheet] ERROR:", e)


def find_product(ms):
    if PRODUCT_DF is None:
        load_sheet()
    if PRODUCT_DF is None:
        return None
    ms = str(ms).strip()
    if not ms:
        return None
    rows = PRODUCT_DF[PRODUCT_DF["Mã sản phẩm"].astype(str) == ms]
    if rows.empty:
        return None
    return rows


def guess_ms_by_content(text):
    """
    Đoán mã sản phẩm theo nội dung mô tả (fallback cho Vision / nội dung chung).
    """
    if PRODUCT_DF is None:
        load_sheet()
    if PRODUCT_DF is None:
        return None

    t = normalize(text)
    if not t:
        return None

    scores = []
    for _, row in PRODUCT_DF.iterrows():
        name = normalize(row.get("Tên sản phẩm", ""))
        desc = normalize(row.get("Mô tả", ""))
        ms = str(row.get("Mã sản phẩm", "")).strip()

        s = 0
        for kw in t.split():
            if kw and kw in name:
                s += 2
            if kw and kw in desc:
                s += 1
        if s > 0:
            scores.append((s, ms))

    if not scores:
        return None

    scores.sort(reverse=True)
    best_score, best_ms = scores[0]
    print(f"[guess_ms_by_content] best_ms={best_ms} score={best_score}")
    # Ngưỡng tối thiểu để nhận
    if best_score < 3:
        return None
    return best_ms


# --------------------------
# EXTRACT MS
# --------------------------
MS_PATTERN = re.compile(r"\bMS\d{6}\b", re.IGNORECASE)


def extract_ms(text):
    """
    Tìm MSxxxxx trong text.
    """
    if not text:
        return None
    m = MS_PATTERN.search(text)
    if m:
        return m.group(0).upper()
    return None


def extract_ms_from_hashtag(text):
    """
    Tìm [MSxxxxx] trong caption hoặc text.
    """
    if not text:
        return None
    m = re.search(r"\[?(MS\d{6})\]?", text, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    return None


# --------------------------
# SHORT LINK (ĐẶT HÀNG)
# --------------------------
def create_short_link(ms):
    """
    Tạo short link đặt hàng theo mã sản phẩm.
    """
    if not SHORTLINK_API:
        return None

    try:
        r = requests.post(SHORTLINK_API, json={"ms": ms}, timeout=10)
        r.raise_for_status()
        data = r.json()
        return data.get("short_url")
    except Exception as e:
        print("[create_short_link] ERROR:", e)
        return None


def build_order_url(ms):
    short = create_short_link(ms)
    if short:
        return short
    # fallback: link thô
    return f"https://x5shop.vn/dat-hang?ms={ms}"


def send_order_link(user_id, ms):
    url = build_order_url(ms)
    text = (
        f"Dạ để đặt hàng mã {ms} anh/chị bấm vào link sau giúp em ạ:\n{url}\n"
        "Anh/chị điền đủ thông tin, bên em sẽ gọi xác nhận & giao hàng sớm nhất ạ."
    )
    send_text(user_id, text)


# --------------------------
# INTENT (ĐẶT HÀNG / SHIP)
# --------------------------
SHIP_PATTERNS = [
    r"\bđặt\b",
    r"\bchốt\b",
    r"\bchot\b",
    r"\bgiao\b",
    r"\bship\b",
    r"\bgửi\b",
    r"\bmua\b",
]


def is_order_ship(text):
    t = normalize(text)
    if "đặt hàng" in t or "chốt đơn" in t:
        return True
    for pat in SHIP_PATTERNS:
        if re.search(pat, t):
            return True
    return False


SYSTEM_INSTRUCT = """
Bạn là trợ lý bán hàng, trả lời chính xác theo dữ liệu sản phẩm.
Không bịa, không thêm thông tin không có trong sheet.
"""


def call_gpt(user_msg, product_summary, hint=""):
    if not client:
        return "Hiện hệ thống AI bận, anh/chị mô tả rõ hơn giúp em ạ."

    content = f"{SYSTEM_INSTRUCT}\n\n{hint}\n\nDữ liệu sản phẩm:\n{product_summary}\n\nCâu hỏi của khách:\n{user_msg}"

    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            temperature=0.4,
            messages=[
                {"role": "system", "content": SYSTEM_INSTRUCT},
                {"role": "user", "content": content},
            ],
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print("[GPT ERROR]", e)
        return "Hệ thống hơi chậm, anh/chị mô tả chi tiết hơn giúp em ạ."


def build_summary(rows, ms):
    name = rows.iloc[0]["Tên sản phẩm"]
    desc = rows.iloc[0]["Mô tả"]
    return f"Mã: {ms}\nTên: {name}\nMô tả:\n{desc}"


# --------------------------
# FB SEND
# --------------------------
def fb_send(payload):
    if not PAGE_ACCESS_TOKEN:
        print("[fb_send] MISSING PAGE_ACCESS_TOKEN")
        print(payload)
        return False

    try:
        url = f"https://graph.facebook.com/v19.0/me/messages?access_token={PAGE_ACCESS_TOKEN}"
        r = requests.post(url, json=payload, timeout=15)
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


def send_text(recipient_id, text):
    payload = {
        "recipient": {"id": recipient_id},
        "message": {"text": text},
        "messaging_type": "RESPONSE",
    }
    return fb_send(payload)


def send_image(recipient_id, image_url):
    payload = {
        "recipient": {"id": recipient_id},
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
    return fb_send(payload)


def send_video(recipient_id, video_url):
    payload = {
        "recipient": {"id": recipient_id},
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
    return fb_send(payload)


# --------------------------
# IGNORE DELIVERY/READ
# --------------------------
def ignore_event(event):
    if "delivery" in event:
        return True
    if "read" in event:
        return True
    return False


# --------------------------
# COMMENT AUTO REPLY LOGIC
# --------------------------
COMMENT_INTENT_PATTERNS = {
    "price": ["bao nhiêu", "nhiu", "giá", "gia", "bn", "bao nhieu", "bnhiu"],
    "size": ["size", "siz", "sai", "mấy ký", "kg", "kí"],
    "color": ["màu", "mau", "tone", "mầu"],
    "ship": ["ship", "giao", "vận chuyển", "gửi về"],
    "consult": ["tư vấn", "tuvan", "hỗ trợ", "tư van"],
}


def detect_comment_intent(message):
    t = normalize(message)
    if not t:
        return "other"

    for intent, kws in COMMENT_INTENT_PATTERNS.items():
        for kw in kws:
            if kw in t:
                return intent

    if "mua" in t or "đặt" in t or "chốt" in t:
        return "order"

    return "other"


def pick_comment_template(intent, name=""):
    if not name:
        name = "chị"

    if intent == "price":
        return f"Dạ em chào {name} ạ, giá chi tiết em inbox cho mình luôn nha. 🥰"
    if intent == "size":
        return f"Dạ {name} ơi, em gửi bảng size chi tiết trong inbox cho mình ạ. 🧵"
    if intent == "color":
        return f"Dạ em chào {name}, mẫu này có nhiều màu xinh lắm ạ, em gửi hình từng màu trong inbox nhé. 🎨"
    if intent == "ship":
        return f"Dạ {name} ơi, em báo phí ship & thời gian nhận hàng cụ thể trong inbox cho mình nha. 🚚"
    if intent == "consult":
        return f"Dạ em chào {name}, em tư vấn chi tiết mẫu – size – màu trong inbox cho mình luôn ạ. 💬"
    if intent == "order":
        return f"Dạ em chào {name}, em hướng dẫn mình đặt hàng nhanh gọn trong inbox ạ. ❤️"

    return f"Dạ em cảm ơn {name} đã quan tâm, em nhắn tin tư vấn chi tiết cho mình trong inbox ạ. 🥰"


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


def intro_product(uid, rows, ms, msg=""):
    """
    Giới thiệu sản phẩm tổng quát khi mới nhận mã.
    """
    summary = build_summary(rows, ms)
    reply = call_gpt(msg or f"Giới thiệu mã {ms}", summary, hint=f"Đang giới thiệu mã {ms}")
    text = f"Sản phẩm {ms} em có tóm tắt như sau ạ:\n{reply}"
    send_text(uid, text)

    imgs = clean_images(rows)
    sent = 0
    for url in imgs:
        if sent >= 3:
            break
        send_image(uid, url)
        sent += 1


def handle_page_outgoing_message(uid, text):
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
        set_ctx(uid, current_ms=ms, post_ms=ms)
    else:
        print("[PAGE MSG] no product code in echo")


# --------------------------
# GPT CONTEXT + VISION ENGINE
# --------------------------

def build_gpt_messages(uid, user_msg, rows, ms):
    """
    Tạo messages gửi lên GPT, dùng toàn bộ ngữ cảnh hội thoại.
    GPT sẽ hiểu khách đang hỏi gì và soạn câu trả lời hoàn chỉnh.
    """
    ctx = get_ctx(uid)
    history = ctx.get("history", [])
    post_ms = ctx.get("post_ms")

    # Chuẩn hóa lịch sử hội thoại thành text
    hist_lines = []
    for role, msg in history:
        hist_lines.append(f"{role}: {msg}")
    hist_text = "\n".join(hist_lines)

    if rows is not None and ms:
        summary = build_summary(rows, ms)
    else:
        summary = "Chưa xác định được sản phẩm nào."

    system_msg = """
Bạn là trợ lý bán hàng của shop thời trang, xưng "em", gọi khách là "anh/chị".

YÊU CẦU BẮT BUỘC:
- Luôn xưng "em" và gọi khách là "anh/chị".
- Giọng lịch sự, chuyên nghiệp, ngắn gọn, dễ hiểu.
- Không bao giờ nói sai mã sản phẩm.
- Nếu đã có mã sản phẩm hiện tại (current_ms) thì coi như đang tư vấn đúng mẫu đó.
- Nếu current_ms rỗng nhưng post_ms có thì ưu tiên dùng post_ms.
- Không tự bịa thông tin ngoài dữ liệu sản phẩm.
- Không hỏi quá 1 câu ngược lại khách trong một lần trả lời.

DỮ LIỆU SẢN PHẨM HIỆN TẠI:
- current_ms: {current_ms}
- post_ms: {post_ms}
- Tóm tắt:
{summary}

LỊCH SỬ HỘI THOẠI (gần nhất):
{hist_text}
""".format(
        current_ms=ms or "None",
        post_ms=post_ms or "None",
        summary=summary,
        hist_text=hist_text,
    )

    user_prompt = """
Tin nhắn mới nhất của khách:
"{msg}"

Nhiệm vụ của bạn:
1) Xác định đang tư vấn sản phẩm mã nào (final_ms). Nếu không có thì để null.
2) Đọc dữ liệu sản phẩm + lịch sử hội thoại để hiểu khách đang hỏi gì.
3) Soạn câu trả lời phù hợp, giọng "em – anh/chị", đúng ngữ cảnh.
4) Nếu phù hợp, có thể gợi ý khách đặt hàng nhưng không ép.

Trả về JSON thuần (không giải thích thêm), theo format:

{
  "final_ms": "MS000123" hoặc null,
  "reply": "nội dung tin nhắn em sẽ gửi cho anh/chị"
}
""".format(msg=user_msg)

    messages = [
        {"role": "system", "content": system_msg},
        {"role": "user", "content": user_prompt},
    ]
    return messages


def gpt_reply_for_user(uid, user_msg):
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
        # fallback: trả lời kiểu cũ
        if rows is not None and ms:
            summary = build_summary(rows, ms)
            fallback = call_gpt(user_msg, summary, hint="Đang tư vấn mã {}".format(ms))
        else:
            fallback = "Dạ em đang xử lý hơi chậm, anh/chị nhắn lại giúp em nội dung cần tư vấn với ạ."
        return ms, fallback

    final_ms = data.get("final_ms")
    reply = (data.get("reply") or "").strip()

    if not reply:
        reply = "Dạ em đang xử lý hơi chậm, anh/chị nhắn lại giúp em nội dung cần tư vấn với ạ."

    return final_ms, reply


def call_gpt_vision_describe_image(image_url):
    """
    Dùng GPT (vision) để mô tả ảnh sản phẩm.
    Trả về đoạn mô tả tiếng Việt ngắn gọn.
    """
    if not client:
        return None

    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "Mô tả ngắn gọn (bằng tiếng Việt) kiểu dáng, chất liệu, phong cách của sản phẩm trong ảnh để em dùng cho việc tìm sản phẩm tương tự trong kho hàng.",
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": image_url,
                            },
                        },
                    ],
                }
            ],
            temperature=0.3,
        )
        desc = resp.choices[0].message.content.strip()
        return desc
    except Exception as e:
        print("[GPT VISION ERROR]", e)
        return None


def analyze_image_and_find_ms(uid, image_url):
    """
    Khi khách gửi ảnh (không có text), dùng GPT Vision mô tả ảnh,
    sau đó đối chiếu với dữ liệu sản phẩm để tìm mã gần đúng.
    """
    load_sheet()
    desc = call_gpt_vision_describe_image(image_url)
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


# --------------------------
# HANDLE FEED CHANGES (COMMENT)
# --------------------------
def handle_change(change):
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
        message = value.get("message", "")
        from_ = value.get("from", {})
        name = from_.get("name", "")

        intent = detect_comment_intent(message)
        reply_text = pick_comment_template(intent, name)
        ok = fb_reply_comment(comment_id, reply_text)
        if ok:
            print(f"[COMMENT REPLY] {comment_id} intent={intent} msg='{message}' -> {reply_text}")
        else:
            print(f"[COMMENT REPLY] FAILED {comment_id}")

    except Exception as e:
        print("[handle_change] ERROR:", e)


def fb_reply_comment(comment_id, text):
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
        data = r.json()
        if "error" in data:
            print("[fb_reply_comment] FB ERROR:", data)
            return False
        return True
    except Exception as e:
        print("[fb_reply_comment] EXC:", e)
        return False


# --------------------------
# WEBHOOK CORE (FINAL)
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
        # 0. Xử lý comment (feed)
        for change in entry.get("changes", []):
            handle_change(change)

        # 1. Xử lý tin nhắn
        for event in entry.get("messaging", []):
            message = event.get("message")

            # Xác định user_id đúng:
            # - Nếu là echo: user = recipient (khách)
            # - Nếu là tin khách gửi: user = sender
            is_echo = bool(message and message.get("is_echo"))
            if is_echo:
                user_id = event.get("recipient", {}).get("id")
            else:
                user_id = event.get("sender", {}).get("id")

            if not user_id:
                print("[WARN] Missing user_id in event:", event)
                continue

            # ECHO (tin do Page/Fchat/Bot gửi)
            if is_echo:
                text = message.get("text", "") or ""
                print(f"[ECHO] -> user {user_id}: {text}")
                # Chỉ dùng ECHO để cập nhật ngữ cảnh sản phẩm nếu có hashtag #MSxxxxx
                handle_page_outgoing_message(user_id, text)
                # KHÔNG trả lời echo
                continue

            # Bỏ qua delivery/read...
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

            print("[MSG] from", user_id, ":", text)

            load_sheet()

            # LỆNH BẬT/TẮT BOT (chỉ khi có text)
            if text:
                if lower in ["tắt bot", "tat bot"]:
                    BOT_ENABLED = False
                    send_text(user_id, "Dạ em tạm dừng tư vấn tự động ạ.")
                    continue
                if lower in ["bật bot", "bat bot"]:
                    BOT_ENABLED = True
                    send_text(user_id, "Dạ em đã bật tư vấn tự động, anh/chị nhắn giúp em mã hoặc mẫu cần xem ạ.")
                    continue

            if not BOT_ENABLED:
                continue

            # Trường hợp KHÁCH GỬI ẢNH (KHÔNG CÓ TEXT)
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
                        intro_product(user_id, rows, ms_img, msg="Khách gửi ảnh sản phẩm, giới thiệu giúp em.")
                    else:
                        send_text(user_id, "Em chưa tìm thấy sản phẩm giống hình anh/chị gửi ạ.")
                else:
                    send_text(
                        user_id,
                        "Em chưa nhận diện được sản phẩm từ hình anh/chị gửi ạ, anh/chị gửi giúp em mã hoặc link bài viết được không ạ?",
                    )
                continue

            # TỪ ĐÂY TRỞ ĐI: CÓ TEXT (có thể kèm ảnh)
            ctx = get_ctx(user_id)
            history = ctx.get("history", [])
            history.append(("user", text))
            ctx["history"] = history[-10:]
            USER_CONTEXT[user_id] = ctx

            current_ms = ctx.get("current_ms")
            print(f"[CTX] current_ms={current_ms}")

            # 1. MÃ RÕ RÀNG TỪ TIN NHẮN KHÁCH (ưu tiên cao nhất)
            explicit_ms = (
                extract_ms_from_hashtag(text)
                or extract_ms(text)
                or guess_ms(text)
            )

            if explicit_ms:
                rows = find_product(explicit_ms)
                if rows is None:
                    send_text(user_id, f"Không tìm thấy sản phẩm {explicit_ms} ạ.")
                else:
                    set_ctx(user_id, current_ms=explicit_ms)
                    intro_product(user_id, rows, explicit_ms, msg=text)
                continue

            # 2. DÙNG GPT PHÂN TÍCH NGỮ CẢNH (KHÔNG DÙNG RULE)
            final_ms, reply = gpt_reply_for_user(user_id, text)

            # Cập nhật lại context với mã mới (nếu có)
            ctx = get_ctx(user_id)
            if final_ms:
                ctx["current_ms"] = final_ms
            USER_CONTEXT[user_id] = ctx

            # Thêm lời chào ở tin nhắn trả lời ĐẦU TIÊN
            if not ctx.get("introduced", False):
                intro = "Dạ em chào anh/chị ạ 😊 Em là trợ lý bán hàng của shop."
                full_reply = intro + "\n" + reply
                ctx["introduced"] = True
                USER_CONTEXT[user_id] = ctx
            else:
                full_reply = reply

            send_text(user_id, full_reply)

            # Lưu lại vào history
            ctx = get_ctx(user_id)
            history = ctx.get("history", [])
            history.append(("assistant", full_reply))
            ctx["history"] = history[-10:]
            USER_CONTEXT[user_id] = ctx

    return "OK", 200


# --------------------------
# API GET PRODUCT
# --------------------------
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

    return {
        "ms": ms,
        "name": row0.get("Tên sản phẩm", ""),
        "price": row0.get("Giá bán", 0),
        "desc": row0.get("Mô tả", ""),
        "image": image,
    }


# --------------------------
# STATIC + ROOT
# --------------------------
@app.route("/")
def index():
    return redirect("/static/index.html")


@app.route("/static/<path:filename>")
def serve_static(filename):
    return send_from_directory("static", filename)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=True)
