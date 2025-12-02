import os
import time
import csv
import io
import re
from collections import defaultdict, deque

import requests
from flask import Flask, request
from openai import OpenAI

# ============================================
# CONFIG
# ============================================
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "YOUR_VERIFY_TOKEN")
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN", "YOUR_PAGE_ACCESS_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "YOUR_OPENAI_API_KEY")

# ID fanpage của bạn
PAGE_ID = "516937221685203"

# Link CSV dữ liệu sản phẩm
SHEET_CSV_URL = os.getenv(
    "SHEET_CSV_URL",
    "https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv"
)

client = OpenAI(api_key=OPENAI_API_KEY)

app = Flask(__name__)

# ============================================
# GLOBAL STATE
# ============================================
PRODUCTS = {}
LAST_LOAD = 0
LOAD_TTL = 300  # cache sheet 5 phút

BOT_ENABLED = True

# Lưu MID đã xử lý để tránh xử lý trùng (Facebook retry)
RECENT_MIDS = deque(maxlen=500)

# Lưu trạng thái theo từng khách
# USER_CONTEXT[psid] = {
#   "state": "FROM_POST" | "GENERAL",
#   "product_id": "...",
#   "post_id": "...",
#   "last_ts": timestamp,
#   "last_msg": {"key": text, "time": timestamp}
# }
USER_CONTEXT = {}

# Cache caption bài viết: post_id -> {caption, time}
POST_CACHE = {}
POST_CACHE_TTL = 600  # 10 phút


# ============================================
# UTILS
# ============================================
def normalize(text: str) -> str:
    return (text or "").lower().strip()


def has_chinese(s: str) -> bool:
    if not s:
        return False
    for ch in s:
        if "\u4e00" <= ch <= "\u9fff":
            return True
    return False


def split_images(cell: str):
    if not cell:
        return []
    parts = re.split(r"[\n,; ]+", cell.strip())
    return [p for p in parts if p.startswith("http")]


def filter_images(urls):
    """
    - Bỏ trùng
    - Bỏ ảnh có watermark chữ Trung (URL chứa ký tự Chinese)
    - GIỮ domain Trung Quốc, GIỮ ảnh hơi mờ
    """
    seen = set()
    clean = []
    for u in urls:
        if not u.startswith("http"):
            continue
        if u in seen:
            continue
        seen.add(u)
        if has_chinese(u):
            # loại url có chữ TQ (thường là watermark)
            continue
        clean.append(u)
    return clean


# ============================================
# FACEBOOK SEND API
# ============================================
def send_text(psid, text):
    url = "https://graph.facebook.com/v19.0/me/messages"
    payload = {
        "recipient": {"id": psid},
        "message": {"text": text},
        "messaging_type": "RESPONSE",
    }
    params = {"access_token": PAGE_ACCESS_TOKEN}
    try:
        r = requests.post(url, json=payload, params=params, timeout=20)
        print("[SEND_TEXT]", r.status_code)
    except Exception as e:
        print("[FB ERROR TEXT]", e)


def send_image(psid, img_url):
    url = "https://graph.facebook.com/v19.0/me/messages"
    payload = {
        "recipient": {"id": psid},
        "message": {
            "attachment": {
                "type": "image",
                "payload": {"url": img_url, "is_reusable": False},
            }
        },
    }
    params = {"access_token": PAGE_ACCESS_TOKEN}
    try:
        r = requests.post(url, json=payload, params=params, timeout=20)
        print("[SEND_IMAGE]", r.status_code)
    except Exception as e:
        print("[FB ERROR IMAGE]", e)


# ============================================
# LOAD PRODUCTS TỪ SHEET
# ============================================
def load_products(force=False):
    global PRODUCTS, LAST_LOAD
    now = time.time()

    if not force and PRODUCTS and (now - LAST_LOAD < LOAD_TTL):
        return

    print("[SHEET] Reloading...")

    try:
        resp = requests.get(SHEET_CSV_URL, timeout=20)
        resp.encoding = "utf-8"
        f = io.StringIO(resp.text)
        reader = csv.DictReader(f)

        tmp = defaultdict(list)
        for row in reader:
            pid = (row.get("Mã sản phẩm") or "").strip()
            if pid:
                tmp[pid].append(row)

        PRODUCTS = dict(tmp)
        LAST_LOAD = now
        print("[SHEET] Loaded", len(PRODUCTS), "products")
    except Exception as e:
        print("[SHEET ERROR]", e)


# ============================================
# TÌM SẢN PHẨM
# ============================================
def score_product(rows, text: str) -> int:
    q = normalize(text)
    if not q:
        return 0

    base = rows[0]
    fields = [
        base.get("Tên sản phẩm") or "",
        base.get("Keyword sản phẩm") or "",
        base.get("Danh mục") or "",
        base.get("Thương hiệu") or "",
    ]
    full = normalize(" ".join(fields))

    score = 0
    for w in q.split():
        if len(w) >= 3 and w in full:
            score += 1
    return score


def find_best_product_by_text(text):
    load_products()
    best_pid, best_rows, best_score = None, None, 0
    for pid, rows in PRODUCTS.items():
        s = score_product(rows, text)
        if s > best_score:
            best_score = s
            best_pid = pid
            best_rows = rows
    if best_score == 0:
        return None, None
    return best_pid, best_rows


# ============================================
# NHÓM GIÁ
# ============================================
def group_by_price(rows):
    groups = defaultdict(lambda: {"colors": set(), "sizes": set()})
    for r in rows:
        price = (r.get("Giá bán") or "").strip()
        if not price:
            continue
        color = (r.get("màu (Thuộc tính)") or "").strip()
        size = (r.get("size (Thuộc tính)") or "").strip()
        groups[price]["colors"].add(color)
        if size:
            groups[price]["sizes"].add(size)
    return groups


def format_price_output(groups):
    if not groups:
        return "Hiện sản phẩm chưa có giá."

    if len(groups) == 1:
        price = next(iter(groups.keys()))
        return f"Giá ưu đãi cho anh/chị hôm nay là: {price}."

    lines = []
    for price, info in groups.items():
        colors = ", ".join(sorted(c for c in info["colors"] if c)) or "Nhiều màu"
        if info["sizes"]:
            sizes = ", ".join(sorted(info["sizes"]))
            lines.append(f"{colors} (size {sizes}) giá {price}.")
        else:
            lines.append(f"{colors} giá {price}.")
    return "\n".join(lines)


# ============================================
# GPT: ƯU ĐIỂM + CTA
# ============================================
def generate_summary_and_cta(name, desc, user_msg):
    prompt = f"""
Hãy viết:
1) 2–3 câu ưu điểm nổi bật từ mô tả
2) 1 câu CTA

Tên sản phẩm: {name}
Mô tả: {desc}
Tin khách: {user_msg}

Định dạng:
[ƯU ĐIỂM]
...
[CTA]
...
"""
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.5,
        )
        content = resp.choices[0].message.content

        parts = re.split(r"\[CTA\]", content, flags=re.IGNORECASE)
        if len(parts) != 2:
            return (
                "Sản phẩm có thiết kế đẹp và bền, phù hợp nhiều nhu cầu sử dụng.",
                "Anh/chị chọn giúp shop mẫu ưng ý để em hỗ trợ ạ!",
            )

        advantages = re.sub(
            r"\[ƯU ĐIỂM\]", "", parts[0], flags=re.IGNORECASE
        ).strip()
        cta = parts[1].strip()
        return advantages, cta

    except Exception as e:
        print("[GPT ERROR]", e)
        return (
            "Sản phẩm chất lượng tốt, mẫu mã hiện đại.",
            "Anh/chị muốn chọn mẫu nào để shop chốt đơn giúp ạ?",
        )


# ============================================
# LẤY MÃ SẢN PHẨM TỪ CAPTION [MSxxxxxx]
# ============================================
def extract_product_code_from_text(text: str):
    if not text:
        return None
    m = re.search(r"\[?(MS\d+)\]?", text, flags=re.IGNORECASE)
    if m:
        return m.group(1).upper()
    return None


def fetch_post_caption(post_id: str) -> str:
    now = time.time()
    if post_id in POST_CACHE:
        data = POST_CACHE[post_id]
        if now - data["time"] < POST_CACHE_TTL:
            return data["caption"]

    try:
        url = f"https://graph.facebook.com/v19.0/{post_id}"
        params = {
            "fields": "message,story",
            "access_token": PAGE_ACCESS_TOKEN,
        }
        r = requests.get(url, params=params, timeout=10)
        j = r.json()
        caption = (j.get("message") or j.get("story") or "")
        POST_CACHE[post_id] = {"caption": caption, "time": now}
        print("[POST] caption for", post_id, "=", caption[:100])
        return caption
    except Exception as e:
        print("[POST ERROR]", e)
        return ""


def resolve_product_from_post(post_id: str):
    if not post_id:
        return None, None

    load_products()
    caption = fetch_post_caption(post_id)
    if not caption:
        return None, None

    # 1) Thử lấy mã [MSxxxxxx] trong caption/hashtag
    code = extract_product_code_from_text(caption)
    if code and code in PRODUCTS:
        return code, PRODUCTS[code]

    # 2) Fallback: dùng caption để tìm sản phẩm phù hợp
    return find_best_product_by_text(caption)


# ============================================
# GỬI GÓI TƯ VẤN SẢN PHẨM
# ============================================
def send_product_consult(psid, rows, user_text):
    """
    Cấu trúc:
    Tên sản phẩm
    ↓
    Ảnh chung (tối đa 5 ảnh, lọc trùng + watermark TQ)
    ↓
    Ưu điểm nổi bật
    ↓
    Giá bán (gộp theo nhóm giá)
    ↓
    CTA
    """
    base = rows[0]
    name = base.get("Tên sản phẩm") or "Sản phẩm"
    desc = base.get("Mô tả") or ""

    # 1. Tên sản phẩm
    send_text(psid, name)

    # 2. Ảnh chung: gom tất cả ảnh từ các dòng cùng mã sản phẩm
    all_urls = []
    for r in rows:
        all_urls.extend(split_images(r.get("Images") or ""))

    # Lọc trùng + bỏ watermark TQ + giới hạn 5 ảnh
    all_urls = filter_images(all_urls)
    all_urls = all_urls[:5]

    sent_images = set()
    for img in all_urls:
        if img in sent_images:
            continue
        sent_images.add(img)
        send_image(psid, img)

    # 3. Ưu điểm + CTA
    advantages, cta = generate_summary_and_cta(name, desc, user_text)

    # 4. Giá
    price_groups = group_by_price(rows)
    price_text = format_price_output(price_groups)

    # 5. Tổng hợp
    final_text = f"{advantages}\n\n{price_text}\n\n{cta}"
    send_text(psid, final_text)


# ============================================
# HANDLE MESSAGE (CHAT)
# ============================================
def handle_message(psid, message, meta=None):
    """
    meta: {"from_post": post_id} nếu có
    """
    global BOT_ENABLED, USER_CONTEXT

    text = message.get("text")
    attachments = message.get("attachments")

    # ===== 1. Lệnh BẬT/TẮT BOT luôn được xử lý đầu tiên =====
    if text:
        t = normalize(text)
        if any(
            k in t
            for k in ["tắt bot", "tat bot", "dừng bot", "dung bot", "stop bot", "off bot"]
        ):
            BOT_ENABLED = False
            send_text(
                psid,
                "🔴 Bot đã TẮT. Em sẽ không tự trả lời nữa.\nĐể bật lại anh/chị nhắn: Bật bot",
            )
            return

        if any(k in t for k in ["bật bot", "bat bot", "start bot", "on bot", "bat lai"]):
            BOT_ENABLED = True
            send_text(psid, "🟢 Bot đã BẬT LẠI. Em sẵn sàng hỗ trợ khách!")
            return

    # ===== 2. Nếu bot đang OFF → bỏ qua toàn bộ =====
    if not BOT_ENABLED:
        print("[BOT OFF] skip message from", psid)
        return

    # Lấy context user
    ctx = USER_CONTEXT.get(psid, {})
    now = time.time()
    state = ctx.get("state", "GENERAL")
    product_id = ctx.get("product_id")

    # ===== 3. Nếu meta báo khách đến từ bài viết (luồng 1) =====
    if meta and meta.get("from_post"):
        post_id = meta["from_post"]
        pid, rows = resolve_product_from_post(post_id)
        if pid and rows:
            state = "FROM_POST"
            product_id = pid
            ctx["state"] = state
            ctx["product_id"] = pid
            ctx["post_id"] = post_id
            ctx["last_ts"] = now
            USER_CONTEXT[psid] = ctx

            send_product_consult(psid, rows, text or "")
            return
        else:
            # không tìm được theo caption → chuyển sang GENERAL
            state = "GENERAL"
            ctx["state"] = state
            USER_CONTEXT[psid] = ctx

    # ===== 4. Khách gửi ảnh =====
    if attachments:
        send_text(
            psid,
            "Shop đã nhận được ảnh ạ. Anh/chị mô tả thêm nhu cầu để em tư vấn đúng mẫu nhất nhé!",
        )
        ctx["last_ts"] = now
        USER_CONTEXT[psid] = ctx
        return

    # ===== 5. Không có text =====
    if not text:
        send_text(psid, "Anh/chị mô tả giúp shop đang tìm gì để em hỗ trợ ạ.")
        ctx["last_ts"] = now
        USER_CONTEXT[psid] = ctx
        return

    # ===== 6. Anti double-reply theo user (3 giây) =====
    last_msg = ctx.get("last_msg")
    key = text
    if last_msg and last_msg.get("key") == key and now - last_msg.get("time", 0) < 3:
        print("[SKIP] duplicate text for same user in 3s")
        return
    ctx["last_msg"] = {"key": key, "time": now}

    # ===== 7. Nếu đã biết product_id từ context → tư vấn tiếp sản phẩm đó =====
    load_products()
    if product_id and product_id in PRODUCTS:
        rows = PRODUCTS[product_id]
        send_product_consult(psid, rows, text)
        ctx["last_ts"] = now
        USER_CONTEXT[psid] = ctx
        return

    # ===== 8. Luồng 3: Khách GENERAL, tìm sản phẩm theo nội dung chat =====
    pid, rows = find_best_product_by_text(text)
    if not pid:
        send_text(
            psid,
            "Shop chưa tìm thấy mẫu phù hợp. Anh/chị mô tả rõ hơn (loại sản phẩm, màu, size...) giúp shop ạ ❤️",
        )
        ctx["last_ts"] = now
        USER_CONTEXT[psid] = ctx
        return

    ctx["state"] = "GENERAL"
    ctx["product_id"] = pid
    ctx["last_ts"] = now
    USER_CONTEXT[psid] = ctx

    send_product_consult(psid, rows, text)


# ============================================
# WEBHOOK
# ============================================
@app.route("/webhook", methods=["GET"])
def verify():
    if request.args.get("hub.verify_token") == VERIFY_TOKEN:
        return request.args.get("hub.challenge")
    return "Sai verify token", 403


@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json()
    print("[WEBHOOK]", data)

    if data.get("object") != "page":
        return "IGNORE", 200

    for entry in data.get("entry", []):
        # TODO: sau này xử lý thêm entry["changes"] cho luồng comment
        # changes = entry.get("changes", [])

        for event in entry.get("messaging", []):
            # ----- SKIP delivery / read / reaction -----
            if "delivery" in event or "read" in event or "reaction" in event:
                print("[SKIP] delivery/read/reaction")
                continue

            sender_id = event.get("sender", {}).get("id")
            if not sender_id:
                continue

            # ----- SKIP mọi event từ chính PAGE -----
            if sender_id == PAGE_ID:
                print("[SKIP] sender is PAGE")
                continue

            message = event.get("message")
            postback = event.get("postback")
            referral = (
                event.get("referral")
                or (postback or {}).get("referral")
                or (message or {}).get("referral")
            )

            # ----- SKIP echo -----
            if message and message.get("is_echo"):
                print("[SKIP] is_echo")
                continue

            # ----- DEDUPE theo MID (Facebook retry) -----
            mid = None
            if message:
                mid = message.get("mid")
            if not mid:
                mid = event.get("mid")

            if mid:
                if mid in RECENT_MIDS:
                    print("[SKIP] duplicate MID")
                    continue
                RECENT_MIDS.append(mid)

            # ----- Chuẩn bị meta để biết khách đến từ bài viết nào -----
            meta = {}
            if referral:
                post_id = referral.get("referer_uri") or referral.get("post_id")
                # referer_uri có thể là URL: .../posts/<post_id>
                if isinstance(post_id, str) and "posts" in post_id:
                    m = re.search(r"/posts/(\d+)", post_id)
                    if m:
                        post_id = m.group(1)
                if isinstance(post_id, str) and post_id.isdigit():
                    meta["from_post"] = post_id

            if message:
                handle_message(sender_id, message, meta=meta)

    return "OK", 200


@app.route("/")
def home():
    return "Chatbot running OK", 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=True)
