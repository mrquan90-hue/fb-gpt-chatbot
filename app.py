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

SHEET_CSV_URL = os.getenv(
    "SHEET_CSV_URL",
    "https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv"
)

client = OpenAI(api_key=OPENAI_API_KEY)
app = Flask(__name__)

# ============================================
# GLOBAL VARIABLES
# ============================================
PRODUCTS = {}
LAST_LOAD = 0
LOAD_TTL = 300  # 5 phút reload

BOT_ENABLED = True
RECENT_MIDS = deque(maxlen=300)
USER_CONTEXT = {}   # chống double reply + lưu state

# ============================================
# UTILS
# ============================================
def normalize(text):
    return (text or "").lower().strip()


def has_chinese(s: str):
    if not s:
        return False
    for ch in s:
        if "\u4e00" <= ch <= "\u9fff":
            return True
    return False


def split_images(cell):
    if not cell:
        return []
    parts = re.split(r"[\n,; ]+", cell.strip())
    return [p for p in parts if p.startswith("http")]


def filter_images(urls):
    """Loại trùng + watermark chữ Trung."""
    seen = set()
    clean = []
    for u in urls:
        if not u.startswith("http"):
            continue
        if u in seen:
            continue
        seen.add(u)
        if has_chinese(u):
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
        "messaging_type": "RESPONSE"
    }
    params = {"access_token": PAGE_ACCESS_TOKEN}
    try:
        r = requests.post(url, json=payload, params=params, timeout=20)
        print("[SEND_TEXT]", r.status_code, r.text)
    except Exception as e:
        print("[FB ERROR TEXT]", e)


def send_image(psid, img_url):
    url = "https://graph.facebook.com/v19.0/me/messages"
    payload = {
        "recipient": {"id": psid},
        "message": {
            "attachment": {
                "type": "image",
                "payload": {"url": img_url, "is_reusable": False}
            }
        }
    }
    params = {"access_token": PAGE_ACCESS_TOKEN}

    try:
        r = requests.post(url, json=payload, params=params, timeout=25)
        print("[SEND_IMAGE]", r.status_code, r.text)
    except Exception as e:
        print("[FB ERROR IMAGE]", e)

# ============================================
# LOAD SHEET
# ============================================
def load_products(force=False):
    global PRODUCTS, LAST_LOAD

    now = time.time()
    if not force and PRODUCTS and (now - LAST_LOAD < LOAD_TTL):
        return

    print("[SHEET] Loading sheet...")

    try:
        resp = requests.get(SHEET_CSV_URL, timeout=30)
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
        print(f"[SHEET] Loaded {len(PRODUCTS)} products")

    except Exception as e:
        print("[SHEET ERROR]", e)

# ============================================
# PRODUCT SEARCH
# ============================================
def find_by_code(text):
    msg = normalize(text)
    tokens = msg.split()

    load_products()

    # Tìm theo mã sản phẩm
    for pid, rows in PRODUCTS.items():
        if normalize(pid) in tokens:
            return pid, rows

    # Tìm theo mã mẫu mã
    for pid, rows in PRODUCTS.items():
        for r in rows:
            v = normalize(r.get("Mã mẫu mã") or "")
            if v and v in tokens:
                return pid, rows

    return None, None


def score_product(rows, text):
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


def find_best_product(text):
    pid, rows = find_by_code(text)
    if pid:
        return pid, rows

    load_products()
    best_pid = None
    best_rows = None
    best_score = 0

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
# PRICE
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
        return "Hiện sản phẩm chưa có thông tin giá."

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

# ============================================
# GPT summary + CTA
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
            temperature=0.5
        )
        content = resp.choices[0].message.content

        parts = re.split(r"\[CTA\]", content, flags=re.IGNORECASE)
        if len(parts) == 2:
            advantages = re.sub(r"\[ƯU ĐIỂM\]", "", parts[0], flags=re.IGNORECASE).strip()
            cta = parts[1].strip()
            return advantages, cta

        # fallback
        return (
            "Sản phẩm có thiết kế đẹp, chất liệu bền và phù hợp nhiều nhu cầu.",
            "Anh/chị chọn giúp shop màu/size để em hỗ trợ ạ!"
        )

    except Exception as e:
        print("[GPT ERROR]", e)
        return (
            "Sản phẩm có thiết kế đẹp và sử dụng tiện lợi.",
            "Anh/chị ưng mẫu nào shop chốt đơn giúp ạ!"
        )

# ============================================
# SEND PRODUCT CONSULT
# ============================================
def send_product_consult(psid, rows, user_text):
    base = rows[0]
    name = base.get("Tên sản phẩm") or "Sản phẩm"
    desc = base.get("Mô tả") or ""

    send_text(psid, name)

    # =======================
    # GỬI ẢNH KHÔNG TRÙNG
    # =======================
    sent_images = set()

    all_urls = []
    for r in rows:
        all_urls.extend(split_images(r.get("Images") or ""))

    all_urls = filter_images(all_urls)

    for img in all_urls:
        if img in sent_images:
            continue
        sent_images.add(img)
        send_image(psid, img)

    # =======================
    # ƯU ĐIỂM + CTA
    # =======================
    advantages, cta = generate_summary_and_cta(name, desc, user_text)

    # =======================
    # GIÁ
    # =======================
    price_groups = group_by_price(rows)
    price_text = format_price_output(price_groups)

    final_msg = f"{advantages}\n\n{price_text}\n\n{cta}"
    send_text(psid, final_msg)

# ============================================
# HANDLE MESSAGE
# ============================================
def handle_message(psid, message):

    text = message.get("text")
    attachments = message.get("attachments")

    global BOT_ENABLED

    # ========== BOT ON/OFF ==========
    if text:
        t = normalize(text)
        if "tắt bot" in t:
            BOT_ENABLED = False
            send_text(psid, "Bot đã tạm dừng.")
            return
        if "bật bot" in t:
            BOT_ENABLED = True
            send_text(psid, "Bot đã bật lại.")
            return

    if not BOT_ENABLED:
        return

    # ========== Nếu khách gửi ảnh ==========
    if attachments:
        send_text(psid, "Shop đã nhận được ảnh ạ. Anh/chị mô tả nhu cầu giúp shop nhé!")
        return

    # ========== Anti-empty ==========
    if not text:
        send_text(psid, "Anh/chị mô tả giúp shop đang tìm gì để em hỗ trợ ạ.")
        return

    # ============================================
    # ANTI DOUBLE REPLY (fix gửi 2 lần)
    # ============================================
    now = time.time()
    key = f"{psid}:{text}"
    last = USER_CONTEXT.get("last_msg", {})

    if last.get("key") == key and now - last.get("time", 0) < 3:
        print("[SKIP] duplicate reply within 3s")
        return

    USER_CONTEXT["last_msg"] = {"key": key, "time": now}

    # ============================================
    # SEARCH PRODUCT
    # ============================================
    pid, rows = find_best_product(text)
    if not pid:
        send_text(psid, "Shop chưa tìm thấy đúng mẫu. Anh/chị mô tả rõ hơn giúp shop ạ ❤️")
        return

    USER_CONTEXT[psid] = {"last_product": pid}

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
        for event in entry.get("messaging", []):

            # SKIP delivery/read
            if "delivery" in event or "read" in event:
                print("[SKIP] delivery/read")
                continue

            message = event.get("message")
            if not message:
                continue

            # SKIP echo
            if message.get("is_echo"):
                print("[SKIP] echo")
                continue

            psid = event["sender"]["id"]
            mid = message.get("mid")

            # ANTI-LOOP MID
            if mid in RECENT_MIDS:
                print("[SKIP] duplicate MID")
                continue
            RECENT_MIDS.append(mid)

            handle_message(psid, message)

    return "OK", 200


@app.route("/")
def home():
    return "Chatbot OK", 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=True)
