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

PAGE_ID = "516937221685203"   # ID Fanpage của bạn

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
LOAD_TTL = 300

BOT_ENABLED = True

RECENT_MIDS = deque(maxlen=500)
RECENT_MESSAGES = deque(maxlen=500)
USER_CONTEXT = {}

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
    """Remove duplicate images + watermark Chinese."""
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
                "payload": {"url": img_url, "is_reusable": False}
            }
        }
    }
    params = {"access_token": PAGE_ACCESS_TOKEN}
    try:
        r = requests.post(url, json=payload, params=params, timeout=20)
        print("[SEND_IMAGE]", r.status_code)
    except Exception as e:
        print("[FB ERROR IMAGE]", e)

# ============================================
# LOAD PRODUCTS
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

    for pid, rows in PRODUCTS.items():
        if normalize(pid) in tokens:
            return pid, rows

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
# PRICE GROUP
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
        colors = ", ".join(sorted(info["colors"])) if info["colors"] else "Nhiều màu"
        if info["sizes"]:
            sizes = ", ".join(sorted(info["sizes"]))
            lines.append(f"{colors} (size {sizes}) giá {price}.")
        else:
            lines.append(f"{colors} giá {price}.")
    return "\n".join(lines)

# ============================================
# GPT SUMMARY + CTA
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
        if len(parts) != 2:
            return (
                "Sản phẩm có thiết kế đẹp và bền, phù hợp nhiều nhu cầu sử dụng.",
                "Anh/chị chọn giúp shop mẫu ưng ý để em hỗ trợ ạ!"
            )

        advantages = re.sub(r"\[ƯU ĐIỂM\]", "", parts[0], flags=re.IGNORECASE).strip()
        cta = parts[1].strip()
        return advantages, cta

    except Exception as e:
        print("[GPT ERROR]", e)
        return (
            "Sản phẩm chất lượng tốt, mẫu mã hiện đại.",
            "Anh/chị muốn chọn mẫu nào để shop chốt đơn giúp ạ?"
        )

# ============================================
# SEND PRODUCT PACKAGE
# ============================================
def send_product_consult(psid, rows, user_text):
    base = rows[0]
    name = base.get("Tên sản phẩm") or "Sản phẩm"
    desc = base.get("Mô tả") or ""

    send_text(psid, name)

    # IMAGE BLOCK — NO DUPLICATE
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

    advantages, cta = generate_summary_and_cta(name, desc, user_text)

    price_groups = group_by_price(rows)
    price_text = format_price_output(price_groups)

    send_text(psid, f"{advantages}\n\n{price_text}\n\n{cta}")

# ============================================
# HANDLE MESSAGE
# ============================================
def handle_message(psid, message):
    global BOT_ENABLED

    text = message.get("text")
    attachments = message.get("attachments")

    # ===== BOT ON/OFF ALWAYS EXECUTED =====
    if text:
        t = normalize(text)
        if any(k in t for k in ["tắt bot", "tat bot", "dừng bot", "dung bot", "stop bot", "off bot"]):
            BOT_ENABLED = False
            send_text(psid, "🔴 Bot đã TẮT. Em sẽ không tự trả lời nữa.\nĐể bật lại nhắn: Bật bot")
            return

        if any(k in t for k in ["bật bot", "bat bot", "start bot", "on bot", "bat lai"]):
            BOT_ENABLED = True
            send_text(psid, "🟢 Bot đã BẬT LẠI. Em sẵn sàng hỗ trợ khách!")
            return

    # ===== BOT OFF: DO NOTHING =====
    if not BOT_ENABLED:
        print("[BOT OFF] Message skipped")
        return

    # ===== ATTACHMENTS =====
    if attachments:
        send_text(psid, "Shop đã nhận được ảnh ạ. Anh/chị mô tả nhu cầu giúp shop nhé!")
        return

    if not text:
        send_text(psid, "Anh/chị mô tả giúp shop đang tìm gì để em hỗ trợ ạ.")
        return

    # ===== ANTI DOUBLE REPLY (same text in 3s) =====
    now = time.time()
    key = f"{psid}:{text}"
    last = USER_CONTEXT.get("last_msg", {})

    if last.get("key") == key and now - last.get("time", 0) < 3:
        print("[SKIP] duplicate reply 3s")
        return

    USER_CONTEXT["last_msg"] = {"key": key, "time": now}

    # ===== FIND PRODUCT =====
    pid, rows = find_best_product(text)
    if not pid:
        send_text(psid, "Shop chưa tìm thấy mẫu phù hợp. Anh/chị mô tả rõ hơn giúp shop ạ ❤️")
        return

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

            # ===== FULL SKIP: delivery / read / reaction =====
            if "delivery" in event or "read" in event or "reaction" in event:
                return "OK", 200

            # ===== message field check =====
            message = event.get("message")
            if not message:
                return "OK", 200

            sender = event["sender"]["id"]

            # ===== FULL SKIP: ANY EVENT from PAGE =====
            if sender == PAGE_ID:
                print("[SKIP] sender is PAGE")
                return "OK", 200

            # ===== SKIP echo =====
            if message.get("is_echo"):
                print("[SKIP] echo event")
                return "OK", 200

            psid = sender
            mid = message.get("mid")

            # ===== SKIP duplicate MID =====
            if mid in RECENT_MIDS:
                print("[SKIP] duplicate MID")
                return "OK", 200
            RECENT_MIDS.append(mid)

            handle_message(psid, message)

    return "OK", 200


@app.route("/")
def home():
    return "Chatbot running OK", 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=True)
