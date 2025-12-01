import os
import time
import csv
import io
import re
from collections import defaultdict, deque
import threading

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
# GLOBAL STATE
# ============================================
PRODUCTS = {}
LAST_LOAD = 0
LOAD_TTL = 300  # 5 phút

BOT_ENABLED = True           # trạng thái ON/OFF
RECENT_MIDS = deque(maxlen=500)  # tăng kích thước chống lặp MID
RECENT_MIDS_LOCK = threading.Lock()  # khóa để thread-safe
PROCESSED_MIDS = set()       # MID đã xử lý (tránh retry)
PROCESSED_MIDS_LOCK = threading.Lock()
USER_CONTEXT = {}            # chống double-reply + context

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
    """Loại ảnh trùng + có watermark chữ Trung Quốc trong URL."""
    seen = set()
    clean = []
    for u in urls:
        if not u.startswith("http"):
            continue
        if u in seen:
            continue
        seen.add(u)
        if has_chinese(u):
            # chỉ loại ảnh có chữ TQ trong URL (watermark TQ)
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

    # ưu tiên tìm theo mã sản phẩm
    for pid, rows in PRODUCTS.items():
        if normalize(pid) in tokens:
            return pid, rows

    # sau đó tìm theo mã mẫu mã
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
        return "Hiện sản phẩm chưa có thông tin giá."

    # tất cả biến thể cùng 1 giá
    if len(groups) == 1:
        price = next(iter(groups.keys()))
        return f"Giá đặc biệt ưu đãi cho anh/chị hôm nay là: {price}."

    # nhiều mức giá
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
            temperature=0.5,
            timeout=30.0  # thêm timeout
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

    # 1. Tên sản phẩm
    send_text(psid, name)

    # 2. Gửi toàn bộ ảnh (loại trùng + watermark TQ)
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

    # 3. Ưu điểm + CTA
    advantages, cta = generate_summary_and_cta(name, desc, user_text)

    # 4. Giá
    price_groups = group_by_price(rows)
    price_text = format_price_output(price_groups)

    final_msg = f"{advantages}\n\n{price_text}\n\n{cta}"
    send_text(psid, final_msg)

# ============================================
# XỬ LÝ LỆNH TỪ PAGE (cho phép tắt/bật bot từ Page)
# ============================================
def handle_page_command(psid, text):
    """Xử lý lệnh từ Page (không phải từ khách)"""
    global BOT_ENABLED
    
    t = normalize(text)
    
    off_keywords = [
        "tắt bot", "tat bot",
        "dừng bot", "dung bot",
        "stop bot", "off bot",
        "tắt chatbot", "tat chatbot"
    ]
    on_keywords = [
        "bật bot", "bat bot",
        "bật lại bot", "bat lai bot",
        "start bot", "on bot",
        "mở bot", "mo bot"
    ]
    
    if any(k in t for k in off_keywords):
        BOT_ENABLED = False
        print(f"[BOT] switched OFF by PAGE (psid={psid}), text={text}")
        send_text(psid, "✅ Bot đã TẠM DỪNG và sẽ không tự trả lời nữa.\nKhi cần bật lại, anh/chị nhắn: \"Bật bot\" giúp shop ạ.")
        return True
        
    if any(k in t for k in on_keywords):
        BOT_ENABLED = True
        print(f"[BOT] switched ON by PAGE (psid={psid}), text={text}")
        send_text(psid, "✅ Bot đã BẬT LẠI, em sẽ hỗ trợ khách tự động nhé.")
        return True
    
    return False

# ============================================
# HANDLE MESSAGE (có ON/OFF)
# ============================================
def handle_message(psid, message, is_from_page=False):
    global BOT_ENABLED

    text = message.get("text")
    attachments = message.get("attachments")

    # 1) Nếu là tin từ Page, kiểm tra lệnh tắt/bật bot
    if is_from_page and text:
        if handle_page_command(psid, text):
            return

    # 2) Xử lý lệnh từ khách (chỉ khi bot đang ON)
    if text:
        t = normalize(text)

        off_keywords = [
            "tắt bot", "tat bot",
            "dừng bot", "dung bot",
            "stop bot", "off bot",
            "tắt chatbot", "tat chatbot"
        ]
        on_keywords = [
            "bật bot", "bat bot",
            "bật lại bot", "bat lai bot",
            "start bot", "on bot",
            "mở bot", "mo bot"
        ]

        if any(k in t for k in off_keywords):
            BOT_ENABLED = False
            print("[BOT] switched OFF by", psid, "text=", text)
            send_text(psid, "✅ Bot đã TẠM DỪNG và sẽ không tự trả lời nữa.\nKhi cần bật lại, anh/chị nhắn: \"Bật bot\" giúp shop ạ.")
            return

        if any(k in t for k in on_keywords):
            BOT_ENABLED = True
            print("[BOT] switched ON by", psid, "text=", text)
            send_text(psid, "✅ Bot đã BẬT LẠI, em sẽ hỗ trợ khách tự động nhé.")
            return

    # 3) Nếu bot đang OFF -> chỉ log rồi thoát, KHÔNG trả lời
    if not BOT_ENABLED:
        print("[BOT OFF] ignore message from", psid, "text=", text)
        return

    # 4) Khách gửi ảnh
    if attachments:
        send_text(psid, "Shop đã nhận được ảnh ạ. Anh/chị mô tả nhu cầu giúp shop nhé!")
        return

    # 5) Không có text
    if not text:
        send_text(psid, "Anh/chị mô tả giúp shop đang tìm gì để em hỗ trợ ạ.")
        return

    # 6) Anti double reply theo cùng 1 tin trong 3 giây
    now = time.time()
    key = f"{psid}:{text}"
    last = USER_CONTEXT.get("last_msg", {})
    if last.get("key") == key and now - last.get("time", 0) < 3:
        print("[SKIP] duplicate reply within 3s")
        return
    USER_CONTEXT["last_msg"] = {"key": key, "time": now}

    # 7) Tìm sản phẩm và tư vấn
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
            
            # Kiểm tra sender ID để biết là tin từ Page hay từ khách
            sender_id = event.get("sender", {}).get("id", "")
            recipient_id = event.get("recipient", {}).get("id", "")
            
            # Nếu sender là Page ID và recipient là User ID -> tin Page gửi cho User
            # Trong trường hợp này, chúng ta cần xử lý lệnh từ Page
            is_from_page = False
            page_id = "516937221685203"  # Page ID từ log
            
            if sender_id == page_id:
                # Tin Page gửi cho User
                is_from_page = True
            elif recipient_id == page_id:
                # Tin User gửi cho Page
                is_from_page = False
            else:
                # Không xác định, mặc định là từ khách
                is_from_page = False

            # Bỏ qua delivery/read
            if "delivery" in event or "read" in event:
                print("[SKIP] delivery/read")
                continue

            message = event.get("message")
            if not message:
                continue

            # KHÔNG bỏ qua echo nếu là tin từ Page gửi lệnh
            # Chỉ bỏ qua echo thông thường (tin do bot tự gửi)
            if message.get("is_echo") and not is_from_page:
                print("[SKIP] echo (non-command)")
                continue

            psid = event["sender"]["id"]
            mid = message.get("mid")

            # Anti-loop theo MID với thread-safe lock
            with PROCESSED_MIDS_LOCK:
                if mid in PROCESSED_MIDS:
                    print("[SKIP] duplicate MID (already processed)")
                    continue
                PROCESSED_MIDS.add(mid)
                
            # Giới hạn số lượng MID lưu trữ
            with PROCESSED_MIDS_LOCK:
                if len(PROCESSED_MIDS) > 1000:
                    # Giữ lại 500 phần tử gần nhất
                    items = list(PROCESSED_MIDS)
                    PROCESSED_MIDS.clear()
                    PROCESSED_MIDS.update(items[-500:])

            # Thêm vào RECENT_MIDS để chống retry
            with RECENT_MIDS_LOCK:
                RECENT_MIDS.append(mid)

            # Xử lý tin nhắn
            handle_message(psid, message, is_from_page)

    return "OK", 200

# ============================================
# CLEANUP THREAD
# ============================================
def cleanup_processed_mids():
    """Dọn dẹp PROCESSED_MIDS định kỳ để tránh memory leak"""
    while True:
        time.sleep(3600)  # Mỗi giờ dọn dẹp 1 lần
        with PROCESSED_MIDS_LOCK:
            if len(PROCESSED_MIDS) > 500:
                items = list(PROCESSED_MIDS)
                PROCESSED_MIDS.clear()
                PROCESSED_MIDS.update(items[-500:])
                print(f"[CLEANUP] Reduced PROCESSED_MIDS from {len(items)} to 500")

# ============================================
# HOME
# ============================================
@app.route("/")
def home():
    return "Chatbot OK", 200


if __name__ == "__main__":
    # Khởi chạy thread dọn dẹp
    cleanup_thread = threading.Thread(target=cleanup_processed_mids, daemon=True)
    cleanup_thread.start()
    
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)  # Đặt debug=False cho production
