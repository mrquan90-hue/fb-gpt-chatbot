# ============================================================
# FB - GPT CHATBOT — BẢN ỔN ĐỊNH NHẤT
# ============================================================
# Tính năng:
# - CÔNG TẮC BẬT/TẮT BOT "STOP BOT NGAY LẬP TỨC"
# - Chống loop 100% (is_echo + delivery + duplicate mid)
# - Nhận diện 1 sản phẩm = Mã sản phẩm
# - Biến thể = Mã mẫu mã
# - Không bịa sản phẩm
# - Gửi 5 ảnh đẹp nhất, khách yêu cầu mới gửi thêm
# - Lọc ảnh watermark chứa ký tự Trung Quốc
# - Gửi video nếu có
# - Đọc ảnh khách gửi (GPT Vision)
# - BOT_ACTIVE được lưu file → worker restart vẫn không spam lại
# ============================================================

import os
import csv
import io
import re
import json
import requests
from flask import Flask, request

app = Flask(__name__)

# ============================================================
# ENVIRONMENT VARIABLES
# ============================================================
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SHEET_CSV_URL = os.getenv("SHEET_CSV_URL")

ADMIN_PSID = os.getenv("ADMIN_PSID")  # PSID tài khoản quản trị
PAGE_ID = "516937221685203"  # Page ID cố định

OPENAI_URL = "https://api.openai.com/v1/chat/completions"

# ============================================================
# BOT GLOBAL STATE (STOP IMMEDIATELY)
# ============================================================
STATUS_FILE = "bot_status.json"
BOT_ACTIVE = True  # True = hoạt động; False = ngừng toàn bộ ngay lập tức

def save_status():
    global BOT_ACTIVE
    with open(STATUS_FILE, "w") as f:
        json.dump({"BOT_ACTIVE": BOT_ACTIVE}, f)

def load_status():
    global BOT_ACTIVE
    if os.path.exists(STATUS_FILE):
        try:
            BOT_ACTIVE = json.load(open(STATUS_FILE))["BOT_ACTIVE"]
            print(f"[BOT STATE] Restored BOT_ACTIVE = {BOT_ACTIVE}")
        except:
            BOT_ACTIVE = True

load_status()


# ============================================================
# TRACKING to avoid duplicate loops
# ============================================================
LAST_MESSAGE_IDS = {}   # {psid: last_mid}
LAST_IMAGES = {}        # {psid: {"images":[...], "sent":5}}


# ============================================================
# 1. LOAD GOOGLE SHEET CSV
# ============================================================
def load_products():
    try:
        print(f"[Sheet] Fetching CSV: {SHEET_CSV_URL}")
        resp = requests.get(SHEET_CSV_URL, timeout=20)
        resp.encoding = "utf-8"

        csv_file = io.StringIO(resp.text)
        rows = list(csv.DictReader(csv_file))

        print(f"[Sheet] Loaded {len(rows)} rows")
        return rows
    except Exception as e:
        print("[Sheet ERROR]:", e)
        return []


# ============================================================
# 2. EXTRACT IMAGE/VIDEO URLS
# ============================================================
def extract_urls(cell):
    if not cell:
        return []

    raw = str(cell).replace("\n", ",")
    parts = [p.strip() for p in raw.split(",") if p.strip()]

    valid = []
    seen = set()

    for u in parts:
        if not u.startswith("http"):
            continue

        # nhận hình/video
        if not (".jpg" in u or ".jpeg" in u or ".png" in u or ".webp" in u or ".mp4" in u):
            continue

        # loại URL chứa ký tự Trung Quốc trong path → loại watermark
        if re.search(r"[\u4e00-\u9fff]", u):
            continue

        if u not in seen:
            seen.add(u)
            valid.append(u)

    return valid


def get_images_for_rows(rows, max_img=None):
    all_imgs = []
    seen = set()

    for r in rows:
        imgs1 = extract_urls(r.get("Images", ""))
        imgs2 = extract_urls(r.get("Hình sản phẩm", ""))
        for url in imgs1 + imgs2:
            if url not in seen:
                seen.add(url)
                all_imgs.append(url)

    if max_img:
        return all_imgs[:max_img]
    return all_imgs


def get_videos_for_rows(rows, max_vid=1):
    vids = []
    seen = set()

    for r in rows:
        v = extract_urls(r.get("Videos", ""))
        for url in v:
            if url not in seen:
                seen.add(url)
                vids.append(url)

    return vids[:max_vid]


# ============================================================
# 3. SEARCH PRODUCTS
# ============================================================
SEARCH_FIELDS = [
    "Mã sản phẩm",
    "Tên sản phẩm",
    "Keyword sản phẩm",
    "Keyword mẫu mã",
    "Thuộc tính",
    "Thuộc tính sản phẩm",
    "Mô tả",
    "Mã mẫu mã"
]

def search_products(query, rows):
    q = query.lower().strip()
    if not q:
        return []

    matches = []

    for r in rows:
        combined = " ".join(r.get(f, "") for f in SEARCH_FIELDS).lower()
        if q in combined:
            matches.append(r)

    if matches:
        return matches

    # match từ khóa
    tokens = q.split()
    for r in rows:
        combined = " ".join(r.get(f, "") for f in SEARCH_FIELDS).lower()
        if all(t in combined for t in tokens[:2]):
            matches.append(r)

    return matches


def group_variants_by_product(matched, all_rows):
    if not matched:
        return []

    base_code = matched[0].get("Mã sản phẩm", "").strip()
    if not base_code:
        return matched

    grouped = [r for r in all_rows if r.get("Mã sản phẩm", "").strip() == base_code]
    return grouped or matched


# ============================================================
# 4. PRICE TABLE
# ============================================================
def build_price_table(rows):
    lines = []
    for r in rows:
        product = r.get("Mã sản phẩm", "").strip()
        variant = r.get("Mã mẫu mã", "").strip()
        price = r.get("Giá bán", "").strip()
        attr = r.get("Thuộc tính sản phẩm") or r.get("Thuộc tính") or ""

        if price and not price.endswith("đ"):
            price = f"{price}đ"

        line = f"- Mã SP: {product}, Mẫu: {variant}, Giá: {price}, Thuộc tính: {attr}"
        lines.append(line)

    return "\n".join(lines)


# ============================================================
# 5. GPT TEXT
# ============================================================
def call_gpt_text(user_text, variants, all_rows):
    # danh sách gợi ý
    suggestion = []
    for r in all_rows[:40]:
        name = r.get("Tên sản phẩm") or ""
        code = r.get("Mã sản phẩm") or ""
        if name and code:
            suggestion.append(f"- {name} (Mã: {code})")
    suggest_text = "\n".join(suggestion)

    if variants:
        name = variants[0].get("Tên sản phẩm", "")
        code = variants[0].get("Mã sản phẩm", "")
        price_table = build_price_table(variants)

        user_block = (
            f"KHÁCH HỎI: {user_text}\n\n"
            f"SẢN PHẨM CÓ TRONG SHOP:\n"
            f"- Tên: {name}\n"
            f"- Mã sản phẩm: {code}\n\n"
            f"DANH SÁCH BIẾN THỂ:\n{price_table}\n\n"
            f"Chỉ được tư vấn dựa trên danh sách trên."
        )
    else:
        user_block = (
            f"KHÁCH HỎI: {user_text}\n\n"
            f"Không tìm thấy sản phẩm phù hợp.\n"
            f"Dưới đây là 1 số sản phẩm shop đang có:\n{suggest_text}\n\n"
            f"Chỉ gợi ý trong danh sách trên, tuyệt đối không bịa."
        )

    system_prompt = (
        "Bạn là chatbot bán hàng.\n"
        "- 1 sản phẩm = Mã sản phẩm\n"
        "- 1 biến thể = Mã mẫu mã\n"
        "- Không được bịa sản phẩm.\n"
        "- Văn phong nhẹ nhàng, ngắn gọn.\n"
        "- Khi có danh sách biến thể: tư vấn chuẩn theo bảng.\n"
    )

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_block}
        ],
        "temperature": 0.4
    }

    try:
        resp = requests.post(OPENAI_URL, json=payload, headers=headers, timeout=30)
        return resp.json()["choices"][0]["message"]["content"]
    except Exception as e:
        print("[GPT ERROR]", e)
        return "Shop đang xử lý yêu cầu, bạn chờ một chút giúp shop nhé."


# ============================================================
# 6. GPT VISION
# ============================================================
def call_gpt_vision(image_url, rows):
    listing = []
    for r in rows[:80]:
        name = r.get("Tên sản phẩm") or ""
        code = r.get("Mã sản phẩm") or ""
        if name and code:
            listing.append(f"- {name} (Mã: {code})")

    prompt = (
        "Khách gửi 1 ảnh.\n"
        "Hãy mô tả sản phẩm trong ảnh, sau đó gợi ý 1–3 sản phẩm trong danh sách dưới đây.\n"
        "Không được bịa sản phẩm mới.\n\n"
        "DANH SÁCH SẢN PHẨM:\n" + "\n".join(listing)
    )

    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": image_url}}
                ]
            }
        ]
    }

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }

    try:
        resp = requests.post(OPENAI_URL, json=payload, headers=headers, timeout=40)
        return resp.json()["choices"][0]["message"]["content"]
    except:
        return "Shop nhận được ảnh rồi nhưng chưa xem rõ. Bạn gửi lại giúp shop ảnh nét hơn nhé."


# ============================================================
# 7. FACEBOOK SEND APIs
# ============================================================
def fb_send_text(psid, text):
    url = "https://graph.facebook.com/v18.0/me/messages"
    params = {"access_token": PAGE_ACCESS_TOKEN}
    payload = {
        "recipient": {"id": psid},
        "message": {"text": text},
    }
    requests.post(url, params=params, json=payload)


def fb_send_image(psid, url_img):
    url = "https://graph.facebook.com/v18.0/me/messages"
    params = {"access_token": PAGE_ACCESS_TOKEN}
    payload = {
        "recipient": {"id": psid},
        "message": {
            "attachment": {
                "type": "image",
                "payload": {"url": url_img, "is_reusable": True},
            }
        }
    }
    requests.post(url, params=params, json=payload)


def fb_send_video(psid, url_vid):
    url = "https://graph.facebook.com/v18.0/me/messages"
    params = {"access_token": PAGE_ACCESS_TOKEN}
    payload = {
        "recipient": {"id": psid},
        "message": {
            "attachment": {
                "type": "video",
                "payload": {"url": url_vid, "is_reusable": True},
            }
        }
    }
    requests.post(url, params=params, json=payload)


# ============================================================
# 8. SEND MORE IMAGES
# ============================================================
MORE_IMG = [
    "thêm ảnh", "xem thêm", "ảnh khác", "full ảnh",
    "send more", "more image", "more photo"
]

def is_more_image_cmd(text):
    t = text.lower()
    return any(k in t for k in MORE_IMG)


def handle_more_images(psid):
    info = LAST_IMAGES.get(psid)
    if not info:
        fb_send_text(psid, "Không còn ảnh nào khác cho mẫu này ạ ❤️")
        return

    imgs = info["images"]
    sent = info["sent"]

    if sent >= len(imgs):
        fb_send_text(psid, "Shop đã gửi hết ảnh còn lại rồi ạ ❤️")
        return

    batch = imgs[sent: sent+5]
    for i in batch:
        fb_send_image(psid, i)

    LAST_IMAGES[psid]["sent"] += len(batch)
    fb_send_text(psid, "Shop gửi thêm ảnh cho bạn xem rõ hơn nhé ❤️")


# ============================================================
# 9. ADMIN COMMANDS
# ============================================================
def is_admin(psid):
    if not ADMIN_PSID:
        return True
    return psid == ADMIN_PSID


def handle_admin_cmd(psid, text):
    global BOT_ACTIVE

    t = text.lower().strip()

    if t in ["tắt bot", "tat bot", "stop bot", "dừng bot", "dung bot"]:
        BOT_ACTIVE = False
        save_status()
        fb_send_text(psid, "⛔ Bot đã TẮT NGAY LẬP TỨC. Nhân viên sẽ trả lời khách.")
        return True

    if t in ["bật bot", "bat bot", "start bot", "bật lại bot"]:
        BOT_ACTIVE = True
        save_status()
        fb_send_text(psid, "▶ Bot đã BẬT lại.")
        return True

    return False


# ============================================================
# 10. WEBHOOK
# ============================================================
@app.route("/webhook", methods=["GET"])
def verify():
    if request.args.get("hub.mode") == "subscribe" and request.args.get("hub.verify_token") == VERIFY_TOKEN:
        return request.args.get("hub.challenge"), 200
    return "Verify failed", 403


@app.route("/webhook", methods=["POST"])
def webhook():
    global BOT_ACTIVE
    data = request.json

    print("[Webhook EVENT]", data)

    if data.get("object") != "page":
        return "OK", 200

    rows = load_products()

    for entry in data.get("entry", []):
        for ev in entry.get("messaging", []):
            psid = ev.get("sender", {}).get("id")
            msg = ev.get("message", {})

            # --------------------------
            # BLOCK ECHO
            # --------------------------
            if msg.get("is_echo"):
                continue

            # BLOCK delivery / read
            if "delivery" in ev or "read" in ev:
                continue

            # BLOCK duplicate mid
            mid = msg.get("mid")
            if mid:
                if LAST_MESSAGE_IDS.get(psid) == mid:
                    continue
                LAST_MESSAGE_IDS[psid] = mid

            # ADMIN COMMAND
            text = msg.get("text", "")
            if text and is_admin(psid):
                if handle_admin_cmd(psid, text):
                    return "OK", 200

            # BOT OFF → stop immediately
            if not BOT_ACTIVE:
                fb_send_text(psid, "Bot đang tạm dừng để nhân viên hỗ trợ trực tiếp ❤️")
                return "OK", 200

            # --------------------------
            # HANDLE IMAGE INPUT
            # --------------------------
            if "attachments" in msg:
                att = msg["attachments"][0]
                if att.get("type") == "image":
                    img_url = att["payload"]["url"]
                    reply = call_gpt_vision(img_url, rows)
                    fb_send_text(psid, reply)
                    return "OK", 200

            # --------------------------
            # HANDLE TEXT INPUT
            # --------------------------
            if text:
                if is_more_image_cmd(text):
                    handle_more_images(psid)
                    return "OK", 200

                matched = search_products(text, rows)
                variants = group_variants_by_product(matched, rows)

                # Gửi ảnh 5 cái đầu
                if variants:
                    all_imgs = get_images_for_rows(variants)
                    if all_imgs:
                        first_batch = all_imgs[:5]
                        for img in first_batch:
                            fb_send_image(psid, img)

                        LAST_IMAGES[psid] = {
                            "images": all_imgs,
                            "sent": len(first_batch)
                        }

                    # Gửi video
                    vids = get_videos_for_rows(variants)
                    for v in vids:
                        fb_send_video(psid, v)

                # Gọi GPT tư vấn
                reply = call_gpt_text(text, variants, rows)
                fb_send_text(psid, reply)

    return "OK", 200


# ============================================================
# 11. HEALTH CHECK
# ============================================================
@app.route("/")
def home():
    return "Messenger GPT Bot is running.", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
