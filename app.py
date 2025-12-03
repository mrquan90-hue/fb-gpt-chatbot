import os
import time
import re
import io
import requests
import pandas as pd
from flask import Flask, request
from openai import OpenAI

app = Flask(__name__)

# =========================
# 0. CONFIG
# =========================
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN", "")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "verify_token_123")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

BOT_ENABLED = True

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

FB_API = "https://graph.facebook.com/v18.0/me/messages"

def fb_send(payload):
    if not PAGE_ACCESS_TOKEN:
        print("MISSING PAGE_ACCESS_TOKEN")
        print(payload)
        return
    params = {"access_token": PAGE_ACCESS_TOKEN}
    try:
        r = requests.post(FB_API, params=params, json=payload, timeout=8)
        if r.status_code != 200:
            print("[FB SEND ERROR]", r.text)
    except Exception as e:
        print("[FB SEND EXCEPTION]", e)

def send_text(uid, text):
    if not BOT_ENABLED:
        return
    fb_send({"recipient": {"id": uid}, "message": {"text": text}})

def send_image(uid, url):
    fb_send({
        "recipient": {"id": uid},
        "message": {"attachment": {"type": "image", "payload": {"url": url, "is_reusable": True}}}
    })

def send_video(uid, url):
    fb_send({
        "recipient": {"id": uid},
        "message": {"attachment": {"type": "video", "payload": {"url": url, "is_reusable": True}}}
    })

# =========================
# 1. LOAD GOOGLE SHEET
# =========================
SHEET_URL = "https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv"

df = None
LAST_LOAD = 0
TTL = 300

def load_sheet(force=False):
    global df, LAST_LOAD
    now = time.time()
    if not force and df is not None and now - LAST_LOAD < TTL:
        return
    try:
        r = requests.get(SHEET_URL, timeout=15)
        content = r.content.decode("utf-8")
        df_local = pd.read_csv(io.StringIO(content))
        df_local.fillna("", inplace=True)
        df = df_local
        LAST_LOAD = now
        print("[Sheet] Loaded:", len(df))
    except Exception as e:
        print("[LOAD ERROR]", e)

# =========================
# 2. CONTEXT
# =========================
USER_CONTEXT = {}
MEDIA_SENT = {}

def get_ctx(uid):
    return USER_CONTEXT.get(uid, {})

def set_ctx(uid, **kwargs):
    ctx = USER_CONTEXT.get(uid, {})
    ctx.update(kwargs)
    USER_CONTEXT[uid] = ctx
    return ctx

# =========================
# 3. ANTI LOOP FIX (QUAN TRỌNG NHẤT)
# =========================
def ignore_event(event):
    # Delivery event
    if "delivery" in event:
        print("[IGNORE] delivery")
        return True

    # Read event
    if "read" in event:
        print("[IGNORE] read")
        return True

    # Echo event
    if event.get("message", {}).get("is_echo"):
        print("[IGNORE] echo")
        return True

    return False
# =========================
# 4. UTIL
# =========================
def normalize(x):
    return str(x).strip().lower()

def extract_ms(text):
    raw = text.upper()
    m = re.search(r"MS\s*(\d+)", raw)
    if m:
        return "MS" + m.group(1).zfill(6)
    return None

def guess_ms(text):
    if df is None:
        return None
    raw = text.upper()

    # dạng: mã 123, ma 123, mã sp 45...
    m = re.search(r"M[ÃA]?\s*(SP)?\s*(\d{3,})", raw)
    if m:
        code = "MS" + m.group(2).zfill(6)
        if code in df["Mã sản phẩm"].values:
            return code

    # dạng chỉ gõ số "123"
    nums = re.findall(r"\d{3,6}", raw)
    if len(nums) == 1:
        code = "MS" + nums[0].zfill(6)
        if code in df["Mã sản phẩm"].values:
            return code

    return None

def find_product(ms):
    if df is None:
        return None
    rows = df[df["Mã sản phẩm"] == ms]
    return rows if not rows.empty else None

# Intent ship đặt hàng
NEG_SHIP = ["miễn ship", "free ship", "phí ship", "phi ship"]
SHIP_PAT = [
    r"ship\s*\d+",
    r"sip\s*\d+",
    r"ship.*(cái|bộ|đôi)",
    r"sip.*(cái|bộ|đôi)",
]

def is_order_ship(text):
    txt = text.lower()
    for x in NEG_SHIP:
        if x in txt:
            return False
    for p in SHIP_PAT:
        if re.search(p, txt):
            return True
    return False

def clean_images(rows):
    out = []
    seen = set()
    if "Images" not in rows:
        return []
    for cell in rows["Images"]:
        parts = re.split(r"[,;\s\n]+", str(cell))
        for url in parts:
            if url.startswith("http") and url not in seen:
                seen.add(url)
                if "watermark" in url.lower():
                    continue
                out.append(url)
    return out

def get_videos(rows):
    out = []
    if "Videos" not in rows:
        return out
    seen = set()
    for cell in rows["Videos"]:
        parts = re.split(r"[,;\s\n]+", str(cell))
        for url in parts:
            if url.startswith("http") and url not in seen:
                seen.add(url)
                out.append(url)
    return out

def format_price(v):
    try:
        return f"{float(v):,.0f}đ".replace(",", ".")
    except:
        return str(v)

def answer_price(rows, ms):
    prices = rows["Giá bán"].astype(str).str.strip().unique()
    if len(prices) == 1:
        return f"Mẫu {ms} giá khoảng {format_price(prices[0])} anh/chị nhé."
    lines = [f"Mẫu {ms} có nhiều mức giá:"]
    for p in prices:
        lines.append(f"- {format_price(p)}")
    lines.append("Anh/chị cho em xin màu/size để em báo đúng giá.")
    return "\n".join(lines)

def answer_stock(rows, ms):
    stock = rows["Có thể bán"].astype(str).str.lower()
    if all(x in ["0","false","hết hàng","het hang"] for x in stock):
        return f"Mẫu {ms} đang tạm hết hàng ạ."
    return f"Mẫu {ms} vẫn còn hàng anh/chị nha."

def answer_color_size(rows):
    colors = rows["màu (Thuộc tính)"].unique() if "màu (Thuộc tính)" in rows else []
    sizes  = rows["size (Thuộc tính)"].unique() if "size (Thuộc tính)" in rows else []
    out=[]
    if len(colors)>0: out.append("Màu: " + ", ".join(x for x in colors if x))
    if len(sizes)>0: out.append("Size: " + ", ".join(x for x in sizes if x))
    if not out:
        return "Sản phẩm này chưa có dữ liệu màu/size."
    out.append("Anh/chị cho em chiều cao & cân nặng để em tư vấn size chuẩn.")
    return "\n".join(out)

# GPT SYSTEM INSTRUCTION
SYSTEM = """
Bạn là trợ lý bán hàng Messenger.
Không bịa đặt. Không tự tạo giá, kích thước, thời gian ship.
Trả lời ngắn gọn tự nhiên.
"""
def call_gpt(msg, product_data):
    if client is None:
        return "Hệ thống AI đang tạm quá tải."
    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": SYSTEM},
                {"role": "system", "content": product_data},
                {"role": "user", "content": msg}
            ],
            temperature=0.2,
            max_tokens=250
        )
        return resp.choices[0].message.content
    except Exception as e:
        print("[GPT ERROR]", e)
        return "Hiện AI đang lỗi, anh/chị cho em câu hỏi cụ thể hơn."

def intro_product(uid, rows, ms, user_msg):
    set_ctx(uid, current_ms=ms)
    summary = f"Mã {ms}\nTên: {rows.iloc[0]['Tên sản phẩm']}\nMô tả:\n{rows.iloc[0]['Mô tả']}"
    reply = call_gpt(user_msg, summary)
    send_text(uid, reply)
    imgs = clean_images(rows)
    for i in imgs[:5]:
        send_image(uid, i)
        time.sleep(0.3)

@app.route("/webhook", methods=["GET","POST"])
def webhook():

    # ==== VERIFY TOKEN ====
    if request.method == "GET":
        mode = request.args.get("hub.mode")
        token = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")
        if mode == "subscribe" and token == VERIFY_TOKEN:
            return challenge
        return "Verification failed", 403

    # ==== HANDLE POST ====
    data = request.get_json()

    for entry in data.get("entry", []):
        for event in entry.get("messaging", []):

            # ===========================
            # 🔥 FIX LOOP — IGNORE delivery/read/echo
            # ===========================
            if ignore_event(event):
                continue

            # ===========================
            # 1) CHỈ XỬ LÝ message.text
            # ===========================
            if not ("message" in event and "text" in event["message"]):
                continue

            uid = event["sender"]["id"]
            text = event["message"]["text"].strip()
            lower = normalize(text)

            load_sheet()

            # Admin: bật/tắt bot
            if lower in ["tắt bot","tat bot"]:
                global BOT_ENABLED
                BOT_ENABLED=False
                send_text(uid,"❌ Bot đã tắt.")
                continue
            if lower in ["bật bot","bat bot"]:
                BOT_ENABLED=True
                send_text(uid,"✅ Bot đã bật.")
                continue
            if not BOT_ENABLED:
                continue

            ctx = get_ctx(uid)
            current_ms = ctx.get("current_ms")

            # ===========================
            # 2) KIỂM TRA GỬI MÃ SP
            # ===========================
            ms = extract_ms(text) or guess_ms(text)
            if ms:
                rows = find_product(ms)
                if rows is None:
                    send_text(uid, f"Không tìm thấy sản phẩm mã {ms}.")
                else:
                    intro_product(uid, rows, ms, text)
                continue

            # ===========================
            # 3) ĐANG TƯ VẤN 1 SẢN PHẨM
            # ===========================
            if current_ms:
                rows = find_product(current_ms)

                # ship đặt hàng
                if is_order_ship(text):
                    send_text(uid, "Anh/chị muốn chốt đơn ạ? Cho em xin SĐT + địa chỉ nhận hàng nhé ❤️")
                    continue

                # hỏi giá
                if any(k in lower for k in ["giá","bao nhiêu","bn","nhiêu tiền"]):
                    send_text(uid, answer_price(rows, current_ms))
                    continue

                # hỏi tồn kho
                if any(k in lower for k in ["còn","hết hàng","có sẵn"]):
                    send_text(uid, answer_stock(rows, current_ms))
                    continue

                # ảnh
                if any(k in lower for k in ["ảnh","hình","xem mẫu","gửi ảnh"]):
                    imgs = clean_images(rows)
                    if not imgs:
                        send_text(uid, "Sản phẩm chưa có ảnh.")
                    else:
                        send_text(uid, "Em gửi anh/chị ảnh tham khảo:")
                        for img in imgs[:5]:
                            send_image(uid, img)
                            time.sleep(0.3)
                    continue

                # video
                if any(k in lower for k in ["video","clip","tiktok","reels"]):
                    vids = get_videos(rows)
                    if not vids:
                        send_text(uid, "Sản phẩm hiện chưa có video ạ.")
                    else:
                        send_text(uid,"Video tham khảo:")
                        for v in vids[:2]:
                            send_video(uid, v)
                            time.sleep(0.3)
                    continue

                # màu size
                if any(k in lower for k in ["màu","size","kích"]):
                    send_text(uid, answer_color_size(rows))
                    continue

                # còn lại → GPT
                summary = (
                    f"Mã {current_ms}\n"
                    f"Tên: {rows.iloc[0]['Tên sản phẩm']}\n"
                    f"Mô tả:\n{rows.iloc[0]['Mô tả']}"
                )
                reply = call_gpt(text, summary)
                send_text(uid, reply)
                continue

            # ===========================
            # 4) CHƯA CÓ SẢN PHẨM → OFFER TÌM
            # ===========================
            send_text(uid, "Anh/chị gửi giúp em mã sản phẩm (MSxxxxx) hoặc mô tả sản phẩm anh/chị đang xem nhé.")

    return "ok", 200


@app.route("/")
def home():
    return "Chatbot is running", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
