import os
import time
import re
import io
import requests
import pandas as pd
from flask import Flask, request

app = Flask(__name__)

PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN", "")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "")

# =========================
# 1. TRẠNG THÁI BOT + ANTI LOOP + CONTEXT
# =========================
BOT_ENABLED = True                 # lệnh "tắt bot" / "bật bot"
PROCESSED_MIDS = set()            # chống xử lý trùng do Facebook retry
LAST_SENT_MEDIA = {}              # {user_id: set("product-key")}
USER_CONTEXT = {}                 # {user_id: {"current_ms": "MS000018"}}

# =========================
# 2. LOAD GOOGLE SHEET
# =========================
SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv"

df = None
LAST_LOAD = 0
LOAD_TTL = 300  # 5 phút reload 1 lần


def load_sheet(force=False):
    global df, LAST_LOAD
    now = time.time()
    if not force and df is not None and now - LAST_LOAD < LOAD_TTL:
        return
    try:
        resp = requests.get(SHEET_CSV_URL, timeout=20)
        resp.encoding = "utf-8"
        f = io.StringIO(resp.text)
        df_local = pd.read_csv(f)
        df = df_local
        LAST_LOAD = now
        print(f"[Sheet] Loaded {len(df)} rows")
    except Exception as e:
        print("[Sheet] Load ERROR:", e)


# =========================
# 3. GỬI TIN NHẮN FACEBOOK
# =========================
def fb_send(payload):
    """
    Hàm gửi chung – nếu BOT_ENABLED = False thì không gửi gì nữa.
    """
    if not BOT_ENABLED:
        print("[SEND] Bot đang tắt, không gửi gì.")
        return

    url = "https://graph.facebook.com/v19.0/me/messages"
    params = {"access_token": PAGE_ACCESS_TOKEN}
    try:
        r = requests.post(url, json=payload, params=params, timeout=20)
        print("[FB SEND]", r.status_code, r.text[:200])
    except Exception as e:
        print("[FB ERROR]", e)


def send_text(user_id, text):
    fb_send({
        "recipient": {"id": user_id},
        "message": {"text": text}
    })


def send_image(user_id, image_url, product_key=None):
    """
    Chỉ gửi 1 ảnh 1 lần cho mỗi (user, product_key, url).
    """
    if not BOT_ENABLED:
        print("[IMG] Bot OFF, skip image.")
        return

    if product_key:
        if user_id not in LAST_SENT_MEDIA:
            LAST_SENT_MEDIA[user_id] = set()
        key = f"{product_key}|{image_url}"
        if key in LAST_SENT_MEDIA[user_id]:
            print("[IMG] Skip duplicate image:", key)
            return
        LAST_SENT_MEDIA[user_id].add(key)

    fb_send({
        "recipient": {"id": user_id},
        "message": {
            "attachment": {
                "type": "image",
                "payload": {"url": image_url, "is_reusable": True}
            }
        }
    })


# =========================
# 4. ANTI-LOOP
# =========================
def is_echo_event(event):
    msg = event.get("message")
    return bool(msg and msg.get("is_echo"))


def is_delivery_or_read(event):
    """
    Bỏ qua hoàn toàn event delivery / read – KHÔNG ĐƯỢC TRẢ LỜI.
    """
    return ("delivery" in event) or ("read" in event)


def get_mid(event):
    msg = event.get("message")
    if msg:
        return msg.get("mid")
    return None


def is_processed_mid(mid):
    if not mid:
        return False
    if mid in PROCESSED_MIDS:
        return True
    PROCESSED_MIDS.add(mid)
    # giữ set không quá to
    if len(PROCESSED_MIDS) > 2000:
        PROCESSED_MIDS.clear()
        PROCESSED_MIDS.add(mid)
    return False


# =========================
# 5. LOGIC SẢN PHẨM + CONTEXT
# =========================
PRICE_PATTERNS = [
    "bao nhiêu", "bao nhieu", "giá", "gia",
    "nhiêu tiền", "nhieu tien", "bn tien", "bn tiền",
    "bn vậy", "bn v", "giá sao", "gia sao"
]


def extract_ms_from_text(text):
    """
    Tìm mã sản phẩm dạng MSxxxx trong câu chat.
    """
    if not text:
        return None
    m = re.search(r"MS(\d+)", text.upper())
    if m:
        return "MS" + m.group(1)
    return None


def find_product_by_code(ms_code):
    if df is None or "Mã sản phẩm" not in df.columns:
        return None
    subset = df[df["Mã sản phẩm"].astype(str).str.contains(ms_code, na=False)]
    if subset.empty:
        return None
    return subset


def get_clean_images(rows):
    """
    Lấy ảnh từ cột Images, loại trùng, loại URL quá ngắn.
    (Lọc watermark chi tiết có thể thêm sau – hiện ưu tiên ổn định.)
    """
    if "Images" not in rows.columns:
        return []
    all_urls = []
    for cell in rows["Images"].fillna(""):
        parts = re.split(r"[\n,; ]+", str(cell))
        for p in parts:
            url = p.strip()
            if url.startswith("http"):
                all_urls.append(url)
    # loại trùng
    seen = set()
    clean = []
    for u in all_urls:
        if u not in seen:
            seen.add(u)
            clean.append(u)
    return clean


def reply_price_only(user_id, rows, ms_code):
    """
    Khi khách hỏi 'bao nhiêu tiền' sau khi đã tư vấn 1 sản phẩm.
    Không gửi lại ảnh, chỉ trả lời giá cho sản phẩm current_ms.
    """
    # Đọc danh sách giá
    if "Giá bán" not in rows.columns:
        send_text(user_id, f"Hiện em chưa có thông tin giá cho mã {ms_code}, anh/chị cho em xin thêm chút thời gian tra cứu nhé.")
        return

    prices = rows["Giá bán"].dropna().unique()
    try:
        # nếu giá là số, format cho đẹp
        prices_fmt = []
        for p in prices:
            try:
                v = float(str(p).replace(".", "").replace(",", ""))
                prices_fmt.append(f"{v:,.0f}đ")
            except Exception:
                prices_fmt.append(str(p))
    except Exception:
        prices_fmt = [str(p) for p in prices]

    if len(prices) == 0:
        send_text(user_id, f"Hiện sản phẩm {ms_code} chưa có giá niêm yết trên hệ thống.")
    elif len(prices) == 1:
        send_text(user_id, f"Mã {ms_code} hiện đang có giá: {prices_fmt[0]} ạ.")
    else:
        # nhóm theo giá, kèm màu/size
        msg_lines = [f"Bảng giá chi tiết cho mã {ms_code}:"]
        for price in prices:
            sub = rows[rows["Giá bán"] == price]
            colors = sub["màu (Thuộc tính)"].fillna("").unique() if "màu (Thuộc tính)" in sub.columns else []
            sizes = sub["size (Thuộc tính)"].fillna("").unique() if "size (Thuộc tính)" in sub.columns else []
            colors_txt = ", ".join([c for c in colors if c]) or "Nhiều màu"
            sizes_txt = ", ".join([s for s in sizes if s]) or "Nhiều size"

            try:
                v = float(str(price).replace(".", "").replace(",", ""))
                price_txt = f"{v:,.0f}đ"
            except Exception:
                price_txt = str(price)

            msg_lines.append(f"- {colors_txt} ({sizes_txt}) → {price_txt}")
        send_text(user_id, "\n".join(msg_lines))


def consult_product(user_id, rows, ms_code):
    """
    Tư vấn sản phẩm lần đầu: tên + 5 ảnh + gợi ý hỏi thêm.
    Đồng thời GHI NHỚ mã sản phẩm vào USER_CONTEXT.
    """
    global USER_CONTEXT

    name = rows["Tên sản phẩm"].iloc[0] if "Tên sản phẩm" in rows.columns else ms_code

    # 1. Ghi nhớ context sản phẩm
    USER_CONTEXT[user_id] = {
        "current_ms": ms_code,
        "last_ts": time.time()
    }
    print(f"[CONTEXT] {user_id} -> {ms_code}")

    # 2. Gửi tên sản phẩm
    send_text(user_id, f"🔎 {name} (mã {ms_code})")

    # 3. Gửi tối đa 5 ảnh
    imgs = get_clean_images(rows)
    for img in imgs[:5]:
        send_image(user_id, img, product_key=ms_code)
        time.sleep(0.3)

    # 4. Gợi ý hỏi thêm
    send_text(
        user_id,
        "Anh/chị cần em báo giá chi tiết, tư vấn màu/size hay xem thêm hình sản phẩm nào ạ?"
    )


# =========================
# 6. WEBHOOK
# =========================
@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    global BOT_ENABLED

    if request.method == "GET":
        # Xác minh webhook với Facebook
        mode = request.args.get("hub.mode")
        token = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")

        if mode == "subscribe" and token == VERIFY_TOKEN:
            return challenge, 200
        return "Verification failed", 403

    # POST - nhận sự kiện thực tế
    data = request.get_json()
    print("[Webhook]", data)

    if data.get("object") != "page":
        return "ignored", 200

    for entry in data.get("entry", []):
        for event in entry.get("messaging", []):
            # 0. BỎ QUA HOÀN TOÀN delivery / read
            if is_delivery_or_read(event):
                print("[SKIP] delivery/read event")
                continue

            # 1. BỎ QUA ECHO (tin nhắn chính page tự gửi)
            if is_echo_event(event):
                print("[SKIP] echo")
                continue

            # 2. CHỐNG XỬ LÝ TRÙNG mid (Facebook retry)
            mid = get_mid(event)
            if is_processed_mid(mid):
                print("[SKIP] duplicate mid:", mid)
                continue

            sender_id = event.get("sender", {}).get("id")
            if not sender_id:
                continue

            # 3. LỆNH TẮT / BẬT BOT LUÔN ĐƯỢC XỬ LÝ (DÙ ĐANG OFF)
            message = event.get("message", {})
            text = message.get("text", "") or ""
            t_norm = text.lower().strip()

            if t_norm in ["tắt bot", "tat bot", "dừng bot", "dung bot", "stop bot", "off bot"]:
                BOT_ENABLED = False
                fb_send({
                    "recipient": {"id": sender_id},
                    "message": {"text": "⚠️ Bot đã tắt. Em sẽ không tự động trả lời nữa."}
                })
                print("[BOT] turned OFF by", sender_id)
                continue

            if t_norm in ["bật bot", "bat bot", "start bot", "on bot", "bat lai"]:
                BOT_ENABLED = True
                fb_send({
                    "recipient": {"id": sender_id},
                    "message": {"text": "✅ Bot đã bật lại, sẵn sàng hỗ trợ khách."}
                })
                print("[BOT] turned ON by", sender_id)
                continue

            # 4. NẾU BOT ĐANG OFF → KHÔNG XỬ LÝ THÊM GÌ NỮA
            if not BOT_ENABLED:
                print("[SKIP] bot is OFF, ignore message from", sender_id)
                continue

            # 5. LOGIC TƯ VẤN
            load_sheet()

            if not text:
                send_text(sender_id, "Anh/chị mô tả giúp shop đang tìm mã sản phẩm nào ạ?")
                continue

            lower_text = text.lower()

            # 5.1. Nếu KHÁCH HỎI GIÁ / BAO NHIÊU và đã có CONTEXT sản phẩm
            has_price_question = any(p in lower_text for p in PRICE_PATTERNS)
            ctx = USER_CONTEXT.get(sender_id)

            if has_price_question and ctx and ctx.get("current_ms"):
                ms_code = ctx["current_ms"]
                prod_rows = find_product_by_code(ms_code)
                if prod_rows is not None:
                    reply_price_only(sender_id, prod_rows, ms_code)
                else:
                    send_text(sender_id, f"Em đang bị thiếu dữ liệu mã {ms_code}, anh/chị cho em xin lại mã sản phẩm được không ạ?")
                continue

            # 5.2. Nếu KHÁCH GỬI MÃ SẢN PHẨM MỚI → CHUYỂN CONTEXT
            ms_code = extract_ms_from_text(text)
            if ms_code:
                prod_rows = find_product_by_code(ms_code)
                if prod_rows is None:
                    send_text(sender_id, f"Shop không tìm thấy sản phẩm với mã {ms_code}. Anh/chị kiểm tra lại giúp em nhé.")
                else:
                    consult_product(sender_id, prod_rows, ms_code)
                continue

            # 5.3. Không có mã, không có context giá → hỏi khách gửi mã
            if ctx and ctx.get("current_ms"):
                # đã có context nhưng câu hỏi không rõ → gợi ý lại
                ms_code = ctx["current_ms"]
                send_text(
                    sender_id,
                    f"Hiện em đang tư vấn cho anh/chị sản phẩm mã {ms_code}. "
                    f"Anh/chị muốn hỏi thêm về giá, màu, size hay hình ảnh ạ?"
                )
            else:
                send_text(
                    sender_id,
                    "Anh/chị vui lòng gửi mã sản phẩm (dạng MSxxxxx) hoặc mô tả rõ hơn tên sản phẩm để em hỗ trợ nhanh nhất ạ."
                )

    return "ok", 200


@app.route("/")
def home():
    return "Chatbot running.", 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
