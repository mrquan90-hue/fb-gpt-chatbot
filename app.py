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
LAST_SENT_MEDIA = {}              # {user_id: set("product-key|url")}
USER_CONTEXT = {}                 # {user_id: {"current_ms": "MS000018", "last_ts": 123456}}

# =========================
# 2. LOAD GOOGLE SHEET
# =========================
SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv"

df = None
LAST_LOAD = 0
LOAD_TTL = 300  # 5 phút reload 1 lần


def load_sheet(force=False):
    """Tải data sản phẩm từ Google Sheet."""
    global df, LAST_LOAD
    now = time.time()
    if not force and df is not None and now - LAST_LOAD < LOAD_TTL:
        return
    try:
        print(f"[Sheet] Fetching CSV from: {SHEET_CSV_URL}")
        resp = requests.get(SHEET_CSV_URL, timeout=25)
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
# 5. XỬ LÝ SẢN PHẨM & INTENT
# =========================
PRICE_KEYWORDS = [
    "bao nhiêu", "bao nhieu", "giá", "gia",
    "nhiêu tiền", "nhieu tien", "bn tien", "bn tiền",
    "bn vậy", "bn v", "giá sao", "gia sao"
]

COLOR_KEYWORDS = ["màu", "mau", "color"]
SIZE_KEYWORDS = ["size", "sz", "siz", "cỡ", "co", "sai"]
IMAGE_KEYWORDS = ["ảnh", "hình", "hinh", "picture", "pic", "photo"]
VIDEO_KEYWORDS = ["video", "clip", "reels", "tiktok"]
DESC_KEYWORDS = ["mô tả", "chi tiết", "chi tiet", "chất liệu", "chat lieu", "vải gì", "vai gi"]


def normalize_text(text: str) -> str:
    return (text or "").strip().lower()


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


def search_products_by_text(text, limit=5):
    """
    Dùng khi khách hỏi chung chung, chưa có mã sản phẩm.
    Tìm theo Tên sản phẩm / Keyword sản phẩm / Danh mục.
    """
    if df is None:
        return None
    tokens = [t for t in re.split(r"\s+", text) if len(t) >= 3]
    if not tokens:
        # nếu câu quá ngắn, trả vài sản phẩm đầu
        base = df
    else:
        mask = None
        cols = []
        for col in ["Tên sản phẩm", "Keyword sản phẩm", "Danh mục"]:
            if col in df.columns:
                cols.append(col)
        if not cols:
            return None
        mask = False
        for t in tokens:
            pat = re.escape(t)
            token_mask = False
            for col in cols:
                mcol = df[col].astype(str).str.contains(pat, case=False, na=False)
                token_mask = token_mask | mcol
            mask = mask | token_mask
        base = df[mask] if mask is not False else df

    if "Mã sản phẩm" in base.columns:
        uniq = base.drop_duplicates(subset=["Mã sản phẩm"])
    else:
        uniq = base
    return uniq.head(limit)


def get_clean_images(rows):
    """
    Lấy ảnh từ cột Images, loại trùng.
    Không đụng đến watermark cho đơn giản, ưu tiên trả lời đúng sản phẩm.
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


def short_description(row_group):
    """
    Lấy đoạn mô tả ngắn gọn từ cột Mô tả.
    """
    if "Mô tả" not in row_group.columns:
        return ""
    desc = str(row_group["Mô tả"].fillna("").iloc[0])
    desc = desc.strip()
    if not desc:
        return ""
    # lấy 2 câu đầu
    parts = re.split(r"[.!?]\s+", desc)
    if len(parts) > 2:
        return ". ".join(parts[:2]) + "..."
    return desc


def reply_price(user_id, rows, ms_code):
    """
    Trả lời giá dựa trên nhóm biến thể cùng mã sản phẩm.
    """
    if "Giá bán" not in rows.columns:
        send_text(user_id, f"Hiện em chưa có thông tin giá cho mã {ms_code}, anh/chị cho em xin thêm chút thời gian tra cứu nhé.")
        return

    prices = rows["Giá bán"].dropna().unique()
    if len(prices) == 0:
        send_text(user_id, f"Hiện sản phẩm {ms_code} chưa có giá niêm yết trên hệ thống.")
        return

    # Thử parse số để format đẹp
    def fmt_price(x):
        s = str(x).replace(".", "").replace(",", "")
        try:
            v = float(s)
            return f"{v:,.0f}đ"
        except Exception:
            return str(x)

    if len(prices) == 1:
        p_txt = fmt_price(prices[0])
        send_text(
            user_id,
            f"Mã {ms_code} hiện đang có giá ưu đãi: {p_txt} anh/chị nhé. "
            f"Nếu lấy từ 2 sản phẩm trở lên, em có thể xin thêm ưu đãi cho mình ạ. ❤️"
        )
    else:
        # nhóm theo giá -> list màu/size cho từng giá
        msg_lines = [f"Bảng giá chi tiết cho mã {ms_code}:"]
        for price in prices:
            sub = rows[rows["Giá bán"] == price]
            colors = sub["màu (Thuộc tính)"].fillna("").unique() if "màu (Thuộc tính)" in sub.columns else []
            sizes = sub["size (Thuộc tính)"].fillna("").unique() if "size (Thuộc tính)" in sub.columns else []
            colors_txt = ", ".join([c for c in colors if c]) or "Nhiều màu"
            sizes_txt = ", ".join([s for s in sizes if s]) or "Nhiều size"
            price_txt = fmt_price(price)
            msg_lines.append(f"- {colors_txt} ({sizes_txt}) → {price_txt}")
        msg_lines.append("\nAnh/chị chốt giúp em màu, size và số lượng để em lên đơn ạ. ❤️")
        send_text(user_id, "\n".join(msg_lines))


def reply_colors(user_id, rows, ms_code):
    if "màu (Thuộc tính)" not in rows.columns:
        send_text(user_id, "Mẫu này hiện chưa cập nhật đủ thông tin màu, anh/chị cho em xin lại link sản phẩm hoặc mô tả để em kiểm tra kỹ hơn ạ.")
        return
    colors = [c for c in rows["màu (Thuộc tính)"].fillna("").unique() if c]
    if not colors:
        send_text(user_id, "Mẫu này hiện đang có 1 số màu cơ bản, anh/chị cho em biết anh/chị thích tông màu gì (sáng/tối/trung tính) để em gợi ý ạ?")
        return
    send_text(
        user_id,
        "Mẫu này hiện đang có các màu:\n- " + "\n- ".join(colors) +
        "\n\nAnh/chị thích màu nào, em gửi thêm hình thực tế cho mình xem nhé. ❤️"
    )


def reply_sizes(user_id, rows, ms_code):
    if "size (Thuộc tính)" not in rows.columns:
        send_text(user_id, "Hiện hệ thống chưa cập nhật size chi tiết, anh/chị cho em biết chiều cao/cân nặng, em tư vấn theo form chuẩn giúp mình ạ.")
        return
    sizes = [s for s in rows["size (Thuộc tính)"].fillna("").unique() if s]
    if not sizes:
        send_text(user_id, "Mẫu này form freesize, phù hợp nhiều dáng người. Anh/chị cho em xin chiều cao/cân nặng để em check kỹ hơn cho mình nhé.")
        return
    send_text(
        user_id,
        "Size hiện có của mẫu này:\n- " + "\n- ".join(sizes) +
        "\n\nAnh/chị hay mặc size gì để em tư vấn đúng form cho mình ạ?"
    )


def reply_more_images(user_id, rows, ms_code):
    imgs = get_clean_images(rows)
    if not imgs:
        send_text(user_id, "Hiện mẫu này chưa có thêm hình chi tiết trên hệ thống, anh/chị cho em xin Zalo để gửi thêm hình thực tế nhé.")
        return
    count = 0
    for img in imgs:
        send_image(user_id, img, product_key=ms_code)
        count += 1
        time.sleep(0.3)
        if count >= 8:   # giới hạn thêm tối đa 8 ảnh
            break
    send_text(user_id, "Em đã gửi thêm hình thực tế rồi ạ. Anh/chị xem giúp em thấy ok không, em tư vấn thêm màu/size cho mình nhé. ❤️")


def reply_description(user_id, rows, ms_code):
    name = rows["Tên sản phẩm"].iloc[0] if "Tên sản phẩm" in rows.columns else ms_code
    desc = short_description(rows)
    material = ""
    if "Chất liệu" in rows.columns:
        v = str(rows["Chất liệu"].fillna("").iloc[0]).strip()
        if v:
            material = v
    parts = [f"📌 *{name}* (mã {ms_code})"]
    if material:
        parts.append(f"- Chất liệu: {material}")
    if desc:
        parts.append(f"- Mô tả nhanh: {desc}")
    else:
        parts.append("- Mẫu này form đẹp, dễ mặc, phù hợp đi chơi, đi làm hoặc mặc hàng ngày.")
    parts.append("\nAnh/chị cần em tư vấn thêm về độ dày, độ co giãn hay cảm giác mặc lên người không ạ?")
    send_text(user_id, "\n".join(parts))


def consult_product_first_time(user_id, rows, ms_code):
    """Tư vấn lần đầu khi khách gửi mã sản phẩm."""
    global USER_CONTEXT

    name = rows["Tên sản phẩm"].iloc[0] if "Tên sản phẩm" in rows.columns else ms_code

    # 1. Ghi context
    USER_CONTEXT[user_id] = {
        "current_ms": ms_code,
        "last_ts": time.time()
    }
    print(f"[CONTEXT] {user_id} -> {ms_code}")

    # 2. Gửi tên + mô tả ngắn
    desc = short_description(rows)
    text = f"🔎 *{name}* (mã {ms_code})"
    if desc:
        text += f"\n\nƯu điểm nổi bật:\n- {desc}"
    send_text(user_id, text)

    # 3. Gửi tối đa 5 ảnh chung
    imgs = get_clean_images(rows)
    for img in imgs[:5]:
        send_image(user_id, img, product_key=ms_code)
        time.sleep(0.3)

    # 4. Hỏi tiếp
    send_text(
        user_id,
        "Anh/chị muốn em tư vấn thêm về *giá, màu, size hay chất liệu* ạ?"
    )


def detect_intent(text: str):
    """
    Trả về intent đơn giản: price / color / size / image / video / desc / none
    """
    t = normalize_text(text)

    if any(k in t for k in PRICE_KEYWORDS):
        return "price"
    if any(k in t for k in COLOR_KEYWORDS):
        return "color"
    if any(k in t for k in SIZE_KEYWORDS):
        return "size"
    if any(k in t for k in IMAGE_KEYWORDS):
        return "image"
    if any(k in t for k in VIDEO_KEYWORDS):
        return "video"
    if any(k in t for k in DESC_KEYWORDS):
        return "desc"
    return "none"


# =========================
# 6. WEBHOOK FACEBOOK
# =========================
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

    if data.get("object") != "page":
        return "ignored", 200

    for entry in data.get("entry", []):
        for event in entry.get("messaging", []):
            # 0. Bỏ qua delivery / read
            if is_delivery_or_read(event):
                print("[SKIP] delivery/read event")
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

            message = event.get("message", {})
            text = message.get("text", "") or ""
            t_norm = normalize_text(text)

            # 3. Lệnh tắt/bật bot – LUÔN xử lý
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
            current_ms = ctx.get("current_ms") if ctx else None
            current_rows = find_product_by_code(current_ms) if current_ms else None

            # Kiểm tra xem khách có gửi mã mới không
            ms_code_in_text = extract_ms_from_text(text)
            if ms_code_in_text:
                rows = find_product_by_code(ms_code_in_text)
                if rows is None:
                    send_text(sender_id, f"Shop không tìm thấy sản phẩm với mã {ms_code_in_text}. Anh/chị kiểm tra lại giúp em nhé.")
                else:
                    consult_product_first_time(sender_id, rows, ms_code_in_text)
                continue

            # Nếu không có mã mới → dùng intent + context
            intent = detect_intent(text)

            if current_ms and current_rows is not None:
                # Đã có sản phẩm đang tư vấn
                if intent == "price":
                    reply_price(sender_id, current_rows, current_ms)
                    continue
                elif intent == "color":
                    reply_colors(sender_id, current_rows, current_ms)
                    continue
                elif intent == "size":
                    reply_sizes(sender_id, current_rows, current_ms)
                    continue
                elif intent == "image":
                    reply_more_images(sender_id, current_rows, current_ms)
                    continue
                elif intent == "desc":
                    reply_description(sender_id, current_rows, current_ms)
                    continue
                elif intent == "video":
                    send_text(sender_id, "Hiện tại hệ thống chưa có video sẵn cho mẫu này. Anh/chị có thể xem hình chi tiết trước, nếu cần em sẽ gửi thêm video qua Zalo nhé.")
                    continue
                else:
                    # câu hỏi chung chung nhưng đã có sản phẩm
                    send_text(
                        sender_id,
                        f"Hiện em đang tư vấn cho anh/chị sản phẩm mã {current_ms}. "
                        f"Anh/chị muốn hỏi thêm về *giá, màu, size, hình ảnh hay chất liệu* ạ?"
                    )
                    continue

            # Nếu chưa có context sản phẩm nào
            # -> thử search theo nội dung khách hỏi
            results = search_products_by_text(text, limit=5)
            if results is not None and len(results) > 0 and "Mã sản phẩm" in results.columns and "Tên sản phẩm" in results.columns:
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
                    "Hiện tại em chưa xác định được sản phẩm anh/chị cần. "
                    "Anh/chị có thể gửi *mã sản phẩm* (MSxxxxx) hoặc chụp màn hình/bài viết mà anh/chị đang xem giúp em nhé."
                )

    return "ok", 200


@app.route("/")
def home():
    return "Chatbot running.", 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
