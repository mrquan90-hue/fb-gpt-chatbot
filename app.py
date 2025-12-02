import os
import time
import re
import io
import requests
import pandas as pd
import openai
from flask import Flask, request

app = Flask(__name__)

# =========================
# 0. CẤU HÌNH
# =========================
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN", "")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

openai.api_key = OPENAI_API_KEY

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
# 5. XỬ LÝ SẢN PHẨM & GROUNDING
# =========================
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
    (Không lọc watermark ở đây – ưu tiên ổn định.)
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


def build_product_summary(rows, ms_code):
    """
    Tạo 1 dict gọn gàng chứa thông tin sản phẩm để gửi cho GPT.
    """
    row0 = rows.iloc[0]

    def col(name):
        return str(row0[name]) if name in rows.columns else ""

    name = col("Tên sản phẩm")
    desc = col("Mô tả")
    brand = col("Thương hiệu")
    category = col("Danh mục")
    material = col("Chất liệu")
    supplier = col("Nhà cung cấp")

    # Giá theo biến thể
    price_info = []
    if "Giá bán" in rows.columns:
        # group by Giá bán
        for price, sub in rows.groupby("Giá bán"):
            colors = []
            sizes = []
            if "màu (Thuộc tính)" in sub.columns:
                colors = [c for c in sub["màu (Thuộc tính)"].fillna("").unique() if c]
            if "size (Thuộc tính)" in sub.columns:
                sizes = [s for s in sub["size (Thuộc tính)"].fillna("").unique() if s]
            price_info.append({
                "price_raw": str(price),
                "colors": colors,
                "sizes": sizes,
                "variants_count": len(sub)
            })

    # list ảnh (chung)
    imgs = get_clean_images(rows)

    summary = {
        "code": ms_code,
        "name": name,
        "description": desc,
        "brand": brand,
        "category": category,
        "material": material,
        "supplier": supplier,
        "price_variants": price_info,
        "images_count": len(imgs),
        "has_images": len(imgs) > 0,
    }
    return summary


SHOP_POLICIES_TEXT = """
- Hệ thống hiện KHÔNG chứa thông tin chi tiết về phí vận chuyển, thời gian giao hàng, đổi trả, bảo hành, quà tặng, giá sỉ...
- Khi khách hỏi về các nội dung trên, hãy trả lời một cách TRUNG LẬP:
  + Giải thích rằng chính sách cụ thể phụ thuộc từng chương trình và thời điểm.
  + Gợi ý khách để lại số điện thoại, hoặc chờ nhân viên tư vấn chi tiết.
- TUYỆT ĐỐI KHÔNG được tự bịa ra con số cụ thể như "đổi trả 7 ngày", "bảo hành 3 tháng", "freeship toàn quốc"... nếu những con số này KHÔNG có trong dữ liệu sản phẩm.
"""

SYSTEM_INSTRUCTION = """
Bạn là trợ lý bán hàng online nói tiếng Việt, tư vấn qua Facebook Messenger.

NGUYÊN TẮC BẮT BUỘC:
- Chỉ sử dụng thông tin trong 'product_data' và 'shop_policies'.
- KHÔNG được bịa đặt giá, tính năng, chất liệu, kích thước, thời gian giao hàng, chính sách đổi trả, bảo hành, quà tặng, giá sỉ...
- Nếu khách hỏi thông tin mà dữ liệu không có, hãy nói rõ là hệ thống chưa có thông tin chính xác và gợi ý khách để lại SĐT hoặc chờ nhân viên tư vấn thêm.
- Luôn trả lời NGẮN GỌN, TỰ NHIÊN, GIỐNG NGƯỜI THẬT BÁN HÀNG.
- Giữ giọng lịch sự, thân thiện, xưng hô "em" – "anh/chị" hoặc "cô/chú" tùy ngữ cảnh trong câu hỏi của khách.

VỀ SẢN PHẨM:
- Chỉ tư vấn dựa trên sản phẩm hiện tại (product_data['code']), không tự nhảy sang sản phẩm khác.
- Nếu product_data có nhiều mức giá, hãy giải thích ngắn gọn theo nhóm màu/size nếu có.
- Nếu khách hỏi về độ phù hợp (tuổi, dáng người, mục đích sử dụng...), hãy dựa trên mô tả, chất liệu, danh mục để phân tích hợp lý, nhưng KHÔNG được khẳng định những gì trái với dữ liệu.

VỀ NGỮ ĐIỆU:
- Ưu tiên trả lời đúng trọng tâm câu hỏi của khách.
- Có thể gợi ý nhẹ nhàng thêm 1–2 ý nếu cần, nhưng không lan man.
- Cuối câu có thể kèm 1 câu CTA nhẹ: mời khách chọn màu/size, để lại SĐT, hoặc cho biết thêm nhu cầu.
"""


def call_gpt_for_product(user_message, product_summary, conversation_hint=None):
    """
    Gọi GPT để tư vấn như người thật, dựa trên dữ liệu sản phẩm.
    Không bịa thông tin.
    """
    if not OPENAI_API_KEY:
        # Không có key thì trả lời fallback
        return "Hiện hệ thống AI đang tạm thời quá tải, anh/chị cho em xin mã sản phẩm và câu hỏi chi tiết, em sẽ nhờ nhân viên hỗ trợ thêm ạ."

    messages = [
        {"role": "system", "content": SYSTEM_INSTRUCTION},
        {
            "role": "system",
            "content": "Dưới đây là chính sách tổng quát của shop (chỉ mang tính định hướng, không chứa con số cụ thể):\n"
                       + SHOP_POLICIES_TEXT
        },
        {
            "role": "system",
            "content": "Dưới đây là dữ liệu sản phẩm (product_data) mà bạn được phép sử dụng:\n"
                       + str(product_summary)
        },
    ]

    if conversation_hint:
        messages.append({
            "role": "user",
            "content": f"Ngữ cảnh trước đó trong cuộc hội thoại với khách: {conversation_hint}"
        })

    messages.append({
        "role": "user",
        "content": f"Khách vừa hỏi: \"{user_message}\".\n"
                   f"Hãy trả lời đúng trọng tâm, dựa trên product_data, không bịa đặt."
    })

    try:
        resp = openai.ChatCompletion.create(
            model=OPENAI_MODEL,
            messages=messages,
            temperature=0.4,
        )
        answer = resp["choices"][0]["message"]["content"].strip()
        return answer
    except Exception as e:
        print("[OPENAI ERROR]", e)
        return "Hiện tại hệ thống tư vấn tự động đang bận. Anh/chị cho em xin mã sản phẩm và câu hỏi, em sẽ nhờ nhân viên hỗ trợ thêm ạ."


def consult_product_first_time(user_id, rows, ms_code, user_message):
    """Tư vấn lần đầu khi khách gửi mã sản phẩm: gửi ảnh + câu GPT."""
    global USER_CONTEXT

    # 1. Ghi context
    USER_CONTEXT[user_id] = {
        "current_ms": ms_code,
        "last_ts": time.time()
    }
    print(f"[CONTEXT] {user_id} -> {ms_code}")

    # 2. Chuẩn bị summary
    product_summary = build_product_summary(rows, ms_code)

    # 3. Gọi GPT để trả lời lần đầu (giới thiệu sản phẩm + gợi ý hỏi thêm)
    reply = call_gpt_for_product(
        user_message=user_message,
        product_summary=product_summary,
        conversation_hint="Khách vừa gửi mã sản phẩm này, hãy giới thiệu ngắn gọn về sản phẩm và gợi ý khách hỏi thêm về màu, size, giá hoặc tính phù hợp."
    )
    send_text(user_id, reply)

    # 4. Gửi tối đa 5 ảnh chung (1 lần duy nhất mỗi sản phẩm / user)
    imgs = get_clean_images(rows)
    for img in imgs[:5]:
        send_image(user_id, img, product_key=ms_code)
        time.sleep(0.3)


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
                    consult_product_first_time(sender_id, rows, ms_code_in_text, user_message=text)
                continue

            # Nếu có context sản phẩm hiện tại -> tư vấn tiếp bằng GPT trên sản phẩm đó
            if current_ms and current_rows is not None:
                product_summary = build_product_summary(current_rows, current_ms)
                reply = call_gpt_for_product(
                    user_message=text,
                    product_summary=product_summary,
                    conversation_hint=f"Hiện đang tư vấn sản phẩm mã {current_ms} cho khách."
                )
                send_text(sender_id, reply)
                continue

            # Nếu chưa có context sản phẩm nào -> thử search theo nội dung khách hỏi
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
                    "Anh/chị có thể gửi *mã sản phẩm* (MSxxxxx) hoặc mô tả rõ hơn tên sản phẩm/bài viết anh/chị đang xem giúp em nhé."
                )

    return "ok", 200


@app.route("/")
def home():
    return "Chatbot running.", 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
