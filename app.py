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

FB_API_URL = "https://graph.facebook.com/v18.0/me/messages"


# =========================
# 1. GỬI TIN NHẮN FACEBOOK
# =========================
def fb_send(payload):
    if not PAGE_ACCESS_TOKEN:
        print("[fb_send] MISSING PAGE_ACCESS_TOKEN, chỉ in payload:")
        print(payload)
        return False

    params = {"access_token": PAGE_ACCESS_TOKEN}
    try:
        r = requests.post(FB_API_URL, params=params, json=payload, timeout=8)
        if r.status_code != 200:
            print("[fb_send] ERROR:", r.status_code, r.text)
            return False
        return True
    except Exception as e:
        print("[fb_send] EXCEPTION:", e)
        return False


def send_text(user_id, text):
    if not BOT_ENABLED:
        print("[TEXT] Bot OFF, skip:", text)
        return
    fb_send({
        "recipient": {"id": user_id},
        "message": {"text": text}
    })


def send_image(user_id, image_url):
    if not BOT_ENABLED:
        return
    fb_send({
        "recipient": {"id": user_id},
        "message": {
            "attachment": {
                "type": "image",
                "payload": {"url": image_url, "is_reusable": True}
            }
        }
    })


def send_video(user_id, video_url):
    if not BOT_ENABLED:
        return
    fb_send({
        "recipient": {"id": user_id},
        "message": {
            "attachment": {
                "type": "video",
                "payload": {"url": video_url, "is_reusable": True}
            }
        }
    })


# =========================
# 2. LOAD GOOGLE SHEET
# =========================
SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv"

df = None
LAST_LOAD = 0
LOAD_TTL = 300  # 5 phút


def load_sheet(force=False):
    global df, LAST_LOAD
    now = time.time()
    if not force and df is not None and now - LAST_LOAD < LOAD_TTL:
        return
    try:
        print(f"[Sheet] Loading from: {SHEET_CSV_URL}")
        resp = requests.get(SHEET_CSV_URL, timeout=15)
        resp.raise_for_status()
        content = resp.content.decode("utf-8")
        df_local = pd.read_csv(io.StringIO(content))
        df_local.fillna("", inplace=True)
        df = df_local
        LAST_LOAD = now
        print("[Sheet] Loaded:", len(df))
    except Exception as e:
        print("[Sheet] ERROR loading:", e)


# =========================
# 3. CONTEXT & ANTI MEDIA DUP
# =========================
USER_CONTEXT = {}
MEDIA_SENT = {}      # nếu sau này muốn chống trùng ảnh/video
LAST_MESSAGE_MID = {}  # chống xử lý trùng mid (Facebook gửi lại)


def get_ctx(uid):
    return USER_CONTEXT.get(uid, {})


def set_ctx(uid, **kwargs):
    ctx = USER_CONTEXT.get(uid, {})
    ctx.update(kwargs)
    USER_CONTEXT[uid] = ctx
    return ctx


def normalize(text):
    return str(text or "").strip().lower()


# =========================
# 4. ANTI LOOP: BỎ QUA delivery / read / echo
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
    # Echo (tin nhắn do chính page gửi)
    if event.get("message", {}).get("is_echo"):
        print("[IGNORE] echo")
        return True
    return False


# =========================
# 5. XỬ LÝ SẢN PHẨM
# =========================
def extract_ms(text: str):
    if not text:
        return None
    raw = text.upper()
    m = re.search(r"MS\s*(\d+)", raw)
    if m:
        return "MS" + m.group(1).zfill(6)
    return None


def guess_ms(text: str):
    """Đoán mã sp khi khách chỉ gõ số hoặc 'mã 123', 'ma 123'..."""
    global df
    if df is None:
        return None
    raw = text.upper()

    # dạng: mã 123
    m = re.search(r"M[ÃA]?\s*(SP)?\s*(\d{3,})", raw)
    if m:
        code = "MS" + m.group(2).zfill(6)
        if code in df["Mã sản phẩm"].astype(str).values:
            return code

    # dạng chỉ số
    nums = re.findall(r"\d{3,6}", raw)
    if len(nums) == 1:
        code = "MS" + nums[0].zfill(6)
        if code in df["Mã sản phẩm"].astype(str).values:
            return code
    return None


def find_product(ms_code):
    global df
    if df is None:
        return None
    rows = df[df["Mã sản phẩm"].astype(str) == ms_code]
    return rows if not rows.empty else None


def format_price(v):
    try:
        return f"{float(v):,.0f}đ".replace(",", ".")
    except Exception:
        return str(v)


def answer_price(rows, ms_code):
    if "Giá bán" not in rows.columns:
        return f"Hiện em chưa có dữ liệu giá chi tiết cho mã {ms_code}, anh/chị cho em xin nhu cầu cụ thể, em nhờ nhân viên hỗ trợ ạ."

    prices = rows["Giá bán"].astype(str).str.strip().unique()
    if len(prices) == 1:
        return f"Mã {ms_code} giá khoảng {format_price(prices[0])} anh/chị nha."

    lines = [f"Mã {ms_code} có một số mức giá tuỳ màu/size:"]
    for p in prices:
        lines.append(f"- {format_price(p)}")
    lines.append("Anh/chị cho em xin màu/size cụ thể để em báo đúng giá ạ.")
    return "\n".join(lines)


def answer_stock(rows, ms_code):
    if "Có thể bán" not in rows.columns:
        return f"Hiện em chưa có dữ liệu tồn kho chi tiết cho mã {ms_code}, anh/chị cho em xin số lượng cần, em nhờ nhân viên check lại ạ."
    stock = rows["Có thể bán"].astype(str).str.lower()
    if all(x in ["0", "false", "hết hàng", "het hang", "no"] for x in stock):
        return f"Mã {ms_code} hiện đang tạm hết hàng ạ."
    return f"Mã {ms_code} hiện vẫn còn hàng anh/chị nha."


def clean_images(rows):
    if "Images" not in rows.columns:
        return []
    all_urls = []
    for cell in rows["Images"].fillna(""):
        parts = re.split(r"[\n,; ]+", str(cell))
        for p in parts:
            url = p.strip()
            if url.startswith("http"):
                all_urls.append(url)
    seen = set()
    clean = []
    for u in all_urls:
        if u in seen:
            continue
        seen.add(u)
        # chỉ loại URL chứa watermark, KHÔNG loại domain Trung Quốc
        if "watermark" in u.lower():
            continue
        clean.append(u)
    return clean


def get_videos(rows):
    if "Videos" not in rows.columns:
        return []
    all_urls = []
    for cell in rows["Videos"].fillna(""):
        parts = re.split(r"[\n,; ]+", str(cell))
        for p in parts:
            url = p.strip()
            if url.startswith("http"):
                all_urls.append(url)
    seen = set()
    clean = []
    for u in all_urls:
        if u in seen:
            continue
        seen.add(u)
        clean.append(u)
    return clean


def answer_color_size(rows):
    colors = []
    sizes = []
    if "màu (Thuộc tính)" in rows.columns:
        colors = [c for c in rows["màu (Thuộc tính)"].fillna("").unique() if c]
    if "size (Thuộc tính)" in rows.columns:
        sizes = [s for s in rows["size (Thuộc tính)"].fillna("").unique() if s]

    lines = []
    if colors:
        lines.append("Màu hiện có: " + ", ".join(colors))
    if sizes:
        lines.append("Size hiện có: " + ", ".join(sizes))
    if not lines:
        return "Sản phẩm này chưa có dữ liệu màu/size chi tiết. Anh/chị cho em chiều cao & cân nặng, em nhờ nhân viên hỗ trợ chọn size ạ."
    lines.append("Anh/chị cho em chiều cao, cân nặng hoặc size thường mặc để em tư vấn chuẩn hơn.")
    return "\n".join(lines)


def build_product_summary(rows, ms_code):
    name = str(rows.iloc[0].get("Tên sản phẩm", ""))
    desc = str(rows.iloc[0].get("Mô tả", ""))
    return f"Mã: {ms_code}\nTên sản phẩm: {name}\nMô tả:\n{desc}"


# =========================
# 6. SHIP ĐẶT HÀNG vs FREE SHIP
# =========================
NEG_SHIP = [
    "miễn ship", "mien ship", "free ship", "freeship",
    "phí ship", "phi ship", "tiền ship", "tien ship"
]
SHIP_PATTERNS = [
    r"\bship\s*\d+",
    r"\bsip\s*\d+",
    r"\bship\b.*\b(cái|cai|bộ|bo|đôi|doi)\b",
    r"\bsip\b.*\b(cái|cai|bộ|bo|đôi|doi)\b",
]


def is_order_ship(text):
    t = text.lower()
    for bad in NEG_SHIP:
        if bad in t:
            return False
    for pat in SHIP_PATTERNS:
        if re.search(pat, t):
            return True
    return False


# =========================
# 7. GPT TƯ VẤN
# =========================
SYSTEM_INSTRUCTION = """
Bạn là trợ lý bán hàng online, nói tiếng Việt, tư vấn qua Facebook Messenger.

NGUYÊN TẮC:
- Chỉ dựa vào dữ liệu sản phẩm được cung cấp.
- Không bịa giá, kích thước, thời gian giao hàng, chính sách bảo hành/đổi trả.
- Nếu thiếu thông tin, nói rõ là chưa có, gợi ý khách chờ nhân viên tư vấn.
- Trả lời ngắn gọn, tự nhiên, thân thiện.
"""


def call_gpt_for_product(user_message, product_summary, hint=""):
    if client is None:
        return "Hiện hệ thống AI đang tạm bận, anh/chị cho em câu hỏi cụ thể và mã sản phẩm, em nhờ nhân viên hỗ trợ thêm ạ."
    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": SYSTEM_INSTRUCTION},
                {"role": "system", "content": "Dữ liệu sản phẩm:\n" + product_summary},
                {"role": "system", "content": "Ngữ cảnh:\n" + hint},
                {"role": "user", "content": user_message},
            ],
            temperature=0.4,
            max_tokens=300,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print("[GPT ERROR]", e)
        return "Hiện hệ thống AI đang có chút trục trặc, anh/chị cho em xin câu hỏi cụ thể, em nhờ nhân viên hỗ trợ thêm ạ."


def intro_product(user_id, rows, ms_code, user_msg=""):
    set_ctx(user_id, current_ms=ms_code, order_state=None,
            order_color=None, order_size=None, order_quantity=None)
    summary = build_product_summary(rows, ms_code)
    reply = call_gpt_for_product(
        user_message=user_msg or f"Giới thiệu ngắn gọn sản phẩm mã {ms_code}.",
        product_summary=summary,
        hint="Khách vừa gửi mã sản phẩm, hãy giới thiệu ngắn gọn và gợi ý hỏi thêm màu, size, giá."
    )
    send_text(user_id, reply)

    # Gửi 1 loạt ảnh (tối đa 5)
    imgs = clean_images(rows)
    if imgs:
        for img in imgs[:5]:
            send_image(user_id, img)
            time.sleep(0.3)


# =========================
# 8. WEBHOOK
# =========================
@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    global BOT_ENABLED

    # VERIFY
    if request.method == "GET":
        mode = request.args.get("hub.mode")
        token = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")
        if mode == "subscribe" and token == VERIFY_TOKEN:
            return challenge, 200
        return "Verification failed", 403

    data = request.get_json()
    # print("[Webhook] data:", data)

    for entry in data.get("entry", []):
        for event in entry.get("messaging", []):

            # 1. Bỏ qua delivery / read / echo
            if ignore_event(event):
                continue

            sender_id = event["sender"]["id"]
            message = event.get("message")

            # 2. Chỉ xử lý nếu là message.text
            if not (message and "text" in message):
                continue

            text = message["text"].strip()
            lower = normalize(text)

            # 3. Chống xử lý trùng mid (FB retry)
            mid = message.get("mid")
            if mid:
                last_mid = LAST_MESSAGE_MID.get(sender_id)
                if last_mid == mid:
                    print("[IGNORE] duplicate mid for user", sender_id)
                    continue
                LAST_MESSAGE_MID[sender_id] = mid

            # 4. Lệnh bật/tắt bot
            if lower in ["tắt bot", "tat bot"]:
                BOT_ENABLED = False
                send_text(sender_id, "❌ Bot đã tạm dừng trả lời tự động.")
                continue
            if lower in ["bật bot", "bat bot"]:
                BOT_ENABLED = True
                send_text(sender_id, "✅ Bot đã bật lại, sẵn sàng hỗ trợ khách.")
                continue

            if not BOT_ENABLED:
                print("[SKIP] bot OFF")
                continue

            load_sheet()
            ctx = get_ctx(sender_id)
            current_ms = ctx.get("current_ms")
            order_state = ctx.get("order_state")

            # =========================
            # 5. ƯU TIÊN: KHÁCH GỬI MÃ SẢN PHẨM
            # =========================
            ms_code = extract_ms(text) or guess_ms(text)
            if ms_code:
                rows = find_product(ms_code)
                if rows is None:
                    send_text(sender_id, f"Shop chưa tìm thấy sản phẩm mã {ms_code}, anh/chị kiểm tra lại giúp em nhé.")
                else:
                    intro_product(sender_id, rows, ms_code, user_msg=text)
                continue

            # =========================
            # 6. Nếu đang ở TRẠNG THÁI ĐẶT HÀNG (order_state)
            # =========================
            if current_ms:
                rows = find_product(current_ms)
                if rows is None:
                    # mất dữ liệu thì clear context
                    set_ctx(sender_id, current_ms=None, order_state=None)
                else:
                    # ----- STATE: bot đang chờ khách trả lời màu/size -----
                    if order_state == "awaiting_variant":
                        # cố gắng bắt size & màu từ câu trả lời
                        colors = []
                        sizes = []
                        if "màu (Thuộc tính)" in rows.columns:
                            colors = [c for c in rows["màu (Thuộc tính)"].fillna("").unique() if c]
                        if "size (Thuộc tính)" in rows.columns:
                            sizes = [s for s in rows["size (Thuộc tính)"].fillna("").unique() if s]

                        chosen_color = None
                        chosen_size = None

                        txt_upper = text.upper()

                        # bắt size trước
                        for s in sizes:
                            s_str = str(s).upper()
                            # size M, M, XL...
                            if s_str and s_str in txt_upper:
                                chosen_size = s
                                break

                        # bắt màu
                        for c in colors:
                            c_str = str(c).upper()
                            if c_str and c_str in txt_upper:
                                chosen_color = c
                                break

                        # Nếu khách chỉ nói "Size L" (không khớp size nào) thì cứ lưu raw
                        if not chosen_size and sizes:
                            # nếu text có chữ "size" + 1 token phía sau
                            m = re.search(r"size\s*([a-zA-Z0-9]+)", lower)
                            if m:
                                chosen_size = m.group(1).upper()

                        # Lưu vào context
                        ctx_update = {
                            "order_state": "awaiting_contact",
                            "order_size": chosen_size or ctx.get("order_size"),
                            "order_color": chosen_color or ctx.get("order_color"),
                        }
                        set_ctx(sender_id, **ctx_update)

                        msg_lines = ["Dạ em ghi nhận đơn cho sản phẩm này rồi ạ."]
                        if ctx_update.get("order_color"):
                            msg_lines.append(f"- Màu: {ctx_update['order_color']}")
                        if ctx_update.get("order_size"):
                            msg_lines.append(f"- Size: {ctx_update['order_size']}")
                        msg_lines.append("Anh/chị cho em xin SĐT và địa chỉ nhận hàng chi tiết để em tạo đơn giao luôn ạ ❤️")
                        send_text(sender_id, "\n".join(msg_lines))
                        continue

                    # ----- STATE: đang chờ khách gửi SĐT / địa chỉ -----
                    if order_state == "awaiting_contact":
                        # Ở đây chưa parse sâu, chỉ coi như khách đã gửi thông tin
                        send_text(
                            sender_id,
                            "Dạ em cảm ơn anh/chị ạ. Em đã nhận thông tin rồi, chút nữa sẽ có nhân viên gọi xác nhận đơn và báo thời gian giao hàng cụ thể nhé ❤️"
                        )
                        # reset state, vẫn giữ current_ms để tư vấn tiếp nếu cần
                        set_ctx(sender_id, order_state=None)
                        continue

                    # =========================
                    # 7. LOGIC INTENT TRÊN 1 SẢN PHẨM (KHI KHÔNG Ở TRẠNG THÁI ĐẶT HÀNG)
                    # =========================

                    # 7.1 KHÁCH DÙNG "SHIP 1 CÁI", "SIP 2 BỘ" → CHỐT ĐƠN
                    if is_order_ship(text):
                        set_ctx(sender_id, order_state="awaiting_variant",
                                order_quantity=None, order_color=None, order_size=None)
                        send_text(
                            sender_id,
                            "Dạ em cảm ơn anh/chị đã ủng hộ ạ 😍 Anh/chị cho em xin MÀU và SIZE muốn lấy để em chốt đơn giúp mình nhé."
                        )
                        continue

                    # 7.2 HỎI TỒN KHO
                    stock_keywords = [
                        "còn hàng", "con hang", "hết hàng", "het hang",
                        "còn không", "con khong", "còn ko", "con ko", "còn k", "con k",
                        "có sẵn", "co san", "còn size", "con size"
                    ]
                    if any(k in lower for k in stock_keywords):
                        send_text(sender_id, answer_stock(rows, current_ms))
                        continue

                    # 7.3 HỎI GIÁ
                    price_keywords = [
                        "giá", "gia", "bao nhiêu", "bao nhieu",
                        "nhiêu tiền", "nhieu tien", "bn"
                    ]
                    if any(k in lower for k in price_keywords):
                        send_text(sender_id, answer_price(rows, current_ms))
                        continue

                    # 7.4 HỎI ẢNH
                    image_keywords = [
                        "ảnh", "anh", "hình", "hinh", "xem mẫu", "xem mau",
                        "gửi ảnh", "gui anh", "xem hình", "xem hinh"
                    ]
                    if any(k in lower for k in image_keywords):
                        imgs = clean_images(rows)
                        if not imgs:
                            send_text(sender_id, "Sản phẩm này hiện chưa có link ảnh để gửi trực tiếp ạ.")
                        else:
                            send_text(sender_id, "Em gửi anh/chị một số ảnh của sản phẩm để mình xem thêm ạ:")
                            for img in imgs[:5]:
                                send_image(sender_id, img)
                                time.sleep(0.3)
                        continue

                    # 7.5 HỎI VIDEO
                    video_keywords = ["video", "clip", "tiktok", "reels"]
                    if any(k in lower for k in video_keywords):
                        vids = get_videos(rows)
                        if not vids:
                            send_text(sender_id, "Mã này hiện chưa có video sẵn ạ.")
                        else:
                            send_text(sender_id, "Em gửi anh/chị video tham khảo sản phẩm ạ:")
                            for vurl in vids[:2]:
                                send_video(sender_id, vurl)
                                time.sleep(0.3)
                        continue

                    # 7.6 HỎI MÀU / SIZE (nhưng không phải state đặt hàng)
                    color_size_keywords = [
                        "màu", "mau", "màu sắc", "mau sac",
                        "size", "sai", "kích cỡ", "kich co", "kích thước", "kich thuoc"
                    ]
                    if any(k in lower for k in color_size_keywords):
                        send_text(sender_id, answer_color_size(rows))
                        continue

                    # 7.7 CÁC CÂU HỎI KHÁC → GPT
                    summary = build_product_summary(rows, current_ms)
                    reply = call_gpt_for_product(
                        user_message=text,
                        product_summary=summary,
                        hint=f"Đang tư vấn sản phẩm mã {current_ms}."
                    )
                    send_text(sender_id, reply)
                    continue

            # =========================
            # 9. CHƯA XÁC ĐỊNH SẢN PHẨM
            # =========================
            send_text(
                sender_id,
                "Hiện em chưa xác định được sản phẩm anh/chị cần ạ. Anh/chị có thể gửi giúp em mã sản phẩm (MSxxxxx) "
                "hoặc tên/mô tả sản phẩm đang xem để em tư vấn chi tiết nhé."
            )

    return "ok", 200


@app.route("/")
def home():
    return "Chatbot running.", 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
