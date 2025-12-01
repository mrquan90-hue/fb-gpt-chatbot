import os
import json
import time
import re
from flask import Flask, request
import requests
import pandas as pd

app = Flask(__name__)

# =============================
# CẤU HÌNH
# =============================
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "my_verify_token")
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN", "")
SHEET_CSV_URL = os.getenv(
    "SHEET_CSV_URL",
    "https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv",
)

# PSID admin (nếu để rỗng thì ai gõ tắt bot cũng được)
ADMIN_PSID = os.getenv("ADMIN_PSID", "") or None

# =============================
# TRẠNG THÁI BOT
# =============================
BOT_ACTIVE = True
STATUS_FILE = "bot_status.json"
LAST_MID = {}  # chống xử lý lại cùng 1 message


def save_status():
    try:
        with open(STATUS_FILE, "w", encoding="utf-8") as f:
            json.dump({"BOT_ACTIVE": BOT_ACTIVE}, f)
    except Exception as e:
        print("[STATUS] save error:", e)


def load_status():
    global BOT_ACTIVE
    if os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                BOT_ACTIVE = bool(data.get("BOT_ACTIVE", True))
                print(f"[STATUS] Restored BOT_ACTIVE = {BOT_ACTIVE}")
        except Exception as e:
            print("[STATUS] load error:", e)
            BOT_ACTIVE = True


load_status()

# =============================
# LOAD SẢN PHẨM TỪ SHEET
# =============================


def load_products():
    """
    Đọc toàn bộ sản phẩm từ Google Sheet CSV.
    Luôn trả về DataFrame (có thể rỗng nếu lỗi).
    """
    try:
        print(f"[Sheet] Fetching CSV: {SHEET_CSV_URL}")
        df = pd.read_csv(SHEET_CSV_URL, dtype=str).fillna("")
        print(f"[Sheet] Loaded {len(df)} products")
        return df
    except Exception as e:
        print("[Sheet ERROR]", e)
        return pd.DataFrame()


# =============================
# XỬ LÝ ẢNH
# =============================


def parse_image_urls(cell: str):
    """
    Tách URL ảnh từ 1 ô (ngăn cách bởi dấu phẩy, xuống dòng...).
    Không lọc theo domain, không loại link chứa chữ Trung Quốc.
    Chỉ loại rỗng và trùng lặp.
    """
    if not cell:
        return []
    raw = str(cell).replace("\n", ",")
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    urls = []
    seen = set()
    for u in parts:
        if not u.lower().startswith("http"):
            continue
        if u not in seen:
            seen.add(u)
            urls.append(u)
    return urls


# =============================
# GỬI TIN FACEBOOK
# =============================


def send_text(psid: str, text: str):
    if not PAGE_ACCESS_TOKEN:
        print("[WARN] PAGE_ACCESS_TOKEN missing, skip send_text")
        return
    url = "https://graph.facebook.com/v19.0/me/messages"
    params = {"access_token": PAGE_ACCESS_TOKEN}
    payload = {
        "recipient": {"id": psid},
        "message": {"text": text},
    }
    try:
        r = requests.post(url, params=params, json=payload, timeout=15)
        print("[FB SEND TEXT]", r.status_code, r.text[:200])
    except Exception as e:
        print("[FB SEND TEXT ERROR]", e)


def send_image(psid: str, image_url: str):
    if not PAGE_ACCESS_TOKEN:
        print("[WARN] PAGE_ACCESS_TOKEN missing, skip send_image")
        return
    url = "https://graph.facebook.com/v19.0/me/messages"
    params = {"access_token": PAGE_ACCESS_TOKEN}
    payload = {
        "recipient": {"id": psid},
        "message": {
            "attachment": {
                "type": "image",
                "payload": {
                    "url": image_url,
                    "is_reusable": True,
                },
            }
        },
    }
    try:
        r = requests.post(url, params=params, json=payload, timeout=20)
        print("[FB SEND IMAGE]", r.status_code, r.text[:200])
    except Exception as e:
        print("[FB SEND IMAGE ERROR]", e)


def send_video(psid: str, video_url: str):
    if not PAGE_ACCESS_TOKEN:
        print("[WARN] PAGE_ACCESS_TOKEN missing, skip send_video")
        return
    url = "https://graph.facebook.com/v19.0/me/messages"
    params = {"access_token": PAGE_ACCESS_TOKEN}
    payload = {
        "recipient": {"id": psid},
        "message": {
            "attachment": {
                "type": "video",
                "payload": {
                    "url": video_url,
                    "is_reusable": True,
                },
            }
        },
    }
    try:
        r = requests.post(url, params=params, json=payload, timeout=30)
        print("[FB SEND VIDEO]", r.status_code, r.text[:200])
    except Exception as e:
        print("[FB SEND VIDEO ERROR]", e)


# =============================
# XỬ LÝ GIÁ & TEXT
# =============================


def format_price(v: str) -> str:
    """
    Chuẩn hoá giá: nếu là số → chuyển sang xxxk hoặc xx.xxxđ
    nếu đã có k/đ thì giữ nguyên.
    """
    if not v:
        return ""
    s = str(v).strip()
    if any(ch in s for ch in ["k", "K", "đ", "₫"]):
        return s
    if s.isdigit():
        n = int(s)
        if n % 1000 == 0:
            k = n // 1000
            return f"{k}k"
        s_rev = "".join(reversed(s))
        parts = [s_rev[i : i + 3] for i in range(0, len(s_rev), 3)]
        s_dot = ".".join("".join(reversed(p)) for p in parts[::-1])
        return f"{s_dot}đ"
    return s


def extract_highlight(name: str, desc: str) -> str:
    """
    Lấy 2-3 câu ưu điểm nổi bật từ mô tả.
    Không dùng GPT để tránh bịa sản phẩm.
    """
    base = desc.strip()
    if not base:
        return (
            f"{name} là mẫu đang được nhiều khách lựa chọn vì form đẹp, dễ phối đồ "
            f"và phù hợp nhiều dáng người. Chất liệu ổn định, mặc đi làm hay đi chơi đều ok."
        )

    cleaned = re.sub(r"\s+", " ", base)
    parts = re.split(r"([.!?])\s+", cleaned)
    sentences = []
    for i in range(0, len(parts), 2):
        seg = parts[i].strip()
        if not seg:
            continue
        punct = ""
        if i + 1 < len(parts):
            punct = parts[i + 1]
        sent = seg + punct
        sentences.append(sent)
        if len(sentences) >= 3:
            break

    if not sentences:
        return cleaned[:220]

    highlight = " ".join(sentences)
    if len(highlight) > 220:
        highlight = highlight[:217] + "..."
    return highlight


# =============================
# TÌM SẢN PHẨM
# =============================


def search_product_rows(df: pd.DataFrame, text: str) -> pd.DataFrame:
    """
    Tìm sản phẩm theo:
    - Mã sản phẩm
    - Mã mẫu mã
    - Tên sản phẩm
    - Keyword sản phẩm
    """
    if df.empty:
        return df
    t = text.lower().strip()
    if not t:
        return df.iloc[0:0]

    cols = ["Mã sản phẩm", "Mã mẫu mã", "Tên sản phẩm", "Keyword sản phẩm"]
    mask = None
    for col in cols:
        if col in df.columns:
            col_series = df[col].astype(str).str.lower()
            cond = col_series.str.contains(t, na=False)
            mask = cond if mask is None else (mask | cond)

    if mask is None:
        return df.iloc[0:0]

    matched = df[mask]

    # Nếu không thấy → thử tìm theo từ khoá đầu tiên
    if matched.empty:
        tokens = [w for w in re.split(r"\s+", t) if w]
        if not tokens:
            return df.iloc[0:0]
        mask2 = None
        for col in cols:
            if col in df.columns:
                col_series = df[col].astype(str).str.lower()
                cond = col_series.str.contains(tokens[0], na=False)
                mask2 = cond if mask2 is None else (mask2 | cond)
        if mask2 is None:
            return df.iloc[0:0]
        matched = df[mask2]

    return matched


def group_by_product(df: pd.DataFrame, row: pd.Series) -> pd.DataFrame:
    """
    Gom biến thể theo Mã sản phẩm.
    """
    product_code = str(row.get("Mã sản phẩm", "")).strip()
    if not product_code:
        return row.to_frame().T
    group = df[df["Mã sản phẩm"] == product_code]
    if group.empty:
        return row.to_frame().T
    return group


def get_product_images(group: pd.DataFrame):
    """
    Ảnh chung của sản phẩm: lấy từ Hình sản phẩm + Images, bỏ trùng.
    """
    urls = []
    seen = set()
    for _, r in group.iterrows():
        for col in ["Hình sản phẩm", "Images"]:
            if col in group.columns:
                for u in parse_image_urls(r.get(col, "")):
                    if u not in seen:
                        seen.add(u)
                        urls.append(u)
    return urls


def get_images_for_price(group: pd.DataFrame, price_value: str):
    """
    Ảnh theo từng mức giá: gom tất cả ảnh của các dòng có Giá bán = price_value.
    """
    subset = group[group["Giá bán"] == price_value]
    urls = []
    seen = set()
    for _, r in subset.iterrows():
        for col in ["Hình sản phẩm", "Images"]:
            if col in subset.columns:
                for u in parse_image_urls(r.get(col, "")):
                    if u not in seen:
                        seen.add(u)
                        urls.append(u)
    return urls


# =============================
# TƯ VẤN SẢN PHẨM THEO LOGIC MỚI
# =============================


def handle_product_reply(psid: str, text: str):
    df = load_products()
    if df.empty:
        send_text(
            psid,
            "Hiện tại shop chưa tải được danh sách sản phẩm, bạn quay lại giúp shop sau ít phút nhé.",
        )
        return

    matched = search_product_rows(df, text)
    if matched.empty:
        send_text(
            psid,
            "Shop chưa tìm thấy sản phẩm phù hợp với yêu cầu của bạn. "
            "Bạn gửi giúp shop tên sản phẩm hoặc mã sản phẩm cụ thể hơn nhé ❤️",
        )
        return

    # Chọn 1 sản phẩm đầu tiên trong danh sách match
    first = matched.iloc[0]
    group = group_by_product(df, first)

    name = str(first.get("Tên sản phẩm", "")).strip() or "Sản phẩm này"
    desc = str(first.get("Mô tả", "")).strip()
    highlight = extract_highlight(name, desc)

    # Lấy danh sách giá
    prices_raw = list(group["Giá bán"].unique())
    prices = [p for p in prices_raw if str(p).strip() != ""]
    if not prices:
        prices = []

    # Ảnh chung của sản phẩm (5 ảnh đầu)
    general_images = get_product_images(group)
    general_images = general_images[:5]

    # Gửi phần giới thiệu + ưu điểm nổi bật
    intro_text_lines = [
        f"✨ {name}",
        "",
        highlight,
    ]
    send_text(psid, "\n".join(intro_text_lines))

    # Gửi ảnh chung (không trùng)
    global_seen = set()
    for u in general_images:
        if u not in global_seen:
            global_seen.add(u)
            send_image(psid, u)

    # -----------------------
    # B. SẢN PHẨM CHỈ CÓ 1 GIÁ
    # -----------------------
    if len(prices) <= 1:
        price_value = prices[0] if prices else ""
        price_str = format_price(price_value) if price_value else "đang được shop cập nhật"

        # Gửi tất cả ảnh (sau khi gửi phần 5 ảnh chung)
        all_images = get_product_images(group)
        for u in all_images:
            if u not in global_seen:
                global_seen.add(u)
                send_image(psid, u)

        # Gửi giá + CTA
        send_text(psid, f"Giá sản phẩm đặc biệt cho anh/chị hôm nay là: {price_str} miễn ship ạ.")
        send_text(
            psid,
            "Anh/chị ưng mẫu nào, hoặc cần xem thêm hình cứ nhắn cho shop nhé, "
            "shop hỗ trợ chốt đơn liền ạ ❤️",
        )
        return

    # -----------------------
    # A. SẢN PHẨM CÓ NHIỀU GIÁ
    # -----------------------
    formatted_prices = [format_price(p) for p in prices]
    send_text(
        psid,
        "Sản phẩm này đang có nhiều mức giá khác nhau theo mẫu / phân loại.\n"
        "Shop gửi chi tiết từng mức giá để anh/chị dễ so sánh nhé:",
    )

    for raw_p, fmt_p in zip(prices, formatted_prices):
        # Text giới thiệu mức giá
        send_text(psid, f"💰 Giá {fmt_p} áp dụng cho các mẫu sau:")

        # Ảnh tương ứng mức giá này
        imgs = get_images_for_price(group, raw_p)

        # Gửi TẤT CẢ ảnh theo từng mức giá (bỏ trùng toàn sản phẩm)
        for u in imgs:
            if u not in global_seen:
                global_seen.add(u)
                send_image(psid, u)

    send_text(
        psid,
        "Anh/chị thấy ưng mức giá nào hoặc mẫu nào thì nhắn lại giúp shop để chốt đơn nhé ❤️",
    )


# =============================
# WEBHOOK FACEBOOK
# =============================


@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    global BOT_ACTIVE, LAST_MID

    # XÁC THỰC
    if request.method == "GET":
        mode = request.args.get("hub.mode")
        token = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")
        if mode == "subscribe" and token == VERIFY_TOKEN:
            return challenge or "", 200
        return "Verification failed", 403

    # XỬ LÝ SỰ KIỆN
    data = request.json
    print("[WEBHOOK EVENT]", json.dumps(data, ensure_ascii=False)[:1000])

    if data.get("object") != "page":
        return "OK", 200

    for entry in data.get("entry", []):
        for event in entry.get("messaging", []):
            sender_id = event.get("sender", {}).get("id")
            recipient_id = event.get("recipient", {}).get("id")
            if not sender_id:
                continue

            # 1. BỎ QUA ECHO (page tự gửi)
            if "message" in event and event["message"].get("is_echo"):
                print("[ECHO] Skip echo message")
                continue

            # 2. BỎ QUA delivery / read
            if "delivery" in event or "read" in event:
                print("[DELIVERY/READ] Skip status event")
                continue

            msg = event.get("message", {})
            mid = msg.get("mid")
            if mid:
                last = LAST_MID.get(sender_id)
                if last == mid:
                    print("[DUPLICATE] Skip same MID")
                    continue
                LAST_MID[sender_id] = mid

            text = msg.get("text", "") if msg else ""
            text_lower = text.lower().strip() if text else ""

            # 3. TẮT / BẬT BOT
            if text_lower in ["tắt bot", "tat bot", "stop bot", "dừng bot", "dung bot"]:
                if not ADMIN_PSID or sender_id == ADMIN_PSID:
                    BOT_ACTIVE = False
                    save_status()
                    send_text(sender_id, "⛔ Bot đã tạm dừng. Nhân viên sẽ trực tiếp hỗ trợ anh/chị nhé.")
                else:
                    send_text(sender_id, "Bạn không có quyền tắt bot, shop sẽ hỗ trợ bạn ngay ạ ❤️")
                continue

            if text_lower in ["bật bot", "bat bot", "start bot"]:
                if not ADMIN_PSID or sender_id == ADMIN_PSID:
                    BOT_ACTIVE = True
                    save_status()
                    send_text(sender_id, "▶ Bot đã được bật lại, shop tiếp tục hỗ trợ anh/chị nhé ❤️")
                else:
                    send_text(sender_id, "Bạn không có quyền bật bot, shop sẽ hỗ trợ bạn ngay ạ ❤️")
                continue

            # Nếu bot đang tắt → bỏ qua
            if not BOT_ACTIVE:
                print("[BOT OFF] ignore user message")
                continue

            # 4. KHÁCH GỬI ẢNH / TỆP
            attachments = msg.get("attachments", []) if msg else []
            if attachments:
                send_text(
                    sender_id,
                    "Shop đã nhận được hình/đính kèm của anh/chị.\n"
                    "Anh/chị vui lòng nhắn thêm tên sản phẩm hoặc nhu cầu "
                    "(ví dụ: váy trắng, quần jean nam size L...) để shop lọc mẫu phù hợp nhất nhé ❤️",
                )
                continue

            # 5. KHÁCH GỬI TEXT → TƯ VẤN SẢN PHẨM
            if text:
                handle_product_reply(sender_id, text)
            else:
                send_text(
                    sender_id,
                    "Anh/chị có thể gửi giúp shop tên sản phẩm, mã sản phẩm hoặc nhu cầu "
                    "(ví dụ: đầm công sở, áo phông nữ, quần short nam...) để shop tư vấn chi tiết hơn ạ ❤️",
                )

    return "OK", 200


@app.route("/", methods=["GET"])
def home():
    return "Messenger product bot is running.", 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=True)
