import os
import json
import re
import time
import csv
import hashlib
from collections import defaultdict
from urllib.parse import quote
from datetime import datetime

import requests
from flask import Flask, request, send_from_directory
from openai import OpenAI

# ============================================
# FLASK APP
# ============================================
app = Flask(__name__)

# ============================================
# ENV & CONFIG
# ============================================
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN", "").strip()
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
GOOGLE_SHEET_CSV_URL = os.getenv("SHEET_CSV_URL", "").strip()
DOMAIN = os.getenv("DOMAIN", "").strip() or "fb-gpt-chatbot.onrender.com"
FANPAGE_NAME = os.getenv("FANPAGE_NAME", "Shop thời trang")
FCHAT_WEBHOOK_URL = os.getenv("FCHAT_WEBHOOK_URL", "").strip()
FCHAT_TOKEN = os.getenv("FCHAT_TOKEN", "").strip()

if not GOOGLE_SHEET_CSV_URL:
    GOOGLE_SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv"

# ============================================
# OPENAI CLIENT
# ============================================
client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ============================================
# GLOBAL STATE
# ============================================
USER_CONTEXT = defaultdict(lambda: {
    "last_msg_time": 0,
    "last_ms": None,
    "order_state": None,
    "order_data": {},
    "processing_lock": False,
    "postback_count": 0,
    "product_info_sent_ms": None,
    "last_product_info_time": 0,
    "last_postback_time": 0,
    "processed_postbacks": set(),
    "last_product_images_sent": {},
    "product_history": [],
})
PRODUCTS = {}
LAST_LOAD = 0
LOAD_TTL = 300

# Các từ khóa liên quan đến đặt hàng
ORDER_KEYWORDS = [
    "đặt hàng nha",
    "ok đặt",
    "ok mua",
    "ok em",
    "ok e",
    "mua 1 cái",
    "mua cái này",
    "mua luôn",
    "chốt",
    "lấy mã",
    "lấy mẫu",
    "lấy luôn",
    "lấy em này",
    "lấy e này",
    "gửi cho",
    "ship cho",
    "ship 1 cái",
    "chốt 1 cái",
    "cho tôi mua",
    "tôi lấy nhé",
    "cho mình đặt",
    "tôi cần mua",
    "xác nhận đơn hàng giúp tôi",
    "tôi đồng ý mua",
    "làm đơn cho tôi đi",
    "tôi chốt đơn nhé",
    "cho xin 1 cái",
    "cho đặt 1 chiếc",
    "bên shop tạo đơn giúp em",
    "okela",
    "ok bạn",
    "đồng ý",
    "được đó",
    "vậy cũng được",
    "được vậy đi",
    "chốt như bạn nói",
    "ok giá đó đi",
    "lấy mẫu đó đi",
    "tư vấn giúp mình đặt hàng",
    "hướng dẫn mình mua với",
    "bạn giúp mình đặt nhé",
    "muốn có nó quá",
    "muốn mua quá",
    "ưng quá, làm sao để mua",
    "chốt đơn",
    "bán cho em",
    "bán cho em vé",
    "xuống đơn giúp em",
    "đơm hàng",
    "lấy nha",
    "lấy nhé",
    "mua nha",
    "mình lấy đây",
    "shop ơi, của em",
    "vậy lấy cái",
    "thôi lấy cái",
    "order nhé",
]

# Từ khóa kích hoạt carousel
CAROUSEL_KEYWORDS = [
    "xem sản phẩm",
    "show sản phẩm",
    "có gì hot",
    "sản phẩm mới",
    "danh sách sản phẩm",
    "giới thiệu sản phẩm",
    "tất cả sản phẩm",
    "cho xem sản phẩm",
    "có mẫu nào",
    "mẫu mới",
    "hàng mới",
    "xem hàng",
    "show hàng",
]

# ============================================
# HELPER: SEND MESSAGE
# ============================================

def call_facebook_send_api(payload: dict, retry_count=2):
    """Gửi tin nhắn qua Facebook API với cơ chế retry và xử lý lỗi"""
    if not PAGE_ACCESS_TOKEN:
        print("[WARN] PAGE_ACCESS_TOKEN chưa được cấu hình, bỏ qua gửi tin nhắn.")
        return {}
    
    url = f"https://graph.facebook.com/v12.0/me/messages?access_token={PAGE_ACCESS_TOKEN}"
    
    for attempt in range(retry_count):
        try:
            resp = requests.post(url, json=payload, timeout=10)
            
            if resp.status_code == 200:
                return resp.json()
            else:
                error_data = resp.json()
                error_code = error_data.get("error", {}).get("code")
                error_subcode = error_data.get("error", {}).get("error_subcode")
                
                if error_code == 100 and error_subcode == 2018001:
                    print(f"[ERROR] Người dùng đã chặn/hủy kết nối với trang. Không thể gửi tin nhắn.")
                    return {}
                
                print(f"Facebook Send API error (attempt {attempt+1}):", resp.text)
                
                if attempt < retry_count - 1:
                    time.sleep(0.5)
                    
        except Exception as e:
            print(f"Facebook Send API exception (attempt {attempt+1}):", e)
            if attempt < retry_count - 1:
                time.sleep(0.5)
    
    return {}


def send_message(recipient_id: str, text: str):
    if not text:
        return
    if len(text) > 2000:
        print(f"[WARN] Tin nhắn quá dài ({len(text)} ký tự), cắt ngắn lại")
        text = text[:1997] + "..."
    
    payload = {
        "recipient": {"id": recipient_id},
        "message": {"text": text},
    }
    return call_facebook_send_api(payload)


def send_image(recipient_id: str, image_url: str):
    if not image_url:
        return ""
    payload = {
        "recipient": {"id": recipient_id},
        "message": {
            "attachment": {
                "type": "image",
                "payload": {"url": image_url, "is_reusable": True},
            }
        },
    }
    return call_facebook_send_api(payload)


def send_carousel_template(recipient_id: str, elements: list):
    if not elements:
        return ""
    payload = {
        "recipient": {"id": recipient_id},
        "message": {
            "attachment": {
                "type": "template",
                "payload": {
                    "template_type": "generic",
                    "elements": elements[:10],
                },
            }
        },
    }
    return call_facebook_send_api(payload)


def send_quick_replies(recipient_id: str, text: str, quick_replies: list):
    payload = {
        "recipient": {"id": recipient_id},
        "message": {
            "text": text,
            "quick_replies": quick_replies,
        },
    }
    return call_facebook_send_api(payload)


# ============================================
# HELPER: PRODUCTS
# ============================================

def parse_image_urls(raw: str):
    if not raw:
        return []
    parts = re.split(r'[,\n;|]+', raw)
    urls = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        if "alicdn.com" in p or "taobao" in p or "1688.com" in p or p.startswith("http"):
            urls.append(p)
    seen = set()
    result = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            result.append(u)
    return result


def should_use_as_first_image(url: str):
    if not url:
        return False
    return True


def short_description(text: str, limit: int = 220) -> str:
    """Rút gọn mô tả sản phẩm cho dễ đọc trong chat."""
    if not text:
        return ""
    clean = re.sub(r"\s+", " ", str(text)).strip()
    if len(clean) <= limit:
        return clean
    return clean[:limit].rstrip() + "..."


def extract_price_int(price_str: str):
    """Trả về giá dạng int từ chuỗi '849.000đ', '849,000'... Nếu không đọc được trả về None."""
    if not price_str:
        return None
    m = re.search(r"(\d[\d.,]*)", str(price_str))
    if not m:
        return None
    cleaned = m.group(1).replace(".", "").replace(",", "")
    try:
        return int(cleaned)
    except Exception:
        return None


def load_products(force=False):
    """
    Đọc dữ liệu từ Google Sheet CSV, cache trong 300s.
    PHƯƠNG ÁN A: Mỗi dòng = 1 biến thể, gom theo Mã sản phẩm và lưu danh sách variants.
    """
    global PRODUCTS, LAST_LOAD
    now = time.time()
    if not force and PRODUCTS and (now - LAST_LOAD) < LOAD_TTL:
        return

    if not GOOGLE_SHEET_CSV_URL:
        print("❌ GOOGLE_SHEET_CSV_URL chưa được cấu hình! Không thể load sản phẩm.")
        return

    try:
        print(f"🟦 Loading sheet: {GOOGLE_SHEET_CSV_URL}")
        r = requests.get(GOOGLE_SHEET_CSV_URL, timeout=20)
        r.raise_for_status()
        r.encoding = "utf-8"
        content = r.text

        reader = csv.DictReader(content.splitlines())
        products = {}

        for raw_row in reader:
            row = dict(raw_row)

            ms = (row.get("Mã sản phẩm") or "").strip()
            if not ms:
                continue

            ten = (row.get("Tên sản phẩm") or "").strip()
            if not ten:
                continue

            gia_raw = (row.get("Giá bán") or "").strip()
            images = (row.get("Images") or "").strip()
            videos = (row.get("Videos") or "").strip()
            tonkho_raw = (row.get("Tồn kho") or "").strip()
            mota = (row.get("Mô tả") or "").strip()
            mau = (row.get("màu (Thuộc tính)") or "").strip()
            size = (row.get("size (Thuộc tính)") or "").strip()

            gia_int = extract_price_int(gia_raw)
            try:
                tonkho_int = int(str(tonkho_raw)) if str(tonkho_raw).strip() else None
            except Exception:
                tonkho_int = None

            if ms not in products:
                base = {
                    "MS": ms,
                    "Ten": ten,
                    "Gia": gia_raw,
                    "MoTa": mota,
                    "Images": images,
                    "Videos": videos,
                    "Tồn kho": tonkho_raw,
                    "màu (Thuộc tính)": mau,
                    "size (Thuộc tính)": size,
                }
                base["variants"] = []
                base["all_colors"] = set()
                base["all_sizes"] = set()
                products[ms] = base

            p = products[ms]

            if not p.get("Images") and images:
                p["Images"] = images
            if not p.get("Videos") and videos:
                p["Videos"] = videos
            if not p.get("MoTa") and mota:
                p["MoTa"] = mota
            if not p.get("Gia") and gia_raw:
                p["Gia"] = gia_raw
            if not p.get("Tồn kho") and tonkho_raw:
                p["Tồn kho"] = tonkho_raw

            variant = {
                "mau": mau,
                "size": size,
                "gia": gia_int,
                "gia_raw": gia_raw,
                "tonkho": tonkho_int if tonkho_int is not None else tonkho_raw,
            }
            p["variants"].append(variant)

            if mau:
                p["all_colors"].add(mau)
            if size:
                p["all_sizes"].add(size)

        for ms, p in products.items():
            colors = sorted(list(p.get("all_colors") or []))
            sizes = sorted(list(p.get("all_sizes") or []))
            p["màu (Thuộc tính)"] = ", ".join(colors) if colors else p.get("màu (Thuộc tính)", "")
            p["size (Thuộc tính)"] = ", ".join(sizes) if sizes else p.get("size (Thuộc tính)", "")
            p["ShortDesc"] = short_description(p.get("MoTa", ""))

        PRODUCTS = products
        LAST_LOAD = now
        print(f"📦 Loaded {len(PRODUCTS)} products (PHƯƠNG ÁN A).")
    except Exception as e:
        print("❌ load_products ERROR:", e)


# ============================================
# HELPER: POLICY INFO EXTRACTION
# ============================================

def clean_policy_text(text: str) -> str:
    """
    Làm sạch văn bản chính sách, loại bỏ thông tin không cần thiết
    """
    if not text:
        return ""
    
    text = re.sub(r'#\S+', '', text)
    text = re.sub(r'@\S+', '', text)
    text = re.sub(r'http\S+', '', text)
    
    text = re.sub(r'\b\d{10,}\b', '', text)
    text = re.sub(r'\b\d{1,3}[/-]\d{1,3}[/-]\d{1,4}\b', '', text)
    
    text = ' '.join(text.split())
    
    if len(text) > 250:
        sentences = re.split(r'[.!?]', text)
        if sentences and len(sentences[0]) > 50:
            text = sentences[0].strip()
            if not text.endswith('.'):
                text += '.'
        else:
            text = text[:250].rstrip() + '...'
    
    return text


def extract_policy_info_from_description(description: str) -> dict:
    """
    Trích xuất thông tin chính sách từ cột Mô tả trong sheet
    """
    if not description:
        return {}
    
    policies = {}
    lower_desc = description.lower()
    
    sentences = re.split(r'[.!?;\n]+', description)
    sentences = [s.strip() for s in sentences if s.strip()]
    
    def find_sentence_with_keywords(keywords_list):
        for sentence in sentences:
            lower_sentence = sentence.lower()
            if any(keyword in lower_sentence for keyword in keywords_list):
                if len(sentence) > 200:
                    words = sentence.split()
                    if len(words) > 30:
                        return ' '.join(words[:30]) + '...'
                return sentence
        return None
    
    shipping_keywords = ['ship', 'vận chuyển', 'giao hàng', 'phí ship', 'miễn phí ship', 'miễn ship', 'free ship']
    shipping_info = find_sentence_with_keywords(shipping_keywords)
    if shipping_info:
        policies['shipping'] = clean_policy_text(shipping_info)
    
    return_keywords = ['đổi trả', 'hoàn tiền', 'bảo hành', 'đổi hàng', 'trả hàng', 'bảo đảm']
    return_info = find_sentence_with_keywords(return_keywords)
    if return_info:
        policies['return_warranty'] = clean_policy_text(return_info)
    
    payment_keywords = ['thanh toán', 'payment', 'cod', 'chuyển khoản', 'tiền mặt', 'chuyển tiền']
    payment_info = find_sentence_with_keywords(payment_keywords)
    if payment_info:
        policies['payment'] = clean_policy_text(payment_info)
    
    if not policies and len(description) > 0:
        for keyword_set, policy_key in [
            (shipping_keywords, 'shipping'),
            (return_keywords, 'return_warranty'),
            (payment_keywords, 'payment')
        ]:
            for keyword in keyword_set:
                if keyword in lower_desc:
                    idx = lower_desc.find(keyword)
                    if idx != -1:
                        start = max(0, idx - 50)
                        end = min(len(description), idx + 150)
                        excerpt = description[start:end].strip()
                        if excerpt:
                            policies[policy_key] = clean_policy_text(excerpt)
                            break
    
    return policies


def generate_policy_response(product_description: str, question: str) -> str:
    """
    Tạo câu trả lời về chính sách dựa trên mô tả sản phẩm
    """
    policies = extract_policy_info_from_description(product_description)
    lower_question = question.lower()
    
    def clean_response(text, max_length=200):
        if not text:
            return text
        text = re.sub(r'#\S+', '', text)
        text = re.sub(r'@\S+', '', text)
        text = ' '.join(text.split())
        if len(text) > max_length:
            last_period = text[:max_length].rfind('.')
            if last_period > max_length * 0.5:
                return text[:last_period + 1]
            else:
                return text[:max_length].rstrip() + '...'
        return text
    
    if any(keyword in lower_question for keyword in ['ship', 'vận chuyển', 'giao hàng', 'phí ship', 'miễn ship', 'free ship']):
        if 'shipping' in policies:
            response = clean_response(policies['shipping'])
            if 'miễn phí' not in response.lower() and 'miễn ship' not in response.lower():
                if 'miễn' in lower_question or 'free' in lower_question:
                    return f"Dạ, {response}\n\nNếu anh/chị cần biết thêm chi tiết về điều kiện miễn phí ship, em có thể kiểm tra lại với shop ạ."
            return f"Dạ, {response}"
        else:
            return "Hiện tại em không tìm thấy thông tin vận chuyển cụ thể cho sản phẩm này. Chính sách chung của shop là giao hàng toàn quốc, phí ship từ 20-50k tùy khu vực ạ."
    
    elif any(keyword in lower_question for keyword in ['đổi trả', 'hoàn tiền', 'đổi hàng', 'trả hàng']):
        if 'return_warranty' in policies:
            response = clean_response(policies['return_warranty'])
            return f"Dạ, {response}"
        else:
            return "Hiện tại em không tìm thấy thông tin đổi trả cụ thể. Chính sách chung của shop là đổi trả trong 3-7 ngày nếu sản phẩm lỗi, anh/chị giữ nguyên tem mác ạ."
    
    elif 'bảo hành' in lower_question:
        if 'return_warranty' in policies:
            response = clean_response(policies['return_warranty'])
            return f"Dạ, {response}"
        else:
            return "Hiện tại em không tìm thấy thông tin bảo hành cụ thể. Anh/chị vui lòng liên hệ shop để biết chi tiết về chính sách bảo hành ạ."
    
    elif any(keyword in lower_question for keyword in ['thanh toán', 'payment', 'cod', 'chuyển khoản']):
        if 'payment' in policies:
            response = clean_response(policies['payment'])
            return f"Dạ, {response}"
        else:
            return "Shop hỗ trợ thanh toán khi nhận hàng (COD) và chuyển khoản ngân hàng ạ."
    
    else:
        response_parts = []
        if policies:
            response_parts.append("Dạ, thông tin chính sách cho sản phẩm:")
            
            if 'shipping' in policies:
                shipping_info = clean_response(policies['shipping'], 150)
                response_parts.append(f"• Vận chuyển: {shipping_info}")
            
            if 'return_warranty' in policies:
                return_info = clean_response(policies['return_warranty'], 150)
                response_parts.append(f"• Đổi trả/Bảo hành: {return_info}")
            
            if 'payment' in policies:
                payment_info = clean_response(policies['payment'], 150)
                response_parts.append(f"• Thanh toán: {payment_info}")
        else:
            response_parts.append("Hiện tại em không tìm thấy thông tin chính sách cụ thể cho sản phẩm này.")
            response_parts.append("Chính sách chung của shop:")
            response_parts.append("• Giao hàng toàn quốc, phí ship từ 20-50k")
            response_parts.append("• Đổi trả trong 3 ngày nếu sản phẩm lỗi")
            response_parts.append("• Thanh toán khi nhận hàng (COD)")
        
        return "\n".join(response_parts)


# ============================================
# GPT PROMPT
# ============================================

def build_product_system_prompt(product: dict | None, ms: str | None):
    """
    PROMPT cho GPT
    """

    if not client or not OPENAI_API_KEY:
        return None

    if not ms or not product:
        return (
            "Bạn là trợ lý bán hàng online của một shop thời trang trên Facebook. "
            "Giọng điệu thân thiện, tự nhiên, chuyên nghiệp, xưng 'em', gọi khách là 'anh/chị'. "
            "Hiện tại bạn CHƯA có thông tin sản phẩm cụ thể nào. "
            "Khi khách hỏi về sản phẩm, hãy nhẹ nhàng đề nghị họ gửi mã sản phẩm dạng [MSxxxxx] "
            "hoặc gửi hình ảnh sản phẩm để em tra cứu. "
            "Không được bịa thông tin về sản phẩm khi chưa có dữ liệu thật. "
            "Chỉ được phép trả lời các câu hỏi chung chung về quy trình mua hàng, cách đặt hàng, "
            "nhưng vẫn nên hướng khách cung cấp mã sản phẩm để tư vấn chính xác hơn."
        )

    ten = product.get("Ten", "")
    gia = product.get("Gia", "")
    mau = product.get("màu (Thuộc tính)", "")
    size = product.get("size (Thuộc tính)", "")
    tonkho = product.get("Tồn kho", "")
    mota = product.get("MoTa", "")

    policies = extract_policy_info_from_description(mota)

    prompt = f"""
Bạn là TRỢ LÝ TƯ VẤN BÁN HÀNG CHUYÊN NGHIỆP của một shop thời trang trên Facebook.

Phong cách giao tiếp:
- Xưng "em", gọi khách là "anh/chị"
- Giọng điệu: thân thiện, ấm áp, lễ phép, trả lời tự nhiên như đang chat Messenger
- Tập trung giải thích đơn giản, dễ hiểu, ưu tiên lợi ích thực tế cho khách
- Không dùng câu chữ quá máy móc, không liệt kê khô khan như robot

Bạn CHỈ được phép tư vấn dựa trên dữ liệu SẢN PHẨM dưới đây, không được bịa thêm:

• Mã sản phẩm: {ms}
• Tên: {ten}
• Giá bán: {gia}
• Màu có sẵn: {mau or 'Không có thông tin'}
• Size có sẵn: {size or 'Không có thông tin'}
• Tồn kho: {tonkho or 'Không có thông tin'}
• Mô tả: {mota or 'Không có mô tả chi tiết'}

Thông tin chính sách trích từ mô tả (nếu có):
"""

    for k, v in policies.items():
        if k == "shipping":
            prompt += f"- Vận chuyển: {v}\n"
        if k == "return_warranty":
            prompt += f"- Đổi trả/Bảo hành: {v}\n"
        if k == "payment":
            prompt += f"- Thanh toán: {v}\n"

    prompt += """
QUY TẮC TRẢ LỜI:

1. CHỈ sử dụng đúng thông tin có trong dữ liệu sản phẩm ở trên.
2. KHÔNG được bịa thêm chất liệu, xuất xứ, bảo hành… nếu không có trong dữ liệu.
3. Nếu khách hỏi thông tin mà hệ thống không có, hãy nói nhẹ nhàng kiểu:
   "Dạ phần này trong hệ thống chưa có thông tin ạ, em sợ nói sai nên không dám khẳng định."
4. Nếu không có thông tin chính sách cụ thể, có thể dùng chính sách chung:
   - Giao hàng toàn quốc
   - Hỗ trợ đổi trả khi sản phẩm lỗi, còn tem mác
5. Luôn ưu tiên trả lời NGẮN – RÕ – DỄ HIỂU, không viết quá dài dòng.
6. Cuối mỗi câu trả lời, hãy gợi ý khéo:
   - "Anh/chị thích mẫu này màu nào, size gì để em tư vấn chuẩn hơn ạ?"
   - Hoặc: "Nếu anh/chị ưng rồi thì cho em xin thông tin để em lên đơn luôn giúp mình nhé."

Hãy trả lời 100% bằng tiếng Việt, tự nhiên như một nhân viên tư vấn bán hàng đang chat với khách trên Messenger.
"""

    return prompt


def build_chatgpt_reply(uid: str, text: str, ms: str | None):
    """
    Gọi OpenAI để trả lời câu hỏi của khách hàng.
    """
    if not client or not OPENAI_API_KEY:
        return "Hiện tại hệ thống trợ lý AI đang bảo trì, anh/chị nhắn trực tiếp để shop hỗ trợ giúp em với ạ."

    load_products()

    product = None
    if ms and ms in PRODUCTS:
        product = PRODUCTS[ms]
    else:
        return (
            "Em chưa thấy mã sản phẩm trong hệ thống ạ.\n"
            "Anh/chị gửi giúp em mã sản phẩm dạng [MSxxxxx] hoặc gửi lại hình sản phẩm để em kiểm tra chi tiết nhé."
        )

    system_prompt = build_product_system_prompt(product, ms)

    if not system_prompt:
        return (
            "Hiện tại em chưa truy cập được dữ liệu sản phẩm trong hệ thống, "
            "anh/chị vui lòng nhắn lại sau ít phút hoặc inbox trực tiếp fanpage giúp em với ạ."
        )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": text},
    ]

    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.55,
            max_tokens=350,
            timeout=10.0,
        )
        reply = (resp.choices[0].message.content or "").strip()

        if not reply or len(reply) < 10:
            return (
                "Em chưa có đủ thông tin để trả lời chính xác câu này ạ.\n"
                "Anh/chị cho em xin thêm chi tiết (hoặc mã sản phẩm) để em hỗ trợ kỹ hơn nhé."
            )

        return reply

    except Exception as e:
        print("OpenAI error:", e)
        return (
            "Hiện tại em đang gặp chút trục trặc kỹ thuật với trợ lý AI, "
            "anh/chị vui lòng nhắn lại sau ít phút hoặc để lại số điện thoại, "
            "shop sẽ chủ động gọi hỗ trợ mình ạ."
        )


def generate_product_advantage(product_name: str, description: str) -> str:
    """Tạo ưu điểm sản phẩm ngắn gọn từ tên và mô tả"""
    try:
        if client and OPENAI_API_KEY:
            desc_short = description[:300] if description else ""
            
            prompt = f"""Dựa trên tên sản phẩm và mô tả dưới đây, hãy tạo ra MỘT câu ưu điểm ngắn gọn, hấp dẫn (tối đa 15 từ):
            
Tên sản phẩm: {product_name}
Mô tả: {desc_short}

Yêu cầu:
1. Chỉ dựa vào thông tin trên
2. Không thêm thông tin không có trong mô tả
3. Ngắn gọn, dễ hiểu"""
            
            try:
                resp = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "Bạn là chuyên gia tóm tắt ưu điểm sản phẩm."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.5,
                    max_tokens=50,
                    timeout=10
                )
                advantage = resp.choices[0].message.content.strip()
                advantage = advantage.strip('"\'').strip()
                if len(advantage.split()) > 20:
                    words = advantage.split()[:15]
                    advantage = " ".join(words) + "..."
                return advantage
            except Exception as e:
                print(f"Lỗi khi tạo ưu điểm bằng GPT: {e}")
        
        name_lower = product_name.lower()
        
        if any(word in name_lower for word in ['áo', 'áo thun', 't-shirt', 'shirt']):
            return "Chất liệu cotton mềm mại, form dáng chuẩn"
        elif any(word in name_lower for word in ['quần', 'pants', 'jeans', 'trousers']):
            return "Chất liệu bền đẹp, thiết kế thời trang"
        elif any(word in name_lower for word in ['váy', 'đầm', 'dress', 'skirt']):
            return "Thiết kế nữ tính, chất liệu cao cấp"
        elif any(word in name_lower for word in ['giày', 'dép', 'sandal', 'sneaker']):
            return "Thiết kế đẹp, chất liệu bền đẹp"
        elif any(word in name_lower for word in ['túi', 'balo', 'ví', 'bag', 'backpack']):
            return "Thiết kế sang trọng, nhiều ngăn tiện lợi"
        else:
            return "Chất lượng cao cấp, thiết kế thời trang"
            
    except Exception as e:
        print(f"Lỗi trong generate_product_advantage: {e}")
        return "Sản phẩm chất lượng cao"


def generate_product_description_bullets(description: str) -> str:
    """Tạo mô tả sản phẩm dạng bullet points từ mô tả gốc"""
    try:
        if client and OPENAI_API_KEY:
            clean_desc = re.sub(r'#\S+', '', description)
            clean_desc = re.sub(r'@\S+', '', clean_desc)
            clean_desc = ' '.join(clean_desc.split())
            
            if len(clean_desc) < 20:
                return clean_desc
            
            prompt = f"""Từ mô tả sau, tạo 3-5 bullet points ngắn gọn (mỗi bullet tối đa 15 từ):
            
Mô tả: {clean_desc[:500]}

Yêu cầu:
1. Chỉ dùng thông tin từ mô tả
2. Không thêm thông tin mới
3. Ngắn gọn, rõ ràng"""
            
            try:
                resp = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "Bạn là chuyên gia tóm tắt thông tin sản phẩm."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.4,
                    max_tokens=150,
                    timeout=10
                )
                bullets = resp.choices[0].message.content.strip()
                
                if bullets:
                    bullets = bullets.strip('"\'')
                    lines = bullets.split('\n')
                    cleaned_lines = []
                    for line in lines:
                        line = line.strip()
                        if line and not line.startswith('•'):
                            line = f"• {line}"
                        if line:
                            cleaned_lines.append(line)
                    
                    cleaned_lines = cleaned_lines[:5]
                    if cleaned_lines:
                        return "\n".join(cleaned_lines)
            except Exception as e:
                print(f"Lỗi khi tạo bullet points bằng GPT: {e}")
        
        clean_desc = re.sub(r'#\S+', '', description)
        clean_desc = re.sub(r'@\S+', '', clean_desc)
        clean_desc = ' '.join(clean_desc.split())
        
        if len(clean_desc) < 50:
            return clean_desc
        
        sentences = re.split(r'[.!?\n;]+', clean_desc)
        sentences = [s.strip() for s in sentences if s.strip()]
        
        bullets = []
        for sent in sentences:
            if 5 <= len(sent.split()) <= 20:
                bullets.append(f"• {sent}")
            if len(bullets) >= 5:
                break
        
        if bullets:
            return "\n".join(bullets[:5])
        else:
            if len(clean_desc) > 300:
                clean_desc = clean_desc[:297] + "..."
            return clean_desc
            
    except Exception as e:
        print(f"Lỗi trong generate_product_description_bullets: {e}")
        clean_desc = re.sub(r'#\S+', '', description)
        clean_desc = re.sub(r'@\S+', '', clean_desc)
        clean_desc = ' '.join(clean_desc.split())
        if len(clean_desc) > 300:
            clean_desc = clean_desc[:297] + "..."
        return clean_desc


# ============================================
# CẢI THIỆN NGỮ CẢNH
# ============================================

def update_product_context(uid: str, ms: str):
    """Cập nhật ngữ cảnh sản phẩm cho user"""
    ctx = USER_CONTEXT[uid]
    
    ctx["last_ms"] = ms
    
    if "product_history" not in ctx:
        ctx["product_history"] = []
    
    if ms in ctx["product_history"]:
        ctx["product_history"].remove(ms)
    
    ctx["product_history"].insert(0, ms)
    
    if len(ctx["product_history"]) > 5:
        ctx["product_history"] = ctx["product_history"][:5]


def should_use_gpt(text: str, ms: str | None) -> bool:
    """
    Kiểm tra xem có nên dùng GPT để trả lời không
    Chỉ dùng GPT khi có mã sản phẩm VÀ câu hỏi không phải là câu chào hỏi đơn giản
    """
    if not ms:
        return False
    
    lower = text.lower()
    
    # Danh sách các từ chào hỏi đơn giản - không dùng GPT cho các câu này
    greeting_keywords = [
        'chào', 'hi', 'hello', 'xin chào', 'cảm ơn', 'thank', 'thanks',
        'ok', 'okay', 'ừ', 'ừm', 'vâng', 'dạ', 'tạm biệt', 'bye', 'goodbye'
    ]
    
    # Nếu là câu chào hỏi đơn giản -> không dùng GPT
    if any(greeting in lower for greeting in greeting_keywords):
        return False
    
    # Nếu câu quá ngắn (ít hơn 3 ký tự) -> không dùng GPT
    if len(text.strip()) < 3:
        return False
    
    # Dùng GPT cho các câu hỏi còn lại
    return True


def get_relevant_product_for_question(uid: str, text: str) -> str | None:
    """
    Tìm sản phẩm phù hợp nhất cho câu hỏi dựa trên ngữ cảnh
    """
    ctx = USER_CONTEXT[uid]
    
    ms_from_text = detect_ms_from_text(text)
    if ms_from_text and ms_from_text in PRODUCTS:
        return ms_from_text
    
    last_ms = ctx.get("last_ms")
    if last_ms and last_ms in PRODUCTS:
        return last_ms
    
    product_history = ctx.get("product_history", [])
    for ms in product_history:
        if ms in PRODUCTS:
            return ms
    
    return None


# ============================================
# SEND PRODUCT INFO
# ============================================

def send_product_info_debounced(uid: str, ms: str):
    """Gửi thông tin chi tiết sản phẩm theo cấu trúc 6 messenger"""
    ctx = USER_CONTEXT[uid]
    now = time.time()

    last_ms = ctx.get("product_info_sent_ms")
    last_time = ctx.get("last_product_info_time", 0)

    if last_ms == ms and (now - last_time) < 5:
        print(f"[DEBOUNCE] Bỏ qua gửi lại thông tin sản phẩm {ms} cho user {uid} (chưa đủ 5s)")
        return
    elif last_ms != ms:
        ctx["last_product_info_time"] = 0
    
    ctx["product_info_sent_ms"] = ms
    ctx["last_product_info_time"] = now
    ctx["processing_lock"] = True

    try:
        load_products()
        product = PRODUCTS.get(ms)
        if not product:
            send_message(uid, "Em không tìm thấy sản phẩm này trong hệ thống, anh/chị kiểm tra lại mã giúp em ạ.")
            ctx["processing_lock"] = False
            return

        update_product_context(uid, ms)

        product_name = product.get('Ten', 'Sản phẩm')
        send_message(uid, f"📌 {product_name}")
        time.sleep(0.5)

        images_field = product.get("Images", "")
        urls = parse_image_urls(images_field)
        
        unique_images = []
        seen = set()
        for u in urls:
            if u and u not in seen:
                seen.add(u)
                unique_images.append(u)
        
        ctx["last_product_images_sent"][ms] = len(unique_images[:5])
        
        sent_count = 0
        for image_url in unique_images[:5]:
            if image_url:
                send_image(uid, image_url)
                sent_count += 1
                time.sleep(0.7)
        
        if sent_count == 0:
            send_message(uid, "📷 Sản phẩm chưa có hình ảnh ạ.")
        
        time.sleep(0.5)

        mo_ta = product.get("MoTa", "")
        
        if mo_ta:
            description_bullets = generate_product_description_bullets(mo_ta)
            if description_bullets.strip():
                send_message(uid, f"📝 THÔNG TIN SẢN PHẨM:\n{description_bullets}")
            else:
                send_message(uid, "📝 Sản phẩm hiện chưa có thông tin chi tiết ạ.")
        else:
            send_message(uid, "📝 Sản phẩm hiện chưa có thông tin chi tiết ạ.")
        
        time.sleep(0.5)

        advantage = generate_product_advantage(product_name, mo_ta)
        send_message(uid, f"✨ ƯU ĐIỂM NỔI BẬT:\n{advantage}")
        
        time.sleep(0.5)

        variants = product.get("variants", [])
        prices = []
        variant_details = []

        for variant in variants:
            gia_int = variant.get("gia")
            if gia_int and gia_int > 0:
                prices.append(gia_int)
                mau = variant.get("mau", "Mặc định")
                size = variant.get("size", "Mặc định")
                tonkho = variant.get("tonkho", "Còn hàng")
                
                if mau or size:
                    variant_str = f"{mau}" if mau else ""
                    if size:
                        variant_str += f" - {size}" if variant_str else f"{size}"
                    variant_details.append(f"{variant_str}: {gia_int:,.0f}đ")

        if not prices:
            gia_raw = product.get("Gia", "")
            gia_int = extract_price_int(gia_raw)
            if gia_int and gia_int > 0:
                prices.append(gia_int)

        if len(prices) == 0:
            price_msg = "💰 Giá đang cập nhật, vui lòng liên hệ shop để biết chi tiết"
        elif len(set(prices)) == 1:
            price = prices[0]
            if variant_details:
                price_msg = f"💰 GIÁ SẢN PHẨM:\n" + "\n".join(variant_details[:3])
                if len(variant_details) > 3:
                    price_msg += f"\n... và {len(variant_details)-3} phân loại khác"
            else:
                price_msg = f"💰 Giá ưu đãi: {price:,.0f}đ"
        else:
            min_price = min(prices)
            max_price = max(prices)
            if variant_details:
                price_msg = f"💰 GIÁ THEO PHÂN LOẠI:\n" + "\n".join(variant_details[:4])
                if len(variant_details) > 4:
                    price_msg += f"\n... và {len(variant_details)-4} phân loại khác"
            else:
                price_msg = f"💰 Giá chỉ từ {min_price:,.0f}đ đến {max_price:,.0f}đ"

        send_message(uid, price_msg)
        
        time.sleep(0.5)

        domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
        order_link = f"{domain}/order-form?ms={ms}&uid={uid}"
        send_message(uid, f"📋 Đặt hàng ngay tại đây:\n{order_link}")

    except Exception as e:
        print(f"Lỗi khi gửi thông tin sản phẩm: {str(e)}")
        try:
            send_message(uid, f"📌 Sản phẩm: {product.get('Ten', '')}\n\nCó lỗi khi tải thông tin chi tiết. Vui lòng truy cập link dưới đây để đặt hàng:")
            domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
            order_link = f"{domain}/order-form?ms={ms}&uid={uid}"
            send_message(uid, order_link)
        except:
            pass
    finally:
        ctx["processing_lock"] = False


# ============================================
# HANDLE ORDER FORM STATE
# ============================================

def reset_order_state(uid: str):
    ctx = USER_CONTEXT[uid]
    ctx["order_state"] = None
    ctx["order_data"] = {}


def handle_order_form_step(uid: str, text: str):
    """
    Xử lý luồng hỏi thông tin đặt hàng nếu user đang trong trạng thái order_state.
    """
    ctx = USER_CONTEXT[uid]
    state = ctx.get("order_state")
    if not state:
        return False

    data = ctx.get("order_data", {})

    if state == "ask_name":
        data["customerName"] = text.strip()
        ctx["order_state"] = "ask_phone"
        send_message(uid, "Dạ em cảm ơn anh/chị. Anh/chị cho em xin số điện thoại ạ?")
        return True

    if state == "ask_phone":
        phone = re.sub(r"[^\d+]", "", text)
        if len(phone) < 9:
            send_message(uid, "Số điện thoại chưa đúng lắm, anh/chị nhập lại giúp em (tối thiểu 9 số) ạ?")
            return True
        data["phone"] = phone
        ctx["order_state"] = "ask_address"
        send_message(uid, "Dạ vâng. Anh/chị cho em xin địa chỉ nhận hàng (đầy đủ: số nhà, đường, phường/xã, quận/huyện, tỉnh/thành) ạ?")
        return True

    if state == "ask_address":
        data["address"] = text.strip()
        ctx["order_state"] = None
        ctx["order_data"] = data

        summary = (
            "Dạ em tóm tắt lại đơn hàng của anh/chị:\n"
            f"- Sản phẩm: {data.get('productName', '')}\n"
            f"- Mã: {data.get('ms', '')}\n"
            f"- Phân loại: {data.get('color', '')} / {data.get('size', '')}\n"
            f"- Số lượng: {data.get('quantity', '1')}\n"
            f"- Thành tiền dự kiến: {data.get('total', '')}\n"
            f"- Người nhận: {data.get('customerName', '')}\n"
            f"- SĐT: {data.get('phone', '')}\n"
            f"- Địa chỉ: {data.get('address', '')}\n\n"
            "Anh/chị kiểm tra giúp em xem đã đúng chưa ạ?"
        )
        send_message(uid, summary)
        return True

    return False


# ============================================
# HANDLE IMAGE
# ============================================

def handle_image(uid: str, image_url: str):
    """
    Khi khách gửi ảnh, ta không có OCR nên chỉ trả lời chung chung.
    """
    send_message(
        uid,
        "Dạ em cảm ơn anh/chị đã gửi ảnh.\n"
        "Hiện tại em chưa xem được chi tiết trong hình. "
        "Anh/chị giúp em gửi kèm mã sản phẩm hoặc mô tả sản phẩm cần tư vấn nhé.",
    )


# ============================================
# HANDLE TEXT - ĐÃ SỬA LOẠI BỎ ADVICE_KEYWORDS
# ============================================

def detect_ms_from_text(text: str):
    """
    Tìm mã sản phẩm dạng [MS000123] trong tin nhắn.
    """
    ms_list = re.findall(r"\[MS(\d{6})\]", text.upper())
    if ms_list:
        return "MS" + ms_list[0]
    return None


def handle_text(uid: str, text: str):
    """Xử lý tin nhắn văn bản từ người dùng"""
    if not text or len(text.strip()) == 0:
        return
    
    ctx = USER_CONTEXT[uid]

    if ctx.get("processing_lock"):
        print(f"[TEXT SKIP] User {uid} đang được xử lý")
        return

    ctx["processing_lock"] = True

    try:
        load_products()
        ctx["postback_count"] = 0

        if handle_order_form_step(uid, text):
            ctx["processing_lock"] = False
            return

        lower = text.lower()
        
        # KIỂM TRA TỪ KHÓA CAROUSEL
        if any(kw in lower for kw in CAROUSEL_KEYWORDS):
            if PRODUCTS:
                send_message(uid, "Dạ, em đang lấy danh sách sản phẩm cho anh/chị...")
                
                carousel_elements = []
                for i, (ms, product) in enumerate(list(PRODUCTS.items())[:5]):
                    images_field = product.get("Images", "")
                    urls = parse_image_urls(images_field)
                    image_url = urls[0] if urls else ""
                    
                    short_desc = product.get("ShortDesc", "") or short_description(product.get("MoTa", ""))
                    
                    element = {
                        "title": product.get('Ten', ''),
                        "image_url": image_url,
                        "subtitle": short_desc[:80] + "..." if short_desc and len(short_desc) > 80 else (short_desc if short_desc else ""),
                        "default_action": {
                            "type": "web_url",
                            "url": f"{DOMAIN if DOMAIN.startswith('http') else 'https://' + DOMAIN}/order-form?ms={ms}&uid={uid}",
                            "webview_height_ratio": "tall"
                        },
                        "buttons": [
                            {
                                "type": "web_url",
                                "url": f"{DOMAIN if DOMAIN.startswith('http') else 'https://' + DOMAIN}/order-form?ms={ms}&uid={uid}",
                                "title": "🛒 Đặt ngay"
                            },
                            {
                                "type": "postback",
                                "title": "🔍 Xem chi tiết",
                                "payload": f"ADVICE_{ms}"
                            }
                        ]
                    }
                    carousel_elements.append(element)
                
                if carousel_elements:
                    send_carousel_template(uid, carousel_elements)
                    send_message(uid, "📱 Anh/chị vuốt sang trái/phải để xem thêm sản phẩm nhé!")
                    send_message(uid, "💬 Gõ mã sản phẩm (ví dụ: [MS123456]) hoặc bấm 'Xem chi tiết' để xem thông tin và chính sách cụ thể.")
                else:
                    send_message(uid, "Hiện tại shop chưa có sản phẩm nào để hiển thị ạ.")
                
                ctx["processing_lock"] = False
                return
            else:
                send_message(uid, "Hiện tại shop chưa có sản phẩm nào ạ. Vui lòng quay lại sau!")
                ctx["processing_lock"] = False
                return

        # Tìm mã sản phẩm phù hợp nhất cho câu hỏi
        ms = get_relevant_product_for_question(uid, text)
        
        # Nếu có mã sản phẩm trong text, gửi thông tin chi tiết
        detected_ms = detect_ms_from_text(text)
        if detected_ms and detected_ms in PRODUCTS:
            ms = detected_ms
            update_product_context(uid, ms)
            send_product_info_debounced(uid, ms)
            ctx["processing_lock"] = False
            return

        # KIỂM TRA CÂU HỎI VỀ CHÍNH SÁCH
        policy_keywords = [
            'chính sách', 'ship', 'vận chuyển', 'giao hàng', 
            'đổi trả', 'hoàn tiền', 'bảo hành', 'thanh toán',
            'cod', 'payment', 'phí ship', 'miễn ship', 'free ship'
        ]
        
        is_policy_question = any(keyword in lower for keyword in policy_keywords)
        
        if is_policy_question:
            # Xử lý đặc biệt cho câu hỏi ngắn về ship
            if any(keyword in lower for keyword in ['có miễn ship', 'miễn ship', 'free ship']):
                if ms and ms in PRODUCTS:
                    update_product_context(uid, ms)
                    product = PRODUCTS[ms]
                    description = product.get("MoTa", "")
                    
                    response = generate_policy_response(description, "miễn ship")
                    send_message(uid, response)
                else:
                    send_message(uid, "Dạ, shop có miễn phí ship cho đơn hàng từ 1 sản phẩm trở lên ạ. Anh/chị có thể cho em biết mã sản phẩm để em kiểm tra chính sách cụ thể không ạ?")
                
                ctx["processing_lock"] = False
                return
            
            # Nếu là câu hỏi chung về chính sách shop (không liên quan sản phẩm cụ thể)
            general_policy_questions = [
                'shop có chính sách gì',
                'chính sách của shop',
                'chính sách mua hàng',
                'shop ship thế nào',
                'shop đổi trả ra sao'
            ]
            
            if any(q in lower for q in general_policy_questions):
                general_response = (
                    "Chính sách chung của shop:\n"
                    "• Giao hàng toàn quốc, phí ship từ 20-50k tùy khu vực\n"
                    "• Đổi trả trong 3-7 ngày tùy sản phẩm\n"
                    "• Thanh toán khi nhận hàng (COD) hoặc chuyển khoản\n"
                    "• Bảo hành theo chính sách của từng sản phẩm\n\n"
                    "Để biết chính sách cụ thể cho sản phẩm, anh/chị vui lòng cho em biết mã sản phẩm ạ."
                )
                send_message(uid, general_response)
                ctx["processing_lock"] = False
                return
            
            # Nếu đã có mã sản phẩm trong ngữ cảnh
            if ms and ms in PRODUCTS:
                update_product_context(uid, ms)
                product = PRODUCTS[ms]
                description = product.get("MoTa", "")
                
                response = generate_policy_response(description, text)
                send_message(uid, response)
                
                send_message(uid, "Anh/chị có cần em tư vấn thêm về sản phẩm này không ạ?")
            else:
                send_message(uid, "Anh/chị hỏi về sản phẩm nào nhỉ? Vui lòng cho em biết mã sản phẩm để em kiểm tra chính sách cụ thể ạ.")
                
                if PRODUCTS:
                    send_message(uid, "Hoặc anh/chị có thể gõ 'xem sản phẩm' để xem danh sách sản phẩm và chọn sản phẩm cần tư vấn ạ.")
                
            ctx["processing_lock"] = False
            return
        
        # CHỈ dùng GPT khi có mã sản phẩm và câu hỏi phù hợp
        if ms and ms in PRODUCTS and should_use_gpt(text, ms):
            update_product_context(uid, ms)
            
            reply = build_chatgpt_reply(uid, text, ms)
            send_message(uid, reply)
        elif ms and ms in PRODUCTS:
            # Có mã sản phẩm nhưng không dùng GPT -> gửi thông tin cơ bản
            update_product_context(uid, ms)
            product_name = PRODUCTS[ms].get('Ten', '')
            send_message(uid, f"Dạ, anh/chị đang hỏi về sản phẩm [{ms}] {product_name}. Anh/chị cần em tư vấn gì về sản phẩm này ạ?")
        else:
            # Không có mã sản phẩm -> trả lời chung
            # Kiểm tra nếu là câu hỏi tư vấn nhưng chưa có sản phẩm
            if 'giá' in lower or 'bao nhiêu' in lower:
                send_message(uid, "Dạ, để em biết giá cụ thể, anh/chị vui lòng cho em biết mã sản phẩm hoặc gõ 'xem sản phẩm' để xem danh sách ạ.")
            elif 'size' in lower or 'màu' in lower or 'còn hàng' in lower:
                send_message(uid, "Dạ, em chưa biết anh/chị đang hỏi về sản phẩm nào. Anh/chị vui lòng cung cấp mã sản phẩm (ví dụ: [MS123456]) để em tư vấn chi tiết ạ.")
            else:
                # Câu chào hỏi thông thường
                send_message(uid, f"Em chào anh/chị! Em là trợ lý bán hàng của {FANPAGE_NAME}. Anh/chị có thể:")
                send_message(uid, "1. Gửi mã sản phẩm (ví dụ: [MS123456])")
                send_message(uid, "2. Gõ 'xem sản phẩm' để xem danh sách sản phẩm")
                send_message(uid, "3. Hỏi về chính sách mua hàng")

        # Kiểm tra từ khóa đặt hàng
        if ms and ms in PRODUCTS and any(kw in lower for kw in ORDER_KEYWORDS):
            domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
            order_link = f"{domain}/order-form?ms={ms}&uid={uid}"
            send_message(uid, f"📋 Anh/chị có thể đặt hàng ngay tại đây:\n{order_link}")

    except Exception as e:
        print(f"Error in handle_text for {uid}: {e}")
        try:
            send_message(uid, "Dạ em đang gặp chút trục trặc, anh/chị vui lòng thử lại sau ít phút ạ.")
        except:
            pass
    finally:
        if ctx.get("processing_lock"):
            ctx["processing_lock"] = False


# ============================================
# WEBHOOK HANDLER
# ============================================

@app.route("/", methods=["GET"])
def home():
    return "OK", 200


@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        mode = request.args.get("hub.mode")
        token = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")
        
        print(f"[WEBHOOK VERIFY] Mode: {mode}, Token: {token}, Expected: {VERIFY_TOKEN}")
        
        if mode == "subscribe" and token == VERIFY_TOKEN:
            print("[WEBHOOK VERIFY] Success!")
            return challenge, 200
        else:
            print("[WEBHOOK VERIFY] Failed!")
            return "Verification token mismatch", 403

    data = request.get_json() or {}
    print("Webhook received:", json.dumps(data, ensure_ascii=False))

    entry = data.get("entry", [])
    for e in entry:
        messaging = e.get("messaging", [])
        for m in messaging:
            sender_id = m.get("sender", {}).get("id")
            if not sender_id:
                continue

            if m.get("message", {}).get("is_echo"):
                print(f"[ECHO] Bỏ qua tin nhắn từ bot: {sender_id}")
                continue
            
            if m.get("delivery") or m.get("read"):
                continue
            
            if "postback" in m:
                payload = m["postback"].get("payload")
                if payload:
                    ctx = USER_CONTEXT[sender_id]
                    postback_id = m["postback"].get("mid")
                    now = time.time()
                    
                    if postback_id and postback_id in ctx.get("processed_postbacks", set()):
                        print(f"[POSTBACK DUPLICATE] Bỏ qua postback trùng: {postback_id}")
                        continue
                    
                    last_postback_time = ctx.get("last_postback_time", 0)
                    if now - last_postback_time < 1:
                        print(f"[POSTBACK SPAM] User {sender_id} gửi postback quá nhanh")
                        continue
                    
                    if postback_id:
                        if "processed_postbacks" not in ctx:
                            ctx["processed_postbacks"] = set()
                        ctx["processed_postbacks"].add(postback_id)
                        if len(ctx["processed_postbacks"]) > 10:
                            ctx["processed_postbacks"] = set(list(ctx["processed_postbacks"])[-10:])
                    
                    ctx["last_postback_time"] = now
                    handle_postback(sender_id, payload)
                    continue
            
            if "message" in m:
                msg = m["message"]
                text = msg.get("text")
                attachments = msg.get("attachments") or []
                if text:
                    handle_text(sender_id, text)
                elif attachments:
                    for att in attachments:
                        if att.get("type") == "image":
                            image_url = att.get("payload", {}).get("url")
                            if image_url:
                                handle_image(sender_id, image_url)

    return "OK", 200


# ============================================
# POSTBACK HANDLER
# ============================================

def handle_postback(uid: str, payload: str):
    ctx = USER_CONTEXT[uid]
    ctx["postback_count"] = ctx.get("postback_count", 0) + 1

    if payload == "GET_STARTED":
        send_message(
            uid,
            f"Em chào anh/chị, em là trợ lý bán hàng của {FANPAGE_NAME}. "
            "Anh/chị có thể gửi mã sản phẩm (ví dụ: [MS123456]) hoặc gõ 'xem sản phẩm' để xem danh sách sản phẩm ạ.",
        )
        return
    
    elif payload.startswith("ORDER_"):
        ms = payload.replace("ORDER_", "")
        if ms in PRODUCTS:
            domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
            order_link = f"{domain}/order-form?ms={ms}&uid={uid}"
            product_name = PRODUCTS[ms].get('Ten', '')
            send_message(uid, f"🎯 Anh/chị chọn sản phẩm [{ms}] {product_name}!\n\n📋 Đặt hàng ngay tại đây:\n{order_link}")
        return
    
    elif payload.startswith("ADVICE_"):
        ms = payload.replace("ADVICE_", "")
        if ms in PRODUCTS:
            send_product_info_debounced(uid, ms)
        else:
            send_message(uid, "❌ Em không tìm thấy sản phẩm này trong hệ thống. Anh/chị vui lòng kiểm tra lại mã sản phẩm ạ.")
        return

    send_message(uid, "Dạ em đã nhận được thao tác của anh/chị ạ.")


# ============================================
# ORDER FORM PAGE
# ============================================

@app.route("/order-form", methods=["GET"])
def order_form():
    ms = (request.args.get("ms") or "").upper()
    uid = request.args.get("uid") or ""
    if not ms:
        return (
            """
        <html>
        <body style="text-align: center; padding: 50px; font-family: Arial, sans-serif;">
            <h2 style="color: #FF3B30;">⚠️ Không tìm thấy sản phẩm</h2>
            <p>Vui lòng quay lại Messenger và chọn sản phẩm để đặt hàng.</p>
            <a href="/" style="color: #1DB954; text-decoration: none; font-weight: bold;">Quay về trang chủ</a>
        </body>
        </html>
        """,
            400,
        )

    load_products()
    if ms not in PRODUCTS:
        return (
            """
        <html>
        <body style="text-align: center; padding: 50px; font-family: Arial, sans-serif;">
            <h2 style="color: #FF3B30;">⚠️ Sản phẩm không tồn tại</h2>
            <p>Vui lòng quay lại Messenger và chọn sản phẩm khác giúp shop ạ.</p>
            <a href="/" style="color: #1DB954; text-decoration: none; font-weight: bold;">Quay về trang chủ</a>
        </body>
        </html>
        """,
            404,
        )

    row = PRODUCTS[ms]
    images_field = row.get("Images", "")
    urls = parse_image_urls(images_field)
    image = ""
    for u in urls:
        if should_use_as_first_image(u):
            image = u
            break
    if not image and urls:
        image = urls[0]

    size_field = row.get("size (Thuộc tính)", "")
    color_field = row.get("màu (Thuộc tính)", "")

    sizes = []
    if size_field:
        sizes = [s.strip() for s in size_field.split(",") if s.strip()]

    colors = []
    if color_field:
        colors = [c.strip() for c in color_field.split(",") if c.strip()]

    if not sizes:
        sizes = ["Mặc định"]
    if not colors:
        colors = ["Mặc định"]

    price_str = row.get("Gia", "0")
    price_int = extract_price_int(price_str) or 0

    html = f"""
    <html>
    <head>
        <meta charset="utf-8" />
        <title>Đặt hàng - {row.get('Ten','')}</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
    </head>
    <body style="font-family: Arial, sans-serif; margin: 0; padding: 0; background: #f5f5f5;">
        <div style="max-width: 480px; margin: 0 auto; background: #fff; min-height: 100vh;">
            <div style="padding: 16px; border-bottom: 1px solid #eee; text-align: center;">
                <h2 style="margin: 0; font-size: 18px;">ĐẶT HÀNG - {FANPAGE_NAME}</h2>
            </div>
            <div style="padding: 16px;">
                <div style="display: flex; gap: 12px;">
                    <div style="width: 120px; height: 120px; overflow: hidden; border-radius: 8px; background: #f0f0f0;">
                        {"<img src='" + image + "' style='width: 100%; height: 100%; object-fit: cover;' />" if image else ""}
                    </div>
                    <div style="flex: 1;">
                        <h3 style="margin-top: 0; font-size: 16px;">[{ms}] {row.get('Ten','')}</h3>
                        <div style="color: #FF3B30; font-weight: bold; font-size: 16px;" id="price-display">
                            {price_int:,.0f} đ
                        </div>
                    </div>
                </div>

                <div style="margin-top: 16px;">
                    <label for="color" style="display: block; margin-bottom: 4px; font-size: 14px;">Màu sắc:</label>
                    <select id="color" style="width: 100%; padding: 8px; border-radius: 4px; border: 1px solid #ccc;">
                        {''.join(f"<option value='{c}'>{c}</option>" for c in colors)}
                    </select>
                </div>

                <div style="margin-top: 12px;">
                    <label for="size" style="display: block; margin-bottom: 4px; font-size: 14px;">Size:</label>
                    <select id="size" style="width: 100%; padding: 8px; border-radius: 4px; border: 1px solid #ccc;">
                        {''.join(f"<option value='{s}'>{s}</option>" for s in sizes)}
                    </select>
                </div>

                <div style="margin-top: 12px;">
                    <label for="quantity" style="display: block; margin-bottom: 4px; font-size: 14px;">Số lượng:</label>
                    <input type="number" id="quantity" value="1" min="1" style="width: 100%; padding: 8px; border-radius: 4px; border: 1px solid #ccc;" />
                </div>

                <div style="margin-top: 16px; padding: 12px; background: #f9f9f9; border-radius: 8px;">
                    <div style="font-size: 14px; margin-bottom: 4px;">Tạm tính:</div>
                    <div id="total-display" style="font-size: 18px; color: #FF3B30; font-weight: bold;">
                        {price_int:,.0f} đ
                    </div>
                </div>

                <div style="margin-top: 16px;">
                    <label for="customerName" style="display: block; margin-bottom: 4px; font-size: 14px;">Họ và tên:</label>
                    <input type="text" id="customerName" style="width: 100%; padding: 8px; border-radius: 4px; border: 1px solid #ccc;" />
                </div>

                <div style="margin-top: 12px;">
                    <label for="phone" style="display: block; margin-bottom: 4px; font-size: 14px;">Số điện thoại:</label>
                    <input type="tel" id="phone" style="width: 100%; padding: 8px; border-radius: 4px; border: 1px solid #ccc;" />
                </div>

                <div style="margin-top: 12px;">
                    <label for="address" style="display: block; margin-bottom: 4px; font-size: 14px;">Địa chỉ nhận hàng:</label>
                    <textarea id="address" rows="3" style="width: 100%; padding: 8px; border-radius: 4px; border: 1px solid #ccc;"></textarea>
                </div>

                <button onclick="submitOrder()" style="margin-top: 20px; width: 100%; padding: 12px; border-radius: 999px; border: none; background: #1DB954; color: #fff; font-size: 16px; font-weight: bold;">
                    ĐẶT HÀNG NGAY
                </button>

                <p style="margin-top: 12px; font-size: 12px; color: #666; text-align: center;">
                    Shop sẽ gọi xác nhận trong 5-10 phút. Thanh toán khi nhận hàng (COD).
                </p>
            </div>
        </div>

        <script>
            const basePrice = {price_int};

            function formatPrice(n) {{
                return n.toLocaleString('vi-VN') + ' đ';
            }}

            async function updatePriceByVariant() {{
                const color = document.getElementById('color').value;
                const size = document.getElementById('size').value;
                const quantity = parseInt(document.getElementById('quantity').value || '1');

                try {{
                    const res = await fetch(`/api/get-variant-price?ms={ms}&color=${{encodeURIComponent(color)}}&size=${{encodeURIComponent(size)}}`);
                    if (!res.ok) throw new Error('request failed');
                    const data = await res.json();
                    const price = data.price || basePrice;

                    document.getElementById('price-display').innerText = formatPrice(price);
                    document.getElementById('total-display').innerText = formatPrice(price * quantity);
                }} catch (e) {{
                    document.getElementById('price-display').innerText = formatPrice(basePrice);
                    document.getElementById('total-display').innerText = formatPrice(basePrice * quantity);
                }}
            }}

            document.getElementById('color').addEventListener('change', updatePriceByVariant);
            document.getElementById('size').addEventListener('change', updatePriceByVariant);
            document.getElementById('quantity').addEventListener('input', updatePriceByVariant);

            async function submitOrder() {{
                const color = document.getElementById('color').value;
                const size = document.getElementById('size').value;
                const quantity = parseInt(document.getElementById('quantity').value || '1');
                const customerName = document.getElementById('customerName').value;
                const phone = document.getElementById('phone').value;
                const address = document.getElementById('address').value;

                const res = await fetch('/api/submit-order', {{
                    method: 'POST',
                    headers: {{
                        'Content-Type': 'application/json'
                    }},
                    body: JSON.stringify({{
                        ms: "{ms}",
                        uid: "{uid}",
                        color,
                        size,
                        quantity,
                        customerName,
                        phone,
                        address
                    }})
                }});

                const data = await res.json();
                alert(data.message || 'Đã gửi đơn hàng thành công, shop sẽ liên hệ lại anh/chị sớm nhất!');
            }}
        </script>
    </body>
    </html>
    """
    return html


# ============================================
# API ENDPOINTS
# ============================================

@app.route("/api/get-product")
def api_get_product():
    load_products()
    ms = (request.args.get("ms") or "").upper()
    if ms not in PRODUCTS:
        return {"error": "not_found"}, 404

    row = PRODUCTS[ms]
    images_field = row.get("Images", "")
    urls = parse_image_urls(images_field)
    image = urls[0] if urls else ""

    size_field = row.get("size (Thuộc tính)", "")
    color_field = row.get("màu (Thuộc tính)", "")

    sizes = []
    if size_field:
        sizes = [s.strip() for s in size_field.split(",") if s.strip()]

    colors = []
    if color_field:
        colors = [c.strip() for c in color_field.split(",") if c.strip()]

    if not sizes:
        sizes = ["Mặc định"]
    if not colors:
        colors = ["Mặc định"]

    price_str = row.get("Gia", "0")
    price_int = extract_price_int(price_str) or 0

    return {
        "ms": ms,
        "name": row.get("Ten", ""),
        "image": image,
        "sizes": sizes,
        "colors": colors,
        "price": price_int,
        "price_display": f"{price_int:,.0f} đ",
    }


@app.route("/api/get-variant-price")
def api_get_variant_price():
    ms = (request.args.get("ms") or "").upper()
    color = (request.args.get("color") or "").strip()
    size = (request.args.get("size") or "").strip()

    load_products()
    if ms not in PRODUCTS:
        return {"error": "not_found"}, 404

    product = PRODUCTS[ms]
    variants = product.get("variants") or []

    chosen = None
    for v in variants:
        vm = (v.get("mau") or "").strip().lower()
        vs = (v.get("size") or "").strip().lower()
        want_color = color.strip().lower()
        want_size = size.strip().lower()

        if want_color and vm != want_color:
            continue
        if want_size and vs != want_size:
            continue
        chosen = v
        break

    if not chosen and variants:
        chosen = variants[0]

    price = 0
    price_display = product.get("Gia", "0")

    if chosen:
        if chosen.get("gia") is not None:
            price = chosen["gia"]
            price_display = chosen.get("gia_raw") or price_display
        else:
            p_int = extract_price_int(chosen.get("gia_raw"))
            if p_int is not None:
                price = p_int
                price_display = chosen.get("gia_raw") or price_display
            else:
                p_int = extract_price_int(product.get("Gia", "0"))
                price = p_int or 0
    else:
        p_int = extract_price_int(product.get("Gia", "0"))
        price = p_int or 0

    return {
        "ms": ms,
        "color": color,
        "size": size,
        "price": int(price),
        "price_display": price_display,
    }


@app.route("/api/submit-order", methods=["POST"])
def api_submit_order():
    data = request.get_json() or {}
    ms = (data.get("ms") or "").upper()
    uid = data.get("uid") or ""
    color = data.get("color") or ""
    size = data.get("size") or ""
    quantity = int(data.get("quantity") or 1)
    customerName = data.get("customerName") or ""
    phone = data.get("phone") or ""
    address = data.get("address") or ""

    load_products()
    row = PRODUCTS.get(ms)
    if not row:
        return {"error": "not_found", "message": "Sản phẩm không tồn tại"}, 404

    price_str = row.get("Gia", "0")
    price_int = extract_price_int(price_str) or 0
    total = price_int * quantity

    if uid:
        msg = (
            "🎉 Shop đã nhận được đơn hàng mới:\n"
            f"🛍 Sản phẩm: [{ms}] {row.get('Ten','')}\n"
            f"🎨 Phân loại: {color} / {size}\n"
            f"📦 Số lượng: {quantity}\n"
            f"💰 Thành tiền: {total:,.0f} đ\n"
            f"👤 Người nhận: {customerName}\n"
            f"📱 SĐT: {phone}\n"
            f"🏠 Địa chỉ: {address}\n"
            "────────────────────\n"
            "⏰ Shop sẽ gọi điện xác nhận trong 5-10 phút.\n"
            "💳 Thanh toán khi nhận hàng (COD)\n"
            "────────────────────\n"
            "Cảm ơn anh/chị đã đặt hàng! ❤️"
        )
        send_message(uid, msg)

    return {"status": "ok", "message": "Đơn hàng đã được tiếp nhận"}


@app.route("/static/<path:path>")
def static_files(path):
    return send_from_directory("static", path)


@app.route("/health", methods=["GET"])
def health_check():
    """Kiểm tra tình trạng server và bot"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "products_loaded": len(PRODUCTS),
        "last_load_time": LAST_LOAD,
        "openai_configured": bool(client),
        "facebook_configured": bool(PAGE_ACCESS_TOKEN)
    }, 200


# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    print("Starting app on http://0.0.0.0:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)
