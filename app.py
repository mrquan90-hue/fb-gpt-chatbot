import os
import json
import re
import time
import csv
import hashlib
import base64
from collections import defaultdict
from urllib.parse import quote
from datetime import datetime
from typing import Optional, Dict, Any
from io import BytesIO

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
# MAP TIẾNG VIỆT CÓ DẤU SANG KHÔNG DẤU
# ============================================
VIETNAMESE_MAP = {
    'à': 'a', 'á': 'a', 'ả': 'a', 'ã': 'a', 'ạ': 'a',
    'ă': 'a', 'ằ': 'a', 'ắ': 'a', 'ẳ': 'a', 'ẵ': 'a', 'ặ': 'a',
    'â': 'a', 'ầ': 'a', 'ấ': 'a', 'ẩ': 'a', 'ẫ': 'a', 'ậ': 'a',
    'đ': 'd',
    'è': 'e', 'é': 'e', 'ẻ': 'e', 'ẽ': 'e', 'ẹ': 'e',
    'ê': 'e', 'ề': 'e', 'ế': 'e', 'ể': 'e', 'ễ': 'e', 'ệ': 'e',
    'ì': 'i', 'í': 'i', 'ỉ': 'i', 'ĩ': 'i', 'ị': 'i',
    'ò': 'o', 'ó': 'o', 'ỏ': 'o', 'õ': 'o', 'ọ': 'o',
    'ô': 'o', 'ồ': 'o', 'ố': 'o', 'ổ': 'o', 'ỗ': 'o', 'ộ': 'o',
    'ơ': 'o', 'ờ': 'o', 'ớ': 'o', 'ở': 'o', 'ỡ': 'o', 'ợ': 'o',
    'ù': 'u', 'ú': 'u', 'ủ': 'u', 'ũ': 'u', 'ụ': 'u',
    'ư': 'u', 'ừ': 'u', 'ứ': 'u', 'ử': 'u', 'ữ': 'u', 'ự': 'u',
    'ỳ': 'y', 'ý': 'y', 'ỷ': 'y', 'ỹ': 'y', 'ỵ': 'y',
    'À': 'A', 'Á': 'A', 'Ả': 'A', 'Ã': 'A', 'Ạ': 'A',
    'Ă': 'A', 'Ằ': 'A', 'Ắ': 'A', 'Ẳ': 'A', 'Ẵ': 'A', 'Ặ': 'A',
    'Â': 'A', 'Ầ': 'A', 'Ấ': 'A', 'Ẩ': 'A', 'Ẫ': 'A', 'Ậ': 'A',
    'Đ': 'D',
    'È': 'E', 'É': 'E', 'Ẻ': 'E', 'Ẽ': 'E', 'Ẹ': 'E',
    'Ê': 'E', 'Ề': 'E', 'Ế': 'E', 'Ể': 'E', 'Ễ': 'E', 'Ệ': 'E',
    'Ì': 'I', 'Í': 'I', 'Ỉ': 'I', 'Ĩ': 'I', 'Ị': 'I',
    'Ò': 'O', 'Ó': 'O', 'Ỏ': 'O', 'Õ': 'O', 'Ọ': 'O',
    'Ô': 'O', 'Ồ': 'O', 'Ố': 'O', 'Ổ': 'O', 'Ỗ': 'O', 'Ộ': 'O',
    'Ơ': 'O', 'Ờ': 'O', 'Ớ': 'O', 'Ở': 'O', 'Ỡ': 'O', 'Ợ': 'O',
    'Ù': 'U', 'Ú': 'U', 'Ủ': 'U', 'Ũ': 'U', 'Ụ': 'U',
    'Ư': 'U', 'Ừ': 'U', 'Ứ': 'U', 'Ử': 'U', 'Ữ': 'U', 'Ự': 'U',
    'Ỳ': 'Y', 'Ý': 'Y', 'Ỷ': 'Y', 'Ỹ': 'Y', 'Ỵ': 'Y'
}

def normalize_vietnamese(text):
    """Chuẩn hóa tiếng Việt về không dấu"""
    result = text
    for char, replacement in VIETNAMESE_MAP.items():
        result = result.replace(char, replacement)
    return result

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
    "conversation_history": [],
    "referral_source": None,
    "referral_payload": None,
    # Thêm trường cho nhận diện ảnh
    "last_image_analysis": None,
    "last_image_url": None,
    "last_image_base64": None,
})
PRODUCTS = {}
PRODUCTS_BY_NUMBER = {}  # Mapping từ số (không có số 0 đầu) đến mã đầy đủ
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
# HELPER: TẢI VÀ XỬ LÝ ẢNH
# ============================================

def download_image_from_facebook(image_url: str, timeout: int = 10) -> Optional[bytes]:
    """
    Tải ảnh từ Facebook URL với headers phù hợp
    Trả về bytes của ảnh hoặc None nếu thất bại
    """
    try:
        # Facebook cần user-agent để tránh bị chặn
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://www.facebook.com/',
        }
        
        print(f"📥 Đang tải ảnh từ Facebook: {image_url[:100]}...")
        
        response = requests.get(
            image_url, 
            headers=headers, 
            timeout=timeout,
            stream=True
        )
        
        if response.status_code == 200:
            # Kiểm tra content-type
            content_type = response.headers.get('content-type', '')
            if not content_type.startswith('image/'):
                print(f"⚠️ URL không phải ảnh: {content_type}")
                return None
            
            # Đọc ảnh với giới hạn kích thước (max 10MB)
            max_size = 10 * 1024 * 1024  # 10MB
            content = b""
            for chunk in response.iter_content(chunk_size=8192):
                content += chunk
                if len(content) > max_size:
                    print("⚠️ Ảnh quá lớn (>10MB), bỏ qua")
                    return None
            
            print(f"✅ Đã tải ảnh thành công: {len(content)} bytes")
            return content
            
        else:
            print(f"❌ Lỗi tải ảnh: HTTP {response.status_code}")
            return None
            
    except requests.exceptions.Timeout:
        print(f"⏰ Timeout khi tải ảnh")
        return None
    except requests.exceptions.RequestException as e:
        print(f"❌ Lỗi kết nối khi tải ảnh: {str(e)}")
        return None
    except Exception as e:
        print(f"❌ Lỗi không xác định khi tải ảnh: {str(e)}")
        return None

def convert_image_to_base64(image_bytes: bytes) -> Optional[str]:
    """
    Chuyển đổi ảnh bytes sang base64 string
    """
    try:
        # Mã hóa base64
        base64_str = base64.b64encode(image_bytes).decode('utf-8')
        
        # Xác định MIME type từ bytes (đơn giản)
        # Thực tế nên dùng thư viện như python-magic, nhưng tạm thời dùng cách đơn giản
        if image_bytes[:4] == b'\x89PNG':
            mime_type = 'image/png'
        elif image_bytes[:3] == b'\xff\xd8\xff':
            mime_type = 'image/jpeg'
        elif image_bytes[:6] in (b'GIF87a', b'GIF89a'):
            mime_type = 'image/gif'
        elif image_bytes[:4] == b'RIFF' and image_bytes[8:12] == b'WEBP':
            mime_type = 'image/webp'
        else:
            mime_type = 'image/jpeg'  # Mặc định
        
        # Tạo data URL
        data_url = f"data:{mime_type};base64,{base64_str}"
        return data_url
        
    except Exception as e:
        print(f"❌ Lỗi chuyển đổi base64: {str(e)}")
        return None

def get_image_for_analysis(image_url: str) -> Optional[str]:
    """
    Lấy ảnh dưới dạng base64 data URL cho OpenAI
    Thử cả 2 cách: tải về và dùng trực tiếp URL
    """
    # Ưu tiên: Tải ảnh về và chuyển base64
    image_bytes = download_image_from_facebook(image_url)
    
    if image_bytes:
        base64_data = convert_image_to_base64(image_bytes)
        if base64_data:
            print("✅ Sử dụng ảnh base64")
            return base64_data
    
    # Fallback: Dùng URL trực tiếp (nếu OpenAI có thể truy cập)
    print("⚠️ Fallback: Sử dụng URL trực tiếp")
    return image_url

# ============================================
# GPT-4o VISION: PHÂN TÍCH ẢNH SẢN PHẨM
# ============================================

def analyze_image_with_gpt4o(image_url: str):
    """
    Phân tích ảnh sản phẩm thời trang/gia dụng bằng GPT-4o Vision API
    Sử dụng base64 để tránh lỗi tải ảnh từ Facebook
    """
    if not client or not OPENAI_API_KEY:
        print("⚠️ OpenAI client chưa được cấu hình, bỏ qua phân tích ảnh")
        return None
    
    try:
        print(f"🖼️ Đang phân tích ảnh: {image_url[:100]}...")
        
        # Lấy ảnh dưới dạng base64 hoặc URL
        image_content = get_image_for_analysis(image_url)
        
        if not image_content:
            print("❌ Không thể lấy được ảnh để phân tích")
            return None
        
        # Chuẩn bị content cho OpenAI
        if image_content.startswith('data:'):
            # Base64 data URL
            image_message = {
                "type": "image_url",
                "image_url": {
                    "url": image_content
                }
            }
        else:
            # Regular URL (fallback)
            image_message = {
                "type": "image_url",
                "image_url": {
                    "url": image_content
                }
            }
        
        # Gọi OpenAI API với ảnh
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": f"""Bạn là chuyên gia tư vấn thời trang và gia dụng cho {FANPAGE_NAME}.
                    
Hãy phân tích ảnh sản phẩm và trả về JSON với cấu trúc:
{{
    "product_type": "loại sản phẩm (ví dụ: áo thun, quần jeans, váy, đồ gia dụng nhà bếp, v.v.)",
    "main_color": "màu sắc chính (tiếng Việt)",
    "secondary_colors": ["màu phụ 1", "màu phụ 2"],
    "style": "phong cách/kiểu dáng (ví dụ: casual, formal, vintage, hiện đại)",
    "material_guess": "dự đoán chất liệu (nếu nhận diện được)",
    "description": "mô tả chi tiết sản phẩm bằng tiếng Việt (2-3 câu)",
    "keywords": ["từ khóa 1", "từ khóa 2", "từ khóa 3", "từ khóa 4", "từ khóa 5"],
    "confidence_score": 0.95
}}

QUY TẮC QUAN TRỌNG:
1. CHỈ phân tích những gì thấy trong ảnh, không suy đoán thêm
2. product_type phải cụ thể (ví dụ: "áo sơ mi tay ngắn" thay vì chỉ "áo")
3. keywords phải là từ thông dụng để tìm kiếm sản phẩm
4. Trả về CHỈ JSON, không có text nào khác
5. Dùng tiếng Việt cho tất cả các trường"""
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "Phân tích sản phẩm trong ảnh này:"},
                        image_message
                    ]
                }
            ],
            max_tokens=500,
            temperature=0.2,
            response_format={"type": "json_object"}
        )
        
        result_text = response.choices[0].message.content.strip()
        print(f"📊 Kết quả phân tích ảnh: {result_text[:200]}...")
        
        # Parse JSON result
        analysis = json.loads(result_text)
        
        # Thêm timestamp và image_url vào kết quả
        analysis["timestamp"] = time.time()
        analysis["image_url"] = image_url
        
        return analysis
        
    except Exception as e:
        print(f"❌ Lỗi phân tích ảnh với GPT-4o: {str(e)}")
        return None

def find_products_by_image_analysis(uid: str, analysis: dict, limit: int = 3):
    """
    Tìm sản phẩm phù hợp dựa trên phân tích ảnh
    Trả về danh sách mã sản phẩm (MS) phù hợp nhất
    """
    if not analysis or not PRODUCTS:
        return []
    
    # Lấy thông tin từ phân tích
    product_type = analysis.get("product_type", "").lower()
    main_color = analysis.get("main_color", "").lower()
    keywords = [kw.lower() for kw in analysis.get("keywords", [])]
    
    # Chuẩn bị danh sách sản phẩm với điểm số
    scored_products = []
    
    for ms, product in PRODUCTS.items():
        score = 0
        
        # Chuỗi tìm kiếm: tên + mô tả sản phẩm
        search_text = f"{product.get('Ten', '')} {product.get('MoTa', '')}".lower()
        
        # Kiểm tra loại sản phẩm
        if product_type and product_type in search_text:
            score += 5  # Trọng số cao cho loại sản phẩm
        
        # Kiểm tra màu sắc
        if main_color and main_color in search_text:
            score += 3
        
        # Kiểm tra từ khóa
        for keyword in keywords:
            if keyword in search_text:
                score += 2
        
        # Kiểm tra trong thuộc tính màu/size
        color_attr = product.get("màu (Thuộc tính)", "").lower()
        if main_color and main_color in color_attr:
            score += 4
        
        # Ưu tiên sản phẩm có trong lịch sử của user
        ctx = USER_CONTEXT[uid]
        if ms in ctx.get("product_history", []):
            score += 1
        
        # Chỉ thêm sản phẩm có điểm > 0
        if score > 0:
            scored_products.append({
                "ms": ms,
                "score": score,
                "product": product
            })
    
    # Sắp xếp theo điểm số giảm dần
    scored_products.sort(key=lambda x: x["score"], reverse=True)
    
    # Lấy top sản phẩm
    top_products = [item["ms"] for item in scored_products[:limit]]
    
    print(f"🔍 Tìm thấy {len(scored_products)} sản phẩm phù hợp, top {len(top_products)}: {top_products}")
    
    return top_products

def send_product_suggestions(uid: str, product_ms_list: list, analysis: dict = None):
    """Gửi đề xuất sản phẩm dựa trên phân tích ảnh"""
    if not product_ms_list:
        return
    
    # Gửi thông báo tìm thấy sản phẩm
    if analysis:
        product_type = analysis.get("product_type", "sản phẩm")
        main_color = analysis.get("main_color", "")
        
        if main_color:
            send_message(uid, f"🎯 Em phân tích được đây là {product_type} màu {main_color}")
        else:
            send_message(uid, f"🎯 Em phân tích được đây là {product_type}")
    
    send_message(uid, "🔍 Em tìm thấy một số sản phẩm phù hợp:")
    
    # Gửi thông tin từng sản phẩm
    for i, ms in enumerate(product_ms_list[:3], 1):
        if ms in PRODUCTS:
            product = PRODUCTS[ms]
            product_name = product.get('Ten', 'Sản phẩm')
            send_message(uid, f"{i}. 📌 {product_name}")
            
            # Gửi ảnh đầu tiên nếu có
            images_field = product.get("Images", "")
            urls = parse_image_urls(images_field)
            if urls:
                send_image(uid, urls[0])
                time.sleep(0.5)
            
            # Gửi giá
            gia_raw = product.get("Gia", "")
            gia_int = extract_price_int(gia_raw)
            if gia_int:
                send_message(uid, f"💰 Giá: {gia_int:,.0f}đ")
            
            # Gửi nút hành động
            domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
            order_link = f"{domain}/order-form?ms={ms}&uid={uid}"
            send_message(uid, f"🛒 Xem chi tiết & đặt hàng: {order_link}")
            
            time.sleep(0.5)
    
    # Gửi thêm hướng dẫn
    if len(product_ms_list) > 3:
        send_message(uid, f"📱 Còn {len(product_ms_list)-3} sản phẩm phù hợp khác. Anh/chị muốn xem tiếp không ạ?")

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
    global PRODUCTS, LAST_LOAD, PRODUCTS_BY_NUMBER
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
        products_by_number = {}

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
            tonkho_raw = (row.get("Tồn kho") or row.get("Có thể bán") or "").strip()
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
                    "FullRow": row,
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
            
            # Xây dựng mapping từ số (không có số 0 đầu) đến mã đầy đủ
            if ms.startswith("MS"):
                num_part = ms[2:]  # Bỏ "MS"
                # Loại bỏ số 0 ở đầu
                num_without_leading_zeros = num_part.lstrip('0')
                if num_without_leading_zeros:
                    products_by_number[num_without_leading_zeros] = ms

        PRODUCTS = products
        PRODUCTS_BY_NUMBER = products_by_number
        LAST_LOAD = now
        print(f"📦 Loaded {len(PRODUCTS)} products (PHƯƠNG ÁN A).")
        print(f"🔢 Created mapping for {len(PRODUCTS_BY_NUMBER)} product numbers")
    except Exception as e:
        print("❌ load_products ERROR:", e)


# ============================================
# GPT INTEGRATION - XỬ LÝ MỌI CÂU HỎI
# ============================================

def build_comprehensive_product_context(ms: str) -> str:
    """Xây dựng context đầy đủ về sản phẩm cho GPT"""
    if not ms or ms not in PRODUCTS:
        return "KHÔNG CÓ THÔNG TIN SẢN PHẨM"
    
    product = PRODUCTS[ms]
    mota = product.get("MoTa", "")
    
    # Trích xuất thông tin chính sách từ mô tả
    shipping_info = ""
    warranty_info = ""
    return_info = ""
    payment_info = ""
    
    lines = mota.split('\n')
    for line in lines:
        line_lower = line.lower()
        if any(keyword in line_lower for keyword in ['ship', 'vận chuyển', 'giao hàng', 'phí ship', 'miễn ship']):
            shipping_info += line + " "
        elif any(keyword in line_lower for keyword in ['bảo hành', 'warranty', 'đảm bảo']):
            warranty_info += line + " "
        elif any(keyword in line_lower for keyword in ['đổi trả', 'hoàn tiền', 'trả hàng']):
            return_info += line + " "
        elif any(keyword in line_lower for keyword in ['thanh toán', 'payment', 'cod', 'chuyển khoản']):
            payment_info += line + " "
    
    # Thu thập biến thể
    variants_text = ""
    variants = product.get("variants", [])
    if variants:
        variants_text = "Các biến thể có sẵn:\n"
        for i, v in enumerate(variants[:5], 1):
            mau = v.get("mau", "Mặc định")
            size = v.get("size", "Mặc định")
            gia = v.get("gia")
            tonkho = v.get("tonkho", "Còn hàng")
            if gia:
                variants_text += f"{i}. {mau} - {size}: {gia:,.0f}đ (Tồn: {tonkho})\n"
    
    context = f"""
=== THÔNG TIN SẢN PHẨM [{ms}] ===

1. TÊN SẢN PHẨM: {product.get('Ten', '')}

2. GIÁ BÁN: {product.get('Gia', '')}

3. TỒN KHO: {product.get('Tồn kho', 'Chưa có thông tin')}

4. THUỘC TÍNH:
   - Màu sắc: {product.get('màu (Thuộc tính)', 'Chưa có thông tin')}
   - Size: {product.get('size (Thuộc tính)', 'Chưa có thông tin')}

{variants_text}

5. MÔ TẢ CHI TIẾT:
{product.get('MoTa', 'Chưa có mô tả chi tiết')}

6. THÔNG TIN CHÍNH SÁCH:
   - Vận chuyển: {shipping_info if shipping_info else 'Chưa có thông tin cụ thể. Chính sách chung: Giao hàng toàn quốc, phí ship 20-50k. Miễn phí ship cho đơn từ 500k.'}
   - Bảo hành: {warranty_info if warranty_info else 'Chưa có thông tin cụ thể. Chính sách chung: Bảo hành theo chính sách của nhà sản xuất.'}
   - Đổi trả: {return_info if return_info else 'Chưa có thông tin cụ thể. Chính sách chung: Đổi/trả trong 3-7 ngày nếu sản phẩm lỗi, còn nguyên tem mác.'}
   - Thanh toán: {payment_info if payment_info else 'Chưa có thông tin cụ thể. Chính sách chung: Thanh toán khi nhận hàng (COD) hoặc chuyển khoản ngân hàng.'}
"""
    
    return context


def detect_ms_from_text(text: str):
    """Tìm mã sản phẩm trong tin nhắn, hỗ trợ nhiều định dạng"""
    # Ưu tiên tìm theo pattern cũ: [MS\d{6}] hoặc MS\d{6}
    ms_list = re.findall(r"\[MS(\d{6})\]", text.upper())
    if ms_list:
        ms = "MS" + ms_list[0]
        if ms in PRODUCTS:
            return ms
    
    ms_list = re.findall(r"MS(\d{6})", text.upper())
    if ms_list:
        ms = "MS" + ms_list[0]
        if ms in PRODUCTS:
            return ms
    
    # Chuẩn hóa text: chuyển về chữ thường, bỏ dấu tiếng Việt
    text_normalized = normalize_vietnamese(text.lower())
    
    # Tìm số trong chuỗi (hỗ trợ nhiều định dạng số)
    numbers = re.findall(r'\d{1,6}', text_normalized)
    
    if numbers:
        # Lấy số đầu tiên tìm được
        num = numbers[0]
        
        # Loại bỏ số 0 ở đầu
        num_stripped = num.lstrip('0')
        if not num_stripped:  # Nếu tất cả đều là 0
            num_stripped = "0"
        
        # Tìm trong PRODUCTS_BY_NUMBER
        if num_stripped in PRODUCTS_BY_NUMBER:
            return PRODUCTS_BY_NUMBER[num_stripped]
        
        # Nếu không tìm thấy, thử các định dạng khác
        # Tạo các candidate có thể
        candidates = []
        
        # MS + số (không có số 0 đầu)
        candidates.append("MS" + num_stripped)
        
        # MS + số với độ dài 2-6 ký tự (thêm số 0 đầu)
        for length in range(2, 7):
            padded = num_stripped.zfill(length)
            candidates.append("MS" + padded)
        
        # Thử từng candidate
        for candidate in candidates:
            if candidate in PRODUCTS:
                return candidate
    
    # Nếu không tìm thấy số trực tiếp, tìm pattern kết hợp từ khóa và số
    patterns = [
        r'(?:ms|ma|maso|ma so|san pham|tu van|xem)\s*(\d{1,6})',
        r'(\d{1,6})\s*(?:ms|ma|maso|ma so|san pham)?'
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, text_normalized)
        if matches:
            num = matches[0]
            num_stripped = num.lstrip('0')
            if not num_stripped:
                num_stripped = "0"
            
            if num_stripped in PRODUCTS_BY_NUMBER:
                return PRODUCTS_BY_NUMBER[num_stripped]
            
            # Thử các định dạng khác
            candidates = ["MS" + num_stripped]
            for length in range(2, 7):
                padded = num_stripped.zfill(length)
                candidates.append("MS" + padded)
            
            for candidate in candidates:
                if candidate in PRODUCTS:
                    return candidate
    
    return None


def generate_gpt_response(uid: str, user_message: str, ms: str = None):
    """Gọi GPT để trả lời câu hỏi của khách"""
    if not client or not OPENAI_API_KEY:
        return "Hiện tại hệ thống trợ lý AI đang bảo trì, vui lòng thử lại sau ạ."
    
    try:
        # Xây dựng system prompt
        if ms and ms in PRODUCTS:
            product_context = build_comprehensive_product_context(ms)
            system_prompt = f"""Bạn là CHUYÊN GIA TƯ VẤN BÁN HÀNG của {FANPAGE_NAME}.
Bạn đang tư vấn cho sản phẩm có mã: {ms}

THÔNG TIN SẢN PHẨM (BẮT BUỘC CHỈ SỬ DỤNG THÔNG TIN NÀY):
{product_context}

QUY TẮC TRẢ LỜI (TUYỆT ĐỐI TUÂN THỦ):
1. CHỈ sử dụng thông tin có trong "THÔNG TIN SẢN PHẨM" ở trên
2. KHÔNG ĐƯỢC bịa thêm bất kỳ thông tin nào không có trong dữ liệu
3. Nếu không có thông tin, hãy trả lời: "Dạ, phần này trong hệ thống chưa có thông tin ạ, em sợ nói sai nên không dám khẳng định."
4. Nếu khách hỏi về sản phẩm khác, hãy đề nghị khách cung cấp mã sản phẩm mới
5. Giọng điệu: Thân thiện, chuyên nghiệp, xưng "em", gọi khách là "anh/chị"
6. Luôn hướng đến chốt đơn: Cuối mỗi câu trả lời, nhẹ nhàng đề nghị đặt hàng
7. LINK ĐẶT HÀNG: {DOMAIN}/order-form?ms={ms}&uid={uid}

Hãy trả lời bằng tiếng Việt, tự nhiên như đang chat Messenger."""
        else:
            system_prompt = f"""Bạn là CHUYÊN GIA TƯ VẤN BÁN HÀNG của {FANPAGE_NAME}.

HIỆN TẠI BẠN CHƯA BIẾT KHÁCH QUAN TÂM SẢN PHẨM NÀO.

NHIỆM VỤ CỦA BẠN:
1. Hỏi khách về sản phẩm họ quan tâm
2. Đề nghị khách cung cấp mã sản phẩm (ví dụ: [MS123456])
3. Hoặc đề nghị khách gõ "xem sản phẩm" để xem danh sách

QUY TẮC:
1. KHÔNG tự ý giới thiệu chi tiết sản phẩm khi chưa biết mã
2. Luôn hướng khách đến việc cung cấp mã sản phẩm
3. Giọng điệu: Thân thiện, chuyên nghiệp, xưng "em", gọi khách là "anh/chị"

Hãy bắt đầu bằng câu chào và hỏi khách về sản phẩm họ quan tâm."""
        
        # Lấy conversation history
        ctx = USER_CONTEXT[uid]
        conversation = ctx.get("conversation_history", [])
        
        # Giới hạn history
        if len(conversation) > 10:
            conversation = conversation[-10:]
        
        # Tạo messages
        messages = [{"role": "system", "content": system_prompt}]
        
        # Thêm conversation history
        for msg in conversation:
            messages.append(msg)
        
        # Thêm message hiện tại
        messages.append({"role": "user", "content": user_message})
        
        # Gọi OpenAI API
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.7,
            max_tokens=500,
            timeout=15.0,
        )
        
        reply = response.choices[0].message.content.strip()
        
        # Lưu vào conversation history
        conversation.append({"role": "user", "content": user_message})
        conversation.append({"role": "assistant", "content": reply})
        ctx["conversation_history"] = conversation
        
        return reply
        
    except Exception as e:
        print(f"GPT Error: {e}")
        return "Dạ em đang gặp chút trục trặc kỹ thuật. Anh/chị vui lòng thử lại sau ít phút ạ."


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


def get_relevant_product_for_question(uid: str, text: str) -> str | None:
    """Tìm sản phẩm phù hợp nhất cho câu hỏi dựa trên ngữ cảnh"""
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
# SEND PRODUCT INFO (GIỮ NGUYÊN)
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
            short_desc = short_description(mo_ta, 300)
            if short_desc:
                send_message(uid, f"📝 MÔ TẢ:\n{short_desc}")
            else:
                send_message(uid, "📝 Sản phẩm hiện chưa có thông tin chi tiết ạ.")
        else:
            send_message(uid, "📝 Sản phẩm hiện chưa có thông tin chi tiết ạ.")
        
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
# HANDLE IMAGE - VERSION ĐÃ SỬA
# ============================================

def handle_image(uid: str, image_url: str):
    """Xử lý ảnh sản phẩm thông minh với GPT-4o Vision (đã fix lỗi Facebook URL)"""
    if not client or not OPENAI_API_KEY:
        # Fallback về xử lý cũ nếu không có API key
        ctx = USER_CONTEXT[uid]
        ctx["referral_source"] = "image_upload"
        
        response = """📷 Em đã nhận được ảnh từ anh/chị!

Hiện tại hệ thống trợ lý AI đang bảo trì.

Để em tư vấn chính xác, anh/chị vui lòng:
1. Gửi mã sản phẩm (ví dụ: [MS123456])
2. Hoặc gõ số sản phẩm (ví dụ: 123456)
3. Hoặc mô tả sản phẩm trong ảnh

Anh/chị có mã sản phẩm không ạ?"""
        
        send_message(uid, response)
        return
    
    ctx = USER_CONTEXT[uid]
    
    # Gửi thông báo đang xử lý
    send_message(uid, "🖼️ Em đang phân tích ảnh sản phẩm của anh/chị...")
    
    try:
        # 1. Phân tích ảnh bằng GPT-4o Vision (đã sửa lỗi Facebook URL)
        analysis = analyze_image_with_gpt4o(image_url)
        
        if not analysis:
            send_message(uid, "❌ Em chưa phân tích được ảnh này. Anh/chị có thể mô tả sản phẩm hoặc gửi mã sản phẩm được không ạ?")
            return
        
        # 2. Lưu kết quả phân tích vào context
        ctx["last_image_analysis"] = analysis
        ctx["last_image_url"] = image_url
        ctx["referral_source"] = "image_upload_analyzed"
        
        # 3. Tìm sản phẩm phù hợp
        matched_products = find_products_by_image_analysis(uid, analysis, limit=5)
        
        if matched_products:
            # 4. Gửi đề xuất sản phẩm
            send_product_suggestions(uid, matched_products, analysis)
            
            # 5. Gợi ý thêm
            send_message(uid, "💡 Anh/chị muốn:")
            send_message(uid, "1. Xem thêm sản phẩm tương tự")
            send_message(uid, "2. Được tư vấn chi tiết về sản phẩm nào đó")
            send_message(uid, "3. Hoặc gửi ảnh khác để em phân tích")
            
        else:
            # 6. Không tìm thấy sản phẩm phù hợp
            product_type = analysis.get("product_type", "sản phẩm")
            main_color = analysis.get("main_color", "")
            
            if main_color:
                send_message(uid, f"🔍 Em phân tích được đây là {product_type} màu {main_color}")
            else:
                send_message(uid, f"🔍 Em phân tích được đây là {product_type}")
            
            send_message(uid, "Hiện em chưa tìm thấy sản phẩm khớp 100% trong kho.")
            send_message(uid, "Anh/chị có thể:")
            send_message(uid, "1. Gửi thêm ảnh góc khác")
            send_message(uid, "2. Gõ 'xem sản phẩm' để xem toàn bộ danh mục")
            send_message(uid, "3. Mô tả chi tiết hơn về sản phẩm này")
    
    except Exception as e:
        print(f"❌ Lỗi xử lý ảnh: {str(e)}")
        send_message(uid, "❌ Em gặp lỗi khi phân tích ảnh. Anh/chị vui lòng thử lại hoặc gửi mã sản phẩm để em tư vấn ạ!")


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
# HANDLE TEXT - GPT XỬ LÝ MỌI CÂU HỎI
# ============================================

def handle_text(uid: str, text: str):
    """Xử lý tin nhắn văn bản từ người dùng - GPT xử lý mọi câu hỏi"""
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

        # Xử lý order form step (nếu đang trong flow đặt hàng)
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
                        "buttons": [
                            {
                                "type": "web_url",
                                "url": f"{DOMAIN}/order-form?ms={ms}&uid={uid}",
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

        # Tìm mã sản phẩm trong text
        detected_ms = detect_ms_from_text(text)
        
        # Xác định mã sản phẩm sẽ dùng cho GPT
        current_ms = None
        
        # KIỂM TRA NẾU CHỈ GỬI MÃ SẢN PHẨM (KHÔNG CÓ NỘI DUNG KHÁC)
        is_only_product_code = False
        if detected_ms and detected_ms in PRODUCTS:
            # Kiểm tra xem tin nhắn có chỉ chứa mã sản phẩm không
            temp_text = normalize_vietnamese(text.lower())
            
            # Loại bỏ các từ thông dụng chỉ mã sản phẩm
            keywords = ['ms', 'ma', 'maso', 'ma so', 'san pham', 'tu van', 'xem', 'so']
            
            # Loại bỏ mã sản phẩm đầy đủ
            temp_text = re.sub(re.escape(detected_ms.lower()), '', temp_text)
            
            # Loại bỏ các keyword
            for kw in keywords:
                temp_text = re.sub(r'\b' + re.escape(kw) + r'\b', '', temp_text)
            
            # Loại bỏ số trong mã
            ms_number = re.search(r'MS(\d+)', detected_ms)
            if ms_number:
                num = ms_number.group(1)
                # Loại bỏ số 0 ở đầu
                num_stripped = num.lstrip('0')
                if num_stripped:
                    temp_text = re.sub(r'\b' + re.escape(num_stripped) + r'\b', '', temp_text)
                    # Cũng thử loại bỏ số có số 0 đầu
                    for i in range(1, 7):
                        padded = num_stripped.zfill(i)
                        temp_text = re.sub(r'\b' + re.escape(padded) + r'\b', '', temp_text)
            
            # Loại bỏ khoảng trắng và ký tự đặc biệt
            temp_text = re.sub(r'[^\w]', '', temp_text)
            
            # Nếu sau khi loại bỏ tất cả, không còn ký tự nào thì là only product code
            is_only_product_code = len(temp_text.strip()) == 0
        
        if detected_ms and detected_ms in PRODUCTS:
            # Có mã sản phẩm trong tin nhắn
            current_ms = detected_ms
            ctx["last_ms"] = detected_ms
            update_product_context(uid, detected_ms)
            
            # NẾU CHỈ GỬI MÃ SẢN PHẨM: gửi thông tin chi tiết với hình ảnh
            if is_only_product_code:
                send_product_info_debounced(uid, detected_ms)
                ctx["processing_lock"] = False
                return
            # NẾU CÓ KÈM CÂU HỎI KHÁC: tiếp tục xử lý bằng GPT
        else:
            # Dùng mã sản phẩm từ context
            current_ms = get_relevant_product_for_question(uid, text)
        
        # TẤT CẢ CÂU HỎI CÒN LẠI do GPT xử lý
        print(f"[GPT CALL] User: {uid}, MS: {current_ms}, Text: {text}")
        gpt_response = generate_gpt_response(uid, text, current_ms)
        send_message(uid, gpt_response)
        
        # Kiểm tra từ khóa đặt hàng để gửi link
        if current_ms and current_ms in PRODUCTS and any(kw in lower for kw in ORDER_KEYWORDS):
            domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
            order_link = f"{domain}/order-form?ms={current_ms}&uid={uid}"
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
    print("Webhook received:", json.dumps(data, ensure_ascii=False)[:500])

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
            
            # Xử lý referral (từ CTA, ads, bình luận)
            if m.get("referral"):
                ref = m["referral"]
                ctx = USER_CONTEXT[sender_id]
                ctx["referral_source"] = ref.get("source", "unknown")
                ctx["referral_payload"] = ref.get("ref", "")
                print(f"[REFERRAL] User {sender_id} từ {ctx['referral_source']} với payload: {ctx['referral_payload']}")
                
                # Có thể xử lý thêm dựa trên referral payload
                if ctx["referral_payload"] and ctx["referral_payload"].startswith("MS"):
                    ctx["last_ms"] = ctx["referral_payload"]
                    update_product_context(sender_id, ctx["referral_payload"])
            
            if "postback" in m:
                payload = m["postback"].get("payload")
                if payload:
                    ctx = USER_CONTEXT[sender_id]
                    postback_id = m["postback"].get("mid")
                    now = time.time()
                    
                    # Kiểm tra duplicate postback
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
                    
                    # Xử lý postback
                    if payload == "GET_STARTED":
                        ctx["referral_source"] = "get_started"
                        welcome_msg = f"""Chào anh/chị! 👋 
Em là trợ lý AI của {FANPAGE_NAME}.

Để em tư vấn chính xác, anh/chị vui lòng:
1. Gửi mã sản phẩm (ví dụ: [MS123456])
2. Hoặc gõ "xem sản phẩm" để xem danh sách
3. Hoặc mô tả sản phẩm bạn đang tìm

Anh/chị quan tâm sản phẩm nào ạ?"""
                        send_message(sender_id, welcome_msg)
                    
                    elif payload.startswith("ADVICE_"):
                        load_products()  # Đảm bảo đã load sản phẩm
                        ms = payload.replace("ADVICE_", "")
                        if ms in PRODUCTS:
                            ctx["last_ms"] = ms
                            update_product_context(sender_id, ms)
                            send_product_info_debounced(sender_id, ms)
                        else:
                            send_message(sender_id, "❌ Em không tìm thấy sản phẩm này. Anh/chị vui lòng kiểm tra lại mã sản phẩm ạ.")
                    
                    elif payload.startswith("ORDER_"):
                        load_products()  # Đảm bảo đã load sản phẩm
                        ms = payload.replace("ORDER_", "")
                        if ms in PRODUCTS:
                            ctx["last_ms"] = ms
                            update_product_context(sender_id, ms)
                            domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
                            order_link = f"{domain}/order-form?ms={ms}&uid={sender_id}"
                            product_name = PRODUCTS[ms].get('Ten', '')
                            send_message(sender_id, f"🎯 Anh/chị chọn sản phẩm [{ms}] {product_name}!\n\n📋 Đặt hàng ngay tại đây:\n{order_link}")
                        else:
                            send_message(sender_id, "❌ Em không tìm thấy sản phẩm này. Anh/chị vui lòng kiểm tra lại mã sản phẩm ạ.")
                    
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
# ORDER FORM PAGE (GIỮ NGUYÊN)
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
# API ENDPOINTS (GIỮ NGUYÊN)
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
        "openai_vision_available": bool(client and OPENAI_API_KEY),
        "facebook_configured": bool(PAGE_ACCESS_TOKEN),
        "image_processing": "base64+fallback"
    }, 200


# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    print("Starting app on http://0.0.0.0:5000")
    print(f"🟢 GPT-4o Vision API: {'SẴN SÀNG' if client and OPENAI_API_KEY else 'CHƯA CẤU HÌNH'}")
    print(f"🟢 Fanpage: {FANPAGE_NAME}")
    print(f"🟢 Domain: {DOMAIN}")
    print(f"🟢 Image Processing: Base64 + Fallback URL")
    app.run(host="0.0.0.0", port=5000, debug=True)
