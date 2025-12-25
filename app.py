import os
import json
import re
import time
import csv
import hashlib
import base64
import threading
import gzip
import functools
from collections import defaultdict
from urllib.parse import quote
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from io import BytesIO
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

import requests
from flask import Flask, request, send_from_directory, jsonify, render_template_string, make_response
from openai import OpenAI

# ============================================
# FLASK APP
# ============================================
app = Flask(__name__)

# ============================================
# ENV & CONFIG - THÊM POSCAKE VÀ PAGE_ID
# ============================================
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN", "").strip()
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
GOOGLE_SHEET_CSV_URL = os.getenv("SHEET_CSV_URL", "").strip()
DOMAIN = os.getenv("DOMAIN", "").strip() or "fb-gpt-chatbot.onrender.com"
FANPAGE_NAME = os.getenv("FANPAGE_NAME", "Shop thời trang")
FCHAT_WEBHOOK_URL = os.getenv("FCHAT_WEBHOOK_URL", "").strip()
FCHAT_TOKEN = os.getenv("FCHAT_TOKEN", "").strip()

# Cấu hình Poscake Webhook
POSCAKE_API_KEY = os.getenv("POSCAKE_API_KEY", "").strip()
POSCAKE_WEBHOOK_SECRET = os.getenv("POSCAKE_WEBHOOK_SECRET", "").strip()
POSCAKE_STORE_ID = os.getenv("POSCAKE_STORE_ID", "").strip()

# Page ID để xác định comment từ page
PAGE_ID = os.getenv("PAGE_ID", "516937221685203").strip()

# ============================================
# GOOGLE SHEETS API CONFIGURATION
# ============================================
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
GOOGLE_SHEETS_CREDENTIALS_JSON = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON", "").strip()

if not GOOGLE_SHEET_CSV_URL:
    GOOGLE_SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv"

# ============================================
# APP ID CỦA BOT
# ============================================
BOT_APP_IDS = {"645956568292435"}

# ============================================
# GLOBAL LOCKS
# ============================================
POSTBACK_LOCKS = {}

def get_postback_lock(uid: str, payload: str):
    key = f"{uid}_{payload}"
    if key not in POSTBACK_LOCKS:
        POSTBACK_LOCKS[key] = threading.Lock()
    return POSTBACK_LOCKS[key]

# ============================================
# OPENAI CLIENT
# ============================================
client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ============================================
# MAP TIẾNG VIỆT KHÔNG DẤU
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
    if not text: return ""
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
    "real_message_count": 0,
    "product_history": [],
    "conversation_history": [],
    "referral_source": None,
    "referral_payload": None,
    "last_retailer_id": None,
    "catalog_view_time": 0,
    "has_sent_first_carousel": False,
    "idempotent_postbacks": {},
    "processed_message_mids": {},
    "last_processed_text": "",
    # Thêm trường mới cho Poscake
    "poscake_orders": []
})

PRODUCTS = {}
PRODUCTS_BY_NUMBER = {}
LAST_LOAD = 0
LOAD_TTL = 300

# ============================================
# CACHE CHO TÊN FANPAGE
# ============================================
FANPAGE_NAME_CACHE = None
FANPAGE_NAME_CACHE_TIME = 0
FANPAGE_NAME_CACHE_TTL = 3600

def get_fanpage_name_from_api():
    global FANPAGE_NAME_CACHE, FANPAGE_NAME_CACHE_TIME
    
    now = time.time()
    if (FANPAGE_NAME_CACHE and 
        FANPAGE_NAME_CACHE_TIME and 
        (now - FANPAGE_NAME_CACHE_TIME) < FANPAGE_NAME_CACHE_TTL):
        return FANPAGE_NAME_CACHE
    
    if not PAGE_ACCESS_TOKEN:
        FANPAGE_NAME_CACHE = FANPAGE_NAME
        FANPAGE_NAME_CACHE_TIME = now
        return FANPAGE_NAME_CACHE
    
    try:
        url = f"https://graph.facebook.com/v12.0/me?fields=name&access_token={PAGE_ACCESS_TOKEN}"
        response = requests.get(url, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            page_name = data.get('name', FANPAGE_NAME)
            FANPAGE_NAME_CACHE = page_name
            FANPAGE_NAME_CACHE_TIME = now
            return page_name
        else:
            FANPAGE_NAME_CACHE = FANPAGE_NAME
            FANPAGE_NAME_CACHE_TIME = now
            return FANPAGE_NAME_CACHE
    except Exception as e:
        FANPAGE_NAME_CACHE = FANPAGE_NAME
        FANPAGE_NAME_CACHE_TIME = now
        return FANPAGE_NAME_CACHE

# ============================================
# HÀM CẬP NHẬT CONTEXT VỚI MS MỚI VÀ RESET COUNTER
# ============================================

def update_context_with_new_ms(uid: str, new_ms: str, source: str = "unknown"):
    """
    Cập nhật context với MS mới và reset counter để đảm bảo bot gửi carousel
    cho sản phẩm mới khi user gửi tin nhắn đầu tiên
    """
    if not new_ms or new_ms not in PRODUCTS:
        return False
    
    ctx = USER_CONTEXT[uid]
    
    # Lấy MS cũ để so sánh
    old_ms = ctx.get("last_ms")
    
    # Nếu MS mới khác với MS cũ, reset counter
    if old_ms != new_ms:
        print(f"[CONTEXT UPDATE] User {uid}: Chuyển từ {old_ms} sang {new_ms} (nguồn: {source})")
        
        # Reset counter để bot gửi carousel cho sản phẩm mới
        ctx["real_message_count"] = 0
        ctx["has_sent_first_carousel"] = False
        ctx["last_msg_time"] = 0  # Reset thời gian tin nhắn cuối
        ctx["last_processed_text"] = ""  # Reset text đã xử lý
    
    # Cập nhật MS mới
    ctx["last_ms"] = new_ms
    ctx["referral_source"] = source
    
    # Gọi hàm update_product_context cũ
    if "product_history" not in ctx:
        ctx["product_history"] = []
    
    if not ctx["product_history"] or ctx["product_history"][0] != new_ms:
        if new_ms in ctx["product_history"]:
            ctx["product_history"].remove(new_ms)
        ctx["product_history"].insert(0, new_ms)
    
    if len(ctx["product_history"]) > 5:
        ctx["product_history"] = ctx["product_history"][:5]
    
    print(f"[CONTEXT UPDATE] Đã cập nhật MS {new_ms} cho user {uid} (nguồn: {source}, real_message_count: {ctx['real_message_count']})")
    return True

# ============================================
# HÀM PHÁT HIỆN EMOJI/STICKER
# ============================================

def is_emoji_or_sticker_image(image_url: str) -> bool:
    """
    Phát hiện ảnh emoji/sticker dựa trên URL
    """
    if not image_url:
        return True
    
    image_url_lower = image_url.lower()
    
    # Kiểm tra từ khóa đặc trưng của emoji/sticker Facebook
    emoji_keywords = [
        'emoji', 'sticker', 'stickers', 'stickerpack',
        'facebook.com/images/stickers/',
        'fbcdn.net/images/emoji.php',
        'graph.facebook.com/sticker',
        'scontent.xx.fbcdn.net/v/t39.1997-6/',  # Đường dẫn sticker Facebook
        'cdn.jsdelivr.net/emojione/assets',  # Emojione
        'twemoji.maxcdn.com',  # Twemoji
        'noto-website-2.storage.googleapis.com',  # Noto Emoji
    ]
    
    for keyword in emoji_keywords:
        if keyword in image_url_lower:
            return True
    
    # Kiểm tra đuôi file - emoji thường là SVG hoặc định dạng đặc biệt
    emoji_extensions = ['.svg', '.svgs', '.svgz', '.gif', '.apng', '.webp']
    
    for ext in emoji_extensions:
        if image_url_lower.endswith(ext):
            return True
    
    # Kiểm tra pattern URL đặc biệt
    emoji_patterns = [
        r'emoji_\d+\.(png|jpg|gif)',
        r'sticker_\d+\.(png|jpg|gif)',
        r'emoji/[\w\-]+\.(png|jpg|gif)',
        r'stickers/[\w\-]+\.(png|jpg|gif)',
    ]
    
    for pattern in emoji_patterns:
        if re.search(pattern, image_url_lower):
            return True
    
    return False

# ============================================
# HÀM KIỂM TRA ẢNH SẢN PHẨM HỢP LỆ (CẢI TIẾN)
# ============================================

def is_valid_product_image(image_url: str) -> bool:
    """
    Kiểm tra xem ảnh có phải là ảnh sản phẩm hợp lệ không
    Cải tiến để chấp nhận nhiều định dạng URL hơn
    """
    if not image_url:
        return False
    
    image_url_lower = image_url.lower()
    
    # Kiểm tra đuôi file ảnh hợp lệ
    valid_extensions = ['.jpg', '.jpeg', '.png', '.webp', '.gif', '.bmp', '.tiff']
    
    for ext in valid_extensions:
        if ext in image_url_lower:
            return True
    
    # Kiểm tra domain ảnh phổ biến (bao gồm cả Facebook)
    valid_domains = [
        'fbcdn.net', 'scontent.xx', 'scontent.fhan', 'cdn.shopify', 
        'static.nike', 'lzd-img', 'shopee', 'tiki', 'content.pancake.vn',
        'instagram.com', 'cloudinary.com', 'images.unsplash.com',
        'graph.facebook.com', 'facebook.com'
    ]
    
    for domain in valid_domains:
        if domain in image_url_lower:
            return True
    
    # Kiểm tra pattern URL chứa thông tin ảnh
    image_patterns = [
        r'\.(jpg|jpeg|png|webp|gif)(\?|$)',
        r'/photos/',
        r'/images/',
        r'/img/',
        r'/picture/',
        r'/media/',
        r'/upload/'
    ]
    
    for pattern in image_patterns:
        if re.search(pattern, image_url_lower):
            return True
    
    return False

# ============================================
# HÀM TẢI ẢNH VỀ SERVER VÀ CHUYỂN THÀNH BASE64
# ============================================

def download_image_to_base64(image_url: str) -> Optional[str]:
    """
    Tải ảnh từ URL và chuyển thành chuỗi base64.
    Trả về None nếu không tải được.
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'image/webp,image/*,*/*;q=0.8',
            'Accept-Language': 'vi,en-US;q=0.9,en;q=0.8',
            'Referer': 'https://www.facebook.com/'
        }
        
        # Thử tải với timeout ngắn
        response = requests.get(image_url, headers=headers, timeout=5)
        
        if response.status_code == 200:
            # Kiểm tra content type có phải là ảnh không
            content_type = response.headers.get('content-type', '').lower()
            if 'image' in content_type:
                image_data = response.content
                base64_str = base64.b64encode(image_data).decode('utf-8')
                return base64_str
            else:
                print(f"[IMAGE DOWNLOAD] Không phải ảnh: {content_type}")
        else:
            print(f"[IMAGE DOWNLOAD] Lỗi HTTP: {response.status_code}")
    except Exception as e:
        print(f"[IMAGE DOWNLOAD] Lỗi khi tải ảnh: {e}")
    return None

# ============================================
# HÀM PHÂN TÍCH ẢNH BẰNG OPENAI VISION API (CẢI TIẾN)
# ============================================

def analyze_image_with_vision_api(image_url: str) -> str:
    """
    Phân tích ảnh bằng OpenAI Vision API và trả về mô tả text
    Sử dụng base64 để tránh lỗi URL không tải được
    """
    if not client:
        return ""
    
    print(f"[VISION API] Đang phân tích ảnh: {image_url[:100]}...")
    
    try:
        # THỬ 1: Dùng URL trực tiếp (nhanh nhất)
        print(f"[VISION API] Thử dùng URL trực tiếp...")
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": """Bạn là chuyên gia nhận diện sản phẩm thời trang. Hãy mô tả CHI TIẾT và CHÍNH XÁC sản phẩm trong ảnh theo các tiêu chí:

1. LOẠI SẢN PHẨM (bắt buộc): áo thun, áo sơ mi, váy, quần jeans, áo khoác, đầm, v.v.
2. MÀU SẮC CHÍNH (bắt buộc): đỏ, xanh, trắng, đen, hồng, tím, v.v.
3. CHẤT LIỆU (nếu thấy): cotton, linen, jean, lụa, v.v.
4. HỌA TIẾT: trơn, sọc, caro, hoa, hình in, logo, v.v.
5. KIỂU DÁNG: cổ tròn, cổ tim, tay dài, tay ngắn, ôm body, rộng, v.v.
6. ĐẶC ĐIỂM NỔI BẬT: túi, nút, dây kéo, viền, đính đá, v.v.
7. PHONG CÁCH: casual, công sở, dạo phố, dự tiệc, thể thao, v.v.

MÔ TẢ PHẢI NGẮN GỌN nhưng ĐẦY ĐỦ từ khóa quan trọng. Ưu tiên từ khóa thông dụng trong thời trang."""},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": image_url,
                                "detail": "auto"
                            }
                        }
                    ]
                }
            ],
            max_tokens=500,
            temperature=0.1
        )
        
        return response.choices[0].message.content
    except Exception as e:
        print(f"[VISION API URL ERROR] Lỗi khi dùng URL: {e}")
        
        # THỬ 2: Tải ảnh về và dùng base64
        print(f"[VISION API] Đang tải ảnh về để chuyển base64...")
        base64_image = download_image_to_base64(image_url)
        
        if base64_image:
            try:
                print(f"[VISION API] Thử dùng base64...")
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {
                            "role": "user",
                            "content": [
                                {"type": "text", "text": """Mô tả chi tiết sản phẩm trong ảnh, tập trung vào loại sản phẩm, màu sắc, chất liệu, họa tiết và phong cách."""},
                                {
                                    "type": "image_url",
                                    "image_url": {
                                        "url": f"data:image/jpeg;base64,{base64_image}"
                                    }
                                }
                            ]
                        }
                    ],
                    max_tokens=400,
                    temperature=0.1
                )
                return response.choices[0].message.content
            except Exception as e2:
                print(f"[VISION API BASE64 ERROR] Lỗi khi dùng base64: {e2}")
        
        # THỬ 3: Dùng URL đơn giản hóa
        try:
            print(f"[VISION API] Thử dùng URL đơn giản hóa...")
            # Lấy phần base URL không có tham số phức tạp
            simple_url = image_url.split('?')[0]
            if 'fbcdn.net' in simple_url:
                simple_url = simple_url + '?dl=1'
            
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": "Mô tả ngắn sản phẩm trong ảnh."},
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": simple_url,
                                    "detail": "low"
                                }
                            }
                        ]
                    }
                ],
                max_tokens=300,
                temperature=0.1
            )
            return response.choices[0].message.content
        except Exception as e3:
            print(f"[VISION API SIMPLE URL ERROR] Lỗi cuối cùng: {e3}")
    
    return ""

# ============================================
# HÀM TRÍCH XUẤT TỪ KHÓA TỪ MÔ TẢ
# ============================================

def extract_keywords_from_description(description: str) -> set:
    """Trích xuất từ khóa quan trọng từ mô tả"""
    stop_words = {'của', 'và', 'là', 'có', 'trong', 'với', 'cho', 'từ', 'này', 'ảnh', 
                  'sản phẩm', 'phẩm', 'chụp', 'nhìn', 'thấy', 'rất', 'một', 'như', 
                  'bởi', 'các', 'được', 'nên', 'khi', 'hoặc', 'nếu', 'thì', 'mà'}
    
    # Từ khóa quan trọng trong thời trang
    fashion_keywords = {
        'áo', 'quần', 'váy', 'đầm', 'áo thun', 'áo sơ mi', 'jeans', 'khoác', 
        'hoodie', 'sweater', 'jacket', 'blazer', 'cardigan', 'polo', 'tank top',
        'shorts', 'skirt', 'jumpsuit', 'romper', 'leggings', 'jogger'
    }
    
    keywords = set()
    words = description.split()
    
    for word in words:
        word = word.strip('.,!?;:()[]{}"\'').lower()
        if len(word) > 2 and word not in stop_words:
            keywords.add(word)
    
    # Thêm các từ khóa ghép (2-3 từ)
    for i in range(len(words) - 1):
        bigram = f"{words[i]} {words[i+1]}"
        if any(keyword in bigram for keyword in fashion_keywords):
            keywords.add(bigram)
    
    return keywords

# ============================================
# HÀM TÍNH ĐIỂM TƯƠNG ĐỒNG SẢN PHẨM
# ============================================

def calculate_product_similarity_score(ms: str, product: dict, desc_lower: str, desc_keywords: set) -> float:
    """Tính điểm tương đồng giữa sản phẩm và mô tả ảnh"""
    score = 0
    
    # Lấy thông tin sản phẩm
    ten = normalize_vietnamese(product.get("Ten", "").lower())
    mo_ta = normalize_vietnamese(product.get("MoTa", "").lower())
    mau_sac = normalize_vietnamese(product.get("màu (Thuộc tính)", "").lower())
    thuoc_tinh = normalize_vietnamese(product.get("Thuộc tính", "").lower())
    
    # Tạo bộ từ khóa sản phẩm
    product_keywords = set()
    
    # Thêm từ khóa từ tên sản phẩm
    for word in ten.split():
        if len(word) > 1:
            product_keywords.add(word)
    
    # Thêm từ khóa từ mô tả
    for word in mo_ta.split()[:50]:
        word = word.strip('.,!?;:()[]{}"\'').lower()
        if len(word) > 1:
            product_keywords.add(word)
    
    # Thêm màu sắc
    if mau_sac:
        for color in mau_sac.split(','):
            color_clean = color.strip().lower()
            if color_clean:
                product_keywords.add(color_clean)
    
    # Thêm thuộc tính
    if thuoc_tinh:
        for attr in thuoc_tinh.split(','):
            attr_clean = attr.strip().lower()
            if attr_clean:
                product_keywords.add(attr_clean)
    
    # Tính điểm: từ khóa trùng nhau
    common_keywords = desc_keywords.intersection(product_keywords)
    score += len(common_keywords) * 3  # Trọng số cao cho từ khóa trùng
    
    # Ưu tiên các từ khóa quan trọng (loại sản phẩm)
    fashion_keywords = {'áo', 'quần', 'váy', 'đầm', 'áo thun', 'áo sơ mi', 'jeans', 
                       'khoác', 'hoodie', 'sweater', 'jacket', 'blazer'}
    
    for keyword in fashion_keywords:
        if keyword in desc_lower and keyword in ten.lower():
            score += 8  # Trọng số rất cao cho loại sản phẩm trùng
    
    # Ưu tiên màu sắc trùng khớp
    if mau_sac:
        for color in mau_sac.split(','):
            color_clean = color.strip().lower()
            if color_clean in desc_lower:
                score += 5  # Trọng số cao cho màu sắc trùng
    
    # Kiểm tra xem tên sản phẩm có trong mô tả ảnh không
    for word in ten.split():
        if len(word) > 3 and word in desc_lower:
            score += 4
    
    return score

# ============================================
# HÀM TÌM SẢN PHẨM BẰNG MÔ TẢ ẢNH (CẢI TIẾN NÂNG CAO)
# ============================================

def find_product_by_image_description_enhanced(description: str) -> Optional[str]:
    """
    Tìm sản phẩm phù hợp nhất dựa trên mô tả ảnh - CẢI TIẾN NÂNG CAO
    """
    load_products()
    
    if not description or not PRODUCTS:
        return None
    
    # Chuẩn hóa mô tả ảnh
    desc_lower = normalize_vietnamese(description.lower())
    print(f"[IMAGE MATCH ENHANCED] Mô tả ảnh: {desc_lower[:200]}...")
    
    # Tạo danh sách từ khóa quan trọng từ mô tả ảnh
    desc_keywords = extract_keywords_from_description(desc_lower)
    
    # Tìm kiếm sản phẩm với điểm số cải tiến
    product_scores = {}
    
    for ms, product in PRODUCTS.items():
        score = calculate_product_similarity_score(ms, product, desc_lower, desc_keywords)
        
        if score > 0:
            product_scores[ms] = score
    
    if not product_scores:
        print("[IMAGE MATCH] Không tìm thấy sản phẩm nào phù hợp")
        return None
    
    # Sắp xếp theo điểm cao nhất
    sorted_products = sorted(product_scores.items(), key=lambda x: x[1], reverse=True)
    
    # Lấy sản phẩm có điểm cao nhất
    best_ms, best_score = sorted_products[0]
    
    print(f"[IMAGE MATCH SCORES] Điểm cao nhất: {best_ms} với {best_score} điểm")
    
    # Ngưỡng tối thiểu: cần ít nhất 5 điểm để coi là phù hợp
    if best_score >= 5:
        product_name = PRODUCTS[best_ms].get("Ten", "")
        print(f"[IMAGE MATCH SUCCESS] Tìm thấy {best_ms} - {product_name}")
        return best_ms
    
    print(f"[IMAGE MATCH FAIL] Điểm quá thấp: {best_score}")
    return None

# ============================================
# HÀM GỬI CAROUSEL GỢI Ý SẢN PHẨM
# ============================================

def send_suggestion_carousel(uid: str, suggestion_count: int = 3):
    """
    Gửi carousel gợi ý các sản phẩm phổ biến
    """
    load_products()
    
    if not PRODUCTS:
        send_message(uid, "Hiện tại chưa có sản phẩm nào trong hệ thống.")
        return False
    
    # Lấy danh sách sản phẩm (ưu tiên sản phẩm có ảnh)
    valid_products = []
    for ms, product in PRODUCTS.items():
        images_field = product.get("Images", "")
        urls = parse_image_urls(images_field)
        if urls:  # Chỉ lấy sản phẩm có ảnh
            valid_products.append(ms)
    
    # Nếu không đủ sản phẩm có ảnh, lấy tất cả
    if len(valid_products) < suggestion_count:
        valid_products = list(PRODUCTS.keys())
    
    # Lấy ngẫu nhiên hoặc lấy sản phẩm đầu tiên
    suggestion_products = valid_products[:suggestion_count]
    
    elements = []
    for ms in suggestion_products:
        product = PRODUCTS[ms]
        images_field = product.get("Images", "")
        urls = parse_image_urls(images_field)
        image_url = urls[0] if urls else ""
        
        gia_int = extract_price_int(product.get("Gia", "")) or 0
        
        # LẤY TÊN SẢN PHẨM (KHÔNG BAO GỒM MÃ SẢN PHẨM)
        product_name = product.get('Ten', '')
        
        # KIỂM TRA NẾU TÊN ĐÃ CHỨA MÃ SẢN PHẨM, CHỈ GIỮ TÊN
        if f"[{ms}]" in product_name or ms in product_name:
            # Xóa mã sản phẩm khỏi tên
            product_name = product_name.replace(f"[{ms}]", "").replace(ms, "").strip()
        
        element = {
            "title": product_name,  # CHỈ HIỂN THỊ TÊN SẢN PHẨM
            "image_url": image_url,
            "subtitle": f"💰 Giá: {gia_int:,.0f} đ",
            "buttons": [
                {
                    "type": "postback",
                    "title": "🌟 Ưu điểm SP",
                    "payload": f"PRODUCT_HIGHLIGHTS_{ms}"
                },
                {
                    "type": "postback", 
                    "title": "🖼️ Xem ảnh",
                    "payload": f"VIEW_IMAGES_{ms}"
                },
                {
                    "type": "web_url",
                    "url": f"{DOMAIN}/order-form?ms={ms}&uid={uid}",
                    "title": "🛒 Đặt ngay"
                }
            ]
        }
        elements.append(element)
    
    if elements:
        send_carousel_template(uid, elements)
        return True
    return False

# ============================================
# HÀM TÌM SẢN PHẨM TỪ ẢNH (CẢI TIẾN MỚI)
# ============================================

def find_product_by_image(image_url: str) -> Optional[str]:
    """
    Tìm sản phẩm từ ảnh bằng cách sử dụng Vision API để lấy mô tả,
    sau đó so khớp mô tả với tên và mô tả sản phẩm trong database.
    Trả về mã sản phẩm (MS) nếu tìm thấy, ngược lại trả về None.
    """
    # Bước 1: Kiểm tra xem có phải emoji/sticker không
    if is_emoji_or_sticker_image(image_url):
        print(f"[IMAGE CHECK] Đây là emoji/sticker, bỏ qua")
        return None
    
    # Bước 1.5: Kiểm tra ảnh có hợp lệ không
    if not is_valid_product_image(image_url):
        print(f"[INVALID IMAGE] Ảnh không hợp lệ: {image_url[:100]}")
        return None
    
    # Bước 2: Phân tích ảnh để lấy mô tả
    print(f"[IMAGE PROCESS] Đang phân tích ảnh bằng Vision API...")
    image_description = analyze_image_with_vision_api(image_url)
    
    if not image_description:
        print(f"[IMAGE PROCESS] Không thể phân tích ảnh")
        return None
    
    print(f"[IMAGE DESCRIPTION] {image_description[:300]}...")
    
    # Bước 3: Tìm sản phẩm phù hợp với mô tả
    found_ms = find_product_by_image_description_enhanced(image_description)
    
    if found_ms:
        print(f"[IMAGE MATCH] Tìm thấy sản phẩm {found_ms} từ ảnh")
        return found_ms
    
    print(f"[IMAGE MATCH] Không tìm thấy sản phẩm phù hợp")
    return None

# ============================================
# HELPER: TRÍCH XUẤT MÃ SẢN PHẨM
# ============================================

def extract_ms_from_retailer_id(retailer_id: str) -> Optional[str]:
    if not retailer_id:
        return None
    
    parts = retailer_id.split('_')
    if not parts:
        return None
    
    base_id = parts[0].upper()
    if re.match(r'MS\d{6}', base_id):
        return base_id
    
    match = re.search(r'MS(\d+)', base_id)
    if match:
        num = match.group(1)
        num_6 = num.zfill(6)
        return "MS" + num_6
    
    return None

def extract_ms_from_ad_title(ad_title: str) -> Optional[str]:
    if not ad_title:
        return None
    
    ad_title_lower = ad_title.lower()
    
    match = re.search(r'mã\s*(\d{1,6})', ad_title_lower)
    if match:
        num = match.group(1)
        num_6 = num.zfill(6)
        return "MS" + num_6
    
    match = re.search(r'ms\s*(\d{1,6})', ad_title_lower)
    if match:
        num = match.group(1)
        num_6 = num.zfill(6)
        return "MS" + num_6
    
    match = re.search(r'\b(\d{2,6})\b', ad_title)
    if match:
        num = match.group(1)
        num_6 = num.zfill(6)
        return "MS" + num_6
    
    return None

# ============================================
# HELPER: KIỂM TRA ECHO MESSAGE (ĐÃ CẢI THIỆN)
# ============================================

def is_bot_generated_echo(echo_text: str, app_id: str = "", attachments: list = None) -> bool:
    # ƯU TIÊN: Nếu có #MS trong tin nhắn => KHÔNG PHẢI BOT (là comment từ Fchat)
    if echo_text and "#MS" in echo_text.upper():
        return False
    
    if app_id in BOT_APP_IDS:
        return True
    
    if echo_text:
        echo_text_lower = echo_text.lower()
        
        # Các dấu hiệu bot RÕ RÀNG (chỉ những mẫu rất đặc trưng)
        clear_bot_phrases = [
            "🌟 **5 ưu điểm nổi bật**",
            "🛒 đơn hàng mới",
            "🎉 shop đã nhận được đơn hàng",
            "dạ, phần này trong hệ thống chưa có thông tin ạ",
            "dạ em đang gặp chút trục trặc",
            "💰 giá sản phẩm:",
            "📝 mô tả:",
            "📌 [ms",
            "🛒 đơn hàng mới",
            "🎉 shop đã nhận được đơn hàng",
        ]
        
        for phrase in clear_bot_phrases:
            if phrase in echo_text_lower:
                print(f"[ECHO BOT PHRASE] Phát hiện cụm bot: {phrase}")
                return True
        
        # Bot format rõ ràng
        if re.search(r'^\*\*.*\*\*', echo_text) or re.search(r'^\[MS\d+\]', echo_text, re.IGNORECASE):
            print(f"[ECHO BOT FORMAT] Phát hiện format bot")
            return True
        
        # Tin nhắn quá dài (>300) và có cấu trúc bot
        if len(echo_text) > 300 and ("dạ," in echo_text_lower or "ạ!" in echo_text_lower):
            print(f"[ECHO LONG BOT] Tin nhắn dài có cấu trúc bot: {len(echo_text)} chars")
            return True
        
        # Các pattern khác giảm độ nhạy (chỉ nhận diện khi rất rõ)
        bot_patterns_regex = [
            r"dạ,.*\d{1,3}[.,]?\d{0,3}\s*đ.*\d{1,3}[.,]?\d{0,3}\s*đ",  # Nhiều giá tiền (rất có thể là bot)
            r"dạ,.*\d+\s*cm.*\d+\s*cm",  # Nhiều kích thước
        ]
        
        for pattern in bot_patterns_regex:
            if re.search(pattern, echo_text_lower):
                print(f"[ECHO BOT PATTERN] Phát hiện pattern: {pattern}")
                return True
    
    return False

# ============================================
# HÀM LẤY NỘI DUNG BÀI VIẾT TỪ POST_ID
# ============================================

def get_post_content_from_facebook(post_id: str) -> Optional[dict]:
    """
    Lấy nội dung bài viết từ Facebook Graph API
    Trả về dict chứa message và các thông tin khác
    """
    if not PAGE_ACCESS_TOKEN or not post_id:
        print(f"[GET POST CONTENT] Thiếu token hoặc post_id")
        return None
    
    try:
        # Graph API endpoint để lấy nội dung bài viết
        url = f"https://graph.facebook.com/v12.0/{post_id}"
        params = {
            'fields': 'id,message,created_time,permalink_url',
            'access_token': PAGE_ACCESS_TOKEN
        }
        
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print(f"[GET POST CONTENT] Đã lấy nội dung bài viết {post_id}")
            return data
        else:
            print(f"[GET POST CONTENT] Lỗi API {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        print(f"[GET POST CONTENT] Exception: {e}")
        return None

# ============================================
# HÀM TRÍCH XUẤT MS TỪ BÀI VIẾT (TỐI ƯU - ĐÃ CẢI THIỆN)
# ============================================

def extract_ms_from_post_content(post_data: dict) -> Optional[str]:
    """
    Trích xuất mã sản phẩm từ nội dung bài viết - CẢI THIỆN ĐỂ BẮT [MSxxxxxx]
    """
    if not post_data:
        return None
    
    message = post_data.get('message', '')
    post_id = post_data.get('id', '')
    
    print(f"[EXTRACT MS FROM POST] Đang phân tích bài viết {post_id}: {message[:100]}...")
    
    if not message:
        return None
    
    # PHƯƠNG PHÁP 1: Tìm MS trong dấu ngoặc vuông [MSxxxxxx] - TRƯỜNG HỢP ĐẶC BIỆT
    bracket_patterns = [
        r'\[(MS\d{2,6})\]',  # [MS000034] - CHÍNH XÁC TRƯỜNG HỢP TRONG LOG
        r'\[MS\s*(\d{2,6})\]',  # [MS 000034] với khoảng trắng
    ]
    
    for pattern in bracket_patterns:
        matches = re.findall(pattern, message, re.IGNORECASE)
        for match in matches:
            # match là số (2-6 chữ số)
            num_part = match.lstrip('0')
            if not num_part:  # nếu toàn là số 0
                num_part = '0'
            full_ms = f"MS{num_part.zfill(6)}"
            if full_ms in PRODUCTS:
                print(f"[EXTRACT MS FROM POST] Tìm thấy {full_ms} qua bracket pattern {pattern}")
                return full_ms
    
    # PHƯƠNG PHÁP 2: Tìm MSxxxxxx trực tiếp (có thể có khoảng trắng)
    ms_patterns = [
        (r'\[(MS\d{6})\]', True),  # [MS000046] -> đủ 6 số
        (r'\b(MS\d{6})\b', True),  # MS000046
        (r'#(MS\d{6})', True),     # #MS000046
        (r'Mã\s*:\s*(MS\d{6})', True),  # Mã: MS000046
        (r'SP\s*:\s*(MS\d{6})', True),  # SP: MS000046
        (r'MS\s*(\d{6})', False),  # MS 000046 -> chỉ có số
        (r'mã\s*(\d{6})', False),  # mã 000046 -> chỉ có số
        (r'MS\s*(\d{2,5})\b', False),  # MS 34 -> 2-5 chữ số
        (r'mã\s*(\d{2,5})\b', False),  # mã 34 -> 2-5 chữ số
    ]
    
    for pattern, is_full_ms in ms_patterns:
        matches = re.findall(pattern, message, re.IGNORECASE)
        for match in matches:
            if isinstance(match, tuple):
                match = match[0]
            if is_full_ms:
                # match là MSxxxxxx đầy đủ
                full_ms = match.upper()
            else:
                # match chỉ là số
                num_part = str(match).lstrip('0')
                if not num_part:
                    num_part = '0'
                full_ms = f"MS{num_part.zfill(6)}"
            
            if full_ms in PRODUCTS:
                print(f"[EXTRACT MS FROM POST] Tìm thấy {full_ms} qua pattern {pattern}")
                return full_ms
    
    # PHƯƠNG PHÁP 3: Tìm số 6 chữ số
    six_digit_numbers = re.findall(r'\b(\d{6})\b', message)
    for num in six_digit_numbers:
        # Thử với MS đầy đủ
        full_ms = f"MS{num}"
        if full_ms in PRODUCTS:
            print(f"[EXTRACT MS FROM POST] Tìm thấy số 6 chữ số {num} -> {full_ms}")
            return full_ms
        
        # Thử với số không có leading zeros
        clean_num = num.lstrip('0')
        if clean_num and clean_num in PRODUCTS_BY_NUMBER:
            ms = PRODUCTS_BY_NUMBER[clean_num]
            print(f"[EXTRACT MS FROM POST] Tìm thấy số rút gọn {num} -> {ms}")
            return ms
    
    # PHƯƠNG PHÁP 4: Tìm số 2-5 chữ số
    short_numbers = re.findall(r'\b(\d{2,5})\b', message)
    for num in short_numbers:
        clean_num = num.lstrip('0')
        if clean_num and clean_num in PRODUCTS_BY_NUMBER:
            ms = PRODUCTS_BY_NUMBER[clean_num]
            print(f"[EXTRACT MS FROM POST] Tìm thấy số ngắn {num} -> {ms}")
            return ms
    
    print(f"[EXTRACT MS FROM POST] Không tìm thấy MS trong bài viết")
    return None

# ============================================
# HÀM XỬ LÝ COMMENT TỪ FEED (HOÀN CHỈNH - ĐÃ CẢI THIỆN)
# ============================================

def handle_feed_comment(change_data: dict):
    """
    Xử lý comment từ feed với logic:
    1. Lấy post_id từ comment
    2. Lấy nội dung bài viết gốc
    3. Trích xuất MS từ caption
    4. Cập nhật context cho user
    """
    try:
        # 1. Lấy thông tin cơ bản
        from_user = change_data.get("from", {})
        user_id = from_user.get("id")
        user_name = from_user.get("name", "")
        message_text = change_data.get("message", "")
        post_id = change_data.get("post_id", "")
        
        if not user_id or not post_id:
            print(f"[FEED COMMENT] Thiếu user_id hoặc post_id")
            return None
        
        print(f"[FEED COMMENT] User {user_id} ({user_name}) comment: '{message_text}' trên post {post_id}")
        
        # 2. Kiểm tra xem có phải comment từ page không (bỏ qua)
        if PAGE_ID and user_id == PAGE_ID:
            print(f"[FEED COMMENT] Bỏ qua comment từ chính page")
            return None
        
        # 3. Lấy nội dung bài viết gốc
        post_data = get_post_content_from_facebook(post_id)
        
        if not post_data:
            print(f"[FEED COMMENT] Không lấy được nội dung bài viết {post_id}")
            return None
        
        # LOG CHI TIẾT ĐỂ DEBUG
        post_message = post_data.get('message', '')
        print(f"[FEED COMMENT DEBUG] Nội dung bài viết ({len(post_message)} ký tự):")
        print(f"[FEED COMMENT DEBUG] {post_message[:500]}")
        
        # 4. Trích xuất MS từ caption bài viết (DÙNG HÀM ĐÃ CẢI THIỆN)
        detected_ms = extract_ms_from_post_content(post_data)
        
        if not detected_ms:
            print(f"[FEED COMMENT] Không tìm thấy MS trong bài viết {post_id}")
            # Thử tìm thủ công
            if '[MS' in post_message:
                print(f"[FEED COMMENT MANUAL] Phát hiện [MS trong bài viết, cần kiểm tra pattern")
            return None
        
        # 5. Kiểm tra MS có tồn tại trong database
        load_products()
        if detected_ms not in PRODUCTS:
            print(f"[FEED COMMENT] MS {detected_ms} không tồn tại trong database")
            return None
        
        # 6. Cập nhật context cho user (RESET COUNTER để áp dụng first message rule)
        print(f"[FEED COMMENT MS] Phát hiện MS {detected_ms} từ post {post_id} cho user {user_id}")
        
        # Lấy tên sản phẩm (loại bỏ mã nếu có trong tên)
        product = PRODUCTS[detected_ms]
        product_name = product.get('Ten', '')
        if f"[{detected_ms}]" in product_name or detected_ms in product_name:
            product_name = product_name.replace(f"[{detected_ms}]", "").replace(detected_ms, "").strip()
        
        # Gọi hàm cập nhật context mới (reset counter)
        update_context_with_new_ms(user_id, detected_ms, "feed_comment")
        
        # Lưu thêm thông tin về bài viết vào context
        ctx = USER_CONTEXT[user_id]
        ctx["source_post_id"] = post_id
        ctx["source_post_content"] = post_data.get('message', '')[:300]
        ctx["source_post_url"] = post_data.get('permalink_url', '')
        
        # 7. Gửi tin nhắn tự động cho user (tùy chọn)
        # Chỉ gửi nếu user chưa nhắn tin trước đó
        if ctx.get("real_message_count", 0) == 0:
            try:
                # Gửi tin nhắn giới thiệu sản phẩm
                intro_message = f"""Chào {user_name}! 👋 

Em thấy bạn đã bình luận trên bài viết của shop.

📦 **{product_name}**
📌 Mã sản phẩm: {detected_ms}

Để em tư vấn chi tiết về sản phẩm này, bạn vui lòng:
• Gửi "giá bao nhiêu" để xem giá
• Gửi "xem ảnh" để xem hình ảnh thực tế  
• Gửi "đặt hàng" để mua sản phẩm

Hoặc hỏi bất kỳ thông tin gì bạn cần ạ! 😊"""
                
                send_message(user_id, intro_message)
                print(f"[FEED COMMENT AUTO REPLY] Đã gửi tin nhắn tự động cho user {user_id}")
                
                # Tăng counter để không gửi lại lần nữa
                ctx["real_message_count"] = 1
                
            except Exception as e:
                print(f"[FEED COMMENT AUTO REPLY ERROR] Lỗi gửi tin nhắn: {e}")
        
        return detected_ms
        
    except Exception as e:
        print(f"[FEED COMMENT ERROR] Lỗi xử lý comment: {e}")
        import traceback
        traceback.print_exc()
        return None

# ============================================
# HELPER: SEND MESSAGE
# ============================================

def call_facebook_send_api(payload: dict, retry_count=2):
    if not PAGE_ACCESS_TOKEN:
        print("[WARN] PAGE_ACCESS_TOKEN chưa được cấu hình")
        return {}
    
    url = f"https://graph.facebook.com/v12.0/me/messages?access_token={PAGE_ACCESS_TOKEN}"
    
    for attempt in range(retry_count):
        try:
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code == 200:
                return resp.json()
            else:
                if attempt < retry_count - 1:
                    time.sleep(0.5)
        except Exception as e:
            if attempt < retry_count - 1:
                time.sleep(0.5)
    
    return {}

def send_message(recipient_id: str, text: str):
    if not text:
        return
    if len(text) > 2000:
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

def send_image_safe(recipient_id: str, image_url: str, timeout: int = 3):
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
    
    try:
        resp = requests.post(
            f"https://graph.facebook.com/v12.0/me/messages?access_token={PAGE_ACCESS_TOKEN}",
            json=payload,
            timeout=timeout
        )
        if resp.status_code == 200:
            return resp.json()
        else:
            return {}
    except requests.exceptions.Timeout:
        print(f"⏰ Timeout khi gửi ảnh: {image_url[:50]}...")
        return {}
    except Exception as e:
        print(f"Lỗi khi gửi ảnh: {str(e)}")
        return {}

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
        p = re.sub(r'^[\'"\s]+|[\'"\s]+$', '', p)
        
        if re.match(r'^https?://', p) or any(domain in p.lower() for domain in [
            'alicdn.com', 'taobao', '1688.com', '.jpg', '.jpeg', 
            '.png', '.webp', '.gif', 'image', 'img', 'photo'
        ]):
            urls.append(p)
    
    seen = set()
    result = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            result.append(u)
    
    return result

def extract_price_int(price_str: str):
    if not price_str:
        return None
    
    # Loại bỏ ký tự không phải số, dấu chấm, dấu phẩy
    cleaned = re.sub(r'[^\d.,]', '', str(price_str))
    
    if not cleaned:
        return None
    
    # Xử lý các định dạng giá phổ biến
    # 1. Định dạng Việt Nam: 1.000.000
    if cleaned.count('.') > 1 and cleaned.count(',') <= 1:
        # Giả sử dấu chấm là phân cách nghìn, dấu phẩy là thập phân
        cleaned = cleaned.replace('.', '')
        if ',' in cleaned:
            cleaned = cleaned.replace(',', '.')
    
    # 2. Định dạng quốc tế: 1,000,000.00
    elif cleaned.count(',') > 1 and cleaned.count('.') <= 1:
        # Giả sử dấu phẩy là phân cách nghìn, dấu chấm là thập phân
        cleaned = cleaned.replace(',', '')
    
    # 3. Định dạng hỗn hợp
    else:
        # Giữ lại số cuối cùng trước dấu phẩy hoặc chấm
        cleaned = cleaned.replace(',', '').replace('.', '')
    
    try:
        # Lấy phần nguyên nếu có dấu thập phân
        if '.' in cleaned:
            cleaned = cleaned.split('.')[0]
        
        return int(float(cleaned)) if cleaned else None
    except Exception:
        return None

def load_products(force=False):
    global PRODUCTS, LAST_LOAD, PRODUCTS_BY_NUMBER
    now = time.time()
    if not force and PRODUCTS and (now - LAST_LOAD) < LOAD_TTL:
        return

    if not GOOGLE_SHEET_CSV_URL:
        print("❌ GOOGLE_SHEET_CSV_URL chưa được cấu hình!")
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
            thuoc_tinh = (row.get("Thuộc tính") or "").strip()

            gia_int = extract_price_int(gia_raw)
            try:
                tonkho_int = int(str(tonkho_raw)) if str(tonkho_raw).strip() else None
            except Exception:
                tonkho_int = None

            variant_images = parse_image_urls(images)
            variant_image = variant_images[0] if variant_images else ""

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
                    "Thuộc tính": thuoc_tinh,
                    "FullRow": row,
                }
                base["variants"] = []
                base["all_colors"] = set()
                base["all_sizes"] = set()
                products[ms] = base

            p = products[ms]

            variant = {
                "mau": mau,
                "size": size,
                "gia": gia_int,
                "gia_raw": gia_raw,
                "tonkho": tonkho_int if tonkho_int is not None else tonkho_raw,
                "images": images,
                "variant_image": variant_image,
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
            
            if ms.startswith("MS"):
                num_part = ms[2:]
                num_without_leading_zeros = num_part.lstrip('0')
                if num_without_leading_zeros:
                    products_by_number[num_without_leading_zeros] = ms

        PRODUCTS = products
        PRODUCTS_BY_NUMBER = products_by_number
        LAST_LOAD = now
        
        total_variants = sum(len(p['variants']) for p in products.values())
        
        print(f"📦 Loaded {len(PRODUCTS)} products với {total_variants} variants.")
        print(f"🔢 Created mapping for {len(PRODUCTS_BY_NUMBER)} product numbers")
                
    except Exception as e:
        print("❌ load_products ERROR:", e)

def get_variant_image(ms: str, color: str, size: str) -> str:
    if ms not in PRODUCTS:
        return ""
    
    product = PRODUCTS[ms]
    variants = product.get("variants", [])
    
    for variant in variants:
        variant_color = variant.get("mau", "").strip().lower()
        variant_size = variant.get("size", "").strip().lower()
        
        input_color = color.strip().lower()
        input_size = size.strip().lower()
        
        color_match = (not input_color) or (variant_color == input_color) or (input_color == "mặc định" and not variant_color)
        size_match = (not input_size) or (variant_size == input_size) or (input_size == "mặc định" and not variant_size)
        
        if color_match and size_match:
            variant_image = variant.get("variant_image", "")
            if variant_image:
                return variant_image
    
    images_field = product.get("Images", "")
    urls = parse_image_urls(images_field)
    return urls[0] if urls else ""

# ============================================
# HÀM PHÂN TÍCH GIÁ THÔNG MINH
# ============================================

def analyze_product_price_patterns(ms: str) -> dict:
    """
    Phân tích mẫu giá của sản phẩm và trả về cấu trúc dữ liệu rõ ràng
    """
    if ms not in PRODUCTS:
        return {"error": "Product not found"}
    
    product = PRODUCTS[ms]
    variants = product.get("variants", [])
    
    price_by_color = {}
    price_by_size = {}
    price_groups = {}
    
    # 1. Phân tích theo màu
    for variant in variants:
        color = variant.get("mau", "Mặc định").strip()
        size = variant.get("size", "Mặc định").strip()
        price = variant.get("gia", 0)
        
        if price:
            # Nhóm theo màu
            if color not in price_by_color:
                price_by_color[color] = {"price": price, "sizes": set()}
            price_by_color[color]["sizes"].add(size)
            
            # Nhóm theo size
            if size not in price_by_size:
                price_by_size[size] = {"price": price, "colors": set()}
            price_by_size[size]["colors"].add(color)
            
            # Nhóm theo mức giá
            if price not in price_groups:
                price_groups[price] = []
            price_groups[price].append({"color": color, "size": size})
    
    # 2. Kiểm tra xem giá có thay đổi theo màu không
    color_based = True
    for color, data in price_by_color.items():
        if len(data["sizes"]) > 1 and any(v.get("mau", "").strip() == color and v.get("gia", 0) != data["price"] for v in variants):
            color_based = False
            break
    
    # 3. Kiểm tra xem giá có thay đổi theo size không
    size_based = True
    for size, data in price_by_size.items():
        if len(data["colors"]) > 1 and any(v.get("size", "").strip() == size and v.get("gia", 0) != data["price"] for v in variants):
            size_based = False
            break
    
    # 4. Phân tích mẫu giá phức tạp
    complex_pattern = not (color_based or size_based)
    
    # Tạo cấu trúc trả về
    result = {
        "ms": ms,
        "product_name": product.get("Ten", ""),
        "total_variants": len(variants),
        "price_pattern": "unknown",
        "base_price": extract_price_int(product.get("Gia", "")) or 0,
        "detailed_analysis": {}
    }
    
    if color_based and price_by_color:
        result["price_pattern"] = "color_based"
        result["detailed_analysis"] = {
            "type": "color_based",
            "prices": []
        }
        for color, data in sorted(price_by_color.items()):
            result["detailed_analysis"]["prices"].append({
                "color": color,
                "price": data["price"],
                "applicable_sizes": f"Tất cả size ({', '.join(sorted(data['sizes']))})" if data["sizes"] else "Tất cả size"
            })
    
    elif size_based and price_by_size:
        result["price_pattern"] = "size_based"
        result["detailed_analysis"] = {
            "type": "size_based",
            "prices": []
        }
        for size, data in sorted(price_by_size.items()):
            result["detailed_analysis"]["prices"].append({
                "size": size,
                "price": data["price"],
                "applicable_colors": f"Tất cả màu ({', '.join(sorted(data['colors']))})" if data["colors"] else "Tất cả màu"
            })
    
    elif complex_pattern and price_groups:
        result["price_pattern"] = "complex_based"
        result["detailed_analysis"] = {
            "type": "complex_based",
            "price_groups": []
        }
        for price, items in sorted(price_groups.items()):
            if len(items) <= 5:
                variants_list = [f"{item['color']}/{item['size']}" for item in items]
                display_text = ", ".join(variants_list)
            else:
                variants_list = [f"{item['color']}/{item['size']}" for item in items[:3]]
                display_text = f"{', '.join(variants_list)} và {len(items) - 3} phân loại khác"
            
            result["detailed_analysis"]["price_groups"].append({
                "price": price,
                "count": len(items),
                "variants": display_text,
                "all_variants": items[:10]
            })
    else:
        result["price_pattern"] = "single_price"
        result["detailed_analysis"] = {
            "type": "single_price",
            "price": result["base_price"]
        }
    
    return result

def get_product_data_for_gpt(ms: str) -> dict:
    """Lấy dữ liệu sản phẩm dưới dạng dictionary đơn giản cho GPT"""
    if ms not in PRODUCTS:
        return None
    
    product = PRODUCTS[ms]
    
    images_field = product.get("Images", "")
    image_urls = parse_image_urls(images_field)
    unique_images = list(dict.fromkeys(image_urls))[:10]
    
    videos_field = product.get("Videos", "")
    video_urls = parse_image_urls(videos_field)
    
    return {
        "ms": ms,
        "ten": product.get("Ten", ""),
        "mo_ta": product.get("MoTa", ""),
        "gia": product.get("Gia", ""),
        "gia_int": extract_price_int(product.get("Gia", "")),
        "mau_sac": product.get("màu (Thuộc tính)", ""),
        "size": product.get("size (Thuộc tính)", ""),
        "thuoc_tinh": product.get("Thuộc tính", ""),
        "ton_kho": product.get("Tồn kho", ""),
        "images": unique_images,
        "videos": video_urls,
        "variants": product.get("variants", [])[:5],
        "all_colors": list(product.get("all_colors", set())),
        "all_sizes": list(product.get("all_sizes", set()))
    }

# ============================================
# GPT FUNCTION CALLING TOOLS
# ============================================

def get_tools_definition():
    return [
        {
            "type": "function",
            "function": {
                "name": "get_product_price_details",
                "description": "Lấy thông tin giá chi tiết của sản phẩm, bao gồm các biến thể giá theo màu, size hoặc kết hợp",
                "parameters": {
                    "type": "object",
                    "properties": {"ms": {"type": "string", "description": "Mã sản phẩm MSxxxxxx"}},
                    "required": ["ms"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "get_product_basic_info",
                "description": "Lấy thông tin cơ bản của sản phẩm (tên, mô tả, màu sắc, size, thuộc tính, tồn kho)",
                "parameters": {
                    "type": "object",
                    "properties": {"ms": {"type": "string", "description": "Mã sản phẩm MSxxxxxx"}},
                    "required": ["ms"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "send_product_images",
                "description": "Gửi ảnh sản phẩm cho khách xem (tối đa 3 ảnh)",
                "parameters": {
                    "type": "object",
                    "properties": {"ms": {"type": "string", "description": "Mã sản phẩm"}},
                    "required": ["ms"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "send_product_videos",
                "description": "Gửi link video sản phẩm",
                "parameters": {
                    "type": "object",
                    "properties": {"ms": {"type": "string", "description": "Mã sản phẩm"}},
                    "required": ["ms"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "provide_order_link",
                "description": "Cung cấp link đặt hàng khi khách muốn mua",
                "parameters": {
                    "type": "object",
                    "properties": {"ms": {"type": "string", "description": "Mã sản phẩm"}},
                    "required": ["ms"]
                }
            }
        }
    ]

def execute_tool(uid, name, args):
    ctx = USER_CONTEXT[uid]
    ms = args.get("ms", "").upper() or ctx.get("last_ms")
    domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
    
    if name == "get_product_price_details":
        price_analysis = analyze_product_price_patterns(ms)
        
        return json.dumps({
            "status": "success",
            "analysis": price_analysis,
            "formatted_instructions": """GPT HÃY DÙNG DỮ LIỆU NÀY ĐỂ TRẢ LỜI VỀ GIÁ:
            
            1. Nếu price_pattern là 'color_based':
               - Liệt kê từng màu và giá
               - Ví dụ: "Dạ, giá bán là:\nĐỏ: 250.000đ\nXanh: 290.000đ\nTrắng: 315.000đ\nÁp dụng cho tất cả các size ạ!"
               
            2. Nếu price_pattern là 'size_based':
               - Liệt kê từng size và giá
               - Ví dụ: "Dạ, giá bán là:\nM: 250.000đ\nL: 290.000đ\nXL: 315.000đ\nÁp dụng cho tất cả các màu ạ!"
               
            3. Nếu price_pattern là 'complex_based':
               - Nhóm theo từng mức giá
               - Ví dụ: "Em gửi anh chị bảng giá ạ:\n250.000đ (đỏ/M, xanh/L, trắng/L)\n290.000đ (đen/M, tím/2XL, đỏ/XL)\n315.000đ (trắng/2XL, xanh/XL, nâu/S)"
               
            4. Nếu price_pattern là 'single_price':
               - Chỉ cần trả lời một giá duy nhất
               - Ví dụ: "Dạ, giá sản phẩm là 250.000đ ạ!"
               
            LUÔN KẾT THÚC BẰNG: 'Anh/chị cần em tư vấn thêm gì không ạ?'"""
        }, ensure_ascii=False)
    
    elif name == "get_product_basic_info":
        product_data = get_product_data_for_gpt(ms)
        
        return json.dumps({
            "status": "success",
            "data": product_data,
            "instructions": "GPT HÃY DÙNG DỮ LIỆU NÀY ĐỂ TRẢ LỜI VỀ: tên, mô tả, chất liệu, màu sắc, size, thuộc tính, tồn kho (trừ giá). Nếu không có thông tin, nói: 'Dạ, phần này trong hệ thống chưa có thông tin ạ'"
        }, ensure_ascii=False)
    
    elif name == "send_product_images":
        if ms not in PRODUCTS:
            return "Sản phẩm không có ảnh."
        
        product = PRODUCTS[ms]
        images_field = product.get("Images", "")
        urls = parse_image_urls(images_field)
        
        if not urls:
            return "Sản phẩm không có ảnh."
        
        seen = set()
        sent_count = 0
        for url in urls:
            if url not in seen and sent_count < 3:
                send_image_safe(uid, url, timeout=3)
                seen.add(url)
                sent_count += 1
                time.sleep(0.5)
        
        return f"Đã gửi {sent_count} ảnh sản phẩm."
    
    elif name == "send_product_videos":
        if ms not in PRODUCTS:
            return "Sản phẩm không có video."
        
        product = PRODUCTS[ms]
        videos_field = product.get("Videos", "")
        urls = parse_image_urls(videos_field)
        
        if not urls:
            return "Sản phẩm không có video."
        
        for url in urls[:2]:
            send_message(uid, f"📹 Video sản phẩm: {url}")
            time.sleep(0.5)
        
        return "Đã gửi link video."
    
    elif name == "provide_order_link":
        if ms in PRODUCTS:
            link = f"{domain}/order-form?ms={ms}&uid={uid}"
            return json.dumps({
                "order_link": link,
                "ms": ms,
                "product_name": PRODUCTS[ms].get('Ten', '')
            }, ensure_ascii=False)
        return "Không tìm thấy sản phẩm."
    
    return "Tool không xác định."

# ============================================
# CẢI THIỆN NGỮ CẢNH
# ============================================

def update_product_context(uid: str, ms: str):
    ctx = USER_CONTEXT[uid]
    ctx["last_ms"] = ms
    
    if "product_history" not in ctx:
        ctx["product_history"] = []
    
    if not ctx["product_history"] or ctx["product_history"][0] != ms:
        if ms in ctx["product_history"]:
            ctx["product_history"].remove(ms)
        ctx["product_history"].insert(0, ms)
    
    if len(ctx["product_history"]) > 5:
        ctx["product_history"] = ctx["product_history"][:5]
    
    print(f"[CONTEXT UPDATE] User {uid}: last_ms={ms}, history={ctx['product_history']}")

def detect_ms_from_text(text: str) -> Optional[str]:
    """Phát hiện mã sản phẩm từ nhiều dạng text khác nhau - CHỈ khi có tiền tố"""
    if not text: 
        return None
    
    print(f"[DETECT MS DEBUG] Input text: {text}")
    
    # Chuẩn hóa text: lowercase, xóa dấu, xóa khoảng trắng thừa
    text_norm = normalize_vietnamese(text.lower().strip())
    
    # Danh sách các tiền tố cần tìm - CHỈ lấy khi có các tiền tố này
    prefixes = [
        # Dạng chuẩn & đầy đủ
        r'ms', r'mã', r'mã số', r'mã sản phẩm', r'sản phẩm', r'sản phẩm số',
        r'sp',  # Dạng viết tắt
        # Dạng không dấu
        r'ma', r'ma so', r'ma san pham', r'san pham', r'san pham so',
        # Dạng sai chính tả
        r'mã sp', r'ma sp', r'mã s\.phẩm', r'ma san pham so', 
        r'mã sp số', r'ma so sp',
        # Dạng tự nhiên khi khách hỏi (cần có từ khóa)
        r'xem mã', r'xem sp', r'xem sản phẩm', r'cho xem mã', 
        r'tư vấn mã', r'tư vấn sp', r'giới thiệu mã', r'giới thiệu sp'
    ]
    
    # Tạo pattern regex tổng hợp
    # Format: (tiền tố) + (tùy chọn khoảng trắng) + (số 1-6 chữ số, có thể có số 0 ở đầu)
    pattern_str = r'(?:' + '|'.join(prefixes) + r')\s*(\d{1,6})'
    
    # Tìm kiếm với regex
    match = re.search(pattern_str, text_norm)
    
    if match:
        num = match.group(1)
        clean_n = num.lstrip("0")
        
        if clean_n and clean_n in PRODUCTS_BY_NUMBER:
            found_ms = PRODUCTS_BY_NUMBER[clean_n]
            print(f"[DETECT MS DEBUG] Tìm thấy qua tiền tố + số {num}: {found_ms}")
            return found_ms
    
    # THÊM: Tìm MS dạng viết liền hoàn toàn (MSxxxxxx, msxxxxxx, spxxxxxx)
    # Pattern: (MS|ms|sp) + (1-6 chữ số)
    direct_pattern = r'\b(ms|sp|ms|sp)(\d{1,6})\b'
    direct_match = re.search(direct_pattern, text_norm, re.IGNORECASE)
    
    if direct_match:
        num = direct_match.group(2)
        clean_n = num.lstrip("0")
        
        if clean_n and clean_n in PRODUCTS_BY_NUMBER:
            found_ms = PRODUCTS_BY_NUMBER[clean_n]
            print(f"[DETECT MS DEBUG] Tìm thấy dạng viết liền: {found_ms}")
            return found_ms
    
    print(f"[DETECT MS DEBUG] Không tìm thấy MS trong text (chỉ tìm với tiền tố): {text}")
    return None

# ============================================
# GPT FUNCTION CALLING HANDLER
# ============================================

def handle_text_with_function_calling(uid: str, text: str):
    """GPT function calling LUÔN dựa vào last_ms từ context"""
    load_products()
    ctx = USER_CONTEXT[uid]
    
    # ƯU TIÊN 1: Lấy MS từ context (echo Fchat, ad_title, catalog...)
    current_ms = ctx.get("last_ms")
    
    # ƯU TIÊN 2: Nếu phát hiện MS từ text (có tiền tố) thì cập nhật, bất kể có current_ms hay không
    detected_ms = detect_ms_from_text(text)
    if detected_ms and detected_ms in PRODUCTS:
        # Nếu MS mới khác MS cũ, hoặc chưa có MS, thì cập nhật
        if detected_ms != current_ms:
            current_ms = detected_ms
            # SỬ DỤNG HÀM MỚI ĐỂ CẬP NHẬT MS VÀ RESET COUNTER
            update_context_with_new_ms(uid, current_ms, "text_detection")
            print(f"[MS DETECTED] Phát hiện MS từ tin nhắn hiện tại: {current_ms}")
    
    # ƯU TIÊN 3: Nếu vẫn không có, kiểm tra xem tin nhắn có chứa số không
    if not current_ms or current_ms not in PRODUCTS:
        # Tìm bất kỳ số nào trong tin nhắn (1-6 chữ số) với TIỀN TỐ
        text_norm = normalize_vietnamese(text.lower())
        numbers = re.findall(r'\b(?:ms|mã|sp|ma|san pham)\s*(\d{1,6})\b', text_norm, re.IGNORECASE)
        for num in numbers:
            clean_num = num.lstrip('0')
            if clean_num and clean_num in PRODUCTS_BY_NUMBER:
                current_ms = PRODUCTS_BY_NUMBER[clean_num]
                ctx["last_ms"] = current_ms
                # Gọi hàm cập nhật context
                if "product_history" not in ctx:
                    ctx["product_history"] = []
                
                if not ctx["product_history"] or ctx["product_history"][0] != current_ms:
                    if current_ms in ctx["product_history"]:
                        ctx["product_history"].remove(current_ms)
                    ctx["product_history"].insert(0, current_ms)
                
                if len(ctx["product_history"]) > 5:
                    ctx["product_history"] = ctx["product_history"][:5]
                
                print(f"[MS FALLBACK] Tìm thấy MS từ tiền tố + số: {current_ms}")
                break
    
    # ƯU TIÊN 4: Nếu vẫn không có, hỏi lại khách
    if not current_ms or current_ms not in PRODUCTS:
        send_message(uid, "Dạ em chưa biết anh/chị đang hỏi về sản phẩm nào. Vui lòng cho em biết mã sản phẩm (ví dụ: MS000012) ạ!")
        return
    
    fanpage_name = get_fanpage_name_from_api()
    
    system_prompt = f"""Bạn là nhân viên bán hàng của {fanpage_name}.

**SẢN PHẨM ĐANG ĐƯỢC HỎI: {current_ms}**

**QUY TẮC QUAN TRỌNG VỀ MÃ SẢN PHẨM:**
1. CHỈ TRẢ LỜI VỀ SẢN PHẨM HIỆN TẠI: {current_ms}
2. KHÔNG BAO GIỜ được nhắc đến mã sản phẩm khác trong câu trả lời
3. Nếu cần thông tin, chỉ dùng tool với ms={current_ms}
4. Nếu user hỏi về sản phẩm khác, yêu cầu họ cung cấp mã sản phẩm

**QUY TẮC TRẢ LỜI VỀ CHÍNH SÁCH (KHÔNG DÙNG TOOL RIÊNG):**
1. Khi khách hỏi về: vận chuyển, bảo quản, hướng dẫn sử dụng, đổi trả, khuyến mãi, bảo hành, chất liệu, thời gian giao hàng, chính sách đổi trả
   - LUÔN dùng tool 'get_product_basic_info' để lấy MÔ TẢ SẢN PHẨM
   - TỰ ĐỌC và PHÂN TÍCH mô tả để tìm thông tin liên quan
   - Trả lời như một nhân viên thật: tự nhiên, thân thiện, dựa trên thông tin có sẵn

2. Nếu trong mô tả CÓ thông tin liên quan:
   - Trích xuất thông tin chính xác từ mô tả
   - Diễn đạt lại theo cách tự nhiên, dễ hiểu
   - Giữ nguyên ý nghĩa nhưng làm cho câu trả lời thân thiện
   - Ví dụ: "Dạ, theo thông tin sản phẩm thì [trích dẫn thông tin từ mô tả] ạ!"

3. Nếu trong mô tả KHÔNG có thông tin:
   - Trả lời: "Dạ, phần này trong hệ thống chưa có thông tin ạ. Anh/chị vui lòng liên hệ shop để được hỗ trợ chi tiết ạ!"
   - KHÔNG bịa thông tin, KHÔNG đoán mò

**QUY TẮC TRẢ LỜI VỀ GIÁ:**
1. Khi khách hỏi về giá - LUÔN dùng tool 'get_product_price_details'
2. Phân tích kết quả từ tool và trả lời theo định dạng:
   - Giá theo màu: Liệt kê từng màu và giá
   - Giá theo size: Liệt kê từng size và giá
   - Giá phức tạp: Nhóm theo từng mức giá, liệt kê các màu/size trong mỗi nhóm
   - Giá duy nhất: Trả lời một giá duy nhất
3. LUÔN hỏi khách cần tư vấn thêm gì không sau khi trả lời về giá.

**QUY TẮC CHUNG:**
- Xưng "em", gọi "anh/chị"
- Ngắn gọn, thân thiện (1-3 dòng)
- Nếu không có thông tin: "Dạ, phần này trong hệ thống chưa có thông tin ạ"
- Về tồn kho: LUÔN báo "CÒN HÀNG ạ!" nếu khách hỏi

**TOOLS SẴN CÓ:**
1. get_product_price_details - Cho câu hỏi về giá
2. get_product_basic_info - Cho CẢ: thông tin sản phẩm VÀ các câu hỏi về chính sách (đọc mô tả)
3. send_product_images - Cho câu hỏi "xem ảnh"
4. provide_order_link - Cho câu hỏi "đặt hàng", "mua hàng"
5. send_product_videos - Cho câu hỏi "xem video"

**KHI KHÁCH HỎI:**
- "giá bao nhiêu", "bao nhiêu tiền" - get_product_price_details
- "chất liệu gì", "làm bằng gì" - get_product_basic_info (đọc mô tả để tìm thông tin)
- "có những màu nào" - get_product_basic_info (sau đó liệt kê màu từ data)
- "size nào có" - get_product_basic_info (liệt kê size từ data)
- "xem ảnh", "gửi ảnh" - send_product_images
- "có video không" - send_product_videos
- "đặt hàng", "mua hàng" - provide_order_link
- "miễn ship chứ?", "ship bao nhiêu?", "thời gian giao hàng", "có free ship không" - get_product_basic_info (đọc mô tả tìm thông tin ship)
- "bảo quản thế nào?", "giặt như thế nào?", "cách bảo quản" - get_product_basic_info (đọc mô tả tìm hướng dẫn bảo quản)
- "hướng dẫn sử dụng", "cách dùng", "sử dụng thế nào" - get_product_basic_info (đọc mô tả tìm hướng dẫn sử dụng)
- "có giảm giá không?", "chính sách đổi trả", "bảo hành thế nào" - get_product_basic_info (đọc mô tả tìm thông tin chính sách)

**VÍ DỤ XỬ LÝ CHÍNH SÁCH:**
- Khách hỏi: "Có miễn ship không?"
  - Gọi get_product_basic_info
  - Đọc mô tả, tìm thông tin về "ship", "vận chuyển", "miễn phí"
  - Nếu có: "Dạ, theo thông tin sản phẩm thì [trích dẫn thông tin] ạ!"
  - Nếu không: "Dạ, phần này trong hệ thống chưa có thông tin ạ..."

- Khách hỏi: "Bảo quản thế nào?"
  - Gọi get_product_basic_info
  - Tìm hướng dẫn bảo quản trong mô tả
  - Trả lời tự nhiên: "Dạ, sản phẩm này nên [thông tin từ mô tả] ạ!"
"""
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": text}
            ],
            tools=get_tools_definition(),
            tool_choice="auto",
            temperature=0.1
        )
        
        msg = response.choices[0].message
        
        if msg.tool_calls:
            for tool in msg.tool_calls:
                tool_name = tool.function.name
                tool_args = json.loads(tool.function.arguments)
                
                if "ms" not in tool_args:
                    tool_args["ms"] = current_ms
                
                tool_result = execute_tool(uid, tool_name, tool_args)
                
                follow_up_response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": text},
                        msg,
                        {"role": "tool", "tool_call_id": tool.id, "name": tool_name, "content": tool_result}
                    ],
                    temperature=0.1
                )
                
                final_reply = follow_up_response.choices[0].message.content
                send_message(uid, final_reply)
                
                # Lưu lịch sử hội thoại
                ctx["conversation_history"].append({"role": "user", "content": text})
                ctx["conversation_history"].append({"role": "assistant", "content": final_reply})
                ctx["conversation_history"] = ctx["conversation_history"][-10:]
        else:
            send_message(uid, msg.content)
            ctx["conversation_history"].append({"role": "user", "content": text})
            ctx["conversation_history"].append({"role": "assistant", "content": msg.content})
            ctx["conversation_history"] = ctx["conversation_history"][-10:]
            
    except Exception as e:
        print(f"GPT Error: {e}")
        send_message(uid, "Dạ em đang gặp chút trục trặc, anh/chị vui lòng thử lại sau ạ.")

# ============================================
# GỬI CAROUSEL 1 SẢN PHẨM
# ============================================

def send_single_product_carousel(uid: str, ms: str):
    """
    Gửi carousel chỉ với 1 sản phẩm duy nhất
    Sử dụng khi bot đã nhận diện được MS từ ad_title, catalog, Fchat
    """
    if ms not in PRODUCTS:
        print(f"[SINGLE CAROUSEL ERROR] Sản phẩm {ms} không tồn tại")
        return
    
    load_products()
    product = PRODUCTS[ms]
    
    images_field = product.get("Images", "")
    urls = parse_image_urls(images_field)
    image_url = urls[0] if urls else ""
    
    gia_raw = product.get("Gia", "")
    gia_int = extract_price_int(gia_raw) or 0
    
    # LẤY TÊN SẢN PHẨM (KHÔNG BAO GỒM MÃ SẢN PHẨM)
    product_name = product.get('Ten', '')
    
    # KIỂM TRA NẾU TÊN ĐÃ CHỨA MÃ SẢN PHẨM, CHỈ GIỮ TÊN
    if f"[{ms}]" in product_name or ms in product_name:
        # Xóa mã sản phẩm khỏi tên
        product_name = product_name.replace(f"[{ms}]", "").replace(ms, "").strip()
    
    element = {
        "title": product_name,  # CHỈ HIỂN THỊ TÊN SẢN PHẨM
        "image_url": image_url,
        "subtitle": f"💰 Giá: {gia_int:,.0f} đ",
        "buttons": [
            {
                "type": "postback",
                "title": "🌟 Ưu điểm SP",
                "payload": f"PRODUCT_HIGHLIGHTS_{ms}"
            },
            {
                "type": "postback", 
                "title": "🖼️ Xem ảnh",
                "payload": f"VIEW_IMAGES_{ms}"
            },
            {
                "type": "web_url",
                "url": f"{DOMAIN}/order-form?ms={ms}&uid={uid}",
                "title": "🛒 Đặt ngay"
            }
        ]
    }
    
    send_carousel_template(uid, [element])
    
    ctx = USER_CONTEXT[uid]
    ctx["last_ms"] = ms
    
    # Gọi hàm update_product_context cũ để duy trì tính năng cũ
    if "product_history" not in ctx:
        ctx["product_history"] = []
    
    if not ctx["product_history"] or ctx["product_history"][0] != ms:
        if ms in ctx["product_history"]:
            ctx["product_history"].remove(ms)
        ctx["product_history"].insert(0, ms)
    
    if len(ctx["product_history"]) > 5:
        ctx["product_history"] = ctx["product_history"][:5]
    
    ctx["has_sent_first_carousel"] = True
    
    print(f"✅ [SINGLE CAROUSEL] Đã gửi carousel 1 sản phẩm {ms} cho user {uid}")

# ============================================
# HANDLE ORDER FORM STATE
# ============================================

def reset_order_state(uid: str):
    ctx = USER_CONTEXT[uid]
    ctx["order_state"] = None
    ctx["order_data"] = {}

def handle_order_form_step(uid: str, text: str) -> bool:
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
        send_message(uid, "Dạ vâng. Anh/chị cho em xin địa chỉ nhận hàng ạ?")
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
# HANDLE POSTBACK THÔNG MINH
# ============================================

def handle_postback_with_recovery(uid: str, payload: str, postback_id: str = None):
    """
    Xử lý postback - FIX LỖI GỬI LẶP VÔ HẠN
    CHỈ XỬ LÝ 1 LẦN DUY NHẤT CHO MỖI POSTBACK_ID
    """
    now = time.time()
    
    if postback_id:
        idempotency_key = f"{uid}_{postback_id}"
    else:
        idempotency_key = f"{uid}_{payload}_{int(now)}"
    
    ctx = USER_CONTEXT[uid]
    
    if "idempotent_postbacks" not in ctx:
        ctx["idempotent_postbacks"] = {}
    
    if idempotency_key in ctx["idempotent_postbacks"]:
        processed_time = ctx["idempotent_postbacks"][idempotency_key]
        if now - processed_time < 300:
            print(f"[IDEMPOTENCY BLOCK] Bỏ qua postback đã xử lý: {idempotency_key}")
            return True
    
    ctx["idempotent_postbacks"][idempotency_key] = now
    
    if len(ctx["idempotent_postbacks"]) > 50:
        sorted_items = sorted(ctx["idempotent_postbacks"].items(), 
                            key=lambda x: x[1], reverse=True)[:30]
        ctx["idempotent_postbacks"] = dict(sorted_items)
    
    load_products()
    
    # Xử lý các loại postback
    if payload.startswith("PRODUCT_HIGHLIGHTS_"):
        ms = payload.replace("PRODUCT_HIGHLIGHTS_", "")
        if ms in PRODUCTS:
            ctx["last_ms"] = ms
            # Gọi hàm update_product_context cũ
            if "product_history" not in ctx:
                ctx["product_history"] = []
            
            if not ctx["product_history"] or ctx["product_history"][0] != ms:
                if ms in ctx["product_history"]:
                    ctx["product_history"].remove(ms)
                ctx["product_history"].insert(0, ms)
            
            if len(ctx["product_history"]) > 5:
                ctx["product_history"] = ctx["product_history"][:5]
            
            # Lấy thông tin sản phẩm
            product = PRODUCTS[ms]
            mo_ta = product.get("MoTa", "")
            ten_sp = product.get("Ten", "")
            
            if not mo_ta:
                send_message(uid, f"Dạ sản phẩm [{ms}] {ten_sp} chưa có mô tả chi tiết ạ. Anh/chị có thể hỏi về giá, màu sắc, size hoặc đặt hàng ạ!")
                return True
            
            if not client:
                send_message(uid, "Dạ chức năng này tạm thời chưa khả dụng ạ. Anh/chị vui lòng thử lại sau!")
                return True
            
            # Gọi GPT để tóm tắt 5 ưu điểm
            try:
                system_prompt = """Bạn là một trợ lý bán hàng chuyên nghiệp. 
Hãy đọc kỹ mô tả sản phẩm và liệt kê 5 ưu điểm nổi bật nhất của sản phẩm đó. 
Mỗi ưu điểm phải:
1. Ngắn gọn, rõ ràng (1-2 dòng)
2. Bắt đầu bằng dấu gạch đầu dòng (-)
3. Tập trung vào lợi ích cho khách hàng
4. Chỉ trả lời bằng tiếng Việt
5. Không thêm bất kỳ lời giải thích nào khác

Định dạng đầu ra:
- [Ưu điểm 1]
- [Ưu điểm 2]
- [Ưu điểm 3]
- [Ưu điểm 4]
- [Ưu điểm 5]"""
                
                # Giới hạn độ dài của mô tả
                max_length = 3000
                if len(mo_ta) > max_length:
                    mo_ta = mo_ta[:max_length] + "..."
                
                user_prompt = f"""Sản phẩm: {ten_sp}
Mã sản phẩm: {ms}

Mô tả sản phẩm:
{mo_ta}

Hãy liệt kê 5 ưu điểm nổi bật nhất của sản phẩm này theo định dạng yêu cầu."""

                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    temperature=0.3,
                    max_tokens=500
                )
                
                highlights = response.choices[0].message.content
                
                # Đảm bảo định dạng đúng
                if not highlights.startswith("-"):
                    # Thêm dấu gạch đầu dòng nếu GPT quên
                    lines = highlights.strip().split('\n')
                    formatted_lines = []
                    for line in lines:
                        line = line.strip()
                        if line and not line.startswith('-'):
                            formatted_lines.append(f"- {line}")
                        else:
                            formatted_lines.append(line)
                    highlights = '\n'.join(formatted_lines)
                
                # Gửi cho khách hàng với tiêu đề
                message = f"🌟 **5 ƯU ĐIỂM NỔI BẬT CỦA SẢN PHẨM [{ms}]** 🌟\n\n{highlights}\n\n---\nAnh/chị cần em tư vấn thêm gì không ạ?"
                send_message(uid, message)
                
            except Exception as e:
                print(f"Lỗi khi gọi GPT cho ưu điểm sản phẩm: {e}")
                send_message(uid, "Dạ em chưa thể tóm tắt ưu điểm sản phẩm ngay lúc này. Anh/chị có thể xem mô tả chi tiết hoặc hỏi về thông tin khác ạ!")
            
            return True
            
    elif payload.startswith("VIEW_IMAGES_"):
        ms = payload.replace("VIEW_IMAGES_", "")
        if ms in PRODUCTS:
            ctx["last_ms"] = ms
            # Gọi hàm update_product_context cũ
            if "product_history" not in ctx:
                ctx["product_history"] = []
            
            if not ctx["product_history"] or ctx["product_history"][0] != ms:
                if ms in ctx["product_history"]:
                    ctx["product_history"].remove(ms)
                ctx["product_history"].insert(0, ms)
            
            if len(ctx["product_history"]) > 5:
                ctx["product_history"] = ctx["product_history"][:5]
            
            # Gọi GPT để xử lý việc gửi ảnh
            handle_text_with_function_calling(uid, "gửi ảnh sản phẩm cho tôi xem")
            return True
    
    elif payload in ["PRICE_QUERY", "COLOR_QUERY", "SIZE_QUERY", "MATERIAL_QUERY", "STOCK_QUERY"]:
        ms = ctx.get("last_ms")
        
        if ms and ms in PRODUCTS:
            question_map = {
                "PRICE_QUERY": "giá bao nhiêu",
                "COLOR_QUERY": "có những màu gì",
                "SIZE_QUERY": "có size nào",
                "MATERIAL_QUERY": "chất liệu gì",
                "STOCK_QUERY": "còn hàng không"
            }
            
            question = question_map.get(payload, "thông tin sản phẩm")
            handle_text_with_function_calling(uid, question)
            return True
    
    elif payload == "GET_STARTED":
        welcome_msg = f"""Chào anh/chị! 👋 
Em là trợ lý AI của {get_fanpage_name_from_api()}.

Vui lòng gửi mã sản phẩm (ví dụ: MS123456) hoặc mô tả sản phẩm."""
        send_message(uid, welcome_msg)
        return True
    
    return False

# ============================================
# HANDLE TEXT MESSAGES
# ============================================

def handle_text(uid: str, text: str):
    """Xử lý tin nhắn văn bản với logic: chưa gửi carousel → carousel, đã gửi → GPT"""
    if not text or len(text.strip()) == 0:
        return
    
    ctx = USER_CONTEXT[uid]

    if ctx.get("processing_lock"):
        print(f"[TEXT SKIP] User {uid} đang được xử lý")
        return

    ctx["processing_lock"] = True

    try:
        now = time.time()
        last_msg_time = ctx.get("last_msg_time", 0)
        
        # Debounce: kiểm tra tin nhắn trùng lặp
        if now - last_msg_time < 2:
            last_text = ctx.get("last_processed_text", "")
            if text.strip().lower() == last_text.lower():
                print(f"[TEXT DEBOUNCE] Bỏ qua tin nhắn trùng lặp: {text[:50]}...")
                ctx["processing_lock"] = False
                return
        
        ctx["last_msg_time"] = now
        ctx["last_processed_text"] = text.strip().lower()
        
        load_products()
        
        # Tăng counter cho tin nhắn
        if "real_message_count" not in ctx:
            ctx["real_message_count"] = 0
        ctx["real_message_count"] += 1
        message_count = ctx["real_message_count"]
        
        print(f"[MESSAGE COUNT] User {uid}: tin nhắn thứ {message_count}")
        
        # QUY TẮC QUAN TRỌNG:
        # 1. Nếu chưa gửi carousel cho sản phẩm hiện tại (has_sent_first_carousel = False): Gửi carousel, KHÔNG GPT
        # 2. Nếu đã gửi carousel rồi: LUÔN dùng GPT Function Calling
        last_ms = ctx.get("last_ms")
        
        if not ctx.get("has_sent_first_carousel") and last_ms and last_ms in PRODUCTS:
            print(f"🚨 [FIRST CAROUSEL FOR PRODUCT] Chưa gửi carousel cho sản phẩm {last_ms}")
            print(f"🚨 [FIRST CAROUSEL RULE] BỎ QUA nội dung '{text[:50]}...', gửi carousel cho {last_ms}")
            
            # GỬI CAROUSEL CHO SẢN PHẨM ĐÃ ĐƯỢC XÁC ĐỊNH
            send_single_product_carousel(uid, last_ms)
            
            # KHÔNG XỬ LÝ TIN NHẮN NÀY BẰNG GPT
            ctx["processing_lock"] = False
            return
        
        # TỪ TIN NHẮN THỨ 2 TRỞ ĐI: LUÔN DÙNG GPT FUNCTION CALLING
        print(f"✅ [GPT REQUIRED] Tin nhắn thứ {message_count} từ user {uid}, BẮT BUỘC dùng GPT")
        
        # Xử lý order state nếu có
        if handle_order_form_step(uid, text):
            ctx["processing_lock"] = False
            return
        
        # Gọi GPT function calling
        handle_text_with_function_calling(uid, text)

    except Exception as e:
        print(f"Error in handle_text for {uid}: {e}")
        try:
            send_message(uid, "Dạ em đang gặp chút trục trặc, anh/chị vui lòng thử lại sau ạ.")
        except:
            pass
    finally:
        ctx["processing_lock"] = False

# ============================================
# HANDLE IMAGE - CẢI TIẾN VỚI CAROUSEL GỢI Ý
# ============================================

def handle_image(uid: str, image_url: str):
    """Xử lý ảnh sản phẩm với công nghệ AI thông minh và carousel gợi ý"""
    ctx = USER_CONTEXT[uid]
    
    now = time.time()
    last_image_time = ctx.get("last_image_time", 0)
    if now - last_image_time < 3:
        print(f"[IMAGE DEBOUNCE] Bỏ qua ảnh mới, chưa đủ thời gian")
        return
    
    ctx["last_image_time"] = now
    
    # BƯỚC 1: Kiểm tra xem có phải emoji/sticker không
    if is_emoji_or_sticker_image(image_url):
        print(f"[EMOJI DETECTED] Bỏ qua ảnh emoji/sticker: {image_url[:100]}")
        send_message(uid, "😊 Em đã nhận được biểu tượng cảm xúc của anh/chị! Nếu anh/chị muốn xem sản phẩm, vui lòng gửi ảnh thật của sản phẩm hoặc mã sản phẩm ạ!")
        return
    
    # BƯỚC 1.5: Kiểm tra ảnh có hợp lệ không
    if not is_valid_product_image(image_url):
        print(f"[INVALID IMAGE] Ảnh không hợp lệ: {image_url[:100]}")
        send_message(uid, "❌ Ảnh này không rõ hoặc không phải ảnh sản phẩm. Vui lòng gửi ảnh rõ hơn hoặc mã sản phẩm ạ!")
        return
    
    # BƯỚC 2: Thông báo đang xử lý ảnh
    send_message(uid, "🔍 Em đang phân tích ảnh sản phẩm bằng AI, vui lòng đợi một chút ạ...")
    
    # BƯỚC 3: Tìm sản phẩm bằng OpenAI Vision API
    found_ms = find_product_by_image(image_url)
    
    # BƯỚC 4: Xử lý kết quả
    if found_ms:
        print(f"[IMAGE PRODUCT FOUND] Tìm thấy sản phẩm {found_ms} từ ảnh")
        
        # SỬ DỤNG HÀM MỚI ĐỂ CẬP NHẬT MS VÀ RESET COUNTER
        update_context_with_new_ms(uid, found_ms, "image_search")
        
        # Gửi thông báo tìm thấy
        # LẤY TÊN SẢN PHẨM (KHÔNG BAO GỒM MÃ SẢN PHẨM)
        product_name = PRODUCTS[found_ms].get("Ten", "")
        if f"[{found_ms}]" in product_name or found_ms in product_name:
            product_name = product_name.replace(f"[{found_ms}]", "").replace(found_ms, "").strip()
        
        send_message(uid, f"✅ Em đã tìm thấy sản phẩm phù hợp với ảnh!\n\n📦 **{product_name}**")
        
        # Gửi carousel sản phẩm đã tìm thấy
        send_single_product_carousel(uid, found_ms)
        
        # Gửi quick reply để hỏi thêm thông tin
        quick_replies = [
            {
                "content_type": "text",
                "title": "💰 Giá bao nhiêu?",
                "payload": f"PRICE_{found_ms}"
            },
            {
                "content_type": "text",
                "title": "🎨 Màu gì có?",
                "payload": f"COLOR_{found_ms}"
            },
            {
                "content_type": "text",
                "title": "📏 Size nào?",
                "payload": f"SIZE_{found_ms}"
            }
        ]
        
        send_quick_replies(uid, "Anh/chị muốn hỏi thêm thông tin gì về sản phẩm này ạ?", quick_replies)
        
    else:
        print(f"[IMAGE PRODUCT NOT FOUND] Không tìm thấy sản phẩm từ ảnh")
        
        # Gửi thông báo không tìm thấy
        send_message(uid, "❌ Em chưa tìm thấy sản phẩm phù hợp với ảnh này. Có thể anh/chị chụp ảnh chưa rõ hoặc sản phẩm chưa có trong hệ thống.")
        
        # Gợi ý một số sản phẩm bằng CAROUSEL thay vì text
        send_message(uid, "Dưới đây là một số sản phẩm gợi ý cho anh/chị ạ:")
        
        # Gửi carousel gợi ý 3 sản phẩm
        carousel_sent = send_suggestion_carousel(uid, 3)
        
        # Nếu không gửi được carousel, gửi text backup
        if not carousel_sent:
            # Gợi ý một số sản phẩm phổ biến
            popular_products = list(PRODUCTS.keys())[:3]
            if popular_products:
                for ms in popular_products:
                    product = PRODUCTS[ms]
                    # Lấy tên sản phẩm (không bao gồm mã sản phẩm)
                    product_name = product.get('Ten', '')
                    if f"[{ms}]" in product_name or ms in product_name:
                        product_name = product_name.replace(f"[{ms}]", "").replace(ms, "").strip()
                    send_message(uid, f"📦 {product_name}")
        
        send_message(uid, "Vui lòng gửi mã sản phẩm chính xác (ví dụ: MS000004) để em tư vấn chi tiết ạ!")

# ============================================
# GOOGLE SHEETS API FUNCTIONS
# ============================================

def get_google_sheets_service():
    if not GOOGLE_SHEETS_CREDENTIALS_JSON or not GOOGLE_SHEET_ID:
        return None

    try:
        import google.auth
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        
        creds_dict = json.loads(GOOGLE_SHEETS_CREDENTIALS_JSON)
        credentials = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        service = build('sheets', 'v4', credentials=credentials)
        print("✅ Đã khởi tạo Google Sheets service thành công.")
        return service
    except ImportError:
        print("⚠️ Google API libraries chưa được cài đặt.")
        return None
    except Exception as e:
        print(f"❌ Lỗi khi khởi tạo Google Sheets service: {e}")
        return None

def write_order_to_google_sheet_api(order_data: dict):
    service = get_google_sheets_service()
    if service is None:
        return False
    
    sheet_name = "Orders"
    
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        order_id = f"ORD{int(time.time())}_{order_data.get('uid', '')[-4:]}"
        
        new_row = [
            timestamp, order_id, "Mới",
            order_data.get("ms", ""), order_data.get("product_name", ""),
            order_data.get("color", ""), order_data.get("size", ""),
            order_data.get("quantity", 1), order_data.get("unit_price", 0),
            order_data.get("total_price", 0), order_data.get("customer_name", ""),
            order_data.get("phone", ""), order_data.get("address", ""),
            order_data.get("province", ""), order_data.get("district", ""),
            order_data.get("ward", ""), order_data.get("address_detail", ""),
            "COD", "ViettelPost",
            f"Đơn từ Facebook Bot ({order_data.get('referral_source', 'direct')})",
            order_data.get("uid", ""), order_data.get("referral_source", "direct")
        ]
        
        request = service.spreadsheets().values().append(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=f"{sheet_name}!A:V",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [new_row]}
        )
        
        response = request.execute()
        print(f"✅ ĐÃ GHI ĐƠN HÀNG VÀO GOOGLE SHEET THÀNH CÔNG!")
        return True
        
    except Exception as e:
        print(f"❌ Lỗi Google Sheets API: {e}")
        return False

def save_order_to_local_csv(order_data: dict):
    try:
        file_path = "orders_backup.csv"
        file_exists = os.path.exists(file_path)
        
        timestamp = datetime.now().strftime("%Y-%m-d %H:%M:%S")
        order_id = f"ORD{int(time.time())}_{order_data.get('uid', '')[-4:]}"
        
        row_data = {
            "timestamp": timestamp,
            "order_id": order_id,
            "status": "Mới",
            "product_code": order_data.get("ms", ""),
            "product_name": order_data.get("product_name", ""),
            "color": order_data.get("color", ""),
            "size": order_data.get("size", ""),
            "quantity": order_data.get("quantity", 1),
            "unit_price": order_data.get("unit_price", 0),
            "total_price": order_data.get("total_price", 0),
            "customer_name": order_data.get("customer_name", ""),
            "phone": order_data.get("phone", ""),
            "address": order_data.get("address", ""),
            "province": order_data.get("province", ""),
            "district": order_data.get("district", ""),
            "ward": order_data.get("ward", ""),
            "address_detail": order_data.get("address_detail", ""),
            "payment_method": "COD",
            "shipping_method": "ViettelPost",
            "notes": f"Đơn từ Facebook Bot ({order_data.get('referral_source', 'direct')})",
            "fb_user_id": order_data.get("uid", ""),
            "referral_source": order_data.get("referral_source", "direct")
        }
        
        with open(file_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=row_data.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(row_data)
        
        print(f"📁 Đã lưu đơn hàng vào file local backup: {order_id}")
    except Exception as e:
        print(f"❌ Lỗi khi lưu file local backup: {str(e)}")

# ============================================
# POSCAKE WEBHOOK INTEGRATION (PHẦN MỚI)
# ============================================

def send_order_status_message(recipient_id: str, order_data: dict):
    """
    Gửi tin nhắn thông báo trạng thái đơn hàng từ Poscake
    """
    try:
        order_id = order_data.get("order_id", "")
        order_code = order_data.get("order_code", "")
        status = order_data.get("status", "")
        total_amount = order_data.get("total_amount", 0)
        items = order_data.get("items", [])
        
        # Tạo nội dung tin nhắn dựa trên trạng thái
        status_messages = {
            "pending": "📦 ĐƠN HÀNG MỚI",
            "processing": "⚡ ĐANG XỬ LÝ",
            "shipped": "🚚 ĐÃ GIAO HÀNG",
            "delivered": "✅ ĐÃ NHẬN HÀNG",
            "cancelled": "❌ ĐÃ HỦY"
        }
        
        status_text = status_messages.get(status, "📦 CẬP NHẬT ĐƠN HÀNG")
        
        # Xây dựng nội dung tin nhắn
        message = f"""🎊 {status_text}
────────────────
📋 Mã đơn hàng: {order_code}
💰 Tổng tiền: {total_amount:,.0f} đ
📅 Thời gian: {order_data.get('created_at', '')}
────────────────"""

        if items:
            message += "\n📦 Sản phẩm:\n"
            for i, item in enumerate(items[:5], 1):  # Giới hạn 5 sản phẩm
                product_name = item.get("product_name", "")
                quantity = item.get("quantity", 1)
                price = item.get("price", 0)
                message += f"{i}. {product_name} x{quantity} - {price:,.0f} đ\n"
        
        # Thêm thông báo theo trạng thái
        if status == "pending":
            message += "\n⏰ Shop sẽ liên hệ xác nhận trong 5-10 phút."
        elif status == "processing":
            message += "\n🔧 Đơn hàng đang được chuẩn bị."
        elif status == "shipped":
            shipping_info = order_data.get("shipping_info", {})
            tracking_code = shipping_info.get("tracking_code", "")
            carrier = shipping_info.get("carrier", "")
            if tracking_code:
                message += f"\n📮 Mã vận đơn: {tracking_code}"
            if carrier:
                message += f"\n🚚 Đơn vị vận chuyển: {carrier}"
        elif status == "delivered":
            message += "\n✅ Cảm ơn bạn đã mua hàng!"
        elif status == "cancelled":
            message += "\n📞 Liên hệ shop để được hỗ trợ."

        message += "\n────────────────\n💬 Cần hỗ trợ thêm? Gửi tin nhắn cho em ạ! ❤️"

        send_message(recipient_id, message)
        
        # Nếu có tracking code, gửi thêm nút theo dõi đơn hàng
        if status == "shipped":
            tracking_code = order_data.get("shipping_info", {}).get("tracking_code")
            if tracking_code:
                quick_replies = [
                    {
                        "content_type": "text",
                        "title": "📍 Theo dõi đơn hàng",
                        "payload": f"TRACK_ORDER_{tracking_code}"
                    },
                    {
                        "content_type": "text",
                        "title": "📞 Hỗ trợ",
                        "payload": "SUPPORT_ORDER"
                    }
                ]
                send_quick_replies(recipient_id, "Bấm để theo dõi đơn hàng:", quick_replies)
        
        print(f"[POSCAKE NOTIFY] Đã gửi thông báo đơn hàng {order_code} cho user {recipient_id}")
        return True
        
    except Exception as e:
        print(f"[POSCAKE NOTIFY ERROR] Lỗi gửi tin nhắn đơn hàng: {e}")
        return False

def handle_poscake_order_event(event_type: str, data: dict):
    """Xử lý sự kiện đơn hàng từ Poscake"""
    order_data = data.get('data', data.get('order', {}))
    
    print(f"[POSCAKE ORDER] {event_type}: {order_data.get('code', 'No code')}")
    
    # Log chi tiết để debug
    print(f"[POSCAKE ORDER DETAILS] {json.dumps(order_data, ensure_ascii=False)[:300]}")
    
    # Tìm recipient_id từ thông tin khách hàng
    customer = order_data.get('customer', {})
    phone = customer.get('phone', '')
    email = customer.get('email', '')
    
    recipient_id = None
    
    # Tìm user_id từ số điện thoại trong context
    for uid, ctx in USER_CONTEXT.items():
        # Kiểm tra order_data hoặc số điện thoại trong context
        user_phone = ctx.get("order_data", {}).get("phone", "")
        if user_phone and user_phone == phone:
            recipient_id = uid
            break
    
    # Nếu không tìm thấy, thử tìm bằng email
    if not recipient_id and email:
        for uid, ctx in USER_CONTEXT.items():
            user_email = ctx.get("order_data", {}).get("email", "")
            if user_email and user_email == email:
                recipient_id = uid
                break
    
    if recipient_id:
        # Chuẩn bị dữ liệu đơn hàng
        order_info = {
            "order_id": order_data.get('id', ''),
            "order_code": order_data.get('code', ''),
            "status": event_type.replace('order.', ''),
            "total_amount": order_data.get('total', 0),
            "items": order_data.get('items', []),
            "customer": customer,
            "created_at": order_data.get('created_at', ''),
            "updated_at": order_data.get('updated_at', ''),
            "shipping_info": order_data.get('shipping', {})
        }
        
        # Gửi tin nhắn thông báo
        send_order_status_message(recipient_id, order_info)
        
        # Lưu thông tin đơn hàng vào context
        if recipient_id in USER_CONTEXT:
            if "poscake_orders" not in USER_CONTEXT[recipient_id]:
                USER_CONTEXT[recipient_id]["poscake_orders"] = []
            
            # Kiểm tra xem đơn hàng đã tồn tại chưa
            existing_order = next(
                (o for o in USER_CONTEXT[recipient_id]["poscake_orders"] 
                 if o.get("order_id") == order_info["order_id"]), None
            )
            
            if not existing_order:
                USER_CONTEXT[recipient_id]["poscake_orders"].append(order_info)
                # Giữ tối đa 10 đơn hàng gần nhất
                if len(USER_CONTEXT[recipient_id]["poscake_orders"]) > 10:
                    USER_CONTEXT[recipient_id]["poscake_orders"] = USER_CONTEXT[recipient_id]["poscake_orders"][-10:]
            else:
                # Cập nhật trạng thái đơn hàng hiện có
                existing_order.update(order_info)
        
        return jsonify({
            "status": "success",
            "event": event_type,
            "order_code": order_data.get('code'),
            "message_sent": True,
            "recipient_id": recipient_id
        }), 200
    else:
        print(f"[POSCAKE ORDER] Không tìm thấy recipient_id cho đơn hàng {order_data.get('code')}")
        return jsonify({
            "status": "no_recipient",
            "event": event_type,
            "order_code": order_data.get('code'),
            "message": "Không tìm thấy user tương ứng"
        }), 200

# ============================================
# CACHE ADDRESS API (CẢI TIẾN MỚI)
# ============================================

ADDRESS_CACHE = {
    'provinces': None,
    'provinces_updated': 0,
    'districts': {},
    'wards': {}
}

@app.route("/api/cached-provinces", methods=["GET"])
def cached_provinces():
    """Cache API tỉnh/thành để tăng tốc độ load form"""
    now = time.time()
    cache_ttl = 3600  # 1 giờ
    
    if (ADDRESS_CACHE['provinces'] and 
        (now - ADDRESS_CACHE['provinces_updated']) < cache_ttl):
        return jsonify(ADDRESS_CACHE['provinces'])
    
    try:
        response = requests.get('https://provinces.open-api.vn/api/p/', timeout=5)
        if response.status_code == 200:
            ADDRESS_CACHE['provinces'] = response.json()
            ADDRESS_CACHE['provinces_updated'] = now
            return jsonify(ADDRESS_CACHE['provinces'])
    except Exception as e:
        print(f"[ADDRESS API ERROR] Lỗi khi gọi API tỉnh/thành: {e}")
    
    return jsonify([])

@app.route("/poscake-webhook", methods=["POST"])
def poscake_webhook():
    """
    Webhook nhận thông báo từ Poscake
    Poscake sẽ gửi các sự kiện: đơn hàng, sản phẩm, tồn kho
    """
    try:
        # Log headers để debug
        headers = {k.lower(): v for k, v in request.headers.items()}
        print(f"[POSCAKE WEBHOOK] Headers nhận được: {headers}")
        
        # Lấy signature để xác thực
        signature = headers.get('x-poscake-signature') or headers.get('x-signature')
        
        # Xác thực webhook nếu có secret
        if POSCAKE_WEBHOOK_SECRET and signature:
            # Tính toán và so sánh signature
            payload = request.get_data(as_text=True)
            expected_signature = hashlib.sha256(
                f"{payload}{POSCAKE_WEBHOOK_SECRET}".encode()
            ).hexdigest()
            
            if signature != expected_signature:
                print(f"[POSCAKE WEBHOOK] Invalid signature")
                return jsonify({"error": "Invalid signature"}), 401
        
        # Parse JSON data
        data = request.get_json()
        if not data:
            print("[POSCAKE WEBHOOK] No JSON data received")
            return jsonify({"error": "No data"}), 400
        
        print(f"[POSCAKE WEBHOOK] Data received: {json.dumps(data, ensure_ascii=False)[:500]}")
        
        # Xác định loại sự kiện
        event_type = data.get('event')
        
        # Xử lý theo loại sự kiện
        if event_type and 'order' in event_type:
            return handle_poscake_order_event(event_type, data)
        elif event_type and 'product' in event_type:
            # Xử lý sản phẩm (có thể cập nhật PRODUCTS)
            print(f"[POSCAKE PRODUCT] Event: {event_type}")
            return jsonify({"status": "received", "event": event_type}), 200
        elif event_type and 'inventory' in event_type:
            # Xử lý tồn kho
            print(f"[POSCAKE INVENTORY] Event: {event_type}")
            return jsonify({"status": "received", "event": event_type}), 200
        else:
            print(f"[POSCAKE WEBHOOK] Unknown event type: {event_type}")
            return jsonify({"status": "ignored", "event": event_type}), 200
            
    except Exception as e:
        print(f"[POSCAKE WEBHOOK ERROR] {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "Internal server error"}), 500

# ============================================
# TEST WEBHOOK ENDPOINT
# ============================================

@app.route("/test-poscake-webhook", methods=["GET", "POST"])
def test_poscake_webhook():
    """Endpoint để test webhook từ Poscake"""
    if request.method == "GET":
        return jsonify({
            "status": "ready",
            "message": "Poscake Webhook endpoint is ready",
            "endpoint": "/poscake-webhook",
            "instructions": "Configure webhook on Poscake to point to this URL"
        })
    
    # Xử lý POST request (test data)
    data = request.get_json() or {}
    
    print(f"[TEST WEBHOOK] Received data: {json.dumps(data, indent=2)}")
    
    # Log headers
    headers = dict(request.headers)
    print(f"[TEST WEBHOOK] Headers: {json.dumps(headers, indent=2)}")
    
    return jsonify({
        "status": "received",
        "message": "Test webhook received successfully",
        "data_received": data,
        "headers_received": headers,
        "timestamp": datetime.now().isoformat()
    }), 200

# ============================================
# TEST FEED COMMENT ENDPOINT
# ============================================

@app.route("/test-feed-comment", methods=["GET"])
def test_feed_comment():
    """Test endpoint cho feed comment processing"""
    post_id = request.args.get("post_id", "516937221685203_1775049683320893")
    
    # Test hàm get_post_content_from_facebook
    post_data = get_post_content_from_facebook(post_id)
    
    if not post_data:
        return jsonify({
            "status": "error",
            "message": "Không lấy được nội dung bài viết",
            "post_id": post_id
        }), 400
    
    # Test hàm extract_ms_from_post_content
    detected_ms = extract_ms_from_post_content(post_data)
    
    # Test context update
    test_user_id = "test_user_123"
    if detected_ms:
        update_context_with_new_ms(test_user_id, detected_ms, "test_feed_comment")
    
    return jsonify({
        "status": "success",
        "post_id": post_id,
        "post_content_preview": post_data.get('message', '')[:200] + "..." if post_data.get('message') else "No message",
        "detected_ms": detected_ms,
        "ms_exists": detected_ms in PRODUCTS if detected_ms else False,
        "context_updated": detected_ms is not None,
        "test_user_context": USER_CONTEXT.get(test_user_id, {})
    })

# ============================================
# API MỚI: Lấy thông tin biến thể (ảnh, giá)
# ============================================

@app.route("/api/get-variant-info")
def api_get_variant_info():
    ms = (request.args.get("ms") or "").upper()
    color = request.args.get("color", "").strip()
    size = request.args.get("size", "").strip()
    
    load_products()
    if ms not in PRODUCTS:
        return {"error": "not_found"}, 404
    
    product = PRODUCTS[ms]
    
    # Tìm biến thể phù hợp
    target_variant = None
    for variant in product.get("variants", []):
        variant_color = variant.get("mau", "").strip().lower()
        variant_size = variant.get("size", "").strip().lower()
        
        input_color = color.strip().lower()
        input_size = size.strip().lower()
        
        color_match = (not input_color) or (variant_color == input_color) or (input_color == "mặc định" and not variant_color)
        size_match = (not input_size) or (variant_size == input_size) or (input_size == "mặc định" and not variant_size)
        
        if color_match and size_match:
            target_variant = variant
            break
    
    # Nếu không tìm thấy biến thể, dùng thông tin chung
    if target_variant:
        variant_image = target_variant.get("variant_image", "")
        variant_price = target_variant.get("gia", 0)
        variant_price_raw = target_variant.get("gia_raw", "")
    else:
        variant_image = ""
        variant_price = extract_price_int(product.get("Gia", "")) or 0
        variant_price_raw = product.get("Gia", "")
    
    # Nếu không có ảnh biến thể, lấy ảnh đầu tiên của sản phẩm
    if not variant_image:
        images_field = product.get("Images", "")
        urls = parse_image_urls(images_field)
        variant_image = urls[0] if urls else ""
    
    return {
        "ms": ms,
        "color": color,
        "size": size,
        "image": variant_image,
        "price": variant_price,
        "price_raw": variant_price_raw,
        "found_variant": target_variant is not None
    }

# ============================================
# WEBHOOK HANDLER (ĐÃ SỬA ĐỂ XÓA LOGIC FCHAT ECHO)
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
        # Xử lý feed changes (comment trên bài viết)
        if "changes" in e:
            changes = e.get("changes", [])
            for change in changes:
                value = change.get("value", {})
                field = change.get("field")
                
                if field == "feed":
                    print(f"[FEED EVENT] Nhận sự kiện feed")
                    
                    # Kiểm tra xem có phải comment không (có message và post_id)
                    if "message" in value and "post_id" in value:
                        print(f"[FEED COMMENT] Đang xử lý comment từ feed...")
                        
                        # Gọi hàm xử lý comment (ĐÃ CẢI THIỆN)
                        handle_feed_comment(value)
                    
                    continue
        
        messaging = e.get("messaging", [])
        for m in messaging:
            sender_id = m.get("sender", {}).get("id")
            if not sender_id:
                continue
            
            # Bỏ qua delivery/read events sớm
            if m.get("delivery") or m.get("read"):
                continue

            # Xử lý attachment template từ catalog
            if "message" in m and "attachments" in m["message"]:
                attachments = m["message"]["attachments"]
                for att in attachments:
                    if att.get("type") == "template":
                        payload = att.get("payload", {})
                        if "product" in payload:
                            product = payload["product"]
                            elements = product.get("elements", [])
                            if elements and len(elements) > 0:
                                element = elements[0]
                                retailer_id = element.get("retailer_id")
                                
                                if retailer_id:
                                    ctx = USER_CONTEXT[sender_id]
                                    ctx["last_retailer_id"] = retailer_id
                                    ctx["catalog_view_time"] = time.time()
                                    
                                    ms_from_retailer = extract_ms_from_retailer_id(retailer_id)
                                    if ms_from_retailer:
                                        # SỬ DỤNG HÀM MỚI ĐỂ CẬP NHẬT MS VÀ RESET COUNTER
                                        update_context_with_new_ms(sender_id, ms_from_retailer, "catalog")
                                        print(f"[CATALOG] Đã cập nhật MS mới từ catalog: {ms_from_retailer}")

            # XỬ LÝ ECHO MESSAGE - CHỈ BỎ QUA ECHO TỪ BOT, KHÔNG XỬ LÝ FCHAT
            if m.get("message", {}).get("is_echo"):
                recipient_id = m.get("recipient", {}).get("id")
                if not recipient_id:
                    continue
                
                msg = m["message"]
                echo_text = msg.get("text", "")
                app_id = msg.get("app_id", "")
                
                # CHỈ KIỂM TRA NẾU LÀ BOT GENERATED ECHO - KHÔNG XỬ LÝ FCHAT
                if is_bot_generated_echo(echo_text, app_id):
                    print(f"[ECHO BOT] Bỏ qua echo message từ bot: {echo_text[:50]}...")
                else:
                    # Echo từ người dùng (comment) - đã xử lý qua feed, bỏ qua
                    print(f"[ECHO USER] Bỏ qua echo từ người dùng (đã xử lý qua feed): {echo_text[:50]}...")
                continue
            
            # Xử lý sự kiện ORDER từ Facebook Shop - ĐÃ SỬA: KHÔNG GỬI TIN NHẮN
            if "order" in m:
                order_info = m.get("order", {})
                products = order_info.get("products", [])
                
                print(f"[FACEBOOK SHOP ORDER] Đơn hàng mới từ user {sender_id}: {json.dumps(order_info, ensure_ascii=False)[:500]}")
                
                # Trích xuất thông tin đơn hàng
                order_items = []
                total_amount = 0
                
                for product in products:
                    retailer_id = product.get("retailer_id", "")
                    product_name = product.get("name", "")
                    unit_price = product.get("unit_price", 0)
                    quantity = product.get("quantity", 1)
                    currency = product.get("currency", "VND")
                    
                    # Trích xuất mã sản phẩm từ retailer_id
                    ms = extract_ms_from_retailer_id(retailer_id) or "UNKNOWN"
                    
                    item_total = unit_price * quantity
                    total_amount += item_total
                    
                    order_items.append({
                        "ms": ms,
                        "name": product_name,
                        "unit_price": unit_price,
                        "quantity": quantity,
                        "item_total": item_total,
                        "retailer_id": retailer_id
                    })
                
                # KHÔNG GỬI TIN NHẮN CHO ĐƠN HÀNG TỰ FACEBOOK SHOP
                # Chỉ cập nhật context và ghi log
                
                # Cập nhật context với mã sản phẩm đầu tiên (nếu có) và RESET COUNTER
                if order_items and order_items[0]["ms"] != "UNKNOWN":
                    new_ms = order_items[0]["ms"]
                    
                    # SỬ DỤNG HÀM MỚI ĐỂ CẬP NHẬT MS VÀ RESET COUNTER
                    update_context_with_new_ms(sender_id, new_ms, "facebook_shop_order")
                    
                    print(f"[FACEBOOK SHOP ORDER] Đã cập nhật MS mới {new_ms} từ đơn hàng Facebook Shop")
                
                # Ghi log đơn hàng vào hệ thống
                try:
                    order_log = {
                        "user_id": sender_id,
                        "timestamp": datetime.now().isoformat(),
                        "order_data": order_info,
                        "items": order_items,
                        "total_amount": total_amount,
                        "source": "facebook_shop"
                    }
                    
                    # Lưu vào file log
                    with open("facebook_shop_orders.log", "a", encoding="utf-8") as f:
                        f.write(json.dumps(order_log, ensure_ascii=False) + "\n")
                    
                    print(f"[FACEBOOK SHOP ORDER LOG] Đã ghi log đơn hàng từ user {sender_id}")
                except Exception as e:
                    print(f"[FACEBOOK SHOP ORDER ERROR] Lỗi khi ghi log: {e}")
                
                continue  # Đã xử lý xong sự kiện order
            
            # Xử lý referral
            if m.get("referral"):
                ref = m["referral"]
                ctx = USER_CONTEXT[sender_id]
                ctx["referral_source"] = ref.get("source", "unknown")
                referral_payload = ref.get("ref", "")
                ctx["referral_payload"] = referral_payload
                
                # Logic reset counter thông minh: chỉ reset nếu user không hoạt động trong 5 phút
                now = time.time()
                last_msg_time = ctx.get("last_msg_time", 0)
                
                if now - last_msg_time > 300:  # 5 phút không có tin nhắn
                    ctx["real_message_count"] = 0
                    print(f"[REFERRAL RESET COUNTER] Reset real_message_count cho user {sender_id} (inactive > 5m)")
                else:
                    print(f"[REFERRAL NO RESET] Giữ nguyên counter cho user {sender_id}, last_msg cách đây {int(now - last_msg_time)}s")
                
                print(f"[REFERRAL] User {sender_id} từ {ctx['referral_source']} với payload: {referral_payload}")
                
                handled = False
                
                if ref.get("source") == "ADS" and ref.get("ads_context_data"):
                    ads_data = ref.get("ads_context_data", {})
                    ad_title = ads_data.get("ad_title", "")
                    
                    print(f"[ADS REFERRAL] Ad title: {ad_title}")
                    
                    ms_from_ad = extract_ms_from_ad_title(ad_title)
                    if ms_from_ad and ms_from_ad in PRODUCTS:
                        print(f"[ADS PRODUCT] Xác định sản phẩm từ ad_title: {ms_from_ad}")
                        
                        # SỬ DỤNG HÀM MỚI ĐỂ CẬP NHẬT MS VÀ RESET COUNTER
                        update_context_with_new_ms(sender_id, ms_from_ad, "ADS")
                        
                        welcome_msg = f"""Chào anh/chị! 👋 
Em là trợ lý AI của {get_fanpage_name_from_api()}.

Em thấy anh/chị quan tâm đến sản phẩm **[{ms_from_ad}]** từ quảng cáo.
Để xem thông tin chi tiết, anh/chị vui lòng gửi tin nhắn bất kỳ ạ!"""
                        
                        send_message(sender_id, welcome_msg)
                        handled = True
                    
                    if not handled and referral_payload:
                        detected_ms = detect_ms_from_text(referral_payload)
                        if detected_ms and detected_ms in PRODUCTS:
                            print(f"[ADS REFERRAL] Nhận diện mã từ payload: {detected_ms}")
                            
                            # SỬ DỤNG HÀM MỚI ĐỂ CẬP NHẬT MS VÀ RESET COUNTER
                            update_context_with_new_ms(sender_id, detected_ms, "ADS")
                            
                            welcome_msg = f"""Chào anh/chị! 👋 
Em là trợ lý AI của {get_fanpage_name_from_api()}.

Em thấy anh/chị quan tâm đến sản phẩm **[{detected_ms}]**.
Để xem thông tin chi tiết, anh/chị vui lòng gửi tin nhắn bất kỳ ạ!"""
                            
                            send_message(sender_id, welcome_msg)
                            handled = True
                
                if handled:
                    continue
                
                if ctx.get("referral_source") != "ADS" or not ctx.get("last_ms"):
                    ctx["last_ms"] = None
                    ctx["product_history"] = []
                
                if referral_payload:
                    detected_ms = detect_ms_from_text(referral_payload)
                    
                    if detected_ms and detected_ms in PRODUCTS:
                        print(f"[REFERRAL AUTO] Nhận diện mã sản phẩm từ referral: {detected_ms}")
                        
                        # SỬ DỤNG HÀM MỚI ĐỂ CẬP NHẬT MS VÀ RESET COUNTER
                        update_context_with_new_ms(sender_id, detected_ms, "referral")
                        
                        welcome_msg = f"""Chào anh/chị! 👋 
Em là trợ lý AI của {FANPAGE_NAME}.

Em thấy anh/chị quan tâm đến sản phẩm mã [{detected_ms}].
Để xem thông tin chi tiết, anh/chị vui lòng gửi tin nhắn bất kỳ ạ!"""
                        send_message(sender_id, welcome_msg)
                        continue
                    else:
                        ctx["has_sent_first_carousel"] = False
                        welcome_msg = f"""Chào anh/chị! 👋 
Em là trợ lý AI của {FANPAGE_NAME}.

Để em tư vấn chính xác, anh/chị vui lòng:
1. Gửi mã sản phẩm (ví dụ: [MS123456])
2. Hoặc gõ "xem sản phẩm" để xem danh sách
3. Hoặc mô tả sản phẩm bạn đang tìm

Anh/chị quan tâm sản phẩm nào ạ?"""
                        send_message(sender_id, welcome_msg)
                        continue
            
            # Xử lý postback
            if "postback" in m:
                payload = m["postback"].get("payload")
                if payload:
                    postback_id = m["postback"].get("mid")
                    
                    ctx = USER_CONTEXT.get(sender_id, {})
                    last_payload = ctx.get("last_postback_payload")
                    last_payload_time = ctx.get("last_postback_time", 0)
                    
                    now = time.time()
                    if payload == last_payload and (now - last_payload_time) < 1:
                        continue
                    
                    handle_postback_with_recovery(sender_id, payload, postback_id)
                    continue
            
            # Xử lý tin nhắn thường (text & ảnh)
            if "message" in m:
                msg = m["message"]
                text = msg.get("text")
                attachments = msg.get("attachments") or []
                
                msg_mid = msg.get("mid")
                
                if msg_mid:
                    ctx = USER_CONTEXT[sender_id]
                    if "processed_message_mids" not in ctx:
                        ctx["processed_message_mids"] = {}
                    
                    if msg_mid in ctx["processed_message_mids"]:
                        processed_time = ctx["processed_message_mids"][msg_mid]
                        now = time.time()
                        if now - processed_time < 30:
                            print(f"[MSG DUPLICATE] Bỏ qua message đã xử lý: {msg_mid}")
                            continue
                    
                    last_msg_time = ctx.get("last_msg_time", 0)
                    now = time.time()
                    
                    if now - last_msg_time < 0.5:
                        print(f"[MSG DEBOUNCE] Message đến quá nhanh, bỏ qua: {msg_mid}")
                        continue
                    
                    ctx["last_msg_time"] = now
                    ctx["processed_message_mids"][msg_mid] = now
                    
                    if len(ctx["processed_message_mids"]) > 50:
                        sorted_items = sorted(ctx["processed_message_mids"].items(), key=lambda x: x[1], reverse=True)[:30]
                        ctx["processed_message_mids"] = dict(sorted_items)
                
                if text:
                    ctx = USER_CONTEXT[sender_id]
                    if ctx.get("processing_lock"):
                        print(f"[TEXT LOCKED] User {sender_id} đang được xử lý, bỏ qua text: {text[:50]}...")
                        continue
                    
                    handle_text(sender_id, text)
                elif attachments:
                    for att in attachments:
                        if att.get("type") == "image":
                            image_url = att.get("payload", {}).get("url")
                            if image_url:
                                ctx = USER_CONTEXT[sender_id]
                                if ctx.get("processing_lock"):
                                    print(f"[IMAGE LOCKED] User {sender_id} đang được xử lý, bỏ qua image")
                                    continue
                                
                                handle_image(sender_id, image_url)

    return "OK", 200

# ============================================
# ORDER FORM PAGE - CẢI TIẾN MỚI VỚI TỐI ƯU TỐC ĐỘ
# ============================================

@app.route("/order-form", methods=["GET"])
def order_form():
    ms = (request.args.get("ms") or "").upper()
    uid = request.args.get("uid") or ""
    
    # Preload products nếu chưa có
    load_products(force=False)
    
    if not ms:
        response = make_response("""
        <html>
        <body style="text-align: center; padding: 50px; font-family: Arial, sans-serif;">
            <h2 style="color: #FF3B30;">⚠️ Không tìm thấy sản phẩm</h2>
            <p>Vui lòng quay lại Messenger và chọn sản phẩm để đặt hàng.</p>
            <a href="/" style="color: #1DB954; text-decoration: none; font-weight: bold;">Quay về trang chủ</a>
        </body>
        </html>
        """)
        
        # Nén response nếu client hỗ trợ gzip
        @response.call_on_close
        def compress():
            pass
        return response, 400

    # Nếu không có sản phẩm, thử load lại
    if not PRODUCTS:
        load_products(force=True)
        
    if ms not in PRODUCTS:
        response = make_response("""
        <html>
        <body style="text-align: center; padding: 50px; font-family: Arial, sans-serif;">
            <h2 style="color: #FF3B30;">⚠️ Sản phẩm không tồn tại</h2>
            <p>Vui lòng quay lại Messenger và chọn sản phẩm khác giúp shop ạ.</p>
            <a href="/" style="color: #1DB954; text-decoration: none; font-weight: bold;">Quay về trang chủ</a>
        </body>
        </html>
        """)
        
        # Nén response
        @response.call_on_close
        def compress():
            pass
        return response, 404

    current_fanpage_name = get_fanpage_name_from_api()
    
    row = PRODUCTS[ms]
    
    images_field = row.get("Images", "")
    urls = parse_image_urls(images_field)
    default_image = urls[0] if urls else ""

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

    # Tạo HTML với form địa chỉ mới và tối ưu hóa
    html = f"""
    <!DOCTYPE html>
    <html lang="vi">
    <head>
        <meta charset="utf-8" />
        <title>Đặt hàng - {row.get('Ten','')}</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link href="https://cdnjs.cloudflare.com/ajax/libs/select2/4.1.0-rc.0/css/select2.min.css" rel="stylesheet" />
        <style>
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}
            
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
                min-height: 100vh;
                display: flex;
                justify-content: center;
                align-items: center;
                padding: 20px;
                color: #333;
            }}
            
            .container {{
                max-width: 480px;
                width: 100%;
                background: #fff;
                border-radius: 20px;
                box-shadow: 0 10px 30px rgba(0, 0, 0, 0.1);
                overflow: hidden;
            }}
            
            .header {{
                background: linear-gradient(135deg, #1DB954 0%, #17a74d 100%);
                padding: 20px;
                text-align: center;
                color: white;
            }}
            
            .header h2 {{
                font-size: 20px;
                font-weight: 600;
                margin: 0;
            }}
            
            .content {{
                padding: 20px;
            }}
            
            .product-section {{
                display: flex;
                gap: 15px;
                margin-bottom: 25px;
                padding-bottom: 20px;
                border-bottom: 1px solid #eee;
            }}
            
            .product-image-container {{
                width: 120px;
                height: 120px;
                border-radius: 12px;
                overflow: hidden;
                background: #f8f9fa;
                display: flex;
                align-items: center;
                justify-content: center;
                flex-shrink: 0;
            }}
            
            .product-image {{
                width: 100%;
                height: 100%;
                object-fit: cover;
            }}
            
            .placeholder-image {{
                width: 100%;
                height: 100%;
                display: flex;
                align-items: center;
                justify-content: center;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                font-size: 13px;
                text-align: center;
                padding: 10px;
                border-radius: 12px;
            }}
            
            .product-info {{
                flex: 1;
            }}
            
            .product-code {{
                font-size: 12px;
                color: #666;
                background: #f5f5f5;
                padding: 6px 10px;
                border-radius: 6px;
                display: inline-block;
                margin-bottom: 8px;
                font-family: 'Courier New', monospace;
                font-weight: 500;
            }}
            
            .product-title {{
                font-size: 16px;
                font-weight: 600;
                margin: 0 0 8px 0;
                line-height: 1.4;
                color: #222;
            }}
            
            .product-price {{
                color: #FF3B30;
                font-size: 18px;
                font-weight: 700;
            }}
            
            .form-group {{
                margin-bottom: 18px;
            }}
            
            .form-group label {{
                display: block;
                margin-bottom: 6px;
                font-size: 14px;
                font-weight: 500;
                color: #444;
            }}
            
            .form-control {{
                width: 100%;
                padding: 12px 15px;
                border: 2px solid #e1e5e9;
                border-radius: 10px;
                font-size: 14px;
                transition: all 0.3s ease;
                background: #fff;
            }}
            
            .select2-container .select2-selection--single {{
                height: 46px;
                border: 2px solid #e1e5e9;
                border-radius: 10px;
            }}
            
            .select2-container .select2-selection--single .select2-selection__rendered {{
                line-height: 46px;
                padding-left: 15px;
            }}
            
            .select2-container--default .select2-selection--single .select2-selection__arrow {{
                height: 46px;
            }}
            
            .select2-container--default .select2-selection--single {{
                border: 2px solid #e1e5e9;
            }}
            
            .select2-container--default.select2-container--focus .select2-selection--single {{
                border-color: #1DB954;
            }}
            
            .form-control:focus,
            .select2-container--default.select2-container--focus .select2-selection--single {{
                outline: none;
                border-color: #1DB954;
                box-shadow: 0 0 0 3px rgba(29, 185, 84, 0.1);
            }}
            
            .address-row {{
                display: flex;
                gap: 10px;
                margin-bottom: 10px;
            }}
            
            .address-col {{
                flex: 1;
            }}
            
            .total-section {{
                background: #f8f9fa;
                padding: 18px;
                border-radius: 12px;
                margin: 25px 0;
                text-align: center;
            }}
            
            .total-label {{
                font-size: 14px;
                color: #666;
                margin-bottom: 5px;
            }}
            
            .total-amount {{
                font-size: 24px;
                font-weight: 700;
                color: #FF3B30;
            }}
            
            .submit-btn {{
                width: 100%;
                padding: 16px;
                border: none;
                border-radius: 50px;
                background: linear-gradient(135deg, #1DB954 0%, #17a74d 100%);
                color: white;
                font-size: 16px;
                font-weight: 600;
                cursor: pointer;
                transition: all 0.3s ease;
                margin-top: 10px;
                display: flex;
                align-items: center;
                justify-content: center;
                gap: 10px;
            }}
            
            .submit-btn:hover {{
                transform: translateY(-2px);
                box-shadow: 0 5px 15px rgba(29, 185, 84, 0.3);
            }}
            
            .submit-btn:disabled {{
                opacity: 0.7;
                cursor: not-allowed;
                transform: none;
            }}
            
            .loading-spinner {{
                display: inline-block;
                width: 18px;
                height: 18px;
                border: 2px solid rgba(255, 255, 255, 0.3);
                border-top: 2px solid white;
                border-radius: 50%;
                animation: spin 1s linear infinite;
            }}
            
            @keyframes spin {{
                0% {{ transform: rotate(0deg); }}
                100% {{ transform: rotate(360deg); }}
            }}
            
            .note {{
                margin-top: 15px;
                font-size: 12px;
                color: #888;
                text-align: center;
                line-height: 1.5;
            }}
            
            .variant-loading {{
                text-align: center;
                padding: 10px;
                color: #666;
                font-size: 14px;
            }}
            
            @media (max-width: 480px) {{
                .container {{
                    border-radius: 15px;
                }}
                
                .content {{
                    padding: 15px;
                }}
                
                .product-section {{
                    flex-direction: column;
                    text-align: center;
                }}
                
                .product-image-container {{
                    width: 100%;
                    height: 200px;
                    margin: 0 auto 15px;
                }}
                
                .address-row {{
                    flex-direction: column;
                    gap: 10px;
                }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h2>ĐẶT HÀNG - {current_fanpage_name}</h2>
            </div>
            
            <div class="content">
                <!-- Product Info Section -->
                <div class="product-section">
                    <div class="product-image-container" id="image-container">
                        {"<img id='product-image' class='product-image lazy-load' src='https://via.placeholder.com/120x120?text=Loading...' data-src='" + default_image + "' onerror=\"this.onerror=null; this.src='https://via.placeholder.com/120x120?text=No+Image'\" />" if default_image else "<div class='placeholder-image'>Chưa có ảnh sản phẩm</div>"}
                    </div>
                    <div class="product-info">
                        <div class="product-code">Mã: {ms}</div>
                        <h3 class="product-title">{row.get('Ten','')}</h3>
                        <div class="product-price">
                            <span id="price-display">{price_int:,.0f} đ</span>
                            <div id="variant-loading" class="variant-loading" style="display: none;">
                                <small>Đang cập nhật thông tin biến thể...</small>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Order Form -->
                <form id="orderForm">
                    <!-- Color Selection -->
                    <div class="form-group">
                        <label for="color">Màu sắc:</label>
                        <select id="color" class="form-control" onchange="updateVariantInfo()">
                            {''.join(f"<option value='{c}'>{c}</option>" for c in colors)}
                        </select>
                    </div>

                    <!-- Size Selection -->
                    <div class="form-group">
                        <label for="size">Size:</label>
                        <select id="size" class="form-control" onchange="updateVariantInfo()">
                            {''.join(f"<option value='{s}'>{s}</option>" for s in sizes)}
                        </select>
                    </div>

                    <!-- Quantity -->
                    <div class="form-group">
                        <label for="quantity">Số lượng:</label>
                        <input type="number" id="quantity" class="form-control" value="1" min="1" onchange="updatePriceDisplay()">
                    </div>

                    <!-- Total Price -->
                    <div class="total-section">
                        <div class="total-label">Tạm tính:</div>
                        <div class="total-amount" id="total-display">{price_int:,.0f} đ</div>
                    </div>

                    <!-- Customer Information -->
                    <div class="form-group">
                        <label for="customerName">Họ và tên:</label>
                        <input type="text" id="customerName" class="form-control" required>
                    </div>

                    <div class="form-group">
                        <label for="phone">Số điện thoại:</label>
                        <input type="tel" id="phone" class="form-control" required>
                    </div>

                    <!-- Address Section -->
                    <div class="form-group">
                        <label for="province">Tỉnh/Thành phố:</label>
                        <select id="province" class="form-control" style="width: 100%;" required>
                            <option value="">Chọn tỉnh/thành phố</option>
                        </select>
                    </div>

                    <div class="form-group">
                        <label for="district">Quận/Huyện:</label>
                        <select id="district" class="form-control" style="width: 100%;" required disabled>
                            <option value="">Chọn quận/huyện</option>
                        </select>
                    </div>

                    <div class="form-group">
                        <label for="ward">Phường/Xã:</label>
                        <select id="ward" class="form-control" style="width: 100%;" required disabled>
                            <option value="">Chọn phường/xã</option>
                        </select>
                    </div>

                    <div class="form-group">
                        <label for="addressDetail">Địa chỉ chi tiết:</label>
                        <input type="text" id="addressDetail" class="form-control" placeholder="Số nhà, tên đường, thôn/xóm..." required>
                    </div>

                    <!-- Submit Button -->
                    <button type="button" id="submitBtn" class="submit-btn" onclick="submitOrder()">
                        ĐẶT HÀNG NGAY
                    </button>

                    <p class="note">
                        Shop sẽ gọi xác nhận trong 5-10 phút. Thanh toán khi nhận hàng (COD).
                    </p>
                </form>
            </div>
        </div>

        <!-- Sử dụng CDN nhanh hơn -->
        <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery/3.6.0/jquery.min.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/select2/4.1.0-rc.0/js/select2.min.js"></script>
        <script>
            const PRODUCT_MS = "{ms}";
            const PRODUCT_UID = "{uid}";
            let BASE_PRICE = {price_int};
            const DOMAIN = "{'https://' + DOMAIN if not DOMAIN.startswith('http') else DOMAIN}";
            const API_BASE_URL = "{('/api' if DOMAIN.startswith('http') else 'https://' + DOMAIN + '/api')}";
            
            function formatPrice(n) {{
                return n.toLocaleString('vi-VN') + ' đ';
            }}
            
            function updatePriceDisplay() {{
                const quantity = parseInt(document.getElementById('quantity').value || '1');
                document.getElementById('total-display').innerText = formatPrice(BASE_PRICE * quantity);
            }}
            
            // Lazy load ảnh sản phẩm
            function lazyLoadImages() {{
                const lazyImages = document.querySelectorAll('img.lazy-load');
                if ('IntersectionObserver' in window) {{
                    const imageObserver = new IntersectionObserver(function(entries) {{
                        entries.forEach(function(entry) {{
                            if (entry.isIntersecting) {{
                                const lazyImage = entry.target;
                                lazyImage.src = lazyImage.dataset.src;
                                lazyImage.classList.remove('lazy-load');
                                imageObserver.unobserve(lazyImage);
                            }}
                        }});
                    }});
                    
                    lazyImages.forEach(function(lazyImage) {{
                        imageObserver.observe(lazyImage);
                    }});
                }} else {{
                    // Fallback cho trình duyệt cũ
                    lazyImages.forEach(function(lazyImage) {{
                        lazyImage.src = lazyImage.dataset.src;
                    }});
                }}
            }}
            
            // Hàm cập nhật thông tin biến thể (ảnh và giá)
            async function updateVariantInfo() {{
                const color = document.getElementById('color').value;
                const size = document.getElementById('size').value;
                
                // Hiển thị loading
                document.getElementById('variant-loading').style.display = 'block';
                
                try {{
                    const response = await fetch(`${{API_BASE_URL}}/get-variant-info?ms=${{PRODUCT_MS}}&color=${{encodeURIComponent(color)}}&size=${{encodeURIComponent(size)}}`);
                    if (response.ok) {{
                        const data = await response.json();
                        
                        // Cập nhật ảnh sản phẩm
                        const productImage = document.getElementById('product-image');
                        if (data.image) {{
                            productImage.dataset.src = data.image;
                            // Sử dụng lazy loading
                            if (!productImage.classList.contains('lazy-load')) {{
                                productImage.classList.add('lazy-load');
                            }}
                            lazyLoadImages();
                        }}
                        
                        // Cập nhật giá
                        BASE_PRICE = data.price || {price_int};
                        document.getElementById('price-display').innerText = formatPrice(BASE_PRICE);
                        updatePriceDisplay();
                    }}
                }} catch (error) {{
                    console.error('Lỗi khi cập nhật thông tin biến thể:', error);
                }} finally {{
                    document.getElementById('variant-loading').style.display = 'none';
                }}
            }}
            
            // Hàm load danh sách tỉnh/thành từ cache
            async function loadProvinces() {{
                try {{
                    const response = await fetch('/api/cached-provinces');
                    const provinces = await response.json();
                    
                    const provinceSelect = $('#province');
                    provinceSelect.empty();
                    provinceSelect.append('<option value="">Chọn tỉnh/thành phố</option>');
                    
                    provinces.forEach(province => {{
                        provinceSelect.append(`<option value="${{province.code}}">${{province.name}}</option>`);
                    }});
                    
                    // Khởi tạo Select2 sau khi trang đã load
                    setTimeout(() => {{
                        $('#province, #district, #ward').select2({{
                            width: '100%',
                            placeholder: 'Chọn...',
                            allowClear: false
                        }});
                        
                        // Xử lý sự kiện khi chọn tỉnh
                        provinceSelect.on('change', function() {{
                            const provinceCode = $(this).val();
                            if (provinceCode) {{
                                loadDistricts(provinceCode);
                            }} else {{
                                $('#district').val('').trigger('change').prop('disabled', true);
                                $('#ward').val('').trigger('change').prop('disabled', true);
                            }}
                        }});
                    }}, 100);
                    
                }} catch (error) {{
                    console.error('Lỗi khi load tỉnh/thành:', error);
                    // Fallback: hiển thị input text nếu API lỗi
                    $('#province').replaceWith('<input type="text" id="province" class="form-control" placeholder="Nhập tỉnh/thành phố" required>');
                    $('#district').replaceWith('<input type="text" id="district" class="form-control" placeholder="Nhập quận/huyện" required>');
                    $('#ward').replaceWith('<input type="text" id="ward" class="form-control" placeholder="Nhập phường/xã" required>');
                }}
            }}
            
            // Hàm load danh sách quận/huyện
            async function loadDistricts(provinceCode) {{
                try {{
                    const response = await fetch(`https://provinces.open-api.vn/api/p/${{provinceCode}}?depth=2`);
                    const provinceData = await response.json();
                    
                    const districts = provinceData.districts || [];
                    
                    const districtSelect = $('#district');
                    districtSelect.empty();
                    districtSelect.append('<option value="">Chọn quận/huyện</option>');
                    
                    districts.forEach(district => {{
                        districtSelect.append(`<option value="${{district.code}}">${{district.name}}</option>`);
                    }});
                    
                    districtSelect.prop('disabled', false).trigger('change');
                    
                    // Reset ward
                    $('#ward').empty().append('<option value="">Chọn phường/xã</option>').prop('disabled', true).trigger('change');
                    
                    // Xử lý sự kiện khi chọn huyện
                    districtSelect.on('change', function() {{
                        const districtCode = $(this).val();
                        if (districtCode) {{
                            loadWards(districtCode);
                        }} else {{
                            $('#ward').val('').trigger('change').prop('disabled', true);
                        }}
                    }});
                    
                }} catch (error) {{
                    console.error('Lỗi khi load quận/huyện:', error);
                }}
            }}
            
            // Hàm load danh sách phường/xã
            async function loadWards(districtCode) {{
                try {{
                    const response = await fetch(`https://provinces.open-api.vn/api/d/${{districtCode}}?depth=2`);
                    const districtData = await response.json();
                    
                    const wards = districtData.wards || [];
                    
                    const wardSelect = $('#ward');
                    wardSelect.empty();
                    wardSelect.append('<option value="">Chọn phường/xã</option>');
                    
                    wards.forEach(ward => {{
                        wardSelect.append(`<option value="${{ward.code}}">${{ward.name}}</option>`);
                    }});
                    
                    wardSelect.prop('disabled', false).trigger('change');
                    
                }} catch (error) {{
                    console.error('Lỗi khi load phường/xã:', error);
                }}
            }}
            
            async function submitOrder() {{
                // Lấy thông tin từ form
                const formData = {{
                    ms: PRODUCT_MS,
                    uid: PRODUCT_UID,
                    color: document.getElementById('color').value,
                    size: document.getElementById('size').value,
                    quantity: parseInt(document.getElementById('quantity').value || '1'),
                    unitPrice: BASE_PRICE,
                    customerName: document.getElementById('customerName').value.trim(),
                    phone: document.getElementById('phone').value.trim(),
                    province: $('#province').val(),
                    district: $('#district').val(),
                    ward: $('#ward').val(),
                    addressDetail: document.getElementById('addressDetail').value.trim()
                }};
                
                // Validation
                if (!formData.customerName) {{
                    alert('Vui lòng nhập họ và tên');
                    document.getElementById('customerName').focus();
                    return;
                }}
                
                if (!formData.phone) {{
                    alert('Vui lòng nhập số điện thoại');
                    document.getElementById('phone').focus();
                    return;
                }}
                
                // Chuẩn hóa số điện thoại
                let normalizedPhone = formData.phone.replace(/\\s/g, '');
                normalizedPhone = normalizedPhone.replace(/[^\\d+]/g, '');
                
                if (normalizedPhone.startsWith('84') && normalizedPhone.length === 11) {{
                    normalizedPhone = '0' + normalizedPhone.substring(2);
                }}
                
                if (normalizedPhone.startsWith('+84') && normalizedPhone.length === 12) {{
                    normalizedPhone = '0' + normalizedPhone.substring(3);
                }}
                
                const phoneRegex = /^(0\\d{{9}}|84\\d{{9}}|\\+84\\d{{9}})$/;
                if (!phoneRegex.test(normalizedPhone)) {{
                    alert('Số điện thoại không hợp lệ. Vui lòng nhập số điện thoại 10 chữ số (ví dụ: 0982155980) hoặc số quốc tế (+84982155980)');
                    document.getElementById('phone').focus();
                    return;
                }}
                
                formData.phone = normalizedPhone;
                
                if (!formData.province) {{
                    alert('Vui lòng chọn tỉnh/thành phố');
                    $('#province').select2('open');
                    return;
                }}
                
                if (!formData.district) {{
                    alert('Vui lòng chọn quận/huyện');
                    $('#district').select2('open');
                    return;
                }}
                
                if (!formData.ward) {{
                    alert('Vui lòng chọn phường/xã');
                    $('#ward').select2('open');
                    return;
                }}
                
                if (!formData.addressDetail) {{
                    alert('Vui lòng nhập địa chỉ chi tiết');
                    document.getElementById('addressDetail').focus();
                    return;
                }}
                
                // Ghép địa chỉ đầy đủ
                const provinceName = $('#province option:selected').text();
                const districtName = $('#district option:selected').text();
                const wardName = $('#ward option:selected').text();
                
                formData.fullAddress = `${{formData.addressDetail}}, ${{wardName}}, ${{districtName}}, ${{provinceName}}`;
                formData.provinceName = provinceName;
                formData.districtName = districtName;
                formData.wardName = wardName;
                
                const submitBtn = document.getElementById('submitBtn');
                const originalText = submitBtn.innerHTML;
                submitBtn.innerHTML = '<span class="loading-spinner"></span> ĐANG XỬ LÝ...';
                submitBtn.disabled = true;
                
                try {{
                    const response = await fetch(`${{API_BASE_URL}}/submit-order`, {{
                        method: 'POST',
                        headers: {{ 'Content-Type': 'application/json' }},
                        body: JSON.stringify(formData)
                    }});
                    
                    const data = await response.json();
                    
                    if (response.ok) {{
                        // Hiển thị thông báo thành công
                        const total = BASE_PRICE * formData.quantity;
                        const successMessage = `🎉 ĐÃ ĐẶT HÀNG THÀNH CÔNG!

📦 Mã sản phẩm: ${{PRODUCT_MS}}
👤 Khách hàng: ${{formData.customerName}}
📱 SĐT: ${{formData.phone}}
📍 Địa chỉ: ${{formData.fullAddress}}
💰 Đơn giá: ${{BASE_PRICE.toLocaleString('vi-VN')}} đ
📦 Số lượng: ${{formData.quantity}}
💰 Tổng tiền: ${{total.toLocaleString('vi-VN')}} đ

⏰ Shop sẽ liên hệ xác nhận trong 5-10 phút.
🚚 Giao hàng bởi ViettelPost (COD)

Cảm ơn quý khách đã đặt hàng! ❤️`;
                        
                        alert(successMessage);
                        
                        // Reset form sau 2 giây
                        setTimeout(() => {{
                            document.getElementById('orderForm').reset();
                            $('#province, #district, #ward').val('').trigger('change');
                            $('#district').prop('disabled', true);
                            $('#ward').prop('disabled', true);
                            updatePriceDisplay();
                        }}, 2000);
                        
                    }} else {{
                        alert(`❌ ${{data.message || 'Có lỗi xảy ra. Vui lòng thử lại sau'}}`);
                    }}
                }} catch (error) {{
                    console.error('Lỗi khi đặt hàng:', error);
                    alert('❌ Lỗi kết nối. Vui lòng kiểm tra mạng và thử lại!');
                }} finally {{
                    submitBtn.innerHTML = originalText;
                    submitBtn.disabled = false;
                }}
            }}
            
            // Khởi tạo khi trang được tải
            document.addEventListener('DOMContentLoaded', function() {{
                // Load danh sách tỉnh/thành từ cache
                loadProvinces();
                
                // Áp dụng lazy loading cho ảnh
                lazyLoadImages();
                
                // Cập nhật giá khi thay đổi số lượng
                document.getElementById('quantity').addEventListener('input', updatePriceDisplay);
                
                // Gọi cập nhật biến thể lần đầu
                setTimeout(() => {{
                    updateVariantInfo();
                }}, 300);
                
                // Focus vào trường tên
                setTimeout(() => {{
                    document.getElementById('customerName').focus();
                }}, 500);
            }});
        </script>
    </body>
    </html>
    """
    
    response = make_response(html)
    
    # Nén response nếu client hỗ trợ gzip
    if 'gzip' in request.headers.get('Accept-Encoding', '').lower() and len(html) > 500:
        @response.call_on_close
        def compress():
            gzip_buffer = BytesIO()
            with gzip.GzipFile(mode='wb', fileobj=gzip_buffer) as gzip_file:
                gzip_file.write(response.get_data())
            
            response.set_data(gzip_buffer.getvalue())
            response.headers['Content-Encoding'] = 'gzip'
            response.headers['Content-Length'] = len(response.get_data())
    
    return response

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

@app.route("/api/get-variant-image")
def api_get_variant_image():
    ms = (request.args.get("ms") or "").upper()
    color = request.args.get("color", "").strip()
    size = request.args.get("size", "").strip()
    
    load_products()
    if ms not in PRODUCTS:
        return {"error": "not_found"}, 404
    
    variant_image = get_variant_image(ms, color, size)
    
    return {
        "ms": ms,
        "color": color,
        "size": size,
        "image": variant_image
    }

@app.route("/api/submit-order", methods=["POST"])
def api_submit_order():
    data = request.get_json() or {}
    ms = (data.get("ms") or "").upper()
    uid = data.get("uid") or ""
    color = data.get("color") or ""
    size = data.get("size") or ""
    quantity = int(data.get("quantity") or 1)
    customer_name = data.get("customerName") or ""
    phone = data.get("phone") or ""
    
    # Debug log
    print(f"[ORDER DEBUG] MS: {ms}, Color: {color}, Size: {size}")
    
    # Địa chỉ mới
    address_detail = data.get("addressDetail") or ""
    province_name = data.get("provinceName") or ""
    district_name = data.get("districtName") or ""
    ward_name = data.get("wardName") or ""
    full_address = data.get("fullAddress") or ""
    
    # Kiểm tra dữ liệu bắt buộc
    if not all([ms, customer_name, phone, full_address]):
        return {"error": "missing_data", "message": "Vui lòng điền đầy đủ thông tin bắt buộc"}, 400
    
    load_products()
    row = PRODUCTS.get(ms)
    if not row:
        return {"error": "not_found", "message": "Sản phẩm không tồn tại"}, 404

    # QUAN TRỌNG: Tìm giá đúng của biến thể (màu + size)
    unit_price = 0
    variant_found = False
    
    # Debug: Log các biến thể có sẵn
    print(f"[ORDER DEBUG] Tìm biến thể với màu='{color}', size='{size}'")
    
    # Tìm biến thể phù hợp trong danh sách variants
    for idx, variant in enumerate(row.get("variants", [])):
        variant_color = variant.get("mau", "").strip().lower()
        variant_size = variant.get("size", "").strip().lower()
        
        input_color = color.strip().lower()
        input_size = size.strip().lower()
        
        # So khớp màu và size
        color_match = (not input_color) or (variant_color == input_color) or (input_color == "mặc định" and not variant_color)
        size_match = (not input_size) or (variant_size == input_size) or (input_size == "mặc định" and not variant_size)
        
        if color_match and size_match:
            variant_found = True
            # Ưu tiên lấy giá số (gia) trước, nếu không có thì lấy giá dạng chuỗi (gia_raw)
            if variant.get("gia"):
                unit_price = variant.get("gia", 0)
            else:
                # Nếu không có gia dạng số, thử chuyển đổi từ gia_raw
                gia_raw = variant.get("gia_raw", "")
                if gia_raw:
                    unit_price = extract_price_int(gia_raw) or 0
            print(f"[ORDER DEBUG] Biến thể {idx} phù hợp: màu='{variant_color}', size='{variant_size}', giá={unit_price}")
            break
    
    # Nếu không tìm thấy biến thể phù hợp, lấy giá chung của sản phẩm
    if not variant_found or unit_price == 0:
        price_str = row.get("Gia", "0")
        unit_price = extract_price_int(price_str) or 0
        print(f"[ORDER DEBUG] Không tìm thấy biến thể phù hợp, sử dụng giá chung: {unit_price}")
    
    total = unit_price * quantity
    
    # LẤY TÊN SẢN PHẨM (KHÔNG BAO GỒM MÃ SẢN PHẨM)
    product_name = row.get('Ten', '')
    
    # KIỂM TRA NẾU TÊN ĐÃ CHỨA MÃ SẢN PHẨM, CHỈ GIỮ TÊN
    if f"[{ms}]" in product_name or ms in product_name:
        # Xóa mã sản phẩm khỏi tên
        product_name = product_name.replace(f"[{ms}]", "").replace(ms, "").strip()
    
    print(f"[ORDER DEBUG] Biến thể tìm thấy: {variant_found}, Đơn giá: {unit_price}, Tổng tiền: {total}")

    # Gửi tin nhắn xác nhận cho khách hàng nếu có uid hợp lệ
    if uid and len(uid) > 5:  # UID Facebook thường dài
        try:
            ctx = USER_CONTEXT.get(uid, {})
            referral_source = ctx.get("referral_source", "direct")
            
            # Tin nhắn với giá đúng của biến thể (KHÔNG HIỂN THỊ MÃ SẢN PHẨM 2 LẦN)
            msg = (
                "🎉 Shop đã nhận được đơn hàng mới:\n"
                f"🛍 Sản phẩm: {product_name}\n"  # CHỈ HIỂN THỊ TÊN SẢN PHẨM
                f"🎨 Phân loại: {color} / {size}\n"
                f"💰 Đơn giá: {unit_price:,.0f} đ\n"
                f"📦 Số lượng: {quantity}\n"
                f"💰 Thành tiền: {total:,.0f} đ\n"
                f"👤 Người nhận: {customer_name}\n"
                f"📱 SĐT: {phone}\n"
                f"🏠 Địa chỉ: {full_address}\n"
                "────────────────────\n"
                "⏰ Shop sẽ gọi điện xác nhận trong 5-10 phút.\n"
                "🚚 Đơn hàng sẽ được giao bởi ViettelPost\n"
                "💳 Thanh toán khi nhận hàng (COD)\n"
                "────────────────────\n"
                "Cảm ơn anh/chị đã đặt hàng! ❤️"
            )
            send_message(uid, msg)
            print(f"✅ Đã gửi tin nhắn xác nhận cho user {uid}")
            
        except Exception as e:
            print(f"⚠️ Không thể gửi tin nhắn cho user {uid}: {str(e)}")
            # Vẫn tiếp tục xử lý đơn hàng ngay cả khi không gửi được tin nhắn
    
    order_data = {
        "ms": ms,
        "uid": uid,
        "color": color,
        "size": size,
        "quantity": quantity,
        "customer_name": customer_name,
        "phone": phone,
        "address": full_address,
        "address_detail": address_detail,
        "province": province_name,
        "district": district_name,
        "ward": ward_name,
        "product_name": product_name,
        "unit_price": unit_price,  # Lưu giá của biến thể
        "total_price": total,
        "referral_source": ctx.get("referral_source", "direct") if uid else "direct",
        "variant_found": variant_found  # Đánh dấu đã tìm thấy biến thể
    }
    
    # Ghi vào Google Sheets
    write_success = False
    if GOOGLE_SHEET_ID and GOOGLE_SHEETS_CREDENTIALS_JSON:
        write_success = write_order_to_google_sheet_api(order_data)
        if write_success:
            print(f"✅ Đã ghi đơn hàng vào Google Sheets: {ms} - {customer_name}")
        else:
            print(f"⚠️ Không thể ghi vào Google Sheets, sẽ lưu backup")
    
    # Luôn lưu backup local
    save_order_to_local_csv(order_data)
    print(f"📁 Đã lưu backup đơn hàng local: {ms} - {customer_name}")
    
    # Gửi thông báo đến Fchat nếu được cấu hình
    if FCHAT_WEBHOOK_URL and FCHAT_TOKEN:
        try:
            fchat_payload = {
                "token": FCHAT_TOKEN,
                "message": f"🛒 ĐƠN HÀNG MỚI\nMã: {ms}\nKH: {customer_name}\nSĐT: {phone}\nĐơn giá: {unit_price:,.0f}đ\nSố lượng: {quantity}\nTổng: {total:,.0f}đ",
                "metadata": {
                    "order_data": order_data,
                    "timestamp": datetime.now().isoformat()
                }
            }
            requests.post(FCHAT_WEBHOOK_URL, json=fchat_payload, timeout=5)
            print(f"📨 Đã gửi thông báo đến Fchat")
        except Exception as e:
            print(f"⚠️ Không thể gửi notification đến Fchat: {str(e)}")

    return {
        "status": "ok", 
        "message": "Đơn hàng đã được tiếp nhận",
        "order_written": write_success,
        "order_details": {
            "order_id": f"ORD{int(time.time())}_{uid[-4:] if uid else '0000'}",
            "product_code": ms,
            "product_name": product_name,
            "variant": f"{color} / {size}",
            "unit_price": unit_price,
            "quantity": quantity,
            "total": total,
            "customer_name": customer_name,
            "phone": phone,
            "address": full_address,
            "timestamp": datetime.now().isoformat()
        }
    }

# ============================================
# HEALTH CHECK
# ============================================

@app.route("/health", methods=["GET"])
def health_check():
    current_fanpage_name = get_fanpage_name_from_api()
    
    total_variants = sum(len(p['variants']) for p in PRODUCTS.values())
    
    # Kiểm tra feed comment capability
    feed_comment_test = "Ready"
    if PAGE_ACCESS_TOKEN and PAGE_ID:
        feed_comment_test = "✅ Sẵn sàng"
    else:
        feed_comment_test = "⚠️ Cần cấu hình PAGE_ACCESS_TOKEN và PAGE_ID"
    
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "products_loaded": len(PRODUCTS),
        "variants_loaded": total_variants,
        "openai_configured": bool(client),
        "facebook_configured": bool(PAGE_ACCESS_TOKEN),
        "fanpage_name": current_fanpage_name,
        "page_id": PAGE_ID,
        "feed_comment_processing": feed_comment_test,
        "google_sheets_integration": {
            "sheet_id_configured": bool(GOOGLE_SHEET_ID),
            "credentials_configured": bool(GOOGLE_SHEETS_CREDENTIALS_JSON)
        },
        "poscake_integration": {
            "api_key_configured": bool(POSCAKE_API_KEY),
            "webhook_secret_configured": bool(POSCAKE_WEBHOOK_SECRET),
            "store_id_configured": bool(POSCAKE_STORE_ID),
            "endpoints": {
                "webhook": "/poscake-webhook",
                "test": "/test-poscake-webhook"
            }
        },
        "gpt_function_calling": {
            "enabled": True,
            "tools": ["get_product_price_details", "get_product_basic_info", "send_product_images", "send_product_videos", "provide_order_link"],
            "model": "gpt-4o-mini",
            "first_message_logic": "Carousel 1 sản phẩm (chưa gửi carousel)",
            "second_message_logic": "GPT Function Calling (đã gửi carousel)",
            "price_analysis": "Thông minh (color_based, size_based, complex_based, single_price)",
            "policy_handling": "GPT tự đọc mô tả sản phẩm (không dùng tool riêng, không dùng từ khóa)"
        },
        "image_processing": {
            "enabled": True,
            "technology": "OpenAI Vision API (3 phương pháp fallback: URL trực tiếp, base64, URL đơn giản)",
            "emoji_detection": True,
            "product_matching": "Text-based similarity matching nâng cao với trọng số",
            "suggestion_carousel": "Carousel 3 sản phẩm gợi ý khi không tìm thấy từ ảnh"
        },
        "feed_comment_processing": {
            "enabled": True,
            "logic": "Lấy MS từ caption bài viết khi user comment",
            "capabilities": [
                "Detect MS từ bài viết gốc",
                "Auto reply với thông tin sản phẩm",
                "Cập nhật context cho user",
                "Reset counter để áp dụng first message rule"
            ],
            "required_permissions": "pages_read_engagement, pages_messaging"
        },
        "features": {
            "carousel_first_message": True,
            "catalog_support": True,
            "ads_referral_processing": True,
            "fchat_echo_processing": False,  # ĐÃ TẮT
            "image_processing": True,
            "order_form": True,
            "google_sheets_api": True,
            "poscake_webhook": True,
            "facebook_shop_order_processing": True,
            "ms_context_update": True,
            "no_duplicate_ms_display": True,
            "optimized_form_loading": True,
            "address_api_cache": True,
            "lazy_image_loading": True,
            "gzip_compression": True,
            "feed_comment_processing": True  # TÍNH NĂNG MỚI
        }
    }, 200

# ============================================
# HEALTH CHECK NHANH (CHO LOAD BALANCER)
# ============================================

@app.route("/health-light", methods=["GET"])
def health_light():
    """Health check nhanh, không load products"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "order-form",
        "uptime": time.time() - LAST_LOAD if LAST_LOAD > 0 else 0
    }), 200

# ============================================
# PORT CONFIGURATION FOR KOYEB/RENDER
# ============================================
def get_port():
    """Get port from environment variable with fallback"""
    return int(os.environ.get("PORT", 5000))

# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    import os
    
    print("=" * 80)
    print("🟢 KHỞI ĐỘNG FACEBOOK CHATBOT - GPT FUNCTION CALLING MODE")
    print("=" * 80)
    print(f"🟢 Process ID: {os.getpid()}")
    print(f"🟢 Port: {get_port()}")
    print("=" * 80)
    
    print(f"🟢 GPT-4o-mini: {'SẴN SÀNG' if client else 'CHƯA CẤU HÌNH'}")
    print(f"🟢 Fanpage: {get_fanpage_name_from_api()}")
    print(f"🟢 Page ID: {PAGE_ID}")
    print(f"🟢 Domain: {DOMAIN}")
    print(f"🟢 Google Sheets API: {'SẴN SÀNG' if GOOGLE_SHEET_ID and GOOGLE_SHEETS_CREDENTIALS_JSON else 'CHƯA CẤU HÌNH'}")
    print(f"🟢 Poscake Webhook: {'SẴN SÀNG' if POSCAKE_API_KEY else 'CHƯA CẤU HÌNH'}")
    print(f"🟢 OpenAI Function Calling: {'TÍCH HỢP THÀNH CÔNG' if client else 'CHƯA CẤU HÌNH'}")
    print("=" * 80)
    
    print("🔴 CẢI TIẾN MỚI: XỬ LÝ COMMENT TỪ FEED (LẤY MS TỪ CAPTION BÀI VIẾT)")
    print("=" * 80)
    print(f"🔴 1. Feed Comment Processing: Tự động phát hiện MS khi user comment")
    print(f"🔴 2. Logic: Lấy post_id → Lấy nội dung bài viết → Trích xuất MS từ caption")
    print(f"🔴 3. Auto Reply: Gửi tin nhắn giới thiệu sản phẩm khi user comment lần đầu")
    print(f"🔴 4. Context Update: Reset counter để áp dụng first message rule")
    print(f"🔴 5. Test Endpoint: /test-feed-comment?post_id=...")
    print("=" * 80)
    
    print("🔴 CẢI TIẾN MỚI: TỐI ƯU TỐC ĐỘ LOAD TRANG FORM ĐẶT HÀNG")
    print("=" * 80)
    print(f"🔴 1. Prefetch Products: Tự động load products khi truy cập order-form")
    print(f"🔴 2. Address API Cache: Cache dữ liệu tỉnh/thành (/api/cached-provinces)")
    print(f"🔴 3. Lazy Loading Images: Ảnh sản phẩm chỉ load khi cần thiết")
    print(f"🔴 4. Optimized CDN: Sử dụng Cloudflare CDN cho jQuery và Select2")
    print(f"🔴 5. Async Select2: Khởi tạo Select2 sau khi trang đã load")
    print(f"🔴 6. Gzip Compression: Nén HTML response giảm 70% kích thước")
    print(f"🔴 7. Health Check Light: /health-light endpoint nhanh cho load balancer")
    print("=" * 80)
    
    print("🔴 CẢI TIẾN MỚI: XÓA MÃ SẢN PHẨM TRÙNG LẶP")
    print("=" * 80)
    print(f"🔴 Carousel: Chỉ hiển thị tên sản phẩm (đã loại bỏ mã nếu có trong tên)")
    print(f"🔴 Tin nhắn xác nhận đơn hàng: Chỉ hiển thị tên sản phẩm, không hiển thị mã lặp lại")
    print(f"🔴 Tự động xử lý: Kiểm tra nếu tên đã chứa mã thì xóa bỏ mã khỏi tên")
    print("=" * 80)
    
    print("🟢 CẢI TIẾN MỚI: POSCAKE WEBHOOK INTEGRATION")
    print("=" * 80)
    print(f"🟢 Endpoint: /poscake-webhook (POST)")
    print(f"🟢 Test endpoint: /test-poscake-webhook (GET/POST)")
    print(f"🟢 Xác thực: Signature verification với POSCAKE_WEBHOOK_SECRET")
    print(f"🟢 Xử lý sự kiện: order.created, order.updated, order.shipped, order.delivered, order.cancelled")
    print(f"🟢 Tự động gửi tin nhắn: Thông báo trạng thái đơn hàng cho khách")
    print(f"🟢 Context lưu trữ: USER_CONTEXT['poscake_orders'] - lưu 10 đơn hàng gần nhất")
    print("=" * 80)
    
    print("🟢 CẢI TIẾN MỚI: XỬ LÝ ẢNH SẢN PHẨM THÔNG MINH VỚI CAROUSEL GỢI Ý")
    print("=" * 80)
    print(f"🟢 Vision API cải tiến: 3 phương pháp fallback (URL trực tiếp, base64, URL đơn giản)")
    print(f"🟢 Phát hiện emoji/sticker: Loại bỏ ảnh emoji/sticker (dựa trên URL pattern)")
    print(f"🟢 Kiểm tra ảnh hợp lệ: Mở rộng domain và pattern chấp nhận")
    print(f"🟢 Matching nâng cao: Trích xuất từ khóa thông minh, tính điểm tương đồng với trọng số hợp lý")
    print(f"🟢 Carousel gợi ý: Gửi carousel 3 sản phẩm khi không tìm thấy từ ảnh")
    print(f"🟢 Xử lý lỗi: Tải ảnh về server khi Facebook CDN lỗi")
    print(f"🟢 Context cập nhật: Reset counter để áp dụng first message rule khi tìm thấy sản phẩm từ ảnh")
    print("=" * 80)
    
    print("🔴 FORM ĐẶT HÀNG CẢI TIẾN:")
    print("=" * 80)
    print(f"🔴 Cập nhật ảnh và giá theo biến thể: /api/get-variant-info")
    print(f"🔴 Địa chỉ theo API: Tỉnh/Huyện/Xã + địa chỉ chi tiết")
    print(f"🔴 Sử dụng Select2 cho UI tốt hơn")
    print(f"🔴 Fallback khi API địa chỉ lỗi")
    print(f"🔴 FIX: Sửa lỗi validate số điện thoại - chấp nhận 0982155980, +84982155980")
    print(f"🔴 FIX: Thêm xử lý chuẩn hóa số điện thoại tự động")
    print("=" * 80)
    
    print("🔴 FIX THÀNH TIỀN TRONG TIN NHẮN PHẢN HỒI:")
    print("=" * 80)
    print(f"🔴 Tìm giá đúng của biến thể (màu + size) trong hàm api_submit_order")
    print(f"🔴 Cập nhật tin nhắn phản hồi: hiển thị cả đơn giá và thành tiền tính đúng")
    print(f"🔴 Cải thiện hàm extract_price_int để xử lý nhiều định dạng giá")
    print(f"🔴 Thêm debug log để kiểm tra khi có vấn đề")
    print("=" * 80)
    
    print("🟢 TÍNH NĂNG MỚI: XỬ LÝ ĐƠN HÀNG TỰ FACEBOOK SHOP")
    print("=" * 80)
    print(f"🟢 Xử lý sự kiện 'order' từ Facebook Shop")
    print(f"🟢 KHÔNG gửi tin nhắn cảm ơn khi có đơn hàng mới từ Facebook Shop")
    print(f"🟢 Trích xuất mã sản phẩm từ retailer_id")
    print(f"🟢 Hiển thị chi tiết sản phẩm, số lượng, đơn giá, tổng tiền")
    print(f"🟢 Log đơn hàng vào file facebook_shop_orders.log")
    print(f"🟢 Cập nhật context với mã sản phẩm để hỗ trợ tư vấn tiếp theo")
    print("=" * 80)
    
    print("🔴 TẮT TÍNH NĂNG: GHI NHẬN MS TỪ ECHO FCHAT")
    print("=" * 80)
    print(f"🔴 Đã xóa logic xử lý Fchat echo trong webhook handler")
    print(f"🔴 Chỉ xử lý echo từ bot (bỏ qua)")
    print(f"🔴 Echo từ người dùng (comment) đã được xử lý qua feed")
    print("=" * 80)
    
    load_products()
    
    # Lấy port từ biến môi trường
    port = get_port()
    print(f"🟢 Đang khởi động server trên port: {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
