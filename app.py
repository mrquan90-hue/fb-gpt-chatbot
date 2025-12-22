import os
import json
import re
import time
import csv
import hashlib
import base64
import threading
from collections import defaultdict
from urllib.parse import quote
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from io import BytesIO
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

import requests
from flask import Flask, request, send_from_directory, jsonify, render_template_string
from openai import OpenAI

# ============================================
# GOOGLE SHEETS API INTEGRATION - NEW IMPORTS
# ============================================

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    GOOGLE_API_AVAILABLE = True
except ImportError:
    print("⚠️ Google API libraries not installed. Google Sheets integration will be disabled.")
    GOOGLE_API_AVAILABLE = False

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

# ============================================
# GOOGLE SHEETS API CONFIGURATION - NEW
# ============================================
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()  # From spreadsheet URL
GOOGLE_SHEETS_CREDENTIALS_JSON = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON", "").strip()

if not GOOGLE_SHEET_CSV_URL:
    GOOGLE_SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv"

# ============================================
# APP ID CỦA BOT ĐỂ PHÂN BIỆT ECHO MESSAGE
# ============================================
BOT_APP_IDS = {"645956568292435"}  # App ID của bot từ log

# ============================================
# GLOBAL LOCKS FOR POSTBACK PROCESSING
# ============================================
POSTBACK_LOCKS = {}
POSTBACK_LOCK_TIMEOUT = 10  # 10 giây timeout

def get_postback_lock(uid: str, payload: str):
    """Lấy lock duy nhất cho postback của user"""
    key = f"{uid}_{payload}"
    if key not in POSTBACK_LOCKS:
        POSTBACK_LOCKS[key] = threading.Lock()
    return POSTBACK_LOCKS[key]

def cleanup_old_locks():
    """Dọn dẹp locks cũ (tránh memory leak)"""
    global POSTBACK_LOCKS
    current_time = time.time()
    keys_to_remove = []
    
    for key, lock in list(POSTBACK_LOCKS.items()):
        # Giữ lock tối đa 1 giờ
        if hasattr(lock, '_last_used'):
            if current_time - lock._last_used > 3600:
                keys_to_remove.append(key)
    
    for key in keys_to_remove:
        del POSTBACK_LOCKS[key]

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
    "processing_lock_time": 0,
    "postback_count": 0,
    "product_info_sent_ms": None,
    "last_product_info_time": 0,
    "last_postback_time": 0,
    "last_postback_payload": None,
    "processed_postbacks": {},
    "product_info_processing": False,
    "product_info_sent_ms": None,
    "last_product_images_sent": {},
    "product_history": [],
    "conversation_history": [],
    "referral_source": None,
    "referral_payload": None,
    # Thêm trường cho nhận diện ảnh
    "last_image_analysis": None,
    "last_image_url": None,
    "last_image_base64": None,
    "last_image_time": 0,
    "processed_image_mids": set(),
    # Thêm trường cho echo message từ Fchat
    "last_echo_processed_time": 0,
    "processed_echo_mids": set(),
    # Thêm trường cho debounce và duplicate detection
    "processed_message_mids": {},
    "last_processed_text": "",
    # Thêm trường cho gửi ảnh sản phẩm
    "last_all_images_time": 0,
    "last_images_request_time": 0,
    # Thêm trường cho catalog và retailer_id
    "last_retailer_id": None,
    "last_product_id": None,
    "catalog_view_time": 0,
    "last_catalog_product": None,
    # Thêm dict để lưu nhiều sản phẩm từ catalog
    "catalog_products": {},
    # THÊM: Trạng thái cho tin nhắn đầu tiên sau referral
    "first_message_after_referral": False,
    "pending_carousel_ms": None,
    "referral_processed": False,
    # THÊM: Atomic lock để tránh double processing
    "product_info_atomic_lock": False,
    "last_lock_release_time": 0,
    # ▼▼▼ THÊM MỚI: Idempotency key storage cho postback
    "idempotent_postbacks": {},
})

PRODUCTS = {}
PRODUCTS_BY_NUMBER = {}
PRODUCT_TEXT_EMBEDDINGS = {}
LAST_LOAD = 0
LOAD_TTL = 300

# Các từ khóa liên quan đến đặt hàng (GIỮ LẠI vì quan trọng)
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

# ============================================
# CACHE CHO TÊN FANPAGE
# ============================================
FANPAGE_NAME_CACHE = None
FANPAGE_NAME_CACHE_TIME = 0
FANPAGE_NAME_CACHE_TTL = 3600  # Cache trong 1 giờ

def get_fanpage_name_from_api():
    """
    Lấy tên fanpage từ Facebook Graph API với cache
    """
    global FANPAGE_NAME_CACHE, FANPAGE_NAME_CACHE_TIME
    
    now = time.time()
    
    # Kiểm tra cache còn hiệu lực không
    if (FANPAGE_NAME_CACHE and 
        FANPAGE_NAME_CACHE_TIME and 
        (now - FANPAGE_NAME_CACHE_TIME) < FANPAGE_NAME_CACHE_TTL):
        return FANPAGE_NAME_CACHE
    
    if not PAGE_ACCESS_TOKEN:
        print("[WARN] Không có PAGE_ACCESS_TOKEN để lấy tên fanpage")
        FANPAGE_NAME_CACHE = FANPAGE_NAME
        FANPAGE_NAME_CACHE_TIME = now
        return FANPAGE_NAME_CACHE
    
    try:
        url = f"https://graph.facebook.com/v12.0/me?fields=name&access_token={PAGE_ACCESS_TOKEN}"
        response = requests.get(url, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            page_name = data.get('name', FANPAGE_NAME)
            print(f"✅ Lấy tên fanpage từ API thành công: {page_name}")
            
            # Lưu vào cache
            FANPAGE_NAME_CACHE = page_name
            FANPAGE_NAME_CACHE_TIME = now
            return page_name
        else:
            print(f"❌ Lỗi khi lấy tên fanpage: {response.status_code} - {response.text}")
            FANPAGE_NAME_CACHE = FANPAGE_NAME
            FANPAGE_NAME_CACHE_TIME = now
            return FANPAGE_NAME_CACHE
    except Exception as e:
        print(f"❌ Lỗi kết nối khi lấy tên fanpage: {str(e)}")
        FANPAGE_NAME_CACHE = FANPAGE_NAME
        FANPAGE_NAME_CACHE_TIME = now
        return FANPAGE_NAME_CACHE

# ============================================
# HELPER: TRÍCH XUẤT MÃ SẢN PHẨM TỪ RETAILER_ID
# ============================================

def extract_ms_from_retailer_id(retailer_id: str) -> Optional[str]:
    """
    Trích xuất mã sản phẩm từ retailer_id của catalog
    Ví dụ: "MS000019_13" -> "MS000019"
    """
    if not retailer_id:
        return None
    
    # Tách phần đầu trước dấu gạch dưới
    parts = retailer_id.split('_')
    if not parts:
        return None
    
    base_id = parts[0].upper()
    
    # Kiểm tra định dạng MSxxxxxx
    if re.match(r'MS\d{6}', base_id):
        return base_id
    
    # Thử tìm trong mapping
    match = re.search(r'MS(\d+)', base_id)
    if match:
        num = match.group(1)
        num_6 = num.zfill(6)
        candidate = "MS" + num_6
        return candidate
    
    return None

# ============================================
# HELPER: TRÍCH XUẤT MÃ SẢN PHẨM TỪ AD_TITLE
# ============================================

def extract_ms_from_ad_title(ad_title: str) -> Optional[str]:
    """Trích xuất mã sản phẩm từ tiêu đề quảng cáo Facebook"""
    if not ad_title:
        return None
    
    # Tìm "mã 39" hoặc "MS39" trong tiêu đề
    ad_title_lower = ad_title.lower()
    
    # Tìm pattern: "mã 39" -> "MS000039"
    match = re.search(r'mã\s*(\d{1,6})', ad_title_lower)
    if match:
        num = match.group(1)
        num_6 = num.zfill(6)
        return "MS" + num_6
    
    # Tìm pattern: "ms39" -> "MS000039"
    match = re.search(r'ms\s*(\d{1,6})', ad_title_lower)
    if match:
        num = match.group(1)
        num_6 = num.zfill(6)
        return "MS" + num_6
    
    # Tìm trực tiếp số có 2-6 chữ số
    match = re.search(r'\b(\d{2,6})\b', ad_title)
    if match:
        num = match.group(1)
        num_6 = num.zfill(6)
        candidate = "MS" + num_6
        return candidate
    
    return None

# ============================================
# HELPER: KIỂM TRA ECHO MESSAGE CÓ PHẢI TỪ BOT KHÔNG
# ============================================

def is_bot_generated_echo(echo_text: str, app_id: str = "", attachments: list = None) -> bool:
    """
    Kiểm tra xem echo message có phải do bot tạo ra không
    Dựa trên app_id và nội dung tin nhắn
    """
    if not echo_text and not attachments:
        return False
    
    # 1. Kiểm tra theo app_id
    if app_id in BOT_APP_IDS:
        return True
    
    # 2. Kiểm tra theo nội dung (chỉ khi có echo_text)
    if echo_text:
        # Các mẫu tin nhắn đặc trưng của bot
        bot_response_patterns = [
            "Dạ, phần này trong hệ thống chưa có thông tin ạ",
            "em sợ nói sai nên không dám khẳng định",
            "Chào anh/chị! 👋",
            "Em là trợ lý AI",
            "📌 [MS",
            "📝 MÔ TẢ:",
            "💰 GIÁ SẢN PHẨM:",
            "📋 Đặt hàng ngay tại đây:",
            "Dạ em đang gặp chút trục trặc",
            "Dạ, em đang lấy danh sách",
            "Anh/chị vuốt sang trái/phải",
            "💬 Gõ mã sản phẩm",
            "📱 Anh/chị vuốt",
            "🎯 Em phân tích được đây là",
            "🔍 Em tìm thấy",
            "🖼️ Em đang phân tích ảnh",
            "🟢 Phù hợp:",
            "❌ Lỗi phân tích ảnh",
            "⚠️ Không thể lấy được ảnh",
            "📊 Kết quả phân tích ảnh chi tiết",
            "🎉 Shop đã nhận được đơn hàng mới",
            "⏰ Shop sẽ gọi điện xác nhận",
            "💳 Thanh toán khi nhận hàng (COD)",
            "Cảm ơn anh/chị đã đặt hàng",
            "Dạ em cảm ơn anh/chị",
            "Dạ vâng. Anh/chị cho em xin",
            "Dạ em tóm tắt lại đơn hàng",
        ]
        
        for pattern in bot_response_patterns:
            if pattern in echo_text:
                return True
        
        # Kiểm tra theo cấu trúc: bắt đầu bằng emoji và có nhiều dòng
        lines = echo_text.strip().split('\n')
        if lines and len(lines) > 1:
            first_line = lines[0]
            if any(emoji in first_line for emoji in ["👋", "📌", "📝", "💰", "📋", "🎯", "🔍", "🖼️", "🟢", "❌", "⚠️", "📊", "🎉", "⏰", "💳"]):
                return True
    
    # 3. Kiểm tra attachment (hình ảnh từ bot)
    if attachments and (not echo_text or len(echo_text.strip()) < 10):
        # Nếu có attachment và text rỗng/ngắn, có thể là hình ảnh từ bot
        return True
    
    return False

# ============================================
# TRÍCH XUẤT MÀU VÀ SIZE ĐƠN GIẢN
# ============================================

def extract_color_size_simple(text: str):
    """Trích xuất màu và size đơn giản từ tin nhắn"""
    text_lower = text.lower()
    
    color = None
    size = None
    
    # Tìm màu đơn giản
    color_words = ["đỏ", "đen", "trắng", "xanh", "vàng", "hồng", "tím", "nâu", "xám", "be", "cam", "xanh lá", "xanh dương", "đỏ đô", "hồng pastel", "đen tuyền"]
    for c in color_words:
        if c in text_lower:
            color = c
            break
    
    # Tìm size đơn giản
    if "size" in text_lower:
        # Tìm XS, S, M, L, XL, XXL
        size_match = re.search(r'size\s+([XSML0-9]+)', text_lower)
        if size_match:
            size = size_match.group(1).upper()
        else:
            # Tìm trực tiếp
            for s in ["XS", "S", "M", "L", "XL", "XXL", "XXXL"]:
                if s.lower() in text_lower:
                    size = s
                    break
    
    return color, size

# ============================================
# XỬ LÝ CATALOG FOLLOWUP
# ============================================

def handle_catalog_followup(uid: str, text: str) -> bool:
    """
    Xử lý tin nhắn follow-up ngay sau khi xem catalog
    Trả về True nếu đã xử lý, False nếu không phải follow-up
    """
    ctx = USER_CONTEXT[uid]
    now = time.time()
    
    # Kiểm tra xem có phải follow-up từ catalog không
    last_catalog_time = ctx.get("catalog_view_time", 0)
    retailer_id = ctx.get("last_retailer_id")
    
    # Chỉ xử lý trong 30 giây sau khi xem catalog
    if not retailer_id or (now - last_catalog_time) > 30:
        return False
    
    # Trích xuất mã sản phẩm từ retailer_id
    ms = extract_ms_from_retailer_id(retailer_id)
    if not ms or ms not in PRODUCTS:
        return False
    
    print(f"[CATALOG FOLLOWUP] Xử lý tin nhắn sau catalog: {text[:50]}...")
    
    # Cập nhật context
    ctx["last_ms"] = ms
    update_product_context(uid, ms)
    
    # Xử lý bằng Function Calling
    handle_text_with_function_calling(uid, text)
    return True

# ============================================
# XỬ LÝ TIN NHẮN SAU CLICK QUẢNG CÁO ADS
# ============================================

def handle_ads_referral_product(uid: str, text: str) -> bool:
    """
    Xử lý đặc biệt cho tin nhắn sau khi click quảng cáo
    Trả về True nếu xác định được sản phẩm từ context ADS
    """
    ctx = USER_CONTEXT[uid]
    
    # Chỉ xử lý nếu referral từ ADS
    if ctx.get("referral_source") != "ADS":
        return False
    
    # 1. Ưu tiên sử dụng last_ms từ context (đã được set từ ad_title)
    last_ms = ctx.get("last_ms")
    if last_ms and last_ms in PRODUCTS:
        print(f"[ADS CONTEXT] Sử dụng last_ms từ ADS context: {last_ms}")
        
        # Xử lý bằng Function Calling
        handle_text_with_function_calling(uid, text)
        return True
    
    return False

# ============================================
# THÊM: HÀM GỬI CAROUSEL 1 SẢN PHẨM
# ============================================

def send_single_product_carousel(uid: str, ms: str):
    """
    Gửi carousel chỉ với 1 sản phẩm duy nhất
    Sử dụng khi bot đã nhận diện được MS từ ad_title, catalog, Fchat
    """
    if ms not in PRODUCTS:
        return
    
    load_products()
    product = PRODUCTS[ms]
    
    # Lấy ảnh đầu tiên
    images_field = product.get("Images", "")
    urls = parse_image_urls(images_field)
    image_url = urls[0] if urls else ""
    
    # Format giá
    gia_raw = product.get("Gia", "")
    gia_int = extract_price_int(gia_raw) or 0
    
    # Tạo mô tả ngắn
    short_desc = short_description(product.get("MoTa", ""), 120)
    
    # Tạo carousel element cho 1 sản phẩm
    element = {
        "title": f"[{ms}] {product.get('Ten', '')}",
        "image_url": image_url,
        "subtitle": f"💰 {gia_int:,.0f} đ\n{short_desc}",
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
            },
            {
                "type": "postback", 
                "title": "🖼️ Xem ảnh",
                "payload": f"VIEW_IMAGES_{ms}"
            }
        ]
    }
    
    # Gửi carousel
    send_carousel_template(uid, [element])
    
    # QUAN TRỌNG: Cập nhật context ngay sau khi gửi carousel
    ctx = USER_CONTEXT[uid]
    ctx["last_ms"] = ms
    update_product_context(uid, ms)
    
    print(f"[SINGLE CAROUSEL] Đã gửi carousel 1 sản phẩm {ms} cho user {uid}")

# ============================================
# GỬI TOÀN BỘ ẢNH SẢN PHẨM - ĐÃ SỬA LỖI DEADLOCK
# ============================================

def send_all_product_images(uid: str, ms: str, max_images: int = 20):
    """
    Gửi toàn bộ ảnh của sản phẩm (loại trừ trùng)
    
    Args:
        uid: ID người dùng
        ms: Mã sản phẩm
        max_images: Giới hạn số lượng ảnh tối đa (tránh spam)
    """
    if ms not in PRODUCTS:
        send_message(uid, "Em không tìm thấy sản phẩm này trong hệ thống ạ.")
        return
    
    ctx = USER_CONTEXT[uid]
    
    # KIỂM TRA DEBOUNCE: không gửi ảnh quá nhanh
    now = time.time()
    last_image_send_time = ctx.get("last_all_images_time", 0)
    
    if now - last_image_send_time < 5:
        print(f"[IMAGE SEND DEBOUNCE] Bỏ qua gửi ảnh cho {uid}, chưa đủ 5s")
        return
    
    ctx["last_all_images_time"] = now
    
    try:
        product = PRODUCTS[ms]
        product_name = product.get('Ten', 'Sản phẩm')
        
        # Lấy tất cả ảnh từ trường Images
        images_field = product.get("Images", "")
        urls = parse_image_urls(images_field)
        
        # Lọc ảnh trùng và ảnh hợp lệ
        unique_images = []
        seen_urls = set()
        
        for url in urls:
            if url and url.strip() and url not in seen_urls:
                seen_urls.add(url)
                
                # Kiểm tra URL hợp lệ (có chứa domain ảnh)
                url_lower = url.lower()
                if any(domain in url_lower for domain in [
                    'alicdn.com', 'taobao', '1688.com', 'http', 
                    '.jpg', '.jpeg', '.png', '.webp', '.gif',
                    'image', 'img', 'photo', 'static'
                ]):
                    unique_images.append(url)
        
        if not unique_images:
            send_message(uid, f"Sản phẩm [{ms}] hiện chưa có hình ảnh trong hệ thống ạ.")
            return
        
        # Giới hạn số lượng ảnh để tránh spam
        total_images = len(unique_images)
        original_count = len(urls)
        
        if total_images > max_images:
            unique_images = unique_images[:max_images]
            limit_msg = f" (hiển thị {max_images}/{total_images} ảnh đầu tiên)"
        else:
            limit_msg = ""
        
        # Thông báo cho khách
        send_message(uid, f"Dạ em gửi ảnh sản phẩm [{ms}] - {product_name}{limit_msg}:")
        time.sleep(0.8)
        
        # Gửi từng ảnh một với debounce
        sent_count = 0
        last_send_time = 0
        
        for i, image_url in enumerate(unique_images, 1):
            try:
                # Debounce giữa các ảnh
                current_time = time.time()
                if current_time - last_send_time < 0.5:  # 0.5 giây giữa các ảnh
                    time.sleep(0.5 - (current_time - last_send_time))
                
                print(f"🖼️ Gửi ảnh {i}/{len(unique_images)}: {image_url[:80]}...")
                result = send_image(uid, image_url)
                
                if result:
                    sent_count += 1
                    last_send_time = time.time()
                
                # Thêm delay giữa các ảnh để tránh bị rate limit
                if i < len(unique_images):
                    time.sleep(0.8)
                    
            except Exception as e:
                print(f"❌ Lỗi khi gửi ảnh {i}: {str(e)}")
                # Vẫn tiếp tục gửi ảnh tiếp theo
                time.sleep(1.0)  # Delay lâu hơn nếu có lỗi
        
        # Thông báo kết quả
        if sent_count > 0:
            time.sleep(1.0)
            
            # Nếu có ảnh trùng bị bỏ qua
            if original_count > total_images:
                duplicated_count = original_count - total_images
                if duplicated_count > 0:
                    send_message(uid, f"📝 Lưu ý: Đã tự động loại bỏ {duplicated_count} ảnh trùng lặp.")
                    time.sleep(0.8)
            
            # Hỏi khách có cần thêm thông tin không
            send_message(uid, f"✅ Đã gửi {sent_count} ảnh sản phẩm cho anh/chị!")
            time.sleep(0.8)
            send_message(uid, "Anh/chị có muốn xem thông tin chi tiết hoặc đặt hàng sản phẩm này không ạ?")
        else:
            send_message(uid, "❌ Không thể gửi ảnh ngay lúc này. Anh/chị vui lòng thử lại sau ạ.")
    
    except Exception as e:
        print(f"❌ Lỗi trong send_all_product_images: {str(e)}")
        try:
            send_message(uid, "❌ Có lỗi khi tải ảnh sản phẩm. Anh/chị vui lòng thử lại sau ạ.")
        except:
            pass

# ============================================
# HELPER: TẢI VÀ XỬ LÝ ẢNH
# ============================================

def download_image_from_facebook(image_url: str, timeout: int = 10) -> Optional[bytes]:
    """
    Tải ảnh từ Facebook URL với headers phù hợp
    Trả về bytes của ảnh hoặc None nếu thất bại
    """
    try:
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
            content_type = response.headers.get('content-type', '')
            if not content_type.startswith('image/'):
                print(f"⚠️ URL không phải ảnh: {content_type}")
                return None
            
            max_size = 10 * 1024 * 1024
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
        base64_str = base64.b64encode(image_bytes).decode('utf-8')
        
        if image_bytes[:4] == b'\x89PNG':
            mime_type = 'image/png'
        elif image_bytes[:3] == b'\xff\xd8\xff':
            mime_type = 'image/jpeg'
        elif image_bytes[:6] in (b'GIF87a', b'GIF89a'):
            mime_type = 'image/gif'
        elif image_bytes[:4] == b'RIFF' and image_bytes[8:12] == b'WEBP':
            mime_type = 'image/webp'
        else:
            mime_type = 'image/jpeg'
        
        data_url = f"data:{mime_type};base64,{base64_str}"
        return data_url
        
    except Exception as e:
        print(f"❌ Lỗi chuyển đổi base64: {str(e)}")
        return None

def get_image_for_analysis(image_url: str) -> Optional[str]:
    """
    Lấy ảnh dưới dạng base64 data URL cho OpenAI
    """
    image_bytes = download_image_from_facebook(image_url)
    
    if image_bytes:
        base64_data = convert_image_to_base64(image_bytes)
        if base64_data:
            print("✅ Sử dụng ảnh base64")
            return base64_data
    
    print("⚠️ Fallback: Sử dụng URL trực tiếp")
    return image_url

# ============================================
# GPT-4o VISION: PHÂN TÍCH ẢNH SẢN PHẨM
# ============================================

def analyze_image_with_gpt4o(image_url: str):
    """
    Phân tích ảnh sản phẩm thời trang/gia dụng bằng GPT-4o Vision API
    """
    if not client or not OPENAI_API_KEY:
        print("⚠️ OpenAI client chưa được cấu hình, bỏ qua phân tích ảnh")
        return None
    
    try:
        print(f"🖼️ Đang phân tích ảnh: {image_url[:100]}...")
        
        image_content = get_image_for_analysis(image_url)
        
        if not image_content:
            print("❌ Không thể lấy được ảnh để phân tích")
            return None
        
        if image_content.startswith('data:'):
            image_message = {
                "type": "image_url",
                "image_url": {
                    "url": image_content
                }
            }
        else:
            image_message = {
                "type": "image_url",
                "image_url": {
                    "url": image_content
                }
            }
        
        improved_prompt = f"""Bạn là chuyên gia tư vấn thời trang và gia dụng cho {FANPAGE_NAME}.
        
Hãy phân tích ảnh sản phẩm và trả về JSON với cấu trúc:
{{
    "product_category": "Danh mục chính (ví dụ: quần áo, giày dép, túi xách, phụ kiện, đồ gia dụng)",
    "product_type": "Loại sản phẩm cụ thể (ví dụ: áo thun tay ngắn, quần jeans ống đứng, váy dài công sở, giày sneaker)",
    "main_color": "Màu sắc chính (tiếng Việt, mô tả chi tiết)",
    "secondary_colors": ["màu phụ 1", "màu phụ 2"],
    "pattern": "Họa tiết/hoa văn (ví dụ: trơn, sọc, kẻ caro, hoa, chấm bi)",
    "style": "Phong cách/kiểu dáng (ví dụ: casual, formal, vintage, hiện đại, thể thao)",
    "material": "Chất liệu (nếu nhận diện được, ví dụ: cotton, denim, lụa, len)",
    "features": ["Đặc điểm 1", "Đặc điểm 2", "Đặc điểm 3"],
    "season": "Mùa phù hợp (ví dụ: xuân hè, thu đông, cả năm)",
    "occasion": "Dịp sử dụng (ví dụ: đi làm, dự tiệc, đi chơi, ở nhà)",
    "description": "Mô tả chi tiết sản phẩm bằng tiếng Việt (3-4 câu)",
    "search_keywords": ["từ khóa tìm kiếm 1", "từ khóa 2", "từ khóa 3", "từ khóa 4", "từ khóa 5", "từ khóa 6", "từ khóa 7", "từ khóa 8"],
    "confidence_score": 0.95
}}

QUY TẮC QUAN TRỌNG:
1. PHÂN TÍCH KỸ những gì thấy trong ảnh: hình dáng, kiểu dáng, chi tiết, màu sắc, họa tiết
2. product_type phải CỤ THẾ và CHI TIẾT
3. search_keywords phải đa dạng
4. features: liệt kê các đặc điểm nổi bật
5. Trả về CHỈ JSON, không có text nào khác
6. Dùng tiếng Việt cho tất cả các trường"""
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": improved_prompt
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "Hãy phân tích thật kỹ sản phẩm trong ảnh này, chú ý đến từng chi tiết:"},
                        image_message
                    ]
                }
            ],
            max_tokens=800,
            temperature=0.1,
            response_format={"type": "json_object"}
        )
        
        result_text = response.choices[0].message.content.strip()
        print(f"📊 Kết quả phân tích ảnh chi tiết: {result_text[:300]}...")
        
        analysis = json.loads(result_text)
        analysis["search_text"] = create_search_text_from_analysis(analysis)
        analysis["timestamp"] = time.time()
        analysis["image_url"] = image_url
        
        return analysis
        
    except Exception as e:
        print(f"❌ Lỗi phân tích ảnh với GPT-4o: {str(e)}")
        return None

def create_search_text_from_analysis(analysis: dict) -> str:
    """Tạo chuỗi tìm kiếm từ kết quả phân tích ảnh"""
    if not analysis:
        return ""
    
    search_parts = []
    
    if analysis.get("product_type"):
        search_parts.append(analysis["product_type"])
    
    if analysis.get("product_category"):
        search_parts.append(analysis["product_category"])
    
    if analysis.get("main_color"):
        search_parts.append(analysis["main_color"])
    
    if analysis.get("secondary_colors"):
        search_parts.extend(analysis["secondary_colors"])
    
    if analysis.get("pattern") and analysis["pattern"].lower() != "không có":
        search_parts.append(analysis["pattern"])
    
    if analysis.get("style"):
        search_parts.append(analysis["style"])
    
    if analysis.get("material") and analysis["material"].lower() != "không xác định":
        search_parts.append(analysis["material"])
    
    if analysis.get("features"):
        search_parts.extend(analysis["features"])
    
    if analysis.get("season"):
        search_parts.append(analysis["season"])
    
    if analysis.get("occasion"):
        search_parts.append(analysis["occasion"])
    
    if analysis.get("search_keywords"):
        search_parts.extend(analysis["search_keywords"])
    
    search_text = " ".join(search_parts)
    search_text_normalized = normalize_vietnamese(search_text.lower())
    
    stop_words = ["và", "hoặc", "của", "cho", "từ", "đến", "với", "có", "là", "ở", "trong", "trên", "dưới"]
    for word in stop_words:
        search_text_normalized = search_text_normalized.replace(f" {word} ", " ")
    
    return search_text_normalized

# ============================================
# TÌM SẢN PHẨM THEO TỪ KHÓA
# ============================================

def find_product_by_keywords(text: str) -> Optional[str]:
    """Tìm sản phẩm dựa trên từ khóa trong tin nhắn"""
    if not text or not PRODUCTS:
        return None
    
    text_lower = text.lower()
    normalized_text = normalize_vietnamese(text_lower)
    
    print(f"[KEYWORD SEARCH] Tìm sản phẩm cho: {text_lower}")
    
    # Ánh xạ từ khóa -> mã sản phẩm (có thể mở rộng)
    keyword_to_ms = {
        "váy và áo đỏ": "MS000004",
        "bộ váy và áo đỏ": "MS000004", 
        "áo đỏ": "MS000004",
        "set len": "MS000004",
        "váy liền": "MS000004",
        "len dáng dài": "MS000004",
        "che khuyết điểm": "MS000004",
        "nàng mũm mĩm": "MS000004",
    }
    
    # Kiểm tra ánh xạ trực tiếp
    for keyword, ms in keyword_to_ms.items():
        if keyword in normalized_text and ms in PRODUCTS:
            print(f"[KEYWORD MATCH] Tìm thấy qua ánh xạ: {keyword} -> {ms}")
            return ms
    
    # Tìm kiếm động trong tên và mô tả sản phẩm
    best_match = None
    best_score = 0
    
    for ms, product in PRODUCTS.items():
        score = 0
        
        # Tên sản phẩm
        product_name = product.get('Ten', '').lower()
        product_name_norm = normalize_vietnamese(product_name)
        
        # Mô tả
        product_desc = product.get('MoTa', '').lower()
        product_desc_norm = normalize_vietnamese(product_desc)
        
        # Màu sắc
        product_colors = product.get('màu (Thuộc tính)', '').lower()
        product_colors_norm = normalize_vietnamese(product_colors)
        
        # Tách các từ trong tin nhắn
        text_words = set(normalized_text.split())
        
        # Tính điểm cho tên sản phẩm
        for word in text_words:
            if len(word) > 2:  # Bỏ qua từ quá ngắn
                if word in product_name_norm:
                    score += 3
                if word in product_desc_norm:
                    score += 2
                if word in product_colors_norm:
                    score += 2
        
        # Ưu tiên sản phẩm có điểm cao nhất
        if score > best_score:
            best_score = score
            best_match = ms
    
    if best_match and best_score >= 2:  # Ngưỡng tối thiểu
        print(f"[KEYWORD SEARCH] Tìm thấy tốt nhất: {best_match} (điểm: {best_score})")
        return best_match
    
    return None

# ============================================
# TÌM SẢN PHẨM VỚI ĐỘ CHÍNH XÁC CAO
# ============================================

def create_product_search_text(product: dict) -> str:
    """Tạo chuỗi tìm kiếm cho sản phẩm từ dữ liệu"""
    search_parts = []
    
    if product.get('Ten'):
        search_parts.append(product['Ten'])
    
    if product.get('MoTa'):
        search_parts.append(product['MoTa'])
    
    if product.get("màu (Thuộc tính)"):
        search_parts.append(product["màu (Thuộc tính)"])
    
    if product.get("size (Thuộc tính)"):
        search_parts.append(product["size (Thuộc tính)"])
    
    variants = product.get("variants", [])
    for variant in variants:
        if variant.get("mau"):
            search_parts.append(variant["mau"])
        if variant.get("size"):
            search_parts.append(variant["size"])
    
    search_text = " ".join(search_parts)
    search_text_normalized = normalize_vietnamese(search_text.lower())
    
    return search_text_normalized

def calculate_text_similarity(text1: str, text2: str) -> float:
    """Tính độ tương đồng giữa hai văn bản sử dụng TF-IDF và cosine similarity"""
    if not text1 or not text2:
        return 0.0
    
    try:
        vectorizer = TfidfVectorizer()
        tfidf_matrix = vectorizer.fit_transform([text1, text2])
        similarity = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])[0][0]
        return float(similarity)
    except Exception as e:
        print(f"❌ Lỗi tính similarity: {str(e)}")
        return 0.0

def find_products_by_image_analysis_improved(uid: str, analysis: dict, limit: int = 5) -> List[Tuple[str, float]]:
    """
    Tìm sản phẩm phù hợp dựa trên phân tích ảnh
    Trả về dan sách (mã sản phẩm, điểm số) sắp xếp theo điểm giảm dần
    """
    if not analysis or not PRODUCTS:
        return []
    
    analysis_search_text = analysis.get("search_text", "")
    if not analysis_search_text:
        print("❌ Không có search text từ phân tích ảnh")
        return []
    
    print(f"🔍 Tìm kiếm với: {analysis_search_text[:200]}...")
    
    scored_products = []
    
    for ms, product in PRODUCTS.items():
        product_search_text = create_product_search_text(product)
        
        if not product_search_text:
            continue
        
        similarity_score = calculate_text_similarity(analysis_search_text, product_search_text)
        bonus_score = 0
        
        main_color = analysis.get("main_color", "").lower()
        if main_color:
            main_color_normalized = normalize_vietnamese(main_color)
            product_colors = product.get("màu (Thuộc tính)", "").lower()
            product_colors_normalized = normalize_vietnamese(product_colors)
            
            if main_color_normalized in product_colors_normalized:
                bonus_score += 0.3
        
        product_type = analysis.get("product_type", "").lower()
        if product_type:
            product_type_normalized = normalize_vietnamese(product_type)
            product_name = product.get('Ten', '').lower()
            product_name_normalized = normalize_vietnamese(product_name)
            
            type_words = product_type_normalized.split()
            name_words = set(product_name_normalized.split())
            
            matching_words = sum(1 for word in type_words if word in name_words)
            if matching_words > 0:
                bonus_score += (matching_words / len(type_words)) * 0.4
        
        features = analysis.get("features", [])
        if features:
            for feature in features:
                feature_normalized = normalize_vietnamese(feature.lower())
                if feature_normalized in product_search_text:
                    bonus_score += 0.1
        
        total_score = similarity_score + bonus_score
        
        if total_score > 0.1:
            scored_products.append({
                "ms": ms,
                "score": total_score,
                "similarity": similarity_score,
                "bonus": bonus_score,
                "product": product
            })
    
    scored_products.sort(key=lambda x: x["score"], reverse=True)
    top_products = [(item["ms"], item["score"]) for item in scored_products[:limit]]
    
    if scored_products:
        print(f"📊 Tìm thấy {len(scored_products)} sản phẩm có điểm > 0.1")
        for i, item in enumerate(scored_products[:3]):
            print(f"  {i+1}. {item['ms']}: {item['score']:.3f} (similarity: {item['similarity']:.3f}, bonus: {item['bonus']:.3f})")
            print(f"     Tên: {item['product'].get('Ten', '')[:50]}...")
    else:
        print("⚠️ Không tìm thấy sản phẩm nào có điểm > 0.1")
    
    return top_products

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
    
    # Xử lý nhiều định dạng phân cách
    parts = re.split(r'[,\n;|]+', raw)
    urls = []
    
    for p in parts:
        p = p.strip()
        if not p:
            continue
        
        # Loại bỏ các ký tự thừa
        p = re.sub(r'^[\'"\s]+|[\'"\s]+$', '', p)
        
        # Chấp nhận URL bắt đầu bằng http/https hoặc có chứa domain ảnh
        if re.match(r'^https?://', p) or any(domain in p.lower() for domain in [
            'alicdn.com', 'taobao', '1688.com', '.jpg', '.jpeg', 
            '.png', '.webp', '.gif', 'image', 'img', 'photo'
        ]):
            urls.append(p)
    
    # Loại bỏ trùng lặp nhưng giữ thứ tự
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
    Mỗi dòng = 1 biến thể, lưu ảnh tương ứng cho từng variant.
    """
    global PRODUCTS, LAST_LOAD, PRODUCTS_BY_NUMBER, PRODUCT_TEXT_EMBEDDINGS
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
        product_text_embeddings = {}

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

            # Lấy ảnh đầu tiên của dòng này
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
                    "FullRow": row,
                }
                base["variants"] = []
                base["all_colors"] = set()
                base["all_sizes"] = set()
                base["all_images"] = {}  # Dictionary: "mau_size" -> image_url
                products[ms] = base

            p = products[ms]

            variant = {
                "mau": mau,
                "size": size,
                "gia": gia_int,
                "gia_raw": gia_raw,
                "tonkho": tonkho_int if tonkho_int is not None else tonkho_raw,
                "images": images,  # Lưu toàn bộ chuỗi ảnh
                "variant_image": variant_image,  # Ảnh đầu tiên của variant này
            }
            p["variants"].append(variant)

            # Thêm ảnh vào dictionary với key là "mau_size"
            key = f"{mau}_{size}" if mau and size else f"{mau}" if mau else f"{size}" if size else "default"
            if variant_image:  # Chỉ thêm nếu có ảnh
                p["all_images"][key] = variant_image

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
            
            product_text = create_product_search_text(p)
            product_text_embeddings[ms] = product_text
            
            if ms.startswith("MS"):
                num_part = ms[2:]
                num_without_leading_zeros = num_part.lstrip('0')
                if num_without_leading_zeros:
                    products_by_number[num_without_leading_zeros] = ms

        PRODUCTS = products
        PRODUCTS_BY_NUMBER = products_by_number
        PRODUCT_TEXT_EMBEDDINGS = product_text_embeddings
        LAST_LOAD = now
        
        total_variants = sum(len(p['variants']) for p in products.values())
        variants_with_images = sum(1 for p in products.values() for v in p['variants'] if v.get('variant_image'))
        
        print(f"📦 Loaded {len(PRODUCTS)} products với {total_variants} variants.")
        print(f"📊 Variants có ảnh: {variants_with_images}/{total_variants} ({(variants_with_images/total_variants*100):.1f}%)")
        print(f"🔢 Created mapping for {len(PRODUCTS_BY_NUMBER)} product numbers")
        print(f"🔤 Created text embeddings for {len(PRODUCT_TEXT_EMBEDDINGS)} products")
        
        # Debug: In thông tin variants của một sản phẩm
        if PRODUCTS:
            sample_ms = list(PRODUCTS.keys())[0]
            sample_product = PRODUCTS[sample_ms]
            print(f"📊 Sample product {sample_ms}: {len(sample_product['variants'])} variants")
            for i, v in enumerate(sample_product['variants'][:3], 1):
                print(f"  Variant {i}: {v.get('mau')}/{v.get('size')} - Ảnh: {v.get('variant_image', '')[:50]}...")
                
    except Exception as e:
        print("❌ load_products ERROR:", e)

def get_variant_image(ms: str, color: str, size: str) -> str:
    """
    Tìm ảnh của variant dựa trên màu và size
    """
    if ms not in PRODUCTS:
        return ""
    
    product = PRODUCTS[ms]
    variants = product.get("variants", [])
    
    # Tìm variant khớp chính xác
    for variant in variants:
        variant_color = variant.get("mau", "").strip().lower()
        variant_size = variant.get("size", "").strip().lower()
        
        input_color = color.strip().lower()
        input_size = size.strip().lower()
        
        # So sánh màu và size (bỏ qua case và khoảng trắng)
        color_match = (not input_color) or (variant_color == input_color) or (input_color == "mặc định" and not variant_color)
        size_match = (not input_size) or (variant_size == input_size) or (input_size == "mặc định" and not variant_size)
        
        if color_match and size_match:
            variant_image = variant.get("variant_image", "")
            if variant_image:
                return variant_image
    
    # Nếu không tìm thấy variant khớp, thử tìm variant với màu hoặc size khớp một phần
    for variant in variants:
        variant_color = variant.get("mau", "").strip().lower()
        variant_size = variant.get("size", "").strip().lower()
        
        input_color = color.strip().lower()
        input_size = size.strip().lower()
        
        # Nếu có màu và khớp màu, bất kể size
        if input_color and input_color != "mặc định" and variant_color == input_color:
            variant_image = variant.get("variant_image", "")
            if variant_image:
                return variant_image
        
        # Nếu có size và khớp size, bất kể màu
        if input_size and input_size != "mặc định" and variant_size == input_size:
            variant_image = variant.get("variant_image", "")
            if variant_image:
                return variant_image
    
    # Fallback: Lấy ảnh đầu tiên từ sản phẩm
    images_field = product.get("Images", "")
    urls = parse_image_urls(images_field)
    return urls[0] if urls else ""

# ============================================
# OPENAI FUNCTION CALLING (TÍCH HỢP TỪ AI_STUDIO_CODE)
# ============================================

def get_tools_definition():
    """Định nghĩa các công cụ cho OpenAI Function Calling"""
    return [
        {
            "type": "function",
            "function": {
                "name": "get_product_info",
                "description": "Lấy thông tin chi tiết sản phẩm (giá, mô tả, màu sắc) khi khách hỏi hoặc khi cần tư vấn.",
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
                "description": "Gửi ảnh thật của sản phẩm cho khách xem.",
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
                "description": "Cung cấp link form đặt hàng khi khách muốn mua, chốt đơn hoặc đặt hàng.",
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
                "name": "show_featured_carousel",
                "description": "Hiển thị danh sách các sản phẩm mới hoặc nổi bật dưới dạng thẻ quay.",
                "parameters": {"type": "object", "properties": {}}
            }
        }
    ]

def execute_tool(uid, name, args):
    """Thực thi công cụ được gọi bởi OpenAI"""
    ctx = USER_CONTEXT[uid]
    ms = args.get("ms", "").upper() or ctx.get("last_ms")
    domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"

    if name == "get_product_info":
        if ms in PRODUCTS:
            ctx["last_ms"] = ms
            send_product_info_debounced(uid, ms)
            return "Đã gửi thông tin sản phẩm."
        return "Sản phẩm không tồn tại."

    elif name == "send_product_images":
        if ms in PRODUCTS:
            send_all_product_images(uid, ms)
            return "Đã gửi ảnh thành công."
        return "Sản phẩm này hiện chưa có ảnh mẫu."

    elif name == "provide_order_link":
        if ms in PRODUCTS:
            link = f"{domain}/order-form?ms={ms}&uid={uid}"
            send_message(uid, f"Dạ mời anh/chị đặt hàng sản phẩm [{ms}] tại đây nhé:\n{link}")
            return "Đã gửi link đặt hàng."
        return "Em chưa rõ mã sản phẩm khách muốn đặt."

    elif name == "show_featured_carousel":
        elements = []
        for code, p in list(PRODUCTS.items())[:5]:
            urls = parse_image_urls(p["Images"])
            elements.append({
                "title": f"[{code}] {p['Ten']}",
                "image_url": urls[0] if urls else "",
                "subtitle": f"Giá: {p['Gia']}\nBấm chi tiết để xem thêm ảnh.",
                "buttons": [
                    {"type": "web_url", "url": f"{domain}/order-form?ms={code}&uid={uid}", "title": "🛒 Đặt ngay"},
                    {"type": "postback", "title": "🔍 Chi tiết", "payload": f"ADVICE_{code}"}
                ]
            })
        send_carousel_template(uid, elements)
        return "Đã hiển thị danh sách sản phẩm."
    
    return "Hành động không xác định."

def handle_text_with_function_calling(uid: str, text: str):
    """Xử lý tin nhắn bằng OpenAI Function Calling"""
    load_products()
    ctx = USER_CONTEXT[uid]
    
    # ƯU TIÊN: Sử dụng last_ms từ context nếu đã có (trừ khi tin nhắn chứa mã mới)
    current_ms = ctx.get("last_ms")
    
    # Nếu tin nhắn không chứa mã mới và đã có last_ms, giữ nguyên last_ms
    msg_lower = text.lower()
    detected_ms = detect_ms_from_text(text)
    
    if not detected_ms and current_ms and current_ms in PRODUCTS:
        print(f"[CONTEXT PRIORITY] Giữ nguyên last_ms từ context: {current_ms}")
        # KHÔNG thay đổi last_ms, giữ nguyên context hiện tại
    elif detected_ms and detected_ms in PRODUCTS:
        # Nếu có mã mới trong tin nhắn, cập nhật context
        print(f"[NEW MS DETECTED] Phát hiện mã mới: {detected_ms}")
        ctx["last_ms"] = detected_ms
        update_product_context(uid, detected_ms)
        current_ms = detected_ms
    
    fanpage_name = get_fanpage_name_from_api()
    
    # Thêm thông tin last_ms vào system prompt
    system_prompt = f"""Bạn là nhân viên bán hàng của {fanpage_name}.
    CHỈ trả lời dựa trên dữ liệu thật. KHÔNG bịa đặt thông tin.
    Nếu khách hỏi tồn kho, luôn khẳng định CÒN HÀNG.
    Xưng em, gọi anh/chị. Trả lời cực ngắn gọn (dưới 3 dòng).
    
    **QUAN TRỌNG: Sản phẩm khách đang quan tâm là: {current_ms or 'Chưa xác định'}**
    **Nếu khách hỏi về sản phẩm khác (có mã mới), mới chuyển sang sản phẩm đó**
    
    Khi khách muốn mua hoặc chốt, dùng công cụ provide_order_link với mã sản phẩm {current_ms}.
    
    KHI KHÁCH HỎI:
    - "xem sản phẩm" → dùng tool show_featured_carousel
    - "ảnh", "hình", "xem ảnh", "gửi ảnh" → dùng tool send_product_images với mã {current_ms}
    - "còn hàng nào khác", "có mẫu nào khác" → hướng dẫn vào Facebook Shop (không dùng tool)
    
    **NẾU KHÁCH HỎI CHUNG CHUNG (ví dụ: "giá bao nhiêu?") → LUÔN TRẢ LỜI VỀ SẢN PHẨM {current_ms}**
    **NẾU KHÁCH HỎI VỀ SẢN PHẨM KHÁC (có đề cập mã mới) → mới chuyển sang sản phẩm đó**"""

    messages = [{"role": "system", "content": system_prompt}]
    for h in ctx["conversation_history"][-6:]: 
        messages.append(h)
    messages.append({"role": "user", "content": text})

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            tools=get_tools_definition(),
            tool_choice="auto",
            temperature=0.1
        )
        
        msg = response.choices[0].message
        if msg.tool_calls:
            messages.append(msg)
            for tool in msg.tool_calls:
                res = execute_tool(uid, tool.function.name, json.loads(tool.function.arguments))
                messages.append({"role": "tool", "tool_call_id": tool.id, "name": tool.function.name, "content": res})
            
            final_res = client.chat.completions.create(
                model="gpt-4o-mini", 
                messages=messages,
                temperature=0.1
            )
            reply = final_res.choices[0].message.content
        else:
            reply = msg.content

        if reply:
            send_message(uid, reply)
            ctx["conversation_history"].append({"role": "user", "content": text})
            ctx["conversation_history"].append({"role": "assistant", "content": reply})
            ctx["conversation_history"] = ctx["conversation_history"][-10:]

    except Exception as e:
        print(f"Chat Error: {e}")
        send_message(uid, "Dạ em đang gặp chút trục trặc, anh/chị vui lòng thử lại sau ạ.")

# ============================================
# CẢI THIỆN NGỮ CẢNH - THÊM HỖ TRỢ CATALOG
# ============================================

def update_product_context(uid: str, ms: str):
    """Cập nhật ngữ cảnh sản phẩm cho user - GHI NHỚ LỊCH SỬ"""
    ctx = USER_CONTEXT[uid]
    
    # Cập nhật last_ms
    ctx["last_ms"] = ms
    
    # Cập nhật lịch sử sản phẩm
    if "product_history" not in ctx:
        ctx["product_history"] = []
    
    # Chỉ thêm nếu chưa có hoặc không phải sản phẩm cuối cùng
    if not ctx["product_history"] or ctx["product_history"][0] != ms:
        # Loại bỏ nếu đã có trong lịch sử
        if ms in ctx["product_history"]:
            ctx["product_history"].remove(ms)
        
        # Thêm vào đầu danh sách
        ctx["product_history"].insert(0, ms)
    
    # Giới hạn lịch sử (5 sản phẩm gần nhất)
    if len(ctx["product_history"]) > 5:
        ctx["product_history"] = ctx["product_history"][:5]
    
    print(f"[CONTEXT UPDATE] User {uid}: last_ms={ms}, history={ctx['product_history']}")

def get_relevant_product_for_question(uid: str, text: str) -> str | None:
    """Tìm sản phẩm phù hợp nhất cho câu hỏi dựa trên ngữ cảnh"""
    ctx = USER_CONTEXT[uid]
    lower = text.lower()
    
    # 1. Tìm mã sản phẩm trong tin nhắn
    ms_from_text = detect_ms_from_text(text)
    if ms_from_text and ms_from_text in PRODUCTS:
        print(f"[CONTEXT] Phát hiện mã mới trong tin nhắn: {ms_from_text}")
        return ms_from_text
    
    # 2. Sử dụng retailer_id từ catalog
    retailer_id = ctx.get("last_retailer_id")
    if retailer_id:
        ms_from_retailer = extract_ms_from_retailer_id(retailer_id)
        if ms_from_retailer and ms_from_retailer in PRODUCTS:
            print(f"[CATALOG CONTEXT] Sử dụng retailer_id {retailer_id} -> {ms_from_retailer}")
            return ms_from_retailer
    
    # 3. Sử dụng last_ms từ context (ƯU TIÊN CAO)
    last_ms = ctx.get("last_ms")
    if last_ms and last_ms in PRODUCTS:
        print(f"[CONTEXT] Sử dụng last_ms từ context: {last_ms}")
        return last_ms
    
    # 4. Sử dụng product history
    product_history = ctx.get("product_history", [])
    for ms in product_history:
        if ms in PRODUCTS:
            print(f"[CONTEXT] Sử dụng từ product history: {ms}")
            return ms
    
    # 5. Tìm theo từ khóa trong sản phẩm
    found_ms = find_product_by_keywords(text)
    if found_ms and found_ms in PRODUCTS:
        print(f"[CONTEXT] Tìm thấy sản phẩm theo từ khóa: {found_ms}")
        return found_ms
    
    return None

# ============================================
# HÀM HELPER MỚI: TÓM TẮT MÔ TẢ SẢN PHẨM
# ============================================

def format_product_description(description: str) -> str:
    """
    Tóm tắt mô tả sản phẩm bằng 5 gạch đầu dòng
    """
    if not description:
        return "📝 Sản phẩm hiện chưa có thông tin chi tiết ạ."
    
    # Làm sạch văn bản
    clean_desc = re.sub(r'\s+', ' ', description).strip()
    
    # Tách thành các câu dựa trên dấu câu
    sentences = re.split(r'[.!?]+', clean_desc)
    sentences = [s.strip() for s in sentences if len(s.strip()) > 10]
    
    if not sentences:
        return "📝 Sản phẩm hiện chưa có thông tin chi tiết ạ."
    
    # Lấy tối đa 5 câu
    sentences = sentences[:5]
    
    # Format thành gạch đầu dòng
    formatted = "📝 MÔ TẢ SẢN PHẨM:\n"
    for i, sentence in enumerate(sentences, 1):
        formatted += f"• {sentence}\n"
    
    return formatted.strip()

# ============================================
# HÀM HELPER MỚI: PHÂN TÍCH GIÁ THEO BIẾN THỂ
# ============================================

def analyze_variant_prices(variants: list) -> str:
    """
    Phân tích và định dạng giá theo các biến thể
    """
    if not variants:
        return "💰 Giá đang cập nhật, vui lòng liên hệ shop để biết chi tiết"
    
    # Nhóm biến thể theo giá
    price_groups = {}
    for variant in variants:
        price = variant.get("gia")
        if not price:
            continue
            
        color = variant.get("mau", "").strip()
        size = variant.get("size", "").strip()
        
        if price not in price_groups:
            price_groups[price] = []
        
        if color and size:
            price_groups[price].append(f"{color} - {size}")
        elif color:
            price_groups[price].append(color)
        elif size:
            price_groups[price].append(size)
        else:
            price_groups[price].append("Mặc định")
    
    if not price_groups:
        return "💰 Giá đang cập nhật, vui lòng liên hệ shop để biết chi tiết"
    
    # Phân tích mẫu giá
    price_list = list(price_groups.keys())
    
    # Trường hợp 1: Tất cả cùng một giá
    if len(price_list) == 1:
        price = price_list[0]
        return f"💰 Giá ưu đãi: {price:,.0f}đ"
    
    # Phân tích xem giá thay đổi theo màu hay theo size
    color_price_map = {}
    size_price_map = {}
    
    for variant in variants:
        price = variant.get("gia")
        color = variant.get("mau", "").strip()
        size = variant.get("size", "").strip()
        
        if not price:
            continue
        
        if color:
            if color not in color_price_map:
                color_price_map[color] = set()
            color_price_map[color].add(price)
        
        if size:
            if size not in size_price_map:
                size_price_map[size] = set()
            size_price_map[size].add(price)
    
    # Trường hợp 2: Giá thay đổi theo màu (các size có cùng giá cho mỗi màu)
    color_based = True
    for color, prices in color_price_map.items():
        if len(prices) > 1:  # Nếu một màu có nhiều giá khác nhau
            color_based = False
            break
    
    if color_based and color_price_map:
        result = "💰 GIÁ SẢN PHẨM:\n"
        for color, prices in color_price_map.items():
            if prices:
                price = list(prices)[0]
                result += f"{color}: {price:,.0f}đ\n"
        return result.strip()
    
    # Trường hợp 3: Giá thay đổi theo size (các màu có cùng giá cho mỗi size)
    size_based = True
    for size, prices in size_price_map.items():
        if len(prices) > 1:  # Nếu một size có nhiều giá khác nhau
            size_based = False
            break
    
    if size_based and size_price_map:
        result = "💰 GIÁ SẢN PHẨM:\n"
        for size, prices in size_price_map.items():
            if prices:
                price = list(prices)[0]
                result += f"{size}: {price:,.0f}đ\n"
        return result.strip()
    
    # Trường hợp 4: Giá thay đổi phức tạp theo cả màu và size
    result = "💰 GIÁ SẢN PHẨM:\n"
    for price, items in sorted(price_groups.items()):
        if len(items) <= 5:
            items_str = ", ".join(items)
        else:
            items_str = ", ".join(items[:5]) + f" và {len(items)-5} phân loại khác"
        result += f"{price:,.0f}đ cho các phân loại: {items_str}\n"
    
    return result.strip()

# ============================================
# SEND PRODUCT INFO - ĐÃ SỬA THEO YÊU CẦU
# ============================================

def send_product_info_debounced(uid: str, ms: str):
    """Gửi thông tin chi tiết sản phẩm theo cấu trúc tuần tự"""
    ctx = USER_CONTEXT[uid]
    now = time.time()

    # KIỂM TRA DEBOUNCE NÂNG CAO - TĂNG LÊN 15 GIÂY
    last_ms = ctx.get("product_info_sent_ms")
    last_time = ctx.get("last_product_info_time", 0)
    
    # Debounce: 15 giây cho cùng sản phẩm
    if last_ms == ms and (now - last_time) < 15:
        print(f"[PRODUCT INFO STRICT DEBOUNCE] Đã gửi {ms} trong 15s, bỏ qua")
        send_message(uid, f"Em vừa gửi thông tin sản phẩm [{ms}] rồi ạ. Anh/chị cần hỏi thêm gì không?")
        return
    
    # Debounce: 5 giây cho bất kỳ sản phẩm nào
    if (now - last_time) < 5:
        print(f"[GLOBAL PRODUCT DEBOUNCE] Chưa đủ 5s kể từ lần gửi sản phẩm cuối")
        return
    
    # SET LOCK TRƯỚC KHI XỬ LÝ
    if ctx.get("product_info_atomic_lock"):
        print(f"[PRODUCT ATOMIC LOCK] Không thể gửi, atomic lock đang active")
        return
    
    ctx["product_info_atomic_lock"] = True
    
    try:
        load_products()
        product = PRODUCTS.get(ms)
        if not product:
            send_message(uid, "Em không tìm thấy sản phẩm này trong hệ thống, anh/chị kiểm tra lại mã giúp em ạ.")
            ctx["product_info_atomic_lock"] = False
            return

        # **QUAN TRỌNG: Cập nhật context khi gửi sản phẩm mới**
        ctx["last_ms"] = ms
        update_product_context(uid, ms)

        product_name = product.get('Ten', 'Sản phẩm')
        
        # ============================================
        # PHẦN 1: TIÊU ĐỀ SẢN PHẨM
        # ============================================
        print(f"[PRODUCT INFO] Bắt đầu gửi phần 1 cho {ms}")
        send_message(uid, f"📌 {product_name}")
        time.sleep(0.8)  # Chờ gửi xong phần 1
        
        # ============================================
        # PHẦN 2: ẢNH SẢN PHẨM (5 ảnh không trùng)
        # ============================================
        print(f"[PRODUCT INFO] Bắt đầu gửi phần 2 cho {ms}")
        images_field = product.get("Images", "")
        urls = parse_image_urls(images_field)
        
        # Lọc ảnh không trùng và hợp lệ
        unique_images = []
        seen = set()
        for u in urls:
            if u and u not in seen:
                seen.add(u)
                # Kiểm tra URL hợp lệ
                url_lower = u.lower()
                if any(domain in url_lower for domain in [
                    'alicdn.com', 'taobao', '1688.com', 'http', 
                    '.jpg', '.jpeg', '.png', '.webp', '.gif',
                    'image', 'img', 'photo', 'static'
                ]):
                    unique_images.append(u)
        
        ctx["last_product_images_sent"][ms] = len(unique_images[:5])
        
        sent_count = 0
        last_send_time = 0
        
        for image_url in unique_images[:5]:
            if image_url:
                # KIỂM TRA DEBOUNCE GIỮA CÁC ẢNH
                current_time = time.time()
                if current_time - last_send_time < 0.5:
                    wait_time = 0.5 - (current_time - last_send_time)
                    time.sleep(wait_time)
                
                try:
                    print(f"[PRODUCT IMAGE] Gửi ảnh {sent_count+1}/5: {image_url[:80]}...")
                    result = send_image(uid, image_url)
                    
                    if result:
                        sent_count += 1
                        last_send_time = time.time()
                    
                    # Delay giữa các ảnh
                    time.sleep(0.8)
                    
                except Exception as e:
                    print(f"❌ Lỗi khi gửi ảnh: {str(e)}")
                    time.sleep(1.0)  # Delay lâu hơn nếu có lỗi
                    continue
        
        if sent_count == 0:
            send_message(uid, "📷 Sản phẩm chưa có hình ảnh ạ.")
            time.sleep(0.5)
        
        print(f"[PRODUCT INFO] Đã gửi xong phần 2: {sent_count} ảnh")
        time.sleep(0.8)  # Chờ trước khi gửi phần 3
        
        # ============================================
        # PHẦN 3: MÔ TẢ SẢN PHẨM (5 gạch đầu dòng)
        # ============================================
        print(f"[PRODUCT INFO] Bắt đầu gửi phần 3 cho {ms}")
        mo_ta = product.get("MoTa", "")
        description_msg = format_product_description(mo_ta)
        send_message(uid, description_msg)
        print(f"[PRODUCT INFO] Đã gửi xong phần 3")
        time.sleep(0.8)  # Chờ trước khi gửi phần 4
        
        # ============================================
        # PHẦN 4: GIÁ SẢN PHẨM (Phân tích theo biến thể)
        # ============================================
        print(f"[PRODUCT INFO] Bắt đầu gửi phần 4 cho {ms}")
        variants = product.get("variants", [])
        price_msg = analyze_variant_prices(variants)
        send_message(uid, price_msg)
        print(f"[PRODUCT INFO] Đã gửi xong phần 4")
        time.sleep(0.8)  # Chờ trước khi gửi phần 5
        
        # ============================================
        # PHẦN 5: LINK ĐẶT HÀNG
        # ============================================
        print(f"[PRODUCT INFO] Bắt đầu gửi phần 5 cho {ms}")
        domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
        order_link = f"{domain}/order-form?ms={ms}&uid={uid}"
        send_message(uid, f"📋 Đặt hàng ngay tại đây:\n{order_link}")
        print(f"[PRODUCT INFO] Đã gửi xong phần 5")

        # CẬP NHẬT THỜI GIAN SAU KHI GỬI XONG
        ctx["product_info_sent_ms"] = ms
        ctx["last_product_info_time"] = now

    except Exception as e:
        print(f"❌ Lỗi khi gửi thông tin sản phẩm: {str(e)}")
        try:
            # Fallback: Gửi thông tin đơn giản khi có lỗi
            send_message(uid, f"📌 Sản phẩm: {product.get('Ten', '')}")
            domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
            order_link = f"{domain}/order-form?ms={ms}&uid={uid}"
            send_message(uid, f"Có lỗi khi tải thông tin chi tiết. Vui lòng truy cập link dưới đây để đặt hàng:\n{order_link}")
        except:
            pass
    finally:
        # LUÔN RELEASE LOCK
        ctx["product_info_atomic_lock"] = False
        ctx["last_lock_release_time"] = now
        print(f"[PRODUCT INFO] Hoàn tất gửi thông tin cho {ms}, đã release lock")

# ============================================
# HANDLE IMAGE - VERSION CẢI TIẾN ĐỘ CHÍNH XÁC
# ============================================

def handle_image(uid: str, image_url: str):
    """Xử lý ảnh sản phẩm - gửi carousel với 5 sản phẩm phù hợp nhất"""
    if not client or not OPENAI_API_KEY:
        send_message(uid, "📷 Em đã nhận được ảnh! Hiện AI đang bảo trì, anh/chị vui lòng gửi mã sản phẩm để em tư vấn ạ.")
        return
    
    ctx = USER_CONTEXT[uid]
    
    now = time.time()
    last_image_time = ctx.get("last_image_time", 0)
    if now - last_image_time < 3:
        print(f"[IMAGE DEBOUNCE] Bỏ qua ảnh mới, chưa đủ thời gian")
        return
    
    ctx["last_image_time"] = now
    
    send_message(uid, "🖼️ Em đang phân tích ảnh sản phẩm của anh/chị...")
    
    try:
        analysis = analyze_image_with_gpt4o(image_url)
        
        if not analysis:
            send_message(uid, "❌ Em chưa phân tích được ảnh này. Anh/chị có thể mô tả sản phẩm hoặc gửi mã sản phẩm được không ạ?")
            return
        
        ctx["last_image_analysis"] = analysis
        ctx["last_image_url"] = image_url
        ctx["referral_source"] = "image_upload_analyzed"
        
        matched_products = find_products_by_image_analysis_improved(uid, analysis, limit=5)
        
        if matched_products and len(matched_products) > 0:
            product_type = analysis.get("product_type", "sản phẩm")
            main_color = analysis.get("main_color", "")
            confidence = analysis.get("confidence_score", 0)
            
            if main_color:
                analysis_msg = f"🎯 Em phân tích được đây là **{product_type}** màu **{main_color}**"
            else:
                analysis_msg = f"🎯 Em phân tích được đây là **{product_type}**"
            
            if confidence > 0.8:
                analysis_msg += " (độ chính xác cao)"
            elif confidence > 0.6:
                analysis_msg += " (khá chính xác)"
            
            send_message(uid, analysis_msg)
            
            if len(matched_products) == 1:
                send_message(uid, f"🔍 Em tìm thấy 1 sản phẩm phù hợp với ảnh của anh/chị:")
            else:
                send_message(uid, f"🔍 Em tìm thấy {len(matched_products)} sản phẩm phù hợp với ảnh của anh/chị:")
            
            carousel_elements = []
            
            for i, (ms, score) in enumerate(matched_products[:5], 1):
                if ms in PRODUCTS:
                    product = PRODUCTS[ms]
                    
                    images_field = product.get("Images", "")
                    urls = parse_image_urls(images_field)
                    image_url_carousel = urls[0] if urls else ""
                    
                    short_desc = product.get("ShortDesc", "") or short_description(product.get("MoTa", ""))
                    
                    gia_raw = product.get("Gia", "")
                    gia_int = extract_price_int(gia_raw)
                    price_display = f"{gia_int:,.0f}đ" if gia_int else "Liên hệ"
                    
                    match_percentage = min(int(score * 100), 99)
                    subtitle = f"🟢 Phù hợp: {match_percentage}% | 💰 {price_display}"
                    if short_desc:
                        subtitle += f" | {short_desc[:60]}{'...' if len(short_desc) > 60 else ''}"
                    
                    element = {
                        "title": f"[{ms}] {product.get('Ten', '')}",
                        "image_url": image_url_carousel,
                        "subtitle": subtitle,
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
                send_message(uid, "💬 Bấm 'Xem chi tiết' để xem thông tin và chính sách cụ thể của từng sản phẩm.")
                
                first_ms = matched_products[0][0]
                ctx["last_ms"] = first_ms
                update_product_context(uid, first_ms)
            else:
                send_message(uid, "❌ Em không tìm thấy sản phẩm nào phù hợp với ảnh này.")
                send_fallback_suggestions(uid)
            
        else:
            product_type = analysis.get("product_type", "sản phẩm")
            main_color = analysis.get("main_color", "")
            
            if main_color:
                send_message(uid, f"🔍 Em phân tích được đây là {product_type} màu {main_color}")
            else:
                send_message(uid, f"🔍 Em phân tích được đây là {product_type}")
            
            send_message(uid, "Hiện em chưa tìm thấy sản phẩm khớp 100% trong kho.")
            send_fallback_suggestions(uid)
    
    except Exception as e:
        print(f"❌ Lỗi xử lý ảnh: {str(e)}")
        send_message(uid, "❌ Em gặp lỗi khi phân tích ảnh. Anh/chị vui lòng thử lại hoặc gửi mã sản phẩm để em tư vấn ạ!")

def send_fallback_suggestions(uid: str):
    """Gửi gợi ý fallback khi không tìm thấy sản phẩm phù hợp"""
    send_message(uid, "Anh/chị có thể:")
    send_message(uid, "1. Gửi thêm ảnh góc khác của sản phẩm")
    send_message(uid, "2. Gõ 'xem sản phẩm' để xem toàn bộ dan mục")
    send_message(uid, "3. Mô tả chi tiết hơn về sản phẩm này")
    send_message(uid, "4. Hoặc gửi mã sản phẩm nếu anh/chị đã biết mã")

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
# DETECT MS FROM TEXT (TÍCH HỢP TỪ AI_STUDIO_CODE)
# ============================================

def detect_ms_from_text(text: str) -> Optional[str]:
    """Tìm mã sản phẩm trong tin nhắn"""
    if not text: 
        return None
    
    # Ưu tiên MSxxxxxx
    m = re.search(r"MS(\d{2,6})", text.upper())
    if m: 
        full_ms = "MS" + m.group(1).zfill(6)
        return full_ms if full_ms in PRODUCTS else None
    
    # Tìm số đơn thuần
    nums = re.findall(r"\b(\d{2,6})\b", text)
    for n in nums:
        clean_n = n.lstrip("0")
        if clean_n in PRODUCTS_BY_NUMBER: 
            return PRODUCTS_BY_NUMBER[clean_n]
    
    return None

# ============================================
# HANDLE TEXT - XỬ LÝ VỚI FUNCTION CALLING
# ============================================

def handle_text(uid: str, text: str):
    """Xử lý tin nhắn văn bản từ người dùng - SIMPLIFIED VERSION"""
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
        
        # Debounce: kiểm tra tin nhắn trùng lặp - TĂNG TỪ 1s LÊN 2s
        if now - last_msg_time < 2:
            last_text = ctx.get("last_processed_text", "")
            if text.strip().lower() == last_text.lower():
                print(f"[TEXT DEBOUNCE] Bỏ qua tin nhắn trùng lặp: {text[:50]}...")
                ctx["processing_lock"] = False
                return
        
        ctx["last_msg_time"] = now
        ctx["last_processed_text"] = text.strip().lower()
        
        load_products()
        ctx["postback_count"] = 0

        # KIỂM TRA XEM CÓ PHẢI TIN NHẮN ĐẦU TIÊN SAU REFERRAL KHÔNG
        # Nếu có pending_carousel_ms, gửi carousel 1 sản phẩm thay vì dùng function calling
        pending_ms = ctx.get("pending_carousel_ms")
        first_message = ctx.get("first_message_after_referral", False)
        
        if pending_ms and pending_ms in PRODUCTS and first_message:
            print(f"[FIRST MESSAGE] User {uid} gửi tin nhắn đầu tiên sau referral, gửi carousel cho {pending_ms}")
            
            # Gửi carousel 1 sản phẩm
            send_single_product_carousel(uid, pending_ms)
            
            # QUAN TRỌNG: Cập nhật context ngay lập tức
            ctx["last_ms"] = pending_ms
            update_product_context(uid, pending_ms)
            
            # Xóa trạng thái pending
            ctx["pending_carousel_ms"] = None
            ctx["first_message_after_referral"] = False
            
            # KHÔNG xử lý tin nhắn này bằng function calling
            ctx["processing_lock"] = False
            return
        
        # Nếu không phải first message, tiếp tục xử lý bình thường
        if handle_order_form_step(uid, text):
            ctx["processing_lock"] = False
            return
        
        # ƯU TIÊN: Xử lý follow-up từ catalog
        if handle_catalog_followup(uid, text):
            ctx["processing_lock"] = False
            return
        
        # ƯU TIÊN: Xử lý tin nhắn sau click quảng cáo ADS
        if handle_ads_referral_product(uid, text):
            ctx["processing_lock"] = False
            return

        lower = text.lower()
        
        # ƯU TIÊN 1: Xử lý từ khóa đặt hàng TRƯỚC
        if any(kw in lower for kw in ORDER_KEYWORDS):
            # TÌM SẢN PHẨM PHÙ HỢP - ƯU TIÊN CONTEXT HIỆN TẠI
            current_ms = ctx.get("last_ms")
            detected_ms = detect_ms_from_text(text)
            
            # Nếu tin nhắn KHÔNG chứa mã mới và đã có current_ms → giữ nguyên
            if not detected_ms and current_ms and current_ms in PRODUCTS:
                print(f"[CONTEXT PRIORITY] Giữ nguyên sản phẩm hiện tại: {current_ms}")
                # KHÔNG thay đổi context
            elif detected_ms and detected_ms in PRODUCTS:
                # Nếu có mã mới → cập nhật context
                print(f"[NEW MS DETECTED] Chuyển sang sản phẩm mới: {detected_ms}")
                ctx["last_ms"] = detected_ms
                update_product_context(uid, detected_ms)
                current_ms = detected_ms
            else:
                # Tìm sản phẩm phù hợp nhất
                current_ms = get_relevant_product_for_question(uid, text)
                
                # Nếu tìm thấy sản phẩm mới và khác với last_ms hiện tại
                if current_ms and current_ms in PRODUCTS and current_ms != ctx.get("last_ms"):
                    print(f"[CONTEXT UPDATE] Cập nhật last_ms từ {ctx.get('last_ms')} -> {current_ms}")
                    ctx["last_ms"] = current_ms
                    update_product_context(uid, current_ms)
            
            if current_ms and current_ms in PRODUCTS:
                domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
                order_link = f"{domain}/order-form?ms={current_ms}&uid={uid}"
                
                # Trích xuất màu/size đơn giản
                color, size = extract_color_size_simple(text)
                variant_info = ""
                if color or size:
                    variant_info = f" ({color if color else ''}{' - ' if color and size else ''}{size if size else ''})"
                
                # Reply cực ngắn - LUÔN BÁO CÒN HÀNG
                reply = f"Dạ, sản phẩm{variant_info} còn hàng ạ!\nĐặt tại: {order_link}"
                send_message(uid, reply)
                
                # Cập nhật context
                ctx["last_ms"] = current_ms
                update_product_context(uid, current_ms)
                
                ctx["processing_lock"] = False
                return
        
        # TÌM SẢN PHẨM PHÙ HỢP - ƯU TIÊN CONTEXT HIỆN TẠI
        current_ms = ctx.get("last_ms")
        detected_ms = detect_ms_from_text(text)
        
        # Nếu tin nhắn KHÔNG chứa mã mới và đã có current_ms → giữ nguyên
        if not detected_ms and current_ms and current_ms in PRODUCTS:
            print(f"[CONTEXT PRIORITY] Giữ nguyên sản phẩm hiện tại: {current_ms}")
            # KHÔNG thay đổi context
        elif detected_ms and detected_ms in PRODUCTS:
            # Nếu có mã mới → cập nhật context
            print(f"[NEW MS DETECTED] Chuyển sang sản phẩm mới: {detected_ms}")
            ctx["last_ms"] = detected_ms
            update_product_context(uid, detected_ms)
            current_ms = detected_ms
        else:
            # Nếu không có mã mới và cũng không có last_ms, mới đi tìm sản phẩm phù hợp
            current_ms = get_relevant_product_for_question(uid, text)
            if current_ms and current_ms in PRODUCTS and current_ms != ctx.get("last_ms"):
                print(f"[CONTEXT UPDATE] Cập nhật last_ms từ {ctx.get('last_ms')} -> {current_ms}")
                ctx["last_ms"] = current_ms
                update_product_context(uid, current_ms)
        
        # **QUAN TRỌNG: Cập nhật context nếu tìm thấy sản phẩm**
        if current_ms and current_ms in PRODUCTS and current_ms != ctx.get("last_ms"):
            print(f"[CONTEXT UPDATE] Cập nhật last_ms từ {ctx.get('last_ms')} -> {current_ms}")
            ctx["last_ms"] = current_ms
            update_product_context(uid, current_ms)
        
        # Sử dụng Function Calling để xử lý tin nhắn (từ tin nhắn thứ 2 trở đi)
        print(f"[FUNCTION CALLING] User: {uid}, MS: {current_ms}, Text: {text}")
        handle_text_with_function_calling(uid, text)

    except Exception as e:
        print(f"Error in handle_text for {uid}: {e}")
        try:
            send_message(uid, "Dạ em đang gặp chút trục trặc, anh/chị vui lòng thử lại sau ạ.")
        except:
            pass
    finally:
        if ctx.get("processing_lock"):
            ctx["processing_lock"] = False

# ============================================
# GOOGLE SHEETS API FUNCTIONS
# ============================================

def get_google_sheets_service():
    """
    Khởi tạo và trả về đối tượng service của Google Sheets API.
    Sử dụng Service Account credentials từ biến môi trường.
    """
    if not GOOGLE_SHEETS_CREDENTIALS_JSON or not GOOGLE_SHEET_ID:
        print("⚠️ Cảnh báo: Chưa cấu hình đầy đủ GOOGLE_SHEETS_CREDENTIALS_JSON hoặc GOOGLE_SHEET_ID.")
        return None

    if not GOOGLE_API_AVAILABLE:
        print("⚠️ Google API libraries chưa được cài đặt. Hãy thêm vào requirements.txt: google-api-python-client google-auth google-auth-oauthlib google-auth-httplib2")
        return None

    try:
        # Parse JSON credentials từ biến môi trường
        creds_dict = json.loads(GOOGLE_SHEETS_CREDENTIALS_JSON)
        
        # Tạo credentials từ Service Account
        credentials = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        
        # Xây dựng dịch vụ Google Sheets
        service = build('sheets', 'v4', credentials=credentials)
        print("✅ Đã khởi tạo Google Sheets service thành công.")
        return service
        
    except json.JSONDecodeError as e:
        print(f"❌ Lỗi định dạng JSON trong GOOGLE_SHEETS_CREDENTIALS_JSON: {e}")
    except Exception as e:
        print(f"❌ Lỗi không mong muốn khi khởi tạo Google Sheets service: {e}")
    
    return None

def write_order_to_google_sheet_api(order_data: dict):
    """
    GHI ĐƠN HÀNG VÀO GOOGLE SHEET 'Orders' BẰNG GOOGLE SHEETS API.
    
    Args:
        order_data: Dictionary chứa toàn bộ thông tin đơn hàng.
        
    Returns:
        bool: True nếu ghi thành công, False nếu thất bại.
    """
    # Lấy service
    service = get_google_sheets_service()
    if service is None:
        print("❌ Không thể ghi vì không khởi tạo được Google Sheets Service.")
        return False
    
    # Tên sheet (tab) mục tiêu trong Google Sheet
    sheet_name = "Orders"
    
    try:
        # 1. Chuẩn bị dữ liệu hàng (row) mới
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        order_id = f"ORD{int(time.time())}_{order_data.get('uid', '')[-4:]}"
        
        new_row = [
            timestamp,                              # Cột A: Thời gian
            order_id,                               # Cột B: Mã đơn
            "Mới",                                  # Cột C: Trạng thái
            order_data.get("ms", ""),               # D: Mã SP
            order_data.get("product_name", ""),     # E: Tên SP
            order_data.get("color", ""),            # F: Màu
            order_data.get("size", ""),             # G: Size
            order_data.get("quantity", 1),          # H: Số lượng
            order_data.get("unit_price", 0),        # I: Đơn giá
            order_data.get("total_price", 0),       # J: Tổng tiền
            order_data.get("customer_name", ""),    # K: Tên KH
            order_data.get("phone", ""),            # L: SĐT
            order_data.get("address", ""),          # M: Địa chỉ đầy đủ
            order_data.get("province", ""),         # N: Tỉnh
            order_data.get("district", ""),         # O: Quận
            order_data.get("ward", ""),             # P: Phường
            order_data.get("address_detail", ""),   # Q: Chi tiết địa chỉ
            "COD",                                  # R: Thanh toán
            "ViettelPost",                          # S: Vận chuyển
            f"Đơn từ Facebook Bot ({order_data.get('referral_source', 'direct')})", # T: Ghi chú
            order_data.get("uid", ""),              # U: Facebook UID
            order_data.get("referral_source", "direct") # V: Nguồn
        ]
        
        # 2. Gọi API để thêm dòng mới vào cuối sheet
        request = service.spreadsheets().values().append(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=f"{sheet_name}!A:V",  # Ghi vào các cột A đến V
            valueInputOption="USER_ENTERED",  # Dữ liệu được xử lý như người dùng nhập
            insertDataOption="INSERT_ROWS",   # Luôn chèn hàng mới
            body={"values": [new_row]}
        )
        
        response = request.execute()
        
        print(f"✅ ĐÃ GHI ĐƠN HÀNG VÀO GOOGLE SHEET THÀNH CÔNG!")
        print(f"   - Mã đơn: {order_id}")
        print(f"   - Sheet: {sheet_name}")
        print(f"   - Ô được cập nhật: {response.get('updates', {}).get('updatedCells', 'N/A')}")
        
        return True
        
    except HttpError as err:
        # Xử lý lỗi đặc trưng từ Google API
        print(f"❌ Lỗi Google Sheets API khi ghi đơn:")
        print(f"   - Mã lỗi: {err.resp.status}")
        print(f"   - Nội dung: {err.error_details if hasattr(err, 'error_details') else err}")
        
        # Gợi ý khắc phục dựa trên mã lỗi phổ biến 
        if err.resp.status == 403:
            print("   ⚠️ Gợi ý: Service Account có thể chưa được chia sẻ quyền 'Editor' cho Google Sheet này.")
        elif err.resp.status == 404:
            print(f"   ⚠️ Gợi ý: Không tìm thấy Sheet ID '{GOOGLE_SHEET_ID}' hoặc tab '{sheet_name}'. Hãy kiểm tra lại.")
        
    except Exception as e:
        print(f"❌ Lỗi không xác định khi gọi Google Sheets API: {type(e).__name__}: {e}")
    
    return False

def save_order_to_local_csv(order_data: dict):
    """
    Lưu đơn hàng vào file CSV local (backup khi không ghi được Google Sheet)
    """
    try:
        file_path = "orders_backup.csv"
        file_exists = os.path.exists(file_path)
        
        # Chuẩn bị dữ liệu
        timestamp = datetime.now().strftime("%Y-%m%d %H:%M:%S")
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
# IMPROVED: XỬ LÝ POSTBACK VỚI RECOVERY MECHANISM
# ============================================

def handle_postback_with_recovery(uid: str, payload: str, postback_id: str = None):
    """
    Xử lý postback với cơ chế recovery và duplicate protection mạnh mẽ
    - SỬA LẠI HOÀN TOÀN: Tập trung vào idempotency và in-memory lock cho 1 worker
    """
    now = time.time()
    
    # TẠO IDEMPOTENCY KEY - QUAN TRỌNG: Kết hợp uid, payload và postback_id
    if postback_id:
        idempotency_key = f"{uid}_{payload}_{postback_id}"
    else:
        # Nếu không có postback_id, tạo từ timestamp để unique
        idempotency_key = f"{uid}_{payload}_{int(now)}"
    
    ctx = USER_CONTEXT[uid]
    
    # TẠO DICTIONARY CHO IDEMPOTENCY NẾU CHƯA CÓ
    if "idempotent_postbacks" not in ctx:
        ctx["idempotent_postbacks"] = {}
    
    # KIỂM TRA IDEMPOTENCY MẠNH MẼ - 30 GIÂY (QUAN TRỌNG)
    if idempotency_key in ctx["idempotent_postbacks"]:
        processed_time = ctx["idempotent_postbacks"][idempotency_key]
        if now - processed_time < 30:  # TĂNG LÊN 30 GIÂY
            print(f"[IDEMPOTENCY BLOCK] Bỏ qua postback đã xử lý: {idempotency_key}")
            return True
    
    # ĐÁNH DẤU NGAY LẬP TỨC TRƯỚC KHI XỬ LÝ
    ctx["idempotent_postbacks"][idempotency_key] = now
    
    # GIỚI HẠN SIZE CỦA IDEMPOTENCY DICT (tránh memory leak)
    if len(ctx["idempotent_postbacks"]) > 100:
        # Giữ lại 50 postback gần nhất
        sorted_items = sorted(ctx["idempotent_postbacks"].items(), 
                            key=lambda x: x[1], reverse=True)[:50]
        ctx["idempotent_postbacks"] = dict(sorted_items)
    
    # Lấy global lock
    lock = get_postback_lock(uid, payload)
    
    # Thử acquire lock với timeout
    if not lock.acquire(timeout=POSTBACK_LOCK_TIMEOUT):
        print(f"[POSTBACK GLOBAL LOCK] Không thể acquire lock, bỏ qua")
        # Xóa idempotency key vì không xử lý được
        if idempotency_key in ctx["idempotent_postbacks"]:
            del ctx["idempotent_postbacks"][idempotency_key]
        return False
    
    try:
        # KIỂM TRA DEBOUNCE THEO PAYLOAD (giữ nguyên)
        last_payload = ctx.get("last_postback_payload")
        last_payload_time = ctx.get("last_postback_time", 0)
        
        if payload == last_payload and (now - last_payload_time) < 3:
            print(f"[PAYLOAD DEBOUNCE] Bỏ qua payload trùng trong 3s: {payload}")
            return True
        
        # SET LOCK VỚI TIMEOUT
        if ctx.get("processing_lock"):
            lock_time = ctx.get("processing_lock_time", 0)
            # Nếu lock quá 15 giây → force release
            if now - lock_time > 15:
                print(f"[LOCK RECOVERY] Force release lock sau 15s cho user {uid}")
                ctx["processing_lock"] = False
            else:
                print(f"[POSTBACK LOCKED] User {uid} đang được xử lý, bỏ qua")
                return False
        
        # XỬ LÝ POSTBACK
        ctx["processing_lock"] = True
        ctx["processing_lock_time"] = now
        ctx["last_postback_payload"] = payload
        ctx["last_postback_time"] = now
        
        load_products()
        
        if payload.startswith("ADVICE_"):
            ms = payload.replace("ADVICE_", "")
            if ms in PRODUCTS:
                # KIỂM TRA DEBOUNCE: không gửi thông tin sản phẩm quá nhanh
                last_info_ms = ctx.get("product_info_sent_ms")
                last_info_time = ctx.get("last_product_info_time", 0)
                
                # TĂNG DEBOUNCE LÊN 30 GIÂY ĐỂ CHẮC CHẮN
                if last_info_ms == ms and (now - last_info_time) < 30:
                    print(f"[PRODUCT INFO STRICT DEBOUNCE] Đã gửi sản phẩm {ms} trong 30s, bỏ qua")
                    # KHÔNG gửi tin nhắn trả lời để tránh loop
                    return True
                
                ctx["last_ms"] = ms
                update_product_context(uid, ms)
                send_product_info_debounced(uid, ms)
                return True
            else:
                send_message(uid, "❌ Em không tìm thấy sản phẩm này. Anh/chị vui lòng kiểm tra lại mã sản phẩm ạ.")
                return True
                
        elif payload.startswith("ORDER_"):
            ms = payload.replace("ORDER_", "")
            if ms in PRODUCTS:
                ctx["last_ms"] = ms
                update_product_context(uid, ms)
                domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
                order_link = f"{domain}/order-form?ms={ms}&uid={uid}"
                product_name = PRODUCTS[ms].get('Ten', '')
                send_message(uid, f"🎯 Anh/chị chọn sản phẩm [{ms}] {product_name}!\n\n📋 Đặt hàng ngay tại đây:\n{order_link}")
            else:
                send_message(uid, "❌ Em không tìm thấy sản phẩm này. Anh/chị vui lòng kiểm tra lại mã sản phẩm ạ.")
            return True
            
        elif payload.startswith("VIEW_IMAGES_"):
            ms = payload.replace("VIEW_IMAGES_", "")
            if ms in PRODUCTS:
                # KIỂM TRA DEBOUNCE: không gửi ảnh quá nhanh
                last_images_time = ctx.get("last_all_images_time", 0)
                if now - last_images_time < 10:
                    print(f"[IMAGES DEBOUNCE] Đã gửi ảnh trong 10s, bỏ qua")
                    # KHÔNG gửi tin nhắn trả lời
                    return True
                
                ctx["last_ms"] = ms
                update_product_context(uid, ms)
                send_all_product_images(uid, ms)
                return True
            else:
                send_message(uid, "❌ Em không tìm thấy sản phẩm này. Anh/chị vui lòng kiểm tra lại mã sản phẩm ạ.")
                return True
            
        elif payload == "GET_STARTED":
            ctx["referral_source"] = "get_started"
            welcome_msg = f"""Chào anh/chị! 👋 
Em là trợ lý AI của {FANPAGE_NAME}.

Để em tư vấn chính xác, anh/chị vui lòng:
1. Gửi mã sản phẩm (ví dụ: [MS123456])
2. Hoặc gõ "xem sản phẩm" để xem danh sách
3. Hoặc mô tả sản phẩm bạn đang tìm

Anh/chị quan tâm sản phẩm nào ạ?"""
            send_message(uid, welcome_msg)
            return True
            
    except Exception as e:
        print(f"❌ Lỗi xử lý postback: {str(e)}")
        try:
            send_message(uid, "❌ Có lỗi xảy ra. Vui lòng thử lại sau ạ.")
        except:
            pass
    finally:
        # QUAN TRỌNG: LUÔN RELEASE LOCK TRONG FINALLY
        if ctx.get("processing_lock"):
            ctx["processing_lock"] = False
        
        # Release global lock
        lock.release()
        
        # Giữ lại thời gian release để debug
        ctx["last_lock_release_time"] = now
        
        # Cleanup old locks
        cleanup_old_locks()
    
    return False

# ============================================
# WEBHOOK HANDLER - CẢI THIỆN DUPLICATE DETECTION
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

            # ============================================
            # XỬ LÝ ATTACHMENT TEMPLATE TỪ CATALOG - LƯU RETAILER_ID
            # ============================================
            if "message" in m and "attachments" in m["message"]:
                attachments = m["message"]["attachments"]
                for att in attachments:
                    if att.get("type") == "template":
                        payload = att.get("payload", {})
                        # Kiểm tra xem có phải product template không
                        if "product" in payload:
                            product = payload["product"]
                            elements = product.get("elements", [])
                            if elements and len(elements) > 0:
                                element = elements[0]
                                retailer_id = element.get("retailer_id")
                                product_id = element.get("id")
                                
                                if retailer_id:
                                    ctx = USER_CONTEXT[sender_id]
                                    ctx["last_retailer_id"] = retailer_id
                                    ctx["last_product_id"] = product_id
                                    ctx["catalog_view_time"] = time.time()
                                    
                                    # Lưu vào catalog_products dict
                                    if "catalog_products" not in ctx:
                                        ctx["catalog_products"] = {}
                                    ctx["catalog_products"][product_id] = retailer_id
                                    
                                    # Giới hạn kích thước catalog_products
                                    if len(ctx["catalog_products"]) > 10:
                                        # Xóa phần tử cũ nhất
                                        oldest_key = list(ctx["catalog_products"].keys())[0]
                                        del ctx["catalog_products"][oldest_key]
                                    
                                    # Trích xuất mã sản phẩm từ retailer_id
                                    ms_from_retailer = extract_ms_from_retailer_id(retailer_id)
                                    if ms_from_retailer:
                                        ctx["last_catalog_product"] = ms_from_retailer
                                        ctx["last_ms"] = ms_from_retailer
                                        ctx["pending_carousel_ms"] = ms_from_retailer  # Đánh dấu cần gửi carousel
                                        ctx["first_message_after_referral"] = True
                                        update_product_context(sender_id, ms_from_retailer)
                                    
                                    print(f"[CATALOG] Lưu retailer_id: {retailer_id} -> MS: {ms_from_retailer} cho user {sender_id}")

            # ============================================
            # XỬ LÝ ECHO MESSAGE TỪ FCHAT - GIỮ NGUYÊN LOGIC TRÍCH XUẤT MÃ
            # ============================================
            if m.get("message", {}).get("is_echo"):
                # Lấy recipient_id (người nhận tin nhắn echo) - chính là khách hàng
                recipient_id = m.get("recipient", {}).get("id")
                if not recipient_id:
                    continue
                
                # Lấy thông tin echo message
                msg = m["message"]
                msg_mid = msg.get("mid")
                echo_text = msg.get("text", "")
                attachments = msg.get("attachments", [])
                app_id = msg.get("app_id", "")
                
                # **QUAN TRỌNG**: KIỂM TRA CÓ PHẢI ECHO TỪ BOT KHÔNG
                # Nếu là echo từ bot → BỎ QUA để tránh lặp
                if is_bot_generated_echo(echo_text, app_id, attachments):
                    print(f"[ECHO BOT] Bỏ qua echo message từ bot: {echo_text[:50]}...")
                    continue
                
                # **GIỮ NGUYÊN**: Kiểm tra duplicate echo message
                if msg_mid:
                    ctx = USER_CONTEXT[recipient_id]
                    if "processed_echo_mids" not in ctx:
                        ctx["processed_echo_mids"] = set()
                    
                    if msg_mid in ctx["processed_echo_mids"]:
                        print(f"[ECHO DUPLICATE] Bỏ qua echo message đã xử lý: {msg_mid}")
                        continue
                    
                    now = time.time()
                    last_echo_time = ctx.get("last_echo_processed_time", 0)
                    
                    if now - last_echo_time < 2:
                        print(f"[ECHO DEBOUNCE] Bỏ qua echo message, chưa đủ 2s: {msg_mid}")
                        continue
                    
                    ctx["last_echo_processed_time"] = now
                    ctx["processed_echo_mids"].add(msg_mid)
                    
                    if len(ctx["processed_echo_mids"]) > 20:
                        ctx["processed_echo_mids"] = set(list(ctx["processed_echo_mids"])[-20:])
                
                # **GIỮ NGUYÊN LOGIC CŨ**: Xử lý echo từ bình luận người dùng
                print(f"[ECHO USER] Đang xử lý echo từ bình luận người dùng")
                
                # QUAN TRỌNG: Load sản phẩm trước khi tìm mã
                load_products()
                
                # **GIỮ NGUYÊN**: Tìm mã sản phẩm trong tin nhắn echo
                detected_ms = detect_ms_from_text(echo_text)
                
                if detected_ms and detected_ms in PRODUCTS:
                    print(f"[ECHO FCHAT] Phát hiện mã sản phẩm: {detected_ms} cho user: {recipient_id}")
                    
                    # KIỂM TRA LOCK để tránh xử lý song song
                    ctx = USER_CONTEXT[recipient_id]
                    if ctx.get("processing_lock"):
                        print(f"[ECHO LOCKED] User {recipient_id} đang được xử lý, bỏ qua echo")
                        continue
                    
                    ctx["processing_lock"] = True
                    
                    try:
                        # **QUAN TRỌNG: Cập nhật context khi phát hiện mã từ Fchat echo**
                        ctx["last_ms"] = detected_ms
                        ctx["pending_carousel_ms"] = detected_ms  # Đánh dấu cần gửi carousel
                        ctx["first_message_after_referral"] = True
                        ctx["referral_source"] = "fchat_echo"
                        update_product_context(recipient_id, detected_ms)
                        
                        print(f"[CONTEXT UPDATED] Đã ghi nhận mã {detected_ms} vào ngữ cảnh cho user {recipient_id}")
                        
                    finally:
                        ctx["processing_lock"] = False
                else:
                    print(f"[ECHO FCHAT] Không tìm thấy mã sản phẩm trong echo: {echo_text[:100]}...")
                
                continue
            
            if m.get("delivery") or m.get("read"):
                continue
            
            # ============================================
            # XỬ LÝ REFERRAL (TỪ QUẢNG CÁO, FACEBOOK SHOP, CATALOG)
            # ============================================
            if m.get("referral"):
                ref = m["referral"]
                ctx = USER_CONTEXT[sender_id]
                ctx["referral_source"] = ref.get("source", "unknown")
                referral_payload = ref.get("ref", "")
                ctx["referral_payload"] = referral_payload
                
                print(f"[REFERRAL] User {sender_id} từ {ctx['referral_source']} với payload: {referral_payload}")
                
                handled = False
                
                # Xử lý đặc biệt cho ADS với catalog
                if ref.get("source") == "ADS" and ref.get("ads_context_data"):
                    ads_data = ref.get("ads_context_data", {})
                    ad_title = ads_data.get("ad_title", "")
                    
                    print(f"[ADS REFERRAL] Ad title: {ad_title}")
                    
                    # ƯU TIÊN 1: Trích xuất mã từ ad_title
                    ms_from_ad = extract_ms_from_ad_title(ad_title)
                    if msfrom_ad and ms_from_ad in PRODUCTS:
                        print(f"[ADS PRODUCT] Xác định sản phẩm từ ad_title: {ms_from_ad}")
                        
                        # KHÔNG reset context, mà update context với sản phẩm mới
                        ctx["last_ms"] = ms_from_ad
                        ctx["pending_carousel_ms"] = ms_from_ad  # Đánh dấu cần gửi carousel
                        ctx["first_message_after_referral"] = True
                        update_product_context(sender_id, ms_from_ad)
                        
                        # Gửi thông báo ngắn, KHÔNG gửi thông tin chi tiết
                        welcome_msg = f"""Chào anh/chị! 👋 
Em là trợ lý AI của {get_fanpage_name_from_api()}.

Em thấy anh/chị quan tâm đến sản phẩm **[{ms_from_ad}]** từ quảng cáo.
Để xem thông tin chi tiết, anh/chị vui lòng gửi tin nhắn bất kỳ ạ!"""
                        
                        send_message(sender_id, welcome_msg)
                        handled = True
                    
                    # ƯU TIÊN 2: Kiểm tra referral payload
                    if not handled and referral_payload:
                        detected_ms = detect_ms_from_text(referral_payload)
                        if detected_ms and detected_ms in PRODUCTS:
                            print(f"[ADS REFERRAL] Nhận diện mã từ payload: {detected_ms}")
                            ctx["last_ms"] = detected_ms
                            ctx["pending_carousel_ms"] = detected_ms  # Đánh dấu cần gửi carousel
                            ctx["first_message_after_referral"] = True
                            update_product_context(sender_id, detected_ms)
                            
                            welcome_msg = f"""Chào anh/chị! 👋 
Em là trợ lý AI của {get_fanpage_name_from_api()}.

Em thấy anh/chị quan tâm đến sản phẩm **[{detected_ms}]**.
Để xem thông tin chi tiết, anh/chị vui lòng gửi tin nhắn bất kỳ ạ!"""
                            
                            send_message(sender_id, welcome_msg)
                            handled = True
                
                # Nếu đã xử lý xong (ADS có sản phẩm) thì bỏ qua phần sau
                if handled:
                    continue
                
                # CHỈ reset context nếu KHÔNG phải từ ADS hoặc không xác định được sản phẩm
                if ctx.get("referral_source") != "ADS" or not ctx.get("last_ms"):
                    print(f"[REFERRAL RESET] Reset context cho user {sender_id}")
                    ctx["last_ms"] = None
                    ctx["product_history"] = []
                
                # Fallback: Xử lý referral bình thường
                if referral_payload:
                    detected_ms = detect_ms_from_text(referral_payload)
                    
                    if detected_ms and detected_ms in PRODUCTS:
                        print(f"[REFERRAL AUTO] Nhận diện mã sản phẩm từ referral: {detected_ms}")
                        
                        ctx["last_ms"] = detected_ms
                        ctx["pending_carousel_ms"] = detected_ms  # Đánh dấu cần gửi carousel
                        ctx["first_message_after_referral"] = True
                        update_product_context(sender_id, detected_ms)
                        
                        welcome_msg = f"""Chào anh/chị! 👋 
Em là trợ lý AI của {FANPAGE_NAME}.

Em thấy anh/chị quan tâm đến sản phẩm mã [{detected_ms}].
Để xem thông tin chi tiết, anh/chị vui lòng gửi tin nhắn bất kỳ ạ!"""
                        send_message(sender_id, welcome_msg)
                        continue
                    else:
                        welcome_msg = f"""Chào anh/chị! 👋 
Em là trợ lý AI của {FANPAGE_NAME}.

Để em tư vấn chính xác, anh/chị vui lòng:
1. Gửi mã sản phẩm (ví dụ: [MS123456])
2. Hoặc gõ "xem sản phẩm" để xem danh sách
3. Hoặc mô tả sản phẩm bạn đang tìm

Anh/chị quan tâm sản phẩm nào ạ?"""
                        send_message(sender_id, welcome_msg)
                        continue
            
            # ============================================
            # XỬ LÝ POSTBACK (GET_STARTED, ADVICE_, ORDER_)
            # ============================================
            if "postback" in m:
                payload = m["postback"].get("payload")
                if payload:
                    postback_id = m["postback"].get("mid")
                    
                    # KIỂM TRA NHANH TRƯỚC KHI XỬ LÝ
                    ctx = USER_CONTEXT.get(sender_id, {})
                    last_payload = ctx.get("last_postback_payload")
                    last_payload_time = ctx.get("last_postback_time", 0)
                    
                    now = time.time()
                    if payload == last_payload and (now - last_payload_time) < 1:
                        print(f"[WEBHOOK QUICK SKIP] Bỏ qua postback trùng trong 1s: {payload}")
                        continue  # Bỏ qua ngay lập tức
                    
                    # Sử dụng hàm xử lý mới
                    handle_postback_with_recovery(sender_id, payload, postback_id)
                    continue
            
            # ============================================
            # XỬ LÝ TIN NHẮN THƯỜNG (TEXT & ẢNH) - ĐÃ SỬA DUPLICATE CHECK 30s
            # ============================================
            if "message" in m:
                msg = m["message"]
                text = msg.get("text")
                attachments = msg.get("attachments") or []
                
                msg_mid = msg.get("mid")
                timestamp = m.get("timestamp", 0)
                
                if msg_mid:
                    ctx = USER_CONTEXT[sender_id]
                    if "processed_message_mids" not in ctx:
                        ctx["processed_message_mids"] = {}
                    
                    if msg_mid in ctx["processed_message_mids"]:
                        processed_time = ctx["processed_message_mids"][msg_mid]
                        now = time.time()
                        if now - processed_time < 30:  # TĂNG TỪ 3s LÊN 30s ĐỂ TRÁNH DUPLICATE
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

    # Lấy tên fanpage từ API
    current_fanpage_name = get_fanpage_name_from_api()
    
    row = PRODUCTS[ms]
    
    # Lấy ảnh mặc định (ảnh đầu tiên từ sản phẩm)
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

    # Tạo HTML với form địa chỉ sử dụng API miễn phí
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8" />
        <title>Đặt hàng - {row.get('Ten','')}</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
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
                transition: transform 0.3s ease;
            }}
            
            .product-image:hover {{
                transform: scale(1.05);
            }}
            
            .product-image.loading {{
                opacity: 0.7;
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
            
            .form-control:focus {{
                outline: none;
                border-color: #1DB954;
                box-shadow: 0 0 0 3px rgba(29, 185, 84, 0.1);
            }}
            
            .form-control:disabled {{
                background-color: #f8f9fa;
                cursor: not-allowed;
            }}
            
            .address-row {{
                display: flex;
                gap: 10px;
                margin-bottom: 10px;
            }}
            
            .address-col {{
                flex: 1;
            }}
            
            .address-preview {{
                margin-top: 15px;
                padding: 15px;
                background: #f8f9fa;
                border-radius: 10px;
                border-left: 4px solid #1DB954;
                display: none;
            }}
            
            .address-preview-content {{
                font-size: 13px;
                line-height: 1.5;
            }}
            
            .address-preview-content strong {{
                color: #444;
                display: block;
                margin-bottom: 5px;
            }}
            
            .address-preview-content p {{
                margin: 0;
                color: #666;
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
            
            .submit-btn:active {{
                transform: translateY(0);
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
                
                .header h2 {{
                    font-size: 18px;
                }}
                
                .total-amount {{
                    font-size: 22px;
                }}
            }}
            
            .error-message {{
                color: #FF3B30;
                font-size: 12px;
                margin-top: 5px;
                display: none;
            }}
            
            .form-control.error + .error-message {{
                display: block;
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
                        {"<img id='product-image' src='" + default_image + "' class='product-image' onerror=\"this.onerror=null; this.src='https://via.placeholder.com/120x120?text=No+Image'\" />" if default_image else "<div class='placeholder-image'>Chưa có ảnh sản phẩm</div>"}
                    </div>
                    <div class="product-info">
                        <div class="product-code">Mã: {ms}</div>
                        <h3 class="product-title">{row.get('Ten','')}</h3>
                        <div class="product-price" id="price-display">{price_int:,.0f} đ</div>
                    </div>
                </div>

                <!-- Order Form -->
                <form id="orderForm">
                    <!-- Color Selection -->
                    <div class="form-group">
                        <label for="color">Màu sắc:</label>
                        <select id="color" class="form-control">
                            {''.join(f"<option value='{c}'>{c}</option>" for c in colors)}
                        </select>
                    </div>

                    <!-- Size Selection -->
                    <div class="form-group">
                        <label for="size">Size:</label>
                        <select id="size" class="form-control">
                            {''.join(f"<option value='{s}'>{s}</option>" for s in sizes)}
                        </select>
                    </div>

                    <!-- Quantity -->
                    <div class="form-group">
                        <label for="quantity">Số lượng:</label>
                        <input type="number" id="quantity" class="form-control" value="1" min="1">
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

                    <!-- Address Section với Open API -->
                    <div class="form-group">
                        <label>Địa chỉ nhận hàng:</label>
                        
                        <div class="address-row">
                            <div class="address-col">
                                <select id="province" class="form-control" 
                                        onchange="loadDistricts(this.value)">
                                    <option value="">Chọn Tỉnh/Thành phố</option>
                                </select>
                            </div>
                            <div class="address-col">
                                <select id="district" class="form-control" disabled
                                        onchange="loadWards(this.value)">
                                    <option value="">Chọn Quận/Huyện</option>
                                </select>
                            </div>
                            <div class="address-col">
                                <select id="ward" class="form-control" disabled>
                                    <option value="">Chọn Phường/Xã</option>
                                </select>
                            </div>
                        </div>
                        
                        <div class="form-group" style="margin-top: 10px;">
                            <input type="text" id="addressDetail" class="form-control" 
                                   placeholder="Số nhà, tên đường, tòa nhà..." required>
                        </div>
                        
                        <!-- Address Preview -->
                        <div id="addressPreview" class="address-preview"></div>
                        
                        <input type="hidden" id="fullAddress" name="fullAddress">
                        <input type="hidden" id="provinceName">
                        <input type="hidden" id="districtName">
                        <input type="hidden" id="wardName">
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

        <script>
            // Global variables
            const PRODUCT_MS = "{ms}";
            const PRODUCT_UID = "{uid}";
            const BASE_PRICE = {price_int};
            const DOMAIN = "{'https://' + DOMAIN if not DOMAIN.startswith('http') else DOMAIN}";
            const API_BASE_URL = "{('/api' if DOMAIN.startswith('http') else 'https://' + DOMAIN + '/api')}";
            
            // ============================================
            // PRODUCT VARIANT HANDLING
            // ============================================
            
            function formatPrice(n) {{
                return n.toLocaleString('vi-VN') + ' đ';
            }}
            
            async function updateImageByVariant() {{
                const color = document.getElementById('color').value;
                const size = document.getElementById('size').value;
                const imageContainer = document.getElementById('image-container');
                
                // Show loading
                const currentImg = imageContainer.querySelector('img');
                if (currentImg) {{
                    currentImg.classList.add('loading');
                }}
                
                try {{
                    const res = await fetch(`${{API_BASE_URL}}/get-variant-image?ms=${{PRODUCT_MS}}&color=${{encodeURIComponent(color)}}&size=${{encodeURIComponent(size)}}`);
                    if (res.ok) {{
                        const data = await res.json();
                        if (data.image && data.image.trim() !== '') {{
                            let imgElement = imageContainer.querySelector('img');
                            if (!imgElement) {{
                                imgElement = document.createElement('img');
                                imgElement.className = 'product-image';
                                imgElement.onerror = function() {{
                                    this.onerror = null;
                                    this.src = 'https://via.placeholder.com/120x120?text=No+Image';
                                }};
                                imageContainer.innerHTML = '';
                                imageContainer.appendChild(imgElement);
                            }}
                            imgElement.src = data.image;
                        }} else {{
                            imageContainer.innerHTML = '<div class="placeholder-image">Chưa có ảnh cho thuộc tính này</div>';
                        }}
                    }}
                }} catch (e) {{
                    console.error('Error updating image:', e);
                }} finally {{
                    if (currentImg) {{
                        setTimeout(() => currentImg.classList.remove('loading'), 300);
                    }}
                }}
            }}
            
            async function updatePriceByVariant() {{
                const color = document.getElementById('color').value;
                const size = document.getElementById('size').value;
                const quantity = parseInt(document.getElementById('quantity').value || '1');

                try {{
                    const res = await fetch(`${{API_BASE_URL}}/get-variant-price?ms=${{PRODUCT_MS}}&color=${{encodeURIComponent(color)}}&size=${{encodeURIComponent(size)}}`);
                    if (res.ok) {{
                        const data = await res.json();
                        const price = data.price || BASE_PRICE;

                        document.getElementById('price-display').innerText = formatPrice(price);
                        document.getElementById('total-display').innerText = formatPrice(price * quantity);
                    }}
                }} catch (e) {{
                    document.getElementById('price-display').innerText = formatPrice(BASE_PRICE);
                    document.getElementById('total-display').innerText = formatPrice(BASE_PRICE * quantity);
                }}
            }}
            
            async function updateVariantInfo() {{
                await Promise.all([
                    updateImageByVariant(),
                    updatePriceByVariant()
                ]);
            }}
            
            // ============================================
            // VIETNAM ADDRESS API (Open API - provinces.open-api.vn)
            // ============================================
            
            // Load provinces từ Open API
            async function loadProvinces() {{
                const provinceSelect = document.getElementById('province');
                
                try {{
                    // Show loading
                    provinceSelect.innerHTML = '<option value="">Đang tải tỉnh/thành...</option>';
                    provinceSelect.disabled = true;
                    
                    const response = await fetch('https://provinces.open-api.vn/api/p/');
                    const data = await response.json();
                    
                    // Sắp xếp provinces theo tên
                    const provinces = data.sort((a, b) => 
                        a.name.localeCompare(b.name, 'vi')
                    );
                    
                    provinceSelect.innerHTML = '<option value="">Chọn Tỉnh/Thành phố</option>';
                    provinces.forEach(province => {{
                        const option = document.createElement('option');
                        option.value = province.code;
                        option.textContent = province.name;
                        provinceSelect.appendChild(option);
                    }});
                    
                    console.log(`✅ Đã tải ${{provinces.length}} tỉnh/thành phố từ Open API`);
                    
                    // Load preset address từ URL nếu có
                    loadPresetAddress();
                }} catch (error) {{
                    console.error('❌ Lỗi khi load tỉnh/thành:', error);
                    // Fallback to static list
                    loadStaticProvinces();
                }} finally {{
                    provinceSelect.disabled = false;
                }}
            }}
            
            // Load districts dựa trên selected province
            async function loadDistricts(provinceId) {{
                const districtSelect = document.getElementById('district');
                const wardSelect = document.getElementById('ward');
                
                if (!provinceId) {{
                    districtSelect.innerHTML = '<option value="">Chọn Quận/Huyện</option>';
                    wardSelect.innerHTML = '<option value="">Chọn Phường/Xã</option>';
                    districtSelect.disabled = true;
                    wardSelect.disabled = true;
                    updateFullAddress();
                    return;
                }}
                
                try {{
                    districtSelect.innerHTML = '<option value="">Đang tải quận/huyện...</option>';
                    districtSelect.disabled = true;
                    wardSelect.disabled = true;
                    
                    const response = await fetch(`https://provinces.open-api.vn/api/p/${{provinceId}}?depth=2`);
                    const provinceData = await response.json();
                    
                    const districts = provinceData.districts || [];
                    districts.sort((a, b) => a.name.localeCompare(b.name, 'vi'));
                    
                    districtSelect.innerHTML = '<option value="">Chọn Quận/Huyện</option>';
                    districts.forEach(district => {{
                        const option = document.createElement('option');
                        option.value = district.code;
                        option.textContent = district.name;
                        districtSelect.appendChild(option);
                    }});
                    
                    console.log(`✅ Đã tải ${{districts.length}} quận/huyện`);
                    districtSelect.disabled = false;
                    
                    // Clear wards
                    wardSelect.innerHTML = '<option value="">Chọn Phường/Xã</option>';
                    wardSelect.disabled = true;
                }} catch (error) {{
                    console.error('❌ Lỗi khi load quận/huyện:', error);
                    districtSelect.innerHTML = '<option value="">Lỗi tải dữ liệu</option>';
                }} finally {{
                    updateFullAddress();
                }}
            }}
            
            // Load wards dựa trên selected district
            async function loadWards(districtId) {{
                const wardSelect = document.getElementById('ward');
                
                if (!districtId) {{
                    wardSelect.innerHTML = '<option value="">Chọn Phường/Xã</option>';
                    wardSelect.disabled = true;
                    updateFullAddress();
                    return;
                }}
                
                try {{
                    wardSelect.innerHTML = '<option value="">Đang tải phường/xã...</option>';
                    wardSelect.disabled = true;
                    
                    const response = await fetch(`https://provinces.open-api.vn/api/d/${{districtId}}?depth=2`);
                    const districtData = await response.json();
                    
                    const wards = districtData.wards || [];
                    wards.sort((a, b) => a.name.localeCompare(b.name, 'vi'));
                    
                    wardSelect.innerHTML = '<option value="">Chọn Phường/Xã</option>';
                    wards.forEach(ward => {{
                        const option = document.createElement('option');
                        option.value = ward.code;
                        option.textContent = ward.name;
                        wardSelect.appendChild(option);
                    }});
                    
                    console.log(`✅ Đã tải ${{wards.length}} phường/xã`);
                    wardSelect.disabled = false;
                }} catch (error) {{
                    console.error('❌ Lỗi khi load phường/xã:', error);
                    wardSelect.innerHTML = '<option value="">Lỗi tải dữ liệu</option>';
                }} finally {{
                    updateFullAddress();
                }}
            }}
            
            // Fallback: Static province list
            function loadStaticProvinces() {{
                const staticProvinces = [
                    "An Giang", "Bà Rịa - Vũng Tàu", "Bắc Giang", "Bắc Kạn", "Bạc Liêu", 
                    "Bắc Ninh", "Bến Tre", "Bình Định", "Bình Dương", "Bình Phước", 
                    "Bình Thuận", "Cà Mau", "Cao Bằng", "Cần Thơ", "Đà Nẵng", 
                    "Đắk Lắk", "Đắk Nông", "Điện Biên", "Đồng Nai", "Đồng Tháp", 
                    "Gia Lai", "Hà Giang", "Hà Nam", "Hà Nội", "Hà Tĩnh", 
                    "Hải Dương", "Hải Phòng", "Hậu Giang", "Hòa Bình", "Hưng Yên", 
                    "Khánh Hòa", "Kiên Giang", "Kon Tum", "Lai Châu", "Lâm Đồng", 
                    "Lạng Sơn", "Lào Cai", "Long An", "Nam Định", "Nghệ An", 
                    "Ninh Bình", "Ninh Thuận", "Phú Thọ", "Phú Yên", "Quảng Bình", 
                    "Quảng Nam", "Quảng Ngãi", "Quảng Ninh", "Quảng Trị", "Sóc Trăng", 
                    "Sơn La", "Tây Ninh", "Thái Bình", "Thái Nguyên", "Thanh Hóa", 
                    "Thừa Thiên Huế", "Tiền Giang", "TP Hồ Chí Minh", "Trà Vinh", 
                    "Tuyên Quang", "Vĩnh Long", "Vĩnh Phúc", "Yên Bái"
                ];
                
                const provinceSelect = document.getElementById('province');
                provinceSelect.innerHTML = '<option value="">Chọn Tỉnh/Thành phố</option>';
                
                staticProvinces.forEach((province, index) => {{
                    const option = document.createElement('option');
                    option.value = index + 1;
                    option.textContent = province;
                    provinceSelect.appendChild(option);
                }});
                
                provinceSelect.disabled = false;
                console.log('⚠️ Đã tải danh sách tỉnh thành tĩnh (fallback)');
            }}
            
            // Update full address từ tất cả các components
            function updateFullAddress() {{
                const provinceText = document.getElementById('province').options[document.getElementById('province').selectedIndex]?.text || '';
                const districtText = document.getElementById('district').options[document.getElementById('district').selectedIndex]?.text || '';
                const wardText = document.getElementById('ward').options[document.getElementById('ward').selectedIndex]?.text || '';
                const detailText = document.getElementById('addressDetail').value || '';
                
                // Save to hidden fields
                document.getElementById('provinceName').value = provinceText;
                document.getElementById('districtName').value = districtText;
                document.getElementById('wardName').value = wardText;
                
                // Build full address
                const fullAddress = [detailText, wardText, districtText, provinceText]
                    .filter(part => part.trim() !== '')
                    .join(', ');
                
                document.getElementById('fullAddress').value = fullAddress;
                
                // Update preview
                const previewElement = document.getElementById('addressPreview');
                if (fullAddress.trim()) {{
                    previewElement.innerHTML = `
                        <div class="address-preview-content">
                            <strong>Địa chỉ nhận hàng:</strong>
                            <p>${{fullAddress}}</p>
                        </div>
                    `;
                    previewElement.style.display = 'block';
                }} else {{
                    previewElement.style.display = 'none';
                }}
                
                return fullAddress;
            }}
            
            // Load preset address từ URL parameters
            function loadPresetAddress() {{
                const urlParams = new URLSearchParams(window.location.search);
                const presetAddress = urlParams.get('address');
                
                if (presetAddress) {{
                    document.getElementById('addressDetail').value = presetAddress;
                    updateFullAddress();
                }}
            }}
            
            // ============================================
            // FORM VALIDATION AND SUBMISSION
            // ============================================
            
            async function submitOrder() {{
                // Collect form data
                const formData = {{
                    ms: PRODUCT_MS,
                    uid: PRODUCT_UID,
                    color: document.getElementById('color').value,
                    size: document.getElementById('size').value,
                    quantity: parseInt(document.getElementById('quantity').value || '1'),
                    customerName: document.getElementById('customerName').value.trim(),
                    phone: document.getElementById('phone').value.trim(),
                    address: updateFullAddress(),
                    provinceId: document.getElementById('province').value,
                    districtId: document.getElementById('district').value,
                    wardId: document.getElementById('ward').value,
                    provinceName: document.getElementById('provinceName').value,
                    districtName: document.getElementById('districtName').value,
                    wardName: document.getElementById('wardName').value,
                    addressDetail: document.getElementById('addressDetail').value.trim()
                }};
                
                // Validate required fields
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
                
                // Validate phone number
                const phoneRegex = /^(0|\+84)(\d{9,10})$/;
                if (!phoneRegex.test(formData.phone)) {{
                    alert('Số điện thoại không hợp lệ. Vui lòng nhập số điện thoại 10-11 chữ số');
                    document.getElementById('phone').focus();
                    return;
                }}
                
                // Validate address
                if (!formData.provinceId) {{
                    alert('Vui lòng chọn Tỉnh/Thành phố');
                    document.getElementById('province').focus();
                    return;
                }}
                
                if (!formData.districtId) {{
                    alert('Vui lòng chọn Quận/Huyện');
                    document.getElementById('district').focus();
                    return;
                }}
                
                if (!formData.wardId) {{
                    alert('Vui lòng chọn Phường/Xã');
                    document.getElementById('ward').focus();
                    return;
                }}
                
                if (!formData.addressDetail) {{
                    alert('Vui lòng nhập địa chỉ chi tiết (số nhà, tên đường)');
                    document.getElementById('addressDetail').focus();
                    return;
                }}
                
                // Show loading
                const submitBtn = document.getElementById('submitBtn');
                const originalText = submitBtn.innerHTML;
                submitBtn.innerHTML = '<span class="loading-spinner"></span> ĐANG XỬ LÝ...';
                submitBtn.disabled = true;
                
                try {{
                    const response = await fetch(`${{API_BASE_URL}}/submit-order`, {{
                        method: 'POST',
                        headers: {{
                            'Content-Type': 'application/json'
                        }},
                        body: JSON.stringify(formData)
                    }});
                    
                    const data = await response.json();
                    
                    if (response.ok) {{
                        // Success
                        alert('🎉 Đã gửi đơn hàng thành công!\\n\\nShop sẽ liên hệ xác nhận trong 5-10 phút.\\nCảm ơn anh/chị đã đặt hàng! ❤️');
                        
                        // Reset form (optional)
                        document.getElementById('customerName').value = '';
                        document.getElementById('phone').value = '';
                        document.getElementById('addressDetail').value = '';
                        document.getElementById('province').selectedIndex = 0;
                        document.getElementById('district').innerHTML = '<option value="">Chọn Quận/Huyện</option>';
                        document.getElementById('ward').innerHTML = '<option value="">Chọn Phường/Xã</option>';
                        document.getElementById('district').disabled = true;
                        document.getElementById('ward').disabled = true;
                        updateFullAddress();
                        
                    }} else {{
                        // Error
                        alert(`❌ ${{data.message || 'Có lỗi xảy ra. Vui lòng thử lại sau'}}`);
                    }}
                }} catch (error) {{
                    console.error('Lỗi khi gửi đơn hàng:', error);
                    alert('❌ Lỗi kết nối. Vui lòng thử lại sau!');
                }} finally {{
                    // Restore button
                    submitBtn.innerHTML = originalText;
                    submitBtn.disabled = false;
                }}
            }}
            
            // ============================================
            // INITIALIZATION
            // ============================================
            
            document.addEventListener('DOMContentLoaded', function() {{
                // Load provinces
                loadProvinces();
                
                // Event listeners for product variant changes
                document.getElementById('color').addEventListener('change', updateVariantInfo);
                document.getElementById('size').addEventListener('change', updateVariantInfo);
                document.getElementById('quantity').addEventListener('input', updatePriceByVariant);
                
                // Event listeners for address changes
                document.getElementById('province').addEventListener('change', function() {{
                    loadDistricts(this.value);
                    updateFullAddress();
                }});
                
                document.getElementById('district').addEventListener('change', function() {{
                    loadWards(this.value);
                    updateFullAddress();
                }});
                
                document.getElementById('ward').addEventListener('change', updateFullAddress);
                document.getElementById('addressDetail').addEventListener('input', updateFullAddress);
                
                // Initialize product variant info
                updateVariantInfo();
                
                // Enter key to submit form
                document.getElementById('orderForm').addEventListener('keypress', function(e) {{
                    if (e.which === 13) {{
                        e.preventDefault();
                        submitOrder();
                    }}
                }});
                
                // Focus on first field
                setTimeout(() => {{
                    document.getElementById('customerName').focus();
                }}, 500);
            }});
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

@app.route("/api/get-variant-image")
def api_get_variant_image():
    """API trả về ảnh tương ứng với màu và size"""
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
    address = data.get("address") or ""
    
    # Thêm các trường mới từ form địa chỉ
    province_name = data.get("provinceName", "")
    district_name = data.get("districtName", "")
    ward_name = data.get("wardName", "")
    address_detail = data.get("addressDetail", "")
    
    load_products()
    row = PRODUCTS.get(ms)
    if not row:
        return {"error": "not_found", "message": "Sản phẩm không tồn tại"}, 404

    price_str = row.get("Gia", "0")
    price_int = extract_price_int(price_str) or 0
    total = price_int * quantity
    
    product_name = row.get('Ten', '')

    if uid:
        # Lấy referral source từ context
        ctx = USER_CONTEXT.get(uid, {})
        referral_source = ctx.get("referral_source", "direct")
        
        # Tin nhắn chi tiết hơn với thông tin địa chỉ đầy đủ
        msg = (
            "🎉 Shop đã nhận được đơn hàng mới:\n"
            f"🛍 Sản phẩm: [{ms}] {product_name}\n"
            f"🎨 Phân loại: {color} / {size}\n"
            f"📦 Số lượng: {quantity}\n"
            f"💰 Thành tiền: {total:,.0f} đ\n"
            f"👤 Người nhận: {customer_name}\n"
            f"📱 SĐT: {phone}\n"
            f"🏠 Địa chỉ: {address}\n"
            f"📍 Chi tiết: {address_detail}\n"
            f"🗺️ Khu vực: {ward_name}, {district_name}, {province_name}\n"
            "────────────────────\n"
            "⏰ Shop sẽ gọi điện xác nhận trong 5-10 phút.\n"
            "🚚 Đơn hàng sẽ được giao bởi ViettelPost\n"
            "💳 Thanh toán khi nhận hàng (COD)\n"
            "────────────────────\n"
            "Cảm ơn anh/chị đã đặt hàng! ❤️"
        )
        send_message(uid, msg)
    
    # ============================================
    # GHI ĐƠN HÀNG VÀO GOOGLE SHEET QUA API
    # ============================================
    order_data = {
        "ms": ms,
        "uid": uid,
        "color": color,
        "size": size,
        "quantity": quantity,
        "customer_name": customer_name,
        "phone": phone,
        "address": address,
        "province": province_name,
        "district": district_name,
        "ward": ward_name,
        "address_detail": address_detail,
        "product_name": product_name,
        "unit_price": price_int,
        "total_price": total,
        "referral_source": ctx.get("referral_source", "direct")
    }
    
    # Ưu tiên 1: Ghi vào Google Sheet qua API
    write_success = write_order_to_google_sheet_api(order_data)
    
    # Fallback: Nếu không thành công, lưu vào file local backup
    if not write_success:
        print("⚠️ Ghi Google Sheet thất bại, thực hiện lưu vào file local backup...")
        save_order_to_local_csv(order_data)
    
    # Gửi notification đến Fchat webhook (nếu có)
    if FCHAT_WEBHOOK_URL and FCHAT_TOKEN:
        try:
            fchat_payload = {
                "token": FCHAT_TOKEN,
                "message": f"🛒 ĐƠN HÀNG MỚI\nMã: {ms}\nKH: {customer_name}\nSĐT: {phone}\nTổng: {total:,.0f}đ",
                "metadata": {
                    "order_data": order_data,
                    "timestamp": datetime.now().isoformat()
                }
            }
            requests.post(FCHAT_WEBHOOK_URL, json=fchat_payload, timeout=5)
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
            "customer_name": customer_name,
            "phone": phone,
            "address": address,
            "province": province_name,
            "district": district_name,
            "ward": ward_name,
            "total": total,
            "timestamp": datetime.now().isoformat()
        }
    }

@app.route("/static/<path:path>")
def static_files(path):
    return send_from_directory("static", path)

# ============================================
# HEALTH CHECK
# ============================================

@app.route("/health", methods=["GET"])
def health_check():
    """Kiểm tra tình trạng server và bot"""
    current_fanpage_name = get_fanpage_name_from_api()
    
    # Tính tổng số variants và variants có ảnh
    total_variants = 0
    variants_with_images = 0
    
    for ms, product in PRODUCTS.items():
        variants = product.get("variants", [])
        total_variants += len(variants)
        for variant in variants:
            if variant.get("variant_image"):
                variants_with_images += 1
    
    # Kiểm tra Google Sheets Service
    sheets_service_status = "Not Configured"
    if GOOGLE_SHEET_ID and GOOGLE_SHEETS_CREDENTIALS_JSON:
        try:
            service = get_google_sheets_service()
            if service:
                # Thử một thao tác đọc nhẹ để kiểm tra quyền
                result = service.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()
                sheet_title = result.get('properties', {}).get('title', 'Unknown')
                sheets_service_status = f"Connected to Sheet: '{sheet_title}' (ID: {GOOGLE_SHEET_ID[:10]}...)"
            else:
                sheets_service_status = "Service Initialization Failed"
        except Exception as e:
            sheets_service_status = f"Connection Error: {type(e).__name__}"
    
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "products_loaded": len(PRODUCTS),
        "variants_loaded": total_variants,
        "variants_with_images": variants_with_images,
        "variant_images_percentage": f"{(variants_with_images/total_variants*100):.1f}%" if total_variants > 0 else "0%",
        "last_load_time": LAST_LOAD,
        "openai_configured": bool(client),
        "openai_vision_available": bool(client and OPENAI_API_KEY),
        "facebook_configured": bool(PAGE_ACCESS_TOKEN),
        "fanpage_name": current_fanpage_name,
        "google_sheets_integration": {
            "method": "Official Google Sheets API v4",
            "sheet_id_configured": bool(GOOGLE_SHEET_ID),
            "credentials_configured": bool(GOOGLE_SHEETS_CREDENTIALS_JSON),
            "service_status": sheets_service_status,
            "order_write_logic": "Primary API -> Local CSV Backup"
        },
        "fchat_webhook": "Configured" if FCHAT_WEBHOOK_URL and FCHAT_TOKEN else "Not configured",
        "fanpage_name_source": "Facebook Graph API" if FANPAGE_NAME_CACHE and FANPAGE_NAME_CACHE != FANPAGE_NAME else "Environment Variable",
        "fanpage_cache_age": int(time.time() - FANPAGE_NAME_CACHE_TIME) if FANPAGE_NAME_CACHE_TIME else 0,
        "fanpage_cache_valid": (FANPAGE_NAME_CACHE_TIME and (time.time() - FANPAGE_NAME_CACHE_TIME) < FANPAGE_NAME_CACHE_TTL),
        "variant_image_support": "ENABLED (ảnh theo thuộc tính)",
        "variant_image_api": "/api/get-variant-image",
        "image_processing": "base64+fallback",
        "image_debounce_enabled": True,
        "image_carousel": "5_products",
        "search_algorithm": "TF-IDF_cosine_similarity",
        "accuracy_improved": True,
        "fchat_echo_processing": True,
        "bot_echo_filter": True,
        "catalog_support": "Enabled (retailer_id extraction)",
        "catalog_retailer_id_extraction": "MSxxxxxx_xx -> MSxxxxxx",
        "ads_referral_processing": "ENABLED (trích xuất mã từ ad_title)",
        "ads_context_handling": "ENABLED (không reset context khi có sản phẩm từ ADS)",
        "referral_auto_processing": True,
        "message_debounce_enabled": True,
        "duplicate_protection": True,
        "image_send_debounce": "5s",
        "image_request_processing": "Enabled with confidence > 0.85",
        "address_form": "Open API - provinces.open-api.vn (dropdown 3 cấp)",
        "address_validation": "enabled",
        "phone_validation": "regex validation",
        "order_response_mode": "SHORT - Chỉ báo còn hàng khi hỏi tồn kho",
        "price_detailed_response": "ENABLED (hiển thị chi tiết các biến thể giá)",
        "max_gpt_tokens": 150,
        "stock_assumption": "Chỉ báo khi hỏi tồn kho",
        "order_keywords_priority": "HIGH",
        "context_tracking": "ENABLED (tracks last_ms and product_history)",
        "facebook_shop_guidance": "ENABLED (hướng dẫn vào gian hàng khi yêu cầu sản phẩm khác)",
        "openai_function_calling": "ENABLED (tích hợp từ ai_studio_code.py)",
        "tools_available": [
            "get_product_info",
            "send_product_images", 
            "provide_order_link",
            "show_featured_carousel"
        ],
        "function_calling_model": "gpt-4o-mini",
        "system_prompt_optimized": "True",
        "conversation_history_tracking": "ENABLED (10 messages)",
        "first_message_carousel_feature": "ENABLED (gửi carousel 1 sản phẩm cho tin nhắn đầu tiên sau referral)",
        "carousel_trigger_sources": ["ADS (ad_title)", "Catalog (retailer_id)", "Fchat echo"],
        "carousel_buttons": "3 nút: 🛒 Đặt ngay, 🔍 Xem chi tiết, 🖼️ Xem ảnh",
        "first_message_processing": "Carousel 1 sản phẩm → Từ tin nhắn thứ 2: Function Calling",
        "postback_double_processing_fix": "ENABLED (idempotency key + 30s memory + strict duplicate detection)",
        "product_info_debounce": "15s cho cùng sản phẩm, 5s cho bất kỳ sản phẩm",
        "lock_recovery_mechanism": "ENABLED (auto release sau 15s)",
        "idempotency_mechanism": "ENABLED (30s idempotency for postbacks)",
        "worker_mode": "SINGLE WORKER (optimized for Koyeb 1-worker deployment)"
    }, 200

# ============================================
# DEBUG LOCKS ENDPOINT
# ============================================

@app.route("/debug/locks", methods=["GET"])
def debug_locks():
    """Debug locks để kiểm tra deadlock"""
    now = time.time()
    locked_users = []
    
    for uid, ctx in USER_CONTEXT.items():
        if ctx.get("processing_lock"):
            lock_time = ctx.get("processing_lock_time", 0)
            lock_age = now - lock_time
            if lock_age > 5:  # Lock quá 5 giây
                locked_users.append({
                    "uid": uid,
                    "lock_age": lock_age,
                    "last_ms": ctx.get("last_ms"),
                    "last_activity": ctx.get("last_msg_time", 0),
                    "idempotent_postbacks_count": len(ctx.get("idempotent_postbacks", {}))
                })
    
    return jsonify({
        "total_users": len(USER_CONTEXT),
        "locked_users": len(locked_users),
        "locked_details": locked_users,
        "in_memory_locks": len(POSTBACK_LOCKS),
        "timestamp": now
    }), 200

# ============================================
# MAIN - ĐÃ CẬP NHẬT CHO 1 WORKER KOYEB
# ============================================

if __name__ == "__main__":
    import os
    import multiprocessing
    
    print("=" * 80)
    print("🟢 KHỞI ĐỘNG FACEBOOK CHATBOT - SINGLE WORKER MODE")
    print("=" * 80)
    print(f"🟢 Process ID: {os.getpid()}")
    print(f"🟢 Parent Process ID: {os.getppid()}")
    print(f"🟢 CPU Count: {multiprocessing.cpu_count()}")
    print(f"🟢 Worker Mode: SINGLE (optimized for Koyeb)")
    print(f"🟢 Duplicate Protection: IDEMPOTENCY KEY + 30s MEMORY")
    print(f"🟢 Postback Processing: STRICT (each postback processed once)")
    print("=" * 80)
    
    print(f"🟢 GPT-4o Vision API: {'SẴN SÀNG' if client and OPENAI_API_KEY else 'CHƯA CẤU HÌNH'}")
    print(f"🟢 Fanpage: {get_fanpage_name_from_api()}")
    print(f"🟢 Domain: {DOMAIN}")
    print(f"🟢 Google Sheets API: {'SẴN SÀNG' if GOOGLE_SHEET_ID and GOOGLE_SHEETS_CREDENTIALS_JSON else 'CHƯA CẤU HÌNH'}")
    print(f"🟢 Sheet ID: {GOOGLE_SHEET_ID[:20]}..." if GOOGLE_SHEET_ID else "🟡 Chưa cấu hình")
    print(f"🟢 OpenAI Function Calling: {'TÍCH HỢP THÀNH CÔNG' if client else 'CHƯA CẤU HÌNH'}")
    print(f"🟢 Tools Available: get_product_info, send_product_images, provide_order_link, show_featured_carousel")
    print(f"🟢 Image Processing: Base64 + Fallback URL")
    print(f"🟢 Search Algorithm: TF-IDF + Cosine Similarity")
    print(f"🟢 Image Carousel: 5 sản phẩm phù hợp nhất")
    print(f"🟢 Address Form: Open API - provinces.open-api.vn (dropdown 3 cấp)")
    print(f"🟢 Address Validation: BẬT")
    print(f"🟢 Phone Validation: BẬT (regex)")
    print(f"🟢 Image Debounce: 3 giây")
    print(f"🟢 Text Message Debounce: 2 giây (tăng từ 1s)")
    print(f"🟢 Echo Message Debounce: 2 giây")
    print(f"🟢 Bot Echo Filter: BẬT (phân biệt echo từ bot vs Fchat)")
    print(f"🟢 Fchat Echo Processing: BẬT (giữ nguyên logic trích xuất mã từ Fchat)")
    print(f"🟢 Catalog Support: BẬT (trích xuất retailer_id từ catalog)")
    print(f"🟢 Retailer ID Extraction: MSxxxxxx_xx → MSxxxxxx")
    print(f"🟢 ADS Referral Processing: BẬT (trích xuất mã từ ad_title)")
    print(f"🟢 ADS Context: KHÔNG reset khi đã xác định được sản phẩm")
    print(f"🟢 Referral Auto Processing: BẬT")
    print(f"🟢 Duplicate Message Protection: BẬT (30s)")
    print(f"🟢 Image Send Debounce: 5 giây")
    print(f"🟢 Max Images per Product: 20 ảnh")
    print(f"🟢 Catalog Context: Lưu retailer_id và tự động nhận diện sản phẩm")
    print(f"🟢 Fanpage Name Source: Facebook Graph API (cache 1h)")
    print(f"🟢 Variant Image Support: BẬT (ảnh theo từng thuộc tính)")
    print(f"🟢 Variant Image API: /api/get-variant-image")
    print(f"🟢 Form Dynamic Images: BẬT (ảnh thay đổi theo màu/size)")
    print(f"🟢 Catalog Follow-up Processing: BẬT (30 giây sau khi xem catalog)")
    print(f"🟢 ADS Follow-up Processing: BẬT (xử lý tin nhắn sau click quảng cáo)")
    print(f"🟢 Order Backup System: Local CSV khi Google Sheet không kết nối được")
    print(f"🟢 Context Tracking: BẬT (ghi nhớ last_ms và product_history)")
    print(f"🟢 Facebook Shop Guidance: BẬT (hướng dẫn vào gian hàng)")
    print(f"🟢 Price Detailed Response: BẬT (hiển thị chi tiết các biến thể giá)")
    print("=" * 80)
    print("🔴 QUAN TRỌNG: FIX CHO LỖI DUPLICATE POSTBACK")
    print("=" * 80)
    print(f"🔴 BOT ƯU TIÊN CONTEXT HIỆN TẠI")
    print(f"🔴 BOT CHỈ BÁO CÒN HÀNG KHI KHÁCH HỎI VỀ TỒN KHO")
    print(f"🔴 GPT Reply Mode: FUNCTION CALLING (gpt-4o-mini) với CONTEXT PRIORITY")
    print(f"🔴 FIRST MESSAGE: CAROUSEL 1 SẢN PHẨM (không dùng function calling)")
    print(f"🔴 FROM SECOND MESSAGE: FUNCTION CALLING với CONTEXT PRIORITY")
    print(f"🔴 Order Priority: ƯU TIÊN GỬI LINK KHI CÓ TỪ KHÓA ĐẶT HÀNG")
    print(f"🔴 Price Priority: HIỂN THỊ CHI TIẾT KHI KHÁCH HỎI VỀ GIÁ")
    print(f"🔴 Function Calling Integration: HOÀN THÀNH")
    print(f"🔴 POSTBACK FIX: IDEMPOTENCY KEY + 30s MEMORY (sửa vấn đề duplicate)")
    print(f"🔴 Product Info Debounce: 15s cho cùng sản phẩm, 5s cho bất kỳ sản phẩm")
    print(f"🔴 Lock Recovery Mechanism: TỰ ĐỘNG release sau 15s")
    print(f"🔴 Postback Idempotency: MỖI POSTBACK CHỈ XỬ LÝ 1 LẦN DUY NHẤT")
    print(f"🔴 Debug Endpoint: /debug/locks (kiểm tra deadlock)")
    print(f"🔴 Health Check: /health (kiểm tra tình trạng server)")
    print(f"🔴 MÔ TẢ SẢN PHẨM MỚI: 5 gạch đầu dòng")
    print(f"🔴 PHÂN TÍCH GIÁ THÔNG MINH: Theo màu/Size/Nhóm giá")
    print(f"🔴 ẢNH SẢN PHẨM: 5 ảnh không trùng, gửi tuần tự")
    print("=" * 80)
    print("🚀 Starting app on http://0.0.0.0:5000")
    print("=" * 80)
    
    # Load products ngay khi khởi động
    load_products()
    
    app.run(host="0.0.0.0", port=5000, debug=False)
