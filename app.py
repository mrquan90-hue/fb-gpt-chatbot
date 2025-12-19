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
from typing import Optional, Dict, Any, List, Tuple
from io import BytesIO
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

import requests
from flask import Flask, request, send_from_directory, jsonify, render_template_string, render_template
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
    "last_intent_analysis": None,
    # Thêm trường cho catalog và retailer_id
    "last_retailer_id": None,
    "last_product_id": None,
    "catalog_view_time": 0,
    "last_catalog_product": None,
    # Thêm dict để lưu nhiều sản phẩm từ catalog
    "catalog_products": {},
})

PRODUCTS = {}
PRODUCTS_BY_NUMBER = {}
PRODUCT_TEXT_EMBEDDINGS = {}
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

# Các từ khóa liên quan đến yêu cầu sản phẩm khác
CHANGE_PRODUCT_KEYWORDS = [
    "còn hàng nào khác",
    "có cái nào đẹp hơn",
    "có loại nào rẻ hơn",
    "có loại nào đắt hơn",
    "có loại nào dài hơn",
    "có loại nào ấm hơn",
    "có loại nào mát hơn",
    "có loại nào mỏng hơn",
    "có mẫu nào khác",
    "có sản phẩm nào khác",
    "shop còn gì khác",
    "có loại nào khác",
    "có model nào khác",
    "cho xem cái khác",
    "xem hàng khác",
    "hàng khác",
    "mẫu khác",
    "sản phẩm khác",
    "sản phẩm mới",
    "mẫu mới",
    "còn mẫu nào nữa",
    "có đa dạng không",
    "còn kiểu nào",
    "còn loại nào",
    "xem thêm sản phẩm",
    "cho em xem thêm",
    "còn cái nào",
    "còn cái gì",
    "còn gì nữa",
    "có nhiều mẫu không",
    "có đa dạng mẫu không",
    "còn mẫu gì",
    "có nhiều loại không",
    "còn loại gì",
    "có mẫu nào hot",
    "có sản phẩm nào hot",
    "có sản phẩm nào bán chạy",
    "có sản phẩm nào mới nhất",
    "có sản phẩm mới không",
    "có hàng mới không",
    "cập nhật mẫu mới",
    "hàng mới về",
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
# PHÂN TÍCH INTENT VỚI GPT - ĐÃ SỬA ĐỔI
# ============================================

def analyze_intent_with_gpt(uid: str, text: str, ms: str = None) -> dict:
    """
    Phân tích ý định của người dùng với GPT để xác định có phải yêu cầu xem ảnh sản phẩm không
    Trả về dict chứa intent và các thông tin khác
    """
    if not client or not OPENAI_API_KEY:
        return {"intent": "general", "confidence": 0.5, "reason": "GPT not available"}
    
    try:
        ctx = USER_CONTEXT[uid]
        now = time.time()
        
        # Kiểm tra debounce cho phân tích intent (tránh gọi GPT quá nhiều)
        last_intent_time = ctx.get("last_images_request_time", 0)
        if now - last_intent_time < 2:  # 2 giây debounce
            print(f"[INTENT DEBOUNCE] Bỏ qua phân tích intent, chưa đủ 2s")
            return {"intent": "general", "confidence": 0.5, "reason": "Debounce"}
        
        ctx["last_images_request_time"] = now
        
        # Lấy tên sản phẩm nếu có
        product_name = ""
        if ms and ms in PRODUCTS:
            product_name = PRODUCTS[ms].get('Ten', '')
        
        system_prompt = f"""Bạn là trợ lý phân tích ý định trong trò chuyện mua sắm.
    
Sản phẩm hiện tại: {product_name} (Mã: {ms if ms else 'Chưa xác định'})

PHÂN TÍCH TIN NHẮN CỦA KHÁCH HÀNG: "{text}"

QUY TẮC PHÂN TÍCH:
1. TRẢ VỀ "view_images" NẾU KHÁCH:
   - Yêu cầu xem ảnh/hình ảnh/hình của sản phẩm
   - Hỏi "có ảnh không?", "gửi ảnh đi", "cho xem ảnh"
   - Dùng từ: "ảnh mẫu", "hình ảnh", "gửi hình", "xem hình"
   - Ví dụ: "gửi cho tôi xem ảnh mẫu này", "cho xem ảnh sản phẩm", "có ảnh không?"

2. TRẢ VỀ "general" NẾU KHÁCH:
   - Hỏi về giá, size, màu sắc, thông tin khác
   - Hỏi chung chung không liên quan ảnh

QUAN TRỌNG: "gửi cho tôi xem ảnh mẫu này ?" → LUÔN là "view_images" với confidence cao (0.9+)

Trả về JSON:
{{
    "intent": "view_images|general",
    "confidence": 0.0-1.0,
    "reason": "Giải thích ngắn"
}}"""
        
        user_message = f"""Phân tích xem khách có YÊU CẦU XEM ẢNH sản phẩm này không."""
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message}
            ],
            temperature=0.1,
            max_tokens=200,
            response_format={"type": "json_object"},
            timeout=5.0
        )
        
        result_text = response.choices[0].message.content
        print(f"[GPT INTENT RAW] {result_text}")
        
        result = json.loads(result_text)
        ctx["last_intent_analysis"] = result
        
        print(f"[INTENT ANALYSIS] User: {uid}, Text: {text[:50]}..., Intent: {result.get('intent')}, Confidence: {result.get('confidence')}")
        
        return result
        
    except Exception as e:
        print(f"❌ Lỗi phân tích intent: {str(e)}")
        return {"intent": "general", "confidence": 0.3, "reason": f"Error: {str(e)}"}

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
    
    # Xử lý intent
    intent_result = analyze_intent_with_gpt(uid, text, ms)
    
    # Nếu là yêu cầu xem ảnh
    if (intent_result.get('intent') == 'view_images' and 
        intent_result.get('confidence', 0) > 0.3):  # GIẢM XUỐNG 0.3
        send_all_product_images(uid, ms)
        return True
    
    # Nếu không phải xem ảnh, dùng GPT trả lời
    gpt_response = generate_gpt_response(uid, text, ms)
    send_message(uid, gpt_response)
    
    # Kiểm tra từ khóa đặt hàng
    lower = text.lower()
    if any(kw in lower for kw in ORDER_KEYWORDS):
        domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
        order_link = f"{domain}/order-form?ms={ms}&uid={uid}"
        send_message(uid, f"📋 Anh/chị có thể đặt hàng ngay tại đây:\n{order_link}")
    
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
        
        # Phân tích intent
        intent_result = analyze_intent_with_gpt(uid, text, last_ms)
        
        # Nếu là yêu cầu xem ảnh
        if (intent_result.get('intent') == 'view_images' and 
            intent_result.get('confidence', 0) > 0.3):  # GIẢM XUỐNG 0.3
            send_all_product_images(uid, last_ms)
            return True
        
        # GPT trả lời bình thường
        gpt_response = generate_gpt_response(uid, text, last_ms)
        send_message(uid, gpt_response)
        
        # Kiểm tra từ khóa đặt hàng
        lower = text.lower()
        if any(kw in lower for kw in ORDER_KEYWORDS):
            domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
            order_link = f"{domain}/order-form?ms={last_ms}&uid={uid}"
            send_message(uid, f"📋 Anh/chị có thể đặt hàng ngay tại đây:\n{order_link}")
        
        return True
    
    return False

# ============================================
# GỬI TOÀN BỘ ẢNH SẢN PHẨM - ĐÃ SỬA LỖI DEADLOCK VÀ CẢI THIỆN
# ============================================

def send_all_product_images(uid: str, ms: str, max_images: int = 20):
    """
    Gửi toàn bộ ảnh của sản phẩm (loại trừ trùng)
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
        # Vẫn thông báo cho khách biết
        send_message(uid, "Em vừa gửi ảnh rồi ạ. Anh/chị vuốt lên xem lại nhé!")
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
                unique_images.append(url)
        
        if not unique_images:
            send_message(uid, f"Dạ, sản phẩm [{ms}] hiện chưa có hình ảnh trong hệ thống ạ.")
            return
        
        # Giới hạn số lượng ảnh để tránh spam
        if len(unique_images) > max_images:
            unique_images = unique_images[:max_images]
        
        # Thông báo cho khách
        send_message(uid, f"Dạ, em gửi ảnh sản phẩm [{ms}] - {product_name} ạ:")
        time.sleep(0.5)
        
        # Gửi từng ảnh một với debounce
        sent_count = 0
        
        for i, image_url in enumerate(unique_images, 1):
            try:
                print(f"🖼️ Gửi ảnh {i}/{len(unique_images)}")
                result = send_image(uid, image_url)
                
                if result:
                    sent_count += 1
                
                # Thêm delay giữa các ảnh để tránh bị rate limit
                if i < len(unique_images):
                    time.sleep(0.8)
                    
            except Exception as e:
                print(f"❌ Lỗi khi gửi ảnh {i}: {str(e)}")
                time.sleep(1.0)
        
        # Thông báo kết quả
        if sent_count > 0:
            time.sleep(0.5)
            send_message(uid, f"✅ Đã gửi {sent_count} ảnh sản phẩm cho anh/chị!")
            
            # Hỏi khách có cần thêm thông tin không
            time.sleep(0.5)
            domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
            order_link = f"{domain}/order-form?ms={ms}&uid={uid}"
            send_message(uid, f"📋 Đặt hàng ngay tại: {order_link}")
        else:
            send_message(uid, "❌ Không thể gửi ảnh ngay lúc này. Anh/chị vui lòng thử lại sau ạ.")
    
    except Exception as e:
        print(f"❌ Lỗi trong send_all_product_images: {str(e)}")
        send_message(uid, "❌ Có lỗi khi tải ảnh sản phẩm. Anh/chị vui lòng thử lại sau ạ.")

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
    
    # Kiểm tra án xạ trực tiếp
    for keyword, ms in keyword_to_ms.items():
        if keyword in normalized_text and ms in PRODUCTS:
            print(f"[KEYWORD MATCH] Tìm thấy qua án xạ: {keyword} -> {ms}")
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
    Trả về danh sách (mã sản phẩm, điểm số) sắp xếp theo điểm giảm dần
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
# GPT INTEGRATION - XỬ LÝ MỌI CÂU HỎI (ĐÃ SỬA)
# ============================================

def build_comprehensive_product_context(ms: str) -> str:
    """Xây dựng context đầy đủ về sản phẩm cho GPT"""
    if not ms or ms not in PRODUCTS:
        return "KHÔNG CÓ THÔNG TIN SẢN PHẨM"
    
    product = PRODUCTS[ms]
    mota = product.get("MoTa", "")
    
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
    
    variants_text = ""
    variants = product.get("variants", [])
    if variants:
        variants_text = "Các biến thể có sẵn:\n"
        for i, v in enumerate(variants[:5], 1):
            mau = v.get("mau", "Mặc định")
            size = v.get("size", "Mặc định")
            gia = v.get("gia")
            tonkho = v.get("tonkho", "Liên hệ shop")
            if gia:
                variants_text += f"{i}. {mau} - {size}: {gia:,.0f}đ\n"
    
    # Thông tin về hình ảnh
    images_field = product.get("Images", "")
    urls = parse_image_urls(images_field)
    unique_images = len(set(urls))
    image_info = f"Số lượng ảnh: {unique_images}"
    
    # Hiển thị thông tin tồn kho thực tế
    tonkho_info = product.get("Tồn kho", "")
    if not tonkho_info or tonkho_info.strip() == "":
        tonkho_display = "Liên hệ shop để biết tồn kho"
    else:
        tonkho_display = f"Tồn kho: {tonkho_info}"
    
    context = f"""
=== THÔNG TIN SẢN PHẨM [{ms}] ===

1. TÊN SẢN PHẨM: {product.get('Ten', '')}

2. GIÁ BÁN: {product.get('Gia', '')}

3. {tonkho_display}

4. THUỘC TÍNH:
   - Màu sắc: {product.get('màu (Thuộc tính)', 'Chưa có thông tin')}
   - Size: {product.get('size (Thuộc tính)', 'Chưa có thông tin')}

5. HÌNH ẢNH: {image_info}

{variants_text}

6. MÔ TẬP CHI TIẾT:
{product.get('MoTa', 'Chưa có mô tả chi tiết')}

7. THÔNG TIN CHÍNH SÁCH:
   - Vận chuyển: {shipping_info if shipping_info else 'Giao hàng toàn quốc, phí ship 20-50k. Miễn phí ship cho đơn từ 500k.'}
   - Bảo hành: {warranty_info if warranty_info else 'Bảo hành theo chính sách của nhà sản xuất.'}
   - Đổi trả: {return_info if return_info else 'Đổi/trả trong 3-7 ngày nếu sản phẩm lỗi, còn nguyên tem mác.'}
   - Thanh toán: {payment_info if payment_info else 'Thanh toán khi nhận hàng (COD) hoặc chuyển khoản ngân hàng.'}
"""
    
    return context

def detect_ms_from_text(text: str):
    """Tìm mã sản phẩm trong tin nhắn, hỗ trợ nhiều định dạng"""
    # GIỮ NGUYÊN LOGIC GỐC: Hỗ trợ tất cả định dạng
    
    # 1. Tìm [MS123456]
    ms_list = re.findall(r"\[MS(\d{6})\]", text.upper())
    if ms_list:
        ms = "MS" + ms_list[0]
        if ms in PRODUCTS:
            return ms
    
    # 2. Tìm MS123456 (không có dấu [])
    ms_list = re.findall(r"MS(\d{6})", text.upper())
    if ms_list:
        ms = "MS" + ms_list[0]
        if ms in PRODUCTS:
            return ms
    
    # 3. Tìm #MS123456 (thêm hỗ trợ cho Fchat)
    ms_list = re.findall(r"#MS(\d{6})", text.upper())
    if ms_list:
        ms = "MS" + ms_list[0]
        if ms in PRODUCTS:
            return ms
    
    # 4. Tìm số đơn thuần
    text_normalized = normalize_vietnamese(text.lower())
    numbers = re.findall(r'\d{1,6}', text_normalized)
    
    if numbers:
        num = numbers[0]
        num_stripped = num.lstrip('0')
        if not num_stripped:
            num_stripped = "0"
        
        if num_stripped in PRODUCTS_BY_NUMBER:
            return PRODUCTS_BY_NUMBER[num_stripped]
        
        candidates = []
        candidates.append("MS" + num_stripped)
        for length in range(2, 7):
            padded = num_stripped.zfill(length)
            candidates.append("MS" + padded)
        
        for candidate in candidates:
            if candidate in PRODUCTS:
                return candidate
    
    # 5. Tìm pattern kết hợp
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
            
            candidates = ["MS" + num_stripped]
            for length in range(2, 7):
                padded = num_stripped.zfill(length)
                candidates.append("MS" + padded)
            
            for candidate in candidates:
                if candidate in PRODUCTS:
                    return candidate
    
    return None

def generate_gpt_response(uid: str, user_message: str, ms: str = None):
    """Gọi GPT để trả lời câu hỏi của khách - TRẢ LỜI CHI TIẾT VỀ GIÁ"""
    if not client or not OPENAI_API_KEY:
        return "Hiện tại hệ thống trợ lý AI đang bảo trì, vui lòng thử lại sau ạ."
    
    try:
        # Tạo link đặt hàng nếu có mã sản phẩm
        order_link = ""
        if ms and ms in PRODUCTS:
            domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
            order_link = f"{domain}/order-form?ms={ms}&uid={uid}"
        
        if ms and ms in PRODUCTS:
            product_context = build_comprehensive_product_context(ms)
            
            # Lấy thông tin giá chi tiết
            product = PRODUCTS[ms]
            variants = product.get("variants", [])
            
            price_info = ""
            if variants:
                # Nhóm giá theo từng mức
                price_groups = {}
                for v in variants:
                    gia_int = v.get("gia")
                    if gia_int:
                        price_key = f"{gia_int:,.0f}đ"
                        if price_key not in price_groups:
                            price_groups[price_key] = []
                        
                        mau = v.get("mau", "Mặc định")
                        size = v.get("size", "Mặc định")
                        # Tạo mô tả variant
                        variant_desc = []
                        if mau and mau != "Mặc định":
                            variant_desc.append(mau)
                        if size and size != "Mặc định":
                            variant_desc.append(size)
                        
                        if variant_desc:
                            price_groups[price_key].append("/".join(variant_desc))
                
                if len(price_groups) == 1:
                    # Chỉ 1 mức giá
                    price = list(price_groups.keys())[0]
                    variants_count = len(list(price_groups.values())[0])
                    price_info = f"💰 GIÁ: {price} (cho {variants_count} phân loại)"
                else:
                    # Nhiều mức giá
                    price_info = "💰 GIÁ THEO PHÂN LOẠI:\n"
                    for i, (price, variant_list) in enumerate(list(price_groups.items())[:4], 1):
                        if len(variant_list) <= 3:
                            desc = ", ".join(variant_list[:3])
                        else:
                            desc = f"{', '.join(variant_list[:3])} và {len(variant_list)-3} phân loại khác"
                        price_info += f"{i}. {price}: {desc}\n"
                    
                    if len(price_groups) > 4:
                        price_info += f"... và {len(price_groups)-4} mức giá khác\n"
            else:
                # Không có variants, lấy giá từ trường Gia
                gia_raw = product.get("Gia", "")
                gia_int = extract_price_int(gia_raw)
                if gia_int:
                    price_info = f"💰 GIÁ: {gia_int:,.0f}đ"
                else:
                    price_info = "💰 GIÁ: Liên hệ shop"
            
            system_prompt = f"""Bạn là NHÂN VIÊN TƯ VẤN BÁN HÀNG của {FANPAGE_NAME}.
Bạn đang tư vấn sản phẩm mã: {ms}

{price_info}

QUY TẮC TRẢ LỜI:
1. TRẢ LỜI NGẮN GỌN - TỐI ĐA 3-4 DÒNG
2. Khi khách hỏi về giá: TRẢ LỜI ĐẦY ĐỦ THÔNG TIN GIÁ TRÊN
3. Nếu có nhiều mức giá: giải thích ngắn gọn "tùy phân loại màu/size"
4. Chỉ nói "còn hàng" khi khách hỏi về tồn kho/số lượng
5. Nếu khách muốn đặt hàng: GỬI LINK NGAY
6. Link đặt hàng: {order_link}
7. Xưng "em", gọi "anh/chị"
8. LUÔN GIỮ NGỮ CẢNH SẢN PHẨM HIỆN TẠI: [{ms}]

THÔNG TIN SẢN PHẨM BỔ SUNG:
{product_context}

TRẢ LỜI MẪU CHO HỎI GIÁ:
- "Dạ, sản phẩm có các mức giá sau ạ:\n{price_info}\n\nAnh/chị có thể đặt hàng tại: {order_link}"
- "Dạ, giá sản phẩm tùy phân loại ạ. Em gửi anh/chị thông tin chi tiết:\n{price_info}\n\nĐặt hàng ngay tại: {order_link}"

Hãy trả lời TỰ NHIÊN và ĐẦY ĐỦ thông tin giá khi được hỏi."""
        else:
            system_prompt = f"""Bạn là NHÂN VIÊN TƯ VẤN BÁN HÀNG của {FANPAGE_NAME}.

TRẢ LỜI NGẮN GỌN - TỐI ĐA 3 DÒNG
Mục tiêu: Hỏi mã sản phẩm hoặc gợi ý "xem sản phẩm"

Xưng "em", gọi "anh/chị"
Hỏi mã sản phẩm nếu chưa biết."""
        
        ctx = USER_CONTEXT[uid]
        conversation = ctx.get("conversation_history", [])
        
        if len(conversation) > 10:
            conversation = conversation[-10:]
        
        messages = [{"role": "system", "content": system_prompt}]
        
        for msg in conversation:
            messages.append(msg)
        
        messages.append({"role": "user", "content": user_message})
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.7,
            max_tokens=150,  # GIẢM XUỐNG CHỈ 150 tokens
            timeout=15.0,
        )
        
        reply = response.choices[0].message.content.strip()
        
        # **QUAN TRỌNG: Thay thế [link] bằng link thật nếu có**
        if order_link and "[link]" in reply:
            reply = reply.replace("[link]", order_link)
        
        conversation.append({"role": "user", "content": user_message})
        conversation.append({"role": "assistant", "content": reply})
        ctx["conversation_history"] = conversation
        
        return reply
        
    except Exception as e:
        print(f"GPT Error: {e}")
        return "Dạ em đang gặp chút trục trặc kỹ thuật. Anh/chị vui lòng thử lại sau ạ."

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
    """Tìm sản phẩm phù hợp nhất cho câu hỏi dựa trên ngữ cảnh và xử lý từ khóa chuyển đổi sản phẩm"""
    ctx = USER_CONTEXT[uid]
    lower = text.lower()
    
    # **QUAN TRỌNG: Kiểm tra từ khóa yêu cầu sản phẩm khác trước**
    if any(kw in lower for kw in CHANGE_PRODUCT_KEYWORDS):
        # Hướng dẫn vào gian hàng Facebook Shop
        return "GUIDE_TO_FACEBOOK_SHOP"
    
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
    
    # **QUAN TRỌNG: Cập nhật context khi gửi sản phẩm mới**
    print(f"[PRODUCT INFO] Gửi thông tin sản phẩm {ms}, cập nhật context")
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

        # **QUAN TRỌNG: Cập nhật context khi gửi sản phẩm mới**
        ctx["last_ms"] = ms
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
                tonkho = variant.get("tonkho", "Liên hệ shop")
                
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
    send_message(uid, "2. Gõ 'xem sản phẩm' để xem toàn bộ danh mục")
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
# HANDLE TEXT - XỬ LÝ VỚI GPT VÀ PHÂN TÍCH INTENT (ĐÃ SỬA)
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
        now = time.time()
        last_msg_time = ctx.get("last_msg_time", 0)
        
        # Debounce: kiểm tra tin nhắn trùng lặp
        if now - last_msg_time < 1:
            last_text = ctx.get("last_processed_text", "")
            if text.strip().lower() == last_text.lower():
                print(f"[TEXT DEBOUNCE] Bỏ qua tin nhắn trùng lặp: {text[:50]}...")
                ctx["processing_lock"] = False
                return
        
        ctx["last_msg_time"] = now
        ctx["last_processed_text"] = text.strip().lower()
        
        load_products()
        ctx["postback_count"] = 0

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
        
        # **QUAN TRỌNG: Xử lý từ khóa yêu cầu sản phẩm khác**
        if any(kw in lower for kw in CHANGE_PRODUCT_KEYWORDS):
            print(f"[CHANGE PRODUCT] User {uid} yêu cầu sản phẩm khác: {text}")
            
            # Hướng dẫn vào gian hàng Facebook Shop
            guide_message = """Dạ, hiện tại shop có nhiều mẫu mã đa dạng ạ!

Để xem thêm nhiều sản phẩm khác, anh/chị có thể:
1. Bấm vào biểu tượng 🛒 rổ hàng trên Messenger để vào gian hàng
2. Xem danh mục sản phẩm đầy đủ tại Facebook Shop của shop
3. Hoặc gõ "xem sản phẩm" để em gửi danh sách một số sản phẩm nổi bật

Anh/chị muốn xem sản phẩm nào cụ thể ạ?"""
            
            send_message(uid, guide_message)
            ctx["processing_lock"] = False
            return
        
        # ƯU TIÊN 1: Xử lý từ khóa đặt hàng TRƯỚC
        if any(kw in lower for kw in ORDER_KEYWORDS):
            # Tìm sản phẩm phù hợp
            current_ms = get_relevant_product_for_question(uid, text)
            
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
        
        if any(kw in lower for kw in CAROUSEL_KEYWORDS):
            if PRODUCTS:
                send_message(uid, "Dạ, em đang lấy danh sách sản phẩm cho anh/chị...")
                
                # SỬA LỖI VẤN ĐỀ 1: Định nghĩa biến domain
                domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
                
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
                                "url": f"{domain}/order-form?ms={ms}&uid={uid}",
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

        # Tìm sản phẩm phù hợp
        current_ms = get_relevant_product_for_question(uid, text)
        
        # Xử lý đặc biệt khi yêu cầu vào gian hàng
        if current_ms == "GUIDE_TO_FACEBOOK_SHOP":
            guide_message = """Dạ, hiện tại shop có nhiều mẫu mã đa dạng ạ!

Để xem thêm nhiều sản phẩm khác, anh/chị có thể:
1. Bấm vào biểu tượng 🛒 rổ hàng trên Messenger để vào gian hàng
2. Xem danh mục sản phẩm đầy đủ tại Facebook Shop của shop
3. Hoặc gõ "xem sản phẩm" để em gửi danh sách một số sản phẩm nổi bật

Anh/chị muốn xem sản phẩm nào cụ thể ạ?"""
            
            send_message(uid, guide_message)
            ctx["processing_lock"] = False
            return
        
        # Kiểm tra xem có mã sản phẩm mới trong tin nhắn không
        detected_ms = detect_ms_from_text(text)
        if detected_ms and detected_ms in PRODUCTS:
            print(f"[MS DETECTED] Phát hiện mã mới: {detected_ms}")
            current_ms = detected_ms
            ctx["last_ms"] = detected_ms
            update_product_context(uid, detected_ms)
        
        # **QUAN TRỌNG: Cập nhật context nếu tìm thấy sản phẩm**
        if current_ms and current_ms in PRODUCTS and current_ms != ctx.get("last_ms"):
            print(f"[CONTEXT UPDATE] Cập nhật last_ms từ {ctx.get('last_ms')} -> {current_ms}")
            ctx["last_ms"] = current_ms
            update_product_context(uid, current_ms)
        
        # PHÂN TÍCH INTENT KHI CÓ SẢN PHẨM HIỆN TẠI
        if current_ms and current_ms in PRODUCTS:
            # Phân tích intent với GPT để xác định có phải yêu cầu xem ảnh không
            intent_result = analyze_intent_with_gpt(uid, text, current_ms)
            
            # Nếu intent là xem ảnh và confidence đủ cao (>0.3)
            if (intent_result.get('intent') == 'view_images' and 
                intent_result.get('confidence', 0) > 0.3):
                
                print(f"[IMAGE REQUEST DETECTED] User {uid} yêu cầu xem ảnh sản phẩm {current_ms}")
                print(f"[INTENT DETAILS] Confidence: {intent_result.get('confidence')}, Reason: {intent_result.get('reason')}")
                
                # Gửi toàn bộ ảnh sản phẩm
                send_all_product_images(uid, current_ms)
                ctx["processing_lock"] = False  # Release lock sau khi gửi xong
                return
            else:
                print(f"[NO IMAGE REQUEST] Intent: {intent_result.get('intent')}, Confidence: {intent_result.get('confidence')}")
        
        # THÊM PHẦN NÀY: Nếu không có sản phẩm hiện tại nhưng vẫn yêu cầu xem ảnh
        elif not current_ms:
            # Vẫn phân tích intent để xem có phải yêu cầu xem ảnh không
            intent_result = analyze_intent_with_gpt(uid, text, None)
            
            if (intent_result.get('intent') == 'view_images' and 
                intent_result.get('confidence', 0) > 0.3):
                
                print(f"[IMAGE REQUEST NO PRODUCT] User {uid} yêu cầu xem ảnh nhưng chưa có sản phẩm")
                send_message(uid, "Dạ, em chưa biết anh/chị muốn xem ảnh sản phẩm nào ạ. Vui lòng cho em biết mã sản phẩm hoặc mô tả sản phẩm nhé!")
                ctx["processing_lock"] = False
                return
        
        # Nếu không phải yêu cầu xem ảnh, hoặc không xác định được intent rõ ràng
        # thì gọi GPT như bình thường
        print(f"[GPT CALL] User: {uid}, MS: {current_ms}, Text: {text}")
        gpt_response = generate_gpt_response(uid, text, current_ms)
        send_message(uid, gpt_response)

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
# GOOGLE SHEETS API FUNCTIONS - NEW SECTION
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
# WEBHOOK HANDLER - ĐÃ SỬA LỖI GỬI TIN NHẮN LẶP + THÊM HỖ TRỢ CATALOG
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
                                        update_product_context(sender_id, ms_from_retailer)
                                    
                                    print(f"[CATALOG] Lưu retailer_id: {retailer_id} -> MS: {ms_from_retailer} cho user {sender_id}")
                                    
                                    # KHÔNG gửi tin nhắn tự động để tránh spam
                                    # Chỉ lưu retailer_id, chờ khách hỏi mới trả lời

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
                
                # **GIỮ NGUYÊN**: Tìm mã sản phẩm trong tin nhắn echo (hỗ trợ tất cả định dạng)
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
                        ctx["referral_source"] = "fchat_echo"
                        update_product_context(recipient_id, detected_ms)
                        
                        print(f"[CONTEXT UPDATED] Đã ghi nhận mã {detected_ms} vào ngữ cảnh cho user {recipient_id}")
                        
                        # **THAY ĐỔI QUAN TRỌNG**: CHỈ GHI NHẬN NGỮ CẢNH, KHÔNG GỬI TIN NHẮN
                        # Để tránh spam, chỉ ghi nhận mã sản phẩm vào context
                        # Khi khách hỏi tiếp, bot sẽ dùng mã này để trả lời
                        
                    finally:
                        ctx["processing_lock"] = False
                else:
                    print(f"[ECHO FCHAT] Không tìm thấy mã sản phẩm trong echo: {echo_text[:100]}...")
                    # KHÔNG gửi tin nhắn chào nếu không tìm thấy mã sản phẩm
                    # để tránh spam khách hàng
                
                continue
            
            if m.get("delivery") or m.get("read"):
                continue
            
            # ============================================
            # XỬ LÝ REFERRAL (TỪ QUẢNG CÁO, FACEBOOK SHOP, CATALOG) - ĐÃ SỬA CHO ADS
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
                    if ms_from_ad and ms_from_ad in PRODUCTS:
                        print(f"[ADS PRODUCT] Xác định sản phẩm từ ad_title: {ms_from_ad}")
                        
                        # KHÔNG reset context, mà update context với sản phẩm mới
                        ctx["last_ms"] = ms_from_ad
                        update_product_context(sender_id, ms_from_ad)
                        
                        # Gửi thông tin sản phẩm ngay
                        welcome_msg = f"""Chào anh/chị! 👋 
Em là trợ lý AI của {get_fanpage_name_from_api()}.

Em thấy anh/chị quan tâm đến sản phẩm **[{ms_from_ad}]** từ quảng cáo.
Em sẽ gửi thông tin chi tiết sản phẩm ngay ạ!"""
                        
                        send_message(sender_id, welcome_msg)
                        send_product_info_debounced(sender_id, ms_from_ad)
                        handled = True
                    
                    # ƯU TIÊN 2: Kiểm tra referral payload
                    if not handled and referral_payload:
                        detected_ms = detect_ms_from_text(referral_payload)
                        if detected_ms and detected_ms in PRODUCTS:
                            print(f"[ADS REFERRAL] Nhận diện mã từ payload: {detected_ms}")
                            ctx["last_ms"] = detected_ms
                            update_product_context(sender_id, detected_ms)
                            
                            welcome_msg = f"""Chào anh/chị! 👋 
Em là trợ lý AI của {get_fanpage_name_from_api()}.

Em thấy anh/chị quan tâm đến sản phẩm **[{detected_ms}]**.
Em sẽ gửi thông tin chi tiết sản phẩm ngay ạ!"""
                            
                            send_message(sender_id, welcome_msg)
                            send_product_info_debounced(sender_id, detected_ms)
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
                        update_product_context(sender_id, detected_ms)
                        
                        welcome_msg = f"""Chào anh/chị! 👋 
Em là trợ lý AI của {FANPAGE_NAME}.

Em thấy anh/chị quan tâm đến sản phẩm mã [{detected_ms}].
Em sẽ gửi thông tin chi tiết sản phẩm ngay ạ!"""
                        send_message(sender_id, welcome_msg)
                        send_product_info_debounced(sender_id, detected_ms)
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
                        if ctx.get("processing_lock"):
                            print(f"[POSTBACK LOCKED] User {sender_id} đang được xử lý, bỏ qua ADVICE")
                            continue
                        
                        ctx["processing_lock"] = True
                        try:
                            load_products()
                            ms = payload.replace("ADVICE_", "")
                            if ms in PRODUCTS:
                                ctx["last_ms"] = ms
                                update_product_context(sender_id, ms)
                                send_product_info_debounced(sender_id, ms)
                            else:
                                send_message(sender_id, "❌ Em không tìm thấy sản phẩm này. Anh/chị vui lòng kiểm tra lại mã sản phẩm ạ.")
                        finally:
                            ctx["processing_lock"] = False
                    
                    elif payload.startswith("ORDER_"):
                        if ctx.get("processing_lock"):
                            print(f"[POSTBACK LOCKED] User {sender_id} đang được xử lý, bỏ qua ORDER")
                            continue
                        
                        ctx["processing_lock"] = True
                        try:
                            load_products()
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
                        finally:
                            ctx["processing_lock"] = False
                    
                    continue
            
            # ============================================
            # XỬ LÝ TIN NHẮN THƯỜNG (TEXT & ẢNH) - THÊM DEBOUNCE
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
                        if now - processed_time < 3:
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
# ORDER FORM PAGE (SỬA DÙNG FILE TĨNH)
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
    
    # Lấy thông tin sản phẩm để truyền vào template
    product = PRODUCTS[ms]
    
    # Lấy tên fanpage từ API
    current_fanpage_name = get_fanpage_name_from_api()
    
    # Lấy ảnh mặc định (ảnh đầu tiên từ sản phẩm)
    images_field = product.get("Images", "")
    urls = parse_image_urls(images_field)
    default_image = urls[0] if urls else ""
    
    size_field = product.get("size (Thuộc tính)", "")
    color_field = product.get("màu (Thuộc tính)", "")
    
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
    
    price_str = product.get("Gia", "0")
    price_int = extract_price_int(price_str) or 0
    
    # Chuẩn bị domain và api_base_url
    domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
    api_base_url = "/api" if DOMAIN.startswith("http") else f"https://{DOMAIN}/api"
    
    # Render template với dữ liệu sản phẩm
    return render_template(
        "order-form.html",
        ms=ms,
        uid=uid,
        product=product,
        fanpage_name=current_fanpage_name,
        default_image=default_image,
        sizes=sizes,
        colors=colors,
        price_int=price_int,
        domain=domain,
        api_base_url=api_base_url
    )

# ============================================
# API ENDPOINTS (THÊM API GET-VARIANT-IMAGE)
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
    # GHI ĐƠN HÀNG VÀO GOOGLE SHEET QUA API - UPDATED
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
# HEALTH CHECK (ĐÃ CẢI TIẾN) - UPDATED
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
        "referral_auto_processing": True,
        "message_debounce_enabled": True,
        "duplicate_protection": True,
        "intent_analysis": "GPT-based",
        "image_send_debounce": "5s",
        "image_request_processing": "Enabled with confidence > 0.3",  # ĐÃ THAY ĐỔI
        "address_form": "Open API - provinces.open-api.vn (dropdown 3 cấp)",
        "address_validation": "enabled",
        "phone_validation": "regex validation",
        "order_response_mode": "SHORT - Chỉ báo còn hàng khi hỏi tồn kho",
        "price_detailed_response": "ENABLED (hiển thị chi tiết các biến thể giá)",
        "max_gpt_tokens": 150,
        "stock_assumption": "Chỉ báo khi hỏi tồn kho",
        "order_keywords_priority": "HIGH",
        "context_tracking": "ENABLED (tracks last_ms and product_history)",
        "change_product_keywords": f"{len(CHANGE_PRODUCT_KEYWORDS)} từ khóa được định nghĩa",
        "facebook_shop_guidance": "ENABLED (hướng dẫn vào gian hàng khi yêu cầu sản phẩm khác)",
        "ads_context_handling": "ENABLED (không reset context khi có sản phẩm từ ADS)",
        "image_request_detection": "GPT Intent Analysis Only (no keywords)",
        "image_request_threshold": "0.3 confidence",
        "no_keyword_dependency": True  # THÊM FLAG MỚI
    }, 200

# ============================================
# TẠO TEMPLATE DIRECTORY NẾU CHƯA CÓ
# ============================================

def ensure_template_directory():
    """Đảm bảo thư mục templates tồn tại"""
    templates_dir = "templates"
    if not os.path.exists(templates_dir):
        os.makedirs(templates_dir)
        print(f"✅ Đã tạo thư mục {templates_dir}")

# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    # Đảm bảo thư mục templates tồn tại
    ensure_template_directory()
    
    print("Starting app on http://0.0.0.0:5000")
    print(f"🟢 GPT-4o Vision API: {'SẴN SÀNG' if client and OPENAI_API_KEY else 'CHƯA CẤU HÌNH'}")
    print(f"🟢 Fanpage: {get_fanpage_name_from_api()}")
    print(f"🟢 Domain: {DOMAIN}")
    print(f"🟢 Google Sheets API: {'SẴN SÀNG' if GOOGLE_SHEET_ID and GOOGLE_SHEETS_CREDENTIALS_JSON else 'CHƯA CẤU HÌNH'}")
    print(f"🟢 Sheet ID: {GOOGLE_SHEET_ID[:20]}..." if GOOGLE_SHEET_ID else "🟡 Chưa cấu hình")
    print(f"🟢 Image Processing: Base64 + Fallback URL")
    print(f"🟢 Search Algorithm: TF-IDF + Cosine Similarity")
    print(f"🟢 Image Carousel: 5 sản phẩm phù hợp nhất")
    print(f"🟢 Address Form: Open API - provinces.open-api.vn (dropdown 3 cấp)")
    print(f"🟢 Address Validation: BẬT")
    print(f"🟢 Phone Validation: BẬT (regex)")
    print(f"🟢 Image Debounce: 3 giây")
    print(f"🟢 Text Message Debounce: 1 giây")
    print(f"🟢 Echo Message Debounce: 2 giây")
    print(f"🟢 Bot Echo Filter: BẬT (phân biệt echo từ bot vs Fchat)")
    print(f"🟢 Fchat Echo Processing: BẬT (giữ nguyên logic trích xuất mã từ Fchat)")
    print(f"🟢 Catalog Support: BẬT (trích xuất retailer_id từ catalog)")
    print(f"🟢 Retailer ID Extraction: MSxxxxxx_xx → MSxxxxxx")
    print(f"🟢 ADS Referral Processing: BẬT (trích xuất mã từ ad_title)")
    print(f"🟢 ADS Context: KHÔNG reset khi đã xác định được sản phẩm")
    print(f"🟢 Referral Auto Processing: BẬT")
    print(f"🟢 Duplicate Message Protection: BẬT")
    print(f"🟢 Intent Analysis: GPT-based (phát hiện yêu cầu xem ảnh)")
    print(f"🟢 Image Send Debounce: 5 giây")
    print(f"🟢 Image Request Confidence Threshold: 0.3")  # ĐÃ THAY ĐỔI
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
    print(f"🟢 Change Product Keywords: {len(CHANGE_PRODUCT_KEYWORDS)} từ khóa")
    print(f"🟢 Facebook Shop Guidance: BẬT (hướng dẫn vào gian hàng)")
    print(f"🟢 Price Detailed Response: BẬT (hiển thị chi tiết các biến thể giá)")
    print(f"🔴 QUAN TRỌNG: BOT CHỈ BÁO CÒN HÀNG KHI KHÁCH HỎI VỀ TỒN KHO")
    print(f"🔴 GPT Reply Mode: NGẮN GỌN (max 150 tokens)")
    print(f"🔴 Order Priority: ƯU TIÊN GỬI LINK KHI CÓ TỪ KHÓA ĐẶT HÀNG")
    print(f"🔴 Price Priority: HIỂN THỊ CHI TIẾT KHI KHÁCH HỎI VỀ GIÁ")
    print(f"🔴 Image Request Detection: KHÔNG DÙNG KEYWORD, CHỈ DÙNG GPT INTENT ANALYSIS")
    
    app.run(host="0.0.0.0", port=5000, debug=True)
