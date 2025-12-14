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
        print(f"📦 Loaded {len(PRODUCTS)} products (PHƯƠNG ÁN A).")
        print(f"🔢 Created mapping for {len(PRODUCTS_BY_NUMBER)} product numbers")
        print(f"🔤 Created text embeddings for {len(PRODUCT_TEXT_EMBEDDINGS)} products")
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
    """Gọi GPT để trả lời câu hỏi của khách"""
    if not client or not OPENAI_API_KEY:
        return "Hiện tại hệ thống trợ lý AI đang bảo trì, vui lòng thử lại sau ạ."
    
    try:
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
            max_tokens=500,
            timeout=15.0,
        )
        
        reply = response.choices[0].message.content.strip()
        
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
        now = time.time()
        last_msg_time = ctx.get("last_msg_time", 0)
        
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

        lower = text.lower()
        
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

        detected_ms = detect_ms_from_text(text)
        
        current_ms = None
        is_only_product_code = False
        
        if detected_ms and detected_ms in PRODUCTS:
            temp_text = normalize_vietnamese(text.lower())
            
            keywords = ['ms', 'ma', 'maso', 'ma so', 'san pham', 'tu van', 'xem', 'so']
            
            temp_text = re.sub(re.escape(detected_ms.lower()), '', temp_text)
            
            for kw in keywords:
                temp_text = re.sub(r'\b' + re.escape(kw) + r'\b', '', temp_text)
            
            ms_number = re.search(r'MS(\d+)', detected_ms)
            if ms_number:
                num = ms_number.group(1)
                num_stripped = num.lstrip('0')
                if num_stripped:
                    temp_text = re.sub(r'\b' + re.escape(num_stripped) + r'\b', '', temp_text)
                    for i in range(1, 7):
                        padded = num_stripped.zfill(i)
                        temp_text = re.sub(r'\b' + re.escape(padded) + r'\b', '', temp_text)
            
            temp_text = re.sub(r'[^\w]', '', temp_text)
            
            is_only_product_code = len(temp_text.strip()) == 0
        
        if detected_ms and detected_ms in PRODUCTS:
            current_ms = detected_ms
            ctx["last_ms"] = detected_ms
            update_product_context(uid, detected_ms)
            
            if is_only_product_code:
                send_product_info_debounced(uid, detected_ms)
                ctx["processing_lock"] = False
                return
        else:
            current_ms = get_relevant_product_for_question(uid, text)
        
        print(f"[GPT CALL] User: {uid}, MS: {current_ms}, Text: {text}")
        gpt_response = generate_gpt_response(uid, text, current_ms)
        send_message(uid, gpt_response)
        
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
# WEBHOOK HANDLER - ĐÃ SỬA LỖI GỬI TIN NHẮN LẶP
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
                        # **GIỮ NGUYÊN**: Cập nhật context cho người dùng
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
            # XỬ LÝ REFERRAL (TỪ QUẢNG CÁO, FACEBOOK SHOP)
            # ============================================
            if m.get("referral"):
                ref = m["referral"]
                ctx = USER_CONTEXT[sender_id]
                ctx["referral_source"] = ref.get("source", "unknown")
                referral_payload = ref.get("ref", "")
                ctx["referral_payload"] = referral_payload
                
                print(f"[REFERRAL] User {sender_id} từ {ctx['referral_source']} với payload: {referral_payload}")
                
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
        "image_processing": "base64+fallback",
        "image_debounce_enabled": True,
        "image_carousel": "5_products",
        "search_algorithm": "TF-IDF_cosine_similarity",
        "accuracy_improved": True,
        "fchat_echo_processing": True,
        "bot_echo_filter": True,
        "referral_auto_processing": True,
        "message_debounce_enabled": True,
        "duplicate_protection": True
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
    print(f"🟢 Search Algorithm: TF-IDF + Cosine Similarity")
    print(f"🟢 Image Carousel: 5 sản phẩm phù hợp nhất")
    print(f"🟢 Image Debounce: 3 giây")
    print(f"🟢 Text Message Debounce: 1 giây")
    print(f"🟢 Echo Message Debounce: 2 giây")
    print(f"🟢 Bot Echo Filter: BẬT (phân biệt echo từ bot vs Fchat)")
    print(f"🟢 Fchat Echo Processing: BẬT (giữ nguyên logic trích xuất mã từ Fchat)")
    print(f"🟢 Referral Auto Processing: BẬT")
    print(f"🟢 Duplicate Message Protection: BẬT")
    app.run(host="0.0.0.0", port=5000, debug=True)
