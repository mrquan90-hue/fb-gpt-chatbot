import os
import json
import re
import time
import csv
import hashlib
import base64
import threading
from collections import defaultdict
from urllib.parse import quote, urlencode
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from io import BytesIO

# ============================================
# IMPORTS CƠ BẢN
# ============================================
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    print("⚠️ requests not installed")
    REQUESTS_AVAILABLE = False

try:
    from flask import Flask, request, send_from_directory, jsonify, render_template_string, render_template
    FLASK_AVAILABLE = True
except ImportError:
    print("⚠️ flask not installed")
    FLASK_AVAILABLE = False

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    print("⚠️ openai not installed")
    OPENAI_AVAILABLE = False

# ============================================
# GOOGLE SHEETS API
# ============================================
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    GOOGLE_API_AVAILABLE = True
except ImportError:
    print("⚠️ Google API libraries not installed")
    GOOGLE_API_AVAILABLE = False

# ============================================
# FLASK APP
# ============================================
app = Flask(__name__, template_folder='templates', static_folder='static')

# ============================================
# ENV & CONFIG - SỬ DỤNG BIẾN TỪ KOYEB
# ============================================
PORT = int(os.environ.get("PORT", 5000))

PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN", "").strip()
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "Aa.123456").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
GOOGLE_SHEET_CSV_URL = os.getenv("SHEET_CSV_URL", "").strip()
DOMAIN = os.getenv("DOMAIN", "").strip() or "shocked-rheba-khohang24h-5d45ac79.koyeb.app"
FANPAGE_NAME = os.getenv("FANPAGE_NAME", "Shop thời trang")
FCHAT_WEBHOOK_URL = os.getenv("FCHAT_WEBHOOK_URL", "").strip()
FCHAT_TOKEN = os.getenv("FCHAT_TOKEN", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
FREEIMAGE_API_KEY = os.getenv("FREEIMAGE_API_KEY", "").strip()

# Parse Google Sheets credentials từ JSON string
GOOGLE_SHEETS_CREDENTIALS_JSON = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON", "")
if GOOGLE_SHEETS_CREDENTIALS_JSON:
    try:
        GOOGLE_SHEETS_CREDENTIALS = json.loads(GOOGLE_SHEETS_CREDENTIALS_JSON)
    except json.JSONDecodeError as e:
        print(f"❌ Lỗi parse GOOGLE_SHEETS_CREDENTIALS_JSON: {e}")
        GOOGLE_SHEETS_CREDENTIALS = None
else:
    GOOGLE_SHEETS_CREDENTIALS = None

# ============================================
# APP ID CỦA BOT
# ============================================
BOT_APP_IDS = {"645956568292435"}

# ============================================
# OPENAI CLIENT
# ============================================
client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY and OPENAI_AVAILABLE else None

# ============================================
# CACHE CHO DỮ LIỆU ĐỊA CHỈ
# ============================================
ADDRESS_CACHE = {
    'provinces': [],
    'districts': {},  # province_code -> districts
    'wards': {},      # district_code -> wards
    'last_updated': 0,
    'cache_ttl': 24 * 60 * 60  # 24 giờ
}

ADDRESS_LOCK = threading.Lock()

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
    "product_info_sent_ms": None,
    "last_product_info_time": 0,
    "product_history": [],
    "conversation_history": [],
    "referral_source": None,
    "referral_payload": None,
})

PRODUCTS = {}
PRODUCTS_BY_NUMBER = {}
LAST_LOAD = 0
LOAD_TTL = 300

# ============================================
# ADDRESS API FUNCTIONS - GỌI TỪ provinces.open-api.vn
# ============================================

def fetch_provinces():
    """Lấy danh sách tỉnh/thành từ API"""
    try:
        response = requests.get('https://provinces.open-api.vn/api/p/', timeout=10)
        if response.status_code == 200:
            provinces = response.json()
            # Sắp xếp theo tên
            provinces.sort(key=lambda x: x['name'])
            return provinces
        else:
            print(f"❌ Lỗi khi gọi API tỉnh/thành: {response.status_code}")
            return []
    except Exception as e:
        print(f"❌ Lỗi kết nối API tỉnh/thành: {e}")
        return []

def fetch_districts(province_code):
    """Lấy danh sách quận/huyện từ API theo mã tỉnh"""
    try:
        response = requests.get(f'https://provinces.open-api.vn/api/p/{province_code}?depth=2', timeout=10)
        if response.status_code == 200:
            province_data = response.json()
            districts = province_data.get('districts', [])
            districts.sort(key=lambda x: x['name'])
            return districts
        else:
            print(f"❌ Lỗi khi gọi API quận/huyện: {response.status_code}")
            return []
    except Exception as e:
        print(f"❌ Lỗi kết nối API quận/huyện: {e}")
        return []

def fetch_wards(district_code):
    """Lấy danh sách phường/xã từ API theo mã quận"""
    try:
        response = requests.get(f'https://provinces.open-api.vn/api/d/{district_code}?depth=2', timeout=10)
        if response.status_code == 200:
            district_data = response.json()
            wards = district_data.get('wards', [])
            wards.sort(key=lambda x: x['name'])
            return wards
        else:
            print(f"❌ Lỗi khi gọi API phường/xã: {response.status_code}")
            return []
    except Exception as e:
        print(f"❌ Lỗi kết nối API phường/xã: {e}")
        return []

def get_cached_provinces():
    """Lấy danh sách tỉnh/thành từ cache hoặc API"""
    with ADDRESS_LOCK:
        now = time.time()
        
        # Kiểm tra cache còn hiệu lực không
        if (ADDRESS_CACHE['provinces'] and 
            (now - ADDRESS_CACHE['last_updated']) < ADDRESS_CACHE['cache_ttl']):
            return ADDRESS_CACHE['provinces']
        
        # Gọi API để lấy dữ liệu mới
        print("🔄 Đang cập nhật danh sách tỉnh/thành từ API...")
        provinces = fetch_provinces()
        
        if provinces:
            ADDRESS_CACHE['provinces'] = provinces
            ADDRESS_CACHE['last_updated'] = now
            print(f"✅ Đã cập nhật {len(provinces)} tỉnh/thành")
            return provinces
        else:
            # Fallback: trả về cache cũ nếu có, hoặc danh sách rỗng
            return ADDRESS_CACHE['provinces'] or []

def get_cached_districts(province_code):
    """Lấy danh sách quận/huyện từ cache hoặc API"""
    with ADDRESS_LOCK:
        # Kiểm tra trong cache
        if province_code in ADDRESS_CACHE['districts']:
            return ADDRESS_CACHE['districts'][province_code]
        
        # Gọi API để lấy dữ liệu
        print(f"🔄 Đang cập nhật quận/huyện cho tỉnh {province_code}...")
        districts = fetch_districts(province_code)
        
        if districts:
            ADDRESS_CACHE['districts'][province_code] = districts
            return districts
        else:
            return []

def get_cached_wards(district_code):
    """Lấy danh sách phường/xã từ cache hoặc API"""
    with ADDRESS_LOCK:
        # Kiểm tra trong cache
        if district_code in ADDRESS_CACHE['wards']:
            return ADDRESS_CACHE['wards'][district_code]
        
        # Gọi API để lấy dữ liệu
        print(f"🔄 Đang cập nhật phường/xã cho quận {district_code}...")
        wards = fetch_wards(district_code)
        
        if wards:
            ADDRESS_CACHE['wards'][district_code] = wards
            return wards
        else:
            return []

# ============================================
# HELPER FUNCTIONS
# ============================================

def extract_ms_from_retailer_id(retailer_id: str) -> Optional[str]:
    """Trích xuất mã sản phẩm từ retailer_id"""
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

def is_bot_generated_echo(echo_text: str, app_id: str = "") -> bool:
    """Kiểm tra echo message có phải từ bot không"""
    if app_id in BOT_APP_IDS:
        return True
    
    if echo_text:
        bot_patterns = [
            "Dạ, phần này trong hệ thống",
            "Chào anh/chị! 👋",
            "Em là trợ lý AI",
            "📌 [MS",
            "📝 MÔ TẢ:",
            "💰 GIÁ SẢN PHẨM:",
            "📋 Đặt hàng ngay tại đây:",
        ]
        
        for pattern in bot_patterns:
            if pattern in echo_text:
                return True
    
    return False

# ============================================
# FACEBOOK API FUNCTIONS
# ============================================

def call_facebook_send_api(payload: dict, retry_count=2):
    """Gửi tin nhắn qua Facebook API"""
    if not PAGE_ACCESS_TOKEN or not REQUESTS_AVAILABLE:
        print("[WARN] Không có PAGE_ACCESS_TOKEN hoặc requests")
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
                    print(f"[ERROR] Người dùng đã chặn/hủy kết nối")
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
    if not text or not REQUESTS_AVAILABLE:
        return
    if len(text) > 2000:
        text = text[:1997] + "..."
    
    payload = {
        "recipient": {"id": recipient_id},
        "message": {"text": text},
    }
    return call_facebook_send_api(payload)

def send_image(recipient_id: str, image_url: str):
    if not image_url or not REQUESTS_AVAILABLE:
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
    if not elements or not REQUESTS_AVAILABLE:
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

# ============================================
# PRODUCT MANAGEMENT
# ============================================

def parse_image_urls(raw: str):
    """Parse URLs ảnh từ chuỗi"""
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
            '.png', '.webp', '.gif', 'freeimage.host'
        ]):
            urls.append(p)
    
    seen = set()
    result = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            result.append(u)
    
    return result

def short_description(text: str, limit: int = 220) -> str:
    """Rút gọn mô tả"""
    if not text:
        return ""
    clean = re.sub(r"\s+", " ", str(text)).strip()
    if len(clean) <= limit:
        return clean
    return clean[:limit].rstrip() + "..."

def extract_price_int(price_str: str):
    """Trích xuất giá dạng int"""
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
    """Đọc dữ liệu từ Google Sheet CSV"""
    global PRODUCTS, LAST_LOAD, PRODUCTS_BY_NUMBER
    now = time.time()
    if not force and PRODUCTS and (now - LAST_LOAD) < LOAD_TTL:
        return

    if not GOOGLE_SHEET_CSV_URL or not REQUESTS_AVAILABLE:
        print("❌ Không thể load sản phẩm")
        return

    try:
        print(f"🟦 Loading sheet từ CSV: {GOOGLE_SHEET_CSV_URL}")
        r = requests.get(GOOGLE_SHEET_CSV_URL, timeout=30)
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
            mota = (row.get("Mô tả") or "").strip()
            mau = (row.get("màu (Thuộc tính)") or "").strip()
            size = (row.get("size (Thuộc tính)") or "").strip()

            gia_int = extract_price_int(gia_raw)

            if ms not in products:
                products[ms] = {
                    "MS": ms,
                    "Ten": ten,
                    "Gia": gia_raw,
                    "MoTa": mota,
                    "Images": images,
                    "màu (Thuộc tính)": mau,
                    "size (Thuộc tính)": size,
                    "ShortDesc": short_description(mota),
                    "RawRow": row
                }

            if ms.startswith("MS"):
                num_part = ms[2:]
                num_without_leading_zeros = num_part.lstrip('0')
                if num_without_leading_zeros:
                    products_by_number[num_without_leading_zeros] = ms

        PRODUCTS = products
        PRODUCTS_BY_NUMBER = products_by_number
        LAST_LOAD = now
        
        print(f"📦 Đã load {len(PRODUCTS)} sản phẩm")
        print(f"🔢 Đã tạo mapping cho {len(PRODUCTS_BY_NUMBER)} mã số sản phẩm")
        
    except Exception as e:
        print("❌ Lỗi load_products:", e)

def detect_ms_from_text(text: str):
    """Tìm mã sản phẩm trong tin nhắn"""
    if not text:
        return None
    
    # 1. Tìm [MS123456]
    ms_list = re.findall(r"\[MS(\d{6})\]", text.upper())
    if ms_list:
        ms = "MS" + ms_list[0]
        if ms in PRODUCTS:
            return ms
    
    # 2. Tìm #MS123456
    ms_list = re.findall(r"#MS(\d{6})", text.upper())
    if ms_list:
        ms = "MS" + ms_list[0]
        if ms in PRODUCTS:
            return ms
    
    # 3. Tìm MS123456
    ms_list = re.findall(r"MS(\d{6})", text.upper())
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
    
    return None

# ============================================
# GOOGLE SHEETS API FUNCTIONS
# ============================================

def get_google_sheets_service():
    """
    Khởi tạo Google Sheets service
    """
    if not GOOGLE_SHEETS_CREDENTIALS or not GOOGLE_SHEET_ID:
        print("⚠️ Chưa cấu hình đầy đủ Google Sheets")
        return None

    if not GOOGLE_API_AVAILABLE:
        print("⚠️ Google API libraries chưa được cài đặt")
        return None

    try:
        credentials = service_account.Credentials.from_service_account_info(
            GOOGLE_SHEETS_CREDENTIALS,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        
        service = build('sheets', 'v4', credentials=credentials)
        print("✅ Đã khởi tạo Google Sheets service thành công")
        return service
        
    except Exception as e:
        print(f"❌ Lỗi khởi tạo Google Sheets service: {e}")
        return None

def write_order_to_google_sheet(order_data: dict):
    """
    Ghi đơn hàng vào Google Sheet
    """
    service = get_google_sheets_service()
    if service is None:
        print("❌ Không thể ghi vì không khởi tạo được Google Sheets Service")
        return False
    
    sheet_name = "Orders"
    
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        order_id = f"ORD{int(time.time())}_{order_data.get('uid', '')[-4:]}"
        
        new_row = [
            timestamp,
            order_id,
            "Mới",
            order_data.get("ms", ""),
            order_data.get("product_name", ""),
            order_data.get("color", ""),
            order_data.get("size", ""),
            order_data.get("quantity", 1),
            order_data.get("unit_price", 0),
            order_data.get("total_price", 0),
            order_data.get("customer_name", ""),
            order_data.get("phone", ""),
            order_data.get("address", ""),
            "COD",
            "ViettelPost",
            f"Đơn từ Facebook Bot",
            order_data.get("uid", ""),
            order_data.get("referral_source", "direct")
        ]
        
        request = service.spreadsheets().values().append(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=f"{sheet_name}!A:R",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [new_row]}
        )
        
        response = request.execute()
        
        print(f"✅ ĐÃ GHI ĐƠN HÀNG VÀO GOOGLE SHEET THÀNH CÔNG!")
        print(f"   - Mã đơn: {order_id}")
        
        return True
        
    except Exception as e:
        print(f"❌ Lỗi Google Sheets API: {e}")
        return False

# ============================================
# GPT RESPONSE
# ============================================

def generate_gpt_response(uid: str, user_message: str, ms: str = None):
    """Gọi GPT để trả lời"""
    if not client or not OPENAI_API_KEY:
        return "Hiện tại hệ thống trợ lý AI đang bảo trì, vui lòng thử lại sau ạ."
    
    try:
        # Tạo link đặt hàng nếu có mã sản phẩm
        order_link = ""
        if ms and ms in PRODUCTS:
            order_link = f"https://{DOMAIN}/order-form?ms={ms}&uid={uid}"
        
        if ms and ms in PRODUCTS:
            product = PRODUCTS[ms]
            
            system_prompt = f"""Bạn là NHÂN VIÊN TƯ VẤN BÁN HÀNG của {FANPAGE_NAME}.
Bạn đang tư vấn sản phẩm mã: {ms}

THÔNG TIN SẢN PHẨM:
- Tên: {product.get('Ten', '')}
- Giá: {product.get('Gia', 'Liên hệ shop')}
- Mô tả: {product.get('ShortDesc', 'Chưa có mô tả chi tiết')}
- Màu sắc: {product.get('màu (Thuộc tính)', 'Chưa có thông tin')}
- Size: {product.get('size (Thuộc tính)', 'Chưa có thông tin')}

QUY TẮC TRẢ LỜI:
1. TRẢ LỜI NGẮN GỌN - TỐI ĐA 3 DÒNG
2. Dựa vào thông tin sản phẩm trên, KHÔNG bịa thông tin
3. Nếu không biết: "Em chưa có thông tin về phần này ạ"
4. Nếu khách muốn đặt hàng: GỬI LINK NGAY
5. Link đặt hàng: {order_link}
6. Xưng "em", gọi "anh/chị"

Hãy trả lời TỰ NHIÊN và ĐÚNG với thông tin sản phẩm."""
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
            max_tokens=150,
            timeout=10.0,
        )
        
        reply = response.choices[0].message.content.strip()
        
        # Thay thế [link] bằng link thật
        if order_link and "[link]" in reply:
            reply = reply.replace("[link]", order_link)
        
        conversation.append({"role": "user", "content": user_message})
        conversation.append({"role": "assistant", "content": reply})
        ctx["conversation_history"] = conversation
        
        return reply
        
    except Exception as e:
        print(f"GPT Error: {e}")
        return "Dạ em đang gặp chút trục trặc. Anh/chị vui lòng thử lại sau ạ."

# ============================================
# PRODUCT INFO HANDLING
# ============================================

def send_product_info(uid: str, ms: str):
    """Gửi thông tin sản phẩm"""
    ctx = USER_CONTEXT[uid]
    now = time.time()

    last_ms = ctx.get("product_info_sent_ms")
    last_time = ctx.get("last_product_info_time", 0)

    if last_ms == ms and (now - last_time) < 5:
        print(f"[DEBOUNCE] Bỏ qua gửi lại thông tin sản phẩm {ms}")
        return

    ctx["product_info_sent_ms"] = ms
    ctx["last_product_info_time"] = now
    ctx["processing_lock"] = True
    ctx["last_ms"] = ms

    try:
        load_products()
        product = PRODUCTS.get(ms)
        if not product:
            send_message(uid, "Em không tìm thấy sản phẩm này trong hệ thống ạ.")
            ctx["processing_lock"] = False
            return

        product_name = product.get('Ten', 'Sản phẩm')
        send_message(uid, f"📌 {product_name}")
        time.sleep(0.5)

        # Gửi ảnh
        images_field = product.get("Images", "")
        urls = parse_image_urls(images_field)
        
        if urls:
            # Ưu tiên ảnh từ freeimage.host nếu có API key
            if FREEIMAGE_API_KEY:
                for url in urls[:3]:  # Gửi tối đa 3 ảnh
                    if 'freeimage.host' in url:
                        send_image(uid, url)
                        time.sleep(0.7)
                    else:
                        # Có thể upload ảnh lên freeimage.host nếu cần
                        send_image(uid, url)
                        time.sleep(0.7)
            else:
                send_image(uid, urls[0])
                time.sleep(0.7)
        else:
            send_message(uid, "📷 Sản phẩm chưa có hình ảnh ạ.")
        
        time.sleep(0.5)

        # Gửi mô tả
        mo_ta = product.get("MoTa", "")
        if mo_ta:
            short_desc = short_description(mo_ta, 200)
            send_message(uid, f"📝 {short_desc}")
        else:
            send_message(uid, "📝 Sản phẩm hiện chưa có thông tin chi tiết ạ.")
        
        time.sleep(0.5)

        # Gửi giá
        gia_raw = product.get("Gia", "")
        gia_int = extract_price_int(gia_raw)
        if gia_int:
            price_msg = f"💰 Giá: {gia_int:,.0f}đ"
        else:
            price_msg = "💰 Giá đang cập nhật"
        
        send_message(uid, price_msg)
        
        time.sleep(0.5)

        # Gửi link đặt hàng
        order_link = f"https://{DOMAIN}/order-form?ms={ms}&uid={uid}"
        send_message(uid, f"📋 Đặt hàng: {order_link}")

    except Exception as e:
        print(f"Lỗi khi gửi thông tin sản phẩm: {str(e)}")
        try:
            send_message(uid, "Có lỗi khi tải thông tin sản phẩm.")
        except:
            pass
    finally:
        ctx["processing_lock"] = False

# ============================================
# TEXT MESSAGE HANDLING
# ============================================

def handle_text(uid: str, text: str):
    """Xử lý tin nhắn văn bản"""
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
                print(f"[TEXT DEBOUNCE] Bỏ qua tin nhắn trùng lặp")
                ctx["processing_lock"] = False
                return
        
        ctx["last_msg_time"] = now
        ctx["last_processed_text"] = text.strip().lower()
        
        load_products()
        
        lower = text.lower()
        
        # Xử lý từ khóa đặc biệt
        ORDER_KEYWORDS = ["đặt hàng", "mua", "chốt", "lấy mã", "ship cho", "tôi lấy"]
        CAROUSEL_KEYWORDS = ["xem sản phẩm", "show sản phẩm", "có gì hot", "danh sách sản phẩm"]
        
        if any(kw in lower for kw in ORDER_KEYWORDS):
            current_ms = ctx.get("last_ms")
            if current_ms and current_ms in PRODUCTS:
                order_link = f"https://{DOMAIN}/order-form?ms={current_ms}&uid={uid}"
                reply = f"Dạ, sản phẩm còn hàng ạ!\nĐặt tại: {order_link}"
                send_message(uid, reply)
                ctx["processing_lock"] = False
                return
        
        if any(kw in lower for kw in CAROUSEL_KEYWORDS):
            if PRODUCTS:
                send_message(uid, "Dạ, em đang lấy danh sách sản phẩm...")
                
                carousel_elements = []
                
                for i, (ms, product) in enumerate(list(PRODUCTS.items())[:5]):
                    images_field = product.get("Images", "")
                    urls = parse_image_urls(images_field)
                    image_url = urls[0] if urls else ""
                    
                    short_desc = product.get("ShortDesc", "") or short_description(product.get("MoTa", ""))
                    
                    element = {
                        "title": product.get('Ten', ''),
                        "image_url": image_url,
                        "subtitle": short_desc[:80] + "..." if len(short_desc) > 80 else short_desc,
                        "buttons": [
                            {
                                "type": "web_url",
                                "url": f"https://{DOMAIN}/order-form?ms={ms}&uid={uid}",
                                "title": "🛒 Đặt ngay"
                            }
                        ]
                    }
                    carousel_elements.append(element)
                
                if carousel_elements:
                    send_carousel_template(uid, carousel_elements)
                    send_message(uid, "📱 Anh/chị vuốt sang trái/phải để xem thêm sản phẩm nhé!")
                else:
                    send_message(uid, "Hiện tại shop chưa có sản phẩm nào để hiển thị ạ.")
                
                ctx["processing_lock"] = False
                return
            else:
                send_message(uid, "Hiện tại shop chưa có sản phẩm nào ạ.")
                ctx["processing_lock"] = False
                return

        # Tìm mã sản phẩm trong tin nhắn
        detected_ms = detect_ms_from_text(text)
        if detected_ms and detected_ms in PRODUCTS:
            print(f"[MS DETECTED] Phát hiện mã mới: {detected_ms}")
            ctx["last_ms"] = detected_ms
            send_product_info(uid, detected_ms)
            ctx["processing_lock"] = False
            return
        
        # Lấy sản phẩm hiện tại
        current_ms = ctx.get("last_ms")
        
        # Gọi GPT để trả lời
        print(f"[GPT CALL] User: {uid}, MS: {current_ms}")
        gpt_response = generate_gpt_response(uid, text, current_ms)
        send_message(uid, gpt_response)

    except Exception as e:
        print(f"Error in handle_text for {uid}: {e}")
        try:
            send_message(uid, "Dạ em đang gặp chút trục trặc, vui lòng thử lại sau ạ.")
        except:
            pass
    finally:
        if ctx.get("processing_lock"):
            ctx["processing_lock"] = False

# ============================================
# ROUTES
# ============================================

@app.route("/", methods=["GET"])
def home():
    return "Facebook Chatbot đang hoạt động!", 200

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

            # Xử lý echo message
            if m.get("message", {}).get("is_echo"):
                msg = m["message"]
                echo_text = msg.get("text", "")
                app_id = msg.get("app_id", "")
                
                if is_bot_generated_echo(echo_text, app_id):
                    continue
                
                # Tìm mã sản phẩm trong echo
                load_products()
                detected_ms = detect_ms_from_text(echo_text)
                if detected_ms and detected_ms in PRODUCTS:
                    ctx = USER_CONTEXT[sender_id]
                    ctx["last_ms"] = detected_ms
                    print(f"[ECHO FCHAT] Phát hiện mã {detected_ms} cho user {sender_id}")
                else:
                    print(f"[ECHO FCHAT] Không tìm thấy mã sản phẩm trong echo: {echo_text}")
                
                continue
            
            if m.get("delivery") or m.get("read"):
                continue
            
            # Xử lý referral
            if m.get("referral"):
                ref = m["referral"]
                ctx = USER_CONTEXT[sender_id]
                ctx["referral_source"] = ref.get("source", "unknown")
                referral_payload = ref.get("ref", "")
                ctx["referral_payload"] = referral_payload
                
                print(f"[REFERRAL] User {sender_id} từ {ctx['referral_source']}")
                
                # Xử lý catalog referral
                if referral_payload:
                    detected_ms = detect_ms_from_text(referral_payload)
                    if detected_ms and detected_ms in PRODUCTS:
                        ctx["last_ms"] = detected_ms
                        welcome_msg = f"""Chào anh/chị! 👋 
Em là trợ lý AI của {FANPAGE_NAME}.

Em thấy anh/chị quan tâm đến sản phẩm **[{detected_ms}]**.
Em sẽ gửi thông tin chi tiết sản phẩm ngay ạ!"""
                        
                        send_message(sender_id, welcome_msg)
                        send_product_info(sender_id, detected_ms)
                        continue
                
                # Welcome message chung
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
                if payload == "GET_STARTED":
                    welcome_msg = f"""Chào anh/chị! 👋 
Em là trợ lý AI của {FANPAGE_NAME}.

Để em tư vấn chính xác, anh/chị vui lòng:
1. Gửi mã sản phẩm (ví dụ: [MS123456])
2. Hoặc gõ "xem sản phẩm" để xem danh sách
3. Hoặc mô tả sản phẩm bạn đang tìm

Anh/chị quan tâm sản phẩm nào ạ?"""
                    send_message(sender_id, welcome_msg)
                elif payload and payload.startswith("ADVICE_"):
                    ms = payload.replace("ADVICE_", "")
                    if ms in PRODUCTS:
                        ctx = USER_CONTEXT[sender_id]
                        ctx["last_ms"] = ms
                        send_product_info(sender_id, ms)
                continue
            
            # Xử lý tin nhắn thường
            if "message" in m:
                msg = m["message"]
                text = msg.get("text")
                
                if text:
                    handle_text(sender_id, text)

    return "OK", 200

@app.route("/order-form", methods=["GET"])
def order_form():
    """Hiển thị form đặt hàng từ template"""
    ms = (request.args.get("ms") or "").upper()
    uid = request.args.get("uid") or ""
    
    if not ms:
        return render_template('order-form.html', 
                             error="Không tìm thấy sản phẩm",
                             fanpage_name=FANPAGE_NAME)
    
    load_products()
    if ms not in PRODUCTS:
        return render_template('order-form.html',
                             error="Sản phẩm không tồn tại",
                             fanpage_name=FANPAGE_NAME)
    
    product = PRODUCTS[ms]
    
    # Parse màu và size
    colors = []
    sizes = []
    
    color_field = product.get("màu (Thuộc tính)", "")
    if color_field:
        colors = [c.strip() for c in color_field.split(",") if c.strip()]
    
    size_field = product.get("size (Thuộc tính)", "")
    if size_field:
        sizes = [s.strip() for s in size_field.split(",") if s.strip()]
    
    if not colors:
        colors = ["Mặc định"]
    if not sizes:
        sizes = ["Mặc định"]
    
    # Lấy ảnh sản phẩm
    images_field = product.get("Images", "")
    urls = parse_image_urls(images_field)
    image_url = urls[0] if urls else ""
    
    # Lấy giá
    price_str = product.get("Gia", "0")
    price_int = extract_price_int(price_str) or 0
    
    # Pre-load provinces để cache sẵn
    provinces = get_cached_provinces()
    
    return render_template('order-form.html',
                         ms=ms,
                         uid=uid,
                         product=product,
                         colors=colors,
                         sizes=sizes,
                         image_url=image_url,
                         price=price_int,
                         price_display=f"{price_int:,.0f}đ",
                         fanpage_name=FANPAGE_NAME,
                         domain=DOMAIN,
                         provinces_count=len(provinces) if provinces else 0)

@app.route("/static/<path:filename>")
def static_files(filename):
    return send_from_directory('static', filename)

# ============================================
# API ENDPOINTS CHO ĐỊA CHỈ
# ============================================

@app.route("/api/get-vietnam-address", methods=["GET"])
def api_get_vietnam_address():
    """API trả về danh sách tỉnh/thành"""
    try:
        provinces = get_cached_provinces()
        
        if not provinces:
            return jsonify({
                "success": False,
                "message": "Không thể lấy dữ liệu địa chỉ từ API",
                "data": {"provinces": []}
            }), 500
        
        # Format dữ liệu để front-end dễ sử dụng
        formatted_provinces = [
            {
                "code": str(p["code"]),
                "name": p["name"],
                "name_with_type": p.get("name_with_type", p["name"])
            }
            for p in provinces
        ]
        
        return jsonify({
            "success": True,
            "message": f"Đã tải {len(formatted_provinces)} tỉnh/thành",
            "data": {
                "provinces": formatted_provinces
            }
        })
        
    except Exception as e:
        print(f"❌ Lỗi API get-vietnam-address: {e}")
        return jsonify({
            "success": False,
            "message": f"Lỗi server: {str(e)}",
            "data": {"provinces": []}
        }), 500

@app.route("/api/get-districts", methods=["GET"])
def api_get_districts():
    """API trả về danh sách quận/huyện theo tỉnh"""
    province_code = request.args.get("province_code")
    
    if not province_code:
        return jsonify({
            "success": False,
            "message": "Thiếu tham số province_code",
            "data": {"districts": []}
        }), 400
    
    try:
        districts = get_cached_districts(province_code)
        
        if not districts:
            return jsonify({
                "success": False,
                "message": "Không tìm thấy quận/huyện cho tỉnh này",
                "data": {"districts": []}
            }), 404
        
        # Format dữ liệu
        formatted_districts = [
            {
                "code": str(d["code"]),
                "name": d["name"],
                "name_with_type": d.get("name_with_type", d["name"]),
                "province_code": str(d.get("province_code", province_code))
            }
            for d in districts
        ]
        
        return jsonify({
            "success": True,
            "message": f"Đã tải {len(formatted_districts)} quận/huyện",
            "data": {
                "districts": formatted_districts
            }
        })
        
    except Exception as e:
        print(f"❌ Lỗi API get-districts: {e}")
        return jsonify({
            "success": False,
            "message": f"Lỗi server: {str(e)}",
            "data": {"districts": []}
        }), 500

@app.route("/api/get-wards", methods=["GET"])
def api_get_wards():
    """API trả về danh sách phường/xã theo quận"""
    district_code = request.args.get("district_code")
    
    if not district_code:
        return jsonify({
            "success": False,
            "message": "Thiếu tham số district_code",
            "data": {"wards": []}
        }), 400
    
    try:
        wards = get_cached_wards(district_code)
        
        if not wards:
            return jsonify({
                "success": False,
                "message": "Không tìm thấy phường/xã cho quận này",
                "data": {"wards": []}
            }), 404
        
        # Format dữ liệu
        formatted_wards = [
            {
                "code": str(w["code"]),
                "name": w["name"],
                "name_with_type": w.get("name_with_type", w["name"]),
                "district_code": str(w.get("district_code", district_code))
            }
            for w in wards
        ]
        
        return jsonify({
            "success": True,
            "message": f"Đã tải {len(formatted_wards)} phường/xã",
            "data": {
                "wards": formatted_wards
            }
        })
        
    except Exception as e:
        print(f"❌ Lỗi API get-wards: {e}")
        return jsonify({
            "success": False,
            "message": f"Lỗi server: {str(e)}",
            "data": {"wards": []}
        }), 500

# ============================================
# API ENDPOINTS KHÁC
# ============================================

@app.route("/api/get-product", methods=["GET"])
def api_get_product():
    """API lấy thông tin sản phẩm"""
    ms = (request.args.get("ms") or "").upper()
    
    load_products()
    if ms not in PRODUCTS:
        return {"error": "not_found"}, 404
    
    product = PRODUCTS[ms]
    
    # Parse màu và size
    colors = []
    sizes = []
    
    color_field = product.get("màu (Thuộc tính)", "")
    if color_field:
        colors = [c.strip() for c in color_field.split(",") if c.strip()]
    
    size_field = product.get("size (Thuộc tính)", "")
    if size_field:
        sizes = [s.strip() for s in size_field.split(",") if s.strip()]
    
    if not colors:
        colors = ["Mặc định"]
    if not sizes:
        sizes = ["Mặc định"]
    
    # Lấy ảnh
    images_field = product.get("Images", "")
    urls = parse_image_urls(images_field)
    image_url = urls[0] if urls else ""
    
    # Lấy giá
    price_str = product.get("Gia", "0")
    price_int = extract_price_int(price_str) or 0
    
    return {
        "ms": ms,
        "name": product.get("Ten", ""),
        "description": product.get("MoTa", ""),
        "price": price_int,
        "price_display": f"{price_int:,.0f}đ",
        "colors": colors,
        "sizes": sizes,
        "image": image_url
    }

@app.route("/api/submit-order", methods=["POST"])
def api_submit_order():
    """API nhận đơn hàng"""
    data = request.get_json() or {}
    ms = (data.get("ms") or "").upper()
    uid = data.get("uid") or ""
    customer_name = data.get("customerName") or ""
    phone = data.get("phone") or ""
    address = data.get("address") or ""
    province_name = data.get("provinceName", "")
    district_name = data.get("districtName", "")
    ward_name = data.get("wardName", "")
    address_detail = data.get("addressDetail", "")
    color = data.get("color", "")
    size = data.get("size", "")
    quantity = int(data.get("quantity") or 1)
    
    load_products()
    product = PRODUCTS.get(ms)
    if not product:
        return {"error": "not_found", "message": "Sản phẩm không tồn tại"}, 404
    
    # Lấy giá
    price_str = product.get("Gia", "0")
    price_int = extract_price_int(price_str) or 0
    total = price_int * quantity
    
    product_name = product.get('Ten', '')
    
    # Gửi thông báo cho khách hàng qua Messenger
    if uid and PAGE_ACCESS_TOKEN:
        # Xây dựng địa chỉ đầy đủ
        full_address_parts = []
        if address_detail:
            full_address_parts.append(address_detail)
        if ward_name:
            full_address_parts.append(ward_name)
        if district_name:
            full_address_parts.append(district_name)
        if province_name:
            full_address_parts.append(province_name)
        
        full_address = ", ".join(full_address_parts) if full_address_parts else address
        
        msg = (
            "🎉 Shop đã nhận được đơn hàng mới:\n"
            f"🛍 Sản phẩm: [{ms}] {product_name}\n"
            f"🎨 Phân loại: {color} / {size}\n"
            f"📦 Số lượng: {quantity}\n"
            f"💰 Thành tiền: {total:,.0f} đ\n"
            f"👤 Người nhận: {customer_name}\n"
            f"📱 SĐT: {phone}\n"
            f"🏠 Địa chỉ: {full_address}\n"
            "────────────────────\n"
            "⏰ Shop sẽ gọi điện xác nhận trong 5-10 phút.\n"
            "💳 Thanh toán khi nhận hàng (COD)\n"
            "────────────────────\n"
            "Cảm ơn anh/chị đã đặt hàng! ❤️"
        )
        send_message(uid, msg)
    
    # Ghi vào Google Sheets
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
        "referral_source": USER_CONTEXT.get(uid, {}).get("referral_source", "direct")
    }
    
    write_success = write_order_to_google_sheet(order_data)
    
    # Gửi notification đến Fchat (nếu có)
    if FCHAT_WEBHOOK_URL and FCHAT_TOKEN and REQUESTS_AVAILABLE:
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
    
    order_id = f"ORD{int(time.time())}_{uid[-4:] if uid else '0000'}"
    
    return {
        "status": "ok", 
        "message": "Đơn hàng đã được tiếp nhận",
        "order_id": order_id,
        "order_written": write_success,
        "order_details": {
            "order_id": order_id,
            "product_code": ms,
            "product_name": product_name,
            "customer_name": customer_name,
            "phone": phone,
            "address": full_address if 'full_address' in locals() else address,
            "total": total
        }
    }

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint"""
    provinces = get_cached_provinces()
    
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "products_loaded": len(PRODUCTS),
        "openai_configured": bool(client),
        "facebook_configured": bool(PAGE_ACCESS_TOKEN),
        "google_sheets_configured": bool(GOOGLE_SHEETS_CREDENTIALS and GOOGLE_SHEET_ID),
        "freeimage_configured": bool(FREEIMAGE_API_KEY),
        "address_api": {
            "provinces_loaded": len(provinces) if provinces else 0,
            "cache_age_seconds": int(time.time() - ADDRESS_CACHE['last_updated']) if ADDRESS_CACHE['last_updated'] else 0,
            "api_source": "provinces.open-api.vn"
        },
        "domain": DOMAIN,
        "fanpage_name": FANPAGE_NAME,
        "koyeb": True
    }, 200

# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    print("=" * 50)
    print("🚀 KHỞI ĐỘNG BOT TRÊN KOYEB VỚI API ĐỊA CHỈ")
    print("=" * 50)
    
    # Kiểm tra cấu hình
    if not FLASK_AVAILABLE:
        print("❌ Flask chưa được cài đặt!")
        exit(1)
    
    # Load sản phẩm ban đầu
    print("📦 Đang tải sản phẩm...")
    load_products()
    
    # Pre-load địa chỉ tỉnh/thành
    print("🗺️ Đang tải dữ liệu địa chỉ từ API...")
    provinces = get_cached_provinces()
    
    # Thông tin cấu hình
    print("=" * 50)
    print("📋 THÔNG TIN CẤU HÌNH:")
    print(f"   Port: {PORT}")
    print(f"   Domain: {DOMAIN}")
    print(f"   Fanpage: {FANPAGE_NAME}")
    print(f"   Số sản phẩm: {len(PRODUCTS)}")
    print(f"   Tỉnh/thành đã tải: {len(provinces) if provinces else 0}")
    print(f"   OpenAI: {'SẴN SÀNG' if client else 'CHƯA CẤU HÌNH'}")
    print(f"   Facebook: {'SẴN SÀNG' if PAGE_ACCESS_TOKEN else 'CHƯA CẤU HÌNH'}")
    print(f"   Google Sheets: {'SẴN SÀNG' if GOOGLE_SHEETS_CREDENTIALS else 'CHƯA CẤU HÌNH'}")
    print(f"   FreeImage: {'SẴN SÀNG' if FREEIMAGE_API_KEY else 'CHƯA CẤU HÌNH'}")
    print("=" * 50)
    
    # Khởi động server
    print(f"🌐 Khởi động server trên port {PORT}...")
    app.run(host="0.0.0.0", port=PORT, debug=False)
