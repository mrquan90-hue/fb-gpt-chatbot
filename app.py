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
    if app_id in BOT_APP_IDS:
        return True
    
    if echo_text:
        # Mở rộng danh sách pattern nhận diện tin nhắn bot
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
            "Dạ, theo thông tin sản phẩm thì",
            "Dạ, sản phẩm này được làm từ",
            "Dạ, với cân nặng",
            "Dạ, giá bán của sản phẩm",
            "Dạ, theo thông tin sản phẩm",
            "Dạ, sản phẩm này",
            "Anh/chị cần em tư vấn thêm gì",
            "Nếu anh/chị cần thêm thông tin",
            "Cảm ơn anh/chị đã quan tâm",
            "Shop sẽ liên hệ xác nhận",
            "Đơn hàng đã được tiếp nhận",
            "🌟 **5 ƯU ĐIỂM NỔI BẬT**",
            "🛒 ĐƠN HÀNG MỚI",
            "🎉 Shop đã nhận được đơn hàng",
            "Dạ, anh/chị có thể đặt hàng bộ sản phẩm",
            "Dạ, phần này trong hệ thống chưa có",
            "Anh/chị vui lòng liên hệ shop",
        ]
        
        for pattern in bot_response_patterns:
            if pattern in echo_text:
                return True
        
        # Thêm check regex cho các mẫu bot thường gặp
        bot_patterns_regex = [
            r"Dạ,.*\d{1,3}[.,]?\d{0,3}\s*đ",  # Giá tiền
            r"Dạ,.*size\s*[A-Z0-9]+",  # Nhắc đến size
            r"Dạ,.*màu\s*\w+",  # Nhắc đến màu
            r"\[\w+\]\s*\w+",  # Format [MS...] Tên sản phẩm
            r"Dạ,.*\d+\s*ngày",  # Thời gian giao hàng
            r"Dạ,.*\d+\s*kg",  # Cân nặng
            r"Dạ,.*\d+\s*cm",  # Kích thước
        ]
        
        for pattern in bot_patterns_regex:
            if re.search(pattern, echo_text, re.IGNORECASE):
                return True
        
        # Check thêm: nếu bắt đầu bằng "Dạ," và có độ dài > 50 ký tự, rất có thể là bot
        if echo_text.strip().startswith("Dạ,") and len(echo_text) > 50:
            return True
    
    return False

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
    m = re.search(r"(\d[\d.,]*)", str(price_str))
    if not m:
        return None
    cleaned = m.group(1).replace(".", "").replace(",", "")
    try:
        return int(cleaned)
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
    if not text: 
        return None
    
    m = re.search(r"MS(\d{2,6})", text.upper())
    if m: 
        full_ms = "MS" + m.group(1).zfill(6)
        return full_ms if full_ms in PRODUCTS else None
    
    nums = re.findall(r"\b(\d{2,6})\b", text)
    for n in nums:
        clean_n = n.lstrip("0")
        if clean_n in PRODUCTS_BY_NUMBER: 
            return PRODUCTS_BY_NUMBER[clean_n]
    
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
    
    # ƯU TIÊN 2: Nếu không có trong context, tìm trong tin nhắn
    if not current_ms:
        detected_ms = detect_ms_from_text(text)
        if detected_ms and detected_ms in PRODUCTS:
            current_ms = detected_ms
            ctx["last_ms"] = current_ms
            update_product_context(uid, current_ms)
    
    # ƯU TIÊN 3: Nếu vẫn không có, hỏi lại khách
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
    
    element = {
        "title": f"[{ms}] {product.get('Ten', '')}",
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
    update_product_context(uid, ms)
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
            update_product_context(uid, ms)
            
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
            update_product_context(uid, ms)
            
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
    """Xử lý tin nhắn văn bản với logic: tin nhắn 1 → carousel, từ tin nhắn 2 → GPT"""
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
        # 1. Tin nhắn đầu tiên (real_message_count == 1): Gửi carousel, KHÔNG GPT
        # 2. Từ tin nhắn thứ 2 trở đi: LUÔN dùng GPT Function Calling
        last_ms = ctx.get("last_ms")
        
        if message_count == 1 and last_ms and last_ms in PRODUCTS:
            print(f"🚨 [FIRST REAL MESSAGE] Tin nhắn THẬT đầu tiên từ user {uid}")
            print(f"🚨 [FIRST MESSAGE RULE] BỎ QUA nội dung '{text[:50]}...', gửi carousel cho {last_ms}")
            
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
# HANDLE IMAGE
# ============================================

def handle_image(uid: str, image_url: str):
    """Xử lý ảnh sản phẩm"""
    ctx = USER_CONTEXT[uid]
    
    now = time.time()
    last_image_time = ctx.get("last_image_time", 0)
    if now - last_image_time < 3:
        print(f"[IMAGE DEBOUNCE] Bỏ qua ảnh mới, chưa đủ thời gian")
        return
    
    ctx["last_image_time"] = now
    
    send_message(uid, "🖼️ Em đã nhận được ảnh sản phẩm!")
    send_message(uid, "Để em tư vấn chính xác, anh/chị vui lòng gửi mã sản phẩm hoặc mô tả sản phẩm ạ!")

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
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
# WEBHOOK HANDLER (ĐÃ SỬA ĐỂ TRÁNH CẬP NHẬT SAI CONTEXT)
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
        messaging = e.get("messaging", [])
        for m in messaging:
            sender_id = m.get("sender", {}).get("id")
            if not sender_id:
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
                                        ctx["last_ms"] = ms_from_retailer
                                        ctx["referral_source"] = "catalog"
                                        ctx["has_sent_first_carousel"] = False
                                        update_product_context(sender_id, ms_from_retailer)
                                        print(f"[CATALOG] Lưu retailer_id: {retailer_id} -> MS: {ms_from_retailer}")

            # Xử lý echo message từ Fchat - CẢI THIỆN BẢO VỆ
            if m.get("message", {}).get("is_echo"):
                recipient_id = m.get("recipient", {}).get("id")
                if not recipient_id:
                    continue
                
                msg = m["message"]
                msg_mid = msg.get("mid")
                echo_text = msg.get("text", "")
                app_id = msg.get("app_id", "")
                
                # Debug logging
                print(f"[ECHO DEBUG] Text: {echo_text[:100]}")
                print(f"[ECHO DEBUG] App ID: {app_id}")
                
                # Kiểm tra xem echo có phải từ bot không
                if is_bot_generated_echo(echo_text, app_id):
                    print(f"[ECHO BOT] Bỏ qua echo message từ bot: {echo_text[:50]}...")
                    continue
                
                # Kiểm tra trùng lặp
                if msg_mid:
                    ctx = USER_CONTEXT[recipient_id]
                    if "processed_echo_mids" not in ctx:
                        ctx["processed_echo_mids"] = set()
                    
                    if msg_mid in ctx["processed_echo_mids"]:
                        continue
                    
                    now = time.time()
                    last_echo_time = ctx.get("last_echo_processed_time", 0)
                    
                    if now - last_echo_time < 2:
                        continue
                    
                    ctx["last_echo_processed_time"] = now
                    ctx["processed_echo_mids"].add(msg_mid)
                    
                    if len(ctx["processed_echo_mids"]) > 20:
                        ctx["processed_echo_mids"] = set(list(ctx["processed_echo_mids"])[-20:])
                
                print(f"[ECHO USER] Đang xử lý echo từ bình luận người dùng")
                load_products()
                
                detected_ms = detect_ms_from_text(echo_text)
                
                if detected_ms and detected_ms in PRODUCTS:
                    ctx = USER_CONTEXT[recipient_id]
                    
                    # BẢO VỆ QUAN TRỌNG: Kiểm tra xem echo có từ khóa bot không
                    bot_keywords = ["Dạ,", "ạ!", "em ", "anh/chị", "shop ", "của em", "tư vấn", "hỗ trợ"]
                    if any(keyword in echo_text for keyword in bot_keywords) and len(echo_text) > 20:
                        print(f"[ECHO SAFETY] Tin nhắn dài có từ khóa bot, không cập nhật context từ echo")
                        print(f"[ECHO IGNORE] Bỏ qua echo có chứa mã: {detected_ms} (tin nhắn bot)")
                        continue
                    
                    # Chỉ cập nhật nếu user chưa có last_ms hoặc echo ngắn (có thể là comment user)
                    current_ms = ctx.get("last_ms")
                    echo_text_clean = echo_text.strip()
                    
                    if current_ms and len(echo_text_clean) > 10:
                        # Giữ nguyên context hiện tại nếu echo dài
                        print(f"[ECHO CONTEXT GUARD] Giữ nguyên context hiện tại: {current_ms}")
                        
                        # Chỉ cập nhật nếu echo ngắn (có thể là comment user)
                        if len(echo_text_clean) < 30:
                            print(f"[ECHO SHORT] Tin nhắn ngắn ({len(echo_text_clean)} chars), có thể là comment user, cập nhật context")
                        else:
                            continue
                    
                    print(f"[ECHO FCHAT] Phát hiện mã sản phẩm: {detected_ms} cho user: {recipient_id}")
                    
                    if ctx.get("processing_lock"):
                        continue
                    
                    ctx["processing_lock"] = True
                    
                    try:
                        ctx["last_ms"] = detected_ms
                        ctx["has_sent_first_carousel"] = False
                        ctx["referral_source"] = "fchat_echo"
                        update_product_context(recipient_id, detected_ms)
                        
                        print(f"[ECHO CONTEXT] Đã cập nhật context cho user {recipient_id} với MS: {detected_ms}")
                        print(f"[CONTEXT UPDATED] Đã ghi nhận mã {detected_ms} vào ngữ cảnh")
                        
                    finally:
                        ctx["processing_lock"] = False
                else:
                    print(f"[ECHO FCHAT] Không tìm thấy mã sản phẩm trong echo: {echo_text[:100]}...")
                
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
                        
                        ctx["last_ms"] = ms_from_ad
                        ctx["has_sent_first_carousel"] = False
                        ctx["referral_source"] = "ADS"
                        update_product_context(sender_id, ms_from_ad)
                        
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
                            ctx["last_ms"] = detected_ms
                            ctx["has_sent_first_carousel"] = False
                            ctx["referral_source"] = "ADS"
                            update_product_context(sender_id, detected_ms)
                            
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
                        
                        ctx["last_ms"] = detected_ms
                        ctx["has_sent_first_carousel"] = False
                        update_product_context(sender_id, detected_ms)
                        
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
# ORDER FORM PAGE - CẢI TIẾN MỚI
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

    # Tạo HTML với form địa chỉ mới
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8" />
        <title>Đặt hàng - {row.get('Ten','')}</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link href="https://cdn.jsdelivr.net/npm/select2@4.1.0-rc.0/dist/css/select2.min.css" rel="stylesheet" />
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
                        {"<img id='product-image' src='" + default_image + "' class='product-image' onerror=\"this.onerror=null; this.src='https://via.placeholder.com/120x120?text=No+Image'\" />" if default_image else "<div class='placeholder-image'>Chưa có ảnh sản phẩm</div>"}
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

        <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/select2@4.1.0-rc.0/dist/js/select2.min.js"></script>
        <script>
            const PRODUCT_MS = "{ms}";
            const PRODUCT_UID = "{uid}";
            let BASE_PRICE = {price_int};
            const DOMAIN = "{'https://' + DOMAIN if not DOMAIN.startswith('http') else DOMAIN}";
            const API_BASE_URL = "{('/api' if DOMAIN.startswith('http') else 'https://' + DOMAIN + '/api')}";
            
            // Biến lưu thông tin địa chỉ
            let addressData = {{
                provinces: [],
                districts: [],
                wards: []
            }};
            
            function formatPrice(n) {{
                return n.toLocaleString('vi-VN') + ' đ';
            }}
            
            function updatePriceDisplay() {{
                const quantity = parseInt(document.getElementById('quantity').value || '1');
                document.getElementById('total-display').innerText = formatPrice(BASE_PRICE * quantity);
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
                            productImage.src = data.image;
                            productImage.style.display = 'block';
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
            
            // Hàm load danh sách tỉnh/thành
            async function loadProvinces() {{
                try {{
                    const response = await fetch('https://provinces.open-api.vn/api/p/');
                    addressData.provinces = await response.json();
                    
                    const provinceSelect = $('#province');
                    provinceSelect.empty();
                    provinceSelect.append('<option value="">Chọn tỉnh/thành phố</option>');
                    
                    addressData.provinces.forEach(province => {{
                        provinceSelect.append(`<option value="${{province.code}}">${{province.name}}</option>`);
                    }});
                    
                    // Khởi tạo Select2
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
                    
                    addressData.districts = provinceData.districts || [];
                    
                    const districtSelect = $('#district');
                    districtSelect.empty();
                    districtSelect.append('<option value="">Chọn quận/huyện</option>');
                    
                    addressData.districts.forEach(district => {{
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
                    
                    addressData.wards = districtData.wards || [];
                    
                    const wardSelect = $('#ward');
                    wardSelect.empty();
                    wardSelect.append('<option value="">Chọn phường/xã</option>');
                    
                    addressData.wards.forEach(ward => {{
                        wardSelect.append(`<option value="${{ward.code}}">${{ward.name}}</option>`);
                    }});
                    
                    wardSelect.prop('disabled', false).trigger('change');
                    
                }} catch (error) {{
                    console.error('Lỗi khi load phường/xã:', error);
                }}
            }}
            
            // Hàm lấy tên địa chỉ từ mã
            function getAddressName(code, type) {{
                const data = addressData[type];
                const item = data.find(item => item.code == code);
                return item ? item.name : '';
            }}
            
            async function submitOrder() {{
                // Lấy thông tin từ form
                const formData = {{
                    ms: PRODUCT_MS,
                    uid: PRODUCT_UID,
                    color: document.getElementById('color').value,
                    size: document.getElementById('size').value,
                    quantity: parseInt(document.getElementById('quantity').value || '1'),
                    customerName: document.getElementById('customerName').value.trim(),
                    phone: document.getElementById('phone').value.trim(),
                    // Địa chỉ mới
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
                
                const phoneRegex = /^(0|\+84)(\d{9,10})$/;
                if (!phoneRegex.test(formData.phone)) {{
                    alert('Số điện thoại không hợp lệ. Vui lòng nhập số điện thoại 10-11 chữ số (ví dụ: 0912345678 hoặc +84912345678)');
                    document.getElementById('phone').focus();
                    return;
                }}
                
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
                const provinceName = getAddressName(formData.province, 'provinces') || '';
                const districtName = getAddressName(formData.district, 'districts') || '';
                const wardName = getAddressName(formData.ward, 'wards') || '';
                
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
                        alert('🎉 Đã gửi đơn hàng thành công!\\n\\nShop sẽ liên hệ xác nhận trong 5-10 phút.\\nCảm ơn anh/chị đã đặt hàng! ❤️');
                        
                        // Reset form
                        document.getElementById('orderForm').reset();
                        $('#province, #district, #ward').val('').trigger('change');
                        $('#district').prop('disabled', true);
                        $('#ward').prop('disabled', true);
                        
                    }} else {{
                        alert(`❌ ${{data.message || 'Có lỗi xảy ra. Vui lòng thử lại sau'}}`);
                    }}
                }} catch (error) {{
                    alert('❌ Lỗi kết nối. Vui lòng thử lại sau!');
                }} finally {{
                    submitBtn.innerHTML = originalText;
                    submitBtn.disabled = false;
                }}
            }}
            
            // Khởi tạo khi trang được tải
            document.addEventListener('DOMContentLoaded', function() {{
                // Load danh sách tỉnh/thành
                loadProvinces();
                
                // Cập nhật giá khi thay đổi số lượng
                document.getElementById('quantity').addEventListener('input', updatePriceDisplay);
                
                // Gọi cập nhật biến thể lần đầu
                updateVariantInfo();
                
                // Focus vào trường tên
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
    
    # Địa chỉ mới
    address_detail = data.get("addressDetail") or ""
    province_name = data.get("provinceName") or ""
    district_name = data.get("districtName") or ""
    ward_name = data.get("wardName") or ""
    full_address = data.get("fullAddress") or ""
    
    # Nếu không có full_address, ghép từ các thành phần
    if not full_address and address_detail:
        full_address = f"{address_detail}, {ward_name}, {district_name}, {province_name}"
    
    load_products()
    row = PRODUCTS.get(ms)
    if not row:
        return {"error": "not_found", "message": "Sản phẩm không tồn tại"}, 404

    price_str = row.get("Gia", "0")
    price_int = extract_price_int(price_str) or 0
    total = price_int * quantity
    
    product_name = row.get('Ten', '')

    if uid:
        ctx = USER_CONTEXT.get(uid, {})
        referral_source = ctx.get("referral_source", "direct")
        
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
            "🚚 Đơn hàng sẽ được giao bởi ViettelPost\n"
            "💳 Thanh toán khi nhận hàng (COD)\n"
            "────────────────────\n"
            "Cảm ơn anh/chị đã đặt hàng! ❤️"
        )
        send_message(uid, msg)
    
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
        "unit_price": price_int,
        "total_price": total,
        "referral_source": ctx.get("referral_source", "direct") if uid else "direct"
    }
    
    write_success = write_order_to_google_sheet_api(order_data)
    
    if not write_success:
        save_order_to_local_csv(order_data)
    
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
            "address": full_address,
            "total": total,
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
    
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "products_loaded": len(PRODUCTS),
        "variants_loaded": total_variants,
        "openai_configured": bool(client),
        "facebook_configured": bool(PAGE_ACCESS_TOKEN),
        "fanpage_name": current_fanpage_name,
        "google_sheets_integration": {
            "sheet_id_configured": bool(GOOGLE_SHEET_ID),
            "credentials_configured": bool(GOOGLE_SHEETS_CREDENTIALS_JSON)
        },
        "gpt_function_calling": {
            "enabled": True,
            "tools": ["get_product_price_details", "get_product_basic_info", "send_product_images", "send_product_videos", "provide_order_link"],
            "model": "gpt-4o-mini",
            "first_message_logic": "Carousel 1 sản phẩm",
            "second_message_logic": "GPT Function Calling",
            "price_analysis": "Thông minh (color_based, size_based, complex_based, single_price)",
            "policy_handling": "GPT tự đọc mô tả sản phẩm (không dùng tool riêng, không dùng từ khóa)"
        },
        "features": {
            "carousel_first_message": True,
            "catalog_support": True,
            "ads_referral_processing": True,
            "fchat_echo_processing": True,
            "image_processing": True,
            "order_form": True,
            "google_sheets_api": True
        }
    }, 200

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
    print(f"🟢 Domain: {DOMAIN}")
    print(f"🟢 Google Sheets API: {'SẴN SÀNG' if GOOGLE_SHEET_ID and GOOGLE_SHEETS_CREDENTIALS_JSON else 'CHƯA CẤU HÌNH'}")
    print(f"🟢 OpenAI Function Calling: {'TÍCH HỢP THÀNH CÔNG' if client else 'CHƯA CẤU HÌNH'}")
    print("=" * 80)
    
    print("🔴 QUAN TRỌNG: TÍNH NĂNG GPT FUNCTION CALLING")
    print("=" * 80)
    print(f"🔴 Tin nhắn đầu tiên: Carousel 1 sản phẩm")
    print(f"🔴 Từ tin nhắn thứ 2: GPT Function Calling với CONTEXT PRIORITY")
    print(f"🔴 Tools: get_product_price_details, get_product_basic_info, send_product_images, provide_order_link")
    print(f"🔴 Price Analysis: Thông minh (phân tích theo màu, size, complex)")
    print(f"🔴 Policy Handling: GPT tự đọc mô tả (KHÔNG dùng tool riêng, KHÔNG dùng từ khóa)")
    print(f"🔴 Context Tracking: Ghi nhớ MS từ echo Fchat, ad_title, catalog")
    print(f"🔴 Real Message Counter: Đếm tin nhắn thật từ user")
    print(f"🔴 Postback Idempotency: Mỗi postback chỉ xử lý 1 lần")
    print("=" * 80)
    
    print("🔴 CẢI THIỆN BẢO VỆ CONTEXT:")
    print("=" * 80)
    print(f"🔴 Hàm is_bot_generated_echo: Mở rộng pattern nhận diện")
    print(f"🔴 Echo processing: Kiểm tra từ khóa bot, độ dài tin nhắn")
    print(f"🔴 System prompt: Thêm quy tắc không nhắc mã sản phẩm khác")
    print("=" * 80)
    
    print("🔴 FORM ĐẶT HÀNG CẢI TIẾN:")
    print("=" * 80)
    print(f"🔴 Cập nhật ảnh và giá theo biến thể: /api/get-variant-info")
    print(f"🔴 Địa chỉ theo API: Tỉnh/Huyện/Xã + địa chỉ chi tiết")
    print(f"🔴 Sử dụng Select2 cho UI tốt hơn")
    print(f"🔴 Fallback khi API địa chỉ lỗi")
    print("=" * 80)
    
    load_products()
    
    # Lấy port từ biến môi trường
    port = get_port()
    print(f"🟢 Đang khởi động server trên port: {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
