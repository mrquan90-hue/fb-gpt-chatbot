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
# GOOGLE SHEETS API INTEGRATION
# ============================================
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    GOOGLE_API_AVAILABLE = True
except ImportError:
    print("⚠️ Google API libraries not installed.")
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
DOMAIN = os.getenv("DOMAIN", "").strip() or "shocked-rheba-khohang24h-5d45ac79.koyeb.app"
FANPAGE_NAME = os.getenv("FANPAGE_NAME", "Shop thời trang")
FCHAT_WEBHOOK_URL = os.getenv("FCHAT_WEBHOOK_URL", "").strip()
FCHAT_TOKEN = os.getenv("FCHAT_TOKEN", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
GOOGLE_SHEETS_CREDENTIALS_JSON = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON", "").strip()

if not GOOGLE_SHEET_CSV_URL:
    GOOGLE_SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv"

BOT_APP_IDS = {"645956568292435"}
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
    "product_info_sent_ms": None,
    "last_product_info_time": 0,
    "processed_mids": {}, # Để chặn trùng lặp MID
    "product_history": [],
    "conversation_history": [],
    "referral_source": None,
    "last_processed_text": "",
    "catalog_products": {},
    "first_message_after_referral": False,
    "pending_carousel_ms": None,
})

PRODUCTS = {}
PRODUCTS_BY_NUMBER = {}
PRODUCT_TEXT_EMBEDDINGS = {}
LAST_LOAD = 0
LOAD_TTL = 300

ORDER_KEYWORDS = [
    "đặt hàng nha", "ok đặt", "ok mua", "ok em", "ok e", "mua 1 cái", "mua cái này", "mua luôn", 
    "chốt", "lấy mã", "lấy mẫu", "lấy luôn", "lấy em này", "ship cho", "chốt đơn", "bán cho em", "lấy nha", "mua nha", "order nhé"
]
CAROUSEL_KEYWORDS = ["xem sản phẩm", "show sản phẩm", "có gì hot", "sản phẩm mới", "danh sách sản phẩm", "tất cả sản phẩm", "mẫu mới"]
CHANGE_PRODUCT_KEYWORDS = ["còn hàng nào khác", "có mẫu nào khác", "cho xem cái khác", "mẫu khác", "sản phẩm khác"]

VIETNAMESE_MAP = {'à': 'a', 'á': 'a', 'ả': 'a', 'ã': 'a', 'ạ': 'a', 'ă': 'a', 'ằ': 'a', 'ắ': 'a', 'ẳ': 'a', 'ẵ': 'a', 'ặ': 'a', 'â': 'a', 'ầ': 'a', 'ấ': 'a', 'ẩ': 'a', 'ẫ': 'a', 'ậ': 'a', 'đ': 'd', 'è': 'e', 'é': 'e', 'ẻ': 'e', 'ẽ': 'e', 'ẹ': 'e', 'ê': 'e', 'ề': 'e', 'ế': 'e', 'ể': 'e', 'ễ': 'e', 'ệ': 'e', 'ì': 'i', 'í': 'i', 'ỉ': 'i', 'ĩ': 'i', 'ị': 'i', 'ò': 'o', 'ó': 'o', 'ỏ': 'o', 'õ': 'o', 'ọ': 'o', 'ô': 'o', 'ồ': 'o', 'ố': 'o', 'ổ': 'o', 'ỗ': 'o', 'ộ': 'o', 'ơ': 'o', 'ờ': 'o', 'ớ': 'o', 'ở': 'o', 'ỡ': 'o', 'ợ': 'o', 'ù': 'u', 'ú': 'u', 'ủ': 'u', 'ũ': 'u', 'ụ': 'u', 'ư': 'u', 'ừ': 'u', 'ứ': 'u', 'ử': 'u', 'ữ': 'u', 'ự': 'u', 'ỳ': 'y', 'ý': 'y', 'ỷ': 'y', 'ỹ': 'y', 'ỵ': 'y', 'À': 'A', 'Á': 'A', 'Ả': 'A', 'Ã': 'A', 'Ạ': 'A', 'Ă': 'A', 'Ằ': 'A', 'Ắ': 'A', 'Ẳ': 'A', 'Ẵ': 'A', 'Ặ': 'A', 'Â': 'A', 'Ầ': 'A', 'Ấ': 'A', 'Ẩ': 'A', 'Ẫ': 'A', 'Ậ': 'A', 'Đ': 'D', 'È': 'E', 'É': 'E', 'Ẻ': 'E', 'Ẽ': 'E', 'Ẹ': 'E', 'Ê': 'E', 'Ề': 'E', 'Ế': 'E', 'Ể': 'E', 'Ễ': 'E', 'Ệ': 'E', 'Ì': 'I', 'Í': 'I', 'Ỉ': 'I', 'Ĩ': 'I', 'Ị': 'I', 'Ò': 'O', 'Ó': 'O', 'Ỏ': 'O', 'Õ': 'O', 'Ọ': 'O', 'Ô': 'O', 'Ồ': 'O', 'Ố': 'O', 'Ổ': 'O', 'Ỗ': 'O', 'Ộ': 'O', 'Ơ': 'O', 'Ờ': 'O', 'Ớ': 'O', 'Ở': 'O', 'Ỡ': 'O', 'Ợ': 'O', 'Ù': 'U', 'Ú': 'U', 'Ủ': 'U', 'Ũ': 'U', 'Ụ': 'U', 'Ư': 'U', 'Ừ': 'U', 'Ứ': 'U', 'Ử': 'U', 'Ữ': 'U', 'Ự': 'U', 'Ỳ': 'Y', 'Ý': 'Y', 'Ỷ': 'Y', 'Ỹ': 'Y', 'Ỵ': 'Y'}

def normalize_vietnamese(text):
    if not text: return ""
    for char, replacement in VIETNAMESE_MAP.items():
        text = text.replace(char, replacement)
    return text

# ============================================
# DATA HELPERS
# ============================================

def load_products(force=False):
    global PRODUCTS, LAST_LOAD, PRODUCTS_BY_NUMBER, PRODUCT_TEXT_EMBEDDINGS
    now = time.time()
    if not force and PRODUCTS and (now - LAST_LOAD) < LOAD_TTL: return

    try:
        print(f"🟦 Loading sheet: {GOOGLE_SHEET_CSV_URL}")
        r = requests.get(GOOGLE_SHEET_CSV_URL, timeout=20)
        r.raise_for_status()
        reader = csv.DictReader(r.text.splitlines())
        
        prods = {}
        by_num = {}
        for row in reader:
            ms = (row.get("Mã sản phẩm") or "").strip().upper()
            if not ms: continue
            if ms not in prods:
                prods[ms] = {
                    "MS": ms, "Ten": row.get("Tên sản phẩm"), "Gia": row.get("Giá bán"),
                    "MoTa": row.get("Mô tả"), "Images": row.get("Images"), "variants": []
                }
            prods[ms]["variants"].append(row)
            num = ms[2:].lstrip('0')
            if num: by_num[num] = ms
        
        PRODUCTS, PRODUCTS_BY_NUMBER = prods, by_num
        LAST_LOAD = now
        print(f"📦 Loaded {len(PRODUCTS)} products.")
    except Exception as e:
        print(f"❌ Load Error: {e}")

def parse_image_urls(raw: str):
    if not raw: return []
    return [u.strip() for u in re.split(r'[,\n;|]+', raw) if u.strip() and "http" in u]

def short_description(text: str, limit: int = 220):
    if not text: return ""
    clean = re.sub(r"\s+", " ", str(text)).strip()
    return clean[:limit] + "..." if len(clean) > limit else clean

def extract_price_int(price_str: str):
    if not price_str: return None
    m = re.search(r"(\d[\d.,]*)", str(price_str))
    if not m: return None
    try: return int(m.group(1).replace(".", "").replace(",", ""))
    except: return None

def detect_ms_from_text(text: str) -> Optional[str]:
    if not text: return None
    m = re.search(r"MS(\d{2,6})", text.upper())
    if m:
        full_ms = "MS" + m.group(1).zfill(6)
        return full_ms if full_ms in PRODUCTS else None
    nums = re.findall(r"\b(\d{2,6})\b", text)
    for n in nums:
        clean_n = n.lstrip("0")
        if clean_n in PRODUCTS_BY_NUMBER: return PRODUCTS_BY_NUMBER[clean_n]
    return None

# ============================================
# FACEBOOK SEND API
# ============================================

def call_facebook_send_api(payload: dict):
    if not PAGE_ACCESS_TOKEN: return
    url = f"https://graph.facebook.com/v12.0/me/messages?access_token={PAGE_ACCESS_TOKEN}"
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"❌ Send API Error: {e}")

def send_message(uid, text):
    if not text: return
    call_facebook_send_api({"recipient": {"id": uid}, "message": {"text": text}})

def send_image(uid, url):
    if not url: return
    call_facebook_send_api({"recipient": {"id": uid}, "message": {"attachment": {"type": "image", "payload": {"url": url, "is_reusable": True}}}})

def send_carousel_template(uid, elements):
    if not elements: return
    call_facebook_send_api({"recipient": {"id": uid}, "message": {"attachment": {"type": "template", "payload": {"template_type": "generic", "elements": elements[:10]}}}})

# ============================================
# LOGIC CORE: PRODUCT INFO (THREADING READY)
# ============================================

def send_product_info_debounced(uid: str, ms: str):
    """Gửi thông tin sản phẩm đầy đủ - Chạy trong Thread để tránh timeout"""
    ctx = USER_CONTEXT[uid]
    now = time.time()
    
    # Chặn gửi trùng mã trong 10 giây
    if ctx.get("product_info_sent_ms") == ms and (now - ctx.get("last_product_info_time", 0)) < 10:
        return

    ctx["product_info_sent_ms"] = ms
    ctx["last_product_info_time"] = now
    ctx["processing_lock"] = True

    try:
        load_products()
        p = PRODUCTS.get(ms)
        if not p:
            send_message(uid, "Dạ em không tìm thấy thông tin sản phẩm này.")
            return

        ctx["last_ms"] = ms
        
        # Gửi tiêu đề
        send_message(uid, f"📌 [{ms}] {p.get('Ten', 'Sản phẩm')}")
        time.sleep(0.3)

        # Gửi ảnh (tối đa 5 ảnh)
        urls = parse_image_urls(p.get("Images", ""))
        unique_urls = list(dict.fromkeys(urls))[:5]
        for url in unique_urls:
            send_image(uid, url)
            time.sleep(0.6)

        # Gửi mô tả
        send_message(uid, f"📝 MÔ TẢ:\n{short_description(p.get('MoTa', ''), 300)}")
        time.sleep(0.4)

        # Gửi giá
        prices = [extract_price_int(v.get('Giá bán')) for v in p.get('variants', []) if extract_price_int(v.get('Giá bán'))]
        if prices:
            price_msg = f"💰 Giá ưu đãi: **{min(prices):,.0f}đ – {max(prices):,.0f}đ**"
        else:
            price_msg = f"💰 Giá bán: {p.get('Gia', 'Liên hệ shop')}"
        send_message(uid, price_msg)
        
        time.sleep(0.4)
        domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
        send_message(uid, f"📋 Đặt hàng tại đây:\n{domain}/order-form?ms={ms}&uid={uid}")

    except Exception as e:
        print(f"❌ Error in send_product_info_thread: {e}")
    finally:
        ctx["processing_lock"] = False

# ============================================
# CHAT HANDLERS (GPT & TOOLS)
# ============================================

def get_tools_definition():
    return [
        {"type": "function", "function": {"name": "get_product_info", "description": "Lấy chi tiết giá, ảnh, mô tả sản phẩm.", "parameters": {"type": "object", "properties": {"ms": {"type": "string"}}}}},
        {"type": "function", "function": {"name": "show_featured_carousel", "description": "Hiện danh sách sản phẩm nổi bật.", "parameters": {"type": "object", "properties": {}}}},
        {"type": "function", "function": {"name": "provide_order_link", "description": "Gửi link đặt hàng cho khách.", "parameters": {"type": "object", "properties": {"ms": {"type": "string"}}}}}
    ]

def execute_tool(uid, name, args):
    ms = args.get("ms", "").upper() or USER_CONTEXT[uid].get("last_ms")
    if name == "get_product_info":
        if ms: threading.Thread(target=send_product_info_debounced, args=(uid, ms)).start()
    elif name == "provide_order_link":
        if ms:
            domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
            send_message(uid, f"Dạ mời anh/chị đặt hàng tại đây ạ:\n{domain}/order-form?ms={ms}&uid={uid}")
    elif name == "show_featured_carousel":
        load_products()
        elements = []
        for code, p in list(PRODUCTS.items())[:5]:
            img = parse_image_urls(p["Images"])[0] if p["Images"] else ""
            elements.append({
                "title": f"[{code}] {p['Ten']}",
                "image_url": img,
                "subtitle": f"Giá: {p['Gia']}",
                "buttons": [{"type": "postback", "title": "🔍 Chi tiết", "payload": f"ADVICE_{code}"}]
            })
        send_carousel_template(uid, elements)

def handle_text_with_function_calling(uid, text):
    ctx = USER_CONTEXT[uid]
    fanpage_name = get_fanpage_name_from_api()
    system_prompt = f"Bạn là nhân viên bán hàng của {fanpage_name}. Trả lời ngắn gọn, xưng em gọi anh/chị. Nếu khách hỏi còn hàng không, luôn báo CÒN HÀNG."

    messages = [{"role": "system", "content": system_prompt}]
    for h in ctx["conversation_history"][-6:]: messages.append(h)
    messages.append({"role": "user", "content": text})

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini", messages=messages, tools=get_tools_definition(), temperature=0.1
        )
        msg = response.choices[0].message
        if msg.tool_calls:
            for tool in msg.tool_calls:
                execute_tool(uid, tool.function.name, json.loads(tool.function.arguments))
        elif msg.content:
            send_message(uid, msg.content)
            ctx["conversation_history"].append({"role": "user", "content": text})
            ctx["conversation_history"].append({"role": "assistant", "content": msg.content})
    except Exception as e:
        print(f"❌ GPT Error: {e}")

# ============================================
# WEBHOOK HANDLER (DEDUPLICATION & THREADING)
# ============================================

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        if request.args.get("hub.verify_token") == VERIFY_TOKEN:
            return request.args.get("hub.challenge"), 200
        return "Wrong token", 403

    data = request.get_json() or {}
    for entry in data.get("entry", []):
        for m in entry.get("messaging", []):
            sender_id = m.get("sender", {}).get("id")
            if not sender_id: continue

            ctx = USER_CONTEXT[sender_id]

            # --- DEDUPLICATION: Chống trùng MID trong 60 giây ---
            current_mid = m.get("message", {}).get("mid") or m.get("postback", {}).get("mid")
            if current_mid:
                if "processed_mids" not in ctx: ctx["processed_mids"] = {}
                if current_mid in ctx["processed_mids"]:
                    return "OK", 200
                ctx["processed_mids"][current_mid] = time.time()
                # Clean old mids
                now = time.time()
                ctx["processed_mids"] = {k: v for k, v in ctx["processed_mids"].items() if now - v < 60}

            # --- XỬ LÝ POSTBACK ---
            if "postback" in m:
                payload = m["postback"].get("payload", "")
                if payload.startswith("ADVICE_"):
                    ms = payload.replace("ADVICE_", "")
                    threading.Thread(target=send_product_info_debounced, args=(sender_id, ms)).start()
                elif payload.startswith("ORDER_"):
                    ms = payload.replace("ORDER_", "")
                    execute_tool(sender_id, "provide_order_link", {"ms": ms})
                return "OK", 200

            # --- XỬ LÝ TIN NHẮN VĂN BẢN ---
            if "message" in m and "text" in m["message"]:
                text = m["message"]["text"]
                
                # Ưu tiên mã sản phẩm
                ms = detect_ms_from_text(text)
                if ms:
                    threading.Thread(target=send_product_info_debounced, args=(sender_id, ms)).start()
                else:
                    # Xử lý hội thoại bình thường
                    handle_text_with_function_calling(sender_id, text)

            # --- XỬ LÝ ECHO TỪ FCHAT ---
            if m.get("message", {}).get("is_echo"):
                echo_text = m["message"].get("text", "")
                if not is_bot_generated_echo(echo_text, m["message"].get("app_id", "")):
                    detected_ms = detect_ms_from_text(echo_text)
                    if detected_ms:
                        recipient_id = m.get("recipient", {}).get("id")
                        USER_CONTEXT[recipient_id]["last_ms"] = detected_ms
                return "OK", 200

    return "OK", 200

# ============================================
# OTHERS (HTML & API)
# ============================================

def get_fanpage_name_from_api():
    return FANPAGE_NAME

def is_bot_generated_echo(text, app_id):
    if app_id in BOT_APP_IDS: return True
    for p in ["📌 [MS", "📝 MÔ TẢ:", "💰 Giá"]:
        if p in text: return True
    return False

@app.route("/order-form", methods=["GET"])
def order_form():
    ms = (request.args.get("ms") or "").upper()
    return f"<html><body><h2>Đang mở form đặt hàng cho mã {ms}...</h2></body></html>"

@app.route("/", methods=["GET"])
def home():
    return "Bot is running", 200

if __name__ == "__main__":
    load_products()
    app.run(host="0.0.0.0", port=8000)
