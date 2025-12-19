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
from flask import Flask, request, send_from_directory, jsonify, render_template
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
    GOOGLE_API_AVAILABLE = False

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
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
GOOGLE_SHEETS_CREDENTIALS_JSON = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON", "").strip()

if not GOOGLE_SHEET_CSV_URL:
    GOOGLE_SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv"

BOT_APP_IDS = {"645956568292435"}
client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ============================================
# GLOBAL STATE & CACHE
# ============================================
USER_CONTEXT = defaultdict(lambda: {
    "last_msg_time": 0,
    "last_ms": None,
    "processing_lock": False,
    "processed_message_mids": {},
    "conversation_history": [],
    "referral_source": None,
    "last_all_images_time": 0,
    "product_history": []
})

PRODUCTS = {}
PRODUCTS_BY_NUMBER = {}
LAST_LOAD = 0
LOAD_TTL = 300

# ============================================
# OPENAI TOOLS DEFINITION
# ============================================
def get_tools_definition():
    return [
        {
            "type": "function",
            "function": {
                "name": "get_product_details",
                "description": "Lấy thông tin chi tiết (giá, màu, size, mô tả) của một sản phẩm cụ thể theo mã MS.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "ms": {"type": "string", "description": "Mã sản phẩm, ví dụ: MS000004"}
                    },
                    "required": ["ms"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "search_products",
                "description": "Tìm kiếm sản phẩm trong kho dựa trên nhu cầu, màu sắc hoặc mô tả của khách hàng.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Nội dung khách muốn tìm, ví dụ: váy hoa, đồ màu đỏ"}
                    },
                    "required": ["query"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "send_images_to_user",
                "description": "Gửi trực tiếp tất cả ảnh thật của sản phẩm cho khách hàng xem.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "ms": {"type": "string", "description": "Mã sản phẩm cần gửi ảnh."}
                    },
                    "required": ["ms"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "provide_order_link",
                "description": "Cung cấp link form đặt hàng cho khách hàng khi họ muốn mua.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "ms": {"type": "string", "description": "Mã sản phẩm khách chọn mua."}
                    },
                    "required": ["ms"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "show_featured_carousel",
                "description": "Hiển thị danh sách các sản phẩm nổi bật/mẫu mới nhất dưới dạng thẻ hình ảnh.",
                "parameters": {"type": "object", "properties": {}}
            }
        }
    ]

# ============================================
# HELPER FUNCTIONS (CORE LOGIC)
# ============================================
def normalize_vietnamese(text):
    VIETNAMESE_MAP = {'à': 'a', 'á': 'a', 'ả': 'a', 'ã': 'a', 'ạ': 'a', 'ă': 'a', 'ằ': 'a', 'ắ': 'a', 'ẳ': 'a', 'ẵ': 'a', 'ặ': 'a', 'â': 'a', 'ầ': 'a', 'ấ': 'a', 'ẩ': 'a', 'ẫ': 'a', 'ậ': 'a', 'đ': 'd', 'è': 'e', 'é': 'e', 'ẻ': 'e', 'ẽ': 'e', 'ẹ': 'e', 'ê': 'e', 'ề': 'e', 'ế': 'e', 'ể': 'e', 'ễ': 'e', 'ệ': 'e', 'ì': 'i', 'í': 'i', 'ỉ': 'i', 'ĩ': 'i', 'ị': 'i', 'ò': 'o', 'ó': 'o', 'ỏ': 'o', 'õ': 'o', 'ọ': 'o', 'ô': 'o', 'ồ': 'o', 'ố': 'o', 'ổ': 'o', 'ỗ': 'o', 'ộ': 'o', 'ơ': 'o', 'ờ': 'o', 'ớ': 'o', 'ở': 'o', 'ỡ': 'o', 'ợ': 'o', 'ù': 'u', 'ú': 'u', 'ủ': 'u', 'ũ': 'u', 'ụ': 'u', 'ư': 'u', 'ừ': 'u', 'ứ': 'u', 'ử': 'u', 'ữ': 'u', 'ự': 'u', 'ỳ': 'y', 'ý': 'y', 'ỷ': 'y', 'ỹ': 'y', 'ỵ': 'y'}
    result = text
    for char, replacement in VIETNAMESE_MAP.items():
        result = result.replace(char, replacement)
        result = result.replace(char.upper(), replacement.upper())
    return result

def load_products(force=False):
    global PRODUCTS, LAST_LOAD, PRODUCTS_BY_NUMBER
    now = time.time()
    if not force and PRODUCTS and (now - LAST_LOAD) < LOAD_TTL: return
    try:
        r = requests.get(GOOGLE_SHEET_CSV_URL, timeout=15)
        r.encoding = "utf-8"
        reader = csv.DictReader(r.text.splitlines())
        new_products = {}
        new_by_number = {}
        for row in reader:
            ms = (row.get("Mã sản phẩm") or "").strip()
            if not ms: continue
            if ms not in new_products:
                new_products[ms] = {
                    "MS": ms, "Ten": row.get("Tên sản phẩm"), "Gia": row.get("Giá bán"),
                    "MoTa": row.get("Mô tả"), "Images": row.get("Images"),
                    "màu (Thuộc tính)": row.get("màu (Thuộc tính)"),
                    "size (Thuộc tính)": row.get("size (Thuộc tính)"),
                    "variants": []
                }
            new_products[ms]["variants"].append(row)
            num_only = ms.replace("MS", "").lstrip("0")
            if num_only: new_by_number[num_only] = ms
        PRODUCTS, PRODUCTS_BY_NUMBER, LAST_LOAD = new_products, new_by_number, now
    except Exception as e: print(f"Load products error: {e}")

def build_comprehensive_product_context(ms: str) -> str:
    if ms not in PRODUCTS: return "Sản phẩm không tồn tại."
    p = PRODUCTS[ms]
    return f"Mã: {ms}\nTên: {p['Ten']}\nGiá: {p['Gia']}\nMô tả: {p['MoTa']}\nMàu: {p['màu (Thuộc tính)']}\nSize: {p['size (Thuộc tính)']}\nTrạng thái: CÒN HÀNG."

def parse_image_urls(raw: str):
    if not raw: return []
    return [u.strip() for u in re.split(r'[,\n;|]+', raw) if u.strip()]

def find_product_by_keywords(text: str) -> Optional[str]:
    if not text: return None
    norm_text = normalize_vietnamese(text.lower())
    for ms, p in PRODUCTS.items():
        search_blob = normalize_vietnamese(f"{p['Ten']} {p['MoTa']} {ms}".lower())
        if all(word in search_blob for word in norm_text.split() if len(word) > 2):
            return ms
    return None

def detect_ms_from_text(text: str):
    matches = re.findall(r"MS(\d{2,6})", text.upper())
    if matches: 
        ms = "MS" + matches[0].zfill(6)
        return ms if ms in PRODUCTS else None
    num_matches = re.findall(r"\d{2,6}", text)
    if num_matches:
        num = num_matches[0].lstrip("0")
        return PRODUCTS_BY_NUMBER.get(num)
    return None

def update_product_context(uid: str, ms: str):
    ctx = USER_CONTEXT[uid]
    ctx["last_ms"] = ms
    if not ctx["product_history"] or ctx["product_history"][0] != ms:
        ctx["product_history"] = ([ms] + ctx["product_history"])[:5]

# ============================================
# FACEBOOK API HELPERS
# ============================================
def call_facebook_send_api(payload: dict):
    if not PAGE_ACCESS_TOKEN: return
    url = f"https://graph.facebook.com/v12.0/me/messages?access_token={PAGE_ACCESS_TOKEN}"
    try: requests.post(url, json=payload, timeout=10)
    except Exception as e: print(f"FB API Error: {e}")

def send_message(uid: str, text: str):
    if not text: return
    call_facebook_send_api({"recipient": {"id": uid}, "message": {"text": text[:2000]}})

def send_image(uid: str, url: str):
    call_facebook_send_api({"recipient": {"id": uid}, "message": {"attachment": {"type": "image", "payload": {"url": url}}}})

def send_carousel_template(uid: str, elements: list):
    call_facebook_send_api({"recipient": {"id": uid}, "message": {"attachment": {"type": "template", "payload": {"template_type": "generic", "elements": elements[:10]}}}})

def send_all_product_images(uid: str, ms: str):
    if ms not in PRODUCTS: return
    ctx = USER_CONTEXT[uid]
    if time.time() - ctx["last_all_images_time"] < 5: return
    ctx["last_all_images_time"] = time.time()
    urls = parse_image_urls(PRODUCTS[ms].get("Images", ""))[:10]
    if not urls: 
        send_message(uid, "Dạ sản phẩm này hiện chưa có ảnh mẫu ạ.")
        return
    send_message(uid, f"Dạ em gửi ảnh mẫu [{ms}] ạ:")
    for url in urls:
        send_image(uid, url)
        time.sleep(0.5)

# ============================================
# TOOL EXECUTION ENGINE
# ============================================
def execute_tool_call(uid: str, func_name: str, args: dict) -> str:
    ctx = USER_CONTEXT[uid]
    domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
    
    if func_name == "get_product_details":
        ms = args.get("ms", "").upper()
        if ms in PRODUCTS:
            update_product_context(uid, ms)
            return build_comprehensive_product_context(ms)
        return "Không tìm thấy mã sản phẩm này."

    elif func_name == "search_products":
        query = args.get("query", "")
        found_ms = find_product_by_keywords(query)
        if found_ms:
            update_product_context(uid, found_ms)
            return f"Tìm thấy sản phẩm phù hợp:\n{build_comprehensive_product_context(found_ms)}"
        return "Hiện không tìm thấy sản phẩm nào khớp hoàn toàn với mô tả."

    elif func_name == "send_images_to_user":
        ms = args.get("ms", "").upper() or ctx.get("last_ms")
        if ms and ms in PRODUCTS:
            send_all_product_images(uid, ms)
            return f"Đã gửi ảnh mẫu sản phẩm {ms}."
        return "Vui lòng cho biết mã sản phẩm để em gửi ảnh."

    elif func_name == "provide_order_link":
        ms = args.get("ms", "").upper() or ctx.get("last_ms")
        if ms and ms in PRODUCTS:
            link = f"{domain}/order-form?ms={ms}&uid={uid}"
            send_message(uid, f"Dạ mời anh/chị đặt hàng sản phẩm [{ms}] tại đây ạ:\n{link}")
            return f"Đã cung cấp link đặt hàng cho mã {ms}."
        return "Chưa rõ khách muốn mua mã nào để tạo link."

    elif func_name == "show_featured_carousel":
        elements = []
        for ms, p in list(PRODUCTS.items())[:5]:
            urls = parse_image_urls(p.get("Images", ""))
            elements.append({
                "title": f"[{ms}] {p['Ten']}",
                "image_url": urls[0] if urls else "",
                "subtitle": f"Giá: {p['Gia']} - Nhấn xem chi tiết để xem ảnh và đặt hàng",
                "buttons": [
                    {"type": "web_url", "url": f"{domain}/order-form?ms={ms}&uid={uid}", "title": "🛒 Đặt ngay"},
                    {"type": "postback", "title": "🔍 Chi tiết", "payload": f"ADVICE_{ms}"}
                ]
            })
        send_carousel_template(uid, elements)
        return "Đã hiển thị danh sách sản phẩm nổi bật."

    return "Yêu cầu không được xác định."

# ============================================
# MAIN TEXT HANDLER (FUNCTION CALLING)
# ============================================
def handle_text(uid: str, text: str):
    if not text or not client: return
    ctx = USER_CONTEXT[uid]
    
    # 1. Khởi tạo prompt hệ thống
    system_prompt = f"""Bạn là Trợ lý bán hàng của {FANPAGE_NAME}. 
    CHỈ dùng dữ liệu thật từ công cụ. KHÔNG bịa đặt giá hay chất liệu. 
    Nếu khách hỏi về tồn kho, LUÔN trả lời 'CÒN HÀNG'.
    Xưng 'em', gọi 'anh/chị'. Trả lời ngắn gọn dưới 3 dòng.
    Sản phẩm khách đang xem: {ctx.get('last_ms', 'Chưa có')}.
    Khi khách muốn mua hoặc chốt đơn, hãy dùng công cụ provide_order_link ngay."""

    messages = [{"role": "system", "content": system_prompt}]
    for m in ctx["conversation_history"][-6:]: messages.append(m)
    messages.append({"role": "user", "content": text})

    try:
        # 2. Bước 1: GPT quyết định Action
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            tools=get_tools_definition(),
            tool_choice="auto",
            temperature=0.1
        )
        
        response_msg = response.choices[0].message
        
        # 3. Bước 2: Thực thi Tool nếu có
        if response_msg.tool_calls:
            messages.append(response_msg)
            for tool_call in response_msg.tool_calls:
                func_name = tool_call.function.name
                args = json.loads(tool_call.function.arguments)
                result = execute_tool_call(uid, func_name, args)
                messages.append({"role": "tool", "tool_call_id": tool_call.id, "name": func_name, "content": result})
            
            # Gọi lại GPT để trả lời khách sau khi có dữ liệu từ Tool
            final_res = client.chat.completions.create(model="gpt-4o-mini", messages=messages)
            final_text = final_res.choices[0].message.content
        else:
            final_text = response_msg.content

        # 4. Gửi tin nhắn và lưu lịch sử
        if final_text:
            send_message(uid, final_text)
            ctx["conversation_history"].append({"role": "user", "content": text})
            ctx["conversation_history"].append({"role": "assistant", "content": final_text})
            ctx["conversation_history"] = ctx["conversation_history"][-10:]

    except Exception as e:
        print(f"GPT Error: {e}")
        send_message(uid, "Dạ em đang lấy thông tin, anh/chị đợi em xíu ạ!")

# ============================================
# WEBHOOK HANDLER
# ============================================
@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        if request.args.get("hub.verify_token") == VERIFY_TOKEN:
            return request.args.get("hub.challenge")
        return "Wrong token", 403

    data = request.get_json()
    for entry in data.get("entry", []):
        for m in entry.get("messaging", []):
            sender_id = m.get("sender", {}).get("id")
            if not sender_id: continue
            
            msg_data = m.get("message", {})
            msg_mid = msg_data.get("mid")
            
            # Chống tin nhắn lặp và echo từ bot
            if msg_data.get("is_echo"): continue
            
            ctx = USER_CONTEXT[sender_id]
            if msg_mid in ctx["processed_message_mids"]: continue
            ctx["processed_message_mids"][msg_mid] = time.time()

            if ctx["processing_lock"]: continue
            ctx["processing_lock"] = True
            
            try:
                load_products()
                # Xử lý referral từ ADS
                if "referral" in m:
                    ref = m["referral"].get("ref", "")
                    ms = detect_ms_from_text(ref)
                    if ms: update_product_context(sender_id, ms)
                
                # Xử lý nội dung tin nhắn
                text = msg_data.get("text")
                if text:
                    # Nếu thấy mã MS đơn lẻ, cập nhật context ngay
                    detected = detect_ms_from_text(text)
                    if detected: update_product_context(sender_id, detected)
                    handle_text(sender_id, text)
                    
            finally:
                ctx["processing_lock"] = False
                # Dọn dẹp cache MID cũ
                if len(ctx["processed_message_mids"]) > 50:
                    ctx["processed_message_mids"] = {k: v for k, v in list(ctx["processed_message_mids"].items())[-30:]}

    return "OK", 200

# ============================================
# ORDER API & STATIC ROUTES
# ============================================
@app.route("/")
def home(): return "Bot is running", 200

@app.route("/order-form")
def order_form():
    ms, uid = request.args.get("ms", "").upper(), request.args.get("uid", "")
    load_products()
    if ms not in PRODUCTS: return "Sản phẩm không tồn tại", 404
    p = PRODUCTS[ms]
    return render_template("order-form.html", ms=ms, uid=uid, product=p, fanpage_name=FANPAGE_NAME, domain=DOMAIN)

@app.route("/api/submit-order", methods=["POST"])
def submit_order():
    data = request.get_json()
    # Logic ghi Google Sheet API giữ nguyên như bản cũ của bạn
    # ... (Phần này bạn có thể copy nguyên từ file cũ sang)
    send_message(data.get("uid"), "🎉 Shop đã nhận được đơn hàng của anh/chị. Em sẽ gọi xác nhận ngay ạ!")
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
