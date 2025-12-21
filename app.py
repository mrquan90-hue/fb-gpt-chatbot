import os
import json
import re
import time
import csv
import hashlib
import base64
import threading
import glob
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
# REDIS FOR DISTRIBUTED LOCKING
# ============================================
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    print("⚠️ Redis không được cài đặt. Sử dụng file-based locking.")

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

# Redis configuration
REDIS_URL = os.getenv("REDIS_URL", "").strip()
REDIS_HOST = os.getenv("REDIS_HOST", "localhost").strip()
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "").strip()

if not GOOGLE_SHEET_CSV_URL:
    GOOGLE_SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv"

# ============================================
# REDIS CLIENT INITIALIZATION
# ============================================
redis_client = None
if REDIS_AVAILABLE and (REDIS_URL or REDIS_HOST):
    try:
        if REDIS_URL:
            redis_client = redis.from_url(REDIS_URL)
        else:
            redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD or None)
        redis_client.ping()
        print("✅ Redis connected")
    except:
        redis_client = None

# ============================================
# LOCKING MECHANISM
# ============================================
def cleanup_old_locks():
    while True:
        try:
            lock_dir = "message_locks"
            if os.path.exists(lock_dir):
                now = time.time()
                for lock_file in glob.glob(os.path.join(lock_dir, "*.lock")):
                    if now - os.path.getmtime(lock_file) > 300:
                        os.remove(lock_file)
        except: pass
        time.sleep(300)

if not redis_client:
    threading.Thread(target=cleanup_old_locks, daemon=True).start()

def mark_message_processed(mid: str, ttl: int = 60) -> bool:
    if not mid: return True
    key = f"processed:{mid}"
    if redis_client:
        try:
            if redis_client.exists(key): return False
            redis_client.setex(key, ttl, "1")
            return True
        except: pass
    lock_file = os.path.join("message_locks", f"{mid}.lock")
    os.makedirs("message_locks", exist_ok=True)
    if os.path.exists(lock_file) and (time.time() - os.path.getmtime(lock_file) < ttl): return False
    try:
        with open(lock_file, 'w') as f: f.write(str(time.time()))
        return True
    except: return True

def acquire_user_lock(uid: str, ttl: int = 10) -> bool:
    if not uid: return True
    key = f"user_lock:{uid}"
    if redis_client:
        try: return redis_client.set(key, "1", nx=True, ex=ttl)
        except: pass
    ctx = USER_CONTEXT[uid]
    if ctx.get("processing_lock") and (time.time() - ctx.get("lock_start_time", 0) < 30): return False
    ctx["processing_lock"] = True
    ctx["lock_start_time"] = time.time()
    return True

def release_user_lock(uid: str):
    if not uid: return
    if redis_client:
        try: redis_client.delete(f"user_lock:{uid}")
        except: pass
    USER_CONTEXT[uid]["processing_lock"] = False

# ============================================
# CONTEXT PERSISTENCE
# ============================================
CONTEXT_FILE = "user_context.json"
def save_user_context(uid: str):
    try:
        all_c = {}
        if os.path.exists(CONTEXT_FILE):
            with open(CONTEXT_FILE, 'r', encoding='utf-8') as f: all_c = json.load(f)
        all_c[uid] = dict(USER_CONTEXT[uid])
        with open(CONTEXT_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_c, f, ensure_ascii=False, indent=2, default=str)
    except: pass

def load_user_context(uid: str) -> dict:
    try:
        if os.path.exists(CONTEXT_FILE):
            with open(CONTEXT_FILE, 'r', encoding='utf-8') as f:
                return json.load(f).get(uid, {})
    except: pass
    return {}

# ============================================
# UTILS
# ============================================
VIETNAMESE_MAP = {'à': 'a', 'á': 'a', 'ả': 'a', 'ã': 'a', 'ạ': 'a', 'ă': 'a', 'ằ': 'a', 'ắ': 'a', 'ẳ': 'a', 'ẵ': 'a', 'ặ': 'a', 'â': 'a', 'ầ': 'a', 'ấ': 'a', 'ẩ': 'a', 'ẫ': 'a', 'ậ': 'a', 'đ': 'd', 'è': 'e', 'é': 'e', 'ẻ': 'e', 'ẽ': 'e', 'ẹ': 'e', 'ê': 'e', 'ề': 'e', 'ế': 'e', 'ể': 'e', 'ễ': 'e', 'ệ': 'e', 'ì': 'i', 'í': 'i', 'ỉ': 'i', 'ĩ': 'i', 'ị': 'i', 'ò': 'o', 'ó': 'o', 'ỏ': 'o', 'õ': 'o', 'ọ': 'o', 'ô': 'o', 'ồ': 'o', 'ố': 'o', 'ổ': 'o', 'ỗ': 'o', 'ộ': 'o', 'ơ': 'o', 'ờ': 'o', 'ớ': 'o', 'ở': 'o', 'ỡ': 'o', 'ợ': 'o', 'ù': 'u', 'ú': 'u', 'ủ': 'u', 'ũ': 'u', 'ụ': 'u', 'ư': 'u', 'ừ': 'u', 'ứ': 'u', 'ử': 'u', 'ữ': 'u', 'ự': 'u', 'ỳ': 'y', 'ý': 'y', 'ỷ': 'y', 'ỹ': 'y', 'ỵ': 'y'}
def normalize_vietnamese(text):
    if not text: return ""
    res = text.lower()
    for k, v in VIETNAMESE_MAP.items(): res = res.replace(k, v)
    return res

BOT_APP_IDS = {"645956568292435", "1784956665094089"}
client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ============================================
# GLOBAL STATE
# ============================================
USER_CONTEXT = defaultdict(lambda: {
    "last_msg_time": 0, "last_ms": None, "order_state": None, "order_data": {},
    "processing_lock": False, "lock_start_time": 0, "product_history": [],
    "conversation_history": [], "last_processed_text": "", "last_msg_time_processed": 0,
    "catalog_products": {}, "processed_echo_mids": set()
})

PRODUCTS = {}
PRODUCTS_BY_NUMBER = {}
LAST_LOAD = 0
LOAD_TTL = 300

def get_fanpage_name_from_api():
    if not PAGE_ACCESS_TOKEN: return FANPAGE_NAME
    try:
        r = requests.get(f"https://graph.facebook.com/v12.0/me?fields=name&access_token={PAGE_ACCESS_TOKEN}", timeout=5)
        return r.json().get('name', FANPAGE_NAME) if r.status_code == 200 else FANPAGE_NAME
    except: return FANPAGE_NAME

# ============================================
# CORE FUNCTIONS
# ============================================
def extract_ms_from_retailer_id(retailer_id: str) -> Optional[str]:
    if not retailer_id: return None
    base = retailer_id.split('_')[0].upper()
    if re.match(r'MS\d{6}', base): return base
    m = re.search(r'MS(\d+)', base)
    return "MS" + m.group(1).zfill(6) if m else None

def extract_ms_from_ad_title(ad_title: str) -> Optional[str]:
    if not ad_title: return None
    t = ad_title.lower()
    m = re.search(r'(mã|ms)\s*(\d{1,6})', t)
    if m: return "MS" + m.group(2).zfill(6)
    m2 = re.search(r'\b(\d{2,6})\b', ad_title)
    return "MS" + m2.group(1).zfill(6) if m2 else None

def detect_ms_from_text(text: str) -> Optional[str]:
    if not text: return None
    m = re.search(r"MS(\d{2,6})", text.upper())
    if m:
        full = "MS" + m.group(1).zfill(6)
        return full if full in PRODUCTS else None
    nums = re.findall(r"\b(\d{2,6})\b", text)
    for n in nums:
        c = n.lstrip("0")
        if c in PRODUCTS_BY_NUMBER: return PRODUCTS_BY_NUMBER[c]
    return None

def is_bot_generated_echo(echo_text: str, app_id: str = "", attachments: list = None) -> bool:
    if app_id in BOT_APP_IDS: return True
    if not echo_text: return False
    patterns = ["📌 [MS", "MÔ TẢ:", "GIÁ SẢN PHẨM:", "Đặt hàng ngay tại đây:", "Em là trợ lý AI"]
    return any(p in echo_text for p in patterns)

# ============================================
# AI & IMAGE
# ============================================
def analyze_image_with_gpt4o(image_url: str):
    if not client: return None
    try:
        prompt = "Bạn là chuyên gia thời trang. Phân tích ảnh và trả về JSON: product_type, main_color, search_text, confidence_score."
        res = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "system", "content": prompt}, {"role": "user", "content": [{"type": "text", "text": "Phân tích ảnh này:"}, {"type": "image_url", "image_url": {"url": image_url}}]}],
            response_format={"type": "json_object"}
        )
        return json.loads(res.choices[0].message.content)
    except: return None

def find_products_by_image_analysis_improved(uid: str, analysis: dict, limit: int = 5):
    if not analysis or not PRODUCTS: return []
    query = normalize_vietnamese(analysis.get("search_text", ""))
    scored = []
    for ms, p in PRODUCTS.items():
        score = calculate_text_similarity(query, normalize_vietnamese(p.get("Ten", "") + " " + p.get("MoTa", "")))
        if score > 0.1: scored.append((ms, score))
    return sorted(scored, key=lambda x: x[1], reverse=True)[:limit]

def calculate_text_similarity(t1, t2):
    try:
        v = TfidfVectorizer().fit_transform([t1, t2])
        return cosine_similarity(v[0:1], v[1:2])[0][0]
    except: return 0

# ============================================
# CONTEXT & INFO
# ============================================
def update_product_context(uid: str, ms: str):
    ctx = USER_CONTEXT[uid]
    ctx["last_ms"] = ms
    if ms not in ctx["product_history"]: ctx["product_history"].insert(0, ms)
    ctx["product_history"] = ctx["product_history"][:5]
    save_user_context(uid)

def get_relevant_product_for_question(uid: str, text: str):
    ms = detect_ms_from_text(text)
    if ms: return ms
    ctx = USER_CONTEXT[uid]
    if ctx.get("last_ms"): return ctx["last_ms"]
    return ctx["product_history"][0] if ctx.get("product_history") else None

def send_product_info_debounced(uid: str, ms: str):
    ctx = USER_CONTEXT[uid]
    now = time.time()
    if ctx.get("product_info_sent_ms") == ms and (now - ctx.get("last_product_info_time", 0) < 5): return
    ctx["product_info_sent_ms"] = ms
    ctx["last_product_info_time"] = now
    p = PRODUCTS.get(ms)
    if not p: return
    update_product_context(uid, ms)
    send_message(uid, f"📌 {p['Ten']}")
    urls = parse_image_urls(p.get("Images", ""))
    for u in urls[:3]: send_image(uid, u); time.sleep(0.5)
    send_message(uid, f"💰 GIÁ: {p['Gia']}\n📝 {p.get('MoTa', '')[:200]}...")
    send_message(uid, f"📋 Đặt hàng tại: {DOMAIN}/order-form?ms={ms}&uid={uid}")

def send_all_product_images(uid: str, ms: str):
    urls = parse_image_urls(PRODUCTS.get(ms, {}).get("Images", ""))
    if not urls: return
    send_message(uid, f"Dạ gửi anh/chị ảnh sản phẩm [{ms}]:")
    for u in urls[:10]: send_image(uid, u); time.sleep(0.6)

# ============================================
# MESSENGER SEND API
# ============================================
def send_message(uid, text):
    if not text or not PAGE_ACCESS_TOKEN: return
    payload = {"recipient": {"id": uid}, "message": {"text": text}}
    return requests.post(f"https://graph.facebook.com/v12.0/me/messages?access_token={PAGE_ACCESS_TOKEN}", json=payload)

def send_image(uid, url):
    payload = {"recipient": {"id": uid}, "message": {"attachment": {"type": "image", "payload": {"url": url}}}}
    return requests.post(f"https://graph.facebook.com/v12.0/me/messages?access_token={PAGE_ACCESS_TOKEN}", json=payload)

def send_carousel_template(uid, elements):
    payload = {"recipient": {"id": uid}, "message": {"attachment": {"type": "template", "payload": {"template_type": "generic", "elements": elements}}}}
    return requests.post(f"https://graph.facebook.com/v12.0/me/messages?access_token={PAGE_ACCESS_TOKEN}", json=payload)

# ============================================
# TOOLS & HANDLERS
# ============================================
def get_tools_definition():
    return [{"type": "function", "function": {"name": "get_product_info", "parameters": {"type": "object", "properties": {"ms": {"type": "string"}}}}}]

def execute_tool(uid, name, args):
    ms = args.get("ms", "").upper() or USER_CONTEXT[uid].get("last_ms")
    if name == "get_product_info" and ms in PRODUCTS:
        send_product_info_debounced(uid, ms)
        return "Sent info."
    return "Error."

def handle_product_query_directly(uid: str, text: str) -> bool:
    ms = get_relevant_product_for_question(uid, text)
    if not ms: return False
    t = text.lower()
    if any(k in t for k in ["giá", "bao nhiêu", "tiền"]): send_product_info_debounced(uid, ms); return True
    if any(k in t for k in ["ảnh", "hình"]): send_all_product_images(uid, ms); return True
    if any(k in t for k in ["mua", "đặt"]): send_message(uid, f"Link đặt: {DOMAIN}/order-form?ms={ms}&uid={uid}"); return True
    return False

def handle_text_with_function_calling(uid: str, text: str):
    ms = detect_ms_from_text(text)
    if ms: update_product_context(uid, ms)
    if handle_product_query_directly(uid, text): return
    
    msgs = [{"role": "system", "content": f"Bạn là sales của {FANPAGE_NAME}. SP quan tâm: {USER_CONTEXT[uid].get('last_ms')}"}, {"role": "user", "content": text}]
    try:
        res = client.chat.completions.create(model="gpt-4o-mini", messages=msgs, tools=get_tools_definition())
        if res.choices[0].message.tool_calls:
            for tool in res.choices[0].message.tool_calls:
                execute_tool(uid, tool.function.name, json.loads(tool.function.arguments))
        else:
            send_message(uid, res.choices[0].message.content)
    except: pass

def handle_text(uid: str, text: str):
    """Xử lý tin nhắn - KHẮC PHỤC LỖI IM LẶNG"""
    if not text: return
    now = time.time()
    ctx = USER_CONTEXT[uid]
    
    # Chỉ chặn trùng lặp nếu nội dung GIỐNG HỆT và gửi trong vòng 5 giây
    if text.strip().lower() == ctx.get("last_processed_text") and (now - ctx.get("last_msg_time_processed", 0) < 5):
        return
    
    if not acquire_user_lock(uid): return
    try:
        ctx["last_msg_time_processed"] = now
        ctx["last_processed_text"] = text.strip().lower()
        if handle_order_form_step(uid, text): return
        handle_text_with_function_calling(uid, text)
    finally: release_user_lock(uid)

def handle_image(uid: str, url: str):
    send_message(uid, "🖼️ Đang phân tích ảnh...")
    ana = analyze_image_with_gpt4o(url)
    if ana:
        matches = find_products_by_image_analysis_improved(uid, ana)
        if matches:
            send_message(uid, f"🔍 Tìm thấy {len(matches)} sản phẩm tương tự:")
            # Carousel logic here
        else: send_message(uid, "Không tìm thấy SP tương tự.")

# ============================================
# ORDER LOGIC
# ============================================
def handle_order_form_step(uid, text):
    ctx = USER_CONTEXT[uid]
    if not ctx.get("order_state"): return False
    # Logic hỏi tên, sđt...
    return True

def write_order_to_google_sheet_api(data):
    if not GOOGLE_API_AVAILABLE or not GOOGLE_SHEET_ID: return False
    try:
        service = build('sheets', 'v4', credentials=service_account.Credentials.from_service_account_info(json.loads(GOOGLE_SHEETS_CREDENTIALS_JSON), scopes=['https://www.googleapis.com/auth/spreadsheets']))
        row = [datetime.now().strftime("%Y-%m-%d %H:%M:%S"), data.get('ms'), data.get('customerName'), data.get('phone'), data.get('address')]
        service.spreadsheets().values().append(spreadsheetId=GOOGLE_SHEET_ID, range="Orders!A:E", valueInputOption="USER_ENTERED", body={"values": [row]}).execute()
        return True
    except: return False

def save_order_to_local_csv(data):
    with open("orders_backup.csv", "a", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([datetime.now(), data.get('ms'), data.get('customerName'), data.get('phone')])

# ============================================
# WEBHOOK MAIN
# ============================================
@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        if request.args.get("hub.verify_token") == VERIFY_TOKEN: return request.args.get("hub.challenge")
        return "Fail", 403
    
    data = request.get_json()
    for entry in data.get("entry", []):
        for m in entry.get("messaging", []):
            sender_id = m.get("sender", {}).get("id")
            recipient_id = m.get("recipient", {}).get("id")
            
            # Xử lý Echo (Tin nhắn từ shop gửi đi) - KHẮC PHỤC LỖI ID
            if m.get("message", {}).get("is_echo"):
                msg = m["message"]
                if is_bot_generated_echo(msg.get("text", ""), msg.get("app_id"), msg.get("attachments")): continue
                
                # Đối với Echo, ID khách hàng thực sự là RECIPIENT_ID
                actual_uid = recipient_id
                ms = detect_ms_from_text(msg.get("text", ""))
                if ms: update_product_context(actual_uid, ms)
                continue

            if "postback" in m:
                p = m["postback"].get("payload")
                if p == "GET_STARTED": send_message(sender_id, "Chào anh/chị! Gửi mã SP để em tư vấn ạ.")
                elif p.startswith("ADVICE_"): send_product_info_debounced(sender_id, p.split("_")[1])
            
            if "message" in m:
                msg = m["message"]
                if msg.get("text"): handle_text(sender_id, msg["text"])
                elif msg.get("attachments"):
                    for att in msg["attachments"]:
                        if att["type"] == "image": handle_image(sender_id, att["payload"]["url"])
    return "OK", 200

# ============================================
# DATA LOADING
# ============================================
def load_products(force=False):
    global PRODUCTS, PRODUCTS_BY_NUMBER, LAST_LOAD
    if not force and (time.time() - LAST_LOAD < LOAD_TTL): return
    try:
        r = requests.get(GOOGLE_SHEET_CSV_URL)
        reader = csv.DictReader(r.text.splitlines())
        new_p = {}
        for row in reader:
            ms = row.get("Mã sản phẩm", "").strip()
            if ms:
                row["variants"] = [] # Simplified for demo
                new_p[ms] = row
                num = ms.replace("MS", "").lstrip("0")
                if num: PRODUCTS_BY_NUMBER[num] = ms
        PRODUCTS = new_p
        LAST_LOAD = time.time()
    except: pass

def parse_image_urls(raw):
    return [u.strip() for u in re.split(r'[,\n;|]+', raw) if u.strip()]

def extract_price_int(s):
    m = re.search(r"(\d[\d.,]*)", str(s))
    return int(m.group(1).replace(".", "").replace(",", "")) if m else 0

# ============================================
# FLASK ROUTES
# ============================================
@app.route("/")
def home(): return "Bot is running", 200

@app.route("/order-form")
def order_form():
    ms = request.args.get("ms", "")
    uid = request.args.get("uid", "")
    load_products()
    p = PRODUCTS.get(ms, {})
    return render_template_string("""
    <html><head><meta name="viewport" content="width=device-width, initial-scale=1"></head>
    <body style="font-family:sans-serif; padding:20px;">
        <h3>Đặt hàng: {{name}}</h3>
        <p>Mã: {{ms}} | Giá: {{price}}</p>
        <input id="name" placeholder="Họ tên" style="width:100%; padding:10px; margin:5px 0;">
        <input id="phone" placeholder="Số điện thoại" style="width:100%; padding:10px; margin:5px 0;">
        <select id="prov" style="width:100%; padding:10px; margin:5px 0;"><option>Chọn Tỉnh</option></select>
        <button onclick="order()" style="width:100%; padding:15px; background:green; color:white; border:none; border-radius:5px;">XÁC NHẬN</button>
        <script>
            function order(){
                const data = {ms:"{{ms}}", uid:"{{uid}}", customerName:document.getElementById('name').value, phone:document.getElementById('phone').value};
                fetch('/api/submit-order', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(data)})
                .then(r => r.json()).then(d => alert('Thành công!'));
            }
        </script>
    </body></html>
    """, ms=ms, uid=uid, name=p.get("Ten"), price=p.get("Gia"))

@app.route("/api/submit-order", methods=["POST"])
def api_submit_order():
    data = request.json
    write_order_to_google_sheet_api(data)
    save_order_to_local_csv(data)
    send_message(data['uid'], f"✅ Đã nhận đơn hàng {data['ms']} của {data['customerName']}!")
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    load_products()
    app.run(host="0.0.0.0", port=5000)
