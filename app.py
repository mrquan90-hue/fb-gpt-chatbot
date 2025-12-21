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
# INITIAL CONFIG & REDIS
# ============================================
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    GOOGLE_API_AVAILABLE = True
except ImportError:
    GOOGLE_API_AVAILABLE = False

app = Flask(__name__)

# ENV
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN", "").strip()
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
GOOGLE_SHEET_CSV_URL = os.getenv("SHEET_CSV_URL", "").strip()
DOMAIN = os.getenv("DOMAIN", "").strip() or "fb-gpt-chatbot.onrender.com"
FANPAGE_NAME = os.getenv("FANPAGE_NAME", "Shop thời trang")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
GOOGLE_SHEETS_CREDENTIALS_JSON = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON", "").strip()
BOT_APP_IDS = {"645956568292435", "1784956665094089"} # Thêm App ID Fchat/Bot của bạn

if not GOOGLE_SHEET_CSV_URL:
    GOOGLE_SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv"

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ============================================
# CONTEXT & PERSISTENCE (FIXED)
# ============================================
CONTEXT_FILE = "user_context.json"
USER_CONTEXT = defaultdict(lambda: {
    "last_msg_time": 0, "last_ms": None, "product_history": [],
    "order_state": None, "order_data": {}, "conversation_history": [],
    "last_processed_text": "", "last_msg_time_processed": 0,
    "processed_echo_mids": set(), "processing_lock": False, "lock_start_time": 0
})

def save_user_context(uid: str):
    try:
        all_ctx = {}
        if os.path.exists(CONTEXT_FILE):
            with open(CONTEXT_FILE, 'r', encoding='utf-8') as f:
                all_ctx = json.load(f)
        data = dict(USER_CONTEXT[uid])
        if "processed_echo_mids" in data: data["processed_echo_mids"] = list(data["processed_echo_mids"])
        all_ctx[uid] = data
        with open(CONTEXT_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_ctx, f, ensure_ascii=False, indent=2, default=str)
    except: pass

def load_user_context(uid: str):
    try:
        if os.path.exists(CONTEXT_FILE):
            with open(CONTEXT_FILE, 'r', encoding='utf-8') as f:
                all_ctx = json.load(f)
                res = all_ctx.get(uid, {})
                if res and "processed_echo_mids" in res: res["processed_echo_mids"] = set(res["processed_echo_mids"])
                return res
    except: pass
    return {}

# ============================================
# HELPER FUNCTIONS (TRÍCH XUẤT MS)
# ============================================
def normalize_vietnamese(text):
    if not text: return ""
    v_map = {'àáảãạăằắẳẵặâầấẩẫậ':'a','èéẻẽẹêềếểễệ':'e','ìíỉĩị':'i','òóỏõọôồốổỗộơờớởỡợ':'o','ùúủũụưừứửữự':'u','ỳýỷỹỵ':'y','đ':'d'}
    res = text.lower()
    for k, v in v_map.items():
        for char in k: res = res.replace(char, v)
    return res

def extract_ms_from_ad_title(title: str) -> Optional[str]:
    if not title: return None
    match = re.search(r'(?:mã|ms)\s*(\d{1,6})', title.lower())
    if match: return "MS" + match.group(1).zfill(6)
    return None

def extract_ms_from_retailer_id(rid: str) -> Optional[str]:
    if not rid: return None
    base = rid.split('_')[0].upper()
    if base.startswith("MS"): return base
    match = re.search(r'(\d+)', base)
    if match: return "MS" + match.group(1).zfill(6)
    return None

def is_bot_generated_echo(text: str, app_id: str) -> bool:
    if app_id in BOT_APP_IDS: return True
    bot_keywords = ["📌 [MS", "💰 GIÁ", "📋 Đặt hàng", "Chào anh/chị! 👋"]
    return any(k in text for k in bot_keywords)

# ============================================
# PRODUCT DATA
# ============================================
PRODUCTS = {}
PRODUCTS_BY_NUMBER = {}
LAST_LOAD = 0

def load_products(force=False):
    global PRODUCTS, PRODUCTS_BY_NUMBER, LAST_LOAD
    if not force and (time.time() - LAST_LOAD) < 300: return
    try:
        r = requests.get(GOOGLE_SHEET_CSV_URL, timeout=15)
        r.encoding = "utf-8"
        reader = csv.DictReader(r.text.splitlines())
        new_p, new_m = {}, {}
        for row in reader:
            ms = (row.get("Mã sản phẩm") or "").strip().upper()
            if not ms: continue
            if ms not in new_p:
                new_p[ms] = {
                    "MS": ms, "Ten": row.get("Tên sản phẩm", ""), "Gia": row.get("Giá bán", ""),
                    "MoTa": row.get("Mô tả", ""), "Images": row.get("Images", ""), "variants": []
                }
                num = ms.replace("MS", "").lstrip("0")
                if num: new_m[num] = ms
            new_p[ms]["variants"].append(row)
        PRODUCTS, PRODUCTS_BY_NUMBER, LAST_LOAD = new_p, new_m, time.time()
        print(f"📦 Loaded {len(PRODUCTS)} products.")
    except Exception as e: print(f"Load Error: {e}")

# ============================================
# SEND API
# ============================================
def send_message(uid: str, text: str):
    if not text: return
    url = f"https://graph.facebook.com/v12.0/me/messages?access_token={PAGE_ACCESS_TOKEN}"
    payload = {
        "recipient": {"id": uid}, "message": {"text": text[:2000]},
        "messaging_type": "RESPONSE"
    }
    return requests.post(url, json=payload).json()

def send_image(uid: str, url: str):
    api_url = f"https://graph.facebook.com/v12.0/me/messages?access_token={PAGE_ACCESS_TOKEN}"
    return requests.post(api_url, json={
        "recipient": {"id": uid},
        "message": {"attachment": {"type": "image", "payload": {"url": url}}}
    }).json()

def send_product_info(uid: str, ms: str):
    p = PRODUCTS.get(ms)
    if not p: return
    send_message(uid, f"📌 {p['Ten']}")
    imgs = [u.strip() for u in p["Images"].split(",") if "http" in u]
    for img in imgs[:3]: 
        send_image(uid, img)
        time.sleep(0.5)
    send_message(uid, f"📝 MÔ TẢ: {p['MoTa'][:500]}")
    send_message(uid, f"💰 GIÁ: {p['Gia']}")
    order_url = f"https://{DOMAIN}/order-form?ms={ms}&uid={uid}"
    send_message(uid, f"📋 Đặt hàng tại: {order_url}")

# ============================================
# VISION & SEARCH
# ============================================
def analyze_image_with_gpt4o(url: str):
    if not client: return None
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": [
                {"type": "text", "text": "Mô tả sản phẩm thời trang này ngắn gọn bằng các từ khóa màu sắc, kiểu dáng, chất liệu."},
                {"type": "image_url", "image_url": {"url": url}}
            ]}],
            max_tokens=100
        )
        return response.choices[0].message.content
    except: return None

def find_matched_products(search_text: str):
    if not search_text: return []
    corpus = [normalize_vietnamese(p["Ten"] + " " + p["MoTa"]) for p in PRODUCTS.values()]
    keys = list(PRODUCTS.keys())
    vectorizer = TfidfVectorizer().fit_transform(corpus + [normalize_vietnamese(search_text)])
    sim = cosine_similarity(vectorizer[-1], vectorizer[:-1])[0]
    matched = sorted([(keys[i], sim[i]) for i in range(len(keys)) if sim[i] > 0.1], key=lambda x: x[1], reverse=True)
    return matched[:3]

# ============================================
# MAIN HANDLERS (FIXED DEDUPLICATION)
# ============================================
def handle_text(uid: str, text: str):
    now = time.time()
    ctx = USER_CONTEXT[uid]
    
    # --- FIX: CHỐNG TRÙNG LẶP NỚI LỎNG ---
    if text.strip().lower() == ctx["last_processed_text"] and (now - ctx["last_msg_time_processed"] < 5):
        print(f"Skipping duplicate: {text}")
        return
    
    ctx["last_msg_time_processed"] = now
    ctx["last_processed_text"] = text.strip().lower()
    
    # Detect MS
    ms_found = None
    m = re.search(r"MS(\d{2,6})", text.upper())
    if m: ms_found = "MS" + m.group(1).zfill(6)
    else:
        nums = re.findall(r"\b(\d{2,6})\b", text)
        for n in nums:
            if n.lstrip("0") in PRODUCTS_BY_NUMBER:
                ms_found = PRODUCTS_BY_NUMBER[n.lstrip("0")]
                break
    
    if ms_found:
        ctx["last_ms"] = ms_found
        send_product_info(uid, ms_found)
    else:
        # AI Reply
        prompt = f"Khách hỏi: {text}. Sản phẩm cuối họ xem: {ctx['last_ms']}. Hãy trả lời lễ phép."
        res = client.chat.completions.create(model="gpt-4o-mini", messages=[{"role":"system","content":"Bạn là shop bán hàng."},{"role":"user","content":prompt}])
        send_message(uid, res.choices[0].message.content)
    
    save_user_context(uid)

def handle_image_upload(uid: str, url: str):
    send_message(uid, "🖼️ Đang phân tích ảnh của anh/chị...")
    desc = analyze_image_with_gpt4o(url)
    if desc:
        matches = find_matched_products(desc)
        if matches:
            send_message(uid, "🔍 Em tìm thấy sản phẩm tương tự:")
            for ms, score in matches:
                p = PRODUCTS[ms]
                send_message(uid, f"Mã: {ms} - {p['Ten']}\nGiá: {p['Gia']}")
                time.sleep(0.5)
        else: send_message(uid, "Dạ em chưa tìm thấy mẫu này trong kho, anh/chị xem mẫu khác nhé.")
    else: send_message(uid, "Lỗi phân tích ảnh, anh/chị gửi lại mã SP giúp em.")

# ============================================
# WEBHOOK (FULL FEATURES)
# ============================================
@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        if request.args.get("hub.verify_token") == VERIFY_TOKEN: return request.args.get("hub.challenge"), 200
        return "Fail", 403

    data = request.get_json() or {}
    load_products()

    for entry in data.get("entry", []):
        for m in entry.get("messaging", []):
            sender_id = m.get("sender", {}).get("id")
            if not sender_id: continue

            # --- FIX: SYNC CONTEXT ---
            if sender_id not in USER_CONTEXT:
                USER_CONTEXT[sender_id].update(load_user_context(sender_id))
            
            # 1. NHẬN DIỆN TỪ FCHAT (ECHO)
            if m.get("message", {}).get("is_echo"):
                echo_text = m["message"].get("text", "")
                app_id = str(m["message"].get("app_id", ""))
                if not is_bot_generated_echo(echo_text, app_id):
                    ms = extract_ms_from_ad_title(echo_text)
                    if ms: 
                        USER_CONTEXT[sender_id]["last_ms"] = ms
                        save_user_context(sender_id)
                continue

            # 2. NHẬN DIỆN TỪ ADS (REFERRAL)
            if "referral" in m:
                ref_data = m["referral"]
                ad_title = ref_data.get("ads_context_data", {}).get("ad_title", "")
                ms = extract_ms_from_ad_title(ad_title) or detect_ms_from_text(ref_data.get("ref", ""))
                if ms:
                    USER_CONTEXT[sender_id]["last_ms"] = ms
                    send_message(sender_id, f"Chào anh/chị! 👋 Em thấy mình đang quan tâm mã {ms}. Em gửi thông tin nhé:")
                    send_product_info(sender_id, ms)
                    save_user_context(sender_id)
                continue

            # 3. NHẬN DIỆN TỪ CATALOG (ATTACHMENTS)
            if "message" in m and "attachments" in m["message"]:
                for att in m["message"]["attachments"]:
                    if att.get("type") == "template":
                        rid = att.get("payload", {}).get("product", {}).get("elements", [{}])[0].get("retailer_id")
                        ms = extract_ms_from_retailer_id(rid)
                        if ms:
                            USER_CONTEXT[sender_id]["last_ms"] = ms
                            send_message(sender_id, f"Mã sản phẩm anh/chị đang xem là: {ms}")
                            save_user_context(sender_id)
                    # 4. NHẬN DIỆN TỪ ẢNH (VISION)
                    elif att.get("type") == "image":
                        handle_image_upload(sender_id, att["payload"]["url"])

            # 5. TIN NHẮN VĂN BẢN
            if "message" in m and "text" in m["message"] and not m["message"].get("is_echo"):
                handle_text(sender_id, m["message"]["text"])

    return "OK", 200

# (Các Route API Order Form giữ nguyên như bản cũ...)
@app.route("/")
def index(): return "Bot Live", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
