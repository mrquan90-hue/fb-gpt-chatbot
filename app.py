import os
import json
import re
import time
import csv
import base64
from collections import defaultdict
from datetime import datetime
from typing import Optional, List, Dict

import requests
from flask import Flask, request, render_template, jsonify
from openai import OpenAI

# ============================================
# GOOGLE SHEETS API INTEGRATION
# ============================================
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    GOOGLE_API_AVAILABLE = True
except ImportError:
    GOOGLE_API_AVAILABLE = False

app = Flask(__name__)

# ============================================
# CẤU HÌNH BIẾN MÔI TRƯỜNG
# ============================================
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN", "").strip()
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
GOOGLE_SHEET_CSV_URL = os.getenv("SHEET_CSV_URL", "").strip()
DOMAIN = os.getenv("DOMAIN", "").strip() or "fb-gpt-chatbot.koyeb.app"
FANPAGE_NAME = os.getenv("FANPAGE_NAME", "Shop của bạn")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
GOOGLE_SHEETS_CREDENTIALS_JSON = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON", "").strip()
BOT_APP_IDS = {"645956568292435"} # ID Bot của bạn

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ============================================
# QUẢN LÝ TRẠNG THÁI NGƯỜI DÙNG
# ============================================
USER_CONTEXT = defaultdict(lambda: {
    "last_ms": None,
    "processing_lock": False,
    "processed_message_mids": {},
    "conversation_history": [],
    "last_all_images_time": 0,
    "referral_source": None
})

PRODUCTS = {}
PRODUCTS_BY_NUMBER = {}
LAST_LOAD = 0
LOAD_TTL = 300

# ============================================
# CÔNG CỤ TRA CỨU DỮ LIỆU (HELPERS)
# ============================================
def normalize_vietnamese(text):
    if not text: return ""
    v_map = {'à': 'a', 'á': 'a', 'ả': 'a', 'ã': 'a', 'ạ': 'a', 'ă': 'a', 'ằ': 'a', 'ắ': 'a', 'ẳ': 'a', 'ẵ': 'a', 'ặ': 'a', 'â': 'a', 'ầ': 'a', 'ấ': 'a', 'ẩ': 'a', 'ẫ': 'a', 'ậ': 'a', 'đ': 'd', 'è': 'e', 'é': 'e', 'ẻ': 'e', 'ẽ': 'e', 'ẹ': 'e', 'ê': 'e', 'ề': 'e', 'ế': 'e', 'ể': 'e', 'ễ': 'e', 'ệ': 'e', 'ì': 'i', 'í': 'i', 'ỉ': 'i', 'ĩ': 'i', 'ị': 'i', 'ò': 'o', 'ó': 'o', 'ỏ': 'o', 'õ': 'o', 'ọ': 'o', 'ô': 'o', 'ồ': 'o', 'ố': 'o', 'ổ': 'o', 'ỗ': 'o', 'ộ': 'o', 'ơ': 'o', 'ờ': 'o', 'ớ': 'o', 'ở': 'o', 'ỡ': 'o', 'ợ': 'o', 'ù': 'u', 'ú': 'u', 'ủ': 'u', 'ũ': 'u', 'ụ': 'u', 'ư': 'u', 'ừ': 'u', 'ứ': 'u', 'ử': 'u', 'ữ': 'u', 'ự': 'u', 'ỳ': 'y', 'ý': 'y', 'ỷ': 'y', 'ỹ': 'y', 'ỵ': 'y'}
    res = text.lower()
    for k, v in v_map.items(): res = res.replace(k, v)
    return res

def load_products():
    global PRODUCTS, LAST_LOAD, PRODUCTS_BY_NUMBER
    if PRODUCTS and (time.time() - LAST_LOAD) < LOAD_TTL: return
    try:
        r = requests.get(GOOGLE_SHEET_CSV_URL, timeout=15)
        r.encoding = "utf-8"
        reader = csv.DictReader(r.text.splitlines())
        new_p, new_n = {}, {}
        for row in reader:
            ms = (row.get("Mã sản phẩm") or "").strip()
            if not ms: continue
            if ms not in new_p:
                new_p[ms] = {
                    "MS": ms, "Ten": row.get("Tên sản phẩm", ""), "Gia": row.get("Giá bán", ""),
                    "MoTa": row.get("Mô tả", ""), "Images": row.get("Images", ""),
                    "Mau": row.get("màu (Thuộc tính)", ""), "Size": row.get("size (Thuộc tính)", "")
                }
            num = ms.replace("MS", "").lstrip("0")
            if num: new_n[num] = ms
        PRODUCTS, PRODUCTS_BY_NUMBER, LAST_LOAD = new_p, new_n, time.time()
    except Exception as e: print(f"Error loading sheet: {e}")

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

def extract_ms_from_ad_title(title: str) -> Optional[str]:
    if not title: return None
    m = re.search(r"(?:mã|ms)\s*(\d{1,6})", title.lower())
    if m: return "MS" + m.group(1).zfill(6)
    return None

def is_bot_echo(text, app_id, attachments):
    if app_id in BOT_APP_IDS: return True
    if not text: return False
    patterns = ["📌 [MS", "💰 GIÁ", "📋 Đặt hàng", "Chào anh/chị! 👋"]
    return any(p in text for p in patterns)

# ============================================
# TỐI ƯU NHẬN DIỆN ẢNH (2 LỚP: AI + KEYWORD SEARCH)
# ============================================
def get_image_base64(url):
    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            return base64.b64encode(response.content).decode('utf-8')
    except Exception as e:
        print(f"Error downloading image: {e}")
    return None

def search_products_by_keywords(ai_description):
    """Lớp tìm kiếm thứ 2: Quét toàn bộ kho hàng dựa trên mô tả của AI"""
    desc_norm = normalize_vietnamese(ai_description)
    best_ms = None
    max_score = 0
    
    # Lấy các từ khóa có nghĩa (độ dài > 2)
    keywords = [w for w in desc_norm.split() if len(w) > 2]
    if not keywords: return None

    for ms, p in PRODUCTS.items():
        # Tạo vùng dữ liệu để quét từ tên, mô tả, màu sắc
        product_blob = normalize_vietnamese(f"{p['Ten']} {p['MoTa']} {p['Mau']}")
        score = sum(1 for word in keywords if word in product_blob)
        
        if score > max_score:
            max_score = score
            best_ms = ms
            
    # Chỉ trả về mã nếu khớp từ 3 từ khóa trở lên để tránh nhầm lẫn
    return best_ms if max_score >= 3 else None

def handle_image(uid, image_url):
    send_fb_msg(uid, {"text": "🖼️ Em đang xem ảnh mẫu anh/chị gửi, đợi em xíu nhé..."})
    
    base64_img = get_image_base64(image_url)
    if not base64_img:
        send_fb_msg(uid, {"text": "Dạ em gặp chút lỗi khi tải ảnh, anh/chị gửi em xin mã sản phẩm nhé!"})
        return

    load_products()
    # Gửi 60 mã sản phẩm tiêu biểu để AI có dữ liệu so sánh ban đầu
    catalog_summary = "Một số mã tiêu biểu:\n"
    for ms, p in list(PRODUCTS.items())[:60]:
        catalog_summary += f"- {ms}: {p['Ten']}\n"

    try:
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{
                "role": "system",
                "content": f"Bạn là chuyên viên tư vấn của {FANPAGE_NAME}. Nhiệm vụ của bạn là nhận diện sản phẩm thời trang qua ảnh."
            }, {
                "role": "user",
                "content": [
                    {
                        "type": "text", 
                        "text": f"Hãy nhìn ảnh và thực hiện:\n1. Kiểm tra xem có khớp với mã nào dưới đây không:\n{catalog_summary}\n2. Nếu thấy khớp > 80%, chỉ trả về DUY NHẤT mã MSxxxxxx.\n3. Nếu không chắc chắn, hãy mô tả cực chi tiết loại áo/váy, màu sắc, họa tiết để tôi tìm trong kho."
                    },
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{base64_img}"}
                    }
                ]
            }],
            max_tokens=200
        )
        
        ai_res = resp.choices[0].message.content.strip()
        
        # Lớp 1: Kiểm tra xem AI có tìm thấy mã trực tiếp không
        detected_ms = detect_ms_from_text(ai_res)
        
        # Lớp 2: Nếu AI không có mã, dùng mô tả của AI để tự quét kho hàng
        if not detected_ms:
            detected_ms = search_products_by_keywords(ai_res)
        
        if detected_ms and detected_ms in PRODUCTS:
            USER_CONTEXT[uid]["last_ms"] = detected_ms
            p = PRODUCTS[detected_ms]
            send_fb_msg(uid, {"text": f"🎯 Em thấy mẫu này giống mã [{detected_ms}] bên em:\n📌 {p['Ten']}\n💰 Giá: {p['Gia']}"})
            # Gửi thông tin tư vấn chi tiết tiếp theo
            handle_text(uid, f"Tư vấn chi tiết mã {detected_ms}")
        else:
            # Lấy 1 câu mô tả ngắn của AI để trả lời khách nếu không tìm thấy
            short_desc = ai_res.split('.')[0]
            send_fb_msg(uid, {"text": f"Dạ mẫu {short_desc} này em chưa thấy mã sẵn trong kho. Anh/chị cho em xin mã MS để em check nhanh nhé!"})

    except Exception as e:
        print(f"Vision Error: {e}")
        send_fb_msg(uid, {"text": "Dạ em hơi khó nhìn mẫu này, anh/chị nhắn giúp em mã sản phẩm nhé!"})

# ============================================
# AI CORE: CHAT & ACTIONS
# ============================================
def get_tools_definition():
    return [
        {"type": "function", "function": {"name": "get_product_info", "description": "Lấy thông tin giá, màu, size.", "parameters": {"type": "object", "properties": {"ms": {"type": "string"}}, "required": ["ms"]}}},
        {"type": "function", "function": {"name": "send_product_images", "description": "Gửi ảnh thật sản phẩm.", "parameters": {"type": "object", "properties": {"ms": {"type": "string"}}, "required": ["ms"]}}},
        {"type": "function", "function": {"name": "provide_order_link", "description": "Gửi link đặt hàng.", "parameters": {"type": "object", "properties": {"ms": {"type": "string"}}, "required": ["ms"]}}},
        {"type": "function", "function": {"name": "show_featured_carousel", "description": "Hiện sản phẩm nổi bật.", "parameters": {"type": "object", "properties": {}}}}
    ]

def execute_tool(uid, name, args):
    ctx = USER_CONTEXT[uid]
    ms = args.get("ms", "").upper() or ctx.get("last_ms")
    domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"

    if name == "get_product_info":
        if ms in PRODUCTS:
            p = PRODUCTS[ms]
            ctx["last_ms"] = ms
            return f"Mã: {ms}\nTên: {p['Ten']}\nGiá: {p['Gia']}\nMàu: {p['Mau']}\nSize: {p['Size']}\nMô tả: {p['MoTa']}"
        return "Sản phẩm không tồn tại."

    elif name == "send_product_images":
        if ms in PRODUCTS:
            urls = [u.strip() for u in re.split(r'[,\n;|]+', PRODUCTS[ms]["Images"]) if u.strip()]
            if urls:
                send_fb_msg(uid, {"text": f"Dạ ảnh mẫu [{ms}] đây ạ:"})
                for u in urls[:5]: send_fb_msg(uid, {"attachment": {"type": "image", "payload": {"url": u}}})
                return "Đã gửi ảnh."
        return "Sản phẩm chưa có ảnh mẫu."

    elif name == "provide_order_link":
        if ms in PRODUCTS:
            link = f"{domain}/order-form?ms={ms}&uid={uid}"
            send_fb_msg(uid, {"text": f"Dạ mời anh/chị đặt hàng sản phẩm [{ms}] tại đây:\n{link}"})
            return "Đã gửi link."
        return "Chưa rõ mã sản phẩm khách đặt."

    elif name == "show_featured_carousel":
        elements = []
        for code, p in list(PRODUCTS.items())[:5]:
            urls = [u.strip() for u in re.split(r'[,\n;|]+', p["Images"]) if u.strip()]
            elements.append({
                "title": f"[{code}] {p['Ten']}",
                "image_url": urls[0] if urls else "",
                "subtitle": f"Giá: {p['Gia']}",
                "buttons": [{"type": "web_url", "url": f"{domain}/order-form?ms={code}&uid={uid}", "title": "🛒 Đặt ngay"}]
            })
        send_fb_msg(uid, {"attachment": {"type": "template", "payload": {"template_type": "generic", "elements": elements}}})
        return "Đã hiện carousel."
    return "Hành động lỗi."

def handle_text(uid, text):
    load_products()
    ctx = USER_CONTEXT[uid]
    quick_ms = detect_ms_from_text(text)
    if quick_ms: ctx["last_ms"] = quick_ms

    system_prompt = f"""Bạn là nhân viên bán hàng của {FANPAGE_NAME}.
    Trả lời cực ngắn (dưới 3 dòng), xưng em gọi anh/chị.
    Mã đang xem: {ctx.get('last_ms', 'Chưa rõ')}.
    Nếu khách muốn mua hoặc chốt đơn, dùng provide_order_link."""

    messages = [{"role": "system", "content": system_prompt}]
    for h in ctx["conversation_history"][-5:]: messages.append(h)
    messages.append({"role": "user", "content": text})

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            tools=get_tools_definition(),
            tool_choice="auto"
        )
        msg = response.choices[0].message
        if msg.tool_calls:
            messages.append(msg)
            for tool in msg.tool_calls:
                res = execute_tool(uid, tool.function.name, json.loads(tool.function.arguments))
                messages.append({"role": "tool", "tool_call_id": tool.id, "name": tool.function.name, "content": res})
            final_res = client.chat.completions.create(model="gpt-4o-mini", messages=messages)
            reply = final_res.choices[0].message.content
        else:
            reply = msg.content

        if reply:
            send_fb_msg(uid, {"text": reply})
            ctx["conversation_history"].append({"role": "user", "content": text})
            ctx["conversation_history"].append({"role": "assistant", "content": reply})
            ctx["conversation_history"] = ctx["conversation_history"][-10:]
    except Exception as e: print(f"Chat Error: {e}")

# ============================================
# FACEBOOK SEND API
# ============================================
def send_fb_msg(uid, message_payload):
    url = f"https://graph.facebook.com/v12.0/me/messages?access_token={PAGE_ACCESS_TOKEN}"
    try: requests.post(url, json={"recipient": {"id": uid}, "message": message_payload}, timeout=10)
    except: pass

# ============================================
# WEBHOOK HANDLER
# ============================================
@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        if request.args.get("hub.verify_token") == VERIFY_TOKEN:
            return request.args.get("hub.challenge")
        return "Forbidden", 403

    data = request.get_json()
    for entry in data.get("entry", []):
        for m in entry.get("messaging", []):
            uid = m.get("sender", {}).get("id")
            if not uid: continue
            ctx = USER_CONTEXT[uid]
            
            # Xử lý Referral Ads
            if "referral" in m:
                ref_ms = extract_ms_from_ad_title(m["referral"].get("ads_context_data", {}).get("ad_title", ""))
                if ref_ms:
                    ctx["last_ms"] = ref_ms
                    execute_tool(uid, "get_product_info", {"ms": ref_ms})
                continue

            msg = m.get("message", {})
            if msg.get("is_echo"):
                agent_ms = detect_ms_from_text(msg.get("text"))
                if agent_ms: ctx["last_ms"] = agent_ms
                continue

            mid = msg.get("mid")
            if mid and mid in ctx["processed_message_mids"]: continue
            if mid: ctx["processed_message_mids"][mid] = time.time()

            if ctx["processing_lock"]: continue
            ctx["processing_lock"] = True
            try:
                if "text" in msg: handle_text(uid, msg["text"])
                elif "attachments" in msg:
                    for att in msg["attachments"]:
                        if att["type"] == "image": handle_image(uid, att["payload"]["url"])
            finally:
                ctx["processing_lock"] = False
                now = time.time()
                ctx["processed_message_mids"] = {k: v for k, v in ctx["processed_message_mids"].items() if now - v < 3600}
    return "OK", 200

# ============================================
# GOOGLE SHEETS & ROUTES
# ============================================
def write_to_sheet(order):
    if not GOOGLE_API_AVAILABLE or not GOOGLE_SHEET_ID: return False
    try:
        creds = service_account.Credentials.from_service_account_info(json.loads(GOOGLE_SHEETS_CREDENTIALS_JSON), scopes=['https://www.googleapis.com/auth/spreadsheets'])
        service = build('sheets', 'v4', credentials=creds)
        row = [datetime.now().strftime("%Y-%m-%d %H:%M:%S"), order.get("ms"), order.get("customerName"), order.get("phone"), order.get("color"), order.get("size"), order.get("address")]
        service.spreadsheets().values().append(spreadsheetId=GOOGLE_SHEET_ID, range="Orders!A:G", valueInputOption="USER_ENTERED", body={"values": [row]}).execute()
        return True
    except Exception as e: print(f"Sheet Error: {e}"); return False

@app.route("/")
def home(): return "Bot Live", 200

@app.route("/order-form")
def order_form():
    ms, uid = request.args.get("ms", "").upper(), request.args.get("uid", "")
    load_products()
    return render_template("order-form.html", ms=ms, uid=uid, product=PRODUCTS.get(ms), fanpage_name=FANPAGE_NAME)

@app.route("/api/submit-order", methods=["POST"])
def api_submit_order():
    data = request.get_json()
    write_to_sheet(data)
    send_fb_msg(data.get("uid"), {"text": "🎉 Shop đã nhận đơn hàng của anh/chị. Shop sẽ gọi xác nhận ngay nhé!"})
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    # Koyeb sử dụng cổng 8000 mặc định
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
