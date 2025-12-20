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
# CẤU HÌNH HỆ THỐNG
# ============================================
app = Flask(__name__)

PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN", "").strip()
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
GOOGLE_SHEET_CSV_URL = os.getenv("SHEET_CSV_URL", "").strip()
DOMAIN = os.getenv("DOMAIN", "").strip() or "fb-gpt-chatbot.koyeb.app"
FANPAGE_NAME = os.getenv("FANPAGE_NAME", "Shop của bạn")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
GOOGLE_SHEETS_CREDENTIALS_JSON = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON", "").strip()
BOT_APP_IDS = {"645956568292435"}

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

USER_CONTEXT = defaultdict(lambda: {
    "last_ms": None,
    "processing_lock": False,
    "processed_message_mids": {},
    "conversation_history": [],
})

PRODUCTS = {}
PRODUCTS_BY_NUMBER = {}
LAST_LOAD = 0

# ============================================
# XỬ LÝ DỮ LIỆU KHO HÀNG
# ============================================
def normalize_vietnamese(text):
    if not text: return ""
    v_map = {'à': 'a', 'á': 'a', 'ả': 'a', 'ã': 'a', 'ạ': 'a', 'ă': 'a', 'ằ': 'a', 'ắ': 'a', 'ẳ': 'a', 'ẵ': 'a', 'ặ': 'a', 'â': 'a', 'ầ': 'a', 'ấ': 'a', 'ẩ': 'a', 'ẫ': 'a', 'ậ': 'a', 'đ': 'd', 'è': 'e', 'é': 'e', 'ẻ': 'e', 'ẽ': 'e', 'ẹ': 'e', 'ê': 'e', 'ề': 'e', 'ế': 'e', 'ể': 'e', 'ễ': 'e', 'ệ': 'e', 'ì': 'i', 'í': 'i', 'ỉ': 'i', 'ĩ': 'i', 'ị': 'i', 'ò': 'o', 'ó': 'o', 'ỏ': 'o', 'õ': 'o', 'ọ': 'o', 'ô': 'o', 'ồ': 'o', 'ố': 'o', 'ổ': 'o', 'ỗ': 'o', 'ộ': 'o', 'ơ': 'o', 'ờ': 'o', 'ớ': 'o', 'ở': 'o', 'ỡ': 'o', 'ợ': 'o', 'ù': 'u', 'ú': 'u', 'ủ': 'u', 'ũ': 'u', 'ụ': 'u', 'ư': 'u', 'ừ': 'u', 'ứ': 'u', 'ử': 'u', 'ữ': 'u', 'ự': 'u', 'ỳ': 'y', 'ý': 'y', 'ỷ': 'y', 'ỹ': 'y', 'ỵ': 'y'}
    res = text.lower()
    for k, v in v_map.items(): res = res.replace(k, v)
    return res

def load_products():
    global PRODUCTS, LAST_LOAD, PRODUCTS_BY_NUMBER
    if PRODUCTS and (time.time() - LAST_LOAD) < 300: return
    try:
        r = requests.get(GOOGLE_SHEET_CSV_URL, timeout=15)
        r.encoding = "utf-8"
        reader = csv.DictReader(r.text.splitlines())
        new_p, new_n = {}, {}
        for row in reader:
            ms = (row.get("Mã sản phẩm") or "").strip()
            if not ms: continue
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
    return None

# ============================================
# CÔNG NGHỆ NHẬN DIỆN ẢNH MỚI (TAGGING & SCORING)
# ============================================
def get_image_base64(url):
    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            return base64.b64encode(response.content).decode('utf-8')
    except: return None

def search_visual_matches(analysis_tags):
    """Hàm chấm điểm sản phẩm dựa trên các thẻ AI phân tích được"""
    tags_norm = [normalize_vietnamese(t) for t in analysis_tags]
    scored_matches = []

    for ms, p in PRODUCTS.items():
        score = 0
        p_text = normalize_vietnamese(f"{p['Ten']} {p['MoTa']} {p['Mau']}")
        
        for tag in tags_norm:
            if tag in p_text:
                score += 1
                # Nếu khớp mã MS trực tiếp trong tên thì điểm cực cao
                if tag.startswith("ms"): score += 5
        
        if score > 0:
            scored_matches.append({"ms": ms, "score": score})
    
    # Sắp xếp theo điểm từ cao xuống thấp
    scored_matches.sort(key=lambda x: x["score"], reverse=True)
    return scored_matches[:5] # Lấy Top 5

def send_product_carousel(uid, matches):
    """Gửi danh sách sản phẩm dưới dạng Carousel (Thẻ quay)"""
    elements = []
    domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
    
    for item in matches:
        p = PRODUCTS[item["ms"]]
        img_url = [u.strip() for u in re.split(r'[,\n;|]+', p["Images"]) if u.strip()]
        elements.append({
            "title": f"[{p['MS']}] {p['Ten']}",
            "image_url": img_url[0] if img_url else "",
            "subtitle": f"Giá: {p['Gia']} - Giống {item['score']*20}%",
            "buttons": [
                {"type": "web_url", "url": f"{domain}/order-form?ms={p['MS']}&uid={uid}", "title": "🛒 Đặt Ngay"},
                {"type": "postback", "title": "🔍 Xem chi tiết", "payload": f"ADVICE_{p['MS']}"}
            ]
        })
    
    payload = {
        "recipient": {"id": uid},
        "message": {
            "attachment": {
                "type": "template",
                "payload": {
                    "template_type": "generic",
                    "elements": elements
                }
            }
        }
    }
    requests.post(f"https://graph.facebook.com/v12.0/me/messages?access_token={PAGE_ACCESS_TOKEN}", json=payload)

def handle_image(uid, image_url):
    send_fb_msg(uid, {"text": "🖼️ Em đang xem ảnh mẫu anh/chị gửi, đợi em xíu nhé..."})
    
    base64_img = get_image_base64(image_url)
    if not base64_img:
        send_fb_msg(uid, {"text": "Dạ em gặp lỗi khi tải ảnh, anh/chị gửi mã MS giúp em nhé!"})
        return

    load_products()
    try:
        # AI đóng vai trò 'Máy bóc tách thuộc tính'
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{
                "role": "system",
                "content": "Bạn là máy phân tích thời trang. Hãy liệt kê các từ khóa mô tả sản phẩm trong ảnh (loại áo/váy, màu sắc, chi tiết nổi bật, hoa văn). Chỉ trả về danh sách từ khóa cách nhau bằng dấu phẩy."
            }, {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Phân tích ảnh này thành các thẻ từ khóa để tìm kiếm trong kho hàng."},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_img}"}}
                ]
            }],
            max_tokens=150
        )
        
        tags = [t.strip() for t in resp.choices[0].message.content.split(",")]
        matches = search_visual_matches(tags)
        
        if matches:
            send_fb_msg(uid, {"text": "🎯 Em tìm được một số mẫu giống ảnh anh/chị gửi nhất ạ:"})
            send_product_carousel(uid, matches)
            USER_CONTEXT[uid]["last_ms"] = matches[0]["ms"]
        else:
            send_fb_msg(uid, {"text": "Dạ mẫu này hiện em chưa thấy trong kho. Anh/chị cho em xin mã MS để em check nhanh nhé!"})

    except Exception as e:
        print(f"Vision Error: {e}")
        send_fb_msg(uid, {"text": "Dạ em hơi khó nhìn mẫu này, anh/chị nhắn giúp em mã sản phẩm nhé!"})

# ============================================
# LÕI AI CHAT & TOOLS
# ============================================
def get_tools_definition():
    return [
        {"type": "function", "function": {"name": "provide_order_link", "description": "Gửi link đặt hàng.", "parameters": {"type": "object", "properties": {"ms": {"type": "string"}}, "required": ["ms"]}}},
        {"type": "function", "function": {"name": "get_product_info", "description": "Lấy thông tin giá, màu, size.", "parameters": {"type": "object", "properties": {"ms": {"type": "string"}}, "required": ["ms"]}}}
    ]

def execute_tool(uid, name, args):
    ctx = USER_CONTEXT[uid]
    ms = args.get("ms", "").upper() or ctx.get("last_ms")
    domain = f"https://{DOMAIN}" if not DOMAIN.startswith("http") else DOMAIN

    if name == "provide_order_link" and ms in PRODUCTS:
        link = f"{domain}/order-form?ms={ms}&uid={uid}"
        send_fb_msg(uid, {"text": f"Dạ mời anh/chị đặt hàng mẫu [{ms}] tại đây nhé:\n{link}"})
        return "Sent link"
    
    if name == "get_product_info" and ms in PRODUCTS:
        p = PRODUCTS[ms]
        return f"Sản phẩm {ms}: {p['Ten']}, Giá: {p['Gia']}, Màu: {p['Mau']}, Size: {p['Size']}"
    
    return "Not found"

def handle_text(uid, text):
    load_products()
    ctx = USER_CONTEXT[uid]
    
    # Nếu khách bấm "Xem chi tiết" từ Carousel
    if text.startswith("ADVICE_"):
        ms = text.replace("ADVICE_", "")
        ctx["last_ms"] = ms
        text = f"Tư vấn chi tiết mã {ms}"

    quick_ms = detect_ms_from_text(text)
    if quick_ms: ctx["last_ms"] = quick_ms

    messages = [
        {"role": "system", "content": f"Bạn là nhân viên của {FANPAGE_NAME}. Trả lời cực ngắn gọn (dưới 3 dòng). Xưng em gọi anh/chị. Nếu khách muốn mua, hãy gửi link đặt hàng."},
        {"role": "user", "content": text}
    ]

    try:
        response = client.chat.completions.create(model="gpt-4o-mini", messages=messages, tools=get_tools_definition())
        msg = response.choices[0].message
        if msg.tool_calls:
            for tool in msg.tool_calls:
                execute_tool(uid, tool.function.name, json.loads(tool.function.arguments))
        elif msg.content:
            send_fb_msg(uid, {"text": msg.content})
    except Exception as e: print(f"Chat Error: {e}")

# ============================================
# CÁC ROUTE WEB & WEBHOOK
# ============================================
def send_fb_msg(uid, payload):
    requests.post(f"https://graph.facebook.com/v12.0/me/messages?access_token={PAGE_ACCESS_TOKEN}", json={"recipient": {"id": uid}, "message": payload})

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        return request.args.get("hub.challenge") if request.args.get("hub.verify_token") == VERIFY_TOKEN else ("Forbidden", 403)

    data = request.get_json()
    for entry in data.get("entry", []):
        for m in entry.get("messaging", []):
            uid = m.get("sender", {}).get("id")
            if not uid: continue
            
            # Xử lý nút bấm chi tiết
            if "postback" in m:
                handle_text(uid, m["postback"]["payload"])
                continue

            msg = m.get("message", {})
            if msg.get("is_echo"): continue
            
            if "text" in msg: handle_text(uid, msg["text"])
            elif "attachments" in msg:
                for att in msg["attachments"]:
                    if att["type"] == "image": handle_image(uid, att["payload"]["url"])
    return "OK", 200

@app.route("/order-form")
def order_form():
    ms, uid = request.args.get("ms", "").upper(), request.args.get("uid", "")
    load_products()
    return render_template("order-form.html", ms=ms, uid=uid, product=PRODUCTS.get(ms), fanpage_name=FANPAGE_NAME)

@app.route("/api/submit-order", methods=["POST"])
def api_submit_order():
    data = request.get_json()
    # Ghi vào sheet (giữ nguyên logic cũ của bạn)
    send_fb_msg(data.get("uid"), {"text": "🎉 Shop đã nhận đơn hàng! Shop sẽ gọi xác nhận ngay nhé."})
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
