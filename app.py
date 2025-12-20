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
from flask import Flask, request, render_template_string, jsonify
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
FANPAGE_NAME = os.getenv("FANPAGE_NAME", "Shop Thời Trang")
BOT_APP_IDS = {"645956568292435"}

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

USER_CONTEXT = defaultdict(lambda: {
    "last_ms": None,
    "product_history": [],
    "processing_lock": False,
    "processed_message_mids": {},
    "conversation_history": [],
})

PRODUCTS = {}
PRODUCTS_BY_NUMBER = {}
LAST_LOAD = 0

# ============================================
# XỬ LÝ DỮ LIỆU & NHẬN DIỆN MÃ (MS) - TỪ FILE (17)
# ============================================
def normalize_vietnamese(text):
    if not text: return ""
    v_map = {'à': 'a', 'á': 'a', 'ả': 'a', 'ã': 'a', 'ạ': 'a', 'ă': 'a', 'ằ': 'a', 'ắ': 'a', 'ẳ': 'a', 'ẵ': 'a', 'ặ': 'a', 'â': 'a', 'ầ': 'a', 'ấ': 'a', 'ẩ': 'a', 'ẫ': 'a', 'ậ': 'a', 'đ': 'd', 'è': 'e', 'é': 'e', 'ẻ': 'e', 'ẽ': 'e', 'ẹ': 'e', 'ê': 'e', 'ề': 'e', 'ế': 'e', 'ể': 'e', 'ễ': 'e', 'ệ': 'e', 'ì': 'i', 'í': 'i', 'ỉ': 'i', 'ĩ': 'i', 'ị': 'i', 'ò': 'o', 'ó': 'o', 'ỏ': 'o', 'õ': 'o', 'ọ': 'o', 'ô': 'o', 'ồ': 'o', 'ố': 'o', 'ổ': 'o', 'ỗ': 'o', 'ộ': 'o', 'ơ': 'o', 'ờ': 'o', 'ớ': 'o', 'ở': 'o', 'ỡ': 'o', 'ợ': 'o', 'ù': 'u', 'ú': 'u', 'ủ': 'u', 'ũ': 'u', 'ụ': 'u', 'ư': 'u', 'ừ': 'u', 'ứ': 'u', 'ử': 'u', 'ữ': 'u', 'ự': 'u', 'ỳ': 'y', 'ý': 'y', 'ỷ': 'y', 'ỹ': 'y', 'ỵ': 'y'}
    res = text.lower()
    for k, v in v_map.items(): res = res.replace(k, v)
    return res

def parse_image_urls(raw: str):
    if not raw: return []
    parts = re.split(r'[,\n;|]+', raw)
    return [p.strip() for p in parts if p.strip() and (p.startswith('http') or 'alicdn' in p)]

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
            
            # Xử lý biến thể cho giá và ảnh
            variant = {
                "mau": row.get("màu (Thuộc tính)", ""),
                "size": row.get("size (Thuộc tính)", ""),
                "gia": row.get("Giá bán", ""),
                "images": parse_image_urls(row.get("Images", ""))
            }
            
            if ms not in new_p:
                new_p[ms] = {
                    "MS": ms, "Ten": row.get("Tên sản phẩm", ""), "Gia": row.get("Giá bán", ""),
                    "MoTa": row.get("Mô tả", ""), "Images": row.get("Images", ""),
                    "Mau": row.get("màu (Thuộc tính)", ""), "Size": row.get("size (Thuộc tính)", ""),
                    "variants": []
                }
            new_p[ms]["variants"].append(variant)
            
            num = ms.replace("MS", "").lstrip("0")
            if num: new_n[num] = ms
        PRODUCTS, PRODUCTS_BY_NUMBER, LAST_LOAD = new_p, new_n, time.time()
    except Exception as e: print(f"Error loading sheet: {e}")

def detect_ms_from_text(text: str) -> Optional[str]:
    if not text: return None
    text_up = text.upper()
    # Thử tìm các định dạng [MS123456], MS123456, #MS123456
    m = re.search(r"(?:MS|#MS|\[MS)\s*(\d{1,6})", text_up)
    if m:
        num_str = m.group(1).lstrip("0")
        if num_str in PRODUCTS_BY_NUMBER: return PRODUCTS_BY_NUMBER[num_str]
    # Tìm số đơn thuần
    nums = re.findall(r"\b(\d{2,6})\b", text)
    for n in nums:
        clean_n = n.lstrip("0")
        if clean_n in PRODUCTS_BY_NUMBER: return PRODUCTS_BY_NUMBER[clean_n]
    return None

def update_product_context(uid, ms):
    ctx = USER_CONTEXT[uid]
    ctx["last_ms"] = ms
    if ms not in ctx["product_history"]:
        ctx["product_history"].insert(0, ms)
    ctx["product_history"] = ctx["product_history"][:5]

def is_bot_echo(text, app_id, attachments):
    if app_id in BOT_APP_IDS: return True
    if not text: return False
    # Các mẫu tin nhắn đặc trưng của bot từ file (17)
    patterns = ["📌 [MS", "💰 GIÁ", "📋 Đặt hàng", "🎯 Em tìm được", "Dạ em gửi ảnh", "Chào anh/chị! 👋"]
    return any(p in text for p in patterns)

def extract_ms_from_retailer_id(retailer_id: str) -> Optional[str]:
    if not retailer_id: return None
    parts = retailer_id.split('_')
    base_id = parts[0].upper()
    match = re.search(r'MS(\d+)', base_id)
    if match: return "MS" + match.group(1).zfill(6)
    return None

# ============================================
# FUNCTION CALLING TOOLS
# ============================================
def get_tools_definition():
    return [
        {"type": "function", "function": {"name": "get_product_info", "description": "Lấy giá, mô tả, màu sắc khi khách hỏi.", "parameters": {"type": "object", "properties": {"ms": {"type": "string"}}, "required": ["ms"]}}},
        {"type": "function", "function": {"name": "send_product_images", "description": "Gửi ảnh thật của sản phẩm.", "parameters": {"type": "object", "properties": {"ms": {"type": "string"}}, "required": ["ms"]}}},
        {"type": "function", "function": {"name": "provide_order_link", "description": "Gửi link đặt hàng.", "parameters": {"type": "object", "properties": {"ms": {"type": "string"}}, "required": ["ms"]}}},
        {"type": "function", "function": {"name": "show_featured_carousel", "description": "Hiển thị danh sách sản phẩm nổi bật.", "parameters": {"type": "object", "properties": {}}}}
    ]

def execute_tool(uid, name, args):
    ctx = USER_CONTEXT[uid]
    ms = (args.get("ms") or "").upper().strip() or ctx.get("last_ms")
    if ms and not ms.startswith("MS") and ms.isdigit(): ms = "MS" + ms.zfill(6)
    
    if name == "get_product_info" and ms in PRODUCTS:
        p = PRODUCTS[ms]
        update_product_context(uid, ms)
        return f"Sản phẩm [{ms}]: {p['Ten']}. Giá: {p['Gia']}. Màu: {p['Mau']}. Size: {p['Size']}. Mô tả: {p['MoTa']}"

    if name == "send_product_images" and ms in PRODUCTS:
        urls = parse_image_urls(PRODUCTS[ms]["Images"])
        if urls:
            send_fb_msg(uid, {"text": f"Dạ em gửi ảnh mẫu [{ms}] ạ:"})
            for u in urls[:3]: send_fb_msg(uid, {"attachment": {"type": "image", "payload": {"url": u}}})
        return "Đã gửi ảnh."

    if name == "provide_order_link" and ms in PRODUCTS:
        domain = f"https://{DOMAIN}" if not DOMAIN.startswith("http") else DOMAIN
        link = f"{domain}/order-form?ms={ms}&uid={uid}"
        send_fb_msg(uid, {"text": f"Dạ mời anh/chị đặt hàng sản phẩm [{ms}] tại đây:\n{link}"})
        return "Đã gửi link đặt hàng."

    if name == "show_featured_carousel":
        ms_list = list(PRODUCTS.keys())[:5]
        elements = []
        domain = f"https://{DOMAIN}" if not DOMAIN.startswith("http") else DOMAIN
        for m in ms_list:
            p = PRODUCTS[m]
            imgs = parse_image_urls(p["Images"])
            elements.append({
                "title": f"[{m}] {p['Ten']}",
                "image_url": imgs[0] if imgs else "",
                "subtitle": f"Giá: {p['Gia']}",
                "buttons": [
                    {"type": "web_url", "url": f"{domain}/order-form?ms={m}&uid={uid}", "title": "🛒 Đặt ngay"},
                    {"type": "postback", "title": "🔍 Chi tiết", "payload": f"ADVICE_{m}"}
                ]
            })
        payload = {"recipient": {"id": uid}, "message": {"attachment": {"type": "template", "payload": {"template_type": "generic", "elements": elements}}}}
        requests.post(f"https://graph.facebook.com/v12.0/me/messages?access_token={PAGE_ACCESS_TOKEN}", json=payload)
        return "Đã hiển thị danh sách sản phẩm."
    
    return "Dạ hiện tại em không tìm thấy mã sản phẩm này."

# ============================================
# XỬ LÝ TIN NHẮN & WEBHOOK
# ============================================
def handle_text(uid, text):
    load_products()
    ctx = USER_CONTEXT[uid]
    
    if text.startswith("ADVICE_"):
        ms = text.replace("ADVICE_", "")
        info = execute_tool(uid, "get_product_info", {"ms": ms})
        send_fb_msg(uid, {"text": info})
        execute_tool(uid, "provide_order_link", {"ms": ms})
        return

    # Nhận diện MS ngay trong tin nhắn
    found_ms = detect_ms_from_text(text)
    if found_ms: update_product_context(uid, found_ms)

    messages = [{"role": "system", "content": f"Bạn là nhân viên tư vấn của {FANPAGE_NAME}. Trả lời ngắn gọn, thân thiện. Mã đang quan tâm: {ctx.get('last_ms')}. Khi khách hỏi giá hoặc mua, hãy dùng tool."}]
    for h in ctx["conversation_history"][-5:]: messages.append(h)
    messages.append({"role": "user", "content": text})

    try:
        response = client.chat.completions.create(model="gpt-4o-mini", messages=messages, tools=get_tools_definition(), tool_choice="auto")
        msg = response.choices[0].message
        if msg.tool_calls:
            messages.append(msg)
            for tool in msg.tool_calls:
                res = execute_tool(uid, tool.function.name, json.loads(tool.function.arguments))
                messages.append({"role": "tool", "tool_call_id": tool.id, "name": tool.function.name, "content": res})
            final = client.chat.completions.create(model="gpt-4o-mini", messages=messages)
            reply = final.choices[0].message.content
        else:
            reply = msg.content
        
        if reply:
            send_fb_msg(uid, {"text": reply})
            ctx["conversation_history"].append({"role": "user", "content": text})
            ctx["conversation_history"].append({"role": "assistant", "content": reply})
    except Exception as e: print(f"Chat Error: {e}")

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
            
            # Xử lý Referral (Ads & Catalog)
            if "referral" in m:
                ref = m["referral"]
                ad_title = ref.get("ads_context_data", {}).get("ad_title", "")
                ms = detect_ms_from_text(ad_title) or detect_ms_from_text(ref.get("ref", ""))
                if ms: update_product_context(uid, ms)
                handle_text(uid, "Tư vấn cho mình mã này")
                continue

            # Xử lý Catalog Product Tag
            if "message" in m and "attachments" in m["message"]:
                for att in m["message"]["attachments"]:
                    if att.get("type") == "template" and "product" in att.get("payload", {}):
                        rid = att["payload"]["product"]["elements"][0].get("retailer_id")
                        ms = extract_ms_from_retailer_id(rid)
                        if ms: update_product_context(uid, ms)

            msg = m.get("message", {})
            if msg.get("is_echo"):
                if not is_bot_echo(msg.get("text"), msg.get("app_id"), None):
                    ms = detect_ms_from_text(msg.get("text"))
                    if ms: update_product_context(uid, ms)
                continue

            if "text" in msg: handle_text(uid, msg["text"])
            elif "postback" in m: handle_text(uid, m["postback"]["payload"])
            
    return "OK", 200

# ============================================
# ORDER FORM & VARIANT API
# ============================================
@app.route("/api/get-variant-price")
def api_get_price():
    ms, color, size = request.args.get("ms"), request.args.get("color"), request.args.get("size")
    load_products()
    p = PRODUCTS.get(ms)
    if not p: return jsonify({"price": 0})
    for v in p["variants"]:
        if (not color or v["mau"] == color) and (not size or v["size"] == size):
            return jsonify({"price": v["gia"]})
    return jsonify({"price": p["Gia"]})

@app.route("/api/get-variant-image")
def api_get_image():
    ms, color = request.args.get("ms"), request.args.get("color")
    load_products()
    p = PRODUCTS.get(ms)
    if not p: return jsonify({"image": ""})
    for v in p["variants"]:
        if v["mau"] == color and v["images"]:
            return jsonify({"image": v["images"][0]})
    return jsonify({"image": parse_image_urls(p["Images"])[0] if p["Images"] else ""})

@app.route("/order-form")
def order_form():
    ms, uid = request.args.get("ms", "").upper(), request.args.get("uid", "")
    load_products()
    p = PRODUCTS.get(ms)
    if not p: return "Sản phẩm không tồn tại.", 404
    
    colors = sorted(list(set([v["mau"] for v in p["variants"] if v["mau"]]))) or ["Mặc định"]
    sizes = sorted(list(set([v["size"] for v in p["variants"] if v["size"]]))) or ["Free Size"]
    imgs = parse_image_urls(p["Images"])

    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Đặt hàng</title>
        <style>
            body { font-family: sans-serif; padding: 20px; background: #f4f4f4; }
            .card { background: white; padding: 20px; border-radius: 10px; max-width: 400px; margin: auto; }
            img { width: 100%; border-radius: 10px; }
            select, input { width: 100%; padding: 10px; margin: 10px 0; border: 1px solid #ddd; border-radius: 5px; }
            button { width: 100%; padding: 15px; background: #28a745; color: white; border: none; border-radius: 5px; font-weight: bold; }
        </style>
    </head>
    <body>
        <div class="card">
            <img id="main-img" src="{{img}}">
            <h3>{{p.Ten}}</h3>
            <p id="price-txt" style="color: red; font-weight: bold; font-size: 1.2em;">{{p.Gia}}</p>
            <form id="orderForm">
                <label>Màu sắc:</label>
                <select id="color" onchange="updateVariant()">{% for c in colors %}<option value="{{c}}">{{c}}</option>{% endfor %}</select>
                <label>Size:</label>
                <select id="size" onchange="updateVariant()">{% for s in sizes %}<option value="{{s}}">{{s}}</option>{% endfor %}</select>
                <input type="text" id="name" placeholder="Họ tên" required>
                <input type="tel" id="phone" placeholder="Số điện thoại" required>
                <input type="text" id="addr" placeholder="Địa chỉ nhận hàng" required>
                <button type="button" onclick="submitOrder()">XÁC NHẬN ĐẶT HÀNG</button>
            </form>
        </div>
        <script>
            async function updateVariant() {
                const c = document.getElementById('color').value;
                const s = document.getElementById('size').value;
                const resP = await fetch(`/api/get-variant-price?ms={{ms}}&color=${c}&size=${s}`);
                const dataP = await resP.json();
                document.getElementById('price-txt').innerText = dataP.price;
                
                const resI = await fetch(`/api/get-variant-image?ms={{ms}}&color=${c}`);
                const dataI = await resI.json();
                if(dataI.image) document.getElementById('main-img').src = dataI.image;
            }
            async function submitOrder() {
                const body = {
                    uid: "{{uid}}", ms: "{{ms}}",
                    color: document.getElementById('color').value,
                    size: document.getElementById('size').value,
                    name: document.getElementById('name').value,
                    phone: document.getElementById('phone').value,
                    addr: document.getElementById('addr').value
                };
                await fetch('/api/submit-order', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)});
                alert('Đặt hàng thành công!');
            }
        </script>
    </body>
    </html>
    """, p=p, ms=ms, uid=uid, colors=colors, sizes=sizes, img=imgs[0] if imgs else "")

@app.route("/api/submit-order", methods=["POST"])
def api_submit_order():
    data = request.get_json()
    msg = f"🎉 ĐƠN HÀNG MỚI\nSản phẩm: {data['ms']}\nPhân loại: {data['color']} - {data['size']}\nKhách: {data['name']}\nSĐT: {data['phone']}\nĐC: {data['addr']}"
    send_fb_msg(data["uid"], {"text": msg})
    return jsonify({"status": "ok"})

@app.route("/")
def home(): return "Bot is running", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
