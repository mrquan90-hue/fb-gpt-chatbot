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
FANPAGE_NAME = os.getenv("FANPAGE_NAME", "Shop")
BOT_APP_IDS = {"645956568292435"} # App ID của bot để lọc echo

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
# XỬ LÝ DỮ LIỆU & NHẬN DIỆN MÃ (MS) - TỐI ƯU FCHAT
# ============================================
def load_products():
    global PRODUCTS, LAST_LOAD, PRODUCTS_BY_NUMBER
    if PRODUCTS and (time.time() - LAST_LOAD) < 300: return
    try:
        r = requests.get(GOOGLE_SHEET_CSV_URL, timeout=15)
        r.encoding = "utf-8"
        reader = csv.DictReader(r.text.splitlines())
        new_p, new_n = {}, {}
        for row in reader:
            ms = (row.get("Mã sản phẩm") or "").strip().upper()
            if not ms: continue
            
            variant = {
                "mau": row.get("màu (Thuộc tính)", ""),
                "size": row.get("size (Thuộc tính)", ""),
                "gia": row.get("Giá bán", ""),
                "images": [u.strip() for u in re.split(r'[,\n;|]+', row.get("Images", "")) if u.strip()]
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
        print(f"Successfully loaded {len(PRODUCTS)} products")
    except Exception as e: print(f"Error loading sheet: {e}")

def detect_ms_from_text(text: str) -> Optional[str]:
    if not text: return None
    text_up = text.upper()
    # 1. Tìm định dạng có tiền tố: #MS000039, MS39, [MS39]
    m = re.search(r"(?:#MS|MS|MÃ|MÃ SỐ|\[MS)\s*(\d{1,6})", text_up)
    if m:
        num_str = m.group(1).lstrip("0")
        if not num_str: num_str = "0" # Trường hợp MS000000
        if num_str in PRODUCTS_BY_NUMBER: return PRODUCTS_BY_NUMBER[num_str]
    
    # 2. Tìm số đơn thuần (2-6 chữ số)
    nums = re.findall(r"\b(\d{2,6})\b", text)
    for n in nums:
        clean_n = n.lstrip("0")
        if clean_n in PRODUCTS_BY_NUMBER: return PRODUCTS_BY_NUMBER[clean_n]
    return None

def update_product_context(uid, ms):
    if not ms or not uid: return
    ctx = USER_CONTEXT[uid]
    ctx["last_ms"] = ms
    if ms not in ctx["product_history"]:
        ctx["product_history"].insert(0, ms)
    ctx["product_history"] = ctx["product_history"][:5]
    print(f"--- CONTEXT UPDATED for User {uid}: {ms} ---")

def is_bot_echo(text, app_id, attachments):
    # Nếu tin nhắn có app_id của chính bot này -> bỏ qua
    if str(app_id) in BOT_APP_IDS: return True
    if not text: return False
    # Lọc các câu bot hay nói để tránh lặp ngữ cảnh vô lý
    bot_patterns = ["📋 Đặt hàng ngay", "🎯 Em tìm được", "Chào anh/chị! 👋", "Dạ em gửi ảnh"]
    return any(p in text for p in bot_patterns)

# ============================================
# FUNCTION CALLING TOOLS
# ============================================
def get_tools_definition():
    return [
        {"type": "function", "function": {"name": "get_product_info", "description": "Lấy thông tin giá, mô tả sản phẩm.", "parameters": {"type": "object", "properties": {"ms": {"type": "string"}}, "required": ["ms"]}}},
        {"type": "function", "function": {"name": "send_product_images", "description": "Gửi ảnh thật của sản phẩm cho khách.", "parameters": {"type": "object", "properties": {"ms": {"type": "string"}}, "required": ["ms"]}}},
        {"type": "function", "function": {"name": "provide_order_link", "description": "Cung cấp link đặt hàng.", "parameters": {"type": "object", "properties": {"ms": {"type": "string"}}, "required": ["ms"]}}}
    ]

def execute_tool(uid, name, args):
    load_products()
    ctx = USER_CONTEXT[uid]
    ms = (args.get("ms") or "").upper().strip() or ctx.get("last_ms")
    
    if name == "get_product_info" and ms in PRODUCTS:
        p = PRODUCTS[ms]
        update_product_context(uid, ms)
        return f"Sản phẩm [{ms}]: {p['Ten']}. Giá: {p['Gia']}. Màu: {p['Mau']}. Size: {p['Size']}. Mô tả: {p['MoTa']}"

    if name == "send_product_images" and ms in PRODUCTS:
        img_str = PRODUCTS[ms]["Images"]
        urls = [u.strip() for u in re.split(r'[,\n;|]+', img_str) if u.strip()]
        if urls:
            send_fb_msg(uid, {"text": f"Dạ em gửi ảnh mẫu [{ms}] ạ:"})
            for u in urls[:3]: send_fb_msg(uid, {"attachment": {"type": "image", "payload": {"url": u, "is_reusable": True}}})
        return "Đã gửi ảnh."

    if name == "provide_order_link" and ms in PRODUCTS:
        domain = f"https://{DOMAIN}" if not DOMAIN.startswith("http") else DOMAIN
        link = f"{domain}/order-form?ms={ms}&uid={uid}"
        return f"Link đặt hàng cho mã {ms}: {link}"
    
    return "Không tìm thấy thông tin sản phẩm này."

# ============================================
# XỬ LÝ CHÍNH
# ============================================
def handle_text(uid, text):
    load_products()
    ctx = USER_CONTEXT[uid]
    
    # 1. Cập nhật MS nếu khách tự gõ mã trong tin nhắn
    found_ms = detect_ms_from_text(text)
    if found_ms: update_product_context(uid, found_ms)

    # 2. Xây dựng Prompt
    system_prompt = f"""Bạn là trợ lý bán hàng của {FANPAGE_NAME}.
    Ngôn ngữ: Tiếng Việt, thân thiện, ngắn gọn.
    Ngữ cảnh: Khách đang quan tâm đến mã [{ctx.get('last_ms', 'Chưa có')}].
    Nếu khách hỏi giá hoặc chi tiết về mã này, hãy dùng tool để lấy dữ liệu chính xác.
    Nếu chưa có mã, hãy hỏi khéo léo mã khách cần."""

    messages = [{"role": "system", "content": system_prompt}]
    for h in ctx["conversation_history"][-5:]: messages.append(h)
    messages.append({"role": "user", "content": text})

    try:
        response = client.chat.completions.create(model="gpt-4o-mini", messages=messages, tools=get_tools_definition())
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
        
        send_fb_msg(uid, {"text": reply})
        ctx["conversation_history"].append({"role": "user", "content": text})
        ctx["conversation_history"].append({"role": "assistant", "content": reply})
    except Exception as e: print(f"OpenAI Error: {e}")

def send_fb_msg(uid, payload):
    url = f"https://graph.facebook.com/v12.0/me/messages?access_token={PAGE_ACCESS_TOKEN}"
    requests.post(url, json={"recipient": {"id": uid}, "message": payload})

# ============================================
# WEBHOOK (QUAN TRỌNG: FIX ECHO ID)
# ============================================
@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        return request.args.get("hub.challenge") if request.args.get("hub.verify_token") == VERIFY_TOKEN else ("403", 403)
    
    data = request.get_json()
    for entry in data.get("entry", []):
        for m in entry.get("messaging", []):
            
            # TRƯỜNG HỢP ECHO (Fchat gửi tin nhắn)
            if m.get("message", {}).get("is_echo"):
                recipient_id = m.get("recipient", {}).get("id") # ID Khách hàng
                msg_obj = m.get("message", {})
                text_echo = msg_obj.get("text", "")
                app_id = msg_obj.get("app_id", "")
                
                # Kiểm tra nếu không phải bot tự nói thì mới lấy MS
                if not is_bot_echo(text_echo, app_id, None):
                    load_products()
                    ms_from_echo = detect_ms_from_text(text_echo)
                    if ms_from_echo:
                        print(f"Detected MS {ms_from_echo} from Fchat Echo to User {recipient_id}")
                        update_product_context(recipient_id, ms_from_echo)
                continue

            # TRƯỜNG HỢP KHÁCH NHẮN TIN
            sender_id = m.get("sender", {}).get("id")
            if not sender_id: continue
            
            msg = m.get("message", {})
            if "text" in msg:
                handle_text(sender_id, msg["text"])
            elif "postback" in m:
                handle_text(sender_id, m["postback"]["payload"])
                
    return "OK", 200

# ============================================
# VARIANT API & FORM (GIỮ NGUYÊN TÍNH NĂNG CŨ)
# ============================================
@app.route("/api/get-variant-price")
def api_v_price():
    ms, color, size = request.args.get("ms"), request.args.get("color"), request.args.get("size")
    load_products()
    p = PRODUCTS.get(ms)
    if not p: return jsonify({"price": "Liên hệ"})
    for v in p["variants"]:
        if (not color or v["mau"] == color) and (not size or v["size"] == size):
            return jsonify({"price": v["gia"]})
    return jsonify({"price": p["Gia"]})

@app.route("/api/get-variant-image")
def api_v_image():
    ms, color = request.args.get("ms"), request.args.get("color")
    load_products()
    p = PRODUCTS.get(ms)
    if not p or not p["variants"]: return jsonify({"image": ""})
    for v in p["variants"]:
        if v["mau"] == color and v["images"]:
            return jsonify({"image": v["images"][0]})
    return jsonify({"image": ""})

@app.route("/order-form")
def order_form():
    ms, uid = request.args.get("ms", "").upper(), request.args.get("uid", "")
    load_products()
    p = PRODUCTS.get(ms)
    if not p: return "Sản phẩm không tồn tại.", 404
    colors = sorted(list(set([v["mau"] for v in p["variants"] if v["mau"]]))) or ["Mặc định"]
    sizes = sorted(list(set([v["size"] for v in p["variants"] if v["size"]]))) or ["Free Size"]
    img = [u.strip() for u in re.split(r'[,\n;|]+', p["Images"]) if u.strip()][0]
    
    return render_template_string("""
    <!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Đặt hàng</title><style>
        body { font-family: sans-serif; padding: 15px; background: #eee; }
        .card { background: #fff; padding: 20px; border-radius: 10px; max-width: 450px; margin: auto; }
        img { width: 100%; border-radius: 8px; margin-bottom: 15px; }
        select, input { width: 100%; padding: 12px; margin: 8px 0; border: 1px solid #ccc; border-radius: 5px; box-sizing: border-box; }
        button { width: 100%; padding: 15px; background: #28a745; color: #fff; border: none; border-radius: 5px; font-weight: bold; cursor: pointer; }
    </style></head><body><div class="card">
        <img id="v-img" src="{{img}}">
        <h3>{{p.Ten}}</h3>
        <p id="v-price" style="color:red; font-size:1.2em; font-weight:bold;">{{p.Gia}}</p>
        <form id="of">
            <label>Màu sắc:</label>
            <select id="c" onchange="up()">{% for c in colors %}<option value="{{c}}">{{c}}</option>{% endfor %}</select>
            <label>Size:</label>
            <select id="s" onchange="up()">{% for s in sizes %}<option value="{{s}}">{{s}}</option>{% endfor %}</select>
            <input type="text" id="n" placeholder="Họ tên" required>
            <input type="tel" id="p" placeholder="Số điện thoại" required>
            <input type="text" id="a" placeholder="Địa chỉ nhận hàng" required>
            <button type="button" onclick="sub()">XÁC NHẬN ĐẶT HÀNG</button>
        </form>
    </div><script>
        async function up() {
            const c = document.getElementById('c').value, s = document.getElementById('s').value;
            const rp = await fetch(`/api/get-variant-price?ms={{ms}}&color=${c}&size=${s}`);
            const dp = await rp.json(); document.getElementById('v-price').innerText = dp.price;
            const ri = await fetch(`/api/get-variant-image?ms={{ms}}&color=${c}`);
            const di = await ri.json(); if(di.image) document.getElementById('v-img').src = di.image;
        }
        async function sub() {
            const data = { uid:"{{uid}}", ms:"{{ms}}", color:document.getElementById('c').value, size:document.getElementById('s').value, name:document.getElementById('n').value, phone:document.getElementById('p').value, addr:document.getElementById('a').value };
            if(!data.name || !data.phone || !data.addr) return alert("Vui lòng nhập đủ thông tin");
            await fetch('/api/submit-order', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(data) });
            alert("Đặt hàng thành công! Shop sẽ gọi xác nhận ạ.");
        }
    </script></body></html>
    """, p=p, ms=ms, uid=uid, colors=colors, sizes=sizes, img=img)

@app.route("/api/submit-order", methods=["POST"])
def api_submit():
    d = request.get_json()
    info = f"🛒 ĐƠN HÀNG MỚI: {d['ms']} ({d['color']}/{d['size']})\nKhách: {d['name']} - {d['phone']}\nĐC: {d['addr']}"
    send_fb_msg(d['uid'], {"text": info})
    return jsonify({"status": "ok"})

@app.route("/")
def home(): return "Bot Active", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
