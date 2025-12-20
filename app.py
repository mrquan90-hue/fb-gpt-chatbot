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
    patterns = ["📌 [MS", "💰 GIÁ", "📋 Đặt hàng", "🎯 Em tìm được"]
    return any(p in text for p in patterns)

# ============================================
# NHẬN DIỆN ẢNH NÂNG CAO (SCORING & CAROUSEL)
# ============================================
def get_image_base64(url):
    try:
        response = requests.get(url, timeout=15)
        return base64.b64encode(response.content).decode('utf-8') if response.status_code == 200 else None
    except: return None

def search_visual_matches(analysis_tags):
    tags_norm = [normalize_vietnamese(t) for t in analysis_tags]
    scored = []
    for ms, p in PRODUCTS.items():
        score = sum(1 for tag in tags_norm if tag in normalize_vietnamese(f"{p['Ten']} {p['MoTa']} {p['Mau']}"))
        if score > 0: scored.append({"ms": ms, "score": score})
    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored[:5]

def handle_image(uid, image_url):
    send_fb_msg(uid, {"text": "🖼️ Em đang xem ảnh mẫu anh/chị gửi, đợi em xíu nhé..."})
    base64_img = get_image_base64(image_url)
    if not base64_img:
        send_fb_msg(uid, {"text": "Dạ em gặp lỗi tải ảnh, anh/chị nhắn mã MS giúp em nhé!"})
        return
    load_products()
    try:
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "system", "content": "Bạn là máy phân tích thời trang. Hãy liệt kê các từ khóa mô tả sản phẩm (màu sắc, kiểu dáng, họa tiết). Cách nhau bằng dấu phẩy."},
                      {"role": "user", "content": [{"type": "text", "text": "Phân tích ảnh này:"}, {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_img}"}}]}]
        )
        tags = [t.strip() for t in resp.choices[0].message.content.split(",")]
        matches = search_visual_matches(tags)
        if matches:
            send_fb_msg(uid, {"text": "🎯 Em tìm được một số mẫu giống ảnh anh/chị gửi nhất ạ:"})
            send_featured_carousel(uid, [m["ms"] for m in matches])
            USER_CONTEXT[uid]["last_ms"] = matches[0]["ms"]
        else:
            send_fb_msg(uid, {"text": "Dạ mẫu này hiện em chưa thấy trong kho. Anh/chị cho em xin mã MS nhé!"})
    except Exception as e:
        print(f"Vision Error: {e}")
        send_fb_msg(uid, {"text": "Dạ em hơi khó nhìn ảnh, anh/chị nhắn mã MS giúp em nhé!"})

# ============================================
# OPENAI TOOLS (FUNCTION CALLING) - BỔ SUNG ĐỦ 4 TOOLS
# ============================================
def get_tools_definition():
    return [
        {
            "type": "function",
            "function": {
                "name": "get_product_info",
                "description": "Lấy thông tin giá, mô tả, màu sắc khi khách hỏi hoặc cần tư vấn.",
                "parameters": {"type": "object", "properties": {"ms": {"type": "string"}}, "required": ["ms"]}
            }
        },
        {
            "type": "function",
            "function": {
                "name": "send_product_images",
                "description": "Gửi các ảnh thật, ảnh mẫu của sản phẩm.",
                "parameters": {"type": "object", "properties": {"ms": {"type": "string"}}, "required": ["ms"]}
            }
        },
        {
            "type": "function",
            "function": {
                "name": "provide_order_link",
                "description": "Cung cấp link form đặt hàng khi khách muốn mua hoặc chốt đơn.",
                "parameters": {"type": "object", "properties": {"ms": {"type": "string"}}, "required": ["ms"]}
            }
        },
        {
            "type": "function",
            "function": {
                "name": "show_featured_carousel",
                "description": "Hiển thị danh sách sản phẩm mới hoặc nổi bật dưới dạng thẻ quay.",
                "parameters": {"type": "object", "properties": {}}
            }
        }
    ]

def send_featured_carousel(uid, ms_list=None):
    """Hỗ trợ hiển thị Carousel cho cả Tool và Image Match"""
    if not ms_list:
        ms_list = list(PRODUCTS.keys())[:5]
    
    elements = []
    domain = f"https://{DOMAIN}" if not DOMAIN.startswith("http") else DOMAIN
    for ms in ms_list:
        if ms not in PRODUCTS: continue
        p = PRODUCTS[ms]
        imgs = [u.strip() for u in re.split(r'[,\n;|]+', p["Images"]) if u.strip()]
        elements.append({
            "title": f"[{ms}] {p['Ten']}",
            "image_url": imgs[0] if imgs else "",
            "subtitle": f"Giá: {p['Gia']}\nBấm chi tiết để xem thêm ảnh.",
            "buttons": [
                {"type": "web_url", "url": f"{domain}/order-form?ms={ms}&uid={uid}", "title": "🛒 Đặt ngay"},
                {"type": "postback", "title": "🔍 Chi tiết", "payload": f"ADVICE_{ms}"}
            ]
        })
    
    payload = {"recipient": {"id": uid}, "message": {"attachment": {"type": "template", "payload": {"template_type": "generic", "elements": elements}}}}
    requests.post(f"https://graph.facebook.com/v12.0/me/messages?access_token={PAGE_ACCESS_TOKEN}", json=payload)
    return "Carousel sent"

def execute_tool(uid, name, args):
    ctx = USER_CONTEXT[uid]
    ms = args.get("ms", "").upper() or ctx.get("last_ms")
    domain = f"https://{DOMAIN}" if not DOMAIN.startswith("http") else DOMAIN

    if name == "get_product_info" and ms in PRODUCTS:
        p = PRODUCTS[ms]
        ctx["last_ms"] = ms
        return f"Mã: {ms}\nTên: {p['Ten']}\nGiá: {p['Gia']}\nMàu: {p['Mau']}\nSize: {p['Size']}\nMô tả: {p['MoTa']}"

    if name == "send_product_images" and ms in PRODUCTS:
        urls = [u.strip() for u in re.split(r'[,\n;|]+', PRODUCTS[ms]["Images"]) if u.strip()]
        if urls:
            send_fb_msg(uid, {"text": f"Dạ em gửi ảnh mẫu [{ms}] ạ:"})
            for u in urls[:3]: send_fb_msg(uid, {"attachment": {"type": "image", "payload": {"url": u}}})
        return "Images sent successfully."

    if name == "provide_order_link" and ms in PRODUCTS:
        link = f"{domain}/order-form?ms={ms}&uid={uid}"
        send_fb_msg(uid, {"text": f"Dạ mời anh/chị đặt hàng sản phẩm [{ms}] tại đây nhé:\n{link}"})
        return "Order link sent."

    if name == "show_featured_carousel":
        return send_featured_carousel(uid)
    
    return "Sản phẩm không tồn tại hoặc em chưa rõ mã."

def handle_text(uid, text):
    load_products()
    ctx = USER_CONTEXT[uid]
    
    # Xử lý Postback từ Carousel
    if text.startswith("ADVICE_"):
        ms = text.replace("ADVICE_", "")
        info = execute_tool(uid, "get_product_info", {"ms": ms})
        send_fb_msg(uid, {"text": info})
        execute_tool(uid, "provide_order_link", {"ms": ms})
        return

    quick_ms = detect_ms_from_text(text)
    if quick_ms: ctx["last_ms"] = quick_ms

    system_prompt = f"""Bạn là nhân viên bán hàng của {FANPAGE_NAME}.
    Trả lời cực ngắn gọn (dưới 3 dòng), thân thiện. Xưng em gọi anh/chị.
    Mã khách đang quan tâm: {ctx.get('last_ms', 'Chưa rõ')}.
    Sử dụng công cụ để gửi ảnh, báo giá hoặc link đặt hàng khi khách yêu cầu."""

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
                result = execute_tool(uid, tool.function.name, json.loads(tool.function.arguments))
                messages.append({"role": "tool", "tool_call_id": tool.id, "name": tool.function.name, "content": result})
            
            # AI phản hồi sau khi gọi tool
            final_res = client.chat.completions.create(model="gpt-4o-mini", messages=messages)
            reply = final_res.choices[0].message.content
        else:
            reply = msg.content

        if reply:
            send_fb_msg(uid, {"text": reply})
            ctx["conversation_history"].append({"role": "user", "content": text})
            ctx["conversation_history"].append({"role": "assistant", "content": reply})
            ctx["conversation_history"] = ctx["conversation_history"][-10:]

    except Exception as e:
        print(f"Chat Error: {e}")

def send_fb_msg(uid, payload):
    url = f"https://graph.facebook.com/v12.0/me/messages?access_token={PAGE_ACCESS_TOKEN}"
    requests.post(url, json={"recipient": {"id": uid}, "message": payload})

# ============================================
# WEBHOOK HANDLER (REFERRAL & ECHO)
# ============================================
@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        return request.args.get("hub.challenge") if request.args.get("hub.verify_token") == VERIFY_TOKEN else ("Forbidden", 403)
    
    data = request.get_json()
    for entry in data.get("entry", []):
        for m in entry.get("messaging", []):
            uid = m.get("sender", {}).get("id")
            if not uid: continue
            ctx = USER_CONTEXT[uid]

            # Xử lý Referral Ads
            if "referral" in m:
                ref = m["referral"]
                ad_title = ref.get("ads_context_data", {}).get("ad_title", "")
                ms_ad = extract_ms_from_ad_title(ad_title) or detect_ms_from_text(ref.get("ref", ""))
                if ms_ad:
                    ctx["last_ms"] = ms_ad
                    load_products()
                    info = execute_tool(uid, "get_product_info", {"ms": ms_ad})
                    send_fb_msg(uid, {"text": info})
                continue

            if "postback" in m:
                handle_text(uid, m["postback"]["payload"])
                continue

            msg = m.get("message", {})
            if msg.get("is_echo"):
                if not is_bot_echo(msg.get("text"), msg.get("app_id"), None):
                    agent_ms = detect_ms_from_text(msg.get("text"))
                    if agent_ms: ctx["last_ms"] = agent_ms
                continue

            mid = msg.get("mid")
            if mid and mid in ctx["processed_message_mids"]: continue
            if mid: ctx["processed_message_mids"][mid] = time.time()

            if "text" in msg: handle_text(uid, msg["text"])
            elif "attachments" in msg:
                for att in msg["attachments"]:
                    if att["type"] == "image": handle_image(uid, att["payload"]["url"])
    return "OK", 200

# ============================================
# ORDER FORM (FIXED FOR KOYEB)
# ============================================
@app.route("/order-form")
def order_form():
    ms, uid = request.args.get("ms", "").upper(), request.args.get("uid", "")
    load_products()
    product = PRODUCTS.get(ms)
    if not product: return "Sản phẩm không tồn tại.", 404
    
    price_raw = str(product.get("Gia", "0")).replace(".", "").replace(",", "").replace("đ", "").strip()
    price_int = int(re.sub(r'\D', '', price_raw)) if re.sub(r'\D', '', price_raw) else 0
    colors = [c.strip() for c in product.get("Mau", "").split(",") if c.strip()] or ["Mặc định"]
    sizes = [s.strip() for s in product.get("Size", "").split(",") if s.strip()] or ["Free Size"]
    imgs = [u.strip() for u in re.split(r'[,\n;|]+', product.get("Images", "")) if u.strip()]
    
    return render_template("order-form.html", ms=ms, uid=uid, product=product, fanpage_name=FANPAGE_NAME,
                           price_int=price_int, colors=colors, sizes=sizes, default_image=imgs[0] if imgs else "",
                           api_base_url=f"https://{DOMAIN}" if not DOMAIN.startswith("http") else DOMAIN, domain=DOMAIN)

@app.route("/api/submit-order", methods=["POST"])
def api_submit_order():
    data = request.get_json()
    send_fb_msg(data.get("uid"), {"text": "🎉 Shop đã nhận đơn hàng thành công! Shop sẽ gọi xác nhận ngay nhé."})
    return jsonify({"status": "ok"})

@app.route("/")
def home(): return "Bot Live", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
