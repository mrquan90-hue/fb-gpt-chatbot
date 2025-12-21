import os
import requests
import json
import logging
import pandas as pd
from flask import Flask, request, jsonify, render_template
from datetime import datetime, timedelta
import hashlib
import hmac
import re

app = Flask(__name__)

# Cấu hình
VERIFY_TOKEN = os.environ.get('VERIFY_TOKEN', 'your_verify_token')
PAGE_ACCESS_TOKEN = os.environ.get('PAGE_ACCESS_TOKEN', 'your_page_access_token')
PAGE_ID = os.environ.get('FACEBOOK_PAGE_ID', '516937221685203')  # ID từ log

# Các biến toàn cục
products = None
product_embeddings = None
product_mapping = {}
user_contexts = {}
processed_messages = {}  # Track các message đã xử lý

# Hàm lấy tên fanpage
def get_page_name():
    url = f"https://graph.facebook.com/v18.0/{PAGE_ID}?fields=name&access_token={PAGE_ACCESS_TOKEN}"
    try:
        response = requests.get(url, timeout=5)
        data = response.json()
        return data.get('name', 'Unknown Page')
    except Exception as e:
        print(f"Lỗi khi lấy tên fanpage: {e}")
        return 'Unknown Page'

# Hàm gửi tin nhắn qua Messenger
def send_message(recipient_id, message_text, quick_replies=None):
    """
    Gửi tin nhắn tới người dùng qua Facebook Messenger
    """
    try:
        # Kiểm tra recipient_id không phải là page_id
        if str(recipient_id) == str(PAGE_ID):
            print(f"[WARNING] Không gửi tin nhắn cho chính page: {recipient_id}")
            return None
        
        params = {
            "recipient": {"id": recipient_id},
            "message": message_text,
            "messaging_type": "RESPONSE"
        }
        
        if quick_replies:
            params["message"]["quick_replies"] = quick_replies
            
        headers = {"Content-Type": "application/json"}
        
        response = requests.post(
            f"https://graph.facebook.com/v18.0/me/messages?access_token={PAGE_ACCESS_TOKEN}",
            json=params,
            headers=headers
        )
        
        if response.status_code == 200:
            print(f"✅ Đã gửi tin nhắn cho {recipient_id}")
            return response.json()
        else:
            print(f"❌ Lỗi gửi tin nhắn: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Exception khi gửi tin nhắn: {e}")
        return None

# Hàm gửi tin nhắn văn bản đơn giản
def send_text_message(recipient_id, text, quick_replies=None):
    message = {"text": text}
    return send_message(recipient_id, message, quick_replies)

# Hàm gửi carousel sản phẩm
def send_product_carousel(recipient_id, product_ms):
    try:
        if product_ms not in product_mapping:
            send_text_message(recipient_id, "❌ Không tìm thấy sản phẩm này.")
            return
        
        product = product_mapping[product_ms]
        variants = product['variants']
        
        # Tạo các element cho carousel
        elements = []
        
        for variant in variants[:10]:  # Giới hạn 10 variants
            element = {
                "title": f"[{product_ms}] {product['name']}",
                "subtitle": variant['variant_name'],
                "image_url": variant['image_url'],
                "buttons": [
                    {
                        "type": "postback",
                        "title": "🔍 Xem chi tiết",
                        "payload": f"ADVICE_{product_ms}"
                    }
                ]
            }
            elements.append(element)
        
        # Gửi carousel
        message = {
            "attachment": {
                "type": "template",
                "payload": {
                    "template_type": "generic",
                    "elements": elements
                }
            }
        }
        
        result = send_message(recipient_id, message)
        if result:
            print(f"[SINGLE CAROUSEL] Đã gửi carousel {len(elements)} sản phẩm {product_ms} cho user {recipient_id}")
        return result
        
    except Exception as e:
        print(f"❌ Lỗi khi gửi carousel: {e}")
        send_text_message(recipient_id, "❌ Có lỗi khi tải thông tin sản phẩm.")

# Hàm gửi chi tiết sản phẩm
def send_product_advice(recipient_id, product_ms):
    try:
        if product_ms not in product_mapping:
            send_text_message(recipient_id, "❌ Không tìm thấy sản phẩm này.")
            return
        
        product = product_mapping[product_ms]
        variants = product['variants']
        
        # Gửi ảnh đầu tiên
        if variants and variants[0]['image_url']:
            message = {
                "attachment": {
                    "type": "image",
                    "payload": {
                        "url": variants[0]['image_url'],
                        "is_reusable": True
                    }
                }
            }
            send_message(recipient_id, message)
        
        # Gửi mô tả
        description = product.get('description', '')
        if description:
            send_text_message(recipient_id, f"📝 MÔ TẢ:\n{description}")
        
        # Gửi giá
        price_text = "💰 GIÁ SẢN PHẨM:\n"
        unique_variants = []
        seen = set()
        
        for variant in variants[:5]:  # Giới hạn 5 variants
            key = variant['variant_name']
            if key not in seen:
                seen.add(key)
                price = variant.get('price', 'Liên hệ')
                price_text += f"{key}: {price}\n"
        
        if len(variants) > 5:
            price_text += f"... và {len(variants)-5} phân loại khác"
        
        send_text_message(recipient_id, price_text)
        
        # Gửi link đặt hàng
        order_url = f"https://{request.host}/order-form?ms={product_ms}&uid={recipient_id}"
        send_text_message(recipient_id, f"📋 Đặt hàng ngay tại đây:\n{order_url}")
        
        print(f"[ADVICE] Đã gửi chi tiết sản phẩm {product_ms} cho user {recipient_id}")
        
    except Exception as e:
        print(f"❌ Lỗi khi gửi advice: {e}")
        send_text_message(recipient_id, "❌ Có lỗi khi tải thông tin chi tiết.")

# Hàm xử lý tin nhắn văn bản
def process_message(sender_id, message_text):
    global user_contexts
    
    # Khởi tạo context nếu chưa có
    if sender_id not in user_contexts:
        user_contexts[sender_id] = {
            'last_ms': None,
            'history': [],
            'first_message_processed': False
        }
    
    context = user_contexts[sender_id]
    last_ms = context['last_ms']
    
    # Kiểm tra nếu là tin nhắn đầu tiên sau referral
    if not context['first_message_processed'] and last_ms:
        print(f"[FIRST MESSAGE] User {sender_id} gửi tin nhắn đầu tiên sau referral, gửi carousel cho {last_ms}")
        
        # Gửi carousel cho sản phẩm cuối cùng
        send_product_carousel(sender_id, last_ms)
        
        # Đánh dấu đã xử lý tin nhắn đầu tiên
        context['first_message_processed'] = True
        
        print(f"[FIRST MESSAGE DONE] Đã xử lý xong tin nhắn đầu tiên, không chạy tiếp function calling")
        return
    
    # ... (phần xử lý function calling giữ nguyên nếu có)
    # Ở đây chỉ xử lý các tin nhắn thông thường
    response_text = "Xin chào! Tôi là trợ lý bán hàng của shop. Bạn cần tư vấn gì ạ?"
    send_text_message(sender_id, response_text)

# Middleware để log request
@app.before_request
def log_request_info():
    if request.method == 'POST' and request.path == '/webhook':
        data = request.get_json(silent=True) or {}
        
        # Log thông tin cơ bản
        if data.get('object') == 'page':
            for entry in data.get('entry', []):
                for messaging in entry.get('messaging', []):
                    sender_id = messaging.get('sender', {}).get('id', '')
                    is_echo = messaging.get('message', {}).get('is_echo', False)
                    
                    if is_echo:
                        print(f"[ECHO DETECTED] From: {sender_id}, Is Echo: {is_echo}")

# Route webhook chính
@app.route('/webhook', methods=['GET', 'POST'])
def webhook():
    global user_contexts, processed_messages
    
    if request.method == 'GET':
        # Verify webhook
        mode = request.args.get('hub.mode')
        token = request.args.get('hub.verify_token')
        challenge = request.args.get('hub.challenge')
        
        if mode and token:
            if mode == 'subscribe' and token == VERIFY_TOKEN:
                print('WEBHOOK_VERIFIED')
                return challenge, 200
            else:
                return 'Verification token mismatch', 403
        return 'Hello World', 200
    
    elif request.method == 'POST':
        data = request.get_json()
        
        # Xử lý echo message sớm
        if data.get('object') == 'page':
            for entry in data.get('entry', []):
                messaging_events = entry.get('messaging', [])
                for messaging_event in messaging_events:
                    # Lấy message_id để tracking
                    message_id = None
                    if messaging_event.get('message'):
                        message_id = messaging_event['message'].get('mid')
                    elif messaging_event.get('postback'):
                        message_id = messaging_event['postback'].get('mid')
                    
                    # Kiểm tra echo message
                    if messaging_event.get('message') and messaging_event['message'].get('is_echo'):
                        print(f"[ECHO BOT] Bỏ qua echo message từ bot: {messaging_event['message'].get('text', '...')[:50]}")
                        continue
                    
                    # Kiểm tra nếu sender là page (bot)
                    sender_id = messaging_event.get('sender', {}).get('id')
                    if sender_id and str(sender_id) == str(PAGE_ID):
                        print(f"[ECHO BOT] Bỏ qua message từ chính page/bot")
                        continue
                    
                    # Kiểm tra message đã xử lý chưa
                    if message_id and message_id in processed_messages:
                        print(f"[DUPLICATE] Đã xử lý message {message_id}, bỏ qua")
                        continue
                    
                    # Nếu chưa xử lý, thêm vào danh sách
                    if message_id:
                        processed_messages[message_id] = datetime.now()
                        # Giới hạn số lượng message tracking (tránh memory leak)
                        if len(processed_messages) > 1000:
                            # Xóa các message cũ nhất
                            oldest = min(processed_messages, key=processed_messages.get)
                            del processed_messages[oldest]
                    
                    # Xử lý message
                    if messaging_event.get('message'):
                        # Xử lý tin nhắn văn bản
                        sender_id = messaging_event['sender']['id']
                        message_text = messaging_event['message'].get('text', '')
                        
                        if message_text:
                            print(f"[MESSAGE] User {sender_id}: {message_text}")
                            process_message(sender_id, message_text)
                    
                    elif messaging_event.get('postback'):
                        # Xử lý postback từ button
                        sender_id = messaging_event['sender']['id']
                        postback = messaging_event['postback']
                        
                        # Bỏ qua nếu sender là page
                        if str(sender_id) == str(PAGE_ID):
                            print(f"[ECHO BOT] Bỏ qua postback từ chính page")
                            continue
                        
                        payload = postback.get('payload', '')
                        print(f"[POSTBACK] User {sender_id}: {payload}")
                        
                        # Xử lý payload ADVICE
                        if payload.startswith('ADVICE_'):
                            product_ms = payload.replace('ADVICE_', '')
                            
                            # Cập nhật context
                            if sender_id not in user_contexts:
                                user_contexts[sender_id] = {
                                    'last_ms': product_ms,
                                    'history': [product_ms],
                                    'first_message_processed': True
                                }
                            else:
                                user_contexts[sender_id]['last_ms'] = product_ms
                                if product_ms not in user_contexts[sender_id]['history']:
                                    user_contexts[sender_id]['history'].append(product_ms)
                            
                            print(f"[CONTEXT UPDATE] User {sender_id}: last_ms={product_ms}, history={user_contexts[sender_id]['history']}")
                            
                            # Gửi chi tiết sản phẩm
                            send_product_advice(sender_id, product_ms)
                    
                    else:
                        # Delivery, read receipts, etc.
                        pass
        
        return 'OK', 200

# Route cho referral từ comment
@app.route('/referral', methods=['GET'])
def handle_referral():
    global user_contexts
    
    user_id = request.args.get('user_id')
    product_ms = request.args.get('ms')
    
    if not user_id or not product_ms:
        return jsonify({'error': 'Missing parameters'}), 400
    
    print(f"[REFERRAL] User {user_id} referred from product {product_ms}")
    
    # Cập nhật context
    if user_id not in user_contexts:
        user_contexts[user_id] = {
            'last_ms': product_ms,
            'history': [product_ms],
            'first_message_processed': False
        }
    else:
        user_contexts[user_id]['last_ms'] = product_ms
        if product_ms not in user_contexts[user_id]['history']:
            user_contexts[user_id]['history'].append(product_ms)
    
    print(f"[CONTEXT UPDATE] User {user_id}: last_ms={product_ms}, history={user_contexts[user_id]['history']}")
    
    return jsonify({
        'status': 'success',
        'message': f'Đã ghi nhận referral cho user {user_id} với sản phẩm {product_ms}'
    })

# Route cho form đặt hàng
@app.route('/order-form', methods=['GET'])
def order_form():
    ms = request.args.get('ms')
    uid = request.args.get('uid')
    
    if not ms or not uid:
        return "Thiếu thông tin sản phẩm hoặc người dùng", 400
    
    page_name = get_page_name()
    print(f"✅ Lấy tên fanpage từ API thành công: {page_name}")
    
    # Render form đặt hàng
    return render_template('order_form.html', 
                         ms=ms, 
                         uid=uid,
                         page_name=page_name)

# Route cho echo từ bình luận (được gọi bởi Facebook)
@app.route('/echo-comment', methods=['POST'])
def echo_comment():
    data = request.get_json()
    print(f"[ECHO USER] Đang xử lý echo từ bình luận người dùng")
    
    # Xử lý echo từ comment
    if data.get('entry'):
        for entry in data['entry']:
            if 'changes' in entry:
                for change in entry['changes']:
                    value = change.get('value', {})
                    if 'from' in value and 'post' in value:
                        user_id = value['from']['id']
                        user_name = value['from']['name']
                        message = value.get('message', '')
                        post_id = value['post']['id']
                        
                        print(f"[ECHO COMMENT] {user_name} ({user_id}): {message}")
                        
                        # Tìm mã sản phẩm trong message
                        ms_match = re.search(r'#(MS\d+)', message)
                        if ms_match:
                            product_ms = ms_match.group(1)
                            print(f"[ECHO FCHAT] Phát hiện mã sản phẩm: {product_ms} cho user: {user_id}")
                            
                            # Cập nhật context
                            if user_id not in user_contexts:
                                user_contexts[user_id] = {
                                    'last_ms': product_ms,
                                    'history': [product_ms],
                                    'first_message_processed': False
                                }
                            else:
                                user_contexts[user_id]['last_ms'] = product_ms
                                if product_ms not in user_contexts[user_id]['history']:
                                    user_contexts[user_id]['history'].append(product_ms)
                            
                            print(f"[CONTEXT UPDATED] Đã ghi nhận mã {product_ms} vào ngữ cảnh cho user {user_id}")
    
    return jsonify({'status': 'ok'}), 200

# Hàm load sản phẩm từ Google Sheets
def load_products():
    global products, product_mapping, product_embeddings
    
    try:
        print("🟦 Loading sheet: https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv")
        
        # Load CSV từ Google Sheets
        df = pd.read_csv('https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv')
        
        # Xử lý dữ liệu
        products = []
        product_mapping = {}
        
        for _, row in df.iterrows():
            product = {
                'ms': row['MS'],
                'name': row['TÊN SẢN PHẨM'],
                'description': row.get('MÔ TẢ', ''),
                'variants': []
            }
            
            # Xử lý variants
            for i in range(1, 51):  # Giả sử có tối đa 50 variants
                variant_name = row.get(f'Variant {i} Name', '')
                variant_image = row.get(f'Variant {i} Image', '')
                variant_price = row.get(f'Variant {i} Price', '')
                
                if variant_name and pd.notna(variant_name):
                    variant = {
                        'variant_name': variant_name,
                        'image_url': variant_image if pd.notna(variant_image) else '',
                        'price': variant_price if pd.notna(variant_price) else 'Liên hệ'
                    }
                    product['variants'].append(variant)
            
            products.append(product)
            product_mapping[product['ms']] = product
        
        print(f"📦 Loaded {len(products)} products với {sum(len(p['variants']) for p in products)} variants.")
        
        # Tính tỷ lệ variants có ảnh
        total_variants = sum(len(p['variants']) for p in products)
        variants_with_image = sum(1 for p in products for v in p['variants'] if v['image_url'])
        percentage = (variants_with_image / total_variants * 100) if total_variants > 0 else 0
        print(f"📊 Variants có ảnh: {variants_with_image}/{total_variants} ({percentage:.1f}%)")
        
        # Tạo embeddings cho tìm kiếm (giữ nguyên)
        product_embeddings = {}
        for product in products:
            text = f"{product['name']} {product['description']}"
            product_embeddings[product['ms']] = text.lower()
        
        print(f"🔢 Created mapping for {len(product_mapping)} product numbers")
        print(f"🔤 Created text embeddings for {len(product_embeddings)} products")
        
        # Hiển thị sample product
        if 'MS000046' in product_mapping:
            sample = product_mapping['MS000046']
            print(f"📊 Sample product MS000046: {len(sample['variants'])} variants")
            for i, variant in enumerate(sample['variants'][:3], 1):
                print(f"  Variant {i}: {variant['variant_name']} - Ảnh: {variant['image_url'][:50]}...")
        
    except Exception as e:
        print(f"❌ Lỗi khi load products: {e}")
        products = []
        product_mapping = {}

# Route để reload sản phẩm
@app.route('/reload-products', methods=['GET'])
def reload_products():
    load_products()
    return jsonify({
        'status': 'success',
        'message': f'Đã reload {len(products)} sản phẩm'
    })

# Health check endpoint
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'products_loaded': len(products) if products else 0,
        'page_id': PAGE_ID
    })

# Khởi tạo khi server start
@app.before_first_request
def initialize():
    load_products()
    print(f"🚀 Bot đã khởi động với Page ID: {PAGE_ID}")

if __name__ == '__main__':
    # Khởi động server
    port = int(os.environ.get('PORT', 8000))
    print(f"Starting server on port {port}...")
    
    # Load products ngay khi start
    load_products()
    
    app.run(host='0.0.0.0', port=port)
