import os
import json
import time
import requests
import pandas as pd
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import hashlib
from datetime import datetime, timedelta
from collections import OrderedDict

app = Flask(__name__)
CORS(app)

# Facebook Page Access Token
PAGE_ACCESS_TOKEN = os.environ.get('PAGE_ACCESS_TOKEN')
FACEBOOK_API_URL = 'https://graph.facebook.com/v19.0/'

# Google Sheets URL
SHEET_URL = os.environ.get('SHEET_URL', 'https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv')

# Global variables
products = []
variants_dict = {}
product_mapping = {}
user_contexts = {}
last_message_time = {}

# Event Tracker để tránh xử lý trùng lặp
class EventTracker:
    def __init__(self, max_size=2000, ttl=120):
        self.events = OrderedDict()
        self.ttl = ttl
        self.max_size = max_size
    
    def add(self, event_id):
        current_time = time.time()
        # Clean old events
        self.cleanup(current_time)
        
        # Add new event
        self.events[event_id] = current_time
        
        # Limit size
        if len(self.events) > self.max_size:
            self.events.popitem(last=False)
    
    def contains(self, event_id):
        current_time = time.time()
        if event_id in self.events:
            if current_time - self.events[event_id] < self.ttl:
                return True
            else:
                del self.events[event_id]
        return False
    
    def cleanup(self, current_time=None):
        if current_time is None:
            current_time = time.time()
        
        to_remove = []
        for event_id, timestamp in self.events.items():
            if current_time - timestamp > self.ttl:
                to_remove.append(event_id)
        
        for event_id in to_remove:
            del self.events[event_id]

# Initialize event tracker
event_tracker = EventTracker(max_size=2000, ttl=120)

def load_products():
    """Load products from Google Sheets"""
    global products, variants_dict, product_mapping
    try:
        print("🟦 Loading sheet:", SHEET_URL)
        df = pd.read_csv(SHEET_URL)
        df = df.fillna('')
        
        products = []
        variants_dict = {}
        product_mapping = {}
        
        # Group by product number
        for product_num, group in df.groupby('Mã sản phẩm'):
            product_data = {
                'ms': product_num,
                'name': group.iloc[0]['Tên sản phẩm'] if 'Tên sản phẩm' in group.columns else '',
                'category': group.iloc[0]['Loại sản phẩm'] if 'Loại sản phẩm' in group.columns else '',
                'variants': []
            }
            
            # Collect variants
            variants = []
            for _, row in group.iterrows():
                variant = {
                    'color': row['Màu'] if 'Màu' in row else '',
                    'size': row['Kích cỡ'] if 'Kích cỡ' in row else '',
                    'price': row['Giá'] if 'Giá' in row else '',
                    'weight_range': row['Cân nặng'] if 'Cân nặng' in row else '',
                    'image_url': row['Link ảnh'] if 'Link ảnh' in row else ''
                }
                variants.append(variant)
            
            product_data['variants'] = variants
            products.append(product_data)
            
            # Store in variants dict
            variants_dict[product_num] = variants
            
            # Create mapping for quick lookup
            product_mapping[product_num] = product_data
        
        print(f"📦 Loaded {len(products)} products với {sum(len(v) for v in variants_dict.values())} variants.")
        
        # Check which variants have images
        variants_with_images = sum(1 for variants in variants_dict.values() for v in variants if v.get('image_url', '').startswith('http'))
        total_variants = sum(len(v) for v in variants_dict.values())
        print(f"📊 Variants có ảnh: {variants_with_images}/{total_variants} ({variants_with_images/total_variants*100:.1f}%)")
        
        # Sample output for debugging
        if products:
            sample = products[0]
            print(f"🔢 Created mapping for {len(product_mapping)} product numbers")
            print(f"📊 Sample product {sample['ms']}: {len(sample['variants'])} variants")
            for i, variant in enumerate(sample['variants'][:3], 1):
                print(f"  Variant {i}: {variant.get('color', '')}/{variant.get('size', '')} ({variant.get('weight_range', '')}) - Ảnh: {variant.get('image_url', '')[:50]}...")
        
        # Create text embeddings for search
        create_text_embeddings()
        
    except Exception as e:
        print(f"❌ Error loading products: {e}")
        products = []
        variants_dict = {}
        product_mapping = {}

def create_text_embeddings():
    """Create simple text embeddings for product search"""
    global products
    for product in products:
        # Create a searchable text representation
        search_text = f"{product['ms']} {product['name']} {product['category']}"
        for variant in product['variants']:
            search_text += f" {variant['color']} {variant['size']} {variant['weight_range']}"
        product['search_text'] = search_text.lower()
    
    print(f"🔤 Created text embeddings for {len(products)} products")

def send_message(recipient_id, message_text, quick_replies=None, attachment=None):
    """Send message via Facebook Graph API"""
    if not PAGE_ACCESS_TOKEN:
        print("❌ PAGE_ACCESS_TOKEN not set")
        return None
    
    url = f"{FACEBOOK_API_URL}me/messages"
    params = {'access_token': PAGE_ACCESS_TOKEN}
    
    message_data = {
        'recipient': {'id': recipient_id},
        'messaging_type': 'RESPONSE'
    }
    
    if attachment:
        message_data['message'] = {'attachment': attachment}
    elif quick_replies:
        message_data['message'] = {
            'text': message_text,
            'quick_replies': quick_replies
        }
    else:
        message_data['message'] = {'text': message_text}
    
    try:
        response = requests.post(url, params=params, json=message_data, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Error sending message: {e}")
        if hasattr(e, 'response') and e.response:
            print(f"Response: {e.response.text}")
        return None

def send_carousel(recipient_id, product):
    """Send product carousel"""
    elements = []
    
    # Get first 10 variants with images
    variants_with_images = [v for v in product['variants'] if v.get('image_url', '').startswith('http')][:10]
    
    if not variants_with_images:
        # Send simple message if no images
        send_message(recipient_id, f"📦 {product['name']}\nMã: {product['ms']}")
        return
    
    # Create carousel elements for each variant
    for i, variant in enumerate(variants_with_images[:10]):  # Facebook limits to 10 elements
        element = {
            'title': f"{product['ms']} - {variant.get('color', '')} {variant.get('size', '')}",
            'subtitle': f"💰 Giá: {variant.get('price', 'N/A')}\n⚖️ Cân nặng: {variant.get('weight_range', '')}",
            'image_url': variant.get('image_url', '')
        }
        
        # Add default action
        element['default_action'] = {
            'type': 'web_url',
            'url': f"{request.host_url}order-form?ms={product['ms']}&uid={recipient_id}",
            'webview_height_ratio': 'tall'
        }
        
        # Add buttons
        element['buttons'] = [
            {
                'type': 'postback',
                'title': '🔍 Xem chi tiết',
                'payload': f"ADVICE_{product['ms']}"
            },
            {
                'type': 'web_url',
                'title': '🛒 Đặt hàng',
                'url': f"{request.host_url}order-form?ms={product['ms']}&uid={recipient_id}",
                'webview_height_ratio': 'full'
            }
        ]
        
        elements.append(element)
    
    # Send carousel
    attachment = {
        'type': 'template',
        'payload': {
            'template_type': 'generic',
            'elements': elements
        }
    }
    
    send_message(recipient_id, "", attachment=attachment)

def send_product_details(recipient_id, ms_code):
    """Send detailed product information"""
    if ms_code not in product_mapping:
        send_message(recipient_id, f"❌ Không tìm thấy sản phẩm với mã {ms_code}")
        return
    
    product = product_mapping[ms_code]
    
    # Send product name and code
    send_message(recipient_id, f"📌 [{product['ms']}] {product['name']}")
    
    # Send images (first 3 variants with images)
    variants_with_images = [v for v in product['variants'] if v.get('image_url', '').startswith('http')][:3]
    for variant in variants_with_images:
        attachment = {
            'type': 'image',
            'payload': {'url': variant.get('image_url', '')}
        }
        send_message(recipient_id, "", attachment=attachment)
    
    # Send description
    description = f"📝 MÔ TẢ:\n💸 Giá chỉ từ: **{min(v.get('price', '0') for v in product['variants'] if v.get('price', '').isdigit())}K – {max(v.get('price', '0') for v in product['variants'] if v.get('price', '').isdigit())}K** ✨ Diện 1 set là auto trẻ trung, năng động, che dáng cực khéo! Set đồ thể thao phong cách Hàn – Nhật, mặc đi chơi, dạo phố hay tập gym đều xịn xò."
    send_message(recipient_id, description)
    
    # Send price list (first 5 variants)
    price_list = "💰 GIÁ SẢN PHẨM:\n"
    for variant in product['variants'][:5]:
        color = variant.get('color', '')
        size = variant.get('size', '')
        weight = variant.get('weight_range', '')
        price = variant.get('price', 'N/A')
        price_list += f"{color} - {size} ({weight}): {price}\n"
    
    if len(product['variants']) > 5:
        price_list += f"... và {len(product['variants']) - 5} phân loại khác"
    
    send_message(recipient_id, price_list)
    
    # Send order link
    order_url = f"{request.host_url}order-form?ms={ms_code}&uid={recipient_id}"
    send_message(recipient_id, f"📋 Đặt hàng ngay tại đây:\n{order_url}")

def get_page_name():
    """Get Facebook page name"""
    if not PAGE_ACCESS_TOKEN:
        return "Shop"
    
    try:
        url = f"{FACEBOOK_API_URL}me"
        params = {'access_token': PAGE_ACCESS_TOKEN, 'fields': 'name'}
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get('name', 'Shop')
    except Exception as e:
        print(f"❌ Error getting page name: {e}")
        return "Shop"

def process_message(event):
    """Process incoming message"""
    sender_id = event['sender']['id']
    message_text = event['message'].get('text', '').strip().lower()
    
    # Kiểm tra rate limiting (tối thiểu 2 giây giữa các tin nhắn từ cùng user)
    current_time = time.time()
    if sender_id in last_message_time:
        time_diff = current_time - last_message_time[sender_id]
        if time_diff < 2:
            print(f"[RATE LIMIT] Bỏ qua tin nhắn từ {sender_id}, quá nhanh ({time_diff:.2f}s)")
            return
    
    last_message_time[sender_id] = current_time
    
    # Initialize user context if not exists
    if sender_id not in user_contexts:
        user_contexts[sender_id] = {
            'last_ms': '',
            'history': [],
            'created_at': datetime.now().isoformat()
        }
    
    # Check if this is first message after referral
    context = user_contexts[sender_id]
    has_history = len(context['history']) > 0
    
    if has_history and context['last_ms']:
        print(f"[FIRST MESSAGE] User {sender_id} gửi tin nhắn đầu tiên sau referral, gửi carousel cho {context['last_ms']}")
        
        # Send carousel for the product
        if context['last_ms'] in product_mapping:
            product = product_mapping[context['last_ms']]
            send_carousel(sender_id, product)
            
            # Update context
            if context['last_ms'] not in context['history']:
                context['history'].append(context['last_ms'])
            
            print(f"[FIRST MESSAGE DONE] Đã xử lý xong tin nhắn đầu tiên, không chạy tiếp function calling")
        else:
            send_message(sender_id, "Xin lỗi, không tìm thấy thông tin sản phẩm.")
        
        return
    
    # Normal message processing
    if 'giá' in message_text or 'bao nhiêu' in message_text:
        if context['last_ms']:
            send_product_details(sender_id, context['last_ms'])
        else:
            send_message(sender_id, "Vui lòng chọn sản phẩm trước khi hỏi giá ạ!")
    elif 'sản phẩm' in message_text or 'hàng' in message_text:
        send_message(sender_id, "Shop có nhiều sản phẩm thời trang thể thao. Bạn có thể xem qua các sản phẩm nổi bật hoặc cho mình biết bạn đang tìm gì ạ!")
    else:
        # Default response
        if context['last_ms']:
            quick_replies = [
                {
                    'content_type': 'text',
                    'title': '💰 Hỏi giá',
                    'payload': 'ASK_PRICE'
                },
                {
                    'content_type': 'text',
                    'title': '📦 Sản phẩm khác',
                    'payload': 'OTHER_PRODUCTS'
                }
            ]
            send_message(sender_id, "Bạn muốn biết thêm thông tin gì về sản phẩm này ạ?", quick_replies=quick_replies)
        else:
            send_message(sender_id, "Chào bạn! Mình có thể giúp gì cho bạn ạ?")

def process_postback(event):
    """Process postback event"""
    sender_id = event['sender']['id']
    payload = event['postback']['payload']
    
    print(f"[POSTBACK] User {sender_id}: {payload}")
    
    # Kiểm tra và bỏ qua nếu đã xử lý
    timestamp = event.get('timestamp', '')
    event_id = f"pb_{sender_id}_{payload}_{timestamp}"
    if event_tracker.contains(event_id):
        print(f"[DUPLICATE] Bỏ qua postback đã xử lý: {event_id}")
        return
    
    # Đánh dấu đã xử lý
    event_tracker.add(event_id)
    
    # Initialize user context if not exists
    if sender_id not in user_contexts:
        user_contexts[sender_id] = {
            'last_ms': '',
            'history': [],
            'created_at': datetime.now().isoformat()
        }
    
    context = user_contexts[sender_id]
    
    # Check if it's ADVICE_MSxxxxx
    if payload.startswith('ADVICE_'):
        ms_code = payload.replace('ADVICE_', '')
        
        # Cập nhật context một lần duy nhất
        context['last_ms'] = ms_code
        if ms_code not in context['history']:
            context['history'].append(ms_code)
        
        print(f"[CONTEXT UPDATE] User {sender_id}: last_ms={ms_code}, history={context['history']}")
        
        # Gửi chi tiết sản phẩm
        send_product_details(sender_id, ms_code)
        return
    
    # Handle other postbacks
    if payload == 'ASK_PRICE':
        if context['last_ms']:
            send_product_details(sender_id, context['last_ms'])
        else:
            send_message(sender_id, "Vui lòng chọn sản phẩm trước khi hỏi giá ạ!")
    elif payload == 'OTHER_PRODUCTS':
        send_message(sender_id, "Hiện tại shop đang có nhiều sản phẩm thời trang thể thao. Bạn có thể xem thêm tại trang chủ hoặc cho mình biết bạn đang tìm gì ạ!")
    elif payload == 'GET_STARTED':
        send_message(sender_id, "Chào mừng bạn đến với shop! Mình có thể giúp bạn tìm sản phẩm phù hợp.")

@app.route('/webhook', methods=['GET'])
def verify_webhook():
    """Verify webhook for Facebook"""
    mode = request.args.get('hub.mode')
    token = request.args.get('hub.verify_token')
    challenge = request.args.get('hub.challenge')
    
    verify_token = os.environ.get('VERIFY_TOKEN', 'my_verify_token')
    
    if mode == 'subscribe' and token == verify_token:
        print("✅ Webhook verified")
        return challenge
    else:
        print("❌ Webhook verification failed")
        return jsonify({'error': 'Verification failed'}), 403

@app.route('/webhook', methods=['POST'])
def webhook():
    """Main webhook endpoint for Facebook"""
    data = request.get_json()
    
    if not data:
        print("❌ No data received")
        return jsonify({'status': 'no data'}), 200
    
    print(f"Webhook received: {json.dumps(data)[:500]}...")
    
    # Handle page feed updates (comments)
    if 'entry' in data and 'changes' in data['entry'][0]:
        entry = data['entry'][0]
        page_id = entry.get('id')
        
        for change in entry.get('changes', []):
            if change.get('field') == 'feed':
                value = change.get('value')
                
                # Check if it's a comment with product code
                if value.get('item') == 'comment' and value.get('verb') == 'add':
                    from_user = value.get('from', {})
                    comment_message = value.get('message', '').lower()
                    
                    # Look for product codes in comment
                    for product in products:
                        if product['ms'].lower() in comment_message:
                            user_id = from_user.get('id')
                            if user_id:
                                print(f"[ECHO USER] Đang xử lý echo từ bình luận người dùng")
                                
                                # Update user context
                                if user_id not in user_contexts:
                                    user_contexts[user_id] = {
                                        'last_ms': product['ms'],
                                        'history': [product['ms']],
                                        'created_at': datetime.now().isoformat()
                                    }
                                else:
                                    user_contexts[user_id]['last_ms'] = product['ms']
                                    if product['ms'] not in user_contexts[user_id]['history']:
                                        user_contexts[user_id]['history'].append(product['ms'])
                                
                                print(f"[ECHO FCHAT] Phát hiện mã sản phẩm: {product['ms']} cho user: {user_id}")
                                print(f"[CONTEXT UPDATE] User {user_id}: last_ms={product['ms']}, history={user_contexts[user_id]['history']}")
                                print(f"[CONTEXT UPDATED] Đã ghi nhận mã {product['ms']} vào ngữ cảnh cho user {user_id}")
                                
                                # Send echo message (Facebook will handle this)
                                break
    
    # Handle messaging events
    elif 'entry' in data and 'messaging' in data['entry'][0]:
        for entry in data['entry']:
            for messaging_event in entry.get('messaging', []):
                # Kiểm tra và bỏ qua echo messages từ bot
                if messaging_event.get('message', {}).get('is_echo'):
                    print("[ECHO BOT] Bỏ qua echo message từ bot: ...")
                    continue
                
                # Kiểm tra và bỏ qua delivery/read receipts
                if 'delivery' in messaging_event or 'read' in messaging_event:
                    print(f"[SKIP] Bỏ qua delivery/read event")
                    continue
                
                # Tạo event_id duy nhất
                sender_id = messaging_event.get('sender', {}).get('id')
                timestamp = messaging_event.get('timestamp', '')
                event_id = None
                
                if 'message' in messaging_event:
                    mid = messaging_event['message'].get('mid')
                    if mid:
                        event_id = f"msg_{mid}"
                elif 'postback' in messaging_event:
                    payload = messaging_event['postback'].get('payload', '')
                    event_id = f"pb_{sender_id}_{payload}_{timestamp}"
                
                # Kiểm tra trùng lặp
                if event_id and event_tracker.contains(event_id):
                    print(f"[DUPLICATE] Bỏ qua event đã xử lý: {event_id}")
                    continue
                
                # Đánh dấu event đã xử lý
                if event_id:
                    event_tracker.add(event_id)
                
                # Xử lý các loại event
                if 'message' in messaging_event:
                    process_message(messaging_event)
                elif 'postback' in messaging_event:
                    process_postback(messaging_event)
    
    return jsonify({'status': 'ok'}), 200

@app.route('/order-form')
def order_form():
    """Render order form"""
    ms_code = request.args.get('ms', '')
    user_id = request.args.get('uid', '')
    
    # Get product info
    product = None
    if ms_code in product_mapping:
        product = product_mapping[ms_code]
    
    # Get page name
    page_name = get_page_name()
    print(f"✅ Lấy tên fanpage từ API thành công: {page_name}")
    
    return render_template('order_form.html', 
                         product=product, 
                         user_id=user_id,
                         page_name=page_name,
                         host_url=request.host_url)

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'products_loaded': len(products),
        'active_users': len(user_contexts)
    }), 200

@app.route('/debug')
def debug():
    """Debug endpoint"""
    return jsonify({
        'products_count': len(products),
        'user_contexts_count': len(user_contexts),
        'product_mapping_keys': list(product_mapping.keys())[:10],
        'variants_dict_keys': list(variants_dict.keys())[:10]
    }), 200

if __name__ == '__main__':
    # Load products on startup
    load_products()
    
    # Schedule periodic reload (every 5 minutes)
    import threading
    def reload_products_periodically():
        while True:
            time.sleep(300)  # 5 minutes
            load_products()
    
    reload_thread = threading.Thread(target=reload_products_periodically, daemon=True)
    reload_thread.start()
    
    # Run Flask app
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port, debug=False)
