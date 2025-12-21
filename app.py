import os
import json
import time
import requests
import pandas as pd
import numpy as np
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import openai
from sklearn.metrics.pairwise import cosine_similarity
from collections import OrderedDict
from datetime import datetime
import re

app = Flask(__name__)
CORS(app)

# ==================== CONFIGURATION ====================
PAGE_ACCESS_TOKEN = os.environ.get('PAGE_ACCESS_TOKEN')
FACEBOOK_API_URL = 'https://graph.facebook.com/v19.0/'
SHEET_URL = os.environ.get('SHEET_URL', 'https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')

# Initialize OpenAI
if OPENAI_API_KEY:
    openai.api_key = OPENAI_API_KEY

# ==================== GLOBAL VARIABLES ====================
products = []
variants_dict = {}
product_mapping = {}
product_embeddings = {}
product_texts = {}
user_contexts = {}
last_message_time = {}

# ==================== EVENT TRACKER (FIX DUPLICATE) ====================
class EventTracker:
    def __init__(self, max_size=2000, ttl=120):
        self.events = OrderedDict()
        self.ttl = ttl
        self.max_size = max_size
    
    def add(self, event_id):
        current_time = time.time()
        self.cleanup(current_time)
        self.events[event_id] = current_time
        
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
        
        to_remove = [eid for eid, ts in self.events.items() if current_time - ts > self.ttl]
        for eid in to_remove:
            del self.events[eid]

event_tracker = EventTracker()

# ==================== PRODUCT LOADING ====================
def load_products():
    """Load products from Google Sheets"""
    global products, variants_dict, product_mapping, product_embeddings, product_texts
    
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
                'description': group.iloc[0]['Mô tả'] if 'Mô tả' in group.columns else '',
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
                    'image_url': row['Link ảnh'] if 'Link ảnh' in row else '',
                    'stock': row['Tồn kho'] if 'Tồn kho' in row else ''
                }
                variants.append(variant)
            
            product_data['variants'] = variants
            products.append(product_data)
            
            # Store in variants dict
            variants_dict[product_num] = variants
            product_mapping[product_num] = product_data
        
        print(f"📦 Loaded {len(products)} products với {sum(len(v) for v in variants_dict.values())} variants.")
        
        # Create text embeddings for search
        create_text_embeddings()
        
    except Exception as e:
        print(f"❌ Error loading products: {e}")
        import traceback
        traceback.print_exc()

def create_text_embeddings():
    """Create text embeddings for semantic search"""
    global product_embeddings, product_texts
    
    if not OPENAI_API_KEY:
        print("⚠️ OpenAI API key not set, using simple text search")
        return
    
    try:
        for product in products:
            # Create searchable text
            text_parts = [
                product['ms'],
                product['name'],
                product['category'],
                product.get('description', '')
            ]
            
            # Add variants info
            for variant in product['variants'][:5]:  # Limit to first 5 variants
                text_parts.append(variant.get('color', ''))
                text_parts.append(variant.get('size', ''))
                text_parts.append(variant.get('weight_range', ''))
            
            search_text = " ".join(filter(None, text_parts))
            product_texts[product['ms']] = search_text
        
        print(f"🔤 Created text for {len(product_texts)} products")
        
        # Generate embeddings in batches
        batch_size = 20
        product_ms_list = list(product_texts.keys())
        
        for i in range(0, len(product_ms_list), batch_size):
            batch_ms = product_ms_list[i:i+batch_size]
            batch_texts = [product_texts[ms] for ms in batch_ms]
            
            try:
                response = openai.Embedding.create(
                    model="text-embedding-ada-002",
                    input=batch_texts
                )
                
                for idx, ms in enumerate(batch_ms):
                    product_embeddings[ms] = np.array(response['data'][idx]['embedding'])
                
                print(f"✓ Generated embeddings for batch {i//batch_size + 1}")
                
            except Exception as e:
                print(f"❌ Error generating embeddings: {e}")
                # Fallback to simple search
                break
        
        print(f"✅ Created embeddings for {len(product_embeddings)} products")
        
    except Exception as e:
        print(f"❌ Error in create_text_embeddings: {e}")

# ==================== AI/ML FUNCTIONS ====================
def get_text_embedding(text):
    """Get embedding for a text"""
    if not OPENAI_API_KEY:
        return None
    
    try:
        response = openai.Embedding.create(
            model="text-embedding-ada-002",
            input=text
        )
        return np.array(response['data'][0]['embedding'])
    except Exception as e:
        print(f"❌ Error getting embedding: {e}")
        return None

def find_similar_products(query, top_k=5):
    """Find similar products using semantic search"""
    if not product_embeddings:
        return simple_text_search(query, top_k)
    
    query_embedding = get_text_embedding(query)
    if query_embedding is None:
        return simple_text_search(query, top_k)
    
    similarities = []
    for ms, embedding in product_embeddings.items():
        if embedding is not None:
            similarity = cosine_similarity([query_embedding], [embedding])[0][0]
            similarities.append((ms, similarity))
    
    # Sort by similarity
    similarities.sort(key=lambda x: x[1], reverse=True)
    
    # Return top_k products
    results = []
    for ms, score in similarities[:top_k]:
        if ms in product_mapping:
            results.append({
                'product': product_mapping[ms],
                'score': float(score)
            })
    
    return results

def simple_text_search(query, top_k=5):
    """Simple text-based search as fallback"""
    query_lower = query.lower()
    results = []
    
    for product in products:
        score = 0
        
        # Check product code
        if product['ms'].lower() in query_lower:
            score += 3
        
        # Check product name
        if product['name'].lower() in query_lower:
            score += 2
        
        # Check category
        if product['category'].lower() in query_lower:
            score += 1
        
        # Check variants
        for variant in product['variants'][:3]:
            if variant.get('color', '').lower() in query_lower:
                score += 1
            if variant.get('size', '').lower() in query_lower:
                score += 1
        
        if score > 0:
            results.append({
                'product': product,
                'score': score
            })
    
    # Sort by score
    results.sort(key=lambda x: x['score'], reverse=True)
    return results[:top_k]

def search_products_by_text(query, top_k=5):
    """Search products by text query"""
    # First try semantic search
    similar_products = find_similar_products(query, top_k)
    
    if similar_products:
        return similar_products
    
    # Fallback to simple search
    return simple_text_search(query, top_k)

def get_product_advice(ms_code):
    """Get AI-generated advice for a product"""
    if not OPENAI_API_KEY or ms_code not in product_mapping:
        return None
    
    product = product_mapping[ms_code]
    
    prompt = f"""
    Sản phẩm: {product['name']}
    Mã: {product['ms']}
    Danh mục: {product['category']}
    Mô tả: {product.get('description', '')}
    
    Các biến thể:
    {json.dumps(product['variants'][:3], indent=2, ensure_ascii=False)}
    
    Hãy tạo một lời khuyên hữu ích cho khách hàng về sản phẩm này bằng tiếng Việt:
    1. Đặc điểm nổi bật
    2. Ai nên mua sản phẩm này
    3. Tips phối đồ (nếu có)
    
    Giữ ngắn gọn, thân thiện, và hấp dẫn.
    """
    
    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "Bạn là một chuyên gia thời trang thân thiện, nhiệt tình."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=300
        )
        
        return response.choices[0].message.content.strip()
    
    except Exception as e:
        print(f"❌ Error getting product advice: {e}")
        return None

# ==================== LANGUAGE FUNCTIONS ====================
def detect_language(text):
    """Simple language detection"""
    # Check for Vietnamese characters
    vietnamese_chars = set('àáảãạăắằẳẵặâấầẩẫậèéẻẽẹêếềểễệìíỉĩịòóỏõọôốồổỗộơớờởỡợùúủũụưứừửữựỳýỷỹỵđ')
    
    vietnamese_count = sum(1 for char in text.lower() if char in vietnamese_chars)
    english_words = set(text.lower().split())
    
    if vietnamese_count > 0 or any(word in ['xin', 'chào', 'cảm', 'ơn', 'giá', 'bao', 'nhiêu'] for word in english_words):
        return 'vi'
    return 'en'

def translate_to_vietnamese(text):
    """Translate text to Vietnamese"""
    if not OPENAI_API_KEY:
        return text
    
    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "Bạn là một phiên dịch viên chuyên nghiệp."},
                {"role": "user", "content": f"Dịch sang tiếng Việt: {text}"}
            ],
            temperature=0.3,
            max_tokens=200
        )
        
        return response.choices[0].message.content.strip()
    
    except Exception as e:
        print(f"❌ Translation error: {e}")
        return text

# ==================== FACEBOOK API FUNCTIONS ====================
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
        return None

def send_image_url(recipient_id, image_url):
    """Send image from URL"""
    attachment = {
        'type': 'image',
        'payload': {'url': image_url, 'is_reusable': True}
    }
    return send_message(recipient_id, "", attachment=attachment)

def send_carousel(recipient_id, products_list):
    """Send carousel with multiple products"""
    elements = []
    
    for product in products_list[:10]:  # Facebook limits to 10 elements
        # Get first variant with image
        variant_with_image = next((v for v in product['variants'] if v.get('image_url', '').startswith('http')), None)
        
        if not variant_with_image:
            continue
        
        element = {
            'title': f"[{product['ms']}] {product['name'][:60]}",
            'subtitle': f"💸 Giá từ: {min(v.get('price', 'N/A') for v in product['variants'] if v.get('price'))} | 📦 {len(product['variants'])} biến thể",
            'image_url': variant_with_image['image_url'],
            'default_action': {
                'type': 'web_url',
                'url': f"{request.host_url}order-form?ms={product['ms']}&uid={recipient_id}",
                'webview_height_ratio': 'tall'
            },
            'buttons': [
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
        }
        
        elements.append(element)
    
    if not elements:
        # Fallback to text message
        product_list_text = "\n".join([f"• {p['ms']} - {p['name']}" for p in products_list[:5]])
        send_message(recipient_id, f"📦 Các sản phẩm tìm thấy:\n{product_list_text}")
        return
    
    attachment = {
        'type': 'template',
        'payload': {
            'template_type': 'generic',
            'elements': elements
        }
    }
    
    send_message(recipient_id, "", attachment=attachment)

def send_single_carousel(recipient_id, product):
    """Send carousel for a single product (multiple variants)"""
    send_carousel(recipient_id, [product])

def send_product_details(recipient_id, ms_code):
    """Send detailed product information"""
    if ms_code not in product_mapping:
        send_message(recipient_id, f"❌ Không tìm thấy sản phẩm với mã {ms_code}")
        return
    
    product = product_mapping[ms_code]
    
    # Send product name
    send_message(recipient_id, f"📌 [{product['ms']}] {product['name']}")
    
    # Send images (first 3 variants with images)
    variants_with_images = [v for v in product['variants'] if v.get('image_url', '').startswith('http')][:3]
    for variant in variants_with_images:
        send_image_url(recipient_id, variant['image_url'])
    
    # Send AI-generated advice if available
    advice = get_product_advice(ms_code)
    if advice:
        send_message(recipient_id, f"💡 **Lời khuyên từ chuyên gia:**\n{advice}")
    else:
        # Fallback description
        description = product.get('description', '')
        if description:
            send_message(recipient_id, f"📝 MÔ TẢ:\n{description}")
    
    # Send price list
    price_list = "💰 GIÁ SẢN PHẨM:\n"
    unique_prices = set()
    for variant in product['variants'][:5]:
        color = variant.get('color', '')
        size = variant.get('size', '')
        weight = variant.get('weight_range', '')
        price = variant.get('price', 'N/A')
        price_line = f"• {color} - {size}"
        if weight:
            price_line += f" ({weight})"
        price_line += f": {price}"
        price_list += price_line + "\n"
        unique_prices.add(price)
    
    if len(product['variants']) > 5:
        price_list += f"📊 ... và {len(product['variants']) - 5} phân loại khác\n"
    
    if len(unique_prices) == 1:
        price_list = f"💰 GIÁ: {list(unique_prices)[0]} (tất cả phân loại)"
    
    send_message(recipient_id, price_list)
    
    # Send order link
    order_url = f"{request.host_url}order-form?ms={ms_code}&uid={recipient_id}"
    send_message(recipient_id, f"📋 Đặt hàng ngay tại đây:\n{order_url}")

# ==================== MESSAGE PROCESSING ====================
def process_message(event):
    """Process incoming message with AI understanding"""
    sender_id = event['sender']['id']
    message_text = event['message'].get('text', '').strip()
    
    # Rate limiting check
    current_time = time.time()
    if sender_id in last_message_time:
        time_diff = current_time - last_message_time[sender_id]
        if time_diff < 1:  # 1 second minimum between messages
            print(f"[RATE LIMIT] Skipping message from {sender_id}, too fast")
            return
    
    last_message_time[sender_id] = current_time
    
    # Initialize user context
    if sender_id not in user_contexts:
        user_contexts[sender_id] = {
            'last_ms': '',
            'history': [],
            'created_at': datetime.now().isoformat(),
            'language': 'vi'
        }
    
    context = user_contexts[sender_id]
    
    # Detect language
    lang = detect_language(message_text)
    context['language'] = lang
    
    # Check if first message after referral
    has_history = len(context['history']) > 0
    
    if has_history and context['last_ms']:
        print(f"[FIRST MESSAGE] User {sender_id} gửi tin nhắn đầu tiên sau referral")
        
        if context['last_ms'] in product_mapping:
            send_single_carousel(sender_id, product_mapping[context['last_ms']])
            if context['last_ms'] not in context['history']:
                context['history'].append(context['last_ms'])
            
            print(f"[FIRST MESSAGE DONE] Đã xử lý tin nhắn đầu tiên")
        else:
            send_message(sender_id, "Xin lỗi, không tìm thấy thông tin sản phẩm.")
        
        return
    
    # Process normal message with AI
    message_lower = message_text.lower()
    
    # Extract product code if present
    product_code_match = re.search(r'ms\d{6}', message_lower)
    if product_code_match:
        ms_code = product_code_match.group(0).upper()
        if ms_code in product_mapping:
            context['last_ms'] = ms_code
            if ms_code not in context['history']:
                context['history'].append(ms_code)
            
            send_product_details(sender_id, ms_code)
            return
    
    # Check for specific intents
    if any(word in message_lower for word in ['giá', 'bao nhiêu tiền', 'cost', 'price']):
        if context['last_ms']:
            send_product_details(sender_id, context['last_ms'])
        else:
            send_message(sender_id, "Bạn muốn xem giá của sản phẩm nào ạ? Vui lòng cho mình biết mã sản phẩm hoặc mô tả sản phẩm.")
        return
    
    elif any(word in message_lower for word in ['sản phẩm', 'hàng', 'đồ', 'product', 'item']):
        # Search for products
        search_results = search_products_by_text(message_text, top_k=3)
        if search_results:
            products_to_show = [r['product'] for r in search_results]
            send_carousel(sender_id, products_to_show)
            
            # Update context with first product
            if products_to_show:
                first_ms = products_to_show[0]['ms']
                context['last_ms'] = first_ms
                if first_ms not in context['history']:
                    context['history'].append(first_ms)
        else:
            send_message(sender_id, "Xin lỗi, mình không tìm thấy sản phẩm phù hợp. Bạn có thể mô tả rõ hơn được không ạ?")
        return
    
    elif any(word in message_lower for word in ['chào', 'hello', 'hi', 'xin chào']):
        greeting = "Chào bạn! Mình là trợ lý ảo của shop. Mình có thể giúp bạn:\n• Tìm sản phẩm theo mô tả\n• Xem giá và chi tiết sản phẩm\n• Đặt hàng trực tiếp"
        send_message(sender_id, greeting)
        return
    
    elif any(word in message_lower for word in ['cảm ơn', 'thanks', 'thank you']):
        send_message(sender_id, "Không có gì ạ! Nếu bạn cần thêm thông tin gì, cứ hỏi mình nhé! 😊")
        return
    
    else:
        # AI-powered search for other queries
        search_results = search_products_by_text(message_text, top_k=3)
        if search_results:
            products_to_show = [r['product'] for r in search_results]
            send_carousel(sender_id, products_to_show)
            
            if products_to_show:
                first_ms = products_to_show[0]['ms']
                context['last_ms'] = first_ms
                if first_ms not in context['history']:
                    context['history'].append(first_ms)
        else:
            # If no products found, give generic response
            response = "Mình hiểu bạn đang tìm sản phẩm. Bạn có thể:\n1. Nhập mã sản phẩm (ví dụ: MS000016)\n2. Mô tả sản phẩm bạn cần\n3. Gõ 'sản phẩm' để xem các sản phẩm nổi bật"
            send_message(sender_id, response)

def process_postback(event):
    """Process postback event"""
    sender_id = event['sender']['id']
    payload = event['postback']['payload']
    timestamp = event.get('timestamp', '')
    
    # Create unique event ID
    event_id = f"pb_{sender_id}_{payload}_{timestamp}"
    
    # Check for duplicate
    if event_tracker.contains(event_id):
        print(f"[DUPLICATE] Bỏ qua postback đã xử lý: {event_id}")
        return
    
    # Mark as processed
    event_tracker.add(event_id)
    
    print(f"[POSTBACK] User {sender_id}: {payload}")
    
    # Initialize user context
    if sender_id not in user_contexts:
        user_contexts[sender_id] = {
            'last_ms': '',
            'history': [],
            'created_at': datetime.now().isoformat(),
            'language': 'vi'
        }
    
    context = user_contexts[sender_id]
    
    # Handle ADVICE postback
    if payload.startswith('ADVICE_'):
        ms_code = payload.replace('ADVICE_', '')
        
        # Update context
        context['last_ms'] = ms_code
        if ms_code not in context['history']:
            context['history'].append(ms_code)
        
        print(f"[CONTEXT UPDATE] User {sender_id}: last_ms={ms_code}, history={context['history']}")
        
        # Send product details
        send_product_details(sender_id, ms_code)
        return
    
    # Handle other postbacks
    if payload == 'GET_STARTED':
        send_message(sender_id, "Chào mừng bạn đến với shop! Mình có thể giúp bạn tìm sản phẩm ưng ý nhất. 😊")
    
    elif payload == 'VIEW_PRODUCTS':
        # Show some featured products
        featured_products = list(product_mapping.values())[:5]
        send_carousel(sender_id, featured_products)

# ==================== WEBHOOK ENDPOINTS ====================
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
    """Main webhook endpoint"""
    data = request.get_json()
    
    if not data:
        print("❌ No data received")
        return jsonify({'status': 'no data'}), 200
    
    # Log first 200 chars
    data_str = json.dumps(data)
    print(f"Webhook received: {data_str[:200]}...")
    
    # Handle page feed updates (comments)
    if 'entry' in data and 'changes' in data.get('entry', [{}])[0]:
        for entry in data['entry']:
            if 'changes' in entry:
                for change in entry['changes']:
                    if change.get('field') == 'feed':
                        value = change.get('value', {})
                        
                        if value.get('item') == 'comment' and value.get('verb') == 'add':
                            from_user = value.get('from', {})
                            comment_message = value.get('message', '').lower()
                            user_id = from_user.get('id')
                            
                            if user_id:
                                print(f"[ECHO USER] Đang xử lý echo từ bình luận người dùng")
                                
                                # Look for product codes
                                for product in products:
                                    if product['ms'].lower() in comment_message:
                                        # Update context
                                        if user_id not in user_contexts:
                                            user_contexts[user_id] = {
                                                'last_ms': product['ms'],
                                                'history': [product['ms']],
                                                'created_at': datetime.now().isoformat(),
                                                'language': 'vi'
                                            }
                                        else:
                                            user_contexts[user_id]['last_ms'] = product['ms']
                                            if product['ms'] not in user_contexts[user_id]['history']:
                                                user_contexts[user_id]['history'].append(product['ms'])
                                        
                                        print(f"[ECHO FCHAT] Phát hiện mã sản phẩm: {product['ms']} cho user: {user_id}")
                                        print(f"[CONTEXT UPDATED] Đã ghi nhận mã {product['ms']} vào ngữ cảnh")
                                        break
    
    # Handle messaging events
    elif 'entry' in data and 'messaging' in data['entry'][0]:
        for entry in data['entry']:
            for messaging_event in entry.get('messaging', []):
                # Skip echo messages from bot
                if messaging_event.get('message', {}).get('is_echo'):
                    print("[ECHO BOT] Bỏ qua echo message từ bot")
                    continue
                
                # Skip delivery/read receipts
                if 'delivery' in messaging_event or 'read' in messaging_event:
                    print(f"[SKIP] Bỏ qua delivery/read event")
                    continue
                
                # Create unique event ID
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
                
                # Check for duplicate
                if event_id and event_tracker.contains(event_id):
                    print(f"[DUPLICATE] Bỏ qua event đã xử lý: {event_id}")
                    continue
                
                # Mark as processed
                if event_id:
                    event_tracker.add(event_id)
                
                # Process event
                if 'message' in messaging_event:
                    process_message(messaging_event)
                elif 'postback' in messaging_event:
                    process_postback(messaging_event)
    
    return jsonify({'status': 'ok'}), 200

# ==================== ADDITIONAL API ENDPOINTS ====================
@app.route('/order-form')
def order_form():
    """Render order form"""
    ms_code = request.args.get('ms', '')
    user_id = request.args.get('uid', '')
    
    product = None
    if ms_code in product_mapping:
        product = product_mapping[ms_code]
    
    # Get page name
    page_name = "Shinwon Fashion"
    try:
        if PAGE_ACCESS_TOKEN:
            url = f"{FACEBOOK_API_URL}me"
            params = {'access_token': PAGE_ACCESS_TOKEN, 'fields': 'name'}
            response = requests.get(url, params=params, timeout=5)
            if response.status_code == 200:
                page_name = response.json().get('name', 'Shop')
    except:
        pass
    
    return render_template('order_form.html', 
                         product=product, 
                         user_id=user_id,
                         page_name=page_name,
                         host_url=request.host_url)

@app.route('/search', methods=['GET'])
def search_api():
    """API endpoint for product search"""
    query = request.args.get('q', '')
    top_k = int(request.args.get('top_k', 5))
    
    if not query:
        return jsonify({'error': 'Query parameter required'}), 400
    
    results = search_products_by_text(query, top_k)
    
    # Format results
    formatted_results = []
    for result in results:
        product = result['product']
        formatted_results.append({
            'ms': product['ms'],
            'name': product['name'],
            'category': product['category'],
            'variants_count': len(product['variants']),
            'score': result['score'],
            'min_price': min([v.get('price', 0) for v in product['variants'] if v.get('price', '').isdigit()], default=0)
        })
    
    return jsonify({
        'query': query,
        'count': len(formatted_results),
        'results': formatted_results
    })

@app.route('/similar/<ms_code>', methods=['GET'])
def similar_api(ms_code):
    """API endpoint for similar products"""
    top_k = int(request.args.get('top_k', 3))
    
    if ms_code not in product_mapping:
        return jsonify({'error': 'Product not found'}), 404
    
    product = product_mapping[ms_code]
    query_text = f"{product['name']} {product['category']}"
    
    results = find_similar_products(query_text, top_k + 1)
    
    # Filter out the same product
    similar_results = [r for r in results if r['product']['ms'] != ms_code][:top_k]
    
    formatted_results = []
    for result in similar_results:
        p = result['product']
        formatted_results.append({
            'ms': p['ms'],
            'name': p['name'],
            'category': p['category'],
            'similarity': result['score']
        })
    
    return jsonify({
        'original': ms_code,
        'count': len(formatted_results),
        'similar': formatted_results
    })

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'products_loaded': len(products),
        'embeddings_loaded': len(product_embeddings),
        'active_users': len(user_contexts),
        'event_tracker_size': len(event_tracker.events)
    }), 200

@app.route('/debug')
def debug():
    """Debug endpoint"""
    return jsonify({
        'products_count': len(products),
        'product_codes': list(product_mapping.keys())[:10],
        'user_contexts_count': len(user_contexts),
        'openai_available': bool(OPENAI_API_KEY),
        'embeddings_count': len(product_embeddings)
    }), 200

# ==================== MAIN ====================
if __name__ == '__main__':
    # Load products on startup
    load_products()
    
    # Schedule periodic reload
    import threading
    def reload_products_periodically():
        while True:
            time.sleep(300)  # 5 minutes
            print("🔄 Reloading products...")
            load_products()
    
    reload_thread = threading.Thread(target=reload_products_periodically, daemon=True)
    reload_thread.start()
    
    # Run Flask app
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port, debug=False)
