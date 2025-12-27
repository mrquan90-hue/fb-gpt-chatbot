import os
import json
import logging
from flask import Flask, request, jsonify
from datetime import datetime

# ============================================
# CẤU HÌNH LOGGING CHI TIẾT
# ============================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('webhook_debug.log')
    ]
)
logger = logging.getLogger(__name__)

# ============================================
# FLASK APP
# ============================================
app = Flask(__name__)

# ============================================
# BIẾN MÔI TRƯỜNG
# ============================================
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "").strip()
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN", "").strip()
PAGE_ID = os.getenv("PAGE_ID", "").strip()

# ============================================
# HÀM LOG CHI TIẾT
# ============================================
def log_webhook_data(data, event_type="webhook"):
    """Log chi tiết dữ liệu webhook nhận được"""
    try:
        logger.info(f"=== {event_type.upper()} DATA RECEIVED ===")
        logger.info(f"Timestamp: {datetime.now().isoformat()}")
        logger.info(f"Data type: {type(data)}")
        logger.info(f"Data length: {len(str(data)) if data else 0}")
        
        if data:
            # Log cấu trúc tổng quan
            logger.info(f"Top-level keys: {list(data.keys())}")
            
            # Log entries
            entries = data.get('entry', [])
            logger.info(f"Number of entries: {len(entries)}")
            
            for i, entry in enumerate(entries):
                logger.info(f"\n--- Entry {i+1} ---")
                logger.info(f"Entry ID: {entry.get('id', 'N/A')}")
                logger.info(f"Entry time: {entry.get('time', 'N/A')}")
                
                # Messaging events
                messaging_events = entry.get('messaging', [])
                logger.info(f"Number of messaging events: {len(messaging_events)}")
                
                for j, event in enumerate(messaging_events):
                    logger.info(f"\n  Messaging Event {j+1}:")
                    logger.info(f"  Sender ID: {event.get('sender', {}).get('id', 'N/A')}")
                    logger.info(f"  Recipient ID: {event.get('recipient', {}).get('id', 'N/A')}")
                    logger.info(f"  Timestamp: {event.get('timestamp', 'N/A')}")
                    
                    if 'message' in event:
                        logger.info("  Type: MESSAGE")
                        msg = event['message']
                        logger.info(f"    Message ID: {msg.get('mid', 'N/A')}")
                        logger.info(f"    Text: {msg.get('text', 'N/A')[:100]}...")
                        logger.info(f"    Is echo: {msg.get('is_echo', 'N/A')}")
                        logger.info(f"    App ID: {msg.get('app_id', 'N/A')}")
                        
                        if 'attachments' in msg:
                            logger.info(f"    Attachments: {len(msg['attachments'])}")
                    
                    elif 'postback' in event:
                        logger.info("  Type: POSTBACK")
                        logger.info(f"    Payload: {event['postback'].get('payload', 'N/A')}")
                    
                    elif 'referral' in event:
                        logger.info("  Type: REFERRAL")
                        referral = event['referral']
                        logger.info(f"    Source: {referral.get('source', 'N/A')}")
                        logger.info(f"    Ref: {referral.get('ref', 'N/A')}")
                    
                    elif 'read' in event:
                        logger.info("  Type: READ")
                    
                    elif 'delivery' in event:
                        logger.info("  Type: DELIVERY")
                    
                    elif 'order' in event:
                        logger.info("  Type: ORDER")
                        logger.info(f"    Order details: {json.dumps(event['order'], indent=2)}")
                
                # Changes events (cho feed)
                changes = entry.get('changes', [])
                logger.info(f"Number of changes events: {len(changes)}")
                
                for k, change in enumerate(changes):
                    logger.info(f"\n  Change Event {k+1}:")
                    logger.info(f"    Field: {change.get('field', 'N/A')}")
                    value = change.get('value', {})
                    
                    if change.get('field') == 'feed':
                        logger.info("    Type: FEED")
                        logger.info(f"      Post ID: {value.get('post_id', 'N/A')}")
                        logger.info(f"      Sender ID: {value.get('from', {}).get('id', 'N/A')}")
                        logger.info(f"      Sender Name: {value.get('from', {}).get('name', 'N/A')}")
                        logger.info(f"      Message: {value.get('message', 'N/A')[:100]}...")
                        logger.info(f"      Item: {value.get('item', 'N/A')}")
                        logger.info(f"      Verb: {value.get('verb', 'N/A')}")
                        logger.info(f"      Parent ID: {value.get('parent_id', 'N/A')}")
                        logger.info(f"      Comment ID: {value.get('comment_id', 'N/A')}")
            
            logger.info("=" * 50)
    
    except Exception as e:
        logger.error(f"Error logging webhook data: {e}")

# ============================================
# ENDPOINTS
# ============================================
@app.route("/", methods=["GET"])
def home():
    """Trang chủ kiểm tra"""
    return jsonify({
        "status": "online",
        "service": "Facebook Webhook Debug",
        "timestamp": datetime.now().isoformat(),
        "page_id": PAGE_ID,
        "verify_token_configured": bool(VERIFY_TOKEN),
        "page_access_token_configured": bool(PAGE_ACCESS_TOKEN),
        "endpoints": {
            "GET /": "This page",
            "GET /health": "Health check",
            "GET /webhook": "Facebook webhook verification",
            "POST /webhook": "Facebook webhook events",
            "GET /debug/env": "Check environment variables",
            "GET /debug/log": "View recent logs"
        }
    }), 200

@app.route("/health", methods=["GET"])
def health_check():
    """Health check đơn giản"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "message": "Webhook debug service is running"
    }), 200

@app.route("/debug/env", methods=["GET"])
def debug_env():
    """Kiểm tra biến môi trường (ẩn giá trị nhạy cảm)"""
    return jsonify({
        "PAGE_ID": PAGE_ID,
        "VERIFY_TOKEN_SET": "YES" if VERIFY_TOKEN else "NO",
        "VERIFY_TOKEN_LENGTH": len(VERIFY_TOKEN) if VERIFY_TOKEN else 0,
        "PAGE_ACCESS_TOKEN_SET": "YES" if PAGE_ACCESS_TOKEN else "NO",
        "PAGE_ACCESS_TOKEN_LENGTH": len(PAGE_ACCESS_TOKEN) if PAGE_ACCESS_TOKEN else 0,
        "environment_keys": list(os.environ.keys())
    }), 200

@app.route("/debug/log", methods=["GET"])
def debug_log():
    """Xem log gần đây"""
    try:
        with open('webhook_debug.log', 'r') as f:
            lines = f.readlines()
            recent_lines = lines[-100:]  # Lấy 100 dòng cuối
        return jsonify({
            "log_entries": recent_lines,
            "total_lines": len(lines)
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/webhook", methods=["GET"])
def verify_webhook():
    """Xác minh webhook với Facebook"""
    logger.info("=== WEBHOOK VERIFICATION REQUEST ===")
    
    # Log tất cả tham số
    logger.info(f"Request args: {dict(request.args)}")
    
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")
    
    logger.info(f"Mode: {mode}")
    logger.info(f"Token: {token}")
    logger.info(f"Expected token: {VERIFY_TOKEN}")
    logger.info(f"Challenge: {challenge}")
    
    if mode == "subscribe" and token == VERIFY_TOKEN:
        logger.info("✅ Webhook verified successfully!")
        return challenge, 200
    else:
        logger.error("❌ Webhook verification failed!")
        return jsonify({
            "error": "Verification failed",
            "mode": mode,
            "token_received": token,
            "token_expected": VERIFY_TOKEN
        }), 403

@app.route("/webhook", methods=["POST"])
def webhook():
    """Nhận sự kiện từ Facebook webhook"""
    try:
        # Log headers
        headers = {k: v for k, v in request.headers.items()}
        logger.info("=== WEBHOOK POST REQUEST ===")
        logger.info(f"Headers: {json.dumps(headers, indent=2)}")
        logger.info(f"Content-Type: {request.content_type}")
        logger.info(f"Content-Length: {request.content_length}")
        
        # Nhận dữ liệu
        data = request.get_json()
        
        if not data:
            # Thử đọc raw data nếu không parse được JSON
            raw_data = request.get_data(as_text=True)
            logger.warning(f"No JSON data, raw data: {raw_data[:500]}...")
            return jsonify({"status": "no_data"}), 200
        
        # Log dữ liệu chi tiết
        log_webhook_data(data, "webhook_post")
        
        # Xử lý đơn giản: chỉ log, không xử lý gì cả
        logger.info("✅ Webhook received and logged successfully")
        
        return jsonify({"status": "ok"}), 200
        
    except Exception as e:
        logger.error(f"❌ Error processing webhook: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

# ============================================
# THỬ NGHIỆM: ENDPOINT CHO FEED COMMENT RIÊNG
# ============================================
@app.route("/test-feed", methods=["POST"])
def test_feed():
    """Endpoint riêng để test feed comment"""
    try:
        logger.info("=== TEST FEED ENDPOINT ===")
        
        data = request.get_json()
        if not data:
            return jsonify({"status": "no_data"}), 200
        
        logger.info(f"Test feed data: {json.dumps(data, indent=2)}")
        
        # Kiểm tra cấu trúc feed comment
        entries = data.get('entry', [])
        for entry in entries:
            changes = entry.get('changes', [])
            for change in changes:
                if change.get('field') == 'feed':
                    value = change.get('value', {})
                    logger.info("🎯 FEED COMMENT DETECTED!")
                    logger.info(f"Post ID: {value.get('post_id')}")
                    logger.info(f"From: {value.get('from', {}).get('name')} (ID: {value.get('from', {}).get('id')})")
                    logger.info(f"Message: {value.get('message')}")
                    logger.info(f"Verb: {value.get('verb')}")
                    logger.info(f"Item: {value.get('item')}")
        
        return jsonify({"status": "test_feed_received"}), 200
        
    except Exception as e:
        logger.error(f"Error in test feed: {e}")
        return jsonify({"error": str(e)}), 500

# ============================================
# SIMULATE FACEBOOK WEBHOOK
# ============================================
@app.route("/simulate-webhook", methods=["POST"])
def simulate_webhook():
    """Tạo webhook mẫu để test"""
    sample_data = {
        "object": "page",
        "entry": [
            {
                "id": PAGE_ID or "123456789",
                "time": 1678888888,
                "changes": [
                    {
                        "field": "feed",
                        "value": {
                            "from": {
                                "id": "987654321",
                                "name": "Test User"
                            },
                            "post_id": f"{PAGE_ID}_123456789",
                            "message": "Sản phẩm này có mã MS000034 không shop?",
                            "item": "comment",
                            "verb": "add",
                            "parent_id": f"{PAGE_ID}_123456789",
                            "comment_id": "123456789012345"
                        }
                    }
                ]
            }
        ]
    }
    
    logger.info("=== SIMULATED WEBHOOK SENT ===")
    logger.info(f"Sample data: {json.dumps(sample_data, indent=2)}")
    
    return jsonify({
        "status": "simulated",
        "data": sample_data
    }), 200

# ============================================
# CẤU HÌNH PORT CHO KOYEB
# ============================================
def get_port():
    """Lấy port từ biến môi trường"""
    return int(os.environ.get("PORT", 5000))

# ============================================
# KHỞI ĐỘNG
# ============================================
if __name__ == "__main__":
    port = get_port()
    
    print("=" * 60)
    print("🔧 FACEBOOK WEBHOOK DEBUG SERVICE")
    print("=" * 60)
    print(f"📡 Port: {port}")
    print(f"🔑 Verify Token: {'SET' if VERIFY_TOKEN else 'NOT SET'}")
    print(f"📄 Page ID: {PAGE_ID}")
    print(f"🔗 Webhook URL: https://[your-domain]/webhook")
    print("=" * 60)
    print("📝 Logs will be saved to: webhook_debug.log")
    print("=" * 60)
    
    app.run(host="0.0.0.0", port=port, debug=False)
