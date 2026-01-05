# test_fb_api.py
import os
import requests
import sys

# Thêm đường dẫn hiện tại để import app nếu cần
sys.path.append('.')

# Lấy token từ environment
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN", "")
PAGE_ID = "516937221685203"  # ID page của bạn
TEST_USER_ID = "26225402767048945"  # User ID từ log (Thuận Thái)

def test_facebook_api():
    """Kiểm tra Facebook API hoạt động"""
    
    print("=" * 50)
    print("🔍 KIỂM TRA FACEBOOK API")
    print("=" * 50)
    
    # 1. Kiểm tra token
    print(f"1. Kiểm tra PAGE_ACCESS_TOKEN...")
    print(f"   - Token có độ dài: {len(PAGE_ACCESS_TOKEN)} ký tự")
    print(f"   - 10 ký tự đầu: {PAGE_ACCESS_TOKEN[:10]}...")
    
    if not PAGE_ACCESS_TOKEN:
        print("   ❌ ERROR: Không có PAGE_ACCESS_TOKEN")
        return False
    
    # 2. Kiểm tra token hợp lệ
    print(f"\n2. Kiểm tra token hợp lệ với Facebook Graph API...")
    url = f"https://graph.facebook.com/v18.0/me?fields=id,name&access_token={PAGE_ACCESS_TOKEN}"
    
    try:
        response = requests.get(url, timeout=10)
        print(f"   - Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"   ✅ SUCCESS: Page: {data.get('name')}, ID: {data.get('id')}")
        else:
            error_data = response.json().get('error', {})
            print(f"   ❌ ERROR: {error_data.get('message')}")
            print(f"     Code: {error_data.get('code')}")
            print(f"     Type: {error_data.get('type')}")
            return False
    except Exception as e:
        print(f"   ❌ EXCEPTION: {e}")
        return False
    
    # 3. Kiểm tra quyền gửi tin nhắn
    print(f"\n3. Kiểm tra quyền gửi tin nhắn...")
    url = f"https://graph.facebook.com/v18.0/me/subscribed_apps?access_token={PAGE_ACCESS_TOKEN}"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get('data'):
                print(f"   ✅ SUCCESS: App đã được subscribe cho page")
            else:
                print(f"   ⚠️ WARNING: App chưa được subscribe")
        else:
            print(f"   ❌ ERROR: Không thể kiểm tra subscription")
    except Exception as e:
        print(f"   ⚠️ WARNING: Không kiểm tra được subscription: {e}")
    
    # 4. Test gửi tin nhắn đơn giản (text)
    print(f"\n4. Test gửi tin nhắn text đơn giản...")
    
    # Gửi cho chính page (hoặc user đã tương tác)
    recipient_id = PAGE_ID  # Gửi cho page (hoặc dùng TEST_USER_ID nếu muốn gửi cho user)
    
    payload = {
        "recipient": {"id": recipient_id},
        "message": {"text": "🔧 Test message từ script - Nếu nhận được là API hoạt động"}
    }
    
    url = f"https://graph.facebook.com/v18.0/me/messages?access_token={PAGE_ACCESS_TOKEN}"
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        print(f"   - Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            message_id = data.get('message_id', 'Unknown')
            recipient_id = data.get('recipient_id', 'Unknown')
            print(f"   ✅ SUCCESS: Đã gửi tin nhắn thành công!")
            print(f"     Message ID: {message_id}")
            print(f"     Recipient ID: {recipient_id}")
        else:
            error_data = response.json().get('error', {})
            print(f"   ❌ ERROR: {error_data.get('message')}")
            print(f"     Code: {error_data.get('code')}")
            print(f"     Type: {error_data.get('type')}")
            
            # Mã lỗi phổ biến
            error_codes = {
                100: "Invalid parameter",
                190: "Invalid OAuth access token",
                200: "Permissions error",
                210: "Cannot message this user (user hasn't interacted)",
                10: "Application request limit reached"
            }
            
            error_code = error_data.get('code')
            if error_code in error_codes:
                print(f"     Giải thích: {error_codes[error_code]}")
    
    except Exception as e:
        print(f"   ❌ EXCEPTION: {e}")
    
    print(f"\n" + "=" * 50)
    print("✅ KIỂM TRA HOÀN TẤT")
    print("=" * 50)
    
    return True

if __name__ == "__main__":
    test_facebook_api()
