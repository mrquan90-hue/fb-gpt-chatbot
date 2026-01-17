import os
import json
import re
import time
import csv
import hashlib
import base64
import threading
import functools
import schedule
import atexit
from collections import defaultdict
from urllib.parse import quote, urlencode
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from io import BytesIO
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

import requests
from flask import Flask, request, send_from_directory, jsonify, render_template_string, make_response
from openai import OpenAI

# ============================================
# FLASK APP
# ============================================
app = Flask(__name__)

# Middleware để đảm bảo workers được khởi động
@app.before_request
def ensure_workers_initialized():
    """Đảm bảo workers được khởi động - TỐI ƯU CHO KOYEB"""
    global WORKERS_INITIALIZED
    
    if WORKERS_INITIALIZED:
        return None
    
    print(f"[FIRST REQUEST] Khởi động workers nhanh...")
    
    # Khởi động workers ngay lập tức
    initialize_workers_once()
    
    # Load products nếu chưa có
    if not PRODUCTS:
        print(f"[FIRST REQUEST] Đang load products nhanh...")
        threading.Thread(target=load_products, args=(True,), daemon=True).start()
    
    return None
    
# ============================================
# ENV & CONFIG - THÊM POSCAKE, PAGE_ID VÀ FACEBOOK CAPI
# ============================================
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN", "").strip()
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
GOOGLE_SHEET_CSV_URL = os.getenv("SHEET_CSV_URL", "").strip()
DOMAIN = os.getenv("DOMAIN", "").strip() or "fb-gpt-chatbot.onrender.com"
FANPAGE_NAME = os.getenv("FANPAGE_NAME", "shop")
FCHAT_WEBHOOK_URL = os.getenv("FCHAT_WEBHOOK_URL", "").strip()
FCHAT_TOKEN = os.getenv("FCHAT_TOKEN", "").strip()

# Cấu hình Poscake Webhook
POSCAKE_API_KEY = os.getenv("POSCAKE_API_KEY", "").strip()
POSCAKE_WEBHOOK_SECRET = os.getenv("POSCAKE_WEBHOOK_SECRET", "").strip()
POSCAKE_STORE_ID = os.getenv("POSCAKE_STORE_ID", "").strip()

# Page ID để xác định comment từ page
PAGE_ID = os.getenv("PAGE_ID", "").strip()

# Facebook Conversion API Configuration
FACEBOOK_PIXEL_ID = os.getenv("FACEBOOK_PIXEL_ID", "").strip()
FACEBOOK_ACCESS_TOKEN = os.getenv("FACEBOOK_ACCESS_TOKEN", "").strip()
FACEBOOK_API_VERSION = os.getenv("FACEBOOK_API_VERSION", "v18.0").strip()

# Thêm biến cho tính năng trả lời bình luận
ENABLE_COMMENT_REPLY = os.getenv("ENABLE_COMMENT_REPLY", "true").lower() == "true"
WEBSITE_URL = os.getenv("WEBSITE_URL", "").strip()  # Link website từ Google Sheet

# ============================================
# GOOGLE SHEETS API CONFIGURATION
# ============================================
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
GOOGLE_SHEETS_CREDENTIALS_JSON = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON", "").strip()

if not GOOGLE_SHEET_CSV_URL:
    GOOGLE_SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/18eI8Yn-WG8xN0YK8mWqgIOvn-USBhmXBH3sR2drvWus/export?format=csv"

# Tên sheet cho user context trong Google Sheets
USER_CONTEXT_SHEET_NAME = "UserContext"

# ============================================
# APP ID CỦA BOT
# ============================================
BOT_APP_IDS = {"645956568292435"}

# ============================================
# FACEBOOK EVENT QUEUE FOR ASYNC PROCESSING
# ============================================
from queue import Queue

# Queue cho sự kiện Facebook CAPI
FACEBOOK_EVENT_QUEUE = Queue()
FACEBOOK_WORKER_RUNNING = False

# ============================================
# KOYEB FREE TIER SETTINGS - THÊM PHẦN NÀY
# ============================================
KOYEB_KEEP_ALIVE_ENABLED = os.getenv("KOYEB_KEEP_ALIVE", "true").lower() == "true"
KOYEB_KEEP_ALIVE_INTERVAL = int(os.getenv("KOYEB_KEEP_ALIVE_INTERVAL", "10"))  # phút
APP_URL = os.getenv("APP_URL", f"https://{DOMAIN}")
KOYEB_AUTO_WARMUP = os.getenv("KOYEB_AUTO_WARMUP", "true").lower() == "true"

def facebook_event_worker():
    """Worker xử lý sự kiện Facebook bất đồng bộ"""
    global FACEBOOK_WORKER_RUNNING
    FACEBOOK_WORKER_RUNNING = True
    
    print(f"[FACEBOOK WORKER] Worker đã khởi động")
    
    while True:
        try:
            # Lấy sự kiện từ queue (blocking)
            event_data = FACEBOOK_EVENT_QUEUE.get()
            
            # Nếu là tín hiệu dừng
            if event_data is None:
                break
            
            # Xử lý sự kiện
            event_type = event_data.get('event_type')
            
            if event_type == 'ViewContent':
                _send_view_content_async(event_data)
            elif event_type == 'AddToCart':
                _send_add_to_cart_async(event_data)
            elif event_type == 'Purchase':
                _send_purchase_async(event_data)
            elif event_type == 'InitiateCheckout':
                _send_initiate_checkout_async(event_data)
            
            # Đánh dấu task hoàn thành
            FACEBOOK_EVENT_QUEUE.task_done()
            
        except Exception as e:
            print(f"[FACEBOOK WORKER ERROR] {e}")
            time.sleep(1)
    
    FACEBOOK_WORKER_RUNNING = False
    print(f"[FACEBOOK WORKER] Worker đã dừng")

def start_facebook_worker():
    """Khởi động worker xử lý sự kiện Facebook"""
    if not FACEBOOK_WORKER_RUNNING:
        worker_thread = threading.Thread(target=facebook_event_worker, daemon=True)
        worker_thread.start()
        print(f"[FACEBOOK WORKER] Đã khởi động worker thread")
        return worker_thread
    return None

# ============================================
# GLOBAL LOCKS
# ============================================
POSTBACK_LOCKS = {}

def get_postback_lock(uid: str, payload: str):
    key = f"{uid}_{payload}"
    if key not in POSTBACK_LOCKS:
        POSTBACK_LOCKS[key] = threading.Lock()
    return POSTBACK_LOCKS[key]

# ============================================
# OPENAI CLIENT
# ============================================
client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ============================================
# PERSISTENT STORAGE FOR USER_CONTEXT - GOOGLE SHEETS
# ============================================

# Không dùng file JSON nữa, dùng Google Sheets làm database
def init_user_context_sheet():
    """Khởi tạo sheet UserContext nếu chưa tồn tại"""
    if not GOOGLE_SHEET_ID or not GOOGLE_SHEETS_CREDENTIALS_JSON:
        print(f"[INIT SHEET] Chưa cấu hình Google Sheets, bỏ qua khởi tạo UserContext sheet")
        return False
    
    try:
        service = get_google_sheets_service()
        if not service:
            print(f"[INIT SHEET] Không thể khởi tạo Google Sheets service")
            return False
        
        # Lấy thông tin tất cả sheets
        spreadsheet = service.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()
        sheets = spreadsheet.get('sheets', [])
        
        # Kiểm tra xem sheet UserContext đã tồn tại chưa
        sheet_exists = False
        for sheet in sheets:
            if sheet['properties']['title'] == USER_CONTEXT_SHEET_NAME:
                sheet_exists = True
                print(f"[INIT SHEET] Sheet {USER_CONTEXT_SHEET_NAME} đã tồn tại")
                break
        
        if not sheet_exists:
            print(f"[INIT SHEET] Tạo sheet mới: {USER_CONTEXT_SHEET_NAME}")
            # Tạo sheet mới
            requests = [{
                'addSheet': {
                    'properties': {
                        'title': USER_CONTEXT_SHEET_NAME,
                        'gridProperties': {
                            'rowCount': 1000,
                            'columnCount': 12  # Tăng cột để đảm bảo đủ
                        }
                    }
                }
            }]
            
            service.spreadsheets().batchUpdate(
                spreadsheetId=GOOGLE_SHEET_ID,
                body={'requests': requests}
            ).execute()
            
            # Đợi một chút để sheet được tạo
            time.sleep(2)
            
            # Thêm header với đủ các cột cần thiết
            headers = [
                ['user_id', 'last_ms', 'product_history', 'order_data', 
                 'conversation_history', 'real_message_count', 
                 'referral_source', 'last_updated', 'phone', 'customer_name',
                 'last_msg_time', 'has_sent_first_carousel']
            ]
            
            service.spreadsheets().values().update(
                spreadsheetId=GOOGLE_SHEET_ID,
                range=f"{USER_CONTEXT_SHEET_NAME}!A1:L1",
                valueInputOption="USER_ENTERED",
                body={'values': headers}
            ).execute()
            
            print(f"[INIT SHEET] Đã tạo sheet {USER_CONTEXT_SHEET_NAME} thành công")
            return True
        else:
            print(f"[INIT SHEET] Sheet {USER_CONTEXT_SHEET_NAME} đã tồn tại")
            return True
            
    except Exception as e:
        print(f"[INIT SHEET ERROR] Lỗi khi khởi tạo sheet: {e}")
        return False

def save_user_context_to_sheets():
    """Lưu USER_CONTEXT vào Google Sheets - MỖI USER LÀ 1 DÒNG RIÊNG"""
    if not GOOGLE_SHEET_ID or not GOOGLE_SHEETS_CREDENTIALS_JSON:
        print("[SAVE CONTEXT] Chưa cấu hình Google Sheets, bỏ qua lưu context")
        return
    
    try:
        service = get_google_sheets_service()
        if not service:
            print("[SAVE CONTEXT] Không thể khởi tạo Google Sheets service")
            return
        
        # Lấy tất cả dữ liệu hiện tại từ sheet
        try:
            result = service.spreadsheets().values().get(
                spreadsheetId=GOOGLE_SHEET_ID,
                range=f"{USER_CONTEXT_SHEET_NAME}!A2:L"
            ).execute()
            existing_values = result.get('values', [])
        except Exception as e:
            print(f"[SAVE CONTEXT] Lỗi khi lấy dữ liệu cũ: {e}")
            existing_values = []
        
        # Tạo mapping user_id -> row index để cập nhật
        user_row_map = {}
        for i, row in enumerate(existing_values):
            if len(row) > 0 and row[0]:  # Có user_id
                user_row_map[row[0]] = i + 2  # +2 vì bắt đầu từ row 2
        
        # Chuẩn bị các request để cập nhật
        update_requests = []
        
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        for user_id, context in USER_CONTEXT.items():
            # Kiểm tra user_id hợp lệ
            if not user_id or len(user_id.strip()) < 5:
                continue
            
            # Chỉ lưu context có dữ liệu và có last_updated gần đây
            last_updated = context.get("last_updated", 0)
            if isinstance(last_updated, (int, float)):
                if last_updated < time.time() - 86400 * 30:  # 30 ngày
                    continue
            else:
                # Nếu last_updated không phải số, sử dụng thời gian hiện tại
                context["last_updated"] = time.time()
            
            # Chuẩn bị dữ liệu
            product_history = json.dumps(context.get("product_history", []), ensure_ascii=False)
            order_data = json.dumps(context.get("order_data", {}), ensure_ascii=False)
            conversation_history = json.dumps(context.get("conversation_history", []), ensure_ascii=False)
            
            # Lấy số điện thoại và tên từ order_data
            phone = ""
            customer_name = ""
            if context.get("order_data"):
                phone = context["order_data"].get("phone", "")
                customer_name = context["order_data"].get("customer_name", "")
            
            # Lấy các trường khác
            last_ms = context.get("last_ms", "")
            last_msg_time = context.get("last_msg_time", 0)
            real_message_count = context.get("real_message_count", 0)
            referral_source = context.get("referral_source", "")
            has_sent_first_carousel = context.get("has_sent_first_carousel", False)
            
            # Chuẩn bị row data (12 cột)
            row_data = [
                user_id,  # Cột A: user_id
                last_ms,  # Cột B: last_ms
                product_history,  # Cột C: product_history
                order_data,  # Cột D: order_data
                conversation_history,  # Cột E: conversation_history
                str(real_message_count),  # Cột F: real_message_count
                referral_source,  # Cột G: referral_source
                now,  # Cột H: last_updated
                phone,  # Cột I: phone
                customer_name,  # Cột J: customer_name
                str(last_msg_time),  # Cột K: last_msg_time
                str(has_sent_first_carousel)  # Cột L: has_sent_first_carousel
            ]
            
            # Kiểm tra xem user đã có trong sheet chưa
            if user_id in user_row_map:
                # Cập nhật dòng hiện có
                range_name = f"{USER_CONTEXT_SHEET_NAME}!A{user_row_map[user_id]}:L{user_row_map[user_id]}"
                update_requests.append({
                    'range': range_name,
                    'values': [row_data]
                })
            else:
                # Thêm dòng mới (sẽ thêm ở cuối)
                pass
        
        # Nếu có dữ liệu mới, thêm vào cuối
        new_rows = []
        for user_id, context in USER_CONTEXT.items():
            if not user_id or len(user_id.strip()) < 5:
                continue
            
            # Kiểm tra user_id đã có trong user_row_map chưa
            if user_id not in user_row_map:
                # Chuẩn bị row data cho user mới
                product_history = json.dumps(context.get("product_history", []), ensure_ascii=False)
                order_data = json.dumps(context.get("order_data", {}), ensure_ascii=False)
                conversation_history = json.dumps(context.get("conversation_history", []), ensure_ascii=False)
                
                phone = ""
                customer_name = ""
                if context.get("order_data"):
                    phone = context["order_data"].get("phone", "")
                    customer_name = context["order_data"].get("customer_name", "")
                
                last_ms = context.get("last_ms", "")
                last_msg_time = context.get("last_msg_time", 0)
                real_message_count = context.get("real_message_count", 0)
                referral_source = context.get("referral_source", "")
                has_sent_first_carousel = context.get("has_sent_first_carousel", False)
                
                row_data = [
                    user_id,
                    last_ms,
                    product_history,
                    order_data,
                    conversation_history,
                    str(real_message_count),
                    referral_source,
                    now,
                    phone,
                    customer_name,
                    str(last_msg_time),
                    str(has_sent_first_carousel)
                ]
                new_rows.append(row_data)
        
        # Thực hiện cập nhật
        if update_requests or new_rows:
            print(f"[CONTEXT SAVE] Đang lưu {len(update_requests)} updates và {len(new_rows)} new rows vào Google Sheets...")
            
            # Cập nhật các dòng hiện có
            for update_req in update_requests:
                try:
                    service.spreadsheets().values().update(
                        spreadsheetId=GOOGLE_SHEET_ID,
                        range=update_req['range'],
                        valueInputOption="USER_ENTERED",
                        body={'values': update_req['values']}
                    ).execute()
                except Exception as e:
                    print(f"[CONTEXT UPDATE ERROR] Lỗi khi cập nhật user: {e}")
            
            # Thêm dòng mới
            if new_rows:
                try:
                    # Xác định vị trí thêm mới
                    start_row = len(existing_values) + 2  # +2 vì bắt đầu từ row 2
                    range_name = f"{USER_CONTEXT_SHEET_NAME}!A{start_row}"
                    
                    service.spreadsheets().values().append(
                        spreadsheetId=GOOGLE_SHEET_ID,
                        range=range_name,
                        valueInputOption="USER_ENTERED",
                        insertDataOption="INSERT_ROWS",
                        body={'values': new_rows}
                    ).execute()
                    
                    print(f"[CONTEXT SAVE] Đã thêm {len(new_rows)} users mới")
                except Exception as e:
                    print(f"[CONTEXT APPEND ERROR] Lỗi khi thêm users mới: {e}")
            
            print(f"[CONTEXT SAVED] Hoàn thành lưu context vào Google Sheets")
        else:
            print(f"[CONTEXT SAVE] Không có dữ liệu để lưu")
        
    except Exception as e:
        print(f"[CONTEXT SAVE ERROR] Lỗi khi lưu context vào Google Sheets: {e}")
        import traceback
        traceback.print_exc()

def cleanup_inactive_users():
    """Dọn dẹp users không hoạt động để giảm RAM"""
    now = time.time()
    inactive_threshold = 86400  # 24 giờ
    max_users = 1000  # Giới hạn số users trong RAM
    
    users_to_remove = []
    
    for user_id, context in USER_CONTEXT.items():
        last_updated = context.get("last_updated", 0)
        if now - last_updated > inactive_threshold:
            # Lưu context trước khi xóa nếu dirty
            if context.get("dirty", False):
                try:
                    # Lưu riêng user này
                    save_single_user_to_sheets(user_id, context)
                except Exception as e:
                    print(f"[CLEANUP SAVE ERROR] Lỗi khi lưu user {user_id}: {e}")
            users_to_remove.append(user_id)
    
    # Xóa users không hoạt động
    for user_id in users_to_remove:
        del USER_CONTEXT[user_id]
    
    # Giới hạn số lượng users trong RAM
    if len(USER_CONTEXT) > max_users:
        # Lấy danh sách users cũ nhất
        sorted_users = sorted(
            USER_CONTEXT.items(),
            key=lambda x: x[1].get("last_updated", 0)
        )
        
        # Xóa users cũ nhất vượt quá giới hạn
        for i in range(len(USER_CONTEXT) - max_users):
            user_id, context = sorted_users[i]
            if context.get("dirty", False):
                try:
                    save_single_user_to_sheets(user_id, context)
                except Exception as e:
                    print(f"[LIMIT CLEANUP ERROR] Lỗi khi lưu user {user_id}: {e}")
            del USER_CONTEXT[user_id]
    
    if users_to_remove:
        print(f"[CLEANUP] Đã xóa {len(users_to_remove)} users không hoạt động")

def save_single_user_to_sheets(user_id: str, context: dict = None):
    """Lưu riêng 1 user vào Google Sheets NGAY LẬP TỨC"""
    if not GOOGLE_SHEET_ID or not GOOGLE_SHEETS_CREDENTIALS_JSON:
        print("[IMMEDIATE SAVE] Chưa cấu hình Google Sheets, bỏ qua")
        return
    
    try:
        # Nếu không truyền context, lấy từ USER_CONTEXT
        if context is None:
            if user_id not in USER_CONTEXT:
                print(f"[IMMEDIATE SAVE] User {user_id} không tồn tại trong USER_CONTEXT")
                return
            context = USER_CONTEXT[user_id]
        
        service = get_google_sheets_service()
        if not service:
            print("[IMMEDIATE SAVE] Không thể khởi tạo Google Sheets service")
            return
        
        # Lấy dữ liệu từ cache
        user_row_map, existing_values = get_sheet_data_cached()
        
        # Chuẩn bị dữ liệu
        product_history = json.dumps(context.get("product_history", []), ensure_ascii=False)
        order_data = json.dumps(context.get("order_data", {}), ensure_ascii=False)
        conversation_history = json.dumps(context.get("conversation_history", []), ensure_ascii=False)
        
        phone = ""
        customer_name = ""
        if context.get("order_data"):
            phone = context["order_data"].get("phone", "")
            customer_name = context["order_data"].get("customer_name", "")
        
        row_data = [
            user_id,
            context.get("last_ms", ""),
            product_history,
            order_data,
            conversation_history,
            str(context.get("real_message_count", 0)),
            context.get("referral_source", ""),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            phone,
            customer_name,
            str(context.get("last_msg_time", 0)),
            str(context.get("has_sent_first_carousel", False))
        ]
        
        # Kiểm tra xem user đã có trong sheet chưa
        if user_id in user_row_map:
            range_name = f"{USER_CONTEXT_SHEET_NAME}!A{user_row_map[user_id]}:L{user_row_map[user_id]}"
            
            service.spreadsheets().values().update(
                spreadsheetId=GOOGLE_SHEET_ID,
                range=range_name,
                valueInputOption="USER_ENTERED",
                body={'values': [row_data]}
            ).execute()
            
            print(f"[IMMEDIATE SAVE] Đã cập nhật user {user_id} với MS {context.get('last_ms')}")
        else:
            # Thêm dòng mới
            start_row = len(existing_values) + 2
            range_name = f"{USER_CONTEXT_SHEET_NAME}!A{start_row}"
            
            service.spreadsheets().values().append(
                spreadsheetId=GOOGLE_SHEET_ID,
                range=range_name,
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body={'values': [row_data]}
            ).execute()
            
            # Reset cache
            SHEETS_CACHE['last_read'] = 0
            
            print(f"[IMMEDIATE SAVE] Đã thêm mới user {user_id} với MS {context.get('last_ms')}")
        
        # Reset dirty flag và cập nhật thời gian lưu
        if user_id in USER_CONTEXT:
            USER_CONTEXT[user_id]["dirty"] = False
            USER_CONTEXT[user_id]["last_saved"] = time.time()
        
    except Exception as e:
        print(f"[IMMEDIATE SAVE ERROR] Lỗi khi lưu user {user_id}: {e}")
        
def save_user_context_to_sheets_optimized(force_all: bool = False):
    """
    Lưu USER_CONTEXT vào Google Sheets - CHỈ lưu users có dirty = True
    hoặc lâu chưa lưu (> 30 giây)
    """
    if not GOOGLE_SHEET_ID or not GOOGLE_SHEETS_CREDENTIALS_JSON:
        print("[SAVE CONTEXT] Chưa cấu hình Google Sheets")
        return
    
    try:
        service = get_google_sheets_service()
        if not service:
            print("[SAVE CONTEXT] Không thể khởi tạo Google Sheets service")
            return
        
        # Lấy dữ liệu từ cache
        user_row_map, existing_values = get_sheet_data_cached()
        
        # Chuẩn bị dữ liệu để lưu
        update_requests = []
        new_rows = []
        
        now = time.time()
        save_threshold = 30  # Chỉ lưu nếu chưa lưu trong 30 giây
        
        for user_id, context in USER_CONTEXT.items():
            # Kiểm tra user_id hợp lệ
            if not user_id or len(user_id.strip()) < 5:
                continue
            
            # Kiểm tra điều kiện lưu:
            # 1. Nếu force_all = True (lưu tất cả)
            # 2. Hoặc dirty = True và chưa lưu trong 30 giây
            # 3. Hoặc chưa lưu lần nào (last_saved = 0) và active trong 30 ngày
            last_saved = context.get("last_saved", 0)
            last_updated = context.get("last_updated", 0)
            
            should_save = False
            
            if force_all:
                should_save = True
            elif context.get("dirty", False) and (now - last_saved > save_threshold):
                should_save = True
            elif last_saved == 0 and (now - last_updated < 86400 * 30):  # 30 ngày
                should_save = True
            
            if not should_save:
                continue
            
            print(f"[CONTEXT SAVE] Đang lưu user {user_id} (dirty={context.get('dirty')})")
            
            # Chuẩn bị dữ liệu
            product_history = json.dumps(context.get("product_history", []), ensure_ascii=False)
            order_data = json.dumps(context.get("order_data", {}), ensure_ascii=False)
            conversation_history = json.dumps(context.get("conversation_history", []), ensure_ascii=False)
            
            # Lấy số điện thoại và tên từ order_data
            phone = ""
            customer_name = ""
            if context.get("order_data"):
                phone = context["order_data"].get("phone", "")
                customer_name = context["order_data"].get("customer_name", "")
            
            # Lấy các trường khác
            last_ms = context.get("last_ms", "")
            last_msg_time = context.get("last_msg_time", 0)
            real_message_count = context.get("real_message_count", 0)
            referral_source = context.get("referral_source", "")
            has_sent_first_carousel = context.get("has_sent_first_carousel", False)
            
            # Chuẩn bị row data (12 cột)
            row_data = [
                user_id,  # Cột A: user_id
                last_ms,  # Cột B: last_ms
                product_history,  # Cột C: product_history
                order_data,  # Cột D: order_data
                conversation_history,  # Cột E: conversation_history
                str(real_message_count),  # Cột F: real_message_count
                referral_source,  # Cột G: referral_source
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Cột H: last_updated
                phone,  # Cột I: phone
                customer_name,  # Cột J: customer_name
                str(last_msg_time),  # Cột K: last_msg_time
                str(has_sent_first_carousel)  # Cột L: has_sent_first_carousel
            ]
            
            # Kiểm tra xem user đã có trong sheet chưa
            if user_id in user_row_map:
                # Cập nhật dòng hiện có
                range_name = f"{USER_CONTEXT_SHEET_NAME}!A{user_row_map[user_id]}:L{user_row_map[user_id]}"
                update_requests.append({
                    'range': range_name,
                    'values': [row_data]
                })
            else:
                # Thêm dòng mới
                new_rows.append(row_data)
            
            # Đánh dấu đã lưu và reset dirty flag
            context["dirty"] = False
            context["last_saved"] = now
        
        # Thực hiện cập nhật nếu có dữ liệu
        if update_requests or new_rows:
            print(f"[CONTEXT SAVE OPTIMIZED] Đang lưu {len(update_requests)} updates và {len(new_rows)} new rows...")
            
            # Batch update cho các dòng hiện có
            if update_requests:
                try:
                    batch_data = []
                    for req in update_requests:
                        batch_data.append({
                            'range': req['range'],
                            'values': req['values']
                        })
                    
                    body = {
                        'valueInputOption': 'USER_ENTERED',
                        'data': batch_data
                    }
                    
                    service.spreadsheets().values().batchUpdate(
                        spreadsheetId=GOOGLE_SHEET_ID,
                        body=body
                    ).execute()
                    
                    print(f"[CONTEXT SAVE] Đã batch update {len(update_requests)} users")
                except Exception as e:
                    print(f"[CONTEXT UPDATE ERROR] Lỗi batch update: {e}")
            
            # Append các dòng mới
            if new_rows:
                try:
                    start_row = len(existing_values) + 2
                    range_name = f"{USER_CONTEXT_SHEET_NAME}!A{start_row}"
                    
                    service.spreadsheets().values().append(
                        spreadsheetId=GOOGLE_SHEET_ID,
                        range=range_name,
                        valueInputOption="USER_ENTERED",
                        insertDataOption="INSERT_ROWS",
                        body={'values': new_rows}
                    ).execute()
                    
                    print(f"[CONTEXT SAVE] Đã thêm {len(new_rows)} users mới")
                    
                    # Cập nhật cache sau khi thêm mới
                    SHEETS_CACHE['last_read'] = 0  # Reset cache để load lại
                    
                except Exception as e:
                    print(f"[CONTEXT APPEND ERROR] Lỗi khi thêm users mới: {e}")
            
            print(f"[CONTEXT SAVED] Hoàn thành lưu context vào Google Sheets")
        else:
            print(f"[CONTEXT SAVE] Không có dữ liệu dirty để lưu")
        
    except Exception as e:
        print(f"[CONTEXT SAVE ERROR] Lỗi khi lưu context: {e}")
        import traceback
        traceback.print_exc()

# Thay thế hàm cũ bằng hàm tối ưu
def save_user_context_to_sheets():
    """Alias cho hàm tối ưu - để không phải sửa code cũ"""
    save_user_context_to_sheets_optimized()

def load_user_context_from_sheets():
    """Load USER_CONTEXT từ Google Sheets - CHỈ LOAD DÒNG CÓ user_id KHÁC RỖNG"""
    if not GOOGLE_SHEET_ID or not GOOGLE_SHEETS_CREDENTIALS_JSON:
        print("[LOAD CONTEXT] Chưa cấu hình Google Sheets, bỏ qua load context")
        return
    
    try:
        service = get_google_sheets_service()
        if not service:
            return
        
        # Lấy dữ liệu từ sheet (KHÔNG load header)
        result = service.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=f"{USER_CONTEXT_SHEET_NAME}!A2:L"
        ).execute()
        
        values = result.get('values', [])
        
        loaded_count = 0
        for row in values:
            # Kiểm tra dòng có đủ dữ liệu và có user_id không
            if len(row) < 1 or not row[0]:  # Cột đầu tiên là user_id
                continue  # Bỏ qua dòng trống hoặc không có user_id
            
            user_id = row[0].strip()
            if not user_id:
                continue  # Bỏ qua user_id rỗng
            
            # Kiểm tra xem user_id có hợp lệ không (không phải là header)
            if user_id.lower() in ['user_id', 'id', 'uid']:
                continue
            
            # Xóa context cũ nếu có (đảm bảo không bị chồng chéo)
            if user_id in USER_CONTEXT:
                del USER_CONTEXT[user_id]
            
            # Tạo context mặc định
            context = default_user_context()
            
            # Cập nhật từ dữ liệu Google Sheets (CÓ KIỂM TRA TỪNG CỘT)
            # Cột 1: user_id (đã lấy)
            # Cột 2: last_ms
            if len(row) > 1 and row[1]:
                context["last_ms"] = row[1]
            
            # Cột 3: product_history
            if len(row) > 2 and row[2]:
                try:
                    context["product_history"] = json.loads(row[2])
                except:
                    context["product_history"] = []
            
            # Cột 4: order_data
            if len(row) > 3 and row[3]:
                try:
                    context["order_data"] = json.loads(row[3])
                except:
                    context["order_data"] = {}
            
            # Cột 5: conversation_history
            if len(row) > 4 and row[4]:
                try:
                    context["conversation_history"] = json.loads(row[4])
                except:
                    context["conversation_history"] = []
            
            # Cột 6: real_message_count
            if len(row) > 5 and row[5]:
                try:
                    context["real_message_count"] = int(row[5])
                except:
                    context["real_message_count"] = 0
            
            # Cột 7: referral_source
            if len(row) > 6 and row[6]:
                context["referral_source"] = row[6]
            
            # Cột 8: last_updated (timestamp)
            if len(row) > 7 and row[7]:
                try:
                    # Chuyển đổi từ string sang timestamp nếu có thể
                    context["last_updated"] = float(row[7]) if '.' in row[7] else int(row[7])
                except:
                    context["last_updated"] = time.time()
            
            # Cột 9: phone
            if len(row) > 8 and row[8]:
                # Cập nhật phone vào order_data
                if "order_data" not in context:
                    context["order_data"] = {}
                context["order_data"]["phone"] = row[8]
            
            # Cột 10: customer_name
            if len(row) > 9 and row[9]:
                # Cập nhật customer_name vào order_data
                if "order_data" not in context:
                    context["order_data"] = {}
                context["order_data"]["customer_name"] = row[9]
            
            # Cột 11: last_msg_time
            if len(row) > 10 and row[10]:
                try:
                    context["last_msg_time"] = float(row[10])
                except:
                    context["last_msg_time"] = 0
            
            # Cột 12: has_sent_first_carousel
            if len(row) > 11 and row[11]:
                try:
                    context["has_sent_first_carousel"] = row[11].lower() == "true"
                except:
                    context["has_sent_first_carousel"] = False
            
            # Lưu context vào USER_CONTEXT
            USER_CONTEXT[user_id] = context
            loaded_count += 1
        
        print(f"[CONTEXT LOADED] Đã load {loaded_count} users từ Google Sheets")
        
    except Exception as e:
        print(f"[CONTEXT LOAD ERROR] Lỗi khi load context từ Google Sheets: {e}")
        import traceback
        traceback.print_exc()

def get_user_context_from_sheets(user_id: str) -> Optional[Dict]:
    """Load context của 1 user cụ thể từ Google Sheets"""
    if not GOOGLE_SHEET_ID or not GOOGLE_SHEETS_CREDENTIALS_JSON:
        return None
    
    try:
        service = get_google_sheets_service()
        if not service:
            return None
        
        # Lấy tất cả dữ liệu
        result = service.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=f"{USER_CONTEXT_SHEET_NAME}!A2:L"
        ).execute()
        
        values = result.get('values', [])
        
        for row in values:
            if len(row) > 0 and row[0] == user_id:
                # Tìm thấy user
                print(f"[GET CONTEXT DEBUG] Tìm thấy user {user_id} trong Google Sheets")
                print(f"[GET CONTEXT DEBUG] Row data length: {len(row)}")
                
                context = default_user_context()
                
                # Cập nhật từ dữ liệu
                if len(row) > 1 and row[1]:
                    context["last_ms"] = row[1]
                    print(f"[GET CONTEXT DEBUG] last_ms từ cột B: {row[1]}")
                
                if len(row) > 2 and row[2]:
                    try:
                        context["product_history"] = json.loads(row[2])
                        print(f"[GET CONTEXT DEBUG] product_history: {context['product_history'][:3] if context['product_history'] else '[]'}")
                    except:
                        context["product_history"] = []
                        print(f"[GET CONTEXT DEBUG] Lỗi parse product_history: {row[2][:100]}...")
                
                if len(row) > 3 and row[3]:
                    try:
                        context["order_data"] = json.loads(row[3])
                    except:
                        context["order_data"] = {}
                
                if len(row) > 4 and row[4]:
                    try:
                        context["conversation_history"] = json.loads(row[4])
                    except:
                        context["conversation_history"] = []
                
                if len(row) > 5 and row[5]:
                    try:
                        context["real_message_count"] = int(row[5])
                    except:
                        context["real_message_count"] = 0
                
                if len(row) > 6 and row[6]:
                    context["referral_source"] = row[6]
                
                if len(row) > 7 and row[7]:
                    try:
                        context["last_updated"] = float(row[7]) if '.' in row[7] else int(row[7])
                    except:
                        context["last_updated"] = time.time()
                
                if len(row) > 8 and row[8]:
                    if "order_data" not in context:
                        context["order_data"] = {}
                    context["order_data"]["phone"] = row[8]
                
                if len(row) > 9 and row[9]:
                    if "order_data" not in context:
                        context["order_data"] = {}
                    context["order_data"]["customer_name"] = row[9]
                
                if len(row) > 10 and row[10]:
                    try:
                        context["last_msg_time"] = float(row[10])
                    except:
                        context["last_msg_time"] = 0
                
                if len(row) > 11 and row[11]:
                    try:
                        context["has_sent_first_carousel"] = row[11].lower() == "true"
                    except:
                        context["has_sent_first_carousel"] = False
                
                print(f"[GET CONTEXT] Đã load context cho user {user_id} từ Google Sheets")
                print(f"[GET CONTEXT SUMMARY] last_ms: {context.get('last_ms')}, product_history count: {len(context.get('product_history', []))}")
                return context
        
        print(f"[GET CONTEXT] Không tìm thấy context cho user {user_id} trong Google Sheets")
        return None
        
    except Exception as e:
        print(f"[GET CONTEXT ERROR] Lỗi khi load context cho user {user_id}: {e}")
        return None

def delete_user_context_from_sheets(user_id: str):
    """Xóa context của user khỏi Google Sheets (khi cần)"""
    if not GOOGLE_SHEET_ID or not GOOGLE_SHEETS_CREDENTIALS_JSON:
        return False
    
    try:
        service = get_google_sheets_service()
        if not service:
            return False
        
        # Lấy tất cả dữ liệu hiện tại
        result = service.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=f"{USER_CONTEXT_SHEET_NAME}!A2:L"
        ).execute()
        
        values = result.get('values', [])
        
        # Tìm dòng cần xóa
        rows_to_delete = []
        for i, row in enumerate(values):
            if len(row) > 0 and row[0] == user_id:
                rows_to_delete.append(i + 2)  # +2 vì bắt đầu từ row 2
        
        if not rows_to_delete:
            return True  # Không có dòng nào để xóa
        
        # Xóa từ dưới lên để không làm hỏng index
        rows_to_delete.sort(reverse=True)
        
        for row_index in rows_to_delete:
            try:
                # Xóa dòng
                requests = [{
                    'deleteDimension': {
                        'range': {
                            'sheetId': 0,
                            'dimension': 'ROWS',
                            'startIndex': row_index - 1,
                            'endIndex': row_index
                        }
                    }
                }]
                
                service.spreadsheets().batchUpdate(
                    spreadsheetId=GOOGLE_SHEET_ID,
                    body={'requests': requests}
                ).execute()
                
                print(f"[CONTEXT DELETE] Đã xóa context của user {user_id} khỏi Google Sheets")
            except Exception as e:
                print(f"[CONTEXT DELETE ERROR] Lỗi khi xóa user {user_id}: {e}")
        
        return True
        
    except Exception as e:
        print(f"[CONTEXT DELETE ERROR] Lỗi khi xóa context: {e}")
        return False

def get_sheet_data_cached():
    """Lấy dữ liệu từ Google Sheets với cache"""
    global SHEETS_CACHE
    
    now = time.time()
    
    # Nếu cache còn hiệu lực, trả về cache
    if (SHEETS_CACHE['last_read'] > 0 and 
        (now - SHEETS_CACHE['last_read']) < SHEETS_CACHE['cache_ttl'] and
        SHEETS_CACHE['user_row_map']):
        return SHEETS_CACHE['user_row_map'], SHEETS_CACHE['existing_values']
    
    # Nếu không có cấu hình Google Sheets
    if not GOOGLE_SHEET_ID or not GOOGLE_SHEETS_CREDENTIALS_JSON:
        return {}, []
    
    try:
        service = get_google_sheets_service()
        if not service:
            return {}, []
        
        # Lấy dữ liệu từ sheet
        result = service.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=f"{USER_CONTEXT_SHEET_NAME}!A2:L"
        ).execute()
        
        existing_values = result.get('values', [])
        
        # Tạo mapping user_id -> row index
        user_row_map = {}
        for i, row in enumerate(existing_values):
            if len(row) > 0 and row[0]:  # Có user_id
                user_row_map[row[0]] = i + 2  # +2 vì bắt đầu từ row 2
        
        # Cập nhật cache
        SHEETS_CACHE['user_row_map'] = user_row_map
        SHEETS_CACHE['existing_values'] = existing_values
        SHEETS_CACHE['last_read'] = now
        
        print(f"[SHEETS CACHE] Đã load {len(user_row_map)} users từ Google Sheets")
        
        return user_row_map, existing_values
        
    except Exception as e:
        print(f"[SHEETS CACHE ERROR] Lỗi khi load sheet: {e}")
        return {}, []

def periodic_context_save_optimized():
    """Lưu context định kỳ vào Google Sheets - CHỈ lưu users dirty"""
    print(f"[PERIODIC SAVE THREAD] Thread lưu context đã bắt đầu")
    
    # Đợi app khởi động xong
    time.sleep(30)
    
    # Kiểm tra và tạo sheet nếu cần
    if GOOGLE_SHEET_ID and GOOGLE_SHEETS_CREDENTIALS_JSON:
        try:
            init_user_context_sheet()
        except Exception as e:
            print(f"[PERIODIC SAVE INIT ERROR] Lỗi khi khởi tạo sheet: {e}")
    
    last_full_save = 0
    full_save_interval = 3600  # 1 giờ lưu full 1 lần
    
    while True:
        try:
            # Đếm số users dirty
            dirty_count = 0
            active_users = 0
            now = time.time()
            
            for uid, ctx in USER_CONTEXT.items():
                if ctx.get("dirty", False):
                    dirty_count += 1
                if ctx.get("last_updated", 0) > now - 86400:  # 24h
                    active_users += 1
            
            # Kiểm tra có nên lưu full không
            save_full = (now - last_full_save) > full_save_interval
            
            if dirty_count > 0 or save_full:
                print(f"[PERIODIC SAVE] Đang lưu {dirty_count} dirty users và {active_users} active users...")
                
                if save_full:
                    print(f"[PERIODIC SAVE FULL] Lưu toàn bộ active users")
                    save_user_context_to_sheets_optimized(force_all=True)
                    last_full_save = now
                else:
                    save_user_context_to_sheets_optimized(force_all=False)
                
                print(f"[PERIODIC SAVE] Hoàn thành, đợi 1 phút...")
            else:
                if active_users > 0:
                    print(f"[PERIODIC SAVE] Không có dirty users, bỏ qua lưu (Active: {active_users})")
                else:
                    print(f"[PERIODIC SAVE] Không có active users, đợi 5 phút...")
                
        except Exception as e:
            print(f"[PERIODIC SAVE ERROR] Lỗi khi lưu context: {e}")
            import traceback
            traceback.print_exc()
        
        # Sleep ngắn hơn khi có dirty users, dài hơn khi không có
        if dirty_count > 0:
            time.sleep(60)  # 1 phút
        else:
            time.sleep(300)  # 5 phút

# Thay thế hàm cũ
def periodic_context_save():
    """Alias cho hàm tối ưu"""
    periodic_context_save_optimized()
    
def get_user_order_history_from_sheets(user_id: str, phone: str = None) -> List[Dict]:
    """Tra cứu lịch sử đơn hàng từ Google Sheets"""
    if not GOOGLE_SHEET_ID or not GOOGLE_SHEETS_CREDENTIALS_JSON:
        return []
    
    try:
        service = get_google_sheets_service()
        if not service:
            return []
        
        # Lấy dữ liệu từ sheet Orders
        result = service.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEET_ID,
            range="Orders!A:V"  # Lấy tất cả cột
        ).execute()
        
        values = result.get('values', [])
        if len(values) <= 1:  # Chỉ có header
            return []
        
        # Tìm các cột cần thiết
        headers = values[0]
        col_indices = {}
        for i, header in enumerate(headers):
            header_lower = header.lower()
            if 'user' in header_lower or 'uid' in header_lower:
                col_indices['user_id'] = i
            elif 'phone' in header_lower or 'sđt' in header_lower or 'điện thoại' in header_lower:
                col_indices['phone'] = i
            elif 'ms' in header_lower or 'mã' in header_lower or 'product_code' in header_lower:
                col_indices['ms'] = i
            elif 'name' in header_lower or 'tên' in header_lower or 'product_name' in header_lower:
                col_indices['product_name'] = i
            elif 'timestamp' in header_lower or 'thời gian' in header_lower:
                col_indices['timestamp'] = i
        
        user_orders = []
        
        for row in values[1:]:
            if len(row) < max(col_indices.values()) + 1:
                continue
            
            # Kiểm tra xem có khớp user_id hoặc phone không
            row_user_id = row[col_indices.get('user_id', 0)] if col_indices.get('user_id') else ""
            row_phone = row[col_indices.get('phone', 0)] if col_indices.get('phone') else ""
            
            match = False
            if user_id and row_user_id == user_id:
                match = True
            elif phone and row_phone == phone:
                match = True
            
            if match:
                order = {
                    "timestamp": row[col_indices.get('timestamp', 0)] if col_indices.get('timestamp') else "",
                    "ms": row[col_indices.get('ms', 0)] if col_indices.get('ms') else "",
                    "product_name": row[col_indices.get('product_name', 0)] if col_indices.get('product_name') else "",
                    "phone": row_phone,
                    "user_id": row_user_id
                }
                user_orders.append(order)
        
        # Sắp xếp theo thời gian mới nhất
        user_orders.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        return user_orders[:5]  # Trả về 5 đơn gần nhất
        
    except Exception as e:
        print(f"[ORDER HISTORY ERROR] Lỗi khi tra cứu đơn hàng: {e}")
        return []

def default_user_context():
    """Tạo context mặc định cho user mới"""
    return {
        "last_msg_time": 0,
        "last_ms": None,
        "order_state": None,
        "order_data": {},
        "processing_lock": False,
        "real_message_count": 0,
        "product_history": [],
        "conversation_history": [],
        "referral_source": None,
        "referral_payload": None,
        "last_retailer_id": None,
        "catalog_view_time": 0,
        "has_sent_first_carousel": False,
        "idempotent_postbacks": {},
        "processed_message_mids": {},
        "last_processed_text": "",
        "poscake_orders": [],
        "last_updated": time.time(),
        "dirty": False,      # ← THÊM DÒNG NÀY
        "last_saved": 0      # ← THÊM DÒNG NÀY
    }

# ============================================
# DIRTY FLAG HELPER FUNCTIONS
# ============================================

def mark_user_dirty(uid: str):
    """Đánh dấu user cần được lưu vào Google Sheets"""
    if uid in USER_CONTEXT:
        USER_CONTEXT[uid]["dirty"] = True
        USER_CONTEXT[uid]["last_updated"] = time.time()
        
# ============================================
# MAP TIẾNG VIỆT KHÔNG DẤU
# ============================================
VIETNAMESE_MAP = {
    'à': 'a', 'á': 'a', 'ả': 'a', 'ã': 'a', 'ạ': 'a',
    'ă': 'a', 'ằ': 'a', 'ắ': 'a', 'ẳ': 'a', 'ẵ': 'a', 'ặ': 'a',
    'â': 'a', 'ầ': 'a', 'ấ': 'a', 'ẩ': 'a', 'ẫ': 'a', 'ậ': 'a',
    'đ': 'd',
    'è': 'e', 'é': 'e', 'ẻ': 'e', 'ẽ': 'e', 'ẹ': 'e',
    'ê': 'e', 'ề': 'e', 'ế': 'e', 'ể': 'e', 'ễ': 'e', 'ệ': 'e',
    'ì': 'i', 'í': 'i', 'ỉ': 'i', 'ĩ': 'i', 'ị': 'i',
    'ò': 'o', 'ó': 'o', 'ỏ': 'o', 'õ': 'o', 'ọ': 'o',
    'ô': 'o', 'ồ': 'o', 'ố': 'o', 'ổ': 'o', 'ỗ': 'o', 'ộ': 'o',
    'ơ': 'o', 'ờ': 'o', 'ớ': 'o', 'ở': 'o', 'ỡ': 'o', 'ợ': 'o',
    'ù': 'u', 'ú': 'u', 'ủ': 'u', 'ũ': 'u', 'ụ': 'u',
    'ư': 'u', 'ừ': 'u', 'ứ': 'u', 'ử': 'u', 'ữ': 'u', 'ự': 'u',
    'ỳ': 'y', 'ý': 'y', 'ỷ': 'y', 'ỹ': 'y', 'ỵ': 'y',
    'À': 'A', 'Á': 'A', 'Ả': 'A', 'Ã': 'A', 'Ạ': 'A',
    'Ă': 'A', 'Ằ': 'A', 'Ắ': 'A', 'Ẳ': 'A', 'Ẵ': 'A', 'Ặ': 'A',
    'Â': 'A', 'Ầ': 'A', 'Ấ': 'A', 'Ẩ': 'A', 'Ẫ': 'A', 'Ậ': 'A',
    'Đ': 'D',
    'È': 'E', 'É': 'E', 'Ẻ': 'E', 'Ẽ': 'E', 'Ẹ': 'E',
    'Ê': 'E', 'Ề': 'E', 'Ế': 'E', 'Ể': 'E', 'Ễ': 'E', 'Ệ': 'E',
    'Ì': 'I', 'Í': 'I', 'Ỉ': 'I', 'Ĩ': 'I', 'Ị': 'I',
    'Ò': 'O', 'Ó': 'O', 'Ỏ': 'O', 'Õ': 'O', 'Ọ': 'O',
    'Ô': 'O', 'Ồ': 'O', 'Ố': 'O', 'Ổ': 'O', 'Ỗ': 'O', 'Ộ': 'O',
    'Ơ': 'O', 'Ờ': 'O', 'Ớ': 'O', 'Ở': 'O', 'Ỡ': 'O', 'Ợ': 'O',
    'Ù': 'U', 'Ú': 'U', 'Ủ': 'U', 'Ũ': 'U', 'Ụ': 'U',
    'Ư': 'U', 'Ừ': 'U', 'Ứ': 'U', 'Ử': 'U', 'Ữ': 'U', 'Ự': 'U',
    'Ỳ': 'Y', 'Ý': 'Y', 'Ỷ': 'Y', 'Ỹ': 'Y', 'Ỵ': 'Y'
}

def normalize_vietnamese(text):
    if not text: return ""
    result = text
    for char, replacement in VIETNAMESE_MAP.items():
        result = result.replace(char, replacement)
    return result

# ============================================
# GLOBAL STATE
# ============================================
USER_CONTEXT = defaultdict(default_user_context)

# ============================================
# GLOBAL IDEMPOTENCY & ASYNC PROCESSING
# ============================================

# Lưu trữ các message ID đã xử lý trong 5 phút qua để tránh xử lý trùng lặp
PROCESSED_MIDS = {}
PROCESSED_MIDS_LOCK = threading.Lock()
PROCESSED_MIDS_TTL = 300  # 5 phút = 300 giây

# Queue để xử lý tin nhắn bất đồng bộ
MESSAGE_QUEUE = Queue()
MESSAGE_WORKER_RUNNING = False

# Lưu trữ các tin nhắn đang xử lý để tránh race condition
PROCESSING_MESSAGES = {}
PROCESSING_MESSAGES_LOCK = threading.Lock()

# KOYEB FREE TIER OPTIMIZATION
PRODUCTS_LOADED_ON_STARTUP = False
WORKERS_INITIALIZED = False  # Nếu chưa có thì thêm

# Cache để tránh load lại sản phẩm quá nhiều
APP_WARMED_UP = False

PRODUCTS = {}
PRODUCTS_BY_NUMBER = {}
LAST_LOAD = 0
LOAD_TTL = 300

# ============================================
# KOYEB FREE TIER KEEP-ALIVE FUNCTIONS
# ============================================

def keep_alive_ping():
    """Tự động ping chính app để giữ Koyeb không sleep"""
    if not KOYEB_KEEP_ALIVE_ENABLED:
        return
    
    try:
        # Ping endpoint /ping hoặc /health
        ping_url = f"{APP_URL}/ping"
        print(f"[KEEP-ALIVE] Đang ping {ping_url}")
        
        response = requests.get(ping_url, timeout=10)
        if response.status_code == 200:
            print(f"[KEEP-ALIVE] Thành công, app vẫn sống")
        else:
            print(f"[KEEP-ALIVE] Lỗi: {response.status_code}")
    except Exception as e:
        print(f"[KEEP-ALIVE ERROR] {e}")
        # Thử ping lại sau 1 phút
        time.sleep(60)
        try:
            requests.get(f"{APP_URL}/ping", timeout=5)
        except:
            pass

def start_keep_alive_scheduler():
    """Khởi động scheduler để giữ app không sleep"""
    if not KOYEB_KEEP_ALIVE_ENABLED:
        print(f"[KEEP-ALIVE] Tính năng keep-alive đã tắt")
        return
    
    print(f"[KEEP-ALIVE] Bật tính năng keep-alive, ping mỗi {KOYEB_KEEP_ALIVE_INTERVAL} phút")
    
    # Lập lịch ping định kỳ
    schedule.every(KOYEB_KEEP_ALIVE_INTERVAL).minutes.do(keep_alive_ping)
    
    # Chạy scheduler trong thread riêng
    def run_scheduler():
        while True:
            schedule.run_pending()
            time.sleep(60)  # Kiểm tra mỗi phút
    
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Ping ngay lần đầu
    time.sleep(5)
    keep_alive_ping()

def warm_up_app():
    """Làm nóng app: load products và khởi động workers ngay khi start"""
    global APP_WARMED_UP
    if APP_WARMED_UP:
        return
    
    print(f"[WARM-UP] Đang khởi động nhanh app...")
    
    # 1. Load products ngay lập tức (trong thread riêng)
    def load_products_async():
        global PRODUCTS_LOADED_ON_STARTUP
        try:
            print(f"[WARM-UP] Đang load products...")
            load_products(force=True)
            PRODUCTS_LOADED_ON_STARTUP = True
            print(f"[WARM-UP] Đã load {len(PRODUCTS)} products")
        except Exception as e:
            print(f"[WARM-UP ERROR] Lỗi load products: {e}")
    
    # 2. Khởi động workers
    def start_workers_async():
        global WORKERS_INITIALIZED
        try:
            print(f"[WARM-UP] Đang khởi động workers...")
            start_message_worker()
            start_facebook_worker()
            
            # Khởi động thread lưu context định kỳ
            save_thread = threading.Thread(target=periodic_context_save_optimized, daemon=True)
            save_thread.start()
            
            # Khởi động thread dọn dẹp user không hoạt động
            cleanup_thread = threading.Thread(target=periodic_cleanup, daemon=True)
            cleanup_thread.start()
            
            WORKERS_INITIALIZED = True
            print(f"[WARM-UP] Workers đã khởi động")
        except Exception as e:
            print(f"[WARM-UP ERROR] Lỗi khởi động workers: {e}")
    
    # Chạy async để không block startup
    threading.Thread(target=load_products_async, daemon=True).start()
    threading.Thread(target=start_workers_async, daemon=True).start()
    
    # 3. Khởi tạo Google Sheets nếu cần
    if GOOGLE_SHEET_ID and GOOGLE_SHEETS_CREDENTIALS_JSON:
        threading.Thread(target=init_user_context_sheet, daemon=True).start()
    
    APP_WARMED_UP = True
    print(f"[WARM-UP] Hoàn thành khởi động nhanh")

def periodic_cleanup():
    """Dọn dẹp định kỳ để giảm RAM"""
    print(f"[CLEANUP THREAD] Thread dọn dẹp đã bắt đầu")
    time.sleep(60)  # Đợi 1 phút sau khi start
    
    while True:
        try:
            # Dọn dẹp users không hoạt động
            cleanup_inactive_users()
            
            # Dọn dẹp cache cũ
            with PROCESSING_MESSAGES_LOCK:
                now = time.time()
                keys_to_remove = []
                for key, timestamp in PROCESSING_MESSAGES.items():
                    if now - timestamp > 60:  # 60 giây
                        keys_to_remove.append(key)
                
                for key in keys_to_remove:
                    del PROCESSING_MESSAGES[key]
            
            # Dọn dẹp processed MIDs
            with PROCESSED_MIDS_LOCK:
                now = time.time()
                mids_to_remove = []
                for mid, timestamp in PROCESSED_MIDS.items():
                    if now - timestamp > PROCESSED_MIDS_TTL:
                        mids_to_remove.append(mid)
                
                for mid in mids_to_remove:
                    del PROCESSED_MIDS[mid]
            
            print(f"[CLEANUP] Đã dọn dẹp, đợi 5 phút...")
            time.sleep(300)  # 5 phút
            
        except Exception as e:
            print(f"[CLEANUP ERROR] {e}")
            time.sleep(60)

# ============================================
# SỬA HÀM initialize_workers_once() NẾU CÓ
# ============================================

def initialize_workers_once():
    """Khởi động workers một lần duy nhất - TỐI ƯU CHO KOYEB"""
    global WORKERS_INITIALIZED
    
    if WORKERS_INITIALIZED:
        return
    
    print(f"[INIT WORKERS] Đang khởi động workers...")
    
    # 1. Khởi động message worker
    if not MESSAGE_WORKER_RUNNING:
        msg_worker = start_message_worker()
        if msg_worker:
            print(f"[INIT WORKERS] Message worker đã khởi động")
    
    # 2. Khởi động Facebook CAPI worker
    if not FACEBOOK_WORKER_RUNNING:
        fb_worker = start_facebook_worker()
        if fb_worker:
            print(f"[INIT WORKERS] Facebook worker đã khởi động")
    
    # 3. Khởi động thread lưu context định kỳ
    try:
        save_thread = threading.Thread(target=periodic_context_save_optimized, daemon=True)
        save_thread.start()
        print(f"[INIT WORKERS] Thread lưu context đã khởi động")
    except Exception as e:
        print(f"[INIT WORKERS ERROR] Không thể khởi động thread lưu: {e}")
    
    # 4. Khởi động thread dọn dẹp
    try:
        cleanup_thread = threading.Thread(target=periodic_cleanup, daemon=True)
        cleanup_thread.start()
        print(f"[INIT WORKERS] Thread dọn dẹp đã khởi động")
    except Exception as e:
        print(f"[INIT WORKERS ERROR] Không thể khởi động thread dọn dẹp: {e}")
    
    WORKERS_INITIALIZED = True
    print(f"[INIT WORKERS] Tất cả workers đã khởi động xong")
    
def message_background_worker():
    """Worker xử lý tin nhắn bất đồng bộ - KHÔNG BLOCK WEBHOOK"""
    global MESSAGE_WORKER_RUNNING
    MESSAGE_WORKER_RUNNING = True
    
    print(f"[BACKGROUND WORKER] Worker xử lý tin nhắn đã khởi động")
    
    while True:
        try:
            # Lấy tin nhắn từ queue (blocking)
            task = MESSAGE_QUEUE.get()
            
            # Tín hiệu dừng
            if task is None:
                break
            
            # Giải nén dữ liệu
            task_data, client_ip, user_agent = task
            
            # Xử lý tin nhắn
            process_facebook_message(task_data, client_ip, user_agent)
            
            # Đánh dấu task hoàn thành
            MESSAGE_QUEUE.task_done()
            
        except Exception as e:
            print(f"[BACKGROUND WORKER ERROR] {e}")
            import traceback
            traceback.print_exc()
            time.sleep(1)  # Tránh crash loop
    
    MESSAGE_WORKER_RUNNING = False
    print(f"[BACKGROUND WORKER] Worker đã dừng")


def start_message_worker():
    """Khởi động worker xử lý tin nhắn bất đồng bộ"""
    if not MESSAGE_WORKER_RUNNING:
        worker_thread = threading.Thread(target=message_background_worker, daemon=True)
        worker_thread.start()
        print(f"[BACKGROUND WORKER] Đã khởi động worker thread")
        return worker_thread
    return None


def is_message_processed(mid: str) -> bool:
    """Kiểm tra xem message đã được xử lý chưa (trong vòng 5 phút)"""
    if not mid:
        return False
    
    with PROCESSED_MIDS_LOCK:
        now = time.time()
        
        # Dọn dẹp các MIDs cũ
        mids_to_remove = []
        for existing_mid, timestamp in PROCESSED_MIDS.items():
            if now - timestamp > PROCESSED_MIDS_TTL:
                mids_to_remove.append(existing_mid)
        
        for mid_to_remove in mids_to_remove:
            del PROCESSED_MIDS[mid_to_remove]
        
        # Kiểm tra MID hiện tại
        if mid in PROCESSED_MIDS:
            return True
        
        # Thêm MID mới
        PROCESSED_MIDS[mid] = now
        return False


def mark_message_processing(uid: str, message_id: str) -> bool:
    """Đánh dấu tin nhắn đang được xử lý - tránh race condition"""
    key = f"{uid}_{message_id}"
    
    with PROCESSING_MESSAGES_LOCK:
        now = time.time()
        
        # Dọn dẹp các key cũ (> 30 giây)
        keys_to_remove = []
        for existing_key, timestamp in PROCESSING_MESSAGES.items():
            if now - timestamp > 30:  # 30 giây
                keys_to_remove.append(existing_key)
        
        for key_to_remove in keys_to_remove:
            del PROCESSING_MESSAGES[key_to_remove]
        
        # Kiểm tra key hiện tại
        if key in PROCESSING_MESSAGES:
            return False  # Đang xử lý
        
        # Đánh dấu đang xử lý
        PROCESSING_MESSAGES[key] = now
        return True


def mark_message_completed(uid: str, message_id: str):
    """Đánh dấu tin nhắn đã xử lý xong"""
    key = f"{uid}_{message_id}"
    
    with PROCESSING_MESSAGES_LOCK:
        if key in PROCESSING_MESSAGES:
            del PROCESSING_MESSAGES[key]


def queue_message_for_processing(data: dict, client_ip: str, user_agent: str):
    """Thêm tin nhắn vào queue để xử lý bất đồng bộ"""
    # Giới hạn queue size để tránh memory leak
    if MESSAGE_QUEUE.qsize() < 500:  # Max 500 tin nhắn trong queue
        MESSAGE_QUEUE.put((data, client_ip, user_agent))
        return True
    else:
        print(f"[QUEUE FULL] Queue đầy, bỏ qua tin nhắn")
        return False

def process_facebook_message(data: dict, client_ip: str, user_agent: str):
    """
    Xử lý tin nhắn Facebook từ background worker
    Đây là hàm thay thế cho logic xử lý trong webhook trước đây
    """
    try:
        # Kiểm tra dữ liệu hợp lệ
        if not data or 'entry' not in data:
            print(f"[PROCESS MESSAGE] Dữ liệu không hợp lệ")
            return
        
        entries = data['entry']
        
        for entry in entries:
            # ============================================
            # XỬ LÝ SỰ KIỆN CHANGES (COMMENT TỪ FEED)
            # ============================================
            if 'changes' in entry:
                print(f"[PROCESS CHANGES] Phát hiện changes trong entry")
                changes = entry['changes']
                
                for change in changes:
                    field = change.get('field')
                    value = change.get('value', {})
                    
                    if field == 'feed':
                        print(f"[PROCESS FEED CHANGE] Xử lý feed change")
                        
                        # Kiểm tra xem có phải comment mới không
                        verb = value.get('verb', '')
                        if verb == 'add':
                            # Đây là comment mới trên post
                            print(f"[FEED COMMENT VIA CHANGES] Phát hiện comment mới từ feed")
                            
                            # Gọi hàm xử lý comment từ feed
                            try:
                                handle_feed_comment(value)
                            except Exception as e:
                                print(f"[FEED COMMENT PROCESS ERROR] Lỗi xử lý comment: {e}")
                        else:
                            print(f"[FEED CHANGE IGNORE] Bỏ qua change với verb: {verb}")
                    else:
                        print(f"[CHANGE IGNORE] Bỏ qua change field: {field}")
                
                # Đã xử lý changes, tiếp tục vòng lặp
                continue
            
            # ============================================
            # XỬ LÝ SỰ KIỆN MESSAGING (TIN NHẮN, POSTBACK)
            # ============================================
            if 'messaging' not in entry:
                continue
            
            messaging_events = entry['messaging']
            
            for event in messaging_events:
                # Lấy thông tin cơ bản - QUAN TRỌNG: LẤY CẢ SENDER VÀ RECIPIENT
                sender_id = event.get('sender', {}).get('id')
                recipient_id = event.get('recipient', {}).get('id')
                
                if not sender_id:
                    continue
                
                # ============================================
                # QUAN TRỌNG: XỬ LÝ ECHO CHỨA #MS TỪ PAGE - ĐÃ SỬA LỖI
                # ============================================
                if 'message' in event and event['message'].get('is_echo'):
                    echo_text = event['message'].get('text', '')
                    app_id = event['message'].get('app_id', '')
                    
                    # KIỂM TRA NẾU ECHO CHỨA #MS
                    if echo_text and "#MS" in echo_text.upper():
                        print(f"[ECHO WITH #MS DETECTED] Xử lý echo từ page chứa #MS: {echo_text[:100]}")
                        
                        # QUAN TRỌNG: DÙNG recipient_id (user) THAY VÌ sender_id (page)
                        # Nếu không có recipient_id, dùng sender_id (fallback)
                        target_user_id = recipient_id if recipient_id else sender_id
                        
                        # Trích xuất MS từ echo_text
                        referral_match = re.search(r'#MS(\d+)', echo_text.upper())
                        if referral_match:
                            ms_num = referral_match.group(1)
                            ms = f"MS{ms_num.zfill(6)}"
                            
                            # Kiểm tra sản phẩm tồn tại
                            load_products()
                            if ms in PRODUCTS:
                                # CẬP NHẬT CONTEXT CHO USER THỰC, KHÔNG PHẢI PAGE
                                update_context_with_new_ms(target_user_id, ms, "page_echo")
                                
                                # Lưu ngay vào Google Sheets
                                if target_user_id in USER_CONTEXT:
                                    ctx = USER_CONTEXT[target_user_id]
                                    threading.Thread(
                                        target=lambda: save_single_user_to_sheets(target_user_id, ctx),
                                        daemon=True
                                    ).start()
                                    
                                    print(f"[ECHO MS UPDATED] Đã cập nhật MS {ms} cho user {target_user_id} từ page echo")
                                else:
                                    print(f"[ECHO MS WARNING] User {target_user_id} chưa có trong USER_CONTEXT")
                            else:
                                print(f"[ECHO MS INVALID] MS {ms} không tồn tại trong hệ thống")
                        
                        # Bỏ qua xử lý tiếp theo
                        continue
                    else:
                        # Các echo khác vẫn bỏ qua như cũ
                        print(f"[ECHO SKIP] Bỏ qua echo message từ bot: {echo_text[:50]}")
                        continue
                
                # ============================================
                # XỬ LÝ POSTBACK TỪ USER (KHÔNG PHẢI ECHO)
                # ============================================
                if 'postback' in event:
                    payload = event['postback'].get('payload', '')
                    print(f"[POSTBACK PROCESS] User {sender_id}: {payload}")
                    
                    # Xử lý postback với lock
                    postback_lock = get_postback_lock(sender_id, payload)
                    with postback_lock:
                        handle_postback_with_recovery(sender_id, payload)
                    continue
                
                # ============================================
                # XỬ LÝ REFERRAL (từ catalog, ads)
                # ============================================
                if 'referral' in event:
                    referral_data = event['referral']
                    print(f"[REFERRAL PROCESS] User {sender_id}: {referral_data}")
                    
                    # Xử lý catalog referral
                    handle_catalog_referral(sender_id, referral_data)
                    continue
                
                # ============================================
                # XỬ LÝ MESSAGE TỪ USER (KHÔNG PHẢI ECHO)
                # ============================================
                if 'message' in event:
                    message_data = event['message']
                    mid = message_data.get('mid')
                    
                    # Kiểm tra idempotency với MID
                    if mid and is_message_processed(mid):
                        print(f"[DUPLICATE MID] Bỏ qua tin nhắn đã xử lý: {mid}")
                        continue
                    
                    # Kiểm tra xem tin nhắn này đang được xử lý chưa
                    if not mark_message_processing(sender_id, mid if mid else str(time.time())):
                        print(f"[PROCESSING CONFLICT] Tin nhắn đang được xử lý, bỏ qua")
                        continue
                    
                    try:
                        # Kiểm tra nếu là echo từ bot (đã xử lý ở trên)
                        app_id = message_data.get('app_id', '')
                        text_content = message_data.get('text', '')
                        attachments = message_data.get('attachments', [])
                        
                        # KHÔNG kiểm tra is_bot_generated_echo ở đây vì đã xử lý echo ở trên
                        # Chỉ cần kiểm tra app_id để tránh xử lý trùng
                        if app_id and app_id in BOT_APP_IDS and "#MS" not in (text_content or "").upper():
                            print(f"[BOT APP ID SKIP] Bỏ qua tin nhắn từ bot app_id: {app_id}")
                            mark_message_completed(sender_id, mid if mid else str(time.time()))
                            continue
                        
                        # Xử lý tin nhắn văn bản từ USER
                        if 'text' in message_data:
                            text = message_data['text'].strip()
                            print(f"[TEXT PROCESS] User {sender_id}: {text[:100]}")
                            
                            # Kiểm tra nếu là từ Fchat webhook hoặc page echo đã xử lý
                            if text.startswith('#'):
                                # Giả lập referral data cho Fchat
                                referral_match = re.search(r'#MS(\d+)', text.upper())
                                if referral_match:
                                    ms_num = referral_match.group(1)
                                    ms = f"MS{ms_num.zfill(6)}"
                                    if ms in PRODUCTS:
                                        # Cập nhật context với MS từ Fchat
                                        update_context_with_new_ms(sender_id, ms, "fchat_referral")
                                        # Gửi carousel
                                        send_single_product_carousel(sender_id, ms)
                                        # Dùng GPT trả lời nếu có câu hỏi
                                        if len(text) > 10:  # Nếu có thêm nội dung câu hỏi
                                            handle_text_with_function_calling(sender_id, text)
                                    else:
                                        send_message(sender_id, "Dạ, mã sản phẩm không tồn tại trong hệ thống ạ!")
                                else:
                                    send_message(sender_id, "Dạ, vui lòng cung cấp mã sản phẩm hợp lệ ạ!")
                            else:
                                # Xử lý text bình thường từ USER
                                handle_text(sender_id, text)
                        
                        # Xử lý tin nhắn hình ảnh từ USER
                        elif 'attachments' in message_data:
                            for attachment in message_data['attachments']:
                                if attachment.get('type') == 'image':
                                    image_url = attachment.get('payload', {}).get('url')
                                    if image_url:
                                        print(f"[IMAGE PROCESS] User {sender_id}: ảnh")
                                        handle_image(sender_id, image_url)
                                    break
                        
                        # Xử lý tin nhắn từ bài viết (feed comment) - cách cũ
                        elif 'referral' in message_data:
                            referral_data = message_data['referral']
                            if referral_data.get('source') == 'ADS_POST':
                                # Đây là comment từ bài viết
                                print(f"[FEED COMMENT VIA MESSAGE] Phát hiện comment từ bài viết")
                                handle_feed_comment(referral_data)
                        
                    except Exception as e:
                        print(f"[PROCESS ERROR] Lỗi xử lý tin nhắn cho {sender_id}: {e}")
                        import traceback
                        traceback.print_exc()
                        
                        # Gửi thông báo lỗi cho user
                        try:
                            send_message(sender_id, "Dạ em đang gặp chút trục trặc, anh/chị vui lòng thử lại sau ạ!")
                        except:
                            pass
                    
                    finally:
                        # Đánh dấu tin nhắn đã xử lý xong
                        mark_message_completed(sender_id, mid if mid else str(time.time()))
        
        print(f"[PROCESS COMPLETE] Đã xử lý xong batch tin nhắn")
        
    except Exception as e:
        print(f"[PROCESS MESSAGE ERROR] Lỗi tổng thể: {e}")
        import traceback
        traceback.print_exc()
        
# ============================================
# GOOGLE SHEETS CACHE
# ============================================

# Cache để giảm số lần gọi Google Sheets API
SHEETS_CACHE = {
    'last_read': 0,
    'cache_ttl': 30,  # Cache 30 giây
    'user_row_map': {},
    'existing_values': []
}

# ============================================
# ADDRESS API CACHE
# ============================================
ADDRESS_CACHE = {
    'provinces': None,
    'provinces_updated': 0,
    'districts': {},
    'wards': {},
    'cache_ttl': 3600  # 1 giờ
}

# ============================================
# CACHE CHO TÊN FANPAGE
# ============================================
FANPAGE_NAME_CACHE = None
FANPAGE_NAME_CACHE_TIME = 0
FANPAGE_NAME_CACHE_TTL = 3600

def get_fanpage_name_from_api():
    global FANPAGE_NAME_CACHE, FANPAGE_NAME_CACHE_TIME
    
    now = time.time()
    if (FANPAGE_NAME_CACHE and 
        FANPAGE_NAME_CACHE_TIME and 
        (now - FANPAGE_NAME_CACHE_TIME) < FANPAGE_NAME_CACHE_TTL):
        return FANPAGE_NAME_CACHE
    
    if not PAGE_ACCESS_TOKEN:
        FANPAGE_NAME_CACHE = FANPAGE_NAME
        FANPAGE_NAME_CACHE_TIME = now
        return FANPAGE_NAME_CACHE
    
    try:
        url = f"https://graph.facebook.com/v12.0/me?fields=name&access_token={PAGE_ACCESS_TOKEN}"
        response = requests.get(url, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            page_name = data.get('name', FANPAGE_NAME)
            FANPAGE_NAME_CACHE = page_name
            FANPAGE_NAME_CACHE_TIME = now
            return page_name
        else:
            FANPAGE_NAME_CACHE = FANPAGE_NAME
            FANPAGE_NAME_CACHE_TIME = now
            return FANPAGE_NAME_CACHE
    except Exception as e:
        FANPAGE_NAME_CACHE = FANPAGE_NAME
        FANPAGE_NAME_CACHE_TIME = now
        return FANPAGE_NAME_CACHE

# ============================================
# HÀM TẠO TIN NHẮN TIẾP THỊ BẰNG GPT
# ============================================
def generate_marketing_message(ms: str, user_name: str) -> str:
    """
    Tạo tin nhắn tiếp thị bằng GPT dựa trên ưu điểm sản phẩm
    """
    if ms not in PRODUCTS:
        return None
    
    product = PRODUCTS[ms]
    product_name = product.get('Ten', '')
    # Làm sạch tên sản phẩm (loại bỏ mã nếu có)
    if f"[{ms}]" in product_name or ms in product_name:
        product_name = product_name.replace(f"[{ms}]", "").replace(ms, "").strip()
    
    mo_ta = product.get("MoTa", "")
    
    if not client:
        # Fallback nếu không có GPT
        return f"Chào {user_name}! 👋\n\nEm thấy ac đã bình luận trên bài viết của shop và quan tâm đến sản phẩm:\n\n📦 **{product_name}**\n📌 Mã sản phẩm: {ms}\n\nĐây là sản phẩm rất được yêu thích tại shop với nhiều ưu điểm nổi bật! ac có thể hỏi em bất kỳ thông tin gì về sản phẩm này ạ!"
    
    try:
        system_prompt = f"""Bạn là nhân viên bán hàng của {get_fanpage_name_from_api()}.
Hãy tạo một lời chào mời khách hàng dựa trên sản phẩm {product_name} (mã {ms}).
Lời chào cần:
1. Thân thiện, nhiệt tình, chào đón khách hàng
2. Nhấn mạnh vào ưu điểm, điểm nổi bật của sản phẩm dựa trên mô tả
3. Mời gọi khách hàng hỏi thêm thông tin hoặc đặt hàng
4. KHÔNG liệt kê các câu lệnh như "gửi giá bao nhiêu", "xem ảnh", v.v.
5. KHÔNG hướng dẫn khách cách hỏi
6. Tập trung vào ưu điểm và lợi ích sản phẩm
7. Độ dài khoảng 4-5 dòng, tự nhiên
"""
        
        user_prompt = f"""Hãy tạo lời chào cho khách hàng {user_name} vừa bình luận trên bài viết về sản phẩm:
Tên sản phẩm: {product_name}
Mã sản phẩm: {ms}
Mô tả sản phẩm: {mo_ta[:300] if mo_ta else "Chưa có mô tả"}

Hãy tạo lời chào mời thân thiện, tập trung vào ưu điểm sản phẩm."""
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.7,
            max_tokens=300
        )
        
        intro = response.choices[0].message.content
        return f"Chào {user_name}! 👋\n\n{intro}"
    
    except Exception as e:
        print(f"[GPT MARKETING ERROR] Lỗi khi tạo tin nhắn tiếp thị: {e}")
        # Fallback
        return f"Chào {user_name}! 👋\n\nEm thấy ac đã bình luận trên bài viết của shop và quan tâm đến sản phẩm:\n\n📦 **{product_name}**\n📌 Mã sản phẩm: {ms}\n\nĐây là sản phẩm rất được yêu thích tại shop với nhiều ưu điểm nổi bật! ac có thể hỏi em bất kỳ thông tin gì về sản phẩm này ạ!"

def generate_comment_reply_by_gpt(comment_text: str, user_name: str, product_name: str = None, ms: str = None) -> str:
    """
    Tạo nội dung trả lời bình luận bằng GPT
    Dựa trên Website từ PRODUCTS để quyết định nội dung
    """
    # Lấy thông tin website từ PRODUCTS
    website = ""
    if ms and ms in PRODUCTS:
        website = PRODUCTS[ms].get('Website', '')
    
    if not client:
        # Fallback nếu không có GPT
        if website and website.startswith(('http://', 'https://')):
            return f"Cảm ơn {user_name} đã quan tâm! Bạn có thể xem chi tiết sản phẩm và đặt hàng tại: {website}"
        else:
            return f"Cảm ơn {user_name} đã quan tâm! Vui lòng nhắn tin trực tiếp cho page để được tư vấn chi tiết ạ!"
    
    try:
        fanpage_name = get_fanpage_name_from_api()
        
        # Xác định hướng trả lời dựa trên website
        if website and website.startswith(('http://', 'https://')):
            direction = f"Hãy hướng dẫn khách click vào link: {website} để xem chi tiết sản phẩm và đặt hàng."
            context = "Có website để khách hàng truy cập"
        else:
            direction = "Hãy mời khách hàng nhắn tin trực tiếp (inbox) cho page để được tư vấn chi tiết, đo đạc size và đặt hàng."
            context = "Không có website, cần hướng dẫn khách vào inbox"
        
        system_prompt = f"""Bạn là nhân viên bán hàng của {fanpage_name}.
Hãy trả lời bình luận của khách hàng một cách thân thiện, chuyên nghiệp.

QUY TẮC QUAN TRỌNG:
1. {direction}
2. Ngắn gọn, không quá 3 dòng
3. Thân thiện, nhiệt tình
4. KHÔNG được đề cập đến mã sản phẩm (MS) trong câu trả lời
5. KHÔNG được hướng dẫn cách đặt hàng phức tạp
6. KHÔNG được yêu cầu khách cung cấp thông tin cá nhân
7. Chỉ tập trung vào việc hướng dẫn click link website hoặc vào inbox

Ngữ cảnh: {context}
Khách hàng: {user_name}
Bình luận: "{comment_text}"
"""
        
        user_prompt = f"""Hãy tạo câu trả lời cho bình luận của khách hàng {user_name}:
"{comment_text}"

Yêu cầu: {direction}"""

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.7,
            max_tokens=200
        )
        
        reply = response.choices[0].message.content.strip()
        
        # Đảm bảo reply không rỗng
        if not reply:
            if website and website.startswith(('http://', 'https://')):
                reply = f"Cảm ơn {user_name} đã quan tâm! Bạn có thể click vào link: {website} để xem chi tiết sản phẩm và đặt hàng."
            else:
                reply = f"Cảm ơn {user_name} đã quan tâm! Vui lòng nhắn tin trực tiếp cho page để được tư vấn chi tiết ạ!"
        
        return reply
    
    except Exception as e:
        print(f"[GPT COMMENT REPLY ERROR] Lỗi khi tạo trả lời bình luận: {e}")
        # Fallback
        if website and website.startswith(('http://', 'https://')):
            return f"Cảm ơn {user_name} đã quan tâm! Bạn có thể click vào link: {website} để xem chi tiết sản phẩm và đặt hàng."
        else:
            return f"Cảm ơn {user_name} đã quan tâm! Vui lòng nhắn tin trực tiếp cho page để được tư vấn chi tiết ạ!"

def reply_to_facebook_comment(comment_id: str, message: str):
    """
    Gửi trả lời bình luận lên Facebook Graph API VỚI RETRY
    """
    if not PAGE_ACCESS_TOKEN:
        print(f"[REPLY COMMENT ERROR] Thiếu PAGE_ACCESS_TOKEN")
        return False
    
    if not comment_id:
        print(f"[REPLY COMMENT ERROR] Thiếu comment_id")
        return False
    
    max_retries = 3
    base_delay = 2  # giây
    
    for attempt in range(max_retries):
        try:
            # Graph API endpoint để trả lời comment
            url = f"https://graph.facebook.com/v18.0/{comment_id}/comments"
            
            params = {
                'access_token': PAGE_ACCESS_TOKEN,
                'message': message
            }
            
            print(f"[REPLY COMMENT] Attempt {attempt + 1}/{max_retries} - Đang gửi trả lời bình luận {comment_id}")
            
            # Giảm timeout xuống 5 giây nhưng có retry
            response = requests.post(url, params=params, timeout=5)
            
            if response.status_code == 200:
                print(f"[REPLY COMMENT SUCCESS] Đã gửi trả lời bình luận {comment_id}")
                return True
            else:
                print(f"[REPLY COMMENT ERROR] Lỗi {response.status_code}: {response.text[:200]}")
                
                # Kiểm tra các lỗi không thể retry
                if response.status_code in [400, 403, 404]:
                    error_data = response.json().get('error', {})
                    error_message = error_data.get('message', '')
                    
                    # Không retry với các lỗi này
                    if "does not exist" in error_message or "permission" in error_message:
                        print(f"[REPLY COMMENT] Comment không tồn tại hoặc không có quyền, bỏ qua")
                        return False
                
                # Nếu không phải lần thử cuối, đợi rồi thử lại
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)  # Exponential backoff
                    print(f"[REPLY COMMENT RETRY] Đợi {delay} giây trước khi thử lại...")
                    time.sleep(delay)
                    
        except requests.exceptions.Timeout:
            print(f"[REPLY COMMENT TIMEOUT] Timeout lần {attempt + 1}")
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                print(f"[REPLY COMMENT RETRY] Đợi {delay} giây trước khi thử lại...")
                time.sleep(delay)
                continue
            else:
                print(f"[REPLY COMMENT FINAL TIMEOUT] Đã thử {max_retries} lần nhưng vẫn timeout")
                return False
                
        except Exception as e:
            print(f"[REPLY COMMENT EXCEPTION] Lỗi khi gửi trả lời bình luận: {e}")
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                print(f"[REPLY COMMENT RETRY] Đợi {delay} giây trước khi thử lại...")
                time.sleep(delay)
            else:
                return False
    
    return False
        
# ============================================
# HÀM CẬP NHẬT CONTEXT VỚI MS MỚI VÀ RESET COUNTER
# ============================================

def update_context_with_new_ms(uid: str, new_ms: str, source: str = "unknown"):
    """
    Cập nhật context với MS mới và reset counter để đảm bảo bot gửi carousel
    cho sản phẩm mới khi user gửi tin nhắn đầu tiên
    LƯU NGAY VÀO GOOGLE SHEETS khi MS thay đổi
    """
    if not new_ms:
        return False
    
    ctx = USER_CONTEXT[uid]
    
    # Lấy MS cũ để so sánh
    old_ms = ctx.get("last_ms")
    
    # Nếu MS mới khác với MS cũ, reset counter
    if old_ms != new_ms:
        print(f"[CONTEXT UPDATE] User {uid}: Chuyển từ {old_ms} sang {new_ms} (nguồn: {source})")
        
        # Reset COMPLETE để bot gửi carousel cho sản phẩm mới
        ctx["real_message_count"] = 0
        ctx["has_sent_first_carousel"] = False  # QUAN TRỌNG: reset này!
        ctx["last_msg_time"] = 0  # Reset thời gian tin nhắn cuối
        ctx["last_processed_text"] = ""  # Reset text đã xử lý
    else:
        print(f"[CONTEXT NO CHANGE] User {uid}: Vẫn giữ MS {new_ms} (nguồn: {source})")
    
    # Cập nhật MS mới
    ctx["last_ms"] = new_ms
    ctx["referral_source"] = source
    
    # Gọi hàm update_product_context cũ
    if "product_history" not in ctx:
        ctx["product_history"] = []
    
    if not ctx["product_history"] or ctx["product_history"][0] != new_ms:
        if new_ms in ctx["product_history"]:
            ctx["product_history"].remove(new_ms)
        ctx["product_history"].insert(0, new_ms)
    
    if len(ctx["product_history"]) > 5:
        ctx["product_history"] = ctx["product_history"][:5]
    
    # Cập nhật thời gian
    ctx["last_updated"] = time.time()
    ctx["dirty"] = True  # ← THÊM DÒNG NÀY
    
    # ============================================
    # QUAN TRỌNG: LƯU NGAY VÀO GOOGLE SHEETS KHI MS THAY ĐỔI
    # ============================================
    def save_immediately():
        try:
            print(f"[IMMEDIATE SAVE] Đang lưu ngay context cho user {uid} với MS {new_ms}...")
            # Gọi hàm save_single_user_to_sheets trực tiếp
            save_single_user_to_sheets(uid, ctx)
            print(f"[IMMEDIATE SAVE COMPLETE] Đã lưu xong user {uid} vào Google Sheets")
        except Exception as e:
            print(f"[IMMEDIATE SAVE ERROR] Lỗi khi lưu ngay user {uid}: {e}")
    
    # Chạy trong thread riêng để không block bot
    threading.Thread(target=save_immediately, daemon=True).start()
    # ============================================
    
    print(f"[CONTEXT UPDATE COMPLETE] Đã cập nhật MS {new_ms} cho user {uid} (nguồn: {source}, real_message_count: {ctx['real_message_count']}, has_sent_first_carousel: {ctx['has_sent_first_carousel']})")
    
    return True
    
def restore_user_context_on_wakeup(uid: str):
    """Khôi phục context cho user khi app wake up từ sleep - ƯU TIÊN LOAD TỪ SHEETS"""
    # 1. Thử load từ USER_CONTEXT trong RAM (nếu còn)
    if uid in USER_CONTEXT and USER_CONTEXT[uid].get("last_ms"):
        print(f"[RESTORE CONTEXT] User {uid} đã có context trong RAM")
        return True
    
    # 2. Thử load từ Google Sheets (ƯU TIÊN MỚI)
    context_from_sheets = get_user_context_from_sheets(uid)
    if context_from_sheets:
        USER_CONTEXT[uid] = context_from_sheets
        print(f"[RESTORE CONTEXT] Đã khôi phục context cho user {uid} từ Google Sheets")
        return True
    
    # 3. Thử tra cứu đơn hàng từ Google Sheets (Orders sheet)
    orders = get_user_order_history_from_sheets(uid)
    
    if orders:
        latest_order = orders[0]
        last_ms = latest_order.get("ms")
        
        if last_ms and last_ms in PRODUCTS:
            # Cập nhật context với MS từ đơn hàng
            update_context_with_new_ms(uid, last_ms, "restored_from_order_history")
            
            # Lấy thông tin khách hàng
            ctx = USER_CONTEXT[uid]
            ctx["order_data"] = {
                "phone": latest_order.get("phone", ""),
                "customer_name": latest_order.get("customer_name", "")
            }
            
            print(f"[RESTORE CONTEXT] Đã khôi phục context cho user {uid} từ đơn hàng: {last_ms}")
            return True
    
    # 4. Thử tìm bằng số điện thoại trong context của user khác
    for other_uid, other_ctx in USER_CONTEXT.items():
        if other_uid != uid and other_ctx.get("order_data", {}).get("phone"):
            # Kiểm tra xem có đơn hàng nào với số điện thoại này không
            phone = other_ctx["order_data"]["phone"]
            if phone:
                orders_by_phone = get_user_order_history_from_sheets(None, phone)
                if orders_by_phone:
                    latest_order = orders_by_phone[0]
                    last_ms = latest_order.get("ms")
                    
                    if last_ms and last_ms in PRODUCTS:
                        # Cập nhật context
                        update_context_with_new_ms(uid, last_ms, "restored_by_phone_match")
                        
                        # Copy order_data từ user khác
                        ctx = USER_CONTEXT[uid]
                        ctx["order_data"] = other_ctx["order_data"].copy()
                        
                        print(f"[RESTORE CONTEXT] Đã khôi phục context cho user {uid} bằng số điện thoại: {phone}")
                        return True
    
    print(f"[RESTORE CONTEXT] Không thể khôi phục context cho user {uid}")
    return False

# ============================================
# HÀM PHÁT HIỆN EMOJI/STICKER
# ============================================

def is_emoji_or_sticker_image(image_url: str) -> bool:
    """
    Phát hiện ảnh emoji/sticker dựa trên URL
    """
    if not image_url:
        return True
    
    image_url_lower = image_url.lower()
    
    # Kiểm tra từ khóa đặc trưng của emoji/sticker Facebook
    emoji_keywords = [
        'emoji', 'sticker', 'stickers', 'stickerpack',
        'facebook.com/images/stickers/',
        'fbcdn.net/images/emoji.php',
        'graph.facebook.com/sticker',
        'scontent.xx.fbcdn.net/v/t39.1997-6/',  # Đường dẫn sticker Facebook
        'cdn.jsdelivr.net/emojione/assets',  # Emojione
        'twemoji.maxcdn.com',  # Twemoji
        'noto-website-2.storage.googleapis.com',  # Noto Emoji
    ]
    
    for keyword in emoji_keywords:
        if keyword in image_url_lower:
            return True
    
    # Kiểm tra đuôi file - emoji thường là SVG hoặc định dạng đặc biệt
    emoji_extensions = ['.svg', '.svgs', '.svgz', '.gif', '.apng', '.webp']
    
    for ext in emoji_extensions:
        if image_url_lower.endswith(ext):
            return True
    
    # Kiểm tra pattern URL đặc biệt
    emoji_patterns = [
        r'emoji_\d+\.(png|jpg|gif)',
        r'sticker_\d+\.(png|jpg|gif)',
        r'emoji/[\w\-]+\.(png|jpg|gif)',
        r'stickers/[\w\-]+\.(png|jpg|gif)',
    ]
    
    for pattern in emoji_patterns:
        if re.search(pattern, image_url_lower):
            return True
    
    return False

# ============================================
# HÀM KIỂM TRA ẢNH SẢN PHẨM HỢP LỆ (CẢI TIẾN)
# ============================================

def is_valid_product_image(image_url: str) -> bool:
    """
    Kiểm tra xem ảnh có phải là ảnh sản phẩm hợp lệ không
    Cải tiến để chấp nhận nhiều định dạng URL hơn
    """
    if not image_url:
        return False
    
    image_url_lower = image_url.lower()
    
    # Kiểm tra đuôi file ảnh hợp lệ
    valid_extensions = ['.jpg', '.jpeg', '.png', '.webp', '.gif', '.bmp', '.tiff']
    
    for ext in valid_extensions:
        if ext in image_url_lower:
            return True
    
    # Kiểm tra domain ảnh phổ biến (bao gồm cả Facebook)
    valid_domains = [
        'fbcdn.net', 'scontent.xx', 'scontent.fhan', 'cdn.shopify', 
        'static.nike', 'lzd-img', 'shopee', 'tiki', 'content.pancake.vn',
        'instagram.com', 'cloudinary.com', 'images.unsplash.com',
        'graph.facebook.com', 'facebook.com'
    ]
    
    for domain in valid_domains:
        if domain in image_url_lower:
            return True
    
    # Kiểm tra pattern URL chứa thông tin ảnh
    image_patterns = [
        r'\.(jpg|jpeg|png|webp|gif)(\?|$)',
        r'/photos/',
        r'/images/',
        r'/img/',
        r'/picture/',
        r'/media/',
        r'/upload/'
    ]
    
    for pattern in image_patterns:
        if re.search(pattern, image_url_lower):
            return True
    
    return False

# ============================================
# HÀM TẢI ẢNH VỀ SERVER VÀ CHUYỂN THÀNH BASE64
# ============================================

def download_image_to_base64(image_url: str) -> Optional[str]:
    """
    Tải ảnh từ URL và chuyển thành chuỗi base64.
    Trả về None nếu không tải được.
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'image/webp,image/*,*/*;q=0.8',
            'Accept-Language': 'vi,en-US;q=0.9,en;q=0.8',
            'Referer': 'https://www.facebook.com/'
        }
        
        # Thử tải với timeout ngắn
        response = requests.get(image_url, headers=headers, timeout=5)
        
        if response.status_code == 200:
            # Kiểm tra content type có phải là ảnh không
            content_type = response.headers.get('content-type', '').lower()
            if 'image' in content_type:
                image_data = response.content
                base64_str = base64.b64encode(image_data).decode('utf-8')
                return base64_str
            else:
                print(f"[IMAGE DOWNLOAD] Không phải ảnh: {content_type}")
        else:
            print(f"[IMAGE DOWNLOAD] Lỗi HTTP: {response.status_code}")
    except Exception as e:
        print(f"[IMAGE DOWNLOAD] Lỗi khi tải ảnh: {e}")
    return None

# ============================================
# HÀM PHÂN TÍCH ẢNH BẰNG OPENAI VISION API (CẢI TIẾN)
# ============================================

def analyze_image_with_vision_api(image_url: str) -> str:
    """
    Phân tích ảnh bằng OpenAI Vision API và trả về mô tả text
    Sử dụng base64 để tránh lỗi URL không tải được
    """
    if not client:
        return ""
    
    print(f"[VISION API] Đang phân tích ảnh: {image_url[:100]}...")
    
    try:
        # THỬ 1: Dùng URL trực tiếp (nhanh nhất)
        print(f"[VISION API] Thử dùng URL trực tiếp...")
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": """Bạn là chuyên gia nhận diện sản phẩm thời trang. Hãy mô tả CHI TIẾT và CHÍNH XÁC sản phẩm trong ảnh theo các tiêu chí:

1. LOẠI SẢN PHẨM (bắt buộc): áo thun, áo sơ mi, váy, quần jeans, áo khoác, đầm, v.v.
2. MÀU SẮC CHÍNH (bắt buộc): đỏ, xanh, trắng, đen, hồng, tím, v.v.
3. CHẤT LIỆU (nếu thấy): cotton, linen, jean, lụa, v.v.
4. HỌA TIẾT: trơn, sọc, caro, hoa, hình in, logo, v.v.
5. KIỂU DÁNG: cổ tròn, cổ tim, tay dài, tay ngắn, ôm body, rộng, v.v.
6. ĐẶC ĐIỂM NỔI BẬT: túi, nút, dây kéo, viền, đính đá, v.v.
7. PHONG CÁCH: casual, công sở, dạo phố, dự tiệc, thể thao, v.v.

MÔ TẢ PHẢI NGẮN GỌN nhưng ĐẦY ĐỦ từ khóa quan trọng. Ưu tiên từ khóa thông dụng trong thời trang."""},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": image_url,
                                "detail": "auto"
                            }
                        }
                    ]
                }
            ],
            max_tokens=500,
            temperature=0.1
        )
        
        return response.choices[0].message.content
    except Exception as e:
        print(f"[VISION API URL ERROR] Lỗi khi dùng URL: {e}")
        
        # THỬ 2: Tải ảnh về và dùng base64
        print(f"[VISION API] Đang tải ảnh về để chuyển base64...")
        base64_image = download_image_to_base64(image_url)
        
        if base64_image:
            try:
                print(f"[VISION API] Thử dùng base64...")
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {
                            "role": "user",
                            "content": [
                                {"type": "text", "text": """Mô tả chi tiết sản phẩm trong ảnh, tập trung vào loại sản phẩm, màu sắc, chất liệu, họa tiết và phong cách."""},
                                {
                                    "type": "image_url",
                                    "image_url": {
                                        "url": f"data:image/jpeg;base64,{base64_image}"
                                    }
                                }
                            ]
                        }
                    ],
                    max_tokens=400,
                    temperature=0.1
                )
                return response.choices[0].message.content
            except Exception as e2:
                print(f"[VISION API BASE64 ERROR] Lỗi khi dùng base64: {e2}")
        
        # THỬ 3: Dùng URL đơn giản hóa
        try:
            print(f"[VISION API] Thử dùng URL đơn giản hóa...")
            # Lấy phần base URL không có tham số phức tạp
            simple_url = image_url.split('?')[0]
            if 'fbcdn.net' in simple_url:
                simple_url = simple_url + '?dl=1'
            
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": "Mô tả ngắn sản phẩm trong ảnh."},
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": simple_url,
                                    "detail": "low"
                                }
                            }
                        ]
                    }
                ],
                max_tokens=300,
                temperature=0.1
            )
            return response.choices[0].message.content
        except Exception as e3:
            print(f"[VISION API SIMPLE URL ERROR] Lỗi cuối cùng: {e3}")
    
    return ""

# ============================================
# HÀM TRÍCH XUẤT TỪ KHÓA TỪ MÔ TẢ
# ============================================

def extract_keywords_from_description(description: str) -> set:
    """Trích xuất từ khóa quan trọng từ mô tả"""
    stop_words = {'của', 'và', 'là', 'có', 'trong', 'với', 'cho', 'từ', 'này', 'ảnh', 
                  'sản phẩm', 'phẩm', 'chụp', 'nhìn', 'thấy', 'rất', 'một', 'như', 
                  'bởi', 'các', 'được', 'nên', 'khi', 'hoặc', 'nếu', 'thì', 'mà'}
    
    # Từ khóa quan trọng trong thời trang
    fashion_keywords = {
        'áo', 'quần', 'váy', 'đầm', 'áo thun', 'áo sơ mi', 'jeans', 'khoác', 
        'hoodie', 'sweater', 'jacket', 'blazer', 'cardigan', 'polo', 'tank top',
        'shorts', 'skirt', 'jumpsuit', 'romper', 'leggings', 'jogger'
    }
    
    keywords = set()
    words = description.split()
    
    for word in words:
        word = word.strip('.,!?;:()[]{}"\'').lower()
        if len(word) > 2 and word not in stop_words:
            keywords.add(word)
    
    # Thêm các từ khóa ghép (2-3 từ)
    for i in range(len(words) - 1):
        bigram = f"{words[i]} {words[i+1]}"
        if any(keyword in bigram for keyword in fashion_keywords):
            keywords.add(bigram)
    
    return keywords

# ============================================
# HÀM TÍNH ĐIỂM TƯƠNG ĐỔNG SẢN PHẨM
# ============================================

def calculate_product_similarity_score(ms: str, product: dict, desc_lower: str, desc_keywords: set) -> float:
    """Tính điểm tương đồng giữa sản phẩm và mô tả ảnh"""
    score = 0
    
    # Lấy thông tin sản phẩm
    ten = normalize_vietnamese(product.get("Ten", "").lower())
    mo_ta = normalize_vietnamese(product.get("MoTa", "").lower())
    mau_sac = normalize_vietnamese(product.get("màu (Thuộc tính)", "").lower())
    thuoc_tinh = normalize_vietnamese(product.get("Thuộc tính", "").lower())
    
    # Tạo bộ từ khóa sản phẩm
    product_keywords = set()
    
    # Thêm từ khóa từ tên sản phẩm
    for word in ten.split():
        if len(word) > 1:
            product_keywords.add(word)
    
    # Thêm từ khóa từ mô tả
    for word in mo_ta.split()[:50]:
        word = word.strip('.,!?;:()[]{}"\'').lower()
        if len(word) > 1:
            product_keywords.add(word)
    
    # Thêm màu sắc
    if mau_sac:
        for color in mau_sac.split(','):
            color_clean = color.strip().lower()
            if color_clean:
                product_keywords.add(color_clean)
    
    # Thêm thuộc tính
    if thuoc_tinh:
        for attr in thuoc_tinh.split(','):
            attr_clean = attr.strip().lower()
            if attr_clean:
                product_keywords.add(attr_clean)
    
    # Tính điểm: từ khóa trùng nhau
    common_keywords = desc_keywords.intersection(product_keywords)
    score += len(common_keywords) * 3  # Trọng số cao cho từ khóa trùng
    
    # Ưu tiên các từ khóa quan trọng (loại sản phẩm)
    fashion_keywords = {'áo', 'quần', 'váy', 'đầm', 'áo thun', 'áo sơ mi', 'jeans', 
                       'khoác', 'hoodie', 'sweater', 'jacket', 'blazer'}
    
    for keyword in fashion_keywords:
        if keyword in desc_lower and keyword in ten.lower():
            score += 8  # Trọng số rất cao cho loại sản phẩm trùng
    
    # Ưu tiên màu sắc trùng khớp
    if mau_sac:
        for color in mau_sac.split(','):
            color_clean = color.strip().lower()
            if color_clean in desc_lower:
                score += 5  # Trọng số cao cho màu sắc trùng
    
    # Kiểm tra xem tên sản phẩm có trong mô tả ảnh không
    for word in ten.split():
        if len(word) > 3 and word in desc_lower:
            score += 4
    
    return score

# ============================================
# HÀM TÌM SẢN PHẨM BẰNG MÔ TẢ ẢNH (CẢI TIẾN NÂNG CAO)
# ============================================

def find_product_by_image_description_enhanced(description: str) -> Optional[str]:
    """
    Tìm sản phẩm phù hợp nhất dựa trên mô tả ảnh - CẢI TIẾN NÂNG CAO
    """
    load_products()
    
    if not description or not PRODUCTS:
        return None
    
    # Chuẩn hóa mô tả ảnh
    desc_lower = normalize_vietnamese(description.lower())
    print(f"[IMAGE MATCH ENHANCED] Mô tả ảnh: {desc_lower[:200]}...")
    
    # Tạo danh sách từ khóa quan trọng từ mô tả ảnh
    desc_keywords = extract_keywords_from_description(desc_lower)
    
    # Tìm kiếm sản phẩm với điểm số cải tiến
    product_scores = {}
    
    for ms, product in PRODUCTS.items():
        score = calculate_product_similarity_score(ms, product, desc_lower, desc_keywords)
        
        if score > 0:
            product_scores[ms] = score
    
    if not product_scores:
        print("[IMAGE MATCH] Không tìm thấy sản phẩm nào phù hợp")
        return None
    
    # Sắp xếp theo điểm cao nhất
    sorted_products = sorted(product_scores.items(), key=lambda x: x[1], reverse=True)
    
    # Lấy sản phẩm có điểm cao nhất
    best_ms, best_score = sorted_products[0]
    
    print(f"[IMAGE MATCH SCORES] Điểm cao nhất: {best_ms} với {best_score} điểm")
    
    # Ngưỡng tối thiểu: cần ít nhất 5 điểm để coi là phù hợp
    if best_score >= 5:
        product_name = PRODUCTS[best_ms].get("Ten", "")
        print(f"[IMAGE MATCH SUCCESS] Tìm thấy {best_ms} - {product_name}")
        return best_ms
    
    print(f"[IMAGE MATCH FAIL] Điểm quá thấp: {best_score}")
    return None

# ============================================
# HÀM GỬI CAROUSEL GỢI Ý SẢN PHẨM
# ============================================

def send_suggestion_carousel(uid: str, suggestion_count: int = 3):
    """
    Gửi carousel gợi ý các sản phẩm phổ biến
    """
    load_products()
    
    if not PRODUCTS:
        send_message(uid, "Hiện tại chưa có sản phẩm nào trong hệ thống.")
        return False
    
    # Lấy danh sách sản phẩm (ưu tiên sản phẩm có ảnh)
    valid_products = []
    for ms, product in PRODUCTS.items():
        images_field = product.get("Images", "")
        urls = parse_image_urls(images_field)
        if urls:  # Chỉ lấy sản phẩm có ảnh
            valid_products.append(ms)
    
    # Nếu không đủ sản phẩm có ảnh, lấy tất cả
    if len(valid_products) < suggestion_count:
        valid_products = list(PRODUCTS.keys())
    
    # Lấy ngẫu nhiên hoặc lấy sản phẩm đầu tiên
    suggestion_products = valid_products[:suggestion_count]
    
    elements = []
    for ms in suggestion_products:
        product = PRODUCTS[ms]
        images_field = product.get("Images", "")
        urls = parse_image_urls(images_field)
        image_url = urls[0] if urls else ""
        
        gia_int = extract_price_int(product.get("Gia", "")) or 0
        
        # LẤY TÊN SẢN PHẨM (KHÔNG BAO GỒM MÃ SẢN PHẨM)
        product_name = product.get('Ten', '')
        
        # KIỂM TRA NẾU TÊN ĐÃ CHỨA MÃ SẢN PHẨM, CHỈ GIỮ TÊN
        if f"[{ms}]" in product_name or ms in product_name:
            # Xóa mã sản phẩm khỏi tên
            product_name = product_name.replace(f"[{ms}]", "").replace(ms, "").strip()
        
        element = {
            "title": product_name,  # CHỈ HIỂN THỊ TÊN SẢN PHẨM
            "image_url": image_url,
            "subtitle": f"💰 Giá: {gia_int:,.0f} đ",
            "buttons": [
                {
                    "type": "postback",
                    "title": "🌟 Ưu điểm SP",
                    "payload": f"PRODUCT_HIGHLIGHTS_{ms}"
                },
                {
                    "type": "postback", 
                    "title": "🖼️ Xem ảnh",
                    "payload": f"VIEW_IMAGES_{ms}"
                },
                {
                    "type": "web_url",
                    "title": "🛒 Đặt ngay",
                    "url": f"https://{DOMAIN}/messenger-order?ms={ms}&uid={uid}",
                    "webview_height_ratio": "tall",
                    "messenger_extensions": True,
                    "webview_share_button": "hide"
                }
            ]
        }
        elements.append(element)
    
    if elements:
        send_carousel_template(uid, elements)
        return True
    return False

# ============================================
# HÀM TÌM SẢN PHẨM TỪ ẢNH (CẢI TIẾN MỚI)
# ============================================

def find_product_by_image(image_url: str) -> Optional[str]:
    """
    Tìm sản phẩm từ ảnh bằng cách sử dụng Vision API để lấy mô tả,
    sau đó so khớp mô tả với tên và mô tả sản phẩm trong database.
    Trả về mã sản phẩm (MS) nếu tìm thấy, ngược lại trả về None.
    """
    # Bước 1: Kiểm tra xem có phải emoji/sticker không
    if is_emoji_or_sticker_image(image_url):
        print(f"[IMAGE CHECK] Đây là emoji/sticker, bỏ qua")
        return None
    
    # Bước 1.5: Kiểm tra ảnh có hợp lệ không
    if not is_valid_product_image(image_url):
        print(f"[INVALID IMAGE] Ảnh không hợp lệ: {image_url[:100]}")
        return None
    
    # Bước 2: Phân tích ảnh để lấy mô tả
    print(f"[IMAGE PROCESS] Đang phân tích ảnh bằng Vision API...")
    image_description = analyze_image_with_vision_api(image_url)
    
    if not image_description:
        print(f"[IMAGE PROCESS] Không thể phân tích ảnh")
        return None
    
    print(f"[IMAGE DESCRIPTION] {image_description[:300]}...")
    
    # Bước 3: Tìm sản phẩm phù hợp với mô tả
    found_ms = find_product_by_image_description_enhanced(image_description)
    
    if found_ms:
        print(f"[IMAGE MATCH] Tìm thấy sản phẩm {found_ms} từ ảnh")
        return found_ms
    
    print(f"[IMAGE MATCH] Không tìm thấy sản phẩm phù hợp")
    return None

# ============================================
# HELPER: TRÍCH XUẤT MÃ SẢN PHẨM
# ============================================

def extract_ms_from_retailer_id(retailer_id: str) -> Optional[str]:
    if not retailer_id:
        return None
    
    parts = retailer_id.split('_')
    if not parts:
        return None
    
    base_id = parts[0].upper()
    if re.match(r'MS\d{6}', base_id):
        return base_id
    
    match = re.search(r'MS(\d+)', base_id)
    if match:
        num = match.group(1)
        num_6 = num.zfill(6)
        return "MS" + num_6
    
    return None

def extract_ms_from_ad_title(ad_title: str) -> Optional[str]:
    if not ad_title:
        return None
    
    ad_title_lower = ad_title.lower()
    
    match = re.search(r'mã\s*(\d{1,6})', ad_title_lower)
    if match:
        num = match.group(1)
        num_6 = num.zfill(6)
        return "MS" + num_6
    
    match = re.search(r'ms\s*(\d{1,6})', ad_title_lower)
    if match:
        num = match.group(1)
        num_6 = num.zfill(6)
        return "MS" + num_6
    
    match = re.search(r'\b(\d{2,6})\b', ad_title)
    if match:
        num = match.group(1)
        num_6 = num.zfill(6)
        return "MS" + num_6
    
    return None

# ============================================
# HELPER: KIỂM TRA ECHO MESSAGE (ĐÃ CẢI THIỆN)
# ============================================

def is_bot_generated_echo(echo_text: str, app_id: str = "", attachments: list = None) -> bool:
    """
    Kiểm tra xem tin nhắn có phải là echo từ bot không
    Cải tiến để phát hiện chính xác hơn
    """
    # 1. Kiểm tra app_id (ưu tiên cao nhất)
    if app_id and app_id in BOT_APP_IDS:
        print(f"[ECHO CHECK] Phát hiện bot app_id: {app_id}")
        
        # KIỂM TRA QUAN TRỌNG: Nếu là echo từ bot nhưng CHỨA #MS thì KHÔNG coi là echo cần bỏ qua
        if echo_text and "#MS" in echo_text.upper():
            print(f"[ECHO WITH #MS DETECTED] Đây là echo chứa #MS, cho phép xử lý")
            return False  # Quan trọng: Trả về False để cho phép xử lý
            
        return True
    
    # 2. Kiểm tra các pattern đặc trưng của bot trong text
    if echo_text:
        echo_text_lower = echo_text.lower()
        
        # KIỂM TRA QUAN TRỌNG: Nếu tin nhắn chứa #MS, KHÔNG coi là echo (cho dù có các pattern khác)
        if "#MS" in echo_text.upper():
            print(f"[ECHO CHECK] Tin nhắn có #MS => KHÔNG PHẢI BOT (từ page)")
            return False  # Quan trọng: Cho phép xử lý tin nhắn chứa #MS
        
        # Các mẫu câu đặc trưng của bot (thêm các mẫu mới)
        bot_patterns = [
            "🌟 **5 ưu điểm nổi bật**",
            "🛒 đơn hàng mới",
            "🎉 shop đã nhận được đơn hàng",
            "dạ, phần này trong hệ thống chưa có thông tin ạ",
            "dạ em đang gặp chút trục trặc",
            "💰 giá sản phẩm:",
            "📝 mô tả:",
            "📌 [ms",
            "🛒 đơn hàng mới",
            "🎉 shop đã nhận được đơn hàng",
            "dạ em chưa biết anh/chị đang hỏi về sản phẩm nào",  # THÊM MẪU MỚI
            "vui lòng cho em biết mã sản phẩm",  # THÊM MẪU MỚI
            "anh/chị cần em tư vấn thêm gì không ạ",  # THÊM MẪU MỚI
        ]
        
        for phrase in bot_patterns:
            if phrase in echo_text_lower:
                print(f"[ECHO BOT PHRASE] Phát hiện cụm bot: {phrase}")
                return True
        
        # Bot format rõ ràng
        if re.search(r'^\*\*.*\*\*', echo_text) or re.search(r'^\[MS\d+\]', echo_text, re.IGNORECASE):
            print(f"[ECHO BOT FORMAT] Phát hiện format bot")
            return True
        
        # Tin nhắn quá dài (>200) và có cấu trúc bot (giảm ngưỡng từ 300 xuống 200)
        if len(echo_text) > 200 and ("dạ," in echo_text_lower or "ạ!" in echo_text_lower):
            print(f"[ECHO LONG BOT] Tin nhắn dài có cấu trúc bot: {len(echo_text)} chars")
            return True
        
        # Các pattern khác giảm độ nhạy (chỉ nhận diện khi rất rõ)
        bot_patterns_regex = [
            r"dạ,.*\d{1,3}[.,]?\d{0,3}\s*đ.*\d{1,3}[.,]?\d{0,3}\s*đ",  # Nhiều giá tiền (rất có thể là bot)
            r"dạ,.*\d+\s*cm.*\d+\s*cm",  # Nhiều kích thước
        ]
        
        for pattern in bot_patterns_regex:
            if re.search(pattern, echo_text_lower):
                print(f"[ECHO BOT PATTERN] Phát hiện pattern: {pattern}")
                return True
    
    # 3. Kiểm tra nếu là tin nhắn từ khách hàng (có #MS từ Fchat) - ĐÃ XỬ LÝ Ở TRÊN
    # (Đoạn này đã được xử lý ở trên với kiểm tra #MS)
    
    return False
    
# ============================================
# HÀM LẤY NỘI DUNG BÀI VIẾT TỪ FACEBOOK GRAPH API
# ============================================

def get_post_content_from_facebook(post_id: str) -> Optional[dict]:
    """
    Lấy nội dung bài viết từ Facebook Graph API
    """
    if not PAGE_ACCESS_TOKEN:
        print(f"[GET POST CONTENT] Thiếu PAGE_ACCESS_TOKEN")
        return None
    
    try:
        # Facebook Graph API endpoint để lấy nội dung bài viết
        url = f"https://graph.facebook.com/v18.0/{post_id}"
        params = {
            'fields': 'message,created_time,permalink_url',
            'access_token': PAGE_ACCESS_TOKEN
        }
        
        print(f"[GET POST CONTENT] Gọi Facebook Graph API: {url}")
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print(f"[GET POST CONTENT] Đã lấy nội dung bài viết {post_id} từ Facebook Graph API")
            
            # Chuẩn hóa dữ liệu trả về để tương thích với code cũ
            post_data = {
                'id': post_id,
                'message': data.get('message', ''),
                'created_time': data.get('created_time', ''),
                'permalink_url': data.get('permalink_url', '')
            }
            return post_data
        else:
            print(f"[GET POST CONTENT] Lỗi Facebook Graph API {response.status_code}: {response.text[:200]}")
            
            # Nếu token hết hạn hoặc thiếu quyền
            if response.status_code == 400 or response.status_code == 403:
                error_data = response.json().get('error', {})
                error_message = error_data.get('message', '')
                error_code = error_data.get('code', 0)
                print(f"[GET POST CONTENT] Lỗi Facebook API: {error_message} (code: {error_code})")
                
                # Kiểm tra các lỗi phổ biến
                if "access token" in error_message.lower():
                    print(f"[GET POST CONTENT] CÓ THỂ PAGE_ACCESS_TOKEN ĐÃ HẾT HẠN HOẶC KHÔNG ĐỦ QUYỀN!")
                elif "permission" in error_message.lower():
                    print(f"[GET POST CONTENT] THIẾU QUYỀN TRUY CẬP! Cần quyền 'pages_read_engagement'")
            
            return None
            
    except requests.exceptions.Timeout:
        print(f"[GET POST CONTENT] Timeout khi gọi Facebook Graph API")
        return None
    except Exception as e:
        print(f"[GET POST CONTENT] Exception: {e}")
        return None

# ============================================
# HÀM TRÍCH XUẤT MS TỪ BÀI VIẾT (ĐÃ SỬA - CHỈ DÙNG REGEX)
# ============================================

def extract_ms_from_post_content(post_data: dict) -> Optional[str]:
    """
    Trích xuất mã sản phẩm từ nội dung bài viết - CHỈ DÙNG REGEX
    Trả về mã sản phẩm (MSxxxxxx) nếu tìm thấy, ngược lại trả về None
    """
    if not post_data:
        return None
    
    message = post_data.get('message', '')
    post_id = post_data.get('id', '')
    
    print(f"[EXTRACT MS FROM POST] Đang phân tích bài viết {post_id}: {message[:100]}...")
    
    if not message:
        return None
    
    # PHƯƠNG PHÁP 1: Tìm MS trong dấu ngoặc vuông [MSxxxxxx] - ƯU TIÊN CAO NHẤT
    bracket_patterns = [
        r"\[(MS\d{2,6})\]",  # [MS000102]
        r"\[MS\s*(\d{2,6})\]",  # [MS 000102] với khoảng trắng
    ]
    
    for pattern in bracket_patterns:
        matches = re.findall(pattern, message, re.IGNORECASE)
        for match in matches:
            if isinstance(match, tuple):
                match = match[0]
            
            # match có thể là "MS000038" hoặc "000038"
            # Chuẩn hóa về MSxxxxxx
            if match.upper().startswith('MS'):
                # Đã có MS ở đầu, chỉ cần lấy số
                num_part = match[2:].lstrip('0')
            else:
                # Chỉ có số
                num_part = match.lstrip('0')
            
            if not num_part:
                num_part = '0'
            
            full_ms = f"MS{num_part.zfill(6)}"
            print(f"[EXTRACT MS FROM POST] Tìm thấy {full_ms} qua bracket pattern {pattern}")
            return full_ms
    
    # PHƯƠNG PHÁP 2: Tìm MSxxxxxx trực tiếp
    ms_patterns = [
        (r"\[(MS\d{6})\]", True),  # [MS000046] -> đủ 6 số
        (r"\b(MS\d{6})\b", True),  # MS000046
        (r"#(MS\d{6})", True),     # #MS000046
        (r"Mã\s*:\s*(MS\d{6})", True),  # Mã: MS000046
        (r"SP\s*:\s*(MS\d{6})", True),  # SP: MS000046
        (r"MS\s*(\d{6})", False),  # MS 000046 -> chỉ có số
        (r"mã\s*(\d{6})", False),  # mã 000046 -> chỉ có số
        (r"MS\s*(\d{2,5})\b", False),  # MS 34 -> 2-5 chữ số
        (r"mã\s*(\d{2,5})\b", False),  # mã 34 -> 2-5 chữ số
    ]
    
    for pattern, is_full_ms in ms_patterns:
        matches = re.findall(pattern, message, re.IGNORECASE)
        for match in matches:
            if isinstance(match, tuple):
                match = match[0]
            
            if is_full_ms:
                # match là MSxxxxxx đầy đủ
                # Trích xuất số từ MSxxxxxx
                num_part = match[2:].lstrip('0')
            else:
                # match chỉ là số
                num_part = match.lstrip('0')
            
            if not num_part:
                num_part = '0'
            
            full_ms = f"MS{num_part.zfill(6)}"
            print(f"[EXTRACT MS FROM POST] Tìm thấy {full_ms} qua pattern {pattern}")
            return full_ms
    
    # PHƯƠNG PHÁP 3: Tìm số 6 chữ số
    six_digit_numbers = re.findall(r'\b(\d{6})\b', message)
    for num in six_digit_numbers:
        num_part = num.lstrip('0')
        if not num_part:
            num_part = '0'
        full_ms = f"MS{num_part.zfill(6)}"
        print(f"[EXTRACT MS FROM POST] Tìm thấy số 6 chữ số {num} -> {full_ms}")
        return full_ms
    
    # PHƯƠNG PHÁP 4: Tìm số 2-5 chữ số
    short_numbers = re.findall(r'\b(\d{2,5})\b', message)
    for num in short_numbers:
        num_part = num.lstrip('0')
        if not num_part:
            num_part = '0'
        full_ms = f"MS{num_part.zfill(6)}"
        print(f"[EXTRACT MS FROM POST] Tìm thấy số ngắn {num} -> {full_ms}")
        return full_ms
    
    # PHƯƠNG PHÁP 5: Fallback - tìm bất kỳ "MS" nào trong ngoặc vuông
    fallback_pattern = r'\[.*?(MS\d+).*?\]'
    fallback_matches = re.findall(fallback_pattern, message, re.IGNORECASE)
    for match in fallback_matches:
        # Tách số từ MS
        num_match = re.search(r'(\d+)', match)
        if num_match:
            num = num_match.group(1)
            num_part = num.lstrip('0')
            if not num_part:
                num_part = '0'
            full_ms = f"MS{num_part.zfill(6)}"
            print(f"[EXTRACT MS FROM POST] Tìm thấy {full_ms} qua fallback pattern")
            return full_ms
    
    print(f"[EXTRACT MS FROM POST] Không tìm thấy MS trong bài viết")
    return None

# ============================================
# HÀM XỬ LÝ COMMENT TỪ FEED (HOÀN CHỈNH - ĐÃ SỬA SỬ DỤNG FACEBOOK GRAPH API)
# ============================================

def handle_feed_comment(change_data: dict):
    """
    Xử lý comment từ feed với logic:
    1. Lấy post_id từ comment
    2. Lấy nội dung bài viết gốc từ Facebook Graph API
    3. Trích xuất MS từ caption (CHỈ DÙNG REGEX)
    4. Load products và kiểm tra tồn tại
    5. Cập nhật context cho user và gửi tin nhắn tự động
    """
    try:
        # 1. Lấy thông tin cơ bản
        from_user = change_data.get("from", {})
        user_id = from_user.get("id")
        user_name = from_user.get("name", "")
        message_text = change_data.get("message", "")
        post_id = change_data.get("post_id", "")
        comment_id = change_data.get("comment_id", "")
        
        if not user_id or not post_id:
            print(f"[FEED COMMENT] Thiếu user_id hoặc post_id")
            return None
        
        print(f"[FEED COMMENT] User {user_id} ({user_name}) comment: '{message_text}' trên post {post_id}, comment_id: {comment_id}")
        
        # 2. Kiểm tra xem có phải comment từ page không (bỏ qua)
        if PAGE_ID and user_id == PAGE_ID:
            print(f"[FEED COMMENT] Bỏ qua comment từ chính page")
            return None
        
        # 3. Kiểm tra xem có phải comment từ bot không (bỏ qua)
        if str(user_id) in BOT_APP_IDS:
            print(f"[FEED COMMENT] Bỏ qua comment từ bot")
            return None
        
        # 4. Lấy nội dung bài viết gốc từ Facebook Graph API
        post_data = get_post_content_from_facebook(post_id)
        
        if not post_data:
            print(f"[FEED COMMENT] Không lấy được nội dung bài viết {post_id} từ Facebook Graph API")
            return None
        
        # LOG CHI TIẾT ĐỂ DEBUG
        post_message = post_data.get('message', '')
        print(f"[FEED COMMENT DEBUG] Nội dung bài viết ({len(post_message)} ký tự):")
        print(f"[FEED COMMENT DEBUG] {post_message[:500]}")
        
        # 5. Trích xuất MS từ caption bài viết (CHỈ DÙNG REGEX - KHÔNG KIỂM TRA PRODUCTS)
        detected_ms = extract_ms_from_post_content(post_data)
        
        if not detected_ms:
            print(f"[FEED COMMENT] Không tìm thấy MS trong bài viết {post_id}")
            
            # Vẫn thử trả lời bình luận nếu không tìm thấy MS
            if ENABLE_COMMENT_REPLY and comment_id:
                try:
                    # Tạo nội dung trả lời bằng GPT
                    comment_reply = generate_comment_reply_by_gpt(
                        comment_text=message_text,
                        user_name=user_name,
                        product_name="",
                        ms=""
                    )
                    
                    # Gửi trả lời lên Facebook
                    if comment_reply:
                        reply_success = reply_to_facebook_comment(comment_id, comment_reply)
                        
                        if reply_success:
                            print(f"[COMMENT REPLY] Đã trả lời bình luận {comment_id} cho user {user_id} (không có MS)")
                        else:
                            print(f"[COMMENT REPLY ERROR] Không thể gửi trả lời bình luận {comment_id}")
                except Exception as e:
                    print(f"[COMMENT REPLY EXCEPTION] Lỗi khi trả lời bình luận: {e}")
            
            return None
        
        # 6. Load products và kiểm tra MS có tồn tại trong database
        load_products(force=True)  # Load với force=True để đảm bảo có dữ liệu mới nhất
        
        # Kiểm tra nếu MS trực tiếp tồn tại
        if detected_ms not in PRODUCTS:
            print(f"[FEED COMMENT] MS {detected_ms} không tồn tại trong database, tìm trong mapping...")
            # Thử tìm trong mapping số ngắn
            num_part = detected_ms[2:].lstrip('0')
            if num_part and num_part in PRODUCTS_BY_NUMBER:
                detected_ms = PRODUCTS_BY_NUMBER[num_part]
                print(f"[FEED COMMENT] Đã map sang {detected_ms}")
            else:
                print(f"[FEED COMMENT] MS {detected_ms} không tồn tại trong database")
                
                # Vẫn thử trả lời bình luận nếu không tìm thấy sản phẩm
                if ENABLE_COMMENT_REPLY and comment_id:
                    try:
                        # Tạo nội dung trả lời bằng GPT
                        comment_reply = generate_comment_reply_by_gpt(
                            comment_text=message_text,
                            user_name=user_name,
                            product_name="",
                            ms=""
                        )
                        
                        # Gửi trả lời lên Facebook
                        if comment_reply:
                            reply_success = reply_to_facebook_comment(comment_id, comment_reply)
                            
                            if reply_success:
                                print(f"[COMMENT REPLY] Đã trả lời bình luận {comment_id} cho user {user_id} (MS không tồn tại)")
                    except Exception as e:
                        print(f"[COMMENT REPLY EXCEPTION] Lỗi khi trả lời bình luận: {e}")
                
                return None
        
        # 7. Cập nhật context cho user (RESET COUNTER để áp dụng first message rule)
        print(f"[FEED COMMENT MS] Phát hiện MS {detected_ms} từ post {post_id} cho user {user_id}")
        
        # Gọi hàm cập nhật context mới (reset counter)
        update_context_with_new_ms(user_id, detected_ms, "feed_comment")
        
        # Lấy thông tin sản phẩm NGAY tại đây để đảm bảo biến product luôn được định nghĩa
        if detected_ms in PRODUCTS:
            product = PRODUCTS[detected_ms]
            product_name = product.get('Ten', '')
            if f"[{detected_ms}]" in product_name or detected_ms in product_name:
                product_name = product_name.replace(f"[{detected_ms}]", "").replace(detected_ms, "").strip()
        else:
            # Fallback nếu không tìm thấy sản phẩm
            product = None
            product_name = ""
        
        # Lưu thêm thông tin về bài viết vào context
        ctx = USER_CONTEXT[user_id]
        ctx["source_post_id"] = post_id
        ctx["source_post_content"] = post_data.get('message', '')[:300]
        ctx["source_post_url"] = post_data.get('permalink_url', '')
        
        # 8. GỬI TIN NHẮN TỰ ĐỘNG TIẾP THỊ SẢN PHẨM BẰNG GPT
        # Chỉ gửi nếu user chưa nhắn tin trước đó hoặc real_message_count = 0
        if ctx.get("real_message_count", 0) == 0:
            try:
                # Sử dụng GPT để tạo tin nhắn tiếp thị dựa trên ưu điểm sản phẩm
                marketing_message = generate_marketing_message(detected_ms, user_name)
                if marketing_message:
                    send_message(user_id, marketing_message)
                    print(f"[FEED COMMENT AUTO REPLY] Đã gửi tin nhắn tiếp thị bằng GPT cho user {user_id}")
                else:
                    # Fallback nếu không tạo được tin nhắn
                    # Sử dụng biến product_name đã được định nghĩa trước đó
                    if product_name:
                        send_message(user_id, f"Chào {user_name}! 👋\n\nCảm ơn ac đã bình luận. Sản phẩm ac quan tâm là {product_name}. ac có thể hỏi em bất kỳ thông tin gì về sản phẩm này ạ!")
                    else:
                        send_message(user_id, f"Chào {user_name}! 👋\n\nCảm ơn ac đã bình luận trên bài viết của shop ạ! Ac có thể hỏi em bất kỳ thông tin gì về sản phẩm ạ!")
                
                # Tăng counter để không gửi lại lần nữa
                ctx["real_message_count"] = 1
                
            except Exception as e:
                print(f"[FEED COMMENT AUTO REPLY ERROR] Lỗi gửi tin nhắn: {e}")
                # Fallback nếu lỗi
                send_message(user_id, f"Chào {user_name}! 👋\n\nCảm ơn đã bình luận trên bài viết của shop ạ! Ac có thể hỏi em bất kỳ thông tin gì về sản phẩm ạ!")
        else:
            print(f"[FEED COMMENT SKIP AUTO REPLY] User {user_id} đã có real_message_count = {ctx.get('real_message_count')}, bỏ qua auto reply")

        # ============================================
        # 9. TRẢ LỜI BÌNH LUẬN TRÊN FACEBOOK BẰNG GPT (TÍNH NĂNG MỚI)
        # ============================================
        if ENABLE_COMMENT_REPLY and detected_ms and comment_id:
            try:
                # Tạo nội dung trả lời bằng GPT
                comment_reply = generate_comment_reply_by_gpt(
                    comment_text=message_text,
                    user_name=user_name,
                    product_name=product_name,  # Sử dụng biến product_name đã được định nghĩa
                    ms=detected_ms
                )
                
                # Gửi trả lời lên Facebook
                if comment_reply:
                    reply_success = reply_to_facebook_comment(comment_id, comment_reply)
                    
                    if reply_success:
                        print(f"[COMMENT REPLY] Đã trả lời bình luận {comment_id} cho user {user_id}")
                    else:
                        print(f"[COMMENT REPLY ERROR] Không thể gửi trả lời bình luận {comment_id}")
                else:
                    print(f"[COMMENT REPLY ERROR] Không tạo được nội dung trả lời")
                    
            except Exception as e:
                print(f"[COMMENT REPLY EXCEPTION] Lỗi khi trả lời bình luận: {e}")
                import traceback
                traceback.print_exc()
        # ============================================
                
        return detected_ms
        
    except Exception as e:
        print(f"[FEED COMMENT ERROR] Lỗi xử lý comment: {e}")
        import traceback
        traceback.print_exc()
        return None
        
# ============================================
# HELPER: SEND MESSAGE
# ============================================

def call_facebook_send_api(payload: dict, retry_count=2):
    if not PAGE_ACCESS_TOKEN:
        print("[WARN] PAGE_ACCESS_TOKEN chưa được cấu hình")
        return {}
    
    url = f"https://graph.facebook.com/v12.0/me/messages?access_token={PAGE_ACCESS_TOKEN}"
    
    for attempt in range(retry_count):
        try:
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code == 200:
                return resp.json()
            else:
                if attempt < retry_count - 1:
                    time.sleep(0.5)
        except Exception as e:
            if attempt < retry_count - 1:
                time.sleep(0.5)
    
    return {}

def send_message(recipient_id: str, text: str):
    if not text:
        return
    if len(text) > 2000:
        text = text[:1997] + "..."
    
    payload = {
        "recipient": {"id": recipient_id},
        "message": {"text": text},
    }
    return call_facebook_send_api(payload)

def send_image(recipient_id: str, image_url: str):
    if not image_url:
        return ""
    payload = {
        "recipient": {"id": recipient_id},
        "message": {
            "attachment": {
                "type": "image",
                "payload": {"url": image_url, "is_reusable": True},
            }
        },
    }
    return call_facebook_send_api(payload)

def send_image_safe(recipient_id: str, image_url: str, timeout: int = 3):
    if not image_url:
        return ""
    
    payload = {
        "recipient": {"id": recipient_id},
        "message": {
            "attachment": {
                "type": "image",
                "payload": {"url": image_url, "is_reusable": True},
            }
        },
    }
    
    try:
        resp = requests.post(
            f"https://graph.facebook.com/v12.0/me/messages?access_token={PAGE_ACCESS_TOKEN}",
            json=payload,
            timeout=timeout
        )
        if resp.status_code == 200:
            return resp.json()
        else:
            return {}
    except requests.exceptions.Timeout:
        print(f"⏰ Timeout khi gửi ảnh: {image_url[:50]}...")
        return {}
    except Exception as e:
        print(f"Lỗi khi gửi ảnh: {str(e)}")
        return {}

def send_carousel_template(recipient_id: str, elements: list):
    if not elements:
        return ""
    payload = {
        "recipient": {"id": recipient_id},
        "message": {
            "attachment": {
                "type": "template",
                "payload": {
                    "template_type": "generic",
                    "elements": elements[:10],
                },
            }
        },
    }
    return call_facebook_send_api(payload)

def send_quick_replies(recipient_id: str, text: str, quick_replies: list):
    payload = {
        "recipient": {"id": recipient_id},
        "message": {
            "text": text,
            "quick_replies": quick_replies,
        },
    }
    return call_facebook_send_api(payload)

# ============================================
# HÀM GỬI NÚT ĐẶT HÀNG ĐẸP
# ============================================

def send_order_button_template(uid: str, ms: str, product_name: str = None):
    """
    Gửi template với nút đặt hàng đẹp - THAY THẾ CHO VIỆC GỬI LINK THÔ
    """
    if ms not in PRODUCTS:
        return
    
    product = PRODUCTS[ms]
    
    # Lấy thông tin sản phẩm
    if not product_name:
        product_name = product.get('Ten', '')
        if f"[{ms}]" in product_name or ms in product_name:
            product_name = product_name.replace(f"[{ms}]", "").replace(ms, "").strip()
    
    gia_int = extract_price_int(product.get("Gia", "")) or 0
    
    # URL webview đặt hàng
    webview_url = f"https://{DOMAIN}/messenger-order?ms={ms}&uid={uid}"
    
    payload = {
        "recipient": {"id": uid},
        "message": {
            "attachment": {
                "type": "template",
                "payload": {
                    "template_type": "button",
                    "text": f"🎯 **ĐẶT HÀNG {ms}**\n\n📦 {product_name}\n💰 Giá: {gia_int:,.0f} đ\n\nBấm nút bên dưới để vào trang đặt hàng chính thức:",
                    "buttons": [
                        {
                            "type": "web_url",
                            "title": "🛒 ĐẶT HÀNG NGAY",
                            "url": webview_url,
                            "webview_height_ratio": "tall",
                            "messenger_extensions": True,
                            "webview_share_button": "hide"
                        },
                        {
                            "type": "postback",
                            "title": "ℹ️ Thông tin SP",
                            "payload": f"PRODUCT_HIGHLIGHTS_{ms}"
                        },
                        {
                            "type": "postback",
                            "title": "🖼️ Xem ảnh",
                            "payload": f"VIEW_IMAGES_{ms}"
                        }
                    ]
                }
            }
        }
    }
    
    return call_facebook_send_api(payload)

def send_order_button_quick_reply(uid: str, ms: str):
    """
    Gửi nút đặt hàng bằng Quick Replies - rất trực quan trên Messenger
    """
    webview_url = f"https://{DOMAIN}/messenger-order?ms={ms}&uid={uid}"
    
    quick_replies = [
        {
            "content_type": "text",
            "title": "🛒 ĐẶT HÀNG NGAY",
            "payload": f"ORDER_NOW_{ms}"
        },
        {
            "content_type": "text",
            "title": "📞 TƯ VẤN THÊM",
            "payload": "NEED_HELP"
        }
    ]
    
    # Tin nhắn kèm theo nút
    message_text = f"✅ Sẵn sàng đặt hàng **{ms}**!\n\nBấm nút bên dưới để vào trang đặt hàng:"
    
    return send_quick_replies(uid, message_text, quick_replies)

# ============================================
# HELPER: PRODUCTS
# ============================================

def parse_image_urls(raw: str):
    if not raw:
        return []
    
    parts = re.split(r'[,\n;|]+', raw)
    urls = []
    
    for p in parts:
        p = p.strip()
        if not p:
            continue
        p = re.sub(r'^[\'"\s]+|[\'"\s]+$', '', p)
        
        if re.match(r'^https?://', p) or any(domain in p.lower() for domain in [
            'alicdn.com', 'taobao', '1688.com', '.jpg', '.jpeg', 
            '.png', '.webp', '.gif', 'image', 'img', 'photo'
        ]):
            urls.append(p)
    
    seen = set()
    result = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            result.append(u)
    
    return result

def extract_price_int(price_str: str):
    if not price_str:
        return None
    
    # Loại bỏ ký tự không phải số, dấu chấm, dấu phẩy
    cleaned = re.sub(r'[^\d.,]', '', str(price_str))
    
    if not cleaned:
        return None
    
    # Xử lý các định dạng giá phổ biến
    # 1. Định dạng Việt Nam: 1.000.000
    if cleaned.count('.') > 1 and cleaned.count(',') <= 1:
        # Giả sử dấu chấm là phân cách nghìn, dấu phẩy là thập phân
        cleaned = cleaned.replace('.', '')
        if ',' in cleaned:
            cleaned = cleaned.replace(',', '.')
    
    # 2. Định dạng quốc tế: 1,000,000.00
    elif cleaned.count(',') > 1 and cleaned.count('.') <= 1:
        # Giả sử dấu phẩy là phân cách nghìn, dấu chấm là thập phân
        cleaned = cleaned.replace(',', '')
    
    # 3. Định dạng hỗn hợp
    else:
        # Giữ lại số cuối cùng trước dấu phẩy hoặc chấm
        cleaned = cleaned.replace(',', '').replace('.', '')
    
    try:
        # Lấy phần nguyên nếu có dấu thập phân
        if '.' in cleaned:
            cleaned = cleaned.split('.')[0]
        
        return int(float(cleaned)) if cleaned else None
    except Exception:
        return None

def load_products(force=False):
    global PRODUCTS, LAST_LOAD, PRODUCTS_BY_NUMBER
    now = time.time()
    if not force and PRODUCTS and (now - LAST_LOAD) < LOAD_TTL:
        return

    if not GOOGLE_SHEET_CSV_URL:
        print("❌ GOOGLE_SHEET_CSV_URL chưa được cấu hình!")
        return

    try:
        print(f"🟦 Loading sheet: {GOOGLE_SHEET_CSV_URL}")
        r = requests.get(GOOGLE_SHEET_CSV_URL, timeout=20)
        r.raise_for_status()
        r.encoding = "utf-8"
        content = r.text

        reader = csv.DictReader(content.splitlines())
        products = {}
        products_by_number = {}

        for raw_row in reader:
            row = dict(raw_row)

            ms = (row.get("Mã sản phẩm") or "").strip()
            if not ms:
                continue

            ten = (row.get("Tên sản phẩm") or "").strip()
            if not ten:
                continue

            gia_raw = (row.get("Giá bán") or "").strip()
            images = (row.get("Images") or "").strip()
            videos = (row.get("Videos") or "").strip()
            tonkho_raw = (row.get("Tồn kho") or row.get("Có thể bán") or "").strip()
            mota = (row.get("Mô tả") or "").strip()
            mau = (row.get("màu (Thuộc tính)") or "").strip()
            size = (row.get("size (Thuộc tính)") or "").strip()
            thuoc_tinh = (row.get("Thuộc tính") or "").strip()
            website = (row.get("Website") or "").strip()  # <--- THÊM CỘT WEBSITE

            gia_int = extract_price_int(gia_raw)
            try:
                tonkho_int = int(str(tonkho_raw)) if str(tonkho_raw).strip() else None
            except Exception:
                tonkho_int = None

            variant_images = parse_image_urls(images)
            variant_image = variant_images[0] if variant_images else ""

            if ms not in products:
                base = {
                    "MS": ms,
                    "Ten": ten,
                    "Gia": gia_raw,
                    "MoTa": mota,
                    "Images": images,
                    "Videos": videos,
                    "Tồn kho": tonkho_raw,
                    "màu (Thuộc tính)": mau,
                    "size (Thuộc tính)": size,
                    "Thuộc tính": thuoc_tinh,
                    "Website": website,  # <--- THÊM VÀO DICTIONARY
                    "FullRow": row,
                }
                base["variants"] = []
                base["all_colors"] = set()
                base["all_sizes"] = set()
                products[ms] = base

            p = products[ms]

            variant = {
                "mau": mau,
                "size": size,
                "gia": gia_int,
                "gia_raw": gia_raw,
                "tonkho": tonkho_int if tonkho_int is not None else tonkho_raw,
                "images": images,
                "variant_image": variant_image,
            }
            p["variants"].append(variant)

            if mau:
                p["all_colors"].add(mau)
            if size:
                p["all_sizes"].add(size)

        for ms, p in products.items():
            colors = sorted(list(p.get("all_colors") or []))
            sizes = sorted(list(p.get("all_sizes") or []))
            p["màu (Thuộc tính)"] = ", ".join(colors) if colors else p.get("màu (Thuộc tính)", "")
            p["size (Thuộc tính)"] = ", ".join(sizes) if sizes else p.get("size (Thuộc tính)", "")
            
            if ms.startswith("MS"):
                num_part = ms[2:]
                num_without_leading_zeros = num_part.lstrip('0')
                if num_without_leading_zeros:
                    products_by_number[num_without_leading_zeros] = ms

        PRODUCTS = products
        PRODUCTS_BY_NUMBER = products_by_number
        LAST_LOAD = now
        
        total_variants = sum(len(p['variants']) for p in products.values())
        
        print(f"📦 Loaded {len(PRODUCTS)} products với {total_variants} variants.")
        print(f"🔢 Created mapping for {len(PRODUCTS_BY_NUMBER)} product numbers")
                
    except Exception as e:
        print("❌ load_products ERROR:", e)
def get_variant_image(ms: str, color: str, size: str) -> str:
    if ms not in PRODUCTS:
        return ""
    
    product = PRODUCTS[ms]
    variants = product.get("variants", [])
    
    for variant in variants:
        variant_color = variant.get("mau", "").strip().lower()
        variant_size = variant.get("size", "").strip().lower()
        
        input_color = color.strip().lower()
        input_size = size.strip().lower()
        
        color_match = (not input_color) or (variant_color == input_color) or (input_color == "mặc định" and not variant_color)
        size_match = (not input_size) or (variant_size == input_size) or (input_size == "mặc định" and not variant_size)
        
        if color_match and size_match:
            variant_image = variant.get("variant_image", "")
            if variant_image:
                return variant_image
    
    images_field = product.get("Images", "")
    urls = parse_image_urls(images_field)
    return urls[0] if urls else ""

# ============================================
# HÀM PHÂN TÍCH GIÁ THÔNG MINH
# ============================================

def analyze_product_price_patterns(ms: str) -> dict:
    """
    Phân tích mẫu giá của sản phẩm và trả về cấu trúc dữ liệu rõ ràng
    """
    if ms not in PRODUCTS:
        return {"error": "Product not found"}
    
    product = PRODUCTS[ms]
    variants = product.get("variants", [])
    
    price_by_color = {}
    price_by_size = {}
    price_groups = {}
    
    # 1. Phân tích theo màu
    for variant in variants:
        color = variant.get("mau", "Mặc định").strip()
        size = variant.get("size", "Mặc định").strip()
        price = variant.get("gia", 0)
        
        if price:
            # Nhóm theo màu
            if color not in price_by_color:
                price_by_color[color] = {"price": price, "sizes": set()}
            price_by_color[color]["sizes"].add(size)
            
            # Nhóm theo size
            if size not in price_by_size:
                price_by_size[size] = {"price": price, "colors": set()}
            price_by_size[size]["colors"].add(color)
            
            # Nhóm theo mức giá
            if price not in price_groups:
                price_groups[price] = []
            price_groups[price].append({"color": color, "size": size})
    
    # 2. Kiểm tra xem giá có thay đổi theo màu không
    color_based = True
    for color, data in price_by_color.items():
        if len(data["sizes"]) > 1 and any(v.get("mau", "").strip() == color and v.get("gia", 0) != data["price"] for v in variants):
            color_based = False
            break
    
    # 3. Kiểm tra xem giá có thay đổi theo size không
    size_based = True
    for size, data in price_by_size.items():
        if len(data["colors"]) > 1 and any(v.get("size", "").strip() == size and v.get("gia", 0) != data["price"] for v in variants):
            size_based = False
            break
    
    # 4. Phân tích mẫu giá phức tạp
    complex_pattern = not (color_based or size_based)
    
    # Tạo cấu trúc trả về
    result = {
        "ms": ms,
        "product_name": product.get("Ten", ""),
        "total_variants": len(variants),
        "price_pattern": "unknown",
        "base_price": extract_price_int(product.get("Gia", "")) or 0,
        "detailed_analysis": {}
    }
    
    if color_based and price_by_color:
        result["price_pattern"] = "color_based"
        result["detailed_analysis"] = {
            "type": "color_based",
            "prices": []
        }
        for color, data in sorted(price_by_color.items()):
            result["detailed_analysis"]["prices"].append({
                "color": color,
                "price": data["price"],
                "applicable_sizes": f"Tất cả size ({', '.join(sorted(data['sizes']))})" if data["sizes"] else "Tất cả size"
            })
    
    elif size_based and price_by_size:
        result["price_pattern"] = "size_based"
        result["detailed_analysis"] = {
            "type": "size_based",
            "prices": []
        }
        for size, data in sorted(price_by_size.items()):
            result["detailed_analysis"]["prices"].append({
                "size": size,
                "price": data["price"],
                "applicable_colors": f"Tất cả màu ({', '.join(sorted(data['colors']))})" if data["colors"] else "Tất cả màu"
            })
    
    elif complex_pattern and price_groups:
        result["price_pattern"] = "complex_based"
        result["detailed_analysis"] = {
            "type": "complex_based",
            "price_groups": []
        }
        for price, items in sorted(price_groups.items()):
            if len(items) <= 5:
                variants_list = [f"{item['color']}/{item['size']}" for item in items]
                display_text = ", ".join(variants_list)
            else:
                variants_list = [f"{item['color']}/{item['size']}" for item in items[:3]]
                display_text = f"{', '.join(variants_list)} và {len(items) - 3} phân loại khác"
            
            result["detailed_analysis"]["price_groups"].append({
                "price": price,
                "count": len(items),
                "variants": display_text,
                "all_variants": items[:10]
            })
    else:
        result["price_pattern"] = "single_price"
        result["detailed_analysis"] = {
            "type": "single_price",
            "price": result["base_price"]
        }
    
    return result

def get_product_data_for_gpt(ms: str) -> dict:
    """Lấy dữ liệu sản phẩm dưới dạng dictionary đơn giản cho GPT"""
    if ms not in PRODUCTS:
        return None
    
    product = PRODUCTS[ms]
    
    images_field = product.get("Images", "")
    image_urls = parse_image_urls(images_field)
    unique_images = list(dict.fromkeys(image_urls))[:10]
    
    videos_field = product.get("Videos", "")
    video_urls = parse_image_urls(videos_field)
    
    return {
        "ms": ms,
        "ten": product.get("Ten", ""),
        "mo_ta": product.get("MoTa", ""),
        "gia": product.get("Gia", ""),
        "gia_int": extract_price_int(product.get("Gia", "")),
        "mau_sac": product.get("màu (Thuộc tính)", ""),
        "size": product.get("size (Thuộc tính)", ""),
        "thuoc_tinh": product.get("Thuộc tính", ""),
        "ton_kho": product.get("Tồn kho", ""),
        "images": unique_images,
        "videos": video_urls,
        "variants": product.get("variants", [])[:5],
        "all_colors": list(product.get("all_colors", set())),
        "all_sizes": list(product.get("all_sizes", set()))
    }

# ============================================
# GPT FUNCTION CALLING TOOLS
# ============================================

def get_tools_definition():
    return [
        {
            "type": "function",
            "function": {
                "name": "get_product_price_details",
                "description": "Lấy thông tin giá chi tiết của sản phẩm, bao gồm các biến thể giá theo màu, size hoặc kết hợp",
                "parameters": {
                    "type": "object",
                    "properties": {"ms": {"type": "string", "description": "Mã sản phẩm MSxxxxxx"}},
                    "required": ["ms"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "get_product_basic_info",
                "description": "Lấy thông tin cơ bản của sản phẩm (tên, mô tả, màu sắc, size, thuộc tính, tồn kho)",
                "parameters": {
                    "type": "object",
                    "properties": {"ms": {"type": "string", "description": "Mã sản phẩm MSxxxxxx"}},
                    "required": ["ms"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "send_product_images",
                "description": "Gửi ảnh sản phẩm cho khách xem (tối đa 3 ảnh)",
                "parameters": {
                    "type": "object",
                    "properties": {"ms": {"type": "string", "description": "Mã sản phẩm"}},
                    "required": ["ms"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "send_product_videos",
                "description": "Gửi link video sản phẩm",
                "parameters": {
                    "type": "object",
                    "properties": {"ms": {"type": "string", "description": "Mã sản phẩm"}},
                    "required": ["ms"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "provide_order_link",
                "description": "Cung cấp link đặt hàng khi khách muốn mua",
                "parameters": {
                    "type": "object",
                    "properties": {"ms": {"type": "string", "description": "Mã sản phẩm"}},
                    "required": ["ms"]
                }
            }
        }
    ]

def execute_tool(uid, name, args):
    ctx = USER_CONTEXT[uid]
    ms = args.get("ms", "").upper() or ctx.get("last_ms")
    domain = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"
    
    if name == "get_product_price_details":
        price_analysis = analyze_product_price_patterns(ms)
        
        return json.dumps({
            "status": "success",
            "analysis": price_analysis,
            "formatted_instructions": """GPT HÃY DÙNG DỮ LIỆU NÀY ĐỂ TRẢ LỜI VỀ GIÁ:
            
            1. Nếu price_pattern là 'color_based':
               - Liệt kê từng màu và giá
               - Ví dụ: "Dạ, giá bán là:\nĐỏ: 250.000đ\nXanh: 290.000đ\nTrắng: 315.000đ\nMiễn ship toàn quốc và được xem hàng khi giao nhận ạ!"
               
            2. Nếu price_pattern là 'size_based':
               - Liệt kê từng size và giá
               - Ví dụ: "Dạ, giá bán là:\nM: 250.000đ\nL: 290.000đ\nXL: 315.000đ\nMiễn ship toàn quốc và được xem hàng khi giao nhận ạ!"
               
            3. Nếu price_pattern là 'complex_based':
               - Nhóm theo từng mức giá
               - Ví dụ: "Em gửi anh chị bảng giá ạ:\n250.000đ (đỏ/M, xanh/L, trắng/L)\n290.000đ (đen/M, tím/2XL, đỏ/XL)\n315.000đ (trắng/2XL, xanh/XL, nâu/S)"
               
            4. Nếu price_pattern là 'single_price':
               - Chỉ cần trả lời một giá duy nhất
               - Ví dụ: "Dạ, giá sản phẩm là 250.000đ ạ!"
               
            LUÔN KẾT THÚC BẰNG: 'Anh/chị cần em tư vấn thêm gì không ạ?'"""
        }, ensure_ascii=False)
    
    elif name == "get_product_basic_info":
        product_data = get_product_data_for_gpt(ms)
        
        return json.dumps({
            "status": "success",
            "data": product_data,
            "instructions": "GPT HÃY DÙNG DỮ LIỆU NÀY ĐỂ TRẢ LỜI VỀ: tên, mô tả, chất liệu, màu sắc, size, thuộc tính, tồn kho (trừ giá). Nếu không có thông tin, nói: 'Dạ, phần này em chưa có thông tin, ac LH sdt này 0869905991 để trao đổi trực tiếp giúp e nhé!'"
        }, ensure_ascii=False)
    
    elif name == "send_product_images":
        if ms not in PRODUCTS:
            return "Sản phẩm không có ảnh."
        
        product = PRODUCTS[ms]
        images_field = product.get("Images", "")
        urls = parse_image_urls(images_field)
        
        if not urls:
            return "Sản phẩm không có ảnh."
        
        seen = set()
        sent_count = 0
        for url in urls:
            if url not in seen and sent_count < 3:
                send_image_safe(uid, url, timeout=3)
                seen.add(url)
                sent_count += 1
                time.sleep(0.5)
        
        return f"Đã gửi {sent_count} ảnh sản phẩm."
    
    elif name == "send_product_videos":
        if ms not in PRODUCTS:
            return "Sản phẩm không có video."
        
        product = PRODUCTS[ms]
        videos_field = product.get("Videos", "")
        urls = parse_image_urls(videos_field)
        
        if not urls:
            return "Sản phẩm không có video."
        
        for url in urls[:2]:
            send_message(uid, f"📹 Video sản phẩm: {url}")
            time.sleep(0.5)
        
        return "Đã gửi link video."
    
    elif name == "provide_order_link":
        if ms in PRODUCTS:
            # Gửi template với nút đặt hàng đẹp THAY VÌ link thô
            product = PRODUCTS[ms]
            product_name = product.get('Ten', '')
            
            if f"[{ms}]" in product_name or ms in product_name:
                product_name = product_name.replace(f"[{ms}]", "").replace(ms, "").strip()
            
            # Gửi template đẹp
            send_order_button_template(uid, ms, product_name)
            
            return json.dumps({
                "order_sent": True,
                "ms": ms,
                "product_name": product_name,
                "message": "Đã gửi nút đặt hàng"
            }, ensure_ascii=False)
        return "Không tìm thấy sản phẩm."
    
    return "Tool không xác định."

# ============================================
# CẢI THIỆN NGỮ CẢNH
# ============================================

def update_product_context(uid: str, ms: str):
    ctx = USER_CONTEXT[uid]
    ctx["last_ms"] = ms
    
    if "product_history" not in ctx:
        ctx["product_history"] = []
    
    if not ctx["product_history"] or ctx["product_history"][0] != ms:
        if ms in ctx["product_history"]:
            ctx["product_history"].remove(ms)
        ctx["product_history"].insert(0, ms)
    
    if len(ctx["product_history"]) > 5:
        ctx["product_history"] = ctx["product_history"][:5]
    
    ctx["dirty"] = True  # ← THÊM DÒNG NÀY
    ctx["last_updated"] = time.time()
    
    print(f"[CONTEXT UPDATE] User {uid}: last_ms={ms}, history={ctx['product_history']}")
    
def detect_ms_from_text(text: str) -> Optional[str]:
    """Phát hiện mã sản phẩm từ nhiều dạng text khác nhau - CHỈ khi có tiền tố"""
    if not text: 
        return None
    
    print(f"[DETECT MS DEBUG] Input text: {text}")
    
    # Chuẩn hóa text: lowercase, xóa dấu, xóa khoảng trắng thừa
    text_norm = normalize_vietnamese(text.lower().strip())
    
    # Danh sách các tiền tố cần tìm - CHỈ lấy khi có các tiền tố này
    prefixes = [
        # Dạng chuẩn & đầy đủ
        r'ms', r'mã', r'mã số', r'mã sản phẩm', r'sản phẩm', r'sản phẩm số',
        r'sp',  # Dạng viết tắt
        # Dạng không dấu
        r'ma', r'ma so', r'ma san pham', r'san pham', r'san pham so',
        # Dạng sai chính tả
        r'mã sp', r'ma sp', r'mã s\.phẩm', r'ma san pham so', 
        r'mã sp số', r'ma so sp',
        # Dạng tự nhiên khi khách hỏi (cần có từ khóa)
        r'xem mã', r'xem sp', r'xem sản phẩm', r'cho xem mã', 
        r'tư vấn mã', r'tư vấn sp', r'giới thiệu mã', r'giới thiệu sp'
    ]
    
    # Tạo pattern regex tổng hợp
    # Format: (tiền tố) + (tùy chọn khoảng trắng) + (số 1-6 chữ số, có thể có số 0 ở đầu)
    pattern_str = r'(?:' + '|'.join(prefixes) + r')\s*(\d{1,6})'
    
    # Tìm kiếm với regex
    match = re.search(pattern_str, text_norm)
    
    if match:
        num = match.group(1)
        clean_n = num.lstrip("0")
        
        if clean_n and clean_n in PRODUCTS_BY_NUMBER:
            found_ms = PRODUCTS_BY_NUMBER[clean_n]
            print(f"[DETECT MS DEBUG] Tìm thấy qua tiền tố + số {num}: {found_ms}")
            return found_ms
    
    # THÊM: Tìm MS dạng viết liền hoàn toàn (MSxxxxxx, msxxxxxx, spxxxxxx)
    # Pattern: (MS|ms|sp) + (1-6 chữ số)
    direct_pattern = r'\b(ms|sp|ms|sp)(\d{1,6})\b'
    direct_match = re.search(direct_pattern, text_norm, re.IGNORECASE)
    
    if direct_match:
        num = direct_match.group(2)
        clean_n = num.lstrip("0")
        
        if clean_n and clean_n in PRODUCTS_BY_NUMBER:
            found_ms = PRODUCTS_BY_NUMBER[clean_n]
            print(f"[DETECT MS DEBUG] Tìm thấy dạng viết liền: {found_ms}")
            return found_ms
    
    print(f"[DETECT MS DEBUG] Không tìm thấy MS trong text (chỉ tìm với tiền tố): {text}")
    return None

# ============================================
# GPT FUNCTION CALLING HANDLER
# ============================================

def handle_text_with_function_calling(uid: str, text: str):
    """GPT function calling LUÔN dựa vào last_ms từ context"""
    load_products()
    ctx = USER_CONTEXT[uid]
    
    # THÊM: Khôi phục context nếu cần
    if not ctx.get("last_ms") or ctx.get("last_ms") not in PRODUCTS:
        restored = restore_user_context_on_wakeup(uid)
        if restored:
            print(f"[GPT FUNCTION] Đã khôi phục context cho user {uid}")
    
    # ============================================
    # QUAN TRỌNG: ƯU TIÊN CẬP NHẬT MS TỪ TEXT VÀ LƯU NGAY
    # ============================================
    detected_ms = detect_ms_from_text(text)
    if detected_ms and detected_ms in PRODUCTS:
        # Cập nhật MS mới NGAY LẬP TỨC và lưu vào Sheets
        update_context_with_new_ms(uid, detected_ms, "text_detection")
        print(f"[MS DETECTED IN GPT] Phát hiện và cập nhật MS mới: {detected_ms}")
    
    # ƯU TIÊN: Lấy MS từ context (sau khi đã cập nhật từ text nếu có)
    current_ms = ctx.get("last_ms")
    
    # ƯU TIÊN: Nếu vẫn không có, kiểm tra xem tin nhắn có chứa số không
    if not current_ms or current_ms not in PRODUCTS:
        # Tìm bất kỳ số nào trong tin nhắn (1-6 chữ số) với TIỀN TỐ
        text_norm = normalize_vietnamese(text.lower())
        numbers = re.findall(r'\b(?:ms|mã|sp|ma|san pham)\s*(\d{1,6})\b', text_norm, re.IGNORECASE)
        for num in numbers:
            clean_num = num.lstrip('0')
            if clean_num and clean_num in PRODUCTS_BY_NUMBER:
                current_ms = PRODUCTS_BY_NUMBER[clean_num]
                # Cập nhật context với MS mới VÀ LƯU NGAY
                update_context_with_new_ms(uid, current_ms, "text_detection")
                print(f"[MS FALLBACK IN GPT] Tìm thấy MS từ tiền tố + số: {current_ms}")
                break
    
    # ƯU TIÊN: Nếu vẫn không có, hỏi lại khách
    if not current_ms or current_ms not in PRODUCTS:
        send_message(uid, "Dạ em chưa biết anh/chị đang hỏi về sản phẩm nào. Vui lòng cho em biết mã sản phẩm (ví dụ: MS000012) ạ!")
        return
    
    # ============================================
    # TIẾP TỤC XỬ LÝ GPT VỚI MS HIỆN TẠI
    # ============================================
    fanpage_name = get_fanpage_name_from_api()
    
    system_prompt = f"""Bạn là nhân viên bán hàng của {fanpage_name}.

**SẢN PHẨM ĐANG ĐƯỢC HỎI: {current_ms}**

**QUY TẮC QUAN TRỌNG VỀ MÃ SẢN PHẨM:**
1. CHỈ TRẢ LỜI VỀ SẢN PHẨM HIỆN TẠI: {current_ms}
2. KHÔNG BAO GIỜ được nhắc đến mã sản phẩm khác trong câu trả lời
3. Nếu cần thông tin, chỉ dùng tool với ms={current_ms}
4. Nếu user hỏi về sản phẩm khác, yêu cầu họ cung cấp mã sản phẩm

**QUY TẮC TRẢ LỜI VỀ THÔNG TIN CHI TIẾT SẢN PHẨM TỪ CỘT "MÔ TẢ":**
Khi khách hỏi về bất kỳ thông tin chi tiết nào của sản phẩm, bạn PHẢI:
1. LUÔN dùng tool 'get_product_basic_info' để lấy thông tin sản phẩm, bao gồm cột "Mô tả"
2. ĐỌC KỸ toàn bộ nội dung trong cột "Mô tả" để tìm thông tin liên quan
3. TÌM KIẾM các từ khóa liên quan trong "Mô tả":
   - "công suất", "điện áp", "công suất tiêu thụ", "watt", "kW"
   - "lắp đặt", "hướng dẫn lắp đặt", "cách lắp", "thi công"
   - "thông số", "thông số kỹ thuật", "kích thước", "trọng lượng", "chất liệu"
   - "bảo hành", "bảo trì", "sửa chữa"
   - "hướng dẫn sử dụng", "cách dùng", "vận hành"
   - "địa chỉ", "số điện thoại", "liên hệ", "hotline"
   - "thử hàng", "dùng thử", "kiểm tra hàng"
   - "người lắp đặt", "kỹ thuật viên", "nhân viên kỹ thuật"
   - "miễn phí vận chuyển", "phí vận chuyển", "ship", "freeship", "miễn ship", "vận chuyển", "giao hàng", "phí giao hàng"
   - "nguồn nước", "nước máy", "nước giếng", "nước nhiễm đá vôi", "nước nhiễm vôi", "lọc nước", "khả năng lọc", "lọc được nước nào", "nhiễm đá vôi", "lọc đá vôi", "nguồn nước lấy từ đâu"
   - "gia đình", "công sở", "văn phòng", "hộ gia đình", "cá nhân", "tập thể", "phù hợp cho", "đối tượng sử dụng", "dùng cho", "ai dùng được"

4. NẾU TÌM THẤY thông tin trong "Mô tả":
   - Trích xuất thông tin chính xác từ "Mô tả"
   - Diễn đạt lại theo cách tự nhiên, dễ hiểu, thân thiện
   - Giữ nguyên ý nghĩa nhưng làm cho câu trả lời gần gũi với khách hàng
   - Ví dụ: "Dạ, [trích dẫn/paraphrase thông tin từ mô tả] ạ!"

5. NẾU KHÔNG TÌM THẤY thông tin trong "Mô tả":
   - Trả lời: "Dạ, phần này trong hệ thống chưa có thông tin chi tiết ạ. Anh/chị vui lòng liên hệ shop để được hỗ trợ ạ!"
   - TUYỆT ĐỐI KHÔNG bịa thông tin, KHÔNG đoán mò, KHÔNG tạo thông tin giả

**QUY TẮC TRẢ LỜI VỀ GIÁ:**
1. Khi khách hỏi về giá - LUÔN dùng tool 'get_product_price_details'
2. Phân tích kết quả từ tool và trả lời theo định dạng:
   - Giá theo màu: Liệt kê từng màu và giá
   - Giá theo size: Liệt kê từng size và giá
   - Giá phức tạp: Nhóm theo từng mức giá, liệt kê các màu/size trong mỗi nhóm
   - Giá duy nhất: Trả lời một giá duy nhất
3. LUÔN hỏi khách cần tư vấn thêm gì không sau khi trả lời về giá.

**QUY TẮC LIỆT KÊ MÀU SẮC VÀ SIZE (RẤT QUAN TRỌNG):**
1. Khi khách hỏi "có những màu nào", "màu gì", "màu sắc gì" - LUÔN dùng tool 'get_product_basic_info'
2. Sau khi có dữ liệu, liệt kê TẤT CẢ màu sắc có trong 'all_colors' hoặc 'mau_sac'
3. Định dạng trả lời: "Dạ, sản phẩm có các màu: [màu 1], [màu 2], [màu 3] ạ!"
4. Khi khách hỏi "có size nào", "size gì", "kích cỡ nào" - LUÔN dùng tool 'get_product_basic_info'
5. Sau khi có dữ liệu, liệt kê TẤT CẢ size có trong 'all_sizes' hoặc 'size'
6. Định dạng trả lời: "Dạ, sản phẩm có các size: [size 1], [size 2], [size 3] ạ!"
7. Nếu không có thông tin về màu/size: "Dạ, sản phẩm này chỉ có 1 màu/1 size mặc định ạ!"

**QUY TẮC XỬ LÝ ĐẶT HÀNG (RẤT QUAN TRỌNG):**
1. Khi khách hỏi: "đặt hàng", "mua hàng", "mua", "order", "cho tôi đặt", "tôi muốn mua" - LUÔN dùng tool 'provide_order_link'
2. Tool này sẽ tự động gửi nút đặt hàng đẹp cho khách
3. KHÔNG BAO GIỜ tự tạo link thủ công, LUÔN dùng tool
4. Sau khi gọi tool, có thể hỏi thêm: "Anh/chị đã vào trang đặt hàng chưa ạ?"

**CÁC LOẠI CÂU HỎI CẦN XỬ LÝ TỪ "MÔ TẢ":**
1. Câu hỏi về THÔNG SỐ KỸ THUẬT:
   - "Công suất bao nhiêu?" → tìm "công suất", "watt", "kW" trong mô tả
   - "Điện áp bao nhiêu?" → tìm "điện áp", "volt", "V" trong mô tả
   - "Kích thước thế nào?" → tìm "kích thước", "dài rộng cao", "mm", "cm" trong mô tả
   - "Trọng lượng bao nhiêu?" → tìm "trọng lượng", "kg", "gram" trong mô tả
   - "Chất liệu gì?" → tìm "chất liệu", "vật liệu", "làm bằng" trong mô tả

2. Câu hỏi về HƯỚNG DẪN SỬ DỤNG:
   - "Hướng dẫn lắp đặt thế nào?" → tìm "lắp đặt", "hướng dẫn lắp", "thi công" trong mô tả
   - "Cách sử dụng ra sao?" → tìm "hướng dẫn sử dụng", "cách dùng", "vận hành" trong mô tả
   - "Bảo quản thế nào?" → tìm "bảo quản", "bảo dưỡng", "vệ sinh" trong mô tả

3. Câu hỏi về CHÍNH SÁCH & DỊCH VỤ:
   - "Bảo hành bao lâu?" → tìm "bảo hành", "bảo trì", "đổi trả" trong mô tả
   - "Có được thử hàng không?" → tìm "thử hàng", "dùng thử", "kiểm tra" trong mô tả
   - "Ai là người lắp đặt?" → tìm "người lắp đặt", "kỹ thuật viên", "nhân viên" trong mô tả

4. Câu hỏi về PHÍ VẬN CHUYỂN:
   - "Có miễn ship không?" → tìm "miễn phí vận chuyển", "phí vận chuyển", "ship", "freeship" trong mô tả
   - "Có mất phí ship không?" → tìm "phí vận chuyển", "ship", "vận chuyển" trong mô tả
   - "Freeship không?" → tìm "freeship", "miễn phí vận chuyển", "miễn ship" trong mô tả
   - "Phí ship bao nhiêu?" → tìm "phí vận chuyển", "ship", "vận chuyển" trong mô tả

5. Câu hỏi về NGUỒN NƯỚC VÀ KHẢ NĂNG LỌC:
   - "Nguồn nước lấy từ đâu?" → tìm "nguồn nước", "nước máy", "nước giếng" trong mô tả
   - "Lọc nước nhiễm đá vôi không?" → tìm "nhiễm đá vôi", "lọc đá vôi", "nước cứng" trong mô tả
   - "Lọc được những nguồn nước nào?" → tìm "nguồn nước", "lọc được", "khả năng lọc" trong mô tả
   - "Có lọc được nước giếng không?" → tìm "nước giếng", "nguồn nước" trong mô tả

6. Câu hỏi về ĐỐI TƯỢNG SỬ DỤNG:
   - "Phù hợp cho những ai?" → tìm "phù hợp cho", "đối tượng sử dụng", "dùng cho" trong mô tả
   - "Phù hợp cho gia đình hay công sở không?" → tìm "gia đình", "công sở", "văn phòng" trong mô tả
   - "Có dùng cho văn phòng được không?" → tìm "văn phòng", "công sở", "gia đình" trong mô tả
   - "Hộ gia đình dùng được không?" → tìm "hộ gia đình", "gia đình" trong mô tả

7. Câu hỏi về THÔNG TIN SHOP:
   - "Số điện thoại shop là gì?" → tìm "số điện thoại", "liên hệ", "hotline" trong mô tả
   - "Địa chỉ shop ở đâu?" → tìm "địa chỉ", "cửa hàng", "showroom" trong mô tả
   - "Shop có hỗ trợ lắp đặt không?" → tìm "hỗ trợ lắp đặt", "lắp đặt miễn phí" trong mô tả

**QUY TẮC CHUNG:**
- Xưng "em", gọi "anh/chị"
- Ngắn gọn, thân thiện (1-3 dòng là tốt nhất)
- Nếu không có thông tin: "Dạ, phần này trong hệ thống chưa có thông tin ạ"
- Về tồn kho: LUÔN báo "CÒN HÀNG ạ!" nếu khách hỏi (trừ khi biết chắc là hết hàng)
- LUÔN kết thúc bằng câu hỏi: "Anh/chị cần em tư vấn thêm gì không ạ?" hoặc tương tự

**TOOLS SẴN CÓ VÀ KHI NÀO DÙNG:**
1. get_product_price_details - Cho câu hỏi về giá: "giá bao nhiêu", "bao nhiêu tiền"
2. get_product_basic_info - Cho TẤT CẢ câu hỏi về thông tin sản phẩm:
   - "có những màu nào" → liệt kê màu từ 'all_colors'
   - "có size nào" → liệt kê size từ 'all_sizes'
   - "chất liệu gì" → tìm trong mô tả
   - "thông số kỹ thuật" → tìm trong mô tả
   - "công suất bao nhiêu" → tìm trong mô tả
   - "hướng dẫn lắp đặt" → tìm trong mô tả
   - "số điện thoại shop" → tìm trong mô tả
   - "địa chỉ shop" → tìm trong mô tả
   - "có được thử hàng không" → tìm trong mô tả
   - "bảo hành bao lâu" → tìm trong mô tả
   - "có miễn ship không" → tìm trong mô tả
   - "nguồn nước lấy từ đâu" → tìm trong mô tả
   - "lọc nước nhiễm đá vôi không" → tìm trong mô tả
   - "phù hợp cho những ai" → tìm trong mô tả
3. send_product_images - Cho câu hỏi "xem ảnh", "gửi ảnh", "cho xem hình"
4. provide_order_link - Cho câu hỏi "đặt hàng", "mua hàng", "tôi muốn mua", "order"
5. send_product_videos - Cho câu hỏi "xem video", "có video không"
"""
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": text}
            ],
            tools=get_tools_definition(),
            tool_choice="auto",
            temperature=0.1
        )
        
        msg = response.choices[0].message
        
        if msg.tool_calls:
            for tool in msg.tool_calls:
                tool_name = tool.function.name
                tool_args = json.loads(tool.function.arguments)
                
                if "ms" not in tool_args:
                    tool_args["ms"] = current_ms
                
                tool_result = execute_tool(uid, tool_name, tool_args)
                
                follow_up_response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": text},
                        msg,
                        {"role": "tool", "tool_call_id": tool.id, "name": tool_name, "content": tool_result}
                    ],
                    temperature=0.1
                )
                
                final_reply = follow_up_response.choices[0].message.content
                send_message(uid, final_reply)
                
                # Lưu lịch sử hội thoại
                ctx["conversation_history"].append({"role": "user", "content": text})
                ctx["conversation_history"].append({"role": "assistant", "content": final_reply})
                ctx["conversation_history"] = ctx["conversation_history"][-10:]
        else:
            send_message(uid, msg.content)
            ctx["conversation_history"].append({"role": "user", "content": text})
            ctx["conversation_history"].append({"role": "assistant", "content": msg.content})
            ctx["conversation_history"] = ctx["conversation_history"][-10:]
            
    except Exception as e:
        print(f"GPT Error: {e}")
        send_message(uid, "Dạ em đang gặp chút trục trặc, anh/chị vui lòng thử lại sau ạ.")
        
# ============================================
# FACEBOOK CONVERSION API FUNCTIONS - ASYNC
# ============================================

def queue_facebook_event(event_type: str, event_data: dict):
    """
    Thêm sự kiện vào queue để xử lý bất đồng bộ
    KHÔNG chờ kết quả, KHÔNG block bot
    """
    if not FACEBOOK_PIXEL_ID or not FACEBOOK_ACCESS_TOKEN:
        return False
    
    # Thêm vào queue
    queue_item = {
        'event_type': event_type,
        'data': event_data,
        'timestamp': time.time()
    }
    
    # Giới hạn queue size để tránh memory leak
    if FACEBOOK_EVENT_QUEUE.qsize() < 1000:  # Max 1000 sự kiện trong queue
        FACEBOOK_EVENT_QUEUE.put(queue_item)
        return True
    else:
        print(f"[FACEBOOK QUEUE] Queue đầy, bỏ qua sự kiện {event_type}")
        return False

def _send_view_content_async(event_data: dict):
    """Gửi sự kiện ViewContent bất đồng bộ"""
    try:
        data = event_data['data']
        
        payload = {
            "data": [{
                "event_name": "ViewContent",
                "event_time": int(data.get('event_time', time.time())),
                "action_source": "website",
                "user_data": data['user_data'],
                "custom_data": {
                    "currency": "VND",
                    "value": data.get('price', 0),
                    "content_ids": [data.get('ms', '')],
                    "content_name": data.get('product_name', '')[:100],
                    "content_type": "product",
                    "content_category": "fashion",
                }
            }]
        }
        
        # Thêm event_source_url nếu có
        if data.get('event_source_url'):
            payload["data"][0]["event_source_url"] = data['event_source_url']
        
        url = f"https://graph.facebook.com/{FACEBOOK_API_VERSION}/{FACEBOOK_PIXEL_ID}/events"
        
        response = requests.post(
            url,
            params={"access_token": FACEBOOK_ACCESS_TOKEN},
            json=payload,
            timeout=3  # Timeout ngắn, không chờ đợi lâu
        )
        
        if response.status_code == 200:
            print(f"[FACEBOOK CAPI ASYNC] Đã gửi ViewContent cho {data.get('ms')}")
        else:
            print(f"[FACEBOOK CAPI ASYNC ERROR] {response.status_code}: {response.text[:100]}")
            
    except requests.exceptions.Timeout:
        print(f"[FACEBOOK CAPI TIMEOUT] Timeout khi gửi ViewContent")
    except Exception as e:
        print(f"[FACEBOOK CAPI EXCEPTION] {e}")

def _send_add_to_cart_async(event_data: dict):
    """Gửi sự kiện AddToCart bất đồng bộ"""
    try:
        data = event_data['data']
        
        payload = {
            "data": [{
                "event_name": "AddToCart",
                "event_time": int(data.get('event_time', time.time())),
                "action_source": "website",
                "user_data": data['user_data'],
                "custom_data": {
                    "currency": "VND",
                    "value": data.get('price', 0) * data.get('quantity', 1),
                    "content_ids": [data.get('ms', '')],
                    "content_name": data.get('product_name', '')[:100],
                    "content_type": "product",
                    "num_items": data.get('quantity', 1)
                }
            }]
        }
        
        url = f"https://graph.facebook.com/{FACEBOOK_API_VERSION}/{FACEBOOK_PIXEL_ID}/events"
        
        response = requests.post(
            url,
            params={"access_token": FACEBOOK_ACCESS_TOKEN},
            json=payload,
            timeout=3
        )
        
        if response.status_code == 200:
            print(f"[FACEBOOK CAPI ASYNC] Đã gửi AddToCart cho {data.get('ms')}")
        else:
            print(f"[FACEBOOK CAPI ASYNC ERROR] {response.status_code}: {response.text[:100]}")
            
    except requests.exceptions.Timeout:
        print(f"[FACEBOOK CAPI TIMEOUT] Timeout khi gửi AddToCart")
    except Exception as e:
        print(f"[FACEBOOK CAPI EXCEPTION] {e}")

def _send_purchase_async(event_data: dict):
    """Gửi sự kiện Purchase bất đồng bộ"""
    try:
        data = event_data['data']
        
        payload = {
            "data": [{
                "event_name": "Purchase",
                "event_time": int(data.get('event_time', time.time())),
                "action_source": "website",
                "user_data": data['user_data'],
                "custom_data": {
                    "currency": "VND",
                    "value": data.get('total_price', 0),
                    "content_ids": [data.get('ms', '')],
                    "content_name": data.get('product_name', '')[:100],
                    "content_type": "product",
                    "num_items": data.get('quantity', 1),
                    "order_id": data.get('order_id', f"ORD{int(time.time())}")
                }
            }]
        }
        
        # Thêm event_source_url nếu có
        if data.get('event_source_url'):
            payload["data"][0]["event_source_url"] = data['event_source_url']
        
        url = f"https://graph.facebook.com/{FACEBOOK_API_VERSION}/{FACEBOOK_PIXEL_ID}/events"
        
        response = requests.post(
            url,
            params={"access_token": FACEBOOK_ACCESS_TOKEN},
            json=payload,
            timeout=3
        )
        
        if response.status_code == 200:
            print(f"[FACEBOOK CAPI ASYNC] Đã gửi Purchase cho đơn hàng {data.get('order_id')}")
        else:
            print(f"[FACEBOOK CAPI ASYNC ERROR] {response.status_code}: {response.text[:100]}")
            
    except requests.exceptions.Timeout:
        print(f"[FACEBOOK CAPI TIMEOUT] Timeout khi gửi Purchase")
    except Exception as e:
        print(f"[FACEBOOK CAPI EXCEPTION] {e}")

def _send_initiate_checkout_async(event_data: dict):
    """Gửi sự kiện InitiateCheckout bất đồng bộ"""
    try:
        data = event_data['data']
        
        payload = {
            "data": [{
                "event_name": "InitiateCheckout",
                "event_time": int(data.get('event_time', time.time())),
                "action_source": "website",
                "user_data": data['user_data'],
                "custom_data": {
                    "currency": "VND",
                    "value": data.get('price', 0) * data.get('quantity', 1),
                    "content_ids": [data.get('ms', '')],
                    "content_name": data.get('product_name', '')[:100],
                    "content_type": "product",
                    "num_items": data.get('quantity', 1)
                }
            }]
        }
        
        # Thêm event_source_url nếu có
        if data.get('event_source_url'):
            payload["data"][0]["event_source_url"] = data['event_source_url']
        
        url = f"https://graph.facebook.com/{FACEBOOK_API_VERSION}/{FACEBOOK_PIXEL_ID}/events"
        
        response = requests.post(
            url,
            params={"access_token": FACEBOOK_ACCESS_TOKEN},
            json=payload,
            timeout=3
        )
        
        if response.status_code == 200:
            print(f"[FACEBOOK CAPI ASYNC] Đã gửi InitiateCheckout cho {data.get('ms')}")
        else:
            print(f"[FACEBOOK CAPI ASYNC ERROR] {response.status_code}: {response.text[:100]}")
            
    except requests.exceptions.Timeout:
        print(f"[FACEBOOK CAPI TIMEOUT] Timeout khi gửi InitiateCheckout")
    except Exception as e:
        print(f"[FACEBOOK CAPI EXCEPTION] {e}")

def get_fbclid_from_context(uid: str) -> Optional[str]:
    """
    Lấy fbclid từ context của user (nếu có từ referral)
    """
    ctx = USER_CONTEXT.get(uid, {})
    referral_payload = ctx.get("referral_payload", "")
    
    if referral_payload and "fbclid=" in referral_payload:
        match = re.search(r'fbclid=([^&]+)', referral_payload)
        if match:
            return match.group(1)
    
    return None

def prepare_user_data_for_capi(uid: str, phone: str = None, client_ip: str = None, user_agent: str = None):
    """
    Chuẩn bị user_data cho Conversion API
    """
    user_data = {
        "client_user_agent": user_agent or "",
        "client_ip_address": client_ip or "",
    }
    
    # Thêm fbclid nếu có
    fbclid = get_fbclid_from_context(uid)
    if fbclid:
        user_data["fbc"] = f"fb.1.{int(time.time())}.{fbclid}"
    
    # Hash phone nếu có
    if phone:
        # Chuẩn hóa số điện thoại
        phone_clean = re.sub(r'[^\d]', '', phone)
        if phone_clean.startswith('0'):
            phone_clean = '84' + phone_clean[1:]
        elif phone_clean.startswith('+84'):
            phone_clean = phone_clean[1:]
        
        # Hash SHA256
        phone_hash = hashlib.sha256(phone_clean.encode()).hexdigest()
        user_data["ph"] = phone_hash
    
    return user_data

def send_view_content_smart(uid: str, ms: str, product_name: str, price: float, referral_source: str = "direct"):
    """
    Gửi ViewContent THÔNG MINH - chỉ gửi 1 lần mỗi 30 phút cho cùng user + product
    """
    if not FACEBOOK_PIXEL_ID:
        return
    
    # Key cache: user + product
    cache_key = f"{uid}_{ms}"
    
    # Kiểm tra cache trong memory
    if hasattr(send_view_content_smart, 'cache'):
        last_sent = send_view_content_smart.cache.get(cache_key, 0)
        now = time.time()
        
        # Nếu đã gửi trong 30 phút gần đây, bỏ qua
        if now - last_sent < 1800:  # 30 phút = 1800 giây
            print(f"[FACEBOOK CAPI SMART] Đã gửi ViewContent cho {ms} trong 30 phút gần đây, bỏ qua")
            return
    
    # Lấy context để có user_data
    ctx = USER_CONTEXT.get(uid, {})
    phone = ctx.get("order_data", {}).get("phone", "")
    
    # Chuẩn bị user_data đơn giản (không cần IP, user_agent cho ViewContent từ bot)
    user_data = {
        "fbp": f"fb.1.{int(time.time())}.{uid[:10] if uid else str(int(time.time()))}",
    }
    
    # Hash phone nếu có
    if phone:
        phone_clean = re.sub(r'[^\d]', '', phone)
        if phone_clean.startswith('0'):
            phone_clean = '84' + phone_clean[1:]
        phone_hash = hashlib.sha256(phone_clean.encode()).hexdigest()
        user_data["ph"] = phone_hash
    
    # Thêm fbclid nếu có
    fbclid = get_fbclid_from_context(uid)
    if fbclid:
        user_data["fbc"] = f"fb.1.{int(time.time())}.{fbclid}"
    
    # Chuẩn bị event data
    event_data = {
        'uid': uid,
        'ms': ms,
        'product_name': product_name,
        'price': price,
        'user_data': user_data,
        'event_time': int(time.time()),
        'event_source_url': f"https://www.facebook.com/{PAGE_ID}" if PAGE_ID else f"https://{DOMAIN}",
        'referral_source': referral_source
    }
    
    # Thêm vào queue để xử lý bất đồng bộ
    queued = queue_facebook_event('ViewContent', event_data)
    
    if queued:
        # Cập nhật cache
        if not hasattr(send_view_content_smart, 'cache'):
            send_view_content_smart.cache = {}
        send_view_content_smart.cache[cache_key] = time.time()
        
        # Dọn dẹp cache cũ (giữ tối đa 1000 entries)
        if len(send_view_content_smart.cache) > 1000:
            # Giữ 500 entries mới nhất
            items = sorted(send_view_content_smart.cache.items(), key=lambda x: x[1], reverse=True)[:500]
            send_view_content_smart.cache = dict(items)
        
        print(f"[FACEBOOK CAPI SMART] Đã queue ViewContent cho {ms}")
    else:
        print(f"[FACEBOOK CAPI SMART] Không thể queue ViewContent, queue đầy")

def send_add_to_cart_smart(uid: str, ms: str, product_name: str, price: float, quantity: int = 1):
    """
    Gửi AddToCart sự kiện thông minh
    """
    if not FACEBOOK_PIXEL_ID:
        return
    
    ctx = USER_CONTEXT.get(uid, {})
    phone = ctx.get("order_data", {}).get("phone", "")
    
    user_data = prepare_user_data_for_capi(uid, phone)
    
    event_data = {
        'uid': uid,
        'ms': ms,
        'product_name': product_name,
        'price': price,
        'quantity': quantity,
        'user_data': user_data,
        'event_time': int(time.time())
    }
    
    # Thêm vào queue để xử lý bất đồng bộ
    queued = queue_facebook_event('AddToCart', event_data)
    
    if queued:
        print(f"[FACEBOOK CAPI SMART] Đã queue AddToCart cho {ms}")
    else:
        print(f"[FACEBOOK CAPI SMART] Không thể queue AddToCart, queue đầy")

def send_purchase_smart(uid: str, ms: str, product_name: str, order_data: dict):
    """
    Gửi Purchase sự kiện thông minh
    """
    if not FACEBOOK_PIXEL_ID:
        return
    
    phone = order_data.get("phone", "")
    total_price = order_data.get("total_price", 0)
    quantity = order_data.get("quantity", 1)
    
    # Lấy client IP và user agent từ request (nếu có)
    user_data = prepare_user_data_for_capi(uid, phone)
    
    event_data = {
        'uid': uid,
        'ms': ms,
        'product_name': product_name,
        'total_price': total_price,
        'quantity': quantity,
        'user_data': user_data,
        'event_time': int(time.time()),
        'order_id': order_data.get("order_id", f"ORD{int(time.time())}_{uid[-4:] if uid else '0000'}"),
        'event_source_url': f"https://{DOMAIN}/messenger-order?ms={ms}&uid={uid}"
    }
    
    # Thêm vào queue để xử lý bất đồng bộ
    queued = queue_facebook_event('Purchase', event_data)
    
    if queued:
        print(f"[FACEBOOK CAPI SMART] Đã queue Purchase cho {ms}")
    else:
        print(f"[FACEBOOK CAPI SMART] Không thể queue Purchase, queue đầy")

def send_initiate_checkout_smart(uid: str, ms: str, product_name: str, price: float, quantity: int = 1):
    """
    Gửi InitiateCheckout sự kiện thông minh
    """
    if not FACEBOOK_PIXEL_ID:
        return
    
    ctx = USER_CONTEXT.get(uid, {})
    phone = ctx.get("order_data", {}).get("phone", "")
    
    user_data = prepare_user_data_for_capi(uid, phone)
    
    event_data = {
        'uid': uid,
        'ms': ms,
        'product_name': product_name,
        'price': price,
        'quantity': quantity,
        'user_data': user_data,
        'event_time': int(time.time()),
        'event_source_url': f"https://{DOMAIN}/messenger-order?ms={ms}&uid={uid}"
    }
    
    # Thêm vào queue để xử lý bất đồng bộ
    queued = queue_facebook_event('InitiateCheckout', event_data)
    
    if queued:
        print(f"[FACEBOOK CAPI SMART] Đã queue InitiateCheckout cho {ms}")
    else:
        print(f"[FACEBOOK CAPI SMART] Không thể queue InitiateCheckout, queue đầy")

# ============================================
# GỬI CAROUSEL 1 SẢN PHẨM
# ============================================

def send_single_product_carousel(uid: str, ms: str):
    """
    Gửi carousel chỉ với 1 sản phẩm duy nhất
    Sử dụng khi bot đã nhận diện được MS từ ad_title, catalog, Fchat
    """
    if ms not in PRODUCTS:
        print(f"[SINGLE CAROUSEL ERROR] Sản phẩm {ms} không tồn tại")
        return
    
    load_products()
    product = PRODUCTS[ms]
    
    images_field = product.get("Images", "")
    urls = parse_image_urls(images_field)
    image_url = urls[0] if urls else ""
    
    gia_raw = product.get("Gia", "")
    gia_int = extract_price_int(gia_raw) or 0
    
    # LẤY TÊN SẢN PHẨM (KHÔNG BAO GỒM MÃ SẢN PHẨM)
    product_name = product.get('Ten', '')
    
    # KIỂM TRA NẾU TÊN ĐÃ CHỨA MÃ SẢN PHẨM, CHỈ GIỮ TÊN
    if f"[{ms}]" in product_name or ms in product_name:
        # Xóa mã sản phẩm khỏi tên
        product_name = product_name.replace(f"[{ms}]", "").replace(ms, "").strip()
    
    element = {
        "title": product_name,  # CHỈ HIỂN THỊ TÊN SẢN PHẨM
        "image_url": image_url,
        "subtitle": f"💰 Giá: {gia_int:,.0f} đ",
        "buttons": [
            {
                "type": "postback",
                "title": "🌟 Ưu điểm SP",
                "payload": f"PRODUCT_HIGHLIGHTS_{ms}"
            },
            {
                "type": "postback", 
                "title": "🖼️ Xem ảnh",
                "payload": f"VIEW_IMAGES_{ms}"
            },
            {
                "type": "web_url",
                "title": "🛒 Đặt ngay",
                "url": f"https://{DOMAIN}/messenger-order?ms={ms}&uid={uid}",
                "webview_height_ratio": "tall",
                "messenger_extensions": True,
                "webview_share_button": "hide"
            }
        ]
    }
    
    send_carousel_template(uid, [element])
    
    ctx = USER_CONTEXT[uid]
    ctx["last_ms"] = ms
    
    # Gọi hàm update_product_context cũ để duy trì tính năng cũ
    if "product_history" not in ctx:
        ctx["product_history"] = []
    
    if not ctx["product_history"] or ctx["product_history"][0] != ms:
        if ms in ctx["product_history"]:
            ctx["product_history"].remove(ms)
        ctx["product_history"].insert(0, ms)
    
    if len(ctx["product_history"]) > 5:
        ctx["product_history"] = ctx["product_history"][:5]
    
    ctx["has_sent_first_carousel"] = True
    
    # GỬI SỰ KIỆN VIEWCONTENT THÔNG MINH (BẤT ĐỒNG BỘ)
    try:
        # Lấy referral source từ context
        referral_source = ctx.get("referral_source", "direct")
        
        # Gửi sự kiện ViewContent SMART (bất đồng bộ)
        send_view_content_smart(
            uid=uid,
            ms=ms,
            product_name=product_name,
            price=gia_int,
            referral_source=referral_source
        )
        
        print(f"[FACEBOOK CAPI] Đã queue ViewContent cho {ms}")
    except Exception as e:
        print(f"[FACEBOOK CAPI ERROR] Lỗi queue ViewContent: {e}")
        # KHÔNG ảnh hưởng đến việc gửi carousel
    
    print(f"✅ [SINGLE CAROUSEL] Đã gửi carousel 1 sản phẩm {ms} cho user {uid}")

# ============================================
# HANDLE ORDER FORM STATE
# ============================================

def reset_order_state(uid: str):
    ctx = USER_CONTEXT[uid]
    ctx["order_state"] = None
    ctx["order_data"] = {}

def handle_order_form_step(uid: str, text: str) -> bool:
    ctx = USER_CONTEXT[uid]
    state = ctx.get("order_state")
    if not state:
        return False

    data = ctx.get("order_data", {})

    if state == "ask_name":
        data["customerName"] = text.strip()
        ctx["order_state"] = "ask_phone"
        send_message(uid, "Dạ em cảm ơn anh/chị. Anh/chị cho em xin số điện thoại ạ?")
        ctx["dirty"] = True  # ← THÊM DÒNG NÀY
        return True
        
    if state == "ask_phone":
        phone = re.sub(r"[^\d+]", "", text)
        if len(phone) < 9:
            send_message(uid, "Số điện thoại chưa đúng lắm, anh/chị nhập lại giúp em (tối thiểu 9 số) ạ?")
            return True
        data["phone"] = phone
        ctx["order_state"] = "ask_address"
        send_message(uid, "Dạ vâng. Anh/chị cho em xin địa chỉ nhận hàng ạ?")
        ctx["dirty"] = True
        return True

    if state == "ask_address":
        data["address"] = text.strip()
        ctx["order_state"] = None
        ctx["order_data"] = data
        ctx["dirty"] = True

        summary = (
            "Dạ em tóm tắt lại đơn hàng của anh/chị:\n"
            f"- Sản phẩm: {data.get('productName', '')}\n"
            f"- Mã: {data.get('ms', '')}\n"
            f"- Phân loại: {data.get('color', '')} / {data.get('size', '')}\n"
            f"- Số lượng: {data.get('quantity', '1')}\n"
            f"- Thành tiền dự kiến: {data.get('total', '')}\n"
            f"- Người nhận: {data.get('customerName', '')}\n"
            f"- SĐT: {data.get('phone', '')}\n"
            f"- Địa chỉ: {data.get('address', '')}\n\n"
            "Anh/chị kiểm tra giúp em xem đã đúng chưa ạ?"
        )
        send_message(uid, summary)
        return True

    return False

# ============================================
# HANDLE POSTBACK THÔNG MINH - ĐÃ SỬA ĐỂ GỬI NÚT ĐẶT HÀNG ĐẸP
# ============================================

def handle_postback_with_recovery(uid: str, payload: str, postback_id: str = None):
    """
    Xử lý postback - FIX LỖI GỬI LẶP VÔ HẠN
    CHỈ XỬ LÝ 1 LẦN DUY NHẤT CHO MỖI POSTBACK_ID
    THÊM LƯU NGAY VÀO GOOGLE SHEETS KHI CẬP NHẬT MS
    """
    now = time.time()
    
    if postback_id:
        idempotency_key = f"{uid}_{postback_id}"
    else:
        idempotency_key = f"{uid}_{payload}_{int(now)}"
    
    ctx = USER_CONTEXT[uid]
    
    if "idempotent_postbacks" not in ctx:
        ctx["idempotent_postbacks"] = {}
    
    if idempotency_key in ctx["idempotent_postbacks"]:
        processed_time = ctx["idempotent_postbacks"][idempotency_key]
        if now - processed_time < 300:
            print(f"[IDEMPOTENCY BLOCK] Bỏ qua postback đã xử lý: {idempotency_key}")
            return True
    
    ctx["idempotent_postbacks"][idempotency_key] = now
    
    if len(ctx["idempotent_postbacks"]) > 50:
        sorted_items = sorted(ctx["idempotent_postbacks"].items(), 
                            key=lambda x: x[1], reverse=True)[:30]
        ctx["idempotent_postbacks"] = dict(sorted_items)
    
    load_products()
    
    # Xử lý các loại postback
    if payload.startswith("PRODUCT_HIGHLIGHTS_"):
        ms = payload.replace("PRODUCT_HIGHLIGHTS_", "")
        if ms in PRODUCTS:
            ctx["last_ms"] = ms
            ctx["dirty"] = True
            
            # ============================================
            # QUAN TRỌNG: LƯU NGAY VÀO GOOGLE SHEETS KHI CLICK NÚT
            # ============================================
            def save_immediately_postback():
                try:
                    print(f"[POSTBACK IMMEDIATE SAVE] Đang lưu ngay MS {ms} cho user {uid}...")
                    save_single_user_to_sheets(uid, ctx)
                    print(f"[POSTBACK IMMEDIATE SAVE COMPLETE] Đã lưu xong user {uid} vào Google Sheets")
                except Exception as e:
                    print(f"[POSTBACK IMMEDIATE SAVE ERROR] Lỗi khi lưu user {uid}: {e}")
            
            # Chạy trong thread riêng để không block bot
            threading.Thread(target=save_immediately_postback, daemon=True).start()
            # ============================================
            
            # Gọi hàm update_product_context cũ
            if "product_history" not in ctx:
                ctx["product_history"] = []
            
            if not ctx["product_history"] or ctx["product_history"][0] != ms:
                if ms in ctx["product_history"]:
                    ctx["product_history"].remove(ms)
                ctx["product_history"].insert(0, ms)
            
            if len(ctx["product_history"]) > 5:
                ctx["product_history"] = ctx["product_history"][:5]
            
            # Lấy thông tin sản phẩm
            product = PRODUCTS[ms]
            mo_ta = product.get("MoTa", "")
            ten_sp = product.get("Ten", "")
            
            if not mo_ta:
                send_message(uid, f"Dạ sản phẩm [{ms}] {ten_sp} chưa có mô tả chi tiết ạ. Anh/chị có thể hỏi về giá, màu sắc, size hoặc đặt hàng ạ!")
                return True
            
            if not client:
                send_message(uid, "Dạ chức năng này tạm thời chưa khả dụng ạ. Anh/chị vui lòng thử lại sau!")
                return True
            
            # Gọi GPT để tóm tắt 5 ưu điểm
            try:
                system_prompt = """Bạn là một trợ lý bán hàng chuyên nghiệp. 
Hãy đọc kỹ mô tả sản phẩm và liệt kê 5 ưu điểm nổi bật nhất của sản phẩm đó. 
Mỗi ưu điểm phải:
1. Ngắn gọn, rõ ràng (1-2 dòng)
2. Bắt đầu bằng dấu gạch đầu dòng (-)
3. Tập trung vào lợi ích cho khách hàng
4. Chỉ trả lời bằng tiếng Việt
5. Không thêm bất kỳ lời giải thích nào khác

Định dạng đầu ra:
- [Ưu điểm 1]
- [Ưu điểm 2]
- [Ưu điểm 3]
- [Ưu điểm 4]
- [Ưu điểm 5]"""
                
                # Giới hạn độ dài của mô tả
                max_length = 3000
                if len(mo_ta) > max_length:
                    mo_ta = mo_ta[:max_length] + "..."
                
                user_prompt = f"""Sản phẩm: {ten_sp}
Mã sản phẩm: {ms}

Mô tả sản phẩm:
{mo_ta}

Hãy liệt kê 5 ưu điểm nổi bật nhất của sản phẩm này theo định dạng yêu cầu."""

                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    temperature=0.3,
                    max_tokens=500
                )
                
                highlights = response.choices[0].message.content
                
                # Đảm bảo định dạng đúng
                if not highlights.startswith("-"):
                    # Thêm dấu gạch đầu dòng nếu GPT quên
                    lines = highlights.strip().split('\n')
                    formatted_lines = []
                    for line in lines:
                        line = line.strip()
                        if line and not line.startswith('-'):
                            formatted_lines.append(f"- {line}")
                        else:
                            formatted_lines.append(line)
                    highlights = '\n'.join(formatted_lines)
                
                # Gửi cho khách hàng với tiêu đề
                message = f"🌟 **5 ƯU ĐIỂM NỔI BẬT CỦA SẢN PHẨM [{ms}]** 🌟\n\n{highlights}\n\n---\nAnh/chị cần em tư vấn thêm gì không ạ?"
                send_message(uid, message)
                
            except Exception as e:
                print(f"Lỗi khi gọi GPT cho ưu điểm sản phẩm: {e}")
                send_message(uid, "Dạ em chưa thể tóm tắt ưu điểm sản phẩm ngay lúc này. Anh/chị có thể xem mô tả chi tiết hoặc hỏi về thông tin khác ạ!")
            
            return True
            
    elif payload.startswith("VIEW_IMAGES_"):
        ms = payload.replace("VIEW_IMAGES_", "")
        if ms in PRODUCTS:
            ctx["last_ms"] = ms
            ctx["dirty"] = True
            
            # ============================================
            # QUAN TRỌNG: LƯU NGAY VÀO GOOGLE SHEETS KHI CLICK NÚT
            # ============================================
            def save_immediately_postback():
                try:
                    print(f"[POSTBACK IMMEDIATE SAVE] Đang lưu ngay MS {ms} cho user {uid}...")
                    save_single_user_to_sheets(uid, ctx)
                    print(f"[POSTBACK IMMEDIATE SAVE COMPLETE] Đã lưu xong user {uid} vào Google Sheets")
                except Exception as e:
                    print(f"[POSTBACK IMMEDIATE SAVE ERROR] Lỗi khi lưu user {uid}: {e}")
            
            # Chạy trong thread riêng để không block bot
            threading.Thread(target=save_immediately_postback, daemon=True).start()
            # ============================================
            
            # Gọi hàm update_product_context cũ
            if "product_history" not in ctx:
                ctx["product_history"] = []
            
            if not ctx["product_history"] or ctx["product_history"][0] != ms:
                if ms in ctx["product_history"]:
                    ctx["product_history"].remove(ms)
                ctx["product_history"].insert(0, ms)
            
            if len(ctx["product_history"]) > 5:
                ctx["product_history"] = ctx["product_history"][:5]
            
            # Gọi GPT để xử lý việc gửi ảnh
            handle_text_with_function_calling(uid, "gửi ảnh sản phẩm cho tôi xem")
            return True
    
    elif payload.startswith("ORDER_BUTTON_"):
        ms = payload.replace("ORDER_BUTTON_", "")
        if ms in PRODUCTS:
            # CẬP NHẬT MS NGAY KHI CLICK NÚT ĐẶT HÀNG
            ctx["last_ms"] = ms
            ctx["dirty"] = True
            
            # ============================================
            # QUAN TRỌNG: LƯU NGAY VÀO GOOGLE SHEETS KHI CLICK NÚT ĐẶT HÀNG
            # ============================================
            def save_immediately_postback():
                try:
                    print(f"[POSTBACK IMMEDIATE SAVE] Đang lưu ngay MS {ms} cho user {uid} (ORDER_BUTTON)...")
                    save_single_user_to_sheets(uid, ctx)
                    print(f"[POSTBACK IMMEDIATE SAVE COMPLETE] Đã lưu xong user {uid} vào Google Sheets")
                except Exception as e:
                    print(f"[POSTBACK IMMEDIATE SAVE ERROR] Lỗi khi lưu user {uid}: {e}")
            
            # Chạy trong thread riêng để không block bot
            threading.Thread(target=save_immediately_postback, daemon=True).start()
            # ============================================
            
            # Cập nhật product_history
            if "product_history" not in ctx:
                ctx["product_history"] = []
            
            if not ctx["product_history"] or ctx["product_history"][0] != ms:
                if ms in ctx["product_history"]:
                    ctx["product_history"].remove(ms)
                ctx["product_history"].insert(0, ms)
            
            if len(ctx["product_history"]) > 5:
                ctx["product_history"] = ctx["product_history"][:5]
            
            # Gửi sự kiện AddToCart khi click nút đặt hàng
            try:
                product = PRODUCTS[ms]
                product_name = product.get('Ten', '')
                
                if f"[{ms}]" in product_name or ms in product_name:
                    product_name = product_name.replace(f"[{ms}]", "").replace(ms, "").strip()
                
                gia_int = extract_price_int(product.get("Gia", "")) or 0
                
                send_add_to_cart_smart(
                    uid=uid,
                    ms=ms,
                    product_name=product_name,
                    price=gia_int
                )
                
                print(f"[FACEBOOK CAPI] Đã queue AddToCart từ nút đặt hàng: {ms}")
            except Exception as e:
                print(f"[FACEBOOK CAPI ERROR] Lỗi queue AddToCart: {e}")
            
            # THAY VÌ GỬI LINK THÔ, GỬI NÚT ĐẶT HÀNG ĐẸP
            send_order_button_template(uid, ms, product_name)
            
            return True
    
    elif payload in ["PRICE_QUERY", "COLOR_QUERY", "SIZE_QUERY", "MATERIAL_QUERY", "STOCK_QUERY"]:
        ms = ctx.get("last_ms")
        
        if ms and ms in PRODUCTS:
            question_map = {
                "PRICE_QUERY": "giá bao nhiêu",
                "COLOR_QUERY": "có những màu gì",
                "SIZE_QUERY": "có size nào",
                "MATERIAL_QUERY": "chất liệu gì",
                "STOCK_QUERY": "còn hàng không"
            }
            
            question = question_map.get(payload, "thông tin sản phẩm")
            handle_text_with_function_calling(uid, question)
            return True
    
    elif payload == "GET_STARTED":
        welcome_msg = f"""Chào anh/chị! 👋 
Em là nhân viên tư vấn của {get_fanpage_name_from_api()}.

Vui lòng gửi mã sản phẩm (ví dụ: MS123456) hoặc mô tả sản phẩm."""
        send_message(uid, welcome_msg)
        return True
    
    return False

# ============================================
# HANDLE TEXT MESSAGES - ĐÃ SỬA ĐỔI LOGIC CAROUSEL
# ============================================

def handle_text(uid: str, text: str, referral_data: dict = None):
    """Xử lý tin nhắn văn bản với logic mới: 
       ƯU TIÊN XỬ LÝ REFERRAL TỪ CATALOG TRƯỚC KHI XỬ LÝ TEXT"""
    if not text or len(text.strip()) == 0:
        return
    
    ctx = USER_CONTEXT[uid]

    if ctx.get("processing_lock"):
        print(f"[TEXT SKIP] User {uid} đang được xử lý")
        return

    ctx["processing_lock"] = True

    try:
        now = time.time()
        last_msg_time = ctx.get("last_msg_time", 0)
        
        # Debounce: kiểm tra tin nhắn trùng lặp
        if now - last_msg_time < 2:
            last_text = ctx.get("last_processed_text", "")
            if text.strip().lower() == last_text.lower():
                print(f"[TEXT DEBOUNCE] Bỏ qua tin nhắn trùng lặp: {text[:50]}...")
                ctx["processing_lock"] = False
                return
        
        ctx["last_msg_time"] = now
        ctx["last_processed_text"] = text.strip().lower()
        
        load_products()
        
        # ============================================
        # QUAN TRỌNG: ƯU TIÊN XỬ LÝ REFERRAL TỪ CATALOG TRƯỚC
        # ============================================
        if referral_data:
            print(f"[CATALOG REFERRAL DETECTED] Xử lý referral cho user {uid}: {referral_data}")
            
            # Lấy MS từ referral (ad_id hoặc ref)
            ad_id = referral_data.get("ad_id", "")
            ref = referral_data.get("ref", "")
            
            detected_ms = None
            
            # Ưu tiên 1: Trích xuất từ ad_id
            if ad_id:
                detected_ms = extract_ms_from_retailer_id(ad_id)
                if detected_ms:
                    print(f"[CATALOG REFERRAL] Tìm thấy MS từ ad_id {ad_id}: {detected_ms}")
            
            # Ưu tiên 2: Trích xuất từ ref
            if not detected_ms and ref:
                detected_ms = extract_ms_from_ad_title(ref)
                if detected_ms:
                    print(f"[CATALOG REFERRAL] Tìm thấy MS từ ref {ref}: {detected_ms}")
            
            # Nếu tìm thấy MS từ catalog
            if detected_ms and detected_ms in PRODUCTS:
                # Cập nhật context với MS mới từ catalog (RESET COUNTER)
                update_context_with_new_ms(uid, detected_ms, "catalog_referral")
                
                # Gửi carousel ngay lập tức
                print(f"[CATALOG REFERRAL] Gửi carousel cho {detected_ms} từ catalog")
                send_single_product_carousel(uid, detected_ms)
                
                # Nếu text là câu hỏi về giá, dùng GPT trả lời
                text_lower = text.lower()
                if any(keyword in text_lower for keyword in ["giá", "bao nhiêu", "price", "cost"]):
                    print(f"[CATALOG REFERRAL + PRICE QUERY] Dùng GPT trả lời về giá")
                    handle_text_with_function_calling(uid, text)
                else:
                    # Gửi tin nhắn chào mừng
                    product = PRODUCTS[detected_ms]
                    product_name = product.get('Ten', '')
                    if f"[{detected_ms}]" in product_name or detected_ms in product_name:
                        product_name = product_name.replace(f"[{detected_ms}]", "").replace(detected_ms, "").strip()
                    
                    send_message(uid, f"Chào anh/chị! 👋\n\nCảm ơn đã quan tâm đến sản phẩm **{product_name}** từ catalog. Em đã gửi thông tin chi tiết bên trên ạ!")
                
                ctx["processing_lock"] = False
                return
        
        # ============================================
        # THÊM: Khôi phục context nếu cần (khi Koyeb wake up)
        # ============================================
        if not ctx.get("last_ms") or ctx.get("last_ms") not in PRODUCTS:
            restored = restore_user_context_on_wakeup(uid)
            if restored:
                print(f"[TEXT HANDLER] Đã khôi phục context cho user {uid}")
        
        # ============================================
        # QUAN TRỌNG: TRUY XUẤT MS TỪ CONTEXT ĐÃ LOAD
        # ============================================
        
        # THỬ 1: Kiểm tra xem context đã có last_ms chưa
        current_ms = ctx.get("last_ms")
        
        # THỬ 2: Nếu chưa có, thử load từ Google Sheets NGAY LẬP TỨC
        if not current_ms or current_ms not in PRODUCTS:
            print(f"[CONTEXT MISSING] Không tìm thấy MS trong context, đang load từ Google Sheets...")
            
            # Load context từ Google Sheets (trực tiếp, không qua cache)
            context_from_sheets = get_user_context_from_sheets(uid)
            if context_from_sheets:
                # Cập nhật vào USER_CONTEXT (chỉ update các trường cần thiết)
                for key, value in context_from_sheets.items():
                    if key not in ctx or (key == "last_ms" and value):
                        ctx[key] = value
                
                current_ms = ctx.get("last_ms")
                print(f"[CONTEXT RELOAD] Đã load lại context từ Sheets, last_ms: {current_ms}")
                
                # Nếu vẫn không có last_ms, thử lấy từ product_history
                if not current_ms and ctx.get("product_history"):
                    current_ms = ctx["product_history"][0] if ctx["product_history"] else None
                    if current_ms:
                        ctx["last_ms"] = current_ms
                        print(f"[CONTEXT FALLBACK] Lấy MS từ product_history: {current_ms}")
        
        # THỬ 3: Nếu vẫn không có, thử tra cứu từ Orders sheet
        if not current_ms or current_ms not in PRODUCTS:
            print(f"[CONTEXT SEARCH] Đang tìm MS từ lịch sử đơn hàng...")
            orders = get_user_order_history_from_sheets(uid)
            if orders:
                current_ms = orders[0].get("ms")
                if current_ms and current_ms in PRODUCTS:
                    ctx["last_ms"] = current_ms
                    print(f"[CONTEXT FROM ORDERS] Tìm thấy MS từ đơn hàng: {current_ms}")
        
        # ============================================
        # LOG ĐỂ DEBUG
        # ============================================
        print(f"[CONTEXT DEBUG] User {uid}:")
        print(f"  - last_ms: {current_ms}")
        print(f"  - product_history: {ctx.get('product_history', [])[:3]}")
        print(f"  - real_message_count: {ctx.get('real_message_count', 0)}")
        
        # Tăng counter cho tin nhắn
        if "real_message_count" not in ctx:
            ctx["real_message_count"] = 0
        ctx["real_message_count"] += 1
        message_count = ctx["real_message_count"]
        
        print(f"[MESSAGE COUNT] User {uid}: tin nhắn thứ {message_count}")
        
        # Xử lý order state nếu có
        if handle_order_form_step(uid, text):
            ctx["processing_lock"] = False
            return
        
        # ============================================
        # NẾU KHÔNG TÌM THẤY MS TỪ BẤT KỲ NGUỒN NÀO
        # ============================================
        if not current_ms or current_ms not in PRODUCTS:
            print(f"[NO MS FOUND] Không tìm thấy MS cho user {uid} từ bất kỳ nguồn nào")
            
            # Kiểm tra nếu tin nhắn là câu hỏi chung (không có MS)
            general_questions = ['giá', 'bao nhiêu', 'màu gì', 'size nào', 'còn hàng', 'đặt hàng', 'mua', 'tư vấn', 'cách dùng', 'sử dụng']
            text_norm = normalize_vietnamese(text.lower())
            if any(keyword in text_norm for keyword in general_questions):
                # Yêu cầu khách gửi MS cụ thể
                send_message(uid, "Dạ, để em tư vấn chính xác cho anh/chị, vui lòng cho em biết mã sản phẩm hoặc gửi ảnh sản phẩm ạ! 🤗")
            else:
                # Gợi ý khách gửi MS hoặc ảnh
                send_message(uid, "Dạ em chưa biết anh/chị đang hỏi về sản phẩm nào. Vui lòng cho em biết mã sản phẩm hoặc gửi ảnh sản phẩm ạ! 🤗")
            
            ctx["processing_lock"] = False
            return
        
        # ============================================
        # TIẾP TỤC XỬ LÝ VỚI MS ĐÃ CÓ
        # ============================================
        
        print(f"[HAS MS FROM CONTEXT] User {uid} đã có MS từ context: {current_ms}")
        
        # Gửi carousel nếu: chưa gửi carousel cho sản phẩm này VÀ tin nhắn trong 3 tin đầu tiên
        if not ctx.get("has_sent_first_carousel") and message_count <= 3:
            print(f"🚨 [FIRST CAROUSEL FOR PRODUCT] Gửi carousel cho sản phẩm {current_ms} (tin nhắn thứ {message_count})")
            send_single_product_carousel(uid, current_ms)
            ctx["has_sent_first_carousel"] = True
        
        # Dùng GPT để trả lời theo MS HIỆN TẠI
        print(f"✅ [GPT REQUIRED] User {uid} đã có MS {current_ms}, dùng GPT trả lời")
        handle_text_with_function_calling(uid, text)

    except Exception as e:
        print(f"Error in handle_text for {uid}: {e}")
        try:
            send_message(uid, "Dạ em đang gặp chút trục trặc, anh/chị vui lòng thử lại sau ạ.")
        except:
            pass
    finally:
        ctx["processing_lock"] = False
        
# ============================================
# HANDLE IMAGE - CẢI TIẾN VỚI CAROUSEL GỢI Ý
# ============================================

def handle_image(uid: str, image_url: str):
    """Xử lý ảnh sản phẩm với công nghệ AI thông minh và carousel gợi Ý"""
    ctx = USER_CONTEXT[uid]
    
    now = time.time()
    last_image_time = ctx.get("last_image_time", 0)
    if now - last_image_time < 3:
        print(f"[IMAGE DEBOUNCE] Bỏ qua ảnh mới, chưa đủ thời gian")
        return
    
    ctx["last_image_time"] = now
    
    # BƯỚC 1: Kiểm tra xem có phải emoji/sticker không
    if is_emoji_or_sticker_image(image_url):
        print(f"[EMOJI DETECTED] Bỏ qua ảnh emoji/sticker: {image_url[:100]}")
        send_message(uid, "😊 Em đã nhận được biểu tượng cảm xúc của anh/chị! Nếu anh/chị muốn xem sản phẩm, vui lòng gửi ảnh thật của sản phẩm hoặc mã sản phẩm ạ!")
        return
    
    # BƯỚC 1.5: Kiểm tra ảnh có hợp lệ không
    if not is_valid_product_image(image_url):
        print(f"[INVALID IMAGE] Ảnh không hợp lệ: {image_url[:100]}")
        send_message(uid, "❌ Ảnh này không rõ hoặc không phải ảnh sản phẩm. Vui lòng gửi ảnh rõ hơn hoặc mã sản phẩm ạ!")
        return
    
    # BƯỚC 2: Thông báo đang xử lý ảnh
    send_message(uid, "🔍 Em đang phân tích ảnh sản phẩm bằng AI, vui lòng đợi một chút ạ...")
    
    # BƯỚC 3: Tìm sản phẩm bằng OpenAI Vision API
    found_ms = find_product_by_image(image_url)
    
    # BƯỚC 4: Xử lý kết quả
    if found_ms:
        print(f"[IMAGE PRODUCT FOUND] Tìm thấy sản phẩm {found_ms} từ ảnh")
        
        # Cập nhật context với MS mới
        update_context_with_new_ms(uid, found_ms, "image_search")
        
        # Gửi carousel sản phẩm đã tìm thấy
        send_single_product_carousel(uid, found_ms)
        
        # Dùng GPT để giới thiệu sản phẩm
        print(f"✅ [GPT REQUIRED] Tìm thấy sản phẩm từ ảnh, dùng GPT giới thiệu")
        handle_text_with_function_calling(uid, "Giới thiệu sản phẩm này cho tôi")
        
    else:
        print(f"[IMAGE PRODUCT NOT FOUND] Không tìm thấy sản phẩm từ ảnh")
        
        # Gửi thông báo không tìm thấy
        send_message(uid, "❌ Em chưa tìm thấy sản phẩm phù hợp với ảnh này. Có thể anh/chị chụp ảnh chưa rõ hoặc sản phẩm chưa có trong hệ thống.")
        
        # Gợi ý một số sản phẩm bằng CAROUSEL thay vì text
        send_message(uid, "Dưới đây là một số sản phẩm gợi ý cho anh/chị ạ:")
        
        # Gửi carousel gợi ý 3 sản phẩm
        carousel_sent = send_suggestion_carousel(uid, 3)
        
        # Nếu không gửi được carousel, gửi text backup
        if not carousel_sent:
            # Gợi ý một số sản phẩm phổ biến
            popular_products = list(PRODUCTS.keys())[:3]
            if popular_products:
                for ms in popular_products:
                    product = PRODUCTS[ms]
                    # Lấy tên sản phẩm (không bao gồm mã sản phẩm)
                    product_name = product.get('Ten', '')
                    if f"[{ms}]" in product_name or ms in product_name:
                        product_name = product_name.replace(f"[{ms}]", "").replace(ms, "").strip()
                    send_message(uid, f"📦 {product_name}")
        
        send_message(uid, "Vui lòng gửi mã sản phẩm chính xác (ví dụ: MS000004) để em tư vấn chi tiết ạ!")

def handle_catalog_referral(uid: str, referral_data: dict):
    """
    Xử lý referral từ catalog Facebook
    """
    try:
        print(f"[CATALOG REFERRAL HANDLER] Xử lý referral cho user {uid}: {referral_data}")
        
        ad_id = referral_data.get("ad_id", "")
        ref = referral_data.get("ref", "")
        source = referral_data.get("source", "CATALOG")
        
        detected_ms = None
        
        # Ưu tiên 1: Trích xuất từ ad_id
        if ad_id:
            detected_ms = extract_ms_from_retailer_id(ad_id)
            if detected_ms:
                print(f"[CATALOG REFERRAL] Tìm thấy MS từ ad_id {ad_id}: {detected_ms}")
        
        # Ưu tiên 2: Trích xuất từ ref
        if not detected_ms and ref:
            detected_ms = extract_ms_from_ad_title(ref)
            if detected_ms:
                print(f"[CATALOG REFERRAL] Tìm thấy MS từ ref {ref}: {detected_ms}")
        
        if detected_ms:
            # Kiểm tra sản phẩm có tồn tại không
            load_products()
            
            if detected_ms in PRODUCTS:
                # Cập nhật context với MS mới từ catalog (RESET COUNTER)
                update_context_with_new_ms(uid, detected_ms, f"catalog_{source}")
                
                # Gửi carousel ngay lập tức
                print(f"[CATALOG REFERRAL] Gửi carousel cho {detected_ms} từ catalog")
                send_single_product_carousel(uid, detected_ms)
                
                return detected_ms
            else:
                print(f"[CATALOG REFERRAL] MS {detected_ms} không tồn tại trong database")
        else:
            print(f"[CATALOG REFERRAL] Không thể trích xuất MS từ referral")
            
    except Exception as e:
        print(f"[CATALOG REFERRAL ERROR] Lỗi xử lý referral: {e}")
    
    return None

def handle_catalog_template(uid: str, template_payload: dict):
    """
    Xử lý template từ catalog Facebook (có retailer_id) để cập nhật sản phẩm hiện tại.
    Trả về mã sản phẩm nếu thành công, ngược lại None.
    """
    try:
        if template_payload.get('template_type') != 'generic':
            return None

        elements = template_payload.get('elements', [])
        for element in elements:
            retailer_id = element.get('retailer_id')
            if retailer_id:
                detected_ms = extract_ms_from_retailer_id(retailer_id)
                if detected_ms and detected_ms in PRODUCTS:
                    print(f"[CATALOG TEMPLATE] Phát hiện {detected_ms} từ catalog template")
                    
                    # Cập nhật context với MS mới
                    update_context_with_new_ms(uid, detected_ms, "catalog_template")
                    
                    # Gửi carousel
                    send_single_product_carousel(uid, detected_ms)
                    return detected_ms

        return None
    except Exception as e:
        print(f"[CATALOG TEMPLATE ERROR] {e}")
        return None

# ============================================
# GOOGLE SHEETS API FUNCTIONS
# ============================================

def get_google_sheets_service():
    if not GOOGLE_SHEETS_CREDENTIALS_JSON or not GOOGLE_SHEET_ID:
        return None

    try:
        import google.auth
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        
        creds_dict = json.loads(GOOGLE_SHEETS_CREDENTIALS_JSON)
        credentials = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        service = build('sheets', 'v4', credentials=credentials)
        print("✅ Đã khởi tạo Google Sheets service thành công.")
        return service
    except ImportError:
        print("⚠️ Google API libraries chưa được cài đặt.")
        return None
    except Exception as e:
        print(f"❌ Lỗi khi khởi tạo Google Sheets service: {e}")
        return None

def write_order_to_google_sheet_api(order_data: dict):
    """Ghi đơn hàng vào Google Sheets với thông tin giá chính xác"""
    service = get_google_sheets_service()
    if service is None:
        return False
    
    sheet_name = "Orders"
    
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        order_id = f"ORD{int(time.time())}_{order_data.get('uid', '')[-4:]}"
        
        # LẤY GIÁ CHÍNH XÁC TỪ ORDER_DATA
        unit_price = order_data.get("unit_price", 0)
        total_price = order_data.get("total_price", 0)
        quantity = order_data.get("quantity", 1)
        
        # Debug log
        print(f"[GOOGLE SHEET DEBUG] Đang ghi đơn hàng:")
        print(f"  - MS: {order_data.get('ms')}")
        print(f"  - Unit Price: {unit_price}")
        print(f"  - Quantity: {quantity}")
        print(f"  - Total Price: {total_price}")
        
        # Đảm bảo có giá trị số hợp lệ
        try:
            unit_price_float = float(unit_price)
            total_price_float = float(total_price)
            quantity_int = int(quantity)
        except (ValueError, TypeError):
            print(f"[GOOGLE SHEET WARNING] Giá trị số không hợp lệ: unit_price={unit_price}, total_price={total_price}, quantity={quantity}")
            # Fallback: thử lấy giá từ sản phẩm
            ms = order_data.get("ms", "")
            if ms and ms in PRODUCTS:
                product = PRODUCTS[ms]
                unit_price_float = extract_price_int(product.get("Gia", "")) or 0
                quantity_int = int(quantity) if quantity else 1
                total_price_float = unit_price_float * quantity_int
                print(f"[GOOGLE SHEET FALLBACK] Dùng giá fallback: {unit_price_float} x {quantity_int} = {total_price_float}")
            else:
                unit_price_float = 0
                total_price_float = 0
                quantity_int = 1
        
        # Chuẩn bị dòng dữ liệu (22 cột để phù hợp với Google Sheet)
        new_row = [
            timestamp,                          # 1. Thời gian
            order_id,                           # 2. Mã đơn hàng
            "Mới",                              # 3. Trạng thái
            order_data.get("ms", ""),           # 4. Mã sản phẩm
            order_data.get("product_name", ""), # 5. Tên sản phẩm
            order_data.get("color", ""),        # 6. Màu sắc
            order_data.get("size", ""),         # 7. Size
            quantity_int,                       # 8. Số lượng (ĐÃ SỬA)
            unit_price_float,                   # 9. Đơn giá (ĐÃ SỬA)
            total_price_float,                  # 10. Thành tiền (ĐÃ SỬA)
            order_data.get("customer_name", ""),# 11. Tên khách hàng
            order_data.get("phone", ""),        # 12. Số điện thoại
            order_data.get("address", ""),      # 13. Địa chỉ đầy đủ
            order_data.get("province", ""),     # 14. Tỉnh/Thành phố
            order_data.get("district", ""),     # 15. Quận/Huyện
            order_data.get("ward", ""),         # 16. Phường/Xã
            order_data.get("address_detail", ""), # 17. Địa chỉ chi tiết
            "COD",                              # 18. Phương thức thanh toán
            "ViettelPost",                      # 19. Đơn vị vận chuyển
            f"Đơn từ Facebook Bot ({order_data.get('referral_source', 'direct')})", # 20. Ghi chú
            order_data.get("uid", ""),          # 21. Facebook User ID
            order_data.get("referral_source", "direct") # 22. Nguồn đơn hàng
        ]
        
        # Debug dòng dữ liệu
        print(f"[GOOGLE SHEET ROW DATA] Số cột: {len(new_row)}")
        print(f"  Dữ liệu: {new_row}")
        
        # Ghi vào Google Sheets
        request = service.spreadsheets().values().append(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=f"{sheet_name}!A:V",  # 22 cột (A-V)
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [new_row]}
        )
        
        response = request.execute()
        print(f"✅ ĐÃ GHI ĐƠN HÀNG VÀO GOOGLE SHEET THÀNH CÔNG!")
        print(f"   - Mã đơn: {order_id}")
        print(f"   - Sản phẩm: {order_data.get('product_name', '')}")
        print(f"   - Số lượng: {quantity_int}")
        print(f"   - Đơn giá: {unit_price_float:,.0f} đ")
        print(f"   - Thành tiền: {total_price_float:,.0f} đ")
        
        return True
        
    except Exception as e:
        print(f"❌ Lỗi Google Sheets API: {e}")
        import traceback
        traceback.print_exc()
        return False

def save_order_to_local_csv(order_data: dict):
    try:
        file_path = "orders_backup.csv"
        file_exists = os.path.exists(file_path)
        
        timestamp = datetime.now().strftime("%Y-%m-d %H:%M:%S")
        order_id = f"ORD{int(time.time())}_{order_data.get('uid', '')[-4:]}"
        
        row_data = {
            "timestamp": timestamp,
            "order_id": order_id,
            "status": "Mới",
            "product_code": order_data.get("ms", ""),
            "product_name": order_data.get("product_name", ""),
            "color": order_data.get("color", ""),
            "size": order_data.get("size", ""),
            "quantity": order_data.get("quantity", 1),
            "unit_price": order_data.get("unit_price", 0),
            "total_price": order_data.get("total_price", 0),
            "customer_name": order_data.get("customer_name", ""),
            "phone": order_data.get("phone", ""),
            "address": order_data.get("address", ""),
            "province": order_data.get("province", ""),
            "district": order_data.get("district", ""),
            "ward": order_data.get("ward", ""),
            "address_detail": order_data.get("address_detail", ""),
            "payment_method": "COD",
            "shipping_method": "ViettelPost",
            "notes": f"Đơn từ Facebook Bot ({order_data.get('referral_source', 'direct')})",
            "fb_user_id": order_data.get("uid", ""),
            "referral_source": order_data.get("referral_source", "direct")
        }
        
        with open(file_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=row_data.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(row_data)
        
        print(f"📁 Đã lưu đơn hàng vào file local backup: {order_id}")
    except Exception as e:
        print(f"❌ Lỗi khi lưu file local backup: {str(e)}")

# ============================================
# POSCAKE WEBHOOK INTEGRATION (PHẦN MỚI)
# ============================================

def send_order_status_message(recipient_id: str, order_data: dict):
    """
    Gửi tin nhắn thông báo trạng thái đơn hàng từ Poscake
    """
    try:
        order_id = order_data.get("order_id", "")
        order_code = order_data.get("order_code", "")
        status = order_data.get("status", "")
        total_amount = order_data.get("total_amount", 0)
        items = order_data.get("items", [])
        
        # Tạo nội dung tin nhắn dựa trên trạng thái
        status_messages = {
            "pending": "📦 ĐƠN HÀNG MỚI",
            "processing": "⚡ ĐANG XỬ LÝ",
            "shipped": "🚚 ĐÃ GIAO HÀNG",
            "delivered": "✅ ĐÃ NHẬN HÀNG",
            "cancelled": "❌ ĐÃ HỦY"
        }
        
        status_text = status_messages.get(status, "📦 CẬP NHẬT ĐƠN HÀNG")
        
        # Xây dựng nội dung tin nhắn
        message = f"""🎊 {status_text}
────────────────
📋 Mã đơn hàng: {order_code}
💰 Tổng tiền: {total_amount:,.0f} đ
📅 Thời gian: {order_data.get('created_at', '')}
────────────────"""

        if items:
            message += "\n📦 Sản phẩm:\n"
            for i, item in enumerate(items[:5], 1):  # Giới hạn 5 sản phẩm
                product_name = item.get("product_name", "")
                quantity = item.get("quantity", 1)
                price = item.get("price", 0)
                message += f"{i}. {product_name} x{quantity} - {price:,.0f} đ\n"
        
        # Thêm thông báo theo trạng thái
        if status == "pending":
            message += "\n⏰ Shop sẽ liên hệ xác nhận trong 5-10 phút."
        elif status == "processing":
            message += "\n🔧 Đơn hàng đang được chuẩn bị."
        elif status == "shipped":
            shipping_info = order_data.get("shipping_info", {})
            tracking_code = shipping_info.get("tracking_code", "")
            carrier = shipping_info.get("carrier", "")
            if tracking_code:
                message += f"\n📮 Mã vận đơn: {tracking_code}"
            if carrier:
                message += f"\n🚚 Đơn vị vận chuyển: {carrier}"
        elif status == "delivered":
            message += "\n✅ Cảm ơn bạn đã mua hàng!"
        elif status == "cancelled":
            message += "\n📞 Liên hệ shop để được hỗ trợ."

        message += "\n────────────────\n💬 Cần hỗ trợ thêm? Gửi tin nhắn cho em ạ! ❤️"

        send_message(recipient_id, message)
        
        # Nếu có tracking code, gửi thêm nút theo dõi đơn hàng
        if status == "shipped":
            tracking_code = order_data.get("shipping_info", {}).get("tracking_code")
            if tracking_code:
                quick_replies = [
                    {
                        "content_type": "text",
                        "title": "📍 Theo dõi đơn hàng",
                        "payload": f"TRACK_ORDER_{tracking_code}"
                    },
                    {
                        "content_type": "text",
                        "title": "📞 Hỗ trợ",
                        "payload": "SUPPORT_ORDER"
                    }
                ]
                send_quick_replies(recipient_id, "Bấm để theo dõi đơn hàng:", quick_replies)
        
        print(f"[POSCAKE NOTIFY] Đã gửi thông báo đơn hàng {order_code} cho user {recipient_id}")
        return True
        
    except Exception as e:
        print(f"[POSCAKE NOTIFY ERROR] Lỗi gửi tin nhắn đơn hàng: {e}")
        return False

def handle_poscake_order_event(event_type: str, data: dict):
    """Xử lý sự kiện đơn hàng từ Poscake"""
    order_data = data.get('data', data.get('order', {}))
    
    print(f"[POSCAKE ORDER] {event_type}: {order_data.get('code', 'No code')}")
    
    # Log chi tiết để debug
    print(f"[POSCAKE ORDER DETAILS] {json.dumps(order_data, ensure_ascii=False)[:300]}")
    
    # Tìm recipient_id từ thông tin khách hàng
    customer = order_data.get('customer', {})
    phone = customer.get('phone', '')
    email = customer.get('email', '')
    
    recipient_id = None
    
    # Tìm user_id từ số điện thoại trong context
    for uid, ctx in USER_CONTEXT.items():
        # Kiểm tra order_data hoặc số điện thoại trong context
        user_phone = ctx.get("order_data", {}).get("phone", "")
        if user_phone and user_phone == phone:
            recipient_id = uid
            break
    
    # Nếu không tìm thấy, thử tìm bằng email
    if not recipient_id and email:
        for uid, ctx in USER_CONTEXT.items():
            user_email = ctx.get("order_data", {}).get("email", "")
            if user_email and user_email == email:
                recipient_id = uid
                break
    
    if recipient_id:
        # Chuẩn bị dữ liệu đơn hàng
        order_info = {
            "order_id": order_data.get('id', ''),
            "order_code": order_data.get('code', ''),
            "status": event_type.replace('order.', ''),
            "total_amount": order_data.get('total', 0),
            "items": order_data.get('items', []),
            "customer": customer,
            "created_at": order_data.get('created_at', ''),
            "updated_at": order_data.get('updated_at', ''),
            "shipping_info": order_data.get('shipping', {})
        }
        
        # Gửi tin nhắn thông báo
        send_order_status_message(recipient_id, order_info)
        
        # Lưu thông tin đơn hàng vào context
        if recipient_id in USER_CONTEXT:
            if "poscake_orders" not in USER_CONTEXT[recipient_id]:
                USER_CONTEXT[recipient_id]["poscake_orders"] = []
            
            # Kiểm tra xem đơn hàng đã tồn tại chưa
            existing_order = next(
                (o for o in USER_CONTEXT[recipient_id]["poscake_orders"] 
                 if o.get("order_id") == order_info["order_id"]), None
            )
            
            if not existing_order:
                USER_CONTEXT[recipient_id]["poscake_orders"].append(order_info)
                # Giữ tối đa 10 đơn hàng gần nhất
                if len(USER_CONTEXT[recipient_id]["poscake_orders"]) > 10:
                    USER_CONTEXT[recipient_id]["poscake_orders"] = USER_CONTEXT[recipient_id]["poscake_orders"][-10:]
            else:
                # Cập nhật trạng thái đơn hàng hiện có
                existing_order.update(order_info)
        
        return jsonify({
            "status": "success",
            "event": event_type,
            "order_code": order_data.get('code'),
            "message_sent": True,
            "recipient_id": recipient_id
        }), 200
    else:
        print(f"[POSCAKE ORDER] Không tìm thấy recipient_id cho đơn hàng {order_data.get('code')}")
        return jsonify({
            "status": "no_recipient",
            "event": event_type,
            "order_code": order_data.get('code'),
            "message": "Không tìm thấy user tương ứng"
        }), 200

# ============================================
# ADDRESS API FUNCTIONS
# ============================================

def get_provinces():
    """Lấy danh sách tỉnh/thành từ API với cache"""
    now = time.time()
    
    # Kiểm tra cache
    if (ADDRESS_CACHE['provinces'] and 
        (now - ADDRESS_CACHE['provinces_updated']) < ADDRESS_CACHE['cache_ttl']):
        return ADDRESS_CACHE['provinces']
    
    try:
        response = requests.get('https://provinces.open-api.vn/api/p/', timeout=5)
        if response.status_code == 200:
            provinces = response.json()
            # Chỉ lấy các trường cần thiết
            simplified = []
            for p in provinces:
                simplified.append({
                    'code': p.get('code'),
                    'name': p.get('name')
                })
            
            ADDRESS_CACHE['provinces'] = simplified
            ADDRESS_CACHE['provinces_updated'] = now
            return simplified
    except Exception as e:
        print(f"[ADDRESS API ERROR] Lỗi khi gọi API tỉnh/thành: {e}")
    
    return []

def get_districts(province_code):
    """Lấy danh sách quận/huyện từ API với cache"""
    if not province_code:
        return []
    
    # Kiểm tra cache
    if province_code in ADDRESS_CACHE['districts']:
        cached_data = ADDRESS_CACHE['districts'][province_code]
        if time.time() - cached_data['updated'] < ADDRESS_CACHE['cache_ttl']:
            return cached_data['data']
    
    try:
        response = requests.get(f'https://provinces.open-api.vn/api/p/{province_code}?depth=2', timeout=5)
        if response.status_code == 200:
            province_data = response.json()
            districts = province_data.get('districts', [])
            
            simplified = []
            for d in districts:
                simplified.append({
                    'code': d.get('code'),
                    'name': d.get('name')
                })
            
            # Lưu vào cache
            ADDRESS_CACHE['districts'][province_code] = {
                'data': simplified,
                'updated': time.time()
            }
            return simplified
    except Exception as e:
        print(f"[ADDRESS API ERROR] Lỗi khi gọi API quận/huyện: {e}")
    
    return []

def get_wards(district_code):
    """Lấy danh sách phường/xã từ API với cache"""
    if not district_code:
        return []
    
    # Kiểm tra cache
    if district_code in ADDRESS_CACHE['wards']:
        cached_data = ADDRESS_CACHE['wards'][district_code]
        if time.time() - cached_data['updated'] < ADDRESS_CACHE['cache_ttl']:
            return cached_data['data']
    
    try:
        response = requests.get(f'https://provinces.open-api.vn/api/d/{district_code}?depth=2', timeout=5)
        if response.status_code == 200:
            district_data = response.json()
            wards = district_data.get('wards', [])
            
            simplified = []
            for w in wards:
                simplified.append({
                    'code': w.get('code'),
                    'name': w.get('name')
                })
            
            # Lưu vào cache
            ADDRESS_CACHE['wards'][district_code] = {
                'data': simplified,
                'updated': time.time()
            }
            return simplified
    except Exception as e:
        print(f"[ADDRESS API ERROR] Lỗi khi gọi API phường/xã: {e}")
    
    return []

# ============================================
# ADDRESS API ENDPOINTS
# ============================================

@app.route("/api/address/provinces", methods=["GET"])
def api_get_provinces():
    """API lấy danh sách tỉnh/thành"""
    provinces = get_provinces()
    return jsonify(provinces)

@app.route("/api/address/districts/<province_code>", methods=["GET"])
def api_get_districts(province_code):
    """API lấy danh sách quận/huyện theo tỉnh"""
    districts = get_districts(province_code)
    return jsonify(districts)

@app.route("/api/address/wards/<district_code>", methods=["GET"])
def api_get_wards(district_code):
    """API lấy danh sách phường/xã theo quận/huyện"""
    wards = get_wards(district_code)
    return jsonify(wards)

# ============================================
# MESSENGER ORDER WEBVIEW
# ============================================

@app.route("/messenger-order", methods=["GET"])
def messenger_order():
    """Webview form đặt hàng cho Messenger với address dropdown 3 cấp"""
    ms = (request.args.get("ms") or "").upper()
    uid = request.args.get("uid") or ""
    
    # Kiểm tra user agent để tối ưu cho Messenger
    user_agent = request.headers.get('User-Agent', '')
    is_messenger = 'Messenger' in user_agent or 'FBAN' in user_agent
    
    # Preload products nhanh hơn
    load_products(force=False)
    
    if not ms:
        return """
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <title>Không tìm thấy sản phẩm</title>
            <style>
                body {
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    height: 100vh;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    margin: 0;
                    padding: 20px;
                }
                .container {
                    background: white;
                    border-radius: 15px;
                    padding: 40px;
                    text-align: center;
                    box-shadow: 0 10px 40px rgba(0,0,0,0.1);
                    max-width: 400px;
                }
                .error-icon {
                    font-size: 60px;
                    margin-bottom: 20px;
                }
                h2 {
                    color: #FF3B30;
                    margin-bottom: 15px;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="error-icon">⚠️</div>
                <h2>Không tìm thấy sản phẩm</h2>
                <p>Vui lòng quay lại Messenger và chọn sản phẩm để đặt hàng.</p>
            </div>
        </body>
        </html>
        """
    
    # Nếu không có sản phẩm, thử load lại
    if not PRODUCTS:
        load_products(force=True)
        
    if ms not in PRODUCTS:
        return """
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <title>Sản phẩm không tồn tại</title>
            <style>
                body {
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    height: 100vh;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    margin: 0;
                    padding: 20px;
                }
                .container {
                    background: white;
                    border-radius: 15px;
                    padding: 40px;
                    text-align: center;
                    box-shadow: 0 10px 40px rgba(0,0,0,0.1);
                    max-width: 400px;
                }
                .error-icon {
                    font-size: 60px;
                    margin-bottom: 20px;
                }
                h2 {
                    color: #FF3B30;
                    margin-bottom: 15px;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="error-icon">❌</div>
                <h2>Sản phẩm không tồn tại</h2>
                <p>Vui lòng quay lại Messenger và chọn sản phẩm khác giúp shop ạ.</p>
            </div>
        </body>
        </html>
        """
    
    current_fanpage_name = get_fanpage_name_from_api()
    row = PRODUCTS[ms]
    
    # Lấy thông tin sản phẩm với fallback nhanh
    images_field = row.get("Images", "")
    urls = parse_image_urls(images_field)
    default_image = urls[0] if urls else ""
    
    # Sử dụng base64 placeholder để tăng tốc độ load ban đầu
    placeholder_image = "data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTIwIiBoZWlnaHQ9IjEyMCIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48cmVjdCB3aWR0aD0iMTIwIiBoZWlnaHQ9IjEyMCIgZmlsbD0iI2Y1ZjVmNSIvPjx0ZXh0IHg9IjYwIiB5PSI2MCIgZm9udC1mYW1pbHk9IkFyaWFsIiBmb250LXNpemU9IjEyIiBmaWxsPSIjY2NjY2NjIiB0ZXh0LWFuY2hvcj0ibWlkZGxlIiBkeT0iLjNlbSI+TG9hZGluZy4uLjwvdGV4dD48L3N2Zz4="
    
    size_field = row.get("size (Thuộc tính)", "")
    color_field = row.get("màu (Thuộc tính)", "")
    
    sizes = ["Mặc định"]
    colors = ["Mặc định"]
    
    if size_field:
        sizes = [s.strip() for s in size_field.split(",") if s.strip()]
    
    if color_field:
        colors = [c.strip() for c in color_field.split(",") if c.strip()]
    
    price_str = row.get("Gia", "0")
    price_int = extract_price_int(price_str) or 0
    
    # Tên sản phẩm (xóa mã nếu có)
    product_name = row.get('Ten', '')
    if f"[{ms}]" in product_name or ms in product_name:
        product_name = product_name.replace(f"[{ms}]", "").replace(ms, "").strip()
    
    # GỬI SỰ KIỆN INITIATECHECKOUT THÔNG MINH (BẤT ĐỒNG BỘ)
    try:
        # Lấy client IP và user agent
        client_ip = request.headers.get('X-Forwarded-For', request.remote_addr)
        user_agent = request.headers.get('User-Agent', '')
        
        # Gửi sự kiện InitiateCheckout SMART (bất đồng bộ)
        send_initiate_checkout_smart(
            uid=uid,
            ms=ms,
            product_name=product_name,
            price=price_int
        )
        
        print(f"[FACEBOOK CAPI] Đã queue InitiateCheckout cho {uid} - {ms}")
    except Exception as e:
        print(f"[FACEBOOK CAPI ERROR] Lỗi queue InitiateCheckout: {e}")
        # KHÔNG ảnh hưởng đến việc hiển thị form
    
    # Tạo HTML với tối ưu hóa cực nhanh cho Messenger Webview
    html = f"""
    <!DOCTYPE html>
    <html lang="vi">
    <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
        <meta name="facebook-domain-verification" content="" />
        <title>Đặt hàng - {product_name[:30]}...</title>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/select2/4.0.13/css/select2.min.css">
        <style>
            /* Critical CSS - Load ngay */
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}
            
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
                background: #f5f7fa;
                min-height: 100vh;
                color: #333;
                padding: 0;
                overflow-x: hidden;
            }}
            
            .container {{
                max-width: 100%;
                margin: 0 auto;
                background: white;
                min-height: 100vh;
            }}
            
            .header {{
                background: linear-gradient(135deg, #1DB954 0%, #17a74d 100%);
                padding: 20px 15px;
                text-align: center;
                color: white;
                position: sticky;
                top: 0;
                z-index: 100;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }}
            
            .header h2 {{
                font-size: 18px;
                font-weight: 600;
                margin: 0;
            }}
            
            .content {{
                padding: 15px;
                padding-bottom: 30px;
            }}
            
            .product-section {{
                display: flex;
                gap: 12px;
                margin-bottom: 20px;
                padding: 15px;
                background: white;
                border-radius: 12px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.05);
            }}
            
            .product-image-container {{
                width: 100px;
                height: 100px;
                flex-shrink: 0;
                border-radius: 10px;
                overflow: hidden;
                background: #f8f9fa;
                position: relative;
            }}
            
            .product-image {{
                width: 100%;
                height: 100%;
                object-fit: cover;
                transition: opacity 0.3s ease;
            }}
            
            .product-info {{
                flex: 1;
                min-width: 0;
            }}
            
            .product-code {{
                font-size: 11px;
                color: #666;
                background: #f5f5f5;
                padding: 4px 8px;
                border-radius: 6px;
                display: inline-block;
                margin-bottom: 6px;
                font-family: 'Courier New', monospace;
                font-weight: 500;
            }}
            
            .product-title {{
                font-size: 15px;
                font-weight: 600;
                margin: 0 0 6px 0;
                line-height: 1.3;
                color: #222;
                word-break: break-word;
            }}
            
            .product-price {{
                color: #FF3B30;
                font-size: 16px;
                font-weight: 700;
                margin-top: 8px;
            }}
            
            .form-group {{
                margin-bottom: 15px;
            }}
            
            .form-group label {{
                display: block;
                margin-bottom: 6px;
                font-size: 13px;
                font-weight: 500;
                color: #444;
            }}
            
            .form-control {{
                width: 100%;
                padding: 12px 15px;
                border: 1.5px solid #e1e5e9;
                border-radius: 8px;
                font-size: 14px;
                background: white;
                font-family: inherit;
                transition: border-color 0.3s ease;
            }}
            
            .form-control:focus {{
                border-color: #1DB954;
                outline: none;
            }}
            
            .select2-container {{
                width: 100% !important;
            }}
            
            .select2-container--default .select2-selection--single {{
                border: 1.5px solid #e1e5e9;
                border-radius: 8px;
                height: 46px;
                padding: 10px;
            }}
            
            .select2-container--default .select2-selection--single .select2-selection__arrow {{
                height: 44px;
            }}
            
            .address-row {{
                display: flex;
                gap: 10px;
                margin-bottom: 10px;
            }}
            
            .total-section {{
                background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
                padding: 16px;
                border-radius: 12px;
                margin: 20px 0;
                text-align: center;
                border: 1px solid #dee2e6;
            }}
            
            .total-label {{
                font-size: 13px;
                color: #666;
                margin-bottom: 4px;
            }}
            
            .total-amount {{
                font-size: 22px;
                font-weight: 700;
                color: #FF3B30;
            }}
            
            .submit-btn {{
                width: 100%;
                padding: 16px;
                border: none;
                border-radius: 12px;
                background: linear-gradient(135deg, #1DB954 0%, #17a74d 100%);
                color: white;
                font-size: 16px;
                font-weight: 600;
                cursor: pointer;
                transition: all 0.3s ease;
                margin-top: 10px;
                font-family: inherit;
                box-shadow: 0 4px 15px rgba(29, 185, 84, 0.2);
            }}
            
            .submit-btn:disabled {{
                opacity: 0.7;
                cursor: not-allowed;
                box-shadow: none;
            }}
            
            .submit-btn:hover:not(:disabled) {{
                transform: translateY(-2px);
                box-shadow: 0 6px 20px rgba(29, 185, 84, 0.3);
            }}
            
            .loading-spinner {{
                display: inline-block;
                width: 18px;
                height: 18px;
                border: 2px solid rgba(255, 255, 255, 0.3);
                border-top: 2px solid white;
                border-radius: 50%;
                animation: spin 1s linear infinite;
            }}
            
            @keyframes spin {{
                0% {{ transform: rotate(0deg); }}
                100% {{ transform: rotate(360deg); }}
            }}
            
            .note {{
                margin-top: 12px;
                font-size: 11px;
                color: #888;
                text-align: center;
                line-height: 1.4;
            }}
            
            .success-message {{
                text-align: center;
                padding: 40px 20px;
                display: none;
            }}
            
            .success-icon {{
                font-size: 60px;
                color: #1DB954;
                margin-bottom: 20px;
            }}
            
            /* Messenger Webview specific */
            @media (max-width: 480px) {{
                .product-section {{
                    flex-direction: column;
                }}
                
                .product-image-container {{
                    width: 100%;
                    height: 180px;
                }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h2>ĐẶT HÀNG - {current_fanpage_name}</h2>
            </div>
            
            <div class="content" id="orderFormContainer">
                <!-- Product Info Section -->
                <div class="product-section">
                    <div class="product-image-container">
                        <img id="product-image" class="product-image" 
                             src="{placeholder_image}" 
                             data-src="{default_image}" 
                             alt="{product_name}"
                             onerror="this.src='data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTAwIiBoZWlnaHQ9IjEwMCIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48cmVjdCB3aWR0aD0iMTAwIiBoZWlnaHQ9IjEwMCIgZmlsbD0iI2UzZTNlMyIvPjx0ZXh0IHg9IjUwIiB5PSI1MCIgZm9udC1mYW1pbHk9IkFyaWFsIiBmb250LXNpemU9IjEyIiBmaWxsPSIjOTk5OTk5IiB0ZXh0LWFuY2hvcj0ibWlkZGxlIiBkeT0iLjNlbSI+Tm8gSW1hZ2U8L3RleHQ+PC9zdmc+'"
                             loading="lazy">
                    </div>
                    <div class="product-info">
                        <div class="product-code">Mã: {ms}</div>
                        <h3 class="product-title">{product_name}</h3>
                        <div class="product-price">
                            <span id="price-display">{price_int:,.0f} đ</span>
                        </div>
                    </div>
                </div>

                <!-- Order Form -->
                <form id="orderForm">
                    <!-- Color Selection -->
                    <div class="form-group">
                        <label for="color">Màu sắc:</label>
                        <select id="color" class="form-control">
                            {''.join(f"<option value='{c}'>{c}</option>" for c in colors)}
                        </select>
                    </div>

                    <!-- Size Selection -->
                    <div class="form-group">
                        <label for="size">Size:</label>
                        <select id="size" class="form-control">
                            {''.join(f"<option value='{s}'>{s}</option>" for s in sizes)}
                        </select>
                    </div>

                    <!-- Quantity -->
                    <div class="form-group">
                        <label for="quantity">Số lượng:</label>
                        <input type="number" id="quantity" class="form-control" value="1" min="1" max="10">
                    </div>

                    <!-- Total Price -->
                    <div class="total-section">
                        <div class="total-label">Tạm tính:</div>
                        <div class="total-amount" id="total-display">{price_int:,.0f} đ</div>
                    </div>

                    <!-- Customer Information -->
                    <div class="form-group">
                        <label for="customerName">Họ và tên:</label>
                        <input type="text" id="customerName" class="form-control" required>
                    </div>

                    <div class="form-group">
                        <label for="phone">Số điện thoại:</label>
                        <input type="tel" id="phone" class="form-control" required pattern="[0-9]{{10,11}}" placeholder="10-11 số">
                    </div>

                    <!-- Address Section với Select2 -->
                    <div class="form-group">
                        <label for="province">Tỉnh/Thành phố:</label>
                        <select id="province" class="form-control select2" required>
                            <option value="">Chọn tỉnh/thành phố</option>
                        </select>
                    </div>

                    <div class="form-group">
                        <label for="district">Quận/Huyện:</label>
                        <select id="district" class="form-control select2" required disabled>
                            <option value="">Chọn quận/huyện</option>
                        </select>
                    </div>

                    <div class="form-group">
                        <label for="ward">Phường/Xã:</label>
                        <select id="ward" class="form-control select2" required disabled>
                            <option value="">Chọn phường/xã</option>
                        </select>
                    </div>

                    <div class="form-group">
                        <label for="addressDetail">Địa chỉ chi tiết:</label>
                        <input type="text" id="addressDetail" class="form-control" placeholder="Số nhà, tên đường, thôn/xóm..." required>
                    </div>

                    <!-- Submit Button -->
                    <button type="button" id="submitBtn" class="submit-btn">
                        ĐẶT HÀNG NGAY
                    </button>

                    <p class="note">
                        Shop sẽ gọi xác nhận trong 5-10 phút. Thanh toán khi nhận hàng (COD).
                    </p>
                </form>
            </div>
            
            <!-- Success Message (hidden by default) -->
            <div class="content success-message" id="successMessage">
                <div class="success-icon">✅</div>
                <h3 style="color: #222; margin-bottom: 15px;">Cảm ơn bạn đã đặt hàng!</h3>
                <p style="color: #666; line-height: 1.6; margin-bottom: 25px;">
                    Shop sẽ gọi điện xác nhận đơn hàng trong 5-10 phút.<br>
                    Mã đơn hàng: <strong id="orderIdDisplay"></strong>
                </p>
                <p style="color: #888; font-size: 14px; margin-top: 30px;">
                    Bạn có thể đóng trang này hoặc quay lại Messenger để tiếp tục mua sắm.
                </p>
            </div>
        </div>

        <!-- Load Select2 from CDN -->
        <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery/3.6.0/jquery.min.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/select2/4.0.13/js/select2.min.js"></script>
        <script>
            // Khởi tạo Select2
            $(document).ready(function() {{
                $('.select2').select2({{
                    placeholder: 'Chọn...',
                    allowClear: false,
                    width: '100%'
                }});
                
                // Khởi tạo các biến và hàm
                const DOMAIN = '{'https://' + DOMAIN if not DOMAIN.startswith('http') else DOMAIN}';
                const API_BASE_URL = '/api';
                let BASE_PRICE = {price_int};
                let selectedProvinceCode = '';
                let selectedDistrictCode = '';
                
                // Format price function
                function formatPrice(n) {{
                    return new Intl.NumberFormat('vi-VN').format(n) + ' đ';
                }}
                
                // Update price display
                function updatePriceDisplay() {{
                    const quantity = parseInt(document.getElementById('quantity').value) || 1;
                    const total = BASE_PRICE * quantity;
                    document.getElementById('total-display').textContent = formatPrice(total);
                }}
                
                // Load product image after page loads
                function loadProductImage() {{
                    const img = document.getElementById('product-image');
                    if (img.dataset.src) {{
                        const tempImg = new Image();
                        tempImg.onload = function() {{
                            img.src = this.src;
                        }};
                        tempImg.onerror = function() {{
                            img.src = 'data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTAwIiBoZWlnaHQ9IjEwMCIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48cmVjdCB3aWR0aD0iMTAwIiBoZWlnaHQ9IjEwMCIgZmlsbD0iI2UzZTNlMyIvPjx0ZXh0IHg9IjUwIiB5PSI1MCIgZm9udC1mYW1pbHk9IkFyaWFsIiBmb250LXNpemU9IjEyIiBmaWxsPSIjOTk5OTk5IiB0ZXh0LWFuY2hvcj0ibWlkZGxlIiBkeT0iLjNlbSI+Tm8gSW1hZ2U8L3RleHQ+PC9zdmc+';
                        }};
                        tempImg.src = img.dataset.src;
                    }}
                }}
                
                // Get variant info (ảnh và giá)
                async function getVariantInfo(color, size) {{
                    try {{
                        const response = await fetch(`${{API_BASE_URL}}/get-variant-info?ms={ms}&color=${{encodeURIComponent(color)}}&size=${{encodeURIComponent(size)}}`);
                        if (response.ok) {{
                            return await response.json();
                        }}
                    }} catch (error) {{
                        console.log('Không thể lấy thông tin biến thể, sử dụng giá mặc định');
                    }}
                    return null;
                }}
                
                // Update variant info when color/size changes
                async function updateVariantInfo() {{
                    const color = document.getElementById('color').value;
                    const size = document.getElementById('size').value;
                    
                    const variantInfo = await getVariantInfo(color, size);
                    if (variantInfo) {{
                        // Update image
                        const img = document.getElementById('product-image');
                        if (variantInfo.image) {{
                            const tempImg = new Image();
                            tempImg.onload = function() {{
                                img.src = variantInfo.image;
                            }};
                            tempImg.src = variantInfo.image;
                        }}
                        
                        // Update price
                        if (variantInfo.price && variantInfo.price > 0) {{
                            BASE_PRICE = variantInfo.price;
                            document.getElementById('price-display').textContent = formatPrice(BASE_PRICE);
                            updatePriceDisplay();
                        }}
                    }}
                }}
                
                // Load provinces
                async function loadProvinces() {{
                    try {{
                        const response = await fetch(`${{API_BASE_URL}}/address/provinces`);
                        if (response.ok) {{
                            const provinces = await response.json();
                            const provinceSelect = $('#province');
                            
                            provinces.forEach(province => {{
                                provinceSelect.append(new Option(province.name, province.code));
                            }});
                            
                            // Enable province selection
                            provinceSelect.prop('disabled', false);
                            provinceSelect.trigger('change.select2');
                        }}
                    }} catch (error) {{
                        console.error('Lỗi khi tải tỉnh/thành:', error);
                    }}
                }}
                
                // Load districts
                async function loadDistricts(provinceCode) {{
                    if (!provinceCode) return;
                    
                    try {{
                        const response = await fetch(`${{API_BASE_URL}}/address/districts/${{provinceCode}}`);
                        if (response.ok) {{
                            const districts = await response.json();
                            const districtSelect = $('#district');
                            
                            // Clear old options
                            districtSelect.empty();
                            districtSelect.append(new Option('Chọn quận/huyện', ''));
                            
                            districts.forEach(district => {{
                                districtSelect.append(new Option(district.name, district.code));
                            }});
                            
                            // Enable district selection
                            districtSelect.prop('disabled', false);
                            districtSelect.trigger('change.select2');
                            
                            // Clear wards
                            $('#ward').empty().append(new Option('Chọn phường/xã', '')).prop('disabled', true).trigger('change.select2');
                        }}
                    }} catch (error) {{
                        console.error('Lỗi khi tải quận/huyện:', error);
                    }}
                }}
                
                // Load wards
                async function loadWards(districtCode) {{
                    if (!districtCode) return;
                    
                    try {{
                        const response = await fetch(`${{API_BASE_URL}}/address/wards/${{districtCode}}`);
                        if (response.ok) {{
                            const wards = await response.json();
                            const wardSelect = $('#ward');
                            
                            // Clear old options
                            wardSelect.empty();
                            wardSelect.append(new Option('Chọn phường/xã', ''));
                            
 wards.forEach(ward => {{
                                wardSelect.append(new Option(ward.name, ward.code));
                            }});
                            
                            // Enable ward selection
                            wardSelect.prop('disabled', false);
                            wardSelect.trigger('change.select2');
                        }}
                    }} catch (error) {{
                        console.error('Lỗi khi tải phường/xã:', error);
                    }}
                }}
                
                // Submit order
                async function submitOrder() {{
                    const formData = {{
                        ms: '{ms}',
                        uid: '{uid}',
                        color: document.getElementById('color').value,
                        size: document.getElementById('size').value,
                        quantity: parseInt(document.getElementById('quantity').value) || 1,
                        customerName: document.getElementById('customerName').value.trim(),
                        phone: document.getElementById('phone').value.trim(),
                        province: $('#province option:selected').text(),
                        district: $('#district option:selected').text(),
                        ward: $('#ward option:selected').text(),
                        addressDetail: document.getElementById('addressDetail').value.trim()
                    }};
                    
                    // Validate required fields
                    if (!formData.customerName) {{
                        alert('Vui lòng nhập họ và tên');
                        return;
                    }}
                    if (!formData.phone || !/^[0-9]{{10,11}}$/.test(formData.phone)) {{
                        alert('Vui lòng nhập số điện thoại hợp lệ (10-11 số)');
                        return;
                    }}
                    if (!formData.province || formData.province === 'Chọn tỉnh/thành phố') {{
                        alert('Vui lòng chọn tỉnh/thành phố');
                        return;
                    }}
                    if (!formData.district || formData.district === 'Chọn quận/huyện') {{
                        alert('Vui lòng chọn quận/huyện');
                        return;
                    }}
                    if (!formData.ward || formData.ward === 'Chọn phường/xã') {{
                        alert('Vui lòng chọn phường/xã');
                        return;
                    }}
                    if (!formData.addressDetail) {{
                        alert('Vui lòng nhập địa chỉ chi tiết');
                        return;
                    }}
                    
                    // Disable button and show loading
                    const submitBtn = document.getElementById('submitBtn');
                    const originalText = submitBtn.textContent;
                    submitBtn.disabled = true;
                    submitBtn.innerHTML = '<div class="loading-spinner"></div> Đang xử lý...';
                    
                    try {{
                        const response = await fetch('/api/submit-order', {{
                            method: 'POST',
                            headers: {{
                                'Content-Type': 'application/json'
                            }},
                            body: JSON.stringify(formData)
                        }});
                        
                        const result = await response.json();
                        
                        if (response.ok) {{
                            // Success - show success message
                            document.getElementById('orderFormContainer').style.display = 'none';
                            document.getElementById('successMessage').style.display = 'block';
                            document.getElementById('orderIdDisplay').textContent = result.order_id;
                            
                            // Close webview after 5 seconds if in Messenger
                            setTimeout(() => {{
                                if (window.MessengerExtensions) {{
                                    MessengerExtensions.requestCloseBrowser();
                                }}
                            }}, 5000);
                        }} else {{
                            // Error
                            alert('Có lỗi xảy ra: ' + (result.message || 'Vui lòng thử lại sau'));
                            submitBtn.disabled = false;
                            submitBtn.textContent = originalText;
                        }}
                    }} catch (error) {{
                        console.error('Submit error:', error);
                        alert('Có lỗi kết nối, vui lòng thử lại sau');
                        submitBtn.disabled = false;
                        submitBtn.textContent = originalText;
                    }}
                }}
                
                // Initialize
                loadProductImage();
                updatePriceDisplay();
                loadProvinces();
                
                // Event listeners
                document.getElementById('quantity').addEventListener('input', updatePriceDisplay);
                document.getElementById('color').addEventListener('change', updateVariantInfo);
                document.getElementById('size').addEventListener('change', updateVariantInfo);
                document.getElementById('submitBtn').addEventListener('click', submitOrder);
                
                // Select2 change events
                $('#province').on('change', function() {{
                    const provinceCode = $(this).val();
                    if (provinceCode) {{
                        selectedProvinceCode = provinceCode;
                        loadDistricts(provinceCode);
                    }} else {{
                        $('#district').empty().append(new Option('Chọn quận/huyện', '')).prop('disabled', true).trigger('change.select2');
                        $('#ward').empty().append(new Option('Chọn phường/xã', '')).prop('disabled', true).trigger('change.select2');
                    }}
                }});
                
                $('#district').on('change', function() {{
                    const districtCode = $(this).val();
                    if (districtCode) {{
                        selectedDistrictCode = districtCode;
                        loadWards(districtCode);
                    }} else {{
                        $('#ward').empty().append(new Option('Chọn phường/xã', '')).prop('disabled', true).trigger('change.select2');
                    }}
                }});
                
                // Initial variant info update
                updateVariantInfo();
                
                // Messenger Extensions SDK
                if (window.MessengerExtensions) {{
                    MessengerExtensions.getSupportedFeatures(function success(result) {{
                        console.log('Messenger Extensions supported:', result);
                    }}, function error(err) {{
                        console.log('Messenger Extensions error:', err);
                    }});
                }}
            }});
        </script>
    </body>
    </html>
    """
    
    return html

# ============================================
# API XỬ LÝ ĐẶT HÀNG
# ============================================

@app.route("/api/submit-order", methods=["POST"])
def api_submit_order():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "No data provided"}), 400
        
        ms = (data.get("ms") or "").upper()
        uid = data.get("uid", "")
        
        load_products()
        
        if ms not in PRODUCTS:
            return jsonify({"status": "error", "message": "Sản phẩm không tồn tại"}), 404
        
        # Lấy thông tin sản phẩm
        product = PRODUCTS[ms]
        product_name = product.get('Ten', '')
        if f"[{ms}]" in product_name or ms in product_name:
            product_name = product_name.replace(f"[{ms}]", "").replace(ms, "").strip()
        
        # Tạo địa chỉ đầy đủ
        full_address = f"{data.get('addressDetail', '')}, {data.get('ward', '')}, {data.get('district', '')}, {data.get('province', '')}"
        
        # ============================================
        # SỬA LỖI: LẤY ĐÚNG GIÁ THEO PHÂN LOẠI HÀNG
        # ============================================
        
        # Tìm giá chính xác theo màu và size
        unit_price = 0
        found_exact_variant = False
        
        color = data.get("color", "Mặc định")
        size = data.get("size", "Mặc định")
        
        # TRƯỚC HẾT: Tìm biến thể CHÍNH XÁC theo màu và size
        for variant in product.get("variants", []):
            variant_color = variant.get("mau", "").strip()
            variant_size = variant.get("size", "").strip()
            
            # So sánh chính xác
            color_match = False
            size_match = False
            
            # So sánh màu
            if color == "Mặc định":
                color_match = (variant_color == "" or variant_color is None or variant_color == "Mặc định")
            else:
                color_match = (variant_color.lower() == color.lower())
            
            # So sánh size
            if size == "Mặc định":
                size_match = (variant_size == "" or variant_size is None or variant_size == "Mặc định")
            else:
                size_match = (variant_size.lower() == size.lower())
            
            if color_match and size_match:
                unit_price = variant.get("gia", 0)
                found_exact_variant = True
                print(f"[PRICE MATCH] Tìm thấy biến thể chính xác: màu='{variant_color}', size='{variant_size}', giá={unit_price}")
                break
        
        # NẾU KHÔNG TÌM THẤY BIẾN THỂ CHÍNH XÁC
        if not found_exact_variant:
            print(f"[PRICE WARNING] Không tìm thấy biến thể chính xác cho màu='{color}', size='{size}'")
            
            # THỬ 1: Tìm biến thể chỉ khớp màu (bỏ qua size)
            for variant in product.get("variants", []):
                variant_color = variant.get("mau", "").strip()
                
                if color == "Mặc định":
                    color_match = (variant_color == "" or variant_color is None or variant_color == "Mặc định")
                else:
                    color_match = (variant_color.lower() == color.lower())
                
                if color_match:
                    unit_price = variant.get("gia", 0)
                    print(f"[PRICE FALLBACK 1] Dùng giá theo màu: {color} -> {unit_price}")
                    found_exact_variant = True
                    break
            
            # THỬ 2: Tìm biến thể chỉ khớp size (bỏ qua màu)
            if not found_exact_variant:
                for variant in product.get("variants", []):
                    variant_size = variant.get("size", "").strip()
                    
                    if size == "Mặc định":
                        size_match = (variant_size == "" or variant_size is None or variant_size == "Mặc định")
                    else:
                        size_match = (variant_size.lower() == size.lower())
                    
                    if size_match:
                        unit_price = variant.get("gia", 0)
                        print(f"[PRICE FALLBACK 2] Dùng giá theo size: {size} -> {unit_price}")
                        found_exact_variant = True
                        break
            
            # THỬ 3: Lấy giá đầu tiên từ danh sách biến thể
            if not found_exact_variant and product.get("variants"):
                unit_price = product["variants"][0].get("gia", 0)
                print(f"[PRICE FALLBACK 3] Dùng giá biến thể đầu tiên: {unit_price}")
                found_exact_variant = True
        
        # CUỐI CÙNG: Nếu vẫn không có giá, dùng giá chung của sản phẩm
        if unit_price == 0:
            unit_price = extract_price_int(product.get("Gia", "")) or 0
            print(f"[PRICE FALLBACK 4] Dùng giá chung sản phẩm: {unit_price}")
        
        # Tính tổng tiền CHÍNH XÁC
        quantity = int(data.get("quantity", 1))
        total_price = unit_price * quantity
        
        print(f"[PRICE FINAL] Giá đơn vị: {unit_price}, Số lượng: {quantity}, Tổng: {total_price}")
        
        # Chuẩn bị dữ liệu đơn hàng
        order_data = {
            "uid": uid,
            "ms": ms,
            "product_name": product_name,
            "color": data.get("color", "Mặc định"),
            "size": data.get("size", "Mặc định"),
            "quantity": quantity,
            "customer_name": data.get("customerName", ""),
            "phone": data.get("phone", ""),
            "province": data.get("province", ""),
            "district": data.get("district", ""),
            "ward": data.get("ward", ""),
            "address_detail": data.get("addressDetail", ""),
            "address": full_address,
            "unit_price": unit_price,
            "total_price": total_price,
            "referral_source": USER_CONTEXT.get(uid, {}).get("referral_source", "direct")
        }
        
        # Cập nhật context với MS mới từ đơn hàng
        if uid:
            update_context_with_new_ms(uid, ms, "order_form")
            
            # Lưu thông tin khách hàng vào context
            if uid in USER_CONTEXT:
                USER_CONTEXT[uid]["order_data"] = {
                    "phone": data.get("phone", ""),
                    "customer_name": data.get("customerName", ""),
                    "address": full_address,
                    "last_order_time": time.time()
                }
        
        # Tạo order ID
        order_id = f"ORD{int(time.time())}_{uid[-4:] if uid else '0000'}"
        
        # ============================================
        # GỬI TIN NHẮN CẢM ƠN SAU KHI ĐẶT HÀNG THÀNH CÔNG
        # ============================================
        
        if uid:
            try:
                # Xây dựng tin nhắn chi tiết với giá ĐÃ ĐƯỢC SỬA
                full_address = f"{order_data['address_detail']}, {order_data['ward']}, {order_data['district']}, {order_data['province']}"
                
                thank_you_message = f"""🎉 **CẢM ƠN ANH/CHỊ ĐÃ ĐẶT HÀNG!** 🎉

📋 **THÔNG TIN ĐƠN HÀNG**
─────────────────────
🆔 Mã đơn: {order_id}
📦 Sản phẩm: {product_name}
📌 Mã SP: {ms}
🎨 Màu: {order_data['color']}
📏 Size: {order_data['size']}
🔢 Số lượng: {quantity}
💰 Đơn giá: {unit_price:,.0f} đ
💰 Tổng tiền: **{total_price:,.0f} đ**

👤 **THÔNG TIN GIAO HÀNG**
─────────────────────
📛 Người nhận: {order_data['customer_name']}
📱 SĐT: {order_data['phone']}
📍 Địa chỉ: {full_address}

⏰ **THÔNG BÁO**
─────────────────────
Shop sẽ gọi điện xác nhận đơn hàng trong 5-10 phút.
📞 Vui lòng giữ máy để nhận cuộc gọi từ shop!

💬 **HỖ TRỢ**
─────────────────────
Nếu cần thay đổi thông tin đơn hàng hoặc hỗ trợ thêm, vui lòng nhắn tin cho em ạ! ❤️

Cảm ơn anh/chị đã tin tưởng {get_fanpage_name_from_api()}!"""
                
                # Gửi tin nhắn chính
                send_message(uid, thank_you_message)
                
                # Gửi thêm quick replies để tiện tương tác
                time.sleep(0.5)  # Delay nhẹ để tin nhắn không bị dồn
                
                quick_replies = [
                    {
                        "content_type": "text",
                        "title": "📞 Gọi lại cho tôi",
                        "payload": f"CALL_BACK_{order_id}"
                    },
                    {
                        "content_type": "text",
                        "title": "📍 Theo dõi đơn hàng",
                        "payload": f"TRACK_ORDER_{order_id}"
                    },
                    {
                        "content_type": "text", 
                        "title": "🛒 Mua thêm",
                        "payload": "BUY_MORE"
                    }
                ]
                
                send_quick_replies(uid, "Anh/chị có thể bấm các nút bên dưới để:", quick_replies)
                
                # Gửi sự kiện Facebook CAPI Purchase với giá CHÍNH XÁC
                try:
                    send_purchase_smart(
                        uid=uid,
                        ms=ms,
                        product_name=product_name,
                        order_data={
                            "phone": data.get("phone", ""),
                            "total_price": total_price,
                            "quantity": quantity,
                            "order_id": order_id
                        }
                    )
                    print(f"[FACEBOOK CAPI] Đã gửi Purchase event cho đơn hàng {order_id}, giá {total_price}, số lượng {quantity}")
                except Exception as capi_error:
                    print(f"[FACEBOOK CAPI ERROR] Lỗi gửi Purchase event: {capi_error}")
                
                print(f"[ORDER THANK YOU] Đã gửi tin nhắn cảm ơn cho user {uid}, đơn hàng {order_id}, tổng {total_price:,.0f} đ, số lượng {quantity}")
                
            except Exception as msg_error:
                print(f"[ORDER THANK YOU ERROR] Lỗi khi gửi tin nhắn cảm ơn: {msg_error}")
                # KHÔNG ảnh hưởng đến response của API, vẫn trả về thành công
                # Chỉ ghi log lỗi và tiếp tục

        # Lưu vào Google Sheets (nếu có) - SỬA: GỌI SAU KHI ĐÃ CÓ THÔNG TIN GIÁ CHÍNH XÁC
        sheet_success = False
        if GOOGLE_SHEET_ID and GOOGLE_SHEETS_CREDENTIALS_JSON:
            sheet_success = write_order_to_google_sheet_api(order_data)
        
        # Lưu vào file local backup
        save_order_to_local_csv(order_data)
        
        return jsonify({
            "status": "success",
            "message": "Đã nhận đơn hàng thành công!",
            "order_id": order_id,
            "product_name": product_name,
            "unit_price": unit_price,
            "quantity": quantity,
            "total_price": total_price,
            "sheet_saved": sheet_success
        })
        
    except Exception as e:
        print(f"[SUBMIT ORDER ERROR] {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": f"Lỗi xử lý đơn hàng: {str(e)}"}), 500

# ============================================
# API MỚI: Lấy thông tin biến thể (ảnh, giá)
# ============================================

@app.route("/api/get-variant-info")
def api_get_variant_info():
    ms = (request.args.get("ms") or "").upper()
    color = request.args.get("color", "").strip()
    size = request.args.get("size", "").strip()
    
    load_products()
    if ms not in PRODUCTS:
        return {"error": "not_found"}, 404
    
    product = PRODUCTS[ms]
    
    # Tìm biến thể phù hợp
    target_variant = None
    for variant in product.get("variants", []):
        variant_color = variant.get("mau", "").strip().lower()
        variant_size = variant.get("size", "").strip().lower()
        
        input_color = color.strip().lower()
        input_size = size.strip().lower()
        
        color_match = (not input_color) or (variant_color == input_color) or (input_color == "mặc định" and not variant_color)
        size_match = (not input_size) or (variant_size == input_size) or (input_size == "mặc định" and not variant_size)
        
        if color_match and size_match:
            target_variant = variant
            break
    
    # Nếu không tìm thấy biến thể phù hợp, dùng thông tin chung
    if target_variant:
        variant_image = target_variant.get("variant_image", "")
        variant_price = target_variant.get("gia", 0)
        variant_price_raw = target_variant.get("gia_raw", "")
    else:
        variant_image = ""
        variant_price = extract_price_int(product.get("Gia", "")) or 0
        variant_price_raw = product.get("Gia", "")
    
    # Nếu không có ảnh biến thể, lấy ảnh đầu tiên của sản phẩm
    if not variant_image:
        images_field = product.get("Images", "")
        urls = parse_image_urls(images_field)
        variant_image = urls[0] if urls else ""
    
    return {
        "ms": ms,
        "color": color,
        "size": size,
        "image": variant_image,
        "price": variant_price,
        "price_raw": variant_price_raw,
        "found_variant": target_variant is not None
    }

# ============================================
# API MỚI: Xóa context của user
# ============================================

@app.route("/api/clear-user-context/<user_id>", methods=["POST"])
def clear_user_context(user_id):
    """Xóa context của user khỏi cả memory và Google Sheets"""
    try:
        # Xóa khỏi memory
        if user_id in USER_CONTEXT:
            del USER_CONTEXT[user_id]
        
        # Xóa khỏi Google Sheets
        delete_user_context_from_sheets(user_id)
        
        return jsonify({
            "status": "success",
            "message": f"Đã xóa context của user {user_id}",
            "user_id": user_id
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Lỗi khi xóa context: {str(e)}"
        }), 500

# ============================================
# WEBHOOK HANDLER (ĐÃ SỬA ĐỂ XÓA LOGIC FCHAT ECHO)
# ============================================

@app.route("/", methods=["GET"])
def home():
    return "OK", 200

# ============================================
# WEBHOOK HANDLER - ĐÃ CẬP NHẬT ĐỂ XỬ LÝ REFERRAL
# ============================================

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    """Webhook chính nhận sự kiện từ Facebook - TRẢ VỀ NGAY LẬP TỨC"""
    
    # Xử lý GET request (verify webhook)
    if request.method == "GET":
        mode = request.args.get("hub.mode")
        token = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")
        
        print(f"[WEBHOOK GET] Mode: {mode}, Token: {token}")
        
        if mode == "subscribe" and token == VERIFY_TOKEN:
            print(f"[WEBHOOK VERIFIED] Đã xác minh webhook!")
            return challenge, 200
        else:
            print(f"[WEBHOOK VERIFY FAILED] Token không khớp")
            return "Verification token mismatch", 403
    
    # Xử lý POST request (nhận sự kiện)
    elif request.method == "POST":
        # LẤY DỮ LIỆU TRƯỚC KHI TRẢ VỀ
        try:
            data = request.get_json()
        except Exception as e:
            print(f"[WEBHOOK JSON ERROR] {e}")
            return "Invalid JSON", 400
        
        # Lấy client IP và User-Agent
        client_ip = request.headers.get('X-Forwarded-For', request.remote_addr)
        user_agent = request.headers.get('User-Agent', '')
        
        # LOG NGẮN GỌN để debug
        print(f"[WEBHOOK POST] Nhận event từ {client_ip}, User-Agent: {user_agent[:50]}...")
        
        if not data:
            print(f"[WEBHOOK EMPTY] Không có dữ liệu")
            return "EVENT_RECEIVED", 200
        
        # TRẢ VỀ NGAY LẬP TỨC để Facebook không retry
        print(f"[WEBHOOK QUEUING] Đang đưa sự kiện vào queue xử lý bất đồng bộ...")
        
        # Thêm vào queue để xử lý bất đồng bộ
        queued = queue_message_for_processing(data, client_ip, user_agent)
        
        if queued:
            print(f"[WEBHOOK QUEUED] Đã thêm sự kiện vào queue, trả về ngay lập tức")
        else:
            print(f"[WEBHOOK QUEUE FULL] Queue đầy, bỏ qua sự kiện này")
        
        # LUÔN LUÔN trả về 200 OK ngay lập tức
        return "EVENT_RECEIVED", 200

# ============================================
# KOYEB KEEP-ALIVE ENDPOINTS
# ============================================

@app.route('/ping', methods=['GET'])
def ping():
    """Endpoint cho keep-alive và health check"""
    status = {
        "status": "alive",
        "timestamp": datetime.now().isoformat(),
        "workers": {
            "message_worker": MESSAGE_WORKER_RUNNING,
            "facebook_worker": FACEBOOK_WORKER_RUNNING,
            "workers_initialized": WORKERS_INITIALIZED
        },
        "resources": {
            "products_loaded": len(PRODUCTS) if PRODUCTS else 0,
            "users_in_memory": len(USER_CONTEXT),
            "queue_size": MESSAGE_QUEUE.qsize(),
            "facebook_queue_size": FACEBOOK_EVENT_QUEUE.qsize()
        },
        "app": {
            "domain": DOMAIN,
            "fanpage": get_fanpage_name_from_api(),
            "version": "1.0"
        }
    }
    return jsonify(status)

@app.route('/health', methods=['GET'])
def health():
    """Health check đơn giản cho Koyeb"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/warmup', methods=['GET'])
def warmup():
    """Làm nóng app (pre-load)"""
    warm_up_app()
    return jsonify({
        "status": "warming_up",
        "message": "App đang được làm nóng...",
        "products_loaded": len(PRODUCTS) if PRODUCTS else 0,
        "workers": WORKERS_INITIALIZED
    })

@app.route('/stats', methods=['GET'])
def stats():
    """Xem thống kê app"""
    stats_data = {
        "products": {
            "total": len(PRODUCTS),
            "loaded_at": time.ctime(LAST_LOAD) if LAST_LOAD > 0 else "Never"
        },
        "users": {
            "in_memory": len(USER_CONTEXT),
            "active_last_24h": sum(1 for ctx in USER_CONTEXT.values() 
                                 if time.time() - ctx.get("last_updated", 0) < 86400)
        },
        "queues": {
            "message_queue": MESSAGE_QUEUE.qsize(),
            "facebook_queue": FACEBOOK_EVENT_QUEUE.qsize()
        },
        "workers": {
            "message_worker": MESSAGE_WORKER_RUNNING,
            "facebook_worker": FACEBOOK_WORKER_RUNNING,
            "initialized": WORKERS_INITIALIZED
        },
        "environment": {
            "domain": DOMAIN,
            "koyeb_keep_alive": KOYEB_KEEP_ALIVE_ENABLED,
            "app_url": APP_URL
        }
    }
    return jsonify(stats_data)
        
# ============================================
# START CLEANUP THREAD
# ============================================

def start_cleanup_thread():
    """Khởi động thread dọn dẹp định kỳ"""
    def cleanup_worker():
        print(f"[CLEANUP THREAD] Thread dọn dẹp đã khởi động")
        while True:
            try:
                cleanup_inactive_users()
            except Exception as e:
                print(f"[CLEANUP ERROR] Lỗi khi dọn dẹp: {e}")
            time.sleep(1800)  # 30 phút
    
    thread = threading.Thread(target=cleanup_worker, daemon=True)
    thread.start()
    return thread

# Khởi động cleanup thread
start_cleanup_thread()

# ============================================
# KHỞI ĐỘNG WORKERS KHI APP START
# ============================================

# Biến flag để đảm bảo chỉ khởi động workers một lần
WORKERS_INITIALIZED = False

def initialize_workers_once():
    """Khởi động các worker chỉ một lần duy nhất"""
    global WORKERS_INITIALIZED
    
    if WORKERS_INITIALIZED:
        return
    
    print(f"[INIT] Đang khởi động các background workers...")
    
    # Khởi động worker xử lý tin nhắn
    message_worker = start_message_worker()
    
    # Khởi động worker Facebook CAPI
    facebook_worker = start_facebook_worker()
    
    # Khởi động worker lưu context định kỳ
    context_save_thread = threading.Thread(target=periodic_context_save, daemon=True)
    context_save_thread.start()
    
    # Khởi tạo Google Sheets nếu cần
    if GOOGLE_SHEET_ID and GOOGLE_SHEETS_CREDENTIALS_JSON:
        try:
            init_user_context_sheet()
            print(f"[INIT] Đã khởi tạo Google Sheets")
        except Exception as e:
            print(f"[INIT ERROR] Lỗi khởi tạo sheet: {e}")
    
    WORKERS_INITIALIZED = True
    print(f"[INIT] Tất cả workers đã được khởi động")

# Khởi động workers ngay khi app start
initialize_workers_once()
    
# ============================================
# STARTUP OPTIMIZATION FOR KOYEB
# ============================================

# Khởi động keep-alive scheduler khi app start
if KOYEB_KEEP_ALIVE_ENABLED:
    print(f"[STARTUP] Bật keep-alive cho Koyeb Free Tier")
    print(f"[STARTUP] App URL: {APP_URL}")
    print(f"[STARTUP] Ping interval: {KOYEB_KEEP_ALIVE_INTERVAL} phút")
    
    # Khởi động scheduler trong thread riêng
    threading.Thread(target=start_keep_alive_scheduler, daemon=True).start()

# Tự động warm-up khi start (trong production)
if KOYEB_AUTO_WARMUP:
    print(f"[STARTUP] Tự động warm-up app...")
    threading.Thread(target=warm_up_app, daemon=True).start()

# ============================================
# RUN FLASK APP
# ============================================
if __name__ == '__main__':
    # Tắt debug mode để tối ưu performance
    app.run(
        host='0.0.0.0',
        port=int(os.environ.get('PORT', 5000)),
        debug=False,
        threaded=True  # Bật multi-threading
    )
