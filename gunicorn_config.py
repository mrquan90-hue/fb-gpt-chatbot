import multiprocessing
import os

# Số worker processes
workers = int(os.environ.get('GUNICORN_WORKERS', '2'))
threads = int(os.environ.get('GUNICORN_THREADS', '4'))

# Timeout settings - QUAN TRỌNG: Tăng timeout để tránh lỗi
timeout = int(os.environ.get('GUNICORN_TIMEOUT', '60'))
graceful_timeout = int(os.environ.get('GUNICORN_GRACEFUL_TIMEOUT', '30'))
keepalive = int(os.environ.get('GUNICORN_KEEPALIVE', '5'))

# Worker class
worker_class = 'sync'

# Logging
accesslog = '-'
errorlog = '-'
loglevel = 'info'

# Server socket
bind = '0.0.0.0:' + os.environ.get('PORT', '8000')

# Process naming
proc_name = 'fb_chatbot'

# Preload app để tăng tốc độ khởi động
preload_app = True

# Max requests để tránh memory leak
max_requests = 1000
max_requests_jitter = 50
