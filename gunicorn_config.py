import multiprocessing

# Số worker processes
workers = 2
threads = 4

# Timeout settings - QUAN TRỌNG
timeout = 30  # Tăng từ 30 lên 60 giây để tránh timeout
graceful_timeout = 30
keepalive = 5

# Worker class
worker_class = 'sync'  # Hoặc 'gevent' nếu cần xử lý nhiều request đồng thời

# Logging
accesslog = '-'
errorlog = '-'
loglevel = 'info'

# Server socket
bind = '0.0.0.0:5000'

# Process naming
proc_name = 'fb_chatbot'
