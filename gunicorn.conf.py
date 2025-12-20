# gunicorn.conf.py
import os

# Số worker - Trên Koyeb Free chỉ có 1 CPU, nên dùng 2-3 workers
workers = int(os.environ.get("GUNICORN_WORKERS", 2))  # Mặc định 2 workers
worker_class = "sync"

# Timeout settings
timeout = 60  # Tăng timeout lên 60 giây
keepalive = 2

# Logging
accesslog = "-"
errorlog = "-"
loglevel = "info"

# Worker settings
worker_connections = 1000
max_requests = 1000
max_requests_jitter = 50
