# gunicorn.conf.py
import multiprocessing

# Số worker
workers = multiprocessing.cpu_count() * 2 + 1
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
