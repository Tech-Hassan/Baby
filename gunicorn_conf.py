import multiprocessing
import os

port = os.environ.get("PORT", "5002")
bind = f"0.0.0.0:{port}"
workers = 100
worker_class = "uvicorn.workers.UvicornWorker"
max_requests = 100000
max_requests_jitter = 10000
timeout = 300
graceful_timeout = 30
keepalive = 30
worker_connections = 500000
backlog = 262144
accesslog = "-"
errorlog = "-"
loglevel = "info"
capture_output = True
limit_request_line = 65536
preload_app = False
