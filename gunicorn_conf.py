import multiprocessing
import os

port = os.environ.get("PORT", "5002")
bind = f"0.0.0.0:{port}"
workers = min(multiprocessing.cpu_count() * 2 + 1, 8)
worker_class = "uvicorn.workers.UvicornWorker"
timeout = 300
graceful_timeout = 30
keepalive = 30
accesslog = "-"
errorlog = "-"
loglevel = "info"
capture_output = True
preload_app = False
