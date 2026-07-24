import multiprocessing

workers = min(multiprocessing.cpu_count() * 2 + 1, 8)
bind = "0.0.0.0:$PORT"
worker_class = "uvicorn.workers.UvicornWorker"
timeout = 300
graceful_timeout = 30
keepalive = 30
accesslog = "-"
errorlog = "-"
loglevel = "info"
capture_output = True
preload_app = False
