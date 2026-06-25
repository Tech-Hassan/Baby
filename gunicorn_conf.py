import multiprocessing

bind = "0.0.0.0:5002"
workers = 80
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
