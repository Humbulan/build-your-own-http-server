import socket
import threading
import queue
import time
import pymysql
from prometheus_client import CollectorRegistry, Counter, Histogram, push_to_gateway

# ============================================================
# CONFIG
# ============================================================
DB_HOST = '127.0.0.1'
DB_USER = 'root'
DB_PASS = 'RootStrongPass123!'
DB_NAME = 'imperial_nexus'
DB_PORT = 3306
PUSHGATEWAY = 'localhost:9092'
JOB_NAME = 'http_server'
LISTEN_HOST = '0.0.0.0'
LISTEN_PORT = 4221

# ============================================================
# PROMETHEUS
# ============================================================
prom_registry = CollectorRegistry()
prom_requests = Counter('http_requests_total', 'Total HTTP requests',
                        ['method', 'path', 'status'], registry=prom_registry)
prom_latency = Histogram('http_request_duration_seconds', 'HTTP latency',
                         ['method', 'path'],
                         buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
                         registry=prom_registry)
prom_bytes = Counter('http_response_bytes_total', 'Response bytes',
                     ['path'], registry=prom_registry)

def prometheus_push_worker():
    try:
        push_to_gateway(PUSHGATEWAY, job=JOB_NAME, registry=prom_registry)
        print("[Prom] Initial push done")
    except Exception as e:
        print(f"[Prom] Initial push failed: {e}")
    while True:
        time.sleep(15)
        try:
            push_to_gateway(PUSHGATEWAY, job=JOB_NAME, registry=prom_registry)
        except Exception as e:
            print(f"[Prom] Push failed: {e}")

# ============================================================
# DB LOGGING
# ============================================================
log_queue = queue.Queue(maxsize=2000)

def db_log_worker():
    conn = None
    while True:
        entry = log_queue.get()
        if entry is None:
            break
        if conn is None:
            try:
                conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS,
                                       database=DB_NAME, port=DB_PORT,
                                       connect_timeout=2, autocommit=True)
            except Exception as e:
                print(f"[DB Log] Connect failed: {e}")
                conn = None
                continue
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO http_request_logs "
                    "(client_ip, method, path, status_code, response_bytes, latency_ms, user_agent) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    entry
                )
        except Exception as e:
            print(f"[DB Log] Insert failed: {e}")
            try: conn.close()
            except: pass
            conn = None
    if conn:
        try: conn.close()
        except: pass

def log_http_request(client_ip, method, path, status, nbytes, latency_ms, ua):
    try:
        log_queue.put_nowait((client_ip, method, path, status, nbytes, latency_ms, (ua or '')[:255]))
    except queue.Full:
        pass

# ============================================================
# HTTP HANDLER
# ============================================================
def handle_client(conn, addr):
    client_ip = addr[0]
    start = time.time()
    method, path, status, nbytes, ua = '?', '?', 500, 0, ''
    try:
        with conn:
            try:
                data = conn.recv(4096).decode('utf-8', errors='replace')
            except Exception:
                return
            if not data:
                return

            lines = data.split('\r\n')
            request_line = lines[0]
            parts = request_line.split()
            if len(parts) < 2:
                return

            method, path = parts[0], parts[1]

            # Extract User-Agent if present
            for line in lines[1:]:
                if line.lower().startswith('user-agent:'):
                    ua = line.split(':', 1)[1].strip()
                    break

            print(f"[{client_ip}] {method} {path}")

            # Route
            if method == 'GET' and path == '/':
                body = b"Hello from Imperial HTTP Server\n"
                response = (
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Type: text/plain\r\n"
                    b"Content-Length: " + str(len(body)).encode() + b"\r\n"
                    b"\r\n" + body
                )
                status = 200
            elif method == 'GET' and path == '/health':
                body = b'{"status":"ok"}'
                response = (
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Type: application/json\r\n"
                    b"Content-Length: " + str(len(body)).encode() + b"\r\n"
                    b"\r\n" + body
                )
                status = 200
            elif method == 'GET' and path == '/metrics':
                # Tiny metrics endpoint from in-memory counters
                body = b"# see Pushgateway at :9092\n"
                response = (
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Type: text/plain\r\n"
                    b"Content-Length: " + str(len(body)).encode() + b"\r\n"
                    b"\r\n" + body
                )
                status = 200
            else:
                body = b"Not Found\n"
                response = (
                    b"HTTP/1.1 404 Not Found\r\n"
                    b"Content-Type: text/plain\r\n"
                    b"Content-Length: " + str(len(body)).encode() + b"\r\n"
                    b"\r\n" + body
                )
                status = 404

            conn.sendall(response)
            nbytes = len(response)
    except Exception as e:
        print(f"[{client_ip}] Error: {e}")
        status = 500
    finally:
        latency = time.time() - start
        log_http_request(client_ip, method, path, status, nbytes, latency * 1000, ua)
        prom_requests.labels(method=method, path=path, status=str(status)).inc()
        prom_latency.labels(method=method, path=path).observe(latency)
        prom_bytes.labels(path=path).inc(nbytes)
        print(f"[{client_ip}] -> {status} ({nbytes}B, {latency*1000:.1f}ms)")

# ============================================================
# MAIN
# ============================================================
def main():
    threading.Thread(target=db_log_worker, daemon=True).start()
    print("DB logging thread started")
    threading.Thread(target=prometheus_push_worker, daemon=True).start()
    print(f"Prometheus push thread started (gateway={PUSHGATEWAY})")

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((LISTEN_HOST, LISTEN_PORT))
    server_socket.listen(50)
    print(f"HTTP server listening on {LISTEN_HOST}:{LISTEN_PORT}")

    while True:
        try:
            conn, addr = server_socket.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
        except KeyboardInterrupt:
            print("\nShutting down...")
            log_queue.put(None)
            break
        except Exception as e:
            print(f"Server error: {e}")

if __name__ == "__main__":
    main()
