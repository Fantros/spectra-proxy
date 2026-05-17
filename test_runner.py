import subprocess
import time
import socket
import threading
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

# Test Configuration
PROXY_PORT = 18080
TCP_PROXY_PORT = 15432
HTTP_BACKEND_1_PORT = 18081
HTTP_BACKEND_2_PORT = 18082
TCP_BACKEND_PORT = 15433

class MockHTTPHandler1(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return  # Suppress logging to keep console clean
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Backend 1 Response")

class MockHTTPHandler2(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Backend 2 Response")

class ReusableHTTPServer(HTTPServer):
    allow_reuse_address = True

def run_http_server(handler, port, stop_event):
    server = ReusableHTTPServer(('127.0.0.1', port), handler)
    server.timeout = 0.1
    while not stop_event.is_set():
        server.handle_request()
    server.server_close()

def run_tcp_server(port, stop_event):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('127.0.0.1', port))
    server.listen(5)
    server.settimeout(0.2)
    
    while not stop_event.is_set():
        try:
            client_sock, addr = server.accept()
            client_sock.settimeout(1.0)
            data = client_sock.recv(1024)
            if data:
                # Echo data back
                client_sock.sendall(data)
            client_sock.close()
        except socket.timeout:
            continue
        except Exception:
            break
    server.close()

def main():
    print("=" * 60)
    print("🚀 SPECTRA PROXY SYSTEM INTEGRATION TEST SUITE")
    print("=" * 60)

    # 1. Start Mock Backends with independent stop events
    stop_event_1 = threading.Event()
    stop_event_2 = threading.Event()
    stop_event_tcp = threading.Event()
    
    print("[*] Starting Mock HTTP Backend 1 on port {}...".format(HTTP_BACKEND_1_PORT))
    t1 = threading.Thread(target=run_http_server, args=(MockHTTPHandler1, HTTP_BACKEND_1_PORT, stop_event_1))
    t1.daemon = True
    t1.start()

    print("[*] Starting Mock HTTP Backend 2 on port {}...".format(HTTP_BACKEND_2_PORT))
    t2 = threading.Thread(target=run_http_server, args=(MockHTTPHandler2, HTTP_BACKEND_2_PORT, stop_event_2))
    t2.daemon = True
    t2.start()

    print("[*] Starting Mock TCP Echo Backend on port {}...".format(TCP_BACKEND_PORT))
    t3 = threading.Thread(target=run_tcp_server, args=(TCP_BACKEND_PORT, stop_event_tcp))
    t3.daemon = True
    t3.start()

    time.sleep(0.5)

    # 2. Build and Launch Spectra Proxy in the background
    print("[*] Starting Spectra Proxy daemon instantly...")
    proxy_proc = subprocess.Popen(
        ["./target/debug/spectra-proxy", "--config", "test_config.toml", "--log-only"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Wait for the proxy to initialize and bind ports
    print("[*] Waiting for proxy to initialize (3 seconds)...")
    time.sleep(3.0)

    success_checks = 0
    total_checks = 0

    try:
        # TEST 1: Load Balancing (Round-Robin)
        total_checks += 1
        print("\n🔍 Test 1: L7 HTTP Load Balancing (Round-Robin)")
        try:
            resp1 = urllib.request.urlopen("http://127.0.0.1:18080/", timeout=2.0).read().decode('utf-8')
            resp2 = urllib.request.urlopen("http://127.0.0.1:18080/", timeout=2.0).read().decode('utf-8')
            resp3 = urllib.request.urlopen("http://127.0.0.1:18080/", timeout=2.0).read().decode('utf-8')
            
            print("   -> Request 1: {}".format(resp1))
            print("   -> Request 2: {}".format(resp2))
            print("   -> Request 3: {}".format(resp3))
            
            if resp1 != resp2 and resp1 == resp3:
                print("   ✅ PASS: Round-Robin alternates between backends correctly!")
                success_checks += 1
            else:
                print("   ❌ FAIL: Alternation pattern mismatch.")
        except Exception as e:
            print("   ❌ FAIL: HTTP Request failed: {}".format(e))

        # TEST 2: L4 TCP Forwarding
        total_checks += 1
        print("\n🔍 Test 2: L4 TCP Echo Forwarding")
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect(('127.0.0.1', TCP_PROXY_PORT))
            test_msg = b"Hello Spectra Proxy TCP Echo Test!"
            sock.sendall(test_msg)
            response = sock.recv(1024)
            sock.close()
            
            print("   -> Sent:     {}".format(test_msg.decode('utf-8')))
            print("   -> Received: {}".format(response.decode('utf-8')))
            
            if response == test_msg:
                print("   ✅ PASS: TCP Proxy successfully routed and echoed bytes from backend!")
                success_checks += 1
            else:
                print("   ❌ FAIL: Echo mismatch.")
        except Exception as e:
            print("   ❌ FAIL: TCP Connection failed: {}".format(e))

        # TEST 3: Connection Rate Limiting Middleware
        total_checks += 1
        print("\n🔍 Test 3: 🛡️ Connection Rate Limiting Middleware (Early-Drop)")
        try:
            blocked_count = 0
            for i in range(15):
                try:
                    urllib.request.urlopen("http://127.0.0.1:18080/", timeout=1.0)
                except Exception:
                    blocked_count += 1
            print("   -> Flooded 15 HTTP requests in under 0.2 seconds.")
            print("   -> Rejections detected: {}".format(blocked_count))
            if blocked_count > 0:
                print("   ✅ PASS: Rate limiter successfully dropped subsequent spam connections!")
                success_checks += 1
            else:
                print("   ❌ FAIL: All requests passed. Rate limit did not trigger.")
        except Exception as e:
            print("   ❌ FAIL: Rate limit test error: {}".format(e))

        # Wait a moment for the token bucket to refill fully and let the circuit breaker test run cleanly
        time.sleep(6.0)

        # TEST 4: High-Availability Circuit Breaker
        total_checks += 1
        print("\n🔍 Test 4: 🔌 High-Availability Circuit Breaker Fail-over")
        
        # Shut down Backend 1 to simulate a server crash!
        print("   [*] Simulating backend crash: Stopping Mock HTTP Backend 1...")
        stop_event_1.set()
        t1.join()
        
        print("   [*] Generating connection failures to trip the circuit breaker for Backend 1...")
        # Send requests. Because Backend 1 is dead, the proxy's client will fail to connect.
        # Generating at least 3 attempts to trip the breaker.
        for _ in range(5):
            try:
                urllib.request.urlopen("http://127.0.0.1:18080/", timeout=1.0).read()
            except Exception:
                pass
        
        # Send subsequent requests. They should all go to the healthy Backend 2 immediately!
        print("   [*] Requesting after circuit is tripped...")
        success_routes = 0
        for _ in range(3):
            try:
                res = urllib.request.urlopen("http://127.0.0.1:18080/", timeout=1.0).read().decode('utf-8')
                print("   -> Route result: {}".format(res))
                if res == "Backend 2 Response":
                    success_routes += 1
            except Exception as e:
                print("   -> Route error: {}".format(e))

        if success_routes == 3:
            print("   ✅ PASS: Circuit breaker tripped dead backend and seamlessly routed all traffic to Backend 2!")
            success_checks += 1
        else:
            print("   ❌ FAIL: Traffic still attempted to reach crashed backend or hung.")

    finally:
        # 3. Clean up all resources
        print("\n" + "=" * 60)
        print("[*] Gracefully terminating Spectra Proxy daemon...")
        proxy_proc.terminate()
        try:
            proxy_proc.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            proxy_proc.kill()
        
        print("[*] Stopping mock backend servers...")
        stop_event_1.set()
        stop_event_2.set()
        stop_event_tcp.set()
        
        print("=" * 60)
        print("📊 INTEGRATION TEST SUMMARY: {}/{} CHECKS PASSED".format(success_checks, total_checks))
        print("=" * 60)
        
        if success_checks == total_checks:
            print("🏆 RESULT: SYSTEM OPERATIONAL & 100% HEALTHY!")
            print("=" * 60)
            exit(0)
        else:
            print("⚠️ RESULT: SYSTEM CHECK FAILS.")
            print("=" * 60)
            exit(1)

if __name__ == '__main__':
    main()
