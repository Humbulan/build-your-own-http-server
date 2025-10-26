import socket
import threading

def handle_client(conn):
    with conn:
        try:
            data = conn.recv(1024).decode('utf-8')
            if not data:
                return
                
            # Parse the request line (first line)
            lines = data.split('\r\n')
            request_line = lines[0]
            parts = request_line.split()
            
            if len(parts) < 2:
                return
                
            method = parts[0]
            path = parts[1]
            
            print(f"Received {method} request for path: {path}")
            
            # Handle GET requests
            if method == 'GET':
                if path == '/':
                    # Return 200 OK for root path
                    response = "HTTP/1.1 200 OK\r\n\r\n"
                else:
                    # Return 404 Not Found for all other paths
                    response = "HTTP/1.1 404 Not Found\r\n\r\n"
                    
                conn.sendall(response.encode('utf-8'))
                print(f"Sent response: {response.split()[1]}")
                
        except Exception as e:
            print(f"Error handling request: {e}")

def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    # Bind to localhost port 4221
    server_socket.bind(('localhost', 4221))
    server_socket.listen()
    
    print("HTTP server started on port 4221")
    
    while True:
        try:
            conn, addr = server_socket.accept()
            print(f"Connection from {addr}")
            thread = threading.Thread(target=handle_client, args=(conn,))
            thread.start()
        except KeyboardInterrupt:
            print("\nShutting down server...")
            break
        except Exception as e:
            print(f"Server error: {e}")

if __name__ == "__main__":
    main()
