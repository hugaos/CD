from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import socket

class SudokuServerHTTPHandler(BaseHTTPRequestHandler):
    anchor_address = ('localhost', 7000)  # Defina o endereço e a porta do nó âncora

    def do_POST(self):
        if self.path == '/solve':
            self.handle_solve()
        else:
            self.send_error(404, "Endpoint not found")

    def do_GET(self):
        if self.path == '/stats' or self.path == '/network':
            self.forward_to_anchor(self.path.strip("/"))
        else:
            self.send_error(404, "Endpoint not found")

    def handle_solve(self):
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            print(f"Received POST data: {post_data}")
            data = json.loads(post_data.decode('utf-8'))
            print(f"Decoded JSON data: {data}")

            anchor_response = self.send_to_anchor(data, 'solve')
            print(f"Received response from anchor: {anchor_response}")

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(anchor_response)
        except json.JSONDecodeError as e:
            self.send_error(400, f"Bad Request: Unable to decode JSON. Error: {e}")
        except Exception as e:
            self.send_error(500, f"Internal Server Error: {e}")
            print(f"Exception: {e}")

    def forward_to_anchor(self, endpoint):
        try:
            response = self.send_to_anchor({}, endpoint)
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(response)
        except Exception as e:
            self.send_error(500, f"Internal Server Error: {e}")
            print(f"Exception: {e}")

    def send_to_anchor(self, data, endpoint):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(self.anchor_address)
                s.sendall(json.dumps({"type": endpoint, "data": data}).encode('utf-8'))
                return self.receive_full_response(s)
        except Exception as e:
            print(f"Error communicating with anchor: {e}")
            raise

    def receive_full_response(self, sock):
        buffer_size = 4096
        response = b""
        while True:
            part = sock.recv(buffer_size)
            response += part
            if len(part) < buffer_size:
                break
        return response

def run(server_class=HTTPServer, handler_class=SudokuServerHTTPHandler, port=8001):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f'Server running on port {port}...')
    httpd.serve_forever()

if __name__ == '__main__':
    run()
