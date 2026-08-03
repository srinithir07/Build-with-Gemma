import os
import sys
import time
import json
import threading
import urllib.request
import urllib.error
from http.server import HTTPServer, BaseHTTPRequestHandler

try:
    from neonize.client import NewClient
    from neonize.events import ConnectedEv, MessageEv, QREv
    from neonize.utils.jid import JID, build_jid
    HAS_NEONIZE = True
except ImportError:
    HAS_NEONIZE = False
    print("⚠️ [WhatsApp Gateway] Neonize module not found. Optional Python WhatsApp Gateway features disabled until 'pip install neonize' is run.")

# Reconfigure stdout/stderr to use utf-8 to prevent encoding crashes on Windows terminal
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# Global State
whatsapp_state = {
    "connected": False,
    "phone": None,
    "qr": None
}

CLIENT_DB_PATH = os.path.join(os.path.dirname(__file__), "whatsapp_session.db")
client = NewClient(CLIENT_DB_PATH)

def setup_client_events(cl):
    @cl.event(QREv)
    def on_qr(_: NewClient, qr_ev: QREv):
        if qr_ev.Codes:
            qr_str = qr_ev.Codes[0]
            whatsapp_state["qr"] = qr_str
            whatsapp_state["connected"] = False
            print(f"\n[QR CODE GENERATED] Fresh QR code ready for UI scanning.")

    @cl.event(ConnectedEv)
    def on_connected(c: NewClient, _: ConnectedEv):
        whatsapp_state["connected"] = True
        whatsapp_state["qr"] = None
        phone_number = "Unknown"
        try:
            if hasattr(c, "me") and c.me and hasattr(c.me, "JID") and c.me.JID:
                phone_number = getattr(c.me.JID, "User", "Unknown")
        except Exception as e:
            print(f"[WARN] Could not parse phone number: {e}")
        
        whatsapp_state["phone"] = phone_number
        print(f"\n🟢 [CONNECTED] WhatsApp session active for phone +{phone_number}")

    @cl.event(MessageEv)
    def on_message(_: NewClient, message: MessageEv):
        try:
            msg_body = message.Message
            if not msg_body:
                return

            text = None
            if msg_body.conversation:
                text = msg_body.conversation
            elif msg_body.extendedTextMessage and msg_body.extendedTextMessage.text:
                text = msg_body.extendedTextMessage.text

            if not text:
                return

            text_stripped = text.strip()
            chat_jid = message.Info.MessageSource.Chat
            sender_jid = message.Info.MessageSource.Sender
            is_from_me = getattr(message.Info, "IsFromMe", False)

            chat_server = getattr(chat_jid, "Server", "") if chat_jid else ""
            if chat_server not in ["s.whatsapp.net", "lid"]:
                return

            if is_from_me:
                return

            sender_user = getattr(sender_jid, "User", str(sender_jid)) if sender_jid else "unknown"
            print(f"\n📥 [INCOMING MESSAGE] From {sender_user}: {text_stripped}")

            # Forward incoming message to Next.js API /api/whatsapp/receive (PORT=5000 configured in .env)
            ports_to_try = [5000, 3000, 3001]
            payload = {
                "from": sender_user,
                "message": text_stripped,
                "timestamp": new_timestamp_str()
            }
            forwarded = False
            last_err = None
            for port in ports_to_try:
                for host in ["127.0.0.1", "localhost"]:
                    target_url = f"http://{host}:{port}/api/whatsapp/receive"
                    try:
                        data_bytes = json.dumps(payload).encode('utf-8')
                        req = urllib.request.Request(target_url, data=data_bytes, headers={'Content-Type': 'application/json'})
                        with urllib.request.urlopen(req, timeout=10) as resp:
                            if resp.status == 200:
                                print(f"⏩ [FORWARDED TO NEXT.JS] Status: {resp.status} ({target_url})")
                                forwarded = True
                                break
                    except Exception as err:
                        last_err = err
                if forwarded:
                    break

            if not forwarded:
                print(f"⚠️ [FORWARD ERROR] Could not reach Next.js on ports {ports_to_try}. Error: {last_err}")

        except Exception as e:
            print(f"❌ [ERROR] Message handler exception: {e}", file=sys.stderr)

setup_client_events(client)

def new_timestamp_str():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def reset_session():
    global client, whatsapp_state
    print("🔄 [RESET SESSION] Clearing existing session database files...")
    whatsapp_state["connected"] = False
    whatsapp_state["phone"] = None
    whatsapp_state["qr"] = None

    # Delete db session files
    for ext in ["", "-shm", "-wal"]:
        f_path = CLIENT_DB_PATH + ext
        if os.path.exists(f_path):
            try:
                os.remove(f_path)
                print(f"🗑️ Deleted {f_path}")
            except Exception as e:
                print(f"⚠️ Could not delete {f_path}: {e}")

    try:
        client = NewClient(CLIENT_DB_PATH)
        setup_client_events(client)
        def connect_task():
            client.connect()
        t = threading.Thread(target=connect_task, daemon=True)
        t.start()
    except Exception as ex:
        print(f"❌ Error restarting client: {ex}")

class MinimalHTTPRequestHandler(BaseHTTPRequestHandler):
    def _send_cors_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def do_OPTIONS(self):
        self.send_response(200)
        self._send_cors_headers()
        self.end_headers()

    def do_GET(self):
        if self.path == '/status':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self._send_cors_headers()
            self.end_headers()
            response_body = json.dumps(whatsapp_state).encode('utf-8')
            self.wfile.write(response_body)
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == '/reset' or self.path == '/logout':
            reset_session()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"success": True, "message": "Session reset requested. Fresh QR generating."}).encode('utf-8'))
            return

        if self.path == '/send':
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            try:
                data = json.loads(post_data.decode('utf-8'))
                recipient_phone = str(data.get('to', '')).strip().replace('+', '').replace(' ', '').replace('-', '')
                msg_text = data.get('message', '')

                if not recipient_phone or not msg_text:
                    self.send_response(400)
                    self.send_header('Content-Type', 'application/json')
                    self._send_cors_headers()
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "Missing 'to' or 'message'"}).encode('utf-8'))
                    return

                target_jid = build_jid(recipient_phone)
                client.send_message(target_jid, msg_text)
                print(f"📡 [OUTGOING SENT] To +{recipient_phone}: {msg_text[:40]}...")

                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self._send_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps({"success": True, "recipient": recipient_phone}).encode('utf-8'))
            except Exception as e:
                print(f"❌ [SEND ERROR]: {e}")
                self.send_response(500)
                self.send_header('Content-Type', 'application/json')
                self._send_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        return

def run_http_server():
    server_address = ('', 5001)
    httpd = HTTPServer(server_address, MinimalHTTPRequestHandler)
    print("🚀 [GATEWAY REST API] Minimal Python WhatsApp Gateway listening on port 5001...")
    httpd.serve_forever()

if __name__ == "__main__":
    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()

    print("[INFO] Starting Python Neonize WhatsApp Gateway...")
    try:
        client.connect()
    except KeyboardInterrupt:
        print("[INFO] Shutting down WhatsApp gateway...")
