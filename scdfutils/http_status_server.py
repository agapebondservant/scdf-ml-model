import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from mlmetrics import exporter
import logging
logger = logging.getLogger('httpserver')
logger.setLevel(logging.INFO)


class HttpHealthServer(BaseHTTPRequestHandler):
    def do_GET(self):
        if (self.path == '/actuator/health') or (self.path == '/actuator/info'):
            self.send_ok_response()
        elif self.path == '/actuator/prometheus':
            self.send_metrics_response()
        else:
            self.send_missing_response()

    def send_ok_response(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def send_missing_response(self):
        self.send_response(404)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def send_metrics_response(self):
        metrics = exporter.expose_metrics()
        logger.info(f"Metrics: {metrics}")
        if metrics:
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(metrics)
            logger.info("Sent metrics.")
        else:
            logger.info("No metrics to send.")

    @staticmethod
    def keepalive():
        while True:
            continue

    @staticmethod
    def run_thread(port=8080):
        http_server = HTTPServer(('', port), HttpHealthServer)
        thread = threading.Thread(name='httpd_server', target=http_server.serve_forever)
        thread.setDaemon(True)
        thread.start()

        keepalive = threading.Thread(name='httpd_server_keepalive', target=HttpHealthServer.keepalive)
        keepalive.start()
