"""Pipeline metrics exporter stub for Olist Lakehouse monitoring."""

import http.server
import os
import socketserver

PORT = int(os.environ.get("PORT", "9101"))


class MetricsHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/healthz":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")
        elif self.path in ("/metrics", "/metrics/"):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.end_headers()
            metrics = (
                "# HELP olist_pipeline_exporter_up Status of pipeline exporter\n"
                "# TYPE olist_pipeline_exporter_up gauge\n"
                "olist_pipeline_exporter_up 1\n"
            )
            self.wfile.write(metrics.encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass


def run():
    with socketserver.TCPServer(("", PORT), MetricsHandler) as httpd:
        print(f"Serving pipeline metrics on port {PORT}")
        httpd.serve_forever()


if __name__ == "__main__":
    run()
