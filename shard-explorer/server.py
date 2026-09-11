#!/usr/bin/env python3
"""
HTTP server for conda-index shard explorer.
Serves local files and forwards missing files from valid conda subdirs to conda-pypi.
"""

import os
import sys
import urllib.request
import urllib.error
from pathlib import Path
from http.server import HTTPServer, SimpleHTTPRequestHandler
from datetime import datetime

# Valid conda subdirectories
VALID_SUBDIRS = {
    "linux-64",
    "noarch",
    "osx-64",
    "osx-arm64",
    "win-64",
    "win-arm64",
    "linux-aarch64",
    "linux-ppc64le",
    "linux-s390x",
}

REMOTE_BASE = "https://conda.anaconda.org/conda-pypi"
LOCAL_DIR = Path(__file__).parent


class CondaShardHandler(SimpleHTTPRequestHandler):
    """HTTP handler that serves local files and proxies to conda-pypi for subdirs."""

    def do_GET(self):
        """Handle GET requests."""
        # Normalize the path
        path = self.path.split("?")[0]  # Remove query string
        local_path = LOCAL_DIR / path.lstrip("/")

        # Check if file exists locally
        if local_path.exists() and local_path.is_file():
            return super().do_GET()

        # Extract subdir from path
        parts = path.strip("/").split("/")
        if not parts:
            # Root directory - don't proxy
            self.send_error(404, "Not Found")
            return

        subdir = parts[0]

        # Only proxy for valid conda subdirs
        if subdir not in VALID_SUBDIRS:
            return super().do_GET()

        # All other requests within a valid subdir should be forwarded
        self.proxy_to_remote(path, subdir, local_path)

    def proxy_to_remote(self, path, subdir, local_path):
        """Proxy request to remote conda-pypi server and cache locally."""
        remote_url = f"{REMOTE_BASE}{path}"

        try:
            self.log_message("Forwarding: %s -> %s", path, remote_url)

            # Fetch from remote
            with urllib.request.urlopen(remote_url, timeout=30) as response:
                data = response.read()
                headers = dict(response.headers)

            # Save locally if it's a .msgpack.zst file
            if path.endswith(".msgpack.zst"):
                local_path.parent.mkdir(parents=True, exist_ok=True)
                with open(local_path, "wb") as f:
                    f.write(data)
                self.log_message("Cached: %s", local_path)

            # Send response to client
            self.send_response(200)
            for key, value in headers.items():
                if key.lower() not in {"content-encoding"}:  # Avoid issues with chunked responses
                    self.send_header(key, value)
            self.end_headers()
            self.wfile.write(data)

        except urllib.error.HTTPError as e:
            self.send_error(e.code, f"Remote server error: {e.reason}")
        except urllib.error.URLError as e:
            self.send_error(502, f"Failed to fetch from remote: {e.reason}")
        except Exception as e:
            self.log_message("Error proxying %s: %s", path, str(e))
            self.send_error(500, f"Server error: {str(e)}")

    def log_message(self, format, *args):
        """Override to add timestamp."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = format % args
        print(f"[{timestamp}] {message}")


if __name__ == "__main__":
    port = 8000
    handler = CondaShardHandler
    server = HTTPServer(("localhost", port), handler)

    print(f"Server running at http://localhost:{port}/")
    print(f"Serving from: {LOCAL_DIR}")
    print(f"Remote proxy: {REMOTE_BASE}")
    print(f"Valid subdirs for forwarding: {', '.join(sorted(VALID_SUBDIRS))}")
    print("\nPress Ctrl+C to stop.")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")
        sys.exit(0)
