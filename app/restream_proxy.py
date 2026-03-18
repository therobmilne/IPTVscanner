"""
IPTV Restream Proxy
Sits between IPTV provider and Threadfin/Jellyfin.
Multiple local devices can watch the same channel using a single upstream connection.

Architecture:
  Provider <-- 1 connection --> Proxy <-- N connections --> Threadfin/Jellyfin/Devices

How it works:
  1. Generates a rewritten M3U with URLs pointing to this proxy
  2. When a device requests a channel, proxy checks if already streaming
  3. If yes: new client joins existing stream (no new provider connection)
  4. If no: opens one upstream connection to provider, fans out to all clients
  5. When last client disconnects, upstream connection is closed

Threadfin setup:
  - Point Threadfin M3U URL to: http://localhost:{PROXY_PORT}/playlist.m3u
  - Point Threadfin EPG URL to: http://localhost:{PROXY_PORT}/epg.xml
  - All streams are proxied through this server
"""

import logging
import os
import threading
import time
from collections import defaultdict
from pathlib import Path

from flask import Flask, Response, request, send_file

logger = logging.getLogger(__name__)

# Stream buffer size (64KB chunks)
CHUNK_SIZE = 65536
# How long to keep an idle upstream alive after last client disconnects (seconds)
IDLE_TIMEOUT = 10


class ActiveStream:
    """Represents a single active upstream connection being shared across clients."""

    def __init__(self, stream_id: str, upstream_url: str):
        self.stream_id = stream_id
        self.upstream_url = upstream_url
        self.clients = {}  # client_id -> queue
        self.lock = threading.Lock()
        self.running = False
        self.thread = None
        self.last_client_time = time.time()
        self.bytes_served = 0

    def add_client(self, client_id: str):
        """Add a new client to receive chunks from this stream."""
        import queue
        q = queue.Queue(maxsize=64)  # Buffer up to 64 chunks (~4MB)
        with self.lock:
            self.clients[client_id] = q
            self.last_client_time = time.time()
            logger.info(f"Stream {self.stream_id}: client {client_id} joined ({len(self.clients)} total)")
        return q

    def remove_client(self, client_id: str):
        """Remove a client from this stream."""
        with self.lock:
            self.clients.pop(client_id, None)
            self.last_client_time = time.time()
            remaining = len(self.clients)
            logger.info(f"Stream {self.stream_id}: client {client_id} left ({remaining} remaining)")
        return remaining

    def broadcast(self, chunk: bytes):
        """Send a chunk to all connected clients."""
        with self.lock:
            dead_clients = []
            for cid, q in self.clients.items():
                try:
                    q.put_nowait(chunk)
                except Exception:
                    # Queue full, client is too slow — drop them
                    dead_clients.append(cid)
            for cid in dead_clients:
                self.clients.pop(cid, None)
                logger.warning(f"Stream {self.stream_id}: dropped slow client {cid}")
            self.bytes_served += len(chunk)

    @property
    def client_count(self):
        with self.lock:
            return len(self.clients)


class RestreamProxy:
    """Main proxy that manages active streams and serves clients."""

    def __init__(self, config: dict):
        self.config = config
        iptv = config["iptv"]
        self.server = iptv["server"]
        self.username = iptv["username"]
        self.password = iptv["password"]

        self.proxy_port = config.get("proxy", {}).get("port", 8889)
        self.proxy_host = config.get("proxy", {}).get("host", "0.0.0.0")

        # Paths
        paths = config["paths"]
        self.live_tv_dir = Path(os.path.expanduser(paths.get("live_tv", "~/iptv-scanner/LiveTV")))
        self.m3u_path = self.live_tv_dir / "iptv_channels.m3u"
        self.epg_path = self.live_tv_dir / "epg.xml"

        # Active streams
        self.streams = {}  # stream_key -> ActiveStream
        self.streams_lock = threading.Lock()

        # Channel map: stream_id -> original_url
        self.channel_map = {}

        # Stats
        self.total_connections = 0
        self.peak_concurrent = 0

    def load_channels(self):
        """Parse the M3U file to build the channel map."""
        if not self.m3u_path.exists():
            logger.warning(f"M3U file not found: {self.m3u_path}")
            return

        self.channel_map.clear()
        current_id = None
        current_name = None

        with open(self.m3u_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if line.startswith("#EXTINF"):
                    # Extract tvg-id or channel name
                    import re
                    m = re.search(r'tvg-id="([^"]*)"', line)
                    current_id = m.group(1) if m else None
                    m2 = re.search(r',(.+)$', line)
                    current_name = m2.group(1).strip() if m2 else "Unknown"
                elif line and not line.startswith("#"):
                    # This is the URL
                    url = line
                    # Extract stream_id from URL
                    # Format: http://server/live/user/pass/STREAM_ID.ext
                    parts = url.rstrip("/").split("/")
                    if len(parts) >= 2:
                        stream_key = parts[-1].split(".")[0]  # Get ID without extension
                    else:
                        stream_key = str(hash(url))

                    self.channel_map[stream_key] = {
                        "url": url,
                        "name": current_name or stream_key,
                        "tvg_id": current_id or "",
                    }
                    current_id = None
                    current_name = None

        logger.info(f"Loaded {len(self.channel_map)} channels from M3U")

    def generate_proxy_m3u(self, base_url: str) -> str:
        """Generate a rewritten M3U with URLs pointing to this proxy."""
        if not self.channel_map:
            self.load_channels()

        lines = ["#EXTM3U"]

        # Re-read original M3U to preserve all metadata tags
        if self.m3u_path.exists():
            current_extinf = None
            with open(self.m3u_path, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("#EXTM3U"):
                        continue
                    elif line.startswith("#EXTINF") or line.startswith("#EXTVLCOPT"):
                        current_extinf = line
                        lines.append(line)
                    elif line and not line.startswith("#"):
                        # Rewrite URL to proxy
                        parts = line.rstrip("/").split("/")
                        if len(parts) >= 2:
                            stream_key = parts[-1].split(".")[0]
                            ext = parts[-1].split(".")[-1] if "." in parts[-1] else "ts"
                        else:
                            stream_key = str(hash(line))
                            ext = "ts"
                        proxy_url = f"{base_url}/stream/{stream_key}.{ext}"
                        lines.append(proxy_url)
                        current_extinf = None
                    elif line:
                        lines.append(line)

        return "\n".join(lines) + "\n"

    def get_upstream_url(self, stream_key: str) -> str:
        """Get the original provider URL for a stream key."""
        if stream_key in self.channel_map:
            return self.channel_map[stream_key]["url"]
        # Fallback: construct URL from stream key
        return f"{self.server}/live/{self.username}/{self.password}/{stream_key}.ts"

    def get_or_create_stream(self, stream_key: str) -> ActiveStream:
        """Get existing active stream or create a new upstream connection."""
        with self.streams_lock:
            if stream_key in self.streams and self.streams[stream_key].running:
                return self.streams[stream_key]

            upstream_url = self.get_upstream_url(stream_key)
            stream = ActiveStream(stream_key, upstream_url)
            self.streams[stream_key] = stream

            # Start upstream fetcher in background
            stream.running = True
            stream.thread = threading.Thread(
                target=self._upstream_fetcher,
                args=(stream,),
                daemon=True,
            )
            stream.thread.start()
            self.total_connections += 1

            return stream

    def _upstream_fetcher(self, stream: ActiveStream):
        """Background thread that reads from upstream and broadcasts to clients."""
        import requests

        logger.info(f"Upstream started: {stream.stream_id}")

        try:
            with requests.get(
                stream.upstream_url,
                stream=True,
                timeout=(10, 30),
                headers={"User-Agent": "IPTV-StreamManager/1.0"},
            ) as resp:
                resp.raise_for_status()
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    if not stream.running:
                        break
                    if not chunk:
                        continue

                    stream.broadcast(chunk)

                    # Check if we should stop (no clients for IDLE_TIMEOUT)
                    if stream.client_count == 0:
                        elapsed = time.time() - stream.last_client_time
                        if elapsed > IDLE_TIMEOUT:
                            logger.info(f"Upstream idle timeout: {stream.stream_id}")
                            break
        except Exception as e:
            logger.error(f"Upstream error for {stream.stream_id}: {e}")
        finally:
            stream.running = False
            with self.streams_lock:
                self.streams.pop(stream.stream_id, None)
            # Send sentinel to all remaining clients
            with stream.lock:
                for cid, q in stream.clients.items():
                    try:
                        q.put_nowait(None)
                    except Exception:
                        pass
            logger.info(f"Upstream closed: {stream.stream_id}")

    def get_stats(self) -> dict:
        """Get proxy statistics."""
        with self.streams_lock:
            active = {k: {"clients": v.client_count, "bytes": v.bytes_served}
                      for k, v in self.streams.items() if v.running}
        current = sum(s["clients"] for s in active.values())
        self.peak_concurrent = max(self.peak_concurrent, current)
        return {
            "active_streams": len(active),
            "active_clients": current,
            "peak_concurrent": self.peak_concurrent,
            "total_connections": self.total_connections,
            "channels_loaded": len(self.channel_map),
            "streams": active,
        }


def create_proxy_app(config: dict) -> tuple:
    """Create Flask app for the restream proxy."""
    proxy = RestreamProxy(config)
    proxy.load_channels()

    app = Flask(__name__)
    app.proxy = proxy

    client_counter = {"n": 0}
    counter_lock = threading.Lock()

    def next_client_id():
        with counter_lock:
            client_counter["n"] += 1
            return f"c{client_counter['n']}"

    @app.route("/playlist.m3u")
    def playlist():
        """Serve the rewritten M3U playlist pointing to this proxy."""
        # Determine our base URL
        host = request.host
        scheme = request.scheme
        base_url = f"{scheme}://{host}"
        m3u = proxy.generate_proxy_m3u(base_url)
        return Response(m3u, mimetype="audio/x-mpegurl",
                        headers={"Content-Disposition": "inline; filename=playlist.m3u"})

    @app.route("/epg.xml")
    @app.route("/xmltv.xml")
    def epg():
        """Serve the EPG XML file (passthrough)."""
        if proxy.epg_path.exists():
            return send_file(str(proxy.epg_path), mimetype="application/xml")
        # Fallback: redirect to provider EPG
        epg_url = f"{proxy.server}/xmltv.php?username={proxy.username}&password={proxy.password}"
        return Response(status=302, headers={"Location": epg_url})

    @app.route("/stream/<stream_file>")
    def stream(stream_file):
        """Proxy a live stream, sharing upstream connections."""
        stream_key = stream_file.split(".")[0]
        client_id = next_client_id()

        def generate():
            active = proxy.get_or_create_stream(stream_key)
            q = active.add_client(client_id)
            try:
                while True:
                    try:
                        chunk = q.get(timeout=30)
                    except Exception:
                        break  # Timeout, no data
                    if chunk is None:
                        break  # Stream ended
                    yield chunk
            finally:
                remaining = active.remove_client(client_id)
                # If no clients left, the upstream_fetcher will auto-close after IDLE_TIMEOUT

        return Response(
            generate(),
            mimetype="video/mp2t",
            headers={
                "Cache-Control": "no-cache, no-store",
                "Connection": "keep-alive",
                "Access-Control-Allow-Origin": "*",
            },
        )

    @app.route("/api/proxy/stats")
    def api_proxy_stats():
        """Get proxy statistics."""
        from flask import jsonify
        return jsonify(proxy.get_stats())

    @app.route("/api/proxy/reload", methods=["POST"])
    def api_proxy_reload():
        """Reload channel list from M3U."""
        from flask import jsonify
        proxy.load_channels()
        return jsonify({"message": f"Reloaded {len(proxy.channel_map)} channels"})

    @app.route("/")
    def proxy_index():
        """Simple status page."""
        stats = proxy.get_stats()
        return f"""<!DOCTYPE html>
<html><head><title>IPTV Restream Proxy</title>
<style>body{{font-family:monospace;background:#111;color:#eee;padding:40px}}
h1{{color:#38bdf8}}a{{color:#38bdf8}}.stat{{margin:10px 0}}</style></head>
<body>
<h1>IPTV Restream Proxy</h1>
<div class="stat">Channels loaded: <strong>{stats['channels_loaded']}</strong></div>
<div class="stat">Active streams: <strong>{stats['active_streams']}</strong></div>
<div class="stat">Active clients: <strong>{stats['active_clients']}</strong></div>
<div class="stat">Peak concurrent: <strong>{stats['peak_concurrent']}</strong></div>
<div class="stat">Total connections: <strong>{stats['total_connections']}</strong></div>
<hr>
<h3>For Threadfin:</h3>
<div>M3U URL: <a href="/playlist.m3u">http://YOUR_IP:{config.get('proxy',{{}}).get('port',8889)}/playlist.m3u</a></div>
<div>EPG URL: <a href="/epg.xml">http://YOUR_IP:{config.get('proxy',{{}}).get('port',8889)}/epg.xml</a></div>
</body></html>"""

    return app, proxy
