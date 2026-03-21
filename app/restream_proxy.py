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
import queue
import re
import threading
import time
from pathlib import Path


logger = logging.getLogger(__name__)

# Stream buffer size (64KB chunks)
CHUNK_SIZE = 65536
# How long to keep an idle upstream alive after last client disconnects (seconds)
IDLE_TIMEOUT = 10
# Max retries when upstream connection fails or drops
MAX_UPSTREAM_RETRIES = 3
# Delay between retries (seconds)
RETRY_DELAY = 2
# Client queue get timeout (seconds) — how long a client waits for data before giving up
CLIENT_TIMEOUT = 60
# Upstream connect/read timeouts (seconds)
UPSTREAM_CONNECT_TIMEOUT = 15
UPSTREAM_READ_TIMEOUT = 60


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
        self.error = None  # Set if upstream failed permanently

    def add_client(self, client_id: str):
        """Add a new client to receive chunks from this stream."""
        q = queue.Queue(maxsize=128)  # Buffer up to 128 chunks (~8MB)
        with self.lock:
            self.clients[client_id] = q
            self.last_client_time = time.time()
            count = len(self.clients)
        logger.info(f"Stream {self.stream_id}: client {client_id} joined ({count} total)")
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
                except queue.Full:
                    # Queue full, client is too slow — drop them
                    dead_clients.append(cid)
            for cid in dead_clients:
                self.clients.pop(cid, None)
                logger.warning(f"Stream {self.stream_id}: dropped slow client {cid}")
            self.bytes_served += len(chunk)

    def send_sentinel(self):
        """Send None to all clients to signal stream end."""
        with self.lock:
            for cid, q in list(self.clients.items()):
                try:
                    q.put_nowait(None)
                except queue.Full:
                    # Clear queue then send sentinel
                    try:
                        while not q.empty():
                            q.get_nowait()
                        q.put_nowait(None)
                    except Exception:
                        pass

    @property
    def client_count(self):
        with self.lock:
            return len(self.clients)


class RestreamProxy:
    """Main proxy that manages active streams and serves clients."""

    def __init__(self, config: dict):
        self.config = config
        iptv = config["iptv"]
        self.server = iptv["server"].rstrip("/")
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

        # Thread-safe client ID counter
        self._client_counter = 0
        self._counter_lock = threading.Lock()

        # Stats
        self.total_connections = 0
        self.peak_concurrent = 0

    def next_client_id(self):
        """Generate a unique client ID (thread-safe)."""
        with self._counter_lock:
            self._client_counter += 1
            return f"c{self._client_counter}"

    def load_channels(self):
        """Parse the M3U file to build the channel map."""
        if not self.m3u_path.exists():
            logger.warning(f"M3U file not found: {self.m3u_path}")
            return

        new_map = {}
        current_id = None
        current_name = None

        try:
            with open(self.m3u_path, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("#EXTINF"):
                        m = re.search(r'tvg-id="([^"]*)"', line)
                        current_id = m.group(1) if m else None
                        m2 = re.search(r',(.+)$', line)
                        current_name = m2.group(1).strip() if m2 else "Unknown"
                    elif line and not line.startswith("#"):
                        url = line
                        parts = url.rstrip("/").split("/")
                        if len(parts) >= 2:
                            stream_key = parts[-1].split(".")[0]
                        else:
                            stream_key = str(hash(url))

                        new_map[stream_key] = {
                            "url": url,
                            "name": current_name or stream_key,
                            "tvg_id": current_id or "",
                        }
                        current_id = None
                        current_name = None

            self.channel_map = new_map
            logger.info(f"Loaded {len(self.channel_map)} channels from M3U")
        except Exception as e:
            logger.error(f"Failed to load M3U: {e}")

    def generate_proxy_m3u(self, base_url: str) -> str:
        """Generate a rewritten M3U with URLs pointing to this proxy."""
        if not self.channel_map:
            self.load_channels()

        lines = ["#EXTM3U"]

        if self.m3u_path.exists():
            try:
                with open(self.m3u_path, "r", encoding="utf-8", errors="replace") as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith("#EXTM3U"):
                            continue
                        elif line.startswith("#EXTINF") or line.startswith("#EXTVLCOPT"):
                            lines.append(line)
                        elif line and not line.startswith("#"):
                            parts = line.rstrip("/").split("/")
                            if len(parts) >= 2:
                                stream_key = parts[-1].split(".")[0]
                                ext = parts[-1].split(".")[-1] if "." in parts[-1] else "ts"
                            else:
                                stream_key = str(hash(line))
                                ext = "ts"
                            proxy_url = f"{base_url}/stream/{stream_key}.{ext}"
                            lines.append(proxy_url)
                        elif line:
                            lines.append(line)
            except Exception as e:
                logger.error(f"Failed to generate proxy M3U: {e}")

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
            existing = self.streams.get(stream_key)
            if existing and existing.running:
                return existing

            # Clean up dead stream reference if present
            if existing and not existing.running:
                self.streams.pop(stream_key, None)

            upstream_url = self.get_upstream_url(stream_key)
            stream = ActiveStream(stream_key, upstream_url)
            self.streams[stream_key] = stream

            # Start upstream fetcher in background
            stream.running = True
            stream.thread = threading.Thread(
                target=self._upstream_fetcher,
                args=(stream,),
                daemon=True,
                name=f"upstream-{stream_key}",
            )
            stream.thread.start()
            self.total_connections += 1

            return stream

    def _upstream_fetcher(self, stream: ActiveStream):
        """Background thread that reads from upstream and broadcasts to clients.
        Includes retry logic for dropped connections."""
        import requests as req

        retries = 0

        while stream.running and retries <= MAX_UPSTREAM_RETRIES:
            try:
                if retries > 0:
                    logger.info(f"Upstream retry {retries}/{MAX_UPSTREAM_RETRIES}: {stream.stream_id}")
                    time.sleep(RETRY_DELAY)
                    if stream.client_count == 0:
                        elapsed = time.time() - stream.last_client_time
                        if elapsed > RETRY_DELAY:
                            logger.info(f"No clients during retry, stopping: {stream.stream_id}")
                            break

                logger.info(f"Upstream connecting: {stream.stream_id} (attempt {retries + 1})")

                with req.get(
                    stream.upstream_url,
                    stream=True,
                    timeout=(UPSTREAM_CONNECT_TIMEOUT, UPSTREAM_READ_TIMEOUT),
                    headers={"User-Agent": "IPTV-StreamManager/1.0"},
                ) as resp:
                    resp.raise_for_status()

                    logger.info(f"Upstream connected: {stream.stream_id}")
                    retries = 0  # Reset retries on successful connection
                    stream.error = None

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
                                stream.running = False
                                break

                # If stream ended naturally but clients are waiting, retry
                if stream.running and stream.client_count > 0:
                    retries += 1
                    logger.warning(f"Upstream ended unexpectedly, will retry: {stream.stream_id}")
                    continue
                else:
                    break

            except req.exceptions.ConnectionError as e:
                retries += 1
                stream.error = f"Connection error: {e}"
                logger.error(f"Upstream connection error for {stream.stream_id}: {e}")
                if stream.client_count == 0:
                    break
            except req.exceptions.Timeout as e:
                retries += 1
                stream.error = f"Timeout: {e}"
                logger.error(f"Upstream timeout for {stream.stream_id}: {e}")
                if stream.client_count == 0:
                    break
            except req.exceptions.HTTPError as e:
                status = e.response.status_code if e.response is not None else 0
                stream.error = f"HTTP {status}: {e}"
                logger.error(f"Upstream HTTP error for {stream.stream_id}: {e}")
                if 400 <= status < 500:
                    break  # Don't retry client errors (auth, not found)
                retries += 1
                if stream.client_count == 0:
                    break
            except Exception as e:
                retries += 1
                stream.error = str(e)
                logger.error(f"Upstream error for {stream.stream_id}: {e}")
                if stream.client_count == 0:
                    break

        if retries > MAX_UPSTREAM_RETRIES:
            logger.error(f"Upstream max retries exceeded: {stream.stream_id}")
            stream.error = "Max retries exceeded"

        # Cleanup
        stream.running = False
        with self.streams_lock:
            self.streams.pop(stream.stream_id, None)
        stream.send_sentinel()
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


