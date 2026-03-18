"""
Xtream Codes API Client
Communicates with the IPTV provider using the standard Xtream Codes player API.
"""

import requests
import logging
import time
from urllib.parse import urljoin

logger = logging.getLogger(__name__)


class XtreamClient:
    """Client for interacting with an Xtream Codes-compatible IPTV provider."""

    def __init__(self, server: str, username: str, password: str, output_format: str = "ts"):
        self.server = server.rstrip("/")
        self.username = username
        self.password = password
        self.output_format = output_format
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "IPTVStreamManager/1.0"})
        self.server_info = None
        self.user_info = None

    def _api_url(self, action: str = None) -> str:
        """Build a player_api.php URL."""
        base = f"{self.server}/player_api.php?username={self.username}&password={self.password}"
        if action:
            base += f"&action={action}"
        return base

    def _request(self, url: str, retries: int = 3, timeout: int = 60) -> dict | list | None:
        """Make an HTTP GET request with retry logic."""
        for attempt in range(1, retries + 1):
            try:
                resp = self.session.get(url, timeout=timeout)
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.JSONDecodeError:
                logger.error(f"Invalid JSON from {url}")
                return None
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request attempt {attempt}/{retries} failed: {e}")
                if attempt < retries:
                    time.sleep(2 ** attempt)
        logger.error(f"All {retries} attempts failed for {url}")
        return None

    # ------------------------------------------------------------------ auth
    def authenticate(self) -> bool:
        """Authenticate and retrieve account + server info."""
        data = self._request(self._api_url())
        if not data:
            logger.error("Authentication failed - no response")
            return False

        self.user_info = data.get("user_info", {})
        self.server_info = data.get("server_info", {})

        status = self.user_info.get("status")
        if status != "Active":
            logger.error(f"Account status: {status} (expected Active)")
            return False

        logger.info(
            f"Authenticated as {self.user_info.get('username')} | "
            f"Expires: {self.user_info.get('exp_date')} | "
            f"Max connections: {self.user_info.get('max_connections')} | "
            f"Active connections: {self.user_info.get('active_cons')}"
        )
        return True

    # ------------------------------------------------------------ categories
    def get_live_categories(self) -> list:
        """Get all live TV channel categories."""
        return self._request(self._api_url("get_live_categories")) or []

    def get_vod_categories(self) -> list:
        """Get all VOD (movie) categories."""
        return self._request(self._api_url("get_vod_categories")) or []

    def get_series_categories(self) -> list:
        """Get all series (TV show) categories."""
        return self._request(self._api_url("get_series_categories")) or []

    # --------------------------------------------------------------- streams
    def get_live_streams(self, category_id: str = None) -> list:
        """Get live streams, optionally filtered by category."""
        url = self._api_url("get_live_streams")
        if category_id:
            url += f"&category_id={category_id}"
        return self._request(url) or []

    def get_vod_streams(self, category_id: str = None) -> list:
        """Get VOD streams (movies), optionally filtered by category."""
        url = self._api_url("get_vod_streams")
        if category_id:
            url += f"&category_id={category_id}"
        return self._request(url, timeout=120) or []

    def get_series(self, category_id: str = None) -> list:
        """Get series list, optionally filtered by category."""
        url = self._api_url("get_series")
        if category_id:
            url += f"&category_id={category_id}"
        return self._request(url, timeout=120) or []

    # --------------------------------------------------------- detail / info
    def get_vod_info(self, vod_id: str) -> dict | None:
        """Get detailed info for a single VOD item."""
        url = self._api_url("get_vod_info") + f"&vod_id={vod_id}"
        return self._request(url)

    def get_series_info(self, series_id: str) -> dict | None:
        """Get detailed info for a series (seasons + episodes)."""
        url = self._api_url("get_series_info") + f"&series_id={series_id}"
        return self._request(url, timeout=90)

    def get_short_epg(self, stream_id: str, limit: int = 4) -> dict | None:
        """Get short EPG for a live stream."""
        url = self._api_url("get_short_epg") + f"&stream_id={stream_id}&limit={limit}"
        return self._request(url)

    def get_full_epg(self, stream_id: str) -> dict | None:
        """Get full EPG for a live stream."""
        url = self._api_url("get_simple_data_table") + f"&stream_id={stream_id}"
        return self._request(url)

    # -------------------------------------------------------- URL builders
    def build_vod_url(self, stream_id: int, extension: str = None) -> str:
        """Build the direct stream URL for a VOD item."""
        ext = extension or self.output_format
        return f"{self.server}/movie/{self.username}/{self.password}/{stream_id}.{ext}"

    def build_series_url(self, stream_id: int, extension: str = None) -> str:
        """Build the direct stream URL for a series episode."""
        ext = extension or self.output_format
        return f"{self.server}/series/{self.username}/{self.password}/{stream_id}.{ext}"

    def build_live_url(self, stream_id: int) -> str:
        """Build the direct stream URL for a live channel."""
        return f"{self.server}/live/{self.username}/{self.password}/{stream_id}.{self.output_format}"

    def get_m3u_url(self) -> str:
        """Build the full M3U playlist URL."""
        return (
            f"{self.server}/get.php?"
            f"username={self.username}&password={self.password}"
            f"&type=m3u_plus&output={self.output_format}"
        )

    def get_epg_url(self) -> str:
        """Build the XMLTV / EPG URL."""
        return f"{self.server}/xmltv.php?username={self.username}&password={self.password}"
