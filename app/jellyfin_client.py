"""
Jellyfin Integration
Trigger library rescans and manage collections via the Jellyfin API.
"""

import logging
import requests

logger = logging.getLogger(__name__)


class JellyfinClient:
    """Interact with the Jellyfin server API."""

    def __init__(self, config: dict):
        jf = config.get("jellyfin", {})
        self.url = jf.get("url", "").rstrip("/")
        self.api_key = jf.get("api_key", "")
        self.auto_rescan = jf.get("auto_rescan", True)
        self.enabled = bool(self.url)
        self.auth_enabled = bool(self.url and self.api_key and self.api_key != "YOUR_JELLYFIN_API_KEY_HERE")

    def _headers(self) -> dict:
        return {"X-Emby-Token": self.api_key, "Content-Type": "application/json"}

    def trigger_library_scan(self) -> bool:
        """Tell Jellyfin to rescan all libraries."""
        if not self.auth_enabled:
            logger.warning("Jellyfin API key not configured, cannot trigger scan")
            return False
        try:
            resp = requests.post(f"{self.url}/Library/Refresh", headers=self._headers(), timeout=30)
            resp.raise_for_status()
            logger.info("Jellyfin library scan triggered successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to trigger Jellyfin scan: {e}")
            return False

    def get_libraries(self) -> list:
        if not self.auth_enabled:
            return []
        try:
            resp = requests.get(f"{self.url}/Library/VirtualFolders", headers=self._headers(), timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return []

    def create_collection(self, name: str, item_ids: list = None) -> dict | None:
        if not self.auth_enabled:
            return None
        try:
            params = {"Name": name, "IsLocked": False}
            if item_ids:
                params["Ids"] = ",".join(item_ids)
            resp = requests.post(f"{self.url}/Collections", headers=self._headers(), params=params, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Failed to create collection '{name}': {e}")
            return None

    def get_system_info(self) -> dict | None:
        """Get Jellyfin server info (public endpoint, no API key needed)."""
        if not self.enabled:
            return None
        try:
            resp = requests.get(f"{self.url}/System/Info/Public", timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return None
