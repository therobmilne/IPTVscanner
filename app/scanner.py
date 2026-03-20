"""
IPTV Scanner
Scans the provider for VOD movies, TV series, and live channels.
Applies language filters, generates .strm files, and tracks state for incremental scans.
"""

import json
import logging
import os
import re
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

from .xtream_client import XtreamClient

logger = logging.getLogger(__name__)


def sanitize_filename(name: str) -> str:
    """Remove or replace characters that are invalid in file/folder names."""
    # Replace problematic characters
    name = re.sub(r'[<>:"/\\|?*]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    name = name.strip('.')
    return name or "Unknown"


def extract_year(title: str) -> str | None:
    """Try to pull a 4-digit year from a title string."""
    m = re.search(r'\((\d{4})\)', title)
    if m:
        return m.group(1)
    m = re.search(r'(\d{4})$', title.strip())
    if m and 1920 <= int(m.group(1)) <= 2030:
        return m.group(1)
    return None


def detect_quality(name: str, category_name: str = "") -> int:
    """Return a numeric quality score from title + category. Higher = better resolution."""
    combined = (name + " " + category_name).upper()
    # Check for unicode superscripts too
    if any(c in combined for c in ['⁴', '³⁸⁴⁰', 'ᴷ']):
        return 2160
    if any(tag in combined for tag in ['4K', '3840', 'UHD', '2160']):
        return 2160
    if any(tag in combined for tag in ['1080', 'FHD', 'BLURAY', 'BLU-RAY']):
        return 1080
    if re.search(r'\b(?:720)\b', combined):
        return 720
    if re.search(r'\b(?:480|SD)\b', combined):
        return 480
    if any(tag in combined for tag in ['HEVC', 'H265', 'H.265', 'DOLBY', 'ATMOS', 'REMUX']):
        return 1080
    return 720  # Default assumption


def normalize_for_dedup(title: str, year: str = "") -> str:
    """Aggressively normalize a title for dedup matching.
    Handles: articles (The/A/An), punctuation, spacing, unicode, common variants."""
    t = title.lower().strip()
    # Remove leading/trailing articles
    t = re.sub(r'^(the|a|an)\s+', '', t)
    t = re.sub(r',\s*(the|a|an)$', '', t)
    # Remove all punctuation and special chars
    t = re.sub(r'[^a-z0-9\s]', '', t)
    # Collapse whitespace
    t = re.sub(r'\s+', ' ', t).strip()
    # Append year if available
    if year:
        t = f"{t} {year}"
    return t


def clean_title(title: str) -> str:
    """Remove provider prefixes, quality tags, language codes, country codes for clean TMDB matching."""
    cleaned = title

    # --- Step 1: Remove provider prefixes (loop handles chains like "AMZ - 4K - Title") ---
    # Short provider codes
    _CODES = (
        r'AMZ|AMZN|NF|NFLX|DIS|DSNP|DPLUS|D\+|APL|ATVP|ATV|'
        r'HBO|HMAX|HLU|PCK|PCCK|SHO|PMT|PRMNT|UNI|MRV|JB|'
        r'VIA|DSC|SKY|DWA|A\+|ESPN\+?|AMC\+?'
    )
    # Full platform names (longest-match first to avoid partial matches)
    _NAMES = (
        r'NETFLIX|AMAZON\s+PRIME\s+VIDEO|AMAZON\s+PRIME|PRIME\s+VIDEO|AMAZON|PRIME|'
        r'DISNEY\s+PLUS|DISNEY\+|DISNEY|'
        r'APPLE\s+TV\+|APPLE\s+TV\s+PLUS|APPLE\s+TV|APPLE|'
        r'HBO\s+MAX|HBO\s+GO|HBO|MAX|'
        r'HULU|PARAMOUNT\+|PARAMOUNT\s+PLUS|PARAMOUNT|'
        r'PEACOCK|SHOWTIME|STARZ|'
        r'DISCOVERY\+|DISCOVERY\s+PLUS|DISCOVERY|'
        r'ESPN\+?|AMC\+?|BBC\s+IPLAYER|BBC|ITV|CHANNEL\s+4|'
        r'SKY|NOW\s+TV|TUBI|CRACKLE|'
        r'MARVEL|UNIVERSAL|DREAMWORKS|NICKELODEON|VIAPLAY'
    )
    # Separator pattern: space-dash-space, colon, dash, pipe, or just whitespace
    _SEP = r'\s*[-–:|]\s*'

    for _ in range(4):  # up to 4 passes for chains like "AMZ - NF - 4K - Title"
        prev = cleaned
        # Bracket/paren-wrapped codes: [AMZ] Title, (NF) Title
        cleaned = re.sub(
            r'^\s*[\[(]\s*(?:' + _CODES + r'|' + _NAMES + r')\s*[\])]\s*' + r'(?:' + _SEP + r')?',
            '', cleaned, flags=re.IGNORECASE
        )
        # Code with separator: -AMZ - Title, AMZ: Title, AMZ | Title
        cleaned = re.sub(
            r'^-?\s*(?:' + _CODES + r')' + _SEP,
            '', cleaned, flags=re.IGNORECASE
        )
        # Full name with separator: Netflix - Title, Amazon Prime - Title
        cleaned = re.sub(
            r'^-?\s*(?:' + _NAMES + r')\+?' + _SEP,
            '', cleaned, flags=re.IGNORECASE
        )
        # 2-letter language/country code with separator: "EN - Title", "LT - Title"
        cleaned = re.sub(r'^-?\s*[A-Z]{2}\s*[-–|]\s*', '', cleaned)
        if cleaned == prev:
            break  # nothing changed, stop early

    # Handle collection/category prefixes still in the title like "IMDB TOP 250 - Title", "NEW RELEASE - Title"
    cleaned = re.sub(
        r'^(?:IMDB TOP \d+|NEW RELEASE|COLLECTIONS?|STAND[- ]UP COMEDY|DOCU[- ]?MOVIES?|'
        r'DOCU[- ]?SERIES|REALITY|WORKOUT|CONCERTS?|CHRISTMAS|BOXING|UFC|WWE|'
        r'ANIMI|KIDS|WESTERNS?|MUSICAL|FAMILY|ROMANCE|HORROR|THRILLER|'
        r'COMEDY|ACTION|ADVENTURE|DRAMA|SCIENCE FICTION|DOCUMENTAR(?:Y|IES))\s*[-–:]\s*',
        '', cleaned, flags=re.IGNORECASE
    )

    # --- Step 2: Remove quality/format tags ---
    # Remove compound tags first (before individual words)
    for tag in ['DOLBY AUDIO', 'DOLBY ATMOS', 'DOLBY VISION', 'DOLBY DIGITAL',
                'Multi Audio', 'Dual Audio', 'Multi-Sub', 'WEB-DL', 'Blu-Ray']:
        cleaned = re.sub(r'(?i)' + re.escape(tag), '', cleaned)
    # Then individual tags (word boundary to avoid matching inside words like "Wednesday")
    for tag in ['4K', '3840P', '3840p', 'UHD', 'FHD', '1080p', '1080P', '720p', '720P', '480p',
                'HEVC', 'H265', 'H.265', 'x265', 'x264', 'HDR', 'HDR10', 'SDR',
                'BluRay', 'BRRip', 'WEBRip', 'HDTV', 'CAM',
                'MULTI', 'DOLBY', 'AUDIO', 'ATMOS', 'DTS', 'AAC', 'AC3', 'REMUX']:
        cleaned = re.sub(r'(?i)\b' + re.escape(tag) + r'\b', '', cleaned)

    # Remove unicode superscript quality markers (⁴ᴷ ³⁸⁴⁰ᴾ ᴰᴼᴸᴮʸ ᴬᵁᴰᴵᴼ ᴴᴰ ᴿᴬᵂ etc.)
    cleaned = re.sub(r'[⁰¹²³⁴⁵⁶⁷⁸⁹ᴬᴮᴰᴱᴳᴴᴵᴷᴸᴹᴺᴼᴾᴿˢᵀᵁⱽᵂʰᵉᵛᶜᶠᵖˢʸ]+', '', cleaned)

    # --- Step 3: Remove country codes in parentheses: (US), (GB), (DE), etc. ---
    cleaned = re.sub(r'\s*\([A-Z]{2}\)\s*', ' ', cleaned)
    # Remove trailing country code without parens
    cleaned = re.sub(r'\s+[A-Z]{2}\s*$', '', cleaned)

    # --- Step 4: Remove year (we extract it separately) ---
    cleaned = re.sub(r'\(\d{4}\)', '', cleaned)
    cleaned = re.sub(r'\s+\d{4}\s*$', '', cleaned)

    # --- Step 5: Clean up ---
    # Remove dangling separators
    cleaned = re.sub(r'\s*[-–|:]+\s*$', '', cleaned)
    cleaned = re.sub(r'^\s*[-–|:]+\s*', '', cleaned)
    # Remove double spaces, trim
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    # Remove trailing dots or dashes
    cleaned = cleaned.strip('.-– ')

    return cleaned if cleaned else title.strip()


# Ordered longest-match first to avoid partial hits (e.g. "PRIME" before "AMAZON PRIME")
_PLATFORM_MAP = [
    ("Amazon Prime",  r'^-?\s*(?:AMZ|AMZN|AMAZON(?:\s+PRIME(?:\s+VIDEO)?)?|PRIME(?:\s+VIDEO)?)\s*[-–:|]|^\s*[\[(]\s*(?:AMZ|AMZN)\s*[\])]'),
    ("Netflix",       r'^-?\s*(?:NF|NFLX|NETFLIX)\s*[-–:|]|^\s*[\[(]\s*(?:NF|NFLX)\s*[\])]'),
    ("Disney+",       r'^-?\s*(?:DIS|DSNP|DPLUS|D\+|DISNEY(?:\+|\s+PLUS)?)\s*[-–:|]|^\s*[\[(]\s*(?:DIS|DSNP|DPLUS)\s*[\])]'),
    ("Apple TV+",     r'^-?\s*(?:APL|ATVP|ATV|APPLE\s+TV(?:\+|\s+PLUS)?)\s*[-–:|]|^\s*[\[(]\s*(?:APL|ATVP|ATV)\s*[\])]'),
    ("HBO Max",       r'^-?\s*(?:HMAX|HBO\s*MAX|HBO\s*GO|HBO)\s*[-–:|]|^\s*[\[(]\s*(?:HBO|HMAX)\s*[\])]'),
    ("Hulu",          r'^-?\s*(?:HLU|HULU)\s*[-–:|]|^\s*[\[(]\s*HLU\s*[\])]'),
    ("Peacock",       r'^-?\s*(?:PCK|PCCK|PEACOCK)\s*[-–:|]|^\s*[\[(]\s*(?:PCK|PCCK)\s*[\])]'),
    ("Showtime",      r'^-?\s*(?:SHO|SHOWTIME)\s*[-–:|]|^\s*[\[(]\s*SHO\s*[\])]'),
    ("Paramount+",    r'^-?\s*(?:PMT|PRMNT|PARAMOUNT(?:\+|\s+PLUS)?)\s*[-–:|]|^\s*[\[(]\s*(?:PMT|PRMNT)\s*[\])]'),
    ("Starz",         r'^-?\s*(?:STZ|STARZ?)\s*[-–:|]|^\s*[\[(]\s*STZ\s*[\])]'),
    ("Discovery+",    r'^-?\s*(?:DSC|DISCOVERY(?:\+|\s+PLUS)?)\s*[-–:|]|^\s*[\[(]\s*DSC\s*[\])]'),
    ("ESPN+",         r'^-?\s*ESPN\+?\s*[-–:|]|^\s*[\[(]\s*ESPN\s*[\])]'),
    ("AMC+",          r'^-?\s*AMC\+?\s*[-–:|]|^\s*[\[(]\s*AMC\s*[\])]'),
    ("BBC",           r'^-?\s*BBC\s*[-–:|]|^\s*[\[(]\s*BBC\s*[\])]'),
    ("ITV",           r'^-?\s*ITV\s*[-–:|]|^\s*[\[(]\s*ITV\s*[\])]'),
    ("Sky",           r'^-?\s*SKY\s*[-–:|]|^\s*[\[(]\s*SKY\s*[\])]'),
    ("Now TV",        r'^-?\s*NOW\s*TV\s*[-–:|]'),
    ("Tubi",          r'^-?\s*TUBI\s*[-–:|]|^\s*[\[(]\s*TUBI\s*[\])]'),
    ("Viaplay",       r'^-?\s*(?:VIA|VIAPLAY)\s*[-–:|]'),
    ("Marvel",        r'^-?\s*(?:MRV|MARVEL)\s*[-–:|]|^\s*[\[(]\s*MRV\s*[\])]'),
]
_PLATFORM_MAP_COMPILED = [(name, re.compile(pat, re.IGNORECASE)) for name, pat in _PLATFORM_MAP]


def detect_platform_from_name(name: str) -> str:
    """Detect streaming platform from provider title prefix (e.g. 'AMZ - Title' → 'Amazon Prime')."""
    for platform, pattern in _PLATFORM_MAP_COMPILED:
        if pattern.search(name):
            return platform
    return ""


class ScanStats:
    """Track statistics for a scan run."""

    def __init__(self):
        self.started_at = datetime.now(timezone.utc)
        self.finished_at = None
        self.total_vod = 0
        self.total_series = 0
        self.total_episodes = 0
        self.total_live = 0
        self.new_movies = 0
        self.new_episodes = 0
        self.new_channels = 0
        self.skipped_existing = 0
        self.skipped_filtered = 0
        self.dupes_skipped = 0
        self.dupes_replaced = 0
        self.dupes_sweep = 0
        self.failed = 0
        self.errors = []

    def to_dict(self) -> dict:
        dur = None
        dur_str = None
        if self.finished_at:
            dur = (self.finished_at - self.started_at).total_seconds()
            m, s = divmod(int(dur), 60)
            h, m = divmod(m, 60)
            dur_str = f"{h}h {m}m {s}s" if h else f"{m}m {s}s"
        return {
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "duration_seconds": dur,
            "duration": dur_str,
            "total_vod": self.total_vod,
            "total_series": self.total_series,
            "total_episodes": self.total_episodes,
            "total_live": self.total_live,
            "new_movies": self.new_movies,
            "new_episodes": self.new_episodes,
            "new_channels": self.new_channels,
            "skipped_existing": self.skipped_existing,
            "skipped_filtered": self.skipped_filtered,
            "dupes_skipped": self.dupes_skipped,
            "dupes_replaced": self.dupes_replaced,
            "dupes_sweep": self.dupes_sweep,
            "dupes_total": self.dupes_skipped + self.dupes_replaced + self.dupes_sweep,
            "failed": self.failed,
            "errors": self.errors[-50:],
        }


class IPTVScanner:
    """Main scanner that pulls content from the IPTV provider."""

    def __init__(self, config: dict):
        self.config = config
        iptv = config["iptv"]
        self.client = XtreamClient(
            server=iptv["server"],
            username=iptv["username"],
            password=iptv["password"],
            output_format=iptv.get("output_format", "ts"),
        )
        self.paths = config["paths"]
        # Expand ~ in all paths
        for key in self.paths:
            if isinstance(self.paths[key], str) and "~" in self.paths[key]:
                self.paths[key] = os.path.expanduser(self.paths[key])
        self.filters = config.get("filters", {})

        # Category ID whitelist (new approach - much more accurate)
        self.filter_mode = self.filters.get("mode", "whitelist")
        self.vod_cat_ids = set(str(x) for x in self.filters.get("vod_category_ids", []))
        self.series_cat_ids = set(str(x) for x in self.filters.get("series_category_ids", []))
        self.live_cat_ids = set(str(x) for x in self.filters.get("live_category_ids", []))

        # Legacy name-based filters (fallback if no category IDs configured)
        self.include_langs = [p.lower() for p in self.filters.get("include_languages", [])]
        self.exclude_patterns = [p.lower() for p in self.filters.get("exclude_patterns", [])]
        self.exclude_anime = self.filters.get("exclude_anime", True)
        # User-defined tags per category ID (for Jellyfin organization)
        self.category_tags = {str(k): v for k, v in self.filters.get("category_tags", {}).items()}

        # State database (simple JSON file for tracking what's been processed)
        data_dir = Path(self.paths.get("data_dir", "./data"))
        data_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = data_dir / "scan_state.json"
        self.history_file = data_dir / "scan_history.json"
        self.state = self._load_state()

        # Progress tracking (read by dashboard)
        self.progress = {
            "step": "idle",          # idle, authenticating, scanning_movies, scanning_series, scanning_live, building_collections, done
            "step_number": 0,        # 0-5
            "total_steps": 5,
            "step_label": "Idle",
            "items_processed": 0,
            "items_total": 0,
            "percent": 0,
            "eta_seconds": None,
            "step_started_at": None,
            "details": "",
        }
        self.stop_requested = False

    def _load_state(self) -> dict:
        """Load previously processed items from the state file."""
        if self.state_file.exists():
            try:
                with open(self.state_file) as f:
                    return json.load(f)
            except Exception:
                logger.warning("Could not load state file, starting fresh")
        return {"movies": {}, "series": {}, "episodes": {}, "channels": {}}

    def _save_state(self):
        """Persist the current state to disk."""
        with open(self.state_file, "w") as f:
            json.dump(self.state, f, indent=2)

    def _save_history(self, stats: ScanStats):
        """Append scan stats to history."""
        history = []
        if self.history_file.exists():
            try:
                with open(self.history_file) as f:
                    history = json.load(f)
            except Exception:
                pass
        history.append(stats.to_dict())
        # Keep last 100 scan records
        history = history[-100:]
        with open(self.history_file, "w") as f:
            json.dump(history, f, indent=2)

    # -------------------------------------------------------- category filter
    def _cat_id_allowed(self, category_id: str, allowed_set: set) -> bool:
        """Check if a category ID is in the whitelist (if configured)."""
        if not allowed_set:
            # No whitelist configured - fall back to legacy name filter
            return True
        return str(category_id) in allowed_set

    def _passes_language_filter(self, name: str, category_name: str = "") -> bool:
        """Legacy name-based filter - only used if no category IDs are configured."""
        combined = f"{name} {category_name}".lower()

        for pattern in self.exclude_patterns:
            if pattern in combined:
                return False

        if self.include_langs:
            return any(lang in combined for lang in self.include_langs)

        return True

    def _should_include(self, cat_id: str, cat_id_whitelist: set, name: str, cat_name: str) -> bool:
        """Check if an item passes category/language filters."""
        if cat_id_whitelist:
            return self._cat_id_allowed(cat_id, cat_id_whitelist)
        return self._passes_language_filter(name, cat_name)

    def _is_anime(self, name: str, category_name: str = "") -> bool:
        """Check if content is anime based on category name or title patterns."""
        combined = (category_name + " " + name).upper()
        anime_keywords = ['ANIME', 'ANIMI', 'MANGA', 'CRUNCHYROLL', 'FUNIMATION', 'ANIMAX', 'ANIMEZ']
        return any(kw in combined for kw in anime_keywords)

    def _set_progress(self, step: str, step_number: int, label: str, items_processed: int = 0, items_total: int = 0, details: str = ""):
        """Update progress tracking for the dashboard."""
        self.progress["step"] = step
        self.progress["step_number"] = step_number
        self.progress["step_label"] = label
        self.progress["items_processed"] = items_processed
        self.progress["items_total"] = items_total
        self.progress["details"] = details
        if items_total > 0:
            self.progress["percent"] = round((items_processed / items_total) * 100, 1)
        else:
            self.progress["percent"] = 0
        # Estimate ETA based on elapsed time and progress
        started = self.progress.get("_step_started_ts")
        if started and items_processed > 0 and items_total > 0:
            elapsed = time.time() - started
            rate = items_processed / max(elapsed, 0.1)
            remaining = items_total - items_processed
            self.progress["eta_seconds"] = round(remaining / max(rate, 0.001))
        else:
            self.progress["eta_seconds"] = None

    def _start_step(self, step: str, step_number: int, label: str):
        """Mark a new step as started."""
        self.progress["_step_started_ts"] = time.time()
        self.progress["step_started_at"] = datetime.now(timezone.utc).isoformat()
        self._set_progress(step, step_number, label)
        return True

    # -------------------------------------------------------- STRM creation
    def _write_strm(self, filepath: Path, url: str) -> bool:
        """Write a .strm file with the stream URL."""
        try:
            filepath.parent.mkdir(parents=True, exist_ok=True)
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(url)
            return True
        except Exception as e:
            logger.error(f"Failed to write {filepath}: {e}")
            return False

    # ---------------------------------------------------------- movie scan
    def scan_movies(self, stats: ScanStats) -> None:
        """Scan and process all VOD movies from the provider."""
        logger.info("=== Scanning VOD Movies ===")
        self._start_step("scanning_movies", 1, "Scanning movies")
        movies_dir = Path(self.paths["movies"])

        # Get categories first for labeling
        categories = self.client.get_vod_categories()
        cat_map = {str(c.get("category_id") or ""): c.get("category_name", "") for c in categories}
        logger.info(f"Found {len(categories)} VOD categories")

        # Get all VOD streams
        all_vod = self.client.get_vod_streams()
        stats.total_vod = len(all_vod)
        logger.info(f"Found {len(all_vod)} total VOD items from provider")

        # Build dedup maps from existing state
        # Map 1: normalized_title -> (stream_id, quality, folder_name)
        # Map 2: tmdb_id -> (stream_id, quality, folder_name)
        seen_by_title = {}
        seen_by_tmdb = {}
        for sid, info in self.state.get("movies", {}).items():
            t = info.get("title", "")
            y = info.get("year", "")
            q = info.get("quality", 720)
            fn = info.get("strm_path", "")
            nkey = normalize_for_dedup(t, y)
            if nkey:
                seen_by_title[nkey] = (sid, q, fn)
            tmdb_id = info.get("tmdb_id")
            if tmdb_id:
                seen_by_tmdb[str(tmdb_id)] = (sid, q, fn)

        # dedup tracked in stats
        # dedup tracked in stats

        for idx, vod in enumerate(all_vod):
            if self.stop_requested:
                logger.info("Stop requested, halting movie scan")
                break
            if idx % 500 == 0:
                self._set_progress("scanning_movies", 1, "Scanning movies", idx, len(all_vod), f"{stats.new_movies} new, {stats.dupes_skipped} dupes")

            stream_id = str(vod.get("stream_id", ""))
            name = vod.get("name", "Unknown")
            cat_id = str(vod.get("category_id", "") or "")
            cat_name = cat_map.get(cat_id, "")
            extension = vod.get("container_extension", "ts")

            if not self._should_include(cat_id, self.vod_cat_ids, name, cat_name):
                stats.skipped_filtered += 1
                continue

            # Anime filter
            if self.exclude_anime and self._is_anime(name, cat_name):
                stats.skipped_filtered += 1
                continue

            # Already processed (exact stream_id)?
            if stream_id in self.state["movies"]:
                stats.skipped_existing += 1
                continue

            # Build metadata
            platform = detect_platform_from_name(name)
            title = clean_title(name)
            year = extract_year(name) or vod.get("year", "")
            quality = detect_quality(name, cat_name)
            safe_title = sanitize_filename(title)
            folder_name = f"{safe_title} ({year})" if year else safe_title

            # Build stream URL
            stream_url = self.client.build_vod_url(int(stream_id), extension)

            # ============ DEDUP: check both title-based and TMDB-based ============
            tmdb_id = vod.get("tmdb_id")
            norm_key = normalize_for_dedup(title, year)
            is_dupe = False
            existing_sid = None
            existing_quality = 0

            # Check by TMDB ID first (most reliable)
            if tmdb_id and str(tmdb_id) in seen_by_tmdb:
                existing_sid, existing_quality, _ = seen_by_tmdb[str(tmdb_id)]
                is_dupe = True

            # Check by normalized title
            if not is_dupe and norm_key and norm_key in seen_by_title:
                existing_sid, existing_quality, _ = seen_by_title[norm_key]
                is_dupe = True

            # Also check without year for titles that might have year on one but not another
            if not is_dupe and year:
                norm_no_year = normalize_for_dedup(title, "")
                if norm_no_year in seen_by_title:
                    existing_sid, existing_quality, _ = seen_by_title[norm_no_year]
                    is_dupe = True

            if is_dupe:
                if quality <= existing_quality:
                    stats.dupes_skipped += 1
                    stats.skipped_filtered += 1
                    continue
                else:
                    # Higher quality — replace the old one
                    old_info = self.state["movies"].get(existing_sid)
                    if old_info:
                        old_path = Path(old_info.get("strm_path", ""))
                        if old_path.exists():
                            old_path.unlink()
                            try:
                                old_path.parent.rmdir()
                            except OSError:
                                pass
                        # Remove old TMDB entry
                        old_tmdb = old_info.get("tmdb_id")
                        if old_tmdb and str(old_tmdb) in seen_by_tmdb:
                            del seen_by_tmdb[str(old_tmdb)]
                        # Remove old title entry
                        old_norm = normalize_for_dedup(old_info.get("title", ""), old_info.get("year", ""))
                        if old_norm in seen_by_title:
                            del seen_by_title[old_norm]
                        del self.state["movies"][existing_sid]
                        stats.dupes_replaced += 1
                        logger.debug(f"  Replaced {quality}p > {existing_quality}p: {title}")

            # Write .strm file: Movies/Title (Year)/Title (Year).strm
            strm_path = movies_dir / folder_name / f"{folder_name}.strm"

            if self._write_strm(strm_path, stream_url):
                stats.new_movies += 1
                # Update both dedup maps
                if norm_key:
                    seen_by_title[norm_key] = (stream_id, quality, str(strm_path))
                if tmdb_id:
                    seen_by_tmdb[str(tmdb_id)] = (stream_id, quality, str(strm_path))
                # Also index without year
                norm_no_year = normalize_for_dedup(title, "")
                if norm_no_year and norm_no_year != norm_key:
                    seen_by_title[norm_no_year] = (stream_id, quality, str(strm_path))

                self.state["movies"][stream_id] = {
                    "name": name,
                    "title": title,
                    "year": year,
                    "quality": quality,
                    "category": cat_name,
                    "platform": platform,
                    "tags": self.category_tags.get(cat_id, []),
                    "stream_url": stream_url,
                    "strm_path": str(strm_path),
                    "added_at": datetime.now(timezone.utc).isoformat(),
                    "tmdb_id": tmdb_id,
                    "rating": vod.get("rating"),
                    "stream_icon": vod.get("stream_icon", ""),
                }
                if stats.new_movies % 100 == 0:
                    logger.info(f"  ... processed {stats.new_movies} new movies so far")
            else:
                stats.failed += 1
                stats.errors.append(f"Failed to write movie: {name}")

        logger.info(
            f"Movies done: {stats.new_movies} new, "
            f"{stats.skipped_existing} existing, "
            f"{stats.skipped_filtered} filtered out, "
            f"{stats.dupes_skipped} dupes, "
            f"{stats.dupes_replaced} upgraded"
        )

    # --------------------------------------------------------- series scan
    def scan_series(self, stats: ScanStats) -> None:
        """Scan and process all TV series from the provider."""
        logger.info("=== Scanning TV Series ===")
        self._start_step("scanning_series", 2, "Scanning TV series")
        series_dir = Path(self.paths["series"])

        categories = self.client.get_series_categories()
        cat_map = {str(c.get("category_id") or ""): c.get("category_name", "") for c in categories}
        logger.info(f"Found {len(categories)} series categories")

        all_series = self.client.get_series()
        stats.total_series = len(all_series)
        logger.info(f"Found {len(all_series)} total series from provider")

        # Build dedup maps for series
        seen_series_title = {}  # normalized_title -> (series_id, quality)
        seen_series_tmdb = {}   # tmdb_id -> (series_id, quality)
        for sid, info in self.state.get("series", {}).items():
            t = info.get("title", "")
            y = info.get("year", "")
            q = info.get("quality", 720)
            nkey = normalize_for_dedup(t, y)
            if nkey:
                seen_series_title[nkey] = (sid, q)
            tid = info.get("tmdb_id")
            if tid:
                seen_series_tmdb[str(tid)] = (sid, q)

        series_processed = 0
        for series in all_series:
            if self.stop_requested:
                logger.info("Stop requested, halting series scan")
                break
            series_processed += 1
            if series_processed % 50 == 0:
                self._set_progress("scanning_series", 2, "Scanning TV series", series_processed, len(all_series), f"{stats.new_episodes} eps, {stats.dupes_skipped} dupes")

            series_id = str(series.get("series_id", ""))
            name = series.get("name", "Unknown")
            cat_id = str(series.get("category_id", "") or "")
            cat_name = cat_map.get(cat_id, "")

            if not self._should_include(cat_id, self.series_cat_ids, name, cat_name):
                stats.skipped_filtered += 1
                continue

            # Anime filter
            if self.exclude_anime and self._is_anime(name, cat_name):
                stats.skipped_filtered += 1
                continue

            # Already fully processed (exact series_id)?
            if series_id in self.state["series"]:
                existing = self.state["series"][series_id]
                if existing.get("fully_scanned"):
                    stats.skipped_existing += 1
                    continue

            # Early dedup check before expensive API call
            pre_title = clean_title(name)
            pre_year = extract_year(name) or ""
            pre_quality = detect_quality(name, cat_name)
            pre_norm = normalize_for_dedup(pre_title, pre_year)

            # Check by normalized title
            is_dupe = False
            if pre_norm in seen_series_title:
                existing_sid, existing_quality = seen_series_title[pre_norm]
                if pre_quality <= existing_quality:
                    stats.dupes_skipped += 1
                    stats.skipped_filtered += 1
                    continue
                is_dupe = True  # Higher quality, will replace

            # Also check without year
            if not is_dupe and pre_year:
                pre_norm_ny = normalize_for_dedup(pre_title, "")
                if pre_norm_ny in seen_series_title:
                    existing_sid, existing_quality = seen_series_title[pre_norm_ny]
                    if pre_quality <= existing_quality:
                        stats.dupes_skipped += 1
                        stats.skipped_filtered += 1
                        continue

            # Get series details (seasons & episodes)
            try:
                info = self.client.get_series_info(series_id)
                if not info:
                    stats.failed += 1
                    stats.errors.append(f"Failed to get series info: {name}")
                    continue
            except Exception as e:
                logger.warning(f"Exception fetching series '{name}': {e}")
                stats.failed += 1
                continue

            try:
                series_info = info.get("info", {}) or {}
                episodes_raw = info.get("episodes", {})

                # Normalize episodes data - providers return this in different formats:
                #   - dict keyed by season number: {"1": [...], "2": [...]}
                #   - list of episodes (flat): [{...}, {...}]
                #   - None or empty
                episodes_data = {}
                if isinstance(episodes_raw, dict):
                    episodes_data = episodes_raw
                elif isinstance(episodes_raw, list):
                    # Flat list of episodes - group them by season
                    for ep in episodes_raw:
                        if not isinstance(ep, dict):
                            continue
                        sn = str(ep.get("season", ep.get("season_num", 1)))
                        episodes_data.setdefault(sn, []).append(ep)
                # else: skip - no usable episode data

                platform = detect_platform_from_name(name)
                title = clean_title(name)
                try:
                    year = extract_year(name) or (series_info.get("releaseDate", "")[:4] if series_info.get("releaseDate") else "")
                except Exception:
                    year = ""
                quality = detect_quality(name, cat_name)
                safe_title = sanitize_filename(title)
                if year:
                    show_folder = f"{safe_title} ({year})"
                else:
                    show_folder = safe_title

                # Final dedup check with TMDB ID from series_info
                tmdb_id = series_info.get("tmdb_id") or series.get("tmdb_id")
                norm_key = normalize_for_dedup(title, year)

                if tmdb_id and str(tmdb_id) in seen_series_tmdb:
                    existing_sid, existing_quality = seen_series_tmdb[str(tmdb_id)]
                    if quality <= existing_quality:
                        stats.dupes_skipped += 1
                        stats.skipped_filtered += 1
                        continue

                episode_count = 0
                for season_num, episodes in episodes_data.items():
                    if not isinstance(episodes, list):
                        continue

                    try:
                        season_int = int(season_num)
                    except (ValueError, TypeError):
                        season_int = 0
                    season_folder = f"Season {season_int:02d}"

                    for ep in episodes:
                        if not isinstance(ep, dict):
                            continue

                        ep_id = str(ep.get("id", ep.get("stream_id", "")))
                        if not ep_id:
                            continue

                        ep_num = ep.get("episode_num", ep.get("episode", 0))
                        ep_title = ep.get("title", ep.get("name", f"Episode {ep_num}"))
                        ep_ext = ep.get("container_extension", "ts")

                        # Skip already processed episodes
                        if ep_id in self.state["episodes"]:
                            stats.skipped_existing += 1
                            continue

                        try:
                            stream_url = self.client.build_series_url(int(ep_id), ep_ext)
                        except (ValueError, TypeError):
                            stats.failed += 1
                            continue

                        # Build filename: S01E01 - Episode Title.strm
                        try:
                            ep_num_int = int(ep_num)
                        except (ValueError, TypeError):
                            ep_num_int = 0
                        ep_filename = sanitize_filename(
                            f"S{season_int:02d}E{ep_num_int:02d} - {ep_title}"
                        )
                        strm_path = series_dir / show_folder / season_folder / f"{ep_filename}.strm"

                        if self._write_strm(strm_path, stream_url):
                            stats.new_episodes += 1
                            episode_count += 1
                            self.state["episodes"][ep_id] = {
                                "series_id": series_id,
                                "series_name": name,
                                "season": season_num,
                                "episode": ep_num,
                                "title": ep_title,
                                "stream_url": stream_url,
                                "strm_path": str(strm_path),
                                "added_at": datetime.now(timezone.utc).isoformat(),
                            }
                        else:
                            stats.failed += 1

                stats.total_episodes += episode_count

                # Mark series as processed and update dedup maps
                if norm_key:
                    seen_series_title[norm_key] = (series_id, quality)
                    norm_ny = normalize_for_dedup(title, "")
                    if norm_ny and norm_ny != norm_key:
                        seen_series_title[norm_ny] = (series_id, quality)
                if tmdb_id:
                    seen_series_tmdb[str(tmdb_id)] = (series_id, quality)
                self.state["series"][series_id] = {
                    "name": name,
                    "title": title,
                    "year": year,
                    "quality": quality,
                    "category": cat_name,
                    "platform": platform,
                    "tags": self.category_tags.get(cat_id, []),
                    "episode_count": episode_count,
                    "tmdb_id": tmdb_id,
                    "cover": series_info.get("cover", ""),
                    "stream_icon": series_info.get("cover", ""),
                    "added_at": datetime.now(timezone.utc).isoformat(),
                    "fully_scanned": True,
                    "strm_dir": str(series_dir / show_folder),
                }

            except Exception as e:
                logger.warning(f"Error processing series '{name}' (id={series_id}): {e}")
                stats.failed += 1
                stats.errors.append(f"Series error: {name} - {e}")
                continue

            if len(self.state["series"]) % 50 == 0:
                logger.info(f"  ... processed {len(self.state['series'])} series so far")
                # Save state periodically
                self._save_state()

        logger.info(
            f"Series done: {stats.new_episodes} new episodes across "
            f"{stats.total_series} series"
        )

    # -------------------------------------------------------- live TV scan
    def scan_live_channels(self, stats: ScanStats) -> None:
        """Scan live TV channels and build a filtered M3U playlist + EPG."""
        logger.info("=== Scanning Live TV Channels ===")
        self._start_step("scanning_live", 3, "Scanning live TV")
        live_dir = Path(self.paths["live_tv"])
        live_dir.mkdir(parents=True, exist_ok=True)

        categories = self.client.get_live_categories()
        cat_map = {str(c.get("category_id") or ""): c.get("category_name", "") for c in categories}

        all_channels = self.client.get_live_streams()
        stats.total_live = len(all_channels)
        logger.info(f"Found {len(all_channels)} total live channels")

        filtered_channels = []
        for ch in all_channels:
            name = ch.get("name", "")
            cat_id = str(ch.get("category_id", "") or "")
            cat_name = cat_map.get(cat_id, "")

            if self._should_include(cat_id, self.live_cat_ids, name, cat_name):
                ch["_category_name"] = cat_name
                filtered_channels.append(ch)
            else:
                stats.skipped_filtered += 1

        logger.info(f"After filtering: {len(filtered_channels)} channels kept")

        # Write filtered M3U playlist
        m3u_path = live_dir / "iptv_channels.m3u"
        with open(m3u_path, "w", encoding="utf-8") as f:
            f.write('#EXTM3U\n')
            for ch in filtered_channels:
                stream_id = ch.get("stream_id", "")
                name = ch.get("name", "Unknown")
                logo = ch.get("stream_icon", "")
                group = ch.get("_category_name", "")
                epg_id = ch.get("epg_channel_id", "")

                f.write(
                    f'#EXTINF:-1 tvg-id="{epg_id}" '
                    f'tvg-name="{name}" '
                    f'tvg-logo="{logo}" '
                    f'group-title="{group}",{name}\n'
                )
                f.write(self.client.build_live_url(int(stream_id)) + '\n')

                stats.new_channels += 1
                self.state["channels"][str(stream_id)] = {
                    "name": name,
                    "category": group,
                    "epg_id": epg_id,
                }

        logger.info(f"Wrote {len(filtered_channels)} channels to {m3u_path}")
        self.state["live_channel_count"] = len(filtered_channels)

        # Download and filter EPG
        self._generate_epg(filtered_channels, live_dir, stats)

    def _generate_epg(self, channels: list, live_dir: Path, stats: ScanStats) -> None:
        """Download the provider's EPG, filter to our channels only, and save."""
        logger.info("Downloading EPG data...")
        epg_url = self.client.get_epg_url()

        # Collect all tvg-ids from our filtered channels
        wanted_ids: set[str] = set()
        for ch in channels:
            eid = ch.get("epg_channel_id", "")
            if eid:
                wanted_ids.add(eid)
        logger.info(f"EPG: filtering for {len(wanted_ids)} channel IDs")

        raw_path = live_dir / "epg_raw.xml"
        epg_path = live_dir / "epg.xml"

        try:
            resp = self.client.session.get(epg_url, timeout=120, stream=True)
            resp.raise_for_status()

            with open(raw_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)

            file_size = raw_path.stat().st_size / (1024 * 1024)
            logger.info(f"EPG downloaded: {file_size:.1f} MB — filtering...")

            if wanted_ids:
                self._filter_epg(raw_path, epg_path, wanted_ids)
                filtered_size = epg_path.stat().st_size / (1024 * 1024)
                logger.info(f"EPG filtered: {filtered_size:.1f} MB -> {epg_path}")
            else:
                # No IDs to filter on — just use raw
                shutil.copy2(raw_path, epg_path)
                logger.info("EPG: no channel IDs to filter on, using raw")

            # Remove temp raw file
            try:
                raw_path.unlink()
            except Exception:
                pass

        except Exception as e:
            logger.error(f"Failed to download EPG: {e}")
            stats.errors.append(f"EPG download failed: {e}")

    @staticmethod
    def _filter_epg(src: Path, dst: Path, wanted_ids: set) -> None:
        """Stream-parse an XMLTV file and write only elements for wanted channel IDs.
        Handles files of any size without loading them fully into memory."""
        import xml.etree.ElementTree as ET

        with open(dst, "w", encoding="utf-8") as out:
            out.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            out.write('<tv>\n')

            # iterparse so we never hold the whole tree in memory
            context = ET.iterparse(str(src), events=("end",))
            for event, elem in context:
                if elem.tag == "channel":
                    cid = elem.get("id", "")
                    if cid in wanted_ids:
                        out.write(ET.tostring(elem, encoding="unicode"))
                        out.write("\n")
                    elem.clear()
                elif elem.tag == "programme":
                    cid = elem.get("channel", "")
                    if cid in wanted_ids:
                        out.write(ET.tostring(elem, encoding="unicode"))
                        out.write("\n")
                    elem.clear()

            out.write('</tv>\n')

    # ---------------------------------------------------------- full scan
    def switch_provider(self, server: str, username: str, password: str, output_format: str = "ts"):
        """Switch to a different IPTV provider without losing state."""
        self.client = XtreamClient(
            server=server, username=username, password=password,
            output_format=output_format,
        )
        logger.info(f"Switched provider to {server} (user: {username})")

    def run_full_scan(self) -> ScanStats:
        """Execute a complete scan of all content types."""
        stats = ScanStats()
        self.stop_requested = False
        logger.info("=" * 60)
        logger.info("Starting FULL IPTV scan")
        logger.info("=" * 60)

        self._start_step("authenticating", 0, "Authenticating with provider")
        if not self.client.authenticate():
            stats.errors.append("Authentication failed")
            stats.finished_at = datetime.now(timezone.utc)
            self._set_progress("done", 5, "Failed - auth error")
            return stats

        try:
            if not self.stop_requested:
                self.scan_movies(stats)
                self._save_state()

            if not self.stop_requested:
                self.scan_series(stats)
                self._save_state()

            if not self.stop_requested:
                self.scan_live_channels(stats)
                self._save_state()

        except Exception as e:
            logger.exception(f"Scan error: {e}")
            stats.errors.append(str(e))
            stats.failed += 1

        stats.finished_at = datetime.now(timezone.utc)

        # Final dedup sweep
        if not self.stop_requested:
            dedup_results = self.dedup_sweep(stats)
            self._last_dedup_results = dedup_results
        else:
            self._last_dedup_results = {"movies": 0, "series": 0}

        self._save_history(stats)

        stopped_msg = " (stopped early)" if self.stop_requested else ""
        self._set_progress("done", 5, f"Complete{stopped_msg}", details=f"{stats.new_movies} movies, {stats.new_episodes} episodes, {stats.new_channels} channels")

        duration = (stats.finished_at - stats.started_at).total_seconds()
        logger.info("=" * 60)
        logger.info(f"Scan complete in {duration:.0f}s{stopped_msg}")
        logger.info(f"  Movies: {stats.new_movies} new / {stats.total_vod} total from provider")
        logger.info(f"  Episodes: {stats.new_episodes} new / {stats.total_episodes} processed")
        logger.info(f"  Channels: {stats.new_channels}")
        logger.info(f"  Skipped (existing): {stats.skipped_existing}")
        logger.info(f"  Skipped (filtered): {stats.skipped_filtered}")
        logger.info(f"  Failures: {stats.failed}")
        logger.info("=" * 60)

        return stats

    # ----------------------------------------------------- cleanup
    def cleanup_removed(self) -> dict:
        """Remove .strm files for content no longer on the provider."""
        logger.info("Checking for removed content...")
        removed = {"movies": 0, "episodes": 0}

        # Check movies
        current_vod = self.client.get_vod_streams()
        current_vod_ids = {str(v.get("stream_id")) for v in current_vod}

        for sid, info in list(self.state["movies"].items()):
            if sid not in current_vod_ids:
                strm_path = Path(info.get("strm_path", ""))
                if strm_path.exists():
                    strm_path.unlink()
                    # Try to remove empty parent directories
                    try:
                        strm_path.parent.rmdir()
                        strm_path.parent.parent.rmdir()
                    except OSError:
                        pass
                del self.state["movies"][sid]
                removed["movies"] += 1

        self._save_state()
        logger.info(f"Removed {removed['movies']} movies, {removed['episodes']} episodes")
        return removed

    def backfill_clean_titles(self) -> int:
        """Rename on-disk folders/files where stored titles still carry platform prefixes.
        Updates state title, strm_path, and strm_dir so dedup_sweep can match old+new entries."""
        changed = 0

        movies_dir = Path(self.paths.get("movies", ""))
        # --- Movies ---
        for sid, info in list(self.state.get("movies", {}).items()):
            # Backfill platform tag for existing entries that predate this feature
            if not info.get("platform"):
                orig = info.get("name", "")
                if orig:
                    p = detect_platform_from_name(orig)
                    if p:
                        info["platform"] = p
                        changed += 1

            stored_title = info.get("title", "")
            clean = clean_title(stored_title)
            if clean == stored_title:
                continue  # already clean

            strm_path = Path(info.get("strm_path", ""))
            if not strm_path.exists():
                # Just update state, disk already gone/renamed
                info["title"] = clean
                changed += 1
                continue

            year = info.get("year", "")
            safe = sanitize_filename(clean)
            new_folder_name = f"{safe} ({year})" if year else safe
            old_folder = strm_path.parent
            new_folder = old_folder.parent / new_folder_name

            if old_folder == new_folder:
                info["title"] = clean
                changed += 1
                continue

            try:
                if new_folder.exists():
                    # Collision – just update state, leave files alone
                    info["title"] = clean
                    changed += 1
                    continue
                old_folder.rename(new_folder)
                # Update all files inside (rename strm and nfo to match new folder name)
                for f in list(new_folder.iterdir()):
                    stem_clean = f.stem  # old stem — may still carry dirty name
                    new_stem = new_folder_name
                    if f.suffix in (".strm", ".nfo") and stem_clean != new_stem:
                        f.rename(new_folder / f"{new_stem}{f.suffix}")
                new_strm = new_folder / f"{new_folder_name}.strm"
                info["title"] = clean
                info["strm_path"] = str(new_strm)
                changed += 1
                logger.debug(f"Backfill movie: '{stored_title}' → '{clean}'")
            except Exception as e:
                logger.warning(f"Backfill rename failed for movie {sid}: {e}")

        # --- Series ---
        # Build episode index keyed by series_id
        eps_by_series: dict[str, list[str]] = {}
        for ep_id, ep_info in self.state.get("episodes", {}).items():
            s_id = ep_info.get("series_id", "")
            if s_id:
                eps_by_series.setdefault(s_id, []).append(ep_id)

        series_dir = Path(self.paths.get("series", ""))
        for sid, info in list(self.state.get("series", {}).items()):
            # Backfill platform tag for existing entries that predate this feature
            if not info.get("platform"):
                orig = info.get("name", "")
                if orig:
                    p = detect_platform_from_name(orig)
                    if p:
                        info["platform"] = p
                        changed += 1

            stored_title = info.get("title", "")
            clean = clean_title(stored_title)
            if clean == stored_title:
                continue

            year = info.get("year", "")
            safe = sanitize_filename(clean)
            new_folder_name = f"{safe} ({year})" if year else safe

            stored_dir = info.get("strm_dir", "")
            old_folder = Path(stored_dir) if stored_dir else None

            if old_folder and old_folder.exists():
                new_folder = old_folder.parent / new_folder_name
                if old_folder != new_folder:
                    try:
                        if not new_folder.exists():
                            old_folder.rename(new_folder)
                            # Update episode strm_paths
                            old_str = str(old_folder)
                            new_str = str(new_folder)
                            for ep_id in eps_by_series.get(sid, []):
                                ep_info = self.state.get("episodes", {}).get(ep_id)
                                if ep_info:
                                    ep_path = ep_info.get("strm_path", "")
                                    if ep_path.startswith(old_str):
                                        ep_info["strm_path"] = new_str + ep_path[len(old_str):]
                            info["strm_dir"] = new_str
                    except Exception as e:
                        logger.warning(f"Backfill rename failed for series {sid}: {e}")

            info["title"] = clean
            changed += 1
            logger.debug(f"Backfill series: '{stored_title}' → '{clean}'")

        if changed:
            self._save_state()
            logger.info(f"Backfill clean titles: updated {changed} entries")
        return changed

    def dedup_sweep(self, stats: ScanStats = None) -> dict:
        """Post-scan sweep: find and remove any remaining duplicates in library.
        Uses aggressive normalization + TMDB ID matching. Keeps highest quality."""
        logger.info("=== Running post-scan dedup sweep ===")
        # First pass: rename any on-disk folders that still have platform prefixes
        self.backfill_clean_titles()
        removed = {"movies": 0, "series": 0}

        # ------------------------------------------------------------------ helpers
        def _remove_movie(sid: str) -> None:
            info = self.state["movies"].get(sid)
            if not info:
                return
            folder = Path(info.get("strm_path", "")).parent
            if folder.exists():
                shutil.rmtree(folder, ignore_errors=True)
            del self.state["movies"][sid]

        def _remove_series(sid: str, episodes_by_series: dict) -> None:
            info = self.state["series"].get(sid)
            if not info:
                return
            # Delete all episode .strm files and their state entries
            series_folder = None
            for ep_id in episodes_by_series.get(sid, []):
                ep_info = self.state.get("episodes", {}).get(ep_id)
                if ep_info:
                    ep_path = Path(ep_info.get("strm_path", ""))
                    if ep_path.exists():
                        ep_path.unlink()
                    if series_folder is None:
                        series_folder = ep_path.parent.parent
                    self.state["episodes"].pop(ep_id, None)
            # Use stored strm_dir as authoritative source (new entries), fall back to episode-derived path
            stored_dir = info.get("strm_dir")
            if stored_dir:
                series_folder = Path(stored_dir)
            if series_folder and series_folder.exists():
                shutil.rmtree(series_folder, ignore_errors=True)
            del self.state["series"][sid]

        # --- Movies dedup sweep ---
        title_map = {}  # normalized_title -> [(sid, quality, title, year)]
        tmdb_map = {}   # tmdb_id -> [(sid, quality)]

        for sid, info in list(self.state.get("movies", {}).items()):
            t = clean_title(info.get("title", ""))  # normalize through clean_title so old dirty titles match
            y = info.get("year", "")
            q = info.get("quality", 720)
            tid = info.get("tmdb_id")
            nkey = normalize_for_dedup(t, y)
            nkey_ny = normalize_for_dedup(t, "")  # no-year variant
            if nkey:
                title_map.setdefault(nkey, []).append((sid, q, t, y))
            # Index without year separately to catch year-mismatch dupes
            if nkey_ny and nkey_ny != nkey:
                title_map.setdefault(nkey_ny, []).append((sid, q, t, y))
            if tid:
                tmdb_map.setdefault(str(tid), []).append((sid, q))

        # Remove title-based duplicates (with-year and no-year groups)
        for nkey, entries in title_map.items():
            if len(entries) <= 1:
                continue
            entries.sort(key=lambda x: x[1], reverse=True)
            keeper = entries[0]
            for sid, q, t, y in entries[1:]:
                if sid in self.state.get("movies", {}):
                    _remove_movie(sid)
                    removed["movies"] += 1
                    logger.debug(f"  Dedup sweep removed movie: {t} ({y}) [{q}p] — kept [{keeper[1]}p]")

        # Remove TMDB-ID-based duplicates (catches title variants)
        for tid, entries in tmdb_map.items():
            if len(entries) <= 1:
                continue
            live = [(sid, q) for sid, q in entries if sid in self.state.get("movies", {})]
            if len(live) <= 1:
                continue
            live.sort(key=lambda x: x[1], reverse=True)
            for sid, q in live[1:]:
                if sid in self.state.get("movies", {}):
                    info = self.state["movies"].get(sid, {})
                    _remove_movie(sid)
                    removed["movies"] += 1
                    logger.debug(f"  Dedup sweep removed movie by TMDB ID {tid}: {info.get('title', '')} [{q}p]")

        # --- Series dedup sweep ---
        # Build episode index keyed by series_id for efficient cleanup
        episodes_by_series: dict[str, list[str]] = {}
        for ep_id, ep_info in self.state.get("episodes", {}).items():
            s_id = ep_info.get("series_id", "")
            if s_id:
                episodes_by_series.setdefault(s_id, []).append(ep_id)

        series_title_map = {}
        series_tmdb_map = {}
        for sid, info in list(self.state.get("series", {}).items()):
            t = clean_title(info.get("title", ""))  # normalize through clean_title so old dirty titles match
            y = info.get("year", "")
            q = info.get("quality", 720)
            tid = info.get("tmdb_id")
            nkey = normalize_for_dedup(t, y)
            nkey_ny = normalize_for_dedup(t, "")
            if nkey:
                series_title_map.setdefault(nkey, []).append((sid, q, t, y))
            if nkey_ny and nkey_ny != nkey:
                series_title_map.setdefault(nkey_ny, []).append((sid, q, t, y))
            if tid:
                series_tmdb_map.setdefault(str(tid), []).append((sid, q))

        for nkey, entries in series_title_map.items():
            if len(entries) <= 1:
                continue
            entries.sort(key=lambda x: x[1], reverse=True)
            for sid, q, t, y in entries[1:]:
                if sid in self.state.get("series", {}):
                    _remove_series(sid, episodes_by_series)
                    removed["series"] += 1
                    logger.debug(f"  Dedup sweep removed series: {t} ({y}) [{q}p]")

        for tid, entries in series_tmdb_map.items():
            if len(entries) <= 1:
                continue
            live = [(sid, q) for sid, q in entries if sid in self.state.get("series", {})]
            if len(live) <= 1:
                continue
            live.sort(key=lambda x: x[1], reverse=True)
            for sid, q in live[1:]:
                if sid in self.state.get("series", {}):
                    _remove_series(sid, episodes_by_series)
                    removed["series"] += 1

        if removed["movies"] or removed["series"]:
            self._save_state()
            total_removed = removed["movies"] + removed["series"]
            if stats:
                stats.dupes_sweep += total_removed
            logger.info(f"Dedup sweep removed: {removed['movies']} movies, {removed['series']} series")
        else:
            logger.info("Dedup sweep: no duplicates found")

        return removed

    def get_library_stats(self) -> dict:
        """Return current library statistics including enrichment match rate."""
        movies = self.state.get("movies", {})
        series = self.state.get("series", {})

        # Count total episodes from series entries
        total_episodes = sum(s.get("episode_count", 0) for s in series.values())

        # Count live channels from state or M3U file
        live_count = self.state.get("live_channel_count", 0)
        if live_count == 0:
            m3u_path = Path(self.paths.get("live_tv", "")) / "iptv_channels.m3u"
            if m3u_path.exists():
                try:
                    with open(m3u_path, "r", errors="replace") as f:
                        live_count = sum(1 for line in f if line.strip() and not line.startswith("#"))
                except Exception:
                    pass

        # Use cached enrichment counts if fresh (avoids repeated disk hits every poll)
        now = time.time()
        cache = getattr(self, "_enrichment_cache", None)
        if cache and now - cache["ts"] < 30:
            movies_with_tmdb = cache["movies"]
            series_with_tmdb = cache["series"]
        else:
            # Count enriched: TMDB ID in state OR a .nfo sidecar exists on disk
            # (covers items enriched before the tmdb_id state-writeback fix)
            def _has_tmdb(item: dict) -> bool:
                tid = item.get("tmdb_id")
                if tid and str(tid) not in ("0", "", "None"):
                    return True
                sp = item.get("strm_path", "")
                return bool(sp) and Path(sp).with_suffix(".nfo").exists()

            def _series_has_tmdb(item: dict) -> bool:
                tid = item.get("tmdb_id")
                if tid and str(tid) not in ("0", "", "None"):
                    return True
                sd = item.get("strm_dir", "")
                return bool(sd) and (Path(sd) / "tvshow.nfo").exists()

            movies_with_tmdb = sum(1 for m in movies.values() if _has_tmdb(m))
            series_with_tmdb = sum(1 for s in series.values() if _series_has_tmdb(s))
            self._enrichment_cache = {"ts": now, "movies": movies_with_tmdb, "series": series_with_tmdb}

        return {
            "movies": len(movies),
            "series": len(series),
            "episodes": total_episodes,
            "live_channels": live_count,
            "movies_with_tmdb": movies_with_tmdb,
            "series_with_tmdb": series_with_tmdb,
        }

    def rewrite_credentials(self, old_server: str, old_user: str, old_pass: str,
                            new_server: str, new_user: str, new_pass: str) -> dict:
        """Rewrite all .strm file URLs with new provider credentials without rescanning.
        Also updates the M3U file for live TV."""
        results = {"movies_updated": 0, "episodes_updated": 0, "live_updated": False, "errors": []}

        # Pattern: {server}/{type}/{user}/{pass}/{id}.{ext}
        old_pattern = re.escape(old_server) + r'/(movie|series|live)/' + re.escape(old_user) + '/' + re.escape(old_pass) + '/'
        new_repl_fn = lambda m: f"{new_server}/{m.group(1)}/{new_user}/{new_pass}/"

        # Rewrite movie .strm files
        for sid, info in self.state.get("movies", {}).items():
            strm_path = Path(info.get("strm_path", ""))
            if strm_path.exists():
                try:
                    content = strm_path.read_text()
                    new_content = re.sub(old_pattern, new_repl_fn, content)
                    if new_content != content:
                        strm_path.write_text(new_content)
                        info["stream_url"] = new_content.strip()
                        results["movies_updated"] += 1
                except Exception as e:
                    results["errors"].append(f"Movie {sid}: {e}")

        # Rewrite series episode .strm files
        for sid, info in self.state.get("series", {}).items():
            show_folder = info.get("title", "")
            year = info.get("year", "")
            safe_title = sanitize_filename(show_folder)
            folder_name = f"{safe_title} ({year})" if year else safe_title
            series_dir = Path(self.paths["series"]) / folder_name
            if series_dir.exists():
                for strm_file in series_dir.rglob("*.strm"):
                    try:
                        content = strm_file.read_text()
                        new_content = re.sub(old_pattern, new_repl_fn, content)
                        if new_content != content:
                            strm_file.write_text(new_content)
                            results["episodes_updated"] += 1
                    except Exception as e:
                        results["errors"].append(f"Episode {strm_file}: {e}")

        # Rewrite M3U file
        m3u_path = Path(self.paths.get("live_tv", "")) / "iptv_channels.m3u"
        if m3u_path.exists():
            try:
                content = m3u_path.read_text(errors="replace")
                new_content = _re.sub(old_pattern, new_repl_fn, content)
                if new_content != content:
                    m3u_path.write_text(new_content)
                    results["live_updated"] = True
            except Exception as e:
                results["errors"].append(f"M3U: {e}")

        # Update client credentials
        self.client.server = new_server
        self.client.username = new_user
        self.client.password = new_pass

        self._save_state()
        logger.info(f"Credentials rewritten: {results['movies_updated']} movies, {results['episodes_updated']} episodes, live={'yes' if results['live_updated'] else 'no'}")
        return results
