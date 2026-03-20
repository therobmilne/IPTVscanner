"""
TMDB Enricher
Matches provider content against TMDB for metadata enrichment.
Creates Jellyfin-compatible collections (Trending, Recently Released, Top Rated, By Genre, etc.)
"""

import json
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import requests

from .scanner import sanitize_filename

logger = logging.getLogger(__name__)

# TMDB API base
TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMG_BASE = "https://image.tmdb.org/t/p"


class TMDBEnricher:
    """Enrich IPTV content with TMDB metadata and create Jellyfin collections."""

    def __init__(self, config: dict):
        tmdb_cfg = config.get("tmdb", {})
        self.api_key = tmdb_cfg.get("api_key", "")
        self.language = tmdb_cfg.get("language", "en-US")
        self.enabled = tmdb_cfg.get("enabled", True) and self.api_key and self.api_key != "YOUR_TMDB_API_KEY_HERE"

        self.paths = config["paths"]
        self.data_dir = Path(self.paths.get("data_dir", "./data"))
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # Cache file for TMDB lookups
        self.cache_file = self.data_dir / "tmdb_cache.json"
        self.cache = self._load_cache()

        # Rate limiting: TMDB allows ~40 requests/10 seconds
        self._last_request = 0
        self._request_count = 0

    def _load_cache(self) -> dict:
        if self.cache_file.exists():
            try:
                with open(self.cache_file) as f:
                    return json.load(f)
            except Exception:
                pass
        return {"movies": {}, "series": {}, "genres": {}}

    def _save_cache(self):
        with open(self.cache_file, "w") as f:
            json.dump(self.cache, f, indent=2)

    def _tmdb_request(self, endpoint: str, params: dict = None) -> dict | None:
        """Make a rate-limited request to TMDB API."""
        if not self.api_key:
            return None

        # Simple rate limiting
        self._request_count += 1
        if self._request_count % 35 == 0:
            elapsed = time.time() - self._last_request
            if elapsed < 10:
                time.sleep(10 - elapsed)

        self._last_request = time.time()

        all_params = {"api_key": self.api_key, "language": self.language}
        if params:
            all_params.update(params)

        try:
            resp = requests.get(
                f"{TMDB_BASE}{endpoint}",
                params=all_params,
                timeout=15,
            )
            if resp.status_code == 429:
                # Rate limited, wait and retry
                retry_after = int(resp.headers.get("Retry-After", 5))
                logger.warning(f"TMDB rate limited, waiting {retry_after}s...")
                time.sleep(retry_after)
                return self._tmdb_request(endpoint, params)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"TMDB request failed: {endpoint} - {e}")
            return None

    # ----------------------------------------------------------- search
    def search_movie(self, title: str, year: str = None) -> dict | None:
        """Search TMDB for a movie by title (and optionally year)."""
        cache_key = f"{title}|{year or ''}"
        if cache_key in self.cache["movies"]:
            return self.cache["movies"][cache_key]

        params = {"query": title}
        if year:
            params["year"] = year

        data = self._tmdb_request("/search/movie", params)
        if data and data.get("results"):
            result = data["results"][0]  # Best match
            info = {
                "tmdb_id": result["id"],
                "title": result.get("title", title),
                "year": result.get("release_date", "")[:4],
                "overview": result.get("overview", ""),
                "poster": result.get("poster_path", ""),
                "backdrop": result.get("backdrop_path", ""),
                "genre_ids": result.get("genre_ids", []),
                "vote_average": result.get("vote_average", 0),
                "popularity": result.get("popularity", 0),
                "original_language": result.get("original_language", ""),
            }
            self.cache["movies"][cache_key] = info
            return info

        self.cache["movies"][cache_key] = None
        return None

    def search_tv(self, title: str, year: str = None) -> dict | None:
        """Search TMDB for a TV series."""
        cache_key = f"tv|{title}|{year or ''}"
        if cache_key in self.cache["series"]:
            return self.cache["series"][cache_key]

        params = {"query": title}
        if year:
            params["first_air_date_year"] = year

        data = self._tmdb_request("/search/tv", params)
        if data and data.get("results"):
            result = data["results"][0]
            info = {
                "tmdb_id": result["id"],
                "title": result.get("name", title),
                "year": result.get("first_air_date", "")[:4],
                "overview": result.get("overview", ""),
                "poster": result.get("poster_path", ""),
                "backdrop": result.get("backdrop_path", ""),
                "genre_ids": result.get("genre_ids", []),
                "vote_average": result.get("vote_average", 0),
                "popularity": result.get("popularity", 0),
            }
            self.cache["series"][cache_key] = info
            return info

        self.cache["series"][cache_key] = None
        return None

    # ------------------------------------------------- trending / discover
    def get_trending_movies(self, time_window: str = "week") -> list:
        """Get trending movies from TMDB."""
        data = self._tmdb_request(f"/trending/movie/{time_window}")
        return data.get("results", []) if data else []

    def get_trending_tv(self, time_window: str = "week") -> list:
        """Get trending TV shows from TMDB."""
        data = self._tmdb_request(f"/trending/tv/{time_window}")
        return data.get("results", []) if data else []

    def get_popular_movies(self, page: int = 1) -> list:
        data = self._tmdb_request("/movie/popular", {"page": page})
        return data.get("results", []) if data else []

    def get_top_rated_movies(self, page: int = 1) -> list:
        data = self._tmdb_request("/movie/top_rated", {"page": page})
        return data.get("results", []) if data else []

    def get_now_playing(self, page: int = 1) -> list:
        data = self._tmdb_request("/movie/now_playing", {"page": page})
        return data.get("results", []) if data else []

    def get_upcoming_movies(self, page: int = 1) -> list:
        data = self._tmdb_request("/movie/upcoming", {"page": page})
        return data.get("results", []) if data else []

    def discover_movies(self, **kwargs) -> list:
        """Use TMDB Discover endpoint with custom filters."""
        data = self._tmdb_request("/discover/movie", kwargs)
        return data.get("results", []) if data else []

    def get_genre_list(self) -> dict:
        """Get the TMDB genre ID -> name mapping."""
        if self.cache.get("genres") and len(self.cache["genres"]) > 5:
            return self.cache["genres"]

        data = self._tmdb_request("/genre/movie/list")
        if data:
            genres = {str(g["id"]): g["name"] for g in data.get("genres", [])}
            self.cache["genres"] = genres
            self._save_cache()
            return genres
        return {}

    # -------------------------------------------------------- NFO writer
    def write_movie_nfo(self, movie_info: dict, nfo_path: Path) -> bool:
        """Write a Jellyfin-compatible .nfo metadata file for a movie."""
        try:
            root = ET.Element("movie")
            ET.SubElement(root, "title").text = movie_info.get("title", "")
            ET.SubElement(root, "year").text = str(movie_info.get("year", ""))
            ET.SubElement(root, "plot").text = movie_info.get("overview", "")
            ET.SubElement(root, "rating").text = str(movie_info.get("vote_average", ""))
            ET.SubElement(root, "tmdbid").text = str(movie_info.get("tmdb_id", ""))

            if movie_info.get("poster"):
                thumb = ET.SubElement(root, "thumb", aspect="poster")
                thumb.text = f"{TMDB_IMG_BASE}/w500{movie_info['poster']}"
            if movie_info.get("backdrop"):
                fanart = ET.SubElement(root, "fanart")
                thumb = ET.SubElement(fanart, "thumb")
                thumb.text = f"{TMDB_IMG_BASE}/original{movie_info['backdrop']}"

            genres = self.get_genre_list()
            for gid in movie_info.get("genre_ids", []):
                genre_name = genres.get(str(gid))
                if genre_name:
                    ET.SubElement(root, "genre").text = genre_name

            for tag in movie_info.get("tags", []):
                ET.SubElement(root, "tag").text = str(tag)

            tree = ET.ElementTree(root)
            nfo_path.parent.mkdir(parents=True, exist_ok=True)
            ET.indent(tree, space="  ")
            tree.write(nfo_path, encoding="unicode", xml_declaration=True)
            return True
        except Exception as e:
            logger.error(f"Failed to write NFO: {nfo_path} - {e}")
            return False

    def write_tvshow_nfo(self, show_info: dict, nfo_path: Path) -> bool:
        """Write a tvshow.nfo for Jellyfin."""
        try:
            root = ET.Element("tvshow")
            ET.SubElement(root, "title").text = show_info.get("title", "")
            ET.SubElement(root, "year").text = str(show_info.get("year", ""))
            ET.SubElement(root, "plot").text = show_info.get("overview", "")
            ET.SubElement(root, "rating").text = str(show_info.get("vote_average", ""))
            ET.SubElement(root, "tmdbid").text = str(show_info.get("tmdb_id", ""))

            if show_info.get("poster"):
                thumb = ET.SubElement(root, "thumb", aspect="poster")
                thumb.text = f"{TMDB_IMG_BASE}/w500{show_info['poster']}"

            for tag in show_info.get("tags", []):
                ET.SubElement(root, "tag").text = str(tag)

            tree = ET.ElementTree(root)
            nfo_path.parent.mkdir(parents=True, exist_ok=True)
            ET.indent(tree, space="  ")
            tree.write(nfo_path, encoding="unicode", xml_declaration=True)
            return True
        except Exception as e:
            logger.error(f"Failed to write NFO: {nfo_path} - {e}")
            return False

    # ----------------------------------------- collection builder
    def build_collections(self, scan_state: dict) -> dict:
        """
        Create Jellyfin collection directories.
        Each collection is a folder with symlinks or .strm references to matching movies/series.
        Jellyfin can then scan these as separate libraries or use the collection XML files.
        """
        if not self.enabled:
            logger.info("TMDB not configured, skipping collection build")
            return {}

        logger.info("=== Building Smart Collections ===")
        movies_dir = Path(self.paths["movies"])
        collections_dir = movies_dir.parent / "Collections"
        collections_dir.mkdir(parents=True, exist_ok=True)

        # Get current trending/popular from TMDB
        trending = self.get_trending_movies()
        popular = self.get_popular_movies()
        top_rated = self.get_top_rated_movies()
        now_playing = self.get_now_playing()

        trending_titles = {m.get("title", "").lower() for m in trending}
        popular_titles = {m.get("title", "").lower() for m in popular}
        top_rated_titles = {m.get("title", "").lower() for m in top_rated}
        now_playing_titles = {m.get("title", "").lower() for m in now_playing}

        # Also collect by TMDB ID for more accurate matching
        trending_ids = {str(m.get("id")) for m in trending}
        popular_ids = {str(m.get("id")) for m in popular}

        # Build collection assignments
        collection_map = {
            "Trending This Week": [],
            "Popular Right Now": [],
            "Top Rated": [],
            "In Theaters Now": [],
            "Recently Added": [],
        }

        # Genre collections
        genres = self.get_genre_list()
        year = datetime.now().year
        collection_map[f"Best of {year}"] = []
        collection_map[f"Best of {year - 1}"] = []

        for genre_name in genres.values():
            collection_map[f"Genre: {genre_name}"] = []

        # Match our library against TMDB lists
        all_movies = scan_state.get("movies", {})
        recently_added = sorted(
            all_movies.values(),
            key=lambda x: x.get("added_at", ""),
            reverse=True,
        )[:50]

        for sid, movie in all_movies.items():
            title = movie.get("title", "").lower()
            tmdb_id = str(movie.get("tmdb_id", ""))
            movie_year = movie.get("year", "")
            strm_path = movie.get("strm_path", "")

            if not strm_path:
                continue

            # Check trending
            if tmdb_id in trending_ids or title in trending_titles:
                collection_map["Trending This Week"].append(strm_path)

            # Check popular
            if tmdb_id in popular_ids or title in popular_titles:
                collection_map["Popular Right Now"].append(strm_path)

            # Top rated
            if title in top_rated_titles:
                collection_map["Top Rated"].append(strm_path)

            # Now playing
            if title in now_playing_titles:
                collection_map["In Theaters Now"].append(strm_path)

            # Year-based
            if movie_year == str(year):
                collection_map[f"Best of {year}"].append(strm_path)
            elif movie_year == str(year - 1):
                collection_map[f"Best of {year - 1}"].append(strm_path)

            # Genre-based (if we have TMDB match in cache)
            cache_key = f"{movie.get('title', '')}|{movie_year}"
            cached = self.cache["movies"].get(cache_key)
            if cached and cached.get("genre_ids"):
                for gid in cached["genre_ids"]:
                    genre_name = genres.get(str(gid))
                    if genre_name and f"Genre: {genre_name}" in collection_map:
                        collection_map[f"Genre: {genre_name}"].append(strm_path)

        # Recently added
        for movie in recently_added:
            if movie.get("strm_path"):
                collection_map["Recently Added"].append(movie["strm_path"])

        # Write collection files (Jellyfin uses collection.xml + symlinks)
        stats = {}
        for coll_name, paths in collection_map.items():
            if not paths:
                continue
            coll_dir = collections_dir / sanitize_filename(coll_name)
            coll_dir.mkdir(parents=True, exist_ok=True)

            # Write a manifest of the collection
            manifest = coll_dir / "collection_manifest.json"
            with open(manifest, "w") as f:
                json.dump({"name": coll_name, "items": paths, "count": len(paths)}, f, indent=2)

            # Create symlinks to the actual .strm files
            for strm in paths:
                src = Path(strm)
                if src.exists():
                    link = coll_dir / src.name
                    if not link.exists():
                        try:
                            link.symlink_to(src)
                        except Exception:
                            pass

            stats[coll_name] = len(paths)
            logger.info(f"  Collection '{coll_name}': {len(paths)} items")

        self._save_cache()
        return stats

    def enrich_new_items_only(self, scan_state: dict, scan_stats=None) -> dict:
        """
        Targeted enrichment - only process items added in the current scan.
        Has a 5-minute timeout and stops after 3 consecutive network errors.
        """
        if not self.enabled:
            return {"enriched": 0, "failed": 0, "skipped": 0}

        logger.info("=== Targeted TMDB enrichment (new items only) ===")
        enriched = 0
        failed = 0
        skipped = 0
        consecutive_errors = 0
        max_consecutive_errors = 3
        start_time = time.time()
        max_duration = 300  # 5 minutes max

        # Only process movies that were added recently (within last 2 hours)
        cutoff = (datetime.now(timezone.utc).timestamp()) - 7200

        for sid, movie in scan_state.get("movies", {}).items():
            # Timeout check
            if time.time() - start_time > max_duration:
                logger.warning(f"TMDB enrichment timeout after {max_duration}s — continuing without remaining items")
                break
            if consecutive_errors >= max_consecutive_errors:
                logger.warning(f"TMDB enrichment stopped after {max_consecutive_errors} consecutive network errors")
                break

            added_at = movie.get("added_at", "")
            if not added_at:
                continue
            try:
                item_time = datetime.fromisoformat(added_at).timestamp()
                if item_time < cutoff:
                    continue
            except Exception:
                continue

            strm_path = Path(movie.get("strm_path", ""))
            nfo_path = strm_path.with_suffix(".nfo")
            if nfo_path.exists():
                skipped += 1
                continue

            title = movie.get("title", "")
            year = movie.get("year", "")

            # Skip items with clean naming — Jellyfin matches these fine on its own
            if year and len(title) > 2 and title[0].isalpha():
                tmdb_id = movie.get("tmdb_id")
                if tmdb_id and str(tmdb_id) not in ("0", "", "None"):
                    skipped += 1
                    continue

            tmdb_info = self.search_movie(title, year)
            if tmdb_info:
                tmdb_info["tags"] = movie.get("tags", [])
                if movie.get("platform"):
                    tmdb_info["tags"] = list(tmdb_info["tags"]) + [movie["platform"]]
                if self.write_movie_nfo(tmdb_info, nfo_path):
                    movie["tmdb_id"] = tmdb_info.get("id")
                    if tmdb_info.get("poster"):
                        movie["poster_url"] = f"{TMDB_IMG_BASE}/w300{tmdb_info['poster']}"
                    enriched += 1
                    consecutive_errors = 0  # Reset on success
            elif tmdb_info is None:
                # Network error vs not found
                consecutive_errors += 1
                failed += 1
            else:
                failed += 1
                consecutive_errors = 0

            if enriched % 50 == 0 and enriched > 0:
                logger.info(f"  ... enriched {enriched} new movies ({failed} not found)")
                self._save_cache()

        # Enrich new series (with same timeout/error limits)
        for sid, series_item in scan_state.get("series", {}).items():
            if time.time() - start_time > max_duration:
                break
            if consecutive_errors >= max_consecutive_errors:
                break

            added_at = series_item.get("added_at", "")
            if not added_at:
                continue
            try:
                item_time = datetime.fromisoformat(added_at).timestamp()
                if item_time < cutoff:
                    continue
            except Exception:
                continue

            title = series_item.get("title", "")
            year = series_item.get("year", "")

            # For series, try to update the state with TMDB ID if we find one
            if not series_item.get("tmdb_id") or str(series_item.get("tmdb_id", "")) in ("0", "", "None"):
                tmdb_info = self.search_tv(title, year)
                if tmdb_info:
                    series_item["tmdb_id"] = tmdb_info.get("id")
                    if tmdb_info.get("poster"):
                        series_item["poster_url"] = f"{TMDB_IMG_BASE}/w300{tmdb_info['poster']}"
                    enriched += 1
                    consecutive_errors = 0
                elif tmdb_info is None:
                    consecutive_errors += 1
                    failed += 1
                else:
                    failed += 1
                    consecutive_errors = 0
            else:
                skipped += 1

        self._save_cache()
        elapsed = int(time.time() - start_time)
        logger.info(f"Targeted enrichment done in {elapsed}s: {enriched} enriched, {skipped} skipped (Jellyfin will handle), {failed} not found on TMDB")
        return {"enriched": enriched, "failed": failed, "skipped": skipped}

    def enrich_library(self, scan_state: dict) -> dict:
        """
        Look up each movie/series in our library on TMDB and write NFO files.
        This is the metadata enrichment step that makes Jellyfin display proper info.
        """
        if not self.enabled:
            return {"enriched": 0, "failed": 0}

        logger.info("=== Enriching library with TMDB metadata ===")
        enriched = 0
        failed = 0

        # Enrich movies
        for sid, movie in scan_state.get("movies", {}).items():
            strm_path = Path(movie.get("strm_path", ""))
            nfo_path = strm_path.with_suffix(".nfo")

            if nfo_path.exists():
                continue  # Already enriched

            title = movie.get("title", "")
            year = movie.get("year", "")

            tmdb_info = self.search_movie(title, year)
            if tmdb_info:
                tmdb_info["tags"] = movie.get("tags", [])
                if self.write_movie_nfo(tmdb_info, nfo_path):
                    movie["tmdb_id"] = tmdb_info.get("id")
                    enriched += 1
                else:
                    failed += 1
            else:
                failed += 1

            # Don't spam TMDB
            if enriched % 100 == 0 and enriched > 0:
                logger.info(f"  ... enriched {enriched} movies")
                self._save_cache()

        # Enrich series
        for sid, series in scan_state.get("series", {}).items():
            series_dir = None
            # Find the show directory from any episode
            for eid, ep in scan_state.get("episodes", {}).items():
                if ep.get("series_id") == sid:
                    ep_path = Path(ep.get("strm_path", ""))
                    series_dir = ep_path.parent.parent
                    break

            if not series_dir or not series_dir.exists():
                continue

            nfo_path = series_dir / "tvshow.nfo"
            if nfo_path.exists():
                continue

            title = series.get("title", "")
            year = series.get("year", "")

            tmdb_info = self.search_tv(title, year)
            if tmdb_info:
                tmdb_info["tags"] = series.get("tags", [])
                if self.write_tvshow_nfo(tmdb_info, nfo_path):
                    enriched += 1
            else:
                failed += 1

        self._save_cache()
        logger.info(f"Enrichment done: {enriched} items enriched, {failed} failed/not found")
        return {"enriched": enriched, "failed": failed}
