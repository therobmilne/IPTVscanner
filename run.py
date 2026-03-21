#!/usr/bin/env python3
"""
IPTV Stream Manager - Main Entry Point

Usage:
    python run.py dashboard --port 8888   # Dashboard + scheduler (main way to run)
    python run.py scan                     # One-time scan from terminal
    python run.py proxy --proxy-port 8889  # Standalone restream proxy
    python run.py test                     # Test provider connection
"""

import argparse
import logging
import sys
from pathlib import Path

import yaml

from app.scanner import IPTVScanner, ScanStats
from app.tmdb_enricher import TMDBEnricher
from app.jellyfin_client import JellyfinClient
from app.dashboard import create_app


def setup_logging(config: dict) -> None:
    data_dir = Path(config["paths"].get("data_dir", "./data"))
    data_dir.mkdir(parents=True, exist_ok=True)
    log_file = data_dir / "app.log"
    level = getattr(logging, config.get("logging", {}).get("level", "INFO").upper(), logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)-7s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    fh = logging.FileHandler(log_file, mode="a")
    fh.setLevel(level)
    fh.setFormatter(fmt)
    root.addHandler(fh)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(fmt)
    root.addHandler(ch)


def load_config(config_path: str = "config.yaml") -> dict:
    path = Path(config_path)
    if not path.exists():
        print(f"ERROR: Config file not found: {path}")
        sys.exit(1)
    with open(path) as f:
        config = yaml.safe_load(f)
    config["_config_path"] = str(path.resolve())
    return config


def test_connection(config: dict):
    from app.xtream_client import XtreamClient
    iptv = config["iptv"]
    client = XtreamClient(iptv["server"], iptv["username"], iptv["password"])
    print(f"Connecting to {iptv['server']}...")
    if client.authenticate():
        print(f"OK — VOD: {len(client.get_vod_categories())} cats, Series: {len(client.get_series_categories())} cats, Live: {len(client.get_live_categories())} cats")
    else:
        print("FAILED — check credentials")


def run_scan(config: dict) -> ScanStats:
    scanner = IPTVScanner(config)
    enricher = TMDBEnricher(config)
    jellyfin = JellyfinClient(config)
    logger = logging.getLogger("scan")
    logger.info("=" * 60)
    logger.info("IPTV Stream Manager - Starting Scan")
    logger.info("=" * 60)

    stats = scanner.run_full_scan()

    if enricher.enabled and (stats.new_movies > 0 or stats.new_episodes > 0):
        logger.info(f"Running TMDB enrichment...")
        enricher.enrich_new_items_only(scanner.state, stats)
        enricher.build_collections(scanner.state)

    if jellyfin.auth_enabled and jellyfin.auto_rescan:
        jellyfin.trigger_library_scan()

    return stats


def start_scheduler(config, flask_app):
    """Start scan scheduler using shared scheduler module."""
    from app.scheduler import create_scheduler

    def trigger():
        if not flask_app.scan_running:
            flask_app.run_scan_thread()
        else:
            logging.info("Scan already running, skipping scheduled scan")

    return create_scheduler(config, trigger)


def main():
    parser = argparse.ArgumentParser(description="IPTV Stream Manager")
    parser.add_argument("command", nargs="?", default="dashboard",
                        choices=["start", "scan", "enrich", "collections", "dashboard", "test"])
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--movies", action="store_true")
    parser.add_argument("--series", action="store_true")
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--port", type=int)

    args = parser.parse_args()
    config = load_config(args.config)
    setup_logging(config)
    logger = logging.getLogger("main")
    logger.info("IPTV Stream Manager v1.0.0")

    if args.command == "test":
        test_connection(config)
        return

    if args.command == "scan":
        run_scan(config)
        return

    if args.command == "enrich":
        IPTVScanner(config); TMDBEnricher(config).enrich_library(IPTVScanner(config).state)
        return

    if args.command == "collections":
        sc = IPTVScanner(config); TMDBEnricher(config).build_collections(sc.state)
        return

    # ---- Dashboard + Scheduler (default) ----
    scanner = IPTVScanner(config)
    enricher = TMDBEnricher(config)
    jellyfin = JellyfinClient(config)

    flask_app = create_app(config, scanner=scanner, enricher=enricher, jellyfin=jellyfin)
    port = args.port or config.get("dashboard", {}).get("port", 5000)

    # Start scheduler
    flask_app.scheduler = start_scheduler(config, flask_app)

    logger.info(f"Dashboard at http://0.0.0.0:{port}")
    try:
        flask_app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=True)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        if hasattr(flask_app, 'scheduler') and flask_app.scheduler:
            flask_app.scheduler.shutdown()


if __name__ == "__main__":
    main()
