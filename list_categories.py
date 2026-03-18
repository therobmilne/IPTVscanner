#!/usr/bin/env python3
"""
List all categories from your IPTV provider so you can pick which to include.
Usage: python list_categories.py
"""

import yaml
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from app.xtream_client import XtreamClient

def main():
    with open("config.yaml") as f:
        config = yaml.safe_load(f)

    iptv = config["iptv"]
    client = XtreamClient(
        server=iptv["server"],
        username=iptv["username"],
        password=iptv["password"],
    )

    if not client.authenticate():
        print("Authentication failed!")
        return

    # ---- VOD Categories ----
    print("\n" + "=" * 80)
    print("VOD (MOVIE) CATEGORIES")
    print("=" * 80)
    vod_cats = client.get_vod_categories()
    for i, c in enumerate(vod_cats, 1):
        cat_id = c.get("category_id", "?")
        name = c.get("category_name", "Unknown")
        print(f"  {i:4d}. [{cat_id:>6s}]  {name}")
    print(f"\n  Total: {len(vod_cats)} VOD categories")

    # ---- Series Categories ----
    print("\n" + "=" * 80)
    print("SERIES (TV SHOW) CATEGORIES")
    print("=" * 80)
    series_cats = client.get_series_categories()
    for i, c in enumerate(series_cats, 1):
        cat_id = c.get("category_id", "?")
        name = c.get("category_name", "Unknown")
        print(f"  {i:4d}. [{cat_id:>6s}]  {name}")
    print(f"\n  Total: {len(series_cats)} series categories")

    # ---- Live TV Categories ----
    print("\n" + "=" * 80)
    print("LIVE TV CATEGORIES")
    print("=" * 80)
    live_cats = client.get_live_categories()
    for i, c in enumerate(live_cats, 1):
        cat_id = c.get("category_id", "?")
        name = c.get("category_name", "Unknown")
        print(f"  {i:4d}. [{cat_id:>6s}]  {name}")
    print(f"\n  Total: {len(live_cats)} live TV categories")

    # ---- Save to file for easy review ----
    outfile = "data/all_categories.txt"
    os.makedirs("data", exist_ok=True)
    with open(outfile, "w", encoding="utf-8") as f:
        f.write("VOD (MOVIE) CATEGORIES\n")
        f.write("=" * 80 + "\n")
        for i, c in enumerate(vod_cats, 1):
            f.write(f"{i:4d}. [{c.get('category_id', '?'):>6s}]  {c.get('category_name', 'Unknown')}\n")
        f.write(f"\nTotal: {len(vod_cats)}\n\n")

        f.write("SERIES (TV SHOW) CATEGORIES\n")
        f.write("=" * 80 + "\n")
        for i, c in enumerate(series_cats, 1):
            f.write(f"{i:4d}. [{c.get('category_id', '?'):>6s}]  {c.get('category_name', 'Unknown')}\n")
        f.write(f"\nTotal: {len(series_cats)}\n\n")

        f.write("LIVE TV CATEGORIES\n")
        f.write("=" * 80 + "\n")
        for i, c in enumerate(live_cats, 1):
            f.write(f"{i:4d}. [{c.get('category_id', '?'):>6s}]  {c.get('category_name', 'Unknown')}\n")
        f.write(f"\nTotal: {len(live_cats)}\n")

    print(f"\n*** Full list also saved to: {outfile} ***")
    print("*** Open it in a text editor to review at your own pace ***")

if __name__ == "__main__":
    main()
