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

def _format_section(title, categories):
    """Format a category section as a list of lines."""
    lines = [f"\n{'=' * 80}", title, "=" * 80]
    for i, c in enumerate(categories, 1):
        cat_id = c.get("category_id", "?")
        name = c.get("category_name", "Unknown")
        lines.append(f"  {i:4d}. [{cat_id:>6s}]  {name}")
    lines.append(f"\n  Total: {len(categories)} categories")
    return lines

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

    sections = [
        ("VOD (MOVIE) CATEGORIES", client.get_vod_categories()),
        ("SERIES (TV SHOW) CATEGORIES", client.get_series_categories()),
        ("LIVE TV CATEGORIES", client.get_live_categories()),
    ]

    all_lines = []
    for title, cats in sections:
        lines = _format_section(title, cats)
        all_lines.extend(lines)
        for line in lines:
            print(line)

    outfile = "data/all_categories.txt"
    os.makedirs("data", exist_ok=True)
    with open(outfile, "w", encoding="utf-8") as f:
        f.write("\n".join(all_lines) + "\n")

    print(f"\n*** Full list also saved to: {outfile} ***")

if __name__ == "__main__":
    main()
