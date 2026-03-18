# IPTV Stream Manager

A comprehensive Python tool that scans your IPTV provider (Xtream Codes API), generates `.strm` files for Jellyfin, enriches metadata via TMDB, builds smart collections for a Netflix-like experience, generates EPG data for Threadfin, and provides a real-time web dashboard.

---

## What It Does

```
┌─────────────────────────────────────────────────────────────────────┐
│  IPTV Provider (Xtream Codes API)                                  │
│  ├── VOD Movies                                                     │
│  ├── TV Series (with seasons & episodes)                            │
│  └── Live TV Channels                                               │
└──────────┬──────────────────────────────────────────────────────────┘
           │  Scan via API (faster & more structured than parsing M3U)
           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  IPTV Stream Manager                                                │
│  ├── Language Filter (English + Lithuanian only)                    │
│  ├── .strm file generator (Jellyfin-compatible)                     │
│  ├── TMDB metadata enrichment (.nfo files + posters)                │
│  ├── Smart Collections (Trending, Popular, Genre, Year, etc.)       │
│  ├── EPG XML generator (for Threadfin)                              │
│  ├── Filtered M3U playlist (for Threadfin)                          │
│  ├── Incremental scanning (skip existing, fast re-scans)            │
│  ├── Scheduled auto-scan (e.g. 3:30 AM daily)                      │
│  └── Web Dashboard (stats, logs, controls)                          │
└──────────┬──────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Jellyfin                                                           │
│  ├── Movies Library  →  /media/iptv/Movies/*.strm                  │
│  ├── TV Shows Library → /media/iptv/TVShows/*.strm                 │
│  ├── Collections:                                                   │
│  │   ├── Trending This Week                                         │
│  │   ├── Popular Right Now                                          │
│  │   ├── Top Rated                                                  │
│  │   ├── Recently Added                                             │
│  │   ├── Best of 2025 / Best of 2024                               │
│  │   └── Genre: Action, Comedy, Drama, etc.                        │
│  └── Auto library rescan trigger                                    │
│                                                                     │
│  Threadfin                                                          │
│  ├── Filtered M3U  →  /media/iptv/LiveTV/iptv_channels.m3u        │
│  └── EPG XML       →  /media/iptv/LiveTV/epg.xml                  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### 1. Install Python dependencies

```bash
# Clone or copy the project
cd iptv-manager

# Create a virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate   # macOS/Linux
# or: venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure

Edit `config.yaml` with your settings:

```yaml
iptv:
  server: "http://cf.business-cdn-8k.ru"
  username: "9be604719e"
  password: "6f92330fbf"

paths:
  movies: "/media/iptv/Movies"       # Where Jellyfin's movie library points
  series: "/media/iptv/TVShows"      # Where Jellyfin's TV library points
  live_tv: "/media/iptv/LiveTV"      # Where Threadfin reads M3U + EPG
  data_dir: "./data"

tmdb:
  api_key: "YOUR_TMDB_API_KEY"       # Get free at themoviedb.org/settings/api

schedule:
  scan_time: "03:30"                 # Daily auto-scan at 3:30 AM
```

### 3. Test the connection

```bash
python run.py test
```

This will authenticate, list categories, and show total counts of VOD, series, and live channels.

### 4. Run your first scan

```bash
# Full scan (movies + series + live TV)
python run.py scan

# Or scan specific types
python run.py scan --movies
python run.py scan --series
python run.py scan --live
```

### 5. Start the dashboard + scheduler

```bash
python run.py
```

Open `http://localhost:5000` in your browser to see the dashboard.

---

## Testing on Your MacBook

Since your Proxmox server isn't set up yet, you can absolutely test this on your Mac:

```bash
# 1. Open Terminal

# 2. Make sure Python 3 is installed
python3 --version
# If not installed: brew install python3

# 3. Navigate to the project
cd /path/to/iptv-manager

# 4. Create virtual environment
python3 -m venv venv
source venv/bin/activate

# 5. Install dependencies
pip install -r requirements.txt

# 6. Create test output directories
mkdir -p ~/iptv-test/Movies ~/iptv-test/TVShows ~/iptv-test/LiveTV

# 7. Edit config.yaml to use your test paths:
#    movies: "~/iptv-test/Movies"
#    series: "~/iptv-test/TVShows"
#    live_tv: "~/iptv-test/LiveTV"

# 8. Test connection
python run.py test

# 9. Run a scan
python run.py scan

# 10. Start the dashboard
python run.py
# Then open http://localhost:5000
```

---

## Output Structure

After scanning, your file system will look like this:

```
/media/iptv/
├── Movies/
│   ├── English Movies/
│   │   ├── The Shawshank Redemption (1994)/
│   │   │   ├── The Shawshank Redemption (1994).strm    ← stream URL
│   │   │   └── The Shawshank Redemption (1994).nfo     ← TMDB metadata
│   │   ├── Inception (2010)/
│   │   │   ├── Inception (2010).strm
│   │   │   └── Inception (2010).nfo
│   │   └── ...
│   ├── English Movies 4K/
│   │   └── ...
│   └── Lithuanian Movies/
│       └── ...
│
├── TVShows/
│   ├── Breaking Bad (2008)/
│   │   ├── tvshow.nfo                                   ← TMDB show metadata
│   │   ├── Season 01/
│   │   │   ├── S01E01 - Pilot.strm
│   │   │   ├── S01E02 - Cat's in the Bag.strm
│   │   │   └── ...
│   │   └── Season 02/
│   │       └── ...
│   └── ...
│
├── LiveTV/
│   ├── iptv_channels.m3u     ← filtered M3U for Threadfin
│   └── epg.xml               ← EPG guide data
│
└── Collections/
    ├── Trending This Week/
    │   └── (symlinks to .strm files)
    ├── Popular Right Now/
    ├── Top Rated/
    ├── Recently Added/
    ├── Best of 2025/
    ├── Best of 2024/
    ├── Genre Action/
    ├── Genre Comedy/
    └── ...
```

---

## Jellyfin Setup

### Adding Movie & TV Libraries

1. Go to **Jellyfin Dashboard → Libraries → Add Library**
2. For Movies:
   - Content type: **Movies**
   - Folder: `/media/iptv/Movies`
   - Metadata: Enable **The Movie Database**
   - Enable **NFO metadata** reading
3. For TV Shows:
   - Content type: **Shows**
   - Folder: `/media/iptv/TVShows`
   - Same metadata settings

### Netflix-Like Home Screen

For the best Netflix experience in Jellyfin, install these plugins:

1. **Home Screen Sections (HSS)** - Adds Netflix-style rows to the home page
   - Trending, recently added, genre-based rows
   - Repository: `https://github.com/IAmParadox27/jellyfin-plugin-home-sections`

2. **Smart Lists Plugin** - Create dynamic collections from TMDB trending/popular lists
   - Repository: `https://github.com/jyourstone/jellyfin-smartlists-plugin`

3. **Collection Import** - Import collections from MDBList
   - Repository: `https://github.com/jellyfin/jellyfin-plugin-collection-import`

4. **TMDb Box Sets** - Auto-create movie franchise collections
   - Built-in Jellyfin plugin catalog

5. **ElegantFin or JellyFlix Theme** - Netflix-style dark theme
   - Add to Dashboard → Branding → Custom CSS

### Threadfin Setup

1. Point Threadfin to your filtered M3U: `/media/iptv/LiveTV/iptv_channels.m3u`
2. Point Threadfin to your EPG: `/media/iptv/LiveTV/epg.xml`
3. In Jellyfin, add Threadfin as a Live TV tuner

---

## Architecture

### Why Xtream Codes API Instead of M3U Parsing?

Most IPTV providers use the Xtream Codes platform. Instead of downloading and parsing a massive M3U file (which can be slow and lose metadata), we use the **structured JSON API** directly:

- `player_api.php?action=get_vod_categories` → movie categories
- `player_api.php?action=get_vod_streams` → all movies with metadata
- `player_api.php?action=get_series` → all series
- `player_api.php?action=get_series_info&series_id=X` → seasons & episodes
- `player_api.php?action=get_live_streams` → live channels
- `xmltv.php` → EPG data

This gives us **category names, TMDB IDs, ratings, cover art URLs, and structured episode data** — far richer than what you get from M3U.

### Incremental Scanning

The scanner maintains a `data/scan_state.json` file that tracks every processed item by its `stream_id`. On subsequent scans:

- Items already in state → **skipped** (instant)
- New items from provider → **processed** (writes .strm)
- Items removed by provider → optionally cleaned up

First scan: may take 5-15 minutes depending on library size.
Subsequent scans: typically under 2 minutes.

### TMDB Smart Collections

The enricher pulls lists from TMDB's API:
- **Trending** (daily/weekly) → `/trending/movie/week`
- **Popular** → `/movie/popular`
- **Top Rated** → `/movie/top_rated`
- **Now Playing** → `/movie/now_playing`
- **Genre-based** → matched via genre IDs

It then cross-references these against your IPTV library to create collections containing only content you actually have access to.

---

## Arr Stack Integration (Radarr / Sonarr)

While this tool handles IPTV VOD content, your Arr stack can work alongside it:

- **Radarr/Sonarr** manage downloads from traditional sources (Usenet/torrents)
- **This tool** manages IPTV VOD streaming content
- Both can feed into the same Jellyfin instance — just point them to different library folders

To avoid conflicts, keep IPTV content in separate Jellyfin libraries from your Radarr/Sonarr managed content.

---

## Commands Reference

```bash
python run.py                    # Start dashboard + auto-scheduler
python run.py start              # Same as above
python run.py scan               # One-time full scan
python run.py scan --movies      # Scan movies only
python run.py scan --series      # Scan series only
python run.py scan --live        # Scan live channels only
python run.py enrich             # Run TMDB metadata enrichment
python run.py collections        # Rebuild smart collections
python run.py dashboard          # Dashboard only (no scheduler)
python run.py test               # Test IPTV provider connection
python run.py --config my.yaml   # Use alternate config file
python run.py --port 8080        # Override dashboard port
```

---

## Docker (For Your Proxmox Server)

When your server is ready, here's a Docker Compose setup:

```yaml
services:
  iptv-manager:
    build: .
    container_name: iptv-manager
    restart: unless-stopped
    ports:
      - "5000:5000"
    volumes:
      - ./config.yaml:/app/config.yaml
      - ./data:/app/data
      - /media/iptv:/media/iptv
    environment:
      - TZ=America/Toronto
```

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "run.py"]
```

---

## Troubleshooting

**"Authentication failed"** — Verify your IPTV credentials in config.yaml. Try the URL manually in a browser: `http://cf.business-cdn-8k.ru/player_api.php?username=9be604719e&password=6f92330fbf`

**"No movies found"** — Your provider might use different category naming. Run `python run.py test` to see the actual category names, then adjust the language filter patterns in config.yaml.

**Jellyfin doesn't show new content** — After scanning, trigger a library rescan from the dashboard or Jellyfin's admin panel. Make sure your Jellyfin library folder matches the paths in config.yaml.

**TMDB enrichment slow** — TMDB has rate limits (~40 requests/10 seconds). Initial enrichment of a large library will take time. The cache (`data/tmdb_cache.json`) makes subsequent runs much faster.

---

## License

MIT - Use freely, modify as you like.
