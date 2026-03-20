"""
IPTV Stream Manager - Web Dashboard
Simplified: one scan button, scans everything, always works.
Built-in HDHomeRun tuner emulator — Jellyfin connects directly, no Threadfin needed.
"""
import hashlib, json, logging, os, threading, time, uuid
from datetime import datetime, timezone
from pathlib import Path
import yaml
from flask import Flask, render_template, jsonify, request, Response, send_file, redirect

logger = logging.getLogger(__name__)


def _get_device_id(data_dir: Path) -> str:
    """Return a stable hex device ID for HDHomeRun emulation, persisted to disk."""
    id_file = data_dir / "tuner_device_id"
    if id_file.exists():
        return id_file.read_text().strip()
    did = hashlib.md5(uuid.uuid4().bytes).hexdigest()[:8].upper()
    data_dir.mkdir(parents=True, exist_ok=True)
    id_file.write_text(did)
    return did

def create_app(config: dict, scanner=None, enricher=None, jellyfin=None):
    app = Flask(__name__,
        template_folder=os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates"),
        static_folder=os.path.join(os.path.dirname(os.path.dirname(__file__)), "static"))
    app.config["SECRET_KEY"] = "iptv-stream-manager"
    data_dir = Path(config["paths"].get("data_dir", "./data"))
    data_dir.mkdir(parents=True, exist_ok=True)
    config_path = Path(config.get("_config_path", "config.yaml"))
    log_file = data_dir / "app.log"

    # HDHomeRun tuner emulation
    device_id = _get_device_id(data_dir)
    tuner_cfg = config.get("tuner", {})
    logger.info(f"Tuner Device ID: {device_id}")

    app.scan_running = False
    app.last_scan_result = None
    app.last_dedup = {"movies": 0, "series": 0}
    app.last_enrich = {"enriched": 0, "failed": 0, "skipped": 0}
    app.scheduler = None

    # Proxy — always initialize if possible, always auto-start by default
    app.proxy = None
    app.proxy_running = False
    try:
        from .restream_proxy import RestreamProxy
        app.proxy = RestreamProxy(config)
        app.proxy.load_channels()
        # Auto-start proxy streaming (default: True)
        auto_start = config.get("proxy", {}).get("auto_start", True)
        if auto_start:
            app.proxy_running = True
            logger.info(f"Proxy auto-started with {len(app.proxy.channel_map)} channels (set proxy.auto_start: false in config to disable)")
    except ImportError:
        logger.warning("Proxy: 'requests' package required — pip install requests")
    except Exception as e:
        logger.warning(f"Proxy init: {e}")

    # Tuner requires proxy to always be running for stream delivery
    if tuner_cfg.get("enabled", True) and app.proxy and not app.proxy_running:
        app.proxy_running = True
        logger.info("Tuner enabled: proxy force-started for stream delivery")

    # === SCAN RUNNER (used by API + scheduler) ===
    def _do_scan():
        if app.scan_running:
            logger.warning("Scan already running")
            return
        app.scan_running = True
        try:
            stats = scanner.run_full_scan()
            app.last_scan_result = stats.to_dict()
            sweep = getattr(scanner, '_last_dedup_results', {"movies": 0, "series": 0})
            # Combine inline dedup (during scan) + sweep dedup (post-scan)
            app.last_dedup = {
                "movies": stats.dupes_skipped + stats.dupes_replaced + sweep.get("movies", 0),
                "series": sweep.get("series", 0),
                "total": stats.dupes_skipped + stats.dupes_replaced + stats.dupes_sweep
            }

            # TMDB enrichment
            if not scanner.stop_requested and enricher and enricher.enabled:
                try:
                    scanner.progress.update({"step": "enriching", "step_number": 4,
                        "step_label": "TMDB enrichment", "percent": 10, "details": "Enriching new items..."})
                    enrich_result = enricher.enrich_new_items_only(scanner.state, stats)
                    app.last_enrich = enrich_result
                    scanner._save_state()  # persist TMDB IDs written back to state
                    scanner._enrichment_cache = None  # force counter refresh
                    scanner.progress.update({"percent": 80, "details": f"Building collections... ({enrich_result.get('enriched',0)} enriched)"})
                    enricher.build_collections(scanner.state)
                    scanner.progress.update({"percent": 100, "details": "TMDB done"})
                except Exception as e:
                    logger.error(f"TMDB error (non-fatal): {e}")
                    app.last_enrich = {"enriched": 0, "failed": 0, "skipped": 0, "error": str(e)}

            # Jellyfin rescan
            if not scanner.stop_requested and jellyfin and jellyfin.auto_rescan and getattr(jellyfin, 'auth_enabled', False):
                try:
                    scanner.progress.update({"step_label": "Triggering Jellyfin rescan...", "details": ""})
                    jellyfin.trigger_library_scan()
                except Exception as e:
                    logger.error(f"Jellyfin rescan error: {e}")

            # Reload proxy channels after scan
            if app.proxy:
                try:
                    app.proxy.load_channels()
                    logger.info(f"Proxy reloaded: {len(app.proxy.channel_map)} channels")
                except Exception as e:
                    logger.error(f"Proxy reload after scan failed: {e}")
        except Exception as e:
            logger.exception(f"Scan failed: {e}")
        finally:
            app.scan_running = False
            scanner.stop_requested = False
            scanner.progress = {"step":"idle","step_number":0,"total_steps":5,"step_label":"Idle",
                "items_processed":0,"items_total":0,"percent":0,"eta_seconds":None,
                "step_started_at":None,"_step_started_ts":None,"details":""}

    # For scheduler
    app.run_scan_thread = lambda: threading.Thread(target=_do_scan, daemon=True).start()

    # === ROUTES ===
    @app.route("/")
    def index():
        return render_template("dashboard.html")

    @app.route("/api/status")
    def api_status():
        lib = scanner.get_library_stats() if scanner else {}
        history = []
        hf = data_dir / "scan_history.json"
        if hf.exists():
            try:
                with open(hf) as f: history = json.load(f)
            except: pass
        jf_status = None
        if jellyfin and jellyfin.enabled:
            auth = getattr(jellyfin, 'auth_enabled', False)
            try:
                info = jellyfin.get_system_info()
                jf_status = {"name": info.get("ServerName",""), "version": info.get("Version",""),
                    "online": True, "auth": auth} if info else {"online": False, "auth": auth}
            except:
                jf_status = {"online": False, "auth": auth}
        px = None
        if app.proxy:
            px = app.proxy.get_stats()
            px["running"] = app.proxy_running
        tuner = None
        if tuner_cfg.get("enabled", True):
            tuner = {"device_id": device_id, "channels": len(app.proxy.channel_map) if app.proxy else 0,
                "active_streams": len(app.proxy.streams) if app.proxy else 0,
                "tuner_count": tuner_cfg.get("tuner_count", 2)}
        return jsonify({"library": lib, "last_scan": history[-1] if history else None,
            "scan_running": app.scan_running, "scan_progress": scanner.progress if scanner else {},
            "jellyfin": jf_status, "tmdb_enabled": enricher.enabled if enricher else False,
            "proxy": px, "dedup_stats": app.last_dedup, "enrich_stats": app.last_enrich, "tuner": tuner})

    @app.route("/api/scan", methods=["POST"])
    def api_scan():
        if app.scan_running:
            return jsonify({"error": "Scan already running"}), 409
        threading.Thread(target=_do_scan, daemon=True).start()
        return jsonify({"message": "Scan started"})

    @app.route("/api/scan/stop", methods=["POST"])
    def api_stop():
        if not app.scan_running:
            return jsonify({"error": "No scan running"}), 400
        scanner.stop_requested = True
        return jsonify({"message": "Stop requested"})

    @app.route("/api/scan/progress")
    def api_progress():
        return jsonify({"running": app.scan_running, "progress": scanner.progress if scanner else {}})

    @app.route("/api/scan/enrich", methods=["POST"])
    def api_enrich():
        if not enricher or not enricher.enabled:
            return jsonify({"error": "TMDB not configured"}), 400
        r = enricher.enrich_library(scanner.state)
        return jsonify({"message": "Done", **r})

    @app.route("/api/scan/dedup", methods=["POST"])
    def api_dedup():
        if app.scan_running:
            return jsonify({"error": "Scan already running"}), 409
        if not scanner:
            return jsonify({"error": "Scanner not available"}), 400
        def _do_dedup():
            result = scanner.dedup_sweep()
            app.last_dedup = {
                "movies": result.get("movies", 0),
                "series": result.get("series", 0),
                "total": result.get("movies", 0) + result.get("series", 0),
            }
        threading.Thread(target=_do_dedup, daemon=True).start()
        return jsonify({"message": "Dedup started"})

    @app.route("/api/library")
    def api_library():
        platform = request.args.get("platform", "")
        ctype = request.args.get("type", "movies")
        q = request.args.get("search", "").lower()
        pg = int(request.args.get("page", 1))
        pp = int(request.args.get("per_page", 60))
        if ctype not in ("movies", "series"):
            ctype = "movies"
        result = []
        for sid, info in scanner.state.get(ctype, {}).items():
            if platform and info.get("platform", "") != platform:
                continue
            t = info.get("title", "") or info.get("name", "")
            if q and q not in t.lower():
                continue
            result.append({
                "id": sid,
                "title": t,
                "year": info.get("year", ""),
                "platform": info.get("platform", ""),
                "tags": info.get("tags", []),
                "cover": info.get("poster_url") or info.get("stream_icon", "") or info.get("cover", ""),
                "quality": info.get("quality", 720),
                "tmdb_id": info.get("tmdb_id"),
            })
        result.sort(key=lambda x: x.get("title", "").lower())
        total = len(result)
        start = (pg - 1) * pp
        return jsonify({"total": total, "page": pg, "per_page": pp, "items": result[start:start + pp]})

    @app.route("/api/platforms")
    def api_platforms():
        ctype = request.args.get("type", "movies")
        counts = {}
        for sid, info in scanner.state.get(ctype, {}).items():
            p = info.get("platform", "")
            if p:
                counts[p] = counts.get(p, 0) + 1
        return jsonify(dict(sorted(counts.items(), key=lambda x: x[1], reverse=True)))

    @app.route("/api/library/set-platform", methods=["POST"])
    def api_lib_set_platform():
        d = request.json or {}
        sid = str(d.get("id", ""))
        ctype = d.get("type", "movies")
        platform = d.get("platform", "")
        if ctype not in ("movies", "series"):
            return jsonify({"error": "Invalid type"}), 400
        if sid and sid in scanner.state.get(ctype, {}):
            scanner.state[ctype][sid]["platform"] = platform
            scanner._save_state()
            scanner._enrichment_cache = None
            return jsonify({"message": "Updated"})
        return jsonify({"error": "Not found"}), 404

    @app.route("/api/jellyfin/scan", methods=["POST"])
    def api_jf_scan():
        if not jellyfin or not jellyfin.enabled:
            return jsonify({"error": "Jellyfin not configured"}), 400
        if not getattr(jellyfin, 'auth_enabled', False):
            return jsonify({"error": "No API key. Get one from Jellyfin → Dashboard → API Keys"}), 400
        return jsonify({"success": jellyfin.trigger_library_scan()})

    # --- Providers ---
    @app.route("/api/providers")
    def api_providers():
        primary = config.get("iptv", {})
        provs = [{"id":0,"name":primary.get("name","Primary"),"server":primary.get("server",""),"username":primary.get("username","")}]
        for i,p in enumerate(config.get("iptv_providers",[])):
            provs.append({"id":i+1,"name":p.get("name",f"Provider {i+2}"),"server":p.get("server",""),"username":p.get("username","")})
        return jsonify(provs)

    @app.route("/api/providers/add", methods=["POST"])
    def api_prov_add():
        d = request.json or {}
        if not d.get("server") or not d.get("username") or not d.get("password"):
            return jsonify({"error": "All fields required"}), 400
        provs = config.setdefault("iptv_providers", [])
        provs.append({"name":d.get("name",f"Provider {len(provs)+2}"),"server":d["server"],"username":d["username"],"password":d["password"],"output_format":"ts"})
        _save(config, config_path)
        return jsonify({"message": "Added"})

    @app.route("/api/providers/update", methods=["POST"])
    def api_prov_update():
        d = request.json or {}
        pid = d.get("id", 0)
        rewrite = d.get("rewrite_files", False)
        results = None
        if pid == 0:
            old = (config["iptv"].get("server",""), config["iptv"].get("username",""), config["iptv"].get("password",""))
            for k in ("name","server","username"):
                if d.get(k): config["iptv"][k] = d[k]
            if d.get("password"): config["iptv"]["password"] = d["password"]
            if rewrite and d.get("server") and d.get("username") and d.get("password"):
                results = scanner.rewrite_credentials(old[0],old[1],old[2],d["server"],d["username"],d["password"])
                if app.proxy: app.proxy.server,app.proxy.username,app.proxy.password = d["server"],d["username"],d["password"]; app.proxy.load_channels()
        else:
            provs = config.get("iptv_providers",[])
            idx = pid-1
            if idx<0 or idx>=len(provs): return jsonify({"error":"Invalid"}),400
            old = (provs[idx].get("server",""),provs[idx].get("username",""),provs[idx].get("password",""))
            for k in ("name","server","username"):
                if d.get(k): provs[idx][k] = d[k]
            if d.get("password"): provs[idx]["password"] = d["password"]
            if rewrite and d.get("server") and d.get("username") and d.get("password"):
                results = scanner.rewrite_credentials(old[0],old[1],old[2],d["server"],d["username"],d["password"])
        _save(config, config_path)
        return jsonify({"message":"Updated","results":results})

    @app.route("/api/providers/delete", methods=["POST"])
    def api_prov_del():
        d = request.json or {}
        pid = d.get("id",0)
        if pid==0: return jsonify({"error":"Cannot delete primary"}),400
        provs = config.get("iptv_providers",[])
        if pid-1<0 or pid-1>=len(provs): return jsonify({"error":"Invalid"}),400
        provs.pop(pid-1)
        _save(config, config_path)
        return jsonify({"message":"Deleted"})

    # --- Proxy ---
    @app.route("/api/proxy/start", methods=["POST"])
    def api_px_start():
        if not app.proxy: return jsonify({"error":"N/A"}),400
        app.proxy_running = True
        app.proxy.load_channels()  # Also writes proxy M3U to disk
        return jsonify({"message":"Started","channels":len(app.proxy.channel_map)})

    @app.route("/api/proxy/stop", methods=["POST"])
    def api_px_stop():
        if tuner_cfg.get("enabled", True):
            return jsonify({"error": "Cannot stop proxy while tuner is active — Jellyfin needs it for live TV"}), 400
        app.proxy_running = False
        return jsonify({"message":"Stopped"})

    # Resolve paths for static file fallback (works even if proxy object fails to init)
    _live_tv_dir = Path(os.path.expanduser(config["paths"].get("live_tv", "~/iptv-scanner/LiveTV")))
    _raw_m3u_path = _live_tv_dir / "iptv_channels.m3u"
    _proxy_m3u_path = _live_tv_dir / "iptv_proxy.m3u"
    _epg_path = _live_tv_dir / "epg.xml"

    @app.route("/api/proxy/info")
    def api_px_info():
        """Return Threadfin setup URLs for this server."""
        host = request.host
        scheme = request.scheme
        base = f"{scheme}://{host}"
        epg_ready = _epg_path.exists()
        return jsonify({
            "m3u_url": f"{base}/proxy/playlist.m3u",
            "epg_url": f"{base}/proxy/epg.xml",
            "m3u_file": str(_proxy_m3u_path),
            "epg_file": str(_epg_path),
            "channels": len(app.proxy.channel_map) if app.proxy else 0,
            "epg_ready": epg_ready,
            "proxy_running": app.proxy_running,
        })

    @app.route("/proxy/playlist.m3u")
    @app.route("/proxy/playlist")
    def px_m3u():
        # If proxy is running, generate fresh proxy M3U (also writes to disk)
        if app.proxy and app.proxy_running:
            base = f"{request.scheme}://{request.host}/proxy"
            content = app.proxy.generate_proxy_m3u(base)
            # Update the static file on disk with the correct base URL
            app.proxy.write_proxy_m3u(base)
            return Response(content, mimetype="audio/x-mpegurl",
                            headers={"Content-Disposition": "inline; filename=playlist.m3u"})
        # Proxy stopped or not initialized — serve static proxy M3U from disk if it exists
        if _proxy_m3u_path.exists():
            return send_file(str(_proxy_m3u_path), mimetype="audio/x-mpegurl")
        # Last resort — serve raw M3U with direct provider URLs (streams still work, just not proxied)
        if _raw_m3u_path.exists():
            return send_file(str(_raw_m3u_path), mimetype="audio/x-mpegurl")
        return Response("#EXTM3U\n# No channels yet — run a scan first\n", mimetype="audio/x-mpegurl")

    @app.route("/proxy/epg.xml")
    @app.route("/proxy/xmltv.xml")
    def px_epg():
        # Always serve EPG from disk if it exists — regardless of proxy state
        if _epg_path.exists():
            return send_file(str(_epg_path), mimetype="application/xml")
        # Try proxy object path as fallback
        if app.proxy and app.proxy.epg_path.exists():
            return send_file(str(app.proxy.epg_path), mimetype="application/xml")
        return Response('<?xml version="1.0" encoding="UTF-8"?>\n<tv></tv>', mimetype="application/xml")

    @app.route("/proxy/stream/<f>")
    def px_stream(f):
        if not app.proxy or not app.proxy_running:
            return Response("Proxy streaming not running", status=503)
        import queue as _queue
        sk = f.split(".")[0]
        cid = app.proxy.next_client_id()
        def gen():
            active = None
            try:
                active = app.proxy.get_or_create_stream(sk)
                q = active.add_client(cid)
                while True:
                    try:
                        chunk = q.get(timeout=60)
                    except _queue.Empty:
                        if active.running:
                            continue
                        break
                    if chunk is None:
                        break
                    yield chunk
            except GeneratorExit:
                pass
            except Exception as e:
                logger.error(f"Proxy stream error {sk}/{cid}: {e}")
            finally:
                if active:
                    active.remove_client(cid)
        return Response(gen(), mimetype="video/mp2t", headers={"Cache-Control":"no-cache, no-store","Connection":"keep-alive","Access-Control-Allow-Origin":"*"})

    # --- HDHomeRun Tuner Emulation ---
    # Jellyfin discovers this as an HDHomeRun device — no Threadfin needed.
    # Setup: Jellyfin → Dashboard → Live TV → Add Tuner → HD Homerun → http://HOST:PORT

    @app.route("/discover.json")
    def hdhr_discover():
        base_url = f"{request.scheme}://{request.host}"
        return jsonify({
            "BaseURL": base_url,
            "DeviceAuth": "IPTV-Scanner",
            "DeviceID": device_id,
            "FirmwareName": "bin_1.0",
            "FirmwareVersion": "1.0.0",
            "FriendlyName": tuner_cfg.get("device_name", "IPTV Scanner"),
            "LineupURL": f"{base_url}/lineup.json",
            "Manufacturer": "Silicondust",
            "ModelNumber": "HDTC-2US",
            "TunerCount": tuner_cfg.get("tuner_count", 2),
        })

    @app.route("/device.xml")
    def hdhr_device_xml():
        base_url = f"{request.scheme}://{request.host}"
        name = tuner_cfg.get("device_name", "IPTV Scanner")
        xml = f'''<?xml version="1.0" encoding="UTF-8"?>
<root xmlns="urn:schemas-upnp-org:device-1-0">
  <URLBase>{base_url}</URLBase>
  <specVersion><major>1</major><minor>0</minor></specVersion>
  <device>
    <deviceType>urn:schemas-upnp-org:device:MediaServer:1</deviceType>
    <friendlyName>{name}</friendlyName>
    <manufacturer>Silicondust</manufacturer>
    <modelName>HDTC-2US</modelName>
    <modelNumber>HDTC-2US</modelNumber>
    <serialNumber>{device_id}</serialNumber>
    <UDN>uuid:{device_id}</UDN>
  </device>
</root>'''
        return Response(xml, mimetype="application/xml")

    @app.route("/lineup_status.json")
    def hdhr_lineup_status():
        return jsonify({"ScanInProgress": 0, "ScanPossible": 1, "Source": "Cable", "SourceList": ["Cable"]})

    @app.route("/lineup.json")
    def hdhr_lineup():
        base_url = f"{request.scheme}://{request.host}"
        lineup = []
        if app.proxy and app.proxy.channel_map:
            for stream_key, ch_info in sorted(app.proxy.channel_map.items(), key=lambda x: x[1].get("name", "")):
                lineup.append({
                    "GuideName": ch_info.get("name", f"Ch {stream_key}"),
                    "GuideNumber": stream_key,
                    "URL": f"{base_url}/proxy/stream/{stream_key}.ts",
                })
        elif scanner and scanner.state.get("channels"):
            for stream_id, ch_info in sorted(scanner.state["channels"].items(), key=lambda x: x[1].get("name", "")):
                lineup.append({
                    "GuideName": ch_info.get("name", f"Ch {stream_id}"),
                    "GuideNumber": stream_id,
                    "URL": f"{base_url}/proxy/stream/{stream_id}.ts",
                })
        return jsonify(lineup)

    @app.route("/lineup.json", methods=["POST"])
    @app.route("/lineup.post", methods=["GET", "POST"])
    def hdhr_lineup_post():
        return Response("", status=200)

    @app.route("/api/tuner/info")
    def api_tuner_info():
        base_url = f"{request.scheme}://{request.host}"
        return jsonify({
            "enabled": tuner_cfg.get("enabled", True),
            "device_id": device_id,
            "device_name": tuner_cfg.get("device_name", "IPTV Scanner"),
            "tuner_count": tuner_cfg.get("tuner_count", 2),
            "channels": len(app.proxy.channel_map) if app.proxy else 0,
            "active_streams": len(app.proxy.streams) if app.proxy else 0,
            "discover_url": f"{base_url}/discover.json",
            "lineup_url": f"{base_url}/lineup.json",
            "epg_url": f"{base_url}/proxy/epg.xml",
            "jellyfin_tuner_url": base_url,
            "proxy_running": app.proxy_running,
        })

    # --- History/Logs ---
    @app.route("/api/history")
    def api_history():
        hf = data_dir/"scan_history.json"
        if hf.exists():
            with open(hf) as f: return jsonify(json.load(f))
        return jsonify([])

    @app.route("/api/logs")
    def api_logs():
        n = int(request.args.get("lines",300))
        if log_file.exists():
            with open(log_file) as f: lines = f.readlines()
            return jsonify({"lines":lines[-n:]})
        return jsonify({"lines":[]})

    # --- Library ---
    @app.route("/api/movies")
    def api_movies():
        pg,pp = int(request.args.get("page",1)),int(request.args.get("per_page",50))
        q = request.args.get("search","").lower()
        items = [m for m in scanner.state.get("movies",{}).values() if not q or q in m.get("title","").lower()]
        items.sort(key=lambda x:x.get("added_at",""),reverse=True)
        s=(pg-1)*pp
        return jsonify({"total":len(items),"page":pg,"items":items[s:s+pp]})

    @app.route("/api/series")
    def api_series():
        pg,pp = int(request.args.get("page",1)),int(request.args.get("per_page",50))
        q = request.args.get("search","").lower()
        items = [s for s in scanner.state.get("series",{}).values() if not q or q in s.get("title","").lower()]
        items.sort(key=lambda x:x.get("added_at",""),reverse=True)
        s=(pg-1)*pp
        return jsonify({"total":len(items),"page":pg,"items":items[s:s+pp]})

    # --- Categories ---
    @app.route("/api/categories/<ct>")
    def api_cats(ct):
        try:
            if ct=="vod": cats=scanner.client.get_vod_categories(); sel=set(str(c) for c in config.get("filters",{}).get("vod_category_ids",[]))
            elif ct=="series": cats=scanner.client.get_series_categories(); sel=set(str(c) for c in config.get("filters",{}).get("series_category_ids",[]))
            elif ct=="live": cats=scanner.client.get_live_categories(); sel=set(str(c) for c in config.get("filters",{}).get("live_category_ids",[]))
            else: return jsonify({"error":"Bad type"}),400
            cat_tags = config.get("filters", {}).get("category_tags", {})
            r = [{"id":str(c.get("category_id","") or ""),"name":c.get("category_name",""),"selected":str(c.get("category_id","") or "") in sel,"tags":cat_tags.get(str(c.get("category_id","") or ""),[]) } for c in cats]
            r.sort(key=lambda x:x["name"])
            return jsonify(r)
        except Exception as e: return jsonify({"error":str(e)}),500

    @app.route("/api/categories/save", methods=["POST"])
    def api_cats_save():
        d = request.json or {}
        for key in ["vod_category_ids","series_category_ids","live_category_ids"]:
            if key in d: config.setdefault("filters",{})[key] = [int(x) for x in d[key]]
        if "category_tags" in d:
            existing = config.setdefault("filters", {}).setdefault("category_tags", {})
            existing.update(d["category_tags"])
            for k in list(existing.keys()):
                if not existing[k]: del existing[k]
        _save(config, config_path)
        if scanner:
            scanner.vod_cat_ids = set(str(x) for x in config.get("filters",{}).get("vod_category_ids",[]))
            scanner.series_cat_ids = set(str(x) for x in config.get("filters",{}).get("series_category_ids",[]))
            scanner.live_cat_ids = set(str(x) for x in config.get("filters",{}).get("live_category_ids",[]))
        return jsonify({"message":"Saved"})

    # --- Settings ---
    @app.route("/api/settings")
    def api_settings():
        jfu = jellyfin.url if jellyfin else config.get("jellyfin",{}).get("url","")
        jfk = jellyfin.api_key if jellyfin else config.get("jellyfin",{}).get("api_key","")
        if jfk == "YOUR_JELLYFIN_API_KEY_HERE": jfk = ""
        return jsonify({"tmdb_api_key":config.get("tmdb",{}).get("api_key",""),
            "jellyfin_url":jfu,"jellyfin_api_key":jfk,
            "jellyfin_auto_rescan":jellyfin.auto_rescan if jellyfin else True,
            "scan_time":config.get("schedule",{}).get("scan_time","03:30"),
            "scan_frequency":config.get("schedule",{}).get("frequency","daily"),
            "scan_enabled":config.get("schedule",{}).get("enabled",False)})

    @app.route("/api/settings", methods=["POST"])
    def api_settings_save():
        d = request.json or {}
        m = {"tmdb_api_key":("tmdb","api_key"),"jellyfin_url":("jellyfin","url"),"jellyfin_api_key":("jellyfin","api_key"),
             "jellyfin_auto_rescan":("jellyfin","auto_rescan"),"scan_time":("schedule","scan_time"),
             "scan_frequency":("schedule","frequency"),"scan_enabled":("schedule","enabled")}
        for k,(sec,fld) in m.items():
            if k in d: config.setdefault(sec,{})[fld] = d[k]
        # Update live objects
        if enricher and "tmdb_api_key" in d:
            enricher.api_key = d["tmdb_api_key"]; enricher.enabled = bool(d["tmdb_api_key"])
        if jellyfin:
            if "jellyfin_url" in d: jellyfin.url = d["jellyfin_url"].rstrip("/"); jellyfin.enabled = bool(jellyfin.url)
            if "jellyfin_api_key" in d: jellyfin.api_key = d["jellyfin_api_key"]
            if "jellyfin_auto_rescan" in d: jellyfin.auto_rescan = d["jellyfin_auto_rescan"]
            jellyfin.auth_enabled = bool(jellyfin.url and jellyfin.api_key and jellyfin.api_key != "YOUR_JELLYFIN_API_KEY_HERE")
        _save(config, config_path)

        # Restart scheduler if schedule settings changed
        if any(k in d for k in ("scan_time", "scan_frequency", "scan_enabled")):
            _restart_scheduler()

        return jsonify({"message":"Saved"})

    def _restart_scheduler():
        """Stop old scheduler and start new one with current config."""
        try:
            if app.scheduler:
                app.scheduler.shutdown(wait=False)
                app.scheduler = None
                logger.info("Old scheduler stopped")

            sched_config = config.get("schedule", {})
            if not sched_config.get("enabled", False):
                logger.info("Scheduler disabled")
                return

            try:
                from apscheduler.schedulers.background import BackgroundScheduler
            except ImportError:
                logger.warning("APScheduler not installed")
                return

            scan_time = sched_config.get("scan_time", "03:30")
            try:
                hour, minute = map(int, scan_time.split(":"))
            except ValueError:
                hour, minute = 3, 30

            scheduler = BackgroundScheduler()
            freq = sched_config.get("frequency", "daily")

            def scheduled_scan():
                logger.info(f"=== SCHEDULED SCAN triggered ===")
                if not app.scan_running:
                    app.run_scan_thread()
                else:
                    logger.info("Scan already running, skipping")

            if freq == "weekly":
                scheduler.add_job(scheduled_scan, 'cron', day_of_week='mon', hour=hour, minute=minute, id='iptv_scan', replace_existing=True)
            elif freq == "monthly":
                scheduler.add_job(scheduled_scan, 'cron', day=1, hour=hour, minute=minute, id='iptv_scan', replace_existing=True)
            else:
                scheduler.add_job(scheduled_scan, 'cron', hour=hour, minute=minute, id='iptv_scan', replace_existing=True)

            scheduler.start()
            app.scheduler = scheduler
            logger.info(f"Scheduler started: {freq} at {scan_time}")
        except Exception as e:
            logger.error(f"Failed to restart scheduler: {e}")

    return app

def _save(config, path):
    d = {k:v for k,v in config.items() if not k.startswith("_")}
    with open(path,"w") as f: yaml.dump(d, f, default_flow_style=False, sort_keys=False)
