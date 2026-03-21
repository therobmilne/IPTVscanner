"""
IPTV Stream Manager - Web Dashboard
Unified IPTV management: replaces Threadfin + TVHeadend.
Includes HDHomeRun emulator, live TV channel management, playlists, restream proxy.
"""
import json, logging, os, re, threading, time
from datetime import datetime, timezone
from pathlib import Path
import yaml
from flask import Flask, render_template, jsonify, request, Response, send_file

logger = logging.getLogger(__name__)


def create_app(config: dict, scanner=None, enricher=None, jellyfin=None):
    app = Flask(__name__,
        template_folder=os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates"),
        static_folder=os.path.join(os.path.dirname(os.path.dirname(__file__)), "static"))
    app.config["SECRET_KEY"] = "iptv-stream-manager"
    data_dir = Path(config["paths"].get("data_dir", "./data"))
    config_path = Path(config.get("_config_path", "config.yaml"))
    log_file = data_dir / "app.log"
    app.scan_running = False
    app.last_scan_result = None
    app.last_dedup = {"movies": 0, "series": 0}
    app.last_enrich = {"enriched": 0, "failed": 0, "skipped": 0}
    app.scheduler = None

    # Channel mapping state (Threadfin-style)
    # Maps stream_id -> {enabled, ch_num, custom_name, group, epg_id}
    channel_map_file = data_dir / "channel_map.json"
    app.channel_map = _load_json(channel_map_file, {})
    # Legacy compat: migrate old enabled_channels.json
    old_enabled = data_dir / "enabled_channels.json"
    if old_enabled.exists() and not app.channel_map:
        try:
            old = _load_json(old_enabled, {})
            n = 1
            for sid in old:
                app.channel_map[sid] = {"enabled": True, "ch_num": n, "custom_name": "", "group": "", "epg_id": ""}
                n += 1
            _save_json(channel_map_file, app.channel_map)
            logger.info(f"Migrated {len(app.channel_map)} channels from old format")
        except: pass

    # Playlists state
    playlists_file = data_dir / "playlists.json"
    app.playlists = _load_json(playlists_file, [])
    playlists_dir = Path(config["paths"].get("movies", "/media/strm/movies")).parent / "playlists"
    playlists_dir.mkdir(parents=True, exist_ok=True)
    app.playlists_dir = playlists_dir

    # Cached channel list (avoid re-fetching from provider on every page load)
    app._channels_cache = None  # {"ts": timestamp, "data": [...], "categories": [...]}

    # Proxy
    app.proxy = None
    app.proxy_running = False
    try:
        from .restream_proxy import RestreamProxy
        app.proxy = RestreamProxy(config)
        app.proxy.load_channels()
        if config.get("proxy", {}).get("auto_start", True):
            app.proxy_running = True
            logger.info(f"Proxy auto-started with {len(app.proxy.channel_map)} channels")
    except ImportError:
        logger.warning("Proxy: 'requests' package required")
    except Exception as e:
        logger.warning(f"Proxy init: {e}")

    # Tuner config
    tuner_config = config.get("tuner", {})
    app.tuner_enabled = tuner_config.get("enabled", True)
    app.tuner_count = tuner_config.get("tuner_count", 4)
    app.device_name = tuner_config.get("device_name", "IPTV Scanner")

    # === SCAN RUNNER ===
    def _do_scan():
        if app.scan_running:
            return
        app.scan_running = True
        try:
            stats = scanner.run_full_scan()
            app.last_scan_result = stats.to_dict()
            sweep = getattr(scanner, '_last_dedup_results', {"movies": 0, "series": 0})
            app.last_dedup = {
                "movies": stats.dupes_skipped + stats.dupes_replaced + sweep.get("movies", 0),
                "series": sweep.get("series", 0),
                "total": stats.dupes_skipped + stats.dupes_replaced + stats.dupes_sweep
            }
            if not scanner.stop_requested and enricher and enricher.enabled:
                try:
                    scanner.progress.update({"step": "enriching", "step_number": 4,
                        "step_label": "TMDB enrichment", "percent": 10, "details": "Enriching new items..."})
                    enrich_result = enricher.enrich_new_items_only(scanner.state, stats)
                    app.last_enrich = enrich_result
                    scanner._save_state()
                    scanner._enrichment_cache = None
                    scanner.progress.update({"percent": 80, "details": f"Building collections... ({enrich_result.get('enriched',0)} enriched)"})
                    enricher.build_collections(scanner.state)
                    scanner.progress.update({"percent": 100, "details": "TMDB done"})
                except Exception as e:
                    logger.error(f"TMDB error: {e}")
                    app.last_enrich = {"enriched": 0, "failed": 0, "skipped": 0, "error": str(e)}
            if not scanner.stop_requested and jellyfin and jellyfin.auto_rescan and getattr(jellyfin, 'auth_enabled', False):
                try:
                    scanner.progress.update({"step_label": "Triggering Jellyfin rescan...", "details": ""})
                    jellyfin.trigger_library_scan()
                except Exception as e:
                    logger.error(f"Jellyfin rescan error: {e}")
            if app.proxy:
                try:
                    app.proxy.load_channels()
                except Exception as e:
                    logger.error(f"Proxy reload failed: {e}")
            # Invalidate channel cache after scan
            app._channels_cache = None
        except Exception as e:
            logger.exception(f"Scan failed: {e}")
        finally:
            app.scan_running = False
            scanner.stop_requested = False
            scanner.progress = {"step":"idle","step_number":0,"total_steps":5,"step_label":"Idle",
                "items_processed":0,"items_total":0,"percent":0,"eta_seconds":None,
                "step_started_at":None,"_step_started_ts":None,"details":""}

    app.run_scan_thread = lambda: threading.Thread(target=_do_scan, daemon=True).start()

    # =====================================================================
    #  ROUTES
    # =====================================================================

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
        enabled_count = sum(1 for v in app.channel_map.values() if v.get("enabled"))
        return jsonify({
            "library": lib, "last_scan": history[-1] if history else None,
            "scan_running": app.scan_running, "scan_progress": scanner.progress if scanner else {},
            "jellyfin": jf_status, "tmdb_enabled": enricher.enabled if enricher else False,
            "proxy": px, "dedup_stats": app.last_dedup, "enrich_stats": app.last_enrich,
            "tuner": {"enabled": app.tuner_enabled, "device_name": app.device_name, "tuner_count": app.tuner_count},
            "enabled_channels": enabled_count, "playlist_count": len(app.playlists),
        })

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
            app.last_dedup = {"movies": result.get("movies", 0), "series": result.get("series", 0),
                "total": result.get("movies", 0) + result.get("series", 0)}
        threading.Thread(target=_do_dedup, daemon=True).start()
        return jsonify({"message": "Dedup started"})

    # --- Library (used by playlists) ---
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
            result.append({"id": sid, "title": t, "year": info.get("year", ""),
                "platform": info.get("platform", ""), "tags": info.get("tags", []),
                "cover": info.get("poster_url") or info.get("stream_icon", "") or info.get("cover", ""),
                "quality": info.get("quality", 720), "tmdb_id": info.get("tmdb_id")})
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
            if p: counts[p] = counts.get(p, 0) + 1
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
            return jsonify({"error": "No API key"}), 400
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
        app.proxy_running = True; app.proxy.load_channels()
        return jsonify({"message":"Started","channels":len(app.proxy.channel_map)})

    @app.route("/api/proxy/stop", methods=["POST"])
    def api_px_stop():
        app.proxy_running = False
        return jsonify({"message":"Stopped"})

    @app.route("/api/proxy/info")
    def api_px_info():
        host = request.host; scheme = request.scheme; base = f"{scheme}://{host}"
        epg_ready = bool(app.proxy and app.proxy.epg_path.exists()) if app.proxy else False
        return jsonify({"m3u_url": f"{base}/proxy/playlist.m3u", "epg_url": f"{base}/proxy/epg.xml",
            "channels": len(app.proxy.channel_map) if app.proxy else 0, "epg_ready": epg_ready, "proxy_running": app.proxy_running})

    @app.route("/proxy/playlist.m3u")
    @app.route("/proxy/playlist")
    def px_m3u():
        if not app.proxy: return Response("Proxy not configured", status=503)
        base = f"{request.scheme}://{request.host}/proxy"
        return Response(app.proxy.generate_proxy_m3u(base), mimetype="audio/x-mpegurl",
                        headers={"Content-Disposition": "inline; filename=playlist.m3u"})

    @app.route("/proxy/epg.xml")
    @app.route("/proxy/xmltv.xml")
    def px_epg():
        if app.proxy and app.proxy.epg_path.exists():
            return send_file(str(app.proxy.epg_path), mimetype="application/xml")
        return Response("EPG not yet generated", status=404)

    @app.route("/proxy/stream/<f>")
    def px_stream(f):
        if not app.proxy or not app.proxy_running:
            return Response("Proxy not running", status=503)
        import queue as _queue
        sk = f.split(".")[0]
        cid = app.proxy.next_client_id()
        def gen():
            active = None
            try:
                active = app.proxy.get_or_create_stream(sk)
                q = active.add_client(cid)
                while True:
                    try: chunk = q.get(timeout=60)
                    except _queue.Empty:
                        if active.running: continue
                        break
                    if chunk is None: break
                    yield chunk
            except GeneratorExit: pass
            except Exception as e: logger.error(f"Proxy stream error {sk}/{cid}: {e}")
            finally:
                if active: active.remove_client(cid)
        return Response(gen(), mimetype="video/mp2t",
            headers={"Cache-Control":"no-cache,no-store","Connection":"keep-alive","Access-Control-Allow-Origin":"*"})

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

    # --- Categories ---
    @app.route("/api/categories/<ct>")
    def api_cats(ct):
        try:
            if ct=="vod": cats=scanner.client.get_vod_categories(); sel=set(str(c) for c in config.get("filters",{}).get("vod_category_ids",[]))
            elif ct=="series": cats=scanner.client.get_series_categories(); sel=set(str(c) for c in config.get("filters",{}).get("series_category_ids",[]))
            elif ct=="live": cats=scanner.client.get_live_categories(); sel=set(str(c) for c in config.get("filters",{}).get("live_category_ids",[]))
            else: return jsonify({"error":"Bad type"}),400
            cat_tags = config.get("filters", {}).get("category_tags", {})
            r = [{"id":str(c.get("category_id","") or ""),"name":c.get("category_name",""),"selected":str(c.get("category_id","") or "") in sel,
                  "tags":cat_tags.get(str(c.get("category_id","") or ""),[]) } for c in cats]
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
        # Invalidate channel cache since live categories may have changed
        app._channels_cache = None
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
            "scan_enabled":config.get("schedule",{}).get("enabled",False),
            "tuner_enabled":app.tuner_enabled,"tuner_count":app.tuner_count,"device_name":app.device_name})

    @app.route("/api/settings", methods=["POST"])
    def api_settings_save():
        d = request.json or {}
        m = {"tmdb_api_key":("tmdb","api_key"),"jellyfin_url":("jellyfin","url"),"jellyfin_api_key":("jellyfin","api_key"),
             "jellyfin_auto_rescan":("jellyfin","auto_rescan"),"scan_time":("schedule","scan_time"),
             "scan_frequency":("schedule","frequency"),"scan_enabled":("schedule","enabled")}
        for k,(sec,fld) in m.items():
            if k in d: config.setdefault(sec,{})[fld] = d[k]
        if "tuner_enabled" in d: config.setdefault("tuner",{})["enabled"]=d["tuner_enabled"]; app.tuner_enabled=d["tuner_enabled"]
        if "tuner_count" in d: config.setdefault("tuner",{})["tuner_count"]=int(d["tuner_count"]); app.tuner_count=int(d["tuner_count"])
        if "device_name" in d: config.setdefault("tuner",{})["device_name"]=d["device_name"]; app.device_name=d["device_name"]
        if enricher and "tmdb_api_key" in d:
            enricher.api_key = d["tmdb_api_key"]; enricher.enabled = bool(d["tmdb_api_key"])
        if jellyfin:
            if "jellyfin_url" in d: jellyfin.url = d["jellyfin_url"].rstrip("/"); jellyfin.enabled = bool(jellyfin.url)
            if "jellyfin_api_key" in d: jellyfin.api_key = d["jellyfin_api_key"]
            if "jellyfin_auto_rescan" in d: jellyfin.auto_rescan = d["jellyfin_auto_rescan"]
            jellyfin.auth_enabled = bool(jellyfin.url and jellyfin.api_key and jellyfin.api_key != "YOUR_JELLYFIN_API_KEY_HERE")
        _save(config, config_path)
        if any(k in d for k in ("scan_time", "scan_frequency", "scan_enabled")):
            _restart_scheduler()
        return jsonify({"message":"Saved"})

    # =====================================================================
    #  LIVE TV CHANNEL MANAGEMENT (Threadfin-style mapping)
    #  Step 1: Categories page selects which groups to pull from
    #  Step 2: This page lets you map each channel: enable, number, name, group, EPG
    #  Caches provider fetch for 5 min.
    # =====================================================================

    def _get_live_channels():
        """Fetch live channels from whitelisted categories only. Cached 5 min."""
        cache = app._channels_cache
        if cache and (time.time() - cache["ts"]) < 300:
            return cache["data"], cache["categories"]

        live_cat_ids = scanner.live_cat_ids if scanner else set()
        try:
            cats = scanner.client.get_live_categories()
            cat_map = {str(c.get("category_id","") or ""): c.get("category_name","") for c in cats}
            all_ch = []
            if live_cat_ids:
                for cat_id in live_cat_ids:
                    try:
                        ch_list = scanner.client.get_live_streams(cat_id)
                        for ch in ch_list:
                            ch["_cat_name"] = cat_map.get(str(ch.get("category_id","") or ""), "")
                        all_ch.extend(ch_list)
                    except Exception:
                        pass
            else:
                all_ch = scanner.client.get_live_streams()
                for ch in all_ch:
                    ch["_cat_name"] = cat_map.get(str(ch.get("category_id","") or ""), "")
        except Exception as e:
            logger.error(f"Failed to fetch channels: {e}")
            return [], []

        result = []
        for ch in all_ch:
            sid = str(ch.get("stream_id", ""))
            mapping = app.channel_map.get(sid, {})
            result.append({
                "stream_id": sid,
                "name": ch.get("name", ""),
                "logo": ch.get("stream_icon", ""),
                "category_id": str(ch.get("category_id", "") or ""),
                "category": ch.get("_cat_name", ""),
                "epg_id": ch.get("epg_channel_id", ""),
                # Mapping overrides
                "enabled": mapping.get("enabled", False),
                "ch_num": mapping.get("ch_num", 0),
                "custom_name": mapping.get("custom_name", ""),
                "group": mapping.get("group", ""),
                "mapped_epg": mapping.get("epg_id", ""),
            })
        result.sort(key=lambda x: (x["category"], x["name"]))
        categories = sorted(set(c["category"] for c in result if c["category"]))
        app._channels_cache = {"ts": time.time(), "data": result, "categories": categories}
        return result, categories

    @app.route("/api/channels")
    def api_channels():
        """Get live channels with mapping data, paginated."""
        channels, categories = _get_live_channels()

        q = request.args.get("search", "").lower()
        cat_filter = request.args.get("category", "")
        show = request.args.get("show", "")  # "mapped" = only enabled
        pg = int(request.args.get("page", 1))
        pp = int(request.args.get("per_page", 200))

        # Re-sync mapping state
        for ch in channels:
            m = app.channel_map.get(ch["stream_id"], {})
            ch["enabled"] = m.get("enabled", False)
            ch["ch_num"] = m.get("ch_num", 0)
            ch["custom_name"] = m.get("custom_name", "")
            ch["group"] = m.get("group", "")
            ch["mapped_epg"] = m.get("epg_id", "")

        filtered = channels
        if q:
            filtered = [c for c in filtered if q in c["name"].lower() or q in c["category"].lower()
                        or q in c.get("custom_name","").lower()]
        if cat_filter:
            filtered = [c for c in filtered if c["category_id"] == cat_filter]
        if show == "mapped":
            filtered = [c for c in filtered if c["enabled"]]

        total = len(filtered)
        enabled_count = sum(1 for c in channels if c["enabled"])
        start = (pg - 1) * pp
        page_items = filtered[start:start + pp]

        cat_options = []
        for cat in categories:
            cnt = sum(1 for c in channels if c["category"] == cat)
            cat_id = next((c["category_id"] for c in channels if c["category"] == cat), "")
            cat_options.append({"id": cat_id, "name": cat, "count": cnt})

        return jsonify({"channels": page_items, "categories": cat_options,
            "total": total, "enabled_count": enabled_count, "total_all": len(channels),
            "page": pg, "pages": max(1, -(-total // pp))})

    @app.route("/api/channels/map", methods=["POST"])
    def api_channels_map():
        """Update mapping for a single channel (Threadfin-style inline edit)."""
        d = request.json or {}
        sid = str(d.get("stream_id", ""))
        if not sid:
            return jsonify({"error": "stream_id required"}), 400

        existing = app.channel_map.get(sid, {"enabled": False, "ch_num": 0, "custom_name": "", "group": "", "epg_id": ""})
        for field in ("enabled", "ch_num", "custom_name", "group", "epg_id"):
            if field in d:
                existing[field] = d[field]
        app.channel_map[sid] = existing
        _save_json(channel_map_file, app.channel_map)
        return jsonify({"message": "Mapped"})

    @app.route("/api/channels/bulk", methods=["POST"])
    def api_channels_bulk():
        """Bulk enable/disable channels (select all filtered, deselect all, etc.)."""
        d = request.json or {}
        action = d.get("action", "")  # "enable", "disable", "auto_number"
        stream_ids = d.get("stream_ids", [])

        if action == "enable":
            next_num = max((m.get("ch_num", 0) for m in app.channel_map.values()), default=0) + 1
            for sid in stream_ids:
                sid = str(sid)
                if sid not in app.channel_map:
                    app.channel_map[sid] = {"enabled": True, "ch_num": next_num, "custom_name": "", "group": "", "epg_id": ""}
                    next_num += 1
                else:
                    app.channel_map[sid]["enabled"] = True
                    if not app.channel_map[sid].get("ch_num"):
                        app.channel_map[sid]["ch_num"] = next_num
                        next_num += 1
        elif action == "disable":
            for sid in stream_ids:
                sid = str(sid)
                if sid in app.channel_map:
                    app.channel_map[sid]["enabled"] = False
        elif action == "auto_number":
            # Renumber all enabled channels sequentially
            enabled = [(sid, m) for sid, m in app.channel_map.items() if m.get("enabled")]
            enabled.sort(key=lambda x: x[1].get("ch_num", 9999))
            for i, (sid, m) in enumerate(enabled, 1):
                m["ch_num"] = i

        _save_json(channel_map_file, app.channel_map)
        return jsonify({"message": f"Updated {len(stream_ids)} channels"})

    @app.route("/api/channels/save", methods=["POST"])
    def api_channels_save():
        """Save current mapping and regenerate M3U + EPG + restart proxy."""
        _save_json(channel_map_file, app.channel_map)
        enabled = {sid: True for sid, m in app.channel_map.items() if m.get("enabled")}
        count = _regenerate_m3u_mapped(scanner, config, app.channel_map)
        if app.proxy:
            app.proxy.load_channels()
        app._channels_cache = None
        return jsonify({"message": f"Saved & regenerated {count} channels", "count": count})

    # =====================================================================
    #  PLAYLISTS (replaces VOD Library)
    #  Creates M3U playlists from scanned content organized by platform/tags.
    #  Jellyfin can read these as playlist libraries.
    # =====================================================================

    @app.route("/api/playlists")
    def api_playlists():
        """Get all playlists with item counts."""
        result = []
        for pl in app.playlists:
            count = _count_playlist_items(pl, scanner.state)
            result.append({**pl, "item_count": count})
        return jsonify(result)

    @app.route("/api/playlists/create", methods=["POST"])
    def api_playlists_create():
        """Create a new playlist from filter criteria."""
        d = request.json or {}
        name = d.get("name", "").strip()
        if not name:
            return jsonify({"error": "Name required"}), 400
        pl = {
            "id": str(int(time.time() * 1000))[-8:],
            "name": name,
            "type": d.get("type", "movies"),      # movies or series
            "platform": d.get("platform", ""),      # e.g. "Netflix"
            "tags": d.get("tags", []),              # additional tag filters
            "quality_min": d.get("quality_min", 0), # min quality (0=any)
            "search": d.get("search", ""),           # text filter
            "created": datetime.now(timezone.utc).isoformat(),
        }
        app.playlists.append(pl)
        _save_json(playlists_file, app.playlists)
        # Generate the M3U file
        count = _generate_playlist_file(pl, scanner.state, app.playlists_dir, config)
        return jsonify({"message": f"Created playlist '{name}' with {count} items", "playlist": pl})

    @app.route("/api/playlists/delete", methods=["POST"])
    def api_playlists_delete():
        d = request.json or {}
        pid = d.get("id", "")
        app.playlists = [p for p in app.playlists if p["id"] != pid]
        _save_json(playlists_file, app.playlists)
        # Delete the M3U file
        for f in app.playlists_dir.glob(f"{pid}_*.m3u"):
            f.unlink(missing_ok=True)
        return jsonify({"message": "Deleted"})

    @app.route("/api/playlists/regenerate", methods=["POST"])
    def api_playlists_regen():
        """Regenerate all playlist M3U files (e.g. after a scan)."""
        total = 0
        for pl in app.playlists:
            total += _generate_playlist_file(pl, scanner.state, app.playlists_dir, config)
        return jsonify({"message": f"Regenerated {len(app.playlists)} playlists ({total} total items)"})

    @app.route("/api/playlists/preview", methods=["POST"])
    def api_playlists_preview():
        """Preview items that would be in a playlist without saving."""
        d = request.json or {}
        pl = {"type": d.get("type","movies"), "platform": d.get("platform",""),
              "tags": d.get("tags",[]), "quality_min": d.get("quality_min",0), "search": d.get("search","")}
        items = _get_playlist_items(pl, scanner.state)
        # Return first 50 for preview
        preview = [{"title": i.get("title",""), "year": i.get("year",""), "platform": i.get("platform",""),
                     "quality": i.get("quality",720), "cover": i.get("poster_url") or i.get("stream_icon","") or i.get("cover","")}
                    for i in items[:50]]
        return jsonify({"total": len(items), "preview": preview})

    # =====================================================================
    #  HDHomeRun EMULATOR
    # =====================================================================

    @app.route("/discover.json")
    def hdhr_discover():
        if not app.tuner_enabled:
            return Response("Tuner disabled", status=503)
        base = f"{request.scheme}://{request.host}"
        return jsonify({"FriendlyName": app.device_name, "Manufacturer": "IPTV Scanner",
            "ModelNumber": "HDTC-2US", "FirmwareName": "hdhomerun_atsc", "FirmwareVersion": "20230501",
            "DeviceID": "1234ABCD", "DeviceAuth": "iptvscan", "BaseURL": base,
            "LineupURL": f"{base}/lineup.json", "TunerCount": app.tuner_count})

    @app.route("/device.xml")
    def hdhr_device_xml():
        if not app.tuner_enabled:
            return Response("Tuner disabled", status=503)
        base = f"{request.scheme}://{request.host}"
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<root xmlns="urn:schemas-upnp-org:device-1-0">
  <specVersion><major>1</major><minor>0</minor></specVersion>
  <device>
    <deviceType>urn:schemas-upnp-org:device:MediaServer:1</deviceType>
    <friendlyName>{app.device_name}</friendlyName>
    <manufacturer>IPTV Scanner</manufacturer>
    <modelName>HDTC-2US</modelName>
    <modelNumber>HDTC-2US</modelNumber>
    <serialNumber>1234ABCD</serialNumber>
    <UDN>uuid:1234ABCD-iptv-scan</UDN>
  </device>
  <URLBase>{base}</URLBase>
</root>"""
        return Response(xml, mimetype="application/xml")

    @app.route("/lineup_status.json")
    def hdhr_lineup_status():
        return jsonify({"ScanInProgress": 0, "ScanPossible": 1, "Source": "Cable", "SourceList": ["Cable"]})

    @app.route("/lineup.json")
    def hdhr_lineup():
        """HDHomeRun lineup using mapped channel numbers and custom names."""
        if not app.proxy:
            return jsonify([])
        base = f"{request.scheme}://{request.host}"
        lineup = []
        for sk, info in app.proxy.channel_map.items():
            mapping = app.channel_map.get(sk, {})
            if not mapping.get("enabled"):
                continue
            ch_num = mapping.get("ch_num", 0)
            display_name = mapping.get("custom_name") or info.get("name", sk)
            lineup.append({
                "GuideNumber": str(ch_num) if ch_num else str(len(lineup) + 1),
                "GuideName": display_name,
                "URL": f"{base}/proxy/stream/{sk}.ts",
            })
        lineup.sort(key=lambda x: int(x["GuideNumber"]) if x["GuideNumber"].isdigit() else 9999)
        return jsonify(lineup)

    # --- Remaining ---
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

    def _restart_scheduler():
        from .scheduler import restart_scheduler
        def trigger():
            if not app.scan_running: app.run_scan_thread()
            else: logger.info("Scan already running, skipping")
        app.scheduler = restart_scheduler(app.scheduler, config, trigger)

    return app


# =====================================================================
#  HELPERS
# =====================================================================

def _save(config, path):
    d = {k:v for k,v in config.items() if not k.startswith("_")}
    with open(path,"w") as f: yaml.dump(d, f, default_flow_style=False, sort_keys=False)

def _load_json(path, default=None):
    if path.exists():
        try:
            with open(path) as f: return json.load(f)
        except: pass
    return default if default is not None else {}

def _save_json(path, data):
    with open(path, "w") as f: json.dump(data, f, indent=2)

def _regenerate_m3u_mapped(scanner, config, channel_map):
    """Regenerate iptv_channels.m3u using Threadfin-style channel mappings.
    Uses custom names, group overrides, and EPG ID remapping."""
    live_dir = Path(config["paths"]["live_tv"])
    live_dir.mkdir(parents=True, exist_ok=True)
    m3u_path = live_dir / "iptv_channels.m3u"

    enabled = {sid: m for sid, m in channel_map.items() if m.get("enabled")}
    if not enabled:
        # Write empty M3U
        with open(m3u_path, "w") as f: f.write('#EXTM3U\n')
        return 0

    try:
        cats = scanner.client.get_live_categories()
        cat_map = {str(c.get("category_id","") or ""): c.get("category_name","") for c in cats}
        all_ch = scanner.client.get_live_streams()
    except Exception as e:
        logger.error(f"Failed to fetch channels: {e}")
        return 0

    # Build lookup: stream_id -> provider channel data
    ch_lookup = {}
    for ch in all_ch:
        ch_lookup[str(ch.get("stream_id", ""))] = ch

    # Sort by channel number
    sorted_mapped = sorted(enabled.items(), key=lambda x: x[1].get("ch_num", 9999))

    count = 0
    with open(m3u_path, "w", encoding="utf-8") as f:
        f.write('#EXTM3U\n')
        for sid, mapping in sorted_mapped:
            ch = ch_lookup.get(sid)
            if not ch:
                continue
            # Apply mapping overrides
            name = mapping.get("custom_name") or ch.get("name", "Unknown")
            logo = ch.get("stream_icon", "")
            cat_id = str(ch.get("category_id", "") or "")
            group = mapping.get("group") or cat_map.get(cat_id, "")
            epg_id = mapping.get("epg_id") or ch.get("epg_channel_id", "")
            ch_num = mapping.get("ch_num", count + 1)

            f.write(f'#EXTINF:-1 tvg-id="{epg_id}" tvg-name="{name}" tvg-logo="{logo}" '
                    f'tvg-chno="{ch_num}" group-title="{group}",{name}\n')
            f.write(scanner.client.build_live_url(int(sid)) + '\n')
            count += 1

    logger.info(f"Regenerated M3U with {count} mapped channels")

    # Regenerate filtered EPG
    if count > 0:
        wanted_ids = set()
        for sid, mapping in enabled.items():
            ch = ch_lookup.get(sid)
            if ch:
                eid = mapping.get("epg_id") or ch.get("epg_channel_id", "")
                if eid:
                    wanted_ids.add(eid)
        if wanted_ids:
            try:
                from .scanner import IPTVScanner
                raw_path = live_dir / "epg_raw.xml"
                epg_path = live_dir / "epg.xml"
                resp = scanner.client.session.get(scanner.client.get_epg_url(), timeout=120, stream=True)
                resp.raise_for_status()
                with open(raw_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=65536): f.write(chunk)
                IPTVScanner._filter_epg(raw_path, epg_path, wanted_ids)
                try: raw_path.unlink()
                except: pass
            except Exception as e:
                logger.error(f"EPG regen failed: {e}")

    return count

def _get_playlist_items(pl, state):
    """Get items matching a playlist's filter criteria."""
    ctype = pl.get("type", "movies")
    platform = pl.get("platform", "")
    tags = pl.get("tags", [])
    quality_min = pl.get("quality_min", 0)
    search = pl.get("search", "").lower()
    items = []
    for sid, info in state.get(ctype, {}).items():
        if platform and info.get("platform", "") != platform:
            continue
        if quality_min and info.get("quality", 720) < quality_min:
            continue
        if search:
            t = (info.get("title", "") or info.get("name", "")).lower()
            if search not in t:
                continue
        if tags:
            item_tags = info.get("tags", [])
            if not any(t in item_tags for t in tags):
                continue
        items.append(info)
    items.sort(key=lambda x: x.get("title", "").lower())
    return items

def _count_playlist_items(pl, state):
    return len(_get_playlist_items(pl, state))

def _generate_playlist_file(pl, state, playlists_dir, config):
    """Generate an M3U playlist file for Jellyfin."""
    items = _get_playlist_items(pl, state)
    safe_name = re.sub(r'[^a-zA-Z0-9_\- ]', '', pl.get("name", "playlist")).strip()
    filename = f"{pl['id']}_{safe_name}.m3u"
    filepath = playlists_dir / filename

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(f'#EXTM3U\n')
        f.write(f'#PLAYLIST:{pl.get("name", "Playlist")}\n')
        ctype = pl.get("type", "movies")
        for info in items:
            title = info.get("title", "") or info.get("name", "Unknown")
            year = info.get("year", "")
            label = f"{title} ({year})" if year else title
            # Point to the .strm file path
            strm = info.get("strm_path", "")
            if strm:
                f.write(f'#EXTINF:-1,{label}\n')
                f.write(f'{strm}\n')
            else:
                # Fallback to stream URL
                url = info.get("stream_url", "")
                if url:
                    f.write(f'#EXTINF:-1,{label}\n')
                    f.write(f'{url}\n')

    logger.info(f"Playlist '{pl.get('name','')}': {len(items)} items -> {filepath}")
    return len(items)
