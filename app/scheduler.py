"""
Shared scan scheduler -- used by both dashboard and CLI.
"""
import logging

logger = logging.getLogger(__name__)


def create_scheduler(config: dict, scan_fn):
    """Create and start an APScheduler BackgroundScheduler from config.
    scan_fn is called when the scheduled time fires (should be non-blocking or launch a thread)."""
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
    except ImportError:
        logger.warning("APScheduler not installed — pip install apscheduler")
        return None

    sched_config = config.get("schedule", {})
    if not sched_config.get("enabled", False):
        logger.info("Scheduled scanning disabled")
        return None

    scan_time = sched_config.get("scan_time", "03:30")
    try:
        hour, minute = map(int, scan_time.split(":"))
    except ValueError:
        hour, minute = 3, 30

    scheduler = BackgroundScheduler()
    freq = sched_config.get("frequency", "daily")

    def _run():
        logger.info("=== SCHEDULED SCAN triggered ===")
        scan_fn()

    if freq == "weekly":
        scheduler.add_job(_run, 'cron', day_of_week='mon', hour=hour, minute=minute, id='iptv_scan', replace_existing=True)
    elif freq == "monthly":
        scheduler.add_job(_run, 'cron', day=1, hour=hour, minute=minute, id='iptv_scan', replace_existing=True)
    elif freq == "interval":
        interval_hours = sched_config.get("interval_hours", 6)
        scheduler.add_job(_run, 'interval', hours=interval_hours, id='iptv_scan', replace_existing=True)
    else:
        scheduler.add_job(_run, 'cron', hour=hour, minute=minute, id='iptv_scan', replace_existing=True)

    scheduler.start()
    logger.info(f"Scheduler started: {freq} at {scan_time}")
    return scheduler


def restart_scheduler(old_scheduler, config: dict, scan_fn):
    """Stop old scheduler and create a new one with current config."""
    if old_scheduler:
        try:
            old_scheduler.shutdown(wait=False)
        except Exception:
            pass
        logger.info("Old scheduler stopped")
    return create_scheduler(config, scan_fn)
