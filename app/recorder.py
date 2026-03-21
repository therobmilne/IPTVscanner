"""
DVR Recorder -- simple recording scheduler for live TV channels.
Piggybacks on the restream proxy: joins a stream as a "client" and writes chunks to a file.
"""
import json
import logging
import queue
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


class Recording:
    """A single recording entry (scheduled, active, or completed)."""

    def __init__(self, channel_key: str, channel_name: str,
                 start_time: str, end_time: str, recurring: str = "none"):
        self.id = str(uuid.uuid4())[:8]
        self.channel_key = channel_key
        self.channel_name = channel_name
        self.start_time = start_time  # ISO format
        self.end_time = end_time      # ISO format
        self.recurring = recurring    # "none", "daily", "weekly"
        self.status = "scheduled"     # scheduled, recording, completed, failed, cancelled
        self.output_path = ""
        self.file_size = 0
        self.error = ""

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "channel_key": self.channel_key,
            "channel_name": self.channel_name,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "recurring": self.recurring,
            "status": self.status,
            "output_path": self.output_path,
            "file_size": self.file_size,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Recording":
        r = cls(d["channel_key"], d["channel_name"], d["start_time"], d["end_time"], d.get("recurring", "none"))
        r.id = d["id"]
        r.status = d.get("status", "scheduled")
        r.output_path = d.get("output_path", "")
        r.file_size = d.get("file_size", 0)
        r.error = d.get("error", "")
        return r


class DVRRecorder:
    """Manages scheduled and active recordings using the restream proxy."""

    def __init__(self, config: dict, proxy):
        self.config = config
        self.proxy = proxy
        dvr_config = config.get("dvr", {})
        self.enabled = dvr_config.get("enabled", True)
        self.max_concurrent = dvr_config.get("max_concurrent", 2)

        # Recordings directory
        data_dir = Path(config["paths"].get("data_dir", "./data"))
        self.recordings_dir = Path(dvr_config.get("recordings_dir", str(data_dir / "recordings")))
        self.recordings_dir.mkdir(parents=True, exist_ok=True)

        # State
        self.state_file = data_dir / "recordings.json"
        self.recordings: dict[str, Recording] = {}
        self._active_threads: dict[str, threading.Thread] = {}
        self._stop_flags: dict[str, bool] = {}
        self._load_state()

        # Start the scheduling loop
        self._scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._scheduler_thread.start()

    def _load_state(self):
        if self.state_file.exists():
            try:
                with open(self.state_file) as f:
                    data = json.load(f)
                for d in data:
                    r = Recording.from_dict(d)
                    self.recordings[r.id] = r
            except Exception as e:
                logger.warning(f"Could not load recordings state: {e}")

    def _save_state(self):
        try:
            with open(self.state_file, "w") as f:
                json.dump([r.to_dict() for r in self.recordings.values()], f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save recordings state: {e}")

    def schedule_recording(self, channel_key: str, channel_name: str,
                           start_time: str, end_time: str, recurring: str = "none") -> Recording:
        """Schedule a new recording."""
        active_count = sum(1 for r in self.recordings.values() if r.status == "recording")
        if active_count >= self.max_concurrent:
            raise ValueError(f"Max concurrent recordings ({self.max_concurrent}) reached")

        rec = Recording(channel_key, channel_name, start_time, end_time, recurring)

        # Generate output filename
        safe_name = "".join(c if c.isalnum() or c in " -_" else "" for c in channel_name).strip().replace(" ", "_")
        ts = datetime.fromisoformat(start_time).strftime("%Y%m%d_%H%M")
        rec.output_path = str(self.recordings_dir / f"{safe_name}_{ts}.ts")

        self.recordings[rec.id] = rec
        self._save_state()
        logger.info(f"Recording scheduled: {rec.channel_name} [{rec.start_time} -> {rec.end_time}] id={rec.id}")
        return rec

    def cancel_recording(self, recording_id: str) -> bool:
        """Cancel a scheduled or active recording."""
        rec = self.recordings.get(recording_id)
        if not rec:
            return False

        if rec.status == "recording":
            self._stop_flags[recording_id] = True
            rec.status = "cancelled"
        elif rec.status == "scheduled":
            rec.status = "cancelled"

        self._save_state()
        logger.info(f"Recording cancelled: {rec.channel_name} id={recording_id}")
        return True

    def delete_recording(self, recording_id: str) -> bool:
        """Delete a recording entry and optionally its file."""
        rec = self.recordings.get(recording_id)
        if not rec:
            return False

        if rec.status == "recording":
            self._stop_flags[recording_id] = True

        # Delete the file if it exists
        if rec.output_path:
            try:
                Path(rec.output_path).unlink(missing_ok=True)
            except Exception:
                pass

        del self.recordings[recording_id]
        self._save_state()
        return True

    def get_recordings(self) -> list[dict]:
        """Return all recordings sorted by start time (newest first)."""
        recs = sorted(self.recordings.values(), key=lambda r: r.start_time, reverse=True)
        return [r.to_dict() for r in recs]

    def _scheduler_loop(self):
        """Background loop that checks for recordings that need to start."""
        while True:
            try:
                now = datetime.now(timezone.utc)
                for rec in list(self.recordings.values()):
                    if rec.status != "scheduled":
                        continue

                    start = datetime.fromisoformat(rec.start_time)
                    if start.tzinfo is None:
                        start = start.replace(tzinfo=timezone.utc)

                    # Start recording if it's time (within 30 second window)
                    diff = (start - now).total_seconds()
                    if diff <= 30:
                        self._start_recording(rec)

            except Exception as e:
                logger.error(f"Scheduler loop error: {e}")

            time.sleep(10)

    def _start_recording(self, rec: Recording):
        """Start the actual recording in a background thread."""
        if not self.proxy:
            rec.status = "failed"
            rec.error = "Proxy not available"
            self._save_state()
            return

        active_count = sum(1 for r in self.recordings.values() if r.status == "recording")
        if active_count >= self.max_concurrent:
            rec.status = "failed"
            rec.error = "Max concurrent recordings reached"
            self._save_state()
            return

        rec.status = "recording"
        self._stop_flags[rec.id] = False
        self._save_state()

        thread = threading.Thread(target=self._do_record, args=(rec,), daemon=True, name=f"dvr-{rec.id}")
        self._active_threads[rec.id] = thread
        thread.start()

    def _do_record(self, rec: Recording):
        """Worker thread: join stream via proxy and write chunks to file."""
        client_id = f"dvr-{rec.id}"
        active = None

        try:
            end = datetime.fromisoformat(rec.end_time)
            if end.tzinfo is None:
                end = end.replace(tzinfo=timezone.utc)

            logger.info(f"Recording started: {rec.channel_name} -> {rec.output_path}")

            active = self.proxy.get_or_create_stream(rec.channel_key)
            q = active.add_client(client_id)

            with open(rec.output_path, "wb") as f:
                while not self._stop_flags.get(rec.id, False):
                    now = datetime.now(timezone.utc)
                    if now >= end:
                        logger.info(f"Recording end time reached: {rec.channel_name}")
                        break

                    try:
                        chunk = q.get(timeout=30)
                    except queue.Empty:
                        if not active.running:
                            rec.error = "Stream ended unexpectedly"
                            break
                        continue

                    if chunk is None:
                        rec.error = "Stream ended"
                        break

                    f.write(chunk)

            # Update file size
            output = Path(rec.output_path)
            if output.exists():
                rec.file_size = output.stat().st_size

            if not rec.error and not self._stop_flags.get(rec.id, False):
                rec.status = "completed"
                logger.info(f"Recording completed: {rec.channel_name} ({rec.file_size / (1024*1024):.1f} MB)")
            elif rec.status != "cancelled":
                rec.status = "failed"

        except Exception as e:
            rec.status = "failed"
            rec.error = str(e)
            logger.error(f"Recording error: {rec.channel_name} - {e}")
        finally:
            if active:
                active.remove_client(client_id)
            self._stop_flags.pop(rec.id, None)
            self._active_threads.pop(rec.id, None)
            self._save_state()

            # Handle recurring recordings
            if rec.recurring != "none" and rec.status == "completed":
                self._schedule_next(rec)

    def _schedule_next(self, rec: Recording):
        """Schedule the next occurrence of a recurring recording."""
        from datetime import timedelta

        start = datetime.fromisoformat(rec.start_time)
        end = datetime.fromisoformat(rec.end_time)
        duration = end - start

        if rec.recurring == "daily":
            next_start = start + timedelta(days=1)
        elif rec.recurring == "weekly":
            next_start = start + timedelta(weeks=1)
        else:
            return

        next_end = next_start + duration
        self.schedule_recording(
            rec.channel_key, rec.channel_name,
            next_start.isoformat(), next_end.isoformat(),
            rec.recurring
        )
