#!/usr/bin/env python3
import argparse
import os
import re
import shlex
import signal
import subprocess
import sys
import threading
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

BBOX_W = -45
BBOX_E = 56
BBOX_S = 22
BBOX_N = 82
TOTAL_COLS = BBOX_E - BBOX_W
TOTAL_ROWS = BBOX_N - BBOX_S
BAD_EXPECTED_FALLBACK = {"N60E007", "N64E028"}
WATCH_AREAS = {
    "P32": {"lon_min": 6, "lon_max": 11, "lat_min": 60, "lat_max": 63},
    "Q35": {"lon_min": 24, "lon_max": 29, "lat_min": 64, "lat_max": 67},
}
DEFAULT_REPO_ROOT = str(Path(__file__).resolve().parent)
PROGRESS_RE = re.compile(r"^\[(?P<stamp>[^\]]+)\].*\bC:\s+.*\bW:\s+.*\bR:\s+")
CONTOUR_RE = re.compile(
    r'^(?:(?P<ts>\S+)\s+)?hgt file (?:\S*/)?(?P<view>VIEW[13])/(?P<tile>[NS]\d{2}[EW]\d{3})\.hgt: '
    r'(?P<x>\d+)\s+x\s+(?P<y>\d+)\s+points, bbox: '
    r'\((?P<lon1>-?\d+\.\d+),\s*(?P<lat1>-?\d+\.\d+),\s*(?P<lon2>-?\d+\.\d+),\s*(?P<lat2>-?\d+\.\d+)\)'
)
CONTOUR_SCAN_RE = re.compile(r'^(?:(?P<ts>\S+)\s+)?(?P<tile>[NS]\d{2}[EW]\d{3}):\s+(?P<message>.+)$')


class StageError(RuntimeError):
    pass


class InterruptController:
    def __init__(self):
        self.requested = False
        self._count = 0

    def install(self):
        signal.signal(signal.SIGINT, self._handle_sigint)

    def _handle_sigint(self, signum, frame):
        self._count += 1
        if self._count == 1:
            self.requested = True
            return
        raise KeyboardInterrupt


@dataclass
class StageRecord:
    id: str
    title: str
    optional: bool = False
    enabled: bool = True
    status: str = "pending"
    command: str = ""
    note: str = ""
    info_lines: list[str] = field(default_factory=list)
    recent_lines: deque = field(default_factory=lambda: deque(maxlen=12))
    started_at: Optional[float] = None
    ended_at: Optional[float] = None
    error: Optional[str] = None
    detail: Optional[object] = None

    def elapsed_seconds(self):
        if self.started_at is None:
            return None
        end = self.ended_at if self.ended_at is not None else time.time()
        return max(end - self.started_at, 0.0)


@dataclass
class StageDefinition:
    id: str
    title: str
    runner: Callable[["PipelineContext", StageRecord], None]
    optional: bool = False
    enabled: bool = True


@dataclass
class TrackedContainer:
    service: str
    container_id: str
    name: str
    image_tag: str
    image_id: str


@dataclass
class TrackedImage:
    tag: str
    before_id: Optional[str]
    after_id: str


class PipelineDashboard:
    def __init__(self, enabled: bool, pipeline_name: str):
        self.enabled = enabled
        self.pipeline_name = pipeline_name

    def render(self, stages: list[StageRecord], active_stage: Optional[StageRecord], fatal_message: Optional[str] = None):
        if not self.enabled:
            return
        clear_screen()
        total = len(stages)
        completed = sum(1 for stage in stages if stage.status in {"completed", "skipped"})
        running_index = next((idx for idx, stage in enumerate(stages, start=1) if stage.status == "running"), None)
        if running_index is None:
            running_index = min(completed + 1, total) if total else 0
        overall_pct = completed / float(max(total, 1))
        now = datetime.now(timezone.utc)
        total_started = [stage.started_at for stage in stages if stage.started_at is not None]
        total_elapsed = fmt_duration(max(time.time() - min(total_started), 0.0)) if total_started else "n/a"
        active_status = active_stage.status if active_stage else "pending"
        active_command = active_stage.command if active_stage and active_stage.command else "n/a"

        lines = [
            self.pipeline_name,
            "=" * 88,
            f"Step:              {running_index}/{total}  {active_stage.title if active_stage else 'waiting'}",
            f"Stage status:      {active_status}",
            f"Elapsed total:     {total_elapsed}",
            f"Active command:    {active_command}",
            f"Updated:           {fmt_time(now)}",
            bar(overall_pct, 44),
            "",
            "Pipeline Steps:",
        ]

        for idx, stage in enumerate(stages, start=1):
            marker = "=>" if active_stage is stage else "  "
            suffix = f" ({fmt_duration(stage.elapsed_seconds())})" if stage.elapsed_seconds() is not None else ""
            lines.append(f"{marker} {idx:02d}. {stage.title:<30} {stage.status}{suffix}")

        lines.append("")
        if active_stage:
            lines.append(f"{active_stage.title} Details")
            lines.append("-" * 88)
            detail_lines = build_detail_lines(active_stage)
            lines.extend(detail_lines)
        if fatal_message:
            lines.extend(["", "Failure:", fatal_message])
        sys.stdout.write("\033[2J\033[H" + "\n".join(lines) + "\n")
        sys.stdout.flush()

    def close(self):
        if not self.enabled:
            return
        sys.stdout.write("\033[2J\033[H")
        sys.stdout.flush()


class GenericDetail:
    def handle_line(self, line: str, stage: StageRecord):
        return

    def tick(self, ctx: "PipelineContext", stage: StageRecord):
        return

    def lines(self, stage: StageRecord) -> list[str]:
        lines = []
        if stage.note:
            lines.append(stage.note)
        lines.extend(stage.info_lines)
        if stage.recent_lines:
            lines.append("")
            lines.append("Recent output:")
            lines.extend(f"  {line}" for line in list(stage.recent_lines)[-8:])
        return lines or ["No detail available yet."]


class CompletionDetail(GenericDetail):
    def __init__(self, lines: list[str]):
        self._lines = lines

    def lines(self, stage: StageRecord) -> list[str]:
        return list(self._lines)


class ContourDetail(GenericDetail):
    def __init__(self, repo_root: Path):
        self.repo_root = repo_root
        self.project_name = repo_root.name
        self.events = []
        self.tiles = {}
        self.scan_events = []
        self.scan_tiles = {}
        self.latest = None
        self.latest_scan = None
        self.started = None
        self.last_line_seen = None
        self.last_non_hgt_lines = []
        self.container = None
        self.container_status = None
        self.container_exit_code = None
        self.worker_processes = None
        self.active_workers = None
        self.configured_jobs = None

    def handle_line(self, line: str, stage: StageRecord):
        stripped = line.strip()
        if not stripped:
            return
        if "hgt file" in stripped:
            event = self._parse_event(stripped)
            if event is None:
                return
            key = (event["lon"], event["lat"])
            self.tiles[key] = event
            self.events.append(event)
            self.latest = event
            self.last_line_seen = time.time()
            if self.started is None or event["ts"] < self.started:
                self.started = event["ts"]
            return
        scan = self._parse_scan(stripped)
        if scan is not None:
            key = (scan["lon"], scan["lat"])
            if key not in self.scan_tiles:
                self.scan_tiles[key] = scan
                self.scan_events.append(scan)
                if self.started is None or scan["ts"] < self.started:
                    self.started = scan["ts"]
            self.latest_scan = scan
            self.last_line_seen = time.time()
        if not self.last_non_hgt_lines or self.last_non_hgt_lines[-1] != stripped:
            self.last_non_hgt_lines.append(stripped)
            del self.last_non_hgt_lines[:-8]

    def tick(self, ctx: "PipelineContext", stage: StageRecord):
        pattern = f"{self.project_name}-generate-osm-contours-run-"
        container, status = detect_container(pattern)
        if container:
            self.container = container
            self.container_status, self.container_exit_code = inspect_container(container)
            self.worker_processes, self.active_workers, self.configured_jobs = inspect_workers(container)
        else:
            self.container_status = status

    def lines(self, stage: StageRecord) -> list[str]:
        data_state = scan_data_dir(self.repo_root / "data")
        lines = [
            f"Container:         {self.container or 'waiting for contour container'}",
            f"Container status:  {self.container_status or 'n/a'}",
        ]
        if self.container_exit_code is not None and self.container_status not in ("running", "paused", "restarting"):
            lines.append(f"Exit code:         {self.container_exit_code}")
        if self.configured_jobs is not None or self.active_workers is not None:
            lines.append(
                f"Workers:           active={self.active_workers if self.active_workers is not None else 'n/a'} "
                f"jobs={self.configured_jobs if self.configured_jobs is not None else 'n/a'} "
                f"phyghtmap_procs={self.worker_processes if self.worker_processes is not None else 'n/a'}"
            )
        if data_state["pbf_files"]:
            latest_pbf = max(data_state["pbf_files"], key=lambda item: item["mtime"])
            lines.append(f"PBF output:        {latest_pbf['name']} ({human_size(latest_pbf['size'])})")
        else:
            lines.append("PBF output:        not created yet")
        lines.append("")
        if not self.latest and not self.latest_scan:
            lines.append("No parsed contour progress lines yet.")
            if self.last_non_hgt_lines:
                lines.append("Recent non-HGT log lines:")
                lines.extend(f"  {line}" for line in self.last_non_hgt_lines[-5:])
            return lines

        latest_progress = self.latest or self.latest_scan
        processed_tiles = len(self.tiles)
        scanned_tiles = len(self.scan_tiles)
        total_tiles = TOTAL_COLS * TOTAL_ROWS
        progress = processed_tiles / total_tiles if total_tiles else 0.0
        elapsed = (latest_progress["ts"] - self.started).total_seconds() if self.started and latest_progress else None
        overall_speed = processed_tiles / (elapsed / 3600.0) if elapsed and elapsed > 0 else None
        speed_30 = speed_from_events(self.events, 30)
        speed_60 = speed_from_events(self.events, 60)
        speed = speed_30 or speed_60 or overall_speed
        remaining = max(total_tiles - processed_tiles, 0)
        eta = remaining / speed * 3600.0 if speed and speed > 0 else None
        finish = datetime.now(timezone.utc) + timedelta(seconds=eta) if eta else None
        latest_age = time.time() - self.last_line_seen if self.last_line_seen else None
        computed_lons = len({lon for lon, _lat in self.tiles.keys()})
        scanned_lons = len({lon for lon, _lat in self.scan_tiles.keys()})
        views = Counter(ev["view"] for ev in self.tiles.values())

        lines.append(bar(progress, 40))
        if self.latest:
            lines.extend(
                [
                    f"Latest computed:   {self.latest['tile']}  {self.latest['view']}  {self.latest['points_x']}x{self.latest['points_y']}",
                    f"Latest bbox lon:   {self.latest['lon']} to {self.latest['lon'] + 1}",
                    f"Latest bbox lat:   {self.latest['lat']} to {self.latest['lat'] + 1}",
                    f"Latest log time:   {fmt_time(self.latest['ts'])}",
                ]
            )
        elif self.latest_scan:
            lines.extend(
                [
                    "Latest computed:   pending",
                    f"Latest bbox lon:   {self.latest_scan['lon']} to {self.latest_scan['lon'] + 1}",
                    f"Latest bbox lat:   {self.latest_scan['lat']} to {self.latest_scan['lat'] + 1}",
                    f"Latest log time:   {fmt_time(self.latest_scan['ts'])}",
                ]
            )
        if self.latest_scan:
            lines.append(f"Latest scan tile:  {self.latest_scan['tile']}")
        lines.extend(
            [
                f"Latest line age:   {fmt_duration(latest_age)}",
                "",
                f"Tile progress:      {processed_tiles} / {total_tiles}",
                f"Scan coverage:      {scanned_tiles} / {total_tiles}",
                f"Columns computed:   {computed_lons} / {TOTAL_COLS}",
                f"Columns scanned:    {scanned_lons} / {TOTAL_COLS}",
                f"Elapsed contour:    {fmt_duration(elapsed)}",
                f"Speed overall:      {overall_speed:.2f} tiles/hour" if overall_speed else "Speed overall:      n/a",
                f"Speed last 30 min:  {speed_30:.2f} tiles/hour" if speed_30 else "Speed last 30 min:  n/a",
                f"Speed last 60 min:  {speed_60:.2f} tiles/hour" if speed_60 else "Speed last 60 min:  n/a",
                "",
                f"ETA remaining:      {fmt_duration(eta)}",
                f"Estimated finish:   {fmt_time(finish) if finish else 'n/a'}",
                "",
                f"Computed HGT tiles: {len(self.tiles)}",
                f"VIEW1 tiles:        {views.get('VIEW1', 0)}",
                f"VIEW3 tiles:        {views.get('VIEW3', 0)}",
                "",
                "Repair watch:",
            ]
        )
        for name, area in WATCH_AREAS.items():
            lines.append(f"  {area_status(name, area, self.tiles)}")
        if self.last_non_hgt_lines:
            lines.extend(["", "Recent non-HGT log lines:"])
            lines.extend(f"  {line}" for line in self.last_non_hgt_lines[-5:])
        return lines

    def _parse_event(self, line: str):
        match = CONTOUR_RE.search(line)
        if not match:
            return None
        ts = parse_ts(match.group("ts"))
        lon = int(float(match.group("lon1")))
        lat = int(float(match.group("lat1")))
        return {
            "ts": ts,
            "lon": lon,
            "lat": lat,
            "view": match.group("view"),
            "tile": match.group("tile"),
            "points_x": int(match.group("x")),
            "points_y": int(match.group("y")),
            "pos": tile_position(lon, lat),
        }

    def _parse_scan(self, line: str):
        match = CONTOUR_SCAN_RE.search(line)
        if not match:
            return None
        ts = parse_ts(match.group("ts"))
        tile = match.group("tile")
        lon, lat = tile_to_lon_lat(tile)
        return {
            "ts": ts,
            "lon": lon,
            "lat": lat,
            "tile": tile,
            "message": match.group("message"),
            "pos": tile_position(lon, lat),
        }


class ImposmDetail(GenericDetail):
    def __init__(self):
        self.phase = "starting"
        self.latest_progress = None
        self.latest_status = None
        self.reading_took = None
        self.writing_took = None
        self.last_timestamp = None

    def handle_line(self, line: str, stage: StageRecord):
        stripped = line.strip()
        if not stripped:
            return
        if "Importing in normal mode" in stripped:
            self.phase = "starting"
            self.latest_status = stripped
            return
        if "Reading OSM data took:" in stripped:
            self.phase = "writing"
            self.reading_took = stripped.split("Reading OSM data took:", 1)[1].strip()
            self.latest_status = stripped
            return
        if "Writing OSM data took:" in stripped:
            self.phase = "indexing"
            self.writing_took = stripped.split("Writing OSM data took:", 1)[1].strip()
            self.latest_status = stripped
            return
        if "Creating geometry index" in stripped or "Creating generalized tables" in stripped:
            self.phase = "indexing"
            self.latest_status = stripped
            return
        if PROGRESS_RE.search(stripped):
            self.latest_progress = stripped
            if self.reading_took is None:
                self.phase = "reading"
            elif self.writing_took is None:
                self.phase = "writing"
            self.last_timestamp = stripped[: stripped.find("]") + 1] if stripped.startswith("[") else None
            return
        self.latest_status = stripped

    def lines(self, stage: StageRecord) -> list[str]:
        lines = [f"Phase:             {self.phase}"]
        if self.reading_took:
            lines.append(f"Read duration:     {self.reading_took}")
        if self.writing_took:
            lines.append(f"Write duration:    {self.writing_took}")
        if self.latest_progress:
            lines.extend(["", "Latest progress:", f"  {self.latest_progress}"])
        if self.latest_status:
            lines.extend(["", "Latest status:", f"  {self.latest_status}"])
        if stage.recent_lines:
            lines.extend(["", "Recent output:"])
            lines.extend(f"  {line}" for line in list(stage.recent_lines)[-8:])
        return lines


class MBTilesDetail(GenericDetail):
    def __init__(self, manifest_path: Path):
        self.manifest_path = manifest_path
        self.manifest = None
        self.active_lines = []
        self.summary_lines = []

    def handle_line(self, line: str, stage: StageRecord):
        if not line.strip():
            return

    def load_summary(self, shard):
        summary_path = shard.get("summary_path")
        if not summary_path:
            return shard.get("summary") or {}
        summary_file = Path(summary_path)
        if not summary_file.exists():
            return shard.get("summary") or {}
        try:
            return json_load(summary_file)
        except Exception:
            return shard.get("summary") or {}

    def tick(self, ctx: "PipelineContext", stage: StageRecord):
        if not self.manifest_path.exists():
            self.manifest = None
            return
        try:
            self.manifest = json_load(self.manifest_path)
        except Exception:
            self.manifest = None
            return
        manifest = self.manifest
        total_tiles = sum(shard.get("tile_count", 0) for shard in manifest["shards"].values())
        processed = inserted = skipped = errors = retries = visible_size = 0
        counts = {"pending": 0, "running": 0, "done": 0, "failed": 0, "merged": 0}
        active_lines = []
        for shard_id, shard in sorted(manifest["shards"].items(), key=lambda item: (item[1]["zoom"], item[1]["x_start"])):
            counts[shard["status"]] = counts.get(shard["status"], 0) + 1
            summary = shard.get("summary") or {}
            if shard["status"] == "running":
                summary = self.load_summary(shard)
                shard["summary"] = summary
            processed += summary.get("processed", 0)
            inserted += summary.get("inserted", 0)
            skipped += summary.get("skipped", 0)
            errors += summary.get("errors", 0)
            retries += summary.get("retries", 0)
            visible_size += summary.get("visible_file_size", summary.get("file_size", 0))
            if shard["status"] == "running":
                shard_total = summary.get("total_tiles", shard.get("tile_count", 0))
                shard_processed = summary.get("processed", 0)
                percent = shard_processed / float(max(shard_total, 1))
                active_lines.append(
                    f"{shard_id:<16} {percent * 100.0:6.2f}% {shard_processed}/{shard_total} "
                    f"ins={summary.get('inserted', 0)} skip={summary.get('skipped', 0)} err={summary.get('errors', 0)} "
                    f"tile=z{summary.get('current_zoom', shard['zoom'])} x{summary.get('current_x', shard['x_start'])} "
                    f"y{summary.get('current_y', shard['y_start'])} try={summary.get('current_attempt', 1)}/{summary.get('total_attempts', 1)}"
                )
        elapsed = max(time.time() - min(stage.started_at or time.time(), time.time()), 0.0)
        rate = processed / elapsed if elapsed > 0 else 0.0
        remaining = max(total_tiles - processed, 0)
        eta = remaining / rate if rate > 0 else None
        percent = processed / float(max(total_tiles, 1))
        self.active_lines = active_lines
        self.summary_lines = [
            f"{bar(percent, 40)} {percent * 100.0:6.2f}% {processed}/{total_tiles}",
            f"pending={counts.get('pending', 0)} running={counts.get('running', 0)} done={counts.get('done', 0)} failed={counts.get('failed', 0)} merged={counts.get('merged', 0)}",
            f"ins={inserted} skip={skipped} err={errors} ret={retries} rate={rate:.2f}/s eta={fmt_duration(eta)} size={human_size(visible_size)}",
        ]

    def lines(self, stage: StageRecord) -> list[str]:
        if self.manifest is None:
            return GenericDetail().lines(stage)
        lines = list(self.summary_lines)
        lines.extend(["", f"Manifest:          {self.manifest_path}"])
        lines.append(f"Updated at:        {self.manifest.get('updated_at', 'n/a')}")
        lines.append("")
        lines.append("Active shards:")
        if self.active_lines:
            lines.extend(f"  {line}" for line in self.active_lines[:12])
        else:
            lines.append("  none")
        return lines


class PipelineContext:
    def __init__(self, args, repo_root: Path, pipeline_name: str, stages: list[StageRecord]):
        self.args = args
        self.repo_root = repo_root
        self.project_name = repo_root.name
        self.pipeline_name = pipeline_name
        self.stages = stages
        self.dashboard = PipelineDashboard(sys.stdout.isatty(), pipeline_name)
        self.active_stage = None
        self.interrupts = InterruptController()
        self.interrupts.install()
        self.cleanup_actions = []
        self.tracked_containers = {}
        self.tracked_images = {}
        self.post_run_notes = []
        self.cleanup_results = []

    def render(self, fatal_message: Optional[str] = None):
        if self.active_stage and self.active_stage.detail and hasattr(self.active_stage.detail, "tick"):
            self.active_stage.detail.tick(self, self.active_stage)
        self.dashboard.render(self.stages, self.active_stage, fatal_message=fatal_message)

    def register_cleanup(self, action: Callable[[], None]):
        self.cleanup_actions.append(action)

    def run_cleanup(self):
        errors = []
        while self.cleanup_actions:
            action = self.cleanup_actions.pop()
            try:
                action()
            except Exception as exc:
                errors.append(str(exc))
        return errors

    def is_interactive_run(self) -> bool:
        return self.dashboard.enabled and getattr(self.args, "command", "") == "run"

    def track_service_containers(self, service: str, before_ids: list[str], after_ids: list[str]):
        new_ids = [container_id for container_id in after_ids if container_id and container_id not in before_ids]
        for container_id in new_ids:
            metadata = inspect_container_metadata(container_id)
            if metadata is None:
                continue
            self.tracked_containers[container_id] = TrackedContainer(
                service=service,
                container_id=container_id,
                name=metadata["name"],
                image_tag=metadata["image_tag"],
                image_id=metadata["image_id"],
            )

    def track_local_image(self, tag: str, before_id: Optional[str], after_id: Optional[str]):
        if not after_id or after_id == before_id:
            return
        existing = self.tracked_images.get(tag)
        if existing is None:
            self.tracked_images[tag] = TrackedImage(tag=tag, before_id=before_id, after_id=after_id)
            return
        existing.after_id = after_id
        if existing.before_id is None:
            existing.before_id = before_id

    def cleanup_candidates(self):
        containers = sorted(self.tracked_containers.values(), key=lambda item: (item.service, item.name))
        images = sorted(self.tracked_images.values(), key=lambda item: item.tag)
        return containers, images


class HelpFormatter(argparse.RawTextHelpFormatter, argparse.ArgumentDefaultsHelpFormatter):
    def _get_help_string(self, action):
        help_text = action.help or ""
        if '%(default)' in help_text:
            return help_text
        if action.default in (None, argparse.SUPPRESS):
            return help_text
        if action.required:
            return help_text
        return super()._get_help_string(action)


def preprocess_argv(argv):
    if len(argv) >= 2 and argv[0] == "run" and "--dry-run" in argv[1:]:
        forwarded = [arg for arg in argv[1:] if arg != "--dry-run"]
        return ["dry-run", *forwarded]
    return argv


def main():
    description = (
        "Run or validate the contourlines-generator pipeline.\n\n"
        "The wrapper orchestrates contour generation, build generation, PostGIS import, "
        "postserve startup, and parallel MBTiles export. Successful interactive runs "
        "can offer targeted cleanup of run-created containers and local images."
    )
    epilog = (
        "Examples:\n"
        "  python3 generator.py run --prefetch-contours --min-zoom 10 --max-zoom 14 --workers 16 --replicas 16\n"
        "  python3 generator.py dry-run\n"
        "  python3 generator.py run --dry-run\n"
        "\n"
        "Detailed subcommand help:\n"
        "  python3 generator.py run --help\n"
        "  python3 generator.py dry-run --help"
    )
    parser = argparse.ArgumentParser(
        description=description,
        epilog=epilog,
        formatter_class=HelpFormatter,
    )
    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
        title="subcommands",
        description=(
            "Use 'run' for the full pipeline and 'dry-run' for validation only.\n"
            "Real 'run' executions require --min-zoom and --max-zoom.\n"
            "The alias 'run --dry-run' is supported and does not require zoom arguments."
        ),
    )

    run_parser = subparsers.add_parser(
        "run",
        help="Run the full contour-to-MBTiles pipeline.",
        description=(
            "Run the full contour-to-MBTiles pipeline. Requires an explicit zoom range. "
            "Successful interactive runs can offer targeted cleanup for containers and "
            "local images created by that run."
        ),
        epilog="Example: python3 generator.py run --prefetch-contours --min-zoom 10 --max-zoom 14 --workers 16 --replicas 16",
        formatter_class=HelpFormatter,
    )
    add_common_run_args(run_parser)
    run_parser.add_argument("--prefetch-contours", action="store_true", help="Warm the persistent HGT cache before contour generation starts.")
    run_parser.add_argument("--dry-run", action="store_true", help="Alias for 'dry-run'. When present, validation runs without requiring --min-zoom or --max-zoom.")

    dry_run_parser = subparsers.add_parser(
        "dry-run",
        help="Validate builds, startup, readiness, and writability without running the heavy pipeline.",
        description="Validate local build, startup, readiness, and writability checks without running contour generation, import, or MBTiles export.",
        epilog="Example: python3 generator.py dry-run --replicas 1",
        formatter_class=HelpFormatter,
    )
    add_common_base_args(dry_run_parser)
    dry_run_parser.add_argument("--replicas", type=int, default=1, help="Number of postserve-worker replicas to start during the dry-run smoke test.")
    dry_run_parser.add_argument("--target-tiles-per-shard", type=int, default=20000, help="Shard size target used when validating export-related configuration.")
    dry_run_parser.add_argument("--stream-flags", default="--resume --skip-existing", help="Extra flags forwarded to the MBTiles export environment during validation-aware flows.")

    args = parser.parse_args(preprocess_argv(sys.argv[1:]))
    repo_root = Path(args.repo_root).resolve() if args.repo_root else Path(__file__).resolve().parent

    if args.command == "dry-run":
        run_pipeline(repo_root, args, build_dry_run_stage_definitions(args), "Contourlines Generator Dry Run")
        return
    run_pipeline(repo_root, args, build_run_stage_definitions(args), "Contourlines Generator Run")


def add_common_base_args(parser):
    parser.add_argument(
        "--repo-root",
        default=None,
        help="Repository root to use for the pipeline run or validation. Defaults to the directory containing generator.py.",
    )
    parser.add_argument("--interval", type=float, default=1.0, help="Dashboard refresh interval in seconds.")


def add_common_run_args(parser):
    add_common_base_args(parser)
    parser.add_argument("--min-zoom", type=int, required=True, help="Minimum zoom level to export into the final MBTiles output.")
    parser.add_argument("--max-zoom", type=int, required=True, help="Maximum zoom level to export into the final MBTiles output.")
    parser.add_argument("--workers", type=int, default=16, help="Number of contour generation jobs and parallel MBTiles shard workers to run.")
    parser.add_argument("--replicas", type=int, default=16, help="Number of postserve-worker containers to start for tile serving.")
    parser.add_argument("--target-tiles-per-shard", type=int, default=20000, help="Target number of tiles per shard before the export is split into more work units.")
    parser.add_argument("--stream-flags", default="--resume --skip-existing", help="Extra flags forwarded to the MBTiles export make target, for example resume-related flags.")


def build_run_stage_definitions(args):
    return [
        StageDefinition("preflight", "Preflight", stage_run_preflight),
        StageDefinition("prefetch", "Prefetch Contours", stage_run_prefetch, optional=True, enabled=args.prefetch_contours),
        StageDefinition("contours", "Generate Contours", stage_run_contours),
        StageDefinition("build-clean", "Build Clean", lambda ctx, stage: run_generic_command_stage(ctx, stage, ["docker-compose", "run", "--rm", "openmaptiles-tools", "make", "clean"], note="Cleaning generated build artifacts.")),
        StageDefinition("build-generate", "Build Generate", lambda ctx, stage: run_generic_command_stage(ctx, stage, ["docker-compose", "run", "--rm", "openmaptiles-tools", "make"], note="Regenerating build artifacts.")),
        StageDefinition("postgres", "Start Postgres", stage_run_postgres),
        StageDefinition("schema", "Reset Schema", lambda ctx, stage: run_generic_command_stage(ctx, stage, ["make", "forced-clean-sql"], note="Resetting Postgres schema.")),
        StageDefinition("import-osm", "Import OSM", stage_run_import_osm),
        StageDefinition("import-sql", "Import SQL", lambda ctx, stage: run_generic_command_stage(ctx, stage, ["docker-compose", "run", "--rm", "import-sql"], note="Running SQL postprocessing.")),
        StageDefinition("analyze", "Analyze", lambda ctx, stage: run_generic_command_stage(ctx, stage, ["make", "psql-analyze"], note="Running ANALYZE on Postgres.")),
        StageDefinition("postserve", "Start Postserve Pool", stage_run_postserve_pool),
        StageDefinition("mbtiles", "Generate MBTiles", stage_run_mbtiles),
        StageDefinition("verify", "Verify Output", lambda ctx, stage: run_generic_command_stage(ctx, stage, ["make", "verify-mbtiles-parallel"], env=build_stream_env(ctx.args, ctx.repo_root), note="Verifying final MBTiles and shard state.")),
    ]


def build_dry_run_stage_definitions(args):
    return [
        StageDefinition("preflight", "Preflight", stage_dry_run_preflight),
        StageDefinition("compose-config", "Compose Config", stage_dry_run_compose_config),
        StageDefinition("build-images", "Build Local Images", stage_dry_run_build_images),
        StageDefinition("container-smoke", "Container Smoke", stage_dry_run_container_smoke),
        StageDefinition("postgres", "Postgres Smoke", stage_dry_run_postgres_smoke),
        StageDefinition("postserve", "Postserve Smoke", stage_dry_run_postserve_smoke),
    ]


def run_pipeline(repo_root: Path, args, definitions: list[StageDefinition], pipeline_name: str):
    stages = [StageRecord(defn.id, defn.title, optional=defn.optional, enabled=defn.enabled) for defn in definitions]
    ctx = PipelineContext(args, repo_root, pipeline_name, stages)
    fatal_message = None
    try:
        for defn, stage in zip(definitions, stages):
            if not stage.enabled:
                stage.status = "skipped"
                continue
            execute_stage(ctx, defn, stage)
    except Exception as exc:
        fatal_message = str(exc)
        ctx.render(fatal_message=fatal_message)
        raise
    finally:
        cleanup_errors = ctx.run_cleanup()
        if cleanup_errors:
            fatal_message = (fatal_message + " | " if fatal_message else "") + "; ".join(cleanup_errors)
        ctx.render(fatal_message=fatal_message)
        interactive_success = fatal_message is None and ctx.is_interactive_run()
        if interactive_success:
            handle_successful_run_completion(ctx)
        else:
            ctx.dashboard.close()
            if fatal_message is None and getattr(args, "command", "") == "run" and not ctx.dashboard.enabled:
                ctx.post_run_notes.append("Interactive cleanup prompt not offered because stdout is not a TTY.")
            print_pipeline_summary(ctx, fatal_message)


def execute_stage(ctx: PipelineContext, definition: StageDefinition, stage: StageRecord):
    stage.status = "running"
    stage.started_at = time.time()
    ctx.active_stage = stage
    ctx.render()
    try:
        definition.runner(ctx, stage)
        if stage.status == "running":
            stage.status = "completed"
    except KeyboardInterrupt:
        stage.status = "failed"
        stage.error = "interrupted"
        raise StageError(f"stage interrupted: {stage.title}")
    except Exception as exc:
        stage.status = "failed"
        stage.error = str(exc)
        raise
    finally:
        stage.ended_at = time.time()
        ctx.render(fatal_message=stage.error if stage.status == "failed" else None)


def run_generic_command_stage(ctx: PipelineContext, stage: StageRecord, command: list[str], env: Optional[dict] = None, note: Optional[str] = None, detail: Optional[GenericDetail] = None):
    if note:
        stage.note = note
    stage.detail = detail or GenericDetail()
    run_live_command(ctx, stage, command, env=env)


def run_live_command(ctx: PipelineContext, stage: StageRecord, command: list[str], env: Optional[dict] = None):
    stage.command = shlex.join(command)
    detail = stage.detail or GenericDetail()
    stage.detail = detail
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    process = subprocess.Popen(
        command,
        cwd=str(ctx.repo_root),
        env=merged_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    def reader():
        assert process.stdout is not None
        for raw_line in process.stdout:
            line = raw_line.rstrip()
            if not line:
                continue
            stage.recent_lines.append(line)
            detail.handle_line(line, stage)

    thread = threading.Thread(target=reader, daemon=True)
    thread.start()
    while True:
        if ctx.interrupts.requested:
            process.terminate()
        return_code = process.poll()
        ctx.render()
        if return_code is not None:
            break
        time.sleep(max(ctx.args.interval, 0.25))
    thread.join(timeout=2.0)
    ctx.render()
    if process.returncode != 0:
        raise StageError(f"command failed with exit code {process.returncode}: {stage.command}")


def stage_run_preflight(ctx: PipelineContext, stage: StageRecord):
    ensure_repo_state(ctx.repo_root)
    export_bbox = read_bbox_poly_bounds(ctx.repo_root / "data" / "bbox.poly")
    stage.note = "Repository and critical runtime files look sane."
    stage.info_lines = [
        f"Repo root:         {ctx.repo_root}",
        f"Poly file:         {(ctx.repo_root / 'data' / 'bbox.poly')}",
        f"Export bbox:       {export_bbox}",
        f"Contour SQL:       {(ctx.repo_root / 'layers' / 'contour' / 'contour.sql')}",
        earthexplorer_state_line(ctx.repo_root),
    ]


def stage_run_prefetch(ctx: PipelineContext, stage: StageRecord):
    image_tag = "contourgenerator/generate-osm-contours:flat404"
    before_image_id = get_image_id(image_tag)
    run_generic_command_stage(
        ctx,
        stage,
        ["make", "prefetch-contours"],
        note="Warming the persistent HGT cache before contour generation.",
    )
    ctx.track_local_image(image_tag, before_image_id, get_image_id(image_tag))


def stage_run_contours(ctx: PipelineContext, stage: StageRecord):
    jobs = max(1, int(getattr(ctx.args, "workers", 16)))
    stage.note = f"Generating contour PBF from cached HGT data with jobs={jobs}."
    stage.detail = ContourDetail(ctx.repo_root)
    image_tag = "contourgenerator/generate-osm-contours:flat404"
    before_image_id = get_image_id(image_tag)
    run_live_command(
        ctx,
        stage,
        ["make", "generate-contours-parallel"],
        env={"CONTOUR_JOBS": str(jobs)},
    )
    ctx.track_local_image(image_tag, before_image_id, get_image_id(image_tag))
    verify_contour_outputs(ctx.repo_root)
    stage.info_lines = [line for line in build_contour_artifact_lines(ctx.repo_root)]


def stage_run_postgres(ctx: PipelineContext, stage: StageRecord):
    stage.detail = GenericDetail()
    stage.note = "Starting postgres container and waiting for readiness."
    before_ids = get_service_container_ids(ctx.repo_root, "postgres")
    run_live_command(ctx, stage, ["docker-compose", "up", "-d", "postgres"])
    wait_for_postgres(ctx, stage)
    ctx.track_service_containers("postgres", before_ids, get_service_container_ids(ctx.repo_root, "postgres"))


def stage_run_import_osm(ctx: PipelineContext, stage: StageRecord):
    stage.note = "Running imposm import into PostGIS."
    stage.detail = ImposmDetail()
    run_live_command(ctx, stage, ["docker-compose", "run", "--rm", "import-osm"])


def stage_run_postserve_pool(ctx: PipelineContext, stage: StageRecord):
    stage.detail = GenericDetail()
    stage.note = "Starting public postserve plus worker pool and waiting for HTTP health."
    image_tag = "contourgenerator/postserve-streaming:local"
    before_image_id = get_image_id(image_tag)
    before_postserve_ids = get_service_container_ids(ctx.repo_root, "postserve")
    run_live_command(ctx, stage, ["docker-compose", "up", "-d", "--build", "postserve"])
    wait_for_postserve(ctx, stage)
    ctx.track_service_containers("postserve", before_postserve_ids, get_service_container_ids(ctx.repo_root, "postserve"))
    before_worker_ids = get_service_container_ids(ctx.repo_root, "postserve-worker")
    run_live_command(
        ctx,
        stage,
        ["docker-compose", "up", "-d", "--build", "--scale", f"postserve-worker={ctx.args.replicas}", "postserve-worker"],
    )
    wait_for_postserve_workers(ctx, stage, ctx.args.replicas)
    ctx.track_service_containers("postserve-worker", before_worker_ids, get_service_container_ids(ctx.repo_root, "postserve-worker"))
    ctx.track_local_image(image_tag, before_image_id, get_image_id(image_tag))


def stage_run_mbtiles(ctx: PipelineContext, stage: StageRecord):
    export_bbox = read_bbox_poly_bounds(ctx.repo_root / "data" / "bbox.poly")
    ensure_mbtiles_manifest_matches(ctx.repo_root, export_bbox)
    env = build_stream_env(ctx.args, ctx.repo_root)
    stage.note = f"Running the shard-based parallel MBTiles export and final merge for bbox {export_bbox}."
    stage.detail = MBTilesDetail(ctx.repo_root / "data" / "tiles.parts" / "manifest.json")
    run_live_command(ctx, stage, ["make", "generate-mbtiles-parallel"], env=env)


def stage_dry_run_preflight(ctx: PipelineContext, stage: StageRecord):
    ensure_repo_state(ctx.repo_root)
    writable = [ctx.repo_root / "data", ctx.repo_root / "build", ctx.repo_root / "data" / "tiles.parts"]
    stage.note = "Checking required files and local writability."
    stage.info_lines = [earthexplorer_state_line(ctx.repo_root)]
    for path in writable:
        path.mkdir(parents=True, exist_ok=True)
        testfile = path / ".dry-run-write-test"
        testfile.write_text("ok\n", encoding="utf-8")
        testfile.unlink()
        stage.info_lines.append(f"Writable:          {path}")
        ctx.render()


def stage_dry_run_compose_config(ctx: PipelineContext, stage: StageRecord):
    run_generic_command_stage(
        ctx,
        stage,
        ["docker-compose", "config"],
        note="Validating the composed Docker configuration.",
    )


def stage_dry_run_build_images(ctx: PipelineContext, stage: StageRecord):
    run_generic_command_stage(
        ctx,
        stage,
        ["docker-compose", "build", "generate-osm-contours", "postserve", "postserve-worker"],
        note="Building the local images used by contour generation and postserve.",
    )


def stage_dry_run_container_smoke(ctx: PipelineContext, stage: StageRecord):
    stage.note = "Checking that critical service containers can start and see their mounts."
    checks = [
        (
            ["docker-compose", "run", "--rm", "--entrypoint", "/bin/sh", "generate-osm-contours", "-lc", "test -d /import && test -w /import && echo contour-ok"],
            "generate-osm-contours mount smoke passed",
        ),
        (
            ["docker-compose", "run", "--rm", "--entrypoint", "/bin/sh", "openmaptiles-tools", "-lc", "test -d /tileset && test -d /sql && test -w /sql && echo tools-ok"],
            "openmaptiles-tools mount smoke passed",
        ),
        (
            ["docker-compose", "run", "--rm", "--entrypoint", "/bin/sh", "import-osm", "-lc", "test -d /import && test -d /mapping && test -d /cache && test -w /import && test -w /cache && echo import-osm-ok"],
            "import-osm mount smoke passed",
        ),
        (
            ["docker-compose", "run", "--rm", "--entrypoint", "/bin/sh", "import-sql", "-lc", "test -d /sql && test -w /sql && echo import-sql-ok"],
            "import-sql mount smoke passed",
        ),
    ]
    stage.detail = GenericDetail()
    for command, success_line in checks:
        stage.command = shlex.join(command)
        ctx.render()
        completed = subprocess.run(command, cwd=str(ctx.repo_root), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in completed.stdout.splitlines():
            stage.recent_lines.append(line)
        if completed.returncode != 0:
            raise StageError(f"dry-run smoke failed: {stage.command}")
        stage.info_lines.append(success_line)
        ctx.render()


def stage_dry_run_postgres_smoke(ctx: PipelineContext, stage: StageRecord):
    stage.note = "Starting postgres and waiting for readiness, then stopping it after verification."
    run_live_command(ctx, stage, ["docker-compose", "up", "-d", "postgres"])
    ctx.register_cleanup(lambda: subprocess.run(["docker-compose", "stop", "postgres"], cwd=str(ctx.repo_root), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True))
    wait_for_postgres(ctx, stage)


def stage_dry_run_postserve_smoke(ctx: PipelineContext, stage: StageRecord):
    stage.note = "Starting postserve and one worker for HTTP health smoke, then stopping them after verification."
    run_live_command(ctx, stage, ["docker-compose", "up", "-d", "--build", "postserve"])
    ctx.register_cleanup(lambda: subprocess.run(["docker-compose", "stop", "postserve", "postserve-worker"], cwd=str(ctx.repo_root), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True))
    wait_for_postserve(ctx, stage)
    run_live_command(ctx, stage, ["docker-compose", "up", "-d", "--build", "--scale", "postserve-worker=1", "postserve-worker"])
    wait_for_postserve_workers(ctx, stage, 1)


def wait_for_postgres(ctx: PipelineContext, stage: StageRecord):
    attempts = 0
    stage.info_lines = []
    while True:
        attempts += 1
        stage.info_lines = [f"Readiness checks:  {attempts}", "Waiting for pg_isready on postgres..."]
        ctx.render()
        completed = subprocess.run(
            ["docker-compose", "exec", "-T", "postgres", "pg_isready", "-U", "openmaptiles", "-d", "openmaptiles"],
            cwd=str(ctx.repo_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        if completed.returncode == 0:
            stage.info_lines = [f"Readiness checks:  {attempts}", "Postgres is ready."]
            return
        if attempts >= 60:
            raise StageError("postgres did not become ready in time")
        if completed.stdout.strip():
            stage.recent_lines.append(completed.stdout.strip())
        time.sleep(2)


def wait_for_postserve(ctx: PipelineContext, stage: StageRecord):
    attempts = 0
    while True:
        attempts += 1
        postserve_id = get_service_container_id(ctx.repo_root, "postserve")
        postserve_ip = get_container_ip(postserve_id) if postserve_id else ""
        healthy = False
        if postserve_ip:
            healthy = url_head_ok(f"http://{postserve_ip}:8080/health")
        stage.info_lines = [
            f"Postserve checks:  {attempts}",
            f"Container id:      {postserve_id or 'n/a'}",
            f"Container ip:      {postserve_ip or 'n/a'}",
            f"HTTP health:       {'ok' if healthy else 'waiting'}",
        ]
        ctx.render()
        if healthy:
            return
        if attempts >= 60:
            raise StageError("postserve did not become ready in time")
        time.sleep(2)


def wait_for_postserve_workers(ctx: PipelineContext, stage: StageRecord, replicas: int):
    attempts = 0
    while True:
        attempts += 1
        ids = get_service_container_ids(ctx.repo_root, "postserve-worker")
        healthy = 0
        checked = []
        for container_id in ids:
            ip = get_container_ip(container_id)
            ok = bool(ip) and url_head_ok(f"http://{ip}:8080/health")
            if ok:
                healthy += 1
            checked.append(f"{container_id[:12]} {'ok' if ok else 'wait'} {ip or 'n/a'}")
        stage.info_lines = [
            f"Worker checks:     {attempts}",
            f"Workers healthy:   {healthy}/{replicas}",
        ] + [f"  {line}" for line in checked[:8]]
        ctx.render()
        if len(ids) >= replicas and healthy >= replicas:
            return
        if attempts >= 60:
            raise StageError("postserve worker pool did not become ready in time")
        time.sleep(2)


def read_optional_earthexplorer_credentials(repo_root: Path):
    path = repo_root / ".earthexplorerCredentials"
    values = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def earthexplorer_state_line(repo_root: Path):
    values = read_optional_earthexplorer_credentials(repo_root)
    user = values.get("EARTHEXPLORER_USER", "")
    password = values.get("EARTHEXPLORER_PASSWORD", "")
    if not user and "EARTHEXPLORER_USER" not in values:
        user = values.get("USER", "")
    if not password and "EARTHEXPLORER_PASSWORD" not in values:
        password = values.get("PASSWORD", "")
    if not user or not password:
        return "EarthExplorer:    optional credentials missing; srtm3 will be disabled when mixed with other sources"
    if user == "exampleUser" or password == "examplePassword":
        return "EarthExplorer:    placeholder credentials detected; srtm3 will be disabled when mixed with other sources"
    return "EarthExplorer:    credentials detected; srtm3 remains available when requested"


def ensure_repo_state(repo_root: Path):
    required = [
        repo_root / "docker-compose.yml",
        repo_root / "Makefile",
        repo_root / "data" / "bbox.poly",
        repo_root / "layers" / "contour" / "contour.sql",
        repo_root / "contours" / "image",
        repo_root / "services" / "postserve",
        repo_root / "mbtiles" / "export.py",
        repo_root / "mbtiles" / "worker.py",
    ]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise StageError("missing required files: " + ", ".join(missing))
    contour_sql = (repo_root / "layers" / "contour" / "contour.sql").read_text(encoding="utf-8", errors="replace")
    if "WHERE geometry && bbox;" not in contour_sql:
        raise StageError("contour.sql does not contain the expected bbox-only filter")
    compose_text = (repo_root / "docker-compose.yml").read_text(encoding="utf-8", errors="replace")
    if 'image: "contourgenerator/generate-osm-contours:flat404"' not in compose_text:
        raise StageError("docker-compose.yml is not configured for contourgenerator/generate-osm-contours:flat404")
    if 'image: "contourgenerator/postserve-streaming:local"' not in compose_text:
        raise StageError("docker-compose.yml is not configured for the local postserve image")


def build_stream_env(args, repo_root: Optional[Path] = None):
    env = {
        "STREAM_POSTSERVE_REPLICAS": str(getattr(args, "replicas", 16)),
        "STREAM_MIN_ZOOM": str(args.min_zoom),
        "STREAM_MAX_ZOOM": str(args.max_zoom),
        "STREAM_PARALLEL_WORKERS": str(getattr(args, "workers", 16)),
        "STREAM_TARGET_TILES_PER_SHARD": str(getattr(args, "target_tiles_per_shard", 20000)),
        "STREAM_FLAGS": getattr(args, "stream_flags", "--resume --skip-existing"),
    }
    if repo_root is not None:
        env["STREAM_BBOX"] = read_bbox_poly_bounds(repo_root / "data" / "bbox.poly")
    return env


def verify_contour_outputs(repo_root: Path):
    pbf_files = sorted((repo_root / "data").glob("*.pbf"))
    if not pbf_files:
        raise StageError("no contour PBF was generated in data/")


def read_bbox_poly_bounds(poly_path: Path) -> str:
    coords = []
    for raw_line in poly_path.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.upper() == "END":
            continue
        parts = stripped.split()
        if len(parts) < 2:
            continue
        try:
            lon = float(parts[0])
            lat = float(parts[1])
        except ValueError:
            continue
        coords.append((lon, lat))
    if not coords:
        raise StageError(f"could not parse any polygon coordinates from {poly_path}")
    lons = [lon for lon, _ in coords]
    lats = [lat for _, lat in coords]
    return f"{min(lons):.6f},{min(lats):.6f},{max(lons):.6f},{max(lats):.6f}"


def ensure_mbtiles_manifest_matches(repo_root: Path, expected_bbox: str):
    manifest_path = repo_root / "data" / "tiles.parts" / "manifest.json"
    if not manifest_path.exists():
        return
    try:
        manifest = json_load(manifest_path)
    except Exception as exc:
        raise StageError(f"could not read existing shard manifest {manifest_path}: {exc}") from exc
    actual_bbox = str((manifest.get("config") or {}).get("bbox") or "")
    if actual_bbox and actual_bbox != expected_bbox:
        raise StageError(
            "existing data/tiles.parts manifest bbox does not match current data/bbox.poly: "
            f"manifest={actual_bbox} current={expected_bbox}. Remove data/tiles.parts, data/tiles.mbtiles, "
            "and data/tiles.merge-work.mbtiles before rerunning the export."
        )


def build_contour_artifact_lines(repo_root: Path):
    lines = []
    for path in sorted((repo_root / "data").glob("*.pbf")):
        lines.append(f"Contour artifact:   {path.name} ({human_size(path.stat().st_size)})")
    return lines


def build_detail_lines(stage: StageRecord):
    detail = stage.detail or GenericDetail()
    return detail.lines(stage)


def clear_screen():
    sys.stdout.write("\033[2J\033[H")
    sys.stdout.flush()


def parse_ts(value: Optional[str]):
    if not value:
        return datetime.now(timezone.utc)
    value = value.rstrip("Z")
    if "." in value:
        base, frac = value.split(".", 1)
        frac = (frac + "000000")[:6]
        value = f"{base}.{frac}"
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


def tile_to_lon_lat(tile: str):
    lat_sign = 1 if tile[0] == "N" else -1
    lon_sign = 1 if tile[3] == "E" else -1
    lat = lat_sign * int(tile[1:3])
    lon = lon_sign * int(tile[4:7])
    return lon, lat


def tile_position(lon, lat):
    return (lon - BBOX_W) + max(0.0, min(1.0, ((lat - BBOX_S) + 1) / TOTAL_ROWS))


def speed_from_events(events, window_minutes):
    if len(events) < 2:
        return None
    latest = events[-1]
    cutoff = latest["ts"] - timedelta(minutes=window_minutes)
    sample = [event for event in events if cutoff <= event["ts"] <= latest["ts"]]
    if len(sample) < 2:
        return None
    first = sample[0]
    last = sample[-1]
    dt = (last["ts"] - first["ts"]).total_seconds()
    dp = len(sample) - 1
    if dt <= 60 or dp <= 0:
        return None
    return dp / (dt / 3600.0)


def area_status(name, area, tiles):
    expected = []
    for lat in range(area["lat_min"], area["lat_max"] + 1):
        for lon in range(area["lon_min"], area["lon_max"] + 1):
            ns = f"N{lat:02d}" if lat >= 0 else f"S{abs(lat):02d}"
            ew = f"E{lon:03d}" if lon >= 0 else f"W{abs(lon):03d}"
            expected.append((lon, lat, ns + ew))
    seen = []
    v1 = 0
    v3 = 0
    bad_seen = []
    for lon, lat, tile in expected:
        event = tiles.get((lon, lat))
        if not event:
            continue
        seen.append(tile)
        if event["view"] == "VIEW1":
            v1 += 1
        elif event["view"] == "VIEW3":
            v3 += 1
        if tile in BAD_EXPECTED_FALLBACK:
            bad_seen.append(f"{tile}:{event['view']}")
    if not seen:
        return f"{name}: not reached"
    fallback = ", ".join(bad_seen) if bad_seen else "-"
    return f"{name}: {len(seen)}/{len(expected)} seen | VIEW1 {v1} | VIEW3 {v3} | bad/fallback {fallback}"


def scan_data_dir(data_dir: Path):
    result = {"pbf_files": []}
    if not data_dir.is_dir():
        return result
    for path in sorted(data_dir.iterdir()):
        if path.suffix == ".pbf" and path.is_file():
            result["pbf_files"].append(
                {
                    "name": path.name,
                    "size": path.stat().st_size,
                    "mtime": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc),
                }
            )
    return result


def detect_container(pattern: str):
    output = run_cmd_text(["docker", "ps", "--format", "{{.Names}}\t{{.Status}}"])
    if not output:
        return None, None
    matches = []
    for line in output.splitlines():
        if "\t" not in line:
            continue
        name, status = line.split("\t", 1)
        if pattern in name:
            matches.append((name, status))
    return matches[-1] if matches else (None, None)


def inspect_container(name: str):
    output = run_cmd_text(["docker", "inspect", "-f", "{{.State.Status}}\t{{.State.ExitCode}}", name])
    if not output or "\t" not in output:
        return None, None
    status, exit_code = output.split("\t", 1)
    try:
        exit_code = int(exit_code)
    except ValueError:
        exit_code = None
    return status, exit_code


def inspect_container_metadata(container_id: str):
    output = run_cmd_text(
        [
            "docker",
            "inspect",
            "-f",
            "{{.Name}}\t{{.Config.Image}}\t{{.Image}}\t{{.State.Status}}",
            container_id,
        ]
    )
    if not output or "\t" not in output:
        return None
    name, image_tag, image_id, _status = output.split("\t", 3)
    return {
        "name": name.lstrip("/"),
        "image_tag": image_tag,
        "image_id": image_id,
    }


def inspect_workers(name: str):
    output = run_cmd_text(["docker", "top", name])
    if not output:
        return None, None, None
    lines = output.splitlines()[1:]
    worker_processes = len(lines)
    active_workers = 0
    configured_jobs = None
    for line in lines:
        if "phyghtmap" in line and "python" in line:
            active_workers += 1
        match = re.search(r"--jobs(?:=|\s+)(\d+)", line)
        if match:
            configured_jobs = int(match.group(1))
    return worker_processes, active_workers, configured_jobs


def run_cmd_text(command, cwd=None):
    completed = subprocess.run(command, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=False)
    return completed.stdout.strip()


def json_load(path: Path):
    return __import__("json").loads(path.read_text(encoding="utf-8"))


def get_service_container_id(repo_root: Path, service: str):
    output = run_cmd_text(["docker-compose", "ps", "-q", service], cwd=str(repo_root))
    ids = [line.strip() for line in output.splitlines() if line.strip()]
    return ids[0] if ids else None


def get_service_container_ids(repo_root: Path, service: str):
    output = run_cmd_text(["docker-compose", "ps", "-q", service], cwd=str(repo_root))
    return [line.strip() for line in output.splitlines() if line.strip()]


def get_container_ip(container_id: str):
    if not container_id:
        return ""
    return run_cmd_text(["docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", container_id]).strip()


def get_image_id(tag: str):
    output = run_cmd_text(["docker", "image", "inspect", "-f", "{{.Id}}", tag])
    return output.strip() or None


def url_head_ok(url: str):
    completed = subprocess.run(["curl", "-I", "--max-time", "2", url], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=False)
    return completed.returncode == 0


def human_size(num_bytes):
    if num_bytes is None:
        return "n/a"
    size = float(num_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024.0 or unit == "TB":
            return f"{size:.1f}{unit}"
        size /= 1024.0
    return f"{size:.1f}TB"


def fmt_duration(seconds):
    if seconds is None or seconds < 0:
        return "n/a"
    seconds = int(seconds)
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours > 0:
        return f"{hours}h {minutes:02d}m"
    return f"{minutes}m {secs:02d}s"


def fmt_time(value):
    if not value:
        return "n/a"
    if isinstance(value, (int, float)):
        value = datetime.fromtimestamp(value, timezone.utc)
    return value.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def bar(percent, width=36):
    percent = max(0.0, min(1.0, percent))
    filled = int(round(percent * width))
    return "[" + "#" * filled + "-" * (width - filled) + f"] {percent * 100:5.1f}%"


def print_pipeline_summary(ctx: PipelineContext, fatal_message: Optional[str]):
    print(ctx.pipeline_name)
    print("=" * 88)
    for idx, stage in enumerate(ctx.stages, start=1):
        suffix = f" ({fmt_duration(stage.elapsed_seconds())})" if stage.elapsed_seconds() is not None else ""
        print(f"{idx:02d}. {stage.title}: {stage.status}{suffix}")
        if stage.error:
            print(f"    error: {stage.error}")
    if fatal_message:
        print(f"Result: failed - {fatal_message}")
    else:
        print("Result: success")
    for note in ctx.post_run_notes:
        print(note)


def handle_successful_run_completion(ctx: PipelineContext):
    containers, images = ctx.cleanup_candidates()
    if not containers and not images:
        render_completion_screen(ctx, "Run Complete", build_completion_lines(ctx))
        return
    render_completion_screen(ctx, "Run Complete", build_completion_lines(ctx))
    if not prompt_yes_no("Clean up containers and local images created by this run? [N/y] "):
        ctx.post_run_notes.append("Cleanup skipped by user.")
        render_completion_screen(ctx, "Run Complete", build_completion_lines(ctx))
        return
    render_completion_screen(ctx, "Cleanup Review", build_cleanup_review_lines(ctx, containers, images))
    if not prompt_yes_no("Proceed with cleanup? [N/y] "):
        ctx.post_run_notes.append("Cleanup review declined by user.")
        render_completion_screen(ctx, "Run Complete", build_completion_lines(ctx))
        return
    run_targeted_cleanup(ctx, containers, images)
    render_completion_screen(ctx, "Cleanup Complete", build_completion_lines(ctx, include_cleanup_results=True))


def render_completion_screen(ctx: PipelineContext, title: str, detail_lines: list[str]):
    stage = StageRecord(id="complete", title=title, status="completed", detail=CompletionDetail(detail_lines))
    ctx.active_stage = stage
    ctx.render()


def build_completion_lines(ctx: PipelineContext, include_cleanup_results: bool = False) -> list[str]:
    lines = [
        "Run completed successfully.",
        "",
        "Artifacts:",
    ]
    lines.extend(build_artifact_summary_lines(ctx.repo_root))
    containers, images = ctx.cleanup_candidates()
    lines.extend(
        [
            "",
            "Tracked cleanup candidates:",
            f"  Containers:       {len(containers)}",
            f"  Local images:     {len(images)}",
        ]
    )
    if include_cleanup_results:
        lines.extend(["", "Cleanup results:"])
        if ctx.cleanup_results:
            lines.extend(f"  {line}" for line in ctx.cleanup_results)
        else:
            lines.append("  No cleanup actions were executed.")
    elif ctx.post_run_notes:
        lines.extend(["", "Notes:"])
        lines.extend(f"  {line}" for line in ctx.post_run_notes)
    lines.extend(["", "Stage summary:"])
    for idx, stage in enumerate(ctx.stages, start=1):
        suffix = f" ({fmt_duration(stage.elapsed_seconds())})" if stage.elapsed_seconds() is not None else ""
        lines.append(f"  {idx:02d}. {stage.title}: {stage.status}{suffix}")
    return lines


def build_cleanup_review_lines(ctx: PipelineContext, containers: list[TrackedContainer], images: list[TrackedImage]) -> list[str]:
    lines = [
        "The following run-created resources will be stopped and removed.",
        "",
        "Containers:",
    ]
    if containers:
        for item in containers:
            lines.append(
                f"  {item.name} ({short_id(item.container_id)}) image={item.image_tag} [{short_id(item.image_id)}]"
            )
    else:
        lines.append("  none")
    lines.extend(["", "Local images:"])
    if images:
        for item in images:
            lines.append(
                f"  {item.tag} [{short_id(item.after_id)}]"
                + (f" replaces {short_id(item.before_id)}" if item.before_id else " (created during this run)")
            )
    else:
        lines.append("  none")
    return lines


def build_artifact_summary_lines(repo_root: Path) -> list[str]:
    lines = []
    pbf_files = sorted((repo_root / "data").glob("*.pbf"))
    if pbf_files:
        latest_pbf = max(pbf_files, key=lambda item: item.stat().st_mtime)
        lines.append(f"  Contour PBF:      {latest_pbf.name} ({human_size(latest_pbf.stat().st_size)})")
    else:
        lines.append("  Contour PBF:      not found")
    mbtiles_path = repo_root / "data" / "tiles.mbtiles"
    if mbtiles_path.exists():
        lines.append(f"  MBTiles:          {mbtiles_path.name} ({human_size(mbtiles_path.stat().st_size)})")
    else:
        lines.append("  MBTiles:          not found")
    manifest_path = repo_root / "data" / "tiles.parts" / "manifest.json"
    if manifest_path.exists():
        try:
            manifest = json_load(manifest_path)
            failed = sum(1 for shard in manifest.get("shards", {}).values() if shard.get("status") == "failed")
            merged = sum(1 for shard in manifest.get("shards", {}).values() if shard.get("status") == "merged")
            lines.append(f"  Manifest:         merged={merged} failed={failed}")
        except Exception:
            lines.append("  Manifest:         unreadable")
    else:
        lines.append("  Manifest:         not found")
    return lines


def prompt_yes_no(prompt: str) -> bool:
    try:
        return input(prompt).strip().lower() == "y"
    except (EOFError, KeyboardInterrupt):
        return False


def run_targeted_cleanup(ctx: PipelineContext, containers: list[TrackedContainer], images: list[TrackedImage]):
    ctx.cleanup_results = []
    for item in containers:
        stop_result = subprocess.run(
            ["docker", "stop", item.container_id],
            cwd=str(ctx.repo_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        if stop_result.returncode == 0:
            ctx.cleanup_results.append(f"stopped container {item.name} ({short_id(item.container_id)})")
        else:
            ctx.cleanup_results.append(
                f"failed to stop container {item.name} ({short_id(item.container_id)}): {compact_output(stop_result.stdout)}"
            )
        rm_result = subprocess.run(
            ["docker", "rm", item.container_id],
            cwd=str(ctx.repo_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        if rm_result.returncode == 0:
            ctx.cleanup_results.append(f"removed container {item.name} ({short_id(item.container_id)})")
        else:
            ctx.cleanup_results.append(
                f"failed to remove container {item.name} ({short_id(item.container_id)}): {compact_output(rm_result.stdout)}"
            )
    for item in images:
        rm_result = subprocess.run(
            ["docker", "image", "rm", item.tag],
            cwd=str(ctx.repo_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        if rm_result.returncode == 0:
            ctx.cleanup_results.append(f"removed local image {item.tag} [{short_id(item.after_id)}]")
        else:
            ctx.cleanup_results.append(
                f"failed to remove local image {item.tag} [{short_id(item.after_id)}]: {compact_output(rm_result.stdout)}"
            )


def compact_output(output: str) -> str:
    text = " ".join(line.strip() for line in output.splitlines() if line.strip())
    return text or "no output"


def short_id(value: Optional[str]) -> str:
    if not value:
        return "n/a"
    return value.replace("sha256:", "")[:12]


if __name__ == "__main__":
    try:
        main()
    except StageError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        sys.exit(130)
    except Exception as exc:
        print(f"Unhandled error: {exc}", file=sys.stderr)
        sys.exit(1)
