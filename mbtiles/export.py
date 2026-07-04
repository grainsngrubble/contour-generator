#!/usr/bin/env python3
import argparse
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import worker as streaming


class InterruptController:
    def __init__(self):
        self.requested = False
        self._signal_count = 0

    def install(self):
        signal.signal(signal.SIGINT, self._handle_sigint)

    def _handle_sigint(self, signum, frame):
        self._signal_count += 1
        if self._signal_count == 1:
            self.requested = True
            os.write(
                2,
                b"\nInterrupt requested; stopping new shard launches and signaling running workers. Press Ctrl-C again to abort immediately.\n",
            )
            return
        raise KeyboardInterrupt


class ParallelDashboard:
    def __init__(self, enabled, refresh_interval_ms):
        self.enabled = enabled
        self.refresh_interval = max(refresh_interval_ms / 1000.0, 0.25)

    def render(self, manifest, started_at, active, recent_events):
        if not self.enabled:
            return
        total_tiles = sum(shard["tile_count"] for shard in manifest["shards"].values())
        processed = 0
        inserted = 0
        skipped = 0
        errors = 0
        retries = 0
        visible_size = 0
        counts = {"pending": 0, "running": 0, "done": 0, "failed": 0, "merged": 0}
        active_lines = []

        for shard_id, shard in sorted(
            manifest["shards"].items(),
            key=lambda item: (item[1]["zoom"], item[1]["x_start"]),
        ):
            counts[shard["status"]] = counts.get(shard["status"], 0) + 1
            summary = shard.get("summary")
            if summary:
                processed += summary.get("processed", 0)
                inserted += summary.get("inserted", 0)
                skipped += summary.get("skipped", 0)
                errors += summary.get("errors", 0)
                retries += summary.get("retries", 0)
                visible_size += summary.get("visible_file_size", summary.get("file_size", 0))
            if shard["status"] == "running":
                shard_processed = summary.get("processed", 0) if summary else 0
                shard_total = summary.get("total_tiles", shard["tile_count"]) if summary else shard["tile_count"]
                shard_percent = (shard_processed / float(max(shard_total, 1))) * 100.0
                shard_inserted = summary.get("inserted", 0) if summary else 0
                shard_skipped = summary.get("skipped", 0) if summary else 0
                shard_errors = summary.get("errors", 0) if summary else 0
                shard_retries = summary.get("retries", 0) if summary else 0
                current_zoom = summary.get("current_zoom", shard["zoom"]) if summary else shard["zoom"]
                current_x = summary.get("current_x", shard["x_start"]) if summary else shard["x_start"]
                current_y = summary.get("current_y", shard["y_start"]) if summary else shard["y_start"]
                current_attempt = summary.get("current_attempt", 1) if summary else 1
                total_attempts = summary.get("total_attempts", 1) if summary else 1
                endpoint = shard.get("endpoint") or "-"
                active_lines.append(
                    f"{shard_id:<16} {shard_percent:6.2f}% {shard_processed}/{shard_total} "
                    f"ins={shard_inserted} skip={shard_skipped} err={shard_errors} ret={shard_retries} "
                    f"tile=z{current_zoom} x{current_x} y{current_y} try={current_attempt}/{total_attempts} "
                    f"ep={endpoint}"
                )

        elapsed = max(time.time() - started_at, 0.0)
        rate = processed / elapsed if elapsed > 0 else 0.0
        remaining = max(total_tiles - processed, 0)
        eta = remaining / rate if rate > 0 else None
        percent = processed / float(max(total_tiles, 1))
        bar = self._bar(percent)

        lines = [
            "Parallel MBTiles Export",
            f"{bar} {percent * 100.0:6.2f}% {processed}/{total_tiles}",
            (
                f"pending={counts.get('pending', 0)} running={counts.get('running', 0)} "
                f"done={counts.get('done', 0)} failed={counts.get('failed', 0)} merged={counts.get('merged', 0)}"
            ),
            (
                f"ins={inserted} skip={skipped} err={errors} ret={retries} "
                f"rate={rate:.2f}/s eta={streaming.format_duration(eta)} "
                f"elapsed={streaming.format_duration(elapsed)} size={streaming.human_size(visible_size)}"
            ),
            "",
            f"Active Workers ({len(active)}):",
        ]

        if active_lines:
            lines.extend(active_lines[: max(len(active_lines), 1)])
        else:
            lines.append("none")

        lines.append("")
        lines.append("Recent Events:")
        if recent_events:
            lines.extend(recent_events[-6:])
        else:
            lines.append("none")

        sys.stdout.write("\033[2J\033[H" + "\n".join(lines) + "\n")
        sys.stdout.flush()

    def close(self):
        if not self.enabled:
            return
        sys.stdout.write("\033[2J\033[H")
        sys.stdout.flush()

    def _bar(self, percent, width=36):
        filled = int(round(percent * width))
        filled = max(0, min(width, filled))
        return "[{done}{todo}]".format(done="#" * filled, todo="-" * (width - filled))


def parse_args():
    parser = argparse.ArgumentParser(
        description="Parallel MBTiles export using shard-local workers and a safe final merge."
    )
    parser.add_argument("--bbox", required=True)
    parser.add_argument("--min-zoom", type=int, required=True)
    parser.add_argument("--max-zoom", type=int, required=True)
    parser.add_argument("--final-mbtiles", required=True)
    parser.add_argument("--seed-mbtiles", required=True)
    parser.add_argument("--shard-dir", required=True)
    parser.add_argument("--target-tiles-per-shard", type=int, default=20000)
    parser.add_argument("--postserve-endpoints", default="auto")
    parser.add_argument("--worker-count", type=int, default=4)
    parser.add_argument("--commit-interval", type=int, default=1)
    parser.add_argument("--request-timeout", type=int, default=30)
    parser.add_argument("--max-request-timeout", type=int, default=120)
    parser.add_argument("--timeout-retries", type=int, default=2)
    parser.add_argument("--timeout-retry-factor", type=float, default=2.0)
    parser.add_argument("--postgres-timeout-buffer-ms", type=int, default=1000)
    parser.add_argument("--dashboard-refresh-ms", type=int, default=250)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--merge-only", action="store_true")
    return parser.parse_args()


def run_command(command, cwd):
    completed = subprocess.run(
        command,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"command failed ({completed.returncode}): {' '.join(command)}\n{completed.stderr.strip()}"
        )
    return completed.stdout


def normalize_endpoint(raw_endpoint):
    endpoint = raw_endpoint.strip()
    if not endpoint:
        return None
    if "{z}" in endpoint:
        return endpoint
    return endpoint.rstrip("/") + "/tiles/{z}/{x}/{y}.pbf"


def detect_postserve_endpoints(repo_root):
    ids_output = run_command(["docker-compose", "ps", "-q", "postserve-worker"], repo_root)
    container_ids = [line.strip() for line in ids_output.splitlines() if line.strip()]
    if not container_ids:
        raise RuntimeError(
            "no postserve-worker containers found; run make start-postserve-pool-safe first"
        )
    endpoints = []
    for container_id in container_ids:
        ip = run_command(
            [
                "docker",
                "inspect",
                "-f",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                container_id,
            ],
            repo_root,
        ).strip()
        if not ip:
            raise RuntimeError(f"failed to resolve IP for container {container_id}")
        endpoints.append(f"http://{ip}:8080/tiles/{{z}}/{{x}}/{{y}}.pbf")
    endpoints.sort()
    return endpoints


def resolve_postserve_endpoints(args, repo_root):
    if args.postserve_endpoints == "auto":
        return detect_postserve_endpoints(repo_root)
    endpoints = []
    for raw_endpoint in args.postserve_endpoints.split(","):
        endpoint = normalize_endpoint(raw_endpoint)
        if endpoint:
            endpoints.append(endpoint)
    if not endpoints:
        raise RuntimeError("no usable postserve endpoints were provided")
    return endpoints


def shard_id_for(zoom, x_start, x_end):
    return f"z{zoom}-x{x_start}-{x_end}"


def build_shards(args, bbox, shard_dir):
    shards = []
    for zoom in range(args.min_zoom, args.max_zoom + 1):
        min_x, max_x, min_y, max_y = streaming.tile_range_for_bbox(bbox, zoom)
        x_count = max_x - min_x + 1
        y_count = max_y - min_y + 1
        total_tiles = x_count * y_count
        if total_tiles <= args.target_tiles_per_shard:
            band_width = x_count
        else:
            band_width = max(1, args.target_tiles_per_shard // y_count)
        for x_start in range(min_x, max_x + 1, band_width):
            x_end = min(max_x, x_start + band_width - 1)
            shard_name = shard_id_for(zoom, x_start, x_end)
            zoom_dir = shard_dir / f"z{zoom}"
            shard_path = zoom_dir / f"{shard_name}.mbtiles"
            summary_path = zoom_dir / f"{shard_name}.summary.json"
            log_path = zoom_dir / f"{shard_name}.log"
            shard_tiles = (x_end - x_start + 1) * y_count
            shards.append(
                {
                    "id": shard_name,
                    "zoom": zoom,
                    "x_start": x_start,
                    "x_end": x_end,
                    "y_start": min_y,
                    "y_end": max_y,
                    "tile_count": shard_tiles,
                    "mbtiles_path": str(shard_path),
                    "summary_path": str(summary_path),
                    "log_path": str(log_path),
                    "status": "pending",
                    "endpoint": None,
                    "attempts": 0,
                }
            )
    return shards


def manifest_path(shard_dir):
    return shard_dir / "manifest.json"


def merge_work_path(final_mbtiles):
    return final_mbtiles.with_name(final_mbtiles.stem + ".merge-work" + final_mbtiles.suffix)


def write_json_atomic(path, payload):
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination.with_name(destination.name + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    temp_path.replace(destination)


def load_manifest(path):
    return json.loads(path.read_text())


def build_manifest(args, repo_root, bbox, shard_dir):
    shards = build_shards(args, bbox, shard_dir)
    return {
        "version": 1,
        "created_at": int(time.time()),
        "updated_at": int(time.time()),
        "repo_root": str(repo_root),
        "config": {
            "bbox": args.bbox,
            "min_zoom": args.min_zoom,
            "max_zoom": args.max_zoom,
            "final_mbtiles": str(Path(args.final_mbtiles).resolve()),
            "seed_mbtiles": str(Path(args.seed_mbtiles).resolve()),
            "shard_dir": str(shard_dir.resolve()),
            "target_tiles_per_shard": args.target_tiles_per_shard,
        },
        "shards": {shard["id"]: shard for shard in shards},
    }


def validate_manifest(manifest, args, shard_dir):
    config = manifest.get("config", {})
    expected = {
        "bbox": args.bbox,
        "min_zoom": args.min_zoom,
        "max_zoom": args.max_zoom,
        "final_mbtiles": str(Path(args.final_mbtiles).resolve()),
        "seed_mbtiles": str(Path(args.seed_mbtiles).resolve()),
        "shard_dir": str(shard_dir.resolve()),
        "target_tiles_per_shard": args.target_tiles_per_shard,
    }
    if config != expected:
        raise RuntimeError(
            "existing manifest does not match the requested parallel export configuration"
        )


def save_manifest(path, manifest):
    manifest["updated_at"] = int(time.time())
    write_json_atomic(path, manifest)


def prepare_manifest(args, repo_root, bbox, shard_dir):
    shard_dir.mkdir(parents=True, exist_ok=True)
    manifest_file = manifest_path(shard_dir)
    if manifest_file.exists() and args.resume:
        manifest = load_manifest(manifest_file)
        validate_manifest(manifest, args, shard_dir)
    else:
        manifest = build_manifest(args, repo_root, bbox, shard_dir)
    for shard in manifest["shards"].values():
        Path(shard["mbtiles_path"]).parent.mkdir(parents=True, exist_ok=True)
        if shard["status"] == "running":
            shard["status"] = "pending"
            shard["endpoint"] = None
    save_manifest(manifest_file, manifest)
    return manifest_file, manifest


def load_summary(shard):
    summary_path = Path(shard["summary_path"])
    if not summary_path.exists():
        return None
    try:
        return json.loads(summary_path.read_text())
    except json.JSONDecodeError:
        return None


def refresh_running_summaries(manifest, active):
    for shard_id in active.keys():
        shard = manifest["shards"][shard_id]
        summary = load_summary(shard)
        if summary is not None:
            shard["summary"] = summary


def launch_worker(repo_root, args, shard, endpoint):
    command = [
        sys.executable,
        str(repo_root / "mbtiles" / "worker.py"),
        f"--bbox={args.bbox}",
        f"--min-zoom={shard['zoom']}",
        f"--max-zoom={shard['zoom']}",
        f"--mbtiles={shard['mbtiles_path']}",
        f"--tiles-url={endpoint}",
        f"--commit-interval={args.commit_interval}",
        f"--request-timeout={args.request_timeout}",
        f"--max-request-timeout={args.max_request_timeout}",
        f"--timeout-retries={args.timeout_retries}",
        f"--timeout-retry-factor={args.timeout_retry_factor}",
        f"--postgres-timeout-buffer-ms={args.postgres_timeout_buffer_ms}",
        f"--dashboard-refresh-ms={args.dashboard_refresh_ms}",
        f"--x-start={shard['x_start']}",
        f"--x-end={shard['x_end']}",
        f"--y-start={shard['y_start']}",
        f"--y-end={shard['y_end']}",
        f"--summary-json={shard['summary_path']}",
    ]
    if args.resume:
        command.append("--resume")
    if args.skip_existing:
        command.append("--skip-existing")
    log_handle = open(shard["log_path"], "a", buffering=1)
    log_handle.write(
        f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] starting {shard['id']} endpoint={endpoint}\n"
    )
    log_handle.flush()
    process = subprocess.Popen(
        command,
        cwd=str(repo_root),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return process, log_handle


def pending_shard_ids(manifest):
    statuses = {"pending", "failed"}
    return sorted(
        [
            shard_id
            for shard_id, shard in manifest["shards"].items()
            if shard["status"] in statuses
        ],
        key=lambda shard_id: (
            manifest["shards"][shard_id]["zoom"],
            manifest["shards"][shard_id]["x_start"],
        ),
    )


def push_event(events, message):
    timestamp = time.strftime("%H:%M:%S")
    events.append(f"[{timestamp}] {message}")
    del events[:-6]


def execute_shards(repo_root, args, manifest_file, manifest, endpoints):
    if args.worker_count <= 0:
        raise RuntimeError("worker-count must be > 0 unless --merge-only is used")
    available_endpoints = list(endpoints)
    max_workers = min(args.worker_count, len(available_endpoints))
    if max_workers <= 0:
        raise RuntimeError("no postserve endpoints available for parallel execution")

    interrupt = InterruptController()
    interrupt.install()
    started_at = time.time()
    active = {}
    children_signaled = False
    dashboard = ParallelDashboard(sys.stdout.isatty(), args.dashboard_refresh_ms)
    recent_events = []

    try:
        while True:
            refresh_running_summaries(manifest, active)
            pending = pending_shard_ids(manifest)
            while not interrupt.requested and pending and len(active) < max_workers:
                shard_id = pending.pop(0)
                endpoint = available_endpoints.pop(0)
                shard = manifest["shards"][shard_id]
                shard["status"] = "running"
                shard["endpoint"] = endpoint
                shard["attempts"] = shard.get("attempts", 0) + 1
                save_manifest(manifest_file, manifest)
                process, log_handle = launch_worker(repo_root, args, shard, endpoint)
                active[shard_id] = {
                    "process": process,
                    "log_handle": log_handle,
                    "endpoint": endpoint,
                }
                push_event(
                    recent_events,
                    f"start {shard_id} zoom={shard['zoom']} x={shard['x_start']}..{shard['x_end']} endpoint={endpoint}",
                )

            if interrupt.requested and active and not children_signaled:
                for active_state in active.values():
                    active_state["process"].send_signal(signal.SIGINT)
                children_signaled = True
                push_event(recent_events, "interrupt sent to running workers")

            completed_any = False
            for shard_id in list(active.keys()):
                active_state = active[shard_id]
                process = active_state["process"]
                return_code = process.poll()
                if return_code is None:
                    continue
                completed_any = True
                active_state["log_handle"].close()
                available_endpoints.append(active_state["endpoint"])
                shard = manifest["shards"][shard_id]
                summary = load_summary(shard)
                if summary is not None:
                    shard["summary"] = summary
                shard["endpoint"] = None
                if interrupt.requested and return_code in (130, -2):
                    shard["status"] = "running"
                elif return_code == 0 and summary and summary.get("errors", 0) == 0:
                    shard["status"] = "done"
                else:
                    shard["status"] = "failed"
                save_manifest(manifest_file, manifest)
                if summary:
                    push_event(
                        recent_events,
                        f"finish {shard_id} status={shard['status']} ins={summary.get('inserted', 0)} skip={summary.get('skipped', 0)} err={summary.get('errors', 0)}",
                    )
                else:
                    push_event(
                        recent_events,
                        f"finish {shard_id} status={shard['status']} exit={return_code}",
                    )
                del active[shard_id]

            if dashboard.enabled:
                dashboard.render(manifest, started_at, active, recent_events)

            if not active and not pending_shard_ids(manifest):
                break
            if interrupt.requested and not active:
                break
            if not completed_any:
                time.sleep(dashboard.refresh_interval)
    finally:
        dashboard.close()

    if interrupt.requested:
        return 130
    failed = [
        shard_id
        for shard_id, shard in manifest["shards"].items()
        if shard["status"] == "failed"
    ]
    if failed:
        print(f"failed shards: {', '.join(failed)}", flush=True)
        return 1
    return 0


def reset_merged_statuses_if_needed(manifest, merge_work):
    if merge_work.exists():
        return
    for shard in manifest["shards"].values():
        if shard["status"] == "merged":
            shard["status"] = "done"


def prepare_merge_work(manifest_file, manifest, args, bbox):
    final_mbtiles = Path(args.final_mbtiles).resolve()
    seed_mbtiles = Path(args.seed_mbtiles).resolve()
    work_path = merge_work_path(final_mbtiles)
    reset_merged_statuses_if_needed(manifest, work_path)
    if args.resume and work_path.exists():
        save_manifest(manifest_file, manifest)
        return work_path

    if work_path.exists():
        work_path.unlink()

    if seed_mbtiles.exists():
        shutil.copy2(seed_mbtiles, work_path)
    else:
        work_path.parent.mkdir(parents=True, exist_ok=True)
        connection = streaming.open_connection(work_path)
        try:
            streaming.create_schema(connection)
            streaming.upsert_metadata(connection, args.bbox, args.min_zoom, args.max_zoom)
        finally:
            connection.close()

    connection = streaming.open_connection(work_path)
    try:
        streaming.create_schema(connection)
        streaming.upsert_metadata(connection, args.bbox, args.min_zoom, args.max_zoom)
    finally:
        connection.close()
    save_manifest(manifest_file, manifest)
    return work_path


def merge_shard(work_path, shard_path):
    connection = sqlite3.connect(str(work_path))
    connection.execute("PRAGMA synchronous=NORMAL")
    try:
        before = connection.total_changes
        connection.execute("ATTACH DATABASE ? AS sharddb", (str(shard_path),))
        connection.execute(
            """
            INSERT OR IGNORE INTO tiles(zoom_level, tile_column, tile_row, tile_data)
            SELECT zoom_level, tile_column, tile_row, tile_data
            FROM sharddb.tiles
            """
        )
        connection.commit()
        connection.execute("DETACH DATABASE sharddb")
        return connection.total_changes - before
    finally:
        connection.close()


def verify_mbtiles(path):
    connection = sqlite3.connect(str(path))
    try:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        metadata_count = connection.execute("SELECT count(*) FROM metadata").fetchone()[0]
        tile_count = connection.execute("SELECT count(*) FROM tiles").fetchone()[0]
        zoom_counts = [
            {"zoom": zoom, "count": count}
            for zoom, count in connection.execute(
                "SELECT zoom_level, count(*) FROM tiles GROUP BY zoom_level ORDER BY zoom_level"
            )
        ]
    finally:
        connection.close()
    return {
        "integrity": integrity,
        "metadata_count": metadata_count,
        "tile_count": tile_count,
        "zoom_counts": zoom_counts,
        "size": path.stat().st_size if path.exists() else 0,
    }


def merge_all_shards(manifest_file, manifest, args, bbox):
    work_path = prepare_merge_work(manifest_file, manifest, args, bbox)
    final_mbtiles = Path(args.final_mbtiles).resolve()
    done_shards = [
        shard_id
        for shard_id, shard in manifest["shards"].items()
        if shard["status"] in {"done", "merged"}
    ]
    for shard_id in sorted(
        done_shards,
        key=lambda value: (
            manifest["shards"][value]["zoom"],
            manifest["shards"][value]["x_start"],
        ),
    ):
        shard = manifest["shards"][shard_id]
        if shard["status"] == "merged":
            continue
        shard_path = Path(shard["mbtiles_path"])
        if not shard_path.exists():
            raise RuntimeError(f"missing shard MBTiles file for merge: {shard_path}")
        inserted = merge_shard(work_path, shard_path)
        shard["status"] = "merged"
        shard["merged_inserted"] = inserted
        save_manifest(manifest_file, manifest)
        print(
            f"merged shard {shard_id}: inserted={inserted} shard={shard_path}",
            flush=True,
        )

    verification = verify_mbtiles(work_path)
    if verification["integrity"] != "ok":
        raise RuntimeError(f"merge-work integrity check failed: {verification['integrity']}")
    if verification["metadata_count"] <= 0:
        raise RuntimeError("merge-work metadata table is empty")
    work_path.replace(final_mbtiles)
    save_manifest(manifest_file, manifest)
    print(
        f"finalized {final_mbtiles}: tiles={verification['tile_count']} size={streaming.human_size(verification['size'])}",
        flush=True,
    )
    return verification


def main():
    args = parse_args()
    if args.min_zoom < 0 or args.max_zoom < 0:
        raise ValueError("zoom levels must be >= 0")
    if args.min_zoom > args.max_zoom:
        raise ValueError("min-zoom must be <= max-zoom")
    if args.target_tiles_per_shard <= 0:
        raise ValueError("target-tiles-per-shard must be > 0")
    if args.worker_count < 0:
        raise ValueError("worker-count must be >= 0")
    if args.commit_interval <= 0:
        raise ValueError("commit-interval must be > 0")
    if args.request_timeout <= 0:
        raise ValueError("request-timeout must be > 0")
    if args.max_request_timeout < args.request_timeout:
        raise ValueError("max-request-timeout must be >= request-timeout")
    if args.timeout_retries < 0:
        raise ValueError("timeout-retries must be >= 0")
    if args.timeout_retry_factor < 1.0:
        raise ValueError("timeout-retry-factor must be >= 1.0")
    if args.postgres_timeout_buffer_ms < 0:
        raise ValueError("postgres-timeout-buffer-ms must be >= 0")
    if args.dashboard_refresh_ms <= 0:
        raise ValueError("dashboard-refresh-ms must be > 0")

    repo_root = Path(__file__).resolve().parent.parent
    shard_dir = Path(args.shard_dir).resolve()
    bbox = streaming.parse_bbox(args.bbox)
    manifest_file, manifest = prepare_manifest(args, repo_root, bbox, shard_dir)

    if not args.merge_only:
        endpoints = resolve_postserve_endpoints(args, repo_root)
        if args.worker_count > len(endpoints):
            print(
                f"worker-count={args.worker_count} exceeds available endpoints={len(endpoints)}; using {len(endpoints)}",
                flush=True,
            )
            args.worker_count = len(endpoints)
        exit_code = execute_shards(repo_root, args, manifest_file, manifest, endpoints)
        if exit_code != 0:
            return exit_code

    merge_all_shards(manifest_file, manifest, args, bbox)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr, flush=True)
        sys.exit(130)
    except Exception as error:
        print(f"fatal: {error}", file=sys.stderr, flush=True)
        sys.exit(1)
