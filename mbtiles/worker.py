#!/usr/bin/env python3
import argparse
import http.client
import json
import math
import os
import queue
import signal
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
from pathlib import Path


MAX_LAT = 85.05112878
SPINNER_FRAMES = "|/-\\"


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
                b"\nInterrupt requested; finishing the current tile request, committing progress, and stopping before the next tile. Press Ctrl-C again to abort immediately.\n",
            )
            return
        raise KeyboardInterrupt


class Dashboard:
    def __init__(self, enabled, refresh_interval_ms, total_tiles, mbtiles_path):
        self.enabled = enabled
        self.refresh_interval = max(refresh_interval_ms / 1000.0, 0.05)
        self.total_tiles = total_tiles
        self.mbtiles_path = mbtiles_path
        self.last_render = 0.0
        self.last_line_length = 0
        self.spinner_index = 0

    def render(self, state, force=False):
        if not self.enabled:
            return
        now = time.time()
        if not force and (now - self.last_render) < self.refresh_interval:
            return
        self.last_render = now
        spinner = SPINNER_FRAMES[self.spinner_index % len(SPINNER_FRAMES)]
        self.spinner_index += 1
        processed = state["processed"]
        total = max(self.total_tiles, 1)
        percent = processed / float(total)
        bar = self._bar(percent)
        elapsed = max(now - state["started_at"], 0.0)
        rate = processed / elapsed if elapsed > 0 else 0.0
        remaining = max(self.total_tiles - processed, 0)
        eta = remaining / rate if rate > 0 else None
        current_tile = "z{z} x{x} y{y}".format(
            z=state["current_zoom"],
            x=state["current_x"],
            y=state["current_y"],
        )
        current_attempt = state.get("current_attempt", 1)
        total_attempts = state.get("total_attempts", 1)
        current_timeout_seconds = state.get("current_timeout_seconds", 0)
        line = (
            "\r{spinner} {bar} {percent:6.2f}% {processed}/{total} "
            "ins={inserted} skip={skipped} err={errors} retries={retries} rate={rate:.2f}/s "
            "eta={eta} elapsed={elapsed} tile={tile} try={attempt}/{attempts} timeout={timeout}s size={size}{suffix}"
        ).format(
            spinner=spinner,
            bar=bar,
            percent=percent * 100.0,
            processed=processed,
            total=self.total_tiles,
            inserted=state["inserted"],
            skipped=state["skipped"],
            errors=state["errors"],
            retries=state["retries"],
            rate=rate,
            eta=format_duration(eta),
            elapsed=format_duration(elapsed),
            tile=current_tile,
            attempt=current_attempt,
            attempts=total_attempts,
            timeout=current_timeout_seconds,
            size=human_size(get_visible_file_size(self.mbtiles_path)),
            suffix=" [interrupt pending]" if state["interrupted"] else "",
        )
        padding = max(self.last_line_length - len(line), 0)
        sys.stdout.write(line + (" " * padding))
        sys.stdout.flush()
        self.last_line_length = len(line)

    def print_line(self, message):
        if self.enabled:
            sys.stdout.write("\r" + (" " * self.last_line_length) + "\r")
            sys.stdout.flush()
            self.last_line_length = 0
        print(message, flush=True)

    def close(self):
        if self.enabled and self.last_line_length:
            sys.stdout.write("\r" + (" " * self.last_line_length) + "\r")
            sys.stdout.flush()
            self.last_line_length = 0

    def _bar(self, percent, width=28):
        filled = int(round(percent * width))
        filled = max(0, min(width, filled))
        return "[{done}{todo}]".format(done="#" * filled, todo="-" * (width - filled))



class PersistentTileClient:
    def __init__(self, url_template):
        self.url_template = url_template
        sample_url = urllib.parse.urlsplit(url_template.format(z=0, x=0, y=0))
        if sample_url.scheme not in ("http", "https"):
            raise ValueError(f"tiles-url must use http or https, got: {sample_url.scheme}")
        self.scheme = sample_url.scheme
        self.netloc = sample_url.netloc
        self._connection = None
        self._timeout_seconds = None

    def close(self):
        if self._connection is not None:
            try:
                self._connection.close()
            finally:
                self._connection = None
                self._timeout_seconds = None

    def fetch(self, zoom, x, y, timeout_seconds, postgres_timeout_ms):
        connection = self._ensure_connection(timeout_seconds)
        request_url = urllib.parse.urlsplit(self.url_template.format(z=zoom, x=x, y=y))
        if request_url.scheme != self.scheme or request_url.netloc != self.netloc:
            self.close()
            raise ValueError("tiles-url host must stay constant within a worker run")
        path = request_url.path or "/"
        if request_url.query:
            path = f"{path}?{request_url.query}"
        headers = {
            "User-Agent": "mbtiles-streaming-exporter/1.0",
            "X-Request-Timeout-Ms": str(postgres_timeout_ms),
            "Connection": "keep-alive",
        }
        try:
            connection.request("GET", path, headers=headers)
            response = connection.getresponse()
            try:
                status = response.status
                data = response.read()
                should_close = response.getheader("Connection", "").lower() == "close"
            finally:
                response.close()
        except Exception as error:
            self.close()
            raise urllib.error.URLError(error)
        if should_close:
            self.close()
        return status, data

    def _ensure_connection(self, timeout_seconds):
        if self._connection is not None and self._timeout_seconds == timeout_seconds:
            return self._connection
        self.close()
        if self.scheme == "https":
            connection = http.client.HTTPSConnection(self.netloc, timeout=timeout_seconds)
        else:
            connection = http.client.HTTPConnection(self.netloc, timeout=timeout_seconds)
        self._connection = connection
        self._timeout_seconds = timeout_seconds
        return connection


def parse_args():
    parser = argparse.ArgumentParser(
        description="Incrementally export MBTiles from a postserve tile endpoint."
    )
    parser.add_argument("--bbox", required=True, help="west,south,east,north in EPSG:4326")
    parser.add_argument("--min-zoom", type=int, required=True)
    parser.add_argument("--max-zoom", type=int, required=True)
    parser.add_argument("--mbtiles", required=True)
    parser.add_argument(
        "--tiles-url",
        required=True,
        help="Tile URL template, for example http://localhost:8090/tiles/{z}/{x}/{y}.pbf",
    )
    parser.add_argument("--commit-interval", type=int, default=1)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--request-timeout", type=int, default=30)
    parser.add_argument("--max-request-timeout", type=int, default=120)
    parser.add_argument("--timeout-retries", type=int, default=2)
    parser.add_argument("--timeout-retry-factor", type=float, default=2.0)
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--postgres-timeout-buffer-ms", type=int, default=1000)
    parser.add_argument("--dashboard-refresh-ms", type=int, default=250)
    parser.add_argument("--x-start", type=int)
    parser.add_argument("--x-end", type=int)
    parser.add_argument("--y-start", type=int)
    parser.add_argument("--y-end", type=int)
    parser.add_argument("--summary-json")
    return parser.parse_args()


def parse_bbox(raw_bbox):
    parts = [part.strip() for part in raw_bbox.split(",")]
    if len(parts) != 4:
        raise ValueError("bbox must contain west,south,east,north")
    west, south, east, north = [float(part) for part in parts]
    if west >= east:
        raise ValueError("bbox west must be smaller than east")
    if south >= north:
        raise ValueError("bbox south must be smaller than north")
    if south < -MAX_LAT or north > MAX_LAT:
        raise ValueError(f"bbox latitude must stay within +/-{MAX_LAT}")
    return west, south, east, north


def clamp(value, lower, upper):
    return max(lower, min(value, upper))


def lon_to_tile_x(lon, zoom):
    scale = 2 ** zoom
    return (lon + 180.0) / 360.0 * scale


def lat_to_tile_y(lat, zoom):
    lat = clamp(lat, -MAX_LAT, MAX_LAT)
    lat_rad = math.radians(lat)
    scale = 2 ** zoom
    return ((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0) * scale


def tile_range_for_bbox(bbox, zoom):
    west, south, east, north = bbox
    scale = 2 ** zoom
    min_x = int(math.floor(lon_to_tile_x(west, zoom)))
    max_x = int(math.ceil(lon_to_tile_x(east, zoom)) - 1)
    min_y = int(math.floor(lat_to_tile_y(north, zoom)))
    max_y = int(math.ceil(lat_to_tile_y(south, zoom)) - 1)
    min_x = clamp(min_x, 0, scale - 1)
    max_x = clamp(max_x, 0, scale - 1)
    min_y = clamp(min_y, 0, scale - 1)
    max_y = clamp(max_y, 0, scale - 1)
    return min_x, max_x, min_y, max_y


def xyz_to_tms_y(zoom, y):
    return (2 ** zoom - 1) - y


def ensure_output_path(mbtiles_path, resume):
    mbtiles_path.parent.mkdir(parents=True, exist_ok=True)
    if mbtiles_path.exists() and not resume:
        mbtiles_path.unlink()


def open_connection(mbtiles_path):
    connection = sqlite3.connect(str(mbtiles_path))
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


def create_schema(connection):
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS metadata (
            name TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS tiles (
            zoom_level INTEGER NOT NULL,
            tile_column INTEGER NOT NULL,
            tile_row INTEGER NOT NULL,
            tile_data BLOB NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS tiles_zxy_idx
        ON tiles (zoom_level, tile_column, tile_row)
        """
    )
    connection.commit()


def read_metadata(connection):
    return dict(connection.execute("SELECT name, value FROM metadata"))


def parse_existing_zoom(value, fallback):
    if value is None:
        return fallback
    try:
        return int(value)
    except ValueError:
        return fallback


def upsert_metadata(connection, bbox_text, min_zoom, max_zoom):
    existing = read_metadata(connection)
    merged_min = min(min_zoom, parse_existing_zoom(existing.get("minzoom"), min_zoom))
    merged_max = max(max_zoom, parse_existing_zoom(existing.get("maxzoom"), max_zoom))
    metadata = {
        "name": "OpenContourMapTiles",
        "format": "pbf",
        "bounds": bbox_text,
        "minzoom": str(merged_min),
        "maxzoom": str(merged_max),
        "type": "overlay",
        "version": "1",
        "description": "Streaming export from postserve",
    }
    for key, value in metadata.items():
        connection.execute(
            "INSERT OR REPLACE INTO metadata(name, value) VALUES(?, ?)",
            (key, value),
        )
    connection.commit()


def get_file_size(path):
    try:
        return path.stat().st_size
    except OSError:
        return 0


def get_visible_file_size(path):
    size = get_file_size(path)
    for suffix in ("-journal", "-wal", ".journal", ".wal"):
        size += get_file_size(Path(str(path) + suffix))
    return size


def human_size(num_bytes):
    size = float(num_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024.0 or unit == "TB":
            return f"{size:.1f}{unit}"
        size /= 1024.0
    return f"{size:.1f}TB"


def format_duration(seconds):
    if seconds is None or math.isinf(seconds) or math.isnan(seconds):
        return "--:--:--"
    total_seconds = max(int(seconds), 0)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def compute_postgres_timeout_ms(request_timeout_seconds, buffer_ms):
    return max(1000, request_timeout_seconds * 1000 - buffer_ms)


def next_timeout_seconds(current_timeout_seconds, factor, max_timeout_seconds):
    scaled = int(math.ceil(current_timeout_seconds * factor))
    return min(max_timeout_seconds, max(current_timeout_seconds + 1, scaled))


def start_request(client, zoom, x, y, timeout_seconds, postgres_timeout_ms):
    result_queue = queue.Queue(maxsize=1)

    def worker():
        try:
            status, data = client.fetch(zoom, x, y, timeout_seconds, postgres_timeout_ms)
            result_queue.put(("ok", status, data))
        except urllib.error.URLError as error:
            result_queue.put(("url_error", error, None))
        except Exception as error:
            result_queue.put(("error", error, None))

    thread = threading.Thread(target=worker)
    thread.daemon = True
    thread.start()
    return thread, result_queue


def fetch_tile(client, zoom, x, y, timeout_seconds, postgres_timeout_ms, dashboard, state):
    thread, result_queue = start_request(client, zoom, x, y, timeout_seconds, postgres_timeout_ms)
    while thread.is_alive():
        dashboard.render(state)
        thread.join(dashboard.refresh_interval)
    dashboard.render(state, force=True)
    kind, value, data = result_queue.get()
    if kind == "ok":
        return value, data
    raise value


def fetch_tile_with_retries(client, args, zoom, x, y, dashboard, state):
    timeout_seconds = args.request_timeout
    total_attempts = max(args.timeout_retries + 1, 1)
    state["total_attempts"] = total_attempts

    for attempt in range(1, total_attempts + 1):
        postgres_timeout_ms = compute_postgres_timeout_ms(
            timeout_seconds, args.postgres_timeout_buffer_ms
        )
        state["current_attempt"] = attempt
        state["current_timeout_seconds"] = timeout_seconds
        try:
            status, data = fetch_tile(
                client,
                zoom,
                x,
                y,
                timeout_seconds,
                postgres_timeout_ms,
                dashboard,
                state,
            )
        except urllib.error.URLError as error:
            if attempt >= total_attempts:
                raise
            next_timeout = next_timeout_seconds(
                timeout_seconds, args.timeout_retry_factor, args.max_request_timeout
            )
            if next_timeout <= timeout_seconds:
                raise
            next_postgres_timeout_ms = compute_postgres_timeout_ms(
                next_timeout, args.postgres_timeout_buffer_ms
            )
            state["retries"] += 1
            dashboard.print_line(
                f"retry: z={zoom} x={x} y={y} request failed on attempt {attempt}/{total_attempts}: {error}; retrying with client_timeout={next_timeout}s postgres_timeout={next_postgres_timeout_ms}ms"
            )
            timeout_seconds = next_timeout
            continue

        if status == 504 and attempt < total_attempts:
            next_timeout = next_timeout_seconds(
                timeout_seconds, args.timeout_retry_factor, args.max_request_timeout
            )
            if next_timeout > timeout_seconds:
                next_postgres_timeout_ms = compute_postgres_timeout_ms(
                    next_timeout, args.postgres_timeout_buffer_ms
                )
                state["retries"] += 1
                dashboard.print_line(
                    f"retry: z={zoom} x={x} y={y} timed out on attempt {attempt}/{total_attempts} at client_timeout={timeout_seconds}s postgres_timeout={postgres_timeout_ms}ms; retrying with client_timeout={next_timeout}s postgres_timeout={next_postgres_timeout_ms}ms"
                )
                timeout_seconds = next_timeout
                continue

        return status, data, attempt, timeout_seconds, postgres_timeout_ms

    postgres_timeout_ms = compute_postgres_timeout_ms(
        timeout_seconds, args.postgres_timeout_buffer_ms
    )
    return 504, None, total_attempts, timeout_seconds, postgres_timeout_ms


def insert_tile(cursor, zoom, x, y, tile_data, skip_existing):
    tms_y = xyz_to_tms_y(zoom, y)
    if skip_existing:
        cursor.execute(
            """
            INSERT OR IGNORE INTO tiles(zoom_level, tile_column, tile_row, tile_data)
            VALUES(?, ?, ?, ?)
            """,
            (zoom, x, tms_y, tile_data),
        )
    else:
        cursor.execute(
            """
            INSERT INTO tiles(zoom_level, tile_column, tile_row, tile_data)
            VALUES(?, ?, ?, ?)
            """,
            (zoom, x, tms_y, tile_data),
        )
    return cursor.rowcount


def build_zoom_ranges(bbox, min_zoom, max_zoom):
    ranges = []
    total_tiles = 0
    for zoom in range(min_zoom, max_zoom + 1):
        min_x, max_x, min_y, max_y = tile_range_for_bbox(bbox, zoom)
        tile_count = (max_x - min_x + 1) * (max_y - min_y + 1)
        ranges.append((zoom, min_x, max_x, min_y, max_y, tile_count))
        total_tiles += tile_count
    return ranges, total_tiles


def apply_explicit_bounds(args, zoom_ranges):
    use_explicit_bounds = any(
        value is not None for value in (args.x_start, args.x_end, args.y_start, args.y_end)
    )
    if not use_explicit_bounds:
        return zoom_ranges

    updated_ranges = []
    for zoom, min_x, max_x, min_y, max_y, _ in zoom_ranges:
        bounded_min_x = max(min_x, args.x_start)
        bounded_max_x = min(max_x, args.x_end)
        bounded_min_y = max(min_y, args.y_start)
        bounded_max_y = min(max_y, args.y_end)
        if bounded_min_x > bounded_max_x or bounded_min_y > bounded_max_y:
            raise ValueError(
                f"explicit shard bounds are outside the computed bbox tile range for z{zoom}"
            )
        tile_count = (bounded_max_x - bounded_min_x + 1) * (bounded_max_y - bounded_min_y + 1)
        updated_ranges.append(
            (zoom, bounded_min_x, bounded_max_x, bounded_min_y, bounded_max_y, tile_count)
        )
    return updated_ranges


def write_json_atomic(path, payload):
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination.with_name(destination.name + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    temp_path.replace(destination)


def build_summary(args, mbtiles_path, state, total_tiles, zoom_summaries, started_at, status, exit_code):
    elapsed_seconds = time.time() - started_at
    processed = state["processed"]
    remaining = max(total_tiles - processed, 0)
    rate = processed / elapsed_seconds if elapsed_seconds > 0 else 0.0
    eta_seconds = remaining / rate if rate > 0 else None
    return {
        "bbox": args.bbox,
        "min_zoom": args.min_zoom,
        "max_zoom": args.max_zoom,
        "mbtiles": str(mbtiles_path),
        "status": status,
        "exit_code": exit_code,
        "inserted": state["inserted"],
        "skipped": state["skipped"],
        "errors": state["errors"],
        "retries": state["retries"],
        "processed": processed,
        "total_tiles": total_tiles,
        "elapsed_seconds": round(elapsed_seconds, 3),
        "rate": round(rate, 3),
        "eta_seconds": round(eta_seconds, 3) if eta_seconds is not None else None,
        "file_size": get_file_size(mbtiles_path),
        "visible_file_size": get_visible_file_size(mbtiles_path),
        "interrupted": state["interrupted"],
        "zoom_summaries": zoom_summaries,
        "x_start": args.x_start,
        "x_end": args.x_end,
        "y_start": args.y_start,
        "y_end": args.y_end,
        "current_zoom": state["current_zoom"],
        "current_x": state["current_x"],
        "current_y": state["current_y"],
        "current_attempt": state["current_attempt"],
        "total_attempts": state["total_attempts"],
        "current_timeout_seconds": state["current_timeout_seconds"],
        "updated_at": round(time.time(), 3),
    }


def export_tiles(args, bbox):
    mbtiles_path = Path(args.mbtiles).resolve()
    ensure_output_path(mbtiles_path, args.resume)
    connection = open_connection(mbtiles_path)
    create_schema(connection)
    upsert_metadata(connection, args.bbox, args.min_zoom, args.max_zoom)

    zoom_ranges, _ = build_zoom_ranges(bbox, args.min_zoom, args.max_zoom)
    zoom_ranges = apply_explicit_bounds(args, zoom_ranges)
    total_tiles = sum(tile_count for _, _, _, _, _, tile_count in zoom_ranges)
    interrupt = InterruptController()
    interrupt.install()
    dashboard = Dashboard(sys.stdout.isatty(), args.dashboard_refresh_ms, total_tiles, mbtiles_path)
    cursor = connection.cursor()
    tile_client = PersistentTileClient(args.tiles_url)
    skip_existing = args.skip_existing or args.resume
    started_at = time.time()
    state = {
        "inserted": 0,
        "skipped": 0,
        "errors": 0,
        "retries": 0,
        "processed": 0,
        "started_at": started_at,
        "current_zoom": args.min_zoom,
        "current_x": 0,
        "current_y": 0,
        "current_attempt": 1,
        "total_attempts": max(args.timeout_retries + 1, 1),
        "current_timeout_seconds": args.request_timeout,
        "interrupted": False,
    }
    zoom_summaries = []
    exit_code = 0
    status = "completed"
    summary_write_interval = max(args.dashboard_refresh_ms / 1000.0, 0.5)
    last_summary_write_at = 0.0

    def persist_summary(current_status, current_exit_code, force=False):
        nonlocal last_summary_write_at
        if not args.summary_json:
            return
        now = time.time()
        if not force and (now - last_summary_write_at) < summary_write_interval:
            return
        summary = build_summary(
            args,
            mbtiles_path,
            state,
            total_tiles,
            zoom_summaries,
            started_at,
            current_status,
            current_exit_code,
        )
        write_json_atomic(args.summary_json, summary)
        last_summary_write_at = now

    persist_summary("running", 0, force=True)

    try:
        for zoom, min_x, max_x, min_y, max_y, tile_count in zoom_ranges:
            dashboard.print_line(
                f"Starting z{zoom}: x={min_x}..{max_x} y={min_y}..{max_y} total={tile_count} client_timeout={args.request_timeout}s max_client_timeout={args.max_request_timeout}s timeout_retries={args.timeout_retries}"
            )
            zoom_inserted = 0
            zoom_skipped = 0
            zoom_errors = 0
            zoom_processed = 0

            for x in range(min_x, max_x + 1):
                for y in range(min_y, max_y + 1):
                    if interrupt.requested:
                        state["interrupted"] = True
                        break
                    state["current_zoom"] = zoom
                    state["current_x"] = x
                    state["current_y"] = y
                    try:
                        status_code, data, attempt_used, timeout_used, postgres_timeout_ms = fetch_tile_with_retries(
                            tile_client,
                            args,
                            zoom,
                            x,
                            y,
                            dashboard,
                            state,
                        )
                    except urllib.error.URLError as error:
                        state["errors"] += 1
                        state["processed"] += 1
                        zoom_errors += 1
                        zoom_processed += 1
                        dashboard.print_line(
                            f"error: z={zoom} x={x} y={y} request failed: {error}"
                        )
                        persist_summary("running", 0)
                        continue
                    except Exception as error:
                        state["errors"] += 1
                        state["processed"] += 1
                        zoom_errors += 1
                        zoom_processed += 1
                        dashboard.print_line(
                            f"error: z={zoom} x={x} y={y} unexpected failure: {error}"
                        )
                        persist_summary("running", 0)
                        continue

                    if status_code in (204, 404):
                        state["skipped"] += 1
                        state["processed"] += 1
                        zoom_skipped += 1
                        zoom_processed += 1
                        dashboard.render(state, force=True)
                        persist_summary("running", 0)
                        continue
                    if status_code == 504:
                        state["errors"] += 1
                        state["processed"] += 1
                        zoom_errors += 1
                        zoom_processed += 1
                        dashboard.print_line(
                            f"timeout: z={zoom} x={x} y={y} exhausted {attempt_used}/{state['total_attempts']} attempts; last client_timeout={timeout_used}s postgres_timeout={postgres_timeout_ms}ms"
                        )
                        persist_summary("running", 0)
                        continue
                    if status_code != 200:
                        state["errors"] += 1
                        state["processed"] += 1
                        zoom_errors += 1
                        zoom_processed += 1
                        dashboard.print_line(
                            f"error: z={zoom} x={x} y={y} returned HTTP {status_code}"
                        )
                        persist_summary("running", 0)
                        continue
                    if not data:
                        state["skipped"] += 1
                        state["processed"] += 1
                        zoom_skipped += 1
                        zoom_processed += 1
                        dashboard.render(state, force=True)
                        persist_summary("running", 0)
                        continue

                    try:
                        rowcount = insert_tile(cursor, zoom, x, y, data, skip_existing)
                    except sqlite3.IntegrityError:
                        state["skipped"] += 1
                        state["processed"] += 1
                        zoom_skipped += 1
                        zoom_processed += 1
                        dashboard.render(state, force=True)
                        persist_summary("running", 0)
                        continue

                    if rowcount:
                        state["inserted"] += 1
                        zoom_inserted += 1
                        if args.commit_interval <= 1:
                            connection.commit()
                    else:
                        state["skipped"] += 1
                        zoom_skipped += 1
                    state["processed"] += 1
                    zoom_processed += 1
                    dashboard.render(state, force=True)
                    persist_summary("running", 0)

                    if (
                        args.commit_interval > 1
                        and state["inserted"] > 0
                        and state["inserted"] % args.commit_interval == 0
                    ):
                        connection.commit()
                        dashboard.print_line(
                            f"commit: inserted={state['inserted']} skipped={state['skipped']} errors={state['errors']} size={human_size(get_visible_file_size(mbtiles_path))}"
                        )
                        persist_summary("running", 0, force=True)
                if interrupt.requested:
                    break

            connection.commit()
            zoom_summary = {
                "zoom": zoom,
                "x_start": min_x,
                "x_end": max_x,
                "y_start": min_y,
                "y_end": max_y,
                "processed": zoom_processed,
                "total_tiles": tile_count,
                "inserted": zoom_inserted,
                "skipped": zoom_skipped,
                "errors": zoom_errors,
            }
            zoom_summaries.append(zoom_summary)
            dashboard.print_line(
                f"z{zoom} summary: processed={zoom_processed}/{tile_count} inserted={zoom_inserted} skipped={zoom_skipped} errors={zoom_errors} file={human_size(get_visible_file_size(mbtiles_path))}"
            )
            persist_summary("running", 0, force=True)
            if interrupt.requested:
                break
    except KeyboardInterrupt:
        state["interrupted"] = True
        status = "interrupted"
        exit_code = 130
        dashboard.print_line("forced interrupt received; exiting immediately")
        persist_summary(status, exit_code, force=True)
        raise
    finally:
        tile_client.close()
        connection.commit()
        connection.close()
        dashboard.close()

    if state["interrupted"]:
        status = "interrupted"
        exit_code = 130
    elif state["errors"]:
        status = "completed_with_errors"
        exit_code = 1

    elapsed = time.time() - started_at
    dashboard.print_line(
        f"complete: inserted={state['inserted']} skipped={state['skipped']} errors={state['errors']} retries={state['retries']} processed={state['processed']}/{total_tiles} elapsed={format_duration(elapsed)} mbtiles={mbtiles_path} interrupted={state['interrupted']}"
    )

    if args.summary_json:
        summary = build_summary(
            args,
            mbtiles_path,
            state,
            total_tiles,
            zoom_summaries,
            started_at,
            status,
            exit_code,
        )
        write_json_atomic(args.summary_json, summary)

    return exit_code

def main():
    args = parse_args()
    if args.min_zoom < 0 or args.max_zoom < 0:
        raise ValueError("zoom levels must be >= 0")
    if args.min_zoom > args.max_zoom:
        raise ValueError("min-zoom must be <= max-zoom")
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

    shard_values = (args.x_start, args.x_end, args.y_start, args.y_end)
    if any(value is not None for value in shard_values):
        if not all(value is not None for value in shard_values):
            raise ValueError("x-start, x-end, y-start, and y-end must be provided together")
        if args.min_zoom != args.max_zoom:
            raise ValueError("explicit shard bounds require min-zoom and max-zoom to be the same")
        if args.x_start < 0 or args.y_start < 0:
            raise ValueError("explicit shard bounds must be >= 0")
        if args.x_start > args.x_end:
            raise ValueError("x-start must be <= x-end")
        if args.y_start > args.y_end:
            raise ValueError("y-start must be <= y-end")

    bbox = parse_bbox(args.bbox)
    return export_tiles(args, bbox)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr, flush=True)
        sys.exit(130)
    except Exception as error:
        print(f"fatal: {error}", file=sys.stderr, flush=True)
        sys.exit(1)
