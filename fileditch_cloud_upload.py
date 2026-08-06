#!/usr/bin/env python3
"""Stream a public Google Drive file into FileDitch's chunked uploader."""

from __future__ import annotations

import argparse
import json
import math
import secrets
import string
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlencode

import httpx

from cloud_to_cloud_upload import (
    USER_AGENT,
    drive_file_id_from_url,
    drive_range_total,
    resolve_drive_confirmation_url,
)


FILEDITCH_CHUNK_URL = "https://new.fileditch.com/chunked.php"
FILEDITCH_CHUNK_BYTES = 128 * 1024 * 1024
DRIVE_RANGE_BYTES = 4 * 1024 * 1024
DEFAULT_WORKERS = 6
MAX_WORKERS = 6
MAX_ATTEMPTS = 5


def safe_filename(value: str) -> str:
    name = Path(value).name.strip()
    if not name:
        raise ValueError("A destination filename is required.")
    return name[:500]


def randomized_storage_filename(original: str) -> str:
    name = safe_filename(original)
    suffix = Path(name).suffix if Path(name).suffix else ""
    alphabet = string.ascii_letters + string.digits
    token = "".join(secrets.choice(alphabet) for _ in range(12))
    return f"{token}{suffix}"


def parse_json(response: httpx.Response) -> dict[str, object]:
    try:
        value = response.json()
    except ValueError as exc:
        raise RuntimeError(f"FileDitch returned invalid JSON (HTTP {response.status_code}).") from exc
    if not isinstance(value, dict):
        raise RuntimeError("FileDitch returned an invalid response object.")
    return value


def initialize_upload(filename: str, size: int) -> tuple[str, int, int]:
    response = httpx.post(
        FILEDITCH_CHUNK_URL,
        params={"action": "init", "filename": filename, "size": str(size)},
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        timeout=httpx.Timeout(connect=60.0, read=120.0, write=60.0, pool=60.0),
    )
    response.raise_for_status()
    payload = parse_json(response)
    if payload.get("success") is not True:
        raise RuntimeError(str(payload.get("error") or "FileDitch initialization failed."))
    upload_id = str(payload.get("id") or "").strip()
    chunk_size = int(payload.get("chunk_size") or 0)
    total_chunks = int(payload.get("total_chunks") or 0)
    expected_chunks = math.ceil(size / chunk_size) if chunk_size > 0 else 0
    if not upload_id or chunk_size <= 0 or total_chunks != expected_chunks:
        raise RuntimeError("FileDitch returned an invalid chunked upload plan.")
    return upload_id, chunk_size, total_chunks


def iter_drive_range(
    client: httpx.Client,
    candidate_url: str,
    file_id: str,
    start: int,
    end: int,
    total: int,
):
    """Yield one verified Drive range, refreshing the URL only in the caller."""
    expected = end - start + 1
    response = client.build_request(
        "GET",
        candidate_url,
        headers={
            "Range": f"bytes={start}-{end}",
            "Accept-Encoding": "identity",
            "User-Agent": USER_AGENT,
        },
    )
    with client.send(response, stream=True) as source:
        content_range = source.headers.get("Content-Range", "")
        expected_range = f"bytes {start}-{end}/{total}"
        if source.status_code != 206 or content_range.lower() != expected_range.lower():
            raise RuntimeError(
                f"Drive returned an invalid range for {start}-{end}: "
                f"HTTP {source.status_code}, Content-Range={content_range or '<missing>'}."
            )
        received = 0
        for block in source.iter_bytes(chunk_size=1024 * 1024):
            received += len(block)
            yield block
        if received != expected:
            raise RuntimeError(
                f"Drive range {start}-{end} returned {received} bytes; expected {expected}."
            )


def iter_drive_chunk(
    client: httpx.Client,
    candidate_url: str,
    file_id: str,
    start: int,
    end: int,
    total: int,
):
    """Read one FileDitch chunk through small verified Drive ranges."""
    cursor = start
    while cursor <= end:
        range_end = min(end, cursor + DRIVE_RANGE_BYTES - 1)
        yield from iter_drive_range(client, candidate_url, file_id, cursor, range_end, total)
        cursor = range_end + 1


def upload_chunk(
    file_id: str,
    initial_url: str,
    total: int,
    upload_id: str,
    chunk_index: int,
    chunk_size: int,
) -> tuple[int, int, float]:
    start = chunk_index * chunk_size
    end = min(total - 1, start + chunk_size - 1)
    expected = end - start + 1
    last_error = "FileDitch chunk upload failed."
    started = time.monotonic()

    for attempt in range(1, MAX_ATTEMPTS + 1):
        candidate_url = initial_url
        if attempt > 1:
            candidate_url = resolve_drive_confirmation_url(file_id)
        try:
            with httpx.Client(
                follow_redirects=True,
                timeout=httpx.Timeout(connect=60.0, read=900.0, write=900.0, pool=120.0),
                headers={"User-Agent": USER_AGENT},
            ) as client:
                body = iter_drive_chunk(client, candidate_url, file_id, start, end, total)
                response = client.post(
                    FILEDITCH_CHUNK_URL,
                    params={"action": "chunk", "id": upload_id, "i": str(chunk_index)},
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/octet-stream",
                        "Content-Length": str(expected),
                        "User-Agent": USER_AGENT,
                    },
                    content=body,
                )
                payload = parse_json(response)
                if response.status_code == 200 and payload.get("success") is True:
                    return chunk_index, expected, time.monotonic() - started
                last_error = str(payload.get("error") or f"HTTP {response.status_code}")
        except Exception as exc:  # noqa: BLE001 - the next attempt refreshes Drive's session.
            last_error = f"{type(exc).__name__}: {exc}"
        if attempt < MAX_ATTEMPTS:
            time.sleep(min(2 * attempt, 10))

    raise RuntimeError(f"FileDitch chunk {chunk_index} failed: {last_error}")


def finish_upload(upload_id: str) -> dict[str, object]:
    response = httpx.post(
        FILEDITCH_CHUNK_URL,
        params={"action": "finish", "id": upload_id},
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        timeout=httpx.Timeout(connect=60.0, read=120.0, write=60.0, pool=60.0),
    )
    response.raise_for_status()
    payload = parse_json(response)
    if payload.get("success") is not True:
        raise RuntimeError(str(payload.get("error") or "FileDitch finalization failed."))
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True, help="Public Google Drive file URL.")
    parser.add_argument("--filename", required=True, help="Original filename to preserve as the extension only.")
    parser.add_argument("--result-json", required=True)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    args = parser.parse_args()
    if not 1 <= args.workers <= MAX_WORKERS:
        parser.error(f"workers must be between 1 and {MAX_WORKERS}")
    return args


def write_result(path: str, result: dict[str, object]) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(output)


def main() -> int:
    args = parse_args()
    started = time.monotonic()
    result: dict[str, object] = {"ok": False, "size_bytes": 0}
    try:
        file_id = drive_file_id_from_url(args.url)
        direct_url = resolve_drive_confirmation_url(file_id)
        size = drive_range_total(direct_url, file_id)
        storage_filename = randomized_storage_filename(args.filename)
        upload_id, chunk_size, total_chunks = initialize_upload(storage_filename, size)
        result.update({"size_bytes": size, "chunk_size": chunk_size, "total_chunks": total_chunks})
        print(f"Source verified: {size} bytes; FileDitch chunks: {total_chunks}; parallel: {args.workers}.")

        completed = 0
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(
                    upload_chunk,
                    file_id,
                    direct_url,
                    size,
                    upload_id,
                    index,
                    chunk_size,
                ): index
                for index in range(total_chunks)
            }
            for future in as_completed(futures):
                index, count, elapsed = future.result()
                completed += count
                print(
                    f"Chunk {index + 1}/{total_chunks} complete: "
                    f"{completed / (1024 ** 3):.2f}/{size / (1024 ** 3):.2f} GiB "
                    f"({completed * 100 / size:.1f}%), {elapsed:.1f}s."
                )

        payload = finish_upload(upload_id)
        final_size = int(payload.get("size") or 0)
        url = str(payload.get("url") or "").strip()
        if final_size != size or not url.startswith("https://fileditchfiles.st/"):
            raise RuntimeError(f"FileDitch stored {final_size} bytes; expected {size} bytes.")
        elapsed = time.monotonic() - started
        result.update(
            {
                "ok": True,
                "url": url,
                "filename": storage_filename,
                "size_bytes": final_size,
                "elapsed_seconds": round(elapsed, 3),
                "average_mib_per_second": round(size / (1024 ** 2) / max(elapsed, 0.001), 3),
            }
        )
        write_result(args.result_json, result)
        print("FileDitch upload completed; result saved to the private artifact.")
        return 0
    except Exception as exc:  # noqa: BLE001 - keep source details out of public logs.
        result.update({"error": f"{type(exc).__name__}: {exc}", "elapsed_seconds": round(time.monotonic() - started, 3)})
        write_result(args.result_json, result)
        print(f"FileDitch upload failed: {type(exc).__name__}.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
