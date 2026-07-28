#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Cloud-to-Cloud Upload to TransferIt (v2 - Pipelined)

Architecture:
  - DOWNLOAD POOL: N async workers fetch chunks from the source URL concurrently
  - PIPELINE QUEUE: Downloaded chunks are encrypted and queued in memory
  - UPLOAD POOL: M async WebSocket workers drain the queue and push to Transfer.it
  - This means downloading and uploading happen SIMULTANEOUSLY, saturating both pipes.

Features:
  - Auto-resolves landing pages to direct stream links.
  - Auto-refreshes expired URL signatures mid-transfer.
  - AES-128-CTR encryption in memory.
  - Auto mode uses a staged local file for faster R2-to-TransferIt uploads
    when the file fits safely on the GitHub runner disk.
  - Large files retain the zero-local-disk streaming path.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import math
import struct
import asyncio
import tempfile
import time
import uuid
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from datetime import datetime
from typing import Optional
from urllib.parse import parse_qs, urlparse, urljoin

# Ensure UTF-8 output encoding for Windows/Linux console
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import httpx
import websockets
from bs4 import BeautifulSoup
from Cryptodome.Cipher import AES

try:
    import transferit
    from transferit import Transferit
    from transferit._api import MegaAPI, MegaAPIError, SHARE_BASE
    from transferit._crypto import rand_a32, crc32b, encrypt_chunk_and_mac
    from transferit._upload import iter_chunks

    WS_BUFFER_LIMIT = 4 * 1024 * 1024
    DOWNLOAD_CONCURRENCY = 16   # Parallel HTTP Range fetches from source
    UPLOAD_CONCURRENCY = 12     # Parallel WebSocket upload connections
    DEFAULT_STAGED_MAX_GIB = 25.0
    DEFAULT_CLEANUP_ABOVE_GIB = 8.0
    STAGED_DISK_RESERVE_BYTES = 2 * 1024 * 1024 * 1024

    # Use uvloop on Linux for ~2-4x faster async I/O
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        print("⚡ uvloop enabled for maximum async I/O performance")
    except ImportError:
        pass  # Fall back to default event loop
except ImportError as err:
    print(f"[X] Error loading 'transferit-py': {err}")
    print("    Please run: python -m pip install transferit-py")
    sys.exit(1)


class HTTPRemoteFile:
    """Wrapper that presents an HTTP direct URL or landing page with Range header support as a file-like object."""

    def __init__(self, url: str, filename: Optional[str] = None, headers: Optional[dict] = None):
        self.original_url = url
        self.url = url
        self.custom_filename = filename
        self.headers = headers or {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        self.size = 0
        self.filename = ""
        self.async_client = None
        self._refresh_lock = asyncio.Lock() if False else None  # Will init in async context

        self._resolve_url_and_metadata()

    def _resolve_url_and_metadata(self):
        with httpx.Client(follow_redirects=True, timeout=120.0, headers=self.headers) as client:
            resp = client.get(self.url, headers={"Range": "bytes=0-0"})

            # Step 1: Check if input URL is an HTML landing page
            content_type = resp.headers.get("Content-Type", "").lower()
            if "text/html" in content_type:
                print("ℹ️ Input URL is a web landing page. Extracting direct stream link...")
                soup = BeautifulSoup(resp.text, "html.parser")

                direct_link = None
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if "dl=r2" in href.lower():
                        direct_link = urljoin(self.url, href)
                        break

                if not direct_link:
                    for a in soup.find_all("a", href=True):
                        href = a["href"]
                        if "dl=" in href or any(href.lower().endswith(ext) for ext in (".mkv", ".mp4", ".avi", ".mov")):
                            direct_link = urljoin(self.url, href)
                            break

                if not direct_link:
                    raise ValueError(f"Could not find direct download link on landing page {self.url}")

                print(f"✅ Found direct stream link: {direct_link}")
                self.url = direct_link
                # Re-query range header for the direct link
                resp = client.get(self.url, headers={"Range": "bytes=0-0"})

            if resp.status_code != 206:
                raise ValueError(
                    f"Source must support HTTP byte ranges; got HTTP {resp.status_code} for {self.url}"
                )

            # Step 2: Extract total size from Content-Range (e.g. bytes 0-0/2389971451) or Content-Length
            content_range = resp.headers.get("Content-Range", "")
            if "/" in content_range:
                try:
                    self.size = int(content_range.split("/")[-1])
                except ValueError:
                    pass

            if not self.size:
                cl = resp.headers.get("Content-Length", 0)
                try:
                    self.size = int(cl)
                except ValueError:
                    pass

            if not self.size:
                raise ValueError(f"Could not determine file size (Content-Length / Content-Range missing) from {self.url}")

            # Step 3: Derive filename
            if self.custom_filename:
                self.filename = self.custom_filename
            else:
                cd = resp.headers.get("Content-Disposition", "")
                if "filename=" in cd:
                    self.filename = cd.split("filename=")[1].strip('"\\\'  ')
                else:
                    parsed = urlparse(self.url)
                    qs = parse_qs(parsed.query)
                    if "file" in qs and qs["file"][0]:
                        self.filename = qs["file"][0]
                    else:
                        self.filename = os.path.basename(parsed.path) or "downloaded_video.mkv"

    async def _ensure_client(self):
        if self.async_client is None:
            limits = httpx.Limits(max_keepalive_connections=50, max_connections=100)
            self.async_client = httpx.AsyncClient(follow_redirects=True, timeout=120.0, limits=limits)
        if self._refresh_lock is None:
            self._refresh_lock = asyncio.Lock()

    async def read_range_async(self, pos: int, length: int) -> bytes:
        if length <= 0:
            return b""
        await self._ensure_client()
        range_header = {"Range": f"bytes={pos}-{pos + length - 1}"}
        req_headers = {**self.headers, **range_header}
        resp = await self.async_client.get(self.url, headers=req_headers)

        # If the direct URL signature expired mid-transfer, refresh it from the landing page
        if resp.status_code in (403, 404, 410):
            async with self._refresh_lock:
                # Double-check: another worker may have already refreshed
                resp2 = await self.async_client.get(self.url, headers=req_headers)
                if resp2.status_code in (403, 404, 410):
                    print(f"\n🔄 Stream link expired mid-transfer. Auto-refreshing from landing page...")
                    self.url = self.original_url
                    await asyncio.to_thread(self._resolve_url_and_metadata)
                    resp = await self.async_client.get(self.url, headers=req_headers)
                else:
                    resp = resp2

        resp.raise_for_status()
        if resp.status_code != 206:
            raise ValueError(
                f"Source ignored HTTP Range bytes={pos}-{pos + length - 1}: HTTP {resp.status_code}"
            )
        content_range = resp.headers.get("Content-Range", "")
        expected_range = f"bytes {pos}-{pos + length - 1}/"
        if not content_range.lower().startswith(expected_range.lower()):
            raise ValueError(
                f"Source returned an unexpected Content-Range for bytes={pos}-{pos + length - 1}: "
                f"{content_range or '<missing>'}"
            )
        if len(resp.content) != length:
            raise ValueError(
                f"Source returned {len(resp.content)} bytes for range {pos}-{pos + length - 1}; expected {length}"
            )
        return resp.content

    async def close(self):
        if self.async_client:
            await self.async_client.aclose()
            self.async_client = None


async def _pipelined_upload(
    ws_host: str,
    ws_uri: str,
    remote_file: HTTPRemoteFile,
    ul_key: list[int],
    *,
    fileno: int = 1,
    dl_concurrency: int = DOWNLOAD_CONCURRENCY,
    ul_concurrency: int = UPLOAD_CONCURRENCY,
    progress=None,
) -> tuple[bytes, list[list[int]]]:
    """
    Pipelined upload: Download workers feed an async queue, Upload workers drain it.
    Downloads and uploads happen SIMULTANEOUSLY for maximum throughput.
    """
    url = f"wss://{ws_host}/{ws_uri}"
    size = remote_file.size

    chunk_offsets, need_empty_tail = iter_chunks(size)
    dl_queue: list[tuple[int, int]] = list(chunk_offsets)
    if need_empty_tail:
        dl_queue.append((size, 0))

    # Pipeline queue: downloaded+encrypted chunks waiting to be uploaded
    # Each item is (pos, length, ciphertext_bytes)
    pipe = asyncio.Queue(maxsize=32)  # Backpressure: max 32 chunks buffered

    dl_lock = asyncio.Lock()
    macs_by_offset: dict[int, list[int]] = {}
    completion_token: list[bytes | None] = [None]
    done = asyncio.Event()
    bytes_acked = [0]
    lengths_by_offset = dict(chunk_offsets)
    progress_lock = asyncio.Lock()
    dl_finished = asyncio.Event()

    # Process pool for offloading CPU-heavy AES encryption to separate cores
    proc_pool = ProcessPoolExecutor(max_workers=min(4, os.cpu_count() or 2))
    loop = asyncio.get_running_loop()

    # ── Download Workers ──────────────────────────────────────
    async def dl_worker(wid: int):
        while True:
            async with dl_lock:
                if not dl_queue:
                    return
                pos, length = dl_queue.pop(0)

            if done.is_set():
                return

            data = await remote_file.read_range_async(pos, length)
            # Offload CPU-heavy encryption to a separate process core
            ct, mac = await loop.run_in_executor(proc_pool, encrypt_chunk_and_mac, data, ul_key, pos)
            macs_by_offset[pos] = mac
            await pipe.put((pos, length, ct))

    # ── Upload Workers ────────────────────────────────────────
    async def _handle_message(mview: bytes) -> None:
        if len(mview) < 9:
            return
        body, mcrc = mview[:-4], struct.unpack_from("<I", mview, len(mview) - 4)[0]
        if crc32b(body) != mcrc:
            raise MegaAPIError("ws CRC mismatch on server msg")
        mtype = struct.unpack_from("<b", body, 12)[0]
        if mtype < 0:
            raise MegaAPIError(f"server signalled upload error type={mtype}")
        mpos = struct.unpack_from("<Q", body, 4)[0]
        if mtype in (1, 7):
            length = lengths_by_offset.get(mpos, 0)
            async with progress_lock:
                bytes_acked[0] += length
                if progress:
                    progress(min(bytes_acked[0], size), size)
        elif mtype == 3:
            raise MegaAPIError(f"server reports chunk CRC fail at offset {mpos}")
        elif mtype == 4:
            tlen = body[13]
            completion_token[0] = bytes(body[14 : 14 + tlen])
            done.set()

    async def connect_ws_with_retry(ws_url: str, retries: int = 5):
        for attempt in range(retries):
            try:
                return await websockets.connect(
                    ws_url,
                    max_size=None,
                    ping_interval=20,
                    ping_timeout=60,
                    open_timeout=60.0,
                )
            except Exception as e:
                if attempt == retries - 1:
                    raise e
                await asyncio.sleep(1.5 * (attempt + 1))

    async def ul_worker(wid: int):
        try:
            await asyncio.sleep(wid * 0.2)
            async with await connect_ws_with_retry(url) as ws:

                async def recv_loop():
                    async for msg in ws:
                        if isinstance(msg, (bytes, bytearray)):
                            await _handle_message(bytes(msg))
                            if done.is_set():
                                return

                recv_task = asyncio.create_task(recv_loop())
                try:
                    while not done.is_set():
                        # Try to get a chunk from the pipeline
                        try:
                            pos, length, ct = await asyncio.wait_for(pipe.get(), timeout=1.0)
                        except asyncio.TimeoutError:
                            if dl_finished.is_set() and pipe.empty():
                                break
                            continue

                        # Backpressure on the WebSocket send buffer
                        while True:
                            transport = getattr(ws, "transport", None)
                            buffered = (
                                getattr(transport, "_buffer_size", 0)
                                if transport
                                else 0
                            )
                            if buffered < WS_BUFFER_LIMIT or done.is_set():
                                break
                            await asyncio.sleep(0.01)

                        if done.is_set():
                            break

                        header = bytearray(20)
                        struct.pack_into("<I", header, 0, fileno)
                        struct.pack_into("<Q", header, 4, pos)
                        struct.pack_into("<I", header, 12, length)
                        struct.pack_into(
                            "<I", header, 16, crc32b(ct, crc32b(bytes(header[:16])))
                        )
                        await ws.send(bytes(header))
                        if ct:
                            await ws.send(ct)

                    try:
                        await asyncio.wait_for(done.wait(), timeout=120)
                    except asyncio.TimeoutError:
                        pass
                finally:
                    if not recv_task.done():
                        recv_task.cancel()
                        try:
                            await recv_task
                        except (asyncio.CancelledError, Exception):
                            pass
        except Exception:
            done.set()
            raise

    # ── Launch Pipeline ───────────────────────────────────────
    # Start download workers
    n_dl = max(1, min(dl_concurrency, len(dl_queue)))
    dl_tasks = [asyncio.create_task(dl_worker(i)) for i in range(n_dl)]

    # Start upload workers
    n_ul = max(1, min(ul_concurrency, len(list(chunk_offsets) + ([1] if need_empty_tail else []))))
    ul_tasks = [asyncio.create_task(ul_worker(i)) for i in range(n_ul)]

    # Wait for all downloads to finish, then signal upload workers
    dl_results = await asyncio.gather(*dl_tasks, return_exceptions=True)
    dl_finished.set()

    # Wait for all uploads to finish
    ul_results = await asyncio.gather(*ul_tasks, return_exceptions=True)

    await remote_file.close()

    for r in dl_results + ul_results:
        if isinstance(r, Exception) and not isinstance(r, asyncio.CancelledError):
            raise r

    if completion_token[0] is None:
        raise MegaAPIError("upload ended without completion token")

    ordered_macs = [macs_by_offset[o] for o in sorted(macs_by_offset)]
    return completion_token[0], ordered_macs


def upload_url_to_transferit(
    url: str,
    custom_filename: Optional[str] = None,
    dl_concurrency: int = DOWNLOAD_CONCURRENCY,
    ul_concurrency: int = UPLOAD_CONCURRENCY,
    on_progress=None,
    remote_file: HTTPRemoteFile | None = None,
):
    """
    Cloud-to-Cloud Upload: Directly streams from HTTP URL -> Encrypts -> Transfer.it
    Uses a pipelined architecture for maximum throughput.
    """
    print(f"🔗 Inspecting URL: {url}")
    remote_file = remote_file or HTTPRemoteFile(url, filename=custom_filename)
    filename = remote_file.filename
    size_mb = remote_file.size / (1024 * 1024)

    print(f"🎬 Target File: {filename}")
    print(f"📦 Total Size: {size_mb:.2f} MB")
    print(f"⚡ Pipelined Cloud-to-Cloud: {dl_concurrency} download + {ul_concurrency} upload workers\n")

    with Transferit() as client:
        api = client._api
        api.create_ephemeral_session()

        title = filename
        xh, root_h, _ = api.create_transfer(title)

        pools = api.upload_pools()
        def _pick_pool(sz: int) -> tuple[str, str]:
            for entry in pools:
                if len(entry) < 2:
                    continue
                host, uri = entry[0], entry[1]
                limit = entry[2] if len(entry) > 2 else 0
                if not limit or sz <= limit:
                    return host, uri
            raise MegaAPIError(f"no upload pool available: {pools!r}")

        host, uri = _pick_pool(remote_file.size)
        ul_key = rand_a32(6)
        idx = client._next_fileno()

        token, macs = asyncio.run(
            _pipelined_upload(
                host,
                uri,
                remote_file,
                ul_key,
                fileno=idx,
                dl_concurrency=dl_concurrency,
                ul_concurrency=ul_concurrency,
                progress=on_progress,
            )
        )

        api.finalise_file(root_h, token, ul_key, macs, filename)
        api.close_transfer(xh)

        transfer_url = f"{SHARE_BASE}/t/{xh}"
        return transfer_url, filename, remote_file.size


def upload_staged_to_transferit(
    remote_file: HTTPRemoteFile,
    upload_concurrency: int,
) -> tuple[str, str, int]:
    """Download once to runner storage, then use TransferIt's native file uploader."""
    suffix = f"-{uuid.uuid4().hex}-{Path(remote_file.filename).name}"
    temporary_path = Path(tempfile.gettempdir()) / f"transferit{suffix}"
    total = remote_file.size
    downloaded = 0
    download_started = time.monotonic()

    try:
        print(f"Downloading {total / (1024 * 1024):.1f} MiB to the GitHub runner...")
        with httpx.Client(
            follow_redirects=True,
            timeout=httpx.Timeout(connect=60.0, read=120.0, write=120.0, pool=120.0),
            headers=remote_file.headers,
        ) as client:
            with client.stream("GET", remote_file.url) as response:
                response.raise_for_status()
                with temporary_path.open("wb") as output:
                    for block in response.iter_bytes(chunk_size=8 * 1024 * 1024):
                        output.write(block)
                        downloaded += len(block)
                        elapsed = max(time.monotonic() - download_started, 0.001)
                        speed = downloaded / (1024 * 1024) / elapsed
                        sys.stdout.write(
                            f"\rDownloading: {downloaded / (1024 * 1024):.1f} / "
                            f"{total / (1024 * 1024):.1f} MiB ({downloaded * 100 / total:.1f}%) | "
                            f"{speed:.1f} MiB/s"
                        )
                        sys.stdout.flush()
        if downloaded != total:
            raise ValueError(f"Downloaded {downloaded} bytes, expected {total} bytes.")
        download_elapsed = max(time.monotonic() - download_started, 0.001)
        print(
            f"\nDownload finished in {download_elapsed:.0f}s "
            f"({downloaded / (1024 * 1024) / download_elapsed:.1f} MiB/s)."
        )

        upload_started = time.monotonic()
        print("Uploading to TransferIt with the native uploader...")

        def upload_progress(sent: int, expected_total: int) -> None:
            elapsed = max(time.monotonic() - upload_started, 0.001)
            speed = sent / (1024 * 1024) / elapsed
            sys.stdout.write(
                f"\rUploading: {sent / (1024 * 1024):.1f} / "
                f"{expected_total / (1024 * 1024):.1f} MiB "
                f"({sent * 100 / expected_total:.1f}%) | {speed:.1f} MiB/s"
            )
            sys.stdout.flush()

        with Transferit() as client:
            upload_result = client.upload(
                str(temporary_path),
                concurrency=max(1, min(8, upload_concurrency)),
                on_progress=upload_progress,
            )
        transfer_url = str(getattr(upload_result, "url", "")).strip()
        if not transfer_url:
            raise ValueError("TransferIt native uploader returned no share URL.")
        upload_elapsed = max(time.monotonic() - upload_started, 0.001)
        print(
            f"\nUpload finished in {upload_elapsed:.0f}s "
            f"({total / (1024 * 1024) / upload_elapsed:.1f} MiB/s)."
        )
        return transfer_url, remote_file.filename, total
    finally:
        temporary_path.unlink(missing_ok=True)


def free_runner_disk_space() -> None:
    """Remove unused preinstalled toolchains before staging a large file."""
    cleanup_paths = (
        "/usr/share/dotnet",
        "/usr/local/lib/android",
        "/opt/ghc",
        "/usr/local/share/boost",
    )
    print("Freeing unused GitHub runner toolchains for the staged transfer...")
    try:
        completed = subprocess.run(
            ["sudo", "rm", "-rf", *cleanup_paths],
            check=False,
            timeout=300,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        print(f"Runner disk cleanup could not finish: {exc}")
        return
    if completed.returncode:
        print(f"Runner disk cleanup exited with code {completed.returncode}; checking available space.")


def has_staged_disk_capacity(source_size: int) -> bool:
    available = shutil.disk_usage(tempfile.gettempdir()).free
    required = source_size + STAGED_DISK_RESERVE_BYTES
    print(
        f"Runner free space: {available / (1024 * 1024 * 1024):.1f} GiB; "
        f"required: {required / (1024 * 1024 * 1024):.1f} GiB."
    )
    return available >= required


def legacy_main():
    if len(sys.argv) < 2:
        print("Usage: python3 url_to_transferit.py <URL>")
        sys.exit(1)

    target_url = sys.argv[1]

    print("🚀 Starting Cloud-to-Cloud URL Uploader for Transfer.it (v2 - Pipelined)...")
    import time
    try:
        start_time = time.time()
        def show_progress(sent, total):
            pct = (sent / total) * 100 if total else 0
            elapsed = time.time() - start_time
            speed = (sent / (1024 * 1024)) / elapsed if elapsed > 0 else 0
            eta = ((total - sent) / (sent / elapsed)) if sent > 0 and elapsed > 0 else 0
            sys.stdout.write(f"\r⏳ {sent / (1024*1024):.1f} / {total / (1024*1024):.1f} MB ({pct:.1f}%) | 🚀 {speed:.1f} MB/s | ⏱️ ETA: {int(eta)}s   ")
            sys.stdout.flush()

        link, fname, fsize = upload_url_to_transferit(target_url, on_progress=show_progress)
        elapsed = time.time() - start_time
        avg_speed = (fsize / (1024 * 1024)) / elapsed if elapsed > 0 else 0
        print(f"\n\n✅ Cloud-to-Cloud Transfer Completed in {int(elapsed)}s!")
        print(f"🎬 File: {fname}")
        print(f"📊 Average Speed: {avg_speed:.1f} MB/s")
        print(f"🔗 Transfer.it link: {link}\n")

        output_path = Path.cwd() / "transferit_links.txt"
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with output_path.open("a", encoding="utf-8") as f:
            f.write(f"[{ts}] {target_url} -> {link} | {fname}\n")
        print(f"📝 Link appended to: {output_path}")

    except Exception as e:
        print(f"\n❌ Error during Cloud-to-Cloud transfer: {e}")
        sys.exit(1)


def write_result(path: str | None, result: dict) -> None:
    if not path:
        return
    output = Path(path).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f"{output.name}.tmp")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, output)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", nargs="?", help="Direct R2 download URL or landing page URL.")
    parser.add_argument("--url", dest="url_option", help="Direct R2 download URL or landing page URL.")
    parser.add_argument("--filename", help="Filename to preserve in TransferIt.")
    parser.add_argument("--result-json", help="Write a structured success or failure result to this file.")
    parser.add_argument(
        "--mode",
        choices=("auto", "staged", "stream"),
        default="auto",
        help="auto stages files that fit the runner disk; stream keeps data off disk.",
    )
    parser.add_argument(
        "--staged-max-gib",
        type=float,
        default=DEFAULT_STAGED_MAX_GIB,
        help="Largest file that auto mode may stage on GitHub runner storage.",
    )
    parser.add_argument(
        "--cleanup-above-gib",
        type=float,
        default=DEFAULT_CLEANUP_ABOVE_GIB,
        help="Run runner disk cleanup before staging files larger than this size.",
    )
    parser.add_argument("--download-workers", type=int, default=DOWNLOAD_CONCURRENCY)
    parser.add_argument("--upload-workers", type=int, default=UPLOAD_CONCURRENCY)
    args = parser.parse_args()
    if args.url and args.url_option:
        parser.error("Use either the positional URL or --url, not both.")
    args.source_url = args.url_option or args.url
    if not args.source_url:
        parser.error("A source URL is required.")
    if args.download_workers < 1 or args.upload_workers < 1:
        parser.error("Worker counts must be at least 1.")
    if args.staged_max_gib <= 0:
        parser.error("--staged-max-gib must be greater than zero.")
    if args.cleanup_above_gib < 0:
        parser.error("--cleanup-above-gib cannot be negative.")
    return args


def main() -> int:
    args = parse_args()
    target_url = args.source_url
    print("Starting cloud transfer to TransferIt.")
    started_at = time.time()
    result = {
        "ok": False,
        "source_url": target_url,
        "started_at": datetime.now().isoformat(timespec="seconds"),
    }
    try:
        remote_file = HTTPRemoteFile(target_url, filename=args.filename)
        staged_limit = int(args.staged_max_gib * 1024 * 1024 * 1024)
        cleanup_threshold = int(args.cleanup_above_gib * 1024 * 1024 * 1024)
        mode = args.mode
        if mode == "auto":
            mode = "staged" if remote_file.size <= staged_limit else "stream"
        if mode == "staged" and remote_file.size > cleanup_threshold:
            free_runner_disk_space()
        if mode == "staged" and not has_staged_disk_capacity(remote_file.size):
            if args.mode == "staged":
                raise ValueError("GitHub runner does not have enough free disk space for staged mode.")
            print("Insufficient runner disk space after cleanup; falling back to stream mode.")
            mode = "stream"
        print(f"Transfer mode: {mode} ({remote_file.size / (1024 * 1024):.1f} MiB source).")

        def show_progress(sent, total):
            pct = (sent / total) * 100 if total else 0
            elapsed = time.time() - started_at
            speed = (sent / (1024 * 1024)) / elapsed if elapsed > 0 else 0
            eta = ((total - sent) / (sent / elapsed)) if sent > 0 and elapsed > 0 else 0
            sys.stdout.write(
                f"\r{sent / (1024 * 1024):.1f} / {total / (1024 * 1024):.1f} MB "
                f"({pct:.1f}%) | {speed:.1f} MB/s | ETA {int(eta)}s   "
            )
            sys.stdout.flush()

        if mode == "staged":
            link, filename, size = upload_staged_to_transferit(
                remote_file,
                upload_concurrency=args.upload_workers,
            )
        else:
            link, filename, size = upload_url_to_transferit(
                target_url,
                custom_filename=args.filename,
                dl_concurrency=args.download_workers,
                ul_concurrency=args.upload_workers,
                on_progress=show_progress,
                remote_file=remote_file,
            )
        elapsed = time.time() - started_at
        average_speed = (size / (1024 * 1024)) / elapsed if elapsed > 0 else 0
        result.update(
            {
                "ok": True,
                "transfer_url": link,
                "filename": filename,
                "size_bytes": size,
                "elapsed_seconds": round(elapsed, 3),
                "average_mib_per_second": round(average_speed, 3),
                "mode": mode,
            }
        )
        write_result(args.result_json, result)
        print(f"\nTransferIt link: {link}")
        return 0
    except Exception as exc:
        result.update(
            {
                "error": f"{type(exc).__name__}: {exc}",
                "elapsed_seconds": round(time.time() - started_at, 3),
            }
        )
        write_result(args.result_json, result)
        print(f"\nTransferIt failed: {result['error']}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
