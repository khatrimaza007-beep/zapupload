#!/usr/bin/env python3
"""Download a public cloud source to a GitHub runner and upload it to TransferIt."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, parse_qsl, urlencode, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup


USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36"
DEFAULT_STAGED_MAX_GIB = 25.0
DEFAULT_CLEANUP_ABOVE_GIB = 8.0
DISK_RESERVE_BYTES = 2 * 1024 * 1024 * 1024
MEDIA_EXTENSIONS = (".mkv", ".mp4", ".avi", ".mov", ".webm", ".zip", ".rar", ".7z")


@dataclass(frozen=True)
class ResolvedSource:
    original_url: str
    direct_url: str
    filename: str
    size_bytes: int
    kind: str


def detect_source(url: str, requested_kind: str = "") -> str:
    if requested_kind in {"r2", "skydrop", "gphotos", "gdrive", "generic"}:
        return requested_kind
    host = (urlparse(url).hostname or "").lower()
    if host == "drive.google.com" or host.endswith(".drive.google.com"):
        return "gdrive"
    if host == "skydrop.sbs" or host.endswith(".skydrop.sbs"):
        return "skydrop"
    if any(item in host for item in ("googleusercontent.com", "ggpht.com", "photos.google.com", "vidfiles.com")):
        return "gphotos"
    if any(item in host for item in ("kmphotos", "r2.dev", "cloudflare")):
        return "r2"
    return "generic"


def add_skydrop_direct_flag(url: str) -> str:
    parsed = urlparse(url)
    pairs = [(key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True) if key.lower() != "alexdirect"]
    pairs.append(("alexdirect", "1"))
    return parsed._replace(query=urlencode(pairs, doseq=True)).geturl()


def is_html(content_type: str) -> bool:
    return "html" in content_type.lower()


def direct_link_from_html(page_url: str, html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    candidates: list[str] = []
    for anchor in soup.find_all("a", href=True):
        href = str(anchor["href"]).strip()
        if href:
            candidates.append(urljoin(page_url, href))
    for element in soup.find_all(("video", "source")):
        source = str(element.get("src") or "").strip()
        if source:
            candidates.append(urljoin(page_url, source))

    def score(candidate: str) -> int:
        lower = candidate.lower()
        if "dl=r2" in lower:
            return 4
        if "dl=" in lower:
            return 3
        if any(urlparse(lower).path.endswith(extension) for extension in MEDIA_EXTENSIONS):
            return 2
        return 0

    ranked = sorted(((score(candidate), candidate) for candidate in candidates), reverse=True)
    for priority, candidate in ranked:
        if priority:
            return candidate
    raise ValueError("Could not find a direct file URL on the source landing page.")


def filename_from_headers_or_url(headers: httpx.Headers, direct_url: str, requested_filename: str) -> str:
    if requested_filename.strip():
        return Path(requested_filename).name
    content_disposition = headers.get("Content-Disposition", "")
    match = re.search(r"filename\*?=(?:UTF-8''|\")?([^;\"]+)", content_disposition, flags=re.IGNORECASE)
    if match:
        return Path(match.group(1).strip().strip("'\"")).name
    query_name = parse_qs(urlparse(direct_url).query).get("file", [""])[0]
    if query_name:
        return Path(query_name).name
    path_name = Path(urlparse(direct_url).path).name
    return path_name or "downloaded_file.bin"


def response_size(headers: httpx.Headers) -> int:
    content_range = headers.get("Content-Range", "")
    if "/" in content_range:
        try:
            return int(content_range.rsplit("/", 1)[1])
        except ValueError:
            pass
    try:
        return int(headers.get("Content-Length", "0"))
    except ValueError:
        return 0


def inspect_http_source(url: str, requested_filename: str, kind: str) -> ResolvedSource:
    headers = {"User-Agent": USER_AGENT}
    direct_url = url
    with httpx.Client(follow_redirects=True, timeout=httpx.Timeout(120.0), headers=headers) as client:
        for _ in range(2):
            request_headers = {"Range": "bytes=0-0"}
            with client.stream("GET", direct_url, headers=request_headers) as response:
                response.raise_for_status()
                content_type = response.headers.get("Content-Type", "")
                if not is_html(content_type):
                    return ResolvedSource(
                        original_url=url,
                        direct_url=str(response.url),
                        filename=filename_from_headers_or_url(response.headers, str(response.url), requested_filename),
                        size_bytes=response_size(response.headers),
                        kind=kind,
                    )
                html = response.read().decode(response.encoding or "utf-8", errors="replace")
                direct_url = direct_link_from_html(str(response.url), html)
    raise ValueError("Source landing page repeatedly resolved to HTML instead of file bytes.")


def free_runner_disk_space() -> None:
    cleanup_paths = ("/usr/share/dotnet", "/usr/local/lib/android", "/opt/ghc", "/usr/local/share/boost")
    print("Freeing unused GitHub runner toolchains before the staged transfer.")
    try:
        completed = subprocess.run(["sudo", "rm", "-rf", *cleanup_paths], check=False, timeout=300)
    except (OSError, subprocess.SubprocessError) as exc:
        print(f"Runner disk cleanup could not finish: {exc}")
        return
    if completed.returncode:
        print(f"Runner disk cleanup exited with code {completed.returncode}; checking free space anyway.")


def has_disk_capacity(size_bytes: int) -> bool:
    free_bytes = shutil.disk_usage(tempfile.gettempdir()).free
    required = size_bytes + DISK_RESERVE_BYTES
    print(
        f"Runner free space: {free_bytes / (1024 ** 3):.1f} GiB; "
        f"required: {required / (1024 ** 3):.1f} GiB."
    )
    return free_bytes >= required


def download_single_stream(url: str, output_path: Path) -> None:
    started_at = time.monotonic()
    downloaded = 0
    with httpx.Client(
        follow_redirects=True,
        timeout=httpx.Timeout(connect=60.0, read=600.0, write=120.0, pool=120.0),
        headers={"User-Agent": USER_AGENT},
    ) as client:
        with client.stream("GET", url) as response:
            response.raise_for_status()
            expected = response_size(response.headers)
            with output_path.open("wb") as output:
                for block in response.iter_bytes(chunk_size=8 * 1024 * 1024):
                    if not block:
                        continue
                    output.write(block)
                    downloaded += len(block)
                    elapsed = max(time.monotonic() - started_at, 0.001)
                    percent = downloaded * 100 / expected if expected else 0
                    print(
                        f"\rDownloading: {downloaded / (1024 ** 2):.1f} MiB "
                        f"({percent:.1f}%) | {downloaded / (1024 ** 2) / elapsed:.1f} MiB/s",
                        end="",
                        flush=True,
                    )
    print("")


def download_http_source(source: ResolvedSource, output_path: Path, download_workers: int) -> None:
    aria2c = shutil.which("aria2c")
    target_connections = 4 if source.kind in {"gphotos", "skydrop"} else 16
    connections = max(1, min(target_connections, download_workers))
    if aria2c:
        print(f"Downloading with aria2c using {connections} connections ({source.kind}).")
        command = [
            aria2c,
            "-x", str(connections),
            "-s", str(connections),
            "-k", "8M" if connections == 4 else "1M",
            "--file-allocation=none",
            "--allow-overwrite=true",
            "--auto-file-renaming=false",
            "--summary-interval=1",
            "--console-log-level=warn",
            "--user-agent", USER_AGENT,
            "-d", str(output_path.parent),
            "-o", output_path.name,
            source.direct_url,
        ]
        completed = subprocess.run(command, check=False)
        if completed.returncode == 0 and output_path.is_file() and output_path.stat().st_size > 0:
            return
        output_path.unlink(missing_ok=True)
        output_path.with_suffix(f"{output_path.suffix}.aria2").unlink(missing_ok=True)
        print("aria2c failed; retrying once with a single HTTP stream.")
    download_single_stream(source.direct_url, output_path)


def download_gdrive(url: str, output_path: Path) -> None:
    try:
        import gdown
    except ImportError as exc:
        raise RuntimeError("gdown is required for Google Drive sources.") from exc
    print("Downloading Google Drive source with gdown.")
    downloaded = gdown.download(url=url, output=str(output_path), quiet=False, fuzzy=True, resume=False)
    if not downloaded:
        raise ValueError("gdown did not download a file. Check that the Google Drive link is public.")
    actual_path = Path(downloaded)
    if actual_path != output_path:
        actual_path.replace(output_path)
    if not output_path.is_file() or output_path.stat().st_size == 0:
        raise ValueError("gdown completed without a usable local file.")
    if output_path.stat().st_size < 100_000:
        sample = output_path.read_bytes()[:4096].lower()
        if b"<html" in sample or b"<!doctype" in sample:
            raise ValueError("Google Drive returned an HTML page. Check link sharing permissions.")


def upload_to_transferit(local_path: Path, upload_workers: int) -> str:
    try:
        from transferit import Transferit
    except ImportError as exc:
        raise RuntimeError("transferit-py is required for the upload step.") from exc
    total = local_path.stat().st_size
    started_at = time.monotonic()
    print("Uploading to TransferIt with the native file uploader.")

    def progress(sent: int, expected: int) -> None:
        elapsed = max(time.monotonic() - started_at, 0.001)
        percent = sent * 100 / expected if expected else 0
        print(
            f"\rUploading: {sent / (1024 ** 2):.1f} / {expected / (1024 ** 2):.1f} MiB "
            f"({percent:.1f}%) | {sent / (1024 ** 2) / elapsed:.1f} MiB/s",
            end="",
            flush=True,
        )

    with Transferit() as client:
        result = client.upload(
            str(local_path),
            concurrency=max(1, min(8, upload_workers)),
            on_progress=progress,
        )
    print("")
    link = str(getattr(result, "url", result)).strip()
    if not link.startswith("https://transfer.it/t/"):
        raise ValueError("TransferIt native uploader returned no valid share URL.")
    return link


def write_result(path: str | None, result: dict[str, Any]) -> None:
    if not path:
        return
    output = Path(path).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f"{output.name}.tmp")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, output)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", nargs="?", help="Public R2, SKYDROP, Google Photos, or Google Drive URL.")
    parser.add_argument("--url", dest="url_option", help="Public R2, SKYDROP, Google Photos, or Google Drive URL.")
    parser.add_argument("--source-kind", choices=("r2", "skydrop", "gphotos", "gdrive", "generic"))
    parser.add_argument("--filename", help="Filename to preserve in TransferIt.")
    parser.add_argument("--result-json", help="Write a structured success or failure result to this file.")
    parser.add_argument("--mode", choices=("auto", "staged"), default="auto")
    parser.add_argument("--staged-max-gib", type=float, default=DEFAULT_STAGED_MAX_GIB)
    parser.add_argument("--cleanup-above-gib", type=float, default=DEFAULT_CLEANUP_ABOVE_GIB)
    parser.add_argument("--download-workers", type=int, default=16)
    parser.add_argument("--upload-workers", type=int, default=8)
    args = parser.parse_args()
    if args.url and args.url_option:
        parser.error("Use either the positional URL or --url, not both.")
    args.source_url = (args.url_option or args.url or "").strip()
    parsed = urlparse(args.source_url)
    if parsed.scheme != "https" or not parsed.hostname:
        parser.error("--url must be a public HTTPS URL.")
    if args.staged_max_gib <= 0 or args.cleanup_above_gib < 0:
        parser.error("Staging limits must be positive.")
    if args.download_workers < 1 or args.upload_workers < 1:
        parser.error("Worker counts must be at least 1.")
    return args


def main() -> int:
    args = parse_args()
    source_url = args.source_url
    kind = detect_source(source_url, args.source_kind or "")
    if kind == "skydrop":
        source_url = add_skydrop_direct_flag(source_url)
    started_at = time.monotonic()
    result: dict[str, Any] = {"ok": False, "source_url": source_url, "source_kind": kind}
    temp_dir: Path | None = None
    try:
        if kind == "gdrive":
            filename = Path(args.filename).name if args.filename else "google_drive_download.bin"
            source = ResolvedSource(source_url, source_url, filename, 0, kind)
        else:
            source = inspect_http_source(source_url, args.filename or "", kind)
        result["resolved_url"] = source.direct_url
        if source.size_bytes:
            result["size_bytes"] = source.size_bytes
            print(f"Source: {source.kind}; size: {source.size_bytes / (1024 ** 3):.2f} GiB.")
            staged_limit = int(args.staged_max_gib * 1024 ** 3)
            if source.size_bytes > staged_limit:
                raise ValueError(
                    f"Source is {source.size_bytes / (1024 ** 3):.2f} GiB, above the staged limit "
                    f"of {args.staged_max_gib:.2f} GiB."
                )
            if source.size_bytes > int(args.cleanup_above_gib * 1024 ** 3):
                free_runner_disk_space()
            if not has_disk_capacity(source.size_bytes):
                raise ValueError("GitHub runner does not have enough free disk space for this staged transfer.")
        else:
            print(f"Source: {source.kind}; size is not available before download.")

        temp_dir = Path(tempfile.mkdtemp(prefix="transferit-"))
        local_path = temp_dir / Path(source.filename).name
        download_started = time.monotonic()
        if source.kind == "gdrive":
            download_gdrive(source_url, local_path)
        else:
            download_http_source(source, local_path, args.download_workers)
        size_bytes = local_path.stat().st_size
        print(
            f"Download finished in {time.monotonic() - download_started:.0f}s "
            f"({size_bytes / (1024 ** 2) / max(time.monotonic() - download_started, 0.001):.1f} MiB/s)."
        )
        transfer_url = upload_to_transferit(local_path, args.upload_workers)
        elapsed = time.monotonic() - started_at
        result.update(
            {
                "ok": True,
                "transfer_url": transfer_url,
                "filename": local_path.name,
                "size_bytes": size_bytes,
                "elapsed_seconds": round(elapsed, 3),
                "average_mib_per_second": round(size_bytes / (1024 ** 2) / max(elapsed, 0.001), 3),
                "mode": "staged",
            }
        )
        write_result(args.result_json, result)
        print(f"TransferIt link: {transfer_url}")
        return 0
    except Exception as exc:  # noqa: BLE001 - result artifact must report every failure.
        result.update({"error": f"{type(exc).__name__}: {exc}", "elapsed_seconds": round(time.monotonic() - started_at, 3)})
        write_result(args.result_json, result)
        print(f"TransferIt failed: {result['error']}", file=sys.stderr)
        return 1
    finally:
        if temp_dir is not None:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
