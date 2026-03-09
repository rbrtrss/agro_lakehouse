"""Shared async HTTP utilities with retry and progress reporting."""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
from rich.progress import (
    Progress,
    SpinnerColumn,
    DownloadColumn,
    TransferSpeedColumn,
    TimeElapsedColumn,
    BarColumn,
    TextColumn,
)


_MAX_RETRIES = 3
_BACKOFF_BASE = 2  # seconds


async def download_file(url: str, dest_path: Path) -> Path:
    """Download *url* to *dest_path*, retrying up to 3 times with exponential backoff.

    Returns the destination path on success.
    """
    dest_path = Path(dest_path)
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(follow_redirects=True, timeout=60) as client:
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[bold blue]{task.description}"),
                    BarColumn(),
                    DownloadColumn(),
                    TransferSpeedColumn(),
                    TimeElapsedColumn(),
                ) as progress:
                    task = progress.add_task(
                        f"Downloading {dest_path.name}", total=None
                    )
                    async with client.stream("GET", url) as response:
                        response.raise_for_status()
                        total = int(response.headers.get("content-length", 0)) or None
                        progress.update(task, total=total)
                        with dest_path.open("wb") as fh:
                            async for chunk in response.aiter_bytes(chunk_size=65536):
                                fh.write(chunk)
                                progress.advance(task, len(chunk))
            return dest_path
        except (httpx.HTTPError, OSError) as exc:
            if attempt == _MAX_RETRIES:
                raise RuntimeError(
                    f"Failed to download {url} after {_MAX_RETRIES} attempts: {exc}"
                ) from exc
            wait = _BACKOFF_BASE**attempt
            print(
                f"  [attempt {attempt}/{_MAX_RETRIES}] error: {exc}. Retrying in {wait}s…"
            )
            await asyncio.sleep(wait)

    raise RuntimeError("Unreachable")  # pragma: no cover
