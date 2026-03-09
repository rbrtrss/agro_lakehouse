"""Download a sample INDEC exports CSV from datos.gob.ar via the CKAN API."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import httpx
from rich.console import Console

# Add repo root to path so ingestion.utils is importable when run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ingestion.utils.http import download_file

CKAN_API = "https://datos.gob.ar/api/3/action/package_show"
DATASET_ID = "sspm-exportaciones-por-provincia-por-pais-destino"
DEST_DIR = Path(__file__).resolve().parents[2] / "data" / "samples" / "indec"

console = Console()


async def main() -> None:
    console.rule("[bold green]INDEC — sample download")
    console.print(f"Fetching dataset metadata for [cyan]{DATASET_ID}[/cyan]…")

    async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
        resp = await client.get(CKAN_API, params={"id": DATASET_ID})
        resp.raise_for_status()
        data = resp.json()

    if not data.get("success"):
        console.print(f"[red]CKAN API error:[/red] {data.get('error')}")
        sys.exit(1)

    resources = data["result"]["resources"]
    csv_resources = [r for r in resources if r.get("format", "").upper() == "CSV"]

    if not csv_resources:
        console.print("[yellow]No CSV resources found — listing all formats:[/yellow]")
        for r in resources:
            console.print(f"  {r.get('format')} — {r.get('name')} — {r.get('url')}")
        sys.exit(1)

    resource = csv_resources[0]
    url: str = resource["url"]
    filename = Path(url.split("?")[0]).name or "indec_exports.csv"
    dest = DEST_DIR / filename

    console.print(f"Found CSV: [cyan]{resource.get('name')}[/cyan]")
    console.print(f"URL: {url}")
    console.print(f"Saving to: {dest}")

    await download_file(url, dest)
    console.print(
        f"[bold green]Done.[/bold green] Saved {dest.stat().st_size / 1024:.1f} KB → {dest}"
    )


if __name__ == "__main__":
    asyncio.run(main())
