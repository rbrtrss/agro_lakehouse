"""Ingest SENASA phytosanitary certs CSV from datos.gob.ar into the Bronze S3 layer."""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from datetime import date, timezone, datetime
from pathlib import Path

import httpx
from rich.console import Console

# Add repo root to path so ingestion.utils is importable when run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ingestion.utils.http import download_file
from ingestion.utils.s3_utils import make_s3_client, object_exists, upload_file

CKAN_API = "https://datos.gob.ar/api/3/action/package_show"
DATASET_ID = "agroindustria-senasa---exportacion-productos-origen-vegetal-certificados"
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "agro-lakehouse-bronze")
SOURCE_KEY = "senasa"

console = Console()


def make_s3_key(filename: str, ingestion_dt: date | None = None) -> str:
    """Return the Hive-partitioned S3 key for *filename*.

    Partitioned by ingestion date (defaults to today UTC).
    Pattern: source=senasa/year=YYYY/month=MM/<filename>
    """
    dt = ingestion_dt or date.today()
    return f"source={SOURCE_KEY}/year={dt.year:04d}/month={dt.month:02d}/{filename}"


async def fetch_csv_resource(client: httpx.AsyncClient) -> dict:
    """Fetch CKAN metadata and return the first CSV resource dict.

    Exits with code 1 on API error or when no CSV resources are found.
    """
    console.print(f"Fetching dataset metadata for [cyan]{DATASET_ID}[/cyan]…")
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

    return csv_resources[0]


async def main() -> None:
    console.rule("[bold green]SENASA — Bronze ingestion")

    s3_client = make_s3_client()

    async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
        resource = await fetch_csv_resource(client)

    url: str = resource["url"]
    filename = Path(url.split("?")[0]).name or "senasa_certs.csv"
    s3_key = make_s3_key(filename)

    console.print(f"Found CSV: [cyan]{resource.get('name')}[/cyan]")
    console.print(f"S3 key: [dim]s3://{BRONZE_BUCKET}/{s3_key}[/dim]")

    if object_exists(s3_client, BRONZE_BUCKET, s3_key):
        console.print(
            f"[yellow]Already exists — skipping upload.[/yellow] "
            f"s3://{BRONZE_BUCKET}/{s3_key}"
        )
        return

    with tempfile.TemporaryDirectory() as tmp:
        local_path = await download_file(url, Path(tmp) / filename)

        raw_name = resource.get("name", "SENASA phytosanitary certs by destination")
        ascii_name = raw_name.encode("ascii", "replace").decode()
        metadata = {
            "source-url": url,
            "ckan-dataset-id": DATASET_ID,
            "ingested-at": datetime.now(timezone.utc).isoformat(),
            "content-description": ascii_name,
        }

        upload_file(s3_client, local_path, BRONZE_BUCKET, s3_key, metadata=metadata)

    console.print(f"[bold green]Uploaded to[/bold green] s3://{BRONZE_BUCKET}/{s3_key}")


if __name__ == "__main__":
    asyncio.run(main())
