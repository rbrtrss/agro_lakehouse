"""Ingest World Bank WDI agricultural indicators for Argentina into the Bronze S3 layer."""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

import httpx
import pandas as pd
from rich.console import Console

# Add repo root to path so ingestion.utils is importable when run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ingestion.utils.s3_utils import make_s3_client, object_exists, upload_file

INDICATORS = {
    "cereal_production_mt": "AG.PRD.CREL.MT",
    "crop_production_index": "AG.PRD.CROP.XD",
    "cereal_yield_kg_ha": "AG.YLD.CREL.KG",
    "agri_land_km2": "AG.LND.AGRI.K2",
    "agri_land_pct": "AG.LND.AGRI.ZS",
}
COUNTRY = "ARG"
DATE_RANGE = "2010:2023"
WB_BASE = "https://api.worldbank.org/v2/country"
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "agro-lakehouse-bronze")
SOURCE_KEY = "worldbank"
FILENAME = "worldbank_agri_indicators_ARG.csv"

console = Console()


def make_s3_key(ingestion_dt: date | None = None) -> str:
    """Return the Hive-partitioned S3 key for the World Bank CSV.

    Partitioned by ingestion date (defaults to today UTC).
    Pattern: source=worldbank/year=YYYY/month=MM/<FILENAME>
    """
    dt = ingestion_dt or date.today()
    return f"source={SOURCE_KEY}/year={dt.year:04d}/month={dt.month:02d}/{FILENAME}"


async def fetch_indicator(
    client: httpx.AsyncClient, name: str, code: str
) -> pd.DataFrame:
    """Fetch a single WDI indicator for Argentina and return as a DataFrame."""
    url = f"{WB_BASE}/{COUNTRY}/indicator/{code}"
    params = {"format": "json", "per_page": 100, "date": DATE_RANGE}

    console.print(f"  Fetching [cyan]{name}[/cyan] ({code})…")
    resp = await client.get(url, params=params)
    resp.raise_for_status()

    payload = resp.json()
    if not isinstance(payload, list) or len(payload) < 2 or not payload[1]:
        console.print(f"  [yellow]No data returned for {code}[/yellow]")
        return pd.DataFrame()

    rows = [
        {
            "country": entry.get("country", {}).get("value"),
            "country_code": entry.get("countryiso3code"),
            "indicator": name,
            "indicator_code": code,
            "year": entry.get("date"),
            "value": entry.get("value"),
        }
        for entry in payload[1]
    ]
    return pd.DataFrame(rows)


async def main() -> None:
    console.rule("[bold green]World Bank WDI — Bronze ingestion")

    s3_client = make_s3_client()
    s3_key = make_s3_key()

    console.print(f"S3 key: [dim]s3://{BRONZE_BUCKET}/{s3_key}[/dim]")

    if object_exists(s3_client, BRONZE_BUCKET, s3_key):
        console.print(
            f"[yellow]Already exists — skipping upload.[/yellow] "
            f"s3://{BRONZE_BUCKET}/{s3_key}"
        )
        return

    async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
        frames = await asyncio.gather(
            *[fetch_indicator(client, name, code) for name, code in INDICATORS.items()]
        )

    non_empty = [f for f in frames if not f.empty]
    if not non_empty:
        console.print(
            "[red]No data returned for any indicator. Check network/API.[/red]"
        )
        sys.exit(1)

    df = pd.concat(non_empty, ignore_index=True)
    df = df.sort_values(["indicator", "year"]).reset_index(drop=True)

    console.print(
        f"Fetched [bold]{len(df)}[/bold] rows across {len(non_empty)} indicators."
    )

    with tempfile.TemporaryDirectory() as tmp:
        local_path = Path(tmp) / FILENAME
        df.to_csv(local_path, index=False)

        metadata = {
            "ingested-at": datetime.now(timezone.utc).isoformat(),
            "indicators": ",".join(INDICATORS.values()),
            "date-range": DATE_RANGE,
            "country": COUNTRY,
        }

        upload_file(s3_client, local_path, BRONZE_BUCKET, s3_key, metadata=metadata)

    console.print(f"[bold green]Uploaded to[/bold green] s3://{BRONZE_BUCKET}/{s3_key}")


if __name__ == "__main__":
    asyncio.run(main())
