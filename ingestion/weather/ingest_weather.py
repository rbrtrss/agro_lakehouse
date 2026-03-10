"""Ingest historical weather data for Argentina's grain-belt provinces into the Bronze S3 layer."""

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

STATIONS = {
    "buenos_aires": {"lat": -34.6, "lon": -58.4},
    "cordoba": {"lat": -31.4, "lon": -64.2},
    "santa_fe": {"lat": -31.6, "lon": -60.7},
    "entre_rios": {"lat": -31.7, "lon": -60.5},
    "la_pampa": {"lat": -36.6, "lon": -64.3},
    "salta": {"lat": -24.8, "lon": -65.4},
    "tucuman": {"lat": -26.8, "lon": -65.2},
}

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "agro-lakehouse-bronze")
SOURCE_KEY = "weather"

EXPECTED_COLUMNS = [
    "date",
    "province",
    "latitude",
    "longitude",
    "temp_max_c",
    "temp_min_c",
    "precipitation_mm",
    "wind_speed_max_kmh",
    "evapotranspiration_mm",
]

console = Console()


def make_s3_key(province: str, ingestion_dt: date | None = None) -> str:
    """Return the Hive-partitioned S3 key for a province's weather CSV.

    Pattern: source=weather/year=YYYY/month=MM/weather_<province>.csv
    """
    dt = ingestion_dt or date.today()
    filename = f"weather_{province}.csv"
    return f"source={SOURCE_KEY}/year={dt.year:04d}/month={dt.month:02d}/{filename}"


async def fetch_province_weather(
    client: httpx.AsyncClient,
    province: str,
    lat: float,
    lon: float,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Fetch daily weather data for a province from Open-Meteo and return as a DataFrame.

    Returns an empty DataFrame if the API returns no daily data.
    Raises httpx.HTTPStatusError on non-2xx responses.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "daily": ",".join(
            [
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_sum",
                "wind_speed_10m_max",
                "et0_fao_evapotranspiration",
            ]
        ),
    }

    resp = await client.get(OPEN_METEO_URL, params=params)
    resp.raise_for_status()

    payload = resp.json()
    daily = payload.get("daily", {})
    times = daily.get("time", [])

    if not times:
        return pd.DataFrame()

    df = pd.DataFrame(
        {
            "date": times,
            "province": province,
            "latitude": lat,
            "longitude": lon,
            "temp_max_c": daily["temperature_2m_max"],
            "temp_min_c": daily["temperature_2m_min"],
            "precipitation_mm": daily["precipitation_sum"],
            "wind_speed_max_kmh": daily["wind_speed_10m_max"],
            "evapotranspiration_mm": daily["et0_fao_evapotranspiration"],
        }
    )
    return df[EXPECTED_COLUMNS]


async def fetch_and_upload_province(
    s3_client,
    province: str,
    lat: float,
    lon: float,
    start_date: date,
    end_date: date,
    ingestion_dt: date | None = None,
) -> str:
    """Fetch weather data for one province and upload to S3 Bronze.

    Returns 'skipped' if the S3 key already exists, 'uploaded' on success.
    """
    s3_key = make_s3_key(province, ingestion_dt)

    if object_exists(s3_client, BRONZE_BUCKET, s3_key):
        console.print(f"[yellow]Already exists — skipping[/yellow] {province}")
        return "skipped"

    async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
        df = await fetch_province_weather(
            client, province, lat, lon, start_date, end_date
        )

    if df.empty:
        console.print(f"[yellow]No data returned for {province}[/yellow]")
        return "skipped"

    filename = f"weather_{province}.csv"
    with tempfile.TemporaryDirectory() as tmp:
        local_path = Path(tmp) / filename
        df.to_csv(local_path, index=False)

        metadata = {
            "ingested-at": datetime.now(timezone.utc).isoformat(),
            "province": province,
            "start-date": start_date.isoformat(),
            "end-date": end_date.isoformat(),
        }
        upload_file(s3_client, local_path, BRONZE_BUCKET, s3_key, metadata=metadata)

    console.print(f"[green]Uploaded[/green] {province} → s3://{BRONZE_BUCKET}/{s3_key}")
    return "uploaded"


async def main() -> None:
    console.rule("[bold green]Open-Meteo Weather — Bronze ingestion")

    end = date.today()
    start = end.replace(year=end.year - 1)
    ingestion_dt = end

    console.print(f"Date range: {start} → {end}")

    s3 = make_s3_client()

    tasks = [
        fetch_and_upload_province(
            s3,
            province,
            coords["lat"],
            coords["lon"],
            start,
            end,
            ingestion_dt,
        )
        for province, coords in STATIONS.items()
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    uploaded = sum(1 for r in results if r == "uploaded")
    skipped = sum(1 for r in results if r == "skipped")
    errors = [r for r in results if isinstance(r, Exception)]

    console.print(
        f"[bold]Done:[/bold] {uploaded} uploaded, {skipped} skipped, {len(errors)} errors"
    )

    for err in errors:
        console.print(f"[red]Error:[/red] {err}")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
