"""Download Argentina agricultural data from the World Bank WDI API and save as CSV.

Uses the World Development Indicators (WDI, source=2) which contains crop production,
land and yield data for Argentina — a proxy for commodity price context.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import httpx
import pandas as pd
from rich.console import Console

DEST_DIR = Path(__file__).resolve().parents[2] / "data" / "samples" / "worldbank"
DEST_FILE = DEST_DIR / "argentina_agri_indicators.csv"

# World Development Indicators available for Argentina
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

console = Console()


async def fetch_indicator(client: httpx.AsyncClient, name: str, code: str) -> pd.DataFrame:
    url = f"{WB_BASE}/{COUNTRY}/indicator/{code}"
    params = {"format": "json", "per_page": 100, "date": DATE_RANGE}

    console.print(f"  Fetching [cyan]{name}[/cyan] ({code})…")
    resp = await client.get(url, params=params)
    resp.raise_for_status()

    payload = resp.json()
    if not isinstance(payload, list) or len(payload) < 2 or not payload[1]:
        console.print(f"  [yellow]No data returned for {code}[/yellow]")
        return pd.DataFrame()

    rows = []
    for entry in payload[1]:
        rows.append({
            "country": entry.get("country", {}).get("value"),
            "country_code": entry.get("countryiso3code"),
            "indicator": name,
            "indicator_code": code,
            "year": entry.get("date"),
            "value": entry.get("value"),
        })
    return pd.DataFrame(rows)


async def main() -> None:
    console.rule("[bold green]World Bank WDI — Argentina agricultural indicators")
    DEST_DIR.mkdir(parents=True, exist_ok=True)

    async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
        frames = await asyncio.gather(
            *[fetch_indicator(client, name, code) for name, code in INDICATORS.items()]
        )

    non_empty = [f for f in frames if not f.empty]
    if not non_empty:
        console.print("[red]No data returned for any indicator. Check network/API.[/red]")
        sys.exit(1)

    df = pd.concat(non_empty, ignore_index=True)
    df = df.sort_values(["indicator", "year"]).reset_index(drop=True)
    df.to_csv(DEST_FILE, index=False)

    console.print(f"\n[bold green]Done.[/bold green] {len(df)} rows → {DEST_FILE}")
    console.print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    asyncio.run(main())
