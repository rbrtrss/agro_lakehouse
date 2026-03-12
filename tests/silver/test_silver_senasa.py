"""
TDD tests for the transform_senasa_df helper in glue/jobs/silver_senasa.py.

These tests run without AWS or Spark — pure pandas only.

Input contract: raw Bronze CSV DataFrame with all 11 original columns:
  fecha, oficina_cf, provincia, provincia_id, pais_destino, pais_destino_id,
  pais_destino_id_iso_3166_1, continente, mercaderia_certificada, transporte, tn
"""

import pandas as pd

from glue.jobs.silver_senasa import transform_senasa_df

EXPECTED_COLUMNS = [
    "fecha",
    "oficina_cf",
    "provincia",
    "provincia_id",
    "pais_destino",
    "pais_destino_id",
    "pais_destino_id_iso_3166_1",
    "continente",
    "mercaderia_certificada",
    "transporte",
    "tn",
    "year",
    "month",
]


def _make_df(**overrides) -> pd.DataFrame:
    """Return a minimal valid raw Bronze-style SENASA DataFrame."""
    base = {
        "fecha": ["2023-03-15", "2023-07-22"],
        "oficina_cf": ["Buenos Aires", "Rosario"],
        "provincia": ["Buenos Aires", "Santa Fe"],
        "provincia_id": ["1", "21"],
        "pais_destino": ["China", "Brasil"],
        "pais_destino_id": ["156", "76"],
        "pais_destino_id_iso_3166_1": ["CN", "BR"],
        "continente": ["Asia", "América del Sur"],
        "mercaderia_certificada": ["Soja", "Trigo"],
        "transporte": ["Marítimo", "Terrestre"],
        "tn": ["5000.0", "1200.5"],
    }
    base.update(overrides)
    return pd.DataFrame(base)


# ── Tests ─────────────────────────────────────────────────────────────────────


def test_output_columns():
    df = transform_senasa_df(_make_df())
    assert list(df.columns) == EXPECTED_COLUMNS


def test_string_cols_trimmed():
    raw = _make_df(
        oficina_cf=["  Buenos Aires  ", "Rosario"],
        pais_destino=["  China  ", "Brasil"],
    )
    df = transform_senasa_df(raw)
    assert df.iloc[0]["oficina_cf"] == "Buenos Aires"
    assert df.iloc[0]["pais_destino"] == "China"


def test_pais_destino_null_dropped():
    raw = _make_df(pais_destino=[None, "Brasil"])
    df = transform_senasa_df(raw)
    assert len(df) == 1
    assert df.iloc[0]["pais_destino"] == "Brasil"


def test_pais_destino_empty_dropped():
    raw = _make_df(pais_destino=["   ", "Brasil"])
    df = transform_senasa_df(raw)
    assert len(df) == 1
    assert df.iloc[0]["pais_destino"] == "Brasil"


def test_fecha_unparseable_dropped():
    raw = _make_df(fecha=["not_a_date", "2023-07-22"])
    df = transform_senasa_df(raw)
    assert len(df) == 1
    assert df.iloc[0]["pais_destino"] == "Brasil"


def test_tn_negative_dropped():
    raw = _make_df(tn=["-1.0", "1200.5"])
    df = transform_senasa_df(raw)
    assert len(df) == 1
    assert df.iloc[0]["pais_destino"] == "Brasil"


def test_tn_null_kept():
    raw = _make_df(tn=[None, "1200.5"])
    df = transform_senasa_df(raw)
    assert len(df) == 2
    assert pd.isna(df.iloc[0]["tn"])


def test_dedup_exact_rows():
    row = {
        "fecha": "2023-03-15",
        "oficina_cf": "Buenos Aires",
        "provincia": "Buenos Aires",
        "provincia_id": "1",
        "pais_destino": "China",
        "pais_destino_id": "156",
        "pais_destino_id_iso_3166_1": "CN",
        "continente": "Asia",
        "mercaderia_certificada": "Soja",
        "transporte": "Marítimo",
        "tn": "5000.0",
    }
    raw = pd.DataFrame([row, row])
    df = transform_senasa_df(raw)
    assert len(df) == 1


def test_year_month_derived():
    df = transform_senasa_df(_make_df())
    assert df.iloc[0]["year"] == 2023
    assert df.iloc[0]["month"] == 3
    assert df.iloc[1]["year"] == 2023
    assert df.iloc[1]["month"] == 7


def test_empty_input():
    empty = pd.DataFrame(
        columns=[
            "fecha",
            "oficina_cf",
            "provincia",
            "provincia_id",
            "pais_destino",
            "pais_destino_id",
            "pais_destino_id_iso_3166_1",
            "continente",
            "mercaderia_certificada",
            "transporte",
            "tn",
        ]
    )
    df = transform_senasa_df(empty)
    assert df.empty
    assert list(df.columns) == EXPECTED_COLUMNS
