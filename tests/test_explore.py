"""Tests for ingestion/explore.py."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pandas as pd

from ingestion.explore import build_markdown, profile_csv


def _write_csv(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(content), encoding="utf-8")
    return path


# --- profile_csv ---


def test_profile_csv_shape(tmp_path):
    csv = _write_csv(
        tmp_path / "f.csv",
        """\
        a,b
        1,2
        3,4
        5,6
    """,
    )
    p = profile_csv(csv)
    assert p["rows"] == 3
    assert p["cols"] == 2


def test_profile_csv_column_names(tmp_path):
    csv = _write_csv(tmp_path / "f.csv", "a,b\n1,2\n")
    p = profile_csv(csv)
    assert p["columns"][0]["name"] == "a"


def test_profile_csv_null_pct(tmp_path):
    # Two rows; column "b" is empty in first row → 50% null
    csv = _write_csv(tmp_path / "f.csv", "a,b\n1,\n2,3\n")
    p = profile_csv(csv)
    b_col = next(c for c in p["columns"] if c["name"] == "b")
    assert b_col["null_pct"] == 50.0


def test_profile_csv_sample_first_non_null(tmp_path):
    # First value is NaN, second is "hello"; dropna().iloc[0] should give "hello"
    csv = _write_csv(tmp_path / "f.csv", "a,x\n1,\n2,hello\n")
    p = profile_csv(csv)
    x_col = next(c for c in p["columns"] if c["name"] == "x")
    assert x_col["sample"] == "hello"


def test_profile_csv_sample_dash_when_all_null(tmp_path):
    # All values empty → "—"
    csv = _write_csv(tmp_path / "f.csv", "a,x\n1,\n2,\n")
    p = profile_csv(csv)
    x_col = next(c for c in p["columns"] if c["name"] == "x")
    assert x_col["sample"] == "—"


def test_profile_csv_head_is_3_rows(tmp_path):
    lines = "a\n" + "\n".join(str(i) for i in range(5))
    csv = _write_csv(tmp_path / "f.csv", lines)
    p = profile_csv(csv)
    assert len(p["head"]) == 3


# --- build_markdown ---


def _make_profile(
    tmp_path: Path, source: str, rows: int, cols: int, columns: list[dict]
) -> dict:
    path = tmp_path / source / "foo.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()
    head_df = pd.DataFrame({c["name"]: [] for c in columns})
    return {
        "path": path,
        "rows": rows,
        "cols": cols,
        "columns": columns,
        "head": head_df,
    }


def test_build_markdown_source_header(tmp_path):
    p = _make_profile(
        tmp_path,
        "indec",
        10,
        2,
        [{"name": "col", "dtype": "object", "null_pct": 0.0, "sample": "x"}],
    )
    md = build_markdown([p])
    assert "## INDEC" in md


def test_build_markdown_contains_shape(tmp_path):
    p = _make_profile(
        tmp_path,
        "indec",
        100,
        3,
        [{"name": "c", "dtype": "int64", "null_pct": 0.0, "sample": "1"}],
    )
    md = build_markdown([p])
    assert "100" in md
    assert "3" in md


def test_build_markdown_contains_column_name(tmp_path):
    p = _make_profile(
        tmp_path,
        "indec",
        5,
        1,
        [{"name": "my_col", "dtype": "object", "null_pct": 0.0, "sample": "v"}],
    )
    md = build_markdown([p])
    assert "`my_col`" in md


def test_build_markdown_escapes_pipe(tmp_path):
    p = _make_profile(
        tmp_path,
        "indec",
        1,
        1,
        [{"name": "col", "dtype": "object", "null_pct": 0.0, "sample": "a|b"}],
    )
    md = build_markdown([p])
    assert "\\|" in md


def test_build_markdown_multiple_profiles(tmp_path):
    p1 = _make_profile(
        tmp_path,
        "indec",
        10,
        1,
        [{"name": "a", "dtype": "object", "null_pct": 0.0, "sample": "x"}],
    )
    p2 = _make_profile(
        tmp_path,
        "senasa",
        20,
        1,
        [{"name": "b", "dtype": "object", "null_pct": 0.0, "sample": "y"}],
    )
    md = build_markdown([p1, p2])
    assert "## INDEC" in md
    assert "## SENASA" in md
