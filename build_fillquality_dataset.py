#!/usr/bin/env python3
"""Build notebook-friendly datasets from fillquality_v2 JSONL outputs.

Outputs:
  fills_enriched.parquet (or .csv fallback)
      one row per fill, with mark-out horizons pivoted into columns

  latency_heatmap.parquet (or .csv fallback)
      long-form rows: ts, stage, bucket_low_ms, bucket_high_ms, count
      suitable for a time x latency heat map in a Jupyter notebook

Prometheus is deliberately not read here.  This script operates on the durable
research/audit files produced by fillquality_v2.py.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import pandas as pd


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_no}: invalid JSON: {exc}") from exc
    return rows


def build_fills(data_dir: Path) -> pd.DataFrame:
    fills = pd.DataFrame(read_jsonl(data_dir / "fills.jsonl"))
    markouts = pd.DataFrame(read_jsonl(data_dir / "markouts.jsonl"))

    if fills.empty:
        return fills

    if not markouts.empty:
        markouts = markouts.copy()
        # oid alone can repeat across runs, so include run_id in the join.
        piv = markouts.pivot_table(
            index=["run_id", "oid"],
            columns="horizon",
            values="markout_bps",
            aggfunc="last",
        )
        piv.columns = [f"markout_{c}_bps" for c in piv.columns]
        piv = piv.reset_index()
        fills = fills.merge(piv, on=["run_id", "oid"], how="left")

    if "ts" in fills:
        fills["datetime_utc"] = pd.to_datetime(fills["ts"], unit="s", utc=True)

    # Useful analysis fields.  These are descriptive, not labels of good/bad.
    if {"slippage_total_bps", "expected_slippage_center_bps"}.issubset(fills.columns):
        fills["slippage_residual_bps"] = (
            fills["slippage_total_bps"] - fills["expected_slippage_center_bps"]
        )
    if {"slippage_total_bps", "expected_slippage_low_bps", "expected_slippage_high_bps"}.issubset(fills.columns):
        fills["outside_expected_band"] = (
            (fills["slippage_total_bps"] < fills["expected_slippage_low_bps"])
            | (fills["slippage_total_bps"] > fills["expected_slippage_high_bps"])
        )

    return fills.sort_values(["run_id", "ts", "oid"]).reset_index(drop=True)


def build_latency_heatmap(data_dir: Path) -> pd.DataFrame:
    rows = []
    for record in read_jsonl(data_dir / "latency.jsonl"):
        ts = record.get("ts")
        run_id = record.get("run_id")
        edges = record.get("edges", [])
        stages = record.get("stages", {})
        if not edges:
            continue
        for stage, counts in stages.items():
            for i, count in enumerate(counts):
                if not count:
                    continue
                low_s = 0.0 if i == 0 else edges[i - 1]
                high_s = math.inf if i >= len(edges) else edges[i]
                rows.append({
                    "run_id": run_id,
                    "ts": ts,
                    "datetime_utc": pd.to_datetime(ts, unit="s", utc=True),
                    "stage": stage,
                    "bucket_index": i,
                    "bucket_low_ms": low_s * 1e3,
                    "bucket_high_ms": (high_s * 1e3 if math.isfinite(high_s) else math.inf),
                    "count": count,
                })
    return pd.DataFrame(rows)


def write_frame(df: pd.DataFrame, path_without_suffix: Path) -> Path:
    parquet = path_without_suffix.with_suffix(".parquet")
    try:
        df.to_parquet(parquet, index=False)
        return parquet
    except (ImportError, ModuleNotFoundError):
        csv = path_without_suffix.with_suffix(".csv")
        df.to_csv(csv, index=False)
        return csv


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path(__file__).resolve().parent / "data")
    parser.add_argument("--out-dir", type=Path, default=None)
    args = parser.parse_args()

    data_dir = args.data_dir.resolve()
    out_dir = (args.out_dir or data_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    fills = build_fills(data_dir)
    heat = build_latency_heatmap(data_dir)

    fills_path = write_frame(fills, out_dir / "fills_enriched")
    heat_path = write_frame(heat, out_dir / "latency_heatmap")

    print(f"fills:   {len(fills):,} rows -> {fills_path}")
    print(f"latency: {len(heat):,} rows -> {heat_path}")


if __name__ == "__main__":
    main()
