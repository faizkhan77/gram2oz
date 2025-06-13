#!/usr/bin/env python3
"""
convert_gold_rates.py 
∙ Stream‑convert per‑gram → per‑ounce
∙ Write to MySQL, and optionally a full CSV and Excel
"""
from __future__ import annotations
import argparse, logging
from decimal import Decimal, getcontext
from pathlib import Path
from typing import List
from contextlib import closing

import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm
from colorlog import ColoredFormatter

# ── CONFIG ─────────────────────────────────────────────────────────
getcontext().prec = 12
GRAMS_PER_OUNCE = Decimal("31.1035")
RATE_COLS: List[str] = [
    "buyRateUSD", "sellRateUSD",
    "buyRateWithMarginUSD", "sellRateWithMarginUSD",
    "buyRateAED", "sellRateAED",
    "buyRateWithMarginAED", "sellRateWithMarginAED",
]
DEFAULT_CHUNKSIZE = 50_000
DATABASE_URL = "mysql+mysqlconnector://root:root@localhost/gold_data"

# ───────────────────────────────────────────────────────────────────

def setup_logging():
    handler = logging.StreamHandler()
    handler.setFormatter(
        ColoredFormatter(
            "%(log_color)s[%(levelname)s] %(message)s",
            log_colors={
                "DEBUG": "cyan", "INFO": "green",
                "WARNING": "yellow", "ERROR": "red",
                "CRITICAL": "bold_red",
            },
        )
    )
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)

def total_rows(csv_path: Path) -> int:
    with csv_path.open("r", encoding="utf-8", errors="ignore") as f:
        return sum(1 for _ in f) - 1

def convert_chunk(df: pd.DataFrame) -> pd.DataFrame:
    for col in RATE_COLS:
        df[f"{col}Oz"] = pd.to_numeric(df[col], errors="coerce") * float(GRAMS_PER_OUNCE)
    return df

def write_chunk_sql(df, engine, table, first, chunksize):
    df.to_sql(
        name=table,
        con=engine,
        if_exists="replace" if first else "append",
        index=False,
        chunksize=chunksize,
        method="multi",
    )

def process(
    csv_path: Path, out_csv: Path | None,
    out_xlsx: Path | None, table: str, chunksize: int
):
    engine = create_engine(DATABASE_URL)
    total = total_rows(csv_path)
    logging.info(f"Total rows detected: {total:,}")

    reader = pd.read_csv(csv_path, chunksize=chunksize, iterator=True)
    pbar = tqdm(total=total, unit="rows", colour="cyan")

    if out_csv and out_csv.exists():
        out_csv.unlink()
    first_chunk = True

    for i, chunk in enumerate(reader):
        chunk = convert_chunk(chunk)

        # 1) MySQL
        write_chunk_sql(chunk, engine, table, first=i == 0, chunksize=chunksize)

        # 2) CSV
        if out_csv:
            chunk.to_csv(out_csv, mode="a", header=first_chunk, index=False)

        # 3) Excel (if enabled)
        if out_xlsx:
            if i == 0:
                converted = chunk.copy()
            else:
                converted = pd.concat([converted, chunk], ignore_index=True)

        first_chunk = False
        pbar.update(len(chunk))

    pbar.close()
    logging.info("All chunks written ✔︎")

    if out_xlsx:
        logging.info("Writing full Excel to %s …", out_xlsx)
        converted.to_excel(out_xlsx, index=False)
        logging.info("Excel saved ✔︎")

    logging.info("Skipping SQL dump: not supported for MySQL ✔︎")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Input CSV")
    ap.add_argument("--output-csv", help="CSV with converted columns")
    ap.add_argument("--output-xlsx", help="Excel with converted columns")
    ap.add_argument("--table", default="gold_rates", help="Table name")
    ap.add_argument("--chunksize", type=int, default=DEFAULT_CHUNKSIZE)
    args = ap.parse_args()

    setup_logging()
    process(
        csv_path=Path(args.input).resolve(),
        out_csv=Path(args.output_csv).resolve() if args.output_csv else None,
        out_xlsx=Path(args.output_xlsx).resolve() if args.output_xlsx else None,
        table=args.table,
        chunksize=args.chunksize,
    )

    logging.info(
        "🎉 Done! MySQL table → %s%s%s",
        args.table,
        f" | CSV → {args.output_csv}" if args.output_csv else "",
        f" | XLSX → {args.output_xlsx}" if args.output_xlsx else "",
    )

if __name__ == "__main__":
    main()
