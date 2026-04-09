# Bybit Historical Data Downloader

Downloads historical trade data from [public.bybit.com](https://public.bybit.com) and converts it to Parquet format.

Data source: [public.bybit.com](https://public.bybit.com) — daily CSV files with all trades for spot and perpetual futures. Described on the [Bybit website](https://www.bybit.com/derivatives/en/history-data) as Public Trading History.

Two separate scripts:

| Script | Data type | Source |
|--------|-----------|--------|
| bybit_futures_trades.py | USDT perpetual futures trades | [public.bybit.com/trading](https://public.bybit.com/trading) |
| bybit_spot_trades.py | USDT spot trades | [public.bybit.com/spot](https://public.bybit.com/spot) |

> **Disclaimer**: This tool is not affiliated with, endorsed by, or connected to Bybit in any way. Data is sourced from publicly available Bybit servers. Use at your own risk. The author assumes no responsibility for any issues arising from the use of this software, including but not limited to data accuracy, availability, or compliance with local regulations, and under no circumstances shall be responsible or liable for any claims, damages, losses, expenses, costs, or liabilities of any kind, including direct or indirect damages for loss of profits.

## Pipeline

```
GET .csv.gz → RAM (gzip.decompress, CRC32 check) → Polars CSV read → sort + dedup → .parquet (ZSTD)
```

Csv.gz files are never written to disk — everything happens in RAM.

## Requirements

- Python 3.10+
- `pip install polars requests tqdm`

## Configuration

Set at the top of the script:

```python
SPECIFIC_SYMBOLS = ["ETHUSDT", "BTCUSDT"]     # None = all USDT perpetuals
CONCURRENT_WORKERS = 3                        # parallel download threads
DOWNLOAD_TIMEOUT = 120                        # seconds
SYMBOL_DELAY = 2.0                            # pause between symbols
```

### Parquet compression

```python
PARQUET_COMPRESSION = "zstd"                  # algorithm
PARQUET_COMPRESSION_LEVEL = 3                 # 1–22 (higher = smaller, slower)
PARQUET_ROW_GROUP_SIZE = 500_000              # rows per row group
```

## Usage

### Spyder / Jupyter

Set `SPECIFIC_SYMBOLS` in the script and run.

### Command line

## Command line — futures trades

```bash
bybit_futures_trades.py --symbols ETHUSDT SOLUSDT
bybit_futures_trades.py --symbols BTCUSDT --workers 3
bybit_futures_trades.py --all
```

## Command line — spot trades

```bash
bybit_spot_trades.py --symbols ETHUSDT SOLUSDT
bybit_spot_trades.py --symbols BTCUSDT --workers 3
bybit_spot_trades.py --all
```

## Output structure

```
bybit_data/
├── futures/
│   └── trades/
│       └── ETHUSDT/
│           ├── ETHUSDT_manifest.txt
│           ├── ETHUSDT2019-10-01.parquet
│           └── ...
└── spot/
    └── trades/
        └── BTCUSDT/
            ├── BTCUSDT_manifest.txt
            ├── BTCUSDT_2022-11-10.parquet
            └── ...
```

Spot data on the server has two file naming patterns: daily (`SYMBOL_YYYY-MM-DD.csv.gz`) and monthly (`SYMBOL-YYYY-MM.csv.gz`). Only daily files are downloaded — monthly files contain the same data in aggregated form and are excluded from download (marked skip in the manifest).

## Manifest

Each symbol has a manifest file (`{symbol}_manifest.txt`):

```
filename                       size      [status]
{symbol}_2019-10-01.csv.gz     0         pending (no status)
{symbol}_2024-01-15.csv.gz     52480     true
{symbol}_2024-01-16.csv.gz     48192     false
{symbol}-2022-11.csv.gz        0         skip
```

| Status | Meaning |
|---|---|
| *(no status)* | File not yet processed |
| `true` | Parquet successfully created |
| `false` | Error (download / gzip / conversion). Retried on next run |
| `skip` | Excluded from download (monthly aggregation, duplicates daily data) |

File size in manifest equals `len(resp.content)` — the actual number of downloaded bytes. No HEAD requests are made at any stage. Integrity is verified by `gzip.decompress()` (CRC32 + deflate).

## Resumability

- **Re-run** skips files with `true` status (checks manifest + .parquet on disk)
- Files with `false` status are automatically retried
- New files appearing on the server are added to the manifest
- Interrupted writes (`.tmp` files) are cleaned up on startup

## Parquet schema

Parquet schema — Futures (11 columns)

| Column | Type |
|---|---|
| timestamp | Int64 |
| symbol | Utf8 |
| side | Utf8 |
| size | Float64 |
| price | Float64 |
| tickDirection | Utf8 |
| trdMatchID | Utf8 |
| grossValue | Float64 |
| homeNotional | Float64 |
| foreignNotional | Float64 |
| RPI | Int32 (null for pre-2021 files) |

Parquet schema — Spot (6 columns)

| Column | Type |
|---|---|
| id | Utf8 |
| timestamp | Int64 |
| price | Float64 |
| volume | Float64 |
| side | Utf8 |
| rpi | Int32 (null for files without rpi in header) |
