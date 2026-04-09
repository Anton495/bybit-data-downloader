# Bybit Historical Data Downloader

Downloads historical trade and orderbook data from Bybit public servers and converts it to Parquet format.

Two data sources, four separate scripts:

| Script | Data type | Source |
|--------|-----------|--------|
| bybit_futures_trades.py | USDT perpetual futures trades | [public.bybit.com/trading](https://public.bybit.com/trading) |
| bybit_spot_trades.py | USDT spot trades | [public.bybit.com/spot](https://public.bybit.com/spot) |
| bybit_futures_orderbook.py | USDT perpetual futures orderbook | [quote-saver.bycsi.com/orderbook/linear](https://quote-saver.bycsi.com/orderbook/linear) |
| bybit_spot_orderbook.py | USDT spot orderbook | [quote-saver.bycsi.com/orderbook/spot](https://quote-saver.bycsi.com/orderbook/spot) |

> **Disclaimer**: This tool is not affiliated with, endorsed by, or connected to Bybit in any way. Data is sourced from publicly available Bybit servers. Use at your own risk. The author assumes no responsibility for any issues arising from the use of this software, including but not limited to data accuracy, availability, or compliance with local regulations, and under no circumstances shall be responsible or liable for any claims, damages, losses, expenses, costs, or liabilities of any kind, including direct or indirect damages for loss of profits.

---

## Trades scripts

Data source: [public.bybit.com](https://public.bybit.com) — daily CSV files with all trades for spot and perpetual futures. Described on the [Bybit website](https://www.bybit.com/derivatives/en/history-data) as Public Trading History.

### Pipeline

```
GET .csv.gz → RAM (gzip.decompress, CRC32 check) → Polars CSV read → sort + dedup → .parquet (ZSTD)
```

Csv.gz files are never written to disk — everything happens in RAM.

### Configuration

Set at the top of the script:

```python
SPECIFIC_SYMBOLS = ["ETHUSDT", "BTCUSDT"]     # None = all USDT perpetuals
CONCURRENT_WORKERS = 3                        # parallel download threads
DOWNLOAD_TIMEOUT = 120                        # seconds
SYMBOL_DELAY = 2.0                            # pause between symbols
```

#### Parquet compression

```python
PARQUET_COMPRESSION = "zstd"                  # algorithm
PARQUET_COMPRESSION_LEVEL = 3                 # 1–22 (higher = smaller, slower)
PARQUET_ROW_GROUP_SIZE = 500_000              # rows per row group
```

### Command line — futures trades

```bash
bybit_futures_trades.py --symbols ETHUSDT SOLUSDT
bybit_futures_trades.py --symbols BTCUSDT --workers 3
bybit_futures_trades.py --all
```

### Command line — spot trades

```bash
bybit_spot_trades.py --symbols ETHUSDT SOLUSDT
bybit_spot_trades.py --symbols BTCUSDT --workers 3
bybit_spot_trades.py --all
```

### Spot trades — monthly files

Spot data on the server has two file naming patterns: daily (`SYMBOL_YYYY-MM-DD.csv.gz`) and monthly (`SYMBOL-YYYY-MM.csv.gz`). Only daily files are downloaded — monthly files contain the same data in aggregated form and are excluded from download (marked skip in the manifest).

---

## Orderbook scripts

Data source: [quote-saver.bycsi.com](https://quote-saver.bycsi.com) — daily .data.zip files containing orderbook snapshots and incremental deltas in JSONL format. Described on the [Bybit website](https://www.bybit.com/derivatives/en/history-data) as OrderBook.

### Pipeline

```
GET .data.zip → RAM (zipfile, CRC-32 check) → JSONL parse (msgspec) → sort → .parquet (ZSTD)
```

Each .data.zip contains a single .data file in JSONL format — one JSON object per line. Two record types exist:
- **snapshot** — full orderbook state (up to 200 or 500 price levels per side)
- **delta** — incremental update with changed bid/ask levels

.data.zip files are never written to disk — everything happens in RAM.

### File naming

```
2025-04-29_BTCUSDT_ob200.data.zip   →   2025-04-29_BTCUSDT_ob200.parquet
```

Format: `YYYY-MM-DD_SYMBOL_ob{depth}.data.zip` where depth is 200 or 500.

### Configuration

Set at the top of the script:

```python
SPECIFIC_SYMBOLS = ["ETHUSDT", "BTCUSDT"]     # None = all USDT pairs
CONCURRENT_WORKERS = 3                        # parallel download threads
DOWNLOAD_TIMEOUT = 600                        # seconds (files are large, up to ~300 MB)
SYMBOL_DELAY = 2.0                            # pause between symbols
PROXY_URL = False                             # e.g. "http://127.0.0.1:2080" or False
```

#### Parquet compression

```python
PARQUET_COMPRESSION = "zstd"                  # algorithm
PARQUET_COMPRESSION_LEVEL = 3                 # 1–22 (higher = smaller, slower)
PARQUET_ROW_GROUP_SIZE = 500_000              # rows per row group
```

### Command line — futures orderbook

```bash
bybit_futures_orderbook.py --symbols ETHUSDT SOLUSDT
bybit_futures_orderbook.py --symbols BTCUSDT --workers 1
bybit_futures_orderbook.py --all
```

### Command line — spot orderbook

```bash
bybit_spot_orderbook.py --symbols ETHUSDT SOLUSDT
bybit_spot_orderbook.py --symbols BTCUSDT --workers 1
bybit_spot_orderbook.py --all
```

### Differences from trades scripts

| | Trades | Orderbook |
|---|---|---|
| File format | .csv.gz (gzip) | .data.zip (zip) |
| Internal format | CSV (comma-separated) | JSONL (one JSON per line) |
| JSON parser | not used | msgspec |
| Integrity check | gzip CRC32 + deflate | zipfile.testzip() (CRC-32) |
| Deduplication | by trdMatchID (unique trade ID) | none (orderbook has no unique key) |
| Default timeout | 120s | 600s |
| Proxy support | no | yes (PROXY_URL) |

---

## Requirements

### Trades scripts

- Python 3.10+
- `pip install polars requests tqdm`

### Orderbook scripts

- Python 3.10+
- `pip install polars requests tqdm msgspec`

---

## Usage

### Spyder / Jupyter

Set `SPECIFIC_SYMBOLS` in the script and run.

### Command line

All scripts support `--all`, `--symbols`, `--workers`, `--timeout` flags:

```
--all                Download all available symbols
--symbols SYM1 SYM2  Download only specified symbols
--workers N          Number of parallel download threads
--timeout N          Download timeout per file in seconds
```

---

## Output structure

```
bybit_data/
├── futures/
│   ├── trades/
│   │   └── ETHUSDT/
│   │       ├── ETHUSDT_manifest.txt
│   │       ├── ETHUSDT2019-10-01.parquet
│   │       └── ...
│   └── orderbook/
│       └── ETHUSDT/
│           ├── ETHUSDT_manifest.txt
│           ├── 2025-04-29_ETHUSDT_ob200.parquet
│           └── ...
└── spot/
    ├── trades/
    │   └── BTCUSDT/
    │       ├── BTCUSDT_manifest.txt
    │       ├── BTCUSDT_2022-11-10.parquet
    │       └── ...
    └── orderbook/
        └── BTCUSDT/
            ├── BTCUSDT_manifest.txt
            ├── 2025-04-29_BTCUSDT_ob200.parquet
            └── ...
```

## Manifest

Each symbol has a manifest file (`{symbol}_manifest.txt`):

### Trades manifest

```
filename                       size      [status]
{symbol}_2019-10-01.csv.gz     0         pending (no status)
{symbol}_2024-01-15.csv.gz     52480     true
{symbol}_2024-01-16.csv.gz     48192     false
{symbol}-2022-11.csv.gz        0         skip
```

### Orderbook manifest

```
filename                              size      [status]
2025-04-29_{symbol}_ob200.data.zip     0         pending (no status)
2025-04-30_{symbol}_ob200.data.zip     52480     true
2025-05-01_{symbol}_ob200.data.zip     48192     false
```

| Status | Meaning |
|---|---|
| *(no status)* | File not yet processed |
| `true` | Parquet successfully created |
| `false` | Error (download / zip / conversion). Retried on next run |
| `skip` | Excluded from download (monthly aggregation, duplicates daily data) — trades only |

File size in manifest equals `len(resp.content)` — the actual number of downloaded bytes. No HEAD requests are made at any stage. Integrity is verified by `gzip.decompress()` (CRC32 + deflate) for trades and by `zipfile.testzip()` (CRC-32) for orderbook.

## Resumability

- **Re-run** skips files with `true` status (checks manifest + .parquet on disk)
- Files with `false` status are automatically retried
- New files appearing on the server are added to the manifest
- Interrupted writes (`.tmp` files) are cleaned up on startup

---

## Parquet schema

### Futures trades (11 columns)

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

### Spot trades (6 columns)

| Column | Type |
|---|---|
| id | Utf8 |
| timestamp | Int64 |
| price | Float64 |
| volume | Float64 |
| side | Utf8 |
| rpi | Int32 (null for files without rpi in header) |

### Orderbook — futures and spot (9 columns, identical schema)

| Column | Type | Description |
|---|---|---|
| topic | Utf8 | "orderbook.200.BTCUSDT" or "orderbook.500.BTCUSDT" |
| type | Utf8 | "snapshot" or "delta" |
| ts | Int64 | Timestamp (ms, Unix epoch) |
| symbol | Utf8 | Trading pair symbol |
| update_id | Int64 | Update ID (deltas only, monotonically +1; null for snapshots) |
| seq | Int64 | Sequence number (deltas only; null for snapshots) |
| cts | Int64 | Exchange create timestamp ms (deltas only; null for snapshots) |
| bids | Utf8 | JSON string: [["price","qty"], ...] |
| asks | Utf8 | JSON string: [["price","qty"], ...] |

bids and asks are stored as JSON strings to preserve all price levels. Each level is a two-element array [price, quantity] where both values are strings. A quantity of "0" means the price level was removed.
