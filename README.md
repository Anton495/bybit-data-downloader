# Bybit Historical Data Downloader

Downloads historical trade and orderbook data from Bybit public servers and converts it to Parquet format.

Two data sources, four separate scripts:

| Script | Data type | Source |
|--------|-----------|--------|
| bybit_spot_trades.py | Spot trades | [public.bybit.com/spot](https://public.bybit.com/spot) |
| bybit_futures_trades.py | Futures trades | [public.bybit.com/trading](https://public.bybit.com/trading) |
| bybit_spot_orderbook.py | Spot orderbook | [quote-saver.bycsi.com/orderbook/spot](https://quote-saver.bycsi.com/orderbook/spot) |
| bybit_futures_orderbook.py | Futures orderbook  | [quote-saver.bycsi.com/orderbook/linear](https://quote-saver.bycsi.com/orderbook/linear) <br> [quote-saver.bycsi.com/orderbook/inverse](https://quote-saver.bycsi.com/orderbook/inverse)|


> **Disclaimer**: This tool is not affiliated with, endorsed by, or connected to Bybit in any way. Data is sourced from publicly available Bybit servers. Use at your own risk. The author assumes no responsibility for any issues arising from the use of this software, including but not limited to data accuracy, availability, or compliance with local regulations, and under no circumstances shall be responsible or liable for any claims, damages, losses, expenses, costs, or liabilities of any kind, including direct or indirect damages for loss of profits.

---

## Trades scripts

Data source: [public.bybit.com](https://public.bybit.com) — daily CSV files with all trades for spot and futures. Described on the [Bybit website](https://www.bybit.com/derivatives/en/history-data) as Public Trading History.

### Pipeline

```
GET .csv.gz → RAM (gzip.decompress, CRC32 check) → Polars CSV read → sort + dedup → .parquet (ZSTD)
```

Csv.gz files are never written to disk — everything happens in RAM.

### File naming

```
Futures:  SYMBOLYYYY-MM-DD.csv.gz    →   SYMBOLYYYY-MM-DD.parquet
Spot:     SYMBOL_YYYY-MM-DD.csv.gz   →   SYMBOL_YYYY-MM-DD.parquet
```

Note the difference: futures filenames have no separator between symbol and date, while spot filenames use an underscore.

### Configuration

Set at the top of the script:

```python
SPECIFIC_SYMBOLS = None                    # None = use DEFAULT_GROUP; ["ETHUSDT"] = specific symbols
DEFAULT_GROUP = "USDT"                     # group for Spyder mode (see Symbol groups)
CONCURRENT_WORKERS = 3                     # parallel download threads
DOWNLOAD_TIMEOUT = 120                     # seconds
SYMBOL_DELAY = 2.0                         # pause between symbols
```

Priority chain: CLI `--symbols` > `symbols_override()` > `SPECIFIC_SYMBOLS` > `DEFAULT_GROUP`.

#### Parquet compression

```python
PARQUET_COMPRESSION = "zstd"                  # algorithm
PARQUET_COMPRESSION_LEVEL = 3                 # 1–22 (higher = smaller, slower)
PARQUET_ROW_GROUP_SIZE = 500_000              # rows per row group
```

### Command line — futures trades

```bash
bybit_futures_trades.py --usdt                         # all USDT perpetual futures
bybit_futures_trades.py --perp                         # PERP contracts
bybit_futures_trades.py --stable                       # stablecoin pairs (USDC, FDUSD, etc. vs USDT)
bybit_futures_trades.py --futures                      # delivery futures (BTC-28FEB26, ETH-01MAR24)
bybit_futures_trades.py --quarterly                    # quarterly futures (BTCUSDH26)
bybit_futures_trades.py --inverse                      # inverse contracts (BTCUSD, ETHUSD)
bybit_futures_trades.py --group STABLE --workers 3     # group name + options
bybit_futures_trades.py --symbols ETHUSDT SOLUSDT      # specific symbols
```

### Command line — spot trades

```bash
bybit_spot_trades.py --usdt                           # all USDT spot pairs
bybit_spot_trades.py --usdc                           # all USDC spot pairs
bybit_spot_trades.py --crypto                         # crypto-quoted pairs (BTC, ETH, SOL, etc.)
bybit_spot_trades.py --fiat                           # fiat pairs (EUR, GBP, BRL, etc.)
bybit_spot_trades.py --leveraged                      # leveraged tokens (BTC3L, ETH3S, etc.)
bybit_spot_trades.py --other                          # other stablecoins (DAI, RLUSD, USDE, etc.)
bybit_spot_trades.py --group FIAT --workers 5         # group name + options
bybit_spot_trades.py --symbols ETHUSDT SOLUSDT        # specific symbols
```

### Spot trades — monthly files

Spot data on the server has two file naming patterns: daily (`SYMBOL_YYYY-MM-DD.csv.gz`) and monthly (`SYMBOL-YYYY-MM.csv.gz`). Only daily files are downloaded — monthly files contain the same data in aggregated form and are excluded from download (marked skip in the manifest).

---

## Orderbook scripts

Data source: [quote-saver.bycsi.com](https://quote-saver.bycsi.com) — daily .data.zip files containing orderbook snapshots and incremental deltas in JSONL format. Described on the Bybit website as OrderBook.

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
SPECIFIC_SYMBOLS = None                   # None = use DEFAULT_GROUP; ["ETHUSDT"] = specific symbols
DEFAULT_GROUP = "USDT"                    # group for Spyder mode (see Symbol groups)
CONCURRENT_WORKERS = 3                    # parallel download threads
DOWNLOAD_TIMEOUT = 600                    # seconds (files are large)
SYMBOL_DELAY = 2.0                        # pause between symbols
PROXY_URL = False                         # e.g. "http://127.0.0.1:2080" or False
```

#### Parquet compression

```python
PARQUET_COMPRESSION = "zstd"                  # algorithm
PARQUET_COMPRESSION_LEVEL = 3                 # 1–22 (higher = smaller, slower)
PARQUET_ROW_GROUP_SIZE = 500_000              # rows per row group
```

### Command line — futures orderbook

```bash
bybit_futures_orderbook.py --usdt                         # all USDT perpetual futures
bybit_futures_orderbook.py --perp                         # PERP contracts
bybit_futures_orderbook.py --stable                       # stablecoin pairs (USDC, FDUSD, etc. vs USDT)
bybit_futures_orderbook.py --futures                      # delivery futures (BTC-28FEB26, ETH-01MAR24)
bybit_futures_orderbook.py --quarterly                    # quarterly futures (BTCUSDH26, ETHUSDM26)
bybit_futures_orderbook.py --inverse                      # inverse contracts (BTCUSD, ETHUSD, SOLUSD)
bybit_futures_orderbook.py --usdt-futures                 # USDT delivery futures (BTCUSDT-01AUG25)
bybit_futures_orderbook.py --group USDT_FUTURES --workers 1
bybit_futures_orderbook.py --symbols ETHUSDT SOLUSDT      # specific symbols
```

### Command line — spot orderbook

```bash
bybit_spot_orderbook.py --usdt                          # all USDT spot pairs
bybit_spot_orderbook.py --usdc                          # all USDC spot pairs
bybit_spot_orderbook.py --crypto                        # crypto-quoted pairs (BTC, ETH, SOL, etc.)
bybit_spot_orderbook.py --fiat                          # fiat pairs (EUR, GBP, BRL, etc.)
bybit_spot_orderbook.py --leveraged                     # leveraged tokens (BTC3L, ETH3S, etc.)
bybit_spot_orderbook.py --other                         # other stablecoins (DAI, RLUSD, USDE, etc.)
bybit_spot_orderbook.py --group USDC --workers 1        # group name + options
bybit_spot_orderbook.py --symbols ETHUSDT SOLUSDT       # specific symbols
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

Set `SPECIFIC_SYMBOLS` to a list of symbols or leave as `None` to use `DEFAULT_GROUP`, then run.

### Command line

All scripts require exactly one selection flag (mutually exclusive):

```
--group NAME                Select symbol group by name
--symbols SYM1 SYM2         Specific symbols
--usdt / --stable / ...     Shorthand flags per script (see below)
--workers N                 Parallel download threads
--timeout N                 Download timeout per file in seconds
```

No `--all` flag — use `--group <NAME>` to select a symbol group (see [Symbol groups](#symbol-groups)).

---

## Symbol groups

Each script defines groups of symbols matched by regex. Shorthand CLI flags are provided for each group. Symbol lists are fetched from the server and filtered at runtime.

### Futures trades — FUTURES_TRADES_GROUPS

| Group | Shorthand | Description | Example symbols |
|---|---|---|---|
| `USDT` | `--usdt` | USDT perpetual futures | ETHUSDT, BTCUSDT, SOLUSDT |
| `PERP` | `--perp` | PERP contracts | BTCPERP, 1000PEPEPERP |
| `STABLE` | `--stable` | Stablecoin pairs | USDCUSDT, FDUSDUSDT, BUSDUSDT |
| `FUTURES` | `--futures` | Delivery futures | BTC-28FEB26, ETH-01MAR24 |
| `QUARTERLY` | `--quarterly` | Quarterly futures | BTCUSDH26, ETHUSDM25 |
| `INVERSE` | `--inverse` | Inverse (crypto-margined) | BTCUSD, ETHUSD, XRPUSD |

### Futures orderbook — FUTURES_ORDERBOOK_GROUPS

| Group | Shorthand | Description | Example symbols |
|---|---|---|---|
| `USDT` | `--usdt` | USDT perpetual futures | ETHUSDT, BTCUSDT, SOLUSDT |
| `PERP` | `--perp` | PERP contracts | BTCPERP, 1000PEPEPERP |
| `STABLE` | `--stable` | Stablecoin pairs | USDCUSDT, FDUSDUSDT, BUSDUSDT |
| `FUTURES` | `--futures` | Delivery futures | BTC-28FEB26, ETH-01MAR24 |
| `QUARTERLY` | `--quarterly` | Quarterly futures | BTCUSDH26, ETHUSDM25 |
| `INVERSE` | `--inverse` | Inverse (crypto-margined) | BTCUSD, ETHUSD, SOLUSD |
| `USDT_FUTURES` | `--usdt-futures` | USDT delivery futures | BTCUSDT-01AUG25, ETHUSDT-02JAN26 |

### Spot trades — SPOT_TRADES_GROUPS

| Group | Shorthand | Description | Example symbols |
|---|---|---|---|
| `USDT` | `--usdt` | USDT spot pairs | ETHUSDT, BTCUSDT, SOLUSDT |
| `USDC` | `--usdc` | USDC spot pairs | ETHUSDC, BTCUSDC, SOLUSDC |
| `CRYPTO` | `--crypto` | Crypto-quoted pairs | ETHBTC, SOLBNB, MNTETH |
| `FIAT` | `--fiat` | Fiat pairs | BTCEUR, ETHBRL, SOLGBP |
| `LEVERAGED` | `--leveraged` | Leveraged tokens | BTC3LUSDT, ETH3SUSDT, ADA2LUSDT |
| `OTHER` | `--other` | Other stablecoins (DAI, RLUSD, USDE, etc.) | BTCDAI, ETHRLUSD, BTCUSDE, BTCUSDQ |

### Spot orderbook — SPOT_ORDERBOOK_GROUPS

| Group | Shorthand | Description | Example symbols |
|---|---|---|---|
| `USDT` | `--usdt` | USDT spot pairs | ETHUSDT, BTCUSDT, SOLUSDT |
| `USDC` | `--usdc` | USDC spot pairs | ETHUSDC, BTCUSDC, SOLUSDC |
| `CRYPTO` | `--crypto` | Crypto-quoted pairs | ETHBTC, SOLBNB, DOTETH |
| `FIAT` | `--fiat` | Fiat pairs | BTCEUR, ETHBRL, SOLGBP |
| `LEVERAGED` | `--leveraged` | Leveraged tokens | BTC3LUSDT, ETH3SUSDT, ADA2LUSDT |
| `OTHER` | `--other` | Other stablecoins (DAI, RLUSD, USDE, etc.) | BTCDAI, ETHRLUSD, BTCUSDE, BTCUSDQ |

Groups are mutually exclusive — each symbol matches at most one group. Symbol lists are fetched from the server at runtime and filtered by group regex.

### Verification — bybit_verify_groups.py

The script `bybit_verify_groups.py` verifies symbol group classification for all 4 downloaders against live server data. It reads regex patterns directly from the script files (no changes to the downloaders are required) and reports matched, unmatched, and overlapping symbols.

```bash
python bybit_verify_groups.py                              # all 4 scripts
python bybit_verify_groups.py --trades                     # futures_trades + spot_trades
python bybit_verify_groups.py --orderbook                  # futures_orderbook + spot_orderbook
python bybit_verify_groups.py --full                       # print all symbols per group
python bybit_verify_groups.py --group USDT                 # show USDT group across selected scripts
python bybit_verify_groups.py --group OTHER                # show OTHER group across selected scripts
python bybit_verify_groups.py --trades --group CRYPTO      # CRYPTO group in trades scripts only
python bybit_verify_groups.py --orderbook --group LEVERAGED
```

After run, the global variable `ALL_SYMBOLS` contains sorted symbols per group per script (can be used in Spyder / Jupyter):

```python
ALL_SYMBOLS = {
    "futures_trades":    {"symbols": [...], "groups": {"USDT": [...], ...}, ...},
    "futures_orderbook": {"symbols": [...], "groups": {"USDT": [...], ...}, ...},
    "futures_orderbook_inverse": {"symbols": [...], "groups": {"QUARTERLY": [...], ...}, ...},
    "spot_trades":       {"symbols": [...], "groups": {"USDT": [...], ...}, ...},
    "spot_orderbook":    {"symbols": [...], "groups": {"USDT": [...], ...}, ...},
}
```

> **Important: check classification accuracy regularly.** New groups are not added in real-time. If Bybit adds a new fiat currency or a new stablecoin, pairs like `BTC{NEW_FIAT}` or `ETH{NEW_STABLE}` may fall into the CRYPTO group until the classification is updated. Similarly, `{NEW_STABLE}USDT` pairs (e.g. `TUSDUSDT`, `FDUSDUSDT`) may fall into the USDT group. Run `bybit_verify_groups.py` periodically and check for unmatched symbols or unexpected symbols in CRYPTO and USDT.
>
> In the future, the project is planned to migrate from static regex-based group definitions to dynamic group formation based on server data, which will improve reliability.


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
filename                               size      [status]
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