# Day 3: Incremental ETL Pipeline

Lipa Haraka keeps a running table of M-Pesa and card transactions in `data/transactions.csv`. Each morning the payment gateway sends a JSON batch of new or changed transactions. `incremental_etl.py` merges a batch into the table: new transactions are inserted, changed ones updated, and repeats ignored. It can be run any number of times and always ends with the same correct table, so a manual re-run can't double-count a batch.

## Project structure

```text
day3-incremental-etl/
|-- incremental_etl.py
|-- data/transactions.csv          the table (20 rows before the first run, 27 after)
|-- incoming/batch_2026-10-21.json
|-- state/checkpoint.json
|-- logs/etl.log
|-- pyproject.toml / uv.lock       uv project files
|-- requirements.txt
`-- README.md
```

## How to run

The project uses [uv](https://docs.astral.sh/uv/) and only the Python standard library, so there are no packages to install.

```powershell
cd E:\Data\day3-incremental-etl
uv run incremental_etl.py
```

uv creates the virtual environment with Python 3.12 the first time. Without uv, `python incremental_etl.py` (Python 3.12+) works the same.

To start again from the original 20 rows, copy the provided `transactions.csv` back into `data/` and delete `state/checkpoint.json`.

Every run logs to the console and appends to `logs/etl.log`. The exit code is 0 on success and 1 if the data file is missing or unreadable.

## Approach

| Task | Function | What it does |
|---|---|---|
| 1 | `load_existing(path)` | Reads `transactions.csv` with `csv.DictReader` into a dict keyed by `transaction_id`, converting `amount_kes` to float. |
| 2 | `load_batch(path)` | Reads a batch JSON file and returns `(batch_id, transactions)`. A missing file, invalid JSON, or no `batch_id`/`transactions` list is logged as an ERROR and the file is skipped. |
| 3 | `classify` | Uses sets on transaction IDs: `batch_ids - existing_ids` are unseen, `batch_ids & existing_ids` already exist, and only those overlapping keys get their field values compared. A dict (`latest`) handles IDs repeated within the batch. |
| 4 | `classify(existing, batch)` | Returns the `new`, `updated`, `duplicate` and `invalid` buckets (rules below). |
| 5 | `upsert(existing, new, updated)` / `save_warehouse(state, path)` | `upsert` copies the table, replaces updated rows and inserts new ones. `save_warehouse` writes the rows sorted by `transaction_id` to a temp file in `data/`, then swaps it in with `os.replace`. A crash mid-write leaves the old file untouched. |
| 6 | `read_checkpoint()` / `write_checkpoint()` | Read and write `state/checkpoint.json`. The checkpoint is written only after the data file is saved. |
| 7 | `setup_logging()` | `logging` with two handlers: the console and `logs/etl.log`. |
| 8 | `main()` | Runs every batch in `incoming/` in name order. Re-runs are safe in two independent ways (see below). |

No pandas is used; the merge is done with dicts and sets.

## Classification rules

Each batch record is first validated, then compared with the stored table.

| Bucket | Rule | What happens |
|---|---|---|
| **invalid** | Missing any field (including `transaction_id`), `amount_kes` not a number or not greater than 0, `status` not PENDING/COMPLETED/FAILED, `updated_at` not an ISO timestamp. Also: differs from the stored row but has the **same** `updated_at`, so there is no way to tell which version is right. | Not loaded. One WARNING line each. |
| **new** | Valid, and its `transaction_id` is not in the table. | Inserted. |
| **updated** | ID is in the table, at least one value differs, and the incoming `updated_at` is **later** than the stored one. | Stored row replaced. |
| **duplicate** | Nothing to change: an exact repeat of the stored row, a repeat of an earlier record in the same batch, or an **older** version of a row we already hold (stale). | Ignored, logged with the reason. |

### Why an older, different record is a duplicate (not updated or invalid)

MP-50012 is stored as FAILED at `2026-10-20T14:45`. The batch has a COMPLETED version from `13:00`, before the stored one. The gateway retries, so this is an earlier state of a transaction we already have, re-sent late. Applying it would roll the table back to an out-of-date status. It is not bad data either (every field is valid), so it doesn't belong in invalid. It is a record we have already loaded, in an older form, so it is classed as a duplicate and logged with the reason `stale: updated_at ... is older than stored ...`, which shows exactly why it was ignored.

### Repeats inside the batch

MP-50021 appears twice with identical values. The first copy is classified normally (new) and the second counts as a duplicate. If two copies of the same ID in one batch had different values, the one with the later `updated_at` would be used and the other counted as a duplicate.

### Assumptions

- `amount_kes` sent as a string with thousands separators (MP-50023, `"3,450.00"`) is a formatting difference, not bad data. It is converted to 3450.00 and loaded.
- A negative amount (MP-50026, `-350.00`) is invalid. Refunds and reversals should come through as their own status, not a negative payment, so it is held back for someone to check.
- Amounts are compared to the cent, so `820` and `820.00` count as the same.
- `updated_at` values are ISO strings in the same format, so comparing them as strings gives time order.
- `high_watermark` in the checkpoint is the latest `updated_at` in the table after the load.

## Results for batch_2026-10-21.json

| Bucket | Count | Records |
|---|---|---|
| New | 7 | MP-50021, MP-50022, MP-50023, MP-50024, MP-50025, MP-50027, MP-50028 |
| Updated | 3 | MP-50004 (PENDING to COMPLETED), MP-50008 (PENDING to FAILED), MP-50015 (960.00 to 1,060.00 and PENDING to COMPLETED) |
| Duplicate | 5 | MP-50002, MP-50013, MP-50019 (already loaded), MP-50021 (repeated in batch), MP-50012 (stale) |
| Invalid | 2 | record at index 9 (no transaction_id), MP-50026 (negative amount) |

17 records read = 7 + 3 + 5 + 2. The table goes from 20 to 27 rows.

## Idempotency: evidence

Re-running is safe in two independent ways:

1. **The checkpoint.** A batch whose `batch_id` is already in `processed_batches` is skipped.
2. **The comparison itself.** Even with no checkpoint, every record in an already-loaded batch now matches the table (or is older than it), so it lands in duplicate and nothing is written.

### Run 1

```text
2026-10-28 17:49:11 INFO  Run started
2026-10-28 17:49:11 INFO  Existing rows loaded: 20
2026-10-28 17:49:11 INFO  Batch BATCH-2026-10-21-01: 17 records read
2026-10-28 17:49:11 INFO  New: 7 | Updated: 3 | Duplicates: 5 | Invalid: 2
2026-10-28 17:49:11 WARNING Invalid record index 9: missing transaction_id
2026-10-28 17:49:11 WARNING Invalid record MP-50026: amount_kes must be positive (-350.00)
2026-10-28 17:49:11 INFO  New MP-50021: Chandarana Kileleshwa 1,780.00 COMPLETED
2026-10-28 17:49:11 INFO  New MP-50022: KPLC Prepaid 2,000.00 COMPLETED
2026-10-28 17:49:11 INFO  New MP-50023: Quickmart Nyali 3,450.00 COMPLETED
2026-10-28 17:49:11 INFO  New MP-50024: Goodlife Pharmacy 1,240.00 PENDING
2026-10-28 17:49:11 INFO  New MP-50025: Text Book Centre 4,320.00 COMPLETED
2026-10-28 17:49:11 INFO  New MP-50027: Nairobi Water 1,875.00 COMPLETED
2026-10-28 17:49:11 INFO  New MP-50028: Naivas Westlands 2,265.00 COMPLETED
2026-10-28 17:49:11 INFO  Duplicate MP-50021: repeated within the batch
2026-10-28 17:49:11 INFO  Duplicate MP-50002: already loaded, no changes
2026-10-28 17:49:11 INFO  Duplicate MP-50012: stale: updated_at 2026-10-20T13:00:00 is older than stored 2026-10-20T14:45:00
2026-10-28 17:49:11 INFO  Duplicate MP-50013: already loaded, no changes
2026-10-28 17:49:11 INFO  Duplicate MP-50019: already loaded, no changes
2026-10-28 17:49:11 INFO  Updated MP-50004: status PENDING -> COMPLETED
2026-10-28 17:49:11 INFO  Updated MP-50008: status PENDING -> FAILED
2026-10-28 17:49:11 INFO  Updated MP-50015: amount_kes 960.00 -> 1,060.00, status PENDING -> COMPLETED
2026-10-28 17:49:12 INFO  Rows after load: 27
2026-10-28 17:49:12 INFO  Run finished in 0.05s
```

### Run 2 (same batch, checkpoint present)

```text
2026-10-28 17:49:12 INFO  Run started
2026-10-28 17:49:12 INFO  Existing rows loaded: 27
2026-10-28 17:49:12 INFO  Batch BATCH-2026-10-21-01 already processed - skipping
2026-10-28 17:49:12 INFO  New: 0 | Updated: 0 | Duplicates: 0 | Invalid: 0
2026-10-28 17:49:12 INFO  Rows after load: 27
2026-10-28 17:49:12 INFO  Run finished in 0.02s
```

`data/transactions.csv` was compared byte for byte with the file after run 1 (`cmp`): identical.

### Run 3 (checkpoint deleted, then run again)

```text
2026-10-28 17:49:20 INFO  Run started
2026-10-28 17:49:20 INFO  Existing rows loaded: 27
2026-10-28 17:49:20 INFO  Batch BATCH-2026-10-21-01: 17 records read
2026-10-28 17:49:20 INFO  New: 0 | Updated: 0 | Duplicates: 15 | Invalid: 2
2026-10-28 17:49:20 WARNING Invalid record index 9: missing transaction_id
2026-10-28 17:49:20 WARNING Invalid record MP-50026: amount_kes must be positive (-350.00)
...    (15 "Duplicate ...: already loaded, no changes" lines)
2026-10-28 17:49:20 INFO  No new or updated rows, transactions.csv left unchanged
2026-10-28 17:49:20 INFO  Rows after load: 27
2026-10-28 17:49:20 INFO  Run finished in 0.01s
```

Without the checkpoint the batch is re-classified, but all 15 valid records are now duplicates, so the data file is not rewritten. Again `cmp` shows it is identical to the file after run 1, and the checkpoint is rebuilt. The full output of all three runs is in `logs/etl.log`.

### Final checkpoint

```json
{
  "last_run_at": "2026-10-28T17:49:20",
  "processed_batches": [
    "BATCH-2026-10-21-01"
  ],
  "high_watermark": "2026-10-21T11:05:00",
  "row_count": 27
}
```

## Error handling

| Situation | Result |
|---|---|
| Batch file is not valid JSON | `ERROR Batch file ... is not valid JSON, skipped`, other batches still run |
| Batch file has no `batch_id` or `transactions` list | `ERROR Batch file ... has no batch_id, skipped` |
| Batch file disappears before it is read | `ERROR Batch file not found, skipped` |
| `data/transactions.csv` missing or has a bad amount | `ERROR ... Nothing was changed.`, exit code 1 |
| Checkpoint file corrupt | WARNING, run continues without it (safe, see run 3) |
| Crash or disk error while saving | Temp file is removed, the old data file is untouched, the checkpoint is not updated, exit code 1 |

The batch-file cases and the missing data file were tested on a copy of the project with a broken `batch_broken.json` and a `batch_noid.json` added to `incoming/`.
