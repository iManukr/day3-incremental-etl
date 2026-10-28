"""Incrementally load payment transaction batches into data/transactions.csv.

Each batch in incoming/ is compared with the stored table and every record is
classified as new, updated, duplicate or invalid. New rows are inserted and
updated rows replaced. Re-running the loader never changes a correct table.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent
DATA_FILE = BASE_DIR / "data" / "transactions.csv"
INCOMING_DIR = BASE_DIR / "incoming"
CHECKPOINT_FILE = BASE_DIR / "state" / "checkpoint.json"
LOG_FILE = BASE_DIR / "logs" / "etl.log"

KEY = "transaction_id"
FIELDS = ["transaction_id", "customer_id", "merchant", "amount_kes", "status", "channel", "updated_at"]
VALID_STATUSES = {"PENDING", "COMPLETED", "FAILED"}

logger = logging.getLogger("incremental_etl")


class DataFileError(Exception):
    """The stored transactions file is missing or cannot be read."""


# Task 7: logging
def setup_logging(log_file: Path = LOG_FILE) -> None:
    """Log to the console and to logs/etl.log (appending, so every run is kept)."""
    log_file.parent.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter("%(asctime)s %(levelname)-5s %(message)s", "%Y-%m-%d %H:%M:%S")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    for handler in (logging.StreamHandler(sys.stdout), logging.FileHandler(log_file, encoding="utf-8")):
        handler.setFormatter(formatter)
        logger.addHandler(handler)


# Task 1: read the existing table
def parse_amount(value: Any) -> float:
    """Turn 1780, 1780.0 or "1,780.00" into a float. Raises ValueError otherwise."""
    if isinstance(value, bool) or value is None:
        raise ValueError(f"amount_kes is not a number ({value!r})")
    if isinstance(value, (int, float)):
        return float(value)
    return float(str(value).replace(",", "").strip())


def load_existing(path: Path = DATA_FILE) -> dict[str, dict[str, Any]]:
    """Read transactions.csv into a dict keyed by transaction_id."""
    if not path.is_file():
        raise DataFileError(f"Data file not found: {path}")
    existing: dict[str, dict[str, Any]] = {}
    try:
        with open(path, newline="", encoding="utf-8-sig") as handle:
            for line_no, row in enumerate(csv.DictReader(handle), start=2):
                row = {key: (value or "").strip() for key, value in row.items() if key}
                try:
                    row["amount_kes"] = parse_amount(row["amount_kes"])
                except (KeyError, ValueError) as exc:
                    raise DataFileError(f"{path.name} line {line_no}: bad amount_kes ({exc})") from exc
                existing[row[KEY]] = row
    except (OSError, UnicodeDecodeError, csv.Error) as exc:
        raise DataFileError(f"Could not read {path}: {exc}") from exc
    return existing


# Task 2: load a batch
def load_batch(path: Path) -> tuple[str, list[dict[str, Any]]] | None:
    """Return (batch_id, transactions), or None if the file is missing or malformed."""
    try:
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
    except FileNotFoundError:
        logger.error("Batch file not found, skipped: %s", path.name)
        return None
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        logger.error("Batch file %s is not valid JSON, skipped: %s", path.name, exc)
        return None

    if not isinstance(payload, dict):
        logger.error("Batch file %s is not a JSON object, skipped", path.name)
        return None
    batch_id = payload.get("batch_id")
    transactions = payload.get("transactions")
    if not isinstance(batch_id, str) or not batch_id.strip():
        logger.error("Batch file %s has no batch_id, skipped", path.name)
        return None
    if not isinstance(transactions, list):
        logger.error("Batch file %s has no transactions list, skipped", path.name)
        return None
    return batch_id.strip(), transactions


# Tasks 3 and 4: compare and classify
def validate_record(record: Any) -> tuple[dict[str, Any] | None, str | None]:
    """Return (normalised record, None) or (None, reason it is invalid)."""
    if not isinstance(record, dict):
        return None, "record is not a JSON object"

    clean: dict[str, Any] = {}
    for field in FIELDS:
        value = record.get(field)
        if field == "amount_kes":
            continue
        if value is None or not str(value).strip():
            return None, f"missing {field}"
        clean[field] = str(value).strip()

    try:
        clean["amount_kes"] = parse_amount(record.get("amount_kes"))
    except ValueError:
        return None, f"amount_kes is not a number ({record.get('amount_kes')!r})"
    if clean["amount_kes"] <= 0:
        return None, f"amount_kes must be positive ({clean['amount_kes']:.2f})"

    clean["status"] = clean["status"].upper()
    if clean["status"] not in VALID_STATUSES:
        return None, f"unknown status {clean['status']!r}"
    try:
        datetime.fromisoformat(clean["updated_at"])
    except ValueError:
        return None, f"updated_at is not an ISO timestamp ({clean['updated_at']!r})"
    return clean, None


def same_values(a: dict[str, Any], b: dict[str, Any]) -> bool:
    """True if two records hold the same data (amounts compared to the cent)."""
    return all(
        round(float(a[f]), 2) == round(float(b[f]), 2) if f == "amount_kes" else str(a[f]) == str(b[f])
        for f in FIELDS
    )


def classify(existing: dict[str, dict], batch: list[Any]) -> dict[str, list]:
    """Split a batch into new, updated, duplicate and invalid records.

    new        ID not in the table.
    updated    ID in the table, values differ, incoming updated_at is later.
    duplicate  Nothing to change: an exact repeat of the stored row or of an
               earlier record in this batch, or an older (stale) version of a
               row we already hold.
    invalid    Fails validation, or differs from the stored row with the same
               updated_at, so we cannot tell which version is right.

    duplicate entries are (record, reason) pairs. invalid entries are
    (label, reason) pairs, where label is the transaction_id or, if there
    is none, the record's position in the batch ("index 9").
    """
    buckets: dict[str, list] = {"new": [], "updated": [], "duplicate": [], "invalid": []}

    # Validate, and resolve IDs that appear more than once in the batch.
    latest: dict[str, dict] = {}
    for index, record in enumerate(batch):
        clean, reason = validate_record(record)
        if reason:
            txn_id = record.get(KEY) if isinstance(record, dict) else None
            buckets["invalid"].append((txn_id or f"index {index}", reason))
            continue
        txn_id = clean[KEY]
        if txn_id not in latest:
            latest[txn_id] = clean
        elif same_values(clean, latest[txn_id]):
            buckets["duplicate"].append((clean, "repeated within the batch"))
        elif clean["updated_at"] > latest[txn_id]["updated_at"]:
            buckets["duplicate"].append((latest[txn_id], "superseded by a later version in the batch"))
            latest[txn_id] = clean
        else:
            buckets["duplicate"].append((clean, "older than another version in the batch"))

    batch_ids = set(latest)
    existing_ids = set(existing)

    for txn_id in sorted(batch_ids - existing_ids):
        buckets["new"].append(latest[txn_id])

    for txn_id in sorted(batch_ids & existing_ids):
        incoming, stored = latest[txn_id], existing[txn_id]
        if same_values(incoming, stored):
            buckets["duplicate"].append((incoming, "already loaded, no changes"))
        elif incoming["updated_at"] > stored["updated_at"]:
            buckets["updated"].append(incoming)
        elif incoming["updated_at"] < stored["updated_at"]:
            buckets["duplicate"].append(
                (incoming, f"stale: updated_at {incoming['updated_at']} is older than stored {stored['updated_at']}")
            )
        else:
            buckets["invalid"].append(
                (txn_id, f"conflicts with stored row that has the same updated_at {stored['updated_at']}")
            )
    return buckets


# Task 5: upsert and save
def upsert(existing: dict[str, dict], new: list[dict], updated: list[dict]) -> dict[str, dict]:
    """Return a new table with new rows inserted and updated rows replaced."""
    state = {txn_id: dict(row) for txn_id, row in existing.items()}
    for row in updated:
        state[row[KEY]] = dict(row)
    for row in new:
        state[row[KEY]] = dict(row)
    return state


def save_warehouse(state: dict[str, dict], path: Path = DATA_FILE) -> None:
    """Write the table sorted by transaction_id, atomically.

    The rows go to a temp file in the same folder first, which then replaces
    the real file in one step (os.replace). A crash mid-write leaves the old
    file untouched.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDS, extrasaction="ignore")
            writer.writeheader()
            for _, row in sorted(state.items()):
                writer.writerow({**row, "amount_kes": f"{float(row['amount_kes']):.2f}"})
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    except BaseException:
        Path(tmp_name).unlink(missing_ok=True)
        raise


# Task 6: checkpoint
def read_checkpoint(path: Path = CHECKPOINT_FILE) -> dict[str, Any]:
    """Return the checkpoint, or an empty one if it does not exist or is unreadable."""
    empty = {"last_run_at": None, "processed_batches": [], "high_watermark": None, "row_count": None}
    if not path.is_file():
        return empty
    try:
        checkpoint = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Checkpoint %s is unreadable (%s); starting without it", path, exc)
        return empty
    if not isinstance(checkpoint, dict) or not isinstance(checkpoint.get("processed_batches"), list):
        logger.warning("Checkpoint %s has an unexpected shape; starting without it", path)
        return empty
    return {**empty, **checkpoint}


def write_checkpoint(checkpoint: dict[str, Any], path: Path = CHECKPOINT_FILE) -> None:
    """Save the checkpoint atomically, the same way as the data file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(checkpoint, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


# Task 8: the run
def show(value: Any) -> str:
    return f"{value:,.2f}" if isinstance(value, float) else str(value)


def log_counts(new: int = 0, updated: int = 0, duplicates: int = 0, invalid: int = 0) -> None:
    logger.info("New: %d | Updated: %d | Duplicates: %d | Invalid: %d", new, updated, duplicates, invalid)


def process_batch(path: Path, checkpoint: dict[str, Any]) -> None:
    """Classify one batch file, merge it into the table and update the checkpoint."""
    loaded = load_batch(path)
    if loaded is None:
        return  # already logged; skip this file and carry on
    batch_id, transactions = loaded

    if batch_id in checkpoint["processed_batches"]:
        logger.info("Batch %s already processed - skipping", batch_id)
        log_counts()
        return

    existing = load_existing()
    logger.info("Batch %s: %d records read", batch_id, len(transactions))
    buckets = classify(existing, transactions)
    log_counts(len(buckets["new"]), len(buckets["updated"]), len(buckets["duplicate"]), len(buckets["invalid"]))

    for label, reason in buckets["invalid"]:
        logger.warning("Invalid record %s: %s", label, reason)
    for row in buckets["new"]:
        logger.info("New %s: %s %s %s", row[KEY], row["merchant"], show(row["amount_kes"]), row["status"])
    for record, reason in buckets["duplicate"]:
        logger.info("Duplicate %s: %s", record[KEY], reason)
    for row in buckets["updated"]:
        before = existing[row[KEY]]
        changes = ", ".join(
            f"{f} {show(before[f])} -> {show(row[f])}" for f in FIELDS
            if f != "updated_at" and not same_values({**before, f: row[f]}, before)
        )
        logger.info("Updated %s: %s", row[KEY], changes or "updated_at only")

    if buckets["new"] or buckets["updated"]:
        state = upsert(existing, buckets["new"], buckets["updated"])
        save_warehouse(state)
    else:
        state = existing
        logger.info("No new or updated rows, %s left unchanged", DATA_FILE.name)

    # Only record the batch once the data file is safely saved.
    checkpoint["processed_batches"].append(batch_id)
    checkpoint["high_watermark"] = max(row["updated_at"] for row in state.values()) if state else None
    checkpoint["row_count"] = len(state)
    checkpoint["last_run_at"] = datetime.now().isoformat(timespec="seconds")
    write_checkpoint(checkpoint)


def main() -> int:
    setup_logging()
    started = time.perf_counter()
    logger.info("Run started")
    try:
        existing = load_existing()
        logger.info("Existing rows loaded: %d", len(existing))
        checkpoint = read_checkpoint()
        batch_files = sorted(INCOMING_DIR.glob("*.json"))
        if not batch_files:
            logger.warning("No batch files found in %s", INCOMING_DIR)
        for path in batch_files:
            process_batch(path, checkpoint)
        logger.info("Rows after load: %d", len(load_existing()))
    except DataFileError as exc:
        logger.error("%s. Nothing was changed.", exc)
        return 1
    except OSError as exc:
        logger.error("Could not save results: %s. The previous data file is untouched.", exc)
        return 1
    logger.info("Run finished in %.2fs", time.perf_counter() - started)
    return 0


if __name__ == "__main__":
    sys.exit(main())
