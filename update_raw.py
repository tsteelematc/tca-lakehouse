import dlt
import json
from pathlib import Path
from typing import Any


def _extract_dynamodb_scalar(value: Any) -> str | None:
    if not isinstance(value, dict) or len(value) != 1:
        return None
    dtype, inner = next(iter(value.items()))
    if dtype in {"S", "N", "BOOL"}:
        return str(inner)
    return None


def load_dynamodb_json_file(path: Path) -> list[dict]:
    """
    Load a DynamoDB PIT-export JSON file as records.
    Supports both JSON arrays and newline-delimited JSON objects.
    """
    content = path.read_text().strip()
    if not content:
        return []

    try:
        raw_records = json.loads(content)
        if isinstance(raw_records, dict):
            raw_records = [raw_records]
    except json.JSONDecodeError:
        raw_records = [json.loads(line) for line in content.splitlines() if line.strip()]

    return raw_records


def build_raw_rows(records: list[dict], file_path: Path) -> list[dict]:
    """
    Build raw-preserved rows that avoid nested table expansion.
    """
    file_modified_at = file_path.stat().st_mtime
    rows: list[dict] = []

    for record_index, record in enumerate(records, start=1):
        item = record.get("Item", record)
        rows.append(
            {
                "source_file": file_path.name,
                "record_index": record_index,
                "file_modified_at": file_modified_at,
                "pk": _extract_dynamodb_scalar(item.get("pk")) if isinstance(item, dict) else None,
                "sk": _extract_dynamodb_scalar(item.get("sk")) if isinstance(item, dict) else None,
                "payload_json": json.dumps(record, separators=(",", ":"), ensure_ascii=False),
            }
        )

    return rows


@dlt.source
def export_files_source(export_folder: str):
    """
    Reads all JSON files in the export folder and yields a raw-preserved
    dlt resource that avoids nested table fan-out.
    """
    folder = Path(export_folder)

    for file_path in sorted(folder.glob("*.json")):
        records = load_dynamodb_json_file(file_path)
        rows = build_raw_rows(records, file_path)

        yield dlt.resource(
            rows,
            name="dynamo_items_raw",
            primary_key=["source_file", "record_index"],
            write_disposition="merge",
        )


def run_pipeline():
    """
    Entry point for updating the RAW layer.
    """
    duckdb_path = Path(__file__).resolve().parent / "tca_data.duckdb"
    pipeline = dlt.pipeline(
        pipeline_name="raw_ingestion",
        destination=dlt.destinations.duckdb(str(duckdb_path)),
        dataset_name="raw"
    )

    pipeline.run(
        export_files_source("s3_export_files")
    )


if __name__ == "__main__":
    run_pipeline()
