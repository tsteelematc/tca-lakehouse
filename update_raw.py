import dlt
import json
from decimal import Decimal
from pathlib import Path
from typing import Any


def _unmarshal_dynamodb_value(value: Any) -> Any:
    if not isinstance(value, dict):
        if isinstance(value, list):
            return [_unmarshal_dynamodb_value(v) for v in value]
        return value

    if len(value) == 1:
        dtype, inner = next(iter(value.items()))
        if dtype == "S":
            return inner
        if dtype == "N":
            return Decimal(inner)
        if dtype == "BOOL":
            return inner
        if dtype == "NULL":
            return None
        if dtype == "M":
            return {k: _unmarshal_dynamodb_value(v) for k, v in inner.items()}
        if dtype == "L":
            return [_unmarshal_dynamodb_value(v) for v in inner]
        if dtype == "SS":
            return list(inner)
        if dtype == "NS":
            return [Decimal(v) for v in inner]
        if dtype == "BS":
            return list(inner)
        if dtype == "B":
            return inner

    return {k: _unmarshal_dynamodb_value(v) for k, v in value.items()}


def load_dynamodb_json_file(path: Path) -> list[dict]:
    """
    Load a DynamoDB PIT-export JSON file and unmarshal DynamoDB JSON types.
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

    items: list[dict] = []
    for record in raw_records:
        item = record.get("Item", record)
        items.append(_unmarshal_dynamodb_value(item))
    return items


def synthesize_incremental(items: list[dict], file_path: Path) -> None:
    """
    PIT exports do not contain updated_at timestamps.
    We synthesize one using the file's modified time.
    """
    ts = file_path.stat().st_mtime
    for item in items:
        item["updated_at"] = ts


@dlt.source
def export_files_source(export_folder: str):
    """
    Reads all JSON files in the export folder and yields a single
    dlt resource that updates the RAW layer idempotently.
    """
    folder = Path(export_folder)

    for file_path in sorted(folder.glob("*.json")):
        items = load_dynamodb_json_file(file_path)
        synthesize_incremental(items, file_path)

        yield dlt.resource(
            items,
            name="dynamo_items",
            primary_key=["pk", "sk"],
            incremental=dlt.sources.incremental("updated_at")
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
