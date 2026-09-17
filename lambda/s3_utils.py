import json
import os
import tempfile
import uuid


def upload_dataframe_to_s3(
    df,
    entity_name,
    year,
    month,
    day,
    s3_client,
    bucket_name
):

    filename = (
        f"{entity_name}_{uuid.uuid4().hex}.csv"
    )

    local_path = (
        f"/tmp/{filename}"
    )

    s3_path = (
        f"bronze/{entity_name}/"
        f"year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"{filename}"
    )

    df.to_csv(
        local_path,
        index=False
    )

    s3_client.upload_file(
        local_path,
        bucket_name,
        s3_path
    )

    return s3_path


def upload_json_lines_to_s3(
    records,
    entity_name,
    year,
    month,
    day,
    s3_client,
    bucket_name
):

    filename = (
        f"{entity_name}_{uuid.uuid4().hex}.ndjson"
    )

    local_path = os.path.join(tempfile.gettempdir(), filename)

    s3_path = (
        f"bronze/{entity_name}/"
        f"year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"{filename}"
    )

    with open(local_path, "w", encoding="utf-8") as f:

        for record in records:
            f.write(
                json.dumps(record, separators=(",", ":"), sort_keys=True)
                + "\n"
            )

    s3_client.upload_file(
        local_path,
        bucket_name,
        s3_path
    )

    return s3_path
