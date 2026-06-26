import boto3

s3 = boto3.client("s3")


def delete_prefix(
    bucket_name,
    prefix
):

    paginator = (
        s3.get_paginator(
            "list_objects_v2"
        )
    )

    for page in paginator.paginate(
        Bucket=bucket_name,
        Prefix=prefix
    ):

        if "Contents" not in page:
            continue

        objects = [
            {"Key": obj["Key"]}
            for obj in page["Contents"]
        ]

        s3.delete_objects(
            Bucket=bucket_name,
            Delete={
                "Objects": objects
            }
        )


def reset_data_lake(
    bucket_name
):

    delete_prefix(
        bucket_name,
        "bronze/"
    )

    delete_prefix(
        bucket_name,
        "silver/"
    )

    delete_prefix(
        bucket_name,
        "gold/"
    )
