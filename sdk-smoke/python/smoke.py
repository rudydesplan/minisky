#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
import time
from typing import Any

from google.auth.credentials import AnonymousCredentials
from googleapiclient.discovery import Resource, build


DEFAULT_PROJECT_ID = "local-dev-project"


def client(service: str, version: str, endpoint: str) -> Resource:
    return build(
        service,
        version,
        credentials=AnonymousCredentials(),
        client_options={"api_endpoint": endpoint},
        cache_discovery=False,
    )


def main() -> None:
    gateway = os.environ.get("MINISKY_ENDPOINT", "").strip().rstrip("/")
    if not gateway:
        raise RuntimeError("MINISKY_ENDPOINT is required")
    project_id = os.environ.get("MINISKY_PROJECT_ID", DEFAULT_PROJECT_ID).strip()
    secondary_project_id = os.environ.get(
        "MINISKY_SECONDARY_PROJECT_ID", "local-secondary-project"
    ).strip()

    bigquery = client(
        "bigquery",
        "v2",
        f"{gateway}/_minisky/bigquery/bigquery/v2/",
    )
    iam = client("iam", "v1", f"{gateway}/_minisky/iam/v1/")
    iam_credentials = client(
        "iamcredentials",
        "v1",
        f"{gateway}/_minisky/iamcredentials/",
    )
    storage = client("storage", "v1", f"{gateway}/_minisky/storage/storage/v1/")

    suffix = f"{time.time_ns() % 1_000_000_000:09d}"
    dataset_id = f"minisky_py_{suffix}"
    table_id = "events"
    account_id = f"minisky-py-{suffix}"
    account_email = f"{account_id}@{project_id}.iam.gserviceaccount.com"
    account_name = f"projects/{project_id}/serviceAccounts/{account_email}"
    bucket_name = f"minisky-py-{suffix}"

    dataset_created = False
    secondary_dataset_created = False
    table_created = False
    account_created = False
    bucket_created = False
    try:
        dataset: dict[str, Any] = {
            "datasetReference": {
                "projectId": project_id,
                "datasetId": dataset_id,
            },
            "location": "US",
        }
        bigquery.datasets().insert(projectId=project_id, body=dataset).execute()
        dataset_created = True
        secondary_dataset = {
            "datasetReference": {
                "projectId": secondary_project_id,
                "datasetId": dataset_id,
            },
            "location": "US",
        }
        bigquery.datasets().insert(
            projectId=secondary_project_id, body=secondary_dataset
        ).execute()
        secondary_dataset_created = True

        table: dict[str, Any] = {
            "tableReference": {
                "projectId": project_id,
                "datasetId": dataset_id,
                "tableId": table_id,
            },
            "schema": {
                "fields": [
                    {"name": "message", "type": "STRING", "mode": "NULLABLE"}
                ]
            },
        }
        bigquery.tables().insert(
            projectId=project_id,
            datasetId=dataset_id,
            body=table,
        ).execute()
        table_created = True

        got_dataset = (
            bigquery.datasets()
            .get(projectId=project_id, datasetId=dataset_id)
            .execute()
        )
        if got_dataset["datasetReference"]["datasetId"] != dataset_id:
            raise RuntimeError("dataset round trip returned the wrong ID")
        got_secondary = (
            bigquery.datasets()
            .get(projectId=secondary_project_id, datasetId=dataset_id)
            .execute()
        )
        if got_secondary["datasetReference"]["projectId"] != secondary_project_id:
            raise RuntimeError("secondary project dataset isolation failed")
        got_table = (
            bigquery.tables()
            .get(
                projectId=project_id,
                datasetId=dataset_id,
                tableId=table_id,
            )
            .execute()
        )
        if got_table["tableReference"]["tableId"] != table_id:
            raise RuntimeError("table round trip returned the wrong ID")

        account = (
            iam.projects()
            .serviceAccounts()
            .create(
                name=f"projects/{project_id}",
                body={
                    "accountId": account_id,
                    "serviceAccount": {
                        "displayName": "MiniSky Python SDK smoke",
                    },
                },
            )
            .execute()
        )
        account_created = True
        if account["email"] != account_email:
            raise RuntimeError("service account create returned the wrong email")
        got_account = (
            iam.projects()
            .serviceAccounts()
            .get(name=account_name)
            .execute()
        )
        if got_account["email"] != account_email:
            raise RuntimeError("service account round trip returned the wrong email")
        credential = (
            iam_credentials.projects()
            .serviceAccounts()
            .generateAccessToken(
                name=f"projects/-/serviceAccounts/{account_email}",
                body={
                    "scope": ["https://www.googleapis.com/auth/cloud-platform"],
                    "lifetime": "300s",
                },
            )
            .execute()
        )
        if not credential.get("accessToken") or not credential.get("expireTime"):
            raise RuntimeError("IAM Credentials returned an incomplete token response")

        bucket = (
            storage.buckets()
            .insert(
                project=project_id,
                body={"name": bucket_name, "location": "US"},
            )
            .execute()
        )
        bucket_created = True
        if bucket["name"] != bucket_name:
            raise RuntimeError("storage bucket create returned the wrong name")
        got_bucket = storage.buckets().get(bucket=bucket_name).execute()
        if got_bucket["name"] != bucket_name:
            raise RuntimeError("storage bucket round trip returned the wrong name")

        print(
            "Python SDK smoke passed: "
            f"dataset={dataset_id} table={table_id} "
            f"service_account={account_email} bucket={bucket_name}"
        )
    finally:
        if table_created:
            bigquery.tables().delete(
                projectId=project_id,
                datasetId=dataset_id,
                tableId=table_id,
            ).execute()
        if dataset_created:
            bigquery.datasets().delete(
                projectId=project_id,
                datasetId=dataset_id,
                deleteContents=True,
            ).execute()
        if secondary_dataset_created:
            bigquery.datasets().delete(
                projectId=secondary_project_id,
                datasetId=dataset_id,
                deleteContents=True,
            ).execute()
        if account_created:
            iam.projects().serviceAccounts().delete(name=account_name).execute()
        if bucket_created:
            storage.buckets().delete(bucket=bucket_name).execute()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Python SDK smoke failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
