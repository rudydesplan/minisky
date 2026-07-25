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

    bigquery = client(
        "bigquery",
        "v2",
        f"{gateway}/_minisky/bigquery/bigquery/v2/",
    )
    iam = client("iam", "v1", f"{gateway}/_minisky/iam/v1/")

    suffix = f"{time.time_ns() % 1_000_000_000:09d}"
    dataset_id = f"minisky_py_{suffix}"
    table_id = "events"
    account_id = f"minisky-py-{suffix}"
    account_email = f"{account_id}@{project_id}.iam.gserviceaccount.com"
    account_name = f"projects/{project_id}/serviceAccounts/{account_email}"

    dataset_created = False
    table_created = False
    account_created = False
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

        print(
            "Python SDK smoke passed: "
            f"dataset={dataset_id} table={table_id} "
            f"service_account={account_email}"
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
        if account_created:
            iam.projects().serviceAccounts().delete(name=account_name).execute()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Python SDK smoke failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
