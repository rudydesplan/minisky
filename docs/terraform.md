# Terraform integration guide

The tracked `terraform/` configuration is a reproducible compatibility example
for the official Google provider. Its tested local scope is deliberately
limited to BigQuery dataset/table metadata, an IAM service account, and a
Docker-backed Storage bucket. The fixture also contains optional local
Phase-15 Redis and Spanner resources and a bounded Phase-16 custom network plus
regional IPv4 subnetwork, disabled by default.

## Local profile

Start MiniSky, then initialize and apply the example:

```bash
terraform -chdir=terraform init -lockfile=readonly
terraform -chdir=terraform apply -var-file=local.tfvars.example
```

The local profile uses a dummy access token and canonical service endpoints
under `/_minisky`. MiniSky does not validate that token against Google.

Inspect the next plan before destroying the stack:

```bash
terraform -chdir=terraform plan -detailed-exitcode \
  -var-file=local.tfvars.example
terraform -chdir=terraform destroy -var-file=local.tfvars.example
```

A no-drift plan exits with status `0`; changes exit with status `2`.

## Cloud profile

The same resources can target Google Cloud:

```bash
cp terraform/cloud.tfvars.example terraform/cloud.tfvars
# Edit cloud.tfvars with your project ID.
terraform -chdir=terraform apply -var-file=cloud.tfvars
```

The cloud profile does not set custom endpoints or an access token. The Google
provider therefore uses its normal endpoints and Application Default
Credentials. Do not commit credential files or populated variable files.

## Reproducibility

The repository pins Google provider `7.41.0` and commits
`terraform/.terraform.lock.hcl`. CI uses Terraform `1.15.8`, checks formatting,
initializes with the lock file in read-only mode, and validates the
configuration.

The opt-in integration gate performs apply, direct resource assertions, a
no-drift plan, destroy, and post-destroy assertions:

```bash
MINISKY_TERRAFORM_INTEGRATION=1 ./scripts/terraform-integration.sh
```

The script requires Docker because the current MiniSky daemon creates an
isolated Docker network and Storage uses `fake-gcs-server`. It refuses to run
when existing MiniSky containers or networks are present.

Set `MINISKY_TERRAFORM_PHASE15=1` to opt the guarded script into the Redis and
Spanner Terraform resources. The separate Phase-15 emulator integration remains
the authoritative data-plane gate.

The bounded Phase-16 network and subnetwork have a separate guarded lifecycle
and import gate:

```bash
MINISKY_PHASE16_SUBNETWORK_TERRAFORM_INTEGRATION=1 \
  ./scripts/phase16-subnetwork-terraform-integration.sh
```

It applies only the opt-in network resources, requires zero drift before and
after daemon restart, detaches and imports both canonical IDs without deleting
the API resources or Docker bridge, repeats the restart/no-drift check, then
destroys in dependency order and verifies durable API and bridge cleanup.

The metadata durability subset has a separate opt-in gate:

```bash
MINISKY_STATE_DURABILITY_INTEGRATION=1 \
  ./scripts/state-durability-integration.sh
```

It covers persisted BigQuery and IAM resources across restart and clean-profile
metadata export/import. It does not claim that Storage objects or other Docker
data are included in snapshots.

See [Terraform compatibility](terraform-compatibility.md) for exact endpoints,
tested versions, CI gating, and unsupported resource families.
