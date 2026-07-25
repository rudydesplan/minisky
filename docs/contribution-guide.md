# MiniSky Developer & Contributing Guide

MiniSky is built to be extensible. If a GCP service is missing, add either a
native Go shim or a Docker-backed emulator through the source registry.

## 1. Project Structure
- **/cmd/minisky:** CLI entry points.
- **/pkg/router:** Reverse proxy and protocol detection.
- **/pkg/orchestrator:** Docker lifecycle and container management.
- **/pkg/shims:** API translation layers (GCE, GKE, BigQuery, Dataproc).
- **/pkg/config/images.json:** Embedded Docker emulator and image definitions.

---

## 2. Developing a Service Shim
When no emulator exists (e.g., for Compute Engine), you must build a "Shim." 

### Step 1: Define the API Surface
Identify the REST/gRPC endpoints that need to be emulated. Implement these handlers in a new package under `pkg/shims/<service-name>`.

### Step 2: Register with the Validator
Enable **High-Fidelity** validation by linking your shim to the corresponding GCP Discovery Document. MiniSky will then automatically validate incoming JSON against the official GCP schema.

### Step 3: Implement Async Operations (LRO)
If the action is asynchronous in GCP (e.g., creating a cluster), your shim must register a task with the **LRO Manager**. The shim should return an `Operation` object immediately and update the operation status once the local Docker container is ready.

### Step 4: Bridge to Local Resources
Your shim should map GCP concepts to Docker or local OS primitives.
- **Example (GCE):** `instances.insert` -> `docker run --name <vm-name>`.
- **Example (BigQuery):** `jobs.query` -> `duckdb.Query(...)`.

### Step 5: Register the Shim
Register the domain with `registry.Register` from the shim's `init` function,
then add a blank import to `pkg/shims/registry_init.go`. For a direct Docker
backend, add its image definition to `pkg/config/images.json` and register its
domain with `registry.RegisterLazyDocker`.

---

## 3. Key Implementation Principles
1. **API Fidelity:** Always match the GCP response schema exactly, including headers like `x-goog-generation`.
2. **Behavioral Fidelity:** Don't just return 200 OK. If GCP uses LROs, return an Operation. If GCP has eventual consistency, simulate it.
3. **Lazy Loading:** Docker-backed services should use the router's lazy registration when no native shim is required.
4. **State Clarity:** Document whether a service is in-memory, file-backed, or volume-backed. Do not imply persistence for in-memory resources.

---

## 4. Local Testing
To test your new service integration:
1. Add tests for routing, validation, and service behavior.
2. Run `go test -race ./cmd/... ./pkg/...`.
3. Rebuild the UI with `cd ui && npm ci && npm run lint && npm run build`.
4. Build the MiniSky binary: `go build ./cmd/minisky`.
5. Run your service using Terraform (point `custom_endpoint` to `localhost:8080`).
6. Verify the resources appear in the **MiniSky Dashboard**.
