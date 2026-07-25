---
name: webapp-testing
description: Test and debug local web applications through safe browser interaction, screenshots, DOM inspection, console evidence, and focused UI checks. Use for MiniSky dashboard verification, local frontend regressions, or exploratory testing of a localhost app. Do not use this workflow against production or third-party sites unless the user explicitly authorizes it.
license: Apache-2.0; upstream terms at https://github.com/anthropics/skills/blob/main/skills/webapp-testing/LICENSE.txt
---

# Web Application Testing

Test observable behavior first and collect enough evidence to explain failures.
Prefer the browser tools already available in the agent environment. Write
Playwright scripts only when the project already has Playwright or the user asks
for a reusable automated test.

> Adapted for MiniSky from Anthropic's `webapp-testing` skill:
> https://github.com/anthropics/skills/tree/main/skills/webapp-testing

## MiniSky application shape

- The dashboard source is in `ui/` and uses React, TypeScript, and Vite.
- `cd ui && npm run dev` starts the Vite development server.
- The dashboard expects the MiniSky management API at
  `http://localhost:8081/api`.
- The installed UI scripts are `dev`, `build`, `lint`, and `preview`.
- Playwright is not a dependency in `ui/package.json`.
- This skill bundles no helper scripts or examples; do not reference nonexistent
  `scripts/` or `examples/` paths.

## Safety boundaries

- Test local URLs by default (`localhost`, `127.0.0.1`, or a user-approved
  development host).
- Never submit real secrets, payment data, or personal data.
- Do not perform deletes, irreversible mutations, account changes, or external
  side effects without explicit authorization.
- Use disposable test data and restore state when practical.
- Do not disable TLS checks, browser security controls, or authentication to
  make a test pass.
- Avoid broad crawling and unbounded loops. Stop after repeated failure, gather
  fresh evidence, and report the blocker.
- Preserve the user's active browser state when possible. Lock shared browser
  sessions if the available browser tooling requires it, and unlock when done.

## Workflow

### 1. Define the check

Identify:

- the local URL and expected behavior;
- whether a backend is required;
- allowed mutations and test data;
- success evidence: visible state, request, console output, or screenshot.

### 2. Reuse or start servers safely

Check whether the required server is already running before starting another.
For MiniSky UI-only work:

```bash
cd ui && npm run dev
```

If the flow needs live API data, start the documented MiniSky service separately
and verify port `8081` is available. Do not guess an undocumented root command.
Run long-lived servers in a managed background terminal and stop only servers
started for the test.

### 3. Inspect before acting

1. Navigate to the page.
2. Wait for a stable, relevant UI condition. `networkidle` is not universally
   reliable for apps with WebSockets or polling.
3. Capture an accessibility/DOM snapshot and, when visual layout matters, a
   screenshot.
4. Check console and failed network requests when available.
5. Choose resilient selectors from the rendered state.

Prefer selectors in this order:

1. accessible role and name;
2. associated label;
3. stable test ID;
4. stable text;
5. CSS selector only when no semantic selector works.

### 4. Exercise the smallest useful flow

Perform only the interactions needed to prove or reproduce the behavior. Assert
meaningful outcomes rather than merely confirming that a click occurred. Use
condition-based waits instead of fixed sleeps.

When a step fails, take a new snapshot before retrying. Do not repeat the same
action without a changed state or new hypothesis.

### 5. Verify and clean up

For dashboard code changes, run the relevant static checks:

```bash
cd ui && npm run lint
cd ui && npm run build
```

Close temporary browser contexts and stop temporary servers. Do not stop a
server that existed before the test.

## Reusable Playwright tests

Only add Playwright when explicitly requested or already configured. If adding a
dependency is in scope, consult current Playwright documentation and use the
repository's package manager rather than inventing a version. Confirm with the
user before installing dependencies.

A reusable test should:

- use the project's chosen Playwright runner and configuration;
- avoid hard-coded credentials and machine-specific paths;
- record screenshots or traces only where useful;
- isolate state and remain deterministic;
- close resources in cleanup hooks;
- document the required MiniSky backend and ports.

## Report

Return:

- behavior tested and environment used;
- passed and failed observations;
- console/network evidence relevant to failures;
- screenshot or trace paths, if created;
- cleanup performed and any untested dependency.