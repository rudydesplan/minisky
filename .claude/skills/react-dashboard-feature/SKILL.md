---
name: react-dashboard-feature
description: "Add or repair a MiniSky embedded dashboard page, drawer, navigation item, typed API interaction, polling flow, or existing terminal WebSocket UI using the repository's React 18, MUI 9, wouter 3, TypeScript, Vite, and Go embed patterns. Do not use for backend-only APIs, generic React advice, a new frontend stack, or observability/event delivery logic."
---

# React Dashboard Feature

Match the dashboard that exists. Avoid introducing a second state, routing, styling, or data-fetching architecture for one feature.

## Inspect before editing

Read:

- `ui/package.json` and lockfile for exact versions and scripts.
- `ui/src/App.tsx` for navigation, layout, and wouter routes.
- `ui/src/theme.ts` and nearby components for MUI usage.
- `ui/src/contexts/` and `ui/src/hooks/useServices.ts` for shared state/polling.
- the closest `*Page.tsx` and `*ManagerDrawer.tsx`.
- `pkg/dashboard/api.go` for the real API surface.
- `ui/embed.go`, `Dockerfile`, and release/CI workflows for embedding.

Current facts:

- React 18.3, MUI/icons 9.0, wouter 3.10, TypeScript 5.6, and Vite 6.4 are pinned.
- The theme is a Google Cloud-style light theme, not dark mode.
- Routes and sidebar items are centralized in `App.tsx`.
- `useServices` polls `/api/services` and `/api/settings` every three seconds.
- The terminal drawer has a WebSocket; there is no generic reconnecting WebSocket hook.
- There is no UI unit-test runner or `test` script.
- Vite output under `ui/dist` is embedded by `ui/embed.go` with SPA fallback.

Do not claim undocumented endpoints, trace/log WebSockets, plugin marketplace, or test infrastructure exists.

## Define the vertical slice

Before coding, identify:

1. User-visible state and action.
2. Existing backend endpoint and exact JSON shape.
3. Loading, empty, error, success, disabled, and permission states.
4. Route/nav/drawer entry points.
5. Refresh mechanism: explicit action, existing polling, or existing WebSocket.
6. Acceptance check available in this repository.

If the backend endpoint is absent, either keep the UI task blocked on that contract or implement it only when application-code scope is authorized. Do not fabricate placeholder production data.

## Test-first approach

For backend behavior, write the Go API test first. For frontend behavior, choose the smallest meaningful executable check:

- existing TypeScript compiler/lint failure for type and hook issues;
- a browser-level check against the local dashboard for interaction and routing;
- a focused UI test only if the task justifies adding a test runner and the user accepts that dependency/configuration change.

Do not add a frontend test framework solely to test static presentation. Still define the before/after acceptance scenario before implementation.

## Components and routing

- Use functional components and typed props; avoid `any`.
- Reuse nearby page/drawer structure and shared context.
- Add top-level routes and navigation through `NAV_ITEMS`/`Switch` in `App.tsx`.
- Use wouter `Link`, `Route`, and `useLocation`; do not add React Router.
- Keep stable resource IDs as React keys; array indexes are acceptable only for immutable, display-only rows.
- Use MUI components and `sx`/theme tokens. Reuse existing colors and spacing before adding one-off styles.
- Preserve keyboard access, visible focus, labels, semantic headings, and responsive drawer widths.
- Confirm destructive actions, disable duplicate submissions, and report failures without exposing raw secrets.

Do not turn the whole app into a new abstraction to add one page.

## Data fetching

Model response data as `unknown`, validate/narrow it, then store typed state. Treat non-2xx responses and malformed JSON as errors.

For polling:

- use a stable `useCallback` and clean up the interval;
- use `AbortController` to cancel in-flight work on unmount or dependency change;
- avoid overlapping requests when a poll takes longer than its interval;
- reset error state only after a successful validated response;
- pause or reduce polling when the page is hidden if traffic is material;
- never put unstable objects/functions in effect dependencies.

Use `npm ci`, not `npm install`, for deterministic repository validation.

## WebSockets

Reuse the terminal drawer pattern only for terminal work. If a reusable socket hook is genuinely needed, test and implement:

- URL construction using `ws:`/`wss:` from the current page;
- open/message/error/close transitions;
- bounded exponential reconnect with jitter;
- cancellation of reconnect timers on unmount or URL change;
- one live socket per hook instance;
- message parsing/validation and a bounded buffer;
- explicit terminal/auth failure behavior.

Do not create a socket in an `onclose` callback without reattaching handlers. Never render untrusted terminal/log content as HTML.

## API and security

- Build query strings with `URLSearchParams`/`encodeURIComponent`.
- Keep credentials and tokens out of URLs, local storage, alerts, and console logs.
- Do not use `dangerouslySetInnerHTML` for API data.
- Add CSRF/auth handling only if the backend contract has it; do not imply the local dashboard is authenticated.
- Show actionable, sanitized errors. Avoid dumping full backend bodies into the UI.
- For install/start/stop actions, reflect pending state and refresh only after checking `response.ok`.

## Build and embedding

The production sequence is:

```bash
cd ui
npm ci
npm run lint
npm run build
cd ..
go test ./pkg/dashboard ./ui
go build -trimpath ./cmd/minisky
```

`npm run build` runs `tsc -b && vite build`. The Go package requires `ui/dist` to exist because of `//go:embed dist/*`. Do not hand-edit `ui/dist`; build it.

Client-side routes rely on the SPA fallback. API routes must be registered by the dashboard server before the UI fallback; do not add `/api/*` routes to wouter as a substitute.

## Acceptance gates

- The acceptance scenario is written before implementation.
- Loading, empty, error, success, and mutation-pending states behave correctly.
- Route navigation works by click and direct refresh.
- Polling/socket work stops on unmount; no duplicate intervals, timers, or sockets remain.
- API responses are checked and narrowed without `any`.
- Keyboard and responsive behavior are verified for changed controls.
- `npm ci`, lint, and build pass.
- Relevant Go dashboard tests and the embedded binary build pass.
- A browser check covers the changed path when no focused UI test exists.
- Generated `ui/dist` is not treated as source, and no unrelated component is reformatted.

Report the route/component, endpoint contract, refresh mechanism, and validations run.
