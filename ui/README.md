# MiniSky Dashboard

The dashboard is a React, TypeScript, Vite, and Material UI application embedded
in the MiniSky Go binary.

## Development

```bash
npm ci
npm run dev
```

Vite serves the development UI with hot module replacement. API requests expect
the MiniSky management API to be available at `http://localhost:8081/api`.

## Verification

```bash
npm run lint
npm run build
npm audit
```

The production build is written to `dist/`. `ui/embed.go` embeds that directory,
so build the UI before compiling MiniSky from source.
