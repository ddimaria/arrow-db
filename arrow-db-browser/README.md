# Arrow DB Browser

## Getting Started

First, install dependencies.

```bash
npm i -g pnpm
pnpm install
```

Next, build the wasm package.  For dev builds:

```bash
pnpm run build:wasm:dev
```

For release builds:

```bash
pnpm run build:wasm:release
```

Serve with hot reload at <http://localhost:5173>.

```bash
pnpm run dev
```

### Lint

```bash
pnpm run lint
```

### Typecheck

```bash
pnpm run typecheck
```

### Build

```bash
pnpm run build
```

### Test

```bash
pnpm run test
```

View and interact with your tests via UI.

```bash
pnpm run test:ui
```
