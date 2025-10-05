.PHONY: help install clean fmt lint test build dev deploy \
        rust-fmt rust-lint rust-test rust-build \
        wasm-build wasm-build-dev wasm-build-release \
        browser-install browser-dev browser-build browser-lint browser-typecheck browser-test \
        check ci

# Default target - show help
help:
	@echo "Arrow DB - Available Make Commands"
	@echo ""
	@echo "🚀 Quick Start:"
	@echo "  make install          - Install all dependencies (Rust + Browser)"
	@echo "  make dev              - Start development server"
	@echo "  make build            - Build everything (WASM + Browser)"
	@echo "  make check            - Run all checks (format, lint, typecheck, test)"
	@echo ""
	@echo "🦀 Rust Commands:"
	@echo "  make rust-fmt         - Format Rust code"
	@echo "  make rust-lint        - Run Clippy on Rust code"
	@echo "  make rust-test        - Run Rust tests"
	@echo "  make rust-build       - Build Rust workspace"
	@echo ""
	@echo "🌐 WASM Commands:"
	@echo "  make wasm-build-dev   - Build WASM in development mode"
	@echo "  make wasm-build-release - Build WASM in release mode"
	@echo ""
	@echo "⚛️  Browser Commands:"
	@echo "  make browser-install  - Install browser dependencies"
	@echo "  make browser-dev      - Start Vite dev server"
	@echo "  make browser-build    - Build browser app"
	@echo "  make browser-lint     - Lint browser code"
	@echo "  make browser-typecheck - Check TypeScript types"
	@echo "  make browser-test     - Run browser tests"
	@echo ""
	@echo "🧹 Cleanup:"
	@echo "  make clean            - Remove build artifacts"
	@echo ""
	@echo "🔄 CI/CD:"
	@echo "  make ci               - Run full CI pipeline locally"
	@echo ""

# Install all dependencies
install: browser-install
	@echo "✅ All dependencies installed"

# ============================================================================
# Rust Commands
# ============================================================================

rust-fmt:
	@echo "🦀 Formatting Rust code..."
	cargo fmt --all

rust-lint:
	@echo "🦀 Running Clippy..."
	cargo clippy --all-targets --all-features -- -D warnings

rust-test:
	@echo "🦀 Running Rust tests..."
	cargo test --all --verbose

rust-build:
	@echo "🦀 Building Rust workspace..."
	cargo build --release

# ============================================================================
# WASM Commands
# ============================================================================

wasm-build-dev:
	@echo "🌐 Building WASM (development mode)..."
	wasm-pack build ./arrow-db-wasm --dev --target web --out-dir ../arrow-db-browser/arrow-db-wasm

wasm-build-release:
	@echo "🌐 Building WASM (release mode)..."
	wasm-pack build ./arrow-db-wasm --target web --out-dir ../arrow-db-browser/arrow-db-wasm

wasm-build: wasm-build-release

# ============================================================================
# Browser Commands
# ============================================================================

browser-install:
	@echo "⚛️  Installing browser dependencies..."
	cd arrow-db-browser && pnpm install

browser-dev: wasm-build-dev
	@echo "⚛️  Starting Vite dev server..."
	cd arrow-db-browser && pnpm dev

browser-build:
	@echo "⚛️  Building browser app..."
	cd arrow-db-browser && pnpm build

browser-lint:
	@echo "⚛️  Linting browser code..."
	cd arrow-db-browser && pnpm lint

browser-typecheck:
	@echo "⚛️  Type checking browser code..."
	cd arrow-db-browser && pnpm typecheck

browser-test:
	@echo "⚛️  Running browser tests..."
	cd arrow-db-browser && pnpm test

# ============================================================================
# Combined Commands
# ============================================================================

# Format all code
fmt: rust-fmt
	@echo "✅ All code formatted"

# Lint all code
lint: rust-lint browser-lint
	@echo "✅ All linting passed"

# Run all tests
test: rust-test browser-test
	@echo "✅ All tests passed"

# Build everything
build: wasm-build-release browser-build
	@echo "✅ Full build complete"

# Run all checks (like CI but local)
check: rust-fmt rust-lint rust-test browser-typecheck browser-lint
	@echo "✅ All checks passed"

# Start development environment
dev: browser-dev

# Clean build artifacts
clean:
	@echo "🧹 Cleaning build artifacts..."
	cargo clean
	rm -rf arrow-db-browser/dist
	rm -rf arrow-db-browser/arrow-db-wasm
	rm -rf arrow-db-browser/node_modules
	@echo "✅ Cleanup complete"

# ============================================================================
# CI/CD Commands
# ============================================================================

# Run the full CI pipeline locally
ci: check build
	@echo "🎉 CI pipeline completed successfully"

# Deploy to GitHub Pages (usually done via CI)
deploy: build
	@echo "📦 Build complete - ready for deployment"
	@echo "ℹ️  Push to main branch to trigger automatic deployment"
