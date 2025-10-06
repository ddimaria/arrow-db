# Deployment Guide

This repository is configured with GitHub Actions to automatically build and deploy to GitHub Pages.

## GitHub Pages Setup

To enable GitHub Pages deployment:

1. Go to your repository on GitHub
2. Navigate to **Settings** → **Pages**
3. Under "Build and deployment", set:
   - **Source**: GitHub Actions
4. Save the changes

## Workflow Overview

The workflow (`.github/workflows/build-and-deploy.yml`) performs the following steps:

### On Pull Requests
- ✅ Lints Rust code (`cargo fmt`, `cargo clippy`)
- ✅ Tests Rust code (`cargo test`)
- ✅ Builds WASM in release mode
- ✅ Builds Vite static assets (without deploying)

### On Push to main/master
- ✅ Lints Rust code
- ✅ Tests Rust code
- ✅ Builds WASM in release mode
- ✅ Builds Vite static assets
- 🚀 Deploys to GitHub Pages

## Manual Deployment

You can also trigger a deployment manually:

1. Go to **Actions** tab in your repository
2. Select the "Build and Deploy" workflow
3. Click "Run workflow"
4. Select the branch and click "Run workflow"

## Base URL

The workflow automatically sets the base URL to `/<repository-name>/` for proper asset loading on GitHub Pages. If you're deploying to a custom domain, you may want to adjust the `BASE_URL` environment variable in the workflow file.

## Viewing Your Site

After successful deployment, your site will be available at:
- `https://<username>.github.io/<repository-name>/` (for repository pages)
- `https://<username>.github.io/` (for user/organization pages with custom settings)

Check the **Actions** tab to monitor deployment progress and the **Environments** section to see deployment history.
