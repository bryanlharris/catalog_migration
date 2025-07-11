#!/bin/bash
set -euo pipefail

# Upload this repository to a Databricks workspace using the Databricks CLI.
# Excludes the .git directory and overwrites any existing workspace files.

usage() {
    echo "Usage: $0 <workspace-path>" >&2
    echo "Example: $0 /Users/me/catalog_migration" >&2
    exit 1
}

if [ "$#" -ne 1 ]; then
    usage
fi

WORKSPACE_PATH="$1"
REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
REPO_NAME="$(basename "$REPO_ROOT")"

if [ "$(basename "$WORKSPACE_PATH")" != "$REPO_NAME" ]; then
    echo "Workspace path '$WORKSPACE_PATH' must end with repo folder name '$REPO_NAME'." >&2
    exit 1
fi

# Verify databricks CLI is installed
if ! command -v databricks > /dev/null; then
    echo "Error: databricks CLI is not installed." >&2
    exit 1
fi

# Create temporary directory excluding .git
TMP_DIR="$(mktemp -d)"
rsync -a --exclude '.git' "$REPO_ROOT/" "$TMP_DIR/"

# Ensure the workspace directory exists
 databricks workspace mkdirs "$WORKSPACE_PATH"

# Import directory recursively, overwriting existing files
 databricks workspace import_dir "$TMP_DIR" "$WORKSPACE_PATH" --overwrite

rm -rf "$TMP_DIR"

 echo "Uploaded '$REPO_NAME' to workspace path '$WORKSPACE_PATH'."
