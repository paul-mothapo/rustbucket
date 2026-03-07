#!/usr/bin/env bash
set -euo pipefail

if [[ $# -gt 0 ]]; then
  PYTHON_BIN="$1"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "Python was not found. Pass the interpreter explicitly, for example: bash ./scripts/run_rust_bucket.sh python3" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${REPO_ROOT}/.venv"

cd "${REPO_ROOT}"

tracked_outputs=(
  "README.md"
  "rust_projects.json"
  "rust_bucket_state.json"
)

resolve_venv_python() {
  if [[ -x "${VENV_DIR}/bin/python" ]]; then
    printf '%s\n' "${VENV_DIR}/bin/python"
    return 0
  fi
  if [[ -x "${VENV_DIR}/Scripts/python.exe" ]]; then
    printf '%s\n' "${VENV_DIR}/Scripts/python.exe"
    return 0
  fi
  return 1
}

if ! VENV_PYTHON="$(resolve_venv_python)"; then
  echo "Creating local virtual environment..."
  if ! "${PYTHON_BIN}" -m venv "${VENV_DIR}"; then
    echo "Failed to create a virtual environment with ${PYTHON_BIN}." >&2
    echo "On Ubuntu/WSL, install venv support with: sudo apt update && sudo apt install -y python3-venv" >&2
    exit 1
  fi
  VENV_PYTHON="$(resolve_venv_python)"
fi

if ! "${VENV_PYTHON}" -m pip --version >/dev/null 2>&1; then
  echo "pip is unavailable inside ${VENV_DIR}." >&2
  echo "On Ubuntu/WSL, install venv support with: sudo apt update && sudo apt install -y python3-venv" >&2
  exit 1
fi

echo "Installing Python dependencies..."
"${VENV_PYTHON}" -m pip install -r requirements.txt

echo "Running Rust Bucket fetch..."
"${VENV_PYTHON}" main.py once

existing_outputs=()
for path in "${tracked_outputs[@]}"; do
  if [[ -f "${path}" ]]; then
    existing_outputs+=("${path}")
  fi
done

if [[ "${#existing_outputs[@]}" -eq 0 ]]; then
  echo "Expected generated output files were not found." >&2
  exit 1
fi

echo "Staging generated files..."
git add -- "${existing_outputs[@]}"

if git diff --cached --quiet; then
  echo "No changes to commit."
  exit 0
fi

commit_date="$(date +%F)"
echo "Committing updates..."
git commit -m "Daily fetch: ${commit_date}"

echo "Pushing to origin..."
git push
