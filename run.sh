#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
compose_file="${root}/docker/compose.yaml"

if [[ $# -eq 0 ]]; then
  set -- up
fi

exec docker compose -f "${compose_file}" "$@"
