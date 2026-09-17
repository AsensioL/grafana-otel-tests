#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
compose_file="${root}/docker/compose.yaml"
env_file="${root}/docker/.env"

if [[ $# -eq 0 ]]; then
  set -- up
fi

if [[ ! -f "${env_file}" ]]; then
  echo "Missing ${env_file}. Copy docker/.env.example to docker/.env and set values." >&2
  exit 1
fi

exec docker compose -f "${compose_file}" --env-file "${env_file}" "$@"
