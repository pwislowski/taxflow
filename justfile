set shell := ["bash", "-c"]

default:
    @just --list

install:
    uv sync

run-all *ARGS:
    uv run -m app.cli all {{ ARGS }}

# For subcommands with arguments
run CMD +ARGS:
    uv run -m app.cli {{ CMD }} {{ ARGS }}

test:
    uv run pytest

test-verbose:
    uv run pytest -vv

_docker_pull:
    docker pull metabase/metabase:latest

dashboard: _docker_pull
    #!/usr/bin/env bash
    set -e
    # Stop and remove existing container if running
    docker rm -f metabase || true
    # Create data dir if needed (adjust path as needed for your storage constraints)
    docker run -d \
        -p 3000:3000 \
        -v ./local:/app/local \
        --name metabase \
        metabase/metabase:latest

stop:
    docker stop metabase
    docker rm metabase

logs:
    docker logs -f metabase
