#!/bin/bash
set -o errexit
set -o pipefail
set -o nounset

IMPORT_DIR="${IMPORT_DIR:-/import}"
PHYGHTMAP_JOBS="${PHYGHTMAP_JOBS:-1}"
PHYGHTMAP_HGTDIR="${PHYGHTMAP_HGTDIR:-$IMPORT_DIR/hgt}"
PHYGHTMAP_DOWNLOAD_ONLY="${PHYGHTMAP_DOWNLOAD_ONLY:-0}"
PHYGHTMAP_SOURCE="${PHYGHTMAP_SOURCE:-view1,view3,srtm3}"
PHYGHTMAP_EXTRA_ARGS="${PHYGHTMAP_EXTRA_ARGS:-}"
EARTHEXPLORER_USER="${EARTHEXPLORER_USER:-}"
EARTHEXPLORER_PASSWORD="${EARTHEXPLORER_PASSWORD:-}"

PHYGHTMAP_EFFECTIVE_SOURCE=""
declare -a EXTRA_ARGS=()

function credentials_valid() {
    [ -n "$EARTHEXPLORER_USER" ] && \
    [ -n "$EARTHEXPLORER_PASSWORD" ] && \
    [ "$EARTHEXPLORER_USER" != "exampleUser" ] && \
    [ "$EARTHEXPLORER_PASSWORD" != "examplePassword" ]
}

function source_uses_srtm3() {
    local item
    for item in "$@"; do
        if [ "$item" = "srtm3" ]; then
            return 0
        fi
    done
    return 1
}

function resolve_effective_source() {
    local creds_ok=0
    local dropped_srtm3=0
    local raw item cleaned
    local -a requested=()
    local -a effective=()

    if credentials_valid; then
        creds_ok=1
    fi

    IFS=',' read -r -a requested <<<"$PHYGHTMAP_SOURCE"
    for raw in "${requested[@]}"; do
        cleaned="$(printf '%s' "$raw" | tr -d '[:space:]')"
        if [ -z "$cleaned" ]; then
            continue
        fi
        if [ "$cleaned" = "srtm3" ] && [ "$creds_ok" -ne 1 ]; then
            dropped_srtm3=1
            continue
        fi
        effective+=("$cleaned")
    done

    if [ "$dropped_srtm3" -eq 1 ] && [ "${#effective[@]}" -eq 0 ]; then
        echo "EarthExplorer credentials missing or placeholder; srtm3-only source cannot continue."
        echo "Create .earthexplorerCredentials from .earthexplorerCredentials.example and provide valid EARTHEXPLORER_USER/EARTHEXPLORER_PASSWORD."
        exit 1
    fi

    if [ "${#effective[@]}" -eq 0 ]; then
        echo "No usable contour source remains after evaluating PHYGHTMAP_SOURCE=$PHYGHTMAP_SOURCE"
        exit 1
    fi

    PHYGHTMAP_EFFECTIVE_SOURCE="$(IFS=,; printf '%s' "${effective[*]}")"

    if [ "$dropped_srtm3" -eq 1 ]; then
        echo "EarthExplorer credentials missing or placeholder; disabling srtm3"
    fi
    echo "Effective contour source list: $PHYGHTMAP_EFFECTIVE_SOURCE"
}

function first_poly_file() {
    local poly_file

    shopt -s nullglob
    for poly_file in "$IMPORT_DIR"/*.poly; do
        printf '%s\n' "$poly_file"
        shopt -u nullglob
        return 0
    done
    shopt -u nullglob

    echo "No poly file for import found."
    echo "Please mount the $IMPORT_DIR volume to a folder containing poly files."
    exit 404
}

function extra_args_array() {
    if [ -z "$PHYGHTMAP_EXTRA_ARGS" ]; then
        return 0
    fi

    # Extra args are split on shell whitespace on purpose to keep the interface simple.
    read -r -a EXTRA_ARGS <<<"$PHYGHTMAP_EXTRA_ARGS"
}

function move_generated_pbf() {
    local pbf_files=()

    shopt -s nullglob
    pbf_files=( *.pbf )
    shopt -u nullglob

    if [ "${#pbf_files[@]}" -eq 0 ]; then
        echo "phyghtmap did not produce any .pbf output"
        exit 1
    fi

    mv "${pbf_files[@]}" "$IMPORT_DIR"/
}

function run_phyghtmap() {
    local poly_file="$1"
    local -a cmd=()

    mkdir -p "$PHYGHTMAP_HGTDIR"
    extra_args_array
    resolve_effective_source

    phyghtmap --version

    cmd=(
        phyghtmap
        --polygon="$poly_file"
        --source="$PHYGHTMAP_EFFECTIVE_SOURCE"
        --hgtdir="$PHYGHTMAP_HGTDIR"
    )

    if source_uses_srtm3 ${PHYGHTMAP_EFFECTIVE_SOURCE//,/ }; then
        cmd+=(
            --earthexplorer-user="$EARTHEXPLORER_USER"
            --earthexplorer-password="$EARTHEXPLORER_PASSWORD"
        )
    fi

    if [ "$PHYGHTMAP_DOWNLOAD_ONLY" = "1" ]; then
        cmd+=(--download-only)
        echo "Running contour prefetch with cache dir $PHYGHTMAP_HGTDIR and source $PHYGHTMAP_EFFECTIVE_SOURCE"
    else
        cmd+=(
            --max-nodes-per-tile=0
            -s 10
            -0
            --pbf
            --jobs="$PHYGHTMAP_JOBS"
        )
        echo "Running contour generation with jobs=$PHYGHTMAP_JOBS cache dir $PHYGHTMAP_HGTDIR and source $PHYGHTMAP_EFFECTIVE_SOURCE"
    fi

    if [ "${#EXTRA_ARGS[@]}" -gt 0 ]; then
        cmd+=("${EXTRA_ARGS[@]}")
    fi

    "${cmd[@]}"

    if [ "$PHYGHTMAP_DOWNLOAD_ONLY" != "1" ]; then
        move_generated_pbf
    fi
}

function generate_osm_with_first_poly() {
    local poly_file

    poly_file="$(first_poly_file)"
    run_phyghtmap "$poly_file"
}

generate_osm_with_first_poly
