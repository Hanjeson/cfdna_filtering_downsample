#!/usr/bin/env bash
set -euo pipefail

# --- Settings ---
DEFAULT_CFG="configs/config_FinaleDB_260224.yaml"
SNAKEFILE="filtering_downsample.smk"
MAX_CORES=120
CONTROLLER_ENV="snakemake_controller"

# --- Dynamic Path Detection ---
# Detect where the user's conda is installed
if ! command -v conda &> /dev/null; then
    echo "Error: conda not found in PATH."
    exit 1
fi
MCD_HOME=$(conda info --base)
WORKDIR="$(cd "$(dirname "$0")" && pwd)"
ENV_PREFIX="$WORKDIR/.conda_envs"
mkdir -p "$ENV_PREFIX"

# --- Argument Parsing ---
if [[ -f "${1:-}" ]]; then
    CFG="$1"
    shift 
else
    CFG="$DEFAULT_CFG"
fi

source "$MCD_HOME/etc/profile.d/conda.sh"

echo "-----------------------------------------------------------"
echo "Running Pipeline (Portable Mode)"
echo "User:       $USER"
echo "Conda Base: $MCD_HOME"
echo "Config:     $CFG"
echo "-----------------------------------------------------------"

# Clear environment variables
unset PYTHONPATH PYTHONHOME CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_SHLVL

# Note: Using 'conda run' with the name we defined in setup.sh
conda run --no-capture-output -n "$CONTROLLER_ENV" snakemake \
  --snakefile "$SNAKEFILE" \
  --configfile "$CFG" \
  --directory "$WORKDIR" \
  --jobs "$MAX_CORES" \
  --printshellcmds \
  --rerun-incomplete \
  --keep-going \
  --software-deployment-method conda \
  --conda-prefix "$ENV_PREFIX" \
  --conda-frontend conda \
  --executor slurm \
  --latency-wait 60 \
  "$@"