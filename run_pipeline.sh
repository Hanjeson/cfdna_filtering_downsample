#!/usr/bin/env bash
set -euo pipefail

# --- Settings ---
# Updated to match your new repo file names
DEFAULT_CFG="configs/config_FinaleDB_260224.yaml"
SNAKEFILE="filtering_downsample.smk"

MAX_CORES=120
MCD_HOME="/home/gpfs/o_mosermat/miniconda3"
CONTROLLER_ENV="snakemake_7"

WORKDIR="$(cd "$(dirname "$0")" && pwd)"
# Conda environments for the rules will be stored here
ENV_PREFIX="$WORKDIR/.conda_envs"
mkdir -p "$ENV_PREFIX"

# --- Argument Parsing ---
# If the first argument is an existing file, treat it as the config override
if [[ -f "${1:-}" ]]; then
    CFG="$1"
    shift 
else
    CFG="$DEFAULT_CFG"
fi

# All REMAINING arguments (like -n, --dry-run, --dag) are stored in "$@"
source "$MCD_HOME/etc/profile.d/conda.sh"

echo "-----------------------------------------------------------"
echo "Running cfDNA Filtering & Downsampling Pipeline"
echo "Workdir:    $WORKDIR"
echo "Snakefile:  $SNAKEFILE"
echo "Config:     $CFG"
echo "Extra flags: $@"
echo "-----------------------------------------------------------"

# Clear environment variables to prevent conflicts with the snakemake controller
unset PYTHONPATH PYTHONHOME CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_SHLVL

# Execute Snakemake using the slurm executor
"$MCD_HOME/bin/conda" run --no-capture-output -n "$CONTROLLER_ENV" snakemake \
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
  --conda-base-path "$MCD_HOME" \
  --precommand "source $MCD_HOME/etc/profile.d/conda.sh; unset PYTHONPATH PYTHONHOME; hash -r" \
  --executor slurm \
  --latency-wait 60 \
  "$@"