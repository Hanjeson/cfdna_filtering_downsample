#!/usr/bin/env bash
set -euo pipefail

echo "Starting Pipeline Setup..."

# 1. Detect Conda
if ! command -v conda &> /dev/null; then
    echo "Error: Conda not found. Please install Miniconda or Mambaforge first."
    exit 1
fi

CONDA_BASE=$(conda info --base)
source "$CONDA_BASE/etc/profile.d/conda.sh"

# 2. Create Controller Environment
if conda info --envs | grep -q "snakemake_controller"; then
    echo "Controller environment 'snakemake_controller' already exists."
else
    echo "Creating controller environment from envs/controller_env.yml..."
    conda env create -f envs/controller_env.yml
fi

# 3. Create Rule Environment (Optional but recommended)
# This forces snakemake to create the environment defined in the rules early
echo "Setup complete. You can now use run_pipeline.sh"