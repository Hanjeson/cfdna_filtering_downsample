import os

# Load configuration
INPUT_DIR = config["input_dir"]
FILTER_DIR = config["filter_dir"]
DOWNSAMPLE_DIR = config["downsample_dir"]
DS_TAGS = config["downsampling"]["tags"]
SEEDS = config["downsampling"]["seeds"]

# Automatically detect samples from the input directory
# Matches files like: EE85876.hg38.frag.bam -> sample = "EE85876"
SAMPLES, = glob_wildcards(os.path.join(INPUT_DIR, "{sample}.hg38.frag.bam"))

rule all:
    input:
        # Create the cross-product of Samples x Tags x Seeds
        expand(
            DOWNSAMPLE_DIR + "/{sample}/{sample}.{ds_tag}_s{seed}.bam",
            sample=SAMPLES,
            ds_tag=DS_TAGS.keys(),
            seed=SEEDS
        )

##############################################
# 1) Filtering (Intermediate Step)
##############################################
rule filtering:
    input:
        bam = INPUT_DIR + "/{sample}.hg38.frag.bam",
        bai = INPUT_DIR + "/{sample}.hg38.frag.bam.bai"
    output:
        # Both marked as temp to be deleted after downsampling is done
        bam = temp(FILTER_DIR + "/{sample}.filtered.bam"),
        bai = temp(FILTER_DIR + "/{sample}.filtered.bam.bai")
    log:
        "logs/filtering/{sample}.log"
    threads: 4
    conda: "envs/downsample.yml"
    resources:
        mem_mb = 32000,
        runtime = 480
    shell:
        r"""
        set -euo pipefail
        mkdir -p $(dirname {output.bam})
        
        PY=$(which python)
        ST=$(which samtools)

        "$PY" scripts/filter_bam.py \
            --input_bam {input.bam} \
            --output_bam {output.bam} \
            --threads {threads} \
            --samtools "$ST" \
            >> {log} 2>&1
        """

##############################################
# 2) Downsampling (Final Step)
##############################################
rule downsampling:
    input:
        # Depends on the output of the filtering rule
        bam = FILTER_DIR + "/{sample}.filtered.bam",
        bai = FILTER_DIR + "/{sample}.filtered.bam.bai"
    output:
        # Final results in sample-specific subfolders
        bam = DOWNSAMPLE_DIR + "/{sample}/{sample}.{ds_tag}_s{seed}.bam",
        bai = DOWNSAMPLE_DIR + "/{sample}/{sample}.{ds_tag}_s{seed}.bam.bai" 
    wildcard_constraints:
        ds_tag = r"ds[0-9p]+M",
        seed = r"[0-9]+" 
    log:
        "logs/downsampling/{sample}.{ds_tag}_s{seed}.log"
    threads: config["downsampling"]["threads"]
    conda: "envs/downsample.yml"
    params:
        # Lookup the read threshold based on the ds_tag wildcard
        n_reads = lambda w: DS_TAGS[w.ds_tag]
    resources:
        mem_mb = config["downsampling"]["mem_mb"],
        runtime = 120
    shell:
        r"""
        set -euo pipefail
        mkdir -p $(dirname {output.bam})
        
        ST=$(which samtools)
        PY=$(which python)

        "$PY" scripts/downsample_bam.py \
            --input_bam {input.bam} \
            --output_bam {output.bam} \
            --threads {threads} \
            --n_reads_threshold {params.n_reads} \
            --seed {wildcards.seed} \
            --samtools "$ST" \
            >> {log} 2>&1
        """