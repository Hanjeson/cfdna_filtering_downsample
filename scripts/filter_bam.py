#!/usr/bin/env python3
import os
import pathlib
import subprocess
import tempfile
import click
import sys

# ---------------------------------------------------------------------
# FILTER PARAMETERS
# ---------------------------------------------------------------------
MAPQ_MIN = 21
TLEN_ABS_MAX = 700
# Allowed Flags: 83, 99, 147, 163 (Implies Mapped, Paired, Proper Pair)
CHROMS = [f"chr{i}" for i in range(1, 23)] + ["chrX", "chrY"]

# ---------------------------------------------------------------------
# AWK SCRIPT
# ---------------------------------------------------------------------
# Robust AWK script to filter Flags, TLEN, and strand-specific Clipping.
# Uses [0-9][0-9]* for regex compatibility across all AWK versions.
AWK_SCRIPT = r"""
BEGIN {
    limit_tlen = 700
}

# 1. Always print the header
/^@/ { print; next }

{
    # 2. MAPQ Filter (Redundant to samtools -q, but double safety)
    if ($5 <= 20) next
    
    # 3. Flag Filter (Whitelist: 83, 99, 147, 163)
    if ($2 != 83 && $2 != 99 && $2 != 147 && $2 != 163) next

    # 4. TLEN Filter (Absolute value < 700)
    len = $9
    if (len < 0) len = -len
    if (len >= limit_tlen) next

    # 5. Clipping & Strand Logic
    # Bit 16 (0x10) in Flag checks for Reverse Strand
    # int($2 / 16) % 2 == 1 -> Reverse Strand
    is_reverse = (int($2 / 16) % 2)

    if (is_reverse) {
        # REVERSE STRAND (-): 
        # Discard if Clipping is at the END ($) of the CIGAR string.
        # Regex: Digit(s) followed by S or H at the end.
        if ($6 ~ /[0-9][0-9]*[SH]$/) next
    } else {
        # FORWARD STRAND (+): 
        # Discard if Clipping is at the START (^) of the CIGAR string.
        # Regex: Start, Digit(s), S or H.
        if ($6 ~ /^[0-9][0-9]*[SH]/) next
    }

    # If all checks pass: Print the line
    print
}
"""

@click.command()
@click.option("--input_bam", required=True, type=click.Path(exists=True, path_type=pathlib.Path), help="Input BAM file")
@click.option("--output_bam", required=True, type=click.Path(path_type=pathlib.Path), help="Output BAM file")
@click.option("--threads", default=4, type=int, help="Number of threads for compression")
@click.option("--samtools", default="samtools", type=str, help="Path to samtools executable")
def filter_bam(input_bam: pathlib.Path, output_bam: pathlib.Path, threads: int, samtools: str) -> None:
    """
    Filters BAM file based on Flags, MAPQ, TLEN, and strand-specific clipping.
    Uses samtools for I/O and AWK for logic to avoid version conflicts.
    """
    os.makedirs(output_bam.parent, exist_ok=True)
    
    # Use a temp file in the same directory to prevent partial files on failure
    tmp_dir = output_bam.parent
    with tempfile.NamedTemporaryFile(prefix=output_bam.stem + ".", suffix=".tmp.bam", dir=tmp_dir, delete=False) as tf:
        tmp_bam_path = pathlib.Path(tf.name)

    print(f"Starting pipeline for: {input_bam.name}", flush=True)

    # Pipeline Construction:
    # 1. samtools view: Reads BAM, filters Chromosomes & MAPQ -> Streams SAM
    cmd_read = [
        samtools, "view", "-h",
        "-q", str(MAPQ_MIN),
        str(input_bam),
        *CHROMS
    ]

    # 2. awk: Filters Flags, TLEN, and Clipping -> Streams SAM
    cmd_awk = ["awk", AWK_SCRIPT]

    # 3. samtools view: Compresses SAM -> Writes BAM
    cmd_write = [
        samtools, "view", "-b",
        "-@", str(max(1, threads - 1)), # One thread for main, rest for compression
        "-o", str(tmp_bam_path)
    ]

    try:
        # Popen Chain
        p1 = subprocess.Popen(cmd_read, stdout=subprocess.PIPE)
        p2 = subprocess.Popen(cmd_awk, stdin=p1.stdout, stdout=subprocess.PIPE, text=True) 
        p3 = subprocess.Popen(cmd_write, stdin=p2.stdout)

        # Close file descriptors to prevent deadlocks and send EOF
        p1.stdout.close()
        p2.stdout.close()

        # Wait for completion
        p3.communicate()

        # Error handling
        if p3.returncode != 0:
            raise subprocess.CalledProcessError(p3.returncode, cmd_write)
        
        # Wait for upstream processes (safety check)
        p1.wait()
        p2.wait()

        # Move Temp to Final
        print("Filtering done. Replacing temp file...", flush=True)
        tmp_bam_path.replace(output_bam)

        # Indexing
        print("Indexing...", flush=True)
        subprocess.run([samtools, "index", "-@", str(threads), str(output_bam)], check=True)
        print(f"Success! Output at: {output_bam}", flush=True)

    except Exception as e:
        print(f"CRITICAL ERROR during filtering: {e}", file=sys.stderr)
        if tmp_bam_path.exists():
            tmp_bam_path.unlink()
        sys.exit(1)

if __name__ == "__main__":
    filter_bam()