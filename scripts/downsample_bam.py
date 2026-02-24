#!/usr/bin/env python3
import os
import pathlib
import shutil
import subprocess
import click
import pysam

def count_reads(bam_path):
    """Counts total reads (mapped + unmapped) using the index."""
    try:
        with pysam.AlignmentFile(str(bam_path), "rb") as bam:
            return bam.mapped + bam.unmapped
    except Exception:
        # Fallback if index is broken
        return int(subprocess.check_output(["samtools", "view", "-c", str(bam_path)]))

@click.command()
@click.option('--input_bam', required=True, type=click.Path(exists=True, path_type=pathlib.Path))
@click.option('--output_bam', required=True, type=click.Path(path_type=pathlib.Path))
@click.option('--threads', default=4, type=int)
@click.option("--n_reads_threshold", type=int, required=True)
@click.option("--seed", type=int, default=42)
@click.option("--samtools", default="samtools")
def main(input_bam, output_bam, threads, n_reads_threshold, seed, samtools):
    # Ensure output directory exists
    output_bam.parent.mkdir(parents=True, exist_ok=True)

    click.echo(f"Processing: {input_bam.name}")
    total_reads = count_reads(input_bam)
    click.echo(f"Total reads: {total_reads}")

    if total_reads <= n_reads_threshold:
        click.echo("Threshold not reached. Copying original file.")
        shutil.copy(str(input_bam), str(output_bam))
        # Also copy index if it exists, otherwise create it
        if pathlib.Path(str(input_bam) + ".bai").exists():
            shutil.copy(str(input_bam) + ".bai", str(output_bam) + ".bai")
        else:
            subprocess.run([samtools, "index", "-@", str(threads), str(output_bam)], check=True)
    else:
        fraction = n_reads_threshold / total_reads
        # Format for samtools: SEED.FRACTION
        subsample_param = f"{seed + fraction:.6f}"
        click.echo(f"Subsampling with fraction {fraction:.6f} (param: {subsample_param})")

        cmd = [
            samtools, "view",
            "-@", str(threads),
            "-b",
            "-s", subsample_param,
            "-o", str(output_bam),
            str(input_bam)
        ]
        subprocess.run(cmd, check=True)
        subprocess.run([samtools, "index", "-@", str(threads), str(output_bam)], check=True)

    click.echo("Done.")

if __name__ == '__main__':
    main()