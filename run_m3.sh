#!/bin/bash
#SBATCH --job-name=m3_benchmark
#SBATCH --time=04:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=32G
#SBATCH --output=m3_benchmark_%j.out

cd /scratch/network/wh8114/COS568-LI-SP26
bash scripts/run_async_benchmark.sh
