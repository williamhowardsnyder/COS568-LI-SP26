#! /usr/bin/env bash
#SBATCH --job-name=run_cos568_benchmarks
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=32G
#SBATCH --time=02:00:00

cd /scratch/network/wh8114/COS568-LI-SP26
module load anaconda3/2023.3

./scripts/run_benchmarks.sh