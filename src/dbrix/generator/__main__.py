"""`python -m dbrix.generator [--seed N] [--scale X]`: build prod_catalog (§3)."""

import argparse

import docker

from dbrix.config import load_config
from dbrix.generator import run_generator

config = load_config()
parser = argparse.ArgumentParser(prog="python -m dbrix.generator", description=__doc__)
parser.add_argument("--seed", type=int, default=config.generator.seed)
parser.add_argument("--scale", type=float, default=config.generator.scale, help="multiplies base row counts")
args = parser.parse_args()
run_generator(docker.from_env(), config.executor.image, config.prod_catalog_dir, config.manifest, args.seed, args.scale)
