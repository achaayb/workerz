import asyncio
import argparse
import os
import sys

parser = argparse.ArgumentParser(prog="python -m workerz.worker")
parser.add_argument("file", help="path to tasks .py file")
args = parser.parse_args()

if not os.environ.get("WORKERZ_LABELS"):
    print("ERROR: WORKERZ_LABELS env var is required", file=sys.stderr)
    sys.exit(1)

from workerz.worker.worker import main
asyncio.run(main(filepath=args.file))
