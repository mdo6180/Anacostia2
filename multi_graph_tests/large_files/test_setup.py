from pathlib import Path
import argparse
import shutil
import logging
import os


parser = argparse.ArgumentParser(description="Run the pipeline after restart test")
parser.add_argument("-r", "--restart", action="store_true", help="Flag to indicate if this is a restart")
args = parser.parse_args()

testing_artifacts_dir = Path("./testing_artifacts")
pipeline1_dir = testing_artifacts_dir / "pipeline1"
pipeline2_dir = testing_artifacts_dir / "pipeline2"
combined_log_path = testing_artifacts_dir / "combined.log"

if args.restart == False:
    if pipeline1_dir.exists() is True:
        shutil.rmtree(pipeline1_dir)
    pipeline1_dir.mkdir(parents=True, exist_ok=True)

    if pipeline2_dir.exists() is True:
        shutil.rmtree(pipeline2_dir)
    pipeline2_dir.mkdir(parents=True, exist_ok=True)

    os.remove(combined_log_path)