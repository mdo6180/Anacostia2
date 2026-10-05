from pathlib import Path
import argparse
import shutil
import logging
import os


parser = argparse.ArgumentParser(description="Run the pipeline after restart test")
parser.add_argument("-p", "--pipeline", type=int, choices=[1, 2], default=None, help="Pipeline number to run")
args = parser.parse_args()

testing_artifacts_dir = Path("./testing_artifacts")
pipeline1_dir = testing_artifacts_dir / "pipeline1"
pipeline2_dir = testing_artifacts_dir / "pipeline2"
combined_log_path = testing_artifacts_dir / "combined.log"

def setup_pipeline1():
    if pipeline1_dir.exists() is True:
        shutil.rmtree(pipeline1_dir)
    pipeline1_dir.mkdir(parents=True, exist_ok=True)

def setup_pipeline2():
    if pipeline2_dir.exists() is True:
        shutil.rmtree(pipeline2_dir)
    pipeline2_dir.mkdir(parents=True, exist_ok=True)


if args.pipeline == 1:
    # If this is a fresh run of pipeline 1, we want to set up the pipeline1 directory.
    setup_pipeline1()

elif args.pipeline == 2:
    # If this is a fresh run of pipeline 2, we want to set up the pipeline2 directory.
    setup_pipeline2()

elif args.pipeline is None:
    # If we are running both pipelines fresh, we want to set up both directories and remove the combined log file to start fresh.
    setup_pipeline1()
    setup_pipeline2()
    os.remove(combined_log_path)