import os
import time
import argparse


raw_artifacts_folder = "./testing_artifacts/data_store1"
dataset_path = "./testing_artifacts/dataset.txt"
record_path = "./testing_artifacts/record.txt"


parser = argparse.ArgumentParser(description="Run the pipeline after restart test")
parser.add_argument("-r", "--restart", action="store_true", help="Flag to indicate if this is a restart")
args = parser.parse_args()

if args.restart == False:
    if os.path.exists(dataset_path):
        os.remove(dataset_path)
    if os.path.exists(record_path):
        os.remove(record_path)


# Note: In a real implementation, these functions would interact with Anacostia's database.
def mark_artifact_used(artifact_path):
    with open(record_path, 'a') as record_file:
        record_file.write(f"USED: {artifact_path}\n")
    print(f"Marked artifact as used: {artifact_path}")

def mark_artifact_using(artifact_path):
    with open(record_path, 'a') as record_file:
        record_file.write(f"USING: {artifact_path}\n")
    print(f"Marked artifact as using: {artifact_path}")


# Note: users can inherit from these context managers to implement more complex logic if needed 
# (e.g., logging, error handling, opening files in binary mode, etc).
class OutputFileManager:
    def __init__(self, filename):
        self.filename = filename

        # the reason why we open in append mode is to avoid overwriting existing data in the dataset file on restarts
        # it is for this reason that we do not use 'w' mode here.
        self.mode = 'a'
        self.file = None
        
    def __enter__(self):
        mark_artifact_using(self.filename)
        self.file = open(self.filename, self.mode)
        return self.file
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        if exc_type is KeyboardInterrupt:
            pass
        else:
            mark_artifact_used(self.filename)

        self.file.close()


class InputFileManager:
    def __init__(self, filename):
        self.filename = filename
        self.mode = 'r'
        self.file = None
        
    def __enter__(self):
        mark_artifact_using(self.filename)
        self.file = open(self.filename, self.mode)
        return self.file
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        if exc_type is KeyboardInterrupt:
            pass
        else:
            mark_artifact_used(self.filename)

        self.file.close()


def get_artifacts_to_process(folder_path):
    # get all files in the folder as artifacts
    all_files = os.listdir(folder_path)
    artifacts = [os.path.join(folder_path, f) for f in all_files if os.path.isfile(os.path.join(folder_path, f))]
    artifacts.sort() # sort to ensure consistent order

    # filter out artifacts that have already been used
    with open(record_path, 'r') as record_file:
        lines = record_file.readlines()
        used_artifacts = []
        for line in lines:
            if line.startswith("USED: "):
                used_artifact = line[len("USED: "):].strip()
                if used_artifact.startswith(folder_path):
                    used_artifacts.append(used_artifact)

    # Note: it seems like it might not be necessary to filter out 'using' artifacts for this simple example. Just filter out used ones.
    unused_artifacts = [a for a in artifacts if a not in used_artifacts]
    return unused_artifacts


# Code below is an example of how users can use context managers to read artifacts and write to dataset.
# For data aggregation nodes, we can use the patter shown below to ensure that artifacts are marked as using/used properly 
# and ensure the pipeline can be restarted safely.
# The pattern is to use an outer context manager for writing to the dataset file (i.e., the outputted file), 
# and an inner context manager for reading each artifact file (i.e., the input files).

# In the case of training nodes, the outer context manager would be for writing the model file, and the inner context manager would be for reading the dataset.
# Suppose the dataset is an image classification dataset stored in a folder with multiple image files.
# We do not use the inner context manager to read each individual image file because each image file is used more than once during training (i.e., multiple epochs);
# thus, marking each image file as used after reading it would mean the training node cannot be restarted properly because all image files would be marked as used.
# Furthermore, marking each image file as used would make it very difficult to display the provenance graph for the trained model on the Anacostia dashboard
# because each image file would be considered a node in the provenance graph, which would clutter the graph and make it hard to interpret.
# Instead, we would use the inner context manager to read the entire dataset folder.
# This differs from data aggregation nodes, where restarts resume from the last unprocessed artifact rather than restarting the entire node.
# With that said, in the case where multiple datasets are used sequentially for training 
# (e.g., model is first trained on dataset A and then further trained on dataset B like in the case of multi-modal robotics and sim-to-real transfer), 
# we would use multiple inner context managers to read each dataset folder.
# In this case, when restart happens, we would start with the datasets whose training phase has not been completed yet (i.e., not marked as used).
# In the case where multiple datasets have to be used simultaneously (e.g., training with multiple data sources), 
# we can scope multiple InputFileManagers for datasets A and B within the same training node execution context, and so on.
# This way, when a restart happens, both datasets would be marked as using and we can restart the training from scratch and ingest both datasets again.
# Note: in the real implementation, we would hash the entire dataset folder and consider it as a single artifact (i.e., one dataset produces one model).
# IMPORTANT DESIGN PRINCIPLE:
# Anacostia tracks provenance and restart state at the level of semantic consumption,
# not at the level of physical file access.
# A file or folder should be treated as a single artifact if partial progress cannot
# be meaningfully resumed after a restart.
with OutputFileManager(dataset_path) as dataset_file:
    artifacts_to_process = get_artifacts_to_process(raw_artifacts_folder)
    print(f"Artifacts to process: {artifacts_to_process}")

    for filepath in artifacts_to_process:
        filename = os.path.basename(filepath)

        with InputFileManager(filepath) as artifact_file:
            content = artifact_file.read()
            time.sleep(2)  # Simulate some processing time
            dataset_file.write(f"{filename}: {content}\n")

        time.sleep(2)  # Simulate some time between processing artifacts to allow for testing when restart happens between artifacts processing
