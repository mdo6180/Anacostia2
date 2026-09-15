import argparse
import logging
import os
from logging import Logger
import shutil
from pathlib import Path
from uuid import uuid4
from dataclasses import asdict
import json

from anacostia.utils.debug import attach_debugger
from anacostia.utils.types import Artifact

from anacostia.utils.package import create_deterministic_tar, gzip_file, partition_file, sha256_file
from anacostia.utils.merkle_tree import ProofEntry, generate_proof, merkle_root
from anacostia.utils.chunk import ChunkInfo, TransferArtifact, TransferManifest, ChunkManifest, TransferManifestSignature