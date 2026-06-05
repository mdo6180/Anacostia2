from setuptools import setup, find_packages
import os
import fnmatch
import pathlib
import shutil


def package_files(directory):
    """
    Recursively collects file paths within a directory relative to the anacostia directory.
    """
    anacostia_dir = os.path.abspath("anacostia")
    paths = []
    exclude = ["*__pycache__*", "*.pyc", "*.pyo", "*.DS_Store"]
    for root, _, filenames in os.walk(directory):
        for filename in filenames:
            pathname = os.path.relpath(os.path.join(root, filename), anacostia_dir)
            if not any([fnmatch.fnmatch(pathname, pattern) for pattern in exclude]):
                paths.append(pathname)
    return paths

static_files = package_files("anacostia/static")


# removing dist/ and anacostia.egg-info/ directories
shutil.rmtree("dist", ignore_errors=True)
shutil.rmtree("anacostia.egg-info", ignore_errors=True)


setup(
    name="anacostia",
    version="1.0.0",
    description="A framework for building MLOps pipelines",
    author="Minh-Quan Do",
    author_email="mdo9@gmu.edu",
    long_description=pathlib.Path("README.md").read_text(),
    long_description_content_type='text/markdown',
    packages=find_packages(),
    package_data={'anacostia': [*static_files]},
    include_package_data=True,
    exclude_package_data={
        '': ['__pycache__', '*.pyc', '*.pyo']
    },
    install_requires=[
        "pydantic",
        "fastapi", 
        "uvicorn[standard]",
        "httpx" 
    ],
    extras_require={
        "aws": ["boto3"]
    }
)