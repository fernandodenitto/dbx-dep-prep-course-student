"""Run the unit tests on Databricks: the bundle's first job task calls this file.

The argument is the folder that holds pyproject.toml, passed by the job as
${workspace.file_path}. On serverless the working directory is not guaranteed, so nothing
here relies on it.
"""

import os
import sys

import pytest

root = sys.argv[1]
sys.dont_write_bytecode = True      # workspace files are not a place for .pyc files
os.chdir(root)

code = pytest.main(["tests", "-v", "-p", "no:cacheprovider", f"--rootdir={root}"])
if code != 0:
    raise SystemExit(f"unit tests failed, pytest exit code {code}")
