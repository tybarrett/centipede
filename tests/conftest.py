import os
import sys

# centipede imports itself by package name, so what has to be importable is the
# directory holding the repository rather than the repository itself.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(REPO_ROOT))
