"""Ensures the project root (this file's directory) is importable as `engine`, `main`, etc."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
