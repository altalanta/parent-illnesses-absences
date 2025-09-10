"""Helper to add/commit/push changes.

This script does NOT store tokens. Configure your remote first, e.g.:

    git remote add origin git@github.com:<USERNAME>/parent-illness-absences.git
    git push -u origin main

If you prefer HTTPS + PAT, use your Git credentials helper or Keychain.
"""

from __future__ import annotations

import subprocess as sp
from pathlib import Path


def run(cmd: list[str]) -> None:
    print("$", " ".join(cmd))
    sp.check_call(cmd)


def main() -> None:
    repo = Path(__file__).resolve().parents[1]
    run(["git", "add", "."]) 
    run(["git", "commit", "-m", "update: work in progress" ])
    run(["git", "push"])


if __name__ == "__main__":
    main()

