"""Allows ``python -m mistral_release`` to run the CLI."""

import sys

from mistral_release.cli import main

if __name__ == "__main__":
    sys.exit(main())
