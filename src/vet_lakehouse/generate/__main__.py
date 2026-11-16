"""Allow ``python -m vet_lakehouse.generate`` as a shortcut for the generate stage."""

import sys

from vet_lakehouse.pipeline import main

if __name__ == "__main__":
    sys.exit(main(["generate", *sys.argv[1:]]))
